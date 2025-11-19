"""Helpers for interacting with the Underdog admin API."""

from __future__ import annotations

import asyncio
import json
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

import httpx
from loguru import logger

from . import db
from .config import settings
from .dispatcher import bot

LOGIN_PATH = "/api/login"
ORDERS_PATH = "/api/v2/orders"
DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=15.0)


class UnderdogAuthError(RuntimeError):
    """Raised when we fail to log into Underdog admin."""


class UnderdogAPIError(RuntimeError):
    """Raised when any Underdog API request fails."""


@dataclass
class _TokenCache:
    token: str
    expires_at: datetime


_token_cache: Optional[_TokenCache] = None


def _build_url(path: str) -> str:
    base = settings.underdog_base_url.rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def _default_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _extract_token(payload: Dict[str, Any]) -> str:
    """Try multiple common response shapes to extract the auth token."""
    if not payload:
        raise UnderdogAuthError("Empty response while logging into Underdog admin")
    token = (
        payload.get("token")
        or payload.get("access_token")
        or payload.get("accessToken")
    )
    if token:
        return str(token)
    data = payload.get("data")
    if isinstance(data, dict):
        nested_token = (
            data.get("token")
            or data.get("access_token")
            or data.get("accessToken")
        )
        if nested_token:
            return str(nested_token)
    raise UnderdogAuthError("Auth response did not contain a token")


async def fetch_token() -> str:
    """Log in to Underdog admin and return the bearer token."""
    if not settings.underdog_email or not settings.underdog_password:
        raise UnderdogAuthError("UNDERDOG_EMAIL or UNDERDOG_PASSWORD is not configured")

    url = _build_url(LOGIN_PATH)
    payload = {"email": settings.underdog_email, "password": settings.underdog_password}
    logger.debug("Logging into Underdog", url=url, email=settings.underdog_email)
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.post(url, json=payload, headers=_default_headers())
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise UnderdogAuthError(f"Login failed with status {exc.response.status_code}") from exc
        try:
            data = resp.json()
        except ValueError as exc:
            raise UnderdogAuthError("Failed to decode JSON from Underdog login response") from exc
    token = _extract_token(data)
    logger.info("Received Underdog token", length=len(token))
    return token


def _cache_valid() -> bool:
    if not _token_cache:
        return False
    return datetime.now(timezone.utc) < _token_cache.expires_at


async def get_token(force_refresh: bool = False) -> str:
    """Return cached Underdog token, refreshing if needed."""
    global _token_cache
    if not force_refresh and _cache_valid():
        logger.debug("Using cached Underdog token", expires_at=_token_cache.expires_at.isoformat())
        return _token_cache.token

    token = await fetch_token()
    ttl = max(1, int(settings.underdog_token_ttl))
    _token_cache = _TokenCache(
        token=token,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl),
    )
    logger.debug("Cached Underdog token", expires_at=_token_cache.expires_at.isoformat())
    return token


async def _api_request(method: str, path: str, *, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None) -> httpx.Response:
    """Call a protected Underdog API endpoint with automatic token handling."""
    url = _build_url(path)
    params = params or {}
    headers = _default_headers()
    token = await get_token()
    headers["Authorization"] = f"Bearer {token}"

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.request(method, url, params=params, json=json_body, headers=headers)

    if resp.status_code == 401:
        logger.warning("Underdog API unauthorized, refreshing token")
        headers["Authorization"] = f"Bearer {await get_token(force_refresh=True)}"
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            resp = await client.request(method, url, params=params, json=json_body, headers=headers)

    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise UnderdogAPIError(f"Request {method} {path} failed with status {resp.status_code}") from exc
    return resp


def _extract_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("items", "orders"):
                if isinstance(data.get(key), list):
                    return data[key]
        if isinstance(payload.get("orders"), list):
            return payload["orders"]
    raise UnderdogAPIError("Unexpected orders response shape")


def _yesterday_iso() -> Dict[str, str]:
    today = datetime.now(timezone.utc).date()
    target = today - timedelta(days=1)
    date_str = target.strftime("%Y-%m-%d")
    return {
        "date_from": date_str,
        "date_to": date_str,
    }


async def fetch_yesterday_orders(status_id: int = 1, telegram_sent: int = 0) -> List[Dict[str, Any]]:
    """Fetch yesterday's orders filtered by status and telegram flags."""
    params = {
        **_yesterday_iso(),
        "status_id": status_id,
        "telegram_sent": telegram_sent,
    }
    logger.info("Fetching Underdog orders", params=params)
    resp = await _api_request("GET", ORDERS_PATH, params=params)
    payload = resp.json()
    orders = _extract_items(payload)
    filtered = [order for order in orders if int(order.get("status_id", 0)) == status_id and int(order.get("telegram_sent", 0)) == telegram_sent]
    logger.info("Received orders", total=len(orders), matched=len(filtered))
    return filtered


async def mark_order_telegram_sent(order_id: int) -> None:
    path = f"{ORDERS_PATH}/{order_id}/telegram-sent"
    await _api_request("PATCH", path)


def _normalize_handle(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed.lstrip("@").lower()


def _build_order_message(order: Dict[str, Any]) -> str:
    order_id = order.get("id")
    name = order.get("name") or order.get("type") or "—"
    count = order.get("count") or 0
    total = order.get("total") or order.get("price") or "0"
    lines = [
        f"✅ Ваш заказ ID {order_id} выполнен",
        "",
        f"Название: {name}",
        f"Количество: {count}",
        f"Сумма: {total}",
    ]
    return "\n".join(str(line) for line in lines)


async def _alert_admins_about_unknown(unknown_orders: List[Dict[str, Any]]) -> None:
    if not settings.admins or not unknown_orders:
        return
    lines = [
        "⚠️ Не смог найти пользователей в боте для заказов:",
        "",
    ]
    for item in unknown_orders[:20]:
        lines.append(
            f"ID {item.get('order_id')}: @{item.get('handle')} ({item.get('owner')}) — {item.get('name')} на {item.get('total')}"
        )
    if len(unknown_orders) > 20:
        lines.append(f"… и ещё {len(unknown_orders) - 20} заказов")
    text = "\n".join(lines)
    for admin_id in settings.admins:
        try:
            await bot.send_message(chat_id=int(admin_id), text=text)
        except Exception as exc:
            logger.warning("Failed to notify admin about unknown orders", admin_id=admin_id, error=str(exc))


async def notify_ready_orders(dry_run: bool = True) -> Dict[str, Any]:
    orders = await fetch_yesterday_orders()
    stats = {
        "total_orders": len(orders),
        "missing_contact": 0,
        "unknown_user": 0,
        "matched_users": 0,
        "notified": 0,
        "errors": 0,
        "unknown_orders": [],
    }
    if not orders:
        return stats

    handles = [_normalize_handle((order.get("owner") or {}).get("telegram")) for order in orders]
    valid_handles = [h for h in handles if h]
    user_map = await db.fetch_users_by_usernames(valid_handles)

    for order, handle in zip(orders, handles):
        if not handle:
            stats["missing_contact"] += 1
            logger.warning("Order lacks Telegram handle", order_id=order.get("id"))
            continue
        user = user_map.get(handle)
        if not user:
            stats["unknown_user"] += 1
            stats["unknown_orders"].append(
                {
                    "order_id": order.get("id"),
                    "owner": (order.get("owner") or {}).get("name"),
                    "handle": handle,
                    "total": order.get("total"),
                    "name": order.get("name"),
                }
            )
            logger.warning("Telegram handle not found among bot users", handle=handle, order_id=order.get("id"))
            continue
        stats["matched_users"] += 1
        message = _build_order_message(order)
        if dry_run:
            logger.info(
                "Dry-run: would notify order owner",
                order_id=order.get("id"),
                telegram_id=user.get("telegram_id"),
                username=user.get("username"),
            )
            stats["notified"] += 1
            continue
        try:
            await bot.send_message(chat_id=int(user["telegram_id"]), text=message)
            await mark_order_telegram_sent(int(order.get("id")))
            stats["notified"] += 1
        except (TelegramForbiddenError, TelegramBadRequest) as exc:
            stats["errors"] += 1
            logger.warning("Failed to deliver notification", order_id=order.get("id"), error=str(exc))
        except Exception as exc:  # pragma: no cover
            stats["errors"] += 1
            logger.exception("Unexpected error while notifying order", order_id=order.get("id"), error=str(exc))

    if stats["unknown_user"] > 0 and not dry_run:
        await _alert_admins_about_unknown(stats["unknown_orders"])

    return stats


async def _amain() -> None:
    parser = ArgumentParser(description="Interact with the Underdog admin API")
    parser.add_argument("--orders", action="store_true", help="Fetch yesterday's pending telegram orders")
    parser.add_argument("--notify", action="store_true", help="Send Telegram notifications for ready orders")
    parser.add_argument("--apply", action="store_true", help="With --notify: actually send messages and mark orders instead of dry-run")
    parser.add_argument("--raw-token", action="store_true", help="Print only the bearer token")
    args = parser.parse_args()

    if args.orders:
        orders = await fetch_yesterday_orders()
        print(json.dumps({"count": len(orders), "orders": orders}, ensure_ascii=False, indent=2))
        return

    if args.notify:
        dry_run = not args.apply
        stats = await notify_ready_orders(dry_run=dry_run)
        print(json.dumps({"dry_run": dry_run, **stats}, ensure_ascii=False, indent=2))
        return

    token = await get_token(force_refresh=args.raw_token)
    print(token)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":  # pragma: no cover
    main()
