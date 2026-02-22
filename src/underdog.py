"""Helpers for interacting with the Underdog admin API."""

from __future__ import annotations

import asyncio
import json
from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

import httpx
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from loguru import logger

from . import db
from .config import settings

LOGIN_PATH = "/api/login"
ORDERS_PATH = "/api/v2/orders"
DOMAINS_PATH = "/api/v2/domains"
IPS_PATH = "/api/v2/ip"
TICKETS_PATH = "/api/v2/tickets"
DEFAULT_TIMEOUT = (30.0, 15.0)


class UnderdogAuthError(RuntimeError):
    """Raised when we fail to log into Underdog admin."""


class UnderdogAPIError(RuntimeError):
    """Raised when any Underdog API request fails."""


@dataclass(slots=True)
class _TokenCache:
    token: str
    expires_at: datetime


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


def _extract_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("items", "orders"):
                items = data.get(key)
                if isinstance(items, list):
                    return items
        if isinstance(payload.get("orders"), list):
            return payload["orders"]
    raise UnderdogAPIError("Unexpected orders response shape")


def _extract_domains(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("items", "domains"):
                items = data.get(key)
                if isinstance(items, list):
                    return items
        if isinstance(payload.get("domains"), list):
            return payload["domains"]
    raise UnderdogAPIError("Unexpected domains response shape")


def _extract_ips(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("items", "ips"):
                items = data.get(key)
                if isinstance(items, list):
                    return items
        if isinstance(payload.get("ips"), list):
            return payload["ips"]
    raise UnderdogAPIError("Unexpected IP response shape")


def _extract_tickets(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("items", "tickets"):
                items = data.get(key)
                if isinstance(items, list):
                    return items
        if isinstance(payload.get("tickets"), list):
            return payload["tickets"]
    raise UnderdogAPIError("Unexpected tickets response shape")


def _normalize_handle(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed.lstrip("@").lower()


def _parse_telegram_id(person: Dict[str, Any]) -> Optional[int]:
    """Из объекта owner/contractor из API достать telegram_id (число), если API отдаёт."""
    if not person:
        return None
    for key in ("telegram_id", "telegram_Id", "telegramId"):
        raw = person.get(key)
        if raw is None:
            continue
        try:
            uid = int(raw)
            if uid > 0:
                return uid
        except (TypeError, ValueError):
            continue
    return None


def _resolve_owner_fields(record: Dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    owner = record.get("owner") or {}
    raw_handle = (
        owner.get("telegram")
        or owner.get("telegram_handle")
        or record.get("telegram")
        or record.get("telegram_handle")
    )
    normalized = _normalize_handle(raw_handle)
    owner_name = owner.get("name")
    return normalized, raw_handle, owner_name


# Статусы заказов дизайна (для уведомлений)
ORDER_STATUS_TEXTS: Dict[int, str] = {
    0: "обработка",
    1: "выполнен",
    2: "в работе",
    3: "на правках",
    4: "отдано на апрув",
    5: "возвращено на доработку",
}


def _order_status_text(status_id: Any) -> str:
    if status_id is None:
        return "—"
    try:
        sid = int(status_id)
        return ORDER_STATUS_TEXTS.get(sid, "unknown")
    except (TypeError, ValueError):
        return "—"


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


def _build_design_order_message(order: Dict[str, Any]) -> str:
    """Сообщение для заказа дизайна (креатив/PWA) с учётом статуса."""
    order_id = order.get("id")
    name = order.get("name") or order.get("type") or "—"
    count = order.get("count") or 0
    total = order.get("total") or order.get("price") or "0"
    status_id = order.get("status_id")
    status_text = _order_status_text(status_id)
    lines = [
        f"📐 Заказ (дизайн) ID {order_id}",
        "",
        f"Название: {name}",
        f"Количество: {count}",
        f"Сумма: {total}",
        f"Статус: {status_text}",
    ]
    return "\n".join(str(line) for line in lines)


def _build_design_assignment_message(
    order: Dict[str, Any],
    designer_name: Optional[str] = None,
    assigned_to_display: Optional[str] = None,
    assigned_to_mention_html: Optional[str] = None,
) -> str:
    """Сообщение о постановке таска. assigned_to_mention_html: HTML с <a href=\"tg://user?id=...\">@user</a> (тег); assigned_to_display: текст, если тега нет; если оба None — «вас» (личное дизайнеру)."""
    order_id = order.get("id")
    name = order.get("name") or order.get("type") or "—"
    owner = order.get("owner") or {}
    owner_name = owner.get("name") or "—"
    if assigned_to_mention_html is not None:
        assign_line = f"Таск назначен на: {assigned_to_mention_html}"
    elif assigned_to_display is not None:
        assign_line = f"Таск назначен на: {assigned_to_display}"
    else:
        assign_line = f"Таск назначен на: вас" + (f" ({designer_name})" if designer_name else "")
    lines = [
        "📐 Вам поставлен таск (дизайн/креатив)",
        "",
        assign_line,
        f"Заказчик: {owner_name}",
        "",
        f"Название: {name}",
        f"ID заказа: {order_id}",
        "Статус: обработка",
    ]
    return "\n".join(str(line) for line in lines)


def _yesterday() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc).date()
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    dt = None  # type: ignore[assignment]
            if dt is None:
                return None
        if dt.tzinfo is None:
            return dt.date()
        return dt.astimezone(timezone.utc).date()
    except Exception:
        return None


@dataclass(slots=True)
class UnderdogClient:
    base_url: str
    email: str
    password: str
    token_ttl: int = 3600
    timeout: httpx.Timeout = field(
        default_factory=lambda: httpx.Timeout(DEFAULT_TIMEOUT[0], connect=DEFAULT_TIMEOUT[1])
    )
    client: Optional[httpx.AsyncClient] = None
    _token_cache: Optional[_TokenCache] = field(default=None, init=False)
    _owns_client: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=self.timeout)
            self._owns_client = True

    async def __aenter__(self) -> "UnderdogClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    async def close(self) -> None:
        if self._owns_client and self.client is not None:
            await self.client.aclose()
            self.client = None
            self._owns_client = False

    def _build_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{path}"

    @staticmethod
    def _default_headers() -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def _refresh_token(self) -> str:
        if not self.email or not self.password:
            raise UnderdogAuthError("UNDERDOG_EMAIL or UNDERDOG_PASSWORD is not configured")
        assert self.client is not None, "HTTP client is not initialized"

        url = self._build_url(LOGIN_PATH)
        payload = {"email": self.email, "password": self.password}
        logger.debug("Logging into Underdog", url=url, email=self.email)
        resp = await self.client.post(url, json=payload, headers=self._default_headers())
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise UnderdogAuthError(f"Login failed with status {exc.response.status_code}") from exc
        try:
            data = resp.json()
        except ValueError as exc:  # pragma: no cover
            raise UnderdogAuthError("Failed to decode JSON from Underdog login response") from exc
        token = _extract_token(data)
        logger.info("Received Underdog token", length=len(token))
        ttl = max(1, int(self.token_ttl))
        self._token_cache = _TokenCache(
            token=token,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl),
        )
        return token

    async def _ensure_token(self, force_refresh: bool = False) -> str:
        if not force_refresh and self._token_cache and datetime.now(timezone.utc) < self._token_cache.expires_at:
            logger.debug(
                "Using cached Underdog token",
                expires_at=self._token_cache.expires_at.isoformat(),
            )
            return self._token_cache.token
        return await self._refresh_token()

    async def get_token(self, *, force_refresh: bool = False) -> str:
        return await self._ensure_token(force_refresh=force_refresh)

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> httpx.Response:
        assert self.client is not None, "HTTP client is not initialized"
        token = await self._ensure_token()
        headers = self._default_headers()
        headers["Authorization"] = f"Bearer {token}"

        url = self._build_url(path)
        resp = await self.client.request(method, url, params=params, json=json_body, headers=headers)

        if resp.status_code == 401:
            logger.warning("Underdog API unauthorized, refreshing token")
            headers["Authorization"] = f"Bearer {await self._ensure_token(force_refresh=True)}"
            resp = await self.client.request(method, url, params=params, json=json_body, headers=headers)

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            try:
                body = resp.text if hasattr(resp, "text") else ""
            except Exception:
                body = ""
            if body and len(body) > 500:
                body = body[:500] + "..."
            raise UnderdogAPIError(
                f"Request {method} {path} failed with status {resp.status_code}: {body}"
            ) from exc
        return resp

    async def fetch_orders_by_type(
        self,
        order_type: str,
        *,
        order_status: int = 1,
    ) -> List[Dict[str, Any]]:
        """Fetch orders from /api/v2/orders?type=<order_type>&order_status=<order_status>."""
        params = {"type": order_type, "order_status": order_status}
        logger.info("Fetching Underdog orders by type", params=params)
        resp = await self.request("GET", ORDERS_PATH, params=params)
        orders = _extract_items(resp.json())
        logger.info("Received orders", type=order_type, count=len(orders))
        return orders

    async def fetch_orders_for_date(
        self,
        target_date: date,
        *,
        status_id: int = 1,
        telegram_sent: int = 0,
    ) -> List[Dict[str, Any]]:
        params = {
            "date_from": target_date.strftime("%Y-%m-%d"),
            "date_to": target_date.strftime("%Y-%m-%d"),
            "status_id": status_id,
            "telegram_sent": telegram_sent,
        }
        logger.info("Fetching Underdog orders", params=params)
        resp = await self.request("GET", ORDERS_PATH, params=params)
        orders = _extract_items(resp.json())
        filtered = [
            order
            for order in orders
            if int(order.get("status_id", 0)) == status_id
            and int(order.get("telegram_sent", 0)) == telegram_sent
        ]
        logger.info("Received orders", total=len(orders), matched=len(filtered))
        return filtered

    async def fetch_yesterday_orders(self, *, status_id: int = 1, telegram_sent: int = 0) -> List[Dict[str, Any]]:
        return await self.fetch_orders_for_date(
            _yesterday(),
            status_id=status_id,
            telegram_sent=telegram_sent,
        )

    ORDER_TYPES_ORDERS_BOT = ("domain", "transferDomain")
    ORDER_TYPES_DESIGN_BOT = ("pwaDesign", "creative")

    async def fetch_orders_for_orders_bot(self) -> List[Dict[str, Any]]:
        """Fetch domain + transferDomain orders (for orders bot). Только с telegram_sent=0."""
        all_orders: List[Dict[str, Any]] = []
        seen_ids: set = set()
        for order_type in self.ORDER_TYPES_ORDERS_BOT:
            orders = await self.fetch_orders_by_type(order_type, order_status=1)
            for o in orders:
                if int(o.get("telegram_sent", 1)) != 0:
                    continue
                oid = o.get("id")
                if oid is not None and oid not in seen_ids:
                    seen_ids.add(oid)
                    all_orders.append(o)
        return all_orders

    async def fetch_orders_for_design_bot(self) -> List[Dict[str, Any]]:
        """Fetch pwaDesign + creative orders (for design bot). Только с telegram_sent=0."""
        all_orders = []
        seen_ids = set()
        for order_type in self.ORDER_TYPES_DESIGN_BOT:
            orders = await self.fetch_orders_by_type(order_type, order_status=1)
            for o in orders:
                if int(o.get("telegram_sent", 1)) != 0:
                    continue
                oid = o.get("id")
                if oid is not None and oid not in seen_ids:
                    seen_ids.add(oid)
                    all_orders.append(o)
        return all_orders

    async def fetch_design_new_tasks(self) -> List[Dict[str, Any]]:
        """Fetch pwaDesign + creative orders with order_status=0 (новый таск, обработка)."""
        all_orders = []
        seen_ids = set()
        for order_type in self.ORDER_TYPES_DESIGN_BOT:
            orders = await self.fetch_orders_by_type(order_type, order_status=0)
            for o in orders:
                oid = o.get("id")
                if oid is not None and oid not in seen_ids:
                    seen_ids.add(oid)
                    all_orders.append(o)
        return all_orders

    async def mark_order_telegram_sent(self, order_id: int) -> None:
        path = f"{ORDERS_PATH}/{order_id}/telegram-sent"
        await self.request("PATCH", path)

    async def fetch_domains(self) -> List[Dict[str, Any]]:
        resp = await self.request("GET", DOMAINS_PATH)
        return _extract_domains(resp.json())

    async def mark_domain_telegram_sent(self, domain_id: int) -> None:
        suffixes = ["telegram-sent", "telegram-notified"]
        last_error: Optional[Exception] = None
        for suffix in suffixes:
            path = f"{DOMAINS_PATH}/{domain_id}/{suffix}"
            try:
                await self.request("PATCH", path)
                return
            except UnderdogAPIError as exc:
                last_error = exc
        if last_error:
            raise last_error

    async def fetch_ips(self) -> List[Dict[str, Any]]:
        resp = await self.request("GET", IPS_PATH)
        return _extract_ips(resp.json())

    async def mark_ip_telegram_sent(self, ip_id: int) -> None:
        path = f"{IPS_PATH}/{ip_id}/telegram-sent"
        await self.request("PATCH", path)

    async def fetch_tickets(self) -> List[Dict[str, Any]]:
        resp = await self.request("GET", TICKETS_PATH)
        return _extract_tickets(resp.json())

    async def mark_ticket_telegram_sent(self, ticket_id: int) -> None:
        path = f"{TICKETS_PATH}/{ticket_id}/telegram-sent"
        await self.request("PATCH", path)

    @classmethod
    def from_settings(cls) -> "UnderdogClient":
        return cls(
            base_url=settings.underdog_base_url,
            email=settings.underdog_email,
            password=settings.underdog_password,
            token_ttl=settings.underdog_token_ttl,
        )


@dataclass(slots=True)
class NotificationStats:
    total_orders: int = 0
    missing_contact: int = 0
    unknown_user: int = 0
    matched_users: int = 0
    notified: int = 0
    errors: int = 0
    unknown_orders: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        payload = {
            "total_orders": self.total_orders,
            "missing_contact": self.missing_contact,
            "unknown_user": self.unknown_user,
            "matched_users": self.matched_users,
            "notified": self.notified,
            "errors": self.errors,
            "unknown_orders": self.unknown_orders,
        }
        if dry_run is not None:
            payload["dry_run"] = dry_run
        return payload


@dataclass(slots=True)
class DomainNotifierStats:
    total_domains: int = 0
    expiring_domains: int = 0
    matched_users: int = 0
    notified_users: int = 0
    notified_domains: int = 0
    missing_contact: int = 0
    unknown_user: int = 0
    errors: int = 0
    unknown_items: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        payload = {
            "total_domains": self.total_domains,
            "expiring_domains": self.expiring_domains,
            "matched_users": self.matched_users,
            "notified_users": self.notified_users,
            "notified_domains": self.notified_domains,
            "missing_contact": self.missing_contact,
            "unknown_user": self.unknown_user,
            "errors": self.errors,
            "unknown_items": self.unknown_items,
        }
        if dry_run is not None:
            payload["dry_run"] = dry_run
        return payload


@dataclass(slots=True)
class IPNotifierStats:
    total_ips: int = 0
    matched_users: int = 0
    notified_users: int = 0
    notified_ips: int = 0
    missing_contact: int = 0
    unknown_user: int = 0
    errors: int = 0
    unknown_items: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        payload = {
            "total_ips": self.total_ips,
            "matched_users": self.matched_users,
            "notified_users": self.notified_users,
            "notified_ips": self.notified_ips,
            "missing_contact": self.missing_contact,
            "unknown_user": self.unknown_user,
            "errors": self.errors,
            "unknown_items": self.unknown_items,
        }
        if dry_run is not None:
            payload["dry_run"] = dry_run
        return payload


@dataclass(slots=True)
class TicketNotifierStats:
    total_tickets: int = 0
    completed_tickets: int = 0
    matched_users: int = 0
    notified_users: int = 0
    notified_tickets: int = 0
    missing_contact: int = 0
    unknown_user: int = 0
    errors: int = 0
    unknown_items: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        payload = {
            "total_tickets": self.total_tickets,
            "completed_tickets": self.completed_tickets,
            "matched_users": self.matched_users,
            "notified_users": self.notified_users,
            "notified_tickets": self.notified_tickets,
            "missing_contact": self.missing_contact,
            "unknown_user": self.unknown_user,
            "errors": self.errors,
            "unknown_items": self.unknown_items,
        }
        if dry_run is not None:
            payload["dry_run"] = dry_run
        return payload


@dataclass(slots=True)
class OrderNotifier:
    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]

    async def notify_ready_orders(
        self,
        *,
        dry_run: bool = True,
        limit_user_ids: Optional[Iterable[int]] = None,
    ) -> NotificationStats:
        orders = await self.underdog.fetch_orders_for_orders_bot()
        limit_set: Optional[set[int]] = None
        if limit_user_ids is not None:
            limit_set = {int(uid) for uid in limit_user_ids}
        stats = NotificationStats(total_orders=len(orders) if limit_set is None else 0)
        if not orders:
            return stats

        handles = [_normalize_handle((order.get("owner") or {}).get("telegram") or (order.get("owner") or {}).get("telegram_handle")) for order in orders]
        valid_handles = [h for h in handles if h]
        user_map = await db.fetch_users_by_usernames(valid_handles)

        for order, handle in zip(orders, handles):
            owner = order.get("owner") or {}
            # Если API отдаёт telegram_id у owner — используем его, иначе ищем по нику в БД
            user_telegram_id: Optional[int] = _parse_telegram_id(owner)
            if user_telegram_id is not None:
                user = {"telegram_id": user_telegram_id, "username": handle or owner.get("telegram") or owner.get("telegram_handle") or ""}
            else:
                if not handle:
                    stats.missing_contact += 1
                    logger.warning("Order lacks Telegram handle", order_id=order.get("id"))
                    continue
                user = user_map.get(handle)
                if not user:
                    stats.unknown_user += 1
                    stats.unknown_orders.append(
                        {
                            "order_id": order.get("id"),
                            "owner": owner.get("name"),
                            "handle": handle,
                            "total": order.get("total"),
                            "name": order.get("name"),
                        }
                    )
                    logger.warning(
                        "Telegram handle not found among bot users",
                        handle=handle,
                        order_id=order.get("id"),
                    )
                    continue
                try:
                    user_telegram_id = int(user.get("telegram_id"))
                except Exception:
                    user_telegram_id = None

            if limit_set is not None and (user_telegram_id not in limit_set):
                continue

            if limit_set is not None:
                stats.total_orders += 1

            stats.matched_users += 1
            message = _build_order_message(order)
            if dry_run:
                logger.info(
                    "Dry-run: would notify order owner",
                    order_id=order.get("id"),
                    telegram_id=user.get("telegram_id"),
                    username=user.get("username"),
                )
                stats.notified += 1
                continue

            try:
                await self.bot.send_message(chat_id=int(user["telegram_id"]), text=message)
                await self.underdog.mark_order_telegram_sent(int(order.get("id")))
                stats.notified += 1
            except (TelegramForbiddenError, TelegramBadRequest) as exc:
                stats.errors += 1
                logger.warning(
                    "Failed to deliver notification",
                    order_id=order.get("id"),
                    error=str(exc),
                )
                await self._notify_admins_delivery_error(order=order, error_text=str(exc))
            except Exception as exc:  # pragma: no cover
                stats.errors += 1
                logger.exception(
                    "Unexpected error while notifying order",
                    order_id=order.get("id"),
                    error=str(exc),
                )
                await self._notify_admins_delivery_error(order=order, error_text=str(exc))

        if stats.unknown_user > 0 and not dry_run:
            await self._alert_admins(stats.unknown_orders)

        return stats

    async def _alert_admins(self, unknown_orders: Iterable[Dict[str, Any]]) -> None:
        orders = list(unknown_orders)
        if not self.admin_ids or not orders:
            return
        lines = [
            "⚠️ Не смог найти пользователей в боте для заказов:",
            "",
        ]
        for item in orders[:20]:
            lines.append(
                f"ID {item.get('order_id')}: @{item.get('handle')} ({item.get('owner')}) — {item.get('name')} на {item.get('total')}"
            )
        if len(orders) > 20:
            lines.append(f"… и ещё {len(orders) - 20} заказов")
        text = "\n".join(lines)
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(chat_id=int(admin_id), text=text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about unknown orders",
                    admin_id=admin_id,
                    error=str(exc),
                )

    async def _notify_admins_delivery_error(
        self,
        *,
        order: Dict[str, Any],
        error_text: str,
    ) -> None:
        if not self.admin_ids:
            return
        owner = order.get("owner") or {}
        normalized_handle = _normalize_handle(owner.get("telegram") or owner.get("telegram_handle"))
        lines = [
            "⚠️ Ошибка отправки уведомления о заказе",
            f"ID: {order.get('id')}",
            f"Покупатель: {owner.get('name') or '—'}",
            f"Username: @{normalized_handle}" if normalized_handle else "Username: (не указан)",
            f"Сумма: {order.get('total') or order.get('price') or '0'}",
            f"Ошибка: {error_text}",
        ]
        text = "\n".join(lines)
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(chat_id=int(admin_id), text=text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about delivery error",
                    admin_id=admin_id,
                    error=str(exc),
                )


@dataclass(slots=True)
class DesignAssignmentNotifier:
    """Уведомление о назначении таска (order_status=0). Рассылка: broadcast-чаты, подписчики, админы."""

    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]
    broadcast_chat_ids: Sequence[int] = ()  # DESIGN_BROADCAST_CHAT_IDS — группа/канал, видят все

    async def notify_design_assignments(
        self,
        *,
        dry_run: bool = True,
    ) -> NotificationStats:
        orders = await self.underdog.fetch_design_new_tasks()
        stats = NotificationStats(total_orders=len(orders))
        if not orders:
            return stats

        subscribers = await db.list_design_bot_subscribers()
        logger.info(
            "Design notify",
            orders=len(orders),
            admin_ids=list(self.admin_ids) if self.admin_ids else "[] (set ADMINS env for admin copies)",
            design_bot_token_set=bool(settings.design_bot_token),
            broadcast_chats=len(self.broadcast_chat_ids),
            subscribers_count=len(subscribers),
        )
        if not subscribers:
            logger.warning(
                "Design notify: подписчиков нет (tg_design_bot_chats пуста). Уведомления получат только админы. "
                "Чтобы приходило всем: каждый должен открыть DesignBot и нажать /start (вебхук DesignBot должен быть настроен в приложении).",
            )

        for order in orders:
            order_id = order.get("id")
            if order_id is None:
                continue
            if await db.is_design_assignment_sent(int(order_id)):
                logger.debug("Design order already sent, skipping", order_id=order_id)
                continue

            logger.info(
                "Design notify: processing order",
                order_id=order_id,
                subscribers_count=len(subscribers),
                admin_ids_count=len(self.admin_ids),
            )
            contractor = order.get("contractor") or {}
            contractor_id = order.get("contractor_id")
            if contractor_id is not None:
                contractor_id = str(contractor_id).strip()
            telegram_from_order = (contractor.get("telegram") or contractor.get("telegram_handle") or "").strip().lstrip("@")
            designer_name: Optional[str] = contractor.get("name")

            # Сначала берём telegram_id из API (если добавили), иначе — по нику из БД, потом по contractor_id
            designer_telegram_id: Optional[int] = _parse_telegram_id(contractor)
            if designer_telegram_id is None and telegram_from_order:
                user = await db.find_user_by_username(telegram_from_order)
                if user:
                    designer_telegram_id = int(user.get("telegram_id"))
            if designer_telegram_id is None and contractor_id:
                designer_telegram_id = await db.get_contractor_telegram_id(contractor_id)

            if designer_telegram_id is not None:
                label = f"@{telegram_from_order}" if telegram_from_order else (designer_name or f"id{designer_telegram_id}")
                assigned_to_mention_html = f'<a href="tg://user?id={designer_telegram_id}">{label}</a>'
                assigned_to_display = None
            else:
                assigned_to_mention_html = None
                # Ник из API (приходит без @) — показываем как имя дизайнера в TG
                assigned_to_display = f"@{telegram_from_order}" if telegram_from_order else (f"contractor_id {contractor_id}" if contractor_id else "—")
            message_for_broadcast = _build_design_assignment_message(
                order,
                designer_name=designer_name,
                assigned_to_display=assigned_to_display,
                assigned_to_mention_html=assigned_to_mention_html,
            )
            message_for_designer = _build_design_assignment_message(order, designer_name=designer_name)
            admin_message = f"📋 Копия админу:\n\n{message_for_broadcast}"
            # Не предупреждаем админа и не считаем unknown, если в сообщении уже есть «Таск назначен на: @username» из API
            if designer_telegram_id is None and not telegram_from_order:
                stats.unknown_user += 1
                stats.unknown_orders.append(
                    {
                        "order_id": order_id,
                        "contractor_id": contractor_id,
                        "owner": (order.get("owner") or {}).get("name"),
                        "name": order.get("name"),
                    }
                )
                logger.debug("Design assignment: no @username in API for contractor", order_id=order_id, contractor_id=contractor_id)

            if dry_run:
                stats.matched_users += 1
                stats.notified += len(self.broadcast_chat_ids) + len(subscribers) + len(self.admin_ids)
                continue

            # 1) В broadcast-чаты (группа/канал) — видят все участники
            for chat_id in self.broadcast_chat_ids:
                try:
                    await self.bot.send_message(chat_id=int(chat_id), text=message_for_broadcast)
                    stats.notified += 1
                except (TelegramForbiddenError, TelegramBadRequest) as exc:
                    stats.errors += 1
                    logger.warning("Failed to send design assignment to broadcast chat", order_id=order_id, chat_id=chat_id, error=str(exc))
                except Exception as exc:
                    stats.errors += 1
                    logger.exception("Unexpected error sending to broadcast chat", order_id=order_id, chat_id=chat_id)

            # 2) Подписчикам (кто нажал /start в DesignBot)
            for chat_id in subscribers:
                text = message_for_designer if (designer_telegram_id is not None and chat_id == designer_telegram_id) else message_for_broadcast
                try:
                    await self.bot.send_message(chat_id=chat_id, text=text)
                    stats.notified += 1
                except (TelegramForbiddenError, TelegramBadRequest) as exc:
                    stats.errors += 1
                    logger.warning("Failed to send design assignment to subscriber", order_id=order_id, chat_id=chat_id, error=str(exc))
                except Exception as exc:
                    stats.errors += 1
                    logger.exception("Unexpected error sending design assignment to subscriber", order_id=order_id, chat_id=chat_id)

            if self.admin_ids:
                for admin_id in self.admin_ids:
                    try:
                        await self.bot.send_message(chat_id=int(admin_id), text=admin_message)
                        logger.info("Sent design assignment copy to admin", admin_id=admin_id, order_id=order_id)
                    except (TelegramForbiddenError, TelegramBadRequest) as exc:
                        logger.warning(
                            "Failed to send design assignment copy to admin (admin must /start the bot that sends: DesignBot if DESIGN_BOT_TOKEN set, else main bot)",
                            admin_id=admin_id,
                            order_id=order_id,
                            error=str(exc),
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to send design assignment copy to admin",
                            admin_id=admin_id,
                            order_id=order_id,
                            error=str(exc),
                        )
            else:
                logger.warning("No admin_ids configured (ADMINS env): admin copy not sent", order_id=order_id)

            await db.mark_design_assignment_sent(int(order_id))
            stats.matched_users += 1
        return stats


@dataclass(slots=True)
class DomainNotifier:
    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]

    async def notify_expiring_domains(
        self,
        *,
        dry_run: bool = True,
        days: int = 30,
    ) -> DomainNotifierStats:
        domains = await self.underdog.fetch_domains()
        stats = DomainNotifierStats(total_domains=len(domains))
        if not domains:
            return stats

        cutoff = datetime.now(timezone.utc).date() + timedelta(days=max(0, int(days)))
        per_handle: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for domain in domains:
            if _is_domain_sent(domain):
                continue
            expires_raw = (
                domain.get("expires_at")
                or domain.get("expires")
                or domain.get("expiration")
            )
            expires_at = _parse_date(expires_raw)
            # API уже отдает только expiring<=30д; если даты нет — всё равно включаем,
            # иначе фильтруем по горизонту.
            if expires_at is not None and expires_at > cutoff:
                continue
            stats.expiring_domains += 1
            handle, raw_handle, owner_name = _resolve_owner_fields(domain)
            if not handle:
                stats.missing_contact += 1
                stats.unknown_items.append(
                    {
                        "domain": domain.get("domain") or domain.get("name"),
                        "expires_at": expires_at.isoformat() if expires_at else None,
                        "owner": owner_name,
                    }
                )
                await self._notify_admins_missing_user(
                    handle=None,
                    entries=[{"raw": domain, "expires_at": expires_at}],
                    dry_run=dry_run,
                )
                continue
            per_handle[handle].append({
                "raw": domain,
                "expires_at": expires_at,
                "display_handle": raw_handle,
                "owner_name": owner_name,
            })

        if not per_handle:
            return stats

        user_map = await db.fetch_users_by_usernames(list(per_handle.keys()))

        for handle, domain_entries in per_handle.items():
            user = user_map.get(handle)
            if not user:
                stats.unknown_user += len(domain_entries)
                stats.unknown_items.extend(
                    {
                        "domain": entry["raw"].get("domain") or entry["raw"].get("name"),
                        "expires_at": entry["expires_at"].isoformat() if entry["expires_at"] else None,
                        "handle": handle,
                    }
                    for entry in domain_entries
                )
                await self._notify_admins_missing_user(
                    handle=handle,
                    entries=domain_entries,
                    dry_run=dry_run,
                )
                continue

            stats.matched_users += 1

            text = _build_domain_notification(domain_entries)
            if dry_run:
                logger.info(
                    "Dry-run: would notify about expiring domains",
                    handle=handle,
                    telegram_id=user.get("telegram_id"),
                )
                stats.notified_users += 1
                stats.notified_domains += len(domain_entries)
                continue

            try:
                await self.bot.send_message(int(user["telegram_id"]), text)
                stats.notified_users += 1
                stats.notified_domains += len(domain_entries)
                # Дублируем отправленное сообщение всем админам для контроля.
                await self._notify_admins_copy(text)
                for entry in domain_entries:
                    domain_id = entry["raw"].get("id")
                    if domain_id is None:
                        continue
                    try:
                        await self.underdog.mark_domain_telegram_sent(int(domain_id))
                    except Exception as exc:
                        stats.errors += 1
                        logger.warning(
                            "Failed to mark domain telegram_sent",
                            domain_id=domain_id,
                            error=str(exc),
                        )
            except Exception as exc:
                stats.errors += 1
                logger.warning(
                    "Failed to send domain expiration message",
                    handle=handle,
                    error=str(exc),
                )
                await self._notify_admins_domain_delivery_error(
                    handle=handle,
                    entries=domain_entries,
                    error_text=str(exc),
                    dry_run=dry_run,
                )

        if stats.unknown_items and dry_run:
            await self._alert_admins(stats.unknown_items)

        return stats

    async def _notify_admins_copy(self, text: str) -> None:
        if not self.admin_ids:
            return
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to send admin copy for domains",
                    admin_id=admin_id,
                    error=str(exc),
                )

    async def _alert_admins(self, unknown_items: List[Dict[str, Any]]) -> None:
        if not self.admin_ids or not unknown_items:
            return
        lines = [
            "⚠️ Не удалось отправить уведомление по доменам:",
            "",
        ]
        for item in unknown_items[:20]:
            lines.append(
                f"{item.get('domain') or '—'} (до {item.get('expires_at') or '—'}) — @{item.get('handle') or '—'}"
            )
        if len(unknown_items) > 20:
            lines.append(f"… и ещё {len(unknown_items) - 20} доменов")
        text = "\n".join(lines)
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about domains",
                    admin_id=admin_id,
                    error=str(exc),
                )

    async def _notify_admins_missing_user(
        self,
        *,
        handle: Optional[str],
        entries: List[Dict[str, Any]],
        dry_run: bool,
    ) -> None:
        if not self.admin_ids or not entries:
            return
        owner_name = (entries[0]["raw"].get("owner") or {}).get("name") if entries else None
        header_lines = [
            "⚠️ Не удалось доставить уведомление о доменах покупателю.",
        ]
        if owner_name:
            header_lines.append(f"Владелец: {owner_name}")
        header_lines.append(f"Username: @{handle}" if handle else "Username: (не указан)")
        header_lines.append("")
        header_lines.append(_build_domain_notification(entries))
        text = "\n".join(header_lines)
        if dry_run:
            logger.info(
                "Dry-run: would alert admins about unknown domain recipient",
                handle=handle,
                owner=owner_name,
            )
            return
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about missing domain recipient",
                    admin_id=admin_id,
                    handle=handle,
                    error=str(exc),
                )

    async def _notify_admins_domain_delivery_error(
        self,
        *,
        handle: Optional[str],
        entries: List[Dict[str, Any]],
        error_text: str,
        dry_run: bool,
    ) -> None:
        if not self.admin_ids or not entries:
            return
        lines = [
            "⚠️ Ошибка отправки уведомления о доменах",
            f"Username: @{handle}" if handle else "Username: (не указан)",
            f"Ошибка: {error_text}",
            "",
            _build_domain_notification(entries),
        ]
        text = "\n".join(lines)
        if dry_run:
            logger.info(
                "Dry-run: would alert admins about domain delivery error",
                handle=handle,
                error=error_text,
            )
            return
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about domain delivery error",
                    admin_id=admin_id,
                    handle=handle,
                    error=str(exc),
                )


@dataclass(slots=True)
class IPNotifier:
    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]

    async def notify_expiring_ips(self, *, dry_run: bool = True, days: int = 7) -> IPNotifierStats:
        ips = await self.underdog.fetch_ips()
        stats = IPNotifierStats(total_ips=len(ips))
        if not ips:
            return stats

        logger.info(
            "IP notify",
            horizon_days=days,
            ips_count=len(ips),
            bot_orders_bot=bool(settings.orders_bot_token),
        )
        per_handle: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        today = datetime.now(timezone.utc).date()
        cutoff = today + timedelta(days=max(0, int(days)))

        for ip_entry in ips:
            if _is_ip_sent(ip_entry):
                continue
            handle, raw_handle, owner_name = _resolve_owner_fields(ip_entry)
            expires_at = _parse_date(
                ip_entry.get("expires_at")
                or ip_entry.get("expires")
                or ip_entry.get("expiration")
            )
            if not expires_at:
                continue
            if expires_at > cutoff:
                continue
            if not handle:
                stats.missing_contact += 1
                stats.unknown_items.append(
                    {
                        "ip": ip_entry.get("ip") or ip_entry.get("address"),
                        "expires_at": expires_at.isoformat() if expires_at else None,
                        "owner": owner_name,
                    }
                )
                await self._notify_admins_missing_ip(
                    handle=None,
                    entries=[{
                        "raw": ip_entry,
                        "expires_at": expires_at,
                        "days_left": (expires_at - today).days if expires_at else None,
                        "display_handle": raw_handle,
                        "owner_name": owner_name,
                    }],
                    dry_run=dry_run,
                )
                continue
            per_handle[handle].append(
                {
                    "raw": ip_entry,
                    "expires_at": expires_at,
                    "days_left": (expires_at - today).days if expires_at else None,
                    "display_handle": raw_handle,
                    "owner_name": owner_name,
                }
            )

        if not per_handle:
            return stats

        user_map = await db.fetch_users_by_usernames(list(per_handle.keys()))

        for handle, ip_entries in per_handle.items():
            user = user_map.get(handle)
            if not user:
                stats.unknown_user += len(ip_entries)
                stats.unknown_items.extend(
                    {
                        "ip": entry["raw"].get("ip") or entry["raw"].get("address"),
                        "expires_at": entry["expires_at"].isoformat() if entry["expires_at"] else None,
                        "handle": handle,
                    }
                    for entry in ip_entries
                )
                await self._notify_admins_missing_ip(
                    handle=handle,
                    entries=ip_entries,
                    dry_run=dry_run,
                )
                continue

            stats.matched_users += 1
            text = _build_ip_notification(ip_entries)
            if dry_run:
                logger.info(
                    "Dry-run: would notify about expiring IPs",
                    handle=handle,
                    telegram_id=user.get("telegram_id"),
                )
                stats.notified_users += 1
                stats.notified_ips += len(ip_entries)
                continue

            try:
                telegram_id = int(user["telegram_id"])
                await self.bot.send_message(telegram_id, text)
                stats.notified_users += 1
                stats.notified_ips += len(ip_entries)
                logger.info(
                    "Sent IP expiration notification",
                    handle=handle,
                    telegram_id=telegram_id,
                    ips_count=len(ip_entries),
                )
                for entry in ip_entries:
                    ip_id = entry["raw"].get("id")
                    if ip_id is None:
                        continue
                    try:
                        await self.underdog.mark_ip_telegram_sent(int(ip_id))
                    except Exception as exc:  # pragma: no cover
                        stats.errors += 1
                        logger.warning(
                            "Failed to mark IP telegram_sent: ip_id=%s path=PATCH /api/v2/ip/{id}/telegram-sent error=%s",
                            ip_id,
                            exc,
                        )
            except (TelegramForbiddenError, TelegramBadRequest) as exc:
                stats.errors += 1
                logger.warning(
                    "IP notification not delivered (user blocked bot or chat not found)",
                    handle=handle,
                    telegram_id=user.get("telegram_id"),
                    error=str(exc),
                )
                await self._notify_admins_ip_delivery_error(
                    handle=handle,
                    entries=ip_entries,
                    error_text=str(exc),
                    dry_run=dry_run,
                )
            except Exception as exc:  # pragma: no cover
                stats.errors += 1
                logger.warning(
                    "Failed to send IP expiration message",
                    handle=handle,
                    error=str(exc),
                )
                await self._notify_admins_ip_delivery_error(
                    handle=handle,
                    entries=ip_entries,
                    error_text=str(exc),
                    dry_run=dry_run,
                )

        if stats.unknown_items and dry_run:
            await self._alert_admins(stats.unknown_items)

        return stats

    async def _notify_admins_missing_ip(
        self,
        *,
        handle: Optional[str],
        entries: List[Dict[str, Any]],
        dry_run: bool,
    ) -> None:
        if not self.admin_ids or not entries:
            return
        header_lines = [
            "⚠️ Не удалось доставить уведомление об IP пользователю.",
        ]
        owner_name = entries[0].get("owner_name")
        if owner_name:
            header_lines.append(f"Владелец: {owner_name}")
        header_lines.append(f"Username: @{handle}" if handle else "Username: (не указан)")
        header_lines.append("")
        header_lines.append(_build_ip_notification(entries))
        text = "\n".join(header_lines)
        if dry_run:
            logger.info(
                "Dry-run: would alert admins about unknown IP recipient",
                handle=handle,
                owner=owner_name,
            )
            return
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about missing IP recipient",
                    admin_id=admin_id,
                    handle=handle,
                    error=str(exc),
                )

    async def _notify_admins_ip_delivery_error(
        self,
        *,
        handle: Optional[str],
        entries: List[Dict[str, Any]],
        error_text: str,
        dry_run: bool,
    ) -> None:
        if not self.admin_ids or not entries:
            return
        lines = [
            "⚠️ Ошибка отправки уведомления об IP",
            f"Username: @{handle}" if handle else "Username: (не указан)",
            f"Ошибка: {error_text}",
            "",
            _build_ip_notification(entries),
        ]
        text = "\n".join(lines)
        if dry_run:
            logger.info(
                "Dry-run: would alert admins about IP delivery error",
                handle=handle,
                error=error_text,
            )
            return
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "Failed to notify admin about IP delivery error",
                    admin_id=admin_id,
                    handle=handle,
                    error=str(exc),
                )

    async def _alert_admins(self, unknown_items: List[Dict[str, Any]]) -> None:
        if not self.admin_ids or not unknown_items:
            return
        lines = [
            "⚠️ Не удалось отправить уведомление по IP:",
            "",
        ]
        for item in unknown_items[:20]:
            lines.append(
                f"{item.get('ip') or '—'} (до {item.get('expires_at') or '—'}) — @{item.get('handle') or '—'}"
            )
        if len(unknown_items) > 20:
            lines.append(f"… и ещё {len(unknown_items) - 20} IP")
        text = "\n".join(lines)
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about IPs",
                    admin_id=admin_id,
                    error=str(exc),
                )


@dataclass(slots=True)
class TicketNotifier:
    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]

    async def notify_completed_tickets(self, *, dry_run: bool = True) -> TicketNotifierStats:
        # Получаем список тикетов
        tickets = await self.underdog.fetch_tickets()
        stats = TicketNotifierStats(total_tickets=len(tickets))
        if not tickets:
            return stats

        # Собираем все уникальные handles для предварительной загрузки пользователей
        handles_to_fetch = set()
        valid_tickets = []
        
        for ticket in tickets:
            # Фильтруем только завершенные тикеты, которые еще не были отправлены
            status = ticket.get("status")
            if status != "completed":
                continue
            
            if _is_ticket_sent(ticket):
                continue
            
            handle, raw_handle, owner_name = _resolve_owner_fields(ticket)
            if handle:
                handles_to_fetch.add(handle)
                valid_tickets.append({
                    "ticket": ticket,
                    "handle": handle,
                    "raw_handle": raw_handle,
                    "owner_name": owner_name,
                })
            else:
                stats.missing_contact += 1
                stats.unknown_items.append(
                    {
                        "ticket_id": ticket.get("id"),
                        "type": ticket.get("type") or ticket.get("ticket_type"),
                        "owner": owner_name,
                    }
                )
                await self._notify_admins_missing_user(
                    handle=None,
                    entries=[{"raw": ticket}],
                    dry_run=dry_run,
                )

        if not valid_tickets:
            # Уведомляем админов о проблемах (и в dry_run, и в реальном режиме)
            if stats.unknown_items:
                await self._alert_admins(stats.unknown_items)
            return stats

        # Загружаем всех пользователей один раз
        user_map = await db.fetch_users_by_usernames(list(handles_to_fetch))

        # Обрабатываем каждый тикет индивидуально: получили -> отправили -> пометили
        for entry in valid_tickets:
            ticket = entry["ticket"]
            handle = entry["handle"]
            ticket_id = ticket.get("id")
            stats.completed_tickets += 1
            
            # Получаем пользователя из кэша
            user = user_map.get(handle)
            
            if not user:
                stats.unknown_user += 1
                stats.unknown_items.append(
                    {
                        "ticket_id": ticket_id,
                        "type": ticket.get("type") or ticket.get("ticket_type"),
                        "handle": handle,
                    }
                )
                await self._notify_admins_missing_user(
                    handle=handle,
                    entries=[{"raw": ticket}],
                    dry_run=dry_run,
                )
                continue

            # Формируем сообщение для одного тикета
            stats.matched_users += 1
            text = _build_ticket_notification([{"raw": ticket}])
            
            if dry_run:
                logger.info(
                    "Dry-run: would notify about completed ticket",
                    handle=handle,
                    ticket_id=ticket_id,
                    telegram_id=user.get("telegram_id"),
                )
                stats.notified_users += 1
                stats.notified_tickets += 1
                continue

            # Отправляем уведомление
            try:
                user_telegram_id = int(user["telegram_id"])
                await self.bot.send_message(user_telegram_id, text)
                stats.notified_users += 1
                stats.notified_tickets += 1
                # Дублируем отправленное сообщение всем админам для контроля
                # (но не самому пользователю, если он админ)
                await self._notify_admins_copy(text, exclude_user_id=user_telegram_id)
                
                # Сразу помечаем тикет как отправленный после успешной отправки
                if ticket_id is not None:
                    try:
                        await self.underdog.mark_ticket_telegram_sent(int(ticket_id))
                    except Exception as exc:
                        stats.errors += 1
                        logger.warning(
                            "Failed to mark ticket telegram_sent",
                            ticket_id=ticket_id,
                            error=str(exc),
                        )
                        # Уведомляем админов об ошибке пометки тикета
                        await self._notify_admins_mark_error(
                            ticket_id=ticket_id,
                            handle=handle,
                            error_text=str(exc),
                            dry_run=dry_run,
                        )
            except Exception as exc:
                stats.errors += 1
                logger.warning(
                    "Failed to send ticket notification message",
                    handle=handle,
                    ticket_id=ticket_id,
                    error=str(exc),
                )
                await self._notify_admins_ticket_delivery_error(
                    handle=handle,
                    entries=[{"raw": ticket}],
                    error_text=str(exc),
                    dry_run=dry_run,
                )

        # Уведомляем админов о проблемах (и в dry_run, и в реальном режиме)
        if stats.unknown_items:
            await self._alert_admins(stats.unknown_items)

        return stats

    async def _notify_admins_copy(self, text: str, exclude_user_id: Optional[int] = None) -> None:
        if not self.admin_ids:
            return
        for admin_id in self.admin_ids:
            # Пропускаем пользователя, если он админ (чтобы не дублировать сообщение)
            if exclude_user_id is not None and int(admin_id) == exclude_user_id:
                continue
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to send admin copy for tickets",
                    admin_id=admin_id,
                    error=str(exc),
                )

    async def _notify_admins_missing_user(
        self,
        *,
        handle: Optional[str],
        entries: List[Dict[str, Any]],
        dry_run: bool,
    ) -> None:
        if not self.admin_ids or not entries:
            return
        owner_name = (entries[0]["raw"].get("owner") or {}).get("name") if entries else None
        header_lines = [
            "⚠️ Не удалось доставить уведомление о тикете покупателю.",
        ]
        if owner_name:
            header_lines.append(f"Владелец: {owner_name}")
        header_lines.append(f"Username: @{handle}" if handle else "Username: (не указан)")
        header_lines.append("")
        header_lines.append(_build_ticket_notification(entries))
        text = "\n".join(header_lines)
        if dry_run:
            logger.info(
                "Dry-run: would alert admins about unknown ticket recipient",
                handle=handle,
                owner=owner_name,
            )
            return
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about missing ticket recipient",
                    admin_id=admin_id,
                    handle=handle,
                    error=str(exc),
                )

    async def _notify_admins_mark_error(
        self,
        *,
        ticket_id: int,
        handle: Optional[str],
        error_text: str,
        dry_run: bool,
    ) -> None:
        """Уведомляет админов об ошибке пометки тикета как отправленного."""
        if not self.admin_ids:
            return
        lines = [
            "⚠️ Ошибка пометки тикета как отправленного",
            f"Тикет ID: {ticket_id}",
            f"Username: @{handle}" if handle else "Username: (не указан)",
            f"Ошибка: {error_text}",
            "",
            "Тикет был отправлен пользователю, но не был помечен как отправленный в системе.",
        ]
        text = "\n".join(lines)
        if dry_run:
            logger.info(
                "Dry-run: would alert admins about ticket mark error",
                ticket_id=ticket_id,
                handle=handle,
                error=error_text,
            )
            return
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about ticket mark error",
                    admin_id=admin_id,
                    ticket_id=ticket_id,
                    error=str(exc),
                )

    async def _notify_admins_ticket_delivery_error(
        self,
        *,
        handle: Optional[str],
        entries: List[Dict[str, Any]],
        error_text: str,
        dry_run: bool,
    ) -> None:
        if not self.admin_ids or not entries:
            return
        lines = [
            "⚠️ Ошибка отправки уведомления о тикете",
            f"Username: @{handle}" if handle else "Username: (не указан)",
            f"Ошибка: {error_text}",
            "",
            _build_ticket_notification(entries),
        ]
        text = "\n".join(lines)
        if dry_run:
            logger.info(
                "Dry-run: would alert admins about ticket delivery error",
                handle=handle,
                error=error_text,
            )
            return
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about ticket delivery error",
                    admin_id=admin_id,
                    handle=handle,
                    error=str(exc),
                )

    async def _alert_admins(self, unknown_items: List[Dict[str, Any]]) -> None:
        if not self.admin_ids or not unknown_items:
            return
        lines = [
            "⚠️ Не удалось отправить уведомление по тикетам:",
            "",
        ]
        for item in unknown_items[:20]:
            lines.append(
                f"Тикет #{item.get('ticket_id') or '—'} ({item.get('type') or '—'}) — @{item.get('handle') or '—'}"
            )
        if len(unknown_items) > 20:
            lines.append(f"… и ещё {len(unknown_items) - 20} тикетов")
        text = "\n".join(lines)
        for admin_id in self.admin_ids:
            try:
                await self.bot.send_message(int(admin_id), text)
            except Exception as exc:
                logger.warning(
                    "Failed to notify admin about tickets",
                    admin_id=admin_id,
                    error=str(exc),
                )


def _is_domain_sent(domain: Dict[str, Any]) -> bool:
    for key in ("telegram_sent", "telegram_notified", "telegramSent", "telegramNotified"):
        value = domain.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                return True
            continue
        try:
            if int(value) == 1:
                return True
        except Exception:
            continue
    return False


def _build_domain_notification(entries: List[Dict[str, Any]]) -> str:
    sorted_entries = sorted(entries, key=lambda item: item.get("expires_at") or date.max)
    lines = ["Срок действия следующих доменов скоро истекает:", ""]
    for entry in sorted_entries:
        domain = entry["raw"].get("domain") or entry["raw"].get("name") or "—"
        expires_at = entry.get("expires_at")
        expires_text = expires_at.strftime("%d.%m.%Y") if expires_at else "неизвестно"
        lines.append(f"- {domain} (истекает {expires_text})")
    lines.extend(
        [
            "",
            "Для продления домена напишите в личку @apr1cot",
        ]
    )
    return "\n".join(lines)


def _build_ip_notification(entries: List[Dict[str, Any]]) -> str:
    sorted_entries = sorted(
        entries,
        key=lambda item: item.get("expires_at") or date.max,
    )
    lines: List[str] = ["⏳ Срок действия следующих IP скоро истекает:", ""]
    for entry in sorted_entries:
        ip_value = entry["raw"].get("ip") or entry["raw"].get("address") or "—"
        expires_at = entry.get("expires_at")
        days_left = entry.get("days_left")
        if expires_at:
            expires_text = expires_at.strftime("%d.%m.%Y")
        else:
            expires_text = "неизвестно"
        if isinstance(days_left, int):
            if days_left >= 0:
                suffix = f" (осталось {days_left} д.)"
            else:
                suffix = f" (просрочено {abs(days_left)} д.)"
        else:
            suffix = ""
        owner_display = entry.get("display_handle") or entry.get("owner_name") or "—"
        if owner_display and isinstance(owner_display, str) and not owner_display.startswith("@"):
            normalized_owner = _normalize_handle(owner_display)
            if normalized_owner:
                owner_display = f"@{normalized_owner}"
        lines.extend(
            [
                f"🖥 IP: {ip_value}",
                "",
                f"📅 Истекает: {expires_text}{suffix}",
                "",
                f"👤 Владелец: {owner_display}",
                "",
                "──────────────────",
                "",
            ]
        )
    lines.extend(
        [
            "Продлить ip: https://dashboard.underdog.click/managers/ips",
        ]
    )
    return "\n".join(lines).rstrip()


def _is_ip_sent(item: Dict[str, Any]) -> bool:
    for key in ("telegram_sent", "telegram_notified", "telegramSent", "telegramNotified"):
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                return True
            continue
        try:
            if int(value) == 1:
                return True
        except Exception:
            continue
    return False


def _get_ticket_type_name(ticket_type: Optional[str]) -> str:
    """Расшифровка типов тикетов."""
    type_map = {
        "transfer_accounts": "Перенос аккаунтов",
        "account_errors": "Ошибки аккаунтов",
        "withdraw_funds": "Вывод средств",
        "topup_nachonacho": "Пополнение Nacho",
        "proxy_issues": "Проблемы с прокси",
        "general_question": "Общий вопрос",
    }
    if not ticket_type:
        return "Неизвестный тип"
    return type_map.get(ticket_type, ticket_type)


def _is_ticket_sent(ticket: Dict[str, Any]) -> bool:
    """Проверяет, был ли тикет уже отправлен."""
    for key in ("telegram_sent", "telegram_notified", "telegramSent", "telegramNotified"):
        value = ticket.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                return True
            continue
        try:
            if int(value) == 1:
                return True
        except Exception:
            continue
    return False


def _build_ticket_notification(entries: List[Dict[str, Any]]) -> str:
    """Формирует сообщение о завершенных тикетах."""
    if len(entries) == 1:
        ticket = entries[0]["raw"]
        ticket_id = ticket.get("id") or "—"
        ticket_type = ticket.get("type") or ticket.get("ticket_type")
        type_name = _get_ticket_type_name(ticket_type)
        lines: List[str] = [
            f"✅ Ваш тикет ({ticket_id}) выполнен:",
            "",
            f"📋 Тип: {type_name}",
        ]
    else:
        lines: List[str] = [f"✅ Выполнено тикетов: {len(entries)}", ""]
        for entry in entries:
            ticket = entry["raw"]
            ticket_id = ticket.get("id") or "—"
            ticket_type = ticket.get("type") or ticket.get("ticket_type")
            type_name = _get_ticket_type_name(ticket_type)
            lines.extend(
                [
                    f"✅ Ваш тикет ({ticket_id}) выполнен:",
                    "",
                    f"📋 Тип: {type_name}",
                    "",
                    "──────────────────",
                    "",
                ]
            )
    
    return "\n".join(lines).rstrip()


def _create_bot() -> Bot:
    token = settings.orders_bot_token or settings.telegram_bot_token
    return Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )


def _create_design_bot() -> Bot:
    token = settings.design_bot_token or settings.telegram_bot_token
    if not settings.design_bot_token:
        logger.warning(
            "DESIGN_BOT_TOKEN not set: design notify will send via main bot (TELEGRAM_BOT_TOKEN). "
            "Admin copy will arrive in main bot, not DesignBot. Set DESIGN_BOT_TOKEN in cron env for DesignBot.",
        )
    return Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )


async def fetch_yesterday_orders(status_id: int = 1, telegram_sent: int = 0) -> List[Dict[str, Any]]:
    async with UnderdogClient.from_settings() as client:
        return await client.fetch_yesterday_orders(status_id=status_id, telegram_sent=telegram_sent)


async def notify_ready_orders(
    dry_run: bool = True,
    limit_user_ids: Optional[Sequence[int]] = None,
    bot_instance: Optional[Bot] = None,
) -> Dict[str, Any]:
    async with UnderdogClient.from_settings() as client:
        if bot_instance is not None:
            notifier = OrderNotifier(client, bot_instance, settings.admins)
            stats = await notifier.notify_ready_orders(
                dry_run=dry_run,
                limit_user_ids=limit_user_ids,
            )
            return stats.to_dict(dry_run=dry_run)
        async with _create_bot() as owned_bot:
            notifier = OrderNotifier(client, owned_bot, settings.admins)
            stats = await notifier.notify_ready_orders(
                dry_run=dry_run,
                limit_user_ids=limit_user_ids,
            )
            return stats.to_dict(dry_run=dry_run)


async def notify_design_assignments(
    dry_run: bool = True,
    bot_instance: Optional[Bot] = None,
) -> Dict[str, Any]:
    """Уведомлять дизайнера, когда ему ставят таск (order_status=0). Результат по contractor_id -> telegram."""
    async with UnderdogClient.from_settings() as client:
        if bot_instance is not None:
            notifier = DesignAssignmentNotifier(client, bot_instance, settings.admins, settings.design_broadcast_chat_ids)
            stats = await notifier.notify_design_assignments(dry_run=dry_run)
            return stats.to_dict(dry_run=dry_run)
        async with _create_design_bot() as owned_bot:
            notifier = DesignAssignmentNotifier(client, owned_bot, settings.admins, settings.design_broadcast_chat_ids)
            stats = await notifier.notify_design_assignments(dry_run=dry_run)
            return stats.to_dict(dry_run=dry_run)


async def notify_expiring_domains(
    *,
    dry_run: bool = True,
    days: int = 30,
    bot_instance: Optional[Bot] = None,
) -> Dict[str, Any]:
    async with UnderdogClient.from_settings() as client:
        if bot_instance is not None:
            notifier = DomainNotifier(client, bot_instance, settings.admins)
            stats = await notifier.notify_expiring_domains(dry_run=dry_run, days=days)
            return stats.to_dict(dry_run=dry_run)
        async with _create_bot() as owned_bot:
            notifier = DomainNotifier(client, owned_bot, settings.admins)
            stats = await notifier.notify_expiring_domains(dry_run=dry_run, days=days)
            return stats.to_dict(dry_run=dry_run)


async def notify_expiring_ips(
    *,
    dry_run: bool = True,
    days: int = 7,
    bot_instance: Optional[Bot] = None,
) -> Dict[str, Any]:
    async with UnderdogClient.from_settings() as client:
        if bot_instance is not None:
            notifier = IPNotifier(client, bot_instance, settings.admins)
            stats = await notifier.notify_expiring_ips(dry_run=dry_run, days=days)
            return stats.to_dict(dry_run=dry_run)
        async with _create_bot() as owned_bot:
            notifier = IPNotifier(client, owned_bot, settings.admins)
            stats = await notifier.notify_expiring_ips(dry_run=dry_run, days=days)
            return stats.to_dict(dry_run=dry_run)


async def notify_completed_tickets(
    *,
    dry_run: bool = True,
    bot_instance: Optional[Bot] = None,
) -> Dict[str, Any]:
    async with UnderdogClient.from_settings() as client:
        if bot_instance is not None:
            notifier = TicketNotifier(client, bot_instance, settings.admins)
            stats = await notifier.notify_completed_tickets(dry_run=dry_run)
            return stats.to_dict(dry_run=dry_run)
        async with _create_bot() as owned_bot:
            notifier = TicketNotifier(client, owned_bot, settings.admins)
            stats = await notifier.notify_completed_tickets(dry_run=dry_run)
            return stats.to_dict(dry_run=dry_run)


async def _amain() -> None:
    parser = ArgumentParser(description="Interact with the Underdog admin API")
    parser.add_argument("--orders", action="store_true", help="Fetch domain+transferDomain orders (orders bot)")
    parser.add_argument("--orders-design", action="store_true", help="Fetch pwaDesign+creative orders with order_status=1 (design bot)")
    parser.add_argument("--orders-design-new", action="store_true", help="Fetch pwaDesign+creative with order_status=0 (new tasks for assignment notify)")
    parser.add_argument("--notify", action="store_true", help="Send Telegram notifications for ready orders (orders bot)")
    parser.add_argument(
        "--notify-design",
        action="store_true",
        help="Notify designers when a task is assigned (order_status=0). Cron: python -m src.underdog --notify-design --apply",
    )
    parser.add_argument("--domains", action="store_true", help="Fetch all domains from Underdog")
    parser.add_argument("--notify-domains", action="store_true", help="Notify Telegram users about expiring domains")
    parser.add_argument("--ips", action="store_true", help="Fetch expiring IPs from Underdog")
    parser.add_argument("--notify-ips", action="store_true", help="Notify Telegram users about expiring IPs")
    parser.add_argument("--tickets", action="store_true", help="Fetch all tickets from Underdog")
    parser.add_argument("--notify-tickets", action="store_true", help="Notify Telegram users about completed tickets")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="With --notify: actually send messages and mark orders instead of dry-run",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Horizon in days for --notify-domains (default: 30)",
    )
    parser.add_argument(
        "--ip-days",
        type=int,
        default=7,
        help="Horizon in days for --notify-ips (default: 7)",
    )
    parser.add_argument("--raw-token", action="store_true", help="Print only the bearer token")
    args = parser.parse_args()

    async with UnderdogClient.from_settings() as client:
        if args.orders:
            orders = await client.fetch_orders_for_orders_bot()
            print(json.dumps({"count": len(orders), "orders": orders}, ensure_ascii=False, indent=2))
            return

        if args.orders_design:
            orders = await client.fetch_orders_for_design_bot()
            print(json.dumps({"count": len(orders), "orders": orders}, ensure_ascii=False, indent=2))
            return

        if args.orders_design_new:
            orders = await client.fetch_design_new_tasks()
            print(json.dumps({"count": len(orders), "orders": orders}, ensure_ascii=False, indent=2))
            return

        if args.notify:
            dry_run = not args.apply
            async with _create_bot() as bot_instance:
                notifier = OrderNotifier(client, bot_instance, settings.admins)
                stats = await notifier.notify_ready_orders(dry_run=dry_run)
            print(json.dumps(stats.to_dict(dry_run=dry_run), ensure_ascii=False, indent=2))
            return

        if args.notify_design:
            dry_run = not args.apply
            async with _create_design_bot() as bot_instance:
                notifier = DesignAssignmentNotifier(client, bot_instance, settings.admins, settings.design_broadcast_chat_ids)
                stats = await notifier.notify_design_assignments(dry_run=dry_run)
            print(json.dumps(stats.to_dict(dry_run=dry_run), ensure_ascii=False, indent=2))
            return

        if args.domains:
            domains = await client.fetch_domains()
            print(json.dumps({"count": len(domains), "domains": domains}, ensure_ascii=False, indent=2))
            return

        if args.notify_domains:
            dry_run = not args.apply
            async with _create_bot() as bot_instance:
                notifier = DomainNotifier(client, bot_instance, settings.admins)
                stats = await notifier.notify_expiring_domains(dry_run=dry_run, days=max(0, args.days))
            print(json.dumps(stats.to_dict(dry_run=dry_run), ensure_ascii=False, indent=2))
            return

        if args.ips:
            ips = await client.fetch_ips()
            print(json.dumps({"count": len(ips), "ips": ips}, ensure_ascii=False, indent=2))
            return

        if args.notify_ips:
            dry_run = not args.apply
            horizon = max(0, args.ip_days)
            async with _create_bot() as bot_instance:
                notifier = IPNotifier(client, bot_instance, settings.admins)
                stats = await notifier.notify_expiring_ips(dry_run=dry_run, days=horizon)
            print(json.dumps(stats.to_dict(dry_run=dry_run), ensure_ascii=False, indent=2))
            return

        if args.tickets:
            tickets = await client.fetch_tickets()
            print(json.dumps({"count": len(tickets), "tickets": tickets}, ensure_ascii=False, indent=2))
            return

        if args.notify_tickets:
            dry_run = not args.apply
            async with _create_bot() as bot_instance:
                notifier = TicketNotifier(client, bot_instance, settings.admins)
                stats = await notifier.notify_completed_tickets(dry_run=dry_run)
            print(json.dumps(stats.to_dict(dry_run=dry_run), ensure_ascii=False, indent=2))
            return

        token = await client.get_token(force_refresh=args.raw_token)
        print(token)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":  # pragma: no cover
    main()
