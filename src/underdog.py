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
            raise UnderdogAPIError(f"Request {method} {path} failed with status {resp.status_code}") from exc
        return resp

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
        orders = await self.underdog.fetch_yesterday_orders()
        limit_set: Optional[set[int]] = None
        if limit_user_ids is not None:
            limit_set = {int(uid) for uid in limit_user_ids}
        stats = NotificationStats(total_orders=len(orders) if limit_set is None else 0)
        if not orders:
            return stats

        handles = [_normalize_handle((order.get("owner") or {}).get("telegram")) for order in orders]
        valid_handles = [handle for handle in handles if handle]
        user_map = await db.fetch_users_by_usernames(valid_handles)

        for order, handle in zip(orders, handles):
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
                        "owner": (order.get("owner") or {}).get("name"),
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
            except Exception as exc:  # pragma: no cover
                stats.errors += 1
                logger.exception(
                    "Unexpected error while notifying order",
                    order_id=order.get("id"),
                    error=str(exc),
                )

        if stats.unknown_user > 0 and not dry_run:
            await self._alert_admins(stats.unknown_orders)

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
            if not expires_at or expires_at > cutoff:
                continue
            stats.expiring_domains += 1
            handle = _normalize_handle(
                (domain.get("owner") or {}).get("telegram")
                or (domain.get("owner") or {}).get("telegram_handle")
                or domain.get("telegram")
                or domain.get("telegram_handle")
            )
            if not handle:
                stats.missing_contact += 1
                stats.unknown_items.append(
                    {
                        "domain": domain.get("domain") or domain.get("name"),
                        "expires_at": expires_at.isoformat() if expires_at else None,
                        "owner": (domain.get("owner") or {}).get("name"),
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

        if stats.unknown_items and dry_run:
            await self._alert_admins(stats.unknown_items)

        return stats

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


def _create_bot() -> Bot:
    token = settings.orders_bot_token or settings.telegram_bot_token
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


async def _amain() -> None:
    parser = ArgumentParser(description="Interact with the Underdog admin API")
    parser.add_argument("--orders", action="store_true", help="Fetch yesterday's pending telegram orders")
    parser.add_argument("--notify", action="store_true", help="Send Telegram notifications for ready orders")
    parser.add_argument("--domains", action="store_true", help="Fetch all domains from Underdog")
    parser.add_argument("--notify-domains", action="store_true", help="Notify Telegram users about expiring domains")
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
    parser.add_argument("--raw-token", action="store_true", help="Print only the bearer token")
    args = parser.parse_args()

    async with UnderdogClient.from_settings() as client:
        if args.orders:
            orders = await client.fetch_yesterday_orders()
            print(json.dumps({"count": len(orders), "orders": orders}, ensure_ascii=False, indent=2))
            return

        if args.notify:
            dry_run = not args.apply
            async with _create_bot() as bot_instance:
                notifier = OrderNotifier(client, bot_instance, settings.admins)
                stats = await notifier.notify_ready_orders(dry_run=dry_run)
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

        token = await client.get_token(force_refresh=args.raw_token)
        print(token)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":  # pragma: no cover
    main()
