"""Shared parsing helpers for Underdog payloads, handles, admin recipients and local delivery marks."""

from __future__ import annotations
import json
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence
from loguru import logger
from .. import db
from ..config import settings
from ..utils.html import safe


class UnderdogAuthError(RuntimeError):
    """Raised when we fail to log into Underdog admin."""


class UnderdogAPIError(RuntimeError):
    """Raised when any Underdog API request fails."""


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


def _log_underdog_raw_json_response(
    *,
    method: str,
    path: str,
    raw: Any,
    max_chars: int = 150_000,
) -> None:
    """Пишет в лог сырой JSON ответа Underdog (структура + тело, с обрезкой по размеру)."""
    summary: Dict[str, Any] = {"root_type": type(raw).__name__}
    if isinstance(raw, dict):
        summary["root_keys"] = list(raw.keys())[:80]
    elif isinstance(raw, list):
        summary["list_len"] = len(raw)
        if raw and isinstance(raw[0], dict):
            sample = raw[0]
            summary["first_item_keys"] = list(sample.keys())[:60]
    try:
        body = json.dumps(raw, ensure_ascii=False, indent=2, default=str)
    except (TypeError, ValueError):
        body = repr(raw)
    if len(body) > max_chars:
        cut = len(body) - max_chars
        body = body[:max_chars] + f"\n... [{cut} chars truncated]"
    logger.info(
        "Underdog {} {} — raw JSON (log_raw/debug). Summary: {}\n{}",
        method,
        path,
        json.dumps(summary, ensure_ascii=False),
        body,
    )


def _extract_ip_record_id(raw: Any) -> Optional[int]:
    """ID записи IP в Underdog (в разных ответах может быть id / Id / ip_id)."""
    if not isinstance(raw, dict):
        return None
    for key in ("id", "Id", "ID", "ip_id", "ipId"):
        val = raw.get(key)
        if val is None:
            continue
        try:
            return int(val)
        except (TypeError, ValueError):
            continue
    return None


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


def _resolve_corporate_owner_fields(record: Dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Корпоративный @ из Underdog — единственный канал для IP/доменов (как заказы/тикеты)."""
    owner = record.get("owner") or {}
    raw_handle = owner.get("corporate_telegram") or record.get("corporate_telegram")
    normalized = _normalize_handle(raw_handle)
    owner_name = owner.get("name")
    return normalized, raw_handle, owner_name


def _corp_handle_admin_line(handle: Optional[str]) -> str:
    if handle:
        return f"Корп. Telegram: @{safe(handle)}"
    return "Корп. Telegram: (не указан)"


async def resolve_underdog_notify_admin_ids() -> List[int]:
    """Telegram IDs для алертов Underdog: числа и @username из UNDERDOG_NOTIFY_ADMINS."""
    ids: List[int] = list(settings.underdog_notify_admins)
    names = list(settings.underdog_notify_admin_usernames)
    if names:
        user_map = await db.fetch_users_by_usernames(names)
        for uname in names:
            row = user_map.get(uname.strip().lstrip("@").lower())
            if row and row.get("telegram_id") is not None:
                ids.append(int(row["telegram_id"]))
            else:
                logger.warning(
                    "Underdog notify admin @username not found in tg_users (нужен /start в боте)",
                    username=uname,
                )
    if ids:
        seen: set[int] = set()
        unique: List[int] = []
        for uid in ids:
            if uid not in seen:
                seen.add(uid)
                unique.append(uid)
        return unique
    return list(settings.admins)


def _resolve_order_owner_handle(order: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Корпоративный Telegram @ (owner или заказ/тикет) — единственный источник для Orders bot.

    Поле ``owner.telegram_id`` из API не используем: там часто устаревший личный аккаунт.
    Доставка только на chat_id из ``tg_users`` после /start, матч по этому нику.
    """
    owner = order.get("owner") or {}
    raw_handle = owner.get("corporate_telegram") or order.get("corporate_telegram")
    return _normalize_handle(raw_handle), raw_handle


def _entry_ids(entries: Sequence[Any], id_of) -> List[str]:
    ids = []
    for entry in entries:
        try:
            eid = id_of(entry)
        except Exception:
            eid = None
        if eid is not None:
            ids.append(str(eid))
    return ids


async def _drop_locally_sent(kind: str, chat_id: int, entries: List[Any], id_of, mark_remote) -> List[Any]:
    """Entries already delivered to chat_id (tg_underdog_sent) are not sent again — only the Underdog PATCH is retried.

    Returns the entries that still need a Telegram message.
    """
    sent = await db.underdog_sent_ids(kind, _entry_ids(entries, id_of), chat_id)
    if not sent:
        return entries
    fresh = []
    for entry in entries:
        ids = _entry_ids([entry], id_of)
        if ids and ids[0] in sent:
            try:
                await mark_remote(int(ids[0]))
                logger.info("Underdog {} {} already delivered to {}, re-marked telegram_sent", kind, ids[0], chat_id)
            except Exception as exc:
                logger.warning("Underdog {} {} re-mark failed: {}", kind, ids[0], exc)
        else:
            fresh.append(entry)
    return fresh


def _to_utc_aware(dt: datetime) -> datetime:
    """Normalize datetime from DB/API to UTC-aware."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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


def _is_telegram_sent(item: Dict[str, Any]) -> bool:
    """Underdog marks a domain/IP/ticket as delivered with any of these flags (bool or 1)."""
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
