"""Orders bot: expiring IPs -> owner (admin alerts via the main bot)."""

from __future__ import annotations
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from loguru import logger
from ... import db
from ...config import settings
from ...utils.html import safe
from ..client import UnderdogClient
from ..common import _corp_handle_admin_line, _drop_locally_sent, _entry_ids, _extract_ip_record_id, _is_telegram_sent, _parse_date, _resolve_corporate_owner_fields
from ..messages import _build_ip_notification
from ..stats import IPNotifierStats, _log_ip_notify_delivery_report
from ..telegram_confirm import _telegram_http_ok_for_underdog, _telegram_message_confirmed
from .base import AdminAlerts


@dataclass(slots=True)
class IPNotifier(AdminAlerts):
    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]
    # Сообщения пользователям — через bot (часто orders_bot). Алерты админам: сначала admin_bot (главный бот),
    # иначе админы без чата с orders bot не получат уведомления.
    admin_bot: Optional[Bot] = None

    async def _mark_ip_entries_in_underdog(
        self,
        *,
        handle: str,
        telegram_id: int,
        ip_entries: List[Dict[str, Any]],
        stats: IPNotifierStats,
    ) -> None:
        """
        После успешной отправки в TG — PATCH telegram-sent для каждого IP.
        Ошибки только в stats.underdog_mark_failures; наружу не пробрасываем (иначе ложный send_failures).
        """
        try:
            for idx, entry in enumerate(ip_entries):
                raw = entry.get("raw") if isinstance(entry, dict) else None
                if not isinstance(raw, dict):
                    stats.errors += 1
                    stats.underdog_mark_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "ip_id": None,
                            "exc_type": "ValueError",
                            "error": "запись IP без dict raw — нет id для PATCH",
                        }
                    )
                    logger.warning(
                        "IP notify skip PATCH: raw не dict",
                        handle=handle,
                        entry_index=idx,
                    )
                    continue
                ip_id = _extract_ip_record_id(raw)
                if ip_id is None:
                    stats.errors += 1
                    stats.underdog_mark_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "ip_id": None,
                            "exc_type": "ValueError",
                            "error": "в объекте IP нет поля id/Id/ip_id",
                        }
                    )
                    logger.warning(
                        "IP notify skip PATCH: не извлечён id",
                        handle=handle,
                        raw_keys=list(raw.keys())[:40],
                    )
                    continue
                try:
                    await self.underdog.mark_ip_telegram_sent(ip_id)
                except Exception as exc:  # pragma: no cover
                    stats.errors += 1
                    stats.underdog_mark_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "ip_id": ip_id,
                            "exc_type": type(exc).__name__,
                            "error": str(exc),
                        }
                    )
                    logger.warning(
                        "Failed to mark IP telegram_sent: ip_id={} path=PATCH /api/v2/ip/{{id}}/telegram-sent error={}",
                        ip_id,
                        exc,
                    )
                if idx < len(ip_entries) - 1:
                    await asyncio.sleep(0.35)
        except Exception as exc:  # pragma: no cover
            stats.errors += 1
            stats.underdog_mark_failures.append(
                {
                    "handle": handle,
                    "telegram_id": telegram_id,
                    "ip_id": None,
                    "exc_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            logger.opt(exception=exc).error(
                "Unexpected error while marking IPs in Underdog after TG send",
                handle=handle,
                telegram_id=telegram_id,
            )

    async def notify_expiring_ips(self, *, dry_run: bool = True, days: int = 7) -> IPNotifierStats:
        """
        Список IP приходит с Underdog API уже отфильтрованным (кому нужно уведомление).
        Клиентский фильтр по горизонту дат не применяем; параметр days оставлен для совместимости API/CLI.
        """
        _ = days  # совместимость с POST /underdog/ip/notify и CLI --ip-days
        ips = await self.underdog.fetch_ips()
        stats = IPNotifierStats(total_ips=len(ips))
        if not ips:
            logger.info("IP notify: список IP пуст, рассылка не требуется")
            return stats

        logger.info(
            "IP notify",
            ips_count=len(ips),
            bot_orders_bot=bool(settings.orders_bot_token),
            note="список IP с API — без доп. фильтра по дате на нашей стороне",
        )
        per_handle: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        today = datetime.now(timezone.utc).date()

        for ip_entry in ips:
            if _is_telegram_sent(ip_entry):
                continue
            handle, raw_handle, owner_name = _resolve_corporate_owner_fields(ip_entry)
            expires_at = _parse_date(
                ip_entry.get("expires_at")
                or ip_entry.get("expires")
                or ip_entry.get("expiration")
            )
            days_left = (expires_at - today).days if expires_at else None
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
                        "days_left": days_left,
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
                    "days_left": days_left,
                    "display_handle": raw_handle,
                    "owner_name": owner_name,
                }
            )

        if not per_handle:
            _log_ip_notify_delivery_report(stats, dry_run=dry_run)
            return stats

        user_map = await db.fetch_users_by_usernames(list(per_handle.keys()), orders_recipients=True)

        for handle, ip_entries in per_handle.items():
            user = user_map.get(handle)
            if not user:
                stats.unknown_user += len(ip_entries)
                stats.no_recipient_mapping += len(ip_entries)
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
                stats.dry_run_preview.append(
                    {
                        "handle": handle,
                        "telegram_id": user.get("telegram_id"),
                        "ips_count": len(ip_entries),
                        "ips": [
                            e["raw"].get("ip") or e["raw"].get("address")
                            for e in ip_entries
                        ],
                    }
                )
                continue

            try:
                telegram_id = int(user["telegram_id"])
                ip_entries = await _drop_locally_sent(
                    "ip", telegram_id, ip_entries, lambda e: _extract_ip_record_id(e["raw"]), self.underdog.mark_ip_telegram_sent
                )
                if not ip_entries:
                    continue
                text = _build_ip_notification(ip_entries)
                msg = await self._send_and_log(telegram_id, text, context="orders_bot_ip_expiration", extra={
                        "handle": handle,
                        "ips_count": len(ip_entries),
                        "ips": [
                            e["raw"].get("ip") or e["raw"].get("address")
                            for e in ip_entries
                        ],
                    })
                # В Underdog помечаем telegram_sent только при ok/message_id и HTTP 200
                if not _telegram_message_confirmed(msg):
                    stats.errors += 1
                    stats.send_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "exc_type": "TelegramResponse",
                            "error": "ответ без message_id, Underdog не обновлён",
                        }
                    )
                    logger.error(
                        "Telegram returned message without message_id — не помечаем IP telegram_sent в Underdog",
                        handle=handle,
                        telegram_id=telegram_id,
                    )
                    await self._notify_admins_ip_delivery_error(
                        handle=handle,
                        entries=ip_entries,
                        error_text="Telegram: ok!==true или нет message_id, Underdog не обновлён",
                        dry_run=dry_run,
                    )
                elif not _telegram_http_ok_for_underdog(msg):
                    stats.errors += 1
                    stats.send_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "exc_type": "TelegramHttpStatus",
                            "error": f"HTTP не 200 (получен {getattr(msg, '__tg_http_status__', None)!r}), Underdog не обновлён",
                        }
                    )
                    logger.error(
                        "Telegram HTTP !== 200 — не помечаем IP telegram_sent в Underdog",
                        handle=handle,
                        telegram_id=telegram_id,
                        http_status=getattr(msg, "__tg_http_status__", None),
                    )
                    await self._notify_admins_ip_delivery_error(
                        handle=handle,
                        entries=ip_entries,
                        error_text="Telegram: HTTP статус не 200, Underdog не обновлён",
                        dry_run=dry_run,
                    )
                else:
                    stats.notified_users += 1
                    stats.notified_ips += len(ip_entries)
                    stats.delivered.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "message_id": getattr(msg, "message_id", None),
                            "ips_count": len(ip_entries),
                            "ips": [
                                e["raw"].get("ip") or e["raw"].get("address")
                                for e in ip_entries
                            ],
                        }
                    )
                    await db.mark_underdog_sent(
                        "ip", _entry_ids(ip_entries, lambda e: _extract_ip_record_id(e["raw"])), telegram_id
                    )
                    await self._mark_ip_entries_in_underdog(
                        handle=handle,
                        telegram_id=telegram_id,
                        ip_entries=ip_entries,
                        stats=stats,
                    )
                    hk = handle or "none"
                    await db.admin_notify_throttle_clear(f"ip:delivery_err:h:{hk}")
                    await db.admin_notify_throttle_clear(f"ip:missing:h:{hk}")
            except (TelegramForbiddenError, TelegramBadRequest) as exc:
                stats.errors += 1
                stats.send_failures.append(
                    {
                        "handle": handle,
                        "telegram_id": user.get("telegram_id"),
                        "exc_type": type(exc).__name__,
                        "error": str(exc),
                        "ips": [
                            e["raw"].get("ip") or e["raw"].get("address")
                            for e in ip_entries
                        ],
                    }
                )
                logger.warning(
                    "IP notification not delivered (user blocked bot or chat not found)",
                    handle=handle,
                    telegram_id=user.get("telegram_id"),
                    exc_type=type(exc).__name__,
                    error=str(exc),
                    ips=[
                        e["raw"].get("ip") or e["raw"].get("address")
                        for e in ip_entries
                    ],
                )
                await self._notify_admins_ip_delivery_error(
                    handle=handle,
                    entries=ip_entries,
                    error_text=str(exc),
                    dry_run=dry_run,
                )
            except Exception as exc:  # pragma: no cover
                stats.errors += 1
                stats.send_failures.append(
                    {
                        "handle": handle,
                        "telegram_id": user.get("telegram_id"),
                        "exc_type": type(exc).__name__,
                        "error": str(exc),
                        "ips": [
                            e["raw"].get("ip") or e["raw"].get("address")
                            for e in ip_entries
                        ],
                    }
                )
                logger.warning(
                    "Failed to send IP expiration message",
                    handle=handle,
                    telegram_id=user.get("telegram_id"),
                    exc_type=type(exc).__name__,
                    error=str(exc),
                    ips=[
                        e["raw"].get("ip") or e["raw"].get("address")
                        for e in ip_entries
                    ],
                )
                await self._notify_admins_ip_delivery_error(
                    handle=handle,
                    entries=ip_entries,
                    error_text=str(exc),
                    dry_run=dry_run,
                )

        if stats.unknown_items and dry_run:
            await self._alert_admins(stats.unknown_items)

        _log_ip_notify_delivery_report(stats, dry_run=dry_run)
        return stats

    async def _notify_admins_missing_ip(
        self, *, handle: Optional[str], entries: List[Dict[str, Any]], dry_run: bool
    ) -> None:
        if not entries:
            return
        lines = ["⚠️ Не удалось доставить уведомление об IP пользователю."]
        owner_name = entries[0].get("owner_name")
        if owner_name:
            lines.append(f"Владелец: {safe(owner_name)}")
        lines += [_corp_handle_admin_line(handle), "", _build_ip_notification(entries)]
        await self._alert_admins_throttled(
            "\n".join(lines),
            dedupe_key=f"ip:missing:h:{handle or 'none'}",
            what=f"unknown IP recipient @{handle}",
            dry_run=dry_run,
        )

    async def _notify_admins_ip_delivery_error(
        self, *, handle: Optional[str], entries: List[Dict[str, Any]], error_text: str, dry_run: bool
    ) -> None:
        if not entries:
            return
        lines = [
            "⚠️ Ошибка отправки уведомления об IP",
            _corp_handle_admin_line(handle),
            f"Ошибка: {safe(error_text)}",
            "",
            _build_ip_notification(entries),
        ]
        await self._alert_admins_throttled(
            "\n".join(lines),
            dedupe_key=f"ip:delivery_err:h:{handle or 'none'}",
            what=f"IP delivery error @{handle}",
            dry_run=dry_run,
        )

    async def _alert_admins(self, unknown_items: List[Dict[str, Any]]) -> None:
        await self._alert_admins_digest(
            unknown_items,
            title="⚠️ Не удалось отправить уведомление по IP:",
            render=lambda item: (
                f"{safe(item.get('ip') or '—')} (до {safe(item.get('expires_at') or '—')}) "
                f"— @{safe(item.get('handle') or '—')}"
            ),
            noun="IP",
            dedupe_key="ip:unknown_digest",
        )
