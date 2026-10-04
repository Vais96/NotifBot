"""Orders bot: expiring domains -> owner."""

from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence
from aiogram import Bot
from loguru import logger
from ... import db
from ...utils.html import safe
from ..client import UnderdogClient
from ..common import _corp_handle_admin_line, _drop_locally_sent, _entry_ids, _is_telegram_sent, _parse_date, _resolve_corporate_owner_fields
from ..messages import _build_domain_notification
from ..stats import DomainNotifierStats
from ..telegram_confirm import _telegram_api_response_dict, _telegram_http_ok_for_underdog, _telegram_message_confirmed
from .base import AdminAlerts


@dataclass(slots=True)
class DomainNotifier(AdminAlerts):
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
            if _is_telegram_sent(domain):
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
            handle, raw_handle, owner_name = _resolve_corporate_owner_fields(domain)
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

        user_map = await db.fetch_users_by_usernames(list(per_handle.keys()), orders_recipients=True)

        for handle, domain_entries in per_handle.items():
            user = user_map.get(handle)
            if not user:
                stats.unknown_user += len(domain_entries)
                stats.no_recipient_mapping += len(domain_entries)
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
                chat_id = int(user["telegram_id"])
                domain_entries = await _drop_locally_sent(
                    "domain", chat_id, domain_entries, lambda e: e["raw"].get("id"), self.underdog.mark_domain_telegram_sent
                )
                if not domain_entries:
                    continue
                text = _build_domain_notification(domain_entries)
                msg = await self._send_and_log(chat_id, text, context="orders_bot_domain_expiration", extra={
                        "handle": handle,
                        "domains_count": len(domain_entries),
                    })
                if not _telegram_message_confirmed(msg):
                    stats.errors += 1
                    stats.delivery_failed += len(domain_entries)
                    logger.error(
                        "Telegram ok!==true (нет message_id) — не помечаем domain telegram_sent в Underdog",
                        telegram_id=chat_id,
                        response=_telegram_api_response_dict(msg),
                    )
                elif not _telegram_http_ok_for_underdog(msg):
                    stats.errors += 1
                    stats.delivery_failed += len(domain_entries)
                    logger.error(
                        "Telegram HTTP !== 200 — не помечаем domain telegram_sent в Underdog",
                        telegram_id=chat_id,
                        http_status=getattr(msg, "__tg_http_status__", None),
                        response=_telegram_api_response_dict(msg),
                    )
                else:
                    stats.notified_users += 1
                    stats.notified_domains += len(domain_entries)
                    await self._notify_admins_copy(text)
                    await db.mark_underdog_sent("domain", _entry_ids(domain_entries, lambda e: e["raw"].get("id")), chat_id)
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
                    hk = handle or "none"
                    await db.admin_notify_throttle_clear(f"domains:delivery_err:h:{hk}")
                    await db.admin_notify_throttle_clear(f"domains:missing:h:{hk}")
            except Exception as exc:
                stats.errors += 1
                stats.delivery_failed += len(domain_entries)
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
        await self._send_to_admins(text, what="domains copy")

    async def _alert_admins(self, unknown_items: List[Dict[str, Any]]) -> None:
        await self._alert_admins_digest(
            unknown_items,
            title="⚠️ Не удалось отправить уведомление по доменам:",
            render=lambda item: (
                f"{safe(item.get('domain') or '—')} (до {safe(item.get('expires_at') or '—')}) "
                f"— @{safe(item.get('handle') or '—')}"
            ),
            noun="доменов",
            dedupe_key="domains:unknown_digest",
        )

    async def _notify_admins_missing_user(
        self, *, handle: Optional[str], entries: List[Dict[str, Any]], dry_run: bool
    ) -> None:
        if not entries:
            return
        owner_name = (entries[0]["raw"].get("owner") or {}).get("name")
        lines = ["⚠️ Не удалось доставить уведомление о доменах покупателю."]
        if owner_name:
            lines.append(f"Владелец: {safe(owner_name)}")
        lines += [_corp_handle_admin_line(handle), "", _build_domain_notification(entries)]
        await self._alert_admins_throttled(
            "\n".join(lines),
            dedupe_key=f"domains:missing:h:{handle or 'none'}",
            what=f"unknown domain recipient @{handle}",
            dry_run=dry_run,
        )

    async def _notify_admins_domain_delivery_error(
        self, *, handle: Optional[str], entries: List[Dict[str, Any]], error_text: str, dry_run: bool
    ) -> None:
        if not entries:
            return
        lines = [
            "⚠️ Ошибка отправки уведомления о доменах",
            _corp_handle_admin_line(handle),
            f"Ошибка: {safe(error_text)}",
            "",
            _build_domain_notification(entries),
        ]
        await self._alert_admins_throttled(
            "\n".join(lines),
            dedupe_key=f"domains:delivery_err:h:{handle or 'none'}",
            what=f"domain delivery error @{handle}",
            dry_run=dry_run,
        )
