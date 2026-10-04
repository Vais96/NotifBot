"""Orders bot: completed tickets -> owner."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence
from aiogram import Bot
from loguru import logger
from ... import db
from ...utils.html import safe
from ..client import UnderdogClient
from ..common import _corp_handle_admin_line, _drop_locally_sent, _is_telegram_sent, _resolve_order_owner_handle
from ..messages import _build_ticket_notification
from ..stats import TicketNotifierStats
from ..telegram_confirm import _telegram_api_response_dict, _telegram_http_ok_for_underdog, _telegram_message_confirmed
from .base import AdminAlerts


@dataclass(slots=True)
class TicketNotifier(AdminAlerts):
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
            
            if _is_telegram_sent(ticket):
                continue

            owner = ticket.get("owner") or {}
            owner_name = owner.get("name")
            handle, raw_handle = _resolve_order_owner_handle(ticket)

            if handle:
                handles_to_fetch.add(handle)
                valid_tickets.append(
                    {
                        "ticket": ticket,
                        "handle": handle,
                        "raw_handle": raw_handle,
                        "owner_name": owner_name,
                    }
                )
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
        user_map = await db.fetch_users_by_usernames(list(handles_to_fetch), orders_recipients=True)

        # Обрабатываем каждый тикет индивидуально: получили -> отправили -> пометили
        for entry in valid_tickets:
            ticket = entry["ticket"]
            handle = entry.get("handle")
            ticket_id = ticket.get("id")
            stats.completed_tickets += 1

            user = user_map.get(handle) if handle else None
            if user is not None:
                db_user = await db.get_user(int(user["telegram_id"]))
                if db_user is not None and not bool(db_user.get("is_active")):
                    stats.unknown_user += 1
                    stats.no_recipient_mapping += 1
                    stats.unknown_items.append(
                        {
                            "ticket_id": ticket_id,
                            "type": ticket.get("type") or ticket.get("ticket_type"),
                            "handle": handle,
                        }
                    )
                    logger.warning(
                        "Ticket recipient is deactivated in tg_users, skip send",
                        telegram_id=user.get("telegram_id"),
                        ticket_id=ticket_id,
                    )
                    continue

            if not user:
                stats.unknown_user += 1
                stats.no_recipient_mapping += 1
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
                if not await _drop_locally_sent(
                    "ticket", user_telegram_id, [ticket], lambda t: t.get("id"), self.underdog.mark_ticket_telegram_sent
                ):
                    continue
                msg = await self._send_and_log(user_telegram_id, text, context="orders_bot_ticket_completed", extra={"ticket_id": ticket_id, "handle": handle})
                if not _telegram_message_confirmed(msg):
                    stats.errors += 1
                    stats.delivery_failed += 1
                    logger.error(
                        "Ticket notify: Telegram ok!==true — Underdog telegram_sent не меняем",
                        ticket_id=ticket_id,
                        chat_id=user_telegram_id,
                        response=_telegram_api_response_dict(msg),
                    )
                    await self._notify_admins_ticket_delivery_error(
                        handle=handle,
                        entries=[{"raw": ticket}],
                        error_text="Telegram: ok!==true или нет message_id, тикет не помечен отправленным",
                        dry_run=dry_run,
                    )
                elif not _telegram_http_ok_for_underdog(msg):
                    stats.errors += 1
                    stats.delivery_failed += 1
                    logger.error(
                        "Ticket notify: Telegram HTTP !== 200 — Underdog telegram_sent не меняем",
                        ticket_id=ticket_id,
                        chat_id=user_telegram_id,
                        http_status=getattr(msg, "__tg_http_status__", None),
                        response=_telegram_api_response_dict(msg),
                    )
                    await self._notify_admins_ticket_delivery_error(
                        handle=handle,
                        entries=[{"raw": ticket}],
                        error_text="Telegram: HTTP статус не 200, тикет не помечен отправленным",
                        dry_run=dry_run,
                    )
                else:
                    stats.notified_users += 1
                    stats.notified_tickets += 1
                    await self._notify_admins_copy(text, exclude_user_id=user_telegram_id)
                    if ticket_id is not None:
                        await db.mark_underdog_sent("ticket", [str(ticket_id)], user_telegram_id)
                        try:
                            await self.underdog.mark_ticket_telegram_sent(int(ticket_id))
                            tid = int(ticket_id)
                            await db.admin_notify_throttle_clear(f"tickets:delivery_err:{tid}")
                            await db.admin_notify_throttle_clear(f"tickets:mark_err:{tid}")
                            await db.admin_notify_throttle_clear(f"tickets:missing_user:{tid}")
                        except Exception as exc:
                            stats.errors += 1
                            logger.warning(
                                "Failed to mark ticket telegram_sent",
                                ticket_id=ticket_id,
                                error=str(exc),
                            )
                            await self._notify_admins_mark_error(
                                ticket_id=ticket_id,
                                handle=handle,
                                error_text=str(exc),
                                dry_run=dry_run,
                            )
            except Exception as exc:
                stats.errors += 1
                stats.delivery_failed += 1
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
        await self._send_to_admins(text, what="tickets copy", exclude_user_id=exclude_user_id)

    async def _notify_admins_missing_user(
        self, *, handle: Optional[str], entries: List[Dict[str, Any]], dry_run: bool
    ) -> None:
        if not entries:
            return
        raw = entries[0].get("raw")
        owner_name = (raw.get("owner") or {}).get("name") if isinstance(raw, dict) else None
        tid = raw.get("id") if isinstance(raw, dict) else None
        lines = ["⚠️ Не удалось доставить уведомление о тикете покупателю."]
        if owner_name:
            lines.append(f"Владелец: {safe(owner_name)}")
        lines += [_corp_handle_admin_line(handle), "", _build_ticket_notification(entries)]
        await self._alert_admins_throttled(
            "\n".join(lines),
            dedupe_key=f"tickets:missing_user:{tid}" if tid is not None else "tickets:missing_user:unknown",
            what=f"unknown ticket recipient @{handle}",
            dry_run=dry_run,
        )

    async def _notify_admins_mark_error(
        self, *, ticket_id: int, handle: Optional[str], error_text: str, dry_run: bool
    ) -> None:
        """Тикет доставлен, но не помечен telegram_sent в Underdog."""
        lines = [
            "⚠️ Ошибка пометки тикета как отправленного",
            f"Тикет ID: {safe(ticket_id)}",
            _corp_handle_admin_line(handle),
            f"Ошибка: {safe(error_text)}",
            "",
            "Тикет был отправлен пользователю, но не был помечен как отправленный в системе.",
        ]
        await self._alert_admins_throttled(
            "\n".join(lines),
            dedupe_key=f"tickets:mark_err:{int(ticket_id)}",
            what=f"ticket {ticket_id} mark error",
            dry_run=dry_run,
        )

    async def _notify_admins_ticket_delivery_error(
        self, *, handle: Optional[str], entries: List[Dict[str, Any]], error_text: str, dry_run: bool
    ) -> None:
        if not entries:
            return
        raw = entries[0].get("raw")
        tid = raw.get("id") if isinstance(raw, dict) else None
        lines = [
            "⚠️ Ошибка отправки уведомления о тикете",
            _corp_handle_admin_line(handle),
            f"Ошибка: {safe(error_text)}",
            "",
            _build_ticket_notification(entries),
        ]
        await self._alert_admins_throttled(
            "\n".join(lines),
            dedupe_key=f"tickets:delivery_err:{tid}" if tid is not None else "tickets:delivery_err:unknown",
            what=f"ticket delivery error @{handle}",
            dry_run=dry_run,
        )

    async def _alert_admins(self, unknown_items: List[Dict[str, Any]]) -> None:
        await self._alert_admins_digest(
            unknown_items,
            title="⚠️ Не удалось отправить уведомление по тикетам:",
            render=lambda item: (
                f"Тикет #{safe(item.get('ticket_id') or '—')} ({safe(item.get('type') or '—')}) "
                f"— @{safe(item.get('handle') or '—')}"
            ),
            noun="тикетов",
            dedupe_key="tickets:unknown_digest",
        )
