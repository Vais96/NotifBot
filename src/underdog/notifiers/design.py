"""DesignBot: assignment, completion, SLA 24h and not-in-progress 48h notifications."""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional, Sequence
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from loguru import logger
from ... import db
from ...config import settings
from ...telegram_rate_limit import limited_send_message
from ...utils.html import safe
from ..client import UnderdogClient
from ..common import _parse_telegram_id, _to_utc_aware
from ..messages import _build_design_assignment_message, _build_design_completion_message, _build_design_not_in_progress_48h_message, _build_design_sla_warning_message, _format_duration_ru
from ..stats import NotificationStats


async def _finish_design_delivery(kind: str, order_id: int, delivered: int, mark_sent) -> bool:
    """Помечаем sent, если доставлено хоть кому-то; иначе не помечаем (повтор в след. цикле) и warning через throttle."""
    if delivered:
        await mark_sent(order_id)
        return True
    if await db.admin_notify_throttle_allow_send(f"design:{kind}:undelivered:{order_id}"):
        logger.warning("Design {} not delivered to anyone, not marked sent: order_id={}", kind, order_id)
    return False


async def _resolve_designer_telegram_id_from_order(
    order: Dict[str, Any],
    *,
    subscriber_ids: Optional[Iterable[int]] = None,
) -> tuple[Optional[int], Optional[str], Optional[str], Optional[str]]:
    contractor = order.get("contractor") or {}
    contractor_id = order.get("contractor_id")
    if contractor_id is not None:
        contractor_id = str(contractor_id).strip() or None
    telegram_from_order = (contractor.get("telegram") or contractor.get("telegram_handle") or "").strip().lstrip("@") or None
    designer_name: Optional[str] = contractor.get("name")

    designer_telegram_id: Optional[int] = _parse_telegram_id(contractor)
    if designer_telegram_id is None and telegram_from_order:
        user = await db.find_user_by_username(telegram_from_order)
        if user:
            designer_telegram_id = int(user.get("telegram_id"))
    if designer_telegram_id is None and contractor_id:
        designer_telegram_id = await db.get_contractor_telegram_id(contractor_id)
    if designer_telegram_id is None and telegram_from_order and subscriber_ids:
        designer_telegram_id = await db.find_telegram_id_among_subscribers_by_username(
            telegram_from_order,
            subscriber_ids,
        )
    return designer_telegram_id, contractor_id, telegram_from_order, designer_name


def _is_design_order_completed(order: Dict[str, Any]) -> bool:
    """Best-effort completion detection from different Underdog payload shapes."""
    status_id = order.get("status_id")
    try:
        if status_id is not None and int(status_id) == 1:
            return True
    except Exception:
        pass
    for key in ("status", "state", "order_status"):
        raw = order.get(key)
        if raw is None:
            continue
        s = str(raw).strip().lower()
        if s in ("completed", "complete", "done", "success", "выполнен"):
            return True
    return False


def _is_design_order_taken_in_progress(order: Dict[str, Any]) -> bool:
    """True when designer already moved task to 'в работе' or further in workflow."""
    if _is_design_order_completed(order):
        return True
    status_id = order.get("status_id")
    try:
        if status_id is not None:
            sid = int(status_id)
            if sid == 2:
                return True
            if sid >= 3:
                return True
    except (TypeError, ValueError):
        pass
    for key in ("status", "state", "status_name", "statusName"):
        raw = order.get(key)
        if not raw:
            continue
        s = str(raw).strip().lower()
        if "в работе" in s or "in progress" in s or s in ("in_progress", "working", "in work"):
            return True
    return False


def _is_design_order_awaiting_take_in_progress(order: Dict[str, Any]) -> bool:
    """True while task is still not taken in work."""
    return not _is_design_order_taken_in_progress(order)


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
            admin_ids=list(self.admin_ids) if self.admin_ids else "[] (set UNDERDOG_NOTIFY_ADMINS or ADMINS)",
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
                assigned_to_mention_html = f'<a href="tg://user?id={designer_telegram_id}">{safe(label)}</a>'
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

            delivered = 0
            # 1) В broadcast-чаты (группа/канал) — видят все участники
            for chat_id in self.broadcast_chat_ids:
                try:
                    await limited_send_message(self.bot, int(chat_id), text=message_for_broadcast)
                    stats.notified += 1
                    delivered += 1
                except (TelegramForbiddenError, TelegramBadRequest) as exc:
                    stats.errors += 1
                    logger.warning("Failed to send design assignment to broadcast chat", order_id=order_id, chat_id=chat_id, error=str(exc))
                except Exception:
                    stats.errors += 1
                    logger.exception("Unexpected error sending to broadcast chat", order_id=order_id, chat_id=chat_id)

            # 2) Подписчикам (кто нажал /start в DesignBot)
            for chat_id in subscribers:
                text = message_for_designer if (designer_telegram_id is not None and chat_id == designer_telegram_id) else message_for_broadcast
                try:
                    await limited_send_message(self.bot, chat_id, text=text)
                    stats.notified += 1
                    delivered += 1
                except (TelegramForbiddenError, TelegramBadRequest) as exc:
                    stats.errors += 1
                    logger.warning("Failed to send design assignment to subscriber", order_id=order_id, chat_id=chat_id, error=str(exc))
                except Exception:
                    stats.errors += 1
                    logger.exception("Unexpected error sending design assignment to subscriber", order_id=order_id, chat_id=chat_id)

            if self.admin_ids:
                for admin_id in self.admin_ids:
                    try:
                        await limited_send_message(self.bot, int(admin_id), text=admin_message)
                        delivered += 1
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
                logger.warning(
                    "No admin_ids configured (UNDERDOG_NOTIFY_ADMINS / ADMINS): admin copy not sent",
                    order_id=order_id,
                )

            if await _finish_design_delivery("assignment", int(order_id), delivered, db.mark_design_assignment_sent):
                stats.matched_users += 1
        return stats


@dataclass(slots=True)
class DesignCompletionNotifier:
    """Уведомление о выполнении таска (order_status=1)."""

    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]
    broadcast_chat_ids: Sequence[int] = ()  # DESIGN_BROADCAST_CHAT_IDS — группа/канал, видят все

    async def notify_design_completions(
        self,
        *,
        dry_run: bool = True,
    ) -> NotificationStats:
        # Fetch broadly, then detect completion from payload fields (status_id/status/state).
        orders_all = await self.underdog.fetch_design_orders_for_statuses(range(0, 8))
        orders = [o for o in orders_all if _is_design_order_completed(o)]
        stats = NotificationStats(total_orders=len(orders))
        if not orders:
            return stats

        subscribers = await db.list_design_bot_subscribers()
        now = datetime.now(timezone.utc)

        for order in orders:
            order_id = order.get("id")
            if order_id is None:
                continue
            order_id = int(order_id)

            if await db.is_design_completion_sent(order_id):
                logger.debug("Design completion already sent, skipping", order_id=order_id)
                continue

            contractor = order.get("contractor") or {}
            contractor_id = order.get("contractor_id")
            if contractor_id is not None:
                contractor_id = str(contractor_id).strip()
            telegram_from_order = (contractor.get("telegram") or contractor.get("telegram_handle") or "").strip().lstrip("@")
            designer_name: Optional[str] = contractor.get("name")

            designer_telegram_id: Optional[int] = _parse_telegram_id(contractor)
            if designer_telegram_id is None and telegram_from_order:
                user = await db.find_user_by_username(telegram_from_order)
                if user:
                    designer_telegram_id = int(user.get("telegram_id"))
            if designer_telegram_id is None and contractor_id:
                designer_telegram_id = await db.get_contractor_telegram_id(contractor_id)

            duration_text: Optional[str] = None
            assigned_at = await db.get_design_assignment_sent_at(order_id)
            if assigned_at is not None:
                duration_text = _format_duration_ru(now - _to_utc_aware(assigned_at))

            message_personal = _build_design_completion_message(order, duration_text=duration_text)

            assigned_to_mention_html: Optional[str] = None
            if designer_telegram_id is not None:
                label = (
                    f"@{telegram_from_order}"
                    if telegram_from_order
                    else (designer_name or f"id{designer_telegram_id}")
                )
                assigned_to_mention_html = f'<a href="tg://user?id={designer_telegram_id}">{safe(label)}</a>'

            message_for_broadcast = message_personal
            if assigned_to_mention_html is not None:
                message_for_broadcast = f"{message_personal}\nДизайнер: {assigned_to_mention_html}"

            admin_message = f"📋 Копия админу:\n\n{message_for_broadcast}"

            if dry_run:
                stats.matched_users += 1
                stats.notified += len(self.broadcast_chat_ids)
                if designer_telegram_id is not None and designer_telegram_id in subscribers:
                    stats.notified += 1
                stats.notified += len(self.admin_ids)
                continue

            delivered = 0
            # 1) В broadcast-чаты
            for chat_id in self.broadcast_chat_ids:
                try:
                    await limited_send_message(self.bot, int(chat_id), text=message_for_broadcast)
                    stats.notified += 1
                    delivered += 1
                except Exception as exc:  # pragma: no cover
                    stats.errors += 1
                    logger.warning(
                        "Failed to send design completion to broadcast chat",
                        order_id=order_id,
                        chat_id=chat_id,
                        error=str(exc),
                    )

            # 2) Лично дизайнеру (только если он в подписчиках)
            if designer_telegram_id is not None and designer_telegram_id in subscribers:
                try:
                    await limited_send_message(self.bot, designer_telegram_id, text=message_personal)
                    stats.notified += 1
                    delivered += 1
                except Exception as exc:  # pragma: no cover
                    stats.errors += 1
                    logger.warning(
                        "Failed to send design completion to designer",
                        order_id=order_id,
                        designer_telegram_id=designer_telegram_id,
                        error=str(exc),
                    )

            # 3) Админам
            if self.admin_ids:
                for admin_id in self.admin_ids:
                    try:
                        await limited_send_message(self.bot, int(admin_id), text=admin_message)
                        delivered += 1
                    except Exception as exc:  # pragma: no cover
                        stats.errors += 1
                        logger.warning(
                            "Failed to send design completion copy to admin",
                            order_id=order_id,
                            admin_id=admin_id,
                            error=str(exc),
                        )

            if await _finish_design_delivery("completion", order_id, delivered, db.mark_design_completion_sent):
                stats.matched_users += 1

        return stats


@dataclass(slots=True)
class DesignSLA24hNotifier:
    """Уведомление: прошло 24 часа после назначения, а статус ещё не выполнен."""

    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]
    broadcast_chat_ids: Sequence[int] = ()  # DESIGN_BROADCAST_CHAT_IDS — группа/канал, видят все
    sla_hours: int = 24

    async def notify_design_sla_24h(
        self,
        *,
        dry_run: bool = True,
    ) -> NotificationStats:
        orders_all = await self.underdog.fetch_design_orders_for_statuses(range(0, 8))
        orders_by_id: Dict[int, Dict[str, Any]] = {}
        for o in orders_all:
            oid = o.get("id")
            if oid is None:
                continue
            if _is_design_order_completed(o):
                continue
            orders_by_id[int(oid)] = o

        stats = NotificationStats(total_orders=len(orders_by_id))
        if not orders_by_id:
            return stats

        subscribers = await db.list_design_bot_subscribers()
        subscribers_set = set(subscribers)
        now = datetime.now(timezone.utc)
        sla_delta = timedelta(hours=int(self.sla_hours))

        for order_id, order in orders_by_id.items():
            if await db.is_design_completion_sent(order_id):
                continue
            if await db.is_design_sla_24h_alert_sent(order_id):
                continue

            assigned_at = await db.get_design_assignment_sent_at(order_id)
            if assigned_at is None:
                # Нет базового "времени получения таски" (мы не успели/не смогли отметить назначение)
                continue

            passed = now - _to_utc_aware(assigned_at)
            if passed < sla_delta:
                continue

            contractor = order.get("contractor") or {}
            contractor_id = order.get("contractor_id")
            if contractor_id is not None:
                contractor_id = str(contractor_id).strip()
            telegram_from_order = (contractor.get("telegram") or contractor.get("telegram_handle") or "").strip().lstrip("@")
            designer_name: Optional[str] = contractor.get("name")

            designer_telegram_id: Optional[int] = _parse_telegram_id(contractor)
            if designer_telegram_id is None and telegram_from_order:
                user = await db.find_user_by_username(telegram_from_order)
                if user:
                    designer_telegram_id = int(user.get("telegram_id"))
            if designer_telegram_id is None and contractor_id:
                designer_telegram_id = await db.get_contractor_telegram_id(contractor_id)

            passed_text = _format_duration_ru(passed)
            message_personal = _build_design_sla_warning_message(order, passed_text=passed_text)

            assigned_to_mention_html: Optional[str] = None
            if designer_telegram_id is not None:
                label = (
                    f"@{telegram_from_order}"
                    if telegram_from_order
                    else (designer_name or f"id{designer_telegram_id}")
                )
                assigned_to_mention_html = f'<a href="tg://user?id={designer_telegram_id}">{safe(label)}</a>'

            message_for_broadcast = message_personal
            if assigned_to_mention_html is not None:
                message_for_broadcast = f"{message_personal}\nДизайнер: {assigned_to_mention_html}"
            admin_message = f"📋 Копия админу:\n\n{message_for_broadcast}"

            if dry_run:
                stats.matched_users += 1
                if designer_telegram_id is not None and designer_telegram_id in subscribers_set:
                    stats.notified += 1
                stats.notified += len(self.broadcast_chat_ids) + len(self.admin_ids)
                continue

            delivered = 0
            # 1) Designer personal message
            if designer_telegram_id is not None and designer_telegram_id in subscribers_set:
                try:
                    await limited_send_message(self.bot, designer_telegram_id, text=message_personal)
                    stats.notified += 1
                    delivered += 1
                except Exception as exc:  # pragma: no cover
                    stats.errors += 1
                    logger.warning(
                        "Failed to send design SLA warning to designer",
                        order_id=order_id,
                        designer_telegram_id=designer_telegram_id,
                        error=str(exc),
                    )

            # 2) Broadcast chats (optional)
            for chat_id in self.broadcast_chat_ids:
                try:
                    await limited_send_message(self.bot, int(chat_id), text=message_for_broadcast)
                    stats.notified += 1
                    delivered += 1
                except Exception as exc:  # pragma: no cover
                    stats.errors += 1
                    logger.warning(
                        "Failed to send design SLA warning to broadcast chat",
                        order_id=order_id,
                        chat_id=chat_id,
                        error=str(exc),
                    )

            # 3) Admin copies
            if self.admin_ids:
                for admin_id in self.admin_ids:
                    try:
                        await limited_send_message(self.bot, int(admin_id), text=admin_message)
                        delivered += 1
                    except Exception as exc:  # pragma: no cover
                        stats.errors += 1
                        logger.warning(
                            "Failed to send design SLA warning copy to admin",
                            order_id=order_id,
                            admin_id=admin_id,
                            error=str(exc),
                        )

            if await _finish_design_delivery("sla_24h", order_id, delivered, db.mark_design_sla_24h_alert_sent):
                stats.matched_users += 1

        return stats


@dataclass(slots=True)
class DesignNotInProgress48hNotifier:
    """Reminder: 2 days passed from assignment, but task is still not taken in work."""

    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int] = ()
    broadcast_chat_ids: Sequence[int] = ()
    reminder_hours: int = 48

    async def notify_design_not_in_progress_48h(
        self,
        *,
        dry_run: bool = True,
    ) -> NotificationStats:
        pending_assignments = await db.list_design_assignments_pending_take_in_progress_reminder(
            self.reminder_hours
        )
        new_tasks = await self.underdog.fetch_design_new_tasks()
        new_tasks_by_id: Dict[int, Dict[str, Any]] = {}
        for o in new_tasks:
            oid = o.get("id")
            if oid is None:
                continue
            new_tasks_by_id[int(oid)] = o

        stats = NotificationStats(total_orders=len(pending_assignments))
        if not pending_assignments:
            return stats

        subscribers = await db.list_design_bot_subscribers()
        subscribers_set = set(subscribers)
        now = datetime.now(timezone.utc)
        reminder_delta = timedelta(hours=int(self.reminder_hours))

        for row in pending_assignments:
            order_id = int(row["order_id"])
            if await db.is_design_not_in_progress_48h_sent(order_id):
                continue

            order = new_tasks_by_id.get(order_id)
            if order is None:
                logger.debug(
                    "Take-in-progress reminder skipped: order not in order_status=0",
                    order_id=order_id,
                )
                continue
            if not _is_design_order_awaiting_take_in_progress(order):
                logger.debug(
                    "Take-in-progress reminder skipped: task already in progress",
                    order_id=order_id,
                    status_id=order.get("status_id"),
                )
                continue

            assigned_at = row.get("created_at")
            if assigned_at is None:
                assigned_at = await db.get_design_assignment_sent_at(order_id)
            if assigned_at is None:
                logger.debug("Take-in-progress reminder skipped: no assignment timestamp", order_id=order_id)
                continue

            passed = now - _to_utc_aware(assigned_at)
            if passed < reminder_delta:
                continue

            designer_telegram_id, contractor_id, telegram_from_order, designer_name = (
                await _resolve_designer_telegram_id_from_order(order, subscriber_ids=subscribers)
            )

            passed_text = _format_duration_ru(passed)
            message_personal = _build_design_not_in_progress_48h_message(
                order,
                passed_text=passed_text,
                reminder_hours=int(self.reminder_hours),
            )

            assigned_to_mention_html: Optional[str] = None
            if designer_telegram_id is not None:
                label = (
                    f"@{telegram_from_order}"
                    if telegram_from_order
                    else (designer_name or f"id{designer_telegram_id}")
                )
                assigned_to_mention_html = f'<a href="tg://user?id={designer_telegram_id}">{safe(label)}</a>'

            message_for_broadcast = message_personal
            if assigned_to_mention_html is not None:
                message_for_broadcast = f"{message_personal}\nДизайнер: {assigned_to_mention_html}"
            admin_message = f"📋 Копия админу:\n\n{message_for_broadcast}"

            if dry_run:
                stats.matched_users += 1
                if designer_telegram_id is not None and designer_telegram_id in subscribers_set:
                    stats.notified += 1
                stats.notified += len(self.broadcast_chat_ids) + len(self.admin_ids)
                continue

            delivered = 0

            if designer_telegram_id is not None and designer_telegram_id in subscribers_set:
                try:
                    await limited_send_message(self.bot, designer_telegram_id, text=message_personal)
                    stats.notified += 1
                    delivered += 1
                    logger.info(
                        "Design take-in-progress reminder sent to designer",
                        order_id=order_id,
                        designer_telegram_id=designer_telegram_id,
                    )
                except Exception as exc:  # pragma: no cover
                    stats.errors += 1
                    stats.delivery_failed += 1
                    logger.warning(
                        "Failed to send take-in-progress reminder to designer",
                        order_id=order_id,
                        designer_telegram_id=designer_telegram_id,
                        error=str(exc),
                    )
            else:
                logger.info(
                    "Design take-in-progress reminder: designer DM skipped (not linked or not subscribed)",
                    order_id=order_id,
                    contractor_id=contractor_id,
                    telegram=telegram_from_order,
                )

            for chat_id in self.broadcast_chat_ids:
                try:
                    await limited_send_message(self.bot, int(chat_id), text=message_for_broadcast)
                    stats.notified += 1
                    delivered += 1
                except Exception as exc:  # pragma: no cover
                    stats.errors += 1
                    logger.warning(
                        "Failed to send take-in-progress reminder to broadcast chat",
                        order_id=order_id,
                        chat_id=chat_id,
                        error=str(exc),
                    )

            if self.admin_ids:
                for admin_id in self.admin_ids:
                    try:
                        await limited_send_message(self.bot, int(admin_id), text=admin_message)
                        stats.notified += 1
                        delivered += 1
                    except Exception as exc:  # pragma: no cover
                        stats.errors += 1
                        logger.warning(
                            "Failed to send take-in-progress reminder copy to admin",
                            order_id=order_id,
                            admin_id=admin_id,
                            error=str(exc),
                        )

            if await _finish_design_delivery(
                "not_in_progress_48h", order_id, delivered, db.mark_design_not_in_progress_48h_sent
            ):
                stats.matched_users += 1

        return stats
