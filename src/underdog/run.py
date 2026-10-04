"""Entry points used by the app and CLI: build client + bot + notifier, run, return stats dict."""

from __future__ import annotations
import inspect
from contextlib import AsyncExitStack
from typing import Any, Dict, Optional, Sequence
from aiogram import Bot
from loguru import logger
from ..config import secret, settings
from ..telegram_rate_limit import make_bot
from .client import UnderdogClient
from .common import resolve_underdog_notify_admin_ids
from .notifiers.design import DesignAssignmentNotifier, DesignCompletionNotifier, DesignNotInProgress48hNotifier, DesignSLA24hNotifier
from .notifiers.domains import DomainNotifier
from .notifiers.ips import IPNotifier
from .notifiers.orders import OrderNotifier
from .notifiers.tickets import TicketNotifier


def _create_bot() -> Bot:
    token = settings.orders_bot_token or settings.telegram_bot_token
    return make_bot(token)


def _orders_and_main_bots_differ() -> bool:
    """True если orders bot и основной бот — разные токены (нужен отдельный admin_bot для алертов админу)."""
    o = secret(settings.orders_bot_token).strip()
    m = secret(settings.telegram_bot_token).strip()
    return bool(o and m and o != m)


def _create_main_bot() -> Bot:
    """Только TELEGRAM_BOT_TOKEN — для админских DM, когда рассылка идёт через ORDERS_BOT_TOKEN."""
    token = secret(settings.telegram_bot_token).strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required when using a separate main bot for admin alerts")
    return make_bot(token)


def _create_design_bot() -> Bot:
    token = settings.design_bot_token or settings.telegram_bot_token
    if not settings.design_bot_token:
        logger.warning(
            "DESIGN_BOT_TOKEN not set: design notify will send via main bot (TELEGRAM_BOT_TOKEN). "
            "Admin copy will arrive in main bot, not DesignBot. Set DESIGN_BOT_TOKEN in cron env for DesignBot.",
        )
    return make_bot(token)


async def _run_notifier(dry_run: bool, bot_instance: Optional[Bot], make_owned_bot, build, run) -> Dict[str, Any]:
    """Client + bot (the app's bot, or an owned one closed afterwards) -> notifier -> stats dict.

    build(client, bot, admin_ids, stack) may be async; `stack` owns any extra bots it opens.
    """
    async with UnderdogClient.from_settings() as client, AsyncExitStack() as stack:
        bot = bot_instance if bot_instance is not None else await stack.enter_async_context(make_owned_bot())
        notifier = build(client, bot, await resolve_underdog_notify_admin_ids(), stack)
        if inspect.isawaitable(notifier):
            notifier = await notifier
        stats = await run(notifier)
        return stats.to_dict(dry_run=dry_run)


async def notify_ready_orders(
    dry_run: bool = True,
    limit_user_ids: Optional[Sequence[int]] = None,
    bot_instance: Optional[Bot] = None,
) -> Dict[str, Any]:
    return await _run_notifier(
        dry_run, bot_instance, _create_bot,
        lambda client, bot, admins, _: OrderNotifier(client, bot, admins),
        lambda n: n.notify_ready_orders(dry_run=dry_run, limit_user_ids=limit_user_ids),
    )


async def notify_design_assignments(dry_run: bool = True, bot_instance: Optional[Bot] = None) -> Dict[str, Any]:
    """Уведомлять дизайнера, когда ему ставят таск (order_status=0). Результат по contractor_id -> telegram."""
    return await _run_notifier(
        dry_run, bot_instance, _create_design_bot,
        lambda client, bot, admins, _: DesignAssignmentNotifier(client, bot, admins, settings.design_broadcast_chat_ids),
        lambda n: n.notify_design_assignments(dry_run=dry_run),
    )


async def notify_design_completions(dry_run: bool = True, bot_instance: Optional[Bot] = None) -> Dict[str, Any]:
    """Уведомлять дизайнера о выполнении таска (order_status=1) + время выполнения."""
    return await _run_notifier(
        dry_run, bot_instance, _create_design_bot,
        lambda client, bot, admins, _: DesignCompletionNotifier(client, bot, admins, settings.design_broadcast_chat_ids),
        lambda n: n.notify_design_completions(dry_run=dry_run),
    )


async def notify_design_sla_24h(dry_run: bool = True, bot_instance: Optional[Bot] = None) -> Dict[str, Any]:
    """Уведомлять: прошло SLA (24 часа) после назначения, а статус ещё не стал 'выполнен'."""
    return await _run_notifier(
        dry_run, bot_instance, _create_design_bot,
        lambda client, bot, admins, _: DesignSLA24hNotifier(client, bot, admins, settings.design_broadcast_chat_ids),
        lambda n: n.notify_design_sla_24h(dry_run=dry_run),
    )


async def notify_design_not_in_progress_48h(dry_run: bool = True, bot_instance: Optional[Bot] = None) -> Dict[str, Any]:
    """Напоминать через 2 дня после назначения, если таск не переведен в статус «в работе»."""
    return await _run_notifier(
        dry_run, bot_instance, _create_design_bot,
        lambda client, bot, admins, _: DesignNotInProgress48hNotifier(
            client, bot, admins, settings.design_broadcast_chat_ids,
            reminder_hours=settings.design_take_in_progress_reminder_hours,
        ),
        lambda n: n.notify_design_not_in_progress_48h(dry_run=dry_run),
    )


async def notify_expiring_domains(*, dry_run: bool = True, days: int = 30, bot_instance: Optional[Bot] = None) -> Dict[str, Any]:
    return await _run_notifier(
        dry_run, bot_instance, _create_bot,
        lambda client, bot, admins, _: DomainNotifier(client, bot, admins),
        lambda n: n.notify_expiring_domains(dry_run=dry_run, days=days),
    )


async def notify_expiring_ips(
    *,
    dry_run: bool = True,
    days: int = 7,
    bot_instance: Optional[Bot] = None,
    admin_bot_instance: Optional[Bot] = None,
) -> Dict[str, Any]:
    async def build(client, bot, admins, stack):
        admin_bot = admin_bot_instance
        # CLI/cron without the app: open the main bot for admin DMs when it differs from the orders bot
        if admin_bot is None and bot_instance is None and _orders_and_main_bots_differ():
            admin_bot = await stack.enter_async_context(_create_main_bot())
        return IPNotifier(client, bot, admins, admin_bot=admin_bot)

    return await _run_notifier(
        dry_run, bot_instance, _create_bot, build, lambda n: n.notify_expiring_ips(dry_run=dry_run, days=days)
    )


async def notify_completed_tickets(*, dry_run: bool = True, bot_instance: Optional[Bot] = None) -> Dict[str, Any]:
    return await _run_notifier(
        dry_run, bot_instance, _create_bot,
        lambda client, bot, admins, _: TicketNotifier(client, bot, admins),
        lambda n: n.notify_completed_tickets(dry_run=dry_run),
    )
