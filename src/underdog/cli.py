"""CLI: python -m src.underdog --notify-... (see README)."""

from __future__ import annotations
import asyncio
import json
from argparse import ArgumentParser
from typing import Any, Dict
from ..config import settings
from .client import IPS_PATH, UnderdogClient
from .common import _extract_ips, resolve_underdog_notify_admin_ids
from .notifiers.design import DesignAssignmentNotifier, DesignCompletionNotifier, DesignNotInProgress48hNotifier, DesignSLA24hNotifier
from .notifiers.domains import DomainNotifier
from .notifiers.ips import IPNotifier
from .notifiers.orders import OrderNotifier
from .notifiers.tickets import TicketNotifier
from .run import _create_bot, _create_design_bot, _create_main_bot, _orders_and_main_bots_differ


async def _amain() -> None:
    parser = ArgumentParser(description="Interact with the Underdog admin API")
    parser.add_argument("--orders", action="store_true", help="Fetch domain+transferDomain orders (orders bot)")
    parser.add_argument("--orders-design", action="store_true", help="Fetch pwaDesign+creative orders with order_status=1 (design bot)")
    parser.add_argument("--orders-design-new", action="store_true", help="Fetch pwaDesign+creative with order_status=0 (new tasks for assignment notify)")
    parser.add_argument("--notify", action="store_true", help="Send Telegram notifications for ready orders (orders bot)")
    parser.add_argument(
        "--notify-design",
        action="store_true",
        help="Design notify pipeline: assignments + completions + SLA24h + 48h not-in-progress reminder. Cron: python -m src.underdog --notify-design --apply",
    )
    parser.add_argument("--domains", action="store_true", help="Fetch all domains from Underdog")
    parser.add_argument("--notify-domains", action="store_true", help="Notify Telegram users about expiring domains")
    parser.add_argument("--ips", action="store_true", help="Fetch expiring IPs from Underdog")
    parser.add_argument(
        "--ips-raw",
        action="store_true",
        help="With --ips: also include raw Underdog JSON (same as API body before parsing)",
    )
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
                notifier = OrderNotifier(client, bot_instance, await resolve_underdog_notify_admin_ids())
                stats = await notifier.notify_ready_orders(dry_run=dry_run)
            print(json.dumps(stats.to_dict(dry_run=dry_run), ensure_ascii=False, indent=2))
            return

        if args.notify_design:
            dry_run = not args.apply
            async with _create_design_bot() as bot_instance:
                assignment_notifier = DesignAssignmentNotifier(
                    client, bot_instance, await resolve_underdog_notify_admin_ids(), settings.design_broadcast_chat_ids
                )
                completion_notifier = DesignCompletionNotifier(
                    client, bot_instance, await resolve_underdog_notify_admin_ids(), settings.design_broadcast_chat_ids
                )
                sla_notifier = DesignSLA24hNotifier(
                    client, bot_instance, await resolve_underdog_notify_admin_ids(), settings.design_broadcast_chat_ids
                )
                not_in_progress_48h_notifier = DesignNotInProgress48hNotifier(
                    client,
                    bot_instance,
                    await resolve_underdog_notify_admin_ids(),
                    settings.design_broadcast_chat_ids,
                    reminder_hours=settings.design_take_in_progress_reminder_hours,
                )
                assignments = await assignment_notifier.notify_design_assignments(dry_run=dry_run)
                completions = await completion_notifier.notify_design_completions(dry_run=dry_run)
                sla_24h = await sla_notifier.notify_design_sla_24h(dry_run=dry_run)
                not_in_progress_48h = await not_in_progress_48h_notifier.notify_design_not_in_progress_48h(
                    dry_run=dry_run
                )
            print(
                json.dumps(
                    {
                        "assignments": assignments.to_dict(dry_run=dry_run),
                        "completions": completions.to_dict(dry_run=dry_run),
                        "sla_24h": sla_24h.to_dict(dry_run=dry_run),
                        "not_in_progress_48h": not_in_progress_48h.to_dict(dry_run=dry_run),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        if args.domains:
            domains = await client.fetch_domains()
            print(json.dumps({"count": len(domains), "domains": domains}, ensure_ascii=False, indent=2))
            return

        if args.notify_domains:
            dry_run = not args.apply
            async with _create_bot() as bot_instance:
                notifier = DomainNotifier(client, bot_instance, await resolve_underdog_notify_admin_ids())
                stats = await notifier.notify_expiring_domains(dry_run=dry_run, days=max(0, args.days))
            print(json.dumps(stats.to_dict(dry_run=dry_run), ensure_ascii=False, indent=2))
            return

        if args.ips:
            resp = await client.request("GET", IPS_PATH)
            raw = resp.json()
            ips = _extract_ips(raw)
            payload: Dict[str, Any] = {"count": len(ips), "ips": ips}
            if args.ips_raw:
                payload["raw"] = raw
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
            return

        if args.notify_ips:
            dry_run = not args.apply
            horizon = max(0, args.ip_days)
            async with _create_bot() as bot_instance:
                if _orders_and_main_bots_differ():
                    async with _create_main_bot() as admin_bot:
                        notifier = IPNotifier(
                            client,
                            bot_instance,
                            await resolve_underdog_notify_admin_ids(),
                            admin_bot=admin_bot,
                        )
                        stats = await notifier.notify_expiring_ips(dry_run=dry_run, days=horizon)
                else:
                    notifier = IPNotifier(
                        client,
                        bot_instance,
                        await resolve_underdog_notify_admin_ids(),
                        admin_bot=None,
                    )
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
                notifier = TicketNotifier(client, bot_instance, await resolve_underdog_notify_admin_ids())
                stats = await notifier.notify_completed_tickets(dry_run=dry_run)
            print(json.dumps(stats.to_dict(dry_run=dry_run), ensure_ascii=False, indent=2))
            return

        token = await client.get_token(force_refresh=args.raw_token)
        print(token)


def main() -> None:
    asyncio.run(_amain())
