import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.underdog import IPNotifier, TicketNotifier  # noqa: E402

SEND = "src.underdog.notifiers.base.limited_send_message"
THROTTLE = "src.underdog.notifiers.base.db.admin_notify_throttle_allow_send"


class AdminAlertsTests(unittest.IsolatedAsyncioTestCase):
    async def test_admin_bot_first_then_orders_bot(self) -> None:
        main_bot, orders_bot = object(), object()
        send = AsyncMock(side_effect=[RuntimeError("blocked"), None])
        notifier = IPNotifier(underdog=None, bot=orders_bot, admin_ids=[1], admin_bot=main_bot)
        with patch(SEND, send):
            await notifier._send_to_admins("hi", what="test")
        self.assertEqual([c.args[0] for c in send.await_args_list], [main_bot, orders_bot])

    async def test_throttled_alert_respects_dry_run_and_throttle(self) -> None:
        notifier = TicketNotifier(underdog=None, bot=object(), admin_ids=[1, 2])
        send, throttle = AsyncMock(), AsyncMock(side_effect=[True, False])
        with patch(SEND, send), patch(THROTTLE, throttle):
            await notifier._notify_admins_mark_error(ticket_id=5, handle="h", error_text="e", dry_run=True)
            self.assertEqual(send.await_count, 0)
            await notifier._notify_admins_mark_error(ticket_id=5, handle="h", error_text="e", dry_run=False)
            await notifier._notify_admins_mark_error(ticket_id=5, handle="h", error_text="e", dry_run=False)
        self.assertEqual(send.await_count, 2)  # two admins, second call throttled
        throttle.assert_awaited_with("tickets:mark_err:5")

    async def test_digest_truncates_and_excludes_recipient_copy(self) -> None:
        notifier = TicketNotifier(underdog=None, bot=object(), admin_ids=[1, 2])
        send = AsyncMock()
        items = [{"ticket_id": i, "type": "t", "handle": "h"} for i in range(25)]
        with patch(SEND, send), patch(THROTTLE, AsyncMock(return_value=True)):
            await notifier._alert_admins(items)
            text = send.await_args.kwargs["text"]
            self.assertIn("… и ещё 5 тикетов", text)
            send.reset_mock()
            await notifier._notify_admins_copy("copy", exclude_user_id=2)
        self.assertEqual([c.args[1] for c in send.await_args_list], [1])


class _Ctx:
    """Async context manager standing in for UnderdogClient / owned Bot."""

    def __init__(self, name):
        self.name, self.closed = name, False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True


class RunNotifierTests(unittest.IsolatedAsyncioTestCase):
    async def test_ip_run_opens_main_bot_for_admins_and_closes_owned_bots(self) -> None:
        from src.underdog import run

        orders_bot, main_bot, seen = _Ctx("orders"), _Ctx("main"), {}

        class _Stats:
            def to_dict(self, dry_run):
                return {"dry_run": dry_run}

        async def fake_notify(self, *, dry_run, days):
            seen["bot"], seen["admin_bot"] = self.bot, self.admin_bot
            return _Stats()

        with (
            patch.object(run.UnderdogClient, "from_settings", lambda: _Ctx("client")),
            patch.object(run, "resolve_underdog_notify_admin_ids", AsyncMock(return_value=[1])),
            patch.object(run, "_create_bot", lambda: orders_bot),
            patch.object(run, "_create_main_bot", lambda: main_bot),
            patch.object(run, "_orders_and_main_bots_differ", lambda: True),
            patch.object(IPNotifier, "notify_expiring_ips", fake_notify),
        ):
            result = await run.notify_expiring_ips(dry_run=True, days=3)
            given = object()
            await run.notify_expiring_ips(dry_run=False, bot_instance=given)
        self.assertEqual(result, {"dry_run": True})
        self.assertTrue(orders_bot.closed and main_bot.closed)
        self.assertIs(seen["bot"], given)  # app passes its bot: no extra bots opened
        self.assertIsNone(seen["admin_bot"])


class OrderDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_marks_locally_before_patch_and_does_not_resend_after_patch_failure(self) -> None:
        from types import SimpleNamespace

        from src.underdog import OrderNotifier, UnderdogAPIError

        order = {"id": 7, "name": "Domain", "total": "10", "owner": {"corporate_telegram": "@Buyer"}}
        underdog = SimpleNamespace(
            fetch_orders_for_orders_bot=AsyncMock(return_value=[order]),
            mark_order_telegram_sent=AsyncMock(side_effect=[UnderdogAPIError("503"), None]),
        )
        sent_store: set = set()

        async def sent_ids(kind, ids, chat_id):
            return {i for i in ids if (kind, i, chat_id) in sent_store}

        async def mark_sent(kind, ids, chat_id):
            sent_store.update((kind, i, chat_id) for i in ids)

        send = AsyncMock(return_value=SimpleNamespace(message_id=1, **{"__tg_http_status__": 200}))
        db_path = "src.underdog.notifiers.orders.db"
        with (
            patch(f"{db_path}.fetch_users_by_usernames", AsyncMock(return_value={"buyer": {"telegram_id": 42}})),
            patch(f"{db_path}.get_user", AsyncMock(return_value={"telegram_id": 42, "is_active": 1})),
            patch(f"{db_path}.underdog_sent_ids", sent_ids),
            patch(f"{db_path}.mark_underdog_sent", mark_sent),
            patch(f"{db_path}.admin_notify_throttle_clear", AsyncMock()),
            patch(THROTTLE, AsyncMock(return_value=False)),
            patch(SEND, send),
        ):
            notifier = OrderNotifier(underdog=underdog, bot=object(), admin_ids=[])
            await notifier.notify_ready_orders(dry_run=False)  # sent, PATCH fails
            await notifier.notify_ready_orders(dry_run=False)  # PATCH retried, no second message
        self.assertEqual(send.await_count, 1)
        self.assertEqual(underdog.mark_order_telegram_sent.await_count, 2)
        self.assertIn(("order", "7", 42), sent_store)


if __name__ == "__main__":
    unittest.main()
