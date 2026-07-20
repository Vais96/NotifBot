import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.underdog import (  # noqa: E402
    DesignNotInProgress48hNotifier,
    _build_design_not_in_progress_48h_message,
)


class _UnderdogStub:
    async def fetch_design_new_tasks(self):
        return [
            {
                "id": 34912,
                "name": "Новые креативы",
                "status_id": 0,
                "contractor_id": "77",
                "contractor": {"telegram": "designer"},
            }
        ]


class DesignReminderTests(unittest.IsolatedAsyncioTestCase):
    def test_message_starts_with_required_call_to_action(self) -> None:
        text = _build_design_not_in_progress_48h_message(
            {"id": 34912, "name": "Новые креативы", "status_id": 0},
            passed_text="2 дн. 1 ч.",
            reminder_hours=48,
        )
        self.assertTrue(text.startswith("⚠️ ОБНОВИТЕ СТАТУС ЗАДАЧИ #34912"))
        self.assertIn("Прошло 48 часов", text)

    async def test_reminder_is_sent_to_assigned_designer_after_48_hours(self) -> None:
        assigned_at = datetime.now(timezone.utc) - timedelta(hours=49)
        send_message = AsyncMock()
        mark_sent = AsyncMock()
        with (
            patch(
                "src.underdog.db.list_design_assignments_pending_take_in_progress_reminder",
                AsyncMock(return_value=[{"order_id": 34912, "created_at": assigned_at}]),
            ),
            patch(
                "src.underdog.db.is_design_not_in_progress_48h_sent",
                AsyncMock(return_value=False),
            ),
            patch(
                "src.underdog.db.list_design_bot_subscribers",
                AsyncMock(return_value=[123456]),
            ),
            patch(
                "src.underdog._resolve_designer_telegram_id_from_order",
                AsyncMock(return_value=(123456, "77", "designer", "Designer")),
            ),
            patch("src.underdog.limited_send_message", send_message),
            patch("src.underdog.db.mark_design_not_in_progress_48h_sent", mark_sent),
        ):
            stats = await DesignNotInProgress48hNotifier(
                underdog=_UnderdogStub(),
                bot=object(),
                reminder_hours=48,
            ).notify_design_not_in_progress_48h(dry_run=False)
        send_message.assert_awaited_once()
        self.assertEqual(send_message.await_args.args[1], 123456)
        self.assertIn("ОБНОВИТЕ СТАТУС ЗАДАЧИ #34912", send_message.await_args.kwargs["text"])
        mark_sent.assert_awaited_once_with(34912)
        self.assertEqual(stats.notified, 1)


if __name__ == "__main__":
    unittest.main()
