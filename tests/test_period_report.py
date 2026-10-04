import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from aiogram.types import Update  # noqa: E402

from src import handlers  # noqa: E402,F401
from src.dispatcher import bot, dp  # noqa: E402

USER = {"id": 5, "is_bot": False, "first_name": "x"}
CHAT = {"id": 5, "type": "private"}


async def _feed(update: dict):
    calls = []

    async def fake_request(self_, bot_, method, timeout=None):
        calls.append(method)
        if type(method).__name__ == "SendMessage":
            from aiogram.types import Message
            return Message.model_validate({"message_id": 9, "date": 0, "chat": CHAT, "text": method.text}, context={"bot": bot_})
        return True

    report = AsyncMock()
    with (
        patch.object(type(bot.session), "make_request", fake_request),
        patch("src.handlers.reports.period._send_period_report", report),
        patch("src.handlers.pending.db.get_pending_action", AsyncMock(return_value=None)),
    ):
        await dp.feed_update(bot, Update.model_validate({"update_id": 1, **update}))
    return calls, report


class PeriodReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_week_command(self) -> None:
        calls, report = await _feed({"message": {"message_id": 1, "date": 0, "chat": CHAT, "from": USER, "text": "/week"}})
        report.assert_awaited_once_with(5, 5, "Последние 7 дней", 7, False)
        self.assertEqual([type(c).__name__ for c in calls], ["SendMessage", "DeleteMessage"])

    async def test_yesterday_button(self) -> None:
        calls, report = await _feed({"callback_query": {
            "id": "q", "chat_instance": "c", "data": "report:yesterday", "from": USER,
            "message": {"message_id": 1, "date": 0, "chat": CHAT, "text": "menu"},
        }})
        report.assert_awaited_once_with(5, 5, "Вчера", None, True)
        self.assertEqual(type(calls[0]).__name__, "AnswerCallbackQuery")


if __name__ == "__main__":
    unittest.main()
