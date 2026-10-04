import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from aiogram.types import Update  # noqa: E402

from src import handlers  # noqa: E402,F401
from src.dispatcher import bot, dp  # noqa: E402

ADMIN, USER = 900001, 900002


def _callback(user_id: int, data: str) -> Update:
    return Update.model_validate({
        "update_id": 1,
        "callback_query": {
            "id": "q1", "chat_instance": "c", "data": data,
            "from": {"id": user_id, "is_bot": False, "first_name": "x"},
            "message": {"message_id": 1, "date": 0, "chat": {"id": user_id, "type": "private"}, "text": "menu"},
        },
    })


class AdminOnlyTests(unittest.IsolatedAsyncioTestCase):
    async def _feed(self, user_id: int, data: str):
        calls = []

        async def fake_request(self_, bot_, method, timeout=None):
            calls.append(method)
            return True

        with (
            patch.object(type(bot.session), "make_request", fake_request),
            patch("src.handlers.common.db.get_user", AsyncMock(return_value=None)),
            patch("src.handlers.aliases.db.set_pending_action", AsyncMock()) as pending,
            patch("src.handlers.common.ADMIN_IDS", {ADMIN}),
        ):
            await dp.feed_update(bot, _callback(user_id, data))
        return calls, pending

    async def test_non_admin_gets_no_rights(self) -> None:
        calls, pending = await self._feed(USER, "alias:new")
        pending.assert_not_awaited()
        self.assertEqual([type(c).__name__ for c in calls], ["AnswerCallbackQuery"])
        self.assertEqual(calls[0].text, "Нет прав")
        self.assertTrue(calls[0].show_alert)

    async def test_admin_runs_handler(self) -> None:
        calls, pending = await self._feed(ADMIN, "alias:new")
        pending.assert_awaited_once_with(ADMIN, "alias:new", None)
        self.assertIn("SendMessage", [type(c).__name__ for c in calls])


if __name__ == "__main__":
    unittest.main()
