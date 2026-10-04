import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.constants import PendingAction  # noqa: E402
from src.handlers import pending  # noqa: E402


def _message(text: str):
    return SimpleNamespace(text=text, from_user=SimpleNamespace(id=7), answer=AsyncMock())


class PendingTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, stored: str, text: str):
        message = _message(text)
        set_alias, clear = AsyncMock(), AsyncMock()
        with (
            patch("src.handlers.pending.db.get_pending_action", AsyncMock(return_value=(stored, None))),
            patch("src.handlers.pending.db.set_alias", set_alias),
            patch("src.handlers.pending.db.clear_pending_action", clear),
        ):
            await pending.on_text_fallback(message)
        return message.answer.await_args.args[0], set_alias, clear

    def test_parse_action(self) -> None:
        self.assertEqual(pending.parse_action("alias:setbuyer:a:b"), (PendingAction.ALIAS_SET_BUYER, "a:b"))
        self.assertEqual(pending.parse_action("kpi:set:daily"), (PendingAction.KPI_SET, "daily"))
        self.assertEqual(pending.parse_action("team:new"), (None, ""))

    async def test_cancel_words_everywhere(self) -> None:
        for stored in ("alias:new", "alias:setbuyer:foo", "domain:check"):
            reply, set_alias, clear = await self._run(stored, "Отмена")
            set_alias.assert_not_awaited()
            clear.assert_awaited_once_with(7)
        self.assertEqual(reply, "Готово. Проверка доменов завершена")

    async def test_dash_cancels_new_alias_but_clears_assignment(self) -> None:
        reply, set_alias, _ = await self._run("alias:new", "-")
        self.assertEqual(reply, "Отменено.")
        set_alias.assert_not_awaited()
        reply, set_alias, _ = await self._run("alias:setbuyer:foo", "-")
        set_alias.assert_awaited_once_with("foo", buyer_id=None)
        self.assertEqual(reply, "Buyer назначен")


if __name__ == "__main__":
    unittest.main()
