import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src import handlers  # noqa: E402,F401 — registers all handlers on dp
from src.dispatcher import dp  # noqa: E402
from src.handlers import fb, pending  # noqa: E402


def _callbacks(observer):
    return [h.callback for h in observer.handlers]


class FbDocumentHandlerTests(unittest.IsolatedAsyncioTestCase):
    def test_document_handler_registered_before_catch_all(self) -> None:
        callbacks = _callbacks(dp.message)
        self.assertIn(fb.on_document_upload, callbacks)
        self.assertLess(callbacks.index(fb.on_document_upload), callbacks.index(pending.on_text_fallback))

    def test_drilldown_callbacks_registered(self) -> None:
        callbacks = _callbacks(dp.callback_query)
        self.assertIn(fb.on_fb_upload_account_detail, callbacks)
        self.assertIn(fb.on_fb_report_account_detail, callbacks)

    async def test_pending_fb_await_csv_is_processed(self) -> None:
        status_msg = SimpleNamespace(edit_text=AsyncMock())
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=7),
            document=SimpleNamespace(file_size=100, file_name="ads.csv", mime_type="text/csv"),
            answer=AsyncMock(return_value=status_msg),
        )
        process = AsyncMock(return_value=True)
        clear = AsyncMock()
        with (
            patch("src.handlers.fb.db.get_pending_action", AsyncMock(return_value=("fb:await_csv", None))),
            patch("src.handlers.fb.bot.download", AsyncMock()),
            patch("src.handlers.fb.fb_csv.parse_fb_csv", return_value="parsed"),
            patch("src.handlers.fb.process_fb_csv_upload", process),
            patch("src.handlers.fb.db.clear_pending_action", clear),
        ):
            await fb.on_document_upload(message)
        self.assertEqual(process.await_args.kwargs["parsed"], "parsed")
        clear.assert_awaited_once_with(7)

    async def test_document_ignored_without_pending(self) -> None:
        message = SimpleNamespace(from_user=SimpleNamespace(id=7), answer=AsyncMock())
        with patch("src.handlers.fb.db.get_pending_action", AsyncMock(return_value=None)):
            await fb.on_document_upload(message)
        message.answer.assert_not_awaited()


class FbAccountReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_report_has_drilldown_buttons_and_cache(self) -> None:
        from datetime import date

        from src.handlers import reports

        rows = [
            {"account_name": "Acc <1>", "campaign_name": "C1", "spend": "100", "revenue": "150", "ftd": 2,
             "impressions": 1000, "clicks": 10, "registrations": 4, "buyer_id": 1, "curr_flag_id": 1},
            {"account_name": "Acc <1>", "campaign_name": "C2", "spend": "50", "revenue": "0", "ftd": 0,
             "impressions": 0, "clicks": 0, "registrations": 0, "buyer_id": 1, "curr_flag_id": 1},
        ]
        send = AsyncMock()
        cache = AsyncMock()
        with (
            patch("src.handlers.reports.db.fetch_fb_campaign_month_report", AsyncMock(return_value=rows)),
            patch("src.handlers.reports.is_admin", AsyncMock(return_value=True)),
            patch("src.handlers.reports.db.list_fb_flags", AsyncMock(return_value=[{"id": 1, "code": "GREEN", "severity": 0}])),
            patch("src.handlers.reports.db.list_users", AsyncMock(return_value=[])),
            patch("src.handlers.reports.db.set_ui_cache_list", cache),
            patch("src.handlers.reports.bot.send_message", send),
        ):
            await reports._send_fb_account_report(5, date(2026, 9, 15), 7)

        markup = send.await_args_list[0].kwargs["reply_markup"]
        self.assertEqual(markup.inline_keyboard[0][0].callback_data, "fbar:2026-09-01:0")
        user_id, kind, values = cache.await_args.args
        self.assertEqual((user_id, kind, len(values)), (7, "fbar:2026-09-01", 1))
        detail = "\n".join(fb._build_account_detail_messages(__import__("json").loads(values[0])))
        self.assertIn("<b>Acc &lt;1&gt;</b>", detail)
        self.assertIn("C2", detail)


if __name__ == "__main__":
    unittest.main()
