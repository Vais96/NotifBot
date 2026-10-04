import os
import unittest
from unittest.mock import AsyncMock, patch

import httpx

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src import app as app_module  # noqa: E402
from src import underdog  # noqa: E402


class _FakeInbox:
    """In-memory tg_inbound_postbacks with the same state machine as the db.* functions."""

    def __init__(self):
        self.rows: dict[int, dict] = {}

    async def enqueue(self, raw, fingerprint):
        rid = len(self.rows) + 1
        self.rows[rid] = {"raw": dict(raw), "status": "pending", "attempts": 0, "error": None}
        return rid

    async def claim(self, rid):
        row = self.rows.get(rid)
        if not row or row["status"] not in ("pending", "failed"):
            return None
        row["attempts"] += 1
        return dict(row["raw"])

    async def finish(self, rid, status, error=None):
        self.rows[rid].update(status=status, error=error)

    async def list_retry(self, max_attempts=3, min_age_seconds=60):
        return [rid for rid, r in self.rows.items() if r["status"] in ("pending", "failed") and r["attempts"] < max_attempts]

    def patches(self):
        return (
            patch("src.app.db.enqueue_inbound_postback", self.enqueue),
            patch("src.app.db.claim_inbound_postback", self.claim),
            patch("src.app.db.finish_inbound_postback", self.finish),
            patch("src.app.db.list_inbound_postbacks_for_retry", self.list_retry),
        )


class InboundPostbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_postback_survives_kill_between_ack_and_processing(self) -> None:
        inbox = _FakeInbox()
        data = {"status": "sale", "subid": "abc", "payout": "100"}
        p1, p2, p3, p4 = inbox.patches()
        dropped = []
        with p1, p2, p3, p4:
            # "kill -9": the row is stored and acknowledged, but the processing task never runs
            with patch("src.app._spawn", lambda coro, name: dropped.append(coro.close())):
                await app_module._accept_postback(dict(data))
            self.assertEqual(inbox.rows[1]["status"], "pending")

            # next start: recovery picks the pending row and processes it
            process = AsyncMock(return_value={"ok": True, "sale": True})
            with patch("src.app._process_keitaro_postback", process):
                await app_module._recover_inbound_postbacks(delay=0)
        process.assert_awaited_once_with(data, inbound_id=1)
        self.assertEqual(inbox.rows[1]["status"], "done")

    async def test_failure_marks_failed_and_duplicate_marks_duplicate(self) -> None:
        inbox = _FakeInbox()
        p1, p2, p3, p4 = inbox.patches()
        with p1, p2, p3, p4:
            await inbox.enqueue({"status": "sale"}, None)
            await inbox.enqueue({"status": "sale"}, None)
            with patch("src.app._process_keitaro_postback", AsyncMock(side_effect=RuntimeError("tg down"))):
                await app_module._process_inbound_postback(1)
            with patch("src.app._process_keitaro_postback", AsyncMock(return_value={"duplicate": True})):
                await app_module._process_inbound_postback(2)
            self.assertEqual(inbox.rows[1]["status"], "failed")
            self.assertIn("tg down", inbox.rows[1]["error"])
            self.assertEqual(inbox.rows[2]["status"], "duplicate")
            # failed rows are retried until attempts run out
            ok = AsyncMock(return_value={"ok": True})
            with patch("src.app._process_keitaro_postback", ok):
                await app_module._recover_inbound_postbacks(delay=0)
            self.assertEqual(inbox.rows[1]["status"], "done")
            ok.assert_awaited_once()

    async def test_storage_failure_falls_back_to_in_memory_processing(self) -> None:
        spawned = []
        with (
            patch("src.app.db.enqueue_inbound_postback", AsyncMock(side_effect=RuntimeError("db down"))),
            patch("src.app._spawn", lambda coro, name: spawned.append(name) or coro.close()),
        ):
            await app_module._accept_postback({"status": "sale"})
        self.assertEqual(spawned, ["keitaro-postback-fallback"])


class UnderdogRetryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        token_patch = patch.object(underdog.UnderdogClient, "_ensure_token", AsyncMock(return_value="tok"))
        token_patch.start()
        self.addCleanup(token_patch.stop)

    def _client(self, handler) -> underdog.UnderdogClient:
        return underdog.UnderdogClient(
            base_url="https://ud.test", email="e", password="p",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

    async def test_retries_5xx_then_succeeds(self) -> None:
        statuses = iter([503, 429, 200])
        client = self._client(lambda request: httpx.Response(next(statuses), json={}))
        with patch("src.underdog.asyncio.sleep", AsyncMock()):
            resp = await client.request("GET", "/api/x")
        self.assertEqual(resp.status_code, 200)

    async def test_transport_errors_wrapped(self) -> None:
        def boom(request):
            raise httpx.ConnectError("refused", request=request)

        client = self._client(boom)
        with patch("src.underdog.asyncio.sleep", AsyncMock()), self.assertRaises(underdog.UnderdogAPIError):
            await client.request("GET", "/api/x")

    async def test_4xx_not_retried(self) -> None:
        calls = []
        client = self._client(lambda request: calls.append(1) or httpx.Response(404, text="nope"))
        with self.assertRaises(underdog.UnderdogAPIError):
            await client.request("GET", "/api/x")
        self.assertEqual(len(calls), 1)


class UnderdogSentGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_already_delivered_entry_is_only_remarked(self) -> None:
        mark_remote = AsyncMock()
        entries = [{"id": 1}, {"id": 2}]
        with patch("src.underdog.db.underdog_sent_ids", AsyncMock(return_value={"1"})):
            fresh = await underdog._drop_locally_sent("domain", 42, entries, lambda e: e["id"], mark_remote)
        self.assertEqual(fresh, [{"id": 2}])
        mark_remote.assert_awaited_once_with(1)


if __name__ == "__main__":
    unittest.main()
