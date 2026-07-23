import os
import unittest
from unittest.mock import AsyncMock, patch


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src import app as app_module  # noqa: E402
from src import db  # noqa: E402


class _Cursor:
    def __init__(self, existing_event):
        self.rowcount = 1
        self.existing_event = existing_event
        self.queries = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, query, params):
        self.queries.append((query, params))

    async def fetchone(self):
        return self.existing_event


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def cursor(self):
        return self._cursor


class _Pool:
    def __init__(self, cursor):
        self._connection = _Connection(cursor)

    def acquire(self):
        return self._connection


class KeitaroDedupeTests(unittest.IsolatedAsyncioTestCase):
    def test_payout_correction_keeps_same_fingerprint(self) -> None:
        first = app_module._keitaro_sale_postback_fingerprint(
            {"subid": "abc", "profit": "250.79", "offer_id": 1867}
        )
        second = app_module._keitaro_sale_postback_fingerprint(
            {"subid": "abc", "profit": "251.04", "offer_id": 1867}
        )
        self.assertEqual(first, second)

    def test_different_click_ids_have_different_fingerprints(self) -> None:
        first = app_module._keitaro_sale_postback_fingerprint({"subid": "abc", "profit": "251"})
        second = app_module._keitaro_sale_postback_fingerprint({"subid": "xyz", "profit": "251"})
        self.assertNotEqual(first, second)

    async def test_old_event_with_same_click_id_is_rejected(self) -> None:
        cursor = _Cursor(existing_event=(1,))
        with patch("src.db.init_pool", AsyncMock(return_value=_Pool(cursor))):
            claimed = await db.claim_keitaro_sale_postback(
                "new-click-only-key",
                click_id="3avl6p17igil",
            )
        self.assertFalse(claimed)
        self.assertEqual(len(cursor.queries), 2)

    async def test_new_click_id_is_claimed(self) -> None:
        cursor = _Cursor(existing_event=None)
        with patch("src.db.init_pool", AsyncMock(return_value=_Pool(cursor))):
            claimed = await db.claim_keitaro_sale_postback(
                "new-click-only-key",
                click_id="new-click",
            )
        self.assertTrue(claimed)


if __name__ == "__main__":
    unittest.main()
