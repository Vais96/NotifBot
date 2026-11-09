import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.services import offer_cap  # noqa: E402
from src.services.keitaro_postbacks import build_notification_text  # noqa: E402

STATE = {
    "offerId": 7, "name": "Orobet <CL>", "cap": 30, "period": "month", "spent": 24, "left": 6,
    "campaigns": ["Maksim_UA_1", "maksim_UA_2", "olga_PL_3"],
}


class OfferCapTests(unittest.TestCase):
    def test_deposit_message_shows_cap_left(self) -> None:
        text = build_notification_text({"offer_id": "7", "offer_name": "Orobet"}, cap_line=offer_cap.cap_line(STATE))
        self.assertIn("🚦 <b>ОСТАЛОСЬ КАПЫ:</b> <code>6 из 30</code>", text)
        self.assertIsNone(offer_cap.cap_line(None))

    def test_low_threshold_and_period_suffix(self) -> None:
        self.assertTrue(offer_cap.is_cap_low(STATE))
        self.assertFalse(offer_cap.is_cap_low({**STATE, "left": 7}))
        self.assertFalse(offer_cap.is_cap_low(None))
        self.assertEqual(offer_cap.cap_text({**STATE, "period": "day"}), "6 из 30/день")
        self.assertIn("Капа закончилась", offer_cap.cap_low_text({**STATE, "left": 0}))
        self.assertIn("Orobet &lt;CL&gt;", offer_cap.cap_low_text(STATE))

    def test_one_alert_per_cap_window(self) -> None:
        self.assertEqual(offer_cap.cap_low_key(STATE, "2026-11-09"), "7:2026-11:30")
        self.assertEqual(offer_cap.cap_low_key({**STATE, "period": "day"}, "2026-11-09"), "7:2026-11-09:30")
        # Raised cap → new key → the buyer is warned again.
        self.assertEqual(offer_cap.cap_low_key({**STATE, "period": "fixed", "cap": 50}, "2026-11-09"), "7:fixed:50")


class BuyerCapAlertTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_buyers_who_poured_the_offer_get_it_once(self) -> None:
        from src import app

        aliases = {"maksim": {"buyer_id": 42}, "olga": {"buyer_id": None}}
        find_alias = AsyncMock(side_effect=lambda prefix: aliases.get(prefix))
        send = AsyncMock()
        claim = AsyncMock(side_effect=[True, False])
        with (
            patch.object(app, "notify_buyer", send),
            patch.object(app.db, "find_alias", find_alias),
            patch.object(app.db, "claim_cap_low_alert", claim),
        ):
            await app._notify_cap_low(STATE)
            await app._notify_cap_low(STATE)
            await app._notify_cap_low({**STATE, "left": 20})
            await app._notify_cap_low({**STATE, "campaigns": []})
        send.assert_awaited_once()
        self.assertEqual(send.await_args.args[0], 42)
        self.assertEqual(offer_cap.campaign_prefixes(STATE), {"maksim", "olga"})


class FetchCapStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_skips_without_numeric_offer_or_config(self) -> None:
        self.assertIsNone(await offer_cap.fetch_cap_state("{offer_id}", "c1"))
        with patch.object(offer_cap.settings, "new_admin_api_url", ""):
            self.assertIsNone(await offer_cap.fetch_cap_state("7", "c1"))


if __name__ == "__main__":
    unittest.main()
