import os
import unittest


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.handlers.menu import main_menu  # noqa: E402
from src.keitaro import parse_campaign_name  # noqa: E402
from src.binom_sync import BINOM_ID_OFFSET, _prepare_rows, extract_campaign_list  # noqa: E402


class BinomSyncParseTests(unittest.TestCase):
    def test_extracts_bare_binom_list(self) -> None:
        payload = [
            {"id": 8, "name": "130_PWAPartners[loteriakino.com -> solantrania.com]", "trafficSourceId": 1},
            "junk",
        ]
        rows = extract_campaign_list(payload)
        self.assertEqual(len(rows), 1)
        prepared = _prepare_rows(rows)
        self.assertEqual(prepared[0]["id"], BINOM_ID_OFFSET + 8)
        self.assertEqual(prepared[0]["alias_key"], "130")
        self.assertEqual(prepared[0]["source_domain"], "loteriakino.com")
        self.assertEqual(prepared[0]["target_domain"], "solantrania.com")

    def test_rejects_unexpected_payload(self) -> None:
        with self.assertRaises(ValueError):
            extract_campaign_list({"error": "forbidden"})

    def test_extracts_wrapped_campaign_list(self) -> None:
        rows = extract_campaign_list({"campaigns": [{"id": 1, "name": "a"}]})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], 1)

    def test_prepare_rows_keeps_only_campaigns_with_domains(self) -> None:
        prepared = _prepare_rows(
            [
                {"id": 1, "name": "ars_offer [src.com -> dst.com]"},
                {"id": 2, "name": "no-domains-here"},
                {"id": "bad"},
            ]
        )
        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared[0]["source_domain"], "src.com")
        self.assertEqual(prepared[0]["target_domain"], "dst.com")
        self.assertEqual(parse_campaign_name(prepared[0]["name"])["alias_key"], "ars")


class HelperMenuTests(unittest.TestCase):
    def test_helper_menu_has_no_domain_check_or_admin_items(self) -> None:
        keyboard = main_menu(False, "helper")
        callbacks = [btn.callback_data for row in keyboard.inline_keyboard for btn in row]
        self.assertNotIn("menu:checkdomain", callbacks)
        self.assertNotIn("menu:refreshdomains", callbacks)
        self.assertNotIn("menu:helpers", callbacks)
        self.assertNotIn("menu:adskey", callbacks)


if __name__ == "__main__":
    unittest.main()
