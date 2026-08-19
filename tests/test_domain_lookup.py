import os
import unittest


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.handlers.menu import main_menu  # noqa: E402
from src.keitaro import campaign_row_matches_domain, hosts_match, parse_campaign_name  # noqa: E402
from src.keitaro_sync import _prepare_rows, extract_campaign_list  # noqa: E402
from src.utils.domain import extract_domains  # noqa: E402


class HostMatchingTests(unittest.TestCase):
    def test_exact_and_www(self) -> None:
        self.assertTrue(hosts_match("example.com", "example.com"))
        self.assertTrue(hosts_match("www.example.com", "example.com"))
        self.assertTrue(hosts_match("example.com", "https://www.example.com/path"))

    def test_subdomain_matches_parent(self) -> None:
        self.assertTrue(hosts_match("land.example.com", "example.com"))
        self.assertTrue(hosts_match("example.com", "land.example.com"))

    def test_does_not_match_suffix_inside_label(self) -> None:
        self.assertFalse(hosts_match("notexample.com", "example.com"))
        self.assertFalse(hosts_match("example.com", "ample.com"))


class CampaignRowMatchTests(unittest.TestCase):
    def test_matches_source_or_target(self) -> None:
        row = {
            "name": "ars_offer [source.com -> lander.io]",
            "source_domain": "source.com",
            "target_domain": "lander.io",
        }
        self.assertTrue(campaign_row_matches_domain(row, "source.com"))
        self.assertTrue(campaign_row_matches_domain(row, "lander.io"))
        self.assertTrue(campaign_row_matches_domain(row, "ads.source.com"))

    def test_matches_domains_parsed_from_name(self) -> None:
        row = {"name": "buyer_geo [fb-land.net -> offer.site]", "source_domain": None, "target_domain": None}
        self.assertTrue(campaign_row_matches_domain(row, "fb-land.net"))
        self.assertFalse(campaign_row_matches_domain(row, "other.net"))


class ExtractDomainsTests(unittest.TestCase):
    def test_normalizes_urls_and_skips_junk(self) -> None:
        domains, invalid = extract_domains("https://www.Foo.COM/x, bar.online, ???")
        self.assertEqual(domains[0], "foo.com")
        self.assertIn("bar.online", domains)


class KeitaroSyncParseTests(unittest.TestCase):
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
    def test_helper_menu_puts_domain_check_first(self) -> None:
        keyboard = main_menu(False, "helper")
        first = keyboard.inline_keyboard[0][0]
        self.assertEqual(first.text, "Проверить домен")
        self.assertEqual(first.callback_data, "menu:checkdomain")
        callbacks = [btn.callback_data for row in keyboard.inline_keyboard for btn in row]
        self.assertNotIn("menu:refreshdomains", callbacks)
        self.assertNotIn("menu:helpers", callbacks)


if __name__ == "__main__":
    unittest.main()
