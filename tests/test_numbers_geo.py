import os
import unittest
from decimal import Decimal

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.db import _parse_payout  # noqa: E402
from src.fb_csv import _detect_geo  # noqa: E402
from src.services.daily_revenue import _fmt_amount  # noqa: E402
from src.services.keitaro_postbacks import _format_payout, is_sale  # noqa: E402
from src.utils.numbers import parse_decimal, round_money  # noqa: E402


class NumbersGeoTests(unittest.TestCase):
    def test_parse_decimal_locales(self) -> None:
        for raw, expected in {"1,234.56": "1234.56", "1 234,56": "1234.56", "1.234,56": "1234.56", "12,5": "12.5"}.items():
            self.assertEqual(parse_decimal(raw), Decimal(expected), raw)
        self.assertIsNone(parse_decimal("abc"))

    def test_payout_keeps_event_on_garbage(self) -> None:
        self.assertEqual(_parse_payout("1.5 USD"), Decimal("1.5"))
        self.assertEqual(_parse_payout(0), Decimal(0))
        self.assertIsNone(_parse_payout("{conversion.revenue}"))
        self.assertIsNone(_parse_payout("n/a"))

    def test_money_rounds_half_up(self) -> None:
        self.assertEqual(round_money(2.5), 3)
        self.assertEqual(_fmt_amount(1234.5), "1 235")
        self.assertEqual(_format_payout("252.5"), "253")

    def test_geo_only_country_codes(self) -> None:
        self.assertEqual(_detect_geo("PWA_CPA_PL_x"), "PL")
        self.assertEqual(_detect_geo("x_UK_y"), "UK")
        self.assertIsNone(_detect_geo("USD_EUR_RUB_CBO"))

    def test_ftd_is_sale(self) -> None:
        self.assertTrue(is_sale({"status": "ftd"}))


if __name__ == "__main__":
    unittest.main()
