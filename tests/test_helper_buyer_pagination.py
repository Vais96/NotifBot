import os
import unittest


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.handlers.helpers import _buyer_picker  # noqa: E402


class HelperBuyerPaginationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.buyers = [
            {"telegram_id": index, "username": f"buyer{index}", "full_name": None}
            for index in range(1, 32)
        ]

    def test_first_page_has_30_buyers_and_next_button(self) -> None:
        text, keyboard = _buyer_picker(self.buyers, helper_id=100, page=0)

        self.assertIn("страница 1/2", text)
        self.assertEqual(len(keyboard.inline_keyboard), 31)
        self.assertEqual(
            keyboard.inline_keyboard[-1][0].callback_data,
            "helper:setbuyer:100:1",
        )

    def test_last_page_has_remaining_buyer_and_previous_button(self) -> None:
        text, keyboard = _buyer_picker(self.buyers, helper_id=100, page=1)

        self.assertIn("страница 2/2", text)
        self.assertEqual(
            keyboard.inline_keyboard[0][0].callback_data,
            "helper:assign:100:31",
        )
        self.assertEqual(
            keyboard.inline_keyboard[-1][0].callback_data,
            "helper:setbuyer:100:0",
        )


if __name__ == "__main__":
    unittest.main()
