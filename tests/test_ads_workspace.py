import os
import unittest


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src.handlers.menu import main_menu  # noqa: E402
from src.services.ads_workspace import (  # noqa: E402
    ads_key_label,
    can_use_ads_key,
    format_ads_key_message,
)


class AdsKeyAccessTests(unittest.TestCase):
    def test_buyer_lead_mentor_head_and_admin_can_use(self) -> None:
        for role in ("buyer", "lead", "mentor", "head", "admin"):
            self.assertTrue(can_use_ads_key(False, role), role)
        self.assertTrue(can_use_ads_key(True, "helper"))
        self.assertFalse(can_use_ads_key(False, "helper"))
        self.assertFalse(can_use_ads_key(False, "buyer", is_active=False))

    def test_menu_shows_ads_key_first_for_those_roles(self) -> None:
        for role in ("buyer", "lead", "mentor", "head"):
            first = main_menu(False, role).inline_keyboard[0][0]
            self.assertEqual(first.callback_data, "menu:adskey", role)
            self.assertEqual(first.text, "Мой ключ для АДС")
        first = main_menu(True, "admin").inline_keyboard[0][0]
        self.assertEqual(first.callback_data, "menu:adskey")

    def test_helper_menu_hides_ads_key(self) -> None:
        callbacks = [btn.callback_data for row in main_menu(False, "helper").inline_keyboard for btn in row]
        self.assertNotIn("menu:adskey", callbacks)

    def test_label_prefers_full_name(self) -> None:
        self.assertEqual(ads_key_label({"full_name": "Иван", "username": "ivan"}, "other"), "Иван")
        self.assertEqual(ads_key_label({"full_name": "", "username": "ivan"}, None), "ivan")
        self.assertEqual(ads_key_label(None, "nick"), "nick")

    def test_invite_message_includes_copyable_code(self) -> None:
        text = format_ads_key_message({"status": "invite", "code": "K7QP4M2T", "hours": 720})
        self.assertIn("<code>K7QP4M2T</code>", text)
        self.assertIn("720 ч", text)

    def test_active_message_shows_last4(self) -> None:
        text = format_ads_key_message({"status": "active", "last4": "ab&c"})
        self.assertIn("<code>····ab&amp;c</code>", text)
        self.assertIn("выдай новый код", text.lower())


if __name__ == "__main__":
    unittest.main()
