import os
import unittest
from html.parser import HTMLParser

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src import underdog  # noqa: E402
from src.orders_bot import _format_user_line  # noqa: E402
from src.utils.html import safe, user_label  # noqa: E402

EVIL = "<b>Bob & Co"
TELEGRAM_TAGS = {"b", "strong", "i", "em", "u", "s", "code", "pre", "a", "tg-spoiler", "blockquote"}


class _TagCollector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags: list[str] = []

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)


def _assert_renders(test: unittest.TestCase, text: str, expected_tags: list[str]) -> None:
    """External text must not add tags: only the template's own tags remain."""
    parser = _TagCollector()
    parser.feed(text)
    test.assertEqual(parser.tags, expected_tags)
    test.assertTrue(set(parser.tags) <= TELEGRAM_TAGS)
    test.assertNotIn(EVIL, text)


class HtmlSafetyTests(unittest.TestCase):
    def test_safe_and_user_label(self) -> None:
        self.assertEqual(safe(EVIL), "&lt;b&gt;Bob &amp; Co")
        self.assertEqual(safe(None), "")
        self.assertEqual(user_label({"full_name": EVIL}), "&lt;b&gt;Bob &amp; Co")
        self.assertEqual(user_label({"username": "@a<b"}), "@a&lt;b")
        self.assertEqual(user_label(None, 5), "<code>5</code>")

    def test_underdog_messages_escape_api_fields(self) -> None:
        order = {"id": 1, "name": EVIL, "total": "10", "owner": {"name": EVIL}}
        _assert_renders(self, underdog._build_order_message(order), [])
        _assert_renders(self, underdog._build_design_assignment_message(order, assigned_to_display=EVIL), [])
        domain_text = underdog._build_domain_notification([{"raw": {"domain": EVIL}, "expires_at": None}])
        _assert_renders(self, domain_text, [])

    def test_orders_bot_user_line(self) -> None:
        line = _format_user_line({"telegram_id": 1, "username": "x", "full_name": EVIL, "is_active": 1})
        _assert_renders(self, line, [])


if __name__ == "__main__":
    unittest.main()
