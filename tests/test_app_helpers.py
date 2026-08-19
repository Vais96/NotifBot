import os
import unittest

from fastapi import HTTPException


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src import app as app_module  # noqa: E402
from src.services.keitaro_postbacks import (  # noqa: E402
    build_notification_text,
    has_meaningful_fields,
    is_sale,
    sale_postback_fingerprint,
)


class NotificationFormattingTests(unittest.TestCase):
    def test_untrusted_postback_values_are_html_escaped(self) -> None:
        text = build_notification_text(
            {
                "campaign_name": "buyer_<alpha>&beta",
                "offer_id": "<42>",
                "offer_name": "A & B",
                "subid": "x<y",
                "sub_id_2": "a&b",
                "sub_id_3": "z>q",
                "profit": "10",
                "currency": "U&S",
            }
        )

        self.assertIn("<code>buyer</code>", text)
        self.assertIn("buyer_&lt;alpha&gt;&amp;beta", text)
        self.assertIn("&lt;42&gt; | A &amp; B", text)
        self.assertIn("x&lt;y", text)
        self.assertIn("a&amp;b", text)
        self.assertIn("z&gt;q", text)
        self.assertIn("U&amp;S", text)
        self.assertNotIn("<42>", text)

    def test_placeholder_only_payload_is_not_meaningful(self) -> None:
        self.assertFalse(has_meaningful_fields({"status": "{conversion.status}"}))

    def test_sale_status_is_normalized(self) -> None:
        self.assertTrue(is_sale({"conversion_status": "SALE"}))
        self.assertFalse(is_sale({"status": "rejected"}))

    def test_fingerprint_ignores_payout_corrections_without_conversion_id(self) -> None:
        first = sale_postback_fingerprint({"subid": "abc", "profit": "250.79", "offer_id": 42})
        second = sale_postback_fingerprint({"subid": "abc", "profit": "251.04", "offer_id": "42"})
        self.assertEqual(first, second)

    def test_different_click_ids_have_different_fingerprints(self) -> None:
        first = sale_postback_fingerprint({"subid": "abc", "profit": "251"})
        second = sale_postback_fingerprint({"subid": "xyz", "profit": "251"})
        self.assertNotEqual(first, second)

    def test_conversion_id_takes_priority_in_fingerprint(self) -> None:
        first = sale_postback_fingerprint({"conversion_id": "conv-1", "subid": "a"})
        second = sale_postback_fingerprint({"conversion_id": "conv-1", "subid": "b"})
        self.assertEqual(first, second)


class PostbackAuthorizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_token = app_module.settings.postback_token
        app_module.settings.postback_token = "secret"

    def tearDown(self) -> None:
        app_module.settings.postback_token = self.original_token

    def test_bearer_scheme_is_case_insensitive(self) -> None:
        app_module._authorize_postback("bearer secret", {})

    def test_inline_token_is_supported(self) -> None:
        app_module._authorize_postback(None, {"token": "secret"})

    def test_missing_token_is_unauthorized(self) -> None:
        with self.assertRaises(HTTPException) as context:
            app_module._authorize_postback(None, {})
        self.assertEqual(context.exception.status_code, 401)

    def test_wrong_token_is_forbidden(self) -> None:
        with self.assertRaises(HTTPException) as context:
            app_module._authorize_postback("Bearer wrong", {})
        self.assertEqual(context.exception.status_code, 403)

    def test_credentials_are_removed_before_persistence(self) -> None:
        data = {"token": "secret", "auth": "legacy-secret", "subid": "click-1"}
        app_module._remove_postback_credentials(data)
        self.assertEqual(data, {"subid": "click-1"})


if __name__ == "__main__":
    unittest.main()
