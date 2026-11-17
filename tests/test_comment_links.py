import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from aiogram.types import Update  # noqa: E402

from src import app as app_module  # noqa: E402
from src import handlers  # noqa: E402,F401
from src.constants import PendingAction  # noqa: E402
from src.dispatcher import bot, dp  # noqa: E402
from src.handlers.menu import main_menu  # noqa: E402
from src.services.comment_links import (  # noqa: E402
    InvalidBaseLinkError,
    InvalidShortDomainError,
    InvalidSlugError,
    ShortLinkTakenError,
    build_comment_link,
    create_short_link_for_url,
    extract_campaign_metadata,
    extract_first_line_url_and_rest,
    extract_request_host,
    format_result_message,
    merge_redirect_query,
    normalize_short_domain,
    normalize_slug,
    parse_base_link,
    parse_fields_input,
    short_link_url,
)


def _text_update(user_id: int, text: str, update_id: int = 1) -> Update:
    return Update.model_validate({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "date": 0,
            "chat": {"id": user_id, "type": "private"},
            "from": {"id": user_id, "is_bot": False, "first_name": "Buyer", "username": "buyer1"},
            "text": text,
        },
    })


def _callback_update(user_id: int, data: str, update_id: int = 2) -> Update:
    return Update.model_validate({
        "update_id": update_id,
        "callback_query": {
            "id": f"q{update_id}",
            "chat_instance": "c",
            "data": data,
            "from": {"id": user_id, "is_bot": False, "first_name": "Buyer", "username": "buyer1"},
            "message": {
                "message_id": 10,
                "date": 0,
                "chat": {"id": user_id, "type": "private"},
                "text": "msg",
            },
        },
    })


class CommentLinksServiceTests(unittest.TestCase):
    def test_parse_base_link_detects_static_and_pixel(self) -> None:
        raw = "https://luckytop.online/?sub1=77&sub2={{campaign.name}}&sub3={{ad.name}}&sub4=TH&fbp=1491853589548065"
        parsed = parse_base_link(raw)
        self.assertEqual(parsed.host, "luckytop.online")
        self.assertTrue(parsed.has_pixel)
        self.assertEqual(parsed.pixel_value, "1491853589548065")
        self.assertEqual(parsed.detected_static, {"sub1": "77", "sub4": "TH", "fbp": "1491853589548065"})

    def test_parse_base_link_accepts_bare_domain_and_rejects_invalid(self) -> None:
        parsed = parse_base_link("luckytop.online")
        self.assertEqual(parsed.normalized_url, "https://luckytop.online/")
        self.assertFalse(parsed.has_pixel)
        with self.assertRaises(InvalidBaseLinkError):
            parse_base_link("not a url")
        with self.assertRaises(InvalidBaseLinkError):
            parse_base_link("localhost")

    def test_parse_fields_input_key_value_and_positional(self) -> None:
        kv_text = (
            "Кампания: 77_TH_1740624179668966_15.11\n"
            "Адсет: adset_1\n"
            "Объявление: creo_1\n"
            "ID каба: 11223344\n"
            "Домен: myshort.online\n"
            "Название: thai-slot-1\n"
            "Пиксель: ."
        )
        fields, structured = parse_fields_input(kv_text)
        self.assertTrue(structured)
        self.assertEqual(
            fields,
            {
                "campaign_name": "77_TH_1740624179668966_15.11",
                "adset_name": "adset_1",
                "ad_name": "creo_1",
                "account_id": "11223344",
                "short_domain": "myshort.online",
                "custom_slug": "thai-slot-1",
            },
        )

        pos_text = "cmp_1\n.\ncreo_2\n998877"
        fields2, structured2 = parse_fields_input(pos_text)
        self.assertTrue(structured2)
        self.assertEqual(fields2, {"campaign_name": "cmp_1", "ad_name": "creo_2", "account_id": "998877"})

        fields3, structured3 = parse_fields_input("single_campaign_name")
        self.assertFalse(structured3)
        self.assertEqual(fields3, {})

    def test_build_comment_link_binom_substitution_and_extra_subs(self) -> None:
        base = "https://luckytop.online/?sub1=77&sub2={{campaign.name}}&sub3={{ad.name}}&sub4=TH&fbp=1491853589548065"
        built = build_comment_link(
            base,
            {
                "campaign_name": "77_TH_1740624179668966_15.11",
                "adset_name": "adset 1",
                "ad_name": "creo_1",
                "account_id": "1234567890",
                "short_domain": "myshort.online",
                "custom_slug": "ignored-in-query",
            },
        )
        self.assertTrue(built.has_pixel)
        self.assertEqual(
            built.params,
            (
                ("sub1", "77"),
                ("sub2", "77_TH_1740624179668966_15.11"),
                ("sub3", "creo_1"),
                ("sub4", "TH"),
                ("sub5", "adset 1"),
                ("sub6", "1234567890"),
                ("fbp", "1491853589548065"),
            ),
        )
        self.assertIn("sub5=adset%201", built.long_url)
        self.assertIn("sub6=1234567890", built.long_url)
        self.assertIn("fbp=1491853589548065", built.long_url)
        self.assertNotIn("custom_slug", built.long_url)
        self.assertNotIn("short_domain", built.long_url)

    def test_build_comment_link_drops_unexpanded_macros_and_avoids_sub_collision(self) -> None:
        base = "https://luckytop.online/?sub1=77&sub2={{campaign.name}}&sub3={{ad.name}}&sub5=split"
        built = build_comment_link(base, {"campaign_name": "cmp_1", "adset_name": "adset_1"})
        self.assertEqual(
            built.params,
            (
                ("sub1", "77"),
                ("sub2", "cmp_1"),
                ("sub5", "split"),
                ("sub6", "adset_1"),
            ),
        )
        self.assertFalse(built.has_pixel)
        self.assertNotIn("{{", built.long_url)

    def test_extract_campaign_metadata(self) -> None:
        self.assertEqual(
            extract_campaign_metadata("77_TH_1740624179668966_15.11"),
            {"sub1": "77", "geo": "TH", "account_id": "1740624179668966"},
        )
        self.assertEqual(
            extract_campaign_metadata("bronsmarmelad_vn_1234567890_16.11"),
            {"sub1": "bronsmarmelad", "geo": "VN", "account_id": "1234567890"},
        )
        self.assertEqual(extract_campaign_metadata("simple_campaign"), {})

        built = build_comment_link("https://luckytop.online", {"campaign_name": "77_TH_1740624179668966_15.11"})
        self.assertEqual(
            built.params,
            (
                ("sub1", "77"),
                ("sub2", "77_TH_1740624179668966_15.11"),
                ("sub4", "TH"),
                ("sub6", "1740624179668966"),
            ),
        )

    def test_extract_first_line_url_and_rest(self) -> None:
        url, rest = extract_first_line_url_and_rest("https://luckytop.online/?sub1=77\nКампания: c1\nКаб: 55")
        self.assertEqual(url, "https://luckytop.online/?sub1=77")
        self.assertEqual(rest, "Кампания: c1\nКаб: 55")

    def test_slug_domain_normalization_and_root_url(self) -> None:
        self.assertEqual(normalize_slug("  Thai Slot 1  "), "thai-slot-1")
        with self.assertRaises(InvalidSlugError):
            normalize_slug("a")
        with self.assertRaises(InvalidSlugError):
            normalize_slug("bad/slug")
        with self.assertRaises(InvalidSlugError):
            normalize_slug("health")

        self.assertEqual(normalize_short_domain("https://Go.MyBrand.com/path"), "go.mybrand.com")
        with self.assertRaises(InvalidShortDomainError):
            normalize_short_domain("bad domain")
        with self.assertRaises(InvalidShortDomainError):
            normalize_short_domain("localhost")

        self.assertEqual(extract_request_host("Go.Brand.com, proxy", "example.test:8080"), "go.brand.com")
        self.assertEqual(extract_request_host(None, "Example.Test:443"), "example.test")

        self.assertEqual(short_link_url("games", "mybrand.online"), "https://mybrand.online/games")
        with patch("src.services.comment_links.settings.short_link_base_url", ""):
            self.assertEqual(short_link_url("games"), "https://example.test/games")

    def test_merge_redirect_query_forwards_fbclid_without_overwriting_subs(self) -> None:
        target = "https://luckytop.online/?sub1=77&sub2=cmp&fbp=12345"
        merged = merge_redirect_query(target, [("fbclid", "IwY2abc"), ("sub1", "999")])
        self.assertIn("sub1=77", merged)
        self.assertIn("fbp=12345", merged)
        self.assertIn("fbclid=IwY2abc", merged)
        self.assertNotIn("999", merged)

    def test_format_result_message_escapes_html_and_warns_without_pixel(self) -> None:
        built = build_comment_link("https://luckytop.online/?sub1=77", {"campaign_name": "a<b&c"})
        text = format_result_message(built, "https://myshort.online/my-link")
        self.assertIn("<code>https://myshort.online/my-link</code>", text)
        self.assertIn("a&lt;b&amp;c", text)
        self.assertIn("Пиксель (fbp) не указан", text)


class ShortLinkCreationAndRedirectTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_short_link_scoped_to_domain(self) -> None:
        with patch("src.services.comment_links.db.create_short_link", AsyncMock(return_value=True)) as create_mock:
            url = await create_short_link_for_url(
                "https://luckytop.online/?sub1=77",
                42,
                custom_slug="Games",
                domain="myshort.online",
            )
        self.assertEqual(url, "https://myshort.online/games")
        create_mock.assert_awaited_once_with(
            "games",
            "https://luckytop.online/?sub1=77",
            42,
            domain="myshort.online",
        )

        with (
            patch("src.services.comment_links.db.create_short_link", AsyncMock(return_value=False)),
            self.assertRaises(ShortLinkTakenError),
        ):
            await create_short_link_for_url(
                "https://luckytop.online/?sub1=77",
                42,
                custom_slug="games",
                domain="myshort.online",
            )

    async def test_create_short_link_auto_slug_retries_on_collision(self) -> None:
        create_mock = AsyncMock(side_effect=[False, True])
        with (
            patch("src.services.comment_links.db.create_short_link", create_mock),
            patch("src.services.comment_links.generate_random_slug", side_effect=["dup111", "ok2222"]),
        ):
            url = await create_short_link_for_url("https://luckytop.online/?sub1=77", 42, domain="myshort.online")
        self.assertEqual(url, "https://myshort.online/ok2222")
        self.assertEqual(create_mock.await_count, 2)


class RedirectEndpointTests(unittest.TestCase):
    def test_root_and_legacy_r_redirects_forward_host_and_fbclid(self) -> None:
        client = TestClient(app_module.app)
        resolve_mock = AsyncMock(return_value=("myshort.online", "https://luckytop.online/?sub1=77&fbp=149185"))
        with (
            patch("src.app.db.resolve_short_link", resolve_mock),
            patch("src.app.db.increment_short_link_clicks", AsyncMock()),
        ):
            resp = client.get(
                "/games?fbclid=IwY2xyz",
                headers={"host": "myshort.online"},
                follow_redirects=False,
            )
        resolve_mock.assert_awaited_once_with("games", host="myshort.online")
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.headers["location"], "https://luckytop.online/?sub1=77&fbp=149185&fbclid=IwY2xyz")

        with (
            patch("src.app.db.resolve_short_link", AsyncMock(return_value=("", "https://luckytop.online/?sub1=77"))),
            patch("src.app.db.increment_short_link_clicks", AsyncMock()),
        ):
            resp_legacy = client.get("/r/legacy1?fbclid=abc", follow_redirects=False)
        self.assertEqual(resp_legacy.status_code, 302)
        self.assertEqual(resp_legacy.headers["location"], "https://luckytop.online/?sub1=77&fbclid=abc")

        with patch("src.app.db.resolve_short_link", AsyncMock(return_value=None)):
            resp_missing = client.get("/unknown", follow_redirects=False)
        self.assertEqual(resp_missing.status_code, 404)

        # Static routes still work and are not shadowed by /{slug}
        self.assertEqual(client.get("/health").json(), {"status": "ok"})


class CommentLinksHandlerTests(unittest.IsolatedAsyncioTestCase):
    def test_menu_exposes_comment_link_for_all_roles(self) -> None:
        for role in ("buyer", "lead", "mentor", "head", "admin", "helper"):
            callbacks = [btn.callback_data for row in main_menu(role == "admin", role).inline_keyboard for btn in row]
            self.assertIn("menu:commentlink", callbacks, role)

    async def test_link_command_one_message_with_custom_domain_and_slug(self) -> None:
        calls = []

        async def fake_request(self_, bot_, method, timeout=None):
            calls.append(method)
            if type(method).__name__ == "SendMessage":
                return SimpleNamespace(message_id=99, chat=SimpleNamespace(id=method.chat_id), text=method.text)
            return True

        set_pending = AsyncMock()
        clear_pending = AsyncMock()
        set_cache = AsyncMock()
        domains_rows = [{"domain": "myshort.online", "owner_id": 501, "is_shared": 0, "is_active": 1}]

        with (
            patch.object(type(bot.session), "make_request", fake_request),
            patch("src.handlers.comment_links.db.upsert_user", AsyncMock()),
            patch("src.handlers.comment_links.db.set_ui_cache_list", set_cache),
            patch("src.handlers.comment_links.db.get_ui_cache_value", AsyncMock(return_value=None)),
            patch("src.handlers.comment_links.db.set_pending_action", set_pending),
            patch("src.handlers.comment_links.db.clear_pending_action", clear_pending),
            patch("src.handlers.comment_links.is_admin", AsyncMock(return_value=False)),
            patch("src.services.comment_links.db.list_short_domains_for_user", AsyncMock(return_value=domains_rows)),
            patch("src.handlers.pending.db.clear_pending_action", clear_pending),
            patch(
                "src.handlers.pending.db.get_pending_action",
                AsyncMock(return_value=(f"{PendingAction.COMMENT_LINK}:base", None)),
            ),
            patch("src.services.comment_links.db.create_short_link", AsyncMock(return_value=True)),
        ):
            await dp.feed_update(bot, _text_update(501, "/link", update_id=1))
            set_pending.assert_awaited_once_with(501, f"{PendingAction.COMMENT_LINK}:base", None)

            payload = (
                "https://luckytop.online/?sub1=77&sub2={{campaign.name}}&sub3={{ad.name}}&sub4=TH&fbp=1491853589548065\n"
                "Кампания: 77_TH_15.11\n"
                "Адсет: adset_a\n"
                "Объявление: creo_b\n"
                "Каб: 555666\n"
                "Домен: myshort.online\n"
                "Название: thai-creo-b"
            )
            await dp.feed_update(bot, _text_update(501, payload, update_id=2))

        clear_pending.assert_awaited_with(501)
        sent = [c for c in calls if type(c).__name__ == "SendMessage"]
        self.assertTrue(sent)
        final_text = sent[-1].text
        self.assertIn("https://myshort.online/thai-creo-b", final_text)
        self.assertIn("sub5=adset_a", final_text)
        self.assertIn("sub6=555666", final_text)
        self.assertIn("fbp=1491853589548065", final_text)

    async def test_guided_skip_shows_built_link_and_asks_custom_slug_then_noshort(self) -> None:
        calls = []

        async def fake_request(self_, bot_, method, timeout=None):
            calls.append(method)
            if type(method).__name__ == "SendMessage":
                return SimpleNamespace(message_id=77, chat=SimpleNamespace(id=method.chat_id), text=method.text)
            return True

        draft = {
            "base_url": "https://luckytop.online/?sub1=77&fbp=1491853589548065",
            "has_pixel": True,
            "fields": {"campaign_name": "cmp", "adset_name": "ads", "ad_name": "ad1"},
        }
        set_pending = AsyncMock()
        clear_pending = AsyncMock()
        with (
            patch.object(type(bot.session), "make_request", fake_request),
            patch(
                "src.handlers.comment_links.db.get_pending_action",
                AsyncMock(return_value=(f"{PendingAction.COMMENT_LINK}:step:account_id", None)),
            ),
            patch("src.handlers.comment_links.db.get_ui_cache_value", AsyncMock(return_value=json.dumps(draft))),
            patch("src.handlers.comment_links.db.set_ui_cache_list", AsyncMock()),
            patch("src.handlers.comment_links.db.set_pending_action", set_pending),
            patch("src.handlers.comment_links.db.clear_pending_action", clear_pending),
            patch("src.handlers.comment_links.is_admin", AsyncMock(return_value=False)),
            patch("src.services.comment_links.db.list_short_domains_for_user", AsyncMock(return_value=[])),
        ):
            await dp.feed_update(bot, _callback_update(502, "clink:skip", update_id=3))
            set_pending.assert_awaited_once_with(502, f"{PendingAction.COMMENT_LINK}:slug", None)
            sent_after_skip = [c for c in calls if type(c).__name__ == "SendMessage"][-1].text
            self.assertIn("Полная ссылка собрана", sent_after_skip)
            self.assertIn("fbp=1491853589548065", sent_after_skip)

            await dp.feed_update(bot, _callback_update(502, "clink:noshort", update_id=4))

        clear_pending.assert_awaited_once_with(502)
        final_text = [c for c in calls if type(c).__name__ == "SendMessage"][-1].text
        self.assertIn("Полная ссылка с сабами", final_text)
        self.assertNotIn("Короткая ссылка:", final_text)

    async def test_buyer_can_add_personal_domain_and_use_root_slug(self) -> None:
        calls = []

        async def fake_request(self_, bot_, method, timeout=None):
            calls.append(method)
            if type(method).__name__ == "SendMessage":
                return SimpleNamespace(message_id=88, chat=SimpleNamespace(id=method.chat_id), text=method.text)
            return True

        add_dom = AsyncMock(return_value="added")
        clear_pending = AsyncMock()
        with (
            patch.object(type(bot.session), "make_request", fake_request),
            patch(
                "src.handlers.pending.db.get_pending_action",
                AsyncMock(return_value=(f"{PendingAction.COMMENT_LINK}:add_domain", None)),
            ),
            patch("src.handlers.comment_links.is_admin", AsyncMock(return_value=False)),
            patch("src.handlers.comment_links.db.add_short_domain", add_dom),
            patch("src.handlers.comment_links.db.set_ui_cache_list", AsyncMock()),
            patch("src.handlers.comment_links.db.clear_pending_action", clear_pending),
            patch("src.services.comment_links.db.list_short_domains_for_user", AsyncMock(return_value=[
                {"domain": "buyerdom.online", "owner_id": 503, "is_shared": 0, "is_active": 1},
            ])),
        ):
            await dp.feed_update(bot, _text_update(503, "https://buyerdom.online/", update_id=5))

        add_dom.assert_awaited_once_with("buyerdom.online", 503, is_shared=False, allow_takeover=False)
        clear_pending.assert_awaited_once_with(503)
        sent = [c for c in calls if type(c).__name__ == "SendMessage"]
        self.assertTrue(any("buyerdom.online" in m.text for m in sent))

    async def test_guided_campaign_autofills_account_id_and_skips_account_step(self) -> None:
        calls = []

        async def fake_request(self_, bot_, method, timeout=None):
            calls.append(method)
            if type(method).__name__ == "SendMessage":
                return SimpleNamespace(message_id=91, chat=SimpleNamespace(id=method.chat_id), text=method.text)
            return True

        draft = {
            "base_url": "https://luckytop.online/?sub1=77&sub2={{campaign.name}}&sub3={{ad.name}}&sub4=TH&fbp=149185",
            "has_pixel": True,
            "fields": {},
        }
        set_pending = AsyncMock()
        set_cache = AsyncMock()

        with (
            patch.object(type(bot.session), "make_request", fake_request),
            patch(
                "src.handlers.pending.db.get_pending_action",
                AsyncMock(return_value=(f"{PendingAction.COMMENT_LINK}:params", None)),
            ),
            patch("src.handlers.comment_links.db.get_ui_cache_value", AsyncMock(return_value=json.dumps(draft))),
            patch("src.handlers.comment_links.db.set_ui_cache_list", set_cache),
            patch("src.handlers.comment_links.db.set_pending_action", set_pending),
        ):
            await dp.feed_update(bot, _text_update(504, "77_TH_1740624179668966_15.11", update_id=6))

        set_pending.assert_awaited_once_with(504, f"{PendingAction.COMMENT_LINK}:step:adset_name", None)
        sent_text = [c for c in calls if type(c).__name__ == "SendMessage"][-1].text
        self.assertIn("1740624179668966", sent_text)
        self.assertIn("Шаг 2/3", sent_text)


if __name__ == "__main__":
    unittest.main()
