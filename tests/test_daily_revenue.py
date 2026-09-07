import os
import unittest
from datetime import date, datetime, time, timezone
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src import app as app_module  # noqa: E402
from src import daily_revenue_report  # noqa: E402
from src.services.daily_revenue import (  # noqa: E402
    ReportScope,
    SaleStat,
    build_report_messages,
    resolve_scope,
)
from src.services.keitaro_postbacks import build_notification_text  # noqa: E402


USERS = [
    {"telegram_id": 1, "username": "head", "full_name": "Head One", "role": "head", "team_id": None, "is_active": 1},
    {"telegram_id": 2, "username": "lead_a", "full_name": "Lead A", "role": "lead", "team_id": 10, "is_active": 1},
    {"telegram_id": 3, "username": "buyer_a1", "full_name": "Buyer A1", "role": "buyer", "team_id": 10, "is_active": 1},
    {"telegram_id": 4, "username": "buyer_a2", "full_name": "Buyer A2", "role": "buyer", "team_id": 10, "is_active": 1},
    {"telegram_id": 5, "username": "mentor_a", "full_name": "Mentor A", "role": "mentor", "team_id": 10, "is_active": 1},
    {"telegram_id": 6, "username": "buyer_b1", "full_name": "Buyer B1", "role": "buyer", "team_id": 20, "is_active": 1},
    {"telegram_id": 7, "username": "lonely", "full_name": "No Team", "role": "buyer", "team_id": None, "is_active": 1},
    {"telegram_id": 8, "username": "observer", "full_name": "Observer", "role": "lead", "team_id": None, "is_active": 1},
    {"telegram_id": 9, "username": "old_lead", "full_name": "Fired Lead", "role": "lead", "team_id": 20, "is_active": 0},
]
TEAMS = {10: "Команда A", 20: "Команда B"}
SALES = [
    SaleStat(user_id=3, count=2, revenue=300.0),
    SaleStat(user_id=4, count=1, revenue=100.4),
    SaleStat(user_id=5, count=1, revenue=50.0),
    SaleStat(user_id=6, count=3, revenue=1250.0),
    SaleStat(user_id=7, count=1, revenue=20.0),
    SaleStat(user_id=None, count=1, revenue=99.0),
]


def _user(telegram_id: int) -> dict:
    return next(u for u in USERS if u["telegram_id"] == telegram_id)


class NotificationRevenueLineTests(unittest.TestCase):
    def test_revenue_line_is_above_daily_counter(self) -> None:
        text = build_notification_text(
            {"campaign_name": "Nikita_PWA", "profit": "269"},
            daily_count=2,
            daily_revenue=538.4,
        )
        self.assertIn("💰 <b>ПРОФИТ:</b> <code>269 </code>", text)
        self.assertIn("💵 <b>ДОХОД ЗА ДЕНЬ:</b> <code>538 </code>", text)
        self.assertLess(text.index("ДОХОД ЗА ДЕНЬ"), text.index("ДЕПОЗИТОВ ЗА ДЕНЬ"))

    def test_revenue_line_omitted_without_value(self) -> None:
        text = build_notification_text({"campaign_name": "Nikita_PWA", "profit": "269"}, daily_count=2)
        self.assertNotIn("ДОХОД", text)


class ResolveDailyRevenueTests(unittest.IsolatedAsyncioTestCase):
    async def test_never_goes_backwards_and_seeds_from_current_payout(self) -> None:
        app_module._daily_revenue_cache.clear()
        self.assertEqual(await app_module._resolve_daily_revenue(42, 0, 269.0), 269.0)
        self.assertEqual(await app_module._resolve_daily_revenue(42, 538.0, 269.0), 538.0)
        # stale DB value must not lower the displayed total
        self.assertEqual(await app_module._resolve_daily_revenue(42, 400.0, 100.0), 538.0)


class ScopeTests(unittest.TestCase):
    def test_head_sees_everything(self) -> None:
        scope = resolve_scope(_user(1), USERS)
        self.assertTrue(scope.all)
        self.assertTrue(scope.includes(None))

    def test_lead_sees_own_team_without_mentor_deposits(self) -> None:
        scope = resolve_scope(_user(2), USERS)
        self.assertEqual(scope.buyer_ids, {2, 3, 4})

    def test_lead_gets_extra_teams_and_alias_buyers(self) -> None:
        scope = resolve_scope(_user(2), USERS, extra_lead_teams=[20], alias_buyers=[7])
        # inactive team members (9) still count towards the team's deposits
        self.assertEqual(scope.buyer_ids, {2, 3, 4, 6, 7, 9})

    def test_observer_lead_without_team(self) -> None:
        scope = resolve_scope(_user(8), USERS, extra_lead_teams=[20])
        self.assertEqual(scope.buyer_ids, {6, 9})

    def test_mentor_sees_subscribed_teams_including_mentors(self) -> None:
        scope = resolve_scope(_user(5), USERS, mentor_teams=[10])
        self.assertEqual(scope.buyer_ids, {2, 3, 4, 5})

    def test_buyers_and_inactive_users_get_nothing(self) -> None:
        self.assertIsNone(resolve_scope(_user(3), USERS))
        self.assertIsNone(resolve_scope(_user(9), USERS))


class ReportTextTests(unittest.TestCase):
    def test_head_report_groups_by_team_and_person(self) -> None:
        messages = build_report_messages(date(2026, 9, 7), SALES, ReportScope(all=True), USERS, TEAMS)
        self.assertEqual(len(messages), 1)
        text = messages[0]
        self.assertIn("ИТОГИ ДНЯ · 07.09.2026", text)
        self.assertIn("💵 Доход: <b>1 819</b>", text)
        self.assertIn("📈 Депозитов: <b>9</b>", text)
        self.assertIn("🏷 <b>Команда B</b> — 1 250 · 3 деп.", text)
        self.assertIn("🏷 <b>Команда A</b> — 450 · 4 деп.", text)
        self.assertIn("  • Buyer A1 (@buyer_a1) — 300 · 2", text)
        self.assertIn("🏷 <b>Без команды</b> — 20 · 1 деп.", text)
        self.assertIn("Не распределено (без байера)</b> — 99 · 1 деп.", text)
        # richest team first, "no team" and unrouted at the end
        self.assertLess(text.index("Команда B"), text.index("Команда A"))
        self.assertLess(text.index("Команда A"), text.index("Без команды"))
        self.assertLess(text.index("Без команды"), text.index("Не распределено"))

    def test_lead_report_is_limited_to_scope(self) -> None:
        scope = resolve_scope(_user(2), USERS)
        text = build_report_messages(date(2026, 9, 7), SALES, scope, USERS, TEAMS)[0]
        self.assertIn("💵 Доход: <b>400</b>", text)
        self.assertIn("📈 Депозитов: <b>3</b>", text)
        self.assertNotIn("Команда B", text)
        self.assertNotIn("Mentor A", text)
        self.assertNotIn("Не распределено", text)

    def test_empty_scope_message(self) -> None:
        text = build_report_messages(date(2026, 9, 7), SALES, ReportScope(buyer_ids={999}), USERS, TEAMS)[0]
        self.assertIn("депозитов по вашим командам не было", text)

    def test_names_are_html_escaped(self) -> None:
        users = [
            {"telegram_id": 1, "username": "h", "full_name": "H", "role": "head", "team_id": None, "is_active": 1},
            {"telegram_id": 3, "username": None, "full_name": "<b>Evil</b>", "role": "buyer", "team_id": 10, "is_active": 1},
        ]
        text = build_report_messages(
            date(2026, 9, 7), [SaleStat(3, 1, 10.0)], ReportScope(all=True), users, {10: "A & B"}
        )[0]
        self.assertIn("&lt;b&gt;Evil&lt;/b&gt;", text)
        self.assertIn("A &amp; B", text)

    def test_long_report_is_split(self) -> None:
        users = [{"telegram_id": 1, "username": "h", "full_name": "H", "role": "head", "team_id": None, "is_active": 1}]
        sales = []
        for i in range(200):
            users.append({"telegram_id": 100 + i, "username": f"u{i}", "full_name": f"Buyer {i}", "role": "buyer", "team_id": 10, "is_active": 1})
            sales.append(SaleStat(100 + i, 1, 10.0))
        messages = build_report_messages(date(2026, 9, 7), sales, ReportScope(all=True), users, {10: "A"})
        self.assertGreater(len(messages), 1)
        self.assertTrue(all(len(m) <= 3500 for m in messages))


class ScheduleTests(unittest.TestCase):
    def test_next_run_today_or_tomorrow(self) -> None:
        tz = ZoneInfo("Europe/Warsaw")
        now = datetime(2026, 9, 7, 20, 0, tzinfo=timezone.utc)  # 22:00 Warsaw
        run = daily_revenue_report.next_run_at(now, time(23, 55), tz)
        self.assertEqual(run.astimezone(tz).strftime("%Y-%m-%d %H:%M"), "2026-09-07 23:55")
        now = datetime(2026, 9, 7, 22, 0, tzinfo=timezone.utc)  # 00:00 Warsaw next day
        run = daily_revenue_report.next_run_at(now, time(23, 55), tz)
        self.assertEqual(run.astimezone(tz).strftime("%Y-%m-%d %H:%M"), "2026-09-08 23:55")

    def test_day_window_converts_local_day_to_utc(self) -> None:
        tz = ZoneInfo("Europe/Warsaw")
        now = datetime(2026, 9, 7, 21, 55, tzinfo=timezone.utc)
        local_start, start_utc, end_utc = daily_revenue_report.day_window(now, tz)
        self.assertEqual(local_start.date(), date(2026, 9, 7))
        self.assertEqual(start_utc, datetime(2026, 9, 6, 22, 0))
        self.assertEqual(end_utc, datetime(2026, 9, 7, 22, 0))

    def test_parse_report_time(self) -> None:
        self.assertEqual(daily_revenue_report.parse_report_time("23:55"), time(23, 55))
        self.assertIsNone(daily_revenue_report.parse_report_time(""))
        self.assertIsNone(daily_revenue_report.parse_report_time("nope"))


class BuildMessagesTests(unittest.IsolatedAsyncioTestCase):
    async def test_messages_built_for_report_roles_only(self) -> None:
        rows = [{"user_id": s.user_id, "count": s.count, "revenue": s.revenue} for s in SALES]
        with (
            patch("src.daily_revenue_report.db.sales_by_user_between", AsyncMock(return_value=rows)),
            patch("src.daily_revenue_report.db.list_users", AsyncMock(return_value=USERS)),
            patch(
                "src.daily_revenue_report.db.list_teams",
                AsyncMock(return_value=[{"id": k, "name": v} for k, v in TEAMS.items()]),
            ),
            patch("src.daily_revenue_report.db.list_extra_lead_teams", AsyncMock(return_value=[])),
            patch("src.daily_revenue_report.db.list_alias_lead_buyers", AsyncMock(return_value=[])),
            patch("src.daily_revenue_report.db.list_mentor_teams", AsyncMock(return_value=[10])),
        ):
            messages = await daily_revenue_report.build_daily_revenue_messages(
                datetime(2026, 9, 7, 23, 55, tzinfo=timezone.utc)
            )
        self.assertEqual(set(messages), {1, 2, 5, 8})
        self.assertIn("1 819", messages[1][0])
        self.assertIn("400", messages[2][0])
        self.assertIn("450", messages[5][0])
        self.assertIn("не было", messages[8][0])

    async def test_send_dry_run_and_delivery(self) -> None:
        built = {1: ["a"], 2: ["b1", "b2"]}
        notify = AsyncMock()
        with (
            patch("src.daily_revenue_report.build_daily_revenue_messages", AsyncMock(return_value=built)),
            patch("src.daily_revenue_report.notify_buyer", notify),
        ):
            stats = await daily_revenue_report.send_daily_revenue_report(dry_run=True)
            self.assertEqual(stats["recipients"], 2)
            self.assertEqual(stats["preview"]["2"], ["b1", "b2"])
            notify.assert_not_awaited()
            stats = await daily_revenue_report.send_daily_revenue_report(dry_run=False)
        self.assertEqual(stats["sent"], 2)
        self.assertEqual(notify.await_count, 3)


if __name__ == "__main__":
    unittest.main()
