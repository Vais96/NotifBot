import os
import unittest
from unittest.mock import AsyncMock, patch


os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("DATABASE_URL", "mysql://user:pass@localhost/test")
os.environ.setdefault("BASE_URL", "https://example.test")

from src import app as app_module  # noqa: E402


class AliasDailyCounterTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_alias_counts_even_when_assigned_user_is_admin(self) -> None:
        user_id = 8331051687
        app_module._daily_counter_cache.clear()
        data = {
            "status": "sale",
            "conversion_id": "alias-admin-sale-1",
            "campaign_name": "ArseniySimich_PWAPartners",
            "profit": "252",
        }
        log_event = AsyncMock()
        count_sales = AsyncMock(return_value=3)
        notify = AsyncMock()

        with (
            patch("src.app.db.claim_keitaro_sale_postback", AsyncMock(return_value=True)),
            patch(
                "src.app.db.find_alias",
                AsyncMock(return_value={"alias": "arseniysimich", "buyer_id": user_id, "lead_id": None}),
            ),
            patch("src.app.db.log_event", log_event),
            patch("src.app.db.count_today_user_sales", count_sales),
            patch("src.app.db.get_kpi", AsyncMock(return_value={})),
            patch(
                "src.app.db.list_users",
                AsyncMock(
                    return_value=[
                        {
                            "telegram_id": user_id,
                            "username": "arseniy",
                            "role": "admin",
                            "is_active": 1,
                            "team_id": None,
                        }
                    ]
                ),
            ),
            patch("src.app.db.list_helpers_by_buyer", AsyncMock(return_value=[])),
            patch("src.app.notify_buyer", notify),
        ):
            result = await app_module._process_keitaro_postback(data)

        log_event.assert_awaited_once_with(data, user_id)
        count_sales.assert_awaited_once_with(user_id)
        self.assertTrue(result["routed"])
        self.assertTrue(
            any("ДЕПОЗИТОВ ЗА ДЕНЬ" in call.args[1] for call in notify.await_args_list)
        )


if __name__ == "__main__":
    unittest.main()
