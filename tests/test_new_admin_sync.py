import unittest

from src.new_admin_sync import (
    DirectoryEmployee,
    NewAdminSyncError,
    apply_directory_detail,
    enrich_keitaro_names,
    keitaro_alias_key,
    normalize_employees,
)


def _employee(**overrides: object) -> DirectoryEmployee:
    defaults: dict[str, object] = {
        "external_id": "cms-id",
        "telegram_id": 8948797431,
        "username": "nikolai_underdog",
        "full_name": "Николай Петрученко",
        "role": "buyer",
        "team_name": "Команда Дмитрия Шишманов",
        "helper_for_telegram_id": None,
        "helper_for_username": None,
        "helper_for_external_id": None,
        "is_active": True,
    }
    defaults.update(overrides)
    return DirectoryEmployee(**defaults)  # type: ignore[arg-type]


class NewAdminEmployeeNormalizationTests(unittest.TestCase):
    def test_normalizes_nested_response_and_helper_assignment(self) -> None:
        employees = normalize_employees({"data": {"users": [{
            "telegramId": "123", "telegram": "@Buyer", "fullName": "Buyer Name",
            "role": "buyer", "team": {"name": "Alpha"},
        }, {
            "telegram_id": 456, "username": "helper", "position": "assistant",
            "department": "Alpha", "buyer": {"telegramId": 123, "username": "Buyer"},
        }]}})
        self.assertEqual(len(employees), 2)
        self.assertEqual(employees[0].telegram_id, 123)
        self.assertEqual(employees[0].username, "buyer")
        self.assertEqual(employees[0].team_name, "Alpha")
        self.assertIsNone(employees[0].keitaro_name)
        self.assertEqual(employees[1].role, "helper")
        self.assertEqual(employees[1].helper_for_telegram_id, 123)

    def test_manager_is_a_lead_and_disabled_employee_is_inactive(self) -> None:
        employee = normalize_employees({"data": [{
            "id": "admin-id", "telegram": "lead", "position": "Buyer",
            "status": "DISABLED", "team": "Alpha",
            "teamMemberships": [{"isManager": True}],
        }]})[0]
        self.assertEqual(employee.role, "lead")
        self.assertFalse(employee.is_active)

    def test_observer_memberships_are_collected_separately_from_primary_team(self) -> None:
        employee = normalize_employees({"data": [{
            "telegram": "vladyslav_underdog",
            "fullName": "Владислав Сергиенко",
            "position": "Buyer",
            "status": "ACTIVE",
            "team": "Команда Владислава Сергиенко",
            "teamMemberships": [
                {"teamName": "Команда Владислава Сергиенко", "isManager": True, "isObserver": False},
                {"teamName": "Команда Олега Синявина", "isManager": False, "isObserver": True},
                {"teamName": "Команда Дмитрия Шишманов", "isManager": False, "isObserver": True},
            ],
        }]})[0]
        self.assertEqual(employee.role, "lead")
        self.assertEqual(employee.team_name, "Команда Владислава Сергиенко")
        self.assertEqual(
            employee.observer_team_names,
            ("Команда Олега Синявина", "Команда Дмитрия Шишманов"),
        )

    def test_bizdev_position_maps_to_head(self) -> None:
        employee = normalize_employees({"data": [{
            "telegram": "maria_underdog",
            "position": "Bizdev",
            "status": "ACTIVE",
        }]})[0]
        self.assertEqual(employee.role, "head")

    def test_rejects_unknown_response_shape(self) -> None:
        with self.assertRaises(NewAdminSyncError):
            normalize_employees({"data": {"unexpected": True}})

    def test_keitaro_name_from_list_payload_becomes_alias_key(self) -> None:
        employee = normalize_employees({"data": [{
            "id": "cmsbsogbs08ign0l3hamr2gef",
            "telegram": "@Nikolai_underdog",
            "fullName": "Николай Петрученко",
            "position": "Buyer",
            "keitaroName": "NikolaiPetrychenko",
        }]})[0]
        self.assertEqual(employee.keitaro_name, "NikolaiPetrychenko")
        self.assertEqual(keitaro_alias_key(employee.keitaro_name), "nikolaipetrychenko")

    def test_user_card_fills_keitaro_name_omitted_from_the_list(self) -> None:
        employee = apply_directory_detail(_employee(keitaro_name=None), {
            "id": "cms-id",
            "keitaroName": "NikolaiPetrychenko",
            "telegramId": 8948797431,
        })
        self.assertEqual(employee.keitaro_name, "NikolaiPetrychenko")


class NewAdminKeitaroNameEnrichmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_user_cards_when_list_omits_keitaro_name(self) -> None:
        class _Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"data": {"keitaroName": "NikolaiPetrychenko"}}

        class _Client:
            def __init__(self) -> None:
                self.urls: list[str] = []

            async def get(self, url: str, headers: dict | None = None) -> _Response:
                self.urls.append(url)
                return _Response()

        client = _Client()
        employees = await enrich_keitaro_names(
            [_employee(keitaro_name=None)],
            client=client,  # type: ignore[arg-type]
            headers={"X-API-Key": "k"},
            base_url="https://testdashboard.underdog.click/api",
        )
        self.assertEqual(employees[0].keitaro_name, "NikolaiPetrychenko")
        self.assertEqual(
            client.urls,
            ["https://testdashboard.underdog.click/api/users/cms-id"],
        )

    async def test_skips_detail_fetch_when_list_already_has_keitaro_name(self) -> None:
        class _Client:
            async def get(self, url: str, headers: dict | None = None) -> None:
                raise AssertionError(f"unexpected fetch {url}")

        employees = await enrich_keitaro_names(
            [_employee(keitaro_name="NikolaiPetrychenko")],
            client=_Client(),  # type: ignore[arg-type]
            headers={},
            base_url="https://example.test/api",
        )
        self.assertEqual(employees[0].keitaro_name, "NikolaiPetrychenko")


if __name__ == "__main__":
    unittest.main()
