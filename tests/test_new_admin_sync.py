import unittest

from src.new_admin_sync import NewAdminSyncError, normalize_employees


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

    def test_rejects_unknown_response_shape(self) -> None:
        with self.assertRaises(NewAdminSyncError):
            normalize_employees({"data": {"unexpected": True}})


if __name__ == "__main__":
    unittest.main()
