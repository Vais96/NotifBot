"""Synchronize the local Telegram directory from the new Admin API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import httpx
from loguru import logger

from . import db
from .config import settings


USERS_PATH = "/users"
_ROLE_MAP = {
    "buyer": "buyer", "баер": "buyer", "байер": "buyer",
    "lead": "lead", "team lead": "lead", "тимлид": "lead", "лид": "lead",
    "head": "head", "руководитель": "head",
    "admin": "admin", "administrator": "admin",
    "mentor": "mentor", "ментор": "mentor",
    "helper": "helper", "assistant": "helper", "помощник": "helper",
}


class NewAdminSyncError(RuntimeError):
    """The employee directory could not be fetched or interpreted."""


def _first(value: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        candidate = value.get(key)
        if candidate not in (None, ""):
            return candidate
    return None


def _as_int(value: Any) -> int | None:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _handle(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip().lstrip("@").lower()
    return result or None


def _name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        value = _first(value, "name", "title", "displayName")
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _person_ref(value: Any) -> tuple[int | None, str | None]:
    if isinstance(value, Mapping):
        return (
            _as_int(_first(value, "telegramId", "telegram_id", "telegramID")),
            _handle(_first(value, "username", "telegram", "telegramUsername")),
        )
    return _as_int(value), _handle(value) if _as_int(value) is None else None


@dataclass(frozen=True, slots=True)
class DirectoryEmployee:
    telegram_id: int | None
    username: str | None
    full_name: str | None
    role: str | None
    team_name: str | None
    helper_for_telegram_id: int | None
    helper_for_username: str | None


def normalize_employees(payload: Any) -> list[DirectoryEmployee]:
    """Accept the common response and field variants used by the Admin API."""
    if isinstance(payload, Mapping):
        payload = _first(payload, "data", "users", "items", "employees")
        if isinstance(payload, Mapping):
            payload = _first(payload, "users", "items", "employees")
    if not isinstance(payload, list):
        raise NewAdminSyncError("Unexpected /users response shape")

    employees: list[DirectoryEmployee] = []
    for raw in payload:
        if not isinstance(raw, Mapping):
            continue
        raw_role = _first(raw, "role", "position", "jobTitle", "job_title")
        role = _ROLE_MAP.get(str(raw_role).strip().lower()) if raw_role is not None else None
        team = _first(raw, "team", "department", "teamName", "team_name")
        if isinstance(team, list):
            team = team[0] if team else None
        helper_for = _first(
            raw, "buyer", "buyerUser", "buyer_user", "assistantFor", "assistant_for", "helperFor"
        )
        helper_id, helper_username = _person_ref(helper_for)
        employees.append(DirectoryEmployee(
            telegram_id=_as_int(_first(raw, "telegramId", "telegram_id", "telegramID")),
            username=_handle(_first(raw, "username", "telegram", "telegramUsername", "telegram_username")),
            full_name=_name(_first(raw, "fullName", "full_name", "name", "displayName")),
            role=role,
            team_name=_name(team),
            helper_for_telegram_id=helper_id,
            helper_for_username=helper_username,
        ))
    return employees


async def fetch_employees() -> list[DirectoryEmployee]:
    if not settings.new_admin_api_url or not settings.new_admin_api_key:
        raise NewAdminSyncError("NEW_ADMIN_API_URL or NEW_ADMIN_API_KEY is not configured")
    url = settings.new_admin_api_url.rstrip("/") + USERS_PATH
    headers = {"Accept": "application/json", "X-API-Key": settings.new_admin_api_key}
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=15.0)) as client:
        response = await client.get(url, headers=headers)
    try:
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPStatusError, ValueError) as exc:
        raise NewAdminSyncError(f"GET /users failed with status {response.status_code}") from exc
    return normalize_employees(payload)


async def sync_employees(employees: Iterable[DirectoryEmployee]) -> dict[str, int]:
    """Apply resolved directory records to local users, teams and helper links."""
    result = await db.sync_employee_directory(list(employees))
    logger.info("New Admin employee directory synchronized", **result)
    return result


async def run_sync() -> dict[str, int]:
    return await sync_employees(await fetch_employees())
