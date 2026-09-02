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
    "bizdev": "head", "biz dev": "head", "business development": "head",
    "биздевом": "head", "биздева": "head", "биздэв": "head", "бизнес дев": "head",
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


def _role(raw_position: Any, raw_roles: Any, *, is_team_manager: bool) -> str | None:
    """Map the Admin directory job title to the bot permission role."""
    position = str(raw_position or "").strip().lower()
    if "assistant" in position or "помощ" in position:
        return "helper"
    if is_team_manager:
        return "lead"
    candidates = [position]
    if isinstance(raw_roles, list):
        candidates.extend(str(item).strip().lower() for item in raw_roles)
    for candidate in candidates:
        if candidate in _ROLE_MAP:
            return _ROLE_MAP[candidate]
    return None


def _observer_team_names(memberships: Any) -> tuple[str, ...]:
    """Team names the employee should see as an Admin-directory observer."""
    if not isinstance(memberships, list):
        return ()
    names: list[str] = []
    seen: set[str] = set()
    for item in memberships:
        if not isinstance(item, Mapping) or not item.get("isObserver"):
            continue
        name = _name(_first(item, "teamName", "team_name", "name", "team"))
        if not name or name == "-":
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return tuple(names)


@dataclass(frozen=True, slots=True)
class DirectoryEmployee:
    external_id: str | None
    telegram_id: int | None
    username: str | None
    full_name: str | None
    role: str | None
    team_name: str | None
    helper_for_telegram_id: int | None
    helper_for_username: str | None
    helper_for_external_id: str | None
    is_active: bool
    observer_team_names: tuple[str, ...] = ()


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
        memberships = raw.get("teamMemberships")
        is_team_manager = isinstance(memberships, list) and any(
            isinstance(item, Mapping) and bool(item.get("isManager")) for item in memberships
        )
        role = _role(
            _first(raw, "position", "role", "jobTitle", "job_title"), raw.get("roles"),
            is_team_manager=is_team_manager,
        )
        team = _first(raw, "team", "department", "teamName", "team_name")
        if isinstance(team, list):
            team = team[0] if team else None
        helper_for = _first(
            raw, "buyer", "buyerUser", "buyer_user", "assistantFor", "assistant_for", "helperFor"
        )
        helper_id, helper_username = _person_ref(helper_for)
        employees.append(DirectoryEmployee(
            external_id=str(raw["id"]).strip() if raw.get("id") else None,
            telegram_id=_as_int(_first(raw, "telegramId", "telegram_id", "telegramID")),
            username=_handle(_first(raw, "username", "telegram", "telegramUsername", "telegram_username")),
            full_name=_name(_first(raw, "fullName", "full_name", "name", "displayName")),
            role=role,
            team_name=_name(team),
            helper_for_telegram_id=helper_id,
            helper_for_username=helper_username,
            helper_for_external_id=str(raw["managerId"]).strip() if raw.get("managerId") else None,
            is_active=str(raw.get("status") or "ACTIVE").upper() == "ACTIVE",
            observer_team_names=_observer_team_names(memberships),
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
