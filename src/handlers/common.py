"""Shared permission helpers for bot handlers: is_admin, get_actor, admin_only, callback parsing."""

import functools
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from aiogram.types import CallbackQuery
from loguru import logger

from .. import db
from ..constants import Role
from ..dispatcher import ADMIN_IDS


async def is_admin(user_id: int, row: Optional[Mapping[str, Any]] = None) -> bool:
    """Admin = ADMINS env OR an active tg_users row with role='admin'. Pass `row` to skip the DB lookup."""
    if user_id in ADMIN_IDS:
        return True
    if row is None:
        try:
            row = await db.get_user(user_id)
        except Exception as exc:
            logger.warning("is_admin: failed to load user {}: {}", user_id, exc)
            return False
    return bool(row) and row.get("role") == Role.ADMIN and bool(row.get("is_active", 1))


STALE_BUTTON = "Устаревшая кнопка"


def callback_parts(call: Any, count: int) -> Optional[list[str]]:
    """`a:b:c` -> exactly `count` non-empty parts (the last keeps any ':'); None for a malformed/stale button."""
    parts = (call.data or "").split(":", count - 1)
    return parts if len(parts) == count and all(parts) else None


NO_RIGHTS = "Нет прав"


@dataclass(frozen=True)
class Actor:
    """Who is acting: DB row + derived permissions (role is "admin" for admins)."""

    user_id: int
    row: Optional[Mapping[str, Any]]
    role: Optional[str]
    is_admin: bool
    team_ids: list
    lead_team_ids: list

    @property
    def has_lead_access(self) -> bool:
        return self.is_admin or bool(self.lead_team_ids) or self.role in (Role.LEAD, Role.HEAD)


async def get_actor(user_id: int) -> Actor:
    row = await db.get_user(user_id)
    admin = await is_admin(user_id, row)
    team_id = (row or {}).get("team_id")
    return Actor(
        user_id=user_id,
        row=row,
        role=Role.ADMIN if admin else (row or {}).get("role"),
        is_admin=admin,
        team_ids=[int(team_id)] if team_id is not None else [],
        lead_team_ids=list(await db.list_user_lead_teams(user_id)),
    )


def admin_only(handler):
    """Handler decorator: non-admins get «Нет прав» (alert for buttons) and the handler does not run."""

    @functools.wraps(handler)  # aiogram reads the wrapped signature to pick kwargs
    async def wrapper(event, *args, **kwargs):
        if await is_admin(event.from_user.id):
            return await handler(event, *args, **kwargs)
        if isinstance(event, CallbackQuery):
            return await event.answer(NO_RIGHTS, show_alert=True)
        return await event.answer(NO_RIGHTS)

    return wrapper
