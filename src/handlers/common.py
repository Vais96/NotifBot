"""Shared permission helpers for bot handlers."""

from typing import Any, Mapping, Optional

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
