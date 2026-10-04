"""Pending multi-step bot actions (tg_pending_actions)."""

from typing import Optional, Tuple
from .pool import execute, fetch_one


async def set_pending_action(admin_id: int, action: str, target_user_id: Optional[int]) -> None:
    await execute(
        """
        INSERT INTO tg_pending_actions(admin_id, action, target_user_id)
        VALUES(%s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE action=new.action, target_user_id=new.target_user_id, created_at=CURRENT_TIMESTAMP
        """,
        (admin_id, action, target_user_id))


async def get_pending_action(admin_id: int) -> Optional[Tuple[str, Optional[int]]]:
    row = await fetch_one("SELECT action, target_user_id FROM tg_pending_actions WHERE admin_id=%s", (admin_id,), dict_rows=True)
    if not row:
        return None
    return row["action"], row["target_user_id"]


async def clear_pending_action(admin_id: int) -> None:
    await execute("DELETE FROM tg_pending_actions WHERE admin_id=%s", (admin_id,))
