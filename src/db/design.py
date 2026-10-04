"""DesignBot subscribers and per-order notification marks; Underdog contractor -> Telegram."""

from typing import Optional, List, Dict, Any, Iterable
from datetime import datetime
from .users import find_user_by_username
from .pool import cursor, execute, fetch_all, fetch_one


async def add_design_bot_subscriber(chat_id: int) -> None:
    """Register a chat as design bot subscriber (on /start)."""
    await execute(
        "INSERT IGNORE INTO tg_design_bot_chats (chat_id) VALUES (%s)",
        (chat_id,))


async def list_design_bot_subscribers() -> List[int]:
    """Return all chat_ids subscribed to design bot."""
    rows = await fetch_all("SELECT chat_id FROM tg_design_bot_chats ORDER BY created_at ASC")
    return [int(r[0]) for r in rows] if rows else []


async def is_design_assignment_sent(order_id: int) -> bool:
    """True if we already sent 'task assigned' notification for this order."""
    async with cursor() as cur:
        await cur.execute("SELECT 1 FROM tg_design_assignment_sent WHERE order_id = %s", (order_id,))
        return (await cur.fetchone()) is not None


async def mark_design_assignment_sent(order_id: int) -> None:
    """Mark that we sent 'task assigned' notification for this order."""
    await execute(
        "INSERT IGNORE INTO tg_design_assignment_sent (order_id) VALUES (%s)",
        (order_id,))


async def is_design_completion_sent(order_id: int) -> bool:
    """True if we already sent 'task completed' notification for this order."""
    async with cursor() as cur:
        await cur.execute("SELECT 1 FROM tg_design_completion_sent WHERE order_id = %s", (order_id,))
        return (await cur.fetchone()) is not None


async def mark_design_completion_sent(order_id: int) -> None:
    """Mark that we sent 'task completed' notification for this order."""
    await execute(
        "INSERT IGNORE INTO tg_design_completion_sent (order_id) VALUES (%s)",
        (order_id,))


async def get_design_assignment_sent_at(order_id: int) -> Optional[datetime]:
    """Return UTC datetime when we first sent 'task assigned' for this order."""
    row = await fetch_one(
        "SELECT created_at FROM tg_design_assignment_sent WHERE order_id = %s",
        (order_id,))
    if not row:
        return None
    return row[0]


async def list_design_assignments_pending_take_in_progress_reminder(
    reminder_hours: int,
) -> List[Dict[str, Any]]:
    """Assignments older than reminder_hours without a take-in-progress reminder yet."""
    async with cursor(dict_rows=True) as cur:
        await cur.execute(
            """
            SELECT a.order_id, a.created_at
            FROM tg_design_assignment_sent a
            LEFT JOIN tg_design_not_in_progress_48h_sent r ON r.order_id = a.order_id
            WHERE r.order_id IS NULL
              AND a.created_at <= (UTC_TIMESTAMP() - INTERVAL %s HOUR)
            ORDER BY a.created_at ASC
            """,
            (int(reminder_hours),),
        )
        return await cur.fetchall() or []


async def find_telegram_id_among_subscribers_by_username(
    username: Optional[str],
    subscriber_ids: Iterable[int],
) -> Optional[int]:
    """Match Underdog @username to a DesignBot subscriber telegram_id."""
    if not username:
        return None
    ids = [int(x) for x in subscriber_ids if x]
    if not ids:
        return None
    handle = username.strip().lstrip("@").lower()
    placeholders = ",".join(["%s"] * len(ids))
    row = await fetch_one(
        f"""
        SELECT telegram_id
        FROM tg_users
        WHERE is_active = 1
          AND LOWER(username) = %s
          AND telegram_id IN ({placeholders})
        LIMIT 1
        """,
        (handle, *ids), dict_rows=True)
    if row and row.get("telegram_id") is not None:
        return int(row["telegram_id"])
    return None


async def is_design_sla_24h_alert_sent(order_id: int) -> bool:
    """True if we already sent 'SLA 24h exceeded' warning notification for this order."""
    async with cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM tg_design_sla_24h_alert_sent WHERE order_id = %s",
            (order_id,),
        )
        return (await cur.fetchone()) is not None


async def mark_design_sla_24h_alert_sent(order_id: int) -> None:
    """Mark that we sent 'SLA 24h exceeded' warning notification for this order."""
    await execute(
        "INSERT IGNORE INTO tg_design_sla_24h_alert_sent (order_id) VALUES (%s)",
        (order_id,))


async def is_design_not_in_progress_48h_sent(order_id: int) -> bool:
    """True if we already sent 'not in progress after 48h' reminder for this order."""
    async with cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM tg_design_not_in_progress_48h_sent WHERE order_id = %s",
            (order_id,),
        )
        return (await cur.fetchone()) is not None


async def mark_design_not_in_progress_48h_sent(order_id: int) -> None:
    """Mark that we sent 'not in progress after 48h' reminder for this order."""
    await execute(
        "INSERT IGNORE INTO tg_design_not_in_progress_48h_sent (order_id) VALUES (%s)",
        (order_id,))


async def get_contractor_telegram_id(contractor_id: str) -> Optional[int]:
    """Resolve Underdog contractor_id to telegram_id (from tg_underdog_contractor_telegram or tg_users by username)."""
    if not contractor_id:
        return None
    row = await fetch_one(
        "SELECT telegram_id, telegram_username FROM tg_underdog_contractor_telegram WHERE contractor_id = %s",
        (str(contractor_id).strip(),), dict_rows=True)
    # find_user_by_username takes its own connection — call it after releasing ours
    if row and row.get("telegram_id"):
        return int(row["telegram_id"])
    username = (row or {}).get("telegram_username")
    if username:
        user = await find_user_by_username(str(username).strip().lstrip("@").lower())
        if user:
            return int(user["telegram_id"])
    return None
