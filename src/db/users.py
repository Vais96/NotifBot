"""tg_users and helper -> buyer links."""

from typing import Optional, List, Dict, Any, Iterable
from .pool import cursor, execute, fetch_all, fetch_one, transaction


async def upsert_user(telegram_id: int, username: Optional[str], full_name: Optional[str]) -> None:
    await execute(
        """
        INSERT INTO tg_users(telegram_id, username, full_name)
        VALUES(%s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            username = COALESCE(NULLIF(new.username, ''), tg_users.username),
            full_name = COALESCE(NULLIF(new.full_name, ''), tg_users.full_name)
        """,
        (telegram_id, username, full_name))


async def list_users() -> List[Dict[str, Any]]:
    return await fetch_all("SELECT telegram_id, username, full_name, role, team_id, is_active, created_at FROM tg_users ORDER BY created_at DESC", dict_rows=True)


async def get_user(telegram_id: int) -> Optional[Dict[str, Any]]:
    row = await fetch_one(
        "SELECT telegram_id, username, full_name, role, team_id, is_active, created_at FROM tg_users WHERE telegram_id=%s",
        (telegram_id,), dict_rows=True)
    return row


async def set_user_role(telegram_id: int, role: str) -> None:
    assert role in ("buyer", "lead", "head", "admin", "mentor", "helper")
    await execute("UPDATE tg_users SET role=%s WHERE telegram_id=%s", (role, telegram_id))


async def set_orders_opt_out(telegram_id: int, opt_out: bool) -> None:
    """Orders-bot unsubscribe (/unsubscribe, cleared by /start in orders bot); main bot is unaffected."""
    await execute("UPDATE tg_users SET orders_opt_out=%s WHERE telegram_id=%s", (1 if opt_out else 0, telegram_id))


async def set_user_active(telegram_id: int, is_active: bool) -> None:
    await execute("UPDATE tg_users SET is_active=%s WHERE telegram_id=%s", (1 if is_active else 0, telegram_id))


async def get_helper_buyer(helper_id: int) -> Optional[int]:
    """Возвращает buyer_id, к которому привязан помощник, или None."""
    row = await fetch_one("SELECT buyer_id FROM tg_helper_buyer WHERE helper_id=%s", (helper_id,))
    return int(row[0]) if row and row[0] is not None else None


async def set_helper_buyer(helper_id: int, buyer_id: int) -> None:
    """Привязывает помощника к байеру (один помощник — один байер)."""
    await execute(
        """
        INSERT INTO tg_helper_buyer (helper_id, buyer_id) VALUES (%s, %s) AS new
        ON DUPLICATE KEY UPDATE buyer_id = new.buyer_id
        """,
        (helper_id, buyer_id))


async def remove_helper_and_promote_to_buyer(helper_id: int) -> None:
    """
    Удаляет помощника как helper:
    - снимает привязку к buyer
    - переводит роль в buyer
    """
    async with transaction() as cur:
        await cur.execute("DELETE FROM tg_helper_buyer WHERE helper_id=%s", (helper_id,))
        await cur.execute("UPDATE tg_users SET role='buyer' WHERE telegram_id=%s", (helper_id,))


async def deactivate_user(telegram_id: int) -> None:
    """
    Мягкое удаление пользователя из бота:
    - is_active=0
    - удаляем helper-привязки (как helper и как buyer)
    """
    async with transaction() as cur:
        await cur.execute("UPDATE tg_users SET is_active=0 WHERE telegram_id=%s", (telegram_id,))
        await cur.execute("DELETE FROM tg_helper_buyer WHERE helper_id=%s OR buyer_id=%s", (telegram_id, telegram_id))


async def list_helpers_by_buyer(buyer_id: int) -> List[int]:
    """Список telegram_id помощников, привязанных к данному байеру (для уведомлений о депозитах)."""
    rows = await fetch_all(
        "SELECT helper_id FROM tg_helper_buyer WHERE buyer_id = %s",
        (buyer_id,))
    return [int(r[0]) for r in rows] if rows else []


async def list_helpers_with_buyers() -> List[Dict[str, Any]]:
    """Список помощников (role=helper) с привязкой к байеру (для админки)."""
    async with cursor(dict_rows=True) as cur:
        await cur.execute(
            """
            SELECT hu.telegram_id AS helper_id, hu.username AS helper_username, hu.full_name AS helper_name,
                   h.buyer_id, bu.username AS buyer_username, bu.full_name AS buyer_name, h.created_at
            FROM tg_users hu
            LEFT JOIN tg_helper_buyer h ON h.helper_id = hu.telegram_id
            LEFT JOIN tg_users bu ON bu.telegram_id = h.buyer_id
            WHERE hu.role = 'helper'
            ORDER BY hu.telegram_id DESC
            """
        )
        return await cur.fetchall() or []


async def list_users_as_buyer_candidates() -> List[Dict[str, Any]]:
    """Пользователи, которых можно назначить байером для помощника (buyer, lead, mentor)."""
    async with cursor(dict_rows=True) as cur:
        await cur.execute(
            """
            SELECT telegram_id, username, full_name, role, team_id, is_active
            FROM tg_users
            WHERE is_active = 1 AND role IN ('buyer', 'lead', 'mentor', 'head')
            ORDER BY full_name, username
            """
        )
        return await cur.fetchall() or []


async def fetch_users_by_usernames(usernames: Iterable[str], *, orders_recipients: bool = False) -> Dict[str, Dict[str, Any]]:
    """orders_recipients=True — для рассылок orders-бота: без отписавшихся (/unsubscribe)."""
    opt_out_sql = "AND orders_opt_out=0 " if orders_recipients else ""
    normalized = []
    for raw in usernames:
        if not raw:
            continue
        handle = raw.strip().lstrip("@").lower()
        if handle:
            normalized.append(handle)
    if not normalized:
        return {}
    placeholders = ",".join(["%s"] * len(normalized))
    rows = await fetch_all(
        f"SELECT telegram_id, username, full_name FROM tg_users WHERE is_active=1 {opt_out_sql}AND LOWER(username) IN ({placeholders})",
        tuple(normalized), dict_rows=True)
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        username = (row.get("username") or "").strip().lstrip("@").lower()
        if username:
            result[username] = row
    return result


async def find_user_by_username(username: Optional[str]) -> Optional[Dict[str, Any]]:
    if not username:
        return None
    users = await fetch_users_by_usernames([username])
    key = username.strip().lstrip("@").lower()
    return users.get(key)
