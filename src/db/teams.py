"""Teams and team leads (primary lead role + tg_team_leads_extra)."""

from typing import Optional, List, Dict, Any
from .pool import cursor, execute, fetch_all


async def list_teams() -> List[Dict[str, Any]]:
    return await fetch_all("SELECT id, name, created_at FROM tg_teams ORDER BY id DESC", dict_rows=True)


async def set_team_lead_override(team_id: int, user_id: int) -> None:
    """Assign user as lead for team without changing primary role (mentor lead scenario)."""
    await execute(
        """
        INSERT INTO tg_team_leads_extra(team_id, user_id)
        VALUES(%s, %s) AS new
        ON DUPLICATE KEY UPDATE user_id=new.user_id, created_at=CURRENT_TIMESTAMP
        """,
        (team_id, user_id))


async def clear_team_lead_override(team_id: int) -> None:
    await execute("DELETE FROM tg_team_leads_extra WHERE team_id=%s", (team_id,))


async def list_team_leads(team_id: int) -> List[int]:
    """Return Telegram IDs of active leads for the given team (role=lead or mentor overrides)."""
    leads: List[int] = []
    async with cursor() as cur:
        await cur.execute(
            """
            SELECT telegram_id
            FROM tg_users
            WHERE role='lead' AND team_id=%s AND is_active=1
            """,
            (team_id,)
        )
        rows = await cur.fetchall()
        leads.extend(int(r[0]) for r in rows if r and r[0] is not None)
        await cur.execute(
            """
            SELECT u.telegram_id
            FROM tg_team_leads_extra e
            JOIN tg_users u ON u.telegram_id = e.user_id
            WHERE e.team_id=%s AND u.is_active=1
            """,
            (team_id,)
        )
        extra_rows = await cur.fetchall()
        leads.extend(int(r[0]) for r in extra_rows if r and r[0] is not None)
    seen: set[int] = set()
    unique: List[int] = []
    for lid in leads:
        if lid not in seen:
            seen.add(lid)
            unique.append(lid)
    return unique


async def list_user_lead_teams(user_id: int) -> List[int]:
    """Return team IDs the user leads (primary role lead/head or extra assignment)."""
    teams: List[int] = []
    async with cursor() as cur:
        await cur.execute("SELECT role, team_id, is_active FROM tg_users WHERE telegram_id=%s", (user_id,))
        row = await cur.fetchone()
        if row:
            role, team_id, is_active = row[0], row[1], row[2]
            if is_active and role in ("lead", "head") and team_id is not None:
                teams.append(int(team_id))
        await cur.execute(
            """
            SELECT e.team_id
            FROM tg_team_leads_extra e
            JOIN tg_users u ON u.telegram_id = e.user_id
            WHERE e.user_id=%s AND u.is_active=1
            """,
            (user_id,)
        )
        rows = await cur.fetchall()
        teams.extend(int(r[0]) for r in rows if r and r[0] is not None)
    seen: set[int] = set()
    unique: List[int] = []
    for tid in teams:
        if tid not in seen:
            seen.add(tid)
            unique.append(tid)
    return unique


async def get_primary_lead_team(user_id: int) -> Optional[int]:
    teams = await list_user_lead_teams(user_id)
    return teams[0] if teams else None


async def list_extra_lead_teams(user_id: int) -> List[int]:
    """Teams the user receives deposits for via tg_team_leads_extra (Admin observers)."""
    rows = await fetch_all("SELECT team_id FROM tg_team_leads_extra WHERE user_id=%s", (user_id,))
    return [int(r[0]) for r in rows if r and r[0] is not None]
