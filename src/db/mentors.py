"""Mentor -> team subscriptions (tg_mentor_teams)."""

from typing import List
from .pool import execute, fetch_all


# --- Mentor helpers ---
async def add_mentor_team(mentor_id: int, team_id: int) -> None:
    await execute(
        """
        INSERT INTO tg_mentor_teams(mentor_id, team_id)
        VALUES(%s, %s)
        ON DUPLICATE KEY UPDATE created_at=CURRENT_TIMESTAMP
        """,
        (mentor_id, team_id))


async def remove_mentor_team(mentor_id: int, team_id: int) -> None:
    await execute("DELETE FROM tg_mentor_teams WHERE mentor_id=%s AND team_id=%s", (mentor_id, team_id))


async def list_mentor_teams(mentor_id: int) -> List[int]:
    rows = await fetch_all("SELECT team_id FROM tg_mentor_teams WHERE mentor_id=%s", (mentor_id,))
    return [int(r[0]) for r in rows]


async def list_team_mentors(team_id: int) -> List[int]:
    rows = await fetch_all("SELECT mentor_id FROM tg_mentor_teams WHERE team_id=%s", (team_id,))
    return [int(r[0]) for r in rows]
