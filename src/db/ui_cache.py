"""Per-user cached lists behind compact callback_data (tg_ui_cache)."""

from typing import Optional, List
from .pool import fetch_one, transaction


async def set_ui_cache_list(user_id: int, kind: str, values: List[str]) -> None:
    async with transaction() as cur:
        await cur.execute("DELETE FROM tg_ui_cache WHERE user_id=%s AND kind=%s", (user_id, kind))
        if values:
            await cur.executemany(
                "INSERT INTO tg_ui_cache(user_id, kind, idx, value) VALUES(%s, %s, %s, %s)",
                [(user_id, kind, i, val) for i, val in enumerate(values)],
            )


async def get_ui_cache_value(user_id: int, kind: str, idx: int) -> Optional[str]:
    row = await fetch_one(
        "SELECT value FROM tg_ui_cache WHERE user_id=%s AND kind=%s AND idx=%s",
        (user_id, kind, idx))
    return str(row[0]) if row and row[0] is not None else None
