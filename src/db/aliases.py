"""Campaign prefix aliases (tg_aliases) used for deposit routing."""

from typing import Optional, List, Dict, Any, Iterable
from .pool import execute, fetch_all, fetch_one


async def list_alias_lead_buyers(lead_id: int) -> List[int]:
    """Buyers whose alias names this user as lead (tg_aliases.lead_id)."""
    rows = await fetch_all(
        "SELECT DISTINCT buyer_id FROM tg_aliases WHERE lead_id=%s AND buyer_id IS NOT NULL",
        (lead_id,),
        )
    return [int(r[0]) for r in rows if r and r[0] is not None]


async def find_alias(alias: Optional[str]) -> Optional[Dict[str, Any]]:
    if not alias:
        return None
    return await fetch_one("SELECT alias, buyer_id, lead_id FROM tg_aliases WHERE alias=%s", (alias.lower(),), dict_rows=True)


async def find_campaign_name(alias: Optional[str]) -> Optional[str]:
    """Admin full name for a campaign prefix, even when the employee has no Telegram."""
    if not alias:
        return None
    row = await fetch_one("SELECT full_name FROM tg_campaign_names WHERE alias=%s", (alias.strip().lower(),))
    return (row[0] or None) if row else None


_UNSET = object()


async def set_alias(alias: str, buyer_id: Any = _UNSET, lead_id: Any = _UNSET) -> None:
    a = (alias or "").strip().lower()
    if not a:
        return
    # Atomic upsert: on existing alias only the provided fields change
    updates = [f"{col} = new.{col}" for col, val in (("buyer_id", buyer_id), ("lead_id", lead_id)) if val is not _UNSET]
    await execute(
        "INSERT INTO tg_aliases(alias, buyer_id, lead_id) VALUES(%s, %s, %s) AS new "
        "ON DUPLICATE KEY UPDATE " + (", ".join(updates) or "alias = tg_aliases.alias"),
        (a, None if buyer_id is _UNSET else buyer_id, None if lead_id is _UNSET else lead_id))


async def list_aliases() -> List[Dict[str, Any]]:
    return await fetch_all("SELECT alias, buyer_id, lead_id FROM tg_aliases ORDER BY alias ASC", dict_rows=True)


async def fetch_alias_map(aliases: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    names = [a.strip().lower() for a in aliases if a and a.strip()]
    if not names:
        return {}
    placeholders = ",".join(["%s"] * len(names))
    rows = await fetch_all(
        f"SELECT alias, buyer_id, lead_id FROM tg_aliases WHERE alias IN ({placeholders})",
        tuple(names), dict_rows=True)
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        alias = (row.get("alias") or "").strip().lower()
        if not alias:
            continue
        result[alias] = row
    return result


async def delete_alias(alias: str) -> None:
    await execute("DELETE FROM tg_aliases WHERE alias=%s", (alias.lower(),))
