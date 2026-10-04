"""Durable inbox for Keitaro postbacks (tg_inbound_postbacks)."""

from typing import Optional, List, Dict, Any
from datetime import datetime
import json
from .pool import _dt_as_utc_naive, cursor, execute


async def enqueue_inbound_postback(raw: Dict[str, Any], fingerprint: Optional[str]) -> int:
    async with cursor() as cur:
        await cur.execute(
            "INSERT INTO tg_inbound_postbacks (fingerprint, raw) VALUES (%s, %s)",
            (fingerprint, json.dumps(raw, ensure_ascii=False)),
        )
        return int(cur.lastrowid)


async def claim_inbound_postback(inbound_id: int) -> Optional[Dict[str, Any]]:
    """Take a pending/failed row for processing (attempts += 1). None if already done/duplicate or missing."""
    async with cursor() as cur:
        await cur.execute(
            "UPDATE tg_inbound_postbacks SET attempts = attempts + 1 WHERE id=%s AND status IN ('pending','failed')",
            (inbound_id,),
        )
        if int(cur.rowcount or 0) != 1:
            return None
        await cur.execute("SELECT raw FROM tg_inbound_postbacks WHERE id=%s", (inbound_id,))
        row = await cur.fetchone()
        return json.loads(row[0]) if row else None


async def finish_inbound_postback(inbound_id: int, status: str, error: Optional[str] = None) -> None:
    assert status in ("done", "failed", "duplicate")
    await execute(
        "UPDATE tg_inbound_postbacks SET status=%s, error=%s, processed_at=UTC_TIMESTAMP() WHERE id=%s",
        (status, (error or None) and error[:4000], inbound_id))


async def list_inbound_postbacks_for_retry(max_attempts: int = 3, min_age_seconds: int = 60) -> List[int]:
    async with cursor() as cur:
        await cur.execute(
            "SELECT id FROM tg_inbound_postbacks WHERE status IN ('pending','failed') AND attempts < %s "
            "AND created_at < UTC_TIMESTAMP() - INTERVAL %s SECOND ORDER BY id",
            (max_attempts, min_age_seconds),
        )
        return [int(r[0]) for r in await cur.fetchall()]


async def requeue_inbound_postbacks(
    *,
    ids: Optional[List[int]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    dry_run: bool = False,
) -> List[int]:
    """Select rows for replay and (unless dry_run) reset them to pending.

    Explicit ids — any status (manual resend). Time range [since, until) in UTC — only pending/failed.
    """
    async with cursor() as cur:
        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            await cur.execute(f"SELECT id FROM tg_inbound_postbacks WHERE id IN ({placeholders}) ORDER BY id", tuple(ids))
        else:
            await cur.execute(
                "SELECT id FROM tg_inbound_postbacks WHERE status IN ('pending','failed') "
                "AND created_at >= %s AND created_at < %s ORDER BY id",
                (_dt_as_utc_naive(since), _dt_as_utc_naive(until)),
            )
        found = [int(r[0]) for r in await cur.fetchall()]
        if found and not dry_run:
            placeholders = ",".join(["%s"] * len(found))
            await cur.execute(
                f"UPDATE tg_inbound_postbacks SET status='pending', attempts=0, error=NULL WHERE id IN ({placeholders})",
                tuple(found),
            )
        return found
