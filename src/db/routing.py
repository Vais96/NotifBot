"""Postback routing (tg_routes), sale dedupe and tg_events logging."""

from typing import Optional, List, Dict, Any
from loguru import logger
from ..constants import SALE_STATUSES
from ..utils.numbers import extract_decimal
import json
from decimal import Decimal
from .pool import cursor, execute, fetch_all, fetch_one


async def add_route(user_id: int, offer: Optional[str], country: Optional[str], source: Optional[str], priority: int = 0) -> int:
    async with cursor() as cur:
        await cur.execute(
            """
            INSERT INTO tg_routes(user_id, offer, country, source, priority)
            VALUES(%s, %s, %s, %s, %s)
            """,
            (user_id, offer, country, source, priority)
        )
        return cur.lastrowid


async def list_routes() -> List[Dict[str, Any]]:
    return await fetch_all(
        """
        SELECT r.id, r.user_id, u.username, u.full_name, r.offer, r.country, r.source, r.priority, r.is_active, r.created_at
        FROM tg_routes r
        JOIN tg_users u ON u.telegram_id = r.user_id
        ORDER BY r.priority DESC, r.created_at DESC
        """, dict_rows=True)


async def find_user_for_postback(offer: Optional[str], country: Optional[str], source: Optional[str]) -> Optional[int]:
    row = await fetch_one(
        """
        SELECT user_id,
               ((offer IS NOT NULL) + (country IS NOT NULL) + (source IS NOT NULL)) AS weight
        FROM tg_routes
        WHERE is_active=1
          AND (%s IS NULL OR offer IS NULL OR offer=%s)
          AND (%s IS NULL OR country IS NULL OR country=%s)
          AND (%s IS NULL OR source IS NULL OR source=%s)
        ORDER BY weight DESC, priority DESC, created_at DESC
        LIMIT 1
        """,
        (offer, offer, country, country, source, source))
    return int(row[0]) if row else None


async def claim_keitaro_sale_postback(
    fingerprint: str,
    *,
    click_id: Optional[str] = None,
    inbound_id: Optional[int] = None,
) -> bool:
    """Atomically claim a sale and reject retries of click IDs saved before this key format.

    A claim made by the same tg_inbound_postbacks row (retry after a crash) stays ours.
    """
    async with cursor() as cur:
        await cur.execute(
            "INSERT IGNORE INTO tg_keitaro_sale_dedupe (dedupe_key, inbound_id) VALUES (%s, %s)",
            (fingerprint, inbound_id),
        )
        if int(cur.rowcount or 0) != 1:
            if inbound_id is None:
                return False
            await cur.execute(
                "SELECT inbound_id FROM tg_keitaro_sale_dedupe WHERE dedupe_key=%s", (fingerprint,)
            )
            row = await cur.fetchone()
            return bool(row) and row[0] is not None and int(row[0]) == int(inbound_id)

        normalized_click_id = str(click_id or "").strip()
        if not normalized_click_id:
            return True

        # The stable click-only key may not exist for events processed by older
        # releases. Keep the new key, but suppress delivery if that click was
        # already logged as a sale.
        placeholders = ",".join(["%s"] * len(SALE_STATUSES))
        await cur.execute(
            f"""
            SELECT 1
            FROM tg_events
            WHERE clickid = %s
              AND LOWER(TRIM(COALESCE(status, ''))) IN ({placeholders})
            LIMIT 1
            """,
            (normalized_click_id, *SALE_STATUSES),
        )
        return (await cur.fetchone()) is None


def _first_present(raw: Dict[str, Any], *keys: str) -> Any:
    """First value that is not None/"" (a payout of 0 is a real value, unlike `a or b`)."""
    for key in keys:
        value = raw.get(key)
        if value is not None and value != "":
            return value
    return None


def _parse_payout(value: Any) -> Optional[Decimal]:
    """'1.5 USD' -> 1.5, '1,234.56' -> 1234.56, '{conversion.revenue}' / garbage -> None (event is still logged)."""
    text = str(value).strip() if value is not None else ""
    if not text or (text.startswith("{") and text.endswith("}")):
        return None
    amount = extract_decimal(value)
    if amount is None:
        logger.warning("Unparseable postback payout {!r}, storing NULL", value)
    return amount


async def log_event(raw: Dict[str, Any], routed_user_id: Optional[int], inbound_id: Optional[int] = None) -> None:
    """Insert into tg_events; a retry of the same inbound postback (unique inbound_id) is a no-op."""
    payout = _first_present(
        raw, "payout", "revenue", "conversion_revenue", "profit", "conversion_profit", "conversion_cost"
    )
    await execute(
        """
        INSERT INTO tg_events(status, offer, country, source, payout, currency, clickid, raw, routed_user_id, inbound_id)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE id = id
        """,
        (
            _first_present(raw, "status", "action"),
            _first_present(raw, "offer", "offer_name", "campaign", "campaign_name"),
            _first_present(raw, "country", "geo"),
            _first_present(raw, "source", "traffic_source_name", "traffic_source", "affiliate", "traffic_source_id"),
            _parse_payout(payout),
            _first_present(raw, "currency", "revenue_currency", "payout_currency"),
            _first_present(raw, "clickid", "click_id", "subid", "sub_id", "tid"),
            json.dumps(raw, ensure_ascii=False),
            routed_user_id,
            inbound_id,
        ))
