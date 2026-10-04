"""Sales counters and report aggregates over tg_events; KPI and report filters."""

from typing import Optional, List, Dict, Any, Tuple
from datetime import timedelta, datetime, timezone
from ..constants import SALE_STATUSES
from .pool import cursor, execute, fetch_all, fetch_one


def _today_utc_window() -> Tuple[datetime, datetime]:
    now_utc = datetime.now(timezone.utc)
    start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


# Campaign prefix stored in tg_events.raw — the buyer alias, and the only buyer identity
# left on a deposit that never routed to a Telegram user.
_EVENT_ALIAS_EXPR = """LOWER(TRIM(SUBSTRING_INDEX(COALESCE(
                          JSON_UNQUOTE(JSON_EXTRACT(tg_events.raw, '$.campaign_name')),
                          JSON_UNQUOTE(JSON_EXTRACT(tg_events.raw, '$."campaign.name"')),
                          JSON_UNQUOTE(JSON_EXTRACT(tg_events.raw, '$.campaign')),
                          ''
                      ), '_', 1)))"""


# Sale-like events that belong to a user: routed to them directly, or sent under one of
# their aliases (covers rows logged before alias routing became authoritative).
_USER_SALES_TODAY_WHERE = f"""
        WHERE (
                routed_user_id=%s
                OR EXISTS (
                    SELECT 1
                    FROM tg_aliases a
                    WHERE a.buyer_id=%s
                      AND a.alias = {_EVENT_ALIAS_EXPR}
                )
              )
          AND created_at >= %s AND created_at < %s
          AND LOWER(TRIM(COALESCE(status, ''))) IN ({{placeholders}})
"""


async def _user_sales_today_scalar(select_expr: str, user_id: int) -> Any:
    start, end = _today_utc_window()
    placeholders = ",".join(["%s"] * len(SALE_STATUSES))
    query = f"SELECT {select_expr} FROM tg_events" + _USER_SALES_TODAY_WHERE.format(placeholders=placeholders)
    row = await fetch_one(query, (user_id, user_id, start, end, *SALE_STATUSES))
    return row[0] if row else None


async def count_today_user_sales(user_id: int) -> int:
    """Return today's sales routed to the user or assigned to one of their aliases."""
    value = await _user_sales_today_scalar("COUNT(*)", user_id)
    return int(value or 0)


async def sum_today_user_profit(user_id: int) -> float:
    """Sum payout of today's sales (UTC day) routed to the user or sent under their aliases."""
    value = await _user_sales_today_scalar("COALESCE(SUM(payout), 0)", user_id)
    return float(value or 0)


async def today_alias_sales(alias: str) -> Tuple[int, float]:
    """Today's deposit count and payout sum for a campaign prefix, routed or not.

    A buyer without a ``tg_aliases`` row never gets a ``routed_user_id``, so the per-buyer
    daily lines would go missing exactly where they matter. The campaign prefix still
    identifies them, so count by it when routing produced no user.
    """
    normalized = (alias or "").strip().lower()
    if not normalized:
        return 0, 0.0
    start, end = _today_utc_window()
    placeholders = ",".join(["%s"] * len(SALE_STATUSES))
    query = f"""
        SELECT COUNT(*), COALESCE(SUM(payout), 0)
        FROM tg_events
        WHERE {_EVENT_ALIAS_EXPR} = %s
          AND created_at >= %s AND created_at < %s
          AND LOWER(TRIM(COALESCE(status, ''))) IN ({placeholders})
    """
    row = await fetch_one(query, (normalized, start, end, *SALE_STATUSES))
    if not row:
        return 0, 0.0
    return int(row[0] or 0), float(row[1] or 0)


async def sales_by_user_between(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Per-buyer deposit count and payout sum for sale-like events in [start, end) (UTC).

    Unrouted events (routed_user_id IS NULL) are returned under user_id=None so callers
    can decide whether to show them.
    """
    placeholders = ",".join(["%s"] * len(SALE_STATUSES))
    query = f"""
        SELECT routed_user_id, COUNT(*), COALESCE(SUM(payout), 0)
        FROM tg_events
        WHERE created_at >= %s AND created_at < %s
          AND LOWER(TRIM(COALESCE(status, ''))) IN ({placeholders})
        GROUP BY routed_user_id
    """
    rows = await fetch_all(query, (start, end, *SALE_STATUSES))
    return [
        {
            "user_id": int(r[0]) if r[0] is not None else None,
            "count": int(r[1] or 0),
            "revenue": float(r[2] or 0),
        }
        for r in rows
    ]


async def get_kpi(user_id: int) -> Dict[str, Any]:
    row = await fetch_one("SELECT user_id, daily_goal, weekly_goal FROM tg_kpi WHERE user_id=%s", (user_id,), dict_rows=True)
    return row or {"user_id": user_id, "daily_goal": None, "weekly_goal": None}


async def set_kpi(user_id: int, daily_goal: Optional[int] = None, weekly_goal: Optional[int] = None) -> None:
    await execute(
        """
        INSERT INTO tg_kpi(user_id, daily_goal, weekly_goal)
        VALUES(%s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            daily_goal=new.daily_goal,
            weekly_goal=new.weekly_goal
        """,
        (user_id, daily_goal, weekly_goal))


_OFFER_NAME_EXPR = "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer_name')), offer)"
# creative from common raw JSON fields; empty strings skipped
_CREATIVE_EXPR = "COALESCE(" + ", ".join(
    f"NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.{key}')), '')"
    for key in ("creative", "banner", "ad_name", "adset_name", "ad", "creative_name", "sub_id_2", "sub2", "utm_content")
) + ")"


def _events_where(
    start, end, user_ids: List[int], offer: Optional[str], creative: Optional[str], *, sales_only: bool
) -> Tuple[str, List[Any]]:
    """WHERE for tg_events in [start, end) of the given users (+ optional sale/offer/creative filters)."""
    clauses = ["created_at >= %s AND created_at < %s"]
    params: List[Any] = [start, end]
    if sales_only:
        clauses.append(f"LOWER(TRIM(COALESCE(status,''))) IN ({','.join(['%s'] * len(SALE_STATUSES))})")
        params += SALE_STATUSES
    clauses.append(f"routed_user_id IN ({','.join(['%s'] * len(user_ids)) if user_ids else 'NULL'})")
    params += user_ids
    if offer:
        clauses.append(
            "(offer = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer_name')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer')) = %s)"
        )
        params += [offer, offer, offer]
    if creative:
        clauses.append(
            "(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.creative')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.banner')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.ad_name')) = %s)"
        )
        params += [creative, creative, creative]
    return " AND ".join(clauses), params


async def _grouped_counts(
    cur, key_expr: str, where: str, params: List[Any], *, extra: str = "", order: str = "COUNT(*) DESC", limit: str = ""
) -> list:
    await cur.execute(
        f"SELECT {key_expr} AS k, COUNT(*) FROM tg_events WHERE {where}{extra} GROUP BY k ORDER BY {order}{limit}",
        params,
    )
    return list(await cur.fetchall() or [])


async def aggregate_sales(user_ids: List[int], start, end, offer: Optional[str] = None, creative: Optional[str] = None, filter_user_ids: Optional[List[int]] = None) -> Dict[str, Any]:
    """
    Return dict with keys: count, profit, top_offer, top_offer_count, geo_dist, creative_dist, buyer_dist, offer_dist, total.
    Filters: offer (by raw->'offer' or stored offer), creative (by raw JSON keys: creative/name/banner), time window [start, end).
    """
    if not user_ids:
        return {
            "count": 0,
            "profit": 0.0,
            "top_offer": None,
            "geo_dist": {},
            "creative_dist": {},
            "buyer_dist": {},
            "offer_dist": {},
            "total": 0,
        }
    if filter_user_ids is not None:
        base_set = set(user_ids)
        user_ids = [uid for uid in filter_user_ids if uid in base_set]
    any_where, any_params = _events_where(start, end, user_ids, offer, creative, sales_only=False)
    where, params = _events_where(start, end, user_ids, offer, creative, sales_only=True)
    async with cursor() as cur:
        await cur.execute(f"SELECT COUNT(*) FROM tg_events WHERE {any_where}", any_params)
        total = int((await cur.fetchone())[0] or 0)
        await cur.execute(f"SELECT COUNT(*), COALESCE(SUM(payout),0) FROM tg_events WHERE {where}", params)
        row = await cur.fetchone()
        count, profit = int(row[0] or 0), float(row[1] or 0)
        geo_rows = await _grouped_counts(
            cur, "country", where, params, extra=" AND country IS NOT NULL AND country <> ''", limit=" LIMIT 10"
        )
        creative_rows = await _grouped_counts(cur, _CREATIVE_EXPR, where, params, limit=" LIMIT 10")
        buyer_rows = await _grouped_counts(cur, "routed_user_id", where, params)
        offer_rows = await _grouped_counts(cur, _OFFER_NAME_EXPR, where, params, order="COUNT(*) DESC, k ASC")
    return {
        "count": count,
        "profit": profit,
        "top_offer": offer_rows[0][0] if offer_rows else None,
        "top_offer_count": int(offer_rows[0][1] or 0) if offer_rows else 0,
        "geo_dist": {str(k): int(c) for k, c in geo_rows if k is not None and str(k).strip() not in ("", "-")},
        "creative_dist": {str(k): int(c) for k, c in creative_rows if k is not None and str(k).strip() != ""},
        "buyer_dist": {int(k): int(c) for k, c in buyer_rows if k is not None},
        "offer_dist": {(str(k).strip() if k is not None and str(k).strip() else "(пусто)"): int(c) for k, c in offer_rows},
        "total": total,
    }


async def trend_daily_sales(user_ids: List[int], days: int = 7) -> List[Tuple[str, int]]:
    """Return list of (YYYY-MM-DD, count) for last N days (UTC)."""
    from datetime import datetime, timezone, timedelta
    placeholders_status = ",".join(["%s"] * len(SALE_STATUSES))
    placeholders_users = ",".join(["%s"] * len(user_ids)) if user_ids else "NULL"
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days-1)
    async with cursor() as cur:
        query = f"""
            SELECT DATE(CONVERT_TZ(created_at, '+00:00', '+00:00')) AS d, COUNT(*)
            FROM tg_events
            WHERE created_at >= %s AND created_at < %s + INTERVAL 1 DAY
              AND LOWER(TRIM(COALESCE(status,''))) IN ({placeholders_status})
              AND routed_user_id IN ({placeholders_users})
            GROUP BY d
            ORDER BY d ASC
        """
        params = [start, now, *SALE_STATUSES, *user_ids] if user_ids else [start, now, *SALE_STATUSES]
        await cur.execute(query, params)
        rows = await cur.fetchall()
        return [(str(r[0]), int(r[1])) for r in rows]


async def get_report_filter(user_id: int) -> Dict[str, Any]:
    row = await fetch_one("SELECT offer, creative, buyer_id, team_id FROM tg_report_filters WHERE user_id=%s", (user_id,), dict_rows=True)
    return row or {"offer": None, "creative": None, "buyer_id": None, "team_id": None}


async def set_report_filter(user_id: int, offer: Optional[str], creative: Optional[str], buyer_id: Optional[int] = None, team_id: Optional[int] = None) -> None:
    await execute(
        """
        INSERT INTO tg_report_filters(user_id, offer, creative, buyer_id, team_id)
        VALUES(%s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE offer=new.offer, creative=new.creative, buyer_id=new.buyer_id, team_id=new.team_id
        """,
        (user_id, offer, creative, buyer_id, team_id))


async def clear_report_filter(user_id: int) -> None:
    await execute("DELETE FROM tg_report_filters WHERE user_id=%s", (user_id,))


async def list_offers_for_users(user_ids: List[int]) -> List[str]:
    if not user_ids:
        return []
    placeholders = ",".join(["%s"] * len(user_ids))
    async with cursor() as cur:
        query = f"""
            SELECT DISTINCT off FROM (
                SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer_name')), offer) AS off
                FROM tg_events
                WHERE routed_user_id IN ({placeholders})
            ) t
            WHERE off IS NOT NULL AND off <> ''
            ORDER BY off ASC
        """
        await cur.execute(query, (*user_ids,))
        rows = await cur.fetchall()
        return [str(r[0]) for r in rows if r and r[0]]


async def list_creatives_for_users(user_ids: List[int], offer: Optional[str] = None) -> List[str]:
    if not user_ids:
        return []
    placeholders = ",".join(["%s"] * len(user_ids))
    offer_sql = ""
    params: List[Any] = [*user_ids]
    if offer:
        offer_sql = " AND (offer = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer_name')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer')) = %s)"
        params += [offer, offer, offer]
    async with cursor() as cur:
        query = f"""
            SELECT DISTINCT COALESCE(
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.creative')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.banner')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.ad_name')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.adset_name')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.ad')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.creative_name')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub_id_2')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub2')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.utm_content')), '')
                ) AS cr
            FROM tg_events
            WHERE routed_user_id IN ({placeholders})
            {offer_sql}
            ORDER BY cr ASC
        """
        await cur.execute(query, (*params,))
        rows = await cur.fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
