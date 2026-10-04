"""Keitaro campaign cache (domain lookup, inferred buyers, campaign stats)."""

from typing import Optional, List, Dict, Any, Tuple, Iterable
from datetime import date, timedelta, datetime
from ..constants import SALE_STATUSES
from .pool import _executemany_rows, fetch_all, transaction


async def upsert_keitaro_campaigns(rows: List[Dict[str, Any]]) -> int:
    """Insert new Keitaro campaigns and update existing ones without wiping old rows."""
    if not rows:
        return 0
    payload = []
    for row in rows:
        cid = int(row.get("id"))
        name = str(row.get("name") or "")
        prefix = row.get("prefix")
        alias_raw = row.get("alias_key")
        alias_key = alias_raw.lower() if isinstance(alias_raw, str) and alias_raw else None
        source_domain = (row.get("source_domain") or None)
        target_domain = (row.get("target_domain") or None)
        payload.append((cid, name, prefix, alias_key, source_domain, target_domain))
    async with transaction() as cur:
        await _executemany_rows(cur,
            """
            INSERT INTO keitaro_campaigns(id, name, prefix, alias_key, source_domain, target_domain, updated_at)
            VALUES(%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP) AS new
            ON DUPLICATE KEY UPDATE
                name=new.name,
                prefix=new.prefix,
                alias_key=new.alias_key,
                source_domain=new.source_domain,
                target_domain=new.target_domain,
                updated_at=CURRENT_TIMESTAMP
            """,
            payload,
        )
    return len(payload)


async def find_campaigns_by_domain(domain: str) -> List[Dict[str, Any]]:
    if not domain:
        return []
    from ..keitaro import campaign_row_matches_domain, normalize_domain

    value = normalize_domain(domain) or domain.strip().lower()
    if not value:
        return []
    like_host = f"%.{value}"
    like_name = f"%{value}%"
    rows = await fetch_all(
        """
        SELECT id, name, prefix, alias_key, source_domain, target_domain, updated_at
        FROM keitaro_campaigns
        WHERE source_domain=%s OR target_domain=%s
           OR source_domain LIKE %s OR target_domain LIKE %s
           OR name LIKE %s
        ORDER BY prefix IS NULL, prefix ASC, name ASC
        """,
        (value, value, like_host, like_host, like_name), dict_rows=True)
    matched: List[Dict[str, Any]] = []
    seen_ids: set[int] = set()
    for row in rows or []:
        if not campaign_row_matches_domain(row, value):
            continue
        try:
            cid = int(row.get("id"))
        except Exception:
            cid = None
        if cid is not None:
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
        matched.append(row)
    return matched


async def infer_campaign_buyers(identifiers: Iterable[str], lookback_days: int = 45) -> Dict[str, int]:
    names = {s.strip().lower() for s in identifiers if s and isinstance(s, str) and s.strip()}
    if not names:
        return {}
    placeholders = ",".join(["%s"] * len(names))
    cname_expr = """
        LOWER(
            COALESCE(
                NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub_id_2')), ''),
                NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub2')), ''),
                NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub_id2')), ''),
                NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.campaign')), '')
            )
        )
    """
    start_ts = datetime.utcnow() - timedelta(days=max(1, lookback_days))
    rows = await fetch_all(
        f"""
        SELECT
            campaign_name,
            routed_user_id,
            cnt,
            last_event
        FROM (
            SELECT
                {cname_expr} AS campaign_name,
                routed_user_id,
                COUNT(*) AS cnt,
                MAX(created_at) AS last_event
            FROM tg_events
            WHERE created_at >= %s
              AND routed_user_id IS NOT NULL
            GROUP BY campaign_name, routed_user_id
        ) agg
        WHERE campaign_name IS NOT NULL
          AND campaign_name <> ''
          AND campaign_name IN ({placeholders})
        ORDER BY campaign_name ASC, cnt DESC, last_event DESC
        """,
        (start_ts, *names), dict_rows=True)
    result: Dict[str, int] = {}
    for row in rows or []:
        campaign_name = (row.get("campaign_name") or "").strip().lower()
        routed_user_id = row.get("routed_user_id")
        if not campaign_name or routed_user_id is None:
            continue
        if campaign_name in result:
            continue
        try:
            result[campaign_name] = int(routed_user_id)
        except Exception:
            continue
    return result


async def fetch_keitaro_campaign_stats(
    campaign_names: Iterable[str],
    period_start: Optional[date],
    period_end: Optional[date]
) -> Dict[str, Dict[Any, Any]]:
    names = [c.strip() for c in campaign_names if c and c.strip()]
    if not names or not period_start or not period_end:
        return {"daily": {}, "totals": {}}
    start = min(period_start, period_end)
    end = max(period_start, period_end)
    end_exclusive = end + timedelta(days=1)
    placeholders_names = ",".join(["%s"] * len(names))
    placeholders_status = ",".join(["%s"] * len(SALE_STATUSES))
    query = f"""
        SELECT
            DATE(fc.conversion_time_utc) AS day_date,
            fc.sub_id_2 AS campaign_name,
            COUNT(*) AS ftd,
            SUM(COALESCE(fc.revenue, 0)) AS revenue
        FROM fact_conversions fc
        WHERE fc.conversion_time_utc >= %s
          AND fc.conversion_time_utc < %s
          AND fc.sub_id_2 IS NOT NULL
          AND fc.sub_id_2 <> ''
          AND fc.sub_id_2 IN ({placeholders_names})
          AND LOWER(fc.status) IN ({placeholders_status})
        GROUP BY fc.sub_id_2, DATE(fc.conversion_time_utc)
    """
    params: List[Any] = [start, end_exclusive]
    params.extend(names)
    params.extend(SALE_STATUSES)
    daily: Dict[Tuple[str, date], Dict[str, Any]] = {}
    totals: Dict[str, Dict[str, Any]] = {}
    rows = await fetch_all(query, tuple(params), dict_rows=True)
    for row in rows or []:
        campaign = str(row.get("campaign_name"))
        day = row.get("day_date")
        ftd = int(row.get("ftd") or 0)
        revenue = float(row.get("revenue") or 0)
        daily[(campaign, day)] = {"ftd": ftd, "revenue": revenue}
        agg = totals.setdefault(campaign, {"ftd": 0, "revenue": 0.0})
        agg["ftd"] += ftd
        agg["revenue"] += revenue
    return {"daily": daily, "totals": totals}
