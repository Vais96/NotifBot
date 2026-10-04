"""Facebook CSV uploads, campaign daily/totals/state and reports (fb_*)."""

from typing import Optional, List, Dict, Any, Iterable
from datetime import date, datetime
from loguru import logger
from ..constants import SALE_STATUSES
from .pool import _executemany_rows, cursor, fetch_all


async def create_fb_csv_upload(
    uploaded_by: int,
    buyer_id: Optional[int],
    original_filename: str,
    period_start: Optional[date],
    period_end: Optional[date],
    row_count: int,
    has_totals: bool
) -> int:
    async with cursor() as cur:
        await cur.execute(
            """
            INSERT INTO fb_csv_uploads(uploaded_by, buyer_id, original_filename, period_start, period_end, row_count, has_totals)
            VALUES(%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                uploaded_by,
                buyer_id,
                original_filename,
                period_start,
                period_end,
                row_count,
                1 if has_totals else 0,
            )
        )
        return cur.lastrowid


async def bulk_insert_fb_csv_rows(upload_id: int, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    payload = []
    for row in rows:
        payload.append(
            (
                upload_id,
                row.get("account_name"),
                row.get("campaign_name"),
                row.get("adset_name"),
                row.get("ad_name"),
                row.get("day_date"),
                row.get("currency"),
                row.get("spend"),
                row.get("impressions"),
                row.get("clicks"),
                row.get("leads"),
                row.get("registrations"),
                row.get("cpc"),
                row.get("ctr"),
                1 if row.get("is_total") else 0,
            )
        )
    async with cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO fb_csv_rows(
                upload_id, account_name, campaign_name, adset_name, ad_name,
                day_date, currency, spend, impressions, clicks, leads,
                registrations, cpc, ctr, is_total
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            payload,
        )


async def upsert_fb_accounts(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    payload = []
    for row in records:
        payload.append(
            (
                row.get("account_name"),
                row.get("buyer_id"),
                row.get("owner_since"),
            )
        )
    async with cursor() as cur:
        await _executemany_rows(cur,
            """
            INSERT INTO fb_accounts(account_name, buyer_id, owner_since)
            VALUES(%s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
                buyer_id=new.buyer_id,
                owner_since=COALESCE(fb_accounts.owner_since, new.owner_since),
                owner_until=NULL,
                updated_at=CURRENT_TIMESTAMP,
                is_active=1
            """,
            payload,
        )


async def upsert_fb_campaign_daily(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    payload = []
    for row in records:
        payload.append(
            (
                row.get("campaign_name"),
                row.get("day_date"),
                row.get("account_name"),
                row.get("buyer_id"),
                row.get("geo"),
                row.get("spend"),
                row.get("impressions"),
                row.get("clicks"),
                row.get("registrations"),
                row.get("leads"),
                row.get("ftd"),
                row.get("revenue"),
                row.get("ctr"),
                row.get("cpc"),
                row.get("roi"),
                row.get("ftd_rate"),
                row.get("status_id"),
                row.get("flag_id"),
                row.get("upload_id"),
            )
        )
    async with cursor() as cur:
        await _executemany_rows(cur,
            """
            INSERT INTO fb_campaign_daily(
                campaign_name, day_date, account_name, buyer_id, geo,
                spend, impressions, clicks, registrations, leads, ftd, revenue,
                ctr, cpc, roi, ftd_rate, status_id, flag_id, upload_id
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
                account_name=new.account_name,
                buyer_id=new.buyer_id,
                geo=new.geo,
                spend=new.spend,
                impressions=new.impressions,
                clicks=new.clicks,
                registrations=new.registrations,
                leads=new.leads,
                ftd=new.ftd,
                revenue=new.revenue,
                ctr=new.ctr,
                cpc=new.cpc,
                roi=new.roi,
                ftd_rate=new.ftd_rate,
                status_id=new.status_id,
                flag_id=new.flag_id,
                upload_id=new.upload_id
            """,
            payload,
        )


async def upsert_fb_campaign_totals(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    payload = []
    for row in records:
        payload.append(
            (
                row.get("campaign_name"),
                row.get("account_name"),
                row.get("buyer_id"),
                row.get("geo"),
                row.get("spend"),
                row.get("impressions"),
                row.get("clicks"),
                row.get("registrations"),
                row.get("leads"),
                row.get("ftd"),
                row.get("revenue"),
                row.get("ctr"),
                row.get("cpc"),
                row.get("roi"),
                row.get("ftd_rate"),
                row.get("status_id"),
                row.get("flag_id"),
            )
        )
    async with cursor() as cur:
        await _executemany_rows(cur,
            """
            INSERT INTO fb_campaign_totals(
                campaign_name, account_name, buyer_id, geo, spend, impressions, clicks,
                registrations, leads, ftd, revenue, ctr, cpc, roi, ftd_rate, status_id, flag_id
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
                account_name=new.account_name,
                buyer_id=new.buyer_id,
                geo=new.geo,
                spend=new.spend,
                impressions=new.impressions,
                clicks=new.clicks,
                registrations=new.registrations,
                leads=new.leads,
                ftd=new.ftd,
                revenue=new.revenue,
                ctr=new.ctr,
                cpc=new.cpc,
                roi=new.roi,
                ftd_rate=new.ftd_rate,
                status_id=new.status_id,
                flag_id=new.flag_id
            """,
            payload,
        )


async def fetch_fb_campaign_state(campaign_names: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    names = [c for c in campaign_names if c]
    if not names:
        return {}
    placeholders = ",".join(["%s"] * len(names))
    rows = await fetch_all(
        f"SELECT campaign_name, status_id, flag_id, buyer_comment, lead_comment, updated_by, updated_at FROM fb_campaign_state WHERE campaign_name IN ({placeholders})",
        tuple(names), dict_rows=True)
    return {str(row["campaign_name"]): row for row in rows}


async def upsert_fb_campaign_state(states: List[Dict[str, Any]]) -> None:
    if not states:
        return
    payload = []
    for row in states:
        payload.append(
            (
                row.get("campaign_name"),
                row.get("status_id"),
                row.get("flag_id"),
                row.get("buyer_comment"),
                row.get("lead_comment"),
                row.get("updated_by"),
            )
        )
    async with cursor() as cur:
        await _executemany_rows(cur,
            """
            INSERT INTO fb_campaign_state(campaign_name, status_id, flag_id, buyer_comment, lead_comment, updated_by)
            VALUES(%s, %s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
                status_id=new.status_id,
                flag_id=new.flag_id,
                buyer_comment=new.buyer_comment,
                lead_comment=new.lead_comment,
                updated_by=new.updated_by
            """,
            payload,
        )


async def log_fb_campaign_history(entries: List[Dict[str, Any]]) -> None:
    if not entries:
        return
    payload = []
    for row in entries:
        payload.append(
            (
                row.get("campaign_name"),
                row.get("changed_by"),
                row.get("old_status_id"),
                row.get("new_status_id"),
                row.get("old_flag_id"),
                row.get("new_flag_id"),
                row.get("note"),
            )
        )
    async with cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO fb_campaign_history(
                campaign_name, changed_by, old_status_id, new_status_id, old_flag_id, new_flag_id, note
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s)
            """,
            payload,
        )


async def list_fb_flags() -> List[Dict[str, Any]]:
    return await fetch_all("SELECT id, code, title, severity, description FROM fb_flags ORDER BY severity DESC, id ASC", dict_rows=True)


async def list_fb_available_months(limit: int = 12) -> List[date]:
    query = (
        """
        SELECT DATE_SUB(day_date, INTERVAL DAY(day_date) - 1 DAY) AS month_start
        FROM fb_campaign_daily
        GROUP BY month_start
        ORDER BY month_start DESC
        LIMIT %s
        """
    )
    months: List[date] = []
    rows = await fetch_all(query, (limit,), dict_rows=True)
    for row in rows or []:
        value = row.get("month_start")
        if isinstance(value, datetime):
            months.append(value.date())
        elif isinstance(value, date):
            months.append(value)
        elif isinstance(value, str):
            try:
                months.append(datetime.strptime(value, "%Y-%m-%d").date())
            except ValueError:
                continue
    return months


async def fetch_fb_campaign_month_report(month_start: date) -> List[Dict[str, Any]]:
    if not isinstance(month_start, date):
        raise ValueError("month_start must be a date instance")
    normalized = month_start.replace(day=1)
    if normalized.month == 12:
        month_end = date(normalized.year + 1, 1, 1)
    else:
        month_end = date(normalized.year, normalized.month + 1, 1)
    placeholders_status = ",".join(["%s"] * len(SALE_STATUSES))
    query = (
        f"""
        WITH month_data AS (
            SELECT
                d.campaign_name,
                MAX(d.account_name) AS account_name,
                MAX(d.buyer_id) AS buyer_id,
                SUM(COALESCE(d.spend, 0)) AS spend,
                SUM(COALESCE(d.impressions, 0)) AS impressions,
                SUM(COALESCE(d.clicks, 0)) AS clicks,
                SUM(COALESCE(d.registrations, 0)) AS registrations,
                SUM(COALESCE(d.leads, 0)) AS leads,
                SUM(COALESCE(d.ftd, 0)) AS ftd,
                SUM(COALESCE(d.revenue, 0)) AS revenue
            FROM fb_campaign_daily d
            WHERE d.day_date >= %s AND d.day_date < %s
            GROUP BY d.campaign_name
                        ),
                        conversion_data AS (
            SELECT
                fc.sub_id_2 AS campaign_name,
                COUNT(*) AS ftd,
                SUM(COALESCE(fc.revenue, 0)) AS revenue
            FROM fact_conversions fc
            WHERE fc.conversion_time_utc >= %s
              AND fc.conversion_time_utc < %s
              AND fc.sub_id_2 IS NOT NULL
              AND fc.sub_id_2 <> ''
                                    AND LOWER(fc.status) IN ({placeholders_status})
            GROUP BY fc.sub_id_2
        ),
        prev_flags AS (
            SELECT campaign_name, new_flag_id, changed_at
            FROM (
                SELECT
                    h.*, ROW_NUMBER() OVER (PARTITION BY h.campaign_name ORDER BY h.changed_at DESC, h.id DESC) AS rn
                FROM fb_campaign_history h
                WHERE h.changed_at < %s
            ) ranked
            WHERE rn = 1
        ),
        curr_flags AS (
            SELECT campaign_name, new_flag_id, changed_at
            FROM (
                SELECT
                    h.*, ROW_NUMBER() OVER (PARTITION BY h.campaign_name ORDER BY h.changed_at DESC, h.id DESC) AS rn
                FROM fb_campaign_history h
            ) ranked
            WHERE rn = 1
        )
        SELECT
            md.campaign_name,
            md.account_name,
            md.buyer_id,
            md.spend,
            md.impressions,
            md.clicks,
            md.registrations,
            md.leads,
            COALESCE(conv.ftd, md.ftd, 0) AS ftd,
            COALESCE(conv.revenue, md.revenue, 0) AS revenue,
            prev_flags.new_flag_id AS prev_flag_id,
            prev_flags.changed_at AS prev_flag_changed_at,
            curr_flags.new_flag_id AS curr_flag_id,
            curr_flags.changed_at AS curr_flag_changed_at,
            st.flag_id AS state_flag_id,
            st.status_id AS state_status_id
        FROM month_data md
        LEFT JOIN conversion_data conv ON conv.campaign_name = md.campaign_name
        LEFT JOIN prev_flags ON prev_flags.campaign_name = md.campaign_name
        LEFT JOIN curr_flags ON curr_flags.campaign_name = md.campaign_name
        LEFT JOIN fb_campaign_state st ON st.campaign_name = md.campaign_name
        ORDER BY md.spend DESC
        """
    )
    params: List[Any] = [normalized, month_end, normalized, month_end]
    params.extend(SALE_STATUSES)
    params.append(normalized)
    rows = await fetch_all(query, tuple(params), dict_rows=True)
    return rows or []


async def recompute_fb_campaign_totals(campaign_names: Iterable[str]) -> List[Dict[str, Any]]:
    names = [c.strip() for c in campaign_names if c and c.strip()]
    if not names:
        return []
    placeholders = ",".join(["%s"] * len(names))
    rows = await fetch_all(
        f"""
        SELECT
            campaign_name,
            MAX(account_name) AS account_name,
            MAX(buyer_id) AS buyer_id,
            MAX(geo) AS geo,
            SUM(COALESCE(spend, 0)) AS spend,
            SUM(COALESCE(impressions, 0)) AS impressions,
            SUM(COALESCE(clicks, 0)) AS clicks,
            SUM(COALESCE(registrations, 0)) AS registrations,
            SUM(COALESCE(leads, 0)) AS leads,
            SUM(COALESCE(ftd, 0)) AS ftd,
            SUM(COALESCE(revenue, 0)) AS revenue
        FROM fb_campaign_daily
        WHERE campaign_name IN ({placeholders})
        GROUP BY campaign_name
        """,
        tuple(names), dict_rows=True)
    state_map = await fetch_fb_campaign_state(names)
    records: List[Dict[str, Any]] = []
    for row in rows or []:
        campaign = str(row.get("campaign_name"))
        spend = float(row.get("spend") or 0.0)
        impressions = int(row.get("impressions") or 0)
        clicks = int(row.get("clicks") or 0)
        registrations = int(row.get("registrations") or 0)
        ftd = int(row.get("ftd") or 0)
        revenue = float(row.get("revenue") or 0.0)
        ctr = (clicks / impressions * 100) if impressions else None
        cpc = (spend / clicks) if clicks else None
        roi = ((revenue - spend) / spend * 100) if spend else None
        ftd_rate = (ftd / registrations * 100) if registrations else None
        state = state_map.get(campaign) or {}
        records.append(
            {
                "campaign_name": campaign,
                "account_name": row.get("account_name"),
                "buyer_id": row.get("buyer_id"),
                "geo": row.get("geo"),
                "spend": spend,
                "impressions": impressions,
                "clicks": clicks,
                "registrations": registrations,
                "leads": int(row.get("leads") or 0),
                "ftd": ftd,
                "revenue": revenue,
                "ctr": ctr,
                "cpc": cpc,
                "roi": roi,
                "ftd_rate": ftd_rate,
                "status_id": state.get("status_id"),
                "flag_id": state.get("flag_id"),
            }
        )
    if records:
        await upsert_fb_campaign_totals(records)
    return records


async def reset_fb_upload_data() -> None:
    tables = (
        "fb_campaign_history",
        "fb_campaign_daily",
        "fb_campaign_totals",
        "fb_campaign_state",
        "fb_csv_rows",
        "fb_csv_uploads",
        "fb_accounts",
    )
    async with cursor() as cur:
        await cur.execute("SET FOREIGN_KEY_CHECKS=0")
        try:
            for table in tables:
                try:
                    await cur.execute(f"TRUNCATE TABLE {table}")
                    logger.info("Truncated table {} during FB data reset", table)
                except Exception as exc:
                    logger.error("Failed to truncate table {}: {}", table, exc)
                    raise
        finally:
            await cur.execute("SET FOREIGN_KEY_CHECKS=1")
