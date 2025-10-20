import aiomysql
from typing import Optional, List, Dict, Any, Tuple
from loguru import logger
from .config import settings
import urllib.parse
import ssl
import json

_pool: Optional[aiomysql.Pool] = None

def _parse_mysql_dsn(dsn: str) -> Dict[str, Any]:
    # supports mysql://user:pass@host:port/db?charset=utf8mb4
    url = urllib.parse.urlparse(dsn)
    if url.scheme not in ("mysql", "mysql+aiomysql"):
        raise ValueError("DATABASE_URL must start with mysql://")
    qs = urllib.parse.parse_qs(url.query)
    params: Dict[str, Any] = {
        "host": url.hostname or "localhost",
        "port": url.port or 3306,
        "user": urllib.parse.unquote(url.username or "root"),
        "password": urllib.parse.unquote(url.password or ""),
        "db": (url.path or "/")[1:] or None,
        "charset": qs.get("charset", ["utf8mb4"])[0],
        "autocommit": True,
        "connect_timeout": int(qs.get("connect_timeout", [10])[0]),
    }
    ssl_flag = (qs.get("ssl", ["false"])[0]).lower() in ("1", "true", "on", "required", "require")
    if ssl_flag:
        params["ssl"] = ssl.create_default_context()
    return params

SCHEMA_SQL = [
    # users with roles and team
    """
    CREATE TABLE IF NOT EXISTS tg_users (
        telegram_id BIGINT PRIMARY KEY,
        username VARCHAR(255) NULL,
        full_name VARCHAR(255) NULL,
        role ENUM('buyer','lead','head','admin','mentor') NOT NULL DEFAULT 'buyer',
        team_id BIGINT NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS tg_teams (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS tg_routes (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        user_id BIGINT NOT NULL,
        offer VARCHAR(255) NULL,
        country VARCHAR(8) NULL,
        source VARCHAR(64) NULL,
        priority INT NOT NULL DEFAULT 0,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_tg_routes_active (is_active),
        INDEX idx_tg_routes_match (offer, country, source),
        CONSTRAINT fk_tg_routes_user FOREIGN KEY (user_id) REFERENCES tg_users (telegram_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS tg_events (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        status VARCHAR(64) NULL,
        offer VARCHAR(255) NULL,
        country VARCHAR(8) NULL,
        source VARCHAR(64) NULL,
        payout DECIMAL(12,2) NULL,
        currency VARCHAR(8) NULL,
        clickid VARCHAR(255) NULL,
        raw JSON NOT NULL,
        routed_user_id BIGINT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # alias-based routing: alias -> buyer/lead
    """
    CREATE TABLE IF NOT EXISTS tg_aliases (
        alias VARCHAR(255) PRIMARY KEY,
        buyer_id BIGINT NULL,
        lead_id BIGINT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_tg_alias_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL,
        CONSTRAINT fk_tg_alias_lead FOREIGN KEY (lead_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # simple pending action storage per admin for inline flows
    """
    CREATE TABLE IF NOT EXISTS tg_pending_actions (
        admin_id BIGINT PRIMARY KEY,
        action VARCHAR(255) NOT NULL,
        target_user_id BIGINT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # mentors following teams (many-to-many)
    """
    CREATE TABLE IF NOT EXISTS tg_mentor_teams (
        mentor_id BIGINT NOT NULL,
        team_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (mentor_id, team_id),
        CONSTRAINT fk_tg_mentor_user FOREIGN KEY (mentor_id) REFERENCES tg_users (telegram_id) ON DELETE CASCADE,
        CONSTRAINT fk_tg_mentor_team FOREIGN KEY (team_id) REFERENCES tg_teams (id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # KPI goals per user (daily/weekly deposits target)
    """
    CREATE TABLE IF NOT EXISTS tg_kpi (
        user_id BIGINT PRIMARY KEY,
        daily_goal INT NULL,
        weekly_goal INT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_tg_kpi_user FOREIGN KEY (user_id) REFERENCES tg_users (telegram_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Report filters per user
    """
    CREATE TABLE IF NOT EXISTS tg_report_filters (
        user_id BIGINT PRIMARY KEY,
        offer VARCHAR(255) NULL,
        creative VARCHAR(255) NULL,
        buyer_id BIGINT NULL,
        team_id BIGINT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_tg_filters_user FOREIGN KEY (user_id) REFERENCES tg_users (telegram_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # UI cache for short callback data mapping (e.g., offers/creatives lists)
    """
    CREATE TABLE IF NOT EXISTS tg_ui_cache (
        user_id BIGINT NOT NULL,
        kind VARCHAR(32) NOT NULL,
        idx INT NOT NULL,
        value TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, kind, idx),
        INDEX idx_ui_cache_user_kind (user_id, kind),
        CONSTRAINT fk_tg_ui_cache_user FOREIGN KEY (user_id) REFERENCES tg_users (telegram_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]

async def init_pool() -> aiomysql.Pool:
    global _pool
    if _pool is None:
        logger.info("Creating MySQL pool")
        params = _parse_mysql_dsn(settings.database_url)
        _pool = await aiomysql.create_pool(**params, minsize=1, maxsize=10)
        async with _pool.acquire() as conn:
            async with conn.cursor() as cur:
                for i, stmt in enumerate(SCHEMA_SQL, start=1):
                    try:
                        logger.debug(f"Applying schema statement {i}/{len(SCHEMA_SQL)}")
                        await cur.execute(stmt)
                    except Exception as e:
                        logger.error(f"Schema DDL failed at statement {i}: {stmt}\nError: {e}")
                        raise
                # Ensure 'mentor' exists in role enum (migration for existing installations)
                try:
                    await cur.execute("SHOW COLUMNS FROM tg_users LIKE 'role'")
                    col = await cur.fetchone()
                    if col and isinstance(col, (list, tuple)):
                        col_type = col[1] if len(col) > 1 else ''
                    else:
                        col_type = ''
                    if 'enum(' in col_type.lower() and 'mentor' not in col_type:
                        logger.info("Altering tg_users.role to include 'mentor'")
                        await cur.execute("ALTER TABLE tg_users MODIFY role ENUM('buyer','lead','head','admin','mentor') NOT NULL DEFAULT 'buyer'")
                except Exception as e:
                    logger.warning(f"Failed to ensure mentor in role enum: {e}")
                # Ensure tg_report_filters has buyer_id and team_id columns (migration for existing installations)
                try:
                    await cur.execute("SHOW COLUMNS FROM tg_report_filters")
                    cols = await cur.fetchall()
                    col_names = {str(c[0]) for c in cols} if cols else set()
                    if 'buyer_id' not in col_names:
                        logger.info("Altering tg_report_filters to add buyer_id")
                        await cur.execute("ALTER TABLE tg_report_filters ADD COLUMN buyer_id BIGINT NULL AFTER creative")
                    if 'team_id' not in col_names:
                        logger.info("Altering tg_report_filters to add team_id")
                        await cur.execute("ALTER TABLE tg_report_filters ADD COLUMN team_id BIGINT NULL AFTER buyer_id")
                except Exception as e:
                    logger.warning(f"Failed to ensure columns in tg_report_filters: {e}")
    return _pool

async def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None

async def upsert_user(telegram_id: int, username: Optional[str], full_name: Optional[str]) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_users(telegram_id, username, full_name)
                VALUES(%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    username = COALESCE(NULLIF(VALUES(username), ''), username),
                    full_name = COALESCE(NULLIF(VALUES(full_name), ''), full_name),
                    is_active = 1
                """,
                (telegram_id, username, full_name)
            )

async def list_users() -> List[Dict[str, Any]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT telegram_id, username, full_name, role, team_id, is_active, created_at FROM tg_users ORDER BY created_at DESC")
            return await cur.fetchall()

async def get_user(telegram_id: int) -> Optional[Dict[str, Any]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT telegram_id, username, full_name, role, team_id, is_active, created_at FROM tg_users WHERE telegram_id=%s",
                (telegram_id,)
            )
            row = await cur.fetchone()
            return row

async def set_user_role(telegram_id: int, role: str) -> None:
    assert role in ("buyer", "lead", "head", "admin", "mentor")
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE tg_users SET role=%s WHERE telegram_id=%s", (role, telegram_id))

async def set_user_active(telegram_id: int, is_active: bool) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE tg_users SET is_active=%s WHERE telegram_id=%s", (1 if is_active else 0, telegram_id))

async def create_team(name: str) -> int:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO tg_teams(name) VALUES(%s)", (name,))
            return cur.lastrowid

async def set_user_team(telegram_id: int, team_id: Optional[int]) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE tg_users SET team_id=%s WHERE telegram_id=%s", (team_id, telegram_id))

async def list_teams() -> List[Dict[str, Any]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id, name, created_at FROM tg_teams ORDER BY id DESC")
            return await cur.fetchall()

async def add_route(user_id: int, offer: Optional[str], country: Optional[str], source: Optional[str], priority: int = 0) -> int:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_routes(user_id, offer, country, source, priority)
                VALUES(%s, %s, %s, %s, %s)
                """,
                (user_id, offer, country, source, priority)
            )
            return cur.lastrowid

async def list_routes() -> List[Dict[str, Any]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT r.id, r.user_id, u.username, u.full_name, r.offer, r.country, r.source, r.priority, r.is_active, r.created_at
                FROM tg_routes r
                JOIN tg_users u ON u.telegram_id = r.user_id
                ORDER BY r.priority DESC, r.created_at DESC
                """
            )
            return await cur.fetchall()

async def find_user_for_postback(offer: Optional[str], country: Optional[str], source: Optional[str]) -> Optional[int]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # weight by specificity
            await cur.execute(
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
                (offer, offer, country, country, source, source)
            )
            row = await cur.fetchone()
            return int(row[0]) if row else None

async def log_event(raw: Dict[str, Any], routed_user_id: Optional[int]) -> None:
    pool = await init_pool()
    payload = {
        "status": raw.get("status") or raw.get("action"),
        "offer": raw.get("offer") or raw.get("offer_name") or raw.get("campaign") or raw.get("campaign_name"),
        "country": raw.get("country") or raw.get("geo"),
        "source": raw.get("source") or raw.get("traffic_source_name") or raw.get("traffic_source") or raw.get("affiliate") or raw.get("traffic_source_id"),
        "payout": raw.get("payout") or raw.get("revenue") or raw.get("conversion_revenue") or raw.get("profit") or raw.get("conversion_profit") or raw.get("conversion_cost"),
        "currency": raw.get("currency") or raw.get("revenue_currency") or raw.get("payout_currency"),
        "clickid": raw.get("clickid") or raw.get("click_id") or raw.get("subid") or raw.get("sub_id") or raw.get("tid")
    }
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_events(status, offer, country, source, payout, currency, clickid, raw, routed_user_id)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    payload["status"], payload["offer"], payload["country"], payload["source"],
                    (
                        (lambda v: (lambda s: (float(s) if s not in (None, "") else None))(
                            (str(v).replace(",", ".").strip()) if not (isinstance(v, str) and v.strip().startswith("{") and v.strip().endswith("}")) else None
                        ))(payload["payout"]) if (payload["payout"] not in (None, "")) else None
                    )
                    if True else None,
                    payload["currency"], payload["clickid"], json.dumps(raw, ensure_ascii=False), routed_user_id
                )
            )

async def count_today_user_sales(user_id: int) -> int:
    """Return number of sale-like events for the user since UTC midnight (inclusive)."""
    from datetime import datetime, timezone, timedelta
    pool = await init_pool()
    now_utc = datetime.now(timezone.utc)
    start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    sale_like = (
        "sale", "approved", "approve", "confirmed", "confirm", "purchase", "purchased", "paid", "success"
    )
    placeholders = ",".join(["%s"] * len(sale_like))
    query = f"""
        SELECT COUNT(*)
        FROM tg_events
        WHERE routed_user_id=%s
          AND created_at >= %s AND created_at < %s
          AND LOWER(TRIM(COALESCE(status, ''))) IN ({placeholders})
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (user_id, start, end, *sale_like))
            row = await cur.fetchone()
            return int(row[0]) if row else 0

async def sum_today_user_profit(user_id: int) -> float:
    """Sum payout for sale-like events for the user since UTC midnight (inclusive)."""
    from datetime import datetime, timezone, timedelta
    pool = await init_pool()
    now_utc = datetime.now(timezone.utc)
    start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    sale_like = (
        "sale", "approved", "approve", "confirmed", "confirm", "purchase", "purchased", "paid", "success"
    )
    placeholders = ",".join(["%s"] * len(sale_like))
    query = f"""
        SELECT COALESCE(SUM(payout), 0)
        FROM tg_events
        WHERE routed_user_id=%s
          AND created_at >= %s AND created_at < %s
          AND LOWER(TRIM(COALESCE(status, ''))) IN ({placeholders})
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (user_id, start, end, *sale_like))
            row = await cur.fetchone()
            return float(row[0] or 0)

async def get_kpi(user_id: int) -> Dict[str, Any]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT user_id, daily_goal, weekly_goal FROM tg_kpi WHERE user_id=%s", (user_id,))
            row = await cur.fetchone()
            return row or {"user_id": user_id, "daily_goal": None, "weekly_goal": None}

async def set_kpi(user_id: int, daily_goal: Optional[int] = None, weekly_goal: Optional[int] = None) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Upsert
            await cur.execute(
                """
                INSERT INTO tg_kpi(user_id, daily_goal, weekly_goal)
                VALUES(%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    daily_goal=VALUES(daily_goal),
                    weekly_goal=VALUES(weekly_goal)
                """,
                (user_id, daily_goal, weekly_goal)
            )

async def aggregate_sales(user_ids: List[int], start, end, offer: Optional[str] = None, creative: Optional[str] = None, filter_user_ids: Optional[List[int]] = None) -> Dict[str, Any]:
    """
    Return dict with keys: count, profit, unique_clicks (if available), top_offer, geo_dist, source_dist.
    Filters: offer (by raw->'offer' or stored offer), creative (by raw JSON keys: creative/name/banner), time window [start, end).
    """
    if not user_ids:
        return {"count": 0, "profit": 0.0, "top_offer": None, "geo_dist": {}, "source_dist": {}, "total": 0}
    pool = await init_pool()
    sale_like = (
        "sale", "approved", "approve", "confirmed", "confirm", "purchase", "purchased", "paid", "success"
    )
    placeholders_status = ",".join(["%s"] * len(sale_like))
    # If filter_user_ids provided, intersect with user_ids
    if filter_user_ids is not None:
        base_set = set(user_ids)
        user_ids = [uid for uid in filter_user_ids if uid in base_set]
    placeholders_users = ",".join(["%s"] * len(user_ids)) if user_ids else "NULL"
    offer_filter_sql = ""
    creative_filter_sql = ""
    params: list[Any] = [start, end, *sale_like, *user_ids]
    if offer:
        offer_filter_sql = " AND (offer = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer_name')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer')) = %s)"
        params += [offer, offer, offer]
    if creative:
        # try common fields
        creative_filter_sql = (
            " AND (JSON_UNQUOTE(JSON_EXTRACT(raw, '$.creative')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.banner')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.ad_name')) = %s)"
        )
        params += [creative, creative, creative]
    # total events (any status)
    total_sql = f"""
        SELECT COUNT(*)
        FROM tg_events
        WHERE created_at >= %s AND created_at < %s
          AND routed_user_id IN ({placeholders_users})
          {offer_filter_sql}
          {creative_filter_sql}
    """
    total_params: list[Any] = [start, end]
    if user_ids:
        total_params += [*user_ids]
    if offer:
        total_params += [offer, offer, offer]
    if creative:
        total_params += [creative, creative, creative]

    # totals for sales
    totals_sql = f"""
        SELECT COUNT(*), COALESCE(SUM(payout),0)
        FROM tg_events
        WHERE created_at >= %s AND created_at < %s
          AND LOWER(TRIM(COALESCE(status,''))) IN ({placeholders_status})
          AND routed_user_id IN ({placeholders_users})
          {offer_filter_sql}
          {creative_filter_sql}
    """
    # top offer by offer_name if present, else fall back to stored offer
    top_offer_sql = f"""
            SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer_name')), offer) AS offer_name, COUNT(*) AS cnt
            FROM tg_events
            WHERE created_at >= %s AND created_at < %s
                AND LOWER(TRIM(COALESCE(status,''))) IN ({placeholders_status})
                AND routed_user_id IN ({placeholders_users})
                {offer_filter_sql}
                {creative_filter_sql}
            GROUP BY offer_name
            ORDER BY cnt DESC
            LIMIT 1
    """
    # geo distribution (exclude empty/null)
    geo_sql = f"""
            SELECT country AS k, COUNT(*)
            FROM tg_events
            WHERE created_at >= %s AND created_at < %s
                AND LOWER(TRIM(COALESCE(status,''))) IN ({placeholders_status})
                AND routed_user_id IN ({placeholders_users})
                {offer_filter_sql}
                {creative_filter_sql}
                AND country IS NOT NULL AND country <> ''
            GROUP BY k
            ORDER BY COUNT(*) DESC
            LIMIT 10
    """
    # creative distribution (use common fields in raw JSON; exclude empty)
    creative_sql = f"""
            SELECT COALESCE(
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.creative')), ''),
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.banner')), ''),
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.ad_name')), ''),
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.adset_name')), ''),
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.ad')), ''),
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.creative_name')), ''),
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub_id_2')), ''),
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub2')), ''),
                             NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.utm_content')), '')
                         ) AS k,
                         COUNT(*)
            FROM tg_events
            WHERE created_at >= %s AND created_at < %s
                AND LOWER(TRIM(COALESCE(status,''))) IN ({placeholders_status})
                AND routed_user_id IN ({placeholders_users})
                {offer_filter_sql}
                {creative_filter_sql}
            GROUP BY k
            ORDER BY COUNT(*) DESC
            LIMIT 10
    """
    # buyer distribution (counts per routed_user_id)
    buyer_sql = f"""
            SELECT routed_user_id AS uid, COUNT(*) AS cnt
            FROM tg_events
            WHERE created_at >= %s AND created_at < %s
                AND LOWER(TRIM(COALESCE(status,''))) IN ({placeholders_status})
                AND routed_user_id IN ({placeholders_users})
                {offer_filter_sql}
                {creative_filter_sql}
            GROUP BY uid
            ORDER BY cnt DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # total first
            await cur.execute(total_sql, total_params)
            row = await cur.fetchone()
            total = int(row[0] or 0)
            await cur.execute(totals_sql, params)
            row = await cur.fetchone()
            count = int(row[0] or 0)
            profit = float(row[1] or 0)
            await cur.execute(top_offer_sql, params)
            row = await cur.fetchone()
            top_offer = row[0] if row else None
            top_offer_count = int(row[1] or 0) if row else 0
            await cur.execute(geo_sql, params)
            geo_rows = await cur.fetchall()
            geo_dist = {str(r[0]): int(r[1]) for r in geo_rows if (r[0] is not None and str(r[0]).strip() not in ('', '-'))}
            await cur.execute(creative_sql, params)
            cr_rows = await cur.fetchall()
            creative_dist = {str(r[0]): int(r[1]) for r in cr_rows if r[0] is not None and str(r[0]).strip() != ''}
            # buyer distribution
            await cur.execute(buyer_sql, params)
            by_rows = await cur.fetchall()
            buyer_dist = {int(r[0]): int(r[1]) for r in by_rows if r and r[0] is not None}
    return {"count": count, "profit": profit, "top_offer": top_offer, "top_offer_count": top_offer_count, "geo_dist": geo_dist, "creative_dist": creative_dist, "buyer_dist": buyer_dist, "total": total}

async def trend_daily_sales(user_ids: List[int], days: int = 7) -> List[Tuple[str, int]]:
    """Return list of (YYYY-MM-DD, count) for last N days (UTC)."""
    from datetime import datetime, timezone, timedelta
    pool = await init_pool()
    sale_like = (
        "sale", "approved", "approve", "confirmed", "confirm", "purchase", "purchased", "paid", "success"
    )
    placeholders_status = ",".join(["%s"] * len(sale_like))
    placeholders_users = ",".join(["%s"] * len(user_ids)) if user_ids else "NULL"
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days-1)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            query = f"""
                SELECT DATE(CONVERT_TZ(created_at, '+00:00', '+00:00')) AS d, COUNT(*)
                FROM tg_events
                WHERE created_at >= %s AND created_at < %s + INTERVAL 1 DAY
                  AND LOWER(TRIM(COALESCE(status,''))) IN ({placeholders_status})
                  AND routed_user_id IN ({placeholders_users})
                GROUP BY d
                ORDER BY d ASC
            """
            params = [start, now, *sale_like, *user_ids] if user_ids else [start, now, *sale_like]
            await cur.execute(query, params)
            rows = await cur.fetchall()
            return [(str(r[0]), int(r[1])) for r in rows]

async def get_report_filter(user_id: int) -> Dict[str, Any]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT offer, creative, buyer_id, team_id FROM tg_report_filters WHERE user_id=%s", (user_id,))
            row = await cur.fetchone()
            return row or {"offer": None, "creative": None, "buyer_id": None, "team_id": None}

async def set_report_filter(user_id: int, offer: Optional[str], creative: Optional[str], buyer_id: Optional[int] = None, team_id: Optional[int] = None) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_report_filters(user_id, offer, creative, buyer_id, team_id)
                VALUES(%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE offer=VALUES(offer), creative=VALUES(creative), buyer_id=VALUES(buyer_id), team_id=VALUES(team_id)
                """,
                (user_id, offer, creative, buyer_id, team_id)
            )

async def clear_report_filter(user_id: int) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM tg_report_filters WHERE user_id=%s", (user_id,))

async def find_alias(alias: Optional[str]) -> Optional[Dict[str, Any]]:
    if not alias:
        return None
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT alias, buyer_id, lead_id FROM tg_aliases WHERE alias=%s", (alias.lower(),))
            return await cur.fetchone()

async def set_alias(alias: str, buyer_id: Optional[int] = None, lead_id: Optional[int] = None) -> None:
    pool = await init_pool()
    a = alias.lower()
    # Upsert logic: if row exists, update provided fields; else insert
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT buyer_id, lead_id FROM tg_aliases WHERE alias=%s", (a,))
            row = await cur.fetchone()
            if row:
                new_buyer = buyer_id if buyer_id is not None else row.get("buyer_id")
                new_lead = lead_id if lead_id is not None else row.get("lead_id")
                await cur.execute("UPDATE tg_aliases SET buyer_id=%s, lead_id=%s WHERE alias=%s", (new_buyer, new_lead, a))
            else:
                await cur.execute("INSERT INTO tg_aliases(alias, buyer_id, lead_id) VALUES(%s, %s, %s)", (a, buyer_id, lead_id))

async def list_aliases() -> List[Dict[str, Any]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT alias, buyer_id, lead_id FROM tg_aliases ORDER BY alias ASC")
            return await cur.fetchall()

async def list_offers_for_users(user_ids: List[int]) -> List[str]:
    if not user_ids:
        return []
    pool = await init_pool()
    placeholders = ",".join(["%s"] * len(user_ids))
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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
    pool = await init_pool()
    placeholders = ",".join(["%s"] * len(user_ids))
    offer_sql = ""
    params: List[Any] = [*user_ids]
    if offer:
        offer_sql = " AND (offer = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer_name')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer')) = %s)"
        params += [offer, offer, offer]
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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

async def set_ui_cache_list(user_id: int, kind: str, values: List[str]) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # clear old
            await cur.execute("DELETE FROM tg_ui_cache WHERE user_id=%s AND kind=%s", (user_id, kind))
            # insert new
            for i, val in enumerate(values):
                await cur.execute(
                    "INSERT INTO tg_ui_cache(user_id, kind, idx, value) VALUES(%s, %s, %s, %s)",
                    (user_id, kind, i, val)
                )

async def get_ui_cache_value(user_id: int, kind: str, idx: int) -> Optional[str]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT value FROM tg_ui_cache WHERE user_id=%s AND kind=%s AND idx=%s",
                (user_id, kind, idx)
            )
            row = await cur.fetchone()
            return str(row[0]) if row and row[0] is not None else None

async def delete_alias(alias: str) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM tg_aliases WHERE alias=%s", (alias.lower(),))

async def set_pending_action(admin_id: int, action: str, target_user_id: Optional[int]) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_pending_actions(admin_id, action, target_user_id)
                VALUES(%s, %s, %s)
                ON DUPLICATE KEY UPDATE action=VALUES(action), target_user_id=VALUES(target_user_id), created_at=CURRENT_TIMESTAMP
                """,
                (admin_id, action, target_user_id)
            )

async def get_pending_action(admin_id: int) -> Optional[Tuple[str, Optional[int]]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT action, target_user_id FROM tg_pending_actions WHERE admin_id=%s", (admin_id,))
            row = await cur.fetchone()
            if not row:
                return None
            return row["action"], row["target_user_id"]

async def clear_pending_action(admin_id: int) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM tg_pending_actions WHERE admin_id=%s", (admin_id,))

# --- Mentor helpers ---
async def add_mentor_team(mentor_id: int, team_id: int) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_mentor_teams(mentor_id, team_id)
                VALUES(%s, %s)
                ON DUPLICATE KEY UPDATE created_at=CURRENT_TIMESTAMP
                """,
                (mentor_id, team_id)
            )

async def remove_mentor_team(mentor_id: int, team_id: int) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM tg_mentor_teams WHERE mentor_id=%s AND team_id=%s", (mentor_id, team_id))

async def list_mentor_teams(mentor_id: int) -> List[int]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT team_id FROM tg_mentor_teams WHERE mentor_id=%s", (mentor_id,))
            rows = await cur.fetchall()
            return [int(r[0]) for r in rows]

async def list_team_mentors(team_id: int) -> List[int]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT mentor_id FROM tg_mentor_teams WHERE team_id=%s", (team_id,))
            rows = await cur.fetchall()
            return [int(r[0]) for r in rows]
