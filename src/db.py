import aiomysql
from typing import Optional, List, Dict, Any, Tuple, Iterable
from datetime import date, timedelta, datetime
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
    # extra team lead assignments (e.g., mentor acting as lead)
    """
    CREATE TABLE IF NOT EXISTS tg_team_leads_extra (
        team_id BIGINT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_tg_team_leads_extra_team FOREIGN KEY (team_id) REFERENCES tg_teams (id) ON DELETE CASCADE,
        CONSTRAINT fk_tg_team_leads_extra_user FOREIGN KEY (user_id) REFERENCES tg_users (telegram_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # cached Keitaro campaigns for domain lookups
    """
    CREATE TABLE IF NOT EXISTS keitaro_campaigns (
        id BIGINT PRIMARY KEY,
        name VARCHAR(512) NOT NULL,
        prefix VARCHAR(255) NULL,
        alias_key VARCHAR(255) NULL,
        source_domain VARCHAR(255) NULL,
        target_domain VARCHAR(255) NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_keitaro_campaign_source (source_domain),
        INDEX idx_keitaro_campaign_target (target_domain),
        INDEX idx_keitaro_campaign_alias (alias_key)
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


async def _ensure_fb_reference_data(conn: aiomysql.Connection) -> None:
    async with conn.cursor() as cur:
        try:
            await cur.execute("SELECT COUNT(*) FROM fb_statuses")
            row = await cur.fetchone()
            count_status = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            count_status = 0
        if count_status == 0:
            await cur.executemany(
                "INSERT INTO fb_statuses(code, title, description) VALUES(%s, %s, %s)",
                [
                    ("ACTIVE", "Active", "Кампания активна"),
                    ("TEST", "Test", "Кампания в тесте"),
                    ("DEAD", "Dead", "Кампания остановлена"),
                ],
            )
        try:
            await cur.execute("SELECT COUNT(*) FROM fb_flags")
            row = await cur.fetchone()
            count_flags = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            count_flags = 0
        if count_flags == 0:
            await cur.executemany(
                "INSERT INTO fb_flags(code, title, severity, description) VALUES(%s, %s, %s, %s)",
                [
                    ("GREEN", "Зелёный", 10, "Результат хороший"),
                    ("YELLOW", "Жёлтый", 50, "Требуется внимание"),
                    ("RED", "Красный", 90, "Проблемный результат"),
                ],
            )

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
                try:
                    await _ensure_fb_reference_data(conn)
                except Exception as e:
                    logger.warning(f"Failed to ensure default FB reference data: {e}")
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

async def set_team_lead_override(team_id: int, user_id: int) -> None:
    """Assign user as lead for team without changing primary role (mentor lead scenario)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_team_leads_extra(team_id, user_id)
                VALUES(%s, %s)
                ON DUPLICATE KEY UPDATE user_id=VALUES(user_id), created_at=CURRENT_TIMESTAMP
                """,
                (team_id, user_id)
            )

async def clear_team_lead_override(team_id: int) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM tg_team_leads_extra WHERE team_id=%s", (team_id,))

async def list_team_leads(team_id: int) -> List[int]:
    """Return Telegram IDs of active leads for the given team (role=lead or mentor overrides)."""
    pool = await init_pool()
    leads: List[int] = []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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
    pool = await init_pool()
    teams: List[int] = []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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

async def user_has_lead_privileges(user_id: int) -> bool:
    teams = await list_user_lead_teams(user_id)
    return bool(teams)

async def get_primary_lead_team(user_id: int) -> Optional[int]:
    teams = await list_user_lead_teams(user_id)
    return teams[0] if teams else None

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
    # cached Keitaro campaigns for domain lookups
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
    # Facebook CSV uploads metadata
    """
    CREATE TABLE IF NOT EXISTS fb_csv_uploads (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        uploaded_by BIGINT NOT NULL,
        buyer_id BIGINT NULL,
        original_filename VARCHAR(255) NOT NULL,
        period_start DATE NULL,
        period_end DATE NULL,
        row_count INT NOT NULL DEFAULT 0,
        has_totals TINYINT(1) NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_fb_csv_upload_user FOREIGN KEY (uploaded_by) REFERENCES tg_users (telegram_id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_csv_upload_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Raw rows from CSV uploads
    """
    CREATE TABLE IF NOT EXISTS fb_csv_rows (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        upload_id BIGINT NOT NULL,
        account_name VARCHAR(255) NOT NULL,
        campaign_name VARCHAR(255) NOT NULL,
        adset_name VARCHAR(255) NULL,
        ad_name VARCHAR(255) NULL,
        day_date DATE NULL,
        currency VARCHAR(16) NULL,
        spend DECIMAL(18,6) NULL,
        impressions BIGINT NULL,
        clicks BIGINT NULL,
        leads INT NULL,
        registrations INT NULL,
        cpc DECIMAL(18,6) NULL,
        ctr DECIMAL(18,6) NULL,
        is_total TINYINT(1) NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_fb_rows_upload (upload_id),
        INDEX idx_fb_rows_campaign_day (campaign_name, day_date),
        INDEX idx_fb_rows_account_day (account_name, day_date),
        CONSTRAINT fk_fb_rows_upload FOREIGN KEY (upload_id) REFERENCES fb_csv_uploads (id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Aggregated daily metrics per campaign (after enrichment)
    """
    CREATE TABLE IF NOT EXISTS fb_campaign_daily (
        campaign_name VARCHAR(255) NOT NULL,
        day_date DATE NOT NULL,
        account_name VARCHAR(255) NULL,
        buyer_id BIGINT NULL,
        geo VARCHAR(16) NULL,
        spend DECIMAL(18,6) NULL,
        impressions BIGINT NULL,
        clicks BIGINT NULL,
        registrations INT NULL,
        leads INT NULL,
        ftd INT NULL,
        revenue DECIMAL(18,6) NULL,
        ctr DECIMAL(18,6) NULL,
        cpc DECIMAL(18,6) NULL,
        roi DECIMAL(18,6) NULL,
        ftd_rate DECIMAL(18,6) NULL,
        status_id BIGINT NULL,
        flag_id BIGINT NULL,
        upload_id BIGINT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (campaign_name, day_date),
        INDEX idx_fb_daily_buyer_day (buyer_id, day_date),
        INDEX idx_fb_daily_account_day (account_name, day_date),
        CONSTRAINT fk_fb_daily_upload FOREIGN KEY (upload_id) REFERENCES fb_csv_uploads (id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_daily_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Totals per campaign/account for quick lookups
    """
    CREATE TABLE IF NOT EXISTS fb_campaign_totals (
        campaign_name VARCHAR(255) PRIMARY KEY,
        account_name VARCHAR(255) NULL,
        buyer_id BIGINT NULL,
        geo VARCHAR(16) NULL,
        spend DECIMAL(18,6) NULL,
        impressions BIGINT NULL,
        clicks BIGINT NULL,
        registrations INT NULL,
        leads INT NULL,
        ftd INT NULL,
        revenue DECIMAL(18,6) NULL,
        ctr DECIMAL(18,6) NULL,
        cpc DECIMAL(18,6) NULL,
        roi DECIMAL(18,6) NULL,
        ftd_rate DECIMAL(18,6) NULL,
        status_id BIGINT NULL,
        flag_id BIGINT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_fb_totals_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Accounts ownership history
    """
    CREATE TABLE IF NOT EXISTS fb_accounts (
        account_name VARCHAR(255) PRIMARY KEY,
        buyer_id BIGINT NULL,
        owner_since DATE NULL,
        owner_until DATE NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_fb_accounts_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Status dictionary
    """
    CREATE TABLE IF NOT EXISTS fb_statuses (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        code VARCHAR(32) NOT NULL UNIQUE,
        title VARCHAR(128) NOT NULL,
        description TEXT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Flags dictionary
    """
    CREATE TABLE IF NOT EXISTS fb_flags (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        code VARCHAR(32) NOT NULL UNIQUE,
        title VARCHAR(128) NOT NULL,
        severity INT NOT NULL DEFAULT 0,
        description TEXT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Current state per campaign (status, comments)
    """
    CREATE TABLE IF NOT EXISTS fb_campaign_state (
        campaign_name VARCHAR(255) PRIMARY KEY,
        status_id BIGINT NULL,
        flag_id BIGINT NULL,
        buyer_comment TEXT NULL,
        lead_comment TEXT NULL,
        updated_by BIGINT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_fb_state_status FOREIGN KEY (status_id) REFERENCES fb_statuses (id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_state_flag FOREIGN KEY (flag_id) REFERENCES fb_flags (id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_state_user FOREIGN KEY (updated_by) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # History of changes for audit/notifications
    """
    CREATE TABLE IF NOT EXISTS fb_campaign_history (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        campaign_name VARCHAR(255) NOT NULL,
        changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        changed_by BIGINT NULL,
        old_status_id BIGINT NULL,
        new_status_id BIGINT NULL,
        old_flag_id BIGINT NULL,
        new_flag_id BIGINT NULL,
        note TEXT NULL,
        CONSTRAINT fk_fb_hist_status_old FOREIGN KEY (old_status_id) REFERENCES fb_statuses (id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_hist_status_new FOREIGN KEY (new_status_id) REFERENCES fb_statuses (id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_hist_flag_old FOREIGN KEY (old_flag_id) REFERENCES fb_flags (id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_hist_flag_new FOREIGN KEY (new_flag_id) REFERENCES fb_flags (id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_hist_user FOREIGN KEY (changed_by) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
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


async def fetch_alias_map(aliases: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    names = [a.strip().lower() for a in aliases if a and a.strip()]
    if not names:
        return {}
    pool = await init_pool()
    placeholders = ",".join(["%s"] * len(names))
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"SELECT alias, buyer_id, lead_id FROM tg_aliases WHERE alias IN ({placeholders})",
                tuple(names),
            )
            rows = await cur.fetchall()
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        alias = (row.get("alias") or "").strip().lower()
        if not alias:
            continue
        result[alias] = row
    return result

async def replace_keitaro_campaigns(rows: List[Dict[str, Any]]) -> None:
    """Replace cached Keitaro campaigns with the provided collection."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await conn.begin()
            try:
                await cur.execute("DELETE FROM keitaro_campaigns")
                if rows:
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
                    await cur.executemany(
                        """
                        INSERT INTO keitaro_campaigns(id, name, prefix, alias_key, source_domain, target_domain, updated_at)
                        VALUES(%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        """,
                        payload
                    )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

async def find_campaigns_by_domain(domain: str) -> List[Dict[str, Any]]:
    if not domain:
        return []
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            value = domain.lower()
            await cur.execute(
                """
                SELECT id, name, prefix, alias_key, source_domain, target_domain, updated_at
                FROM keitaro_campaigns
                WHERE source_domain=%s OR target_domain=%s
                ORDER BY prefix IS NULL, prefix ASC, name ASC
                """,
                (value, value)
            )
            rows = await cur.fetchall()
            return rows or []

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


async def infer_campaign_buyers(identifiers: Iterable[str], lookback_days: int = 45) -> Dict[str, int]:
    names = {s.strip().lower() for s in identifiers if s and isinstance(s, str) and s.strip()}
    if not names:
        return {}
    pool = await init_pool()
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
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
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
                (start_ts, *names),
            )
            rows = await cur.fetchall()
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


# --- Facebook CSV uploads / analytics helpers ---

async def create_fb_csv_upload(
    uploaded_by: int,
    buyer_id: Optional[int],
    original_filename: str,
    period_start: Optional[date],
    period_end: Optional[date],
    row_count: int,
    has_totals: bool
) -> int:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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
    pool = await init_pool()
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
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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
    pool = await init_pool()
    payload = []
    for row in records:
        payload.append(
            (
                row.get("account_name"),
                row.get("buyer_id"),
                row.get("owner_since"),
            )
        )
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO fb_accounts(account_name, buyer_id, owner_since)
                VALUES(%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    buyer_id=VALUES(buyer_id),
                    owner_since=COALESCE(owner_since, VALUES(owner_since)),
                    owner_until=NULL,
                    updated_at=CURRENT_TIMESTAMP,
                    is_active=1
                """,
                payload,
            )


async def upsert_fb_campaign_daily(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    pool = await init_pool()
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
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO fb_campaign_daily(
                    campaign_name, day_date, account_name, buyer_id, geo,
                    spend, impressions, clicks, registrations, leads, ftd, revenue,
                    ctr, cpc, roi, ftd_rate, status_id, flag_id, upload_id
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    account_name=VALUES(account_name),
                    buyer_id=VALUES(buyer_id),
                    geo=VALUES(geo),
                    spend=VALUES(spend),
                    impressions=VALUES(impressions),
                    clicks=VALUES(clicks),
                    registrations=VALUES(registrations),
                    leads=VALUES(leads),
                    ftd=VALUES(ftd),
                    revenue=VALUES(revenue),
                    ctr=VALUES(ctr),
                    cpc=VALUES(cpc),
                    roi=VALUES(roi),
                    ftd_rate=VALUES(ftd_rate),
                    status_id=VALUES(status_id),
                    flag_id=VALUES(flag_id),
                    upload_id=VALUES(upload_id)
                """,
                payload,
            )


async def upsert_fb_campaign_totals(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    pool = await init_pool()
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
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO fb_campaign_totals(
                    campaign_name, account_name, buyer_id, geo, spend, impressions, clicks,
                    registrations, leads, ftd, revenue, ctr, cpc, roi, ftd_rate, status_id, flag_id
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    account_name=VALUES(account_name),
                    buyer_id=VALUES(buyer_id),
                    geo=VALUES(geo),
                    spend=VALUES(spend),
                    impressions=VALUES(impressions),
                    clicks=VALUES(clicks),
                    registrations=VALUES(registrations),
                    leads=VALUES(leads),
                    ftd=VALUES(ftd),
                    revenue=VALUES(revenue),
                    ctr=VALUES(ctr),
                    cpc=VALUES(cpc),
                    roi=VALUES(roi),
                    ftd_rate=VALUES(ftd_rate),
                    status_id=VALUES(status_id),
                    flag_id=VALUES(flag_id)
                """,
                payload,
            )


async def fetch_fb_campaign_state(campaign_names: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    names = [c for c in campaign_names if c]
    if not names:
        return {}
    pool = await init_pool()
    placeholders = ",".join(["%s"] * len(names))
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"SELECT campaign_name, status_id, flag_id, buyer_comment, lead_comment, updated_by, updated_at FROM fb_campaign_state WHERE campaign_name IN ({placeholders})",
                tuple(names),
            )
            rows = await cur.fetchall()
    return {str(row["campaign_name"]): row for row in rows}


async def upsert_fb_campaign_state(states: List[Dict[str, Any]]) -> None:
    if not states:
        return
    pool = await init_pool()
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
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO fb_campaign_state(campaign_name, status_id, flag_id, buyer_comment, lead_comment, updated_by)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status_id=VALUES(status_id),
                    flag_id=VALUES(flag_id),
                    buyer_comment=VALUES(buyer_comment),
                    lead_comment=VALUES(lead_comment),
                    updated_by=VALUES(updated_by)
                """,
                payload,
            )


async def log_fb_campaign_history(entries: List[Dict[str, Any]]) -> None:
    if not entries:
        return
    pool = await init_pool()
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
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO fb_campaign_history(
                    campaign_name, changed_by, old_status_id, new_status_id, old_flag_id, new_flag_id, note
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                """,
                payload,
            )


async def list_fb_statuses() -> List[Dict[str, Any]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id, code, title, description FROM fb_statuses ORDER BY id ASC")
            return await cur.fetchall()


async def list_fb_flags() -> List[Dict[str, Any]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id, code, title, severity, description FROM fb_flags ORDER BY severity DESC, id ASC")
            return await cur.fetchall()


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
    pool = await init_pool()
    placeholders_names = ",".join(["%s"] * len(names))
    sale_like = (
        "sale",
        "approved",
        "approve",
        "confirmed",
        "confirm",
        "purchase",
        "paid",
        "success",
        "ftd",
    )
    placeholders_status = ",".join(["%s"] * len(sale_like))
    query = f"""
        SELECT
            t.campaign_name,
            t.day_date,
            COUNT(*) AS ftd,
            SUM(COALESCE(t.payout, 0)) AS revenue
        FROM (
            SELECT
                DATE(created_at) AS day_date,
                COALESCE(
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub_id_2')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub2')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.sub_id2')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.campaign_name')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.campaign')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.campaignName')), ''),
                    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.subid')), '')
                ) AS campaign_name,
                LOWER(TRIM(COALESCE(status, ''))) AS status_norm,
                payout
            FROM tg_events
            WHERE created_at >= %s AND created_at < %s
        ) AS t
        WHERE t.campaign_name IS NOT NULL
          AND t.campaign_name IN ({placeholders_names})
          AND t.status_norm IN ({placeholders_status})
        GROUP BY t.campaign_name, t.day_date
    """
    params: List[Any] = [start, end + timedelta(days=1)]
    params.extend(names)
    params.extend(sale_like)
    daily: Dict[Tuple[str, date], Dict[str, Any]] = {}
    totals: Dict[str, Dict[str, Any]] = {}
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, tuple(params))
            rows = await cur.fetchall()
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


async def list_fb_available_months(limit: int = 12) -> List[date]:
    pool = await init_pool()
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
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (limit,))
            rows = await cur.fetchall()
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
    pool = await init_pool()
    sale_like = (
        "sale",
        "approved",
        "approve",
        "confirmed",
        "confirm",
        "purchase",
        "purchased",
        "paid",
        "success",
        "ftd",
    )
    placeholders_status = ",".join(["%s"] * len(sale_like))
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
    params.extend(sale_like)
    params.append(normalized)
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, tuple(params))
            rows = await cur.fetchall()
    return rows or []


async def fetch_fb_monthly_summary(limit: int = 12) -> List[Dict[str, Any]]:
    pool = await init_pool()
    sale_like = (
        "sale",
        "approved",
        "approve",
        "confirmed",
        "confirm",
        "purchase",
        "purchased",
        "paid",
        "success",
        "ftd",
    )
    placeholders_status = ",".join(["%s"] * len(sale_like))
    query = (
        f"""
        WITH monthly_fb AS (
            SELECT
                DATE_SUB(d.day_date, INTERVAL DAY(d.day_date) - 1 DAY) AS month_start,
                COUNT(DISTINCT d.campaign_name) AS campaign_count,
                COUNT(DISTINCT d.account_name) AS account_count,
                SUM(COALESCE(d.spend, 0)) AS spend,
                SUM(COALESCE(d.impressions, 0)) AS impressions,
                SUM(COALESCE(d.clicks, 0)) AS clicks,
                SUM(COALESCE(d.registrations, 0)) AS registrations
            FROM fb_campaign_daily d
            GROUP BY month_start
        ),
        monthly_conv AS (
            SELECT
                DATE_SUB(DATE(fc.conversion_time_utc), INTERVAL DAY(DATE(fc.conversion_time_utc)) - 1 DAY) AS month_start,
                COUNT(DISTINCT fc.sub_id_2) AS campaign_count,
                COUNT(*) AS ftd,
                SUM(COALESCE(fc.revenue, 0)) AS revenue
            FROM fact_conversions fc
            WHERE fc.sub_id_2 IS NOT NULL
              AND fc.sub_id_2 <> ''
              AND LOWER(fc.status) IN ({placeholders_status})
              AND EXISTS (
                    SELECT 1
                    FROM fb_campaign_daily d
                    WHERE d.campaign_name = fc.sub_id_2
                )
            GROUP BY month_start
        ),
        all_months AS (
            SELECT month_start FROM monthly_fb
            UNION
            SELECT month_start FROM monthly_conv
        )
        SELECT
            am.month_start,
            COALESCE(fb.campaign_count, conv.campaign_count, 0) AS campaign_count,
            COALESCE(fb.account_count, 0) AS account_count,
            COALESCE(fb.spend, 0) AS spend,
            COALESCE(conv.revenue, 0) AS revenue,
            COALESCE(conv.ftd, 0) AS ftd,
            COALESCE(fb.impressions, 0) AS impressions,
            COALESCE(fb.clicks, 0) AS clicks,
            COALESCE(fb.registrations, 0) AS registrations
        FROM all_months am
        LEFT JOIN monthly_fb fb ON fb.month_start = am.month_start
        LEFT JOIN monthly_conv conv ON conv.month_start = am.month_start
        ORDER BY am.month_start DESC
        LIMIT %s
        """
    )
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            params: List[Any] = list(sale_like)
            params.append(limit)
            await cur.execute(query, tuple(params))
            rows = await cur.fetchall()
    result: List[Dict[str, Any]] = []
    for row in rows or []:
        value = row.get("month_start")
        month_value: Optional[date] = None
        if isinstance(value, datetime):
            month_value = value.date().replace(day=1)
        elif isinstance(value, date):
            month_value = value.replace(day=1)
        elif isinstance(value, str):
            try:
                month_value = datetime.strptime(value, "%Y-%m-%d").date().replace(day=1)
            except ValueError:
                month_value = None
        if month_value is None:
            continue
        row["month_start"] = month_value
        result.append(row)
    return result


async def recompute_fb_campaign_totals(campaign_names: Iterable[str]) -> List[Dict[str, Any]]:
    names = [c.strip() for c in campaign_names if c and c.strip()]
    if not names:
        return []
    pool = await init_pool()
    placeholders = ",".join(["%s"] * len(names))
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
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
                tuple(names),
            )
            rows = await cur.fetchall()
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
