import asyncio
import aiomysql
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any, AsyncIterator, Tuple, Iterable, Sequence
from datetime import date, timedelta, datetime, timezone
from loguru import logger
from .config import secret, settings
from .constants import SALE_STATUSES, Role
from .utils.numbers import extract_decimal
import urllib.parse
import ssl
import json
import re
from decimal import Decimal

_pool: Optional[aiomysql.Pool] = None
_pool_lock = asyncio.Lock()

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
        role ENUM('buyer','lead','head','admin','mentor','helper') NOT NULL DEFAULT 'buyer',
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
    # helper -> buyer assignment (helper sees only that buyer's deposits)
    """
    CREATE TABLE IF NOT EXISTS tg_helper_buyer (
        helper_id BIGINT PRIMARY KEY,
        buyer_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_tg_helper_buyer_helper FOREIGN KEY (helper_id) REFERENCES tg_users (telegram_id) ON DELETE CASCADE,
        CONSTRAINT fk_tg_helper_buyer_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE CASCADE
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
    # design bot: chat_ids that /started the bot (receive design order notifications)
    """
    CREATE TABLE IF NOT EXISTS tg_design_bot_chats (
        chat_id BIGINT PRIMARY KEY,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # design: order_ids for which we already sent "task assigned" notification (avoid duplicate)
    """
    CREATE TABLE IF NOT EXISTS tg_design_assignment_sent (
        order_id BIGINT PRIMARY KEY,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # design: order_ids for which we already sent "task completed" notification (avoid duplicate)
    """
    CREATE TABLE IF NOT EXISTS tg_design_completion_sent (
        order_id BIGINT PRIMARY KEY,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # design: order_ids for which we already sent "SLA 24h exceeded" warning notification (avoid duplicate)
    """
    CREATE TABLE IF NOT EXISTS tg_design_sla_24h_alert_sent (
        order_id BIGINT PRIMARY KEY,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # design: order_ids for which we already sent "still not in progress after 48h" reminder (avoid duplicate)
    """
    CREATE TABLE IF NOT EXISTS tg_design_not_in_progress_48h_sent (
        order_id BIGINT PRIMARY KEY,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Underdog contractor_id -> telegram (designer): to notify the right person when task is assigned
    """
    CREATE TABLE IF NOT EXISTS tg_underdog_contractor_telegram (
        contractor_id VARCHAR(32) PRIMARY KEY,
        telegram_username VARCHAR(64) NULL,
        telegram_id BIGINT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Dedupe admin Telegram alerts (Orders bot etc.): max once per hour per key, then silence after 24h from first send
    """
    CREATE TABLE IF NOT EXISTS tg_admin_notify_throttle (
        dedupe_key VARCHAR(384) NOT NULL PRIMARY KEY,
        first_sent_at DATETIME NOT NULL,
        last_sent_at DATETIME NOT NULL,
        INDEX idx_admin_throttle_first (first_sent_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Keitaro may send the same sale S2S postback twice; fingerprint prevents double log + notify + daily count
    """
    CREATE TABLE IF NOT EXISTS tg_keitaro_sale_dedupe (
        dedupe_key CHAR(64) NOT NULL PRIMARY KEY,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Facebook CSV ingestion (fb_*), referenced by services/fb_uploads.py and reports
    """
    CREATE TABLE IF NOT EXISTS fb_csv_uploads (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        uploaded_by BIGINT NULL,
        buyer_id BIGINT NULL,
        original_filename VARCHAR(255) NOT NULL,
        period_start DATE NULL,
        period_end DATE NULL,
        row_count INT NOT NULL DEFAULT 0,
        has_totals TINYINT(1) NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_fb_csv_upload_user  FOREIGN KEY (uploaded_by) REFERENCES tg_users (telegram_id) ON DELETE SET NULL,
        CONSTRAINT fk_fb_csv_upload_buyer FOREIGN KEY (buyer_id)   REFERENCES tg_users (telegram_id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS fb_csv_rows (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        upload_id BIGINT NOT NULL,
        account_name VARCHAR(255) NULL,
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
    """
    CREATE TABLE IF NOT EXISTS fb_statuses (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        code VARCHAR(32) NOT NULL UNIQUE,
        title VARCHAR(128) NOT NULL,
        description TEXT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS fb_flags (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        code VARCHAR(32) NOT NULL UNIQUE,
        title VARCHAR(128) NOT NULL,
        severity INT NOT NULL DEFAULT 0,
        description TEXT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
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
    # Underdog items delivered to a chat: guards against re-sending when the telegram-sent PATCH failed
    """
    CREATE TABLE IF NOT EXISTS tg_underdog_sent (
        kind ENUM('order','domain','ip','ticket') NOT NULL,
        external_id VARCHAR(64) NOT NULL,
        chat_id BIGINT NOT NULL,
        sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (kind, external_id, chat_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    # Durable inbox for Keitaro postbacks: row is written before the 200, processed afterwards, retried on startup
    """
    CREATE TABLE IF NOT EXISTS tg_inbound_postbacks (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        fingerprint VARCHAR(64) NULL,
        raw JSON NOT NULL,
        status ENUM('pending','done','failed','duplicate') NOT NULL DEFAULT 'pending',
        attempts TINYINT NOT NULL DEFAULT 0,
        error TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        processed_at TIMESTAMP NULL,
        INDEX idx_inbound_status_created (status, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]

# (table, column, ADD COLUMN clause) — applied when SHOW COLUMNS lacks the column
_COLUMN_MIGRATIONS: List[Tuple[str, str, str]] = [
    ("tg_report_filters", "buyer_id", "buyer_id BIGINT NULL AFTER creative"),
    ("tg_report_filters", "team_id", "team_id BIGINT NULL AFTER buyer_id"),
    ("tg_events", "inbound_id", "inbound_id BIGINT NULL"),
    ("tg_users", "orders_opt_out", "orders_opt_out TINYINT(1) NOT NULL DEFAULT 0"),
    ("tg_keitaro_sale_dedupe", "inbound_id", "inbound_id BIGINT NULL"),
]
# (table, index name, ADD clause) — applied when SHOW INDEX lacks the index
_INDEX_MIGRATIONS: List[Tuple[str, str, str]] = [
    ("tg_events", "idx_events_clickid", "INDEX idx_events_clickid (clickid)"),
    ("tg_events", "idx_events_created", "INDEX idx_events_created (created_at)"),
    ("tg_events", "idx_events_routed_created", "INDEX idx_events_routed_created (routed_user_id, created_at)"),
    ("tg_events", "uq_events_inbound", "UNIQUE INDEX uq_events_inbound (inbound_id)"),
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

_ROW_ALIAS_INSERT_RE = re.compile(r"^(.*?\bVALUES\s*)(\([^()]*\))(\s+AS\s+new\b.*)$", re.S | re.I)


async def _executemany_rows(cur: aiomysql.Cursor, sql: str, rows: Iterable[Sequence[Any]], chunk: int = 500) -> None:
    """executemany for `INSERT ... VALUES (...) AS new ON DUPLICATE KEY UPDATE`: one multi-row statement per chunk.

    aiomysql batches only `VALUES (...) ON DUPLICATE`, the row-alias form would run row by row.
    """
    match = _ROW_ALIAS_INSERT_RE.match(sql)
    assert match, "expected INSERT ... VALUES (...) AS new ..."
    prefix, row_sql, suffix = match.groups()
    rows = [tuple(r) for r in rows]
    for i in range(0, len(rows), chunk):
        part = rows[i : i + chunk]
        await cur.execute(prefix + ",".join([row_sql] * len(part)) + suffix, [v for r in part for v in r])


@asynccontextmanager
async def _transaction() -> AsyncIterator[aiomysql.Cursor]:
    """Cursor inside BEGIN/COMMIT; ROLLBACK on any exception (pool connections are autocommit otherwise)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                yield cur
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise


async def _apply_schema(conn: aiomysql.Connection) -> None:
    async with conn.cursor() as cur:
        for i, stmt in enumerate(SCHEMA_SQL, start=1):
            try:
                await cur.execute(stmt)
            except Exception as e:
                logger.error("Schema DDL failed at statement {}: {}\nError: {}", i, stmt, e)
                raise
        # Ensure 'mentor'/'helper' exist in role enum (migration for existing installations)
        try:
            await cur.execute("SHOW COLUMNS FROM tg_users LIKE 'role'")
            col = await cur.fetchone()
            col_type = str(col[1]).lower() if col and len(col) > 1 else ""
            if "enum(" in col_type and ("mentor" not in col_type or "helper" not in col_type):
                logger.info("Altering tg_users.role to include 'mentor' and 'helper'")
                await cur.execute("ALTER TABLE tg_users MODIFY role ENUM('buyer','lead','head','admin','mentor','helper') NOT NULL DEFAULT 'buyer'")
        except Exception as e:
            logger.warning("Failed to ensure mentor/helper in role enum: {}", e)
        for table, column, clause in _COLUMN_MIGRATIONS:
            try:
                await cur.execute(f"SHOW COLUMNS FROM {table} LIKE %s", (column,))
                if not await cur.fetchone():
                    logger.info("Altering {}: ADD COLUMN {}", table, column)
                    await cur.execute(f"ALTER TABLE {table} ADD COLUMN {clause}")
            except Exception as e:
                logger.warning("Failed to add column {}.{}: {}", table, column, e)
        for table, index, clause in _INDEX_MIGRATIONS:
            try:
                await cur.execute(f"SHOW INDEX FROM {table} WHERE Key_name = %s", (index,))
                if not await cur.fetchone():
                    logger.info("Altering {}: ADD {}", table, index)
                    await cur.execute(f"ALTER TABLE {table} ADD {clause}")
            except Exception as e:
                logger.warning("Failed to add index {}.{}: {}", table, index, e)
    try:
        await _ensure_fb_reference_data(conn)
    except Exception as e:
        logger.warning("Failed to ensure default FB reference data: {}", e)


async def _create_pool() -> aiomysql.Pool:
    params = _parse_mysql_dsn(secret(settings.database_url))
    last_error: Optional[Exception] = None
    for attempt in range(1, 6):
        pool: Optional[aiomysql.Pool] = None
        try:
            # Session in UTC: TIMESTAMP columns are compared with datetime.now(timezone.utc) in Python
            pool = await aiomysql.create_pool(
                **params, minsize=1, maxsize=10, pool_recycle=3600, init_command="SET time_zone='+00:00'"
            )
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
            return pool
        except Exception as e:
            last_error = e
            if pool is not None:
                pool.close()
                await pool.wait_closed()
            logger.warning("MySQL connection attempt {}/5 failed: {}. Retrying in {}s...", attempt, e, attempt * 2)
            if attempt < 5:
                await asyncio.sleep(attempt * 2)
    assert last_error is not None
    raise last_error


async def init_pool() -> aiomysql.Pool:
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is not None:
            return _pool
        logger.info("Creating MySQL pool")
        pool = await _create_pool()
        try:
            async with pool.acquire() as conn:
                await _apply_schema(conn)
        except Exception:
            # Do not keep a pool with a half-applied schema: next call retries the DDL
            pool.close()
            await pool.wait_closed()
            raise
        _pool = pool
    return _pool

async def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None


_ADMIN_ALERT_MIN_INTERVAL = timedelta(hours=1)
_ADMIN_ALERT_MUTE_AFTER_FIRST = timedelta(hours=24)


def _utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _dt_as_utc_naive(dt: Any) -> datetime:
    if not isinstance(dt, datetime):
        return _utc_naive()
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


async def admin_notify_throttle_allow_send(dedupe_key: str) -> bool:
    """Отправить ли админский алерт в Telegram. Первый раз пишем в БД; далее не чаще раза в час; спустя 24 ч с первого — больше не слать (пока не clear)."""
    k = (dedupe_key or "none")[:384]
    now = _utc_naive().replace(microsecond=0)
    try:
        pool = await init_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # One atomic statement: rowcount 1 = new key, 2 = window passed (bumped), 0 = throttled
                await cur.execute(
                    """
                    INSERT INTO tg_admin_notify_throttle (dedupe_key, first_sent_at, last_sent_at)
                    VALUES (%s, %s, %s) AS new
                    ON DUPLICATE KEY UPDATE last_sent_at = IF(
                        tg_admin_notify_throttle.first_sent_at > new.last_sent_at - INTERVAL %s SECOND
                        AND tg_admin_notify_throttle.last_sent_at <= new.last_sent_at - INTERVAL %s SECOND,
                        new.last_sent_at,
                        tg_admin_notify_throttle.last_sent_at
                    )
                    """,
                    (
                        k, now, now,
                        int(_ADMIN_ALERT_MUTE_AFTER_FIRST.total_seconds()),
                        int(_ADMIN_ALERT_MIN_INTERVAL.total_seconds()),
                    ),
                )
                return int(cur.rowcount or 0) in (1, 2)
    except Exception as e:
        logger.warning("admin_notify_throttle_allow_send failed, allowing send", key=k, error=str(e))
        return True


async def admin_notify_throttle_clear(dedupe_key: str) -> None:
    """Сбросить троттлинг для ключа (например после успешной доставки заказа)."""
    k = (dedupe_key or "")[:384]
    if not k:
        return
    try:
        pool = await init_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM tg_admin_notify_throttle WHERE dedupe_key=%s", (k,))
    except Exception as e:
        logger.warning("admin_notify_throttle_clear failed", key=k, error=str(e))


async def underdog_sent_ids(kind: str, external_ids: List[str], chat_id: int) -> set:
    if not external_ids:
        return set()
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(external_ids))
            await cur.execute(
                f"SELECT external_id FROM tg_underdog_sent WHERE kind=%s AND chat_id=%s AND external_id IN ({placeholders})",
                (kind, chat_id, *external_ids),
            )
            return {str(r[0]) for r in await cur.fetchall()}


async def mark_underdog_sent(kind: str, external_ids: List[str], chat_id: int) -> None:
    if not external_ids:
        return
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT IGNORE INTO tg_underdog_sent (kind, external_id, chat_id) VALUES (%s, %s, %s)",
                [(kind, eid, chat_id) for eid in external_ids],
            )


async def add_design_bot_subscriber(chat_id: int) -> None:
    """Register a chat as design bot subscriber (on /start)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO tg_design_bot_chats (chat_id) VALUES (%s)",
                (chat_id,),
            )


async def list_design_bot_subscribers() -> List[int]:
    """Return all chat_ids subscribed to design bot."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT chat_id FROM tg_design_bot_chats ORDER BY created_at ASC")
            rows = await cur.fetchall()
            return [int(r[0]) for r in rows] if rows else []


async def is_design_assignment_sent(order_id: int) -> bool:
    """True if we already sent 'task assigned' notification for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM tg_design_assignment_sent WHERE order_id = %s", (order_id,))
            return (await cur.fetchone()) is not None


async def mark_design_assignment_sent(order_id: int) -> None:
    """Mark that we sent 'task assigned' notification for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO tg_design_assignment_sent (order_id) VALUES (%s)",
                (order_id,),
            )


async def is_design_completion_sent(order_id: int) -> bool:
    """True if we already sent 'task completed' notification for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM tg_design_completion_sent WHERE order_id = %s", (order_id,))
            return (await cur.fetchone()) is not None


async def mark_design_completion_sent(order_id: int) -> None:
    """Mark that we sent 'task completed' notification for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO tg_design_completion_sent (order_id) VALUES (%s)",
                (order_id,),
            )


async def get_design_assignment_sent_at(order_id: int) -> Optional[datetime]:
    """Return UTC datetime when we first sent 'task assigned' for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT created_at FROM tg_design_assignment_sent WHERE order_id = %s",
                (order_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return row[0]


async def list_design_assignments_pending_take_in_progress_reminder(
    reminder_hours: int,
) -> List[Dict[str, Any]]:
    """Assignments older than reminder_hours without a take-in-progress reminder yet."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT a.order_id, a.created_at
                FROM tg_design_assignment_sent a
                LEFT JOIN tg_design_not_in_progress_48h_sent r ON r.order_id = a.order_id
                WHERE r.order_id IS NULL
                  AND a.created_at <= (UTC_TIMESTAMP() - INTERVAL %s HOUR)
                ORDER BY a.created_at ASC
                """,
                (int(reminder_hours),),
            )
            return await cur.fetchall() or []


async def find_telegram_id_among_subscribers_by_username(
    username: Optional[str],
    subscriber_ids: Iterable[int],
) -> Optional[int]:
    """Match Underdog @username to a DesignBot subscriber telegram_id."""
    if not username:
        return None
    ids = [int(x) for x in subscriber_ids if x]
    if not ids:
        return None
    handle = username.strip().lstrip("@").lower()
    pool = await init_pool()
    placeholders = ",".join(["%s"] * len(ids))
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"""
                SELECT telegram_id
                FROM tg_users
                WHERE is_active = 1
                  AND LOWER(username) = %s
                  AND telegram_id IN ({placeholders})
                LIMIT 1
                """,
                (handle, *ids),
            )
            row = await cur.fetchone()
            if row and row.get("telegram_id") is not None:
                return int(row["telegram_id"])
    return None


async def is_design_sla_24h_alert_sent(order_id: int) -> bool:
    """True if we already sent 'SLA 24h exceeded' warning notification for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM tg_design_sla_24h_alert_sent WHERE order_id = %s",
                (order_id,),
            )
            return (await cur.fetchone()) is not None


async def mark_design_sla_24h_alert_sent(order_id: int) -> None:
    """Mark that we sent 'SLA 24h exceeded' warning notification for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO tg_design_sla_24h_alert_sent (order_id) VALUES (%s)",
                (order_id,),
            )


async def is_design_not_in_progress_48h_sent(order_id: int) -> bool:
    """True if we already sent 'not in progress after 48h' reminder for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM tg_design_not_in_progress_48h_sent WHERE order_id = %s",
                (order_id,),
            )
            return (await cur.fetchone()) is not None


async def mark_design_not_in_progress_48h_sent(order_id: int) -> None:
    """Mark that we sent 'not in progress after 48h' reminder for this order."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO tg_design_not_in_progress_48h_sent (order_id) VALUES (%s)",
                (order_id,),
            )


async def get_contractor_telegram_id(contractor_id: str) -> Optional[int]:
    """Resolve Underdog contractor_id to telegram_id (from tg_underdog_contractor_telegram or tg_users by username)."""
    if not contractor_id:
        return None
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT telegram_id, telegram_username FROM tg_underdog_contractor_telegram WHERE contractor_id = %s",
                (str(contractor_id).strip(),),
            )
            row = await cur.fetchone()
    # find_user_by_username takes its own connection — call it after releasing ours
    if row and row.get("telegram_id"):
        return int(row["telegram_id"])
    username = (row or {}).get("telegram_username")
    if username:
        user = await find_user_by_username(str(username).strip().lstrip("@").lower())
        if user:
            return int(user["telegram_id"])
    return None


async def upsert_user(telegram_id: int, username: Optional[str], full_name: Optional[str]) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_users(telegram_id, username, full_name)
                VALUES(%s, %s, %s) AS new
                ON DUPLICATE KEY UPDATE
                    username = COALESCE(NULLIF(new.username, ''), tg_users.username),
                    full_name = COALESCE(NULLIF(new.full_name, ''), tg_users.full_name)
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
    assert role in ("buyer", "lead", "head", "admin", "mentor", "helper")
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE tg_users SET role=%s WHERE telegram_id=%s", (role, telegram_id))

async def set_orders_opt_out(telegram_id: int, opt_out: bool) -> None:
    """Orders-bot unsubscribe (/unsubscribe, cleared by /start in orders bot); main bot is unaffected."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE tg_users SET orders_opt_out=%s WHERE telegram_id=%s", (1 if opt_out else 0, telegram_id))


async def set_user_active(telegram_id: int, is_active: bool) -> None:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE tg_users SET is_active=%s WHERE telegram_id=%s", (1 if is_active else 0, telegram_id))


async def get_helper_buyer(helper_id: int) -> Optional[int]:
    """Возвращает buyer_id, к которому привязан помощник, или None."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT buyer_id FROM tg_helper_buyer WHERE helper_id=%s", (helper_id,))
            row = await cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None


async def set_helper_buyer(helper_id: int, buyer_id: int) -> None:
    """Привязывает помощника к байеру (один помощник — один байер)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_helper_buyer (helper_id, buyer_id) VALUES (%s, %s) AS new
                ON DUPLICATE KEY UPDATE buyer_id = new.buyer_id
                """,
                (helper_id, buyer_id),
            )


async def remove_helper_and_promote_to_buyer(helper_id: int) -> None:
    """
    Удаляет помощника как helper:
    - снимает привязку к buyer
    - переводит роль в buyer
    """
    async with _transaction() as cur:
        await cur.execute("DELETE FROM tg_helper_buyer WHERE helper_id=%s", (helper_id,))
        await cur.execute("UPDATE tg_users SET role='buyer' WHERE telegram_id=%s", (helper_id,))


async def deactivate_user(telegram_id: int) -> None:
    """
    Мягкое удаление пользователя из бота:
    - is_active=0
    - удаляем helper-привязки (как helper и как buyer)
    """
    async with _transaction() as cur:
        await cur.execute("UPDATE tg_users SET is_active=0 WHERE telegram_id=%s", (telegram_id,))
        await cur.execute("DELETE FROM tg_helper_buyer WHERE helper_id=%s OR buyer_id=%s", (telegram_id, telegram_id))


async def list_helpers_by_buyer(buyer_id: int) -> List[int]:
    """Список telegram_id помощников, привязанных к данному байеру (для уведомлений о депозитах)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT helper_id FROM tg_helper_buyer WHERE buyer_id = %s",
                (buyer_id,),
            )
            rows = await cur.fetchall()
            return [int(r[0]) for r in rows] if rows else []


async def list_helpers_with_buyers() -> List[Dict[str, Any]]:
    """Список помощников (role=helper) с привязкой к байеру (для админки)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT hu.telegram_id AS helper_id, hu.username AS helper_username, hu.full_name AS helper_name,
                       h.buyer_id, bu.username AS buyer_username, bu.full_name AS buyer_name, h.created_at
                FROM tg_users hu
                LEFT JOIN tg_helper_buyer h ON h.helper_id = hu.telegram_id
                LEFT JOIN tg_users bu ON bu.telegram_id = h.buyer_id
                WHERE hu.role = 'helper'
                ORDER BY hu.telegram_id DESC
                """
            )
            return await cur.fetchall() or []


async def list_users_as_buyer_candidates() -> List[Dict[str, Any]]:
    """Пользователи, которых можно назначить байером для помощника (buyer, lead, mentor)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT telegram_id, username, full_name, role, team_id, is_active
                FROM tg_users
                WHERE is_active = 1 AND role IN ('buyer', 'lead', 'mentor', 'head')
                ORDER BY full_name, username
                """
            )
            return await cur.fetchall() or []


async def fetch_users_by_usernames(usernames: Iterable[str], *, orders_recipients: bool = False) -> Dict[str, Dict[str, Any]]:
    """orders_recipients=True — для рассылок orders-бота: без отписавшихся (/unsubscribe)."""
    opt_out_sql = "AND orders_opt_out=0 " if orders_recipients else ""
    normalized = []
    for raw in usernames:
        if not raw:
            continue
        handle = raw.strip().lstrip("@").lower()
        if handle:
            normalized.append(handle)
    if not normalized:
        return {}
    pool = await init_pool()
    placeholders = ",".join(["%s"] * len(normalized))
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"SELECT telegram_id, username, full_name FROM tg_users WHERE is_active=1 {opt_out_sql}AND LOWER(username) IN ({placeholders})",
                tuple(normalized),
            )
            rows = await cur.fetchall()
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        username = (row.get("username") or "").strip().lstrip("@").lower()
        if username:
            result[username] = row
    return result


async def find_user_by_username(username: Optional[str]) -> Optional[Dict[str, Any]]:
    if not username:
        return None
    users = await fetch_users_by_usernames([username])
    key = username.strip().lstrip("@").lower()
    return users.get(key)


async def list_teams() -> List[Dict[str, Any]]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id, name, created_at FROM tg_teams ORDER BY id DESC")
            return await cur.fetchall()

def _norm_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@").lower()


async def sync_employee_directory(employees: List[Any]) -> Dict[str, int]:
    """Persist one trusted employee-directory snapshot atomically.

    A record without a Telegram ID is matched only to an existing local username;
    this prevents creating unusable bot users from an ambiguous directory entry.
    """
    pool = await init_pool()
    stats = {"received": len(employees), "matched": 0, "skipped": 0, "users_updated": 0,
             "teams_created": 0, "teams_deleted": 0, "helper_links_updated": 0,
             "observer_links_updated": 0, "aliases_upserted": 0}
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            try:
                await cur.execute("SELECT telegram_id, username FROM tg_users")
                rows = await cur.fetchall() or []
                username_to_id: Dict[str, int] = {}
                ambiguous_usernames: set[str] = set()
                for row in rows:
                    key = _norm_username(row.get("username"))
                    if not key:
                        continue
                    other = username_to_id.get(key)
                    if other is not None and other != int(row["telegram_id"]):
                        logger.warning(
                            "Duplicate username in tg_users, not matching it: @{} -> {} and {}",
                            key, other, int(row["telegram_id"]),
                        )
                        ambiguous_usernames.add(key)
                    username_to_id[key] = int(row["telegram_id"])
                for key in ambiguous_usernames:
                    username_to_id.pop(key, None)
                known_ids = {int(row["telegram_id"]) for row in rows}
                await cur.execute("SELECT id, name FROM tg_teams")
                team_rows = await cur.fetchall() or []
                teams = {str(row["name"]).strip().lower(): int(row["id"]) for row in team_rows}

                # The directory is authoritative for teams. Disabled employees do not keep a
                # historic team alive in the bot's active team list.
                authoritative_team_names: set[str] = set()
                for person in employees:
                    if not getattr(person, "is_active", True):
                        continue
                    for raw_name in (getattr(person, "team_name", None), *tuple(
                        getattr(person, "observer_team_names", ()) or ()
                    )):
                        if not raw_name or str(raw_name).strip() == "-":
                            continue
                        authoritative_team_names.add(str(raw_name).strip().lower())
                stale_team_ids = [
                    int(row["id"]) for row in team_rows
                    if str(row["name"]).strip().lower() not in authoritative_team_names
                ]
                if stale_team_ids:
                    placeholders = ",".join(["%s"] * len(stale_team_ids))
                    await cur.execute(
                        f"UPDATE tg_users SET team_id=NULL WHERE team_id IN ({placeholders})",
                        tuple(stale_team_ids),
                    )
                    await cur.execute(
                        f"DELETE FROM tg_teams WHERE id IN ({placeholders})",
                        tuple(stale_team_ids),
                    )
                    stats["teams_deleted"] = len(stale_team_ids)
                    teams = {key: value for key, value in teams.items() if value not in stale_team_ids}

                def resolve(person: Any) -> Optional[int]:
                    uid = getattr(person, "telegram_id", None)
                    if uid:
                        return int(uid)
                    return username_to_id.get(_norm_username(getattr(person, "username", None)))

                resolved: List[Tuple[Any, int]] = []
                external_id_to_telegram_id: Dict[str, int] = {}
                for person in employees:
                    uid = resolve(person)
                    if uid is None:
                        stats["skipped"] += 1
                        continue
                    stats["matched"] += 1
                    if uid not in known_ids:
                        await cur.execute(
                            "INSERT INTO tg_users(telegram_id, username, full_name) VALUES(%s, %s, %s)",
                            (uid, getattr(person, "username", None), getattr(person, "full_name", None)),
                        )
                        known_ids.add(uid)
                        username = _norm_username(getattr(person, "username", None))
                        if username and username not in ambiguous_usernames:
                            username_to_id[username] = uid
                    resolved.append((person, uid))
                    external_id = getattr(person, "external_id", None)
                    if external_id:
                        external_id_to_telegram_id[str(external_id)] = uid

                for person, uid in resolved:
                    team_id = None
                    team_name = getattr(person, "team_name", None)
                    if team_name and getattr(person, "is_active", True) and str(team_name).strip() != "-":
                        key = str(team_name).strip().lower()
                        team_id = teams.get(key)
                        if team_id is None:
                            await cur.execute("INSERT INTO tg_teams(name) VALUES(%s)", (team_name,))
                            team_id = int(cur.lastrowid)
                            teams[key] = team_id
                            stats["teams_created"] += 1
                    await cur.execute(
                        "UPDATE tg_users SET username=COALESCE(NULLIF(%s, ''), username), "
                        "full_name=COALESCE(NULLIF(%s, ''), full_name), "
                        "role=COALESCE(%s, role), team_id=%s WHERE telegram_id=%s",
                        (getattr(person, "username", None), getattr(person, "full_name", None),
                         getattr(person, "role", None), team_id, uid),
                    )
                    await cur.execute(
                        "UPDATE tg_users SET is_active=%s WHERE telegram_id=%s",
                        (1 if getattr(person, "is_active", True) else 0, uid),
                    )
                    stats["users_updated"] += 1

                for person, helper_id in resolved:
                    if getattr(person, "role", None) != "helper":
                        continue
                    buyer_id = getattr(person, "helper_for_telegram_id", None)
                    if buyer_id is None:
                        external_id = getattr(person, "helper_for_external_id", None)
                        buyer_id = external_id_to_telegram_id.get(str(external_id)) if external_id else None
                    if buyer_id is None:
                        buyer_id = username_to_id.get(_norm_username(getattr(person, "helper_for_username", None)))
                    if buyer_id and int(buyer_id) in known_ids:
                        await cur.execute(
                            "INSERT INTO tg_helper_buyer(helper_id, buyer_id) VALUES(%s, %s) AS new "
                            "ON DUPLICATE KEY UPDATE buyer_id=new.buyer_id",
                            (helper_id, int(buyer_id)),
                        )
                        stats["helper_links_updated"] += 1

                # Admin isObserver memberships become extra leads so those users
                # receive team deposits and see those teams in reports.
                observer_user_ids: set[int] = set()
                desired_extra: dict[int, int] = {}
                for person, uid in resolved:
                    if not getattr(person, "is_active", True):
                        continue
                    for team_name in getattr(person, "observer_team_names", ()) or ():
                        key = str(team_name).strip().lower()
                        if not key or key == "-":
                            continue
                        team_id = teams.get(key)
                        if team_id is None:
                            await cur.execute("INSERT INTO tg_teams(name) VALUES(%s)", (team_name,))
                            team_id = int(cur.lastrowid)
                            teams[key] = team_id
                            stats["teams_created"] += 1
                        existing = desired_extra.get(team_id)
                        if existing is not None and existing != uid:
                            logger.warning(
                                "Multiple observers for team; keeping first extra lead",
                                team_id=team_id,
                                kept_user_id=existing,
                                skipped_user_id=uid,
                            )
                            continue
                        desired_extra[team_id] = uid
                        observer_user_ids.add(uid)

                if observer_user_ids:
                    placeholders = ",".join(["%s"] * len(observer_user_ids))
                    await cur.execute(
                        f"SELECT team_id, user_id FROM tg_team_leads_extra WHERE user_id IN ({placeholders})",
                        tuple(observer_user_ids),
                    )
                    for row in await cur.fetchall() or []:
                        team_id = int(row["team_id"])
                        user_id = int(row["user_id"])
                        if desired_extra.get(team_id) != user_id:
                            await cur.execute(
                                "DELETE FROM tg_team_leads_extra WHERE team_id=%s AND user_id=%s",
                                (team_id, user_id),
                            )

                # Directory users without observer teams lose stale extra-lead rows. Mentors keep theirs:
                # cb_set_role(mentor) writes tg_team_leads_extra manually.
                former_observer_ids = sorted(
                    uid for person, uid in resolved
                    if uid not in observer_user_ids and getattr(person, "role", None) != Role.MENTOR
                )
                if former_observer_ids:
                    placeholders = ",".join(["%s"] * len(former_observer_ids))
                    await cur.execute(
                        f"DELETE FROM tg_team_leads_extra WHERE user_id IN ({placeholders})",
                        tuple(former_observer_ids),
                    )
                    if cur.rowcount:
                        logger.info("Removed {} stale extra-lead rows (no observer teams in Admin)", cur.rowcount)

                for team_id, user_id in desired_extra.items():
                    await cur.execute(
                        """
                        INSERT INTO tg_team_leads_extra(team_id, user_id)
                        VALUES(%s, %s) AS new
                        ON DUPLICATE KEY UPDATE user_id=new.user_id, created_at=CURRENT_TIMESTAMP
                        """,
                        (team_id, user_id),
                    )
                    stats["observer_links_updated"] += 1

                # Admin «Имя в Keitaro» is the campaign prefix used for deposit routing.
                seen_aliases: Dict[str, int] = {}
                for person, uid in resolved:
                    if not getattr(person, "is_active", True):
                        continue
                    alias = str(getattr(person, "keitaro_name", None) or "").strip().lower()
                    if not alias:
                        continue
                    existing_owner = seen_aliases.get(alias)
                    if existing_owner is not None and existing_owner != uid:
                        logger.warning(
                            "Duplicate Admin keitaroName; keeping first alias owner",
                            alias=alias,
                            kept_user_id=existing_owner,
                            skipped_user_id=uid,
                        )
                        continue
                    seen_aliases[alias] = uid
                    await cur.execute("SELECT buyer_id FROM tg_aliases WHERE alias=%s", (alias,))
                    previous = await cur.fetchone()
                    previous_buyer = previous.get("buyer_id") if previous else None
                    if previous_buyer is not None and int(previous_buyer) != uid:
                        logger.warning(
                            "Admin keitaroName reassigns existing alias",
                            alias=alias,
                            previous_buyer_id=int(previous_buyer),
                            new_buyer_id=uid,
                        )
                    await cur.execute(
                        """
                        INSERT INTO tg_aliases(alias, buyer_id, lead_id)
                        VALUES(%s, %s, NULL) AS new
                        ON DUPLICATE KEY UPDATE buyer_id=new.buyer_id
                        """,
                        (alias, uid),
                    )
                    stats["aliases_upserted"] += 1
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
    return stats

async def set_team_lead_override(team_id: int, user_id: int) -> None:
    """Assign user as lead for team without changing primary role (mentor lead scenario)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO tg_team_leads_extra(team_id, user_id)
                VALUES(%s, %s) AS new
                ON DUPLICATE KEY UPDATE user_id=new.user_id, created_at=CURRENT_TIMESTAMP
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

async def claim_keitaro_sale_postback(
    fingerprint: str,
    *,
    click_id: Optional[str] = None,
    inbound_id: Optional[int] = None,
) -> bool:
    """Atomically claim a sale and reject retries of click IDs saved before this key format.

    A claim made by the same tg_inbound_postbacks row (retry after a crash) stays ours.
    """
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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
    pool = await init_pool()
    payout = _first_present(
        raw, "payout", "revenue", "conversion_revenue", "profit", "conversion_profit", "conversion_cost"
    )
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
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
                ),
            )


async def enqueue_inbound_postback(raw: Dict[str, Any], fingerprint: Optional[str]) -> int:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO tg_inbound_postbacks (fingerprint, raw) VALUES (%s, %s)",
                (fingerprint, json.dumps(raw, ensure_ascii=False)),
            )
            return int(cur.lastrowid)


async def claim_inbound_postback(inbound_id: int) -> Optional[Dict[str, Any]]:
    """Take a pending/failed row for processing (attempts += 1). None if already done/duplicate or missing."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE tg_inbound_postbacks SET status=%s, error=%s, processed_at=UTC_TIMESTAMP() WHERE id=%s",
                (status, (error or None) and error[:4000], inbound_id),
            )


async def list_inbound_postbacks_for_retry(max_attempts: int = 3, min_age_seconds: int = 60) -> List[int]:
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
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
    pool = await init_pool()
    start, end = _today_utc_window()
    placeholders = ",".join(["%s"] * len(SALE_STATUSES))
    query = f"SELECT {select_expr} FROM tg_events" + _USER_SALES_TODAY_WHERE.format(placeholders=placeholders)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (user_id, user_id, start, end, *SALE_STATUSES))
            row = await cur.fetchone()
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
    pool = await init_pool()
    start, end = _today_utc_window()
    placeholders = ",".join(["%s"] * len(SALE_STATUSES))
    query = f"""
        SELECT COUNT(*), COALESCE(SUM(payout), 0)
        FROM tg_events
        WHERE {_EVENT_ALIAS_EXPR} = %s
          AND created_at >= %s AND created_at < %s
          AND LOWER(TRIM(COALESCE(status, ''))) IN ({placeholders})
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (normalized, start, end, *SALE_STATUSES))
            row = await cur.fetchone()
    if not row:
        return 0, 0.0
    return int(row[0] or 0), float(row[1] or 0)


async def sales_by_user_between(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Per-buyer deposit count and payout sum for sale-like events in [start, end) (UTC).

    Unrouted events (routed_user_id IS NULL) are returned under user_id=None so callers
    can decide whether to show them.
    """
    pool = await init_pool()
    placeholders = ",".join(["%s"] * len(SALE_STATUSES))
    query = f"""
        SELECT routed_user_id, COUNT(*), COALESCE(SUM(payout), 0)
        FROM tg_events
        WHERE created_at >= %s AND created_at < %s
          AND LOWER(TRIM(COALESCE(status, ''))) IN ({placeholders})
        GROUP BY routed_user_id
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (start, end, *SALE_STATUSES))
            rows = await cur.fetchall()
    return [
        {
            "user_id": int(r[0]) if r[0] is not None else None,
            "count": int(r[1] or 0),
            "revenue": float(r[2] or 0),
        }
        for r in rows
    ]


async def list_extra_lead_teams(user_id: int) -> List[int]:
    """Teams the user receives deposits for via tg_team_leads_extra (Admin observers)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT team_id FROM tg_team_leads_extra WHERE user_id=%s", (user_id,))
            rows = await cur.fetchall()
            return [int(r[0]) for r in rows if r and r[0] is not None]


async def list_alias_lead_buyers(lead_id: int) -> List[int]:
    """Buyers whose alias names this user as lead (tg_aliases.lead_id)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT DISTINCT buyer_id FROM tg_aliases WHERE lead_id=%s AND buyer_id IS NOT NULL",
                (lead_id,),
            )
            rows = await cur.fetchall()
            return [int(r[0]) for r in rows if r and r[0] is not None]

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
                VALUES(%s, %s, %s) AS new
                ON DUPLICATE KEY UPDATE
                    daily_goal=new.daily_goal,
                    weekly_goal=new.weekly_goal
                """,
                (user_id, daily_goal, weekly_goal)
            )

async def aggregate_sales(user_ids: List[int], start, end, offer: Optional[str] = None, creative: Optional[str] = None, filter_user_ids: Optional[List[int]] = None) -> Dict[str, Any]:
    """
    Return dict with keys: count, profit, top_offer, geo_dist, creative_dist, buyer_dist, offer_dist, total.
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
    pool = await init_pool()
    placeholders_status = ",".join(["%s"] * len(SALE_STATUSES))
    # If filter_user_ids provided, intersect with user_ids
    if filter_user_ids is not None:
        base_set = set(user_ids)
        user_ids = [uid for uid in filter_user_ids if uid in base_set]
    placeholders_users = ",".join(["%s"] * len(user_ids)) if user_ids else "NULL"
    offer_filter_sql = ""
    creative_filter_sql = ""
    params: list[Any] = [start, end, *SALE_STATUSES, *user_ids]
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
    # offer distribution (counts per offer) for detailed buyer reports
    offer_dist_sql = f"""
            SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw, '$.offer_name')), offer) AS offer_name, COUNT(*) AS cnt
            FROM tg_events
            WHERE created_at >= %s AND created_at < %s
                AND LOWER(TRIM(COALESCE(status,''))) IN ({placeholders_status})
                AND routed_user_id IN ({placeholders_users})
                {offer_filter_sql}
                {creative_filter_sql}
            GROUP BY offer_name
            ORDER BY cnt DESC, offer_name ASC
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
            # offer distribution
            await cur.execute(offer_dist_sql, params)
            off_rows = await cur.fetchall()
            offer_dist = {
                (str(r[0]).strip() if r[0] is not None and str(r[0]).strip() else "(пусто)"): int(r[1])
                for r in off_rows
            }
    return {
        "count": count,
        "profit": profit,
        "top_offer": top_offer,
        "top_offer_count": top_offer_count,
        "geo_dist": geo_dist,
        "creative_dist": creative_dist,
        "buyer_dist": buyer_dist,
        "offer_dist": offer_dist,
        "total": total,
    }

async def trend_daily_sales(user_ids: List[int], days: int = 7) -> List[Tuple[str, int]]:
    """Return list of (YYYY-MM-DD, count) for last N days (UTC)."""
    from datetime import datetime, timezone, timedelta
    pool = await init_pool()
    placeholders_status = ",".join(["%s"] * len(SALE_STATUSES))
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
            params = [start, now, *SALE_STATUSES, *user_ids] if user_ids else [start, now, *SALE_STATUSES]
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
                VALUES(%s, %s, %s, %s, %s) AS new
                ON DUPLICATE KEY UPDATE offer=new.offer, creative=new.creative, buyer_id=new.buyer_id, team_id=new.team_id
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

_UNSET = object()

async def set_alias(alias: str, buyer_id: Any = _UNSET, lead_id: Any = _UNSET) -> None:
    pool = await init_pool()
    a = (alias or "").strip().lower()
    if not a:
        return
    # Atomic upsert: on existing alias only the provided fields change
    updates = [f"{col} = new.{col}" for col, val in (("buyer_id", buyer_id), ("lead_id", lead_id)) if val is not _UNSET]
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO tg_aliases(alias, buyer_id, lead_id) VALUES(%s, %s, %s) AS new "
                "ON DUPLICATE KEY UPDATE " + (", ".join(updates) or "alias = tg_aliases.alias"),
                (a, None if buyer_id is _UNSET else buyer_id, None if lead_id is _UNSET else lead_id),
            )

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
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await conn.begin()
            try:
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
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
    return len(payload)


async def find_campaigns_by_domain(domain: str) -> List[Dict[str, Any]]:
    if not domain:
        return []
    from .keitaro import campaign_row_matches_domain, normalize_domain

    value = normalize_domain(domain) or domain.strip().lower()
    if not value:
        return []
    like_host = f"%.{value}"
    like_name = f"%{value}%"
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, name, prefix, alias_key, source_domain, target_domain, updated_at
                FROM keitaro_campaigns
                WHERE source_domain=%s OR target_domain=%s
                   OR source_domain LIKE %s OR target_domain LIKE %s
                   OR name LIKE %s
                ORDER BY prefix IS NULL, prefix ASC, name ASC
                """,
                (value, value, like_host, like_host, like_name),
            )
            rows = await cur.fetchall()
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
    async with _transaction() as cur:
        await cur.execute("DELETE FROM tg_ui_cache WHERE user_id=%s AND kind=%s", (user_id, kind))
        if values:
            await cur.executemany(
                "INSERT INTO tg_ui_cache(user_id, kind, idx, value) VALUES(%s, %s, %s, %s)",
                [(user_id, kind, i, val) for i, val in enumerate(values)],
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
                VALUES(%s, %s, %s) AS new
                ON DUPLICATE KEY UPDATE action=new.action, target_user_id=new.target_user_id, created_at=CURRENT_TIMESTAMP
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
    end_exclusive = end + timedelta(days=1)
    pool = await init_pool()
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
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, tuple(params))
            rows = await cur.fetchall()
    return rows or []


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
    pool = await init_pool()
    async with pool.acquire() as conn:  # type: ignore[attr-defined]
        async with conn.cursor() as cur:
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
