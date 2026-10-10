"""DDL (SCHEMA_SQL) and idempotent column/index migrations applied on pool creation."""

import aiomysql
from typing import List, Tuple
from loguru import logger


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
    # campaign prefix (Admin «Имя в Keitaro» / employee number) -> Admin full name, for every
    # active employee incl. those without Telegram: names the БАЙЕР line of unrouted deposits
    """
    CREATE TABLE IF NOT EXISTS tg_campaign_names (
        alias VARCHAR(255) PRIMARY KEY,
        full_name VARCHAR(255) NOT NULL,
        has_telegram TINYINT(1) NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
