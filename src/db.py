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
                ON DUPLICATE KEY UPDATE username=VALUES(username), full_name=VALUES(full_name), is_active=1
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
                    float(payload["payout"]) if (payload["payout"] not in (None, "")) else None,
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
          AND LOWER(COALESCE(status, '')) IN ({placeholders})
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (user_id, start, end, *sale_like))
            row = await cur.fetchone()
            return int(row[0]) if row else 0

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
