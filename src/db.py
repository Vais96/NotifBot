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
        role ENUM('buyer','lead','head','admin') NOT NULL DEFAULT 'buyer',
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
    assert role in ("buyer", "lead", "head", "admin")
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
