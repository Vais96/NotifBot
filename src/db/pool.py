"""Connection pool and query helpers: init_pool, cursor/execute/fetch_one/fetch_all, transaction."""

import asyncio
import aiomysql
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, AsyncIterator, Iterable, Sequence
from datetime import datetime, timezone
from loguru import logger
from ..config import secret, settings
import urllib.parse
import ssl
import re
from .schema import _apply_schema


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
async def transaction(dict_rows: bool = False) -> AsyncIterator[aiomysql.Cursor]:
    """Cursor inside BEGIN/COMMIT; ROLLBACK on any exception (pool connections are autocommit otherwise)."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with (conn.cursor(aiomysql.DictCursor) if dict_rows else conn.cursor()) as cur:
                yield cur
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise


@asynccontextmanager
async def cursor(dict_rows: bool = False) -> AsyncIterator[aiomysql.Cursor]:
    """Autocommit cursor on a pooled connection; dict_rows=True -> rows as dicts."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        async with (conn.cursor(aiomysql.DictCursor) if dict_rows else conn.cursor()) as cur:
            yield cur


async def execute(sql: str, args: Any = None) -> int:
    """Run one statement; returns rowcount."""
    async with cursor() as cur:
        await cur.execute(sql, args)
        return int(cur.rowcount or 0)


async def fetch_one(sql: str, args: Any = None, *, dict_rows: bool = False) -> Any:
    async with cursor(dict_rows) as cur:
        await cur.execute(sql, args)
        return await cur.fetchone()


async def fetch_all(sql: str, args: Any = None, *, dict_rows: bool = False) -> list:
    async with cursor(dict_rows) as cur:
        await cur.execute(sql, args)
        return list(await cur.fetchall() or [])


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


def _utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _dt_as_utc_naive(dt: Any) -> datetime:
    if not isinstance(dt, datetime):
        return _utc_naive()
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt
