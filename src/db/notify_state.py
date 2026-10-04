"""Admin alert throttling and Underdog delivery bookkeeping (tg_admin_notify_throttle, tg_underdog_sent)."""

from typing import List
from datetime import timedelta
from loguru import logger
from .pool import _utc_naive, cursor, execute


_ADMIN_ALERT_MIN_INTERVAL = timedelta(hours=1)


_ADMIN_ALERT_MUTE_AFTER_FIRST = timedelta(hours=24)


async def admin_notify_throttle_allow_send(dedupe_key: str) -> bool:
    """Отправить ли админский алерт в Telegram. Первый раз пишем в БД; далее не чаще раза в час; спустя 24 ч с первого — больше не слать (пока не clear)."""
    k = (dedupe_key or "none")[:384]
    now = _utc_naive().replace(microsecond=0)
    try:
        async with cursor() as cur:
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
        await execute("DELETE FROM tg_admin_notify_throttle WHERE dedupe_key=%s", (k,))
    except Exception as e:
        logger.warning("admin_notify_throttle_clear failed", key=k, error=str(e))


async def underdog_sent_ids(kind: str, external_ids: List[str], chat_id: int) -> set:
    if not external_ids:
        return set()
    async with cursor() as cur:
        placeholders = ",".join(["%s"] * len(external_ids))
        await cur.execute(
            f"SELECT external_id FROM tg_underdog_sent WHERE kind=%s AND chat_id=%s AND external_id IN ({placeholders})",
            (kind, chat_id, *external_ids),
        )
        return {str(r[0]) for r in await cur.fetchall()}


async def mark_underdog_sent(kind: str, external_ids: List[str], chat_id: int) -> None:
    if not external_ids:
        return
    async with cursor() as cur:
        await cur.executemany(
            "INSERT IGNORE INTO tg_underdog_sent (kind, external_id, chat_id) VALUES (%s, %s, %s)",
            [(kind, eid, chat_id) for eid in external_ids],
        )
