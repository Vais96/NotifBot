"""Ограничение исходящих sendMessage с учётом лимитов Telegram Bot API."""

from __future__ import annotations

import asyncio
import time
import weakref
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter
from loguru import logger

# Запас к официальным лимитам (~30/s общий, 1/s на чат, ~20/мин в группы/каналы).
GLOBAL_MAX_PER_SECOND = 25
PRIVATE_MIN_INTERVAL_S = 1.05
GROUP_WINDOW_S = 60.0
GROUP_MAX_PER_WINDOW = 18
GROUP_MIN_GAP_S = 3.1


class TelegramOutboundRateLimiter:
    """
    Один экземпляр на каждый объект Bot (токен).

    Порядок: сначала ждём лимит чата (личный или группа), затем глобальный слот.
    """

    __slots__ = (
        "_global_lock",
        "_global_times",
        "_chat_locks",
        "_private_last",
        "_group_windows",
        "_group_last",
    )

    def __init__(self) -> None:
        self._global_lock = asyncio.Lock()
        self._global_times: Deque[float] = deque()
        self._chat_locks: Dict[int, asyncio.Lock] = {}
        self._private_last: Dict[int, float] = {}
        self._group_windows: Dict[int, Deque[float]] = defaultdict(deque)
        self._group_last: Dict[int, float] = {}

    def _lock_for(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self._chat_locks:
            self._chat_locks[chat_id] = asyncio.Lock()
        return self._chat_locks[chat_id]

    async def _wait_global_slot(self) -> None:
        while True:
            async with self._global_lock:
                now = time.monotonic()
                while self._global_times and self._global_times[0] <= now - 1.0:
                    self._global_times.popleft()
                if len(self._global_times) < GLOBAL_MAX_PER_SECOND:
                    self._global_times.append(now)
                    return
                delay = 1.0 - (now - self._global_times[0]) + 0.02
            await asyncio.sleep(max(delay, 0.02))

    async def _private_sleep_if_needed(self, chat_id: int) -> None:
        now = time.monotonic()
        last = self._private_last.get(chat_id)
        if last is not None:
            need = PRIVATE_MIN_INTERVAL_S - (now - last)
            if need > 0:
                await asyncio.sleep(need)

    async def _group_sleep_if_needed(self, chat_id: int) -> None:
        now = time.monotonic()
        q = self._group_windows[chat_id]
        while q and q[0] <= now - GROUP_WINDOW_S:
            q.popleft()
        if len(q) >= GROUP_MAX_PER_WINDOW:
            wait = GROUP_WINDOW_S - (now - q[0]) + 0.05
            await asyncio.sleep(wait)
            now = time.monotonic()
            while q and q[0] <= now - GROUP_WINDOW_S:
                q.popleft()
        last = self._group_last.get(chat_id)
        if last is not None:
            gap = GROUP_MIN_GAP_S - (now - last)
            if gap > 0:
                await asyncio.sleep(gap)
                now = time.monotonic()

    async def acquire_before_send(self, chat_id: int) -> None:
        cid = int(chat_id)
        async with self._lock_for(cid):
            if cid > 0:
                await self._private_sleep_if_needed(cid)
            else:
                await self._group_sleep_if_needed(cid)
            await self._wait_global_slot()
            now = time.monotonic()
            if cid > 0:
                self._private_last[cid] = now
            else:
                q = self._group_windows[cid]
                while q and q[0] <= now - GROUP_WINDOW_S:
                    q.popleft()
                q.append(now)
                self._group_last[cid] = now


_limiters: weakref.WeakKeyDictionary[Bot, TelegramOutboundRateLimiter] = weakref.WeakKeyDictionary()


def _limiter_for_bot(bot: Bot) -> TelegramOutboundRateLimiter:
    lim = _limiters.get(bot)
    if lim is None:
        lim = TelegramOutboundRateLimiter()
        _limiters[bot] = lim
    return lim


async def limited_send_message(
    bot: Bot,
    chat_id: int,
    *,
    max_flood_retries: int = 8,
    **kwargs: Any,
) -> Any:
    """
    send_message с учётом лимитов и повтором после TelegramRetryAfter (Flood control).
    """
    cid = int(chat_id)
    limiter = _limiter_for_bot(bot)
    attempt = 0
    last_exc: Optional[TelegramRetryAfter] = None
    while attempt < max_flood_retries:
        await limiter.acquire_before_send(cid)
        try:
            return await bot.send_message(cid, **kwargs)
        except TelegramRetryAfter as exc:
            last_exc = exc
            wait = float(getattr(exc, "retry_after", 1) or 1) + 0.35
            logger.warning(
                "Telegram FloodWait (retry_after={}), backoff {:.1f}s chat_id={}",
                getattr(exc, "retry_after", "?"),
                wait,
                cid,
            )
            await asyncio.sleep(wait)
            attempt += 1
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("limited_send_message: retries exhausted without exception")
