"""Ограничение исходящих sendMessage с учётом лимитов Telegram Bot API.

Логирование ошибок Telegram из этого модуля:
- 429 Flood control: на каждой попытке — loguru ``warning`` (retry_after, chat_id);
  если после всех повторов отправка не удалась — ``error`` и проброс исключения.
- Остальные исключения aiogram из ``send_message`` не перехватываются здесь —
  их обрабатывают вызывающие места (например, ``underdog``: Forbidden/BadRequest и общий ``Exception``).

HTTP-статус ответа Bot API сохраняется для каждого успешного вызова (см. ``StatusCapturingAiohttpSession``)
и прокидывается в объект ``Message`` как поле ``__tg_http_status__``, чтобы при пометке ``telegram_sent``
в Underdog требовать не только ``ok``/``message_id`` в теле JSON, но и именно HTTP 200.
"""

from __future__ import annotations

import asyncio
import time
import weakref
from collections import defaultdict, deque
from contextvars import ContextVar
from typing import Any, Deque, Dict, Optional, cast

from aiohttp import ClientError
from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter
from aiogram.methods import TelegramMethod
from aiogram.methods.base import TelegramType
from loguru import logger

# Последний HTTP-статус ответа Bot API в рамках текущего запроса метода (устанавливает session).
_telegram_http_status_ctx: ContextVar[Optional[int]] = ContextVar(
    "telegram_http_status_ctx", default=None
)

_ensure_session_lock = asyncio.Lock()


class StatusCapturingAiohttpSession(AiohttpSession):
    """
    Как AiohttpSession, но перед разбором JSON сохраняет HTTP-статус (обычно 200 при успехе).
    Telegram нередко отдаёт JSON с ``ok: false`` всё ещё с HTTP 200 — это обрабатывает check_response.
    """

    async def make_request(
        self, bot: Bot, method: TelegramMethod[TelegramType], timeout: Optional[int] = None
    ) -> TelegramType:
        session = await self.create_session()

        url = self.api.api_url(token=bot.token, method=method.__api_method__)
        form = self.build_form_data(bot=bot, method=method)

        try:
            async with session.post(
                url, data=form, timeout=self.timeout if timeout is None else timeout
            ) as resp:
                raw_result = await resp.text()
        except asyncio.TimeoutError:
            _telegram_http_status_ctx.set(None)
            raise TelegramNetworkError(method=method, message="Request timeout error")
        except ClientError as e:
            _telegram_http_status_ctx.set(None)
            raise TelegramNetworkError(method=method, message=f"{type(e).__name__}: {e}")
        _telegram_http_status_ctx.set(resp.status)
        response = self.check_response(
            bot=bot, method=method, status_code=resp.status, content=raw_result
        )
        return cast(TelegramType, response.result)


async def _ensure_status_capturing_session(bot: Bot) -> None:
    if isinstance(bot.session, StatusCapturingAiohttpSession):
        return
    async with _ensure_session_lock:
        if isinstance(bot.session, StatusCapturingAiohttpSession):
            return
        old = bot.session
        kwargs: Dict[str, Any] = {
            "timeout": old.timeout,
            "json_loads": old.json_loads,
            "json_dumps": old.json_dumps,
            "api": old.api,
        }
        if isinstance(old, AiohttpSession):
            kwargs["proxy"] = old.proxy
            kwargs["limit"] = int(old._connector_init.get("limit", 100))
        bot.session = StatusCapturingAiohttpSession(**kwargs)
        try:
            await old.close()
        except Exception:
            pass

# Запас к официальным лимитам (~30/s общий, 1/s на чат, ~20/мин в группы/каналы).
GLOBAL_MAX_PER_SECOND = 25
PRIVATE_MIN_INTERVAL_S = 1.05
GROUP_WINDOW_S = 60.0
GROUP_MAX_PER_WINDOW = 18
GROUP_MIN_GAP_S = 3.1
# Не блокируем ETL на часы из-за одного проблемного чата.
MAX_RETRY_AFTER_SECONDS = 90.0


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
    max_retry_after_seconds: float = MAX_RETRY_AFTER_SECONDS,
    **kwargs: Any,
) -> Any:
    """
    send_message с учётом лимитов и повтором после TelegramRetryAfter (Flood control).

    Успешный ответ дополняется ``__tg_http_status__`` (ожидается 200), если доступен
    захват через ``StatusCapturingAiohttpSession``.
    """
    await _ensure_status_capturing_session(bot)
    cid = int(chat_id)
    limiter = _limiter_for_bot(bot)
    attempt = 0
    last_exc: Optional[TelegramRetryAfter] = None
    while attempt < max_flood_retries:
        _telegram_http_status_ctx.set(None)
        await limiter.acquire_before_send(cid)
        try:
            msg = await bot.send_message(cid, **kwargs)
            st = _telegram_http_status_ctx.get()
            if st is not None and hasattr(msg, "model_copy"):
                msg = msg.model_copy(update={"__tg_http_status__": st})
            return msg
        except TelegramRetryAfter as exc:
            last_exc = exc
            ra = getattr(exc, "retry_after", None)
            wait = float(ra if ra is not None else 1) + 0.35
            if wait > max_retry_after_seconds:
                logger.error(
                    "Telegram 429 FloodWait слишком длинный (retry_after={}s > {}s), "
                    "не ждём и отдаём ошибку выше, chat_id={}",
                    ra,
                    max_retry_after_seconds,
                    cid,
                )
                raise exc
            # Ответ API: 429 Too Many Requests + retry_after (секунды ожидания).
            logger.warning(
                "Telegram 429 FloodWait: attempt {}/{}, retry_after={}s, backoff {:.1f}s, chat_id={}",
                attempt + 1,
                max_flood_retries,
                ra,
                wait,
                cid,
            )
            await asyncio.sleep(wait)
            attempt += 1
    if last_exc is not None:
        logger.error(
            "Telegram 429 FloodWait: исчерпаны повторы send_message ({}), chat_id={}, последний retry_after={}: {}",
            max_flood_retries,
            cid,
            getattr(last_exc, "retry_after", None),
            last_exc,
        )
        raise last_exc
    raise RuntimeError("limited_send_message: retries exhausted without exception")
