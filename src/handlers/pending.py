"""Text replies for pending multi-step actions (tg_pending_actions) + catch-all for stale buttons.

Registered last: its @dp.message() / @dp.callback_query() catch everything other handlers did not take.
"""

from typing import Awaitable, Callable, Dict, Optional, Tuple

from aiogram.types import CallbackQuery, Message
from loguru import logger

from .. import db
from ..constants import PendingAction, Role
from ..dispatcher import dp
from ..handlers.domains import notify_helper_domain_access
from ..handlers.users import _resolve_user_id
from ..handlers.youtube import handle_youtube_download
from ..utils.domain import lookup_domains_text
from ..utils.html import safe
from .common import STALE_BUTTON

PendingHandler = Callable[[Message, str], Awaitable[object]]
_HANDLERS: Dict[PendingAction, PendingHandler] = {}

UNKNOWN_USER_TEXT = (
    "Не удалось распознать пользователя. Пришлите numeric ID или @username. "
    "Если пользователь не писал боту, попросите его отправить /start."
)


def pending(action: PendingAction):
    def register(handler: PendingHandler) -> PendingHandler:
        _HANDLERS[action] = handler
        return handler
    return register


def parse_action(raw: str) -> Tuple[Optional[PendingAction], str]:
    """'alias:setbuyer:foo' -> (ALIAS_SET_BUYER, 'foo'); unknown -> (None, '')."""
    for action in sorted(PendingAction, key=len, reverse=True):
        if raw == action:
            return action, ""
        if raw.startswith(action + ":"):
            return action, raw[len(action) + 1:]
    return None, ""


@pending(PendingAction.FB_AWAIT_CSV)
async def _fb_await_csv(message: Message, _arg: str):
    # The CSV itself is handled by handlers/fb.py (document handler); here only text arrives.
    if (message.text or "").strip().lower() in ("-", "стоп", "stop"):
        await db.clear_pending_action(message.from_user.id)
        return await message.answer("Загрузка CSV отменена")
    return await message.answer("Пришлите CSV файлом или '-' чтобы отменить ожидание")


@pending(PendingAction.ALIAS_NEW)
async def _alias_new(message: Message, _arg: str):
    await db.set_alias(message.text.strip())
    await db.clear_pending_action(message.from_user.id)
    return await message.answer("Алиас создан. Откройте Алиасы в меню, чтобы назначить buyer/lead")


@pending(PendingAction.DOMAIN_CHECK)
async def _domain_check(message: Message, _arg: str):
    text = (message.text or "").strip()
    if text.lower() in ("-", "stop", "стоп"):
        await db.clear_pending_action(message.from_user.id)
        return await message.answer("Готово. Проверка доменов завершена")
    result = await lookup_domains_text(text)
    return await message.answer(result + "\n\nОтправьте следующий домен или '-' чтобы завершить")


@pending(PendingAction.YOUTUBE_AWAIT_URL)
async def _youtube_await_url(message: Message, _arg: str):
    return await handle_youtube_download(message)


async def _alias_assign(message: Message, alias: str, field: str, done_text: str):
    value = message.text.strip()
    user_id = None
    if value != "-":  # '-' clears the assignment
        try:
            user_id = await _resolve_user_id(value)
        except ValueError:
            await db.clear_pending_action(message.from_user.id)
            return await message.answer(UNKNOWN_USER_TEXT)
    await db.set_alias(alias, **{field: user_id})
    await db.clear_pending_action(message.from_user.id)
    return await message.answer(done_text)


@pending(PendingAction.ALIAS_SET_BUYER)
async def _alias_set_buyer(message: Message, alias: str):
    return await _alias_assign(message, alias, "buyer_id", "Buyer назначен")


@pending(PendingAction.ALIAS_SET_LEAD)
async def _alias_set_lead(message: Message, alias: str):
    return await _alias_assign(message, alias, "lead_id", "Lead назначен")


@pending(PendingAction.MENTOR_ADD)
async def _mentor_add(message: Message, _arg: str):
    try:
        uid = await _resolve_user_id(message.text.strip())
    except Exception:
        await db.clear_pending_action(message.from_user.id)
        return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username.")
    try:
        await db.upsert_user(uid, None, None)
    except Exception:
        pass
    await db.set_user_role(uid, Role.MENTOR)
    await db.clear_pending_action(message.from_user.id)
    return await message.answer("Назначен ментором")


@pending(PendingAction.HELPER_ADD)
async def _helper_add(message: Message, _arg: str):
    value = (message.text or "").strip()
    if value.lower() in ("-", "отмена", "cancel"):
        await db.clear_pending_action(message.from_user.id)
        return await message.answer("Отменено.")
    try:
        uid = await _resolve_user_id(value)
    except ValueError as e:
        return await message.answer(safe(str(e)))
    user = await db.get_user(uid)
    if not user:
        return await message.answer("Пользователь не найден в базе. Пусть нажмёт /start в боте.")
    await db.set_user_role(uid, Role.HELPER)
    await db.clear_pending_action(message.from_user.id)
    name = user.get("full_name") or user.get("username") or uid
    await notify_helper_domain_access(uid)
    return await message.answer(
        f"Пользователь {safe(name)} (@{safe(user.get('username') or uid)}) назначен помощником.\n"
        "Откройте «Помощники» в меню и нажмите «Назначить байера» рядом с ним.\n"
        "Помощнику уже доступны /checkdomain и кнопка «Проверить домен»."
    )


@pending(PendingAction.KPI_SET)
async def _kpi_set(message: Message, which: str):
    value = message.text.strip()
    goal = None
    if value != "-":  # '-' clears the goal
        try:
            goal = max(0, int(value))
        except Exception:
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Нужно целое число или '-' для очистки")
    current = await db.get_kpi(message.from_user.id)
    daily, weekly = current.get("daily_goal"), current.get("weekly_goal")
    if which == "daily":
        daily = goal
    else:
        weekly = goal
    await db.set_kpi(message.from_user.id, daily_goal=daily, weekly_goal=weekly)
    await db.clear_pending_action(message.from_user.id)
    return await message.answer("KPI обновлен")


@dp.message()
async def on_text_fallback(message: Message):
    if message.text and message.text.startswith("/"):
        return
    stored = await db.get_pending_action(message.from_user.id)
    if not stored:
        return
    action, arg = parse_action(stored[0])
    handler = _HANDLERS.get(action) if action else None
    if handler is None:
        logger.warning("Unknown pending action {!r} for {}", stored[0], message.from_user.id)
        return
    try:
        await handler(message, arg)
    except Exception:
        logger.exception("Pending action {} failed", stored[0])
        await message.answer("Ошибка обработки ввода")


@dp.callback_query()
async def on_stale_callback(call: CallbackQuery):
    """Buttons from removed/old menus get an answer instead of an endless spinner."""
    await call.answer(STALE_BUTTON, show_alert=True)
