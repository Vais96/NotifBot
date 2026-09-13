"""Ads Workspace key tab for buyers, leads, mentors and heads."""

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..dispatcher import ADMIN_IDS, bot, dp
from .. import db
from ..services.ads_workspace import (
    AdsWorkspaceError,
    ads_key_label,
    can_use_ads_key,
    format_ads_key_message,
    request_ads_key,
)


def _rotate_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Выдать новый код", callback_data="adskey:rotate")],
    ])


def _confirm_rotate_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Да, отозвать старый", callback_data="adskey:rotate:yes"),
            InlineKeyboardButton(text="Отмена", callback_data="adskey:back"),
        ],
    ])


async def _actor_may_use(user_id: int) -> tuple[bool, dict | None]:
    me = await db.get_user(user_id)
    is_admin = user_id in ADMIN_IDS
    role = (me or {}).get("role")
    if is_admin:
        role = "admin"
    active = True if not me else bool(me.get("is_active", 1))
    return can_use_ads_key(is_admin, role, active), me


async def send_ads_key(chat_id: int, user_id: int, username: str | None, *, rotate: bool = False) -> None:
    allowed, me = await _actor_may_use(user_id)
    if not allowed:
        await bot.send_message(chat_id, "Ключ Ads Workspace доступен байерам, лидам, менторам и хэдам.")
        return
    label = ads_key_label(me, username)
    try:
        payload = await request_ads_key(user_id, label, rotate=rotate)
    except AdsWorkspaceError as exc:
        if exc.code == "not_configured":
            await bot.send_message(chat_id, "Выдача ключей ещё не подключена. Напиши администратору.")
            return
        await bot.send_message(chat_id, "Не удалось получить ключ. Попробуй позже или напиши администратору.")
        return
    markup = _rotate_keyboard() if payload.get("status") == "active" else None
    await bot.send_message(chat_id, format_ads_key_message(payload), reply_markup=markup)


@dp.message(Command("adskey"))
async def on_adskey(message: Message):
    await send_ads_key(message.chat.id, message.from_user.id, message.from_user.username)


@dp.callback_query(F.data == "adskey:rotate")
async def cb_adskey_rotate(call: CallbackQuery):
    allowed, _ = await _actor_may_use(call.from_user.id)
    if not allowed:
        return await call.answer("Нет доступа", show_alert=True)
    await call.message.answer(
        "Старый ключ перестанет работать во всех профилях. Парк кабинетов сохранится. Продолжить?",
        reply_markup=_confirm_rotate_keyboard(),
    )
    return await call.answer()


@dp.callback_query(F.data == "adskey:rotate:yes")
async def cb_adskey_rotate_yes(call: CallbackQuery):
    await send_ads_key(call.message.chat.id, call.from_user.id, call.from_user.username, rotate=True)
    return await call.answer()


@dp.callback_query(F.data == "adskey:back")
async def cb_adskey_back(call: CallbackQuery):
    await send_ads_key(call.message.chat.id, call.from_user.id, call.from_user.username)
    return await call.answer("Отменено")
