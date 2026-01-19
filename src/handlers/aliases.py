"""Alias management handlers."""

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..dispatcher import ADMIN_IDS, bot, dp
from .. import db
from ..handlers.users import _resolve_user_id


def alias_row_controls(alias: str, buyer_id: int | None, lead_id: int | None) -> InlineKeyboardMarkup:
    """Build alias row controls keyboard."""
    buttons = [
        [InlineKeyboardButton(text="Set buyer", callback_data=f"alias:setbuyer:{alias}")],
        [InlineKeyboardButton(text="Set lead", callback_data=f"alias:setlead:{alias}")],
        [InlineKeyboardButton(text="Delete", callback_data=f"alias:delete:{alias}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def _send_aliases(chat_id: int, actor_id: int):
    """Send list of aliases."""
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    rows = await db.list_aliases()
    if not rows:
        await bot.send_message(chat_id, "Алиасов пока нет.")
    else:
        for r in rows:
            text = f"<b>{r['alias']}</b> → buyer={r['buyer_id'] or '-'} | lead={r['lead_id'] or '-'}"
            await bot.send_message(chat_id, text, reply_markup=alias_row_controls(r['alias'], r['buyer_id'], r['lead_id']))
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Добавить алиас", callback_data="alias:new")]])
    await bot.send_message(chat_id, "Управление алиасами:", reply_markup=kb)


@dp.message(Command("aliases"))
async def on_aliases(message: Message):
    """Handle /aliases command."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    rows = await db.list_aliases()
    if not rows:
        await message.answer("Алиасов пока нет.")
    else:
        for r in rows:
            text = f"<b>{r['alias']}</b> → buyer={r['buyer_id'] or '-'} | lead={r['lead_id'] or '-'}"
            await message.answer(text, reply_markup=alias_row_controls(r['alias'], r['buyer_id'], r['lead_id']))
    # кнопка для создания нового алиаса
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Добавить алиас", callback_data="alias:new")]])
    await message.answer("Управление алиасами:", reply_markup=kb)


@dp.message(Command("setalias"))
async def on_setalias(message: Message):
    """Handle /setalias command."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /setalias <alias> buyer=<id|-> lead=<id|->
    parts = message.text.split()
    if len(parts) < 2:
        return await message.answer("Использование: /setalias <alias> buyer=<id|-> lead=<id|->")
    alias = parts[1]
    buyer_id = None
    lead_id = None
    for p in parts[2:]:
        if p.startswith("buyer="):
            v = p.split("=", 1)[1]
            buyer_id = None if v == '-' else int(v)
        if p.startswith("lead="):
            v = p.split("=", 1)[1]
            lead_id = None if v == '-' else int(v)
    await db.set_alias(alias, buyer_id, lead_id)
    await message.answer("Алиас сохранён")


@dp.message(Command("delalias"))
async def on_delalias(message: Message):
    """Handle /delalias command."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("Использование: /delalias <alias>")
    await db.delete_alias(parts[1])
    await message.answer("Алиас удалён")


@dp.callback_query(F.data == "alias:new")
async def cb_alias_new(call: CallbackQuery):
    """Handle alias creation callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "alias:new", None)
    await call.message.answer("Введите имя алиаса (префикс campaign_name до _):")
    await call.answer()


@dp.callback_query(F.data.startswith("alias:setbuyer:"))
async def cb_alias_setbuyer(call: CallbackQuery):
    """Handle alias buyer setting callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.set_pending_action(call.from_user.id, f"alias:setbuyer:{alias}", None)
    await call.message.answer(f"Пришлите Telegram ID или @username покупателя для алиаса {alias}, или '-' чтобы убрать")
    await call.answer()


@dp.callback_query(F.data.startswith("alias:setlead:"))
async def cb_alias_setlead(call: CallbackQuery):
    """Handle alias lead setting callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.set_pending_action(call.from_user.id, f"alias:setlead:{alias}", None)
    await call.message.answer(f"Пришлите Telegram ID или @username лида для алиаса {alias}, или '-' чтобы убрать")
    await call.answer()


@dp.callback_query(F.data.startswith("alias:delete:"))
async def cb_alias_delete(call: CallbackQuery):
    """Handle alias deletion callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.delete_alias(alias)
    await call.message.edit_text(f"Алиас {alias} удалён")
    await call.answer()
