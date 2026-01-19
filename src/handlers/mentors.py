"""Mentor management handlers."""

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..dispatcher import ADMIN_IDS, bot, dp
from .. import db
from ..handlers.users import _resolve_user_id
from loguru import logger


def _mentor_row_controls(mentor_id: int) -> InlineKeyboardMarkup:
    """Build mentor row controls keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подписки", callback_data=f"mentor:subs:{mentor_id}")],
        [InlineKeyboardButton(text="Снять роль", callback_data=f"mentor:unset:{mentor_id}")]
    ])


def _mentor_add_controls() -> InlineKeyboardMarkup:
    """Build mentor add controls keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить ментора", callback_data="mentor:add")]
    ])


async def _send_mentors(chat_id: int, actor_id: int):
    """Send mentors management interface."""
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    users = await db.list_users()
    mentors = [u for u in users if u.get("role") == "mentor"]
    if not mentors:
        return await bot.send_message(chat_id, "Менторы:\nПока нет менторов.", reply_markup=_mentor_add_controls())
    await bot.send_message(chat_id, "Менторы:", reply_markup=_mentor_add_controls())
    for u in mentors[:25]:
        text = (
            f"<b>{u['full_name'] or '-'}</b> @{u['username'] or '-'}\n"
            f"ID: <code>{u['telegram_id']}</code>\n"
            f"Role: <code>{u['role']}</code> | Team: <code>{u['team_id'] or '-'}</code> | Active: <code>{'yes' if u['is_active'] else 'no'}</code>"
        )
        await bot.send_message(chat_id, text, reply_markup=_mentor_row_controls(int(u['telegram_id'])))


def _mentor_subs_keyboard(mentor_id: int, teams: list[dict], followed: set[int]) -> InlineKeyboardMarkup:
    """Build mentor subscriptions keyboard."""
    rows = []
    for t in teams[:50]:
        tid = int(t['id'])
        mark = "✅" if tid in followed else "➕"
        rows.append([InlineKeyboardButton(text=f"{mark} #{tid} {t['name']}", callback_data=f"mentor:toggle:{mentor_id}:{tid}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="mentor:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "mentor:add")
async def cb_mentor_add(call: CallbackQuery):
    """Handle mentor add callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "mentor:add", None)
    await call.message.answer("Пришлите Telegram ID или @username пользователя, которому назначить роль mentor")
    await call.answer()


@dp.callback_query(F.data.startswith("mentor:unset:"))
async def cb_mentor_unset(call: CallbackQuery):
    """Handle mentor unset callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, _, mid = call.data.split(":", 2)
    try:
        mid_i = int(mid)
        await db.set_user_role(mid_i, "buyer")
        await call.answer("Роль mentor снята")
    except Exception as e:
        logger.exception(e)
        await call.answer("Ошибка", show_alert=True)


@dp.callback_query(F.data.startswith("mentor:subs:"))
async def cb_mentor_subs(call: CallbackQuery):
    """Handle mentor subscriptions callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, _, mid = call.data.split(":", 2)
    mid_i = int(mid)
    teams = await db.list_teams()
    followed = set(await db.list_mentor_teams(mid_i))
    kb = _mentor_subs_keyboard(mid_i, teams, followed)
    await call.message.answer(f"Подписки ментора <code>{mid_i}</code>:", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data.startswith("mentor:toggle:"))
async def cb_mentor_toggle(call: CallbackQuery):
    """Handle mentor toggle subscription callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, _, mid, tid = call.data.split(":", 3)
    mid_i = int(mid)
    tid_i = int(tid)
    followed = set(await db.list_mentor_teams(mid_i))
    try:
        if tid_i in followed:
            await db.remove_mentor_team(mid_i, tid_i)
        else:
            await db.add_mentor_team(mid_i, tid_i)
    except Exception as e:
        logger.exception(e)
    teams = await db.list_teams()
    followed = set(await db.list_mentor_teams(mid_i))
    kb = _mentor_subs_keyboard(mid_i, teams, followed)
    try:
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        await call.message.answer(f"Подписки ментора <code>{mid_i}</code>:", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "mentor:back")
async def cb_mentor_back(call: CallbackQuery):
    """Handle mentor back callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await _send_mentors(call.message.chat.id, call.from_user.id)
    await call.answer()


@dp.message(Command("addmentor"))
async def on_add_mentor(message: Message):
    """Handle /addmentor command."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /addmentor <telegram_id|@username>
    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("Использование: /addmentor <telegram_id|@username>")
    try:
        uid = await _resolve_user_id(parts[1])
        await db.set_user_role(uid, "mentor")
        await message.answer("Роль mentor назначена")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка назначения роли")


@dp.message(Command("mentor_follow"))
async def on_mentor_follow(message: Message):
    """Handle /mentor_follow command."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /mentor_follow <mentor_id> <team_id>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /mentor_follow <mentor_id> <team_id>")
    try:
        mentor_id = int(parts[1])
        team_id = int(parts[2])
        await db.add_mentor_team(mentor_id, team_id)
        await message.answer("Ментор подписан на команду")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка подписки")


@dp.message(Command("mentor_unfollow"))
async def on_mentor_unfollow(message: Message):
    """Handle /mentor_unfollow command."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /mentor_unfollow <mentor_id> <team_id>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /mentor_unfollow <mentor_id> <team_id>")
    try:
        mentor_id = int(parts[1])
        team_id = int(parts[2])
        await db.remove_mentor_team(mentor_id, team_id)
        await message.answer("Ментор отписан от команды")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка отписки")
