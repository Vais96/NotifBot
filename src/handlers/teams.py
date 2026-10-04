"""Team management handlers."""

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..dispatcher import bot, dp
from .common import STALE_BUTTON, callback_parts, is_admin
from ..constants import Role
from .. import db
from ..utils.html import safe

TEAM_PICKER_PAGE_SIZE = 40


def _same_team(user_team_id, team_id: int) -> bool:
    if user_team_id is None:
        return False
    return int(user_team_id) == int(team_id)


def _myteam_menu() -> InlineKeyboardMarkup:
    """Build my team menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Состав команды", callback_data="myteam:list")],
    ])


async def _send_myteam(chat_id: int, actor_id: int):
    """Send my team management interface."""
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == actor_id), None)
    lead_team_ids = await db.list_user_lead_teams(actor_id)
    if await is_admin(actor_id):
        lead_team_ids = [int(me.get("team_id"))] if me and me.get("team_id") else []
    if not lead_team_ids:
        return await bot.send_message(chat_id, "Недостаточно прав или вы не закреплены за командой")
    await bot.send_message(chat_id, "Моя команда — управление", reply_markup=_myteam_menu())


def _teams_menu() -> InlineKeyboardMarkup:
    """Build teams menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список команд", callback_data="teams:list")],
        [InlineKeyboardButton(text="Участники", callback_data="teams:members")],
    ])


async def _send_teams(chat_id: int, actor_id: int):
    """Send teams management interface."""
    if not await is_admin(actor_id):
        return await bot.send_message(chat_id, "Только для админов")
    await bot.send_message(chat_id, "Команды — управление", reply_markup=_teams_menu())


@dp.callback_query(F.data == "myteam:list")
async def cb_myteam_list(call: CallbackQuery):
    """Handle my team list callback."""
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    team_id = await db.get_primary_lead_team(call.from_user.id)
    if await is_admin(call.from_user.id) and not team_id:
        team_id = int(me.get("team_id")) if me and me.get("team_id") else None
    if team_id is None:
        return await call.answer("Нет прав", show_alert=True)
    members = [u for u in users if u.get("team_id") is not None and int(u.get("team_id")) == int(team_id)]
    if not members:
        await call.message.answer("Состав пуст")
    else:
        lines = [f"• <code>{u['telegram_id']}</code> @{safe(u['username'] or '-')} ({u['role']})" for u in members]
        await call.message.answer("Состав команды:\n" + "\n".join(lines))
    await call.answer()


@dp.callback_query(F.data == "teams:list")
async def cb_teams_list(call: CallbackQuery):
    """Handle teams list callback."""
    if not await is_admin(call.from_user.id):
        return await call.answer("Нет прав", show_alert=True)
    teams = await db.list_teams()
    if not teams:
        await call.message.answer("Команд нет")
        return await call.answer()
    lines = [f"#{t['id']} — {safe(t['name'])}" for t in teams]
    await call.message.answer("Команды:\n" + "\n".join(lines))
    await call.answer()


@dp.callback_query(F.data == "teams:members")
async def cb_team_members(call: CallbackQuery):
    """Handle team members callback."""
    if not await is_admin(call.from_user.id):
        return await call.answer("Нет прав", show_alert=True)
    teams = await db.list_teams()
    if not teams:
        await call.message.answer("Команд нет")
        return await call.answer()
    buttons = [[InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"team:members:{t['id']}")] for t in teams[:50]]
    await call.message.answer("Выберите команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()


@dp.callback_query(F.data.startswith("team:members:"))
async def cb_team_members_manage(call: CallbackQuery):
    """Handle team members management callback."""
    if not await is_admin(call.from_user.id):
        return await call.answer("Нет прав", show_alert=True)
    parts = callback_parts(call, 3)
    if not parts or not parts[2].isdigit():
        return await call.answer(STALE_BUTTON, show_alert=True)
    team_id = int(parts[2])
    users = await db.list_users()
    members = [u for u in users if _same_team(u.get("team_id"), team_id)]
    if members:
        await call.message.answer("Участники:\n" + "\n".join(f"• <code>{u['telegram_id']}</code> @{safe(u['username'] or '-')} ({u['role']})" for u in members))
    else:
        await call.message.answer("Участники: пусто")
    await call.answer()


@dp.message(Command("createteam"))
async def on_create_team(message: Message):
    """Handle /createteam command."""
    await message.answer("Команды создаются только в Admin API.")


@dp.message(Command("setteam"))
async def on_set_team(message: Message):
    """Handle /setteam command."""
    await message.answer("Состав команд обновляется только из Admin API.")


@dp.message(Command("listteams"))
async def on_list_teams(message: Message):
    """Handle /listteams command (lead/head/admin only)."""
    me = await db.get_user(message.from_user.id)
    if not (
        await is_admin(message.from_user.id, me)
        or (me or {}).get("role") in (Role.LEAD, Role.HEAD)
        or await db.list_user_lead_teams(message.from_user.id)
    ):
        return await message.answer("Нет прав")
    teams = await db.list_teams()
    if not teams:
        return await message.answer("Команд нет")
    lines = [f"#{t['id']} — {safe(t['name'])}" for t in teams]
    await message.answer("Команды:\n" + "\n".join(lines))
