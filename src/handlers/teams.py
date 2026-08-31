"""Team management handlers."""

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..dispatcher import ADMIN_IDS, bot, dp
from .. import db
from ..handlers.users import _resolve_user_id

TEAM_PICKER_PAGE_SIZE = 40


def _same_team(user_team_id, team_id: int) -> bool:
    if user_team_id is None:
        return False
    return int(user_team_id) == int(team_id)


def _user_picker_label(user: dict) -> str:
    username = user.get("username")
    if username:
        return f"@{username}"
    return str(user["telegram_id"])


def _team_add_picker_kb(team_id: int, users: list[dict], page: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    total = len(users)
    pages = max((total - 1) // TEAM_PICKER_PAGE_SIZE + 1, 1)
    page = max(0, min(page, pages - 1))
    start = page * TEAM_PICKER_PAGE_SIZE
    for u in users[start : start + TEAM_PICKER_PAGE_SIZE]:
        rows.append([
            InlineKeyboardButton(
                text=f"Добавить {_user_picker_label(u)}",
                callback_data=f"team:add:{team_id}:{u['telegram_id']}",
            )
        ])
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"team:add_page:{team_id}:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{pages}", callback_data="team:noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"team:add_page:{team_id}:{page + 1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _team_remove_picker_kb(team_id: int, users: list[dict], page: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    total = len(users)
    pages = max((total - 1) // TEAM_PICKER_PAGE_SIZE + 1, 1)
    page = max(0, min(page, pages - 1))
    start = page * TEAM_PICKER_PAGE_SIZE
    for u in users[start : start + TEAM_PICKER_PAGE_SIZE]:
        rows.append([
            InlineKeyboardButton(
                text=f"Убрать {_user_picker_label(u)}",
                callback_data=f"team:remove:{team_id}:{u['telegram_id']}",
            )
        ])
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"team:remove_page:{team_id}:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{pages}", callback_data="team:noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"team:remove_page:{team_id}:{page + 1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    if actor_id in ADMIN_IDS:
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
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    await bot.send_message(chat_id, "Команды — управление", reply_markup=_teams_menu())


@dp.callback_query(F.data == "myteam:list")
async def cb_myteam_list(call: CallbackQuery):
    """Handle my team list callback."""
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    team_id = await db.get_primary_lead_team(call.from_user.id)
    if call.from_user.id in ADMIN_IDS and not team_id:
        team_id = int(me.get("team_id")) if me and me.get("team_id") else None
    if team_id is None:
        return await call.answer("Нет прав", show_alert=True)
    members = [u for u in users if u.get("team_id") is not None and int(u.get("team_id")) == int(team_id)]
    if not members:
        await call.message.answer("Состав пуст")
    else:
        lines = [f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} ({u['role']})" for u in members]
        await call.message.answer("Состав команды:\n" + "\n".join(lines))
    await call.answer()


@dp.callback_query(F.data == "myteam:add")
async def cb_myteam_add(call: CallbackQuery):
    """Handle my team add callback."""
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


@dp.callback_query(F.data == "myteam:remove")
async def cb_myteam_remove(call: CallbackQuery):
    """Handle my team remove callback."""
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


@dp.callback_query(F.data.startswith("myteam:remove:"))
async def cb_myteam_remove_user(call: CallbackQuery):
    """Handle my team remove user callback."""
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


@dp.callback_query(F.data == "teams:list")
async def cb_teams_list(call: CallbackQuery):
    """Handle teams list callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    teams = await db.list_teams()
    if not teams:
        await call.message.answer("Команд нет")
        return await call.answer()
    lines = [f"#{t['id']} — {t['name']}" for t in teams]
    await call.message.answer("Команды:\n" + "\n".join(lines))
    await call.answer()


@dp.callback_query(F.data == "teams:new")
async def cb_team_new(call: CallbackQuery):
    """Handle team creation callback."""
    await call.answer("Команды создаются в Admin API", show_alert=True)


@dp.callback_query(F.data == "teams:setlead")
async def cb_team_setlead(call: CallbackQuery):
    """Handle team set lead callback."""
    await call.answer("Лиды назначаются в Admin API", show_alert=True)


@dp.callback_query(F.data.startswith("team:choose_for_lead:"))
async def cb_team_choose_for_lead(call: CallbackQuery):
    """Handle team choose for lead callback."""
    await call.answer("Лиды назначаются в Admin API", show_alert=True)


@dp.callback_query(F.data == "teams:members")
async def cb_team_members(call: CallbackQuery):
    """Handle team members callback."""
    if call.from_user.id not in ADMIN_IDS:
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
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    team_id = int(call.data.split(":", 2)[2])
    users = await db.list_users()
    members = [u for u in users if _same_team(u.get("team_id"), team_id)]
    if members:
        await call.message.answer("Участники:\n" + "\n".join(f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} ({u['role']})" for u in members))
    else:
        await call.message.answer("Участники: пусто")
    await call.answer()


@dp.callback_query(F.data == "team:noop")
async def cb_team_noop(call: CallbackQuery):
    await call.answer()


@dp.callback_query(F.data.startswith("team:add_page:"))
async def cb_team_add_page(call: CallbackQuery):
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


@dp.callback_query(F.data.startswith("team:remove_page:"))
async def cb_team_remove_page(call: CallbackQuery):
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


@dp.callback_query(F.data.startswith("team:refresh_names:"))
async def cb_team_refresh_names(call: CallbackQuery):
    """Handle team refresh names callback."""
    await call.answer("Профили и состав обновляются из Admin API", show_alert=True)


@dp.callback_query(F.data.startswith("team:add:"))
async def cb_team_add_member(call: CallbackQuery):
    """Handle team add member callback."""
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


@dp.callback_query(F.data.startswith("team:remove:"))
async def cb_team_remove_member(call: CallbackQuery):
    """Handle team remove member callback."""
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


@dp.callback_query(F.data.startswith("team:choose:"))
async def cb_team_choose(call: CallbackQuery):
    """Handle team choose callback."""
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


@dp.callback_query(F.data.startswith("team:set:"))
async def cb_team_set(call: CallbackQuery):
    """Handle team set callback."""
    await call.answer("Состав команд обновляется из Admin API", show_alert=True)


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
    """Handle /listteams command."""
    teams = await db.list_teams()
    if not teams:
        return await message.answer("Команд нет")
    lines = [f"#{t['id']} — {t['name']}" for t in teams]
    await message.answer("Команды:\n" + "\n".join(lines))
