"""Team management handlers."""

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..dispatcher import ADMIN_IDS, bot, dp
from .. import db
from ..handlers.users import _resolve_user_id


def _myteam_menu() -> InlineKeyboardMarkup:
    """Build my team menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Состав команды", callback_data="myteam:list")],
        [InlineKeyboardButton(text="Добавить по ID", callback_data="myteam:add")],
        [InlineKeyboardButton(text="Убрать участника", callback_data="myteam:remove")],
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
        [InlineKeyboardButton(text="Список команд", callback_data="teams:list"), InlineKeyboardButton(text="Создать команду", callback_data="teams:new")],
        [InlineKeyboardButton(text="Назначить лида", callback_data="teams:setlead")],
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
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    team_id = await db.get_primary_lead_team(call.from_user.id)
    if call.from_user.id in ADMIN_IDS and not team_id:
        team_id = int(me.get("team_id")) if me and me.get("team_id") else None
    if team_id is None:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, f"myteam:add:{team_id}", None)
    await call.message.answer("Пришлите Telegram ID пользователя для добавления в вашу команду")
    await call.answer()


@dp.callback_query(F.data == "myteam:remove")
async def cb_myteam_remove(call: CallbackQuery):
    """Handle my team remove callback."""
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
        return await call.answer()
    buttons = [[InlineKeyboardButton(text=f"Убрать @{u['username'] or u['telegram_id']}", callback_data=f"myteam:remove:{u['telegram_id']}")] for u in members[:25]]
    await call.message.answer("Кого убрать?", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()


@dp.callback_query(F.data.startswith("myteam:remove:"))
async def cb_myteam_remove_user(call: CallbackQuery):
    """Handle my team remove user callback."""
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    team_id = await db.get_primary_lead_team(call.from_user.id)
    if call.from_user.id in ADMIN_IDS and not team_id:
        team_id = int(me.get("team_id")) if me and me.get("team_id") else None
    if team_id is None:
        return await call.answer("Нет прав", show_alert=True)
    uid = int(call.data.split(":", 2)[2])
    # ensure target is in same team
    target = next((u for u in users if u["telegram_id"] == uid), None)
    if not target or target.get("team_id") is None or int(target.get("team_id")) != int(team_id):
        return await call.answer("Можно убирать только из своей команды", show_alert=True)
    await db.set_user_team(uid, None)
    await call.answer("Убран из команды")


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
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "team:new", None)
    await call.message.answer("Введите название новой команды:")
    await call.answer()


@dp.callback_query(F.data == "teams:setlead")
async def cb_team_setlead(call: CallbackQuery):
    """Handle team set lead callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "team:setlead:ask_team", None)
    teams = await db.list_teams()
    if not teams:
        await call.message.answer("Команд нет")
        return await call.answer()
    buttons = [[InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"team:choose_for_lead:{t['id']}")] for t in teams[:50]]
    await call.message.answer("Выберите команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()


@dp.callback_query(F.data.startswith("team:choose_for_lead:"))
async def cb_team_choose_for_lead(call: CallbackQuery):
    """Handle team choose for lead callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    team_id = int(call.data.split(":", 2)[2])
    await db.set_pending_action(call.from_user.id, f"team:setlead:{team_id}", None)
    await call.message.answer("Пришлите Telegram ID или @username пользователя, которого назначить лидом этой команды")
    await call.answer()


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
    members = [u for u in users if u.get("team_id") == team_id]
    non_members = [u for u in users if u.get("team_id") != team_id]
    # render lists in chunks
    if members:
        await call.message.answer("Участники:\n" + "\n".join(f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} ({u['role']})" for u in members[:50]))
    else:
        await call.message.answer("Участники: пусто")
    # controls
    add_buttons = [[InlineKeyboardButton(text=f"Добавить @{u['username'] or u['telegram_id']}", callback_data=f"team:add:{team_id}:{u['telegram_id']}")] for u in non_members[:25]]
    remove_buttons = [[InlineKeyboardButton(text=f"Убрать @{u['username'] or u['telegram_id']}", callback_data=f"team:remove:{team_id}:{u['telegram_id']}")] for u in members[:25]]
    action_buttons = [[InlineKeyboardButton(text="Обновить имена", callback_data=f"team:refresh_names:{team_id}")]]
    if add_buttons:
        await call.message.answer("Добавить в команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=add_buttons))
    if remove_buttons:
        await call.message.answer("Убрать из команды:", reply_markup=InlineKeyboardMarkup(inline_keyboard=remove_buttons))
    # refresh button
    await call.message.answer("Действия:", reply_markup=InlineKeyboardMarkup(inline_keyboard=action_buttons))
    await call.answer()


@dp.callback_query(F.data.startswith("team:refresh_names:"))
async def cb_team_refresh_names(call: CallbackQuery):
    """Handle team refresh names callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    team_id = int(call.data.split(":", 2)[2])
    users = await db.list_users()
    members = [u for u in users if u.get("team_id") == team_id]
    updated = 0
    for u in members:
        uid = int(u["telegram_id"])  # type: ignore
        try:
            chat = await bot.get_chat(uid)
            uname = chat.username or u.get("username")
            try:
                fn = getattr(chat, "first_name", None) or ""
                ln = getattr(chat, "last_name", None) or ""
                name = (fn + (" " + ln if ln else "")).strip()
                fullname = name or u.get("full_name")
            except Exception:
                fullname = u.get("full_name")
            await db.upsert_user(uid, uname, fullname)
            updated += 1
        except Exception:
            # ignore fetch errors
            pass
    await call.answer("Готово")
    await call.message.answer(f"Обновлено профилей: {updated}")


@dp.callback_query(F.data.startswith("team:add:"))
async def cb_team_add_member(call: CallbackQuery):
    """Handle team add member callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    # callback format: team:add:<team_id>:<user_id>
    _, _, team_id, uid = call.data.split(":", 3)
    # Ensure user exists and enrich with Telegram username/full_name if possible; preserve existing values
    try:
        existing = await db.get_user(int(uid))
        tg_username = None
        tg_fullname = None
        try:
            chat = await bot.get_chat(int(uid))
            tg_username = chat.username
            # Build full name from first/last if full_name not available
            try:
                fn = getattr(chat, "first_name", None) or ""
                ln = getattr(chat, "last_name", None) or ""
                name = (fn + (" " + ln if ln else "")).strip()
                tg_fullname = name or None
            except Exception:
                tg_fullname = None
        except Exception:
            # fetching chat can fail for privacy/blocked; ignore
            pass
        final_username = tg_username or (existing.get("username") if existing else None)
        final_fullname = tg_fullname or (existing.get("full_name") if existing else None)
        await db.upsert_user(int(uid), final_username, final_fullname)
    except Exception:
        # As a fallback, at least ensure a stub row exists without overwriting name fields
        try:
            await db.upsert_user(int(uid), None, None)
        except Exception:
            pass
    await db.set_user_team(int(uid), int(team_id))
    await call.answer("Добавлен")


@dp.callback_query(F.data.startswith("team:remove:"))
async def cb_team_remove_member(call: CallbackQuery):
    """Handle team remove member callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    # callback format: team:remove:<team_id>:<user_id>
    _, _, team_id, uid = call.data.split(":", 3)
    await db.set_user_team(int(uid), None)
    await call.answer("Убран")


@dp.callback_query(F.data.startswith("team:choose:"))
async def cb_team_choose(call: CallbackQuery):
    """Handle team choose callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    uid = int(call.data.split(":", 2)[2])
    teams = await db.list_teams()
    buttons = []
    for t in teams[:50]:
        buttons.append([InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"team:set:{uid}:{t['id']}")])
    buttons.append([InlineKeyboardButton(text="Убрать из команды", callback_data=f"team:set:{uid}:")])
    await call.message.answer("Выберите команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()


@dp.callback_query(F.data.startswith("team:set:"))
async def cb_team_set(call: CallbackQuery):
    """Handle team set callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    parts = call.data.split(":")
    uid = int(parts[2])
    team_id = None if len(parts) < 4 or not parts[3] else int(parts[3])
    await db.set_user_team(uid, team_id)
    await call.answer("Команда обновлена")


@dp.message(Command("createteam"))
async def on_create_team(message: Message):
    """Handle /createteam command."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /createteam <name>
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        return await message.answer("Использование: /createteam <name>")
    team_id = await db.create_team(parts[1])
    await message.answer(f"Команда создана: id={team_id}")


@dp.message(Command("setteam"))
async def on_set_team(message: Message):
    """Handle /setteam command."""
    # admin/head: назначить юзера в команду
    # /setteam <telegram_id> <team_id|-> (- означает убрать из команды)
    me = message.from_user.id
    if me not in ADMIN_IDS:
        return await message.answer("Только для админов")
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /setteam <telegram_id> <team_id|->")
    try:
        uid = await _resolve_user_id(parts[1])
    except Exception:
        return await message.answer("Не удалось распознать пользователя. Используйте numeric ID или @username")
    team_raw = parts[2]
    team_id = None if team_raw == '-' else int(team_raw)
    await db.set_user_team(uid, team_id)
    await message.answer("OK")


@dp.message(Command("listteams"))
async def on_list_teams(message: Message):
    """Handle /listteams command."""
    teams = await db.list_teams()
    if not teams:
        return await message.answer("Команд нет")
    lines = [f"#{t['id']} — {t['name']}" for t in teams]
    await message.answer("Команды:\n" + "\n".join(lines))
