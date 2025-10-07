from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums.parse_mode import ParseMode
from aiogram.client.default import DefaultBotProperties
from loguru import logger
from .config import settings
from . import db

bot = Bot(token=settings.telegram_bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

# Single dispatcher shared with FastAPI webhook
dp = Dispatcher()

ADMIN_IDS = set(settings.admins)

# Helper: resolve user reference to Telegram ID (supports numeric ID, @username, tg://user?id=...)
async def _resolve_user_id(identifier: str) -> int:
    s = (identifier or "").strip()
    # tg://user?id=123
    if s.startswith("tg://user?id="):
        value = s.split("=", 1)[1]
        return int(value)
    # @username
    if s.startswith("@"):
        uname = s[1:].strip().lower()
        users = await db.list_users()
        hit = next((u for u in users if (u.get("username") or "").lower() == uname), None)
        if not hit:
            raise ValueError("username_not_found")
        return int(hit["telegram_id"])  # type: ignore
    # numeric id
    return int(s)
def main_menu(is_admin: bool, role: str | None = None) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="Кто я", callback_data="menu:whoami"), InlineKeyboardButton(text="Правила", callback_data="menu:listroutes")],
    ]
    if is_admin:
        buttons += [
            [InlineKeyboardButton(text="Пользователи", callback_data="menu:listusers"), InlineKeyboardButton(text="Управление", callback_data="menu:manage")],
            [InlineKeyboardButton(text="Команды", callback_data="menu:teams"), InlineKeyboardButton(text="Алиасы", callback_data="menu:aliases")],
        ]
    else:
        # For lead/head expose 'Моя команда'
        if role in ("lead", "head"):
            buttons += [[InlineKeyboardButton(text="Моя команда", callback_data="menu:myteam")]]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# Helpers used by inline menu to avoid using call.message.from_user (which is the bot)
async def _send_whoami(chat_id: int, user_id: int, username: str | None):
    await bot.send_message(chat_id, f"Ваш Telegram ID: <code>{user_id}</code>\nUsername: @{username or '-'}")

async def _send_list_users(chat_id: int, actor_id: int):
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = my["role"] if my else "buyer"
    if actor_id in ADMIN_IDS:
        my_role = "admin"
    my_team = my.get("team_id") if my else None
    visible = []
    for u in users:
        if my_role in ("admin", "head"):
            visible.append(u)
        elif my_role == "lead":
            if u.get("team_id") == my_team:
                visible.append(u)
        else:
            if u["telegram_id"] == actor_id:
                visible.append(u)
    if not visible:
        return await bot.send_message(chat_id, "Нет данных для отображения")
    lines = []
    for u in visible:
        display_role = u['role']
        if u['telegram_id'] == actor_id and actor_id in ADMIN_IDS:
            display_role = 'admin'
        lines.append(f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} — {u['full_name'] or ''} | role={display_role} | team={u['team_id'] or '-'}")
    await bot.send_message(chat_id, "Пользователи:\n" + "\n".join(lines))

async def _send_list_routes(chat_id: int, actor_id: int):
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = (my or {}).get("role", "buyer")
    if actor_id in ADMIN_IDS:
        my_role = "admin"
    my_team = (my or {}).get("team_id")
    rows = await db.list_routes()
    def visible(r: dict) -> bool:
        if my_role in ("admin", "head"):
            return True
        if my_role == "lead":
            ru = next((u for u in users if u["telegram_id"] == r["user_id"]), None)
            return ru and ru.get("team_id") == my_team
        return r["user_id"] == actor_id
    vis = [r for r in rows if visible(r)]
    if not vis:
        return await bot.send_message(chat_id, "Правил нет или нет доступа")
    def fmt(r):
        return f"#{r['id']} -> <code>{r['user_id']}</code> (@{r['username'] or '-'}) | offer={r['offer'] or '*'} | geo={r['country'] or '*'} | src={r['source'] or '*'} | prio={r['priority']}"
    await bot.send_message(chat_id, "Правила:\n" + "\n".join(fmt(r) for r in vis))

async def _send_manage(chat_id: int, actor_id: int):
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    users = await db.list_users()
    if not users:
        return await bot.send_message(chat_id, "Пока нет пользователей, попросите нажать /start")
    for u in users[:25]:
        text = f"<b>{u['full_name'] or '-'}</b> @{u['username'] or '-'}\nID: <code>{u['telegram_id']}</code>\nRole: <code>{u['role']}</code> | Team: <code>{u['team_id'] or '-'}</code> | Active: <code>{'yes' if u['is_active'] else 'no'}</code>"
        await bot.send_message(chat_id, text, reply_markup=_user_row_controls(u))

async def _send_aliases(chat_id: int, actor_id: int):
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    rows = await db.list_aliases()
    if not rows:
        await bot.send_message(chat_id, "Алиасов пока нет.")
    else:
        for r in rows[:25]:
            text = f"<b>{r['alias']}</b> → buyer={r['buyer_id'] or '-'} | lead={r['lead_id'] or '-'}"
            await bot.send_message(chat_id, text, reply_markup=alias_row_controls(r['alias'], r['buyer_id'], r['lead_id']))
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Добавить алиас", callback_data="alias:new")]])
    await bot.send_message(chat_id, "Управление алиасами:", reply_markup=kb)

# --- Lead/Head: My team management ---
def _myteam_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Состав команды", callback_data="myteam:list")],
        [InlineKeyboardButton(text="Добавить по ID", callback_data="myteam:add")],
        [InlineKeyboardButton(text="Убрать участника", callback_data="myteam:remove")],
    ])

async def _send_myteam(chat_id: int, actor_id: int):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == actor_id), None)
    role = (me or {}).get("role")
    if (actor_id not in ADMIN_IDS) and (role not in ("lead", "head")):
        return await bot.send_message(chat_id, "Недостаточно прав")
    if not me or not me.get("team_id"):
        return await bot.send_message(chat_id, "Вы не состоите в команде")
    await bot.send_message(chat_id, "Моя команда — управление", reply_markup=_myteam_menu())

@dp.callback_query(F.data == "myteam:list")
async def cb_myteam_list(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    if not me or (me["role"] not in ("lead", "head") and call.from_user.id not in ADMIN_IDS):
        return await call.answer("Нет прав", show_alert=True)
    team_id = me.get("team_id")
    if not team_id:
        await call.message.answer("У вас нет команды")
        return await call.answer()
    members = [u for u in users if u.get("team_id") == team_id]
    if not members:
        await call.message.answer("Состав пуст")
    else:
        lines = [f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} ({u['role']})" for u in members]
        await call.message.answer("Состав команды:\n" + "\n".join(lines))
    await call.answer()

@dp.callback_query(F.data == "myteam:add")
async def cb_myteam_add(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    if not me or (me["role"] not in ("lead", "head") and call.from_user.id not in ADMIN_IDS):
        return await call.answer("Нет прав", show_alert=True)
    if not me.get("team_id"):
        return await call.answer("Нет команды", show_alert=True)
    await db.set_pending_action(call.from_user.id, "myteam:add", None)
    await call.message.answer("Пришлите Telegram ID пользователя для добавления в вашу команду")
    await call.answer()

@dp.callback_query(F.data == "myteam:remove")
async def cb_myteam_remove(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    if not me or (me["role"] not in ("lead", "head") and call.from_user.id not in ADMIN_IDS):
        return await call.answer("Нет прав", show_alert=True)
    team_id = me.get("team_id")
    if not team_id:
        return await call.answer("Нет команды", show_alert=True)
    members = [u for u in users if u.get("team_id") == team_id]
    if not members:
        await call.message.answer("Состав пуст")
        return await call.answer()
    buttons = [[InlineKeyboardButton(text=f"Убрать @{u['username'] or u['telegram_id']}", callback_data=f"myteam:remove:{u['telegram_id']}")] for u in members[:25]]
    await call.message.answer("Кого убрать?", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()

@dp.callback_query(F.data.startswith("myteam:remove:"))
async def cb_myteam_remove_user(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    if not me or (me["role"] not in ("lead", "head") and call.from_user.id not in ADMIN_IDS):
        return await call.answer("Нет прав", show_alert=True)
    team_id = me.get("team_id")
    uid = int(call.data.split(":", 2)[2])
    # ensure target is in same team
    target = next((u for u in users if u["telegram_id"] == uid), None)
    if not target or target.get("team_id") != team_id:
        return await call.answer("Можно убирать только из своей команды", show_alert=True)
    await db.set_user_team(uid, None)
    await call.answer("Убран из команды")
# --- Teams management (admin) ---
def _teams_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список команд", callback_data="teams:list"), InlineKeyboardButton(text="Создать команду", callback_data="teams:new")],
        [InlineKeyboardButton(text="Назначить лида", callback_data="teams:setlead")],
        [InlineKeyboardButton(text="Участники", callback_data="teams:members")],
    ])

async def _send_teams(chat_id: int, actor_id: int):
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    await bot.send_message(chat_id, "Команды — управление", reply_markup=_teams_menu())

@dp.callback_query(F.data == "teams:list")
async def cb_teams_list(call: CallbackQuery):
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
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "team:new", None)
    await call.message.answer("Введите название новой команды:")
    await call.answer()

@dp.callback_query(F.data == "teams:setlead")
async def cb_team_setlead(call: CallbackQuery):
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
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    team_id = int(call.data.split(":", 2)[2])
    await db.set_pending_action(call.from_user.id, f"team:setlead:{team_id}", None)
    await call.message.answer("Пришлите Telegram ID или @username пользователя, которого назначить лидом этой команды")
    await call.answer()

@dp.callback_query(F.data == "teams:members")
async def cb_team_members(call: CallbackQuery):
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
    if add_buttons:
        await call.message.answer("Добавить в команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=add_buttons))
    if remove_buttons:
        await call.message.answer("Убрать из команды:", reply_markup=InlineKeyboardMarkup(inline_keyboard=remove_buttons))
    await call.answer()

@dp.callback_query(F.data.startswith("team:add:"))
async def cb_team_add_member(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    # callback format: team:add:<team_id>:<user_id>
    _, _, team_id, uid = call.data.split(":", 3)
    # ensure user exists in DB (stub if needed)
    try:
        await db.upsert_user(int(uid), None, None)
    except Exception:
        pass
    await db.set_user_team(int(uid), int(team_id))
    await call.answer("Добавлен")

@dp.callback_query(F.data.startswith("team:remove:"))
async def cb_team_remove_member(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    # callback format: team:remove:<team_id>:<user_id>
    _, _, team_id, uid = call.data.split(":", 3)
    await db.set_user_team(int(uid), None)
    await call.answer("Убран")

@dp.message(Command("menu"))
async def on_menu(message: Message):
    is_admin = message.from_user.id in ADMIN_IDS
    # get role to expose lead/head specific menu
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == message.from_user.id), None)
    role = (me or {}).get("role")
    if is_admin:
        role = "admin"
    await message.answer("Меню:", reply_markup=main_menu(is_admin, role))

@dp.callback_query(F.data.startswith("menu:"))
async def on_menu_click(call: CallbackQuery):
    key = call.data.split(":",1)[1]
    if key == "whoami":
        await _send_whoami(call.message.chat.id, call.from_user.id, call.from_user.username)
        return await call.answer()
    if key == "listroutes":
        await _send_list_routes(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "listusers":
        await _send_list_users(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "manage":
        await _send_manage(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "teams":
        await _send_teams(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "aliases":
        await _send_aliases(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "myteam":
        await _send_myteam(call.message.chat.id, call.from_user.id)
        return await call.answer()

@dp.message(CommandStart())
async def on_start(message: Message):
    await db.upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    # Автоповышение роли для ID из ADMINS
    if message.from_user.id in ADMIN_IDS:
        try:
            await db.set_user_role(message.from_user.id, "admin")
        except Exception:
            pass
    await message.answer("Привет! Ты зарегистрирован. Роль по умолчанию: buyer (если не админ). Админ может изменить роль и добавить правила.")

@dp.message(Command("help"))
async def on_help(message: Message):
    await message.answer(
        "Доступные команды:\n"
        "/start — регистрация\n"
        "/help — помощь\n"
        "/ping — проверка связи (pong)\n"
        "/whoami — показать свой Telegram ID\n"
        "/addrule — добавить правило (админ/хэд)\n"
        "/listusers — список пользователей (зависит от роли)\n"
        "/listroutes — список правил (видимость по роли)\n"
        "/setrole — назначить роль (admin)\n"
        "/createteam — создать команду (admin)\n"
        "/setteam — назначить пользователя в команду (admin/head)\n"
        "/listteams — список команд\n"
        "/aliases — алиасы (admin): связать campaign_name с buyer/lead"
    )

@dp.message(Command("ping"))
async def on_ping(message: Message):
    await message.answer("pong")

@dp.message(Command("whoami"))
async def on_whoami(message: Message):
    uid = message.from_user.id
    uname = message.from_user.username
    await message.answer(f"Ваш Telegram ID: <code>{uid}</code>\nUsername: @{uname or '-'}")
@dp.message(Command("listusers"))
async def on_list_users(message: Message):
    me = message.from_user.id
    users = await db.list_users()
    # role-based visibility: admin sees all; head sees all; lead sees their team; buyer sees only self
    visible = []
    # get my role and team
    my = next((u for u in users if u["telegram_id"] == me), None)
    my_role = my["role"] if my else "buyer"
    if me in ADMIN_IDS:
        my_role = "admin"
    my_team = my.get("team_id") if my else None
    for u in users:
        if my_role in ("admin", "head"):
            visible.append(u)
        elif my_role == "lead":
            if u.get("team_id") == my_team:
                visible.append(u)
        else:  # buyer
            if u["telegram_id"] == me:
                visible.append(u)
    if not visible:
        return await message.answer("Нет данных для отображения")
    rendered = []
    for u in visible:
        display_role = u['role']
        if u['telegram_id'] == me and me in ADMIN_IDS:
            display_role = 'admin'
        rendered.append(f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} — {u['full_name'] or ''} | role={display_role} | team={u['team_id'] or '-'}")
    lines = rendered
    await message.answer("Пользователи:\n" + "\n".join(lines))

def _user_row_controls(u: dict) -> InlineKeyboardMarkup:
    uid = u["telegram_id"]
    role = u["role"]
    is_active = u["is_active"]
    buttons = [
        [InlineKeyboardButton(text="buyer", callback_data=f"role:{uid}:buyer"),
         InlineKeyboardButton(text="lead", callback_data=f"role:{uid}:lead"),
         InlineKeyboardButton(text="head", callback_data=f"role:{uid}:head"),
         InlineKeyboardButton(text="admin", callback_data=f"role:{uid}:admin")],
        [InlineKeyboardButton(text=("Deactivate" if is_active else "Activate"), callback_data=f"active:{uid}:{0 if is_active else 1}")],
        [InlineKeyboardButton(text="Set team", callback_data=f"team:choose:{uid}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("manage"))
async def on_manage(message: Message):
    # Only admins (для MVP) видят управление
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    users = await db.list_users()
    if not users:
        return await message.answer("Пока нет пользователей, попросите нажать /start")
    # Покажем по одному пользователю за сообщение для наглядности
    for u in users[:25]:  # не спамим много
        text = f"<b>{u['full_name'] or '-'}</b> @{u['username'] or '-'}\nID: <code>{u['telegram_id']}</code>\nRole: <code>{u['role']}</code> | Team: <code>{u['team_id'] or '-'}</code> | Active: <code>{'yes' if u['is_active'] else 'no'}</code>"
        await message.answer(text, reply_markup=_user_row_controls(u))

def alias_row_controls(alias: str, buyer_id: int | None, lead_id: int | None) -> InlineKeyboardMarkup:
    a = alias
    buttons = [
        [InlineKeyboardButton(text=f"Set buyer ({buyer_id or '-'})", callback_data=f"alias:setbuyer:{a}")],
        [InlineKeyboardButton(text=f"Set lead ({lead_id or '-'})", callback_data=f"alias:setlead:{a}")],
        [InlineKeyboardButton(text="Delete", callback_data=f"alias:delete:{a}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("aliases"))
async def on_aliases(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    rows = await db.list_aliases()
    if not rows:
        await message.answer("Алиасов пока нет.")
    else:
        for r in rows[:25]:
            text = f"<b>{r['alias']}</b> → buyer={r['buyer_id'] or '-'} | lead={r['lead_id'] or '-'}"
            await message.answer(text, reply_markup=alias_row_controls(r['alias'], r['buyer_id'], r['lead_id']))
    # кнопка для создания нового алиаса
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Добавить алиас", callback_data="alias:new")]])
    await message.answer("Управление алиасами:", reply_markup=kb)

@dp.message(Command("setalias"))
async def on_setalias(message: Message):
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
            v = p.split("=",1)[1]
            buyer_id = None if v == '-' else int(v)
        if p.startswith("lead="):
            v = p.split("=",1)[1]
            lead_id = None if v == '-' else int(v)
    await db.set_alias(alias, buyer_id, lead_id)
    await message.answer("Алиас сохранён")

@dp.message(Command("delalias"))
async def on_delalias(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("Использование: /delalias <alias>")
    await db.delete_alias(parts[1])
    await message.answer("Алиас удалён")

@dp.callback_query(F.data == "alias:new")
async def cb_alias_new(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "alias:new", None)
    await call.message.answer("Введите имя алиаса (префикс campaign_name до _):")
    await call.answer()

@dp.callback_query(F.data.startswith("alias:setbuyer:"))
async def cb_alias_setbuyer(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.set_pending_action(call.from_user.id, f"alias:setbuyer:{alias}", None)
    await call.message.answer(f"Пришлите Telegram ID или @username покупателя для алиаса {alias}, или '-' чтобы убрать")
    await call.answer()

@dp.callback_query(F.data.startswith("alias:setlead:"))
async def cb_alias_setlead(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.set_pending_action(call.from_user.id, f"alias:setlead:{alias}", None)
    await call.message.answer(f"Пришлите Telegram ID или @username лида для алиаса {alias}, или '-' чтобы убрать")
    await call.answer()

@dp.callback_query(F.data.startswith("alias:delete:"))
async def cb_alias_delete(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.delete_alias(alias)
    await call.message.edit_text(f"Алиас {alias} удалён")
    await call.answer()

@dp.callback_query(F.data.startswith("team:choose:"))
async def cb_team_choose(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    uid = int(call.data.split(":", 2)[2])
    teams = await db.list_teams()
    buttons = []
    for t in teams[:50]:
        buttons.append([InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"team:set:{uid}:{t['id']}")])
    buttons.append([InlineKeyboardButton(text="Удалить из команды", callback_data=f"team:set:{uid}:-")])
    await call.message.answer("Выберите команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()

@dp.callback_query(F.data.startswith("team:set:"))
async def cb_team_set(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, _, uid, team_raw = call.data.split(":", 3)
    team_id = None if team_raw == '-' else int(team_raw)
    await db.set_user_team(int(uid), team_id)
    await call.answer("Команда обновлена")

@dp.message()
async def on_text_fallback(message: Message):
    # обработка pending actions для алиасов и команд
    pending = await db.get_pending_action(message.from_user.id)
    if not pending:
        return  # игнорируем обычные сообщения, чтобы не засорять чат
    action, _ = pending
    try:
        if action == "alias:new":
            alias = message.text.strip()
            await db.set_alias(alias)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Алиас создан. Откройте Алиасы в меню, чтобы назначить buyer/lead")
        if action.startswith("alias:setbuyer:"):
            alias = action.split(":", 2)[2]
            v = message.text.strip()
            if v == '-':
                buyer_id = None
            else:
                try:
                    buyer_id = await _resolve_user_id(v)
                except ValueError:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username. Если пользователь не писал боту, попросите его отправить /start.")
            await db.set_alias(alias, buyer_id=buyer_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Buyer назначен")
        if action.startswith("alias:setlead:"):
            alias = action.split(":", 2)[2]
            v = message.text.strip()
            if v == '-':
                lead_id = None
            else:
                try:
                    lead_id = await _resolve_user_id(v)
                except ValueError:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username. Если пользователь не писал боту, попросите его отправить /start.")
            await db.set_alias(alias, lead_id=lead_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Lead назначен")
        if action == "team:new":
            name = message.text.strip()
            tid = await db.create_team(name)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer(f"Команда создана: id={tid}")
        if action.startswith("team:setlead:"):
            # format: team:setlead:<team_id>
            team_id = int(action.split(":", 2)[2])
            v = message.text.strip()
            uid = None
            # support tg://user?id=123
            if v.startswith("tg://user?id="):
                try:
                    uid = int(v.split("=",1)[1])
                except Exception:
                    uid = None
            # support @username
            if uid is None and v.startswith("@"):
                uname = v[1:].strip().lower()
                users = await db.list_users()
                hit = next((u for u in users if (u.get("username") or "").lower() == uname), None)
                if hit:
                    uid = int(hit["telegram_id"])  # type: ignore
            # fallback to numeric ID
            if uid is None:
                try:
                    uid = int(v)
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Не удалось распознать пользователя. Пришлите numeric Telegram ID или @username. Если пользователь не писал боту, попросите его отправить /start.")
            # ensure user exists, set user's team and elevate role to lead
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_team(uid, team_id)
            await db.set_user_role(uid, "lead")
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Лид назначен")
        if action == "myteam:add":
            # leads can add only to their own team
            users = await db.list_users()
            me = next((u for u in users if u["telegram_id"] == message.from_user.id), None)
            if not me or me.get("role") not in ("lead", "head"):
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Нет прав")
            team_id = me.get("team_id")
            if not team_id:
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("У вас нет команды")
            v = message.text.strip()
            uid = None
            if v.startswith("tg://user?id="):
                try:
                    uid = int(v.split("=",1)[1])
                except Exception:
                    uid = None
            if uid is None and v.startswith("@"):
                uname = v[1:].strip().lower()
                hit = next((u for u in users if (u.get("username") or "").lower() == uname), None)
                if hit:
                    uid = int(hit["telegram_id"])  # type: ignore
            if uid is None:
                try:
                    uid = int(v)
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username. Если пользователь не писал боту, попросите его отправить /start.")
            # Ensure target exists (create stub if not) before assigning team
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_team(uid, team_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Пользователь добавлен в вашу команду")
    except Exception as e:
        logger.exception(e)
        return await message.answer("Ошибка обработки ввода")

@dp.callback_query(F.data.startswith("role:"))
async def cb_set_role(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, uid, role = call.data.split(":", 2)
    await db.set_user_role(int(uid), role)
    u = await db.get_user(int(uid))
    if u:
        await call.message.edit_reply_markup(reply_markup=_user_row_controls(u))
        await call.answer("Роль обновлена")
    else:
        await call.answer("Пользователь не найден", show_alert=True)

@dp.callback_query(F.data.startswith("active:"))
async def cb_set_active(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, uid, active = call.data.split(":", 2)
    await db.set_user_active(int(uid), bool(int(active)))
    u = await db.get_user(int(uid))
    if u:
        await call.message.edit_reply_markup(reply_markup=_user_row_controls(u))
        await call.answer("Статус обновлен")
    else:
        await call.answer("Пользователь не найден", show_alert=True)

@dp.message(Command("listroutes"))
async def on_list_routes(message: Message):
    me = message.from_user.id
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == me), None)
    my_role = (my or {}).get("role", "buyer")
    if me in ADMIN_IDS:
        my_role = "admin"
    my_team = (my or {}).get("team_id")
    rows = await db.list_routes()
    # filter by role
    def visible(r: dict) -> bool:
        if my_role in ("admin", "head"):
            return True
        if my_role == "lead":
            # find route's user and compare team
            ru = next((u for u in users if u["telegram_id"] == r["user_id"]), None)
            return ru and ru.get("team_id") == my_team
        # buyer: only own
        return r["user_id"] == me
    vis = [r for r in rows if visible(r)]
    if not vis:
        return await message.answer("Правил нет или нет доступа")
    def fmt(r):
        return f"#{r['id']} -> <code>{r['user_id']}</code> (@{r['username'] or '-'}) | offer={r['offer'] or '*'} | geo={r['country'] or '*'} | src={r['source'] or '*'} | prio={r['priority']}"
    await message.answer("Правила:\n" + "\n".join(fmt(r) for r in vis))

@dp.message(Command("addrule"))
async def on_add_rule(message: Message):
    # Разрешено admin/head. Format: /addrule user_id [offer=*] [country=*] [source=*] [priority=0]
    try:
        parts = message.text.split()
        if len(parts) < 2:
            raise ValueError
        user_id = int(parts[1])
        kwargs = {"offer": None, "country": None, "source": None, "priority": 0}
        for p in parts[2:]:
            if "=" in p:
                k, v = p.split("=", 1)
                if k in ("offer", "country", "source"):
                    kwargs[k] = None if v == "*" else v
                elif k == "priority":
                    kwargs["priority"] = int(v)
        # permissions
        users = await db.list_users()
        me = message.from_user.id
        my = next((u for u in users if u["telegram_id"] == me), None)
        my_role = (my or {}).get("role", "buyer")
        my_team = (my or {}).get("team_id")
        if my_role not in ("admin", "head") and me not in ADMIN_IDS:
            return await message.answer("Недостаточно прав (нужна роль admin/head)")
        if my_role == "head":
            target = next((u for u in users if u["telegram_id"] == user_id), None)
            if not target or target.get("team_id") != my_team:
                return await message.answer("Можно добавлять правила только для своей команды")
        rid = await db.add_route(user_id, kwargs["offer"], kwargs["country"], kwargs["source"], kwargs["priority"])
        await message.answer(f"OK, создано правило #{rid}")
    except Exception as e:
        logger.exception(e)
        await message.answer("Использование: /addrule <user_id> offer=OFF|* country=RU|* source=FB|* priority=0")

@dp.message(Command("setrole"))
async def on_set_role(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /setrole <telegram_id> <buyer|lead|head|admin>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /setrole <telegram_id> <buyer|lead|head|admin>")
    try:
        uid = await _resolve_user_id(parts[1])
        role = parts[2]
        await db.set_user_role(uid, role)
        await message.answer("OK")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка установки роли")

@dp.message(Command("createteam"))
async def on_create_team(message: Message):
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
    teams = await db.list_teams()
    if not teams:
        return await message.answer("Команд нет")
    lines = [f"#{t['id']} — {t['name']}" for t in teams]
    await message.answer("Команды:\n" + "\n".join(lines))

async def notify_buyer(buyer_id: int, text: str):
    try:
        await bot.send_message(chat_id=buyer_id, text=text)
    except Exception as e:
        logger.warning(f"Failed to notify buyer {buyer_id}: {e}")
