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
        [InlineKeyboardButton(text="Отчеты", callback_data="menu:reports"), InlineKeyboardButton(text="KPI", callback_data="menu:kpi")],
    ]
    if is_admin:
        buttons += [
            [InlineKeyboardButton(text="Пользователи", callback_data="menu:listusers"), InlineKeyboardButton(text="Управление", callback_data="menu:manage")],
            [InlineKeyboardButton(text="Команды", callback_data="menu:teams"), InlineKeyboardButton(text="Алиасы", callback_data="menu:aliases")],
            [InlineKeyboardButton(text="Менторы", callback_data="menu:mentors")],
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
    if key == "mentors":
        await _send_mentors(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "myteam":
        await _send_myteam(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "reports":
        await _send_reports_menu(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "kpi":
        await _send_kpi_menu(call.message.chat.id, call.from_user.id)
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
        "/aliases — алиасы (admin): связать campaign_name с buyer/lead\n"
        "/addmentor — назначить роль mentor (admin)\n"
        "/mentor_follow — подписать ментора на команду (admin)\n"
        "/mentor_unfollow — отписать ментора от команды (admin)"
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
         InlineKeyboardButton(text="admin", callback_data=f"role:{uid}:admin"),
         InlineKeyboardButton(text="mentor", callback_data=f"role:{uid}:mentor")],
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

# ===== Mentors management (admin) =====
def _mentor_row_controls(mentor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подписки", callback_data=f"mentor:subs:{mentor_id}")],
        [InlineKeyboardButton(text="Снять роль", callback_data=f"mentor:unset:{mentor_id}")]
    ])

def _mentor_add_controls() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить ментора", callback_data="mentor:add")]
    ])

async def _send_mentors(chat_id: int, actor_id: int):
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
    rows = []
    for t in teams[:50]:
        tid = int(t['id'])
        mark = "✅" if tid in followed else "➕"
        rows.append([InlineKeyboardButton(text=f"{mark} #{tid} {t['name']}", callback_data=f"mentor:toggle:{mentor_id}:{tid}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="mentor:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "mentor:add")
async def cb_mentor_add(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "mentor:add", None)
    await call.message.answer("Пришлите Telegram ID или @username пользователя, которому назначить роль mentor")
    await call.answer()

@dp.callback_query(F.data.startswith("mentor:unset:"))
async def cb_mentor_unset(call: CallbackQuery):
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
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await _send_mentors(call.message.chat.id, call.from_user.id)
    await call.answer()

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
        if action == "mentor:add":
            v = message.text.strip()
            try:
                uid = await _resolve_user_id(v)
            except Exception:
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username.")
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_role(uid, "mentor")
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Назначен ментором")
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
        if action.startswith("kpi:set:"):
            which = action.split(":", 2)[2]
            v = message.text.strip()
            goal_val = None
            if v != '-':
                try:
                    goal_val = int(v)
                    if goal_val < 0:
                        goal_val = 0
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Нужно целое число или '-' для очистки")
            current = await db.get_kpi(message.from_user.id)
            daily = current.get('daily_goal')
            weekly = current.get('weekly_goal')
            if which == 'daily':
                daily = goal_val
            else:
                weekly = goal_val
            await db.set_kpi(message.from_user.id, daily_goal=daily, weekly_goal=weekly)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("KPI обновлен")
        if action.startswith("report:filter:"):
            which = action.split(":", 2)[2]
            val = message.text.strip()
            cur = await db.get_report_filter(message.from_user.id)
            offer = cur.get('offer')
            creative = cur.get('creative')
            buyer_id = cur.get('buyer_id')
            team_id = cur.get('team_id')
            if which == 'offer':
                offer = None if val == '-' else val
            elif which == 'creative':
                creative = None if val == '-' else val
            elif which == 'buyer':
                if val == '-':
                    buyer_id = None
                else:
                    try:
                        uid = await _resolve_user_id(val)
                        buyer_id = uid
                    except Exception:
                        await db.clear_pending_action(message.from_user.id)
                        return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username.")
            elif which == 'team':
                if val == '-':
                    team_id = None
                else:
                    try:
                        team_id = int(val)
                    except Exception:
                        await db.clear_pending_action(message.from_user.id)
                        return await message.answer("Неверный формат. Нужен ID команды.")
            await db.set_report_filter(message.from_user.id, offer, creative, buyer_id=buyer_id, team_id=team_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Фильтр сохранен")
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
    # /setrole <telegram_id> <buyer|lead|head|admin|mentor>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /setrole <telegram_id> <buyer|lead|head|admin|mentor>")
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

# ===== Reports =====
def _reports_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сегодня", callback_data="report:today"), InlineKeyboardButton(text="Вчера", callback_data="report:yesterday")],
        [InlineKeyboardButton(text="Неделя", callback_data="report:week")],
        [InlineKeyboardButton(text="Выбрать оффер", callback_data="report:pick:offer"), InlineKeyboardButton(text="Выбрать крео", callback_data="report:pick:creative")],
        [InlineKeyboardButton(text="Выбрать байера", callback_data="report:pick:buyer"), InlineKeyboardButton(text="Выбрать команду", callback_data="report:pick:team")],
        [InlineKeyboardButton(text="Сбросить фильтры", callback_data="report:f:clear")],
    ])

async def _send_reports_menu(chat_id: int, actor_id: int):
    await bot.send_message(chat_id, "Отчеты — выберите период:", reply_markup=_reports_menu())

async def _resolve_scope_user_ids(actor_id: int) -> list[int]:
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = (me or {}).get("role", "buyer")
    if actor_id in ADMIN_IDS:
        my_role = "admin"
    if my_role in ("admin", "head"):
        return [int(u["telegram_id"]) for u in users if u.get("is_active")]
    if my_role == "lead":
        team_id = me.get("team_id") if me else None
        return [int(u["telegram_id"]) for u in users if u.get("team_id") == team_id and u.get("is_active")]
    if my_role == "mentor":
        # aggregate all users from teams the mentor follows
        team_ids = set(await db.list_mentor_teams(actor_id))
        ids = [int(u["telegram_id"]) for u in users if (u.get("team_id") in team_ids) and u.get("is_active")]
        # include own id as well
        if actor_id not in ids:
            ids.append(actor_id)
        return ids
    return [actor_id]

def _report_text(title: str, agg: dict) -> str:
    lines = [f"📊 <b>{title}</b>"]
    lines.append(f"📈 Депозитов: <b>{agg.get('count',0)}</b>")
    profit = agg.get('profit', 0.0)
    lines.append(f"💰 Профит: <b>{int(round(profit))}</b>")
    total = agg.get('total', 0)
    if total:
        cr = (agg.get('count',0) / total) * 100.0
        lines.append(f"🎯 CR: <b>{cr:.1f}%</b> (из {total})")
    if agg.get('top_offer'):
        lines.append(f"🏆 Топ-оффер: <code>{agg['top_offer']}</code>")
    if agg.get('geo_dist'):
        # filter out unknown entries
        geo_items = [(k, v) for k, v in agg['geo_dist'].items() if k and k != '-' ]
        if geo_items:
            geos = ", ".join(f"{k}:{v}" for k, v in geo_items[:5])
            lines.append(f"🌍 Гео: {geos}")
    # replace sources with top creatives
    if agg.get('creative_dist'):
        cr_items = [(k, v) for k, v in agg['creative_dist'].items() if k and str(k).strip()]
        if cr_items:
            crs = ", ".join(f"{k}:{v}" for k, v in cr_items[:5])
            lines.append(f"🎬 Креативы: {crs}")
    return "\n".join(lines)

async def _send_period_report(chat_id: int, actor_id: int, title: str, days: int | None = None, yesterday: bool = False):
    from datetime import datetime, timezone, timedelta
    users = await db.list_users()
    user_ids = await _resolve_scope_user_ids(actor_id)
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    if yesterday:
        end = start
        start = end - timedelta(days=1)
    if days is not None:
        start = (now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days-1))
        end = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    filt = await db.get_report_filter(actor_id)
    filter_user_ids: list[int] | None = None
    if filt.get('buyer_id') or filt.get('team_id'):
        me = next((u for u in users if u["telegram_id"] == actor_id), None)
        role = (me or {}).get("role", "buyer")
        if actor_id in ADMIN_IDS:
            role = "admin"
        allowed_ids = set(user_ids)
        if filt.get('buyer_id'):
            bid = int(filt['buyer_id'])
            filter_user_ids = [bid] if bid in allowed_ids else []
        elif filt.get('team_id'):
            tid = int(filt['team_id'])
            team_ids = [int(u['telegram_id']) for u in users if u.get('team_id') == tid and u.get('is_active')]
            filter_user_ids = [uid for uid in team_ids if uid in allowed_ids]
    agg = await db.aggregate_sales(user_ids, start, end, offer=filt.get('offer'), creative=filt.get('creative'), filter_user_ids=filter_user_ids)
    text = _report_text(title, agg)
    if days == 7 and not yesterday:
        trend = await db.trend_daily_sales(user_ids, days=7)
        if trend:
            tline = ", ".join(f"{d.split('-')[-1]}:{c}" for d, c in trend)
            text += f"\n📅 Тренд (7д): {tline}"
    if filt.get('offer') or filt.get('creative') or filt.get('buyer_id') or filt.get('team_id'):
        fparts = []
        if filt.get('offer'):
            fparts.append(f"offer=<code>{filt['offer']}</code>")
        if filt.get('creative'):
            fparts.append(f"creative=<code>{filt['creative']}</code>")
        if filt.get('buyer_id'):
            fparts.append(f"buyer=<code>{filt['buyer_id']}</code>")
        if filt.get('team_id'):
            fparts.append(f"team=<code>{filt['team_id']}</code>")
        text += "\n🔎 Фильтры: " + ", ".join(fparts)
    await bot.send_message(chat_id, text, reply_markup=_reports_menu())

@dp.callback_query(F.data == "report:today")
async def cb_report_today(call: CallbackQuery):
    await _send_period_report(call.message.chat.id, call.from_user.id, "Сегодня", None, False)
    await call.answer()

@dp.callback_query(F.data == "report:yesterday")
async def cb_report_yesterday(call: CallbackQuery):
    await _send_period_report(call.message.chat.id, call.from_user.id, "Вчера", None, True)
    await call.answer()

@dp.callback_query(F.data == "report:week")
async def cb_report_week(call: CallbackQuery):
    await _send_period_report(call.message.chat.id, call.from_user.id, "Последние 7 дней", 7, False)
    await call.answer()

@dp.message(Command("today"))
async def on_today(message: Message):
    await _send_period_report(message.chat.id, message.from_user.id, "Сегодня")

@dp.message(Command("yesterday"))
async def on_yesterday(message: Message):
    await _send_period_report(message.chat.id, message.from_user.id, "Вчера", None, True)

@dp.message(Command("week"))
async def on_week(message: Message):
    await _send_period_report(message.chat.id, message.from_user.id, "Последние 7 дней", 7)

@dp.callback_query(F.data.startswith("report:f:"))
async def cb_report_filter(call: CallbackQuery):
    _, _, key = call.data.split(":", 2)
    if key == "clear":
        await db.clear_report_filter(call.from_user.id)
        await call.message.answer("Фильтры сброшены")
        await call.answer()
        return
    await call.answer()

def _teams_picker_kb(teams: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for t in teams[:50]:
        rows.append([InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"report:set:team:{t['id']}")])
    rows.append([InlineKeyboardButton(text="Очистить", callback_data="report:set:team:-")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _buyers_picker_kb(users: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for u in users[:50]:
        cap = f"@{u['username'] or u['telegram_id']} ({u['full_name'] or ''})"
        rows.append([InlineKeyboardButton(text=cap, callback_data=f"report:set:buyer:{u['telegram_id']}")])
    rows.append([InlineKeyboardButton(text="Очистить", callback_data="report:set:buyer:-")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _offers_picker_kb(offers: list[str]) -> InlineKeyboardMarkup:
    rows = []
    for o in offers[:50]:
        cap = o[:60] if o else "(пусто)"
        rows.append([InlineKeyboardButton(text=cap, callback_data=f"report:set:offer:{o}")])
    rows.append([InlineKeyboardButton(text="Очистить", callback_data="report:set:offer:-")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _creatives_picker_kb(creatives: list[str]) -> InlineKeyboardMarkup:
    rows = []
    for c in creatives[:50]:
        cap = c[:60] if c else "(пусто)"
        rows.append([InlineKeyboardButton(text=cap, callback_data=f"report:set:creative:{c}")])
    rows.append([InlineKeyboardButton(text="Очистить", callback_data="report:set:creative:-")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "report:pick:team")
async def cb_report_pick_team(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    role = (me or {}).get("role", "buyer")
    if call.from_user.id in ADMIN_IDS:
        role = "admin"
    teams = await db.list_teams()
    allowed_team_ids: set[int] = set()
    if role == "admin" or role == "head":
        allowed_team_ids = {int(t['id']) for t in teams}
    elif role == "lead":
        if me and me.get('team_id'):
            allowed_team_ids = {int(me.get('team_id'))}
    elif role == "mentor":
        allowed_team_ids = set(await db.list_mentor_teams(call.from_user.id))
    else:
        allowed_team_ids = set()
    teams_vis = [t for t in teams if int(t['id']) in allowed_team_ids]
    if not teams_vis:
        await call.message.answer("Нет доступных команд")
    else:
        await call.message.answer("Выберите команду:", reply_markup=_teams_picker_kb(teams_vis))
    await call.answer()

@dp.callback_query(F.data == "report:pick:buyer")
async def cb_report_pick_buyer(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    role = (me or {}).get("role", "buyer")
    if call.from_user.id in ADMIN_IDS:
        role = "admin"
    scope_ids = set(await _resolve_scope_user_ids(call.from_user.id))
    buyers = [u for u in users if int(u['telegram_id']) in scope_ids]
    # Respect currently selected team filter if present
    cur = await db.get_report_filter(call.from_user.id)
    if cur and cur.get('team_id'):
        try:
            team_id_filter = int(cur['team_id'])
            buyers = [u for u in buyers if (u.get('team_id') and int(u['team_id']) == team_id_filter)]
        except Exception:
            pass
    if not buyers:
        await call.message.answer("Нет доступных байеров")
    else:
        await call.message.answer("Выберите байера:", reply_markup=_buyers_picker_kb(buyers))
    await call.answer()

@dp.callback_query(F.data == "report:pick:offer")
async def cb_report_pick_offer(call: CallbackQuery):
    users = await db.list_users()
    # scope by role
    scope_ids = set(await _resolve_scope_user_ids(call.from_user.id))
    # apply buyer/team filters if set
    cur = await db.get_report_filter(call.from_user.id)
    buyers = [u for u in users if int(u['telegram_id']) in scope_ids]
    if cur and cur.get('team_id'):
        try:
            team_id_filter = int(cur['team_id'])
            buyers = [u for u in buyers if (u.get('team_id') and int(u['team_id']) == team_id_filter)]
        except Exception:
            pass
    if cur and cur.get('buyer_id'):
        try:
            buyer_id_filter = int(cur['buyer_id'])
            buyers = [u for u in buyers if int(u['telegram_id']) == buyer_id_filter]
        except Exception:
            pass
    user_ids = [int(u['telegram_id']) for u in buyers]
    offers = await db.list_offers_for_users(user_ids)
    if not offers:
        await call.message.answer("Нет доступных офферов")
    else:
        await call.message.answer("Выберите оффер:", reply_markup=_offers_picker_kb(offers))
    await call.answer()

@dp.callback_query(F.data == "report:pick:creative")
async def cb_report_pick_creative(call: CallbackQuery):
    users = await db.list_users()
    scope_ids = set(await _resolve_scope_user_ids(call.from_user.id))
    cur = await db.get_report_filter(call.from_user.id)
    buyers = [u for u in users if int(u['telegram_id']) in scope_ids]
    if cur and cur.get('team_id'):
        try:
            team_id_filter = int(cur['team_id'])
            buyers = [u for u in buyers if (u.get('team_id') and int(u['team_id']) == team_id_filter)]
        except Exception:
            pass
    if cur and cur.get('buyer_id'):
        try:
            buyer_id_filter = int(cur['buyer_id'])
            buyers = [u for u in buyers if int(u['telegram_id']) == buyer_id_filter]
        except Exception:
            pass
    user_ids = [int(u['telegram_id']) for u in buyers]
    offer_filter = cur.get('offer') if cur else None
    creatives = await db.list_creatives_for_users(user_ids, offer_filter)
    if not creatives:
        await call.message.answer("Нет доступных креативов")
    else:
        await call.message.answer("Выберите крео:", reply_markup=_creatives_picker_kb(creatives))
    await call.answer()

@dp.callback_query(F.data.startswith("report:set:"))
async def cb_report_set_filter_quick(call: CallbackQuery):
    _, _, which, value = call.data.split(":", 3)
    cur = await db.get_report_filter(call.from_user.id)
    offer = cur.get('offer')
    creative = cur.get('creative')
    buyer_id = cur.get('buyer_id')
    team_id = cur.get('team_id')
    if which == 'team':
        team_id = None if value == '-' else int(value)
    elif which == 'buyer':
        buyer_id = None if value == '-' else int(value)
    elif which == 'offer':
        offer = None if value == '-' else value
    elif which == 'creative':
        creative = None if value == '-' else value
    await db.set_report_filter(call.from_user.id, offer, creative, buyer_id=buyer_id, team_id=team_id)
    await call.answer("Фильтр обновлён")

# ===== KPI =====
def _kpi_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мои KPI", callback_data="kpi:mine")],
        [InlineKeyboardButton(text="Изменить дневной", callback_data="kpi:set:daily"), InlineKeyboardButton(text="Изменить недельный", callback_data="kpi:set:weekly")],
    ])

async def _send_kpi_menu(chat_id: int, actor_id: int):
    kpi = await db.get_kpi(actor_id)
    lines = ["KPI:"]
    lines.append(f"Дневной: <b>{kpi.get('daily_goal') or '-'}</b>")
    lines.append(f"Недельный: <b>{kpi.get('weekly_goal') or '-'}</b>")
    await bot.send_message(chat_id, "\n".join(lines), reply_markup=_kpi_menu())

@dp.callback_query(F.data == "kpi:mine")
async def cb_kpi_mine(call: CallbackQuery):
    await _send_kpi_menu(call.message.chat.id, call.from_user.id)
    await call.answer()

@dp.callback_query(F.data.startswith("kpi:set:"))
async def cb_kpi_set(call: CallbackQuery):
    _, _, which = call.data.split(":", 2)
    await db.set_pending_action(call.from_user.id, f"kpi:set:{which}", None)
    await call.message.answer("Пришлите целевое число депозитов (целое), либо '-' чтобы очистить")
    await call.answer()

# --- Mentor management (admin) ---
@dp.message(Command("addmentor"))
async def on_add_mentor(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /addmentor <telegram_id>
    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("Использование: /addmentor <telegram_id>")
    try:
        uid = await _resolve_user_id(parts[1])
        await db.set_user_role(uid, "mentor")
        await message.answer("OK, назначен ментором")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка назначения роли mentor")

@dp.message(Command("mentor_follow"))
async def on_mentor_follow(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /mentor_follow <mentor_id> <team_id>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /mentor_follow <mentor_id> <team_id>")
    try:
        mid = await _resolve_user_id(parts[1])
        team_id = int(parts[2])
        await db.add_mentor_team(mid, team_id)
        await message.answer("OK, подписан на команду")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка подписки ментора на команду")

@dp.message(Command("mentor_unfollow"))
async def on_mentor_unfollow(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /mentor_unfollow <mentor_id> <team_id>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /mentor_unfollow <mentor_id> <team_id>")
    try:
        mid = await _resolve_user_id(parts[1])
        team_id = int(parts[2])
        await db.remove_mentor_team(mid, team_id)
        await message.answer("OK, отписан от команды")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка отписки ментора от команды")
