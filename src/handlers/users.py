"""User management handlers."""

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..dispatcher import ADMIN_IDS, bot, dp
from .. import db
from loguru import logger


async def _send_whoami(chat_id: int, user_id: int, username: str | None):
    """Send whoami message."""
    await bot.send_message(chat_id, f"Ваш Telegram ID: <code>{user_id}</code>\nUsername: @{username or '-'}")


async def _send_list_users(chat_id: int, actor_id: int):
    """Send list of users based on role visibility."""
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = my["role"] if my else "buyer"
    if actor_id in ADMIN_IDS:
        my_role = "admin"
    my_team = my.get("team_id") if my else None
    lead_team_ids = await db.list_user_lead_teams(actor_id) if my_role not in ("admin", "head") else []
    visible = []
    for u in users:
        if my_role in ("admin", "head"):
            visible.append(u)
        elif lead_team_ids:
            team_id = u.get("team_id")
            if team_id is not None and int(team_id) in lead_team_ids:
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
    """Send list of routes based on role visibility."""
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = (my or {}).get("role", "buyer")
    if actor_id in ADMIN_IDS:
        my_role = "admin"
    my_team = (my or {}).get("team_id")
    lead_team_ids = await db.list_user_lead_teams(actor_id) if my_role not in ("admin", "head") else []
    rows = await db.list_routes()
    def visible(r: dict) -> bool:
        if my_role in ("admin", "head"):
            return True
        if lead_team_ids:
            ru = next((u for u in users if u["telegram_id"] == r["user_id"]), None)
            if not ru:
                return False
            team_id = ru.get("team_id")
            return team_id is not None and int(team_id) in lead_team_ids
        return r["user_id"] == actor_id
    vis = [r for r in rows if visible(r)]
    if not vis:
        return await bot.send_message(chat_id, "Правил нет или нет доступа")
    def fmt(r):
        return f"#{r['id']} -> <code>{r['user_id']}</code> (@{r['username'] or '-'}) | offer={r['offer'] or '*'} | geo={r['country'] or '*'} | src={r['source'] or '*'} | prio={r['priority']}"
    await bot.send_message(chat_id, "Правила:\n" + "\n".join(fmt(r) for r in vis))


def _user_row_controls(u: dict) -> InlineKeyboardMarkup:
    """Build user row controls keyboard."""
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


async def _send_manage(chat_id: int, actor_id: int):
    """Send manage interface for admins."""
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    users = await db.list_users()
    if not users:
        return await bot.send_message(chat_id, "Пока нет пользователей, попросите нажать /start")
    for u in users[:25]:
        text = f"<b>{u['full_name'] or '-'}</b> @{u['username'] or '-'}\nID: <code>{u['telegram_id']}</code>\nRole: <code>{u['role']}</code> | Team: <code>{u['team_id'] or '-'}</code> | Active: <code>{'yes' if u['is_active'] else 'no'}</code>"
        await bot.send_message(chat_id, text, reply_markup=_user_row_controls(u))


async def _resolve_user_id(identifier: str) -> int:
    """Resolve user ID from identifier (numeric ID or @username)."""
    try:
        return int(identifier)
    except ValueError:
        pass
    if identifier.startswith("@"):
        username = identifier[1:]
        users = await db.list_users()
        user = next((u for u in users if u.get("username") == username), None)
        if user:
            return user["telegram_id"]
        raise ValueError(f"User @{username} not found")
    raise ValueError(f"Invalid user identifier: {identifier}")


@dp.message(Command("listusers"))
async def on_list_users(message: Message):
    """Handle /listusers command."""
    me = message.from_user.id
    users = await db.list_users()
    # role-based visibility: admin sees all; head sees all; lead sees their team; buyer sees only self
    visible = []
    # get my role and team
    my = next((u for u in users if u["telegram_id"] == me), None)
    my_role = my["role"] if my else "buyer"
    if me in ADMIN_IDS:
        my_role = "admin"
    lead_team_ids = await db.list_user_lead_teams(me) if my_role not in ("admin", "head") else []
    for u in users:
        if my_role in ("admin", "head"):
            visible.append(u)
        elif lead_team_ids:
            team_id = u.get("team_id")
            if team_id is not None and int(team_id) in lead_team_ids:
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


@dp.message(Command("manage"))
async def on_manage(message: Message):
    """Handle /manage command - admin user management."""
    # Only admins (для MVP) видят управление
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    await _send_manage(message.chat.id, message.from_user.id)


@dp.message(Command("listroutes"))
async def on_list_routes(message: Message):
    """Handle /listroutes command."""
    me = message.from_user.id
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == me), None)
    my_role = (my or {}).get("role", "buyer")
    if me in ADMIN_IDS:
        my_role = "admin"
    my_team = (my or {}).get("team_id")
    lead_team_ids = await db.list_user_lead_teams(me) if my_role not in ("admin", "head") else []
    rows = await db.list_routes()
    # filter by role
    def visible(r: dict) -> bool:
        if my_role in ("admin", "head"):
            return True
        if lead_team_ids:
            ru = next((u for u in users if u["telegram_id"] == r["user_id"]), None)
            if not ru:
                return False
            team_id = ru.get("team_id")
            return team_id is not None and int(team_id) in lead_team_ids
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
    """Handle /addrule command."""
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
    """Handle /setrole command."""
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


@dp.callback_query(F.data.startswith("role:"))
async def cb_set_role(call: CallbackQuery):
    """Handle role change callback."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, uid, role = call.data.split(":", 2)
    await db.set_user_role(int(uid), role)
    u = await db.get_user(int(uid))
    if u and u.get("team_id") is not None:
        team_id = int(u.get("team_id"))
        if role == "mentor":
            await db.set_team_lead_override(team_id, int(uid))
        elif role != "lead":
            await db.clear_team_lead_override(team_id)
    if u:
        await call.message.edit_reply_markup(reply_markup=_user_row_controls(u))
        await call.answer("Роль обновлена")
    else:
        await call.answer("Пользователь не найден", show_alert=True)


@dp.callback_query(F.data.startswith("active:"))
async def cb_set_active(call: CallbackQuery):
    """Handle active status change callback."""
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
