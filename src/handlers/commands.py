"""Basic command handlers: /start, /help, /ping, /whoami."""

from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from ..dispatcher import ADMIN_IDS, dp
from .. import db


@dp.message(CommandStart())
async def on_start(message: Message):
    """Handle /start command - register user."""
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
    """Handle /help command - show available commands."""
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
    """Handle /ping command - respond with pong."""
    await message.answer("pong")


@dp.message(Command("whoami"))
async def on_whoami(message: Message):
    """Handle /whoami command - show user's Telegram ID."""
    uid = message.from_user.id
    uname = message.from_user.username
    await message.answer(f"Ваш Telegram ID: <code>{uid}</code>\nUsername: @{uname or '-'}")
