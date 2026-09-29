"""Basic command handlers: /start, /help, /ping, /whoami."""

from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from ..dispatcher import ADMIN_IDS, dp
from .. import db
from ..handlers.menu import send_user_menu


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
    me = await db.get_user(message.from_user.id)
    role = (me or {}).get("role")
    if role == "helper":
        await message.answer(
            "Привет! Вы помощник.\n"
            "Проверять домены: /checkdomain или кнопка «Проверить домен» в меню."
        )
    else:
        await message.answer(
            "Привет! Ты зарегистрирован. Роли, команды и привязки синхронизируются из Admin API."
        )
    await send_user_menu(message.chat.id, message.from_user.id)


@dp.message(Command("help"))
async def on_help(message: Message):
    """Handle /help command - show available commands."""
    await message.answer(
        "Доступные команды:\n"
        "/start — регистрация\n"
        "/menu — открыть меню\n"
        "/today — отчёт за сегодня\n"
        "/yesterday — отчёт за вчера\n"
        "/week — отчёт за 7 дней\n"
        "/adskey — мой ключ Ads Workspace (байер/лид/ментор/хэд)\n"
        "/checkdomain — проверить домен (кто ведёт кампанию)\n"
        "/whoami — показать свой Telegram ID\n"
        "/ping — проверка связи (pong)\n"
        "/help — помощь"
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
