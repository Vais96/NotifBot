"""Basic command handlers: /start, /help, /ping, /whoami."""

from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from ..dispatcher import dp
from .common import is_admin
from .. import db
from ..handlers.menu import send_user_menu


@dp.message(CommandStart())
async def on_start(message: Message):
    """Handle /start command - register user."""
    await db.upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    # Автоповышение роли для ID из ADMINS
    if await is_admin(message.from_user.id):
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


_HELP = (
    "Доступные команды:\n"
    "/start — регистрация\n"
    "/menu — открыть меню\n"
    "/today — отчёт за сегодня\n"
    "/yesterday — отчёт за вчера\n"
    "/week — отчёт за 7 дней\n"
    "/adskey — мой ключ Ads Workspace (байер/лид/ментор/хэд)\n"
    "/checkdomain — проверить домен (кто ведёт кампанию)\n"
    "/listteams — список команд (лид/хэд/админ)\n"
    "/whoami — показать свой Telegram ID\n"
    "/ping — проверка связи (pong)\n"
    "/help — помощь\n"
    "\n"
    "Если бот ждёт ввод (домен, ID, число), «отмена» / «cancel» / «-» прекращает ожидание."
)
_HELP_ADMIN = (
    "\n\nАдмину:\n"
    "/listusers, /manage — пользователи и роли\n"
    "/setrole &lt;telegram_id&gt; &lt;buyer|lead|head|admin|mentor&gt;\n"
    "/listroutes, /addrule &lt;user_id&gt; offer=… country=… source=… priority=0 — правила роутинга\n"
    "/aliases, /setalias &lt;alias&gt; buyer=&lt;id|-&gt; lead=&lt;id|-&gt;, /delalias &lt;alias&gt;\n"
    "/addmentor &lt;id|@username&gt;, /mentor_follow &lt;mentor_id&gt; &lt;team_id&gt;, /mentor_unfollow &lt;mentor_id&gt; &lt;team_id&gt;"
)


@dp.message(Command("help"))
async def on_help(message: Message):
    """Handle /help command - show available commands (admin section only for admins)."""
    await message.answer(_HELP + (_HELP_ADMIN if await is_admin(message.from_user.id) else ""))


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
