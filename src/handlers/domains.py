"""Domain checking handlers."""

from aiogram.filters import Command
from aiogram.types import Message
from loguru import logger

from ..dispatcher import bot, dp
from .. import db
from ..utils.domain import lookup_domains_text

HELPER_DOMAIN_HINT = (
    "Вас назначили помощником.\n"
    "Проверять домены: /checkdomain или кнопка «Проверить домен» в /menu."
)


async def notify_helper_domain_access(user_id: int) -> None:
    """Tell a newly assigned helper how to check domains."""
    try:
        await bot.send_message(user_id, HELPER_DOMAIN_HINT)
    except Exception as exc:
        logger.warning("Failed to notify helper about domain check", user_id=user_id, error=exc)


@dp.message(Command("checkdomain"))
async def on_checkdomain(message: Message):
    """Handle /checkdomain command. Available to all roles, including helpers."""
    text = message.text or ""
    parts = text.split(maxsplit=1)
    await db.set_pending_action(message.from_user.id, "domain:check", None)
    if len(parts) < 2:
        return await message.answer("Пришлите домен, например salongierpl.online")
    result = await lookup_domains_text(parts[1])
    await message.answer(result + "\n\nОтправьте следующий домен или '-' чтобы завершить")
