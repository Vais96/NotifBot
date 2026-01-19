"""Domain checking handlers."""

from aiogram.filters import Command
from aiogram.types import Message

from ..dispatcher import dp
from .. import db
from ..utils.domain import lookup_domains_text


@dp.message(Command("checkdomain"))
async def on_checkdomain(message: Message):
    """Handle /checkdomain command."""
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await db.set_pending_action(message.from_user.id, "domain:check", None)
        return await message.answer("Пришлите домен, например salongierpl.online")
    result = await lookup_domains_text(parts[1])
    await message.answer(result + "\n\nОтправьте следующий домен или '-' чтобы завершить")
