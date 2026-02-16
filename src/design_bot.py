"""Design bot — уведомления о заказах на дизайн (Underdog API: creative, pwaDesign)."""

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

from .config import settings
from . import db

design_bot = Bot(
    token=settings.design_bot_token or settings.telegram_bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
design_dp = Dispatcher()


@design_dp.message(CommandStart())
async def on_design_start(message: Message) -> None:
    """Регистрируем подписчика и приветствуем."""
    chat_id = message.chat.id
    await db.add_design_bot_subscriber(chat_id)
    await message.answer(
        "Привет! Ты подписан на уведомления о заказах на дизайн (креативы и PWA). "
        "Когда появятся новые заказы — пришлю сюда."
    )
