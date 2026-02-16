"""Design bot — уведомления о заказах на дизайн (Underdog API: creative, pwaDesign)."""

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from loguru import logger

from .config import settings
from . import db

design_bot = Bot(
    token=settings.design_bot_token or settings.telegram_bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
design_dp = Dispatcher()


@design_dp.message(CommandStart())
async def on_design_start(message: Message) -> None:
    """Регистрируем подписчика (tg_design_bot_chats) и приветствуем — без этого в рассылку не попадёте."""
    chat_id = message.chat.id
    await db.add_design_bot_subscriber(chat_id)
    logger.info("Design bot: new subscriber", chat_id=chat_id, username=message.from_user.username)
    await message.answer(
        "Привет! Ты подписан на уведомления о заказах на дизайн (креативы и PWA). "
        "Когда появятся новые заказы — пришлю сюда."
    )
