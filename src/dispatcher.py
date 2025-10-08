from aiogram import Bot, Dispatcher
from aiogram.enums.parse_mode import ParseMode
from aiogram.client.default import DefaultBotProperties
from loguru import logger
from .config import settings

# Central bot/dispatcher objects
bot = Bot(token=settings.telegram_bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

ADMIN_IDS = set(settings.admins)

async def notify_buyer(buyer_id: int, text: str):
    try:
        await bot.send_message(chat_id=buyer_id, text=text)
    except Exception as e:
        logger.warning(f"Failed to notify buyer {buyer_id}: {e}")
