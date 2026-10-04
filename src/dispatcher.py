from aiogram import Dispatcher
from .config import settings
from .telegram_rate_limit import limited_send_message, make_bot

# Central bot/dispatcher objects
bot = make_bot(settings.telegram_bot_token)
dp = Dispatcher()

ADMIN_IDS = set(settings.admins)

async def notify_buyer(buyer_id: int, text: str):
    """Rate-limited send; raises on failure — callers count delivered/failed."""
    await limited_send_message(bot, buyer_id, text=text)
