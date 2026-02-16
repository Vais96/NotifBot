"""Design bot — уведомления о заказах на дизайн (Underdog API). Пока только подключение."""

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

from .config import settings

design_bot = Bot(
    token=settings.design_bot_token or settings.telegram_bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
design_dp = Dispatcher()


@design_dp.message(CommandStart())
async def on_design_start(message: Message) -> None:
    """Приветствие — функционал заказов будет подключён после изменений API Underdog."""
    await message.answer(
        "Привет! Это бот уведомлений о заказах на дизайн. "
        "Подключение настроено, функционал заказов будет добавлен позже."
    )
