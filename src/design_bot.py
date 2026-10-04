"""Design bot — уведомления о заказах на дизайн (Underdog API: creative, pwaDesign)."""

from aiogram import Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from loguru import logger

from .config import settings
from . import db
from .telegram_rate_limit import make_bot

design_bot = make_bot(settings.design_bot_token or settings.telegram_bot_token)
design_dp = Dispatcher()


@design_dp.message(CommandStart())
async def on_design_start(message: Message) -> None:
    """Регистрируем подписчика (tg_design_bot_chats) и приветствуем — без этого в рассылку не попадёте."""
    chat_id = message.chat.id
    user = message.from_user
    try:
        if user and message.chat.type == "private":  # в группе chat_id — это группа, не пользователь
            await db.upsert_user(
                user.id,
                user.username,
                " ".join(filter(None, [user.first_name, user.last_name])) or None,
            )
        await db.add_design_bot_subscriber(chat_id)
        count = len(await db.list_design_bot_subscribers())
        logger.info(
            "Design bot: subscriber added",
            chat_id=chat_id,
            username=message.from_user.username,
            total_subscribers_now=count,
        )
    except Exception as e:
        logger.exception("Design bot: failed to add subscriber to tg_design_bot_chats", chat_id=chat_id, error=str(e))
        await message.answer(
            "Подписка не сохранилась (ошибка БД). Напишите админу или попробуйте позже."
        )
        return
    await message.answer(
        "Привет! Ты подписан на уведомления о заказах на дизайн (креативы и PWA). "
        "Когда появятся новые заказы — пришлю сюда."
    )
