"""Lightweight dispatcher for the dedicated orders notification bot."""

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from loguru import logger

from .config import settings
from . import db, underdog

orders_bot = Bot(
    token=settings.orders_bot_token or settings.telegram_bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
orders_dp = Dispatcher()


@orders_dp.message(CommandStart())
async def on_orders_start(message: Message) -> None:
    """Register the user and immediately attempt to deliver pending orders."""
    user = message.from_user
    await db.upsert_user(user.id, user.username, user.full_name)
    await message.answer(
        "Привет! Ты зарегистрирован в боте заказов. Ищу все невручённые заказы…"
    )
    try:
        stats = await underdog.notify_ready_orders(
            dry_run=False,
            limit_user_ids=[user.id],
            bot_instance=orders_bot,
        )
    except Exception as exc:
        logger.exception("Failed to notify about pending orders", exc_info=exc)
        await message.answer(
            "Не удалось проверить заказы прямо сейчас. Попробуй ещё раз чуть позже."
        )
        return

    notified = int(stats.get("notified") or 0)
    unknown = int(stats.get("unknown_user") or 0)

    if notified > 0:
        await message.answer(
            f"Готово: отправили {notified} заказ(ов). Как появятся новые — сразу напишем тут."
        )
    else:
        await message.answer(
            "Пока нет новых заказов. Бот сообщит автоматически, когда появятся выполнения."
        )

    if unknown > 0:
        await message.answer(
            "Есть заказы, где не нашли твой username. Проверь, что он указан в Telegram, и напиши админу."
        )
