"""Lightweight dispatcher for the dedicated orders notification bot."""

from datetime import datetime
from typing import Any, Dict, List

from aiogram import Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup
from loguru import logger

from .config import settings
from .handlers.common import is_admin
from . import db, underdog
from .telegram_rate_limit import make_bot
from .utils.formatting import chunk_lines
from .utils.html import safe

orders_bot = make_bot(settings.orders_bot_token or settings.telegram_bot_token)
orders_dp = Dispatcher()

def build_menu_keyboard(*, is_admin: bool) -> ReplyKeyboardMarkup:
    rows: List[List[KeyboardButton]] = [
        [
            KeyboardButton(text="/menu"),
            KeyboardButton(text="/adminstatus"),
        ]
    ]
    if is_admin:
        rows.append([
            KeyboardButton(text="/users"),
        ])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=False,
    )


@orders_dp.message(CommandStart())
async def on_orders_start(message: Message) -> None:
    """Register the user and immediately attempt to deliver pending orders."""
    user = message.from_user
    await db.upsert_user(user.id, user.username, user.full_name)
    await db.set_orders_opt_out(user.id, False)
    keyboard = build_menu_keyboard(is_admin=await is_admin(user.id))
    await message.answer(
        "Привет! Ты зарегистрирован в боте заказов. Ищу все невручённые заказы…",
        reply_markup=keyboard,
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


@orders_dp.message(Command(commands=["menu", "help"]))
async def show_orders_menu(message: Message) -> None:
    admin = await is_admin(message.from_user.id)
    lines = [
        "📋 <b>Меню бота заказов</b>",
        "\n",
        "• /start — зарегистрироваться и получить невручённые заказы",
        "• /menu — показать это меню",
        "• /adminstatus — проверить, видит ли бот вас как админа",
    ]
    if not admin:
        lines.append(
            "\n⚠️ Чтобы получать служебные уведомления, сначала нажмите /start и разрешите боту писать вам."
        )
    else:
        lines.extend(
            [
                "",
                "Команды для админов:",
                "• /users — список пользователей, нажавших /start",
                "• /unsubscribe <telegram_id> — отписать пользователя от бота",
            ]
        )
    await message.answer("\n".join(lines), reply_markup=build_menu_keyboard(is_admin=admin))


@orders_dp.message(Command("adminstatus"))
async def show_admin_status(message: Message) -> None:
    admin = await is_admin(message.from_user.id)
    if admin:
        await message.answer(
            "✅ Ты в списке админов. Служебные уведомления будут приходить сюда.",
            reply_markup=build_menu_keyboard(is_admin=True),
        )
    else:
        await message.answer(
            "❌ Этого аккаунта нет в списке ADMINS. Обнови переменную окружения или обратись к администратору.",
            reply_markup=build_menu_keyboard(is_admin=False),
        )


def _format_username(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        handle = value.strip()
        if handle.startswith("@"):
            return handle
        return f"@{handle}"
    return "—"


def _format_user_line(user: Dict[str, Any]) -> str:
    telegram_id = user.get("telegram_id")
    username = _format_username(user.get("username"))
    full_name = user.get("full_name") or "—"
    created_at = user.get("created_at")
    created_text = "—"
    if isinstance(created_at, datetime):
        created_text = created_at.strftime("%d.%m.%Y")
    status = "✅" if int(user.get("is_active") or 0) == 1 else "🚫"
    return f"{status} {safe(full_name)} ({safe(username)}) — ID {telegram_id}, с {created_text}"


@orders_dp.message(Command("users"))
async def list_bot_users(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Эта команда доступна только администраторам.")
        return
    users = await db.list_users()
    if not users:
        await message.answer("Пока нет зарегистрированных пользователей.")
        return
    active_users = [user for user in users if int(user.get("is_active") or 0) == 1]
    inactive_count = len(users) - len(active_users)
    header_lines = [
        "👥 <b>Активные пользователи бота</b>",
        f"Всего активных: {len(active_users)}",
    ]
    if inactive_count:
        header_lines.append(f"Неактивных: {inactive_count}")
    header_lines.append("")

    user_lines = [_format_user_line(user) for user in active_users]
    user_chunks = chunk_lines(user_lines) if user_lines else []
    total_parts = len(user_chunks)

    if not user_chunks:
        await message.answer("\n".join(header_lines), reply_markup=build_menu_keyboard(is_admin=True))
        return

    for index, chunk in enumerate(user_chunks, start=1):
        lines: List[str] = []
        if index == 1:
            lines.extend(header_lines)
        lines.append(f"Часть {index}/{total_parts}")
        lines.append(chunk)
        if index == total_parts:
            lines.extend(
                [
                    "",
                    "Чтобы отписать пользователя, отправь команду /unsubscribe <telegram_id>.",
                ]
            )
        await message.answer(
            "\n".join(lines),
            reply_markup=build_menu_keyboard(is_admin=True) if index == total_parts else None,
        )


@orders_dp.message(Command("unsubscribe"))
async def unsubscribe_user(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Эта команда доступна только администраторам.")
        return
    text = message.text or ""
    parts = text.split()
    if len(parts) < 2:
        await message.answer("Укажи ID: /unsubscribe 123456789")
        return
    target_raw = parts[1]
    try:
        target_id = int(target_raw)
    except ValueError:
        await message.answer("ID должен быть числом.")
        return
    if await is_admin(target_id):
        await message.answer("Нельзя отписать администратора через эту команду.")
        return
    user = await db.get_user(target_id)
    if not user:
        await message.answer("Пользователь с таким ID не найден.")
        return
    await db.set_orders_opt_out(target_id, True)
    username = _format_username(user.get("username"))
    full_name = user.get("full_name") or "—"
    await message.answer(
        f"🚫 Пользователь {safe(full_name)} ({safe(username)}) (ID {target_id}) помечен как отписанный. "
        "Он сможет вернуться, снова нажав /start.",
        reply_markup=build_menu_keyboard(is_admin=True),
    )
    logger.info(
        "Admin unsubscribed user",
        admin_id=message.from_user.id,
        target_id=target_id,
        username=username,
    )
