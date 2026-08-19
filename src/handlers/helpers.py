"""Helper (Помощник) management: assign helpers to buyers; helper sees only that buyer's deposits."""

from aiogram import F
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from ..dispatcher import ADMIN_IDS, bot, dp
from .. import db
from loguru import logger

_BUYERS_PER_PAGE = 30


def _helper_row_controls(helper_id: int) -> InlineKeyboardMarkup:
    """Кнопка «Назначить байера» рядом с помощником."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назначить байера", callback_data=f"helper:setbuyer:{helper_id}")],
        [InlineKeyboardButton(text="Удалить помощника", callback_data=f"helper:delete:{helper_id}")],
    ])


def _helper_add_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить помощника", callback_data="helper:add")]
    ])


def _buyer_picker(
    buyers: list[dict], helper_id: int, page: int
) -> tuple[str, InlineKeyboardMarkup]:
    """Build one page of buyer choices for a helper assignment."""
    page_count = max(1, (len(buyers) + _BUYERS_PER_PAGE - 1) // _BUYERS_PER_PAGE)
    page = max(0, min(page, page_count - 1))
    start = page * _BUYERS_PER_PAGE
    buttons = []
    for buyer in buyers[start:start + _BUYERS_PER_PAGE]:
        buyer_id = int(buyer["telegram_id"])
        label = f"@{buyer.get('username') or buyer_id}"
        if buyer.get("full_name"):
            label += f" ({buyer['full_name']})"
        if len(label) > 35:
            label = label[:32] + "..."
        buttons.append([
            InlineKeyboardButton(
                text=label,
                callback_data=f"helper:assign:{helper_id}:{buyer_id}",
            )
        ])
    navigation = []
    if page > 0:
        navigation.append(
            InlineKeyboardButton(
                text="← Назад", callback_data=f"helper:setbuyer:{helper_id}:{page - 1}"
            )
        )
    if page < page_count - 1:
        navigation.append(
            InlineKeyboardButton(
                text="Вперёд →", callback_data=f"helper:setbuyer:{helper_id}:{page + 1}"
            )
        )
    if navigation:
        buttons.append(navigation)
    return (
        f"Выберите байера для помощника (страница {page + 1}/{page_count}):",
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )


async def _send_helpers_list(chat_id: int, actor_id: int):
    """Плашка «Помощники»: список и кнопка добавить."""
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    rows = await db.list_helpers_with_buyers()
    await bot.send_message(chat_id, "Помощники:", reply_markup=_helper_add_button())
    if not rows:
        return await bot.send_message(chat_id, "Пока нет помощников. Нажмите «Добавить помощника» и введите @username.")
    for r in rows:
        helper_id = int(r["helper_id"])
        h_name = r.get("helper_name") or "-"
        h_user = r.get("helper_username") or "-"
        buyer_id = r.get("buyer_id")
        if buyer_id:
            b_user = r.get("buyer_username") or "-"
            b_name = r.get("buyer_name") or "-"
            assign = f" → байер @{b_user} ({b_name})"
        else:
            assign = " — байер не назначен"
        text = (
            f"<b>{h_name}</b> @{h_user}\n"
            f"ID: <code>{helper_id}</code>{assign}"
        )
        await bot.send_message(chat_id, text, reply_markup=_helper_row_controls(helper_id))


@dp.callback_query(F.data == "helper:add")
async def cb_helper_add(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "helper:add", None)
    await call.message.answer(
        "Введите @username пользователя, которого сделать помощником.\n"
        "Пользователь должен хотя бы раз нажать /start в боте."
    )
    await call.answer()


@dp.callback_query(F.data.startswith("helper:setbuyer:"))
async def cb_helper_set_buyer(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    parts = call.data.split(":")
    helper_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    buyers = await db.list_users_as_buyer_candidates()
    if not buyers:
        return await call.answer("Нет ни одного байера/лида/ментора в системе", show_alert=True)
    text, keyboard = _buyer_picker(buyers, helper_id, page)
    if len(parts) > 3:
        await call.message.edit_text(text, reply_markup=keyboard)
    else:
        await call.message.answer(text, reply_markup=keyboard)
    await call.answer()


@dp.callback_query(F.data.startswith("helper:assign:"))
async def cb_helper_assign(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    parts = call.data.split(":")
    helper_id = int(parts[2])
    buyer_id = int(parts[3])
    try:
        await db.set_helper_buyer(helper_id, buyer_id)
        buyer = await db.get_user(buyer_id)
        b_label = f"@{buyer.get('username') or buyer_id}" if buyer else str(buyer_id)
        await call.message.answer(f"Помощник <code>{helper_id}</code> привязан к байеру {b_label}.")
    except Exception as e:
        logger.exception("Failed to set helper buyer", helper_id=helper_id, buyer_id=buyer_id, error=e)
        await call.answer("Ошибка", show_alert=True)
        return
    await call.answer("Готово", show_alert=True)


@dp.callback_query(F.data.startswith("helper:delete:"))
async def cb_helper_delete(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, __, helper_id_s = call.data.split(":", 2)
    helper_id = int(helper_id_s)
    try:
        await db.remove_helper_and_promote_to_buyer(helper_id)
        await call.message.answer(
            f"Пользователь <code>{helper_id}</code> удален из помощников и переведен в роль buyer."
        )
    except Exception as e:
        logger.exception("Failed to delete helper", helper_id=helper_id, error=e)
        return await call.answer("Ошибка удаления помощника", show_alert=True)
    await call.answer("Помощник удален", show_alert=True)

