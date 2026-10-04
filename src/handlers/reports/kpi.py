"""Personal KPI goals."""


from aiogram import F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from ...dispatcher import dp, bot
from ..common import STALE_BUTTON, callback_parts
from ... import db
from ...constants import PendingAction


# ===== KPI =====
def _kpi_menu() -> InlineKeyboardMarkup:
    """Build KPI menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мои KPI", callback_data="kpi:mine")],
        [InlineKeyboardButton(text="Изменить дневной", callback_data="kpi:set:daily"), InlineKeyboardButton(text="Изменить недельный", callback_data="kpi:set:weekly")],
    ])


async def _send_kpi_menu(chat_id: int, actor_id: int):
    """Send KPI menu with current values."""
    kpi = await db.get_kpi(actor_id)
    lines = ["KPI:"]
    lines.append(f"Дневной: <b>{kpi.get('daily_goal') or '-'}</b>")
    lines.append(f"Недельный: <b>{kpi.get('weekly_goal') or '-'}</b>")
    await bot.send_message(chat_id, "\n".join(lines), reply_markup=_kpi_menu())


@dp.callback_query(F.data == "kpi:mine")
async def cb_kpi_mine(call: CallbackQuery):
    """Handle KPI mine callback."""
    await _send_kpi_menu(call.message.chat.id, call.from_user.id)
    await call.answer()


@dp.callback_query(F.data.startswith("kpi:set:"))
async def cb_kpi_set(call: CallbackQuery):
    """Handle KPI set callback."""
    parts = callback_parts(call, 3)
    if not parts:
        return await call.answer(STALE_BUTTON, show_alert=True)
    _, _, which = parts
    await db.set_pending_action(call.from_user.id, f"{PendingAction.KPI_SET}:{which}", None)
    await call.message.answer("Пришлите целевое число депозитов (целое), либо '-' чтобы очистить")
    await call.answer()
