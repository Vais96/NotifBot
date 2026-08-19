"""Menu and navigation handlers."""

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..dispatcher import ADMIN_IDS, bot, dp
from .. import db, keitaro_sync
from ..handlers.users import _send_whoami, _send_list_users, _send_list_routes, _send_manage
from ..handlers.aliases import _send_aliases
from ..handlers.teams import _send_teams, _send_myteam
from ..handlers.mentors import _send_mentors
from ..handlers.helpers import _send_helpers_list
from ..handlers.reports import _send_reports_menu, _send_kpi_menu
from loguru import logger


def main_menu(is_admin: bool, role: str | None = None, has_lead_access: bool = False) -> InlineKeyboardMarkup:
    """Build main menu keyboard."""
    checkdomain_btn = [InlineKeyboardButton(text="Проверить домен", callback_data="menu:checkdomain")]
    if role == "helper" and not is_admin:
        buttons = [
            checkdomain_btn,
            [InlineKeyboardButton(text="Отчеты", callback_data="menu:reports"), InlineKeyboardButton(text="KPI", callback_data="menu:kpi")],
            [InlineKeyboardButton(text="Кто я", callback_data="menu:whoami"), InlineKeyboardButton(text="Правила", callback_data="menu:listroutes")],
            [InlineKeyboardButton(text="Загрузить CSV", callback_data="menu:uploadcsv")],
            [InlineKeyboardButton(text="Скачать видео", callback_data="menu:yt_download")],
        ]
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    buttons = [
        [InlineKeyboardButton(text="Кто я", callback_data="menu:whoami"), InlineKeyboardButton(text="Правила", callback_data="menu:listroutes")],
        [InlineKeyboardButton(text="Отчеты", callback_data="menu:reports"), InlineKeyboardButton(text="KPI", callback_data="menu:kpi")],
    ]
    buttons.append(checkdomain_btn)
    buttons.append([InlineKeyboardButton(text="Загрузить CSV", callback_data="menu:uploadcsv")])
    buttons.append([InlineKeyboardButton(text="Скачать видео", callback_data="menu:yt_download")])
    if is_admin:
        buttons += [
            [InlineKeyboardButton(text="Пользователи", callback_data="menu:listusers"), InlineKeyboardButton(text="Управление", callback_data="menu:manage")],
            [InlineKeyboardButton(text="Команды", callback_data="menu:teams"), InlineKeyboardButton(text="Алиасы", callback_data="menu:aliases")],
            [InlineKeyboardButton(text="Менторы", callback_data="menu:mentors"), InlineKeyboardButton(text="Помощники", callback_data="menu:helpers")],
            [InlineKeyboardButton(text="Обновить домены", callback_data="menu:refreshdomains")],
            [InlineKeyboardButton(text="Очистить FB данные", callback_data="menu:resetfbdata")],
        ]
    else:
        # For lead/head expose 'Моя команда'
        if has_lead_access or role in ("lead", "head"):
            buttons += [[InlineKeyboardButton(text="Моя команда", callback_data="menu:myteam")]]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def user_menu_flags(user_id: int) -> tuple[bool, str | None, bool]:
    """Return (is_admin, role, has_lead_access) for building the main menu."""
    is_admin = user_id in ADMIN_IDS
    me = await db.get_user(user_id)
    role = (me or {}).get("role")
    if is_admin:
        role = "admin"
    has_lead_access = is_admin
    if not has_lead_access:
        lead_team_ids = await db.list_user_lead_teams(user_id)
        has_lead_access = bool(lead_team_ids) or (role in ("lead", "head"))
    return is_admin, role, has_lead_access


async def send_user_menu(chat_id: int, user_id: int, *, intro: str = "Меню:") -> None:
    is_admin, role, has_lead_access = await user_menu_flags(user_id)
    await bot.send_message(chat_id, intro, reply_markup=main_menu(is_admin, role, has_lead_access=has_lead_access))


@dp.message(Command("menu"))
async def on_menu(message: Message):
    """Handle /menu command."""
    await send_user_menu(message.chat.id, message.from_user.id)


@dp.callback_query(F.data.startswith("menu:"))
async def on_menu_click(call: CallbackQuery):
    """Handle menu callback queries."""
    key = call.data.split(":", 1)[1]
    if key == "whoami":
        await _send_whoami(call.message.chat.id, call.from_user.id, call.from_user.username)
        return await call.answer()
    if key == "listroutes":
        await _send_list_routes(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "checkdomain":
        await db.set_pending_action(call.from_user.id, "domain:check", None)
        await call.message.answer("Пришлите домен в формате example.com или ссылку")
        return await call.answer()
    if key == "uploadcsv":
        await db.set_pending_action(call.from_user.id, "fb:await_csv", None)
        await call.message.answer(
            "Пришлите CSV из Facebook Ads Manager.\n"
            "Файл должен содержать колонку 'День' с разбивкой по датам.\n"
            "Чтобы отменить ожидание, отправьте '-'"
        )
        return await call.answer()
    if key == "yt_download":
        await db.set_pending_action(call.from_user.id, "youtube:await_url", None)
        await call.message.answer(
            "Пришлите ссылку на видео YouTube.\n"
            "Если передумаете, отправьте '-' чтобы отменить."
        )
        return await call.answer()
    if key == "refreshdomains":
        if call.from_user.id not in ADMIN_IDS:
            return await call.answer("Нет прав", show_alert=True)
        await call.answer("Начинаю обновление")
        status_msg = await call.message.answer("Запускаю обновление доменов из Keitaro…")
        try:
            count = await keitaro_sync.sync_campaigns()
        except Exception as exc:
            logger.exception("Failed to refresh Keitaro domains", error=exc)
            await status_msg.edit_text("Не удалось обновить домены. Проверь логи и настройки Keitaro API.")
        else:
            await status_msg.edit_text(f"Готово. Добавлено/обновлено {count} кампаний с доменами.")
        return
    if key == "resetfbdata":
        if call.from_user.id not in ADMIN_IDS:
            return await call.answer("Нет прав", show_alert=True)
        warning_text = (
            "⚠️ <b>Внимание</b>\n"
            "Эта операция очистит все данные, загруженные из FB CSV, включая: "
            "<code>fb_campaign_daily</code>, <code>fb_campaign_totals</code>, <code>fb_campaign_state</code>, "
            "<code>fb_campaign_history</code>, <code>fb_csv_rows</code>, <code>fb_csv_uploads</code> и <code>fb_accounts</code>."
            "\nПродолжить?"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Очистить", callback_data="resetfbdata:confirm"),
                    InlineKeyboardButton(text="Отмена", callback_data="resetfbdata:cancel"),
                ]
            ]
        )
        await call.message.answer(warning_text, reply_markup=kb)
        return await call.answer()
    if key == "listusers":
        await _send_list_users(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "manage":
        await _send_manage(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "teams":
        await _send_teams(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "aliases":
        await _send_aliases(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "mentors":
        await _send_mentors(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "helpers":
        await _send_helpers_list(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "myteam":
        await _send_myteam(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "reports":
        await _send_reports_menu(call.message.chat.id, call.from_user.id)
        return await call.answer()
    if key == "kpi":
        await _send_kpi_menu(call.message.chat.id, call.from_user.id)
        return await call.answer()


@dp.callback_query(F.data == "resetfbdata:confirm")
async def cb_resetfbdata_confirm(call: CallbackQuery):
    """Handle FB data reset confirmation."""
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await call.answer("Очищаю данные…")
    try:
        await db.reset_fb_upload_data()
    except Exception as exc:
        logger.exception("Failed to reset FB upload data", exc_info=exc)
        text = "Не удалось очистить данные. Смотри логи."
    else:
        text = (
            "✅ Очистка завершена."
            " Данные FB CSV удалены, можно загружать свежий отчёт."
        )
    try:
        await call.message.edit_text(text)
    except Exception:
        await call.message.answer(text)


@dp.callback_query(F.data == "resetfbdata:cancel")
async def cb_resetfbdata_cancel(call: CallbackQuery):
    """Handle FB data reset cancellation."""
    await call.answer("Отменено")
    try:
        await call.message.delete()
    except Exception:
        pass
