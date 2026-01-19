import asyncio
import html
import json
import re
import shutil
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from aiogram import F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)
from aiogram.enums.parse_mode import ParseMode
from loguru import logger
from .config import settings
from . import db
from .dispatcher import bot, dp, ADMIN_IDS
from . import keitaro_sync
from .keitaro import normalize_domain, parse_campaign_name
from . import fb_csv
from .services.fb_uploads import (
    CSV_ALLOWED_MIME_TYPES,
    MAX_CSV_FILE_SIZE_BYTES,
    process_fb_csv_upload,
)
from .utils.domain import canonical_alias_key
from .handlers.youtube import handle_youtube_download
from .utils.domain import lookup_domains_text, resolve_campaign_assignments, extract_domains, render_domain_block, MAX_DOMAINS_PER_REQUEST
from .utils.formatting import (
    fmt_money as _fmt_money,
    fmt_percent as _fmt_percent,
    month_label_ru as _month_label_ru,
    as_decimal as _as_decimal,
    format_flag_label as _format_flag_label,
    format_flag_decision as _format_flag_decision,
    format_buyer_label as _format_buyer_label,
    chunk_lines,
)
from .handlers.users import _resolve_user_id

_FLAG_CODE_LABELS = {
    "GREEN": "🟢 Зелёный",
    "YELLOW": "🟡 Жёлтый",
    "RED": "🔴 Красный",
}

_FLAG_REASON_OVERRIDES = {
    "Spend ≥ $200 и FTD = 0": "🟥 Красный флаг",
    "CTR < 0.7%": "⚠️ Жёлтый флаг",
}

# Используем canonical_alias_key из services/campaigns.py
_canonical_alias_key = canonical_alias_key


# _chunk_lines moved to utils/formatting.py


def _parse_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _parse_decimal_optional(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _build_account_detail_messages(payload: Dict[str, Any]) -> List[str]:
    account_name = str(payload.get("account_name") or "Без кабинета")
    flag_label = str(payload.get("flag_label") or "—")
    spend_value = _parse_decimal(payload.get("spend"))
    revenue_value = _parse_decimal(payload.get("revenue"))
    roi_value = _parse_decimal_optional(payload.get("roi"))
    if roi_value is None and spend_value:
        roi_value = (revenue_value - spend_value) / spend_value * Decimal(100)
    ftd_value = int(payload.get("ftd") or 0)
    campaign_count = int(payload.get("campaign_count") or 0)
    ctr_value = _parse_decimal_optional(payload.get("ctr"))
    ftd_rate_value = _parse_decimal_optional(payload.get("ftd_rate"))
    lines: List[str] = []
    lines.append(f"<b>{html.escape(account_name)}</b>")
    lines.append("Флаг кабинета: " + html.escape(flag_label))
    lines.append(
        f"Spend {_fmt_money(spend_value)} | Rev {_fmt_money(revenue_value)} | ROI {_fmt_percent(roi_value)} | FTD {ftd_value} | Кампаний {campaign_count}"
    )
    lines.append(f"CTR {_fmt_percent(ctr_value)} | FTD rate {_fmt_percent(ftd_rate_value)}")
    campaign_lines = payload.get("campaign_lines") or []
    if campaign_lines:
        lines.append("")
        lines.append("<b>Кампании:</b>")
        for idx, item in enumerate(campaign_lines):
            lines.append(str(item))
            if idx < len(campaign_lines) - 1:
                lines.append("")
    else:
        lines.append("")
        lines.append("Кампаний не найдено для этого кабинета.")
    return chunk_lines(lines)

# _resolve_user_id moved to handlers/users.py
# Domain utilities moved to utils/domain.py
# YouTube functions moved to services/youtube.py


# Formatting functions moved to utils/formatting.py


async def _notify_admins_about_exception(context: str, exc: Exception, extra_details: Optional[List[str]] = None) -> None:
    trace = ""
    try:
        trace = "".join(traceback.format_exception(exc.__class__, exc, exc.__traceback__))
    except Exception as format_exc:
        logger.warning("Failed to format exception traceback", exc_info=format_exc)
        trace = str(exc)
    snippet_limit = 3500
    snippet = trace[-snippet_limit:] if len(trace) > snippet_limit else trace
    lines: List[str] = [f"⚠️ {html.escape(context)}"]
    if extra_details:
        for item in extra_details:
            if not item:
                continue
            lines.append(html.escape(item))
    if snippet:
        lines.append("<b>Traceback:</b>")
        lines.append(f"<code>{html.escape(snippet)}</code>")
    message_text = "\n".join(lines)

    recipients: Set[int] = set()
    try:
        users = await db.list_users()
    except Exception as fetch_exc:
        logger.warning("Failed to fetch users for admin alert", exc_info=fetch_exc)
        users = []
    for row in users or []:
        if not row.get("is_active", 1):
            continue
        if row.get("role") != "admin":
            continue
        telegram_id = row.get("telegram_id")
        if telegram_id is None:
            continue
        try:
            recipients.add(int(telegram_id))
        except Exception:
            continue
    for aid in ADMIN_IDS:
        try:
            recipients.add(int(aid))
        except Exception:
            continue
    if not recipients:
        logger.warning("No admin recipients for alert", context=context)
        return
    for rid in recipients:
        try:
            await bot.send_message(rid, message_text, parse_mode=ParseMode.HTML)
        except Exception as send_exc:
            logger.warning("Failed to deliver admin alert", target=rid, exc_info=send_exc)


# Domain utilities moved to utils/domain.py
# Use resolve_campaign_assignments, extract_domains, render_domain_block from utils/domain.py
# Menu and helper functions moved to handlers/ modules
# Teams callbacks moved to handlers/teams.py
# Menu handlers moved to handlers/menu.py


@dp.message(F.document)
async def on_document_upload(message: Message):
    pending = await db.get_pending_action(message.from_user.id)
    if not pending or pending[0] != "fb:await_csv":
        return
    document = message.document
    if document is None:
        return
    if document.file_size and document.file_size > MAX_CSV_FILE_SIZE_BYTES:
        mb_limit = MAX_CSV_FILE_SIZE_BYTES // (1024 * 1024)
        await message.answer(f"Файл слишком большой (> {mb_limit} МБ). Сожмите выгрузку или поделите на несколько файлов.")
        return
    filename = document.file_name or "upload.csv"
    if not filename.lower().endswith(".csv"):
        await message.answer("Мне нужен .csv файл. Отправьте корректную выгрузку.")
        return
    if document.mime_type and document.mime_type not in CSV_ALLOWED_MIME_TYPES:
        await message.answer("Внимание: тип файла не похож на CSV. Попробую обработать, но если что-то пойдёт не так — выгрузите как CSV.")
    status_msg = await message.answer("Получил файл, обрабатываю…")
    buffer = BytesIO()
    try:
        await bot.download(document, destination=buffer)
    except Exception as exc:
        logger.exception("Failed to download CSV from Telegram", exc_info=exc)
        await status_msg.edit_text("Не удалось скачать файл из Telegram. Попробуйте ещё раз.")
        return
    data = buffer.getvalue()
    try:
        parsed = fb_csv.parse_fb_csv(data)
    except Exception as exc:
        logger.exception("Failed to parse Facebook CSV", exc_info=exc)
        await status_msg.edit_text("Не удалось распарсить CSV. Проверьте, что используете стандартную выгрузку из Ads Manager с разделителем запятая.")
        return
    succeeded = await process_fb_csv_upload(
        bot=bot,
        message=message,
        filename=filename,
        parsed=parsed,
        status_msg=status_msg,
        admin_ids=ADMIN_IDS,
        notify_admins=_notify_admins_about_exception,
    )
    if succeeded:
        await db.clear_pending_action(message.from_user.id)


@dp.callback_query(F.data.startswith("fbua:"))
async def on_fb_upload_account_detail(callback: CallbackQuery):
    data = callback.data or ""
    parts = data.split(":", 2)
    if len(parts) != 3:
        await callback.answer("Не удалось открыть кабинет.", show_alert=True)
        return
    _, upload_id_str, idx_str = parts
    try:
        idx = int(idx_str)
    except Exception:
        await callback.answer("Некорректный индекс кабинета.", show_alert=True)
        return
    kind = f"fbua:{upload_id_str}"
    try:
        cached = await db.get_ui_cache_value(callback.from_user.id, kind, idx)
    except Exception as exc:
        logger.warning("Failed to read FB account cache", exc_info=exc)
        await callback.answer("Не удалось прочитать данные.", show_alert=True)
        return
    if not cached:
        await callback.answer("Данные недоступны. Отправьте CSV заново.", show_alert=True)
        return
    try:
        payload = json.loads(cached)
    except Exception as exc:
        logger.warning("Failed to decode FB account payload", exc_info=exc)
        await callback.answer("Ошибка чтения данных.", show_alert=True)
        return
    chunks = _build_account_detail_messages(payload)
    target_chat = callback.message.chat.id if callback.message else callback.from_user.id
    try:
        for chunk in chunks:
            await bot.send_message(target_chat, chunk, parse_mode=ParseMode.HTML)
    except Exception as exc:
        logger.warning("Failed to send FB account detail", exc_info=exc)
        await callback.answer("Не удалось отправить сообщение.", show_alert=True)
        return
    await callback.answer()


@dp.callback_query(F.data.startswith("fbar:"))
async def on_fb_report_account_detail(callback: CallbackQuery):
    data = callback.data or ""
    parts = data.split(":", 2)
    if len(parts) != 3:
        await callback.answer("Некорректный запрос.", show_alert=True)
        return
    _, month_raw, idx_str = parts
    try:
        idx = int(idx_str)
    except Exception:
        await callback.answer("Некорректный индекс.", show_alert=True)
        return
    kind = f"fbar:{month_raw}"
    try:
        cached = await db.get_ui_cache_value(callback.from_user.id, kind, idx)
    except Exception as exc:
        logger.warning("Failed to read FB report account cache", exc_info=exc)
        await callback.answer("Не удалось прочитать данные.", show_alert=True)
        return
    if not cached:
        await callback.answer("Данные устарели. Перестройте отчёт.", show_alert=True)
        return
    try:
        payload = json.loads(cached)
    except Exception as exc:
        logger.warning("Failed to decode FB report account payload", exc_info=exc)
        await callback.answer("Ошибка чтения данных.", show_alert=True)
        return
    chunks = _build_account_detail_messages(payload)
    target_chat = callback.message.chat.id if callback.message else callback.from_user.id
    try:
        for chunk in chunks:
            await bot.send_message(target_chat, chunk, parse_mode=ParseMode.HTML)
    except Exception as exc:
        logger.warning("Failed to send FB report account detail", exc_info=exc)
        await callback.answer("Не удалось отправить сообщение.", show_alert=True)
        return
    await callback.answer()
# Commands and handlers moved to handlers/ modules

# Mentors, aliases, teams handlers moved to handlers/ modules

@dp.message()
async def on_text_fallback(message: Message):
    # Игнорируем текст-команды
    if message.text and message.text.startswith('/'):
        return
    # обработка pending actions для алиасов/команд/менторов
    pending = await db.get_pending_action(message.from_user.id)
    if not pending:
        return  # игнорируем обычные сообщения, чтобы не засорять чат
    action, _ = pending
    try:
        if action == "fb:await_csv":
            text = (message.text or "").strip()
            if text.lower() in ("-", "стоп", "stop"):
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Загрузка CSV отменена")
            return await message.answer("Пришлите CSV файлом или '-' чтобы отменить ожидание")
        if action == "alias:new":
            alias = message.text.strip()
            await db.set_alias(alias)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Алиас создан. Откройте Алиасы в меню, чтобы назначить buyer/lead")
        if action == "domain:check":
            text = (message.text or "").strip()
            if text.lower() in ("-", "stop", "стоп"):
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Готово. Проверка доменов завершена")
            result = await lookup_domains_text(text)
            await message.answer(result + "\n\nОтправьте следующий домен или '-' чтобы завершить")
            return
        if action == "youtube:await_url":
            if await handle_youtube_download(message):
            return
        if action.startswith("alias:setbuyer:"):
            alias = action.split(":", 2)[2]
            v = message.text.strip()
            if v == '-':
                buyer_id = None
            else:
                try:
                    buyer_id = await _resolve_user_id(v)
                except ValueError:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username. Если пользователь не писал боту, попросите его отправить /start.")
            await db.set_alias(alias, buyer_id=buyer_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Buyer назначен")
        if action.startswith("alias:setlead:"):
            alias = action.split(":", 2)[2]
            v = message.text.strip()
            if v == '-':
                lead_id = None
            else:
                try:
                    lead_id = await _resolve_user_id(v)
                except ValueError:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username. Если пользователь не писал боту, попросите его отправить /start.")
            await db.set_alias(alias, lead_id=lead_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Lead назначен")
        if action == "mentor:add":
            v = message.text.strip()
            try:
                uid = await _resolve_user_id(v)
            except Exception:
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username.")
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_role(uid, "mentor")
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Назначен ментором")
        if action == "team:new":
            name = message.text.strip()
            tid = await db.create_team(name)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer(f"Команда создана: id={tid}")
        if action.startswith("team:setlead:"):
            # format: team:setlead:<team_id>
            team_id = int(action.split(":", 2)[2])
            v = message.text.strip()
            uid = None
            # support tg://user?id=123
            if v.startswith("tg://user?id="):
                try:
                    uid = int(v.split("=",1)[1])
                except Exception:
                    uid = None
            # support @username
            if uid is None and v.startswith("@"):
                uname = v[1:].strip().lower()
                users = await db.list_users()
                hit = next((u for u in users if (u.get("username") or "").lower() == uname), None)
                if hit:
                    uid = int(hit["telegram_id"])  # type: ignore
            # fallback to numeric ID
            if uid is None:
                try:
                    uid = int(v)
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Не удалось распознать пользователя. Пришлите numeric Telegram ID или @username. Если пользователь не писал боту, попросите его отправить /start.")
            # ensure user exists, set user's team and elevate role to lead
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_team(uid, team_id)
            user_row = await db.get_user(uid)
            role_before = (user_row or {}).get("role")
            if role_before not in ("mentor", "admin", "head"):
                await db.set_user_role(uid, "lead")
            await db.set_team_lead_override(team_id, uid)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Лид назначен")
        if action.startswith("myteam:add"):
            users = await db.list_users()
            team_id = None
            parts = action.split(":", 2)
            if len(parts) == 3 and parts[2]:
                try:
                    team_id = int(parts[2])
                except Exception:
                    team_id = None
            if team_id is None:
                team_id = await db.get_primary_lead_team(message.from_user.id)
            if team_id is None:
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Нет прав или команда не найдена")
            v = message.text.strip()
            uid = None
            if v.startswith("tg://user?id="):
                try:
                    uid = int(v.split("=",1)[1])
                except Exception:
                    uid = None
            if uid is None and v.startswith("@"):
                uname = v[1:].strip().lower()
                hit = next((u for u in users if (u.get("username") or "").lower() == uname), None)
                if hit:
                    uid = int(hit["telegram_id"])  # type: ignore
            if uid is None:
                try:
                    uid = int(v)
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username. Если пользователь не писал боту, попросите его отправить /start.")
            # Ensure target exists (create stub if not) before assigning team
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_team(uid, team_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Пользователь добавлен в вашу команду")
        if action.startswith("kpi:set:"):
            which = action.split(":", 2)[2]
            v = message.text.strip()
            goal_val = None
            if v != '-':
                try:
                    goal_val = int(v)
                    if goal_val < 0:
                        goal_val = 0
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Нужно целое число или '-' для очистки")
            current = await db.get_kpi(message.from_user.id)
            daily = current.get('daily_goal')
            weekly = current.get('weekly_goal')
            if which == 'daily':
                daily = goal_val
            else:
                weekly = goal_val
            await db.set_kpi(message.from_user.id, daily_goal=daily, weekly_goal=weekly)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("KPI обновлен")
        # report:filter:* больше не используем — вся фильтрация через picker-кнопки
    except Exception as e:
        logger.exception(e)
        return await message.answer("Ошибка обработки ввода")

# User, team, alias, mentor commands moved to handlers/ modules

# notify_buyer moved to dispatcher

# ===== Reports =====
def _reports_menu(actor_id: int) -> InlineKeyboardMarkup:
    # Build dynamic keyboard; chips will be appended in _send_reports_menu where we have async context
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="Сегодня", callback_data="report:today"), InlineKeyboardButton(text="Вчера", callback_data="report:yesterday")],
        [InlineKeyboardButton(text="Неделя", callback_data="report:week")],
        [InlineKeyboardButton(text="FB кампании", callback_data="report:fb:campaigns"), InlineKeyboardButton(text="FB кабинеты", callback_data="report:fb:accounts")],
        [InlineKeyboardButton(text="Выбрать оффер", callback_data="report:pick:offer"), InlineKeyboardButton(text="Выбрать крео", callback_data="report:pick:creative")],
        [InlineKeyboardButton(text="Выбрать байера", callback_data="report:pick:buyer"), InlineKeyboardButton(text="Выбрать команду", callback_data="report:pick:team")],
    ]
    rows.append([InlineKeyboardButton(text="Сбросить фильтры", callback_data="report:f:clear")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _send_reports_menu(chat_id: int, actor_id: int):
    # Build active filters line
    filt = await db.get_report_filter(actor_id)
    text = "Отчеты — выберите период:"
    if filt.get('offer') or filt.get('creative') or filt.get('buyer_id') or filt.get('team_id'):
        users = await db.list_users()
        teams = await db.list_teams()
        fparts: list[str] = []
        if filt.get('offer'):
            fparts.append(f"offer=<code>{filt['offer']}</code>")
        if filt.get('creative'):
            fparts.append(f"creative=<code>{filt['creative']}</code>")
        if filt.get('buyer_id'):
            bid = int(filt['buyer_id'])
            bu = next((u for u in users if int(u['telegram_id']) == bid), None)
            if bu and (bu.get('username') or bu.get('full_name')):
                cap = f"@{bu['username']}" if bu.get('username') else (bu.get('full_name') or str(bid))
            else:
                cap = str(bid)
            fparts.append(f"buyer=<code>{cap}</code>")
        if filt.get('team_id'):
            tid = int(filt['team_id'])
            tn = next((t['name'] for t in teams if int(t['id']) == tid), str(tid))
            fparts.append(f"team=<code>{tn}</code>")
        text += "\n🔎 Фильтры: " + ", ".join(fparts)
    # Build keyboard with chips
    kb = _reports_menu(actor_id)
    chips_rows: list[list[InlineKeyboardButton]] = []
    def trunc(s: str, n: int = 24) -> str:
        s = str(s)
        return s if len(s) <= n else (s[:n-1] + "…")
    chip_row: list[InlineKeyboardButton] = []
    if filt.get('offer'):
        chip_row.append(InlineKeyboardButton(text=f"❌ offer:{trunc(filt['offer'])}", callback_data="report:clear:offer"))
    if filt.get('creative'):
        chip_row.append(InlineKeyboardButton(text=f"❌ cr:{trunc(filt['creative'])}", callback_data="report:clear:creative"))
    if chip_row:
        chips_rows.append(chip_row)
    chip_row2: list[InlineKeyboardButton] = []
    if filt.get('buyer_id'):
        users = await db.list_users()
        bid = int(filt['buyer_id'])
        bu = next((u for u in users if int(u['telegram_id']) == bid), None)
        bcap = f"@{bu['username']}" if bu and bu.get('username') else (bu.get('full_name') if bu and bu.get('full_name') else str(bid))
        chip_row2.append(InlineKeyboardButton(text=f"❌ buyer:{trunc(bcap)}", callback_data="report:clear:buyer"))
    if filt.get('team_id'):
        teams = await db.list_teams()
        tid = int(filt['team_id'])
        tname = next((t['name'] for t in teams if int(t['id']) == tid), str(tid))
        chip_row2.append(InlineKeyboardButton(text=f"❌ team:{trunc(tname)}", callback_data="report:clear:team"))
    if chip_row2:
        chips_rows.append(chip_row2)
    # Append chips rows before the final reset row
    kb.inline_keyboard = kb.inline_keyboard[:-1] + chips_rows + kb.inline_keyboard[-1:]
    await bot.send_message(chat_id, text, reply_markup=kb)


def _build_fb_month_keyboard(kind: str, months: list[date]) -> InlineKeyboardMarkup:
    today_month = date.today().replace(day=1)
    seen: set[date] = {today_month}
    entries: list[tuple[str, date]] = [("📅 Текущий месяц", today_month)]
    for month in months:
        normalized = month.replace(day=1)
        if normalized in seen:
            continue
        entries.append((_month_label_ru(normalized), normalized))
        seen.add(normalized)

    buttons: list[list[InlineKeyboardButton]] = []
    if entries:
        label, value = entries[0]
        buttons.append([
            InlineKeyboardButton(
                text=label,
                callback_data=f"report:fb:month:{kind}:{value.isoformat()}",
            )
        ])
    row: list[InlineKeyboardButton] = []
    for label, value in entries[1:]:
        row.append(
            InlineKeyboardButton(
                text=label,
                callback_data=f"report:fb:month:{kind}:{value.isoformat()}",
            )
        )
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="↩️ Назад", callback_data="report:fb:back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def _send_fb_campaign_report(chat_id: int, month_start: date) -> None:
    month = month_start.replace(day=1)
    rows = await db.fetch_fb_campaign_month_report(month)
    if not rows:
        await bot.send_message(chat_id, f"Нет данных по FB кампаниям за {html.escape(_month_label_ru(month))}.", parse_mode=ParseMode.HTML)
        return
    flag_rows = await db.list_fb_flags()
    flags_by_id: dict[int, dict[str, Any]] = {}
    for row in flag_rows:
        fid = row.get("id")
        if fid is None:
            continue
        try:
            flags_by_id[int(fid)] = row
        except Exception:
            continue
    users = await db.list_users()
    users_by_id: dict[int, dict[str, Any]] = {}
    for user in users:
        tid = user.get("telegram_id")
        if tid is None:
            continue
        try:
            users_by_id[int(tid)] = user
        except Exception:
            continue
    total_spend = Decimal("0")
    total_revenue = Decimal("0")
    total_ftd = 0
    total_impressions = 0
    total_clicks = 0
    total_registrations = 0
    lines: list[str] = []
    for idx, row in enumerate(rows, start=1):
        spend = _as_decimal(row.get("spend"))
        revenue = _as_decimal(row.get("revenue"))
        impressions = int(row.get("impressions") or 0)
        clicks = int(row.get("clicks") or 0)
        registrations = int(row.get("registrations") or 0)
        ftd = int(row.get("ftd") or 0)
        total_spend += spend
        total_revenue += revenue
        total_ftd += ftd
        total_impressions += impressions
        total_clicks += clicks
        total_registrations += registrations
        roi = ((revenue - spend) / spend * Decimal(100)) if spend else None
        ftd_rate = (Decimal(ftd) / Decimal(registrations) * Decimal(100)) if registrations else None
        ctr = (Decimal(clicks) / Decimal(impressions) * Decimal(100)) if impressions else None
        campaign_name = html.escape(str(row.get("campaign_name") or "—"))
        account_name = html.escape(str(row.get("account_name") or "—"))
        buyer_label = _format_buyer_label(row.get("buyer_id"), users_by_id)
        prev_flag_label = html.escape(_format_flag_label(row.get("prev_flag_id"), flags_by_id))
        decision = fb_csv.decide_flag(spend, ctr, roi, ftd)
        curr_flag_label = html.escape(_format_flag_decision(decision))
        line = (
            f"{idx}) <code>{campaign_name}</code> | Акк: <code>{account_name}</code> | "
            f"Байер: {buyer_label} | Spend {_fmt_money(spend)} | FTD {ftd} | "
            f"Rev {_fmt_money(revenue)} | ROI {_fmt_percent(roi)} | FTD rate {_fmt_percent(ftd_rate)} | "
            f"Флаг: {prev_flag_label} → {curr_flag_label}"
        )
        lines.append(line)
        lines.append("")
    header_lines = [
        f"<b>FB кампании — {html.escape(_month_label_ru(month))}</b>",
        f"Кампаний с активностью: <b>{len(rows)}</b>",
        f"Общий Spend: <b>{_fmt_money(total_spend)}</b>",
        f"Общий Rev: <b>{_fmt_money(total_revenue)}</b>",
        f"FTD: <b>{total_ftd}</b>",
    ]
    overall_roi = ((total_revenue - total_spend) / total_spend * Decimal(100)) if total_spend else None
    header_lines.append(f"ROI: <b>{_fmt_percent(overall_roi)}</b>")
    if total_impressions:
        ctr = (Decimal(total_clicks) / Decimal(total_impressions) * Decimal(100)) if total_impressions else None
        header_lines.append(f"CTR: <b>{_fmt_percent(ctr)}</b> ({total_clicks}/{total_impressions})")
    if total_registrations:
        header_lines.append(f"Регистраций: <b>{total_registrations}</b>")
    all_lines: List[str] = header_lines[:]
    if lines:
        all_lines.append("")
        # remove trailing blank line for nicer formatting
        while lines and lines[-1] == "":
            lines.pop()
        all_lines.extend(lines)
    chunks = chunk_lines(all_lines)
    for chunk in chunks:
        await bot.send_message(chat_id, chunk, parse_mode=ParseMode.HTML)


async def _send_fb_account_report(chat_id: int, month_start: date, requester_id: int) -> None:
    month = month_start.replace(day=1)
    rows = await db.fetch_fb_campaign_month_report(month)
    if not rows:
        await bot.send_message(chat_id, f"Нет данных по FB кабинетам за {html.escape(_month_label_ru(month))}.", parse_mode=ParseMode.HTML)
        return
    flag_rows = await db.list_fb_flags()
    flags_by_id: dict[int, dict[str, Any]] = {}
    severity_by_id: dict[int, int] = {}
    for row in flag_rows:
        fid = row.get("id")
        if fid is None:
            continue
        try:
            fid_int = int(fid)
        except Exception:
            continue
        flags_by_id[fid_int] = row
        try:
            severity_by_id[fid_int] = int(row.get("severity") or 0)
        except Exception:
            severity_by_id[fid_int] = 0
    users = await db.list_users()
    users_by_id: dict[int, dict[str, Any]] = {}
    for user in users:
        tid = user.get("telegram_id")
        if tid is None:
            continue
        try:
            users_by_id[int(tid)] = user
        except Exception:
            continue
    accounts: dict[str, dict[str, Any]] = {}
    for row in rows:
        account_name_raw = str(row.get("account_name") or "—")
        entry = accounts.setdefault(
            account_name_raw,
            {
                "spend": Decimal("0"),
                "revenue": Decimal("0"),
                "impressions": 0,
                "clicks": 0,
                "registrations": 0,
                "ftd": 0,
                "campaigns": set(),
                "buyers": set(),
                "prev_flag_id": None,
                "prev_flag_severity": -1,
                "curr_flag_id": None,
                "curr_flag_severity": -1,
                "campaign_lines": [],
            },
        )
        spend = _as_decimal(row.get("spend"))
        revenue = _as_decimal(row.get("revenue"))
        impressions = int(row.get("impressions") or 0)
        clicks = int(row.get("clicks") or 0)
        registrations = int(row.get("registrations") or 0)
        ftd = int(row.get("ftd") or 0)
        entry["spend"] += spend
        entry["revenue"] += revenue
        entry["impressions"] += impressions
        entry["clicks"] += clicks
        entry["registrations"] += registrations
        entry["ftd"] += ftd
        buyer_id = row.get("buyer_id")
        if buyer_id is not None:
            try:
                entry["buyers"].add(int(buyer_id))
            except Exception:
                pass
        campaign_name = row.get("campaign_name")
        if campaign_name:
            entry["campaigns"].add(str(campaign_name))
        roi_single = ((revenue - spend) / spend * Decimal(100)) if spend else None
        ftd_rate_single = (Decimal(ftd) / Decimal(registrations) * Decimal(100)) if registrations else None
        ctr_single = (Decimal(clicks) / Decimal(impressions) * Decimal(100)) if impressions else None
        campaign_decision = fb_csv.decide_flag(spend, ctr_single, roi_single, ftd)
        campaign_flag = _format_flag_decision(campaign_decision)
        entry.setdefault("campaign_lines", []).append(
            "• <code>"
            + html.escape(str(campaign_name or "—"))
            + "</code> — "
            + html.escape(campaign_flag)
            + f". Spend {_fmt_money(spend)} | FTD {ftd} | Rev {_fmt_money(revenue)} | ROI {_fmt_percent(roi_single)}"
        )
        prev_flag_id = row.get("prev_flag_id")
        if prev_flag_id is not None:
            try:
                fid = int(prev_flag_id)
                severity = severity_by_id.get(fid, 0)
                if severity > entry["prev_flag_severity"]:
                    entry["prev_flag_severity"] = severity
                    entry["prev_flag_id"] = prev_flag_id
            except Exception:
                pass
        curr_flag_id = row.get("curr_flag_id") or row.get("state_flag_id")
        if curr_flag_id is not None:
            try:
                fid = int(curr_flag_id)
                severity = severity_by_id.get(fid, 0)
                if severity > entry["curr_flag_severity"]:
                    entry["curr_flag_severity"] = severity
                    entry["curr_flag_id"] = curr_flag_id
            except Exception:
                pass
    for info in accounts.values():
        spend = info["spend"]
        revenue = info["revenue"]
        impressions = info["impressions"]
        clicks = info["clicks"]
        registrations = info["registrations"]
        ctr_value = (
            (Decimal(clicks) / Decimal(impressions) * Decimal(100))
            if impressions
            else None
        )
        roi_value = ((revenue - spend) / spend * Decimal(100)) if spend else None
        decision = fb_csv.decide_flag(spend, ctr_value, roi_value, info["ftd"])
        info["decision"] = decision
        info["roi"] = roi_value
        info["ctr"] = ctr_value
        info["ftd_rate"] = (
            (Decimal(info["ftd"]) / Decimal(registrations) * Decimal(100))
            if registrations
            else None
        )
        info["flag_label"] = _format_flag_decision(decision)
    sorted_accounts = sorted(accounts.items(), key=lambda item: item[1]["spend"], reverse=True)
    total_spend = sum((info["spend"] for _, info in sorted_accounts), Decimal("0"))
    total_revenue = sum((info["revenue"] for _, info in sorted_accounts), Decimal("0"))
    total_ftd = sum(info["ftd"] for _, info in sorted_accounts)
    total_impressions = sum(info["impressions"] for _, info in sorted_accounts)
    total_clicks = sum(info["clicks"] for _, info in sorted_accounts)
    total_registrations = sum(info["registrations"] for _, info in sorted_accounts)
    lines: list[str] = []
    max_items = 20
    display_count = min(max_items, len(sorted_accounts))
    account_cache_values: List[str] = []
    account_keyboard_rows: List[List[InlineKeyboardButton]] = []
    cache_kind = f"fbar:{month.isoformat()}"
    for idx, (account_name_raw, info) in enumerate(sorted_accounts[:max_items], start=1):
        spend = info["spend"]
        revenue = info["revenue"]
        registrations = info["registrations"]
        ftd = info["ftd"]
        roi = ((revenue - spend) / spend * Decimal(100)) if spend else None
        ftd_rate = (Decimal(ftd) / Decimal(registrations) * Decimal(100)) if registrations else None
        buyer_labels = [
            _format_buyer_label(bid, users_by_id)
            for bid in sorted(info["buyers"])
        ]
        if len(buyer_labels) > 3:
            buyers_text = ", ".join(buyer_labels[:3]) + f" (+{len(buyer_labels) - 3})"
        elif buyer_labels:
            buyers_text = ", ".join(buyer_labels)
        else:
            buyers_text = "—"
        prev_flag_label = html.escape(_format_flag_label(info["prev_flag_id"], flags_by_id))
        curr_flag_label = html.escape(info.get("flag_label") or _format_flag_decision(info.get("decision")))
        account_name = html.escape(account_name_raw)
        line = (
            f"{idx}) <code>{account_name}</code> | Кампаний: {len(info['campaigns'])} | "
            f"Байеры: {buyers_text} | Spend {_fmt_money(spend)} | FTD {ftd} | "
            f"Rev {_fmt_money(revenue)} | ROI {_fmt_percent(roi)} | FTD rate {_fmt_percent(ftd_rate)} | "
            f"Флаг: {prev_flag_label} → {curr_flag_label}"
        )
        lines.append(line)
        if idx < display_count:
            lines.append("")
        payload = {
            "account_name": account_name_raw,
            "flag_label": info.get("flag_label"),
            "spend": str(spend),
            "revenue": str(revenue),
            "roi": str(roi) if roi is not None else None,
            "ftd": ftd,
            "campaign_count": len(info["campaigns"]),
            "campaign_lines": info.get("campaign_lines", []),
            "ctr": str(info.get("ctr")) if info.get("ctr") is not None else None,
            "ftd_rate": str(info.get("ftd_rate")) if info.get("ftd_rate") is not None else None,
        }
        account_cache_values.append(json.dumps(payload))
        flag_icon = (info.get("flag_label") or "").split(" ", 1)[0] if info.get("flag_label") else "—"
        short_name = account_name_raw
        if len(short_name) > 28:
            short_name = short_name[:27] + "…"
        button_text = f"{idx}. {flag_icon} {short_name}".strip()
        if len(button_text) > 64:
            button_text = button_text[:63] + "…"
        account_keyboard_rows.append(
            [InlineKeyboardButton(text=button_text, callback_data=f"fbar:{month.isoformat()}:{idx - 1}")]
        )
    header_lines = [
        f"<b>FB кабинеты — {html.escape(_month_label_ru(month))}</b>",
        f"Кабинетов: <b>{len(sorted_accounts)}</b>",
        f"Общий Spend: <b>{_fmt_money(total_spend)}</b>",
        f"Общий Rev: <b>{_fmt_money(total_revenue)}</b>",
        f"FTD: <b>{total_ftd}</b>",
    ]
    overall_roi = ((total_revenue - total_spend) / total_spend * Decimal(100)) if total_spend else None
    header_lines.append(f"ROI: <b>{_fmt_percent(overall_roi)}</b>")
    if total_impressions:
        ctr = (Decimal(total_clicks) / Decimal(total_impressions) * Decimal(100)) if total_impressions else None
        header_lines.append(f"CTR: <b>{_fmt_percent(ctr)}</b> ({total_clicks}/{total_impressions})")
    if total_registrations:
        header_lines.append(f"Регистраций: <b>{total_registrations}</b>")
    summary_lines: List[str] = header_lines[:]
    if lines:
        summary_lines.append("")
        summary_lines.extend(lines)
    if len(sorted_accounts) > max_items:
        summary_lines.append("")
        summary_lines.append(f"Показаны первые {max_items} кабинетов из {len(sorted_accounts)}.")
    keyboard_markup: Optional[InlineKeyboardMarkup] = None
    if account_keyboard_rows:
        summary_lines.append("")
        summary_lines.append("Нажми кнопку ниже, чтобы раскрыть кабинет.")
        keyboard_markup = InlineKeyboardMarkup(inline_keyboard=account_keyboard_rows[:12])
    chunks = chunk_lines(summary_lines)
    if chunks:
        await bot.send_message(chat_id, chunks[0], parse_mode=ParseMode.HTML, reply_markup=keyboard_markup)
        for chunk in chunks[1:]:
            await bot.send_message(chat_id, chunk, parse_mode=ParseMode.HTML)
    else:
        await bot.send_message(chat_id, "Нет кабинетов для отображения.", parse_mode=ParseMode.HTML)
    try:
        await db.set_ui_cache_list(requester_id, cache_kind, account_cache_values)
    except Exception as exc:
        logger.warning("Failed to cache FB account report payloads", exc_info=exc)

async def _resolve_scope_user_ids(actor_id: int) -> list[int]:
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = (me or {}).get("role", "buyer")
    if actor_id in ADMIN_IDS:
        my_role = "admin"
    allowed_roles = {"buyer", "lead", "mentor", "head"}
    if my_role in ("admin", "head"):
        # Include buyers, leads, mentors; exclude admins/heads
        return [int(u["telegram_id"]) for u in users if u.get("is_active") and (u.get("role") in allowed_roles)]
    lead_team_ids = await db.list_user_lead_teams(actor_id)
    scoped_ids: list[int] = []
    if lead_team_ids:
        for team_id in lead_team_ids:
            scoped_ids.extend(
                int(u["telegram_id"]) for u in users
                if u.get("team_id") is not None and int(u.get("team_id")) == int(team_id)
                and u.get("is_active") and (u.get("role") in allowed_roles)
            )
    if my_role == "mentor":
        team_ids = set(await db.list_mentor_teams(actor_id))
        scoped_ids.extend(
            int(u["telegram_id"]) for u in users
            if u.get("team_id") in team_ids and u.get("is_active") and (u.get("role") in allowed_roles)
        )
    if scoped_ids:
        if actor_id not in scoped_ids:
            scoped_ids.append(actor_id)
        # deduplicate while preserving order
        seen: set[int] = set()
        result: list[int] = []
        for uid in scoped_ids:
            if uid not in seen:
                seen.add(uid)
                result.append(uid)
        return result
    return [actor_id]

def _report_text(title: str, agg: dict) -> str:
    lines = [f"📊 <b>{title}</b>"]
    lines.append(f"📈 Депозитов: <b>{agg.get('count',0)}</b>")
    profit = agg.get('profit', 0.0)
    lines.append(f"💰 Профит: <b>{int(round(profit))}</b>")
    total = agg.get('total', 0)
    if total:
        cr = (agg.get('count',0) / total) * 100.0
        lines.append(f"🎯 CR: <b>{cr:.1f}%</b> (из {total})")
    if agg.get('top_offer'):
        toc = agg.get('top_offer_count') or 0
        suffix = f" — {toc}" if toc else ""
        lines.append(f"🏆 Топ-оффер: <code>{agg['top_offer']}</code>{suffix}")
    if agg.get('geo_dist'):
        # filter out unknown entries
        geo_items = [(k, v) for k, v in agg['geo_dist'].items() if k and k != '-' ]
        if geo_items:
            geos = ", ".join(f"{k}:{v}" for k, v in geo_items[:5])
            lines.append(f"🌍 Гео: {geos}")
    # replace sources with top creatives
    if agg.get('creative_dist'):
        cr_items = [(k, v) for k, v in agg['creative_dist'].items() if k and str(k).strip()]
        if cr_items:
            crs = ", ".join(f"{k}:{v}" for k, v in cr_items[:5])
            lines.append(f"🎬 Креативы: {crs}")
    return "\n".join(lines)

async def _send_period_report(chat_id: int, actor_id: int, title: str, days: int | None = None, yesterday: bool = False):
    from datetime import datetime, timezone, timedelta
    try:
        logger.info(f"Building report: title={title}, days={days}, yesterday={yesterday}, actor_id={actor_id}, chat_id={chat_id}")
    users = await db.list_users()
        logger.info(f"Got {len(users)} users")
        if not users:
            logger.warning("No users found in database")
    user_ids = await _resolve_scope_user_ids(actor_id)
        logger.info(f"Resolved {len(user_ids)} user_ids: {user_ids[:5] if user_ids else []}")
        if not user_ids:
            logger.warning(f"No user_ids resolved for actor_id={actor_id}, sending empty report")
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    if yesterday:
        end = start
        start = end - timedelta(days=1)
    if days is not None:
        start = (now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days-1))
        end = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        logger.info(f"Time range: {start} to {end}")
    filt = await db.get_report_filter(actor_id)
        logger.info(f"Filters: {filt}")
    filter_user_ids: list[int] | None = None
    if filt.get('buyer_id') or filt.get('team_id'):
        me = next((u for u in users if u["telegram_id"] == actor_id), None)
        role = (me or {}).get("role", "buyer")
        if actor_id in ADMIN_IDS:
            role = "admin"
        allowed_ids = set(user_ids)
        if filt.get('buyer_id'):
            bid = int(filt['buyer_id'])
            filter_user_ids = [bid] if bid in allowed_ids else []
        elif filt.get('team_id'):
            tid = int(filt['team_id'])
            team_ids = [int(u['telegram_id']) for u in users if u.get('team_id') == tid and u.get('is_active')]
            filter_user_ids = [uid for uid in team_ids if uid in allowed_ids]
        logger.info(f"Calling aggregate_sales with {len(user_ids)} user_ids, start={start}, end={end}")
        try:
    agg = await db.aggregate_sales(user_ids, start, end, offer=filt.get('offer'), creative=filt.get('creative'), filter_user_ids=filter_user_ids)
            logger.info(f"Aggregate result: count={agg.get('count')}, profit={agg.get('profit')}, top_offer={agg.get('top_offer')}")
        except Exception as agg_err:
            logger.exception(f"Error in aggregate_sales: {agg_err}", exc_info=agg_err)
            raise
    text = _report_text(title, agg)
        logger.info("Report text generated")
    # Append buyer breakdown if available
    buyer_dist = agg.get('buyer_dist') or {}
    if buyer_dist:
        # If team filter set, limit to that team (already limited in query by filter_user_ids, but double-check)
        team_filter = filt.get('team_id')
        buyers_map: dict[int, dict] = {int(u['telegram_id']): u for u in users}
        # Order by count desc
        items = sorted(buyer_dist.items(), key=lambda kv: kv[1], reverse=True)
        lines = []
        for uid, cnt in items:
            u = buyers_map.get(int(uid))
            if team_filter:
                try:
                    if not (u and u.get('team_id') and int(u.get('team_id')) == int(team_filter)):
                        continue
                except Exception:
                    continue
            if not u:
                label = f"<code>{uid}</code>"
            else:
                label = f"@{u['username']}" if u.get('username') else (u.get('full_name') or f"<code>{uid}</code>")
            lines.append(f"{label}: <b>{cnt}</b>")
        if lines:
            text += "\n\n" + "\n".join(lines)
    if days == 7 and not yesterday:
            logger.info("Fetching trend data")
        trend = await db.trend_daily_sales(user_ids, days=7)
        if trend:
            tline = ", ".join(f"{d.split('-')[-1]}:{c}" for d, c in trend)
            text += f"\n📅 Тренд (7д): {tline}"
    if filt.get('offer') or filt.get('creative') or filt.get('buyer_id') or filt.get('team_id'):
        teams = await db.list_teams()
        fparts: list[str] = []
        if filt.get('offer'):
            fparts.append(f"offer=<code>{filt['offer']}</code>")
        if filt.get('creative'):
            fparts.append(f"creative=<code>{filt['creative']}</code>")
        if filt.get('buyer_id'):
            bid = int(filt['buyer_id'])
            bu = next((u for u in users if int(u['telegram_id']) == bid), None)
            if bu and (bu.get('username') or bu.get('full_name')):
                cap = f"@{bu['username']}" if bu.get('username') else (bu.get('full_name') or str(bid))
            else:
                cap = str(bid)
            fparts.append(f"buyer=<code>{cap}</code>")
        if filt.get('team_id'):
            tid = int(filt['team_id'])
            tn = next((t['name'] for t in teams if int(t['id']) == tid), str(tid))
            fparts.append(f"team=<code>{tn}</code>")
        text += "\n🔎 Фильтры: " + ", ".join(fparts)
        logger.info(f"Sending report message (length={len(text)})")
        await bot.send_message(chat_id, text, reply_markup=_reports_menu(actor_id), parse_mode=ParseMode.HTML)
        logger.info("Report sent successfully")
    except Exception as e:
        logger.exception(f"Error in _send_period_report: {e}", exc_info=e)
        raise

@dp.callback_query(F.data == "report:fb:campaigns")
async def cb_report_fb_campaigns(call: CallbackQuery):
    months = await db.list_fb_available_months()
    if not months:
        await call.message.answer("Нет данных Facebook. Загрузите CSV, чтобы сформировать отчёт.")
    else:
        await call.message.answer(
            "Выберите месяц для отчёта по кампаниям:",
            reply_markup=_build_fb_month_keyboard("campaigns", months),
        )
    try:
        await call.answer()
    except Exception:
        pass


@dp.callback_query(F.data == "report:fb:accounts")
async def cb_report_fb_accounts(call: CallbackQuery):
    months = await db.list_fb_available_months()
    if not months:
        await call.message.answer("Нет данных Facebook. Загрузите CSV, чтобы сформировать отчёт.")
    else:
        await call.message.answer(
            "Выберите месяц для отчёта по кабинетам:",
            reply_markup=_build_fb_month_keyboard("accounts", months),
        )
    try:
        await call.answer()
    except Exception:
        pass


@dp.callback_query(F.data == "report:fb:back")
async def cb_report_fb_back(call: CallbackQuery):
    await _send_reports_menu(call.message.chat.id, call.from_user.id)
    try:
        await call.answer()
    except Exception:
        pass


@dp.callback_query(F.data.startswith("report:fb:month:"))
async def cb_report_fb_month(call: CallbackQuery):
    parts = call.data.split(":", 4)
    if len(parts) != 5:
        await call.answer("Некорректный запрос", show_alert=True)
        return
    kind = parts[3]
    month_raw = parts[4]
    try:
        month = date.fromisoformat(month_raw)
    except ValueError:
        await call.answer("Некорректная дата", show_alert=True)
        return
    status_msg = await call.message.answer("Готовлю отчёт…")
    try:
        if kind == "campaigns":
            await _send_fb_campaign_report(call.message.chat.id, month)
        elif kind == "accounts":
            await _send_fb_account_report(call.message.chat.id, month, call.from_user.id)
        else:
            await call.message.answer("Неизвестный тип отчёта.")
        await status_msg.edit_text("Отчёт готов.")
    except Exception as exc:
        logger.exception("Failed to build FB report: {}", exc)
        await status_msg.edit_text(
            f"Не удалось построить отчёт: <code>{type(exc).__name__}: {exc}</code>",
            parse_mode=ParseMode.HTML,
        )
    finally:
        try:
            await call.answer()
        except Exception:
            pass


@dp.callback_query(F.data == "report:today")
async def cb_report_today(call: CallbackQuery):
    logger.info(f"Report today requested by user {call.from_user.id}")
    try:
        await call.answer()  # Remove loading indicator immediately
    except Exception as e:
        logger.warning(f"Failed to answer callback: {e}")
    try:
        status_msg = await call.message.answer("Готовлю отчёт…")
        logger.debug("Status message sent")
    except Exception as e:
        logger.warning(f"Failed to send status message: {e}")
        status_msg = None
    try:
        logger.info(f"Calling _send_period_report for user {call.from_user.id}")
        await _send_period_report(call.message.chat.id, call.from_user.id, "Сегодня", None, False)
        logger.info("Report sent successfully")
        if status_msg:
            try:
                await status_msg.delete()
            except Exception:
                pass
    except Exception as e:
        logger.exception(f"Failed to build report for user {call.from_user.id}", exc_info=e)
        error_text = f"Не удалось построить отчёт: <code>{html.escape(str(type(e).__name__))}: {html.escape(str(e))}</code>"
        if status_msg:
            try:
                await status_msg.edit_text(error_text, parse_mode=ParseMode.HTML)
            except Exception:
                try:
                    await call.message.answer(error_text, parse_mode=ParseMode.HTML)
                except Exception as send_err:
                    logger.error(f"Failed to send error message: {send_err}")
        else:
            try:
                await call.message.answer(error_text, parse_mode=ParseMode.HTML)
            except Exception as send_err:
                logger.error(f"Failed to send error message: {send_err}")

@dp.callback_query(F.data == "report:yesterday")
async def cb_report_yesterday(call: CallbackQuery):
    logger.info(f"Report yesterday requested by user {call.from_user.id}")
    try:
        await call.answer()  # Remove loading indicator immediately
    except Exception as e:
        logger.warning(f"Failed to answer callback: {e}")
    try:
        status_msg = await call.message.answer("Готовлю отчёт…")
        logger.debug("Status message sent")
    except Exception as e:
        logger.warning(f"Failed to send status message: {e}")
        status_msg = None
    try:
        logger.info(f"Calling _send_period_report for user {call.from_user.id}")
        await _send_period_report(call.message.chat.id, call.from_user.id, "Вчера", None, True)
        logger.info("Report sent successfully")
        if status_msg:
            try:
                await status_msg.delete()
            except Exception:
                pass
    except Exception as e:
        logger.exception(f"Failed to build report for user {call.from_user.id}", exc_info=e)
        error_text = f"Не удалось построить отчёт: <code>{html.escape(str(type(e).__name__))}: {html.escape(str(e))}</code>"
        if status_msg:
            try:
                await status_msg.edit_text(error_text, parse_mode=ParseMode.HTML)
            except Exception:
                try:
                    await call.message.answer(error_text, parse_mode=ParseMode.HTML)
                except Exception as send_err:
                    logger.error(f"Failed to send error message: {send_err}")
        else:
            try:
                await call.message.answer(error_text, parse_mode=ParseMode.HTML)
            except Exception as send_err:
                logger.error(f"Failed to send error message: {send_err}")

@dp.callback_query(F.data == "report:week")
async def cb_report_week(call: CallbackQuery):
    logger.info(f"Report week requested by user {call.from_user.id}")
    try:
        await call.answer()  # Remove loading indicator immediately
    except Exception as e:
        logger.warning(f"Failed to answer callback: {e}")
    try:
        status_msg = await call.message.answer("Готовлю отчёт…")
        logger.debug("Status message sent")
    except Exception as e:
        logger.warning(f"Failed to send status message: {e}")
        status_msg = None
    try:
        logger.info(f"Calling _send_period_report for user {call.from_user.id}")
        await _send_period_report(call.message.chat.id, call.from_user.id, "Последние 7 дней", 7, False)
        logger.info("Report sent successfully")
        if status_msg:
            try:
                await status_msg.delete()
            except Exception:
                pass
    except Exception as e:
        logger.exception(f"Failed to build report for user {call.from_user.id}", exc_info=e)
        error_text = f"Не удалось построить отчёт: <code>{html.escape(str(type(e).__name__))}: {html.escape(str(e))}</code>"
        if status_msg:
            try:
                await status_msg.edit_text(error_text, parse_mode=ParseMode.HTML)
            except Exception:
                try:
                    await call.message.answer(error_text, parse_mode=ParseMode.HTML)
                except Exception as send_err:
                    logger.error(f"Failed to send error message: {send_err}")
        else:
            try:
                await call.message.answer(error_text, parse_mode=ParseMode.HTML)
            except Exception as send_err:
                logger.error(f"Failed to send error message: {send_err}")

@dp.message(Command("today"))
async def on_today(message: Message):
    try:
        await message.answer("Готовлю отчёт…")
    except Exception:
        pass
    try:
        await _send_period_report(message.chat.id, message.from_user.id, "Сегодня")
    except Exception as e:
        logger.exception(e)
        await message.answer(f"Не удалось построить отчёт: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)

@dp.message(Command("yesterday"))
async def on_yesterday(message: Message):
    try:
        await message.answer("Готовлю отчёт…")
    except Exception:
        pass
    try:
        await _send_period_report(message.chat.id, message.from_user.id, "Вчера", None, True)
    except Exception as e:
        logger.exception(e)
        await message.answer(f"Не удалось построить отчёт: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)

@dp.message(Command("week"))
async def on_week(message: Message):
    try:
        await message.answer("Готовлю отчёт…")
    except Exception:
        pass
    try:
        await _send_period_report(message.chat.id, message.from_user.id, "Последние 7 дней", 7)
    except Exception as e:
        logger.exception(e)
        await message.answer(f"Не удалось построить отчёт: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("report:f:"))
async def cb_report_filter(call: CallbackQuery):
    _, _, key = call.data.split(":", 2)
    if key == "clear":
        await db.clear_report_filter(call.from_user.id)
        await call.message.answer("Фильтры сброшены")
        # Reopen reports menu after full clear (do not auto-send any report)
        try:
            await _send_reports_menu(call.message.chat.id, call.from_user.id)
        except Exception:
            pass
        await call.answer()
        return
    await call.answer()

@dp.callback_query(F.data.startswith("report:clear:"))
async def cb_report_clear_chip(call: CallbackQuery):
    _, _, which = call.data.split(":", 2)
    cur = await db.get_report_filter(call.from_user.id)
    offer = cur.get('offer')
    creative = cur.get('creative')
    buyer_id = cur.get('buyer_id')
    team_id = cur.get('team_id')
    if which == 'offer':
        offer = None
    elif which == 'creative':
        creative = None
    elif which == 'buyer':
        buyer_id = None
    elif which == 'team':
        team_id = None
    await db.set_report_filter(call.from_user.id, offer, creative, buyer_id=buyer_id, team_id=team_id)
    try:
        await call.message.answer("Фильтр снят")
    except Exception:
        pass
    # Reopen menu only (do not auto-send any report)
    await _send_reports_menu(call.message.chat.id, call.from_user.id)
    try:
        await call.answer()
    except Exception:
        pass

def _teams_picker_kb(teams: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for t in teams[:50]:
        rows.append([InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"report:set:team:{t['id']}")])
    rows.append([InlineKeyboardButton(text="Очистить", callback_data="report:set:team:-")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _buyers_picker_kb(users: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for u in users[:50]:
        cap = f"@{u['username'] or u['telegram_id']} ({u['full_name'] or ''})"
        rows.append([InlineKeyboardButton(text=cap, callback_data=f"report:set:buyer:{u['telegram_id']}")])
    rows.append([InlineKeyboardButton(text="Очистить", callback_data="report:set:buyer:-")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _offers_picker_kb(offers: list[str]) -> InlineKeyboardMarkup:
    rows = []
    for i, o in enumerate(offers[:50]):
        cap = (o or "(пусто)")
        if len(cap) > 60:
            cap = cap[:59] + "…"
        rows.append([InlineKeyboardButton(text=cap, callback_data=f"report:set:offer_idx:{i}")])
    rows.append([InlineKeyboardButton(text="Очистить", callback_data="report:set:offer:-")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _creatives_picker_kb(creatives: list[str]) -> InlineKeyboardMarkup:
    rows = []
    for i, c in enumerate(creatives[:50]):
        cap = (c or "(пусто)")
        if len(cap) > 60:
            cap = cap[:59] + "…"
        rows.append([InlineKeyboardButton(text=cap, callback_data=f"report:set:creative_idx:{i}")])
    rows.append([InlineKeyboardButton(text="Очистить", callback_data="report:set:creative:-")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "report:pick:team")
async def cb_report_pick_team(call: CallbackQuery):
    try:
        await call.message.answer("Открываю список команд…")
    except Exception:
        pass
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    role = (me or {}).get("role", "buyer")
    if call.from_user.id in ADMIN_IDS:
        role = "admin"
    teams = await db.list_teams()
    allowed_team_ids: set[int] = set()
    if role == "admin" or role == "head":
        allowed_team_ids = {int(t['id']) for t in teams}
    elif role == "lead":
        if me and me.get('team_id'):
            allowed_team_ids = {int(me.get('team_id'))}
    elif role == "mentor":
        allowed_team_ids = set(await db.list_mentor_teams(call.from_user.id))
    else:
        allowed_team_ids = set()
    teams_vis = [t for t in teams if int(t['id']) in allowed_team_ids]
    if not teams_vis:
        await call.message.answer("Нет доступных команд")
    else:
        await call.message.answer("Выберите команду:", reply_markup=_teams_picker_kb(teams_vis))
    try:
        await call.answer()
    except Exception:
        pass

@dp.callback_query(F.data == "report:pick:buyer")
async def cb_report_pick_buyer(call: CallbackQuery):
    try:
        await call.message.answer("Открываю список байеров…")
    except Exception:
        pass
    try:
        users = await db.list_users()
        scope_ids = set(await _resolve_scope_user_ids(call.from_user.id))
        allowed_roles = {"buyer", "lead", "mentor", "head"}
        buyers = [u for u in users if int(u['telegram_id']) in scope_ids and (u.get('role') in allowed_roles)]
        # Respect currently selected team filter if present
        cur = await db.get_report_filter(call.from_user.id)
        if cur and cur.get('team_id'):
            try:
                team_id_filter = int(cur['team_id'])
                buyers = [u for u in buyers if (u.get('team_id') and int(u['team_id']) == team_id_filter)]
            except Exception:
                pass
        if not buyers:
            await call.message.answer("Нет доступных байеров")
        else:
            await call.message.answer("Выберите байера:", reply_markup=_buyers_picker_kb(buyers))
    except Exception as e:
        logger.exception(e)
        await call.message.answer(f"Ошибка списка байеров: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)
    finally:
        try:
            await call.answer()
        except Exception:
            pass

@dp.callback_query(F.data == "report:pick:offer")
async def cb_report_pick_offer(call: CallbackQuery):
    try:
        await call.message.answer("Открываю офферы…")
        users = await db.list_users()
        # scope by role
        scope_ids = set(await _resolve_scope_user_ids(call.from_user.id))
        # apply buyer/team filters if set
        cur = await db.get_report_filter(call.from_user.id)
        buyers = [u for u in users if int(u['telegram_id']) in scope_ids]
        if cur and cur.get('team_id'):
            try:
                team_id_filter = int(cur['team_id'])
                buyers = [u for u in buyers if (u.get('team_id') and int(u['team_id']) == team_id_filter)]
            except Exception:
                pass
        if cur and cur.get('buyer_id'):
            try:
                buyer_id_filter = int(cur['buyer_id'])
                buyers = [u for u in buyers if int(u['telegram_id']) == buyer_id_filter]
            except Exception:
                pass
        user_ids = [int(u['telegram_id']) for u in buyers]
        offers = await db.list_offers_for_users(user_ids)
        # Cache offers for this user to map short callback index -> value
        await db.set_ui_cache_list(call.from_user.id, "offers", offers)
        if not offers:
            await call.message.answer("Нет доступных офферов")
        else:
            await call.message.answer("Выберите оффер:", reply_markup=_offers_picker_kb(offers))
    except Exception as e:
        logger.exception(e)
        await call.message.answer(f"Ошибка списка офферов: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)
    finally:
        try:
            await call.answer()
        except Exception:
            pass

@dp.callback_query(F.data == "report:pick:creative")
async def cb_report_pick_creative(call: CallbackQuery):
    try:
        await call.message.answer("Открываю креативы…")
        users = await db.list_users()
        scope_ids = set(await _resolve_scope_user_ids(call.from_user.id))
        cur = await db.get_report_filter(call.from_user.id)
        buyers = [u for u in users if int(u['telegram_id']) in scope_ids]
        if cur and cur.get('team_id'):
            try:
                team_id_filter = int(cur['team_id'])
                buyers = [u for u in buyers if (u.get('team_id') and int(u['team_id']) == team_id_filter)]
            except Exception:
                pass
        if cur and cur.get('buyer_id'):
            try:
                buyer_id_filter = int(cur['buyer_id'])
                buyers = [u for u in buyers if int(u['telegram_id']) == buyer_id_filter]
            except Exception:
                pass
        user_ids = [int(u['telegram_id']) for u in buyers]
        offer_filter = cur.get('offer') if cur else None
        creatives = await db.list_creatives_for_users(user_ids, offer_filter)
        await db.set_ui_cache_list(call.from_user.id, "creatives", creatives)
        if not creatives:
            await call.message.answer("Нет доступных креативов")
        else:
            await call.message.answer("Выберите крео:", reply_markup=_creatives_picker_kb(creatives))
    except Exception as e:
        logger.exception(e)
        await call.message.answer(f"Ошибка списка крео: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)
    finally:
        try:
            await call.answer()
        except Exception:
            pass

@dp.callback_query(F.data.startswith("report:set:"))
async def cb_report_set_filter_quick(call: CallbackQuery):
    _, _, which, value = call.data.split(":", 3)
    # Resolve index-based selections from UI cache
    if which == 'offer_idx':
        try:
            idx = int(value)
            resolved = await db.get_ui_cache_value(call.from_user.id, 'offers', idx)
            if resolved is None:
                return await call.answer("Просрочен список, откройте заново", show_alert=True)
            which, value = 'offer', resolved
        except Exception:
            return await call.answer("Некорректный выбор оффера", show_alert=True)
    elif which == 'creative_idx':
        try:
            idx = int(value)
            resolved = await db.get_ui_cache_value(call.from_user.id, 'creatives', idx)
            if resolved is None:
                return await call.answer("Просрочен список, откройте заново", show_alert=True)
            which, value = 'creative', resolved
        except Exception:
            return await call.answer("Некорректный выбор крео", show_alert=True)
    cur = await db.get_report_filter(call.from_user.id)
    offer = cur.get('offer')
    creative = cur.get('creative')
    buyer_id = cur.get('buyer_id')
    team_id = cur.get('team_id')
    if which == 'team':
        team_id = None if value == '-' else int(value)
    elif which == 'buyer':
        buyer_id = None if value == '-' else int(value)
    elif which == 'offer':
        offer = None if value == '-' else value
    elif which == 'creative':
        creative = None if value == '-' else value
    await db.set_report_filter(call.from_user.id, offer, creative, buyer_id=buyer_id, team_id=team_id)
    # Show a short summary and re-open Reports menu with filters displayed
    users = await db.list_users()
    teams = await db.list_teams()
    parts: list[str] = []
    if offer:
        parts.append(f"offer=<code>{offer}</code>")
    if creative:
        parts.append(f"creative=<code>{creative}</code>")
    if buyer_id:
        bid = int(buyer_id)
        bu = next((u for u in users if int(u['telegram_id']) == bid), None)
        bcap = f"@{bu['username']}" if bu and bu.get('username') else (bu.get('full_name') if bu and bu.get('full_name') else str(bid))
        parts.append(f"buyer=<code>{bcap}</code>")
    if team_id:
        tid = int(team_id)
        tname = next((t['name'] for t in teams if int(t['id']) == tid), str(tid))
        parts.append(f"team=<code>{tname}</code>")
    if parts:
        try:
            await call.message.answer("Фильтр обновлён: " + ", ".join(parts), parse_mode=ParseMode.HTML)
        except Exception:
            pass
    # Re-open reports menu with visible filters (do not auto-send any report)
    try:
        await _send_reports_menu(call.message.chat.id, call.from_user.id)
    except Exception:
        pass
    try:
        await call.answer()
    except Exception:
        pass

# KPI handlers moved to handlers/reports.py

# --- Mentor management (admin) ---
@dp.message(Command("addmentor"))
async def on_add_mentor(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /addmentor <telegram_id>
    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("Использование: /addmentor <telegram_id>")
    try:
        uid = await _resolve_user_id(parts[1])
        await db.set_user_role(uid, "mentor")
        await message.answer("OK, назначен ментором")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка назначения роли mentor")

@dp.message(Command("mentor_follow"))
async def on_mentor_follow(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /mentor_follow <mentor_id> <team_id>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /mentor_follow <mentor_id> <team_id>")
    try:
        mid = await _resolve_user_id(parts[1])
        team_id = int(parts[2])
        await db.add_mentor_team(mid, team_id)
        await message.answer("OK, подписан на команду")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка подписки ментора на команду")

@dp.message(Command("mentor_unfollow"))
async def on_mentor_unfollow(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /mentor_unfollow <mentor_id> <team_id>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /mentor_unfollow <mentor_id> <team_id>")
    try:
        mid = await _resolve_user_id(parts[1])
        team_id = int(parts[2])
        await db.remove_mentor_team(mid, team_id)
        await message.answer("OK, отписан от команды")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка отписки ментора от команды")
