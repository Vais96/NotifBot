"""FB CSV upload (pending `fb:await_csv`) and account drill-down callbacks (`fbua:` upload, `fbar:` report)."""

import asyncio
import html
import json
import traceback
from decimal import Decimal
from io import BytesIO
from typing import Any, Dict, List, Optional

from aiogram import F
from aiogram.types import CallbackQuery, Message
from loguru import logger

from .. import db, fb_csv
from ..dispatcher import ADMIN_IDS, bot, dp
from ..services.fb_uploads import CSV_ALLOWED_MIME_TYPES, MAX_CSV_FILE_SIZE_BYTES, process_fb_csv_upload
from ..utils.formatting import as_decimal, chunk_lines, fmt_money, fmt_percent
from ..constants import PendingAction


def _decimal_or_none(value: Any) -> Optional[Decimal]:
    return None if value is None else as_decimal(value)


def _build_account_detail_messages(payload: Dict[str, Any]) -> List[str]:
    account_name = str(payload.get("account_name") or "Без кабинета")
    flag_label = str(payload.get("flag_label") or "—")
    spend_value = as_decimal(payload.get("spend"))
    revenue_value = as_decimal(payload.get("revenue"))
    roi_value = _decimal_or_none(payload.get("roi"))
    if roi_value is None and spend_value:
        roi_value = (revenue_value - spend_value) / spend_value * Decimal(100)
    ftd_value = int(payload.get("ftd") or 0)
    campaign_count = int(payload.get("campaign_count") or 0)
    ctr_value = _decimal_or_none(payload.get("ctr"))
    ftd_rate_value = _decimal_or_none(payload.get("ftd_rate"))
    lines: List[str] = [
        f"<b>{html.escape(account_name)}</b>",
        "Флаг кабинета: " + html.escape(flag_label),
        f"Spend {fmt_money(spend_value)} | Rev {fmt_money(revenue_value)} | ROI {fmt_percent(roi_value)} | FTD {ftd_value} | Кампаний {campaign_count}",
        f"CTR {fmt_percent(ctr_value)} | FTD rate {fmt_percent(ftd_rate_value)}",
    ]
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


async def _notify_admins_about_exception(context: str, exc: Exception, extra_details: Optional[List[str]] = None) -> None:
    trace = "".join(traceback.format_exception(exc.__class__, exc, exc.__traceback__))
    snippet = trace[-3500:]
    lines: List[str] = [f"⚠️ {html.escape(context)}"]
    lines.extend(html.escape(item) for item in extra_details or [] if item)
    if snippet:
        lines.append("<b>Traceback:</b>")
        lines.append(f"<code>{html.escape(snippet)}</code>")
    message_text = "\n".join(lines)

    recipients = {int(aid) for aid in ADMIN_IDS}
    try:
        users = await db.list_users()
    except Exception as fetch_exc:
        logger.warning("Failed to fetch users for admin alert: {}", fetch_exc)
        users = []
    for row in users or []:
        if row.get("is_active", 1) and row.get("role") == "admin" and row.get("telegram_id") is not None:
            recipients.add(int(row["telegram_id"]))
    if not recipients:
        logger.warning("No admin recipients for alert: {}", context)
        return
    for rid in recipients:
        try:
            await bot.send_message(rid, message_text)
        except Exception as send_exc:
            logger.warning("Failed to deliver admin alert to {}: {}", rid, send_exc)


@dp.message(F.document)
async def on_document_upload(message: Message):
    pending = await db.get_pending_action(message.from_user.id)
    if not pending or pending[0] != PendingAction.FB_AWAIT_CSV:
        return
    document = message.document
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
    except Exception:
        logger.exception("Failed to download CSV from Telegram")
        await status_msg.edit_text("Не удалось скачать файл из Telegram. Попробуйте ещё раз.")
        return
    try:
        parsed = await asyncio.to_thread(fb_csv.parse_fb_csv, buffer.getvalue())
    except Exception:
        logger.exception("Failed to parse Facebook CSV")
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


async def _send_account_detail(callback: CallbackQuery, stale_text: str) -> None:
    """`<prefix>:<key>:<idx>` → payload из tg_ui_cache(kind=`<prefix>:<key>`) → сообщение по кабинету."""
    kind, _, idx_str = (callback.data or "").rpartition(":")
    if kind.count(":") != 1 or not idx_str.isdigit():
        await callback.answer("Некорректный запрос.", show_alert=True)
        return
    try:
        cached = await db.get_ui_cache_value(callback.from_user.id, kind, int(idx_str))
    except Exception as exc:
        logger.warning("Failed to read FB account cache {}: {}", kind, exc)
        await callback.answer("Не удалось прочитать данные.", show_alert=True)
        return
    if not cached:
        await callback.answer(stale_text, show_alert=True)
        return
    try:
        chunks = _build_account_detail_messages(json.loads(cached))
    except Exception as exc:
        logger.warning("Failed to decode FB account payload {}: {}", kind, exc)
        await callback.answer("Ошибка чтения данных.", show_alert=True)
        return
    target_chat = callback.message.chat.id if callback.message else callback.from_user.id
    try:
        for chunk in chunks:
            await bot.send_message(target_chat, chunk)
    except Exception as exc:
        logger.warning("Failed to send FB account detail: {}", exc)
        await callback.answer("Не удалось отправить сообщение.", show_alert=True)
        return
    await callback.answer()


@dp.callback_query(F.data.startswith("fbua:"))
async def on_fb_upload_account_detail(callback: CallbackQuery):
    await _send_account_detail(callback, "Данные недоступны. Отправьте CSV заново.")


@dp.callback_query(F.data.startswith("fbar:"))
async def on_fb_report_account_detail(callback: CallbackQuery):
    await _send_account_detail(callback, "Данные устарели. Перестройте отчёт.")
