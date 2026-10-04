"""Facebook campaign/account month reports (role-scoped) with account drill-down buttons."""

import html
import json
from datetime import date
from typing import Any
from decimal import Decimal

from aiogram import F
from aiogram.enums.parse_mode import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from loguru import logger

from ...dispatcher import dp, bot
from ..common import is_admin
from ... import db
from ...utils.formatting import as_decimal as _as_decimal, send_long, fmt_money as _fmt_money, fmt_percent as _fmt_percent, format_buyer_label as _format_buyer_label, format_flag_label as _format_flag_label, month_label_ru as _month_label_ru
from .period import _resolve_scope_user_ids, _send_reports_menu


async def _fb_month_rows_in_scope(month: date, actor_id: int) -> list[dict]:
    """FB rows of the month visible to the actor: admin — all, others — buyers from _resolve_scope_user_ids."""
    rows = await db.fetch_fb_campaign_month_report(month)
    if not rows or await is_admin(actor_id):
        return rows
    allowed = set(await _resolve_scope_user_ids(actor_id))
    return [r for r in rows if r.get("buyer_id") is not None and int(r["buyer_id"]) in allowed]


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
    # first button for current month as a single row
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


async def _send_fb_campaign_report(chat_id: int, month_start: date, requester_id: int) -> None:
    month = month_start.replace(day=1)
    rows = await _fb_month_rows_in_scope(month, requester_id)
    if not rows:
        await bot.send_message(chat_id, f"Нет данных по FB кампаниям за {html.escape(_month_label_ru(month))}.", parse_mode=ParseMode.HTML)
        return
    flag_rows = await db.list_fb_flags()
    flags_by_id = {}
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
        campaign_name = html.escape(str(row.get("campaign_name") or "—"))
        account_name = html.escape(str(row.get("account_name") or "—"))
        buyer_label = _format_buyer_label(row.get("buyer_id"), users_by_id)
        prev_flag_label = html.escape(_format_flag_label(row.get("prev_flag_id"), flags_by_id))
        curr_flag_id = row.get("curr_flag_id") or row.get("state_flag_id")
        curr_flag_label = html.escape(_format_flag_label(curr_flag_id, flags_by_id))
        line = (
            f"{idx}) <code>{campaign_name}</code> | Акк: <code>{account_name}</code> | "
            f"Байер: {buyer_label} | Spend {_fmt_money(spend)} | FTD {ftd} | "
            f"Rev {_fmt_money(revenue)} | ROI {_fmt_percent(roi)} | FTD rate {_fmt_percent(ftd_rate)} | "
            f"Флаг: {prev_flag_label} → {curr_flag_label}"
        )
        lines.append(line)
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
    all_lines: list[str] = header_lines.copy()
    if lines:
        all_lines.append("")
        all_lines.extend(lines)
    await send_long(bot, chat_id, [line.rstrip() for line in all_lines])


async def _send_fb_account_report(chat_id: int, month_start: date, requester_id: int) -> None:
    month = month_start.replace(day=1)
    rows = await _fb_month_rows_in_scope(month, requester_id)
    if not rows:
        await bot.send_message(chat_id, f"Нет данных по FB кабинетам за {html.escape(_month_label_ru(month))}.", parse_mode=ParseMode.HTML)
        return
    flag_rows = await db.list_fb_flags()
    flags_by_id = {}
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
        campaign_roi = ((revenue - spend) / spend * Decimal(100)) if spend else None
        campaign_flag = _format_flag_label(row.get("curr_flag_id") or row.get("state_flag_id"), flags_by_id)
        entry["campaign_lines"].append(
            f"• <code>{html.escape(str(campaign_name or '—'))}</code> — {html.escape(campaign_flag)}. "
            f"Spend {_fmt_money(spend)} | FTD {ftd} | Rev {_fmt_money(revenue)} | ROI {_fmt_percent(campaign_roi)}"
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
    sorted_accounts = sorted(accounts.items(), key=lambda item: item[1]["spend"], reverse=True)
    total_spend = sum((info["spend"] for _, info in sorted_accounts), Decimal("0"))
    total_revenue = sum((info["revenue"] for _, info in sorted_accounts), Decimal("0"))
    total_ftd = sum(info["ftd"] for _, info in sorted_accounts)
    total_impressions = sum(info["impressions"] for _, info in sorted_accounts)
    total_clicks = sum(info["clicks"] for _, info in sorted_accounts)
    total_registrations = sum(info["registrations"] for _, info in sorted_accounts)
    lines: list[str] = []
    max_items = 20
    cache_kind = f"fbar:{month.isoformat()}"
    account_cache_values: list[str] = []
    account_keyboard_rows: list[list[InlineKeyboardButton]] = []
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
        curr_flag_id = info["curr_flag_id"] or info["prev_flag_id"]
        curr_flag_raw = _format_flag_label(curr_flag_id, flags_by_id)
        curr_flag_label = html.escape(curr_flag_raw)
        account_name = html.escape(account_name_raw)
        line = (
            f"{idx}) <code>{account_name}</code> | Кампаний: {len(info['campaigns'])} | "
            f"Байеры: {buyers_text} | Spend {_fmt_money(spend)} | FTD {ftd} | "
            f"Rev {_fmt_money(revenue)} | ROI {_fmt_percent(roi)} | FTD rate {_fmt_percent(ftd_rate)} | "
            f"Флаг: {prev_flag_label} → {curr_flag_label}"
        )
        lines.append(line)
        ctr_value = (Decimal(info["clicks"]) / Decimal(info["impressions"]) * Decimal(100)) if info["impressions"] else None
        account_cache_values.append(json.dumps({
            "account_name": account_name_raw,
            "flag_label": curr_flag_raw,
            "spend": str(spend),
            "revenue": str(revenue),
            "roi": str(roi) if roi is not None else None,
            "ftd": ftd,
            "campaign_count": len(info["campaigns"]),
            "campaign_lines": info["campaign_lines"],
            "ctr": str(ctr_value) if ctr_value is not None else None,
            "ftd_rate": str(ftd_rate) if ftd_rate is not None else None,
        }))
        short_name = account_name_raw if len(account_name_raw) <= 28 else account_name_raw[:27] + "…"
        button_text = f"{idx}. {curr_flag_raw.split(' ', 1)[0]} {short_name}"[:64]
        account_keyboard_rows.append(
            [InlineKeyboardButton(text=button_text, callback_data=f"{cache_kind}:{idx - 1}")]
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
    summary_lines = header_lines[:]
    if lines:
        summary_lines += [""] + lines
    if len(sorted_accounts) > max_items:
        summary_lines += ["", f"Показаны первые {max_items} кабинетов из {len(sorted_accounts)}."]
    keyboard_markup = None
    if account_keyboard_rows:
        summary_lines += ["", "Нажми кнопку ниже, чтобы раскрыть кабинет."]
        keyboard_markup = InlineKeyboardMarkup(inline_keyboard=account_keyboard_rows[:12])
    await send_long(bot, chat_id, summary_lines, reply_markup=keyboard_markup)
    try:
        await db.set_ui_cache_list(requester_id, cache_kind, account_cache_values)
    except Exception as exc:
        logger.warning("Failed to cache FB account report payloads: {}", exc)


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
            await _send_fb_campaign_report(call.message.chat.id, month, call.from_user.id)
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
