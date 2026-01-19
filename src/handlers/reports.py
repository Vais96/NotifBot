import html
from datetime import date
from typing import Any
from decimal import Decimal

from aiogram import F
from aiogram.enums.parse_mode import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from loguru import logger

from ..dispatcher import dp, bot, ADMIN_IDS
from .. import db


_MONTH_NAMES_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


def _month_label_ru(month: date) -> str:
    name = _MONTH_NAMES_RU.get(month.month, month.strftime("%m"))
    return f"{name} {month.year}"


def _fmt_money(value: Decimal | float | int | None) -> str:
    if value is None:
        return "$0.00"
    amount = float(value)
    return f"${amount:,.2f}".replace(",", " ")


def _fmt_percent(value: Decimal | float | None) -> str:
    if value is None:
        return "—"
    return f"{float(value):.1f}%"


def _as_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _format_flag_label(flag_id, flags_by_id: dict[int, dict[str, Any]]) -> str:
    if flag_id is None:
        return "—"
    try:
        fid = int(flag_id)
    except Exception:
        return str(flag_id)
    row = flags_by_id.get(fid)
    if not row:
        return str(fid)
    return row.get("title") or row.get("code") or str(fid)


def _format_buyer_label(buyer_id, users_by_id: dict[int, dict[str, Any]]) -> str:
    if buyer_id is None:
        return "—"
    try:
        uid = int(buyer_id)
    except Exception:
        return html.escape(str(buyer_id))
    user = users_by_id.get(uid)
    if not user:
        return f"<code>{uid}</code>"
    username = user.get("username")
    if username:
        return f"@{html.escape(username)}"
    full_name = user.get("full_name")
    if full_name:
        return html.escape(str(full_name))
    return f"<code>{uid}</code>"

def _reports_menu(actor_id: int) -> InlineKeyboardMarkup:
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


async def _send_fb_campaign_report(chat_id: int, month_start: date) -> None:
    month = month_start.replace(day=1)
    rows = await db.fetch_fb_campaign_month_report(month)
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
    def _chunk_lines(all_lines: list[str], max_length: int = 3500) -> list[str]:
        messages: list[str] = []
        current: list[str] = []
        current_len = 0
        for raw_line in all_lines:
            line = raw_line.rstrip()
            additional = len(line) + 1
            if current and current_len + additional > max_length:
                messages.append("\n".join(current))
                current = [line]
                current_len = len(line) + 1
            else:
                current.append(line)
                current_len += additional
        if current:
            messages.append("\n".join(current))
        return messages or [""]

    all_lines: list[str] = header_lines.copy()
    if lines:
        all_lines.append("")
        all_lines.extend(lines)
    chunks = _chunk_lines(all_lines)
    first_chunk, *rest_chunks = chunks
    await bot.send_message(chat_id, first_chunk, parse_mode=ParseMode.HTML)
    for chunk in rest_chunks:
        await bot.send_message(chat_id, chunk, parse_mode=ParseMode.HTML)


async def _send_fb_account_report(chat_id: int, month_start: date) -> None:
    month = month_start.replace(day=1)
    rows = await db.fetch_fb_campaign_month_report(month)
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
        curr_flag_label = html.escape(_format_flag_label(curr_flag_id, flags_by_id))
        account_name = html.escape(account_name_raw)
        line = (
            f"{idx}) <code>{account_name}</code> | Кампаний: {len(info['campaigns'])} | "
            f"Байеры: {buyers_text} | Spend {_fmt_money(spend)} | FTD {ftd} | "
            f"Rev {_fmt_money(revenue)} | ROI {_fmt_percent(roi)} | FTD rate {_fmt_percent(ftd_rate)} | "
            f"Флаг: {prev_flag_label} → {curr_flag_label}"
        )
        lines.append(line)
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
    text = "\n".join(header_lines)
    if lines:
        text += "\n\n" + "\n".join(lines)
    if len(sorted_accounts) > max_items:
        text += f"\n\nПоказаны первые {max_items} кабинетов из {len(sorted_accounts)}."
    await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)


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
            await _send_fb_account_report(call.message.chat.id, month)
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
    _, _, which = call.data.split(":", 2)
    await db.set_pending_action(call.from_user.id, f"kpi:set:{which}", None)
    await call.message.answer("Пришлите целевое число депозитов (целое), либо '-' чтобы очистить")
    await call.answer()
