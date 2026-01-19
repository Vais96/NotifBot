import html
from datetime import date
from typing import Any
from decimal import Decimal

from aiogram import F
from aiogram.filters import Command
from aiogram.enums.parse_mode import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
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
            agg = await db.aggregate_sales(
                user_ids,
                start,
                end,
                offer=filt.get('offer'),
                creative=filt.get('creative'),
                filter_user_ids=filter_user_ids,
            )
            logger.info(
                f"Aggregate result: count={agg.get('count')}, profit={agg.get('profit')}, top_offer={agg.get('top_offer')}"
            )
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
