"""Period reports (today / yesterday / 7 days) and the reports menu; role scope of visible buyers."""

import html
from contextlib import suppress

from aiogram import F
from aiogram.filters import Command, CommandObject
from aiogram.enums.parse_mode import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from loguru import logger

from ...dispatcher import dp, bot
from ..common import is_admin
from ... import db
from ...utils.html import safe, user_label
from ...utils.numbers import round_money
from ...utils.formatting import send_long


async def _resolve_scope_user_ids(actor_id: int) -> list[int]:
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = (me or {}).get("role", "buyer")
    # Админ: по env ADMINS или по роли в БД — видит всех пользователей в отчётах
    if await is_admin(actor_id, me):
        my_role = "admin"
    # Помощник в отчётах — только как «зритель»:
    # видит депозиты ТОЛЬКО назначенного байера и сам в отчётах не фигурирует.
    if my_role == "helper":
        buyer_id = await db.get_helper_buyer(actor_id)
        if buyer_id is not None:
            return [buyer_id]
        return []
    allowed_roles = {"buyer", "lead", "mentor", "head"}
    if my_role == "admin":
        # Админ видит всю картину — по всем пользователям, независимо от роли/флага is_active.
        return [int(u["telegram_id"]) for u in users]
    if my_role == "head":
        # Голова видит всех активных байеров/лидов/менторов.
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
    lines.append(f"💰 Профит: <b>{round_money(profit)}</b>")
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
        users = await db.list_users()
        if not users:
            logger.warning("No users found in database")
        user_ids = await _resolve_scope_user_ids(actor_id)
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
        filt = await db.get_report_filter(actor_id)
        # Фильтр по байеру хранит Telegram ID. Если там чужой id (CRM/админка) или
        # байер вне доступа — раньше отчёт молча выдавал нули. Снимаем и сообщаем.
        if filt.get('buyer_id'):
            bid = int(filt['buyer_id'])
            known_ids = {int(u['telegram_id']) for u in users if u.get('telegram_id') is not None}
            if bid not in known_ids or bid not in set(user_ids):
                reason = "такого пользователя нет в боте" if bid not in known_ids else "нет доступа к этому байеру"
                logger.warning(f"Dropping unusable buyer filter {bid} for actor {actor_id}: {reason}")
                await db.set_report_filter(
                    actor_id, filt.get('offer'), filt.get('creative'),
                    buyer_id=None, team_id=filt.get('team_id'),
                )
                filt = dict(filt, buyer_id=None)
                await bot.send_message(
                    chat_id,
                    f"⚠️ Фильтр по байеру <code>{bid}</code> снят: {reason}. Отчёт построен без него.",
                    parse_mode=ParseMode.HTML,
                )
        filter_user_ids: list[int] | None = None
        if filt.get('buyer_id') or filt.get('team_id'):
            me = next((u for u in users if u["telegram_id"] == actor_id), None)
            role = (me or {}).get("role", "buyer")
            if await is_admin(actor_id):
                role = "admin"
            allowed_ids = set(user_ids)
            if filt.get('buyer_id'):
                bid = int(filt['buyer_id'])
                filter_user_ids = [bid] if bid in allowed_ids else []
            elif filt.get('team_id'):
                tid = int(filt['team_id'])
                team_ids = [int(u['telegram_id']) for u in users if u.get('team_id') == tid and u.get('is_active')]
                filter_user_ids = [uid for uid in team_ids if uid in allowed_ids]
        try:
            agg = await db.aggregate_sales(
                user_ids,
                start,
                end,
                offer=filt.get('offer'),
                creative=filt.get('creative'),
                filter_user_ids=filter_user_ids,
            )
        except Exception as agg_err:
            logger.exception("Error in aggregate_sales: {}", agg_err)
            raise
        text = _report_text(title, agg)
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
                    label = user_label(u, uid)
                lines.append(f"{label}: <b>{cnt}</b>")
            if lines:
                text += "\n\n" + "\n".join(lines)
        # If a concrete buyer is selected, show offer breakdown for this buyer and period.
        if filt.get('buyer_id'):
            offer_dist = agg.get('offer_dist') or {}
            if offer_dist:
                text += "\n\n🧾 Офферы по выбранному байеру:\n"
                for offer_name, dep_count in offer_dist.items():
                    text += f"• <code>{html.escape(str(offer_name))}</code>: <b>{int(dep_count)}</b>\n"
            else:
                text += "\n\n🧾 Офферы по выбранному байеру: данных нет."
        if days == 7 and not yesterday:
            trend = await db.trend_daily_sales(user_ids, days=7)
            if trend:
                tline = ", ".join(f"{d.split('-')[-1]}:{c}" for d, c in trend)
                text += f"\n📅 Тренд (7д): {tline}"
        if filt.get('offer') or filt.get('creative') or filt.get('buyer_id') or filt.get('team_id'):
            teams = await db.list_teams()
            fparts: list[str] = []
            if filt.get('offer'):
                fparts.append(f"offer=<code>{safe(filt['offer'])}</code>")
            if filt.get('creative'):
                fparts.append(f"creative=<code>{filt['creative']}</code>")
            if filt.get('buyer_id'):
                bid = int(filt['buyer_id'])
                bu = next((u for u in users if int(u['telegram_id']) == bid), None)
                if bu and (bu.get('username') or bu.get('full_name')):
                    cap = safe(f"@{bu['username']}" if bu.get('username') else (bu.get('full_name') or bid))
                else:
                    cap = str(bid)
                fparts.append(f"buyer=<code>{cap}</code>")
            if filt.get('team_id'):
                tid = int(filt['team_id'])
                tn = next((t['name'] for t in teams if int(t['id']) == tid), str(tid))
                fparts.append(f"team=<code>{tn}</code>")
            text += "\n🔎 Фильтры: " + ", ".join(fparts)
        await send_long(bot, chat_id, text, reply_markup=_reports_menu(actor_id))
    except Exception as e:
        logger.exception("Error in _send_period_report: {}", e)
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
            fparts.append(f"offer=<code>{safe(filt['offer'])}</code>")
        if filt.get('creative'):
            fparts.append(f"creative=<code>{filt['creative']}</code>")
        if filt.get('buyer_id'):
            bid = int(filt['buyer_id'])
            bu = next((u for u in users if int(u['telegram_id']) == bid), None)
            if bu and (bu.get('username') or bu.get('full_name')):
                cap = safe(f"@{bu['username']}" if bu.get('username') else (bu.get('full_name') or bid))
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


_PERIODS = {
    "today": ("Сегодня", None, False),
    "yesterday": ("Вчера", None, True),
    "week": ("Последние 7 дней", 7, False),
}


async def _run_period_report(message: Message, actor_id: int, period: str) -> None:
    """Status message -> report (status removed) or the error text in place of the status."""
    title, days, yesterday = _PERIODS[period]
    status_msg = None
    with suppress(Exception):
        status_msg = await message.answer("Готовлю отчёт…")
    try:
        await _send_period_report(message.chat.id, actor_id, title, days, yesterday)
    except Exception as e:
        logger.exception("Failed to build report for user {}", actor_id)
        error_text = f"Не удалось построить отчёт: <code>{safe(f'{type(e).__name__}: {e}')}</code>"
        try:
            if status_msg is None:
                raise RuntimeError("no status message")
            await status_msg.edit_text(error_text)
        except Exception:
            with suppress(Exception):
                await message.answer(error_text)
        return
    if status_msg is not None:
        with suppress(Exception):
            await status_msg.delete()


@dp.callback_query(F.data.in_({"report:today", "report:yesterday", "report:week"}))
async def cb_report_period(call: CallbackQuery):
    with suppress(Exception):
        await call.answer()  # remove the spinner before the (slow) report
    await _run_period_report(call.message, call.from_user.id, call.data.split(":", 1)[1])


@dp.message(Command("today", "yesterday", "week"))
async def on_period_command(message: Message, command: CommandObject):
    await _run_period_report(message, message.from_user.id, command.command)
