"""Report filter chips and pickers (team, buyer, offer, creative)."""


from aiogram import F
from aiogram.enums.parse_mode import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from loguru import logger

from ...dispatcher import dp
from ..common import STALE_BUTTON, callback_parts, is_admin
from ... import db
from ...utils.html import safe
from .period import _resolve_scope_user_ids, _send_reports_menu


@dp.callback_query(F.data.startswith("report:f:"))
async def cb_report_filter(call: CallbackQuery):
    parts = callback_parts(call, 3)
    if not parts:
        return await call.answer(STALE_BUTTON, show_alert=True)
    _, _, key = parts
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
    parts = callback_parts(call, 3)
    if not parts:
        return await call.answer(STALE_BUTTON, show_alert=True)
    _, _, which = parts
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


def _buyers_picker_kb(users: list[dict], page: int = 0, page_size: int = 40) -> InlineKeyboardMarkup:
    rows = []
    total = len(users)
    pages = max((total - 1) // page_size + 1, 1)
    page = max(0, min(page, pages - 1))
    start = page * page_size
    end = start + page_size
    for u in users[start:end]:
        cap = f"@{u['username'] or u['telegram_id']} ({u['full_name'] or ''})"
        rows.append([InlineKeyboardButton(text=cap, callback_data=f"report:set:buyer:{u['telegram_id']}")])
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"report:pick:buyer:page:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{pages}", callback_data="report:noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"report:pick:buyer:page:{page+1}"))
    if nav:
        rows.append(nav)
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
    if await is_admin(call.from_user.id):
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
        # Источник 1: классические "buyer" в tg_users.
        buyers = [u for u in users if int(u['telegram_id']) in scope_ids and (u.get('role') == "buyer")]
        # Источник 2: buyer_id, реально используемые в tg_aliases (ваш основной кейс маршрутизации).
        alias_rows = await db.list_aliases()
        alias_buyer_ids: set[int] = set()
        for row in alias_rows:
            bid = row.get("buyer_id")
            if bid is None:
                continue
            try:
                alias_buyer_ids.add(int(bid))
            except Exception:
                continue
        if alias_buyer_ids:
            buyers_by_id = {int(u["telegram_id"]): u for u in users if u.get("telegram_id") is not None}
            for bid in sorted(alias_buyer_ids):
                if bid not in scope_ids:
                    continue
                u = buyers_by_id.get(bid)
                if u is not None:
                    buyers.append(u)
        # Deduplicate by telegram_id after union from roles + aliases.
        dedup: dict[int, dict] = {}
        for u in buyers:
            try:
                uid = int(u["telegram_id"])
            except Exception:
                continue
            dedup[uid] = u
        buyers = list(dedup.values())
        # Respect currently selected team filter if present
        cur = await db.get_report_filter(call.from_user.id)
        if cur and cur.get('team_id'):
            try:
                team_id_filter = int(cur['team_id'])
                buyers = [u for u in buyers if (u.get('team_id') and int(u['team_id']) == team_id_filter)]
            except Exception:
                pass
        buyers = sorted(
            buyers,
            key=lambda u: (
                str(u.get("username") or "").lower(),
                str(u.get("full_name") or "").lower(),
                int(u.get("telegram_id") or 0),
            ),
        )
        if not buyers:
            await call.message.answer("Нет доступных байеров")
        else:
            await db.set_ui_cache_list(call.from_user.id, "buyers_picker_ids", [int(u["telegram_id"]) for u in buyers])
            await call.message.answer("Выберите байера:", reply_markup=_buyers_picker_kb(buyers, page=0))
    except Exception as e:
        logger.exception(e)
        await call.message.answer(f"Ошибка списка байеров: <code>{safe(f"{type(e).__name__}: {e}")}</code>", parse_mode=ParseMode.HTML)
    finally:
        try:
            await call.answer()
        except Exception:
            pass


@dp.callback_query(F.data.startswith("report:pick:buyer:page:"))
async def cb_report_pick_buyer_page(call: CallbackQuery):
    try:
        page = int(call.data.rsplit(":", 1)[1])
    except Exception:
        return await call.answer("Некорректная страница", show_alert=True)
    # Reconstruct ids list from short-lived UI cache.
    buyers_ids: list[int] = []
    idx = 0
    while True:
        v = await db.get_ui_cache_value(call.from_user.id, "buyers_picker_ids", idx)
        if v is None:
            break
        try:
            buyers_ids.append(int(v))
        except Exception:
            pass
        idx += 1
    if not buyers_ids:
        return await call.answer("Список байеров устарел, откройте заново", show_alert=True)
    users = await db.list_users()
    users_by_id = {int(u["telegram_id"]): u for u in users if u.get("telegram_id") is not None}
    buyers = [users_by_id[uid] for uid in buyers_ids if uid in users_by_id]
    if not buyers:
        return await call.answer("Список байеров пуст", show_alert=True)
    try:
        await call.message.edit_reply_markup(reply_markup=_buyers_picker_kb(buyers, page=page))
    except Exception:
        await call.message.answer("Выберите байера:", reply_markup=_buyers_picker_kb(buyers, page=page))
    await call.answer()


@dp.callback_query(F.data == "report:noop")
async def cb_report_noop(call: CallbackQuery):
    await call.answer()


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
        await call.message.answer(f"Ошибка списка офферов: <code>{safe(f"{type(e).__name__}: {e}")}</code>", parse_mode=ParseMode.HTML)
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
        await call.message.answer(f"Ошибка списка крео: <code>{safe(f"{type(e).__name__}: {e}")}</code>", parse_mode=ParseMode.HTML)
    finally:
        try:
            await call.answer()
        except Exception:
            pass


@dp.callback_query(F.data.startswith("report:set:"))
async def cb_report_set_filter_quick(call: CallbackQuery):
    parts = callback_parts(call, 4)
    if not parts:
        return await call.answer(STALE_BUTTON, show_alert=True)
    _, _, which, value = parts
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
        bcap = safe(f"@{bu['username']}" if bu and bu.get('username') else (bu.get('full_name') if bu and bu.get('full_name') else bid))
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
