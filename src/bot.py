import html
import re
import traceback
from collections import defaultdict
from datetime import date
from decimal import Decimal
from io import BytesIO
from typing import Any, Dict, List, Optional, Set

from aiogram import F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums.parse_mode import ParseMode
from loguru import logger
from .config import settings
from . import db
from .dispatcher import bot, dp, ADMIN_IDS
from . import keitaro_sync
from .keitaro import normalize_domain, parse_campaign_name
from . import fb_csv

# Helper: resolve user reference to Telegram ID (supports numeric ID, @username, tg://user?id=...)
async def _resolve_user_id(identifier: str) -> int:
    s = (identifier or "").strip()
    # tg://user?id=123
    if s.startswith("tg://user?id="):
        value = s.split("=", 1)[1]
        return int(value)
    # @username
    if s.startswith("@"):
        uname = s[1:].strip().lower()
        users = await db.list_users()
        hit = next((u for u in users if (u.get("username") or "").lower() == uname), None)
        if not hit:
            raise ValueError("username_not_found")
        return int(hit["telegram_id"])  # type: ignore
    # numeric id
    return int(s)


_DOMAIN_SPLIT_RE = re.compile(r"[\s,;]+")
MAX_DOMAINS_PER_REQUEST = 10
MAX_CSV_FILE_SIZE_BYTES = 8 * 1024 * 1024
CSV_ALLOWED_MIME_TYPES = {"text/csv", "application/vnd.ms-excel"}


def _fmt_money(value: Decimal | float | int | None) -> str:
    if value is None:
        return "$0.00"
    amount = float(value)
    return f"${amount:,.2f}".replace(",", " ")


def _fmt_percent(value: Decimal | float | None) -> str:
    if value is None:
        return "—"
    return f"{float(value):.1f}%"


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


def _lookup_inferred_buyer(campaign_name: Optional[str], alias_key: Optional[str], inferred: Dict[str, int]) -> Optional[int]:
    for key in (campaign_name, alias_key):
        if not key:
            continue
        candidate = inferred.get(key.strip().lower())
        if candidate is not None:
            try:
                return int(candidate)
            except Exception:
                continue
    return None


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


async def _resolve_campaign_assignments(campaign_names: Set[str]) -> Dict[str, Dict[str, Any]]:
    alias_keys: Dict[str, Optional[str]] = {}
    for name in campaign_names:
        meta = parse_campaign_name(name or "")
        alias_key = (meta.get("alias_key") or "").strip().lower() or None
        if not alias_key and name:
            alias_key = name.split("_", 1)[0].strip().lower() or None
        alias_keys[name] = alias_key
    alias_values = [val for val in alias_keys.values() if val]
    alias_map = await db.fetch_alias_map(alias_values)
    identifiers: Set[str] = set()
    for name in campaign_names:
        if name:
            identifiers.add(name)
    identifiers.update(alias_values)
    inferred = await db.infer_campaign_buyers(identifiers)
    result: Dict[str, Dict[str, Any]] = {}
    for name in campaign_names:
        alias_key = alias_keys.get(name)
        alias_row = alias_map.get(alias_key) if alias_key else None
        buyer_id: Optional[int] = None
        alias_lead_id: Optional[int] = None
        if alias_row:
            buyer_raw = alias_row.get("buyer_id")
            if buyer_raw is not None:
                try:
                    buyer_id = int(buyer_raw)
                except Exception:
                    buyer_id = None
            lead_raw = alias_row.get("lead_id")
            if lead_raw is not None:
                try:
                    alias_lead_id = int(lead_raw)
                except Exception:
                    alias_lead_id = None
        if buyer_id is None:
            buyer_id = _lookup_inferred_buyer(name, alias_key, inferred)
        result[name] = {
            "buyer_id": buyer_id,
            "alias_key": alias_key,
            "alias_lead_id": alias_lead_id,
            "alias_row": alias_row,
        }
    return result


def _extract_domains(raw_text: str) -> tuple[list[str], list[str]]:
    tokens = [t.strip() for t in _DOMAIN_SPLIT_RE.split(raw_text or "") if t.strip()]
    seen: set[str] = set()
    domains: list[str] = []
    invalid: list[str] = []
    for token in tokens:
        normalized = normalize_domain(token)
        if not normalized:
            invalid.append(token)
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        domains.append(normalized)
        if len(domains) >= MAX_DOMAINS_PER_REQUEST:
            break
    return domains, invalid


async def _render_domain_block(
    domain: str,
    alias_cache: dict[str, dict | None],
    user_cache: dict[int, dict | None]
) -> str:
    rows = await db.find_campaigns_by_domain(domain)
    if not rows:
        return f"Кампании для домена <code>{domain}</code>:\n\nНе найдено."
    lines: list[str] = []
    for row in rows[:20]:
        alias_key = (row.get("alias_key") or "").lower()
        prefix = row.get("prefix") or alias_key or (row.get("name") or "-")
        alias_info = None
        if alias_key:
            if alias_key not in alias_cache:
                alias_cache[alias_key] = await db.find_alias(alias_key)
            alias_info = alias_cache[alias_key]
        mention = None
        if alias_info:
            target_id = alias_info.get("lead_id") or alias_info.get("buyer_id")
            if target_id:
                tid = int(target_id)
                if tid not in user_cache:
                    user_cache[tid] = await db.get_user(tid)
                user = user_cache[tid]
                if user:
                    username = user.get("username")
                    fullname = user.get("full_name")
                    if username:
                        mention = f"@{username}"
                    elif fullname:
                        mention = str(fullname)
        if not mention:
            mention = prefix
        header = prefix if mention == prefix else f"{prefix} — {mention}"
        display_domain = row.get("source_domain") or domain
        lines.append(f"{header}\n{display_domain}")
    if len(rows) > 20:
        lines.append(f"… и ещё {len(rows) - 20}")
    return f"Кампании для домена <code>{domain}</code>:\n\n" + "\n\n".join(lines)


async def _lookup_domains_text(raw_text: str) -> str:
    domains, invalid = _extract_domains(raw_text)
    if not domains:
        if invalid:
            listed = ", ".join(invalid[:5])
            suffix = " …" if len(invalid) > 5 else ""
            return f"Не удалось распознать домены: {listed}{suffix}. Пришлите строки вида example.com"
        return "Не удалось распознать домен. Пришлите строку вида salongierpl.online"
    alias_cache: dict[str, dict | None] = {}
    user_cache: dict[int, dict | None] = {}
    blocks = [await _render_domain_block(domain, alias_cache, user_cache) for domain in domains]
    message = "\n\n".join(blocks)
    if len(domains) == MAX_DOMAINS_PER_REQUEST:
        message += "\n\nУчтены только первые 10 доменов за один запрос."
    if invalid:
        listed = ", ".join(invalid[:5])
        suffix = " …" if len(invalid) > 5 else ""
        message += f"\n\nПропущены значения: {listed}{suffix}."
    return message
def main_menu(is_admin: bool, role: str | None = None, has_lead_access: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="Кто я", callback_data="menu:whoami"), InlineKeyboardButton(text="Правила", callback_data="menu:listroutes")],
        [InlineKeyboardButton(text="Отчеты", callback_data="menu:reports"), InlineKeyboardButton(text="KPI", callback_data="menu:kpi")],
    ]
    buttons.append([InlineKeyboardButton(text="Проверить домен", callback_data="menu:checkdomain")])
    buttons.append([InlineKeyboardButton(text="Загрузить CSV", callback_data="menu:uploadcsv")])
    if is_admin:
        buttons += [
            [InlineKeyboardButton(text="Пользователи", callback_data="menu:listusers"), InlineKeyboardButton(text="Управление", callback_data="menu:manage")],
            [InlineKeyboardButton(text="Команды", callback_data="menu:teams"), InlineKeyboardButton(text="Алиасы", callback_data="menu:aliases")],
            [InlineKeyboardButton(text="Менторы", callback_data="menu:mentors")],
            [InlineKeyboardButton(text="Обновить домены", callback_data="menu:refreshdomains")],
            [InlineKeyboardButton(text="Очистить FB данные", callback_data="menu:resetfbdata")],
        ]
    else:
        # For lead/head expose 'Моя команда'
        if has_lead_access or role in ("lead", "head"):
            buttons += [[InlineKeyboardButton(text="Моя команда", callback_data="menu:myteam")]]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# Helpers used by inline menu to avoid using call.message.from_user (which is the bot)
async def _send_whoami(chat_id: int, user_id: int, username: str | None):
    await bot.send_message(chat_id, f"Ваш Telegram ID: <code>{user_id}</code>\nUsername: @{username or '-'}")

async def _send_list_users(chat_id: int, actor_id: int):
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = my["role"] if my else "buyer"
    if actor_id in ADMIN_IDS:
        my_role = "admin"
    my_team = my.get("team_id") if my else None
    lead_team_ids = await db.list_user_lead_teams(actor_id) if my_role not in ("admin", "head") else []
    visible = []
    for u in users:
        if my_role in ("admin", "head"):
            visible.append(u)
        elif lead_team_ids:
            team_id = u.get("team_id")
            if team_id is not None and int(team_id) in lead_team_ids:
                visible.append(u)
        else:
            if u["telegram_id"] == actor_id:
                visible.append(u)
    if not visible:
        return await bot.send_message(chat_id, "Нет данных для отображения")
    lines = []
    for u in visible:
        display_role = u['role']
        if u['telegram_id'] == actor_id and actor_id in ADMIN_IDS:
            display_role = 'admin'
        lines.append(f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} — {u['full_name'] or ''} | role={display_role} | team={u['team_id'] or '-'}")
    await bot.send_message(chat_id, "Пользователи:\n" + "\n".join(lines))

async def _send_list_routes(chat_id: int, actor_id: int):
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == actor_id), None)
    my_role = (my or {}).get("role", "buyer")
    if actor_id in ADMIN_IDS:
        my_role = "admin"
    my_team = (my or {}).get("team_id")
    lead_team_ids = await db.list_user_lead_teams(actor_id) if my_role not in ("admin", "head") else []
    rows = await db.list_routes()
    def visible(r: dict) -> bool:
        if my_role in ("admin", "head"):
            return True
        if lead_team_ids:
            ru = next((u for u in users if u["telegram_id"] == r["user_id"]), None)
            if not ru:
                return False
            team_id = ru.get("team_id")
            return team_id is not None and int(team_id) in lead_team_ids
        return r["user_id"] == actor_id
    vis = [r for r in rows if visible(r)]
    if not vis:
        return await bot.send_message(chat_id, "Правил нет или нет доступа")
    def fmt(r):
        return f"#{r['id']} -> <code>{r['user_id']}</code> (@{r['username'] or '-'}) | offer={r['offer'] or '*'} | geo={r['country'] or '*'} | src={r['source'] or '*'} | prio={r['priority']}"
    await bot.send_message(chat_id, "Правила:\n" + "\n".join(fmt(r) for r in vis))

async def _send_manage(chat_id: int, actor_id: int):
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    users = await db.list_users()
    if not users:
        return await bot.send_message(chat_id, "Пока нет пользователей, попросите нажать /start")
    for u in users[:25]:
        text = f"<b>{u['full_name'] or '-'}</b> @{u['username'] or '-'}\nID: <code>{u['telegram_id']}</code>\nRole: <code>{u['role']}</code> | Team: <code>{u['team_id'] or '-'}</code> | Active: <code>{'yes' if u['is_active'] else 'no'}</code>"
        await bot.send_message(chat_id, text, reply_markup=_user_row_controls(u))

async def _send_aliases(chat_id: int, actor_id: int):
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    rows = await db.list_aliases()
    if not rows:
        await bot.send_message(chat_id, "Алиасов пока нет.")
    else:
        for r in rows[:25]:
            text = f"<b>{r['alias']}</b> → buyer={r['buyer_id'] or '-'} | lead={r['lead_id'] or '-'}"
            await bot.send_message(chat_id, text, reply_markup=alias_row_controls(r['alias'], r['buyer_id'], r['lead_id']))
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Добавить алиас", callback_data="alias:new")]])
    await bot.send_message(chat_id, "Управление алиасами:", reply_markup=kb)

# --- Lead/Head: My team management ---
def _myteam_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Состав команды", callback_data="myteam:list")],
        [InlineKeyboardButton(text="Добавить по ID", callback_data="myteam:add")],
        [InlineKeyboardButton(text="Убрать участника", callback_data="myteam:remove")],
    ])

async def _send_myteam(chat_id: int, actor_id: int):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == actor_id), None)
    lead_team_ids = await db.list_user_lead_teams(actor_id)
    if actor_id in ADMIN_IDS:
        lead_team_ids = [int(me.get("team_id"))] if me and me.get("team_id") else []
    if not lead_team_ids:
        return await bot.send_message(chat_id, "Недостаточно прав или вы не закреплены за командой")
    await bot.send_message(chat_id, "Моя команда — управление", reply_markup=_myteam_menu())

@dp.callback_query(F.data == "myteam:list")
async def cb_myteam_list(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    team_id = await db.get_primary_lead_team(call.from_user.id)
    if call.from_user.id in ADMIN_IDS and not team_id:
        team_id = int(me.get("team_id")) if me and me.get("team_id") else None
    if team_id is None:
        return await call.answer("Нет прав", show_alert=True)
    members = [u for u in users if u.get("team_id") is not None and int(u.get("team_id")) == int(team_id)]
    if not members:
        await call.message.answer("Состав пуст")
    else:
        lines = [f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} ({u['role']})" for u in members]
        await call.message.answer("Состав команды:\n" + "\n".join(lines))
    await call.answer()

@dp.callback_query(F.data == "myteam:add")
async def cb_myteam_add(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    team_id = await db.get_primary_lead_team(call.from_user.id)
    if call.from_user.id in ADMIN_IDS and not team_id:
        team_id = int(me.get("team_id")) if me and me.get("team_id") else None
    if team_id is None:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, f"myteam:add:{team_id}", None)
    await call.message.answer("Пришлите Telegram ID пользователя для добавления в вашу команду")
    await call.answer()

@dp.callback_query(F.data == "myteam:remove")
async def cb_myteam_remove(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    team_id = await db.get_primary_lead_team(call.from_user.id)
    if call.from_user.id in ADMIN_IDS and not team_id:
        team_id = int(me.get("team_id")) if me and me.get("team_id") else None
    if team_id is None:
        return await call.answer("Нет прав", show_alert=True)
    members = [u for u in users if u.get("team_id") is not None and int(u.get("team_id")) == int(team_id)]
    if not members:
        await call.message.answer("Состав пуст")
        return await call.answer()
    buttons = [[InlineKeyboardButton(text=f"Убрать @{u['username'] or u['telegram_id']}", callback_data=f"myteam:remove:{u['telegram_id']}")] for u in members[:25]]
    await call.message.answer("Кого убрать?", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()

@dp.callback_query(F.data.startswith("myteam:remove:"))
async def cb_myteam_remove_user(call: CallbackQuery):
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == call.from_user.id), None)
    team_id = await db.get_primary_lead_team(call.from_user.id)
    if call.from_user.id in ADMIN_IDS and not team_id:
        team_id = int(me.get("team_id")) if me and me.get("team_id") else None
    if team_id is None:
        return await call.answer("Нет прав", show_alert=True)
    uid = int(call.data.split(":", 2)[2])
    # ensure target is in same team
    target = next((u for u in users if u["telegram_id"] == uid), None)
    if not target or target.get("team_id") is None or int(target.get("team_id")) != int(team_id):
        return await call.answer("Можно убирать только из своей команды", show_alert=True)
    await db.set_user_team(uid, None)
    await call.answer("Убран из команды")
# --- Teams management (admin) ---
def _teams_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список команд", callback_data="teams:list"), InlineKeyboardButton(text="Создать команду", callback_data="teams:new")],
        [InlineKeyboardButton(text="Назначить лида", callback_data="teams:setlead")],
        [InlineKeyboardButton(text="Участники", callback_data="teams:members")],
    ])

async def _send_teams(chat_id: int, actor_id: int):
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    await bot.send_message(chat_id, "Команды — управление", reply_markup=_teams_menu())

@dp.callback_query(F.data == "teams:list")
async def cb_teams_list(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    teams = await db.list_teams()
    if not teams:
        await call.message.answer("Команд нет")
        return await call.answer()
    lines = [f"#{t['id']} — {t['name']}" for t in teams]
    await call.message.answer("Команды:\n" + "\n".join(lines))
    await call.answer()

@dp.callback_query(F.data == "teams:new")
async def cb_team_new(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "team:new", None)
    await call.message.answer("Введите название новой команды:")
    await call.answer()

@dp.callback_query(F.data == "teams:setlead")
async def cb_team_setlead(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "team:setlead:ask_team", None)
    teams = await db.list_teams()
    if not teams:
        await call.message.answer("Команд нет")
        return await call.answer()
    buttons = [[InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"team:choose_for_lead:{t['id']}")] for t in teams[:50]]
    await call.message.answer("Выберите команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()

@dp.callback_query(F.data.startswith("team:choose_for_lead:"))
async def cb_team_choose_for_lead(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    team_id = int(call.data.split(":", 2)[2])
    await db.set_pending_action(call.from_user.id, f"team:setlead:{team_id}", None)
    await call.message.answer("Пришлите Telegram ID или @username пользователя, которого назначить лидом этой команды")
    await call.answer()

@dp.callback_query(F.data == "teams:members")
async def cb_team_members(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    teams = await db.list_teams()
    if not teams:
        await call.message.answer("Команд нет")
        return await call.answer()
    buttons = [[InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"team:members:{t['id']}")] for t in teams[:50]]
    await call.message.answer("Выберите команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()

@dp.callback_query(F.data.startswith("team:members:"))
async def cb_team_members_manage(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    team_id = int(call.data.split(":", 2)[2])
    users = await db.list_users()
    members = [u for u in users if u.get("team_id") == team_id]
    non_members = [u for u in users if u.get("team_id") != team_id]
    # render lists in chunks
    if members:
        await call.message.answer("Участники:\n" + "\n".join(f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} ({u['role']})" for u in members[:50]))
    else:
        await call.message.answer("Участники: пусто")
    # controls
    add_buttons = [[InlineKeyboardButton(text=f"Добавить @{u['username'] or u['telegram_id']}", callback_data=f"team:add:{team_id}:{u['telegram_id']}")] for u in non_members[:25]]
    remove_buttons = [[InlineKeyboardButton(text=f"Убрать @{u['username'] or u['telegram_id']}", callback_data=f"team:remove:{team_id}:{u['telegram_id']}")] for u in members[:25]]
    action_buttons = [[InlineKeyboardButton(text="Обновить имена", callback_data=f"team:refresh_names:{team_id}")]]
    if add_buttons:
        await call.message.answer("Добавить в команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=add_buttons))
    if remove_buttons:
        await call.message.answer("Убрать из команды:", reply_markup=InlineKeyboardMarkup(inline_keyboard=remove_buttons))
    # refresh button
    await call.message.answer("Действия:", reply_markup=InlineKeyboardMarkup(inline_keyboard=action_buttons))
    await call.answer()

@dp.callback_query(F.data.startswith("team:refresh_names:"))
async def cb_team_refresh_names(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    team_id = int(call.data.split(":", 2)[2])
    users = await db.list_users()
    members = [u for u in users if u.get("team_id") == team_id]
    updated = 0
    for u in members:
        uid = int(u["telegram_id"])  # type: ignore
        try:
            chat = await bot.get_chat(uid)
            uname = chat.username or u.get("username")
            try:
                fn = getattr(chat, "first_name", None) or ""
                ln = getattr(chat, "last_name", None) or ""
                name = (fn + (" " + ln if ln else "")).strip()
                fullname = name or u.get("full_name")
            except Exception:
                fullname = u.get("full_name")
            await db.upsert_user(uid, uname, fullname)
            updated += 1
        except Exception:
            # ignore fetch errors
            pass
    await call.answer("Готово")
    await call.message.answer(f"Обновлено профилей: {updated}")

@dp.callback_query(F.data.startswith("team:add:"))
async def cb_team_add_member(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    # callback format: team:add:<team_id>:<user_id>
    _, _, team_id, uid = call.data.split(":", 3)
    # Ensure user exists and enrich with Telegram username/full_name if possible; preserve existing values
    try:
        existing = await db.get_user(int(uid))
        tg_username = None
        tg_fullname = None
        try:
            chat = await bot.get_chat(int(uid))
            tg_username = chat.username
            # Build full name from first/last if full_name not available
            try:
                fn = getattr(chat, "first_name", None) or ""
                ln = getattr(chat, "last_name", None) or ""
                name = (fn + (" " + ln if ln else "")).strip()
                tg_fullname = name or None
            except Exception:
                tg_fullname = None
        except Exception:
            # fetching chat can fail for privacy/blocked; ignore
            pass
        final_username = tg_username or (existing.get("username") if existing else None)
        final_fullname = tg_fullname or (existing.get("full_name") if existing else None)
        await db.upsert_user(int(uid), final_username, final_fullname)
    except Exception:
        # As a fallback, at least ensure a stub row exists without overwriting name fields
        try:
            await db.upsert_user(int(uid), None, None)
        except Exception:
            pass
    await db.set_user_team(int(uid), int(team_id))
    await call.answer("Добавлен")

@dp.callback_query(F.data.startswith("team:remove:"))
async def cb_team_remove_member(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    # callback format: team:remove:<team_id>:<user_id>
    _, _, team_id, uid = call.data.split(":", 3)
    await db.set_user_team(int(uid), None)
    await call.answer("Убран")

@dp.message(Command("menu"))
async def on_menu(message: Message):
    is_admin = message.from_user.id in ADMIN_IDS
    # get role to expose lead/head specific menu
    users = await db.list_users()
    me = next((u for u in users if u["telegram_id"] == message.from_user.id), None)
    role = (me or {}).get("role")
    if is_admin:
        role = "admin"
    has_lead_access = is_admin
    if not has_lead_access:
        lead_team_ids = await db.list_user_lead_teams(message.from_user.id)
        has_lead_access = bool(lead_team_ids) or (role in ("lead", "head"))
    await message.answer("Меню:", reply_markup=main_menu(is_admin, role, has_lead_access=has_lead_access))

@dp.callback_query(F.data.startswith("menu:"))
async def on_menu_click(call: CallbackQuery):
    key = call.data.split(":",1)[1]
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
            await status_msg.edit_text(f"Готово. Обновлено {count} записей.")
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
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await call.answer("Отменено")
    try:
        await call.message.edit_text("Операция отменена.")
    except Exception:
        pass


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
    succeeded = await _process_fb_csv_upload(message, filename, parsed, status_msg)
    if succeeded:
        await db.clear_pending_action(message.from_user.id)

async def _process_fb_csv_upload(message: Message, filename: str, parsed: fb_csv.ParsedFbCsv, status_msg: Message) -> bool:
    user_id = message.from_user.id
    campaign_meta: Dict[str, Dict[str, Any]] = {}
    assigned_buyers: Set[int] = set()
    upload_id: Optional[int] = None
    upload_buyer_id: Optional[int] = None
    try:
        campaign_meta = await _resolve_campaign_assignments(parsed.campaign_names)
        assigned_buyers = {
            int(meta.get("buyer_id"))
            for meta in campaign_meta.values()
            if meta.get("buyer_id") is not None
        }
        upload_buyer_id = next(iter(assigned_buyers)) if len(assigned_buyers) == 1 else None
        upload_id = await db.create_fb_csv_upload(
            uploaded_by=user_id,
            buyer_id=upload_buyer_id,
            original_filename=filename,
            period_start=parsed.period_start,
            period_end=parsed.period_end,
            row_count=len(parsed.raw_rows),
            has_totals=parsed.has_totals,
        )
        await db.bulk_insert_fb_csv_rows(upload_id, parsed.raw_rows)
    except Exception as exc:
        logger.exception("Failed to persist FB CSV upload", exc_info=exc)
        await status_msg.edit_text("Не удалось сохранить CSV в базе. Сообщите админу.")
        await _notify_admins_about_exception(
            "Ошибка при сохранении CSV",
            exc,
            [
                f"User ID: {message.from_user.id}",
                f"Filename: {filename}",
            ],
        )
        return False

    if upload_id is None:
        await status_msg.edit_text("Сохранение CSV не завершено. Попробуйте ещё раз позже.")
        return False

    if not parsed.daily_rows:
        await status_msg.edit_text(
            "CSV сохранён, но в файле нет строк с датой. Проверьте, что выгрузка сделана с разбивкой по дням."
        )
        # Обновим владельцев аккаунтов даже если нет дневных строк
        await _update_fb_accounts(parsed, campaign_meta)
        return True

    try:
        keitaro_stats = await db.fetch_keitaro_campaign_stats(
            parsed.campaign_names,
            parsed.period_start,
            parsed.period_end,
        )
        flag_rows = await db.list_fb_flags()
        flag_by_code = {row["code"].upper(): row for row in flag_rows}
        flag_id_to_title = {row["id"]: row["title"] for row in flag_rows}
        flag_id_to_code = {row["id"]: row["code"] for row in flag_rows}
        state_map = await db.fetch_fb_campaign_state(parsed.campaign_names)
        latest_day = parsed.latest_day_by_campaign
        daily_stats = keitaro_stats.get("daily", {})

        def as_decimal(value: Any) -> Decimal:
            if value is None:
                return Decimal("0")
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))

        daily_records: list[dict[str, Any]] = []
        per_campaign_info: dict[str, dict[str, Any]] = {}
        upload_spend = Decimal("0")
        account_buyers: Dict[str, Set[int]] = defaultdict(set)
        aggregated_daily: Dict[tuple[str, date], Dict[str, Any]] = {}

        for row in parsed.daily_rows:
            campaign = row.get("campaign_name")
            day = row.get("day_date")
            if not campaign or not day:
                continue
            meta = campaign_meta.get(campaign, {})
            campaign_buyer_id = meta.get("buyer_id")
            spend_raw = row.get("spend")
            spend = as_decimal(spend_raw) if spend_raw is not None else Decimal("0")
            impressions = row.get("impressions") or 0
            clicks = row.get("clicks") or 0
            leads = row.get("leads")
            registrations = row.get("registrations")
            geo = row.get("geo")
            state = state_map.get(campaign) or {}
            status_id = state.get("status_id")
            account_name = row.get("account_name")
            if account_name and campaign_buyer_id is not None:
                try:
                    account_buyers[account_name].add(int(campaign_buyer_id))
                except Exception:
                    pass

            key = (campaign, day)
            entry = aggregated_daily.setdefault(
                key,
                {
                    "campaign_name": campaign,
                    "day_date": day,
                    "account_name": account_name,
                    "buyer_id": campaign_buyer_id,
                    "geo": geo,
                    "spend": Decimal("0"),
                    "impressions": 0,
                    "clicks": 0,
                    "registrations": 0,
                    "leads": 0,
                    "status_id": status_id,
                },
            )
            entry["spend"] += spend
            entry["impressions"] += int(impressions or 0)
            entry["clicks"] += int(clicks or 0)
            if registrations is not None:
                entry["registrations"] += int(registrations)
            if leads is not None:
                entry["leads"] += int(leads)
            if account_name and not entry.get("account_name"):
                entry["account_name"] = account_name
            if campaign_buyer_id is not None and entry.get("buyer_id") is None:
                entry["buyer_id"] = campaign_buyer_id
            if geo and not entry.get("geo"):
                entry["geo"] = geo
            if status_id is not None:
                entry["status_id"] = status_id

            upload_spend += spend

        for (campaign, day), entry in aggregated_daily.items():
            meta = campaign_meta.get(campaign, {})
            stats = daily_stats.get((campaign, day), {})
            ftd = int(stats.get("ftd") or 0)
            revenue = as_decimal(stats.get("revenue"))
            spend_total = entry["spend"]
            impressions_total = entry["impressions"]
            clicks_total = entry["clicks"]
            registrations_total = entry["registrations"]
            leads_total = entry["leads"]
            ctr = (Decimal(clicks_total) / Decimal(impressions_total) * Decimal(100)) if impressions_total else None
            cpc = (spend_total / Decimal(clicks_total)) if clicks_total else None
            roi = ((revenue - spend_total) / spend_total * Decimal(100)) if spend_total else None
            ftd_rate = (
                (Decimal(ftd) / Decimal(registrations_total) * Decimal(100))
                if registrations_total
                else None
            )
            flag_decision = fb_csv.decide_flag(spend_total, ctr, roi, ftd)
            flag_row = flag_by_code.get(flag_decision.code.upper())
            flag_id = flag_row.get("id") if flag_row else None
            daily_records.append(
                {
                    "campaign_name": campaign,
                    "day_date": day,
                    "account_name": entry.get("account_name"),
                    "buyer_id": entry.get("buyer_id"),
                    "geo": entry.get("geo"),
                    "spend": spend_total,
                    "impressions": impressions_total,
                    "clicks": clicks_total,
                    "registrations": registrations_total,
                    "leads": leads_total,
                    "ftd": ftd,
                    "revenue": revenue,
                    "ctr": ctr,
                    "cpc": cpc,
                    "roi": roi,
                    "ftd_rate": ftd_rate,
                    "status_id": entry.get("status_id"),
                    "flag_id": flag_id,
                    "upload_id": upload_id,
                }
            )
            if latest_day.get(campaign) == day:
                per_campaign_info[campaign] = {
                    "status_id": entry.get("status_id"),
                    "buyer_id": entry.get("buyer_id"),
                    "alias_key": meta.get("alias_key"),
                    "alias_lead_id": meta.get("alias_lead_id"),
                    "day": day,
                }

        if daily_records:
            await db.upsert_fb_campaign_daily(daily_records)

        period_text = "—"
        if parsed.period_start and parsed.period_end:
            if parsed.period_start == parsed.period_end:
                period_text = parsed.period_start.isoformat()
            else:
                period_text = f"{parsed.period_start.isoformat()} — {parsed.period_end.isoformat()}"

        summary_main: list[str] = [
            f"<b>Файл:</b> {html.escape(filename)}",
            f"<b>Период:</b> {html.escape(period_text)}",
            f"<b>Кампаний:</b> {len(parsed.campaign_names)}",
            f"<b>Строк:</b> {len(parsed.raw_rows)}",
            f"<b>Spend за загрузку:</b> {_fmt_money(upload_spend)}",
            f"<b>Байеров в загрузке:</b> {len(assigned_buyers)}",
        ]

        unresolved = sorted(
            campaign for campaign, meta in campaign_meta.items() if meta.get("buyer_id") is None
        )
        if unresolved:
            shown = unresolved[:5]
            suffix = " …" if len(unresolved) > 5 else ""
            summary_main.append(
                "Нет привязки к байеру для: " + ", ".join(html.escape(c) for c in shown) + suffix
            )

        target_month_start: Optional[date] = None
        if parsed.period_end:
            target_month_start = parsed.period_end.replace(day=1)
        elif parsed.period_start:
            target_month_start = parsed.period_start.replace(day=1)
        else:
            target_month_start = date.today().replace(day=1)

        month_summary: Optional[Dict[str, Any]] = None
        month_campaign_rows: List[Dict[str, Any]] = []
        if target_month_start:
            try:
                month_campaign_rows = await db.fetch_fb_campaign_month_report(target_month_start)
            except Exception as exc:
                logger.warning("Failed to fetch FB campaign month report", exc_info=exc)

        def _should_skip_month_campaign(name: Any) -> bool:
            if not name:
                return True
            return "," in str(name)

        aggregated_month_campaigns: Dict[str, Dict[str, Any]] = {}
        aggregated_account_names: Set[str] = set()
        if target_month_start:
            for daily_row in parsed.daily_rows:
                day_value = daily_row.get("day_date")
                if not day_value or day_value.replace(day=1) != target_month_start:
                    continue
                campaign_name = daily_row.get("campaign_name")
                if _should_skip_month_campaign(campaign_name):
                    continue
                entry = aggregated_month_campaigns.setdefault(
                    campaign_name,
                    {
                        "spend": Decimal("0"),
                        "impressions": 0,
                        "clicks": 0,
                        "registrations": 0,
                        "ftd": 0,
                        "revenue": Decimal("0"),
                        "account_name": daily_row.get("account_name"),
                        "flag_id": None,
                    },
                )
                entry["spend"] += as_decimal(daily_row.get("spend"))
                entry["impressions"] += int(daily_row.get("impressions") or 0)
                entry["clicks"] += int(daily_row.get("clicks") or 0)
                entry["registrations"] += int(daily_row.get("registrations") or 0)
                account_name = daily_row.get("account_name")
                if account_name:
                    aggregated_account_names.add(str(account_name))
                    if not entry.get("account_name"):
                        entry["account_name"] = account_name

        month_campaign_lookup: Dict[str, Dict[str, Any]] = {}
        for row in month_campaign_rows:
            campaign_name = row.get("campaign_name")
            if _should_skip_month_campaign(campaign_name):
                continue
            if campaign_name:
                month_campaign_lookup[str(campaign_name)] = row

        for campaign_name, entry in aggregated_month_campaigns.items():
            db_row = month_campaign_lookup.get(campaign_name)
            if not db_row:
                continue
            entry["ftd"] = int(db_row.get("ftd") or entry.get("ftd") or 0)
            entry["revenue"] = as_decimal(db_row.get("revenue"))
            flag_id = db_row.get("curr_flag_id") or db_row.get("state_flag_id") or db_row.get("prev_flag_id")
            if flag_id is not None:
                entry["flag_id"] = flag_id
            account_name = db_row.get("account_name")
            if account_name:
                aggregated_account_names.add(str(account_name))
                if not entry.get("account_name"):
                    entry["account_name"] = account_name

        def _update_month_flag_info(
            campaign: str,
            spend_value: Decimal,
            revenue_value: Decimal,
            impressions_value: int,
            clicks_value: int,
            registrations_value: int,
            ftd_value: int,
            account_name_value: Optional[str],
        ) -> Dict[str, Any]:
            state = state_map.get(campaign) or {}
            meta = campaign_meta.get(campaign, {})
            existing = per_campaign_info.get(campaign, {})
            buyer_id = existing.get("buyer_id", meta.get("buyer_id"))
            alias_key = existing.get("alias_key", meta.get("alias_key"))
            alias_lead_id = existing.get("alias_lead_id", meta.get("alias_lead_id"))
            status_id = existing.get("status_id", state.get("status_id"))
            day_value = latest_day.get(campaign)
            ctr_value = (
                (Decimal(clicks_value) / Decimal(impressions_value) * Decimal(100))
                if impressions_value
                else None
            )
            roi_value = ((revenue_value - spend_value) / spend_value * Decimal(100)) if spend_value else None
            ftd_rate_value = (
                (Decimal(ftd_value) / Decimal(registrations_value) * Decimal(100))
                if registrations_value
                else None
            )
            flag_decision = fb_csv.decide_flag(spend_value, ctr_value, roi_value, ftd_value)
            flag_row = flag_by_code.get(flag_decision.code.upper())
            new_flag_id = flag_row.get("id") if flag_row else None
            info = {
                "decision": flag_decision,
                "flag_id": new_flag_id,
                "old_flag_id": state.get("flag_id"),
                "status_id": status_id,
                "roi": roi_value,
                "spend": spend_value,
                "ftd": ftd_value,
                "revenue": revenue_value,
                "ctr": ctr_value,
                "ftd_rate": ftd_rate_value,
                "reason": flag_decision.reason,
                "buyer_id": buyer_id,
                "alias_key": alias_key,
                "alias_lead_id": alias_lead_id,
                "day": day_value,
                "account_name": account_name_value,
            }
            per_campaign_info[campaign] = info
            return info

        campaign_section: list[str] = []
        if aggregated_month_campaigns and target_month_start:
            account_names_for_summary = set(aggregated_account_names)
            spend_total = sum((entry["spend"] for entry in aggregated_month_campaigns.values()), Decimal("0"))
            revenue_total = sum(
                (entry.get("revenue", Decimal("0")) for entry in aggregated_month_campaigns.values()),
                Decimal("0"),
            )
            impressions_total = sum(entry["impressions"] for entry in aggregated_month_campaigns.values())
            clicks_total = sum(entry["clicks"] for entry in aggregated_month_campaigns.values())
            registrations_total = sum(entry["registrations"] for entry in aggregated_month_campaigns.values())
            ftd_total = sum(int(entry.get("ftd") or 0) for entry in aggregated_month_campaigns.values())
            for entry in aggregated_month_campaigns.values():
                account_name = entry.get("account_name")
                if account_name:
                    account_names_for_summary.add(str(account_name))
            month_summary = {
                "month_start": target_month_start,
                "spend": spend_total,
                "revenue": revenue_total,
                "impressions": impressions_total,
                "clicks": clicks_total,
                "registrations": registrations_total,
                "ftd": ftd_total,
                "campaign_count": len(aggregated_month_campaigns),
                "account_count": len(account_names_for_summary),
            }
            campaign_section.append(
                "<b>Кампании за " + html.escape(_month_label_ru(target_month_start)) + ":</b>"
            )
            sorted_items = sorted(
                aggregated_month_campaigns.items(),
                key=lambda item: float(item[1]["spend"]),
                reverse=True,
            )
            for name, stats in sorted_items:
                spend_value = stats["spend"]
                revenue_value = stats.get("revenue", Decimal("0"))
                ftd_value = int(stats.get("ftd") or 0)
                impressions_value = int(stats.get("impressions") or 0)
                clicks_value = int(stats.get("clicks") or 0)
                registrations_value = int(stats.get("registrations") or 0)
                account_name = stats.get("account_name")
                info = _update_month_flag_info(
                    name,
                    spend_value,
                    revenue_value,
                    impressions_value,
                    clicks_value,
                    registrations_value,
                    ftd_value,
                    account_name,
                )
                flag_label = (
                    flag_id_to_title.get(info.get("flag_id"))
                    or flag_id_to_code.get(info.get("flag_id"))
                    or info["decision"].code
                )
                roi_value = info.get("roi")
                line = "• " + html.escape(str(name))
                if account_name:
                    line += f" ({html.escape(str(account_name))})"
                line += " — " + html.escape(str(flag_label))
                line += (
                    f". Spend {_fmt_money(spend_value)} | FTD {ftd_value} | Rev {_fmt_money(revenue_value)} | ROI {_fmt_percent(roi_value)}"
                )
                campaign_section.append(line)
        elif month_campaign_rows and target_month_start:
            spend_total = Decimal("0")
            revenue_total = Decimal("0")
            impressions_total = 0
            clicks_total = 0
            registrations_total = 0
            ftd_total = 0
            account_names: Set[str] = set()
            valid_campaign_count = 0
            campaign_section.append(
                "<b>Кампании за " + html.escape(_month_label_ru(target_month_start)) + ":</b>"
            )
            sorted_rows = sorted(
                month_campaign_rows,
                key=lambda row: float(row.get("spend") or 0),
                reverse=True,
            )
            for row in sorted_rows:
                name = row.get("campaign_name")
                if _should_skip_month_campaign(name):
                    continue
                spend_value = as_decimal(row.get("spend"))
                revenue_value = as_decimal(row.get("revenue"))
                impressions_value = int(row.get("impressions") or 0)
                clicks_value = int(row.get("clicks") or 0)
                registrations_value = int(row.get("registrations") or 0)
                ftd_value = int(row.get("ftd") or 0)
                account_name = row.get("account_name")
                info = _update_month_flag_info(
                    str(name),
                    spend_value,
                    revenue_value,
                    impressions_value,
                    clicks_value,
                    registrations_value,
                    ftd_value,
                    account_name,
                )
                flag_label = (
                    flag_id_to_title.get(info.get("flag_id"))
                    or flag_id_to_code.get(info.get("flag_id"))
                    or info["decision"].code
                )
                line = "• " + html.escape(str(name))
                if account_name:
                    line += f" ({html.escape(str(account_name))})"
                line += " — " + html.escape(str(flag_label))
                line += (
                    f". Spend {_fmt_money(spend_value)} | FTD {ftd_value} | Rev {_fmt_money(revenue_value)} | ROI {_fmt_percent(info.get('roi'))}"
                )
                campaign_section.append(line)
                spend_total += spend_value
                revenue_total += revenue_value
                impressions_total += impressions_value
                clicks_total += clicks_value
                registrations_total += registrations_value
                ftd_total += ftd_value
                if account_name:
                    account_names.add(str(account_name))
                valid_campaign_count += 1
            month_summary = {
                "month_start": target_month_start,
                "spend": spend_total,
                "revenue": revenue_total,
                "impressions": impressions_total,
                "clicks": clicks_total,
                "registrations": registrations_total,
                "ftd": ftd_total,
                "campaign_count": valid_campaign_count,
                "account_count": len(account_names),
            }

        per_campaign_info = {
            campaign: info for campaign, info in per_campaign_info.items() if info.get("decision")
        }

        states_to_upsert: list[dict[str, Any]] = []
        history_entries: list[dict[str, Any]] = []
        flag_changes: list[dict[str, Any]] = []

        for campaign, info in per_campaign_info.items():
            new_flag_id = info.get("flag_id")
            state_raw = state_map.get(campaign)
            state = state_raw or {}
            old_flag_id = info.get("old_flag_id")
            status_id = info.get("status_id")
            buyer_comment = state.get("buyer_comment")
            lead_comment = state.get("lead_comment")
            if state_raw is None or new_flag_id != old_flag_id:
                states_to_upsert.append(
                    {
                        "campaign_name": campaign,
                        "status_id": status_id,
                        "flag_id": new_flag_id,
                        "buyer_comment": buyer_comment,
                        "lead_comment": lead_comment,
                        "updated_by": user_id,
                    }
                )
            if state_raw is not None or new_flag_id is not None or old_flag_id is not None:
                history_entries.append(
                    {
                        "campaign_name": campaign,
                        "changed_by": user_id,
                        "old_status_id": status_id,
                        "new_status_id": status_id,
                        "old_flag_id": old_flag_id,
                        "new_flag_id": new_flag_id,
                        "note": info.get("reason"),
                    }
                )
            if new_flag_id != old_flag_id:
                old_label = flag_id_to_title.get(old_flag_id) or flag_id_to_code.get(old_flag_id) or "—"
                new_label = (
                    flag_id_to_title.get(new_flag_id)
                    or flag_id_to_code.get(new_flag_id)
                    or info["decision"].code
                )
                flag_changes.append(
                    {
                        "campaign": campaign,
                        "old": old_label,
                        "new": new_label,
                        "reason": info.get("reason", ""),
                        "buyer_id": info.get("buyer_id"),
                        "alias_key": info.get("alias_key"),
                        "alias_lead_id": info.get("alias_lead_id"),
                        "spend": info.get("spend"),
                        "revenue": info.get("revenue"),
                        "ftd": info.get("ftd"),
                        "roi": info.get("roi"),
                        "ctr": info.get("ctr"),
                        "ftd_rate": info.get("ftd_rate"),
                        "day": info.get("day"),
                    }
                )

        if states_to_upsert:
            await db.upsert_fb_campaign_state(states_to_upsert)
        if history_entries:
            await db.log_fb_campaign_history(history_entries)

        await db.recompute_fb_campaign_totals(parsed.campaign_names)
        await _update_fb_accounts(parsed, campaign_meta, account_buyers)

        flag_section: list[str] = []
        if flag_changes:
            max_flag_rows = 30
            flag_section.append("<b>Изменения флагов:</b>")
            for change in flag_changes[:max_flag_rows]:
                flag_section.append(
                    "• "
                    + html.escape(change["campaign"])
                    + ": "
                    + html.escape(change["old"] or "—")
                    + " → "
                    + html.escape(change["new"] or "—")
                    + " ("
                    + html.escape(change.get("reason") or "")
                    + ")"
                )
            remaining = len(flag_changes) - max_flag_rows
            if remaining > 0:
                flag_section.append(f"• ещё {remaining} изменений …")
            summary_main.append("<b>Изменения флагов:</b> см. ниже")
        else:
            summary_main.append("Изменения флагов: нет")

        month_section: list[str] = []
        if month_summary and target_month_start:
            month_label = _month_label_ru(target_month_start)
            spend_total = as_decimal(month_summary.get("spend"))
            revenue_total = as_decimal(month_summary.get("revenue"))
            ftd_total = int(month_summary.get("ftd") or 0)
            registrations_total = int(month_summary.get("registrations") or 0)
            roi_total = ((revenue_total - spend_total) / spend_total * Decimal(100)) if spend_total else None
            ftd_rate_total = (Decimal(ftd_total) / Decimal(registrations_total) * Decimal(100)) if registrations_total else None
            campaign_count = int(month_summary.get("campaign_count") or 0)
            account_count = int(month_summary.get("account_count") or 0)
            month_section.append("<b>" + html.escape(month_label) + "</b>")
            month_section.append(
                f"Spend {_fmt_money(spend_total)} | Rev {_fmt_money(revenue_total)} | ROI {_fmt_percent(roi_total)} | FTD {ftd_total} (FTD rate {_fmt_percent(ftd_rate_total)}) | Кампаний {campaign_count} | Кабинетов {account_count}"
            )

        campaign_section: list[str] = []
        if month_campaign_rows and target_month_start:
            campaign_section.append(
                "<b>Кампании за " + html.escape(_month_label_ru(target_month_start)) + ":</b>"
            )
            sorted_rows = sorted(
                month_campaign_rows,
                key=lambda row: float(row.get("spend") or 0),
                reverse=True,
            )
            for row in sorted_rows:
                name = row.get("campaign_name")
                if _should_skip_month_campaign(name):
                    continue
                spend_value = as_decimal(row.get("spend"))
                revenue_value = as_decimal(row.get("revenue"))
                ftd_value = int(row.get("ftd") or 0)
                roi_value = ((revenue_value - spend_value) / spend_value * Decimal(100)) if spend_value else None
                account_name = row.get("account_name")
                flag_id = row.get("curr_flag_id") or row.get("state_flag_id") or row.get("prev_flag_id")
                flag_label = flag_id_to_title.get(flag_id) or flag_id_to_code.get(flag_id) or "—"
                line = "• " + html.escape(str(name))
                if account_name:
                    line += f" ({html.escape(str(account_name))})"
                line += " — " + html.escape(flag_label)
                line += f". Spend {_fmt_money(spend_value)} | FTD {ftd_value} | Rev {_fmt_money(revenue_value)} | ROI {_fmt_percent(roi_value)}"
                campaign_section.append(line)

        missing_keitaro = sorted(
            c for c in parsed.campaign_names if c not in keitaro_stats.get("totals", {}) and "," not in c
        )
        missing_section: list[str] = []
        if missing_keitaro:
            shown = missing_keitaro[:5]
            suffix = " …" if len(missing_keitaro) > 5 else ""
            missing_section.append(
                "Внимание: нет данных Keitaro для: " + ", ".join(html.escape(c) for c in shown) + suffix
            )

        def build_messages(sections: list[list[str]], max_length: int = 3500) -> list[str]:
            lines: list[str] = []
            for section in sections:
                if not section:
                    continue
                if lines:
                    lines.append("")
                lines.extend(section)
            messages: list[str] = []
            current_lines: list[str] = []
            current_len = 0
            for line in lines:
                appended_len = len(line) + 1
                if appended_len > max_length:
                    # hard truncate overly long lines to fit Telegram limit
                    trimmed = line[: max_length - 4] + " …"
                    line = trimmed
                    appended_len = len(line) + 1
                if current_lines and current_len + appended_len > max_length:
                    messages.append("\n".join(current_lines))
                    current_lines = []
                    current_len = 0
                current_lines.append(line)
                current_len += appended_len
            if current_lines:
                messages.append("\n".join(current_lines))
            return messages or [""]

        sections = [summary_main, flag_section, month_section, campaign_section, missing_section]
        message_chunks = build_messages(sections)
        await status_msg.edit_text(message_chunks[0], parse_mode=ParseMode.HTML)
        for extra_text in message_chunks[1:]:
            await bot.send_message(message.chat.id, extra_text, parse_mode=ParseMode.HTML)
        if flag_changes:
            await _notify_flag_updates(filename, flag_changes)
        return True
    except Exception as exc:
        logger.exception("Failed to process FB CSV data", exc_info=exc)
        await status_msg.edit_text("Во время расчётов произошла ошибка. Сообщите админу.")
        await _notify_admins_about_exception(
            "Ошибка при обработке CSV",
            exc,
            [
                f"User ID: {message.from_user.id}",
                f"Filename: {filename}",
                f"Upload ID: {upload_id or '—'}",
            ],
        )
        return False


async def _update_fb_accounts(
    parsed: fb_csv.ParsedFbCsv,
    campaign_meta: Dict[str, Dict[str, Any]],
    account_buyers: Optional[Dict[str, Set[int]]] = None,
) -> None:
    buyers_map: Dict[str, Set[int]] = {}
    if account_buyers:
        for account, buyers in account_buyers.items():
            buyers_map[account] = {int(b) for b in buyers if b is not None}
    else:
        temp: Dict[str, Set[int]] = defaultdict(set)
        for row in parsed.raw_rows:
            account = row.get("account_name")
            campaign = row.get("campaign_name")
            if not account or not campaign:
                continue
            meta = campaign_meta.get(campaign) or {}
            buyer_id = meta.get("buyer_id")
            if buyer_id is None:
                continue
            try:
                temp.setdefault(account, set()).add(int(buyer_id))
            except Exception:
                continue
        buyers_map = temp
    all_accounts: Set[str] = set()
    all_accounts.update(buyers_map.keys())
    all_accounts.update(name for name in parsed.account_names if name)
    payload: List[Dict[str, Any]] = []
    for account in sorted(all_accounts):
        buyers = buyers_map.get(account, set())
        buyer_id: Optional[int] = None
        if len(buyers) == 1:
            buyer_id = next(iter(buyers))
        payload.append(
            {
                "account_name": account,
                "buyer_id": buyer_id,
                "owner_since": parsed.period_start,
            }
        )
    if not payload:
        return
    try:
        await db.upsert_fb_accounts(payload)
    except Exception as exc:
        logger.warning("Failed to upsert FB accounts", exc_info=exc)


async def _notify_flag_updates(filename: str, flag_changes: List[Dict[str, Any]]) -> None:
    if not flag_changes:
        return
    try:
        users = await db.list_users()
    except Exception as exc:
        logger.warning("Failed to fetch users for flag notifications", exc_info=exc)
        return
    user_map: Dict[int, Dict[str, Any]] = {}
    for row in users:
        telegram_id = row.get("telegram_id")
        if telegram_id is None:
            continue
        try:
            user_map[int(telegram_id)] = row
        except Exception:
            continue
    admins_from_db = {uid for uid, row in user_map.items() if row.get("role") == "admin" and row.get("is_active")}
    heads = {uid for uid, row in user_map.items() if row.get("role") == "head" and row.get("is_active")}
    env_admins: Set[int] = set()
    for aid in ADMIN_IDS:
        try:
            env_admins.add(int(aid))
        except Exception:
            continue
    admin_recipients = admins_from_db | env_admins
    team_leads_cache: Dict[int, List[int]] = {}
    team_mentors_cache: Dict[int, List[int]] = {}
    recipient_messages: Dict[int, List[str]] = defaultdict(list)

    async def get_team_leads_cached(team_id: int) -> List[int]:
        if team_id not in team_leads_cache:
            try:
                team_leads_cache[team_id] = await db.list_team_leads(team_id)
            except Exception as exc:
                logger.warning("Failed to fetch team leads", team_id=team_id, exc_info=exc)
                team_leads_cache[team_id] = []
        return team_leads_cache[team_id]

    async def get_team_mentors_cached(team_id: int) -> List[int]:
        if team_id not in team_mentors_cache:
            try:
                team_mentors_cache[team_id] = await db.list_team_mentors(team_id)
            except Exception as exc:
                logger.warning("Failed to fetch team mentors", team_id=team_id, exc_info=exc)
                team_mentors_cache[team_id] = []
        return team_mentors_cache[team_id]

    def format_user_label(user_id: Optional[int]) -> str:
        if user_id is None:
            return ""
        try:
            uid = int(user_id)
        except Exception:
            return str(user_id)
        info = user_map.get(uid)
        if not info:
            return str(uid)
        username = info.get("username")
        if username:
            return f"@{username}"
        full_name = info.get("full_name")
        if full_name:
            return str(full_name)
        return str(uid)

    for change in flag_changes:
        recipients: Set[int] = set(admin_recipients) | set(heads)
        buyer_id = change.get("buyer_id")
        alias_lead_id = change.get("alias_lead_id")
        if buyer_id is not None:
            try:
                recipients.add(int(buyer_id))
            except Exception:
                pass
        if alias_lead_id is not None:
            try:
                recipients.add(int(alias_lead_id))
            except Exception:
                pass
        team_id: Optional[int] = None
        if buyer_id is not None:
            buyer_row = user_map.get(int(buyer_id))
            if buyer_row:
                tid = buyer_row.get("team_id")
                if tid is not None:
                    try:
                        team_id = int(tid)
                    except Exception:
                        team_id = None
        if team_id is not None:
            leads = await get_team_leads_cached(team_id)
            for lid in leads:
                try:
                    recipients.add(int(lid))
                except Exception:
                    continue
            mentors = await get_team_mentors_cached(team_id)
            for mid in mentors:
                try:
                    recipients.add(int(mid))
                except Exception:
                    continue
        recipients = {
            rid for rid in recipients
            if rid in env_admins
            or rid not in user_map
            or user_map.get(rid, {}).get("is_active", 1)
        }
        campaign = html.escape(change.get("campaign") or "—")
        old_label = html.escape(change.get("old") or "—")
        new_label = html.escape(change.get("new") or "—")
        alias_key = change.get("alias_key")
        alias_text = f"Алиас: {html.escape(alias_key)}" if alias_key else None
        day = change.get("day")
        day_text = None
        if day:
            try:
                day_text = f"Дата: {html.escape(day.isoformat())}"
            except Exception:
                day_text = None
        buyer_label = format_user_label(buyer_id)
        buyer_text = f"Байер: {html.escape(buyer_label)}" if buyer_label else None
        metrics = (
            f"Spend {_fmt_money(change.get('spend'))}, "
            f"FTD {change.get('ftd') or 0}, "
            f"Rev {_fmt_money(change.get('revenue'))}, "
            f"ROI {_fmt_percent(change.get('roi'))}"
        )
        extras: List[str] = []
        ctr_value = change.get("ctr")
        if ctr_value is not None:
            extras.append(f"CTR {_fmt_percent(ctr_value)}")
        ftd_rate_value = change.get("ftd_rate")
        if ftd_rate_value is not None:
            extras.append(f"FTD rate {_fmt_percent(ftd_rate_value)}")
        reason = change.get("reason") or ""
        parts = [f"<b>{campaign}</b>: {old_label} → {new_label}"]
        if day_text:
            parts.append(day_text)
        parts.append(metrics)
        if extras:
            parts.append("; ".join(extras))
        if buyer_text:
            parts.append(buyer_text)
        if alias_text:
            parts.append(alias_text)
        if reason:
            parts.append("Причина: " + html.escape(str(reason)))
        line = "\n".join(parts)
        for rid in recipients:
            recipient_messages[rid].append(line)

    if not recipient_messages:
        return
    header = f"<b>Обновления флагов</b> из {html.escape(filename)}"
    for rid, lines in recipient_messages.items():
        message_text = header + "\n\n" + "\n\n".join(lines)
        try:
            await bot.send_message(rid, message_text, parse_mode=ParseMode.HTML)
        except Exception as exc:
            logger.warning("Failed to send flag notification", user_id=rid, exc_info=exc)
@dp.message(CommandStart())
async def on_start(message: Message):
    await db.upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    # Автоповышение роли для ID из ADMINS
    if message.from_user.id in ADMIN_IDS:
        try:
            await db.set_user_role(message.from_user.id, "admin")
        except Exception:
            pass
    await message.answer("Привет! Ты зарегистрирован. Роль по умолчанию: buyer (если не админ). Админ может изменить роль и добавить правила.")

@dp.message(Command("help"))
async def on_help(message: Message):
    await message.answer(
        "Доступные команды:\n"
        "/start — регистрация\n"
        "/help — помощь\n"
        "/ping — проверка связи (pong)\n"
        "/whoami — показать свой Telegram ID\n"
        "/addrule — добавить правило (админ/хэд)\n"
        "/listusers — список пользователей (зависит от роли)\n"
        "/listroutes — список правил (видимость по роли)\n"
        "/setrole — назначить роль (admin)\n"
        "/createteam — создать команду (admin)\n"
        "/setteam — назначить пользователя в команду (admin/head)\n"
        "/listteams — список команд\n"
        "/aliases — алиасы (admin): связать campaign_name с buyer/lead\n"
        "/addmentor — назначить роль mentor (admin)\n"
        "/mentor_follow — подписать ментора на команду (admin)\n"
        "/mentor_unfollow — отписать ментора от команды (admin)"
    )

@dp.message(Command("ping"))
async def on_ping(message: Message):
    await message.answer("pong")

@dp.message(Command("whoami"))
async def on_whoami(message: Message):
    uid = message.from_user.id
    uname = message.from_user.username
    await message.answer(f"Ваш Telegram ID: <code>{uid}</code>\nUsername: @{uname or '-'}")
@dp.message(Command("listusers"))
async def on_list_users(message: Message):
    me = message.from_user.id
    users = await db.list_users()
    # role-based visibility: admin sees all; head sees all; lead sees their team; buyer sees only self
    visible = []
    # get my role and team
    my = next((u for u in users if u["telegram_id"] == me), None)
    my_role = my["role"] if my else "buyer"
    if me in ADMIN_IDS:
        my_role = "admin"
    lead_team_ids = await db.list_user_lead_teams(me) if my_role not in ("admin", "head") else []
    for u in users:
        if my_role in ("admin", "head"):
            visible.append(u)
        elif lead_team_ids:
            team_id = u.get("team_id")
            if team_id is not None and int(team_id) in lead_team_ids:
                visible.append(u)
        else:  # buyer
            if u["telegram_id"] == me:
                visible.append(u)
    if not visible:
        return await message.answer("Нет данных для отображения")
    rendered = []
    for u in visible:
        display_role = u['role']
        if u['telegram_id'] == me and me in ADMIN_IDS:
            display_role = 'admin'
        rendered.append(f"• <code>{u['telegram_id']}</code> @{u['username'] or '-'} — {u['full_name'] or ''} | role={display_role} | team={u['team_id'] or '-'}")
    lines = rendered
    await message.answer("Пользователи:\n" + "\n".join(lines))

def _user_row_controls(u: dict) -> InlineKeyboardMarkup:
    uid = u["telegram_id"]
    role = u["role"]
    is_active = u["is_active"]
    buttons = [
        [InlineKeyboardButton(text="buyer", callback_data=f"role:{uid}:buyer"),
         InlineKeyboardButton(text="lead", callback_data=f"role:{uid}:lead"),
         InlineKeyboardButton(text="head", callback_data=f"role:{uid}:head"),
         InlineKeyboardButton(text="admin", callback_data=f"role:{uid}:admin"),
         InlineKeyboardButton(text="mentor", callback_data=f"role:{uid}:mentor")],
        [InlineKeyboardButton(text=("Deactivate" if is_active else "Activate"), callback_data=f"active:{uid}:{0 if is_active else 1}")],
        [InlineKeyboardButton(text="Set team", callback_data=f"team:choose:{uid}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("manage"))
async def on_manage(message: Message):
    # Only admins (для MVP) видят управление
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    users = await db.list_users()
    if not users:
        return await message.answer("Пока нет пользователей, попросите нажать /start")
    # Покажем по одному пользователю за сообщение для наглядности
    for u in users[:25]:  # не спамим много
        text = f"<b>{u['full_name'] or '-'}</b> @{u['username'] or '-'}\nID: <code>{u['telegram_id']}</code>\nRole: <code>{u['role']}</code> | Team: <code>{u['team_id'] or '-'}</code> | Active: <code>{'yes' if u['is_active'] else 'no'}</code>"
        await message.answer(text, reply_markup=_user_row_controls(u))

def alias_row_controls(alias: str, buyer_id: int | None, lead_id: int | None) -> InlineKeyboardMarkup:
    a = alias
    buttons = [
        [InlineKeyboardButton(text=f"Set buyer ({buyer_id or '-'})", callback_data=f"alias:setbuyer:{a}")],
        [InlineKeyboardButton(text=f"Set lead ({lead_id or '-'})", callback_data=f"alias:setlead:{a}")],
        [InlineKeyboardButton(text="Delete", callback_data=f"alias:delete:{a}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("aliases"))
async def on_aliases(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    rows = await db.list_aliases()
    if not rows:
        await message.answer("Алиасов пока нет.")
    else:
        for r in rows[:25]:
            text = f"<b>{r['alias']}</b> → buyer={r['buyer_id'] or '-'} | lead={r['lead_id'] or '-'}"
            await message.answer(text, reply_markup=alias_row_controls(r['alias'], r['buyer_id'], r['lead_id']))
    # кнопка для создания нового алиаса
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Добавить алиас", callback_data="alias:new")]])
    await message.answer("Управление алиасами:", reply_markup=kb)


@dp.message(Command("checkdomain"))
async def on_checkdomain(message: Message):
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await db.set_pending_action(message.from_user.id, "domain:check", None)
        return await message.answer("Пришлите домен, например salongierpl.online")
    result = await _lookup_domains_text(parts[1])
    await message.answer(result + "\n\nОтправьте следующий домен или '-' чтобы завершить")

# ===== Mentors management (admin) =====
def _mentor_row_controls(mentor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подписки", callback_data=f"mentor:subs:{mentor_id}")],
        [InlineKeyboardButton(text="Снять роль", callback_data=f"mentor:unset:{mentor_id}")]
    ])

def _mentor_add_controls() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить ментора", callback_data="mentor:add")]
    ])

async def _send_mentors(chat_id: int, actor_id: int):
    if actor_id not in ADMIN_IDS:
        return await bot.send_message(chat_id, "Только для админов")
    users = await db.list_users()
    mentors = [u for u in users if u.get("role") == "mentor"]
    if not mentors:
        return await bot.send_message(chat_id, "Менторы:\nПока нет менторов.", reply_markup=_mentor_add_controls())
    await bot.send_message(chat_id, "Менторы:", reply_markup=_mentor_add_controls())
    for u in mentors[:25]:
        text = (
            f"<b>{u['full_name'] or '-'}</b> @{u['username'] or '-'}\n"
            f"ID: <code>{u['telegram_id']}</code>\n"
            f"Role: <code>{u['role']}</code> | Team: <code>{u['team_id'] or '-'}</code> | Active: <code>{'yes' if u['is_active'] else 'no'}</code>"
        )
        await bot.send_message(chat_id, text, reply_markup=_mentor_row_controls(int(u['telegram_id'])))

def _mentor_subs_keyboard(mentor_id: int, teams: list[dict], followed: set[int]) -> InlineKeyboardMarkup:
    rows = []
    for t in teams[:50]:
        tid = int(t['id'])
        mark = "✅" if tid in followed else "➕"
        rows.append([InlineKeyboardButton(text=f"{mark} #{tid} {t['name']}", callback_data=f"mentor:toggle:{mentor_id}:{tid}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="mentor:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "mentor:add")
async def cb_mentor_add(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "mentor:add", None)
    await call.message.answer("Пришлите Telegram ID или @username пользователя, которому назначить роль mentor")
    await call.answer()

@dp.callback_query(F.data.startswith("mentor:unset:"))
async def cb_mentor_unset(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, _, mid = call.data.split(":", 2)
    try:
        mid_i = int(mid)
        await db.set_user_role(mid_i, "buyer")
        await call.answer("Роль mentor снята")
    except Exception as e:
        logger.exception(e)
        await call.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("mentor:subs:"))
async def cb_mentor_subs(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, _, mid = call.data.split(":", 2)
    mid_i = int(mid)
    teams = await db.list_teams()
    followed = set(await db.list_mentor_teams(mid_i))
    kb = _mentor_subs_keyboard(mid_i, teams, followed)
    await call.message.answer(f"Подписки ментора <code>{mid_i}</code>:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data.startswith("mentor:toggle:"))
async def cb_mentor_toggle(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, _, mid, tid = call.data.split(":", 3)
    mid_i = int(mid)
    tid_i = int(tid)
    followed = set(await db.list_mentor_teams(mid_i))
    try:
        if tid_i in followed:
            await db.remove_mentor_team(mid_i, tid_i)
        else:
            await db.add_mentor_team(mid_i, tid_i)
    except Exception as e:
        logger.exception(e)
    teams = await db.list_teams()
    followed = set(await db.list_mentor_teams(mid_i))
    kb = _mentor_subs_keyboard(mid_i, teams, followed)
    try:
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        await call.message.answer(f"Подписки ментора <code>{mid_i}</code>:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "mentor:back")
async def cb_mentor_back(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await _send_mentors(call.message.chat.id, call.from_user.id)
    await call.answer()

@dp.message(Command("setalias"))
async def on_setalias(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /setalias <alias> buyer=<id|-> lead=<id|->
    parts = message.text.split()
    if len(parts) < 2:
        return await message.answer("Использование: /setalias <alias> buyer=<id|-> lead=<id|->")
    alias = parts[1]
    buyer_id = None
    lead_id = None
    for p in parts[2:]:
        if p.startswith("buyer="):
            v = p.split("=",1)[1]
            buyer_id = None if v == '-' else int(v)
        if p.startswith("lead="):
            v = p.split("=",1)[1]
            lead_id = None if v == '-' else int(v)
    await db.set_alias(alias, buyer_id, lead_id)
    await message.answer("Алиас сохранён")

@dp.message(Command("delalias"))
async def on_delalias(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("Использование: /delalias <alias>")
    await db.delete_alias(parts[1])
    await message.answer("Алиас удалён")

@dp.callback_query(F.data == "alias:new")
async def cb_alias_new(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    await db.set_pending_action(call.from_user.id, "alias:new", None)
    await call.message.answer("Введите имя алиаса (префикс campaign_name до _):")
    await call.answer()

@dp.callback_query(F.data.startswith("alias:setbuyer:"))
async def cb_alias_setbuyer(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.set_pending_action(call.from_user.id, f"alias:setbuyer:{alias}", None)
    await call.message.answer(f"Пришлите Telegram ID или @username покупателя для алиаса {alias}, или '-' чтобы убрать")
    await call.answer()

@dp.callback_query(F.data.startswith("alias:setlead:"))
async def cb_alias_setlead(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.set_pending_action(call.from_user.id, f"alias:setlead:{alias}", None)
    await call.message.answer(f"Пришлите Telegram ID или @username лида для алиаса {alias}, или '-' чтобы убрать")
    await call.answer()

@dp.callback_query(F.data.startswith("alias:delete:"))
async def cb_alias_delete(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    alias = call.data.split(":", 2)[2]
    await db.delete_alias(alias)
    await call.message.edit_text(f"Алиас {alias} удалён")
    await call.answer()

@dp.callback_query(F.data.startswith("team:choose:"))
async def cb_team_choose(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    uid = int(call.data.split(":", 2)[2])
    teams = await db.list_teams()
    buttons = []
    for t in teams[:50]:
        buttons.append([InlineKeyboardButton(text=f"#{t['id']} {t['name']}", callback_data=f"team:set:{uid}:{t['id']}")])
    buttons.append([InlineKeyboardButton(text="Удалить из команды", callback_data=f"team:set:{uid}:-")])
    await call.message.answer("Выберите команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()

@dp.callback_query(F.data.startswith("team:set:"))
async def cb_team_set(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, _, uid, team_raw = call.data.split(":", 3)
    team_id = None if team_raw == '-' else int(team_raw)
    await db.set_user_team(int(uid), team_id)
    await call.answer("Команда обновлена")

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
            result = await _lookup_domains_text(text)
            await message.answer(result + "\n\nОтправьте следующий домен или '-' чтобы завершить")
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

@dp.callback_query(F.data.startswith("role:"))
async def cb_set_role(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, uid, role = call.data.split(":", 2)
    await db.set_user_role(int(uid), role)
    u = await db.get_user(int(uid))
    if u and u.get("team_id") is not None:
        team_id = int(u.get("team_id"))
        if role == "mentor":
            await db.set_team_lead_override(team_id, int(uid))
        elif role != "lead":
            await db.clear_team_lead_override(team_id)
    if u:
        await call.message.edit_reply_markup(reply_markup=_user_row_controls(u))
        await call.answer("Роль обновлена")
    else:
        await call.answer("Пользователь не найден", show_alert=True)

@dp.callback_query(F.data.startswith("active:"))
async def cb_set_active(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет прав", show_alert=True)
    _, uid, active = call.data.split(":", 2)
    await db.set_user_active(int(uid), bool(int(active)))
    u = await db.get_user(int(uid))
    if u:
        await call.message.edit_reply_markup(reply_markup=_user_row_controls(u))
        await call.answer("Статус обновлен")
    else:
        await call.answer("Пользователь не найден", show_alert=True)

@dp.message(Command("listroutes"))
async def on_list_routes(message: Message):
    me = message.from_user.id
    users = await db.list_users()
    my = next((u for u in users if u["telegram_id"] == me), None)
    my_role = (my or {}).get("role", "buyer")
    if me in ADMIN_IDS:
        my_role = "admin"
    my_team = (my or {}).get("team_id")
    lead_team_ids = await db.list_user_lead_teams(me) if my_role not in ("admin", "head") else []
    rows = await db.list_routes()
    # filter by role
    def visible(r: dict) -> bool:
        if my_role in ("admin", "head"):
            return True
        if lead_team_ids:
            ru = next((u for u in users if u["telegram_id"] == r["user_id"]), None)
            if not ru:
                return False
            team_id = ru.get("team_id")
            return team_id is not None and int(team_id) in lead_team_ids
        # buyer: only own
        return r["user_id"] == me
    vis = [r for r in rows if visible(r)]
    if not vis:
        return await message.answer("Правил нет или нет доступа")
    def fmt(r):
        return f"#{r['id']} -> <code>{r['user_id']}</code> (@{r['username'] or '-'}) | offer={r['offer'] or '*'} | geo={r['country'] or '*'} | src={r['source'] or '*'} | prio={r['priority']}"
    await message.answer("Правила:\n" + "\n".join(fmt(r) for r in vis))

@dp.message(Command("addrule"))
async def on_add_rule(message: Message):
    # Разрешено admin/head. Format: /addrule user_id [offer=*] [country=*] [source=*] [priority=0]
    try:
        parts = message.text.split()
        if len(parts) < 2:
            raise ValueError
        user_id = int(parts[1])
        kwargs = {"offer": None, "country": None, "source": None, "priority": 0}
        for p in parts[2:]:
            if "=" in p:
                k, v = p.split("=", 1)
                if k in ("offer", "country", "source"):
                    kwargs[k] = None if v == "*" else v
                elif k == "priority":
                    kwargs["priority"] = int(v)
        # permissions
        users = await db.list_users()
        me = message.from_user.id
        my = next((u for u in users if u["telegram_id"] == me), None)
        my_role = (my or {}).get("role", "buyer")
        my_team = (my or {}).get("team_id")
        if my_role not in ("admin", "head") and me not in ADMIN_IDS:
            return await message.answer("Недостаточно прав (нужна роль admin/head)")
        if my_role == "head":
            target = next((u for u in users if u["telegram_id"] == user_id), None)
            if not target or target.get("team_id") != my_team:
                return await message.answer("Можно добавлять правила только для своей команды")
        rid = await db.add_route(user_id, kwargs["offer"], kwargs["country"], kwargs["source"], kwargs["priority"])
        await message.answer(f"OK, создано правило #{rid}")
    except Exception as e:
        logger.exception(e)
        await message.answer("Использование: /addrule <user_id> offer=OFF|* country=RU|* source=FB|* priority=0")

@dp.message(Command("setrole"))
async def on_set_role(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /setrole <telegram_id> <buyer|lead|head|admin|mentor>
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /setrole <telegram_id> <buyer|lead|head|admin|mentor>")
    try:
        uid = await _resolve_user_id(parts[1])
        role = parts[2]
        await db.set_user_role(uid, role)
        await message.answer("OK")
    except Exception as e:
        logger.exception(e)
        await message.answer("Ошибка установки роли")

@dp.message(Command("createteam"))
async def on_create_team(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Только для админов")
    # /createteam <name>
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        return await message.answer("Использование: /createteam <name>")
    team_id = await db.create_team(parts[1])
    await message.answer(f"Команда создана: id={team_id}")

@dp.message(Command("setteam"))
async def on_set_team(message: Message):
    # admin/head: назначить юзера в команду
    # /setteam <telegram_id> <team_id|-> (- означает убрать из команды)
    me = message.from_user.id
    if me not in ADMIN_IDS:
        return await message.answer("Только для админов")
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("Использование: /setteam <telegram_id> <team_id|->")
    try:
        uid = await _resolve_user_id(parts[1])
    except Exception:
        return await message.answer("Не удалось распознать пользователя. Используйте numeric ID или @username")
    team_raw = parts[2]
    team_id = None if team_raw == '-' else int(team_raw)
    await db.set_user_team(uid, team_id)
    await message.answer("OK")

@dp.message(Command("listteams"))
async def on_list_teams(message: Message):
    teams = await db.list_teams()
    if not teams:
        return await message.answer("Команд нет")
    lines = [f"#{t['id']} — {t['name']}" for t in teams]
    await message.answer("Команды:\n" + "\n".join(lines))

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
    max_items = 20
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
        if idx <= max_items:
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
    text = "\n".join(header_lines)
    if lines:
        text += "\n\n" + "\n".join(lines)
    if len(rows) > max_items:
        text += f"\n\nПоказаны первые {max_items} кампаний из {len(rows)}."
    await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)


async def _send_fb_account_report(chat_id: int, month_start: date) -> None:
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
    users = await db.list_users()
    user_ids = await _resolve_scope_user_ids(actor_id)
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
    agg = await db.aggregate_sales(user_ids, start, end, offer=filt.get('offer'), creative=filt.get('creative'), filter_user_ids=filter_user_ids)
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
                label = f"@{u['username']}" if u.get('username') else (u.get('full_name') or f"<code>{uid}</code>")
            lines.append(f"{label}: <b>{cnt}</b>")
        if lines:
            text += "\n\n" + "\n".join(lines)
    if days == 7 and not yesterday:
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
    await bot.send_message(chat_id, text, reply_markup=_reports_menu(actor_id))

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
    try:
        await call.message.answer("Готовлю отчёт…")
    except Exception:
        pass
    try:
        await _send_period_report(call.message.chat.id, call.from_user.id, "Сегодня", None, False)
    except Exception as e:
        logger.exception(e)
        await call.message.answer(f"Не удалось построить отчёт: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)
    finally:
        try:
            await call.answer()
        except Exception:
            pass

@dp.callback_query(F.data == "report:yesterday")
async def cb_report_yesterday(call: CallbackQuery):
    try:
        await call.message.answer("Готовлю отчёт…")
    except Exception:
        pass
    try:
        await _send_period_report(call.message.chat.id, call.from_user.id, "Вчера", None, True)
    except Exception as e:
        logger.exception(e)
        await call.message.answer(f"Не удалось построить отчёт: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)
    finally:
        try:
            await call.answer()
        except Exception:
            pass

@dp.callback_query(F.data == "report:week")
async def cb_report_week(call: CallbackQuery):
    try:
        await call.message.answer("Готовлю отчёт…")
    except Exception:
        pass
    try:
        await _send_period_report(call.message.chat.id, call.from_user.id, "Последние 7 дней", 7, False)
    except Exception as e:
        logger.exception(e)
        await call.message.answer(f"Не удалось построить отчёт: <code>{type(e).__name__}: {e}</code>", parse_mode=ParseMode.HTML)
    finally:
        try:
            await call.answer()
        except Exception:
            pass

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
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мои KPI", callback_data="kpi:mine")],
        [InlineKeyboardButton(text="Изменить дневной", callback_data="kpi:set:daily"), InlineKeyboardButton(text="Изменить недельный", callback_data="kpi:set:weekly")],
    ])

async def _send_kpi_menu(chat_id: int, actor_id: int):
    kpi = await db.get_kpi(actor_id)
    lines = ["KPI:"]
    lines.append(f"Дневной: <b>{kpi.get('daily_goal') or '-'}</b>")
    lines.append(f"Недельный: <b>{kpi.get('weekly_goal') or '-'}</b>")
    await bot.send_message(chat_id, "\n".join(lines), reply_markup=_kpi_menu())

@dp.callback_query(F.data == "kpi:mine")
async def cb_kpi_mine(call: CallbackQuery):
    await _send_kpi_menu(call.message.chat.id, call.from_user.id)
    await call.answer()

@dp.callback_query(F.data.startswith("kpi:set:"))
async def cb_kpi_set(call: CallbackQuery):
    _, _, which = call.data.split(":", 2)
    await db.set_pending_action(call.from_user.id, f"kpi:set:{which}", None)
    await call.message.answer("Пришлите целевое число депозитов (целое), либо '-' чтобы очистить")
    await call.answer()

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
