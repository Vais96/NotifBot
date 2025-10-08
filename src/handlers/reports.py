from aiogram import F
from aiogram.enums.parse_mode import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from loguru import logger
from ..dispatcher import dp, bot, ADMIN_IDS
from .. import db

def _reports_menu(actor_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="Сегодня", callback_data="report:today"), InlineKeyboardButton(text="Вчера", callback_data="report:yesterday")],
        [InlineKeyboardButton(text="Неделя", callback_data="report:week")],
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
