"""Formatting utilities for messages and data display."""

import html
from datetime import date
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

from .. import fb_csv

_FLAG_CODE_LABELS = {
    "GREEN": "🟢 Зелёный",
    "YELLOW": "🟡 Жёлтый",
    "RED": "🔴 Красный",
}

_FLAG_REASON_OVERRIDES = {
    "Spend ≥ $200 и FTD = 0": "🟥 Красный флаг",
    "CTR < 0.7%": "⚠️ Жёлтый флаг",
}

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


def fmt_money(value: Decimal | float | int | None) -> str:
    """Format money value as currency string."""
    if value is None:
        return "$0.00"
    amount = float(value)
    return f"${amount:,.2f}".replace(",", " ")


def fmt_percent(value: Decimal | float | None) -> str:
    """Format percentage value."""
    if value is None:
        return "—"
    return f"{float(value):.1f}%"


def month_label_ru(month: date) -> str:
    """Format month as Russian label."""
    name = _MONTH_NAMES_RU.get(month.month, month.strftime("%m"))
    return f"{name} {month.year}"


def as_decimal(value) -> Decimal:
    """Convert value to Decimal safely."""
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def format_flag_label(
    flag_id,
    flags_by_id: dict[int, dict[str, Any]],
) -> str:
    """Format flag label from flag ID."""
    if flag_id is None:
        return "—"
    try:
        fid = int(flag_id)
    except Exception:
        return str(flag_id)
    row = flags_by_id.get(fid)
    if not row:
        return str(fid)
    code = (row.get("code") or "").upper()
    if code and code in _FLAG_CODE_LABELS:
        return _FLAG_CODE_LABELS[code]
    title = row.get("title")
    if title:
        return str(title)
    return code or str(fid)


def format_flag_decision(decision: Optional[fb_csv.FlagDecision]) -> str:
    """Format flag decision as string."""
    if not decision:
        return "—"
    reasons = decision.reasons or []
    override_reason = next((reason for reason in reasons if reason in _FLAG_REASON_OVERRIDES), None)
    label = _FLAG_REASON_OVERRIDES.get(override_reason)
    if not label:
        label = _FLAG_CODE_LABELS.get((decision.code or "").upper(), decision.code)
    if reasons:
        return f"{label} ({'; '.join(reasons)})"
    return label


def format_buyer_label(buyer_id, users_by_id: dict[int, dict[str, Any]]) -> str:
    """Format buyer label from buyer ID."""
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


def chunk_lines(lines: List[str], limit: int = 3500) -> List[str]:
    """Split lines into chunks that fit within Telegram message limit."""
    if not lines:
        return [""]
    messages: List[str] = []
    current: List[str] = []
    current_len = 0
    for raw in lines:
        segment = raw or ""
        appended_len = len(segment) + 1
        if current and current_len + appended_len > limit:
            messages.append("\n".join(current))
            current = [segment]
            current_len = len(segment)
            continue
        if len(segment) > limit:
            if current:
                messages.append("\n".join(current))
                current = []
                current_len = 0
            for i in range(0, len(segment), limit):
                messages.append(segment[i : i + limit])
            continue
        current.append(segment)
        current_len += appended_len
    if current:
        messages.append("\n".join(current))
    return messages or [""]


async def send_long(bot: Any, chat_id: int, lines_or_text: str | Iterable[str], reply_markup: Any = None, *, limit: int = 3500) -> None:
    """Send text (or lines) split by lines into Telegram-sized chunks; reply_markup goes on the last chunk."""
    lines = lines_or_text.split("\n") if isinstance(lines_or_text, str) else list(lines_or_text)
    chunks = [chunk for chunk in chunk_lines(lines, limit) if chunk.strip()]
    for idx, chunk in enumerate(chunks):
        await bot.send_message(chat_id, chunk, reply_markup=reply_markup if idx == len(chunks) - 1 else None)
