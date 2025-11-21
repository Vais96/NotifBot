"""Text formatting helpers shared across bot features."""

from __future__ import annotations

import html
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

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


def chunk_lines(lines: List[str], limit: int = 3500) -> List[str]:
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


def fmt_money(value: Decimal | float | int | None) -> str:
    if value is None:
        return "$0.00"
    amount = float(value)
    return f"${amount:,.2f}".replace(",", " ")


def fmt_percent(value: Decimal | float | None) -> str:
    if value is None:
        return "—"
    return f"{float(value):.1f}%"


def month_label_ru(month: date) -> str:
    name = _MONTH_NAMES_RU.get(month.month, month.strftime("%m"))
    return f"{name} {month.year}"


def build_account_detail_messages(payload: Dict[str, Any]) -> List[str]:
    account_name = str(payload.get("account_name") or "Без кабинета")
    flag_label = str(payload.get("flag_label") or "—")
    spend_value = Decimal(str(payload.get("spend") or "0"))
    revenue_value = Decimal(str(payload.get("revenue") or "0"))
    roi_raw = payload.get("roi")
    roi_value = Decimal(str(roi_raw)) if roi_raw not in (None, "None") else None
    ftd_value = int(payload.get("ftd") or 0)
    campaign_count = int(payload.get("campaign_count") or 0)
    ctr_raw = payload.get("ctr")
    ctr_value = Decimal(str(ctr_raw)) if ctr_raw not in (None, "None") else None
    ftd_rate_raw = payload.get("ftd_rate")
    ftd_rate_value = Decimal(str(ftd_rate_raw)) if ftd_rate_raw not in (None, "None") else None
    lines: List[str] = []
    lines.append(f"<b>{html.escape(account_name)}</b>")
    lines.append("Флаг кабинета: " + html.escape(flag_label))
    lines.append(
        f"Spend {fmt_money(spend_value)} | Rev {fmt_money(revenue_value)} | ROI {fmt_percent(roi_value)} | FTD {ftd_value} | Кампаний {campaign_count}"
    )
    lines.append(f"CTR {fmt_percent(ctr_value)} | FTD rate {fmt_percent(ftd_rate_value)}")
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


__all__ = [
    "build_account_detail_messages",
    "chunk_lines",
    "fmt_money",
    "fmt_percent",
    "month_label_ru",
]
