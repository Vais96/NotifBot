"""Telegram message builders for Underdog notifications (HTML-escaped)."""

from __future__ import annotations
from datetime import date, timedelta
from typing import Any, Dict, List, Optional
from ..utils.html import safe
from .common import _normalize_handle


# Статусы заказов дизайна (для уведомлений)
ORDER_STATUS_TEXTS: Dict[int, str] = {
    0: "обработка",
    1: "выполнен",
    2: "в работе",
    3: "на правках",
    4: "отдано на апрув",
    5: "возвращено на доработку",
}


def _order_status_text(status_id: Any) -> str:
    if status_id is None:
        return "—"
    try:
        sid = int(status_id)
        return ORDER_STATUS_TEXTS.get(sid, "unknown")
    except (TypeError, ValueError):
        return "—"


def _build_order_message(order: Dict[str, Any]) -> str:
    order_id = order.get("id")
    name = order.get("name") or order.get("type") or "—"
    count = order.get("count") or 0
    total = order.get("total") or order.get("price") or "0"
    lines = [
        f"✅ Ваш заказ ID {order_id} выполнен",
        "",
        f"Название: {safe(name)}",
        f"Количество: {count}",
        f"Сумма: {safe(total)}",
    ]
    return "\n".join(str(line) for line in lines)


def _build_design_assignment_message(
    order: Dict[str, Any],
    designer_name: Optional[str] = None,
    assigned_to_display: Optional[str] = None,
    assigned_to_mention_html: Optional[str] = None,
) -> str:
    """Сообщение о постановке таска. assigned_to_mention_html: HTML с <a href=\"tg://user?id=...\">@user</a> (тег); assigned_to_display: текст, если тега нет; если оба None — «вас» (личное дизайнеру)."""
    order_id = order.get("id")
    name = order.get("name") or order.get("type") or "—"
    owner = order.get("owner") or {}
    owner_name = owner.get("name") or "—"
    if assigned_to_mention_html is not None:
        assign_line = f"Таск назначен на: {assigned_to_mention_html}"
    elif assigned_to_display is not None:
        assign_line = f"Таск назначен на: {safe(assigned_to_display)}"
    else:
        assign_line = "Таск назначен на: вас" + (f" ({safe(designer_name)})" if designer_name else "")
    lines = [
        "📐 Вам поставлен таск (дизайн/креатив)",
        "",
        assign_line,
        f"Заказчик: {safe(owner_name)}",
        "",
        f"Название: {safe(name)}",
        f"ID заказа: {order_id}",
        "Статус: обработка",
    ]
    return "\n".join(str(line) for line in lines)


def _format_duration_ru(td: timedelta) -> str:
    """Human-readable duration in Russian (e.g. 1д 03ч 12м)."""
    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        total_seconds = 0
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts: List[str] = []
    if days:
        parts.append(f"{days}д")
    if hours or (days and minutes):
        parts.append(f"{hours:02d}ч")
    parts.append(f"{minutes:02d}м")
    return " ".join(parts)


def _build_design_completion_message(
    order: Dict[str, Any],
    *,
    duration_text: Optional[str] = None,
) -> str:
    order_id = order.get("id")
    name = order.get("name") or order.get("type") or "—"
    count = order.get("count") or 0
    total = order.get("total") or order.get("price") or "0"
    status_text = _order_status_text(order.get("status_id"))
    lines = [
        "✅ Заказ дизайна выполнен",
        "",
        f"ID заказа: {order_id}",
        f"Название: {safe(name)}",
        f"Количество: {count}",
        f"Сумма: {safe(total)}",
        f"Статус: {status_text}",
    ]
    if duration_text:
        lines += [f"Время выполнения: {duration_text}"]
    return "\n".join(str(line) for line in lines)


def _build_design_sla_warning_message(
    order: Dict[str, Any],
    *,
    passed_text: str,
) -> str:
    order_id = order.get("id")
    name = order.get("name") or order.get("type") or "—"
    status_text = _order_status_text(order.get("status_id"))
    return "\n".join(
        [
            "⏳ Срок выполнения: почти 24 часа уже прошло, а статус не стал “выполнен”.",
            "",
            f"ID заказа: {order_id}",
            f"Название: {safe(name)}",
            f"Статус: {status_text}",
            f"Прошло с назначения: {passed_text}",
        ]
    )


def _build_design_not_in_progress_48h_message(
    order: Dict[str, Any],
    *,
    passed_text: str,
    reminder_hours: int = 48,
) -> str:
    order_id = order.get("id")
    name = order.get("name") or order.get("type") or "—"
    status_text = _order_status_text(order.get("status_id"))
    return "\n".join(
        [
            f"⚠️ ОБНОВИТЕ СТАТУС ЗАДАЧИ #{order_id}",
            "",
            f"Прошло {reminder_hours} часов после назначения, а задача ещё не взята в работу.",
            f"Название: {safe(name)}",
            f"Текущий статус: {status_text}",
            f"Прошло с назначения: {passed_text}",
            "",
            "Пожалуйста, переведите таск в статус «в работе» в Underdog.",
        ]
    )


def _build_domain_notification(entries: List[Dict[str, Any]]) -> str:
    sorted_entries = sorted(entries, key=lambda item: item.get("expires_at") or date.max)
    lines = ["Срок действия следующих доменов скоро истекает:", ""]
    for entry in sorted_entries:
        domain = entry["raw"].get("domain") or entry["raw"].get("name") or "—"
        expires_at = entry.get("expires_at")
        expires_text = expires_at.strftime("%d.%m.%Y") if expires_at else "неизвестно"
        lines.append(f"- {safe(domain)} (истекает {expires_text})")
    lines.extend(
        [
            "",
            "Для продления домена напишите в личку @PolinaUnderdog",
        ]
    )
    return "\n".join(lines)


def _build_ip_notification(entries: List[Dict[str, Any]]) -> str:
    sorted_entries = sorted(
        entries,
        key=lambda item: item.get("expires_at") or date.max,
    )
    first_entry = sorted_entries[0] if sorted_entries else {}
    owner_name = (first_entry.get("owner_name") or "").strip()
    owner_tg = first_entry.get("display_handle") or ""
    if owner_tg and isinstance(owner_tg, str) and not owner_tg.startswith("@"):
        normalized_owner = _normalize_handle(owner_tg)
        if normalized_owner:
            owner_tg = f"@{normalized_owner}"

    lines: List[str] = ["⏳ Срок действия следующих IP скоро истекает:"]
    if owner_name or owner_tg:
        lines.append("")
        if owner_name:
            lines.append(f"👤 Пользователь: {safe(owner_name)}")
        if owner_tg:
            lines.append(f"TG: {safe(owner_tg)}")
    lines.append("")
    for entry in sorted_entries:
        ip_value = entry["raw"].get("ip") or entry["raw"].get("address") or "—"
        expires_at = entry.get("expires_at")
        days_left = entry.get("days_left")
        if expires_at:
            expires_text = expires_at.strftime("%d.%m.%Y")
        else:
            expires_text = "неизвестно"
        if isinstance(days_left, int):
            if days_left >= 0:
                suffix = f" (осталось {days_left} д.)"
            else:
                suffix = f" (просрочено {abs(days_left)} д.)"
        else:
            suffix = ""
        owner_display = entry.get("display_handle") or entry.get("owner_name") or "—"
        if owner_display and isinstance(owner_display, str) and not owner_display.startswith("@"):
            normalized_owner = _normalize_handle(owner_display)
            if normalized_owner:
                owner_display = f"@{normalized_owner}"
        lines.extend(
            [
                f"🖥 IP: {safe(ip_value)}",
                "",
                f"📅 Истекает: {expires_text}{suffix}",
                "",
                f"👤 Владелец: {safe(owner_display)}",
                "",
                "──────────────────",
                "",
            ]
        )
    lines.extend(
        [
            "Продлить ip: https://dashboard.underdog.click/managers/ips",
        ]
    )
    return "\n".join(lines).rstrip()


def _get_ticket_type_name(ticket_type: Optional[str]) -> str:
    """Расшифровка типов тикетов."""
    type_map = {
        "transfer_accounts": "Перенос аккаунтов",
        "account_errors": "Ошибки аккаунтов",
        "withdraw_funds": "Вывод средств",
        "topup_nachonacho": "Пополнение Nacho",
        "proxy_issues": "Проблемы с прокси",
        "general_question": "Общий вопрос",
    }
    if not ticket_type:
        return "Неизвестный тип"
    return type_map.get(ticket_type, ticket_type)


def _build_ticket_notification(entries: List[Dict[str, Any]]) -> str:
    """Формирует сообщение о завершенных тикетах."""
    if len(entries) == 1:
        ticket = entries[0]["raw"]
        ticket_id = ticket.get("id") or "—"
        ticket_type = ticket.get("type") or ticket.get("ticket_type")
        type_name = _get_ticket_type_name(ticket_type)
        lines: List[str] = [
            f"✅ Ваш тикет ({safe(ticket_id)}) выполнен:",
            "",
            f"📋 Тип: {safe(type_name)}",
        ]
    else:
        lines: List[str] = [f"✅ Выполнено тикетов: {len(entries)}", ""]
        for entry in entries:
            ticket = entry["raw"]
            ticket_id = ticket.get("id") or "—"
            ticket_type = ticket.get("type") or ticket.get("ticket_type")
            type_name = _get_ticket_type_name(ticket_type)
            lines.extend(
                [
                    f"✅ Ваш тикет ({safe(ticket_id)}) выполнен:",
                    "",
                    f"📋 Тип: {safe(type_name)}",
                    "",
                    "──────────────────",
                    "",
                ]
            )
    
    return "\n".join(lines).rstrip()
