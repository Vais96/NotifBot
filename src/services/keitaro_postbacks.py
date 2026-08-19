"""Pure helpers for validating, identifying, and formatting Keitaro postbacks."""

from __future__ import annotations

import hashlib
import html
from datetime import datetime, timezone
from typing import Any, Mapping


MEANINGFUL_KEYS = (
    "profit", "payout", "revenue", "conversion_revenue",
    "currency", "revenue_currency", "payout_currency",
    "offer_id", "offer.id", "offer_name", "offer.name", "offer",
    "subid", "sub_id", "clickid", "click_id", "sub_id_3", "subid3",
    "conversion_sale_time", "conversion.sale_time", "conversion_time",
    "campaign_name", "campaign.name", "campaign",
    "status", "conversion_status", "conversion.status", "status_name", "state", "action",
    "country", "geo", "source", "traffic_source_name", "traffic_source", "affiliate",
)

SALE_LIKE_STATUSES = frozenset(
    {"sale", "approved", "approve", "confirmed", "confirm", "purchase", "purchased", "paid", "success"}
)


def is_unexpanded_placeholder(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return stripped.startswith("{") and stripped.endswith("}")


def raw_status(data: Mapping[str, Any]) -> str:
    value = (
        data.get("status")
        or data.get("conversion_status")
        or data.get("conversion.status")
        or data.get("status_name")
        or data.get("state")
        or data.get("action")
        or ""
    )
    return str(value).lower()


def is_sale(data: Mapping[str, Any]) -> bool:
    return raw_status(data) in SALE_LIKE_STATUSES


def sale_postback_fingerprint(data: Mapping[str, Any]) -> str | None:
    """Build a stable SHA-256 idempotency key, or return None without a click/conversion ID."""
    conversion_id = (
        data.get("conversion_id")
        or data.get("conversion.id")
        or data.get("external_id")
        or data.get("external_conversion_id")
    )
    if conversion_id is not None:
        normalized_id = str(conversion_id).strip()
        if normalized_id and not is_unexpanded_placeholder(normalized_id):
            return _sha256(f"conv:{normalized_id}")

    click_id = (
        data.get("subid")
        or data.get("sub_id")
        or data.get("clickid")
        or data.get("click_id")
        or data.get("tid")
    )
    if click_id is None or not str(click_id).strip():
        return None

    # Trackers may correct payout or conversion time when retrying the same sale.
    # Without a stable conversion_id, SubID/click ID is the only reliable identity.
    return _sha256(f"click:{str(click_id).strip()}")


def has_meaningful_fields(data: Mapping[str, Any]) -> bool:
    for key in MEANINGFUL_KEYS:
        value = data.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized and not is_unexpanded_placeholder(normalized):
            return True
    return False


def build_notification_text(
    data: Mapping[str, Any],
    daily_count: int | None = None,
    kpi_daily_goal: int | None = None,
) -> str:
    payout = _clean(
        data.get("profit")
        or data.get("payout")
        or data.get("revenue")
        or data.get("conversion_revenue")
    )
    currency = _clean(
        data.get("currency") or data.get("revenue_currency") or data.get("payout_currency")
    )
    offer_id = _clean(data.get("offer_id") or data.get("offer.id"))
    offer_name = _clean(data.get("offer_name") or data.get("offer.name") or data.get("offer"))
    subid = _clean(
        data.get("subid") or data.get("sub_id") or data.get("clickid") or data.get("click_id")
    )
    sub_id_2 = _clean(data.get("sub_id_2") or data.get("subid2"))
    sub_id_3 = _clean(data.get("sub_id_3") or data.get("subid3"))
    sale_time = _clean(
        data.get("conversion_sale_time")
        or data.get("conversion.sale_time")
        or data.get("conversion_time")
    )
    campaign_name = _clean(
        data.get("campaign_name") or data.get("campaign.name") or data.get("campaign")
    )

    payout_text = _format_payout(payout)
    sale_time_text = _format_sale_time(sale_time)
    alias = None
    if campaign_name:
        alias = (str(campaign_name).split("_", 1)[0] or "").strip() or None

    lines = [
        f"👤 <b>БАЙЕР:</b> <code>{_html(alias)}</code>",
        f"🎯 <b>ОФФЕР:</b> <code>{_html(offer_id)} | {_html(offer_name)}</code>",
    ]
    if payout_text:
        lines.append(
            f"💰 <b>ПРОФИТ:</b> <code>{_html(payout_text, '')} {_html(currency, '')}</code>"
        )
    lines.append(f"🧩 <b>SubID:</b> <code>{_html(subid)}</code>")
    if campaign_name:
        lines.append(f"📣 <b>КАМПАНИЯ:</b> <code>{_html(campaign_name)}</code>")
    lines.append(f"🔢 <b>SubID3:</b> <code>{_html(sub_id_3)}</code>")
    if sub_id_2:
        lines.append(f"📌 <b>SubID2:</b> <code>{_html(sub_id_2)}</code>")
    if daily_count is not None:
        lines.append(f"📈 <b>ДЕПОЗИТОВ ЗА ДЕНЬ:</b> <code>{daily_count}</code>")
    if daily_count is not None and kpi_daily_goal is not None:
        lines.append(
            f"🎯 <b>Сегодня:</b> <code>{daily_count}/{kpi_daily_goal}</code> депозитов к цели"
        )
    if sale_time_text:
        lines.append(f"🕒 <b>КОНВЕРСИЯ:</b> <code>{_html(sale_time_text)}</code> (UTC +0)")
    return "\n".join(lines)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _clean(value: Any) -> Any:
    return None if is_unexpanded_placeholder(value) else value


def _html(value: Any, fallback: str = "-") -> str:
    if value is None or value == "":
        return fallback
    return html.escape(str(value), quote=False)


def _format_payout(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return str(int(round(float(str(value).replace(",", ".").strip()))))
    except (OverflowError, TypeError, ValueError):
        return str(value)


def _format_sale_time(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return _timestamp_to_text(float(value))

        normalized = str(value).strip()
        if normalized.isdigit():
            return _timestamp_to_text(float(normalized))

        try:
            parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
            if parsed.tzinfo:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.strftime("%Y-%m-%d / %H:%M")
        except ValueError:
            pass

        for date_format in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
        ):
            try:
                return datetime.strptime(normalized, date_format).strftime("%Y-%m-%d / %H:%M")
            except ValueError:
                continue
    except (OverflowError, OSError, TypeError, ValueError):
        pass
    return str(value)


def _timestamp_to_text(timestamp: float) -> str:
    if timestamp > 1e12:
        timestamp /= 1000.0
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d / %H:%M")


__all__ = [
    "MEANINGFUL_KEYS",
    "SALE_LIKE_STATUSES",
    "build_notification_text",
    "has_meaningful_fields",
    "is_sale",
    "is_unexpanded_placeholder",
    "raw_status",
    "sale_postback_fingerprint",
]
