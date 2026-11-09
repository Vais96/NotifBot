"""Остаток капы оффера из админки underD для уведомления о депозите.

Админка считает остаток сама (`GET /api/v1/offers/:id/cap`, ключ с разделом `offers_read`): капа и её
период живут там. `clickId` нужен, чтобы деп, который синк админки ещё не записал, был учтён сверху.
"""

from __future__ import annotations

import html
from typing import Any

import httpx
from loguru import logger

from ..config import secret, settings

# «Капа заканчивается» байеру — при остатке не больше этого (как CAP_LOW_THRESHOLD в админке).
CAP_LOW_THRESHOLD = 6
# Уведомление о депе не должно ждать админку.
_TIMEOUT = httpx.Timeout(5.0, connect=3.0)
_PERIOD_SUFFIX = {"day": "/день", "fixed": " фикс."}


async def fetch_cap_state(offer_id: Any, click_id: Any = None) -> dict | None:
    """Остаток капы или None (капы нет, оффер не найден, админка недоступна) — тогда строки просто нет."""
    oid = str(offer_id or "").strip()
    if not oid.isdigit() or not settings.new_admin_api_url or not settings.new_admin_api_key:
        return None
    url = f"{settings.new_admin_api_url.rstrip('/')}/v1/offers/{oid}/cap"
    params = {"clickId": str(click_id).strip()} if click_id and str(click_id).strip() else None
    headers = {"Accept": "application/json", "X-API-Key": secret(settings.new_admin_api_key)}
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            response = await client.get(url, params=params, headers=headers)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json().get("data")
    except (httpx.HTTPError, ValueError, AttributeError) as exc:
        logger.warning("Offer cap lookup failed: offer={} error={}", oid, exc)
        return None
    if not isinstance(data, dict) or data.get("left") is None or data.get("cap") is None:
        return None
    return data


def cap_text(state: dict) -> str:
    """«6 из 30/день»."""
    return f"{int(state['left'])} из {int(state['cap'])}{_PERIOD_SUFFIX.get(state.get('period'), '')}"


def cap_line(state: dict | None) -> str | None:
    if not state:
        return None
    return f"🚦 <b>ОСТАЛОСЬ КАПЫ:</b> <code>{cap_text(state)}</code>"


def is_cap_low(state: dict | None) -> bool:
    return bool(state) and int(state["left"]) <= CAP_LOW_THRESHOLD


def campaign_prefixes(state: dict) -> set[str]:
    """Алиасы кампаний, ливших оффер за сутки: префикс имени до «_», как в маршрутизации депа."""
    names = state.get("campaigns") or []
    return {p for p in (str(n).split("_", 1)[0].strip().lower() for n in names) if p}


def cap_low_key(state: dict, today: str) -> str:
    """Одно предупреждение на оффер и окно капы; подняли капу — новое окно, предупредим снова."""
    window = {"day": today, "month": today[:7]}.get(state.get("period"), "fixed")
    return f"{state['offerId']}:{window}:{state['cap']}"


def cap_low_text(state: dict) -> str:
    name = html.escape(str(state.get("name") or state.get("offerId")), quote=False)
    left = int(state["left"])
    head = "🛑 <b>Капа закончилась</b>" if left == 0 else "⚠️ <b>Капа заканчивается</b>"
    return "\n".join([
        head,
        "",
        f"🎯 <b>ОФФЕР:</b> <code>{state['offerId']} | {name}</code>",
        f"🚦 <b>ОСТАЛОСЬ КАПЫ:</b> <code>{cap_text(state)}</code>",
        "",
        "Переключайтесь заранее на другие офферы.",
    ])
