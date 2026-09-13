"""Ads Workspace key issuance for NotifBot users."""

from __future__ import annotations

import html

import httpx
from loguru import logger

from ..config import settings

ADS_KEY_ROLES = frozenset({"buyer", "lead", "mentor", "head", "admin"})


class AdsWorkspaceError(RuntimeError):
    def __init__(self, code: str, *, http_status: int | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.http_status = http_status


def can_use_ads_key(is_admin: bool, role: str | None, is_active: bool = True) -> bool:
    if not is_active:
        return False
    if is_admin:
        return True
    return (role or "") in ADS_KEY_ROLES


def ads_key_label(user: dict | None, username: str | None) -> str:
    if user:
        name = str(user.get("full_name") or "").strip()
        if name:
            return name[:80]
        handle = str(user.get("username") or "").strip().lstrip("@")
        if handle:
            return handle[:80]
    handle = (username or "").strip().lstrip("@")
    return handle[:80] if handle else "telegram"


def format_ads_key_message(payload: dict) -> str:
    status = payload.get("status")
    code = str(payload.get("code") or "")
    how = (
        "Как связать профили:\n"
        "1. Открой профиль в AdsPower\n"
        "2. Открой Ads Workspace\n"
        "3. Вставь этот код\n"
        "4. В следующем профиле вставь <b>тот же код</b>\n\n"
        "Один код — на все свои профили. Второй код не нужен."
    )
    if status == "invite" or (status == "active" and code):
        return (
            "<b>Код для Ads Workspace</b>\n\n"
            f"Код: <code>{html.escape(code)}</code>\n\n"
            f"{how}"
        )
    if status == "active":
        last4 = str(payload.get("last4") or "????")
        return (
            "<b>Код для Ads Workspace</b>\n\n"
            f"Код уже активирован в одном профиле (····{html.escape(last4)}).\n\n"
            "Открой этот профиль на минуту, потом снова нажми «Мой ключ для АДС» — "
            "придёт тот же код для остальных профилей.\n\n"
            "«Выдать новый код» нужно только если потерял все профили."
        )
    return "Не удалось получить код. Напиши администратору."


async def request_ads_key(telegram_id: int, label: str, *, rotate: bool = False) -> dict:
    base = (settings.ads_workspace_api_url or "").rstrip("/")
    token = settings.ads_workspace_token or ""
    if not base or not token:
        raise AdsWorkspaceError("not_configured")
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=10.0)) as client:
            response = await client.post(
                f"{base}/internal/ads-key",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"telegramId": telegram_id, "label": label, "rotate": rotate},
            )
    except httpx.HTTPError as exc:
        logger.warning("Ads Workspace key request failed: {}", exc)
        raise AdsWorkspaceError("unreachable") from exc
    if response.status_code >= 400:
        logger.warning("Ads Workspace key HTTP {}: {}", response.status_code, response.text[:240])
        raise AdsWorkspaceError("http_error", http_status=response.status_code)
    payload = response.json()
    if not payload.get("ok"):
        raise AdsWorkspaceError(str(payload.get("error") or "bad_response"))
    return payload
