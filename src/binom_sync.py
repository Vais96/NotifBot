"""Binom v2 campaigns → local `keitaro_campaigns` cache for /checkdomain (since 21.10.2026 campaigns live in Binom)."""

import asyncio
from typing import Any, Dict, List

import httpx
from loguru import logger

from .config import secret, settings
from . import db
from .constants import BINOM_ID_OFFSET
from .keitaro import parse_campaign_name

_sync_lock = asyncio.Lock()


def extract_campaign_list(payload: Any) -> List[Dict[str, Any]]:
    """`/campaign/short/info` returns a bare list; tolerate a wrapped one too."""
    if isinstance(payload, dict):
        payload = next((payload[key] for key in ("items", "campaigns", "rows", "data") if isinstance(payload.get(key), list)), None)
    if not isinstance(payload, list):
        raise ValueError("Unexpected response from Binom: expected list")
    return [row for row in payload if isinstance(row, dict)]


async def _fetch_all_campaigns() -> List[Dict[str, Any]]:
    url = f"{settings.binom_base_url.rstrip('/')}/public/api/v1/campaign/short/info"
    headers = {"accept": "application/json", "api-key": secret(settings.binom_api_key)}
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=60.0)) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return extract_campaign_list(resp.json())


def _prepare_rows(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    for row in raw_rows:
        try:
            cid = int(row.get("id"))
        except Exception:
            continue
        name = str(row.get("name") or "").strip()
        meta = parse_campaign_name(name)
        src = meta.get("source_domain") or None
        dst = meta.get("target_domain") or None
        if not src and not dst:
            continue
        prepared.append(
            {
                "id": BINOM_ID_OFFSET + cid,
                "name": name,
                "prefix": meta.get("prefix"),
                "alias_key": meta.get("alias_key"),
                "source_domain": src,
                "target_domain": dst,
            }
        )
    return prepared


async def sync_campaigns() -> int:
    if not settings.binom_api_key or not settings.binom_base_url:
        raise RuntimeError("Binom credentials are not configured")
    async with _sync_lock:
        logger.info("Fetching campaigns from Binom")
        raw = await _fetch_all_campaigns()
        prepared = _prepare_rows(raw)
        logger.info("Fetched {} campaigns, {} with domains", len(raw), len(prepared))
        await db.upsert_keitaro_campaigns(prepared)
        return len(prepared)


async def amain() -> None:
    try:
        count = await sync_campaigns()
        logger.info("Campaign cache updated, count={}", count)
    finally:
        await db.close_pool()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
