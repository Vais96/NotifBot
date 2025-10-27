import asyncio
from typing import Any, Dict, List

import httpx
from loguru import logger

from .config import settings
from . import db
from .keitaro import parse_campaign_name

PAGE_LIMIT = 200
REQUEST_PAUSE = 0.2


def _build_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "Api-Key": settings.keitaro_api_key,
    }


def _build_base_url() -> str:
    base = settings.keitaro_base_url.rstrip("/")
    return f"{base}/campaigns"


async def _fetch_all_campaigns() -> List[Dict[str, Any]]:
    url = _build_base_url()
    headers = _build_headers()
    offset = 0
    collected: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=30.0)) as client:
        while True:
            params = {"limit": PAGE_LIMIT, "offset": offset}
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list):
                raise ValueError("Unexpected response from Keitaro: expected list")
            if not payload:
                break
            collected.extend(payload)
            logger.debug("Keitaro page fetched", offset=offset, count=len(payload))
            if len(payload) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT
            await asyncio.sleep(REQUEST_PAUSE)
    return collected


def _prepare_rows(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    for row in raw_rows:
        try:
            cid = int(row.get("id"))
        except Exception:
            continue
        name = str(row.get("name") or "").strip()
        meta = parse_campaign_name(name)
        src = (meta.get("source_domain") or None)
        dst = (meta.get("target_domain") or None)
        if not src and not dst:
            continue
        prepared.append(
            {
                "id": cid,
                "name": name,
                "prefix": meta.get("prefix"),
                "alias_key": meta.get("alias_key"),
                "source_domain": src,
                "target_domain": dst,
            }
        )
    return prepared


async def sync_campaigns() -> int:
    if not settings.keitaro_api_key or not settings.keitaro_base_url:
        raise RuntimeError("Keitaro credentials are not configured")
    logger.info("Fetching campaigns from Keitaro")
    raw = await _fetch_all_campaigns()
    prepared = _prepare_rows(raw)
    logger.info("Fetched %s campaigns, %s with domains", len(raw), len(prepared))
    await db.replace_keitaro_campaigns(prepared)
    return len(prepared)


async def amain() -> None:
    try:
        count = await sync_campaigns()
        logger.info("Campaign cache updated", count=count)
    finally:
        await db.close_pool()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
