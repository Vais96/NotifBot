"""Optional Cloudflare API automation for attaching short domains to the Worker proxy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import httpx
from loguru import logger

from ..config import secret, settings
from .comment_links import bot_cname_target

CLOUDFLARE_API_BASE = "https://api.cloudflare.com/client/v4"


class CloudflareProvisionError(RuntimeError):
    """Raised when Cloudflare API provisioning fails."""

    def __init__(self, message: str, *, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class CloudflareZoneNotFoundError(CloudflareProvisionError):
    """Raised when the domain does not exist in the configured Cloudflare account."""


@dataclass(frozen=True)
class CloudflareProvisionResult:
    domain: str
    zone_status: str
    dns_created: Tuple[str, ...]
    routes_created: Tuple[str, ...]
    name_servers: Tuple[str, ...]
    dns_skipped_no_perm: bool = False


def is_cloudflare_configured() -> bool:
    return bool(secret(settings.cloudflare_api_token).strip() and settings.cloudflare_worker_name.strip())


async def _cf_request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    token: str,
    *,
    params: Dict[str, Any] | None = None,
    json_body: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    url = f"{CLOUDFLARE_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = await client.request(method, url, headers=headers, params=params, json=json_body)
    except httpx.HTTPError as exc:
        logger.warning("Cloudflare API {} {} transport error: {}", method, path, exc)
        raise CloudflareProvisionError("Не удалось подключиться к Cloudflare API") from exc

    try:
        payload = resp.json()
    except Exception as exc:
        raise CloudflareProvisionError(f"Cloudflare API вернул HTTP {resp.status_code}") from exc

    if resp.status_code >= 400 or not payload.get("success"):
        errors = payload.get("errors") or []
        msg = "; ".join(str(e.get("message") or e) for e in errors if e) or f"HTTP {resp.status_code}"
        logger.warning("Cloudflare API {} {} failed: {}", method, path, msg)
        raise CloudflareProvisionError(msg, http_status=resp.status_code)
    return payload


async def provision_domain_in_cloudflare(domain: str) -> CloudflareProvisionResult:
    """Ensure proxied DNS records and Worker routes exist for domain and www.domain in Cloudflare."""
    token = secret(settings.cloudflare_api_token).strip()
    worker_name = settings.cloudflare_worker_name.strip()
    if not token or not worker_name:
        raise CloudflareProvisionError("Cloudflare API не настроен")

    norm = domain.strip().lower()
    timeout = httpx.Timeout(15.0, connect=8.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        zones_resp = await _cf_request(client, "GET", "/zones", token, params={"name": norm})
        zones = zones_resp.get("result") or []
        if not zones:
            raise CloudflareZoneNotFoundError(norm)

        zone = zones[0]
        zone_id = str(zone["id"])
        account_id = str(((zone.get("account") or {}) if isinstance(zone.get("account"), dict) else {}).get("id") or "")
        zone_status = str(zone.get("status") or "unknown")
        name_servers = tuple(str(ns) for ns in (zone.get("name_servers") or []) if ns)

        cname_target = bot_cname_target()
        dns_created: List[str] = []
        dns_skipped_no_perm = False
        try:
            for hostname in (norm, f"www.{norm}"):
                rec_resp = await _cf_request(
                    client,
                    "GET",
                    f"/zones/{zone_id}/dns_records",
                    token,
                    params={"name": hostname},
                )
                existing = [
                    r for r in (rec_resp.get("result") or [])
                    if str(r.get("type") or "").upper() in ("A", "AAAA", "CNAME")
                ]
                if existing:
                    continue
                body: Dict[str, Any] = (
                    {"type": "CNAME", "name": hostname, "content": cname_target, "proxied": True, "ttl": 1}
                    if cname_target
                    else {"type": "A", "name": hostname, "content": "192.0.2.1", "proxied": True, "ttl": 1}
                )
                await _cf_request(client, "POST", f"/zones/{zone_id}/dns_records", token, json_body=body)
                dns_created.append(hostname)
        except CloudflareProvisionError as exc:
            if exc.http_status in (401, 403):
                dns_skipped_no_perm = True
                logger.info("Cloudflare DNS skipped for {} (token lacks DNS:Edit permission)", norm)
            else:
                raise

        routes_created: List[str] = []
        try:
            routes_resp = await _cf_request(client, "GET", f"/zones/{zone_id}/workers/routes", token)
            existing_routes = {
                str(r.get("pattern") or "").lower(): str(r.get("script") or "")
                for r in (routes_resp.get("result") or [])
            }
            for pattern in (f"{norm}/*", f"www.{norm}/*"):
                if pattern in existing_routes:
                    continue
                await _cf_request(
                    client,
                    "POST",
                    f"/zones/{zone_id}/workers/routes",
                    token,
                    json_body={"pattern": pattern, "script": worker_name},
                )
                routes_created.append(pattern)
        except CloudflareProvisionError as route_exc:
            if route_exc.http_status in (401, 403) and account_id:
                # Fallback to Workers Custom Domains (requires Account -> Workers Scripts -> Edit)
                try:
                    dom_resp = await _cf_request(client, "GET", f"/accounts/{account_id}/workers/domains", token)
                    existing_doms = {
                        str(d.get("hostname") or "").lower()
                        for d in (dom_resp.get("result") or [])
                    }
                    for hostname in (norm, f"www.{norm}"):
                        if hostname in existing_doms:
                            continue
                        await _cf_request(
                            client,
                            "PUT",
                            f"/accounts/{account_id}/workers/domains",
                            token,
                            json_body={
                                "environment": "production",
                                "hostname": hostname,
                                "service": worker_name,
                                "zone_id": zone_id,
                            },
                        )
                        routes_created.append(hostname)
                    dns_skipped_no_perm = False
                except CloudflareProvisionError as dom_exc:
                    if dom_exc.http_status == 409:
                        raise CloudflareProvisionError(
                            "У токена нет прав Zone → Workers Routes → Edit и Zone → DNS → Edit "
                            "(либо удалите старые A/CNAME записи в DNS домена в Cloudflare, либо добавьте эти права токену)."
                        ) from dom_exc
                    raise
            else:
                raise

    return CloudflareProvisionResult(
        domain=norm,
        zone_status=zone_status,
        dns_created=tuple(dns_created),
        routes_created=tuple(routes_created),
        name_servers=name_servers,
        dns_skipped_no_perm=dns_skipped_no_perm,
    )
