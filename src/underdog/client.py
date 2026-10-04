"""Underdog HTTP API client (auth token cache, retries, typed fetch/mark endpoints)."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence
import httpx
from loguru import logger
from ..config import secret, settings
from .common import UnderdogAPIError, UnderdogAuthError, _extract_domains, _extract_ips, _extract_items, _extract_tickets, _log_underdog_raw_json_response


LOGIN_PATH = "/api/login"


ORDERS_PATH = "/api/v2/orders"


DOMAINS_PATH = "/api/v2/domains"


IPS_PATH = "/api/v2/ip"


TICKETS_PATH = "/api/v2/tickets"


DEFAULT_TIMEOUT = (30.0, 15.0)


REQUEST_ATTEMPTS = 3


REQUEST_BACKOFF_SECONDS = 1.0


@dataclass(slots=True)
class _TokenCache:
    token: str
    expires_at: datetime


def _extract_token(payload: Dict[str, Any]) -> str:
    """Try multiple common response shapes to extract the auth token."""
    if not payload:
        raise UnderdogAuthError("Empty response while logging into Underdog admin")
    token = (
        payload.get("token")
        or payload.get("access_token")
        or payload.get("accessToken")
    )
    if token:
        return str(token)
    data = payload.get("data")
    if isinstance(data, dict):
        nested_token = (
            data.get("token")
            or data.get("access_token")
            or data.get("accessToken")
        )
        if nested_token:
            return str(nested_token)
    raise UnderdogAuthError("Auth response did not contain a token")


@dataclass(slots=True)
class UnderdogClient:
    base_url: str
    email: str
    password: str
    token_ttl: int = 3600
    timeout: httpx.Timeout = field(
        default_factory=lambda: httpx.Timeout(DEFAULT_TIMEOUT[0], connect=DEFAULT_TIMEOUT[1])
    )
    client: Optional[httpx.AsyncClient] = None
    _token_cache: Optional[_TokenCache] = field(default=None, init=False)
    _owns_client: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=self.timeout)
            self._owns_client = True

    async def __aenter__(self) -> "UnderdogClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    async def close(self) -> None:
        if self._owns_client and self.client is not None:
            await self.client.aclose()
            self.client = None
            self._owns_client = False

    def _build_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{path}"

    @staticmethod
    def _default_headers() -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def _refresh_token(self) -> str:
        if not self.email or not self.password:
            raise UnderdogAuthError("UNDERDOG_EMAIL or UNDERDOG_PASSWORD is not configured")
        assert self.client is not None, "HTTP client is not initialized"

        url = self._build_url(LOGIN_PATH)
        payload = {"email": self.email, "password": self.password}
        logger.debug("Logging into Underdog", url=url, email=self.email)
        resp = await self.client.post(url, json=payload, headers=self._default_headers())
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise UnderdogAuthError(f"Login failed with status {exc.response.status_code}") from exc
        try:
            data = resp.json()
        except ValueError as exc:  # pragma: no cover
            raise UnderdogAuthError("Failed to decode JSON from Underdog login response") from exc
        token = _extract_token(data)
        logger.info("Received Underdog token", length=len(token))
        ttl = max(1, int(self.token_ttl))
        self._token_cache = _TokenCache(
            token=token,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl),
        )
        return token

    async def _ensure_token(self, force_refresh: bool = False) -> str:
        if not force_refresh and self._token_cache and datetime.now(timezone.utc) < self._token_cache.expires_at:
            logger.debug(
                "Using cached Underdog token",
                expires_at=self._token_cache.expires_at.isoformat(),
            )
            return self._token_cache.token
        return await self._refresh_token()

    async def get_token(self, *, force_refresh: bool = False) -> str:
        return await self._ensure_token(force_refresh=force_refresh)

    async def _send(
        self, method: str, url: str, params: Optional[Dict[str, Any]], json_body: Optional[Dict[str, Any]]
    ) -> httpx.Response:
        assert self.client is not None, "HTTP client is not initialized"
        headers = self._default_headers()
        headers["Authorization"] = f"Bearer {await self._ensure_token()}"
        resp = await self.client.request(method, url, params=params, json=json_body, headers=headers)
        if resp.status_code == 401:
            logger.warning("Underdog API unauthorized, refreshing token")
            headers["Authorization"] = f"Bearer {await self._ensure_token(force_refresh=True)}"
            resp = await self.client.request(method, url, params=params, json=json_body, headers=headers)
        return resp

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> httpx.Response:
        """Retries 5xx / 429 / transport errors with exponential backoff; all failures raise UnderdogAPIError."""
        url = self._build_url(path)
        for attempt in range(1, REQUEST_ATTEMPTS + 1):
            try:
                resp = await self._send(method, url, params, json_body)
            except httpx.TransportError as exc:
                if attempt == REQUEST_ATTEMPTS:
                    raise UnderdogAPIError(f"Request {method} {path} failed: {type(exc).__name__}: {exc}") from exc
                logger.warning("Underdog {} {} transport error (attempt {}): {}", method, path, attempt, exc)
            else:
                retryable = resp.status_code == 429 or resp.status_code >= 500
                if not retryable or attempt == REQUEST_ATTEMPTS:
                    break
                logger.warning("Underdog {} {} -> {} (attempt {}), retrying", method, path, resp.status_code, attempt)
            await asyncio.sleep(REQUEST_BACKOFF_SECONDS * 2 ** (attempt - 1))

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            body = resp.text or ""
            if len(body) > 500:
                body = body[:500] + "..."
            raise UnderdogAPIError(
                f"Request {method} {path} failed with status {resp.status_code}: {body}"
            ) from exc
        return resp

    async def fetch_orders_by_type(
        self,
        order_type: str,
        *,
        order_status: Optional[int] = 1,
    ) -> List[Dict[str, Any]]:
        """Fetch orders from /api/v2/orders?type=<order_type>[&order_status=<order_status>]."""
        params: Dict[str, Any] = {"type": order_type}
        if order_status is not None:
            params["order_status"] = order_status
        logger.info("Fetching Underdog orders by type", params=params)
        resp = await self.request("GET", ORDERS_PATH, params=params)
        orders = _extract_items(resp.json())
        logger.info("Received orders", type=order_type, count=len(orders))
        return orders


    ORDER_TYPES_ORDERS_BOT = ("domain", "transferDomain")
    ORDER_TYPES_DESIGN_BOT = ("pwaDesign", "creative")

    async def fetch_orders_for_orders_bot(self) -> List[Dict[str, Any]]:
        """Fetch domain + transferDomain orders (for orders bot). Только с telegram_sent=0."""
        all_orders: List[Dict[str, Any]] = []
        seen_ids: set = set()
        for order_type in self.ORDER_TYPES_ORDERS_BOT:
            orders = await self.fetch_orders_by_type(order_type, order_status=1)
            for o in orders:
                if int(o.get("telegram_sent", 1)) != 0:
                    continue
                oid = o.get("id")
                if oid is not None and oid not in seen_ids:
                    seen_ids.add(oid)
                    all_orders.append(o)
        return all_orders

    async def fetch_orders_for_design_bot(self) -> List[Dict[str, Any]]:
        """Fetch pwaDesign + creative orders (for design bot). Только с telegram_sent=0."""
        all_orders = []
        seen_ids = set()
        for order_type in self.ORDER_TYPES_DESIGN_BOT:
            orders = await self.fetch_orders_by_type(order_type, order_status=1)
            for o in orders:
                if int(o.get("telegram_sent", 1)) != 0:
                    continue
                oid = o.get("id")
                if oid is not None and oid not in seen_ids:
                    seen_ids.add(oid)
                    all_orders.append(o)
        return all_orders

    async def fetch_design_new_tasks(self) -> List[Dict[str, Any]]:
        """Fetch pwaDesign + creative orders with order_status=0 (новый таск, обработка)."""
        all_orders = []
        seen_ids = set()
        for order_type in self.ORDER_TYPES_DESIGN_BOT:
            orders = await self.fetch_orders_by_type(order_type, order_status=0)
            for o in orders:
                oid = o.get("id")
                if oid is not None and oid not in seen_ids:
                    seen_ids.add(oid)
                    all_orders.append(o)
        return all_orders

    async def fetch_design_orders_by_status(self, *, order_status: int) -> List[Dict[str, Any]]:
        """
        Fetch pwaDesign + creative orders for a given order_status.
        Unlike fetch_orders_for_design_bot()/fetch_design_new_tasks(), this method DOESN'T filter by telegram_sent.
        """
        all_orders: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()
        for order_type in self.ORDER_TYPES_DESIGN_BOT:
            orders = await self.fetch_orders_by_type(order_type, order_status=order_status)
            for o in orders:
                oid = o.get("id")
                if oid is None:
                    continue
                if oid in seen_ids:
                    continue
                seen_ids.add(oid)
                all_orders.append(o)
        return all_orders

    async def fetch_design_orders_for_statuses(self, statuses: Sequence[int]) -> List[Dict[str, Any]]:
        """Fetch design orders for multiple order_status values and deduplicate by id."""
        all_orders: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()
        for sid in statuses:
            chunk = await self.fetch_design_orders_by_status(order_status=int(sid))
            for o in chunk:
                oid = o.get("id")
                if oid is None:
                    continue
                oid_i = int(oid)
                if oid_i in seen_ids:
                    continue
                seen_ids.add(oid_i)
                all_orders.append(o)
        return all_orders

    async def mark_order_telegram_sent(self, order_id: int) -> None:
        path = f"{ORDERS_PATH}/{order_id}/telegram-sent"
        await self.request("PATCH", path)

    async def fetch_domains(self) -> List[Dict[str, Any]]:
        resp = await self.request("GET", DOMAINS_PATH)
        return _extract_domains(resp.json())

    async def mark_domain_telegram_sent(self, domain_id: int) -> None:
        suffixes = ["telegram-sent", "telegram-notified"]
        last_error: Optional[Exception] = None
        for suffix in suffixes:
            path = f"{DOMAINS_PATH}/{domain_id}/{suffix}"
            try:
                await self.request("PATCH", path)
                return
            except UnderdogAPIError as exc:
                last_error = exc
        if last_error:
            raise last_error

    async def fetch_ips(self, *, log_raw: bool = False) -> List[Dict[str, Any]]:
        """log_raw=True — один раз в лог полный JSON (только для ручной отладки; рассылка вызывает без этого)."""
        resp = await self.request("GET", IPS_PATH)
        raw = resp.json()
        if log_raw:
            _log_underdog_raw_json_response(method="GET", path=IPS_PATH, raw=raw)
        return _extract_ips(raw)

    async def mark_ip_telegram_sent(self, ip_id: int) -> None:
        """PATCH telegram-sent; при 429 от Underdog — повтор с backoff (иначе те же IP уходят в рассылку снова)."""
        path = f"{IPS_PATH}/{ip_id}/telegram-sent"
        max_attempts = 12
        for attempt in range(max_attempts):
            try:
                await self.request("PATCH", path)
                return
            except UnderdogAPIError as exc:
                err_s = str(exc)
                is_ratelimit = "429" in err_s or "Too Many Attempts" in err_s
                if is_ratelimit and attempt < max_attempts - 1:
                    wait_s = min(2.0 * (2**attempt), 90.0)
                    logger.warning(
                        "Underdog 429 on PATCH ip telegram-sent, retry",
                        ip_id=ip_id,
                        attempt=attempt + 1,
                        wait_s=round(wait_s, 1),
                    )
                    await asyncio.sleep(wait_s)
                    continue
                raise

    async def fetch_tickets(self) -> List[Dict[str, Any]]:
        resp = await self.request("GET", TICKETS_PATH)
        return _extract_tickets(resp.json())

    async def mark_ticket_telegram_sent(self, ticket_id: int) -> None:
        path = f"{TICKETS_PATH}/{ticket_id}/telegram-sent"
        await self.request("PATCH", path)

    @classmethod
    def from_settings(cls) -> "UnderdogClient":
        return cls(
            base_url=settings.underdog_base_url,
            email=settings.underdog_email,
            password=secret(settings.underdog_password),
            token_ttl=settings.underdog_token_ttl,
        )
