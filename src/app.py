import asyncio
import hmac
from contextlib import suppress
from datetime import datetime, timezone, date
from typing import Any, Dict, Mapping, Tuple, Optional

from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from loguru import logger
from .config import secret, settings
from .dispatcher import dp, bot, notify_buyer
from .orders_bot import orders_dp, orders_bot
from .design_bot import design_dp, design_bot
from . import handlers  # noqa: F401 ensure handlers are registered
from . import db, underdog, keitaro_sync, new_admin_sync, daily_revenue_report
from .services.keitaro_postbacks import (
    build_notification_text,
    has_meaningful_fields,
    is_sale as is_keitaro_sale,
    sale_postback_fingerprint,
)
from aiogram.types import BotCommand, ErrorEvent, Update
from pydantic import BaseModel, Field

# Sanitize webhook path for route decorator
WEBHOOK_PATH = settings.webhook_secret_path.strip()
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH

ORDERS_WEBHOOK_PATH = settings.orders_webhook_path.strip()
if not ORDERS_WEBHOOK_PATH.startswith("/"):
    ORDERS_WEBHOOK_PATH = "/" + ORDERS_WEBHOOK_PATH

DESIGN_WEBHOOK_PATH = settings.design_webhook_path.strip()
if not DESIGN_WEBHOOK_PATH.startswith("/"):
    DESIGN_WEBHOOK_PATH = "/" + DESIGN_WEBHOOK_PATH

app = FastAPI(title="Keitaro Telegram Notifier")
# One run per notifier type at a time (cron + manual + scheduled loop must not double-send)
_notify_locks: dict[str, asyncio.Lock] = {kind: asyncio.Lock() for kind in ("domains", "ip", "design")}
_NOTIFY_INLINE_WAIT_SECONDS = 25
_design_notify_task: asyncio.Task | None = None
_keitaro_sync_task: asyncio.Task | None = None
_new_admin_sync_task: asyncio.Task | None = None
_daily_revenue_report_task: asyncio.Task | None = None


async def _design_notify_all(dry_run: bool) -> dict:
    """All DesignBot checks once: назначение, выполнение, SLA 24h, 48h not-in-progress."""
    return {
        "assignments": await underdog.notify_design_assignments(dry_run=dry_run, bot_instance=design_bot),
        "completions": await underdog.notify_design_completions(dry_run=dry_run, bot_instance=design_bot),
        "sla_24h": await underdog.notify_design_sla_24h(dry_run=dry_run, bot_instance=design_bot),
        "not_in_progress_48h": await underdog.notify_design_not_in_progress_48h(dry_run=dry_run, bot_instance=design_bot),
    }


async def _run_design_notifications() -> None:
    """Scheduled run; skipped while a manual /underdog/design/notify run holds the lock."""
    lock = _notify_locks["design"]
    if lock.locked():
        logger.info("Scheduled DesignBot check skipped: another run in progress")
        return
    async with lock:
        stats = await _design_notify_all(dry_run=False)
    logger.info("Scheduled DesignBot notification check completed", **stats)


async def _design_notification_loop(interval_seconds: int) -> None:
    """Keep DesignBot notifications working without an external cron process."""
    while True:
        try:
            await _run_design_notifications()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Scheduled DesignBot notification check failed", error=str(exc))
        await asyncio.sleep(interval_seconds)


async def _run_keitaro_domain_sync() -> None:
    """Pull new Keitaro campaigns/domains into the local lookup cache."""
    count = await keitaro_sync.sync_campaigns()
    logger.info("Scheduled Keitaro domain sync completed", count=count)


async def _keitaro_domain_sync_loop(interval_seconds: int) -> None:
    """Refresh the domain cache daily (or at the configured interval)."""
    while True:
        try:
            await _run_keitaro_domain_sync()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Scheduled Keitaro domain sync failed", error=str(exc))
        await asyncio.sleep(interval_seconds)


async def _new_admin_employee_sync_loop(interval_seconds: int) -> None:
    """Keep teams, buyers, helper assignments and Keitaro aliases aligned with the Admin directory."""
    while True:
        try:
            await new_admin_sync.run_sync()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Scheduled New Admin employee sync failed", error=str(exc))
        await asyncio.sleep(interval_seconds)

_DEPOSIT_FOOTERS_BY_USERNAME: dict[str, str] = {
    "egorkunderdog": (
        "\n\nНИХУЯ ТЫ ЕБАШИШЬ БРАТАН! ГАЗУЙ ГАЗУЙ ЛУЧШИ!!! БМВ НЕ СУЩЕСТВУЕТ!!!!"
    ),
    "uladzislau_underdog": (
        "\n\nВибрация прикосновений. Расствориться в моменте...чувствовать еще сильнее..."
    ),
    "underdog_headofbuying": (
        "\n\nТак называемый депозит, так называемый профит"
    ),
    "dianaunderdog": (
        "\n\nРяженка на друиде кастует депы"
    ),
    "maria_underdog": (
        "\n\nСОЛНЫШКО У ТЕБЯ ВСЕ ПОЛУЧИТСЯ! ТЫ САМАЯ САМАЯ ЛУЧШАЯ! Все будет заебись, пусть дальше капают депозитики"
    ),
}


def _buyer_username(user: dict | None) -> str | None:
    if not user:
        return None
    username = (user.get("username") or "").strip().lstrip("@").lower()
    return username or None


async def _buyer_label(user_id: int | None) -> str | None:
    """Buyer's name for the БАЙЕР line (None → formatter falls back to the campaign alias)."""
    if user_id is not None:
        try:
            user = await db.get_user(user_id)
        except Exception as e:
            logger.warning(f"Failed to load buyer {user_id} for the daily revenue label: {e}")
            user = None
        if user:
            full_name = (user.get("full_name") or "").strip()
            if full_name:
                return full_name
            username = (user.get("username") or "").strip().lstrip("@")
            if username:
                return f"@{username}"
    return None


def _deposit_message_for_recipient(
    base_text: str,
    *,
    recipient_id: int,
    buyer_id: int | None,
    buyer_user: dict | None,
    is_sale: bool,
) -> str:
    if is_sale and buyer_id is not None and recipient_id == int(buyer_id):
        footer = _DEPOSIT_FOOTERS_BY_USERNAME.get(_buyer_username(buyer_user) or "")
        if footer:
            return base_text + footer
    return base_text


class DomainNotifyRequest(BaseModel):
    days: int = Field(default=30, ge=0, le=365)
    dry_run: bool = Field(default=True)
    token: Optional[str] = None


class DailyRevenueReportRequest(BaseModel):
    dry_run: bool = Field(default=True, description="True = только превью текста без отправки")
    token: Optional[str] = None


class IPNotifyRequest(BaseModel):
    days: int = Field(default=7, ge=0, le=365)
    dry_run: bool = Field(default=False, description="True = только проверка без отправки; по умолчанию отправляем")
    token: Optional[str] = None


class PostbackReplayRequest(BaseModel):
    """ids — любые статусы; либо окно [from, to) в UTC — только pending/failed."""
    model_config = {"populate_by_name": True}
    ids: Optional[list[int]] = Field(default=None, max_length=1000)
    since: Optional[datetime] = Field(default=None, alias="from")
    until: Optional[datetime] = Field(default=None, alias="to")
    dry_run: bool = Field(default=True)
    token: Optional[str] = None


async def _run_notify_job(kind: str, factory, dry_run: bool) -> dict:
    """Run under the per-type lock in a background task; return stats if it finishes quickly, else running=True."""
    lock = _notify_locks[kind]
    if lock.locked():
        return {"ok": False, "busy": True}
    await lock.acquire()

    async def job():
        try:
            return await factory()
        finally:
            lock.release()

    task = _spawn(job(), f"underdog-{kind}-notify")
    done, _ = await asyncio.wait({task}, timeout=_NOTIFY_INLINE_WAIT_SECONDS)
    if not done:
        return {"ok": True, "dry_run": dry_run, "running": True}
    return {"ok": True, "dry_run": dry_run, "stats": task.result()}


def _extract_bearer_token(authorization: str | None) -> str | None:
    """Return a normalized Bearer token, accepting the auth scheme case-insensitively."""
    if not authorization:
        return None
    scheme, separator, credentials = authorization.strip().partition(" ")
    if not separator or scheme.lower() != "bearer":
        return None
    return credentials.strip() or None


def _token_matches(supplied: Any) -> bool:
    if supplied is None:
        return False
    return hmac.compare_digest(str(supplied).strip(), secret(settings.postback_token))


def _require_internal_token(authorization: str | None, inline_token: Optional[str] = None) -> None:
    if not settings.postback_token:
        return
    supplied = _extract_bearer_token(authorization)
    if not supplied:
        supplied = inline_token.strip() if inline_token else None
    if not supplied:
        raise HTTPException(401, "Unauthorized")
    if not _token_matches(supplied):
        raise HTTPException(403, "Forbidden")


def _authorize_postback(authorization: str | None, data: Mapping[str, Any]) -> None:
    """Authorize a tracker callback using a header or its token/auth field."""
    if not settings.postback_token:
        return
    supplied = _extract_bearer_token(authorization) or data.get("token") or data.get("auth")
    if not supplied:
        raise HTTPException(401, "Unauthorized")
    if not _token_matches(supplied):
        raise HTTPException(403, "Forbidden")


def _remove_postback_credentials(data: dict[str, Any]) -> None:
    """Do not persist tracker credentials in tg_events.raw or background-task logs."""
    data.pop("token", None)
    data.pop("auth", None)

_daily_counter_lock = asyncio.Lock()
# Keyed by Telegram id, or by "alias:<campaign prefix>" when the deposit never routed to a user
_StatsKey = int | str
_daily_counter_cache: Dict[_StatsKey, Tuple[date, int]] = {}
_daily_revenue_cache: Dict[_StatsKey, Tuple[date, float]] = {}

# Per-user locks so concurrent postbacks for the same buyer get correct sequential daily counts
_user_locks: Dict[_StatsKey, asyncio.Lock] = {}
_user_locks_guard = asyncio.Lock()


def _lock_for_user(user_id: _StatsKey) -> asyncio.Lock:
    """Return a lock for the given user (creates on first use). Caller must hold _user_locks_guard when mutating."""
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]


async def _resolve_daily_counter(user_id: _StatsKey, db_value: int | None) -> int:
    """Stabilize daily deposit counter so it never goes backwards even if DB lagged."""
    today = datetime.now(timezone.utc).date()
    base_value = db_value or 0
    async with _daily_counter_lock:
        cached = _daily_counter_cache.get(user_id)
        if not cached or cached[0] != today:
            display = base_value if base_value > 0 else 1
        else:
            _, last_value = cached
            if base_value > last_value:
                display = base_value
            else:
                # If DB value is equal or less than the last displayed value,
                # keep the last value to avoid showing a lower number.
                # Do NOT increment when equal — that caused off-by-one duplicates.
                display = last_value
        _daily_counter_cache[user_id] = (today, display)
    if base_value and base_value < display:
        logger.debug(
            "Daily counter adjusted due to stale DB value",
            user_id=user_id,
            db_value=base_value,
            display_value=display,
        )
    return display

def _parse_amount(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None


async def _resolve_daily_revenue(user_id: _StatsKey, db_value: float | None, current_payout: float | None) -> float:
    """Stabilize the daily revenue total the same way as the deposit counter (never goes backwards)."""
    today = datetime.now(timezone.utc).date()
    base_value = float(db_value or 0)
    async with _daily_counter_lock:
        cached = _daily_revenue_cache.get(user_id)
        if not cached or cached[0] != today:
            display = base_value if base_value > 0 else float(current_payout or 0)
        else:
            _, last_value = cached
            display = base_value if base_value > last_value else last_value
        _daily_revenue_cache[user_id] = (today, display)
    return display


_background_tasks: set[asyncio.Task] = set()


def _spawn(coro, name: str) -> asyncio.Task:
    """create_task with a strong reference (the loop keeps only weak refs to tasks)."""
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


def _log_postback_result(data: dict, result: dict, inbound_id: int | None = None) -> None:
    logger.info(
        "Keitaro postback processed",
        inbound_id=inbound_id,
        subid=data.get("subid") or data.get("sub_id"),
        routed=result.get("routed"),
        sale=result.get("sale"),
        duplicate=result.get("duplicate"),
    )


async def _run_keitaro_postback_job(data: dict) -> None:
    """Fallback when the inbox INSERT failed: in-memory processing as before (no retry)."""
    try:
        _log_postback_result(data, await _process_keitaro_postback(data))
    except Exception:
        logger.exception("Keitaro postback background job failed")


_INBOUND_MAX_ATTEMPTS = 3
_inbound_in_flight: set[int] = set()


async def _process_inbound_postback(inbound_id: int) -> None:
    """Process one tg_inbound_postbacks row: done / duplicate on success, failed (with error) on exception."""
    if inbound_id in _inbound_in_flight:
        return
    _inbound_in_flight.add(inbound_id)
    try:
        data = await db.claim_inbound_postback(inbound_id)
        if data is None:
            return
        try:
            result = await _process_keitaro_postback(data, inbound_id=inbound_id)
        except Exception as e:
            logger.exception("Keitaro postback {} failed", inbound_id)
            await db.finish_inbound_postback(inbound_id, "failed", f"{type(e).__name__}: {e}")
            return
        await db.finish_inbound_postback(inbound_id, "duplicate" if result.get("duplicate") else "done")
        _log_postback_result(data, result, inbound_id)
    except Exception:
        logger.exception("Inbound postback {} bookkeeping failed", inbound_id)
    finally:
        _inbound_in_flight.discard(inbound_id)


async def _process_inbound_postbacks(ids: list[int]) -> None:
    for inbound_id in ids:
        await _process_inbound_postback(inbound_id)


_INBOUND_RECOVERY_DELAY_SECONDS = 60


async def _recover_inbound_postbacks(delay: float = _INBOUND_RECOVERY_DELAY_SECONDS) -> None:
    """On startup: rows left pending by a restart/crash and failed rows with attempts left.

    Railway overlaps old/new deployments: wait, and take only rows older than the delay,
    so rows the old instance is still processing are not picked up twice.
    """
    await asyncio.sleep(delay)
    try:
        ids = await db.list_inbound_postbacks_for_retry(_INBOUND_MAX_ATTEMPTS, int(delay))
    except Exception:
        logger.exception("Failed to list inbound postbacks for retry")
        return
    if ids:
        logger.warning("Recovering {} inbound postbacks: {}", len(ids), ids)
        await _process_inbound_postbacks(ids)


async def _accept_postback(data: dict) -> None:
    """Persist first, then process in background; Keitaro gets 200 only after the row is stored."""
    fingerprint = sale_postback_fingerprint(data) if is_keitaro_sale(data) else None
    try:
        inbound_id = await db.enqueue_inbound_postback(data, fingerprint)
    except Exception:
        logger.exception("Failed to store inbound postback, processing in memory")
        _spawn(_run_keitaro_postback_job(dict(data)), "keitaro-postback-fallback")
        return
    _spawn(_process_inbound_postback(inbound_id), f"keitaro-postback-{inbound_id}")


async def _process_keitaro_postback(data: dict, inbound_id: int | None = None) -> dict:
    if is_keitaro_sale(data):
        fp = sale_postback_fingerprint(data)
        if fp:
            click_id = (
                data.get("subid")
                or data.get("sub_id")
                or data.get("clickid")
                or data.get("click_id")
                or data.get("tid")
            )
            try:
                first = await db.claim_keitaro_sale_postback(
                    fp,
                    click_id=str(click_id).strip() if click_id is not None else None,
                    inbound_id=inbound_id,
                )
            except Exception as e:
                logger.warning(f"Keitaro sale dedupe failed, processing anyway: {e}")
                first = True
            if not first:
                logger.info(
                    "Duplicate Keitaro sale postback ignored",
                    fingerprint=fp,
                    subid=data.get("subid") or data.get("sub_id"),
                )
                return {
                    "ok": True,
                    "duplicate": True,
                    "routed": False,
                    "buyer_id": None,
                    "fallback": False,
                    "sale": True,
                }

    # Try alias-based routing by campaign_name prefix
    campaign_name = data.get("campaign_name") or data.get("campaign")
    alias_key = None
    if campaign_name:
        alias_key = (campaign_name.split("_", 1)[0] or "").strip()
    alias = await db.find_alias(alias_key)

    buyer_id = alias.get("buyer_id") if alias else None
    routed_via_alias = buyer_id is not None
    if not buyer_id:
        buyer_id = await db.find_user_for_postback(
            offer=data.get("offer") or data.get("offer_name") or data.get("campaign") or data.get("campaign_name"),
            country=data.get("country") or data.get("geo"),
            source=data.get("source") or data.get("traffic_source_name") or data.get("traffic_source") or data.get("affiliate")
        )

    # Fallback to an admin if still not routed
    used_fallback = False
    if not buyer_id:
        # Prefer ADMINS env, else try any DB user with admin role
        if settings.admins:
            buyer_id = settings.admins[0]
            used_fallback = True
        else:
            try:
                users = await db.list_users()
                admin_user = next((u for u in users if (u.get("role") == "admin")), None)
                if admin_user:
                    buyer_id = int(admin_user["telegram_id"])  # type: ignore
                    used_fallback = True
            except Exception:
                pass
    routed_id = None
    # An explicit alias assignment is authoritative even when that Telegram user also has
    # an admin/head role. Only role-filter recipients found through generic routes.
    try:
        routed_id = buyer_id
        if used_fallback and routed_id:
            routed_id = None
        elif not routed_via_alias:
            try:
                users = await db.list_users()
                ru = next((u for u in users if u["telegram_id"] == routed_id), None)
                if ru and (ru.get("role") not in {"buyer", "lead", "mentor", "head"}):
                    routed_id = None
            except Exception:
                pass
        await db.log_event(data, routed_id, inbound_id)
    except Exception as e:
        logger.warning(f"Failed to log event: {e}")
        routed_id = None

    stats_user_id: int | None = None
    if routed_id is not None:
        try:
            stats_user_id = int(routed_id)
        except Exception as e:
            logger.warning(f"Failed to coerce routed user id {routed_id}: {e}")

    # do not return early: admins should still receive notifications even if not routed

    # Map status and accept only sale-like statuses
    is_sale = is_keitaro_sale(data)
    payout = data.get("profit") or data.get("payout") or data.get("revenue") or data.get("conversion_revenue")
    currency = data.get("currency") or data.get("revenue_currency") or data.get("payout_currency")
    offer_id = data.get("offer_id") or data.get("offer.id")
    offer_name = data.get("offer_name") or data.get("offer.name") or data.get("offer")
    subid = data.get("subid") or data.get("sub_id") or data.get("clickid") or data.get("click_id")
    sub_id_3 = data.get("sub_id_3") or data.get("subid3")
    sale_time = data.get("conversion_sale_time") or data.get("conversion.sale_time") or data.get("conversion_time")
    campaign_name = data.get("campaign_name") or data.get("campaign.name")
    # Clean unexpanded placeholders like "{conversion.sale_time}"
    def _clean(v):
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("{") and s.endswith("}"):
                return None
        return v
    payout = _clean(payout)
    currency = _clean(currency)
    offer_id = _clean(offer_id)
    offer_name = _clean(offer_name)
    subid = _clean(subid)
    sub_id_3 = _clean(sub_id_3)
    sale_time = _clean(sale_time)
    campaign_name = _clean(campaign_name)

    # Build text via unified formatter (with optional daily deposits count)
    # Serialize by user_id so concurrent postbacks for the same buyer get correct sequential daily counts
    daily_count: int | None = None
    daily_revenue: float | None = None
    kpi_daily_goal: int | None = None
    buyer_label: str | None = None
    alias_prefix = (alias_key or "").strip().lower() or None
    # Unrouted deposits (buyer without a tg_aliases row) still carry the campaign prefix,
    # so fall back to it — the daily lines must reach every recipient, not just routed ones.
    stats_key: _StatsKey | None = stats_user_id if stats_user_id is not None else (
        f"alias:{alias_prefix}" if alias_prefix else None
    )
    if is_sale and stats_key is not None:
        db_daily_count: int | None = None
        db_daily_revenue: float | None = None
        async with _user_locks_guard:
            user_lock = _lock_for_user(stats_key)
        async with user_lock:
            if stats_user_id is not None:
                try:
                    db_daily_count = await db.count_today_user_sales(stats_user_id)
                except Exception as e:
                    logger.warning(f"Failed to get daily count: {e}")
                # ДОХОД ЗА ДЕНЬ: total of every deposit so far today (ПРОФИТ shows only this one)
                try:
                    db_daily_revenue = await db.sum_today_user_profit(stats_user_id)
                except Exception as e:
                    logger.warning(f"Failed to get daily revenue: {e}")
            else:
                try:
                    db_daily_count, db_daily_revenue = await db.today_alias_sales(alias_prefix)
                except Exception as e:
                    logger.warning(f"Failed to get daily stats for alias {alias_prefix}: {e}")
            try:
                daily_count = await _resolve_daily_counter(stats_key, db_daily_count)
            except Exception as e:
                logger.warning(f"Failed to adjust daily counter: {e}")
                daily_count = db_daily_count
            try:
                daily_revenue = await _resolve_daily_revenue(
                    stats_key, db_daily_revenue, _parse_amount(payout)
                )
            except Exception as e:
                logger.warning(f"Failed to adjust daily revenue: {e}")
                daily_revenue = db_daily_revenue
        if stats_user_id is not None:
            try:
                kpi = await db.get_kpi(stats_user_id)
                kpi_daily_goal = kpi.get("daily_goal")
            except Exception as e:
                logger.warning(f"Failed to get KPI: {e}")
    buyer_label = await _buyer_label(stats_user_id)
    text = build_notification_text(
        data,
        daily_count=daily_count,
        kpi_daily_goal=kpi_daily_goal,
        daily_revenue=daily_revenue,
        buyer_label=buyer_label,
    )

    # Determine recipients
    recipient_ids: set[int] = set()
    buyer_user: dict | None = None
    try:
        users = await db.list_users()
        # admins always receive all notifications
        admins_db = [u for u in users if u.get("role") == "admin" and u.get("is_active")]
        for u in admins_db:
            recipient_ids.add(int(u["telegram_id"]))  # type: ignore
        # plus ADMINS from env, if provided
        if settings.admins:
            for aid in settings.admins:
                try:
                    recipient_ids.add(int(aid))
                except Exception:
                    pass
        # for sale events, also notify buyer, team leads (or alias lead), and all heads
        if is_sale:
            if buyer_id:
                recipient_ids.add(int(buyer_id))
            if alias:
                alias_lead_id = alias.get("lead_id")
                if alias_lead_id:
                    recipient_ids.add(int(alias_lead_id))
            buyer_user = next((u for u in users if u.get("telegram_id") == buyer_id), None)
            if buyer_user and buyer_user.get("team_id"):
                team_id = buyer_user.get("team_id")
                # If buyer is NOT a mentor, notify team leads; mentors' own deposits are not visible to leads
                if (buyer_user.get("role") != "mentor"):
                    try:
                        lead_ids = await db.list_team_leads(int(team_id))
                        for lid in lead_ids:
                            recipient_ids.add(int(lid))
                    except Exception as e:
                        logger.warning(f"Failed to include team leads: {e}")
                # mentors subscribed to this team
                try:
                    mentor_ids = await db.list_team_mentors(int(team_id))
                    for mid in mentor_ids:
                        recipient_ids.add(int(mid))
                except Exception as e:
                    logger.warning(f"Failed to include mentors: {e}")
            heads = [u for u in users if u.get("role") == "head" and u.get("is_active")]
            for u in heads:
                recipient_ids.add(int(u["telegram_id"]))  # type: ignore
            # помощники, привязанные к этому байеру — тоже получают уведомление о депозите
            if buyer_id:
                try:
                    helper_ids = await db.list_helpers_by_buyer(int(buyer_id))
                    for hid in helper_ids:
                        recipient_ids.add(hid)
                except Exception as e:
                    logger.warning(f"Failed to include helpers for buyer: {e}")
    except Exception as e:
        logger.warning(f"Failed to expand recipients: {e}")
        if not recipient_ids:
            raise

    # Send message to all recipients (deduped)
    delivered = 0
    for rid in recipient_ids:
        try:
            message_text = _deposit_message_for_recipient(
                text,
                recipient_id=rid,
                buyer_id=int(buyer_id) if buyer_id else None,
                buyer_user=buyer_user,
                is_sale=is_sale,
            )
            await notify_buyer(rid, message_text)
            delivered += 1
        except Exception as e:
            logger.warning(f"Notify failed for {rid}: {e}")
    if recipient_ids and not delivered:
        # Nobody got it (Telegram/network down) — fail so the inbox row is retried
        raise RuntimeError(f"Postback not delivered to any of {len(recipient_ids)} recipients")
    return {"ok": True, "routed": bool(buyer_id), "buyer_id": buyer_id, "fallback": used_fallback, "sale": is_sale}


@app.on_event("startup")
async def on_startup():
    global _design_notify_task, _keitaro_sync_task, _new_admin_sync_task, _daily_revenue_report_task
    try:
        await db.init_pool()
    except Exception as e:
        # Log and re-raise so Railway logs show root cause
        logger.exception(f"DB init failed: {e}")
        raise
    _spawn(_recover_inbound_postbacks(), "inbound-postback-recovery")
    if not settings.postback_token:
        logger.warning("POSTBACK_TOKEN is empty: /keitaro/postback and internal endpoints accept requests without auth")
    # set webhook for Telegram
    webhook_secret = secret(settings.telegram_webhook_secret) or None
    url = settings.base_url.rstrip("/") + WEBHOOK_PATH
    try:
        await bot.set_webhook(url, secret_token=webhook_secret)
        logger.info("Main Telegram webhook configured")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")

    orders_token = settings.orders_bot_token
    if orders_token and orders_token != settings.telegram_bot_token:
        orders_url = settings.base_url.rstrip("/") + ORDERS_WEBHOOK_PATH
        try:
            await orders_bot.set_webhook(orders_url, secret_token=webhook_secret)
            logger.info("Orders Telegram webhook configured")
        except Exception as e:
            logger.error(f"Failed to set orders webhook: {e}")
    # Set command menu for the bot (helps users discover commands)
    try:
        await bot.set_my_commands([
            BotCommand(command="menu", description="Открыть меню"),
            BotCommand(command="today", description="Отчет за сегодня"),
            BotCommand(command="yesterday", description="Отчет за вчера"),
            BotCommand(command="week", description="Отчет за 7 дней"),
            BotCommand(command="checkdomain", description="Проверить домен"),
            BotCommand(command="whoami", description="Ваш Telegram ID"),
            BotCommand(command="ping", description="Проверка связи"),
            BotCommand(command="help", description="Помощь"),
        ])
    except Exception as e:
        logger.warning(f"Failed to set bot commands: {e}")

    orders_commands = [
        BotCommand(command="start", description="Получить невручённые заказы"),
        BotCommand(command="menu", description="Меню бота заказов"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="adminstatus", description="Проверить статус админа"),
    ]
    try:
        await orders_bot.set_my_commands(orders_commands)
    except Exception as e:
        logger.warning(f"Failed to set orders bot commands: {e}")

    design_token = settings.design_bot_token
    if design_token and design_token not in (settings.telegram_bot_token, orders_token):
        design_url = settings.base_url.rstrip("/") + DESIGN_WEBHOOK_PATH
        try:
            await design_bot.set_webhook(design_url, secret_token=webhook_secret)
            logger.info("Design Telegram webhook configured")
        except Exception as e:
            logger.error(f"Failed to set design webhook: {e}")
        try:
            await design_bot.set_my_commands([
                BotCommand(command="start", description="Приветствие"),
            ])
        except Exception as e:
            logger.warning(f"Failed to set design bot commands: {e}")

    interval = max(0, int(settings.design_notify_interval_seconds))
    if design_token and interval > 0:
        # Telegram delivery is deduplicated in DB, so the first check can run immediately.
        _design_notify_task = asyncio.create_task(
            _design_notification_loop(max(60, interval)),
            name="design-notification-loop",
        )
        logger.info("DesignBot scheduled checks enabled", interval_seconds=max(60, interval))

    keitaro_interval = max(0, int(settings.keitaro_sync_interval_seconds))
    if settings.keitaro_api_key and settings.keitaro_base_url and keitaro_interval > 0:
        _keitaro_sync_task = asyncio.create_task(
            _keitaro_domain_sync_loop(max(60, keitaro_interval)),
            name="keitaro-domain-sync-loop",
        )
        logger.info("Keitaro domain sync enabled", interval_seconds=max(60, keitaro_interval))

    new_admin_interval = max(0, int(settings.new_admin_sync_interval_seconds))
    if settings.new_admin_api_url and settings.new_admin_api_key and new_admin_interval > 0:
        _new_admin_sync_task = asyncio.create_task(
            _new_admin_employee_sync_loop(max(60, new_admin_interval)),
            name="new-admin-employee-sync-loop",
        )
        logger.info("New Admin employee sync enabled", interval_seconds=max(60, new_admin_interval))

    report_time = daily_revenue_report.parse_report_time(settings.daily_revenue_report_time)
    if report_time is not None:
        report_tz = daily_revenue_report.report_timezone()
        _daily_revenue_report_task = asyncio.create_task(
            daily_revenue_report.daily_revenue_report_loop(report_time, report_tz),
            name="daily-revenue-report-loop",
        )
        logger.info(
            "Daily revenue report enabled",
            at=report_time.strftime("%H:%M"),
            tz=str(report_tz),
        )

@app.on_event("shutdown")
async def on_shutdown():
    global _design_notify_task, _keitaro_sync_task, _new_admin_sync_task, _daily_revenue_report_task
    for task in (_design_notify_task, _keitaro_sync_task, _new_admin_sync_task, _daily_revenue_report_task):
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    _design_notify_task = None
    _keitaro_sync_task = None
    _new_admin_sync_task = None
    _daily_revenue_report_task = None
    if _background_tasks:
        # Let in-flight postbacks/updates finish; unfinished inbox rows stay pending for the next start
        await asyncio.wait(set(_background_tasks), timeout=5)
    await db.close_pool()
    # Close aiogram bot aiohttp sessions to avoid "Unclosed client session" warnings
    for bot_instance in (bot, orders_bot, design_bot):
        try:
            session = getattr(bot_instance, "session", None)
            if session is not None and hasattr(session, "close"):
                await session.close()
        except Exception as e:
            logger.warning(f"Failed to close bot session: {e}")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/db/ping")
async def db_ping():
    try:
        row = await db.fetch_one("SELECT 1")
        return {"ok": True, "result": row and int(row[0])}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(500, f"DB ping failed: {e}")

@app.post("/keitaro/postback")
async def keitaro_postback(
    request: Request,
    authorization: str | None = Header(default=None),
):
    # Parse body leniently; if anything fails, continue with query params only
    content_type = (request.headers.get("content-type") or "").lower()
    data = {}
    if "application/json" in content_type:
        try:
            parsed = await request.json()
            data = dict(parsed) if isinstance(parsed, Mapping) else {}
        except Exception:
            data = {}
    elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        try:
            form = await request.form()
            data = {k: v for k, v in form.items()}
        except Exception:
            data = {}
    # Always merge query params (act as defaults)
    if request.query_params:
        for k, v in request.query_params.items():
            data.setdefault(k, v)

    # Prefer the header, but accept token/auth fields for trackers without header configuration.
    _authorize_postback(authorization, data)
    _remove_postback_credentials(data)

    # If no meaningful fields are present, return 200 with a simple ACK body
    if not has_meaningful_fields(data):
        return JSONResponse({"success": 200})

    # Keitaro S2S often uses ~5s HTTP timeout — сохраняем в очередь и отвечаем, обработка в фоне
    await _accept_postback(dict(data))
    return JSONResponse({"ok": True, "accepted": True})

# Some trackers send GET S2S callbacks; mirror POST handler for query params
@app.get("/keitaro/postback")
async def keitaro_postback_get(
    request: Request,
    authorization: str | None = Header(default=None),
):
    try:
        # Parse query parameters as a dict
        data = dict(request.query_params)

        _authorize_postback(authorization, data)
        _remove_postback_credentials(data)

        # If no meaningful fields are present, return 200 with a simple ACK body
        if not has_meaningful_fields(data):
            return JSONResponse({"success": 200})

        await _accept_postback(dict(data))
        return JSONResponse({"ok": True, "accepted": True})

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"GET postback handler failed: {e}")
        return {"ok": True}


@app.post("/admin/postbacks/replay")
async def replay_postbacks_endpoint(
    payload: PostbackReplayRequest,
    authorization: str | None = Header(default=None),
):
    """Переотправить постбеки из tg_inbound_postbacks: строки → pending, обработка в фоне."""
    _require_internal_token(authorization, payload.token)
    if not payload.ids and not (payload.since and payload.until):
        raise HTTPException(422, "Pass ids or both from/to")
    ids = await db.requeue_inbound_postbacks(
        ids=payload.ids, since=payload.since, until=payload.until, dry_run=payload.dry_run
    )
    if ids and not payload.dry_run:
        _spawn(_process_inbound_postbacks(ids), "inbound-postback-replay")
    return {"ok": True, "dry_run": payload.dry_run, "matched": len(ids), "ids": ids}


@app.post("/underdog/domains/notify")
async def notify_expiring_domains_endpoint(
    payload: DomainNotifyRequest,
    authorization: str | None = Header(default=None),
):
    _require_internal_token(authorization, payload.token)
    return await _run_notify_job(
        "domains",
        lambda: underdog.notify_expiring_domains(dry_run=payload.dry_run, days=payload.days, bot_instance=orders_bot),
        payload.dry_run,
    )


@app.post("/underdog/ip/notify")
async def notify_expiring_ips_endpoint(
    payload: IPNotifyRequest,
    authorization: str | None = Header(default=None),
):
    _require_internal_token(authorization, payload.token)
    return await _run_notify_job(
        "ip",
        lambda: underdog.notify_expiring_ips(
            dry_run=payload.dry_run, days=payload.days, bot_instance=orders_bot, admin_bot_instance=bot
        ),
        payload.dry_run,
    )


@app.get("/underdog/design/subscribers")
async def design_subscribers(
    authorization: str | None = Header(default=None),
):
    """Кто в рассылке DesignBot: список chat_id из tg_design_bot_chats (кто нажал /start в DesignBot)."""
    _require_internal_token(authorization)
    try:
        chat_ids = await db.list_design_bot_subscribers()
        return {"subscribers_count": len(chat_ids), "subscriber_chat_ids": chat_ids}
    except Exception as e:
        logger.exception("Failed to list design subscribers: {}", e)
        raise HTTPException(500, str(e))


@app.post("/underdog/design/notify")
async def notify_design_endpoint(
    payload: DomainNotifyRequest,
    authorization: str | None = Header(default=None),
):
    """Уведомлять по дизайну: назначение, выполнение, SLA24h и 48h not-in-progress reminder."""
    _require_internal_token(authorization, payload.token)
    if not settings.design_bot_token:
        return JSONResponse(
            {"ok": False, "error": "DESIGN_BOT_TOKEN not configured"},
            status_code=503,
        )

    return await _run_notify_job("design", lambda: _design_notify_all(payload.dry_run), payload.dry_run)


@app.post("/reports/daily-revenue")
async def daily_revenue_report_endpoint(
    payload: DailyRevenueReportRequest,
    authorization: str | None = Header(default=None),
):
    """Сводка дохода за день по командам для лидов/менторов/хэдов (ручной запуск или проверка)."""
    _require_internal_token(authorization, payload.token)
    stats = await daily_revenue_report.send_daily_revenue_report(dry_run=payload.dry_run)
    return {"ok": True, "dry_run": payload.dry_run, "stats": stats}


async def _on_handler_error(event: ErrorEvent) -> bool:
    """Global aiogram errors handler: log and tell the user instead of leaving a spinner."""
    logger.opt(exception=event.exception).error("Telegram handler failed: update_id={}", event.update.update_id)
    with suppress(Exception):
        if event.update.callback_query:
            await event.update.callback_query.answer("Ошибка, попробуйте ещё раз", show_alert=False)
        elif event.update.message:
            await event.update.message.answer("Ошибка, попробуйте ещё раз")
    return True


for _dispatcher in (dp, orders_dp, design_dp):
    _dispatcher.errors.register(_on_handler_error)


async def _feed_update(dispatcher, bot_instance, update: Update, name: str) -> None:
    try:
        await dispatcher.feed_update(bot_instance, update)
    except Exception:
        logger.exception("{} webhook update {} failed", name, update.update_id)


async def _accept_webhook(request: Request, dispatcher, bot_instance, name: str) -> JSONResponse:
    """ACK Telegram immediately; long handlers (yt-dlp, reports, sync) must not trigger Telegram retries."""
    expected = secret(settings.telegram_webhook_secret)
    supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token") or ""
    if expected and not hmac.compare_digest(supplied, expected):
        logger.warning("{} webhook rejected: bad or missing secret token", name)
        return JSONResponse({"ok": False}, status_code=403)
    try:
        update = Update.model_validate(await request.json())
    except Exception as e:
        # Never 500 to Telegram: log and ACK to avoid retries blocking updates
        logger.exception("{} webhook payload invalid: {}", name, e)
        return JSONResponse({"ok": True})
    if name == "design":
        # Лог при каждом апдейте — если при /start в DesignBot здесь пусто, вебхук не доходит
        msg = update.message
        logger.info(
            "Design webhook received",
            update_id=update.update_id,
            chat_id=msg.chat.id if msg else None,
            text=(msg.text or "")[:50] if msg else None,
        )
    _spawn(_feed_update(dispatcher, bot_instance, update, name), f"{name}-update-{update.update_id}")
    return JSONResponse({"ok": True})


@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    return await _accept_webhook(request, dp, bot, "main")


@app.post(ORDERS_WEBHOOK_PATH)
async def orders_telegram_webhook(request: Request):
    if not settings.orders_bot_token or settings.orders_bot_token == settings.telegram_bot_token:
        return JSONResponse({"ok": True})
    return await _accept_webhook(request, orders_dp, orders_bot, "orders")


@app.post(DESIGN_WEBHOOK_PATH)
async def design_telegram_webhook(request: Request):
    if not settings.design_bot_token or settings.design_bot_token == settings.telegram_bot_token:
        return JSONResponse({"ok": True})
    return await _accept_webhook(request, design_dp, design_bot, "design")
