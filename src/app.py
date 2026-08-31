import asyncio
import hmac
from contextlib import suppress
from datetime import datetime, timezone, date
from typing import Any, Dict, Mapping, Tuple, Optional

from fastapi import FastAPI, Request, HTTPException, Header, BackgroundTasks
from fastapi.responses import JSONResponse
from loguru import logger
from .config import settings
from .dispatcher import dp, bot, notify_buyer
from .orders_bot import orders_dp, orders_bot
from .design_bot import design_dp, design_bot
from . import handlers  # noqa: F401 ensure handlers are registered
from . import db, underdog, keitaro_sync, new_admin_sync
from .services.keitaro_postbacks import (
    build_notification_text,
    has_meaningful_fields,
    is_sale as is_keitaro_sale,
    sale_postback_fingerprint,
)
from aiogram.types import Update, BotCommand
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
_design_notify_task: asyncio.Task | None = None
_keitaro_sync_task: asyncio.Task | None = None
_new_admin_sync_task: asyncio.Task | None = None


async def _run_design_notifications() -> None:
    """Run all DesignBot notification checks once."""
    assignment_stats = await underdog.notify_design_assignments(
        dry_run=False,
        bot_instance=design_bot,
    )
    completion_stats = await underdog.notify_design_completions(
        dry_run=False,
        bot_instance=design_bot,
    )
    sla_stats = await underdog.notify_design_sla_24h(
        dry_run=False,
        bot_instance=design_bot,
    )
    reminder_stats = await underdog.notify_design_not_in_progress_48h(
        dry_run=False,
        bot_instance=design_bot,
    )
    logger.info(
        "Scheduled DesignBot notification check completed",
        assignments=assignment_stats,
        completions=completion_stats,
        sla_24h=sla_stats,
        not_in_progress_48h=reminder_stats,
    )


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
    """Keep teams, buyers and helper assignments aligned with the Admin directory."""
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


class IPNotifyRequest(BaseModel):
    days: int = Field(default=7, ge=0, le=365)
    dry_run: bool = Field(default=False, description="True = только проверка без отправки; по умолчанию отправляем")
    token: Optional[str] = None


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
    return hmac.compare_digest(str(supplied).strip(), settings.postback_token)


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
_daily_counter_cache: Dict[int, Tuple[date, int]] = {}

# Per-user locks so concurrent postbacks for the same buyer get correct sequential daily counts
_user_locks: Dict[int, asyncio.Lock] = {}
_user_locks_guard = asyncio.Lock()


def _lock_for_user(user_id: int) -> asyncio.Lock:
    """Return a lock for the given user (creates on first use). Caller must hold _user_locks_guard when mutating."""
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]


async def _resolve_daily_counter(user_id: int, db_value: int | None) -> int:
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

async def _run_keitaro_postback_job(data: dict) -> None:
    """Фоновая обработка: Keitaro ждёт ответ ~5 с, иначе cURL 28 — отдаём 200 раньше."""
    try:
        result = await _process_keitaro_postback(data)
        logger.info(
            "Keitaro postback processed",
            subid=data.get("subid") or data.get("sub_id"),
            routed=result.get("routed"),
            sale=result.get("sale"),
            duplicate=result.get("duplicate"),
        )
    except Exception:
        logger.exception("Keitaro postback background job failed")


async def _process_keitaro_postback(data: dict) -> dict:
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
        await db.log_event(data, routed_id)
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
    kpi_daily_goal: int | None = None
    if is_sale and stats_user_id is not None:
        db_daily_count: int | None = None
        async with _user_locks_guard:
            user_lock = _lock_for_user(stats_user_id)
        async with user_lock:
            try:
                db_daily_count = await db.count_today_user_sales(stats_user_id)
            except Exception as e:
                logger.warning(f"Failed to get daily count: {e}")
            try:
                daily_count = await _resolve_daily_counter(stats_user_id, db_daily_count)
            except Exception as e:
                logger.warning(f"Failed to adjust daily counter: {e}")
                daily_count = db_daily_count
        try:
            kpi = await db.get_kpi(stats_user_id)
            kpi_daily_goal = kpi.get("daily_goal")
        except Exception as e:
            logger.warning(f"Failed to get KPI: {e}")
    text = build_notification_text(data, daily_count=daily_count, kpi_daily_goal=kpi_daily_goal)

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

    # Send message to all recipients (deduped)
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
        except Exception as e:
            logger.warning(f"Notify failed for {rid}: {e}")
    return {"ok": True, "routed": bool(buyer_id), "buyer_id": buyer_id, "fallback": used_fallback, "sale": is_sale}


@app.on_event("startup")
async def on_startup():
    global _design_notify_task, _keitaro_sync_task, _new_admin_sync_task
    try:
        await db.init_pool()
    except Exception as e:
        # Log and re-raise so Railway logs show root cause
        logger.exception(f"DB init failed: {e}")
        raise
    # set webhook for Telegram
    secret_path = settings.webhook_secret_path.strip()
    if not secret_path.startswith("/"):
        secret_path = "/" + secret_path
    url = settings.base_url.rstrip("/") + secret_path
    try:
        await bot.set_webhook(url)
        logger.info("Main Telegram webhook configured")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")

    orders_token = settings.orders_bot_token
    if orders_token and orders_token != settings.telegram_bot_token:
        orders_url = settings.base_url.rstrip("/") + ORDERS_WEBHOOK_PATH
        try:
            await orders_bot.set_webhook(orders_url)
            logger.info("Orders Telegram webhook configured")
        except Exception as e:
            logger.error(f"Failed to set orders webhook: {e}")
    # Set command menu for the bot (helps users discover commands)
    try:
        await bot.set_my_commands([
            BotCommand(command="menu", description="Открыть меню"),
            BotCommand(command="checkdomain", description="Проверить домен"),
            BotCommand(command="help", description="Помощь"),
            BotCommand(command="ping", description="Проверка связи"),
            BotCommand(command="whoami", description="Ваш Telegram ID"),
            BotCommand(command="listroutes", description="Список правил"),
            BotCommand(command="listusers", description="Список пользователей"),
            BotCommand(command="manage", description="Управление (admin)"),
            BotCommand(command="aliases", description="Алиасы (admin)"),
            BotCommand(command="today", description="Отчет за сегодня"),
            BotCommand(command="yesterday", description="Отчет за вчера"),
            BotCommand(command="week", description="Отчет за 7 дней"),
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
            await design_bot.set_webhook(design_url)
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

@app.on_event("shutdown")
async def on_shutdown():
    global _design_notify_task, _keitaro_sync_task, _new_admin_sync_task
    for task in (_design_notify_task, _keitaro_sync_task, _new_admin_sync_task):
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    _design_notify_task = None
    _keitaro_sync_task = None
    _new_admin_sync_task = None
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
        pool = await db.init_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                row = await cur.fetchone()
        return {"ok": True, "result": row and int(row[0])}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(500, f"DB ping failed: {e}")

@app.post("/keitaro/postback")
async def keitaro_postback(
    request: Request,
    background_tasks: BackgroundTasks,
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

    # Keitaro S2S often uses ~5s HTTP timeout — отвечаем сразу, обработку делаем в фоне
    background_tasks.add_task(_run_keitaro_postback_job, dict(data))
    return JSONResponse({"ok": True, "accepted": True})

# Some trackers send GET S2S callbacks; mirror POST handler for query params
@app.get("/keitaro/postback")
async def keitaro_postback_get(
    request: Request,
    background_tasks: BackgroundTasks,
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

        background_tasks.add_task(_run_keitaro_postback_job, dict(data))
        return JSONResponse({"ok": True, "accepted": True})

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"GET postback handler failed: {e}")
        return {"ok": True}


@app.post("/underdog/domains/notify")
async def notify_expiring_domains_endpoint(
    payload: DomainNotifyRequest,
    authorization: str | None = Header(default=None),
):
    _require_internal_token(authorization, payload.token)
    stats = await underdog.notify_expiring_domains(
        dry_run=payload.dry_run,
        days=payload.days,
        bot_instance=orders_bot,
    )
    return {"ok": True, "dry_run": payload.dry_run, "stats": stats}


@app.post("/underdog/ip/notify")
async def notify_expiring_ips_endpoint(
    payload: IPNotifyRequest,
    authorization: str | None = Header(default=None),
):
    _require_internal_token(authorization, payload.token)
    stats = await underdog.notify_expiring_ips(
        dry_run=payload.dry_run,
        days=payload.days,
        bot_instance=orders_bot,
        admin_bot_instance=bot,
    )
    return {"ok": True, "dry_run": payload.dry_run, "stats": stats}


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
        logger.exception("Failed to list design subscribers: %s", e)
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

    assignment_stats = await underdog.notify_design_assignments(
        dry_run=payload.dry_run,
        bot_instance=design_bot,
    )
    completion_stats = await underdog.notify_design_completions(
        dry_run=payload.dry_run,
        bot_instance=design_bot,
    )
    sla_24h_stats = await underdog.notify_design_sla_24h(
        dry_run=payload.dry_run,
        bot_instance=design_bot,
    )
    not_in_progress_48h_stats = await underdog.notify_design_not_in_progress_48h(
        dry_run=payload.dry_run,
        bot_instance=design_bot,
    )
    return {
        "ok": True,
        "dry_run": payload.dry_run,
        "stats": {
            "assignments": assignment_stats,
            "completions": completion_stats,
            "sla_24h": sla_24h_stats,
            "not_in_progress_48h": not_in_progress_48h_stats,
        },
    }


@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        payload = await request.json()
        update = Update.model_validate(payload)
        await dp.feed_update(bot, update)
        return JSONResponse({"ok": True})
    except Exception as e:
        # Never 500 to Telegram: log and ACK to avoid retries blocking updates
        logger.exception(f"Webhook update handling failed: {e}")
        return JSONResponse({"ok": True})


@app.post(ORDERS_WEBHOOK_PATH)
async def orders_telegram_webhook(request: Request):
    if not settings.orders_bot_token or settings.orders_bot_token == settings.telegram_bot_token:
        return JSONResponse({"ok": True})
    try:
        payload = await request.json()
        update = Update.model_validate(payload)
        await orders_dp.feed_update(orders_bot, update)
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.exception(f"Orders webhook handling failed: {e}")
        return JSONResponse({"ok": True})


@app.post(DESIGN_WEBHOOK_PATH)
async def design_telegram_webhook(request: Request):
    if not settings.design_bot_token or settings.design_bot_token == settings.telegram_bot_token:
        return JSONResponse({"ok": True})
    try:
        payload = await request.json()
        update = Update.model_validate(payload)
        # Лог при каждом апдейте — если при /start в DesignBot здесь пусто, вебхук не доходит
        msg = update.message
        logger.info(
            "Design webhook received",
            update_id=update.update_id,
            chat_id=msg.chat.id if msg else None,
            text=(msg.text or "")[:50] if msg else None,
        )
        await design_dp.feed_update(design_bot, update)
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.exception(f"Design webhook handling failed: {e}")
        return JSONResponse({"ok": True})
