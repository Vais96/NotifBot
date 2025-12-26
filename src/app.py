import asyncio
from datetime import datetime, timezone, date
from typing import Dict, Tuple, Optional

from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from loguru import logger
import json
from .config import settings
from .dispatcher import dp, bot, notify_buyer
from .orders_bot import orders_dp, orders_bot
from . import handlers  # noqa: F401 ensure handlers are registered
from . import db, underdog
from aiogram.types import Update, BotCommand
from pydantic import BaseModel, Field

# Sanitize webhook path for route decorator
WEBHOOK_PATH = settings.webhook_secret_path.strip()
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH

ORDERS_WEBHOOK_PATH = settings.orders_webhook_path.strip()
if not ORDERS_WEBHOOK_PATH.startswith("/"):
    ORDERS_WEBHOOK_PATH = "/" + ORDERS_WEBHOOK_PATH

app = FastAPI(title="Keitaro Telegram Notifier")

# Helpers to detect unexpanded placeholders and empty postbacks
def _is_unexpanded_placeholder(v: str) -> bool:
    try:
        s = v.strip()
        return s.startswith("{") and s.endswith("}")
    except Exception:
        return False

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


class DomainNotifyRequest(BaseModel):
    days: int = Field(default=30, ge=0, le=365)
    dry_run: bool = Field(default=True)
    token: Optional[str] = None


class IPNotifyRequest(BaseModel):
    days: int = Field(default=7, ge=0, le=365)
    dry_run: bool = Field(default=True)
    token: Optional[str] = None


def _require_internal_token(authorization: str | None, inline_token: Optional[str] = None) -> None:
    if not settings.postback_token:
        return
    supplied = None
    if authorization and authorization.startswith("Bearer "):
        supplied = authorization.split(" ", 1)[1].strip()
    if not supplied:
        supplied = inline_token.strip() if inline_token else None
    if not supplied:
        raise HTTPException(401, "Unauthorized")
    if supplied != settings.postback_token:
        raise HTTPException(403, "Forbidden")

_daily_counter_lock = asyncio.Lock()
_daily_counter_cache: Dict[int, Tuple[date, int]] = {}


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

def _has_meaningful_postback_fields(data: dict) -> bool:
    if not data:
        return False
    for k in MEANINGFUL_KEYS:
        if k in data:
            v = data.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if not s:
                continue
            if _is_unexpanded_placeholder(s):
                continue
            return True
    return False

# Unified message formatter used by both POST and GET handlers
def _build_notification_text(data: dict, daily_count: int | None = None, kpi_daily_goal: int | None = None) -> str:
    # Extract fields
    payout = data.get("profit") or data.get("payout") or data.get("revenue") or data.get("conversion_revenue")
    currency = data.get("currency") or data.get("revenue_currency") or data.get("payout_currency")
    offer_id = data.get("offer_id") or data.get("offer.id")
    offer_name = data.get("offer_name") or data.get("offer.name") or data.get("offer")
    subid = data.get("subid") or data.get("sub_id") or data.get("clickid") or data.get("click_id")
    sub_id_3 = data.get("sub_id_3") or data.get("subid3")
    sale_time = data.get("conversion_sale_time") or data.get("conversion.sale_time") or data.get("conversion_time")
    campaign_name = data.get("campaign_name") or data.get("campaign.name") or data.get("campaign")

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

    # Helpers: round profit to integer and format conversion time as yyyy-mm-dd / HH:MM in UTC
    def _format_payout(p):
        if p is None:
            return None
        try:
            s = str(p).replace(",", ".").strip()
            value = float(s)
            return str(int(round(value)))
        except Exception:
            return str(p)

    def _format_sale_time(v):
        from datetime import datetime, timezone
        if v is None:
            return None
        try:
            if isinstance(v, (int, float)):
                ts = float(v)
                if ts > 1e12:
                    ts = ts / 1000.0
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d / %H:%M")
            s = str(v).strip()
            if s.isdigit():
                ts = float(s)
                if ts > 1e12:
                    ts = ts / 1000.0
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d / %H:%M")
            s_norm = s.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s_norm)
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc)
                return dt.strftime("%Y-%m-%d / %H:%M")
            except Exception:
                pass
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.strftime("%Y-%m-%d / %H:%M")
                except Exception:
                    continue
        except Exception:
            pass
        return str(v)

    payout_fmt = _format_payout(payout)
    sale_time_fmt = _format_sale_time(sale_time)

    alias_name = None
    if campaign_name:
        alias_name = (str(campaign_name).split("_", 1)[0] or "").strip() or None

    # Pretty emoji-rich layout
    lines: list[str] = []
    lines.append(f"👤 <b>БАЙЕР:</b> <code>{alias_name or '-'}</code>")
    lines.append(f"🎯 <b>ОФФЕР:</b> <code>{offer_id or '-'} | {offer_name or '-'}</code>")
    if payout_fmt:
        lines.append(f"💰 <b>ПРОФИТ:</b> <code>{payout_fmt} {currency or ''}</code>")
    lines.append(f"🧩 <b>SubID:</b> <code>{subid or '-'}</code>")
    if campaign_name:
        lines.append(f"📣 <b>КАМПАНИЯ:</b> <code>{campaign_name}</code>")
    lines.append(f"🔢 <b>SubID3:</b> <code>{sub_id_3 or '-'}</code>")
    if daily_count is not None:
        lines.append(f"📈 <b>ДЕПОЗИТОВ ЗА ДЕНЬ:</b> <code>{daily_count}</code>")
    # KPI progress if available
    if (daily_count is not None) and (kpi_daily_goal is not None):
        lines.append(f"🎯 <b>Сегодня:</b> <code>{daily_count}/{kpi_daily_goal}</code> депозитов к цели")
    if sale_time_fmt:
        lines.append(f"🕒 <b>КОНВЕРСИЯ:</b> <code>{sale_time_fmt}</code> (UTC +0)")

    return "\n".join(lines)

@app.on_event("startup")
async def on_startup():
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
        logger.info(f"Webhook set to {url}")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")

    orders_token = settings.orders_bot_token
    if orders_token and orders_token != settings.telegram_bot_token:
        orders_url = settings.base_url.rstrip("/") + ORDERS_WEBHOOK_PATH
        try:
            await orders_bot.set_webhook(orders_url)
            logger.info(f"Orders webhook set to {orders_url}")
        except Exception as e:
            logger.error(f"Failed to set orders webhook: {e}")
    # Set command menu for the bot (helps users discover commands)
    try:
        await bot.set_my_commands([
            BotCommand(command="menu", description="Открыть меню"),
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

@app.on_event("shutdown")
async def on_shutdown():
    await db.close_pool()

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
async def keitaro_postback(request: Request, authorization: str | None = Header(default=None)):
    # Parse body leniently; if anything fails, continue with query params only
    content_type = (request.headers.get("content-type") or "").lower()
    data = {}
    if "application/json" in content_type:
        try:
            data = await request.json()
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

    # Optional token verification: set POSTBACK_TOKEN env and configure Keitaro header Authorization: Bearer <token>
    if settings.postback_token:
        supplied_token = None
        # Prefer header, but accept token/auth from params/body for trackers without header config
        if authorization and authorization.startswith("Bearer "):
            supplied_token = authorization.split(" ", 1)[1]
        if not supplied_token:
            supplied_token = data.get("token") or data.get("auth")
        if not supplied_token:
            raise HTTPException(401, "Unauthorized")
        if supplied_token != settings.postback_token:
            raise HTTPException(403, "Forbidden")

    # If no meaningful fields are present, return 200 with a simple ACK body
    if not _has_meaningful_postback_fields(data):
        return JSONResponse({"success": 200})

    # Try alias-based routing by campaign_name prefix
    campaign_name = data.get("campaign_name") or data.get("campaign")
    alias_key = None
    if campaign_name:
        alias_key = (campaign_name.split("_", 1)[0] or "").strip()
    alias = await db.find_alias(alias_key)

    buyer_id = alias.get("buyer_id") if alias else None
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
    # Log event with final routed user id only if it's a real performer (buyer/lead/mentor); avoid fallback and admin/head
    try:
        routed_id = buyer_id
        if used_fallback and routed_id:
            routed_id = None
        else:
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
    raw_status_value = (
        data.get("status")
        or data.get("conversion_status")
        or data.get("conversion.status")
        or data.get("status_name")
        or data.get("state")
        or data.get("action")
        or ""
    )
    raw_status = str(raw_status_value).lower()
    sale_like = {"sale", "approved", "approve", "confirmed", "confirm", "purchase", "purchased", "paid", "success"}
    is_sale = raw_status in sale_like
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
    # Helpers: round profit to integer and format conversion time as yyyy-mm-dd / HH:MM in UTC
    def _format_payout(p):
        if p is None:
            return None
        try:
            s = str(p).replace(",", ".").strip()
            value = float(s)
            return str(int(round(value)))
        except Exception:
            return str(p)

    def _format_sale_time(v):
        from datetime import datetime, timezone
        if v is None:
            return None
        try:
            if isinstance(v, (int, float)):
                ts = float(v)
                if ts > 1e12:
                    ts = ts / 1000.0
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d / %H:%M")
            s = str(v).strip()
            if s.isdigit():
                ts = float(s)
                if ts > 1e12:
                    ts = ts / 1000.0
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d / %H:%M")
            s_norm = s.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s_norm)
                # Convert to UTC; assume naive timestamps are UTC already
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc)
                return dt.strftime("%Y-%m-%d / %H:%M")
            except Exception:
                pass
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.strftime("%Y-%m-%d / %H:%M")
                except Exception:
                    continue
        except Exception:
            pass
        return str(v)

    payout_fmt = _format_payout(payout)
    sale_time_fmt = _format_sale_time(sale_time)

    # Build text via unified formatter (with optional daily deposits count)
    daily_count: int | None = None
    kpi_daily_goal: int | None = None
    if is_sale and stats_user_id is not None:
        db_daily_count: int | None = None
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
    text = _build_notification_text(data, daily_count=daily_count, kpi_daily_goal=kpi_daily_goal)

    # Determine recipients
    recipient_ids: set[int] = set()
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
    except Exception as e:
        logger.warning(f"Failed to expand recipients: {e}")

    # Send message to all recipients (deduped)
    for rid in recipient_ids:
        try:
            await notify_buyer(rid, text)
        except Exception as e:
            logger.warning(f"Notify failed for {rid}: {e}")
    return {"ok": True, "routed": bool(buyer_id), "buyer_id": buyer_id, "fallback": used_fallback, "sale": is_sale}

# Some trackers send GET S2S callbacks; mirror POST handler for query params
@app.get("/keitaro/postback")
async def keitaro_postback_get(request: Request, authorization: str | None = Header(default=None)):
    try:
        # Parse query parameters as a dict
        data = dict(request.query_params)

        # Optional token verification identical to POST
        if settings.postback_token:
            supplied_token = None
            if authorization and authorization.startswith("Bearer "):
                supplied_token = authorization.split(" ", 1)[1]
            if not supplied_token:
                supplied_token = data.get("token") or data.get("auth")
            if not supplied_token:
                raise HTTPException(401, "Unauthorized")
            if supplied_token != settings.postback_token:
                raise HTTPException(403, "Forbidden")

        # If no meaningful fields are present, return 200 with a simple ACK body
        if not _has_meaningful_postback_fields(data):
            return JSONResponse({"success": 200})

        campaign_name = data.get("campaign_name") or data.get("campaign")
        alias_key = None
        if campaign_name:
            alias_key = (campaign_name.split("_", 1)[0] or "").strip()
        alias = await db.find_alias(alias_key)
        buyer_id = alias.get("buyer_id") if alias else None
        if not buyer_id:
            buyer_id = await db.find_user_for_postback(
                offer=data.get("offer") or data.get("offer_name") or data.get("campaign") or data.get("campaign_name"),
                country=data.get("country") or data.get("geo"),
                source=data.get("source") or data.get("traffic_source_name") or data.get("traffic_source") or data.get("affiliate")
            )
        used_fallback = False
        if not buyer_id:
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

        # Same logic for GET: attribute only for buyer/lead/mentor/head; avoid fallback and admin
        routed_id = buyer_id
        if used_fallback and routed_id:
            routed_id = None
        else:
            try:
                users = await db.list_users()
                ru = next((u for u in users if u["telegram_id"] == routed_id), None)
                if ru and (ru.get("role") not in {"buyer", "lead", "mentor", "head"}):
                    routed_id = None
            except Exception:
                pass
        try:
            await db.log_event(data, routed_id)
        except Exception as e:
            logger.warning(f"GET postback log_event failed: {e}")
            routed_id = None

        # do not return early: admins must still receive notifications

        raw_status_value = (
            data.get("status")
            or data.get("conversion_status")
            or data.get("conversion.status")
            or data.get("status_name")
            or data.get("state")
            or data.get("action")
            or ""
        )
        raw_status = str(raw_status_value).lower()
        sale_like = {"sale", "approved", "approve", "confirmed", "confirm", "purchase", "purchased", "paid", "success"}
        is_sale = raw_status in sale_like
        payout = data.get("profit") or data.get("payout") or data.get("revenue") or data.get("conversion_revenue")
        currency = data.get("currency") or data.get("revenue_currency") or data.get("payout_currency")
        offer_id = data.get("offer_id") or data.get("offer.id")
        offer_name = data.get("offer_name") or data.get("offer.name") or data.get("offer")
        subid = data.get("subid") or data.get("sub_id") or data.get("clickid") or data.get("click_id")
        sub_id_3 = data.get("sub_id_3") or data.get("subid3")
        sale_time = data.get("conversion_sale_time") or data.get("conversion.sale_time") or data.get("conversion_time")
        campaign_name = data.get("campaign_name") or data.get("campaign.name")
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
        stats_user_id: int | None = None
        if routed_id is not None:
            try:
                stats_user_id = int(routed_id)
            except Exception as e:
                logger.warning(f"Failed to coerce routed user id {routed_id}: {e}")

        daily_count: int | None = None
        kpi_daily_goal: int | None = None
        if is_sale and stats_user_id is not None:
            db_daily_count: int | None = None
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
        text = _build_notification_text(data, daily_count=daily_count, kpi_daily_goal=kpi_daily_goal)

        # Determine recipients
        recipient_ids: set[int] = set()
        try:
            users = await db.list_users()
            # include admins always
            admins_db = [u for u in users if u.get("role") == "admin" and u.get("is_active")]
            for u in admins_db:
                recipient_ids.add(int(u["telegram_id"]))  # type: ignore
            if settings.admins:
                for aid in settings.admins:
                    try:
                        recipient_ids.add(int(aid))
                    except Exception:
                        pass
            # for sale events, include other recipients
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
        except Exception as e:
            logger.warning(f"Failed to expand recipients: {e}")

        for rid in recipient_ids:
            try:
                await notify_buyer(rid, text)
            except Exception as e:
                logger.warning(f"Notify failed for {rid}: {e}")
        return {"ok": True, "routed": bool(buyer_id), "buyer_id": buyer_id, "fallback": used_fallback, "sale": is_sale}
    except HTTPException:
        # propagate 4xx/5xx from our explicit raises
        raise
    except Exception as e:
        # Never 500 to Keitaro GET callbacks — acknowledge and log
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
    )
    return {"ok": True, "dry_run": payload.dry_run, "stats": stats}


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
