from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from loguru import logger
import json
import html
from .config import settings
from .bot import dp, bot, notify_buyer
from . import db
from aiogram.types import Update

# Sanitize webhook path for route decorator
WEBHOOK_PATH = settings.webhook_secret_path.strip()
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH

app = FastAPI(title="Keitaro Telegram Notifier")

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
    # Log event with final routed user id (even if None)
    try:
        await db.log_event(data, buyer_id)
    except Exception as e:
        logger.warning(f"Failed to log event: {e}")

    if not buyer_id:
        # no route matched and no admin configured
        return JSONResponse({"ok": True, "routed": False})

    # Map status to lead/sale only
    raw_status = (data.get("status") or data.get("action") or "").lower()
    status = "sale" if raw_status in ("sale", "approved", "conversion", "confirmed") else "lead"
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
    buyer_alias = None
    lead_alias = None
    if alias:
        buyer_alias = alias.get("buyer_id")
        lead_alias = alias.get("lead_id")

    lines = [
        f"<b>Status:</b> <code>{status}</code>",
        f"<b>Offer:</b> <code>{offer_id or '-'} | {offer_name or '-'}\n</code>",
        f"<b>SubID (user):</b> <code>{subid or '-'}\n</code>",
    ]
    if payout:
        lines.append(f"<b>Profit:</b> <code>{payout} {currency or ''}</code>")
    if sub_id_3:
        lines.append(f"<b>Sub ID 3:</b> <code>{sub_id_3}</code>")
    if sale_time:
        lines.append(f"<b>Conversion sale time:</b> <code>{sale_time}</code>")
    if campaign_name:
        lines.append(f"<b>Campaign:</b> <code>{campaign_name}</code>")

    # Append raw payload dump for debugging/verification
    raw_for_dump = {k: v for k, v in data.items() if str(k).lower() not in ("token", "auth", "authorization")}
    try:
        raw_sorted = dict(sorted(raw_for_dump.items(), key=lambda kv: str(kv[0])))
    except Exception:
        raw_sorted = raw_for_dump
    raw_json = json.dumps(raw_sorted, ensure_ascii=False, indent=2)
    raw_json_esc = html.escape(raw_json)
    # Keep total message under Telegram 4096 chars
    MAX_RAW = 3500
    if len(raw_json_esc) > MAX_RAW:
        extra = len(raw_json_esc) - MAX_RAW
        raw_json_esc = raw_json_esc[:MAX_RAW] + f"\n... (truncated {extra} chars)"

    text = "\n".join(lines) + "\n\n<b>Все поля (как пришли):</b>\n<pre><code>" + raw_json_esc + "</code></pre>"

    await notify_buyer(buyer_id, text)
    return {"ok": True, "routed": True, "buyer_id": buyer_id, "fallback": used_fallback}

# Some trackers send GET S2S callbacks; mirror POST handler for query params
@app.get("/keitaro/postback")
async def keitaro_postback_get(request: Request, authorization: str | None = Header(default=None)):
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
    await db.log_event(data, buyer_id)

    if not buyer_id:
        return JSONResponse({"ok": True, "routed": False})

    raw_status = (data.get("status") or data.get("action") or "").lower()
    status = "sale" if raw_status in ("sale", "approved", "conversion", "confirmed") else "lead"
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

    lines = [
        f"<b>Status:</b> <code>{status}</code>",
        f"<b>Offer:</b> <code>{offer_id or '-'} | {offer_name or '-'}\n</code>",
        f"<b>SubID (user):</b> <code>{subid or '-'}\n</code>",
    ]
    if payout:
        lines.append(f"<b>Profit:</b> <code>{payout} {currency or ''}</code>")
    if sub_id_3:
        lines.append(f"<b>Sub ID 3:</b> <code>{sub_id_3}</code>")
    if sale_time:
        lines.append(f"<b>Conversion sale time:</b> <code>{sale_time}</code>")
    if campaign_name:
        lines.append(f"<b>Campaign:</b> <code>{campaign_name}</code>")

    raw_for_dump = {k: v for k, v in data.items() if str(k).lower() not in ("token", "auth", "authorization")}
    try:
        raw_sorted = dict(sorted(raw_for_dump.items(), key=lambda kv: str(kv[0])))
    except Exception:
        raw_sorted = raw_for_dump
    raw_json = json.dumps(raw_sorted, ensure_ascii=False, indent=2)
    raw_json_esc = html.escape(raw_json)
    MAX_RAW = 3500
    if len(raw_json_esc) > MAX_RAW:
        extra = len(raw_json_esc) - MAX_RAW
        raw_json_esc = raw_json_esc[:MAX_RAW] + f"\n... (truncated {extra} chars)"

    text = "\n".join(lines) + "\n\n<b>Все поля (как пришли):</b>\n<pre><code>" + raw_json_esc + "</code></pre>"

    await notify_buyer(buyer_id, text)
    return {"ok": True, "routed": True, "buyer_id": buyer_id, "fallback": used_fallback}

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
