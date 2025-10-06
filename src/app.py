from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from loguru import logger
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
    try:
        # Keitaro can send form-encoded or JSON
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            data = await request.json()
        else:
            form = await request.form()
            data = {k: v for k, v in form.items()}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(400, "Invalid payload")

    # Optional token verification: set POSTBACK_TOKEN env and configure Keitaro header Authorization: Bearer <token>
    if settings.postback_token:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "Unauthorized")
        token = authorization.split(" ", 1)[1]
        if token != settings.postback_token:
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
    await db.log_event(data, buyer_id)

    if not buyer_id:
        # no route matched, silently accept
        return JSONResponse({"ok": True, "routed": False})

    # Map status to lead/sale only
    raw_status = (data.get("status") or data.get("action") or "").lower()
    status = "sale" if raw_status in ("sale", "approved", "conversion", "confirmed") else "lead"
    payout = data.get("profit") or data.get("payout") or data.get("revenue") or data.get("conversion_revenue")
    currency = data.get("currency") or data.get("revenue_currency") or data.get("payout_currency")
    offer_id = data.get("offer_id")
    offer_name = data.get("offer_name") or data.get("offer")
    subid = data.get("subid") or data.get("sub_id") or data.get("clickid") or data.get("click_id")
    sub_id_3 = data.get("sub_id_3") or data.get("subid3")
    sale_time = data.get("conversion_sale_time") or data.get("conversion_time")
    campaign_name = data.get("campaign_name")
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

    text = "\n".join(lines)

    await notify_buyer(buyer_id, text)
    return {"ok": True, "routed": True, "buyer_id": buyer_id}

# Some trackers send GET S2S callbacks; mirror POST handler for query params
@app.get("/keitaro/postback")
async def keitaro_postback_get(request: Request, authorization: str | None = Header(default=None)):
    # Parse query parameters as a dict
    data = dict(request.query_params)

    # Optional token verification identical to POST
    if settings.postback_token:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "Unauthorized")
        token = authorization.split(" ", 1)[1]
        if token != settings.postback_token:
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
    await db.log_event(data, buyer_id)

    if not buyer_id:
        return JSONResponse({"ok": True, "routed": False})

    raw_status = (data.get("status") or data.get("action") or "").lower()
    status = "sale" if raw_status in ("sale", "approved", "conversion", "confirmed") else "lead"
    payout = data.get("profit") or data.get("payout") or data.get("revenue") or data.get("conversion_revenue")
    currency = data.get("currency") or data.get("revenue_currency") or data.get("payout_currency")
    offer_id = data.get("offer_id")
    offer_name = data.get("offer_name") or data.get("offer")
    subid = data.get("subid") or data.get("sub_id") or data.get("clickid") or data.get("click_id")
    sub_id_3 = data.get("sub_id_3") or data.get("subid3")
    sale_time = data.get("conversion_sale_time") or data.get("conversion_time")
    campaign_name = data.get("campaign_name")

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

    text = "\n".join(lines)

    await notify_buyer(buyer_id, text)
    return {"ok": True, "routed": True, "buyer_id": buyer_id}

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
