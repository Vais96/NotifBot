from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from loguru import logger
from .config import settings
from .bot import dp, bot, notify_buyer
from . import db
from aiogram.types import Update

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

    buyer_id = await db.find_user_for_postback(
        offer=data.get("offer") or data.get("offer_name") or data.get("campaign") or data.get("campaign_name"),
        country=data.get("country") or data.get("geo"),
        source=data.get("source") or data.get("traffic_source_name") or data.get("traffic_source") or data.get("affiliate")
    )
    await db.log_event(data, buyer_id)

    if not buyer_id:
        # no route matched, silently accept
        return JSONResponse({"ok": True, "routed": False})

    status = data.get("status") or data.get("action") or "conversion"
    # Keitaro may use revenue/profit fields; accept them as payout fallback
    payout = data.get("payout") or data.get("revenue") or data.get("conversion_revenue") or data.get("profit")
    currency = data.get("currency")
    offer = data.get("offer") or data.get("offer_name") or data.get("campaign_name")
    country = data.get("country") or data.get("geo")
    clickid = data.get("clickid") or data.get("click_id") or data.get("subid")

    lines = [
        f"<b>{status.upper()}</b>",
        f"Offer: <code>{offer or '-'}\n</code>",
        f"Geo: <code>{country or '-'}\n</code>",
        f"ClickID: <code>{clickid or '-'}\n</code>",
    ]
    if payout:
        lines.append(f"Payout: <b>{payout} {currency or ''}</b>")
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

    buyer_id = await db.find_user_for_postback(
        offer=data.get("offer") or data.get("offer_name") or data.get("campaign") or data.get("campaign_name"),
        country=data.get("country") or data.get("geo"),
        source=data.get("source") or data.get("traffic_source_name") or data.get("traffic_source") or data.get("affiliate")
    )
    await db.log_event(data, buyer_id)

    if not buyer_id:
        return JSONResponse({"ok": True, "routed": False})

    status = data.get("status") or data.get("action") or "conversion"
    payout = data.get("payout") or data.get("revenue") or data.get("conversion_revenue") or data.get("profit")
    currency = data.get("currency")
    offer = data.get("offer") or data.get("offer_name") or data.get("campaign_name")
    country = data.get("country") or data.get("geo")
    clickid = data.get("clickid") or data.get("click_id") or data.get("subid")

    lines = [
        f"<b>{status.upper()}</b>",
        f"Offer: <code>{offer or '-'}\n</code>",
        f"Geo: <code>{country or '-'}\n</code>",
        f"ClickID: <code>{clickid or '-'}\n</code>",
    ]
    if payout:
        lines.append(f"Payout: <b>{payout} {currency or ''}</b>")
    text = "\n".join(lines)

    await notify_buyer(buyer_id, text)
    return {"ok": True, "routed": True, "buyer_id": buyer_id}

@app.post(settings.webhook_secret_path)
async def telegram_webhook(request: Request):
    payload = await request.json()
    update = Update.model_validate(payload)
    await dp.feed_update(bot, update)
    return JSONResponse({"ok": True})
