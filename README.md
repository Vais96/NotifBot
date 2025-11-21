# Keitaro -> Telegram Notifier Bot

A FastAPI + aiogram bot that receives Keitaro S2S postbacks and notifies the appropriate user in Telegram based on routing rules. Supports roles and teams.

## Features
- FastAPI endpoint to receive Keitaro postbacks (form or JSON)
- aiogram v3 bot with webhook
- MySQL 8 via aiomysql (can use your existing DB)
- Roles and teams (buyer/lead/head/admin) with role-based visibility

## Env vars (.env)
- TELEGRAM_BOT_TOKEN: Telegram bot token for the main bot (deposits, admin commands)
- ORDERS_BOT_TOKEN: Optional separate Telegram bot token used only for order notifications (falls back to TELEGRAM_BOT_TOKEN)
- ORDERS_WEBHOOK_PATH: HTTP path for the orders bot webhook (default `/telegram/orders-webhook`)
- DATABASE_URL: MySQL connection URL (mysql://user:pass@host:3306/dbname?charset=utf8mb4)
- BASE_URL: Public HTTPS URL of your deployed app (Railway)
- WEBHOOK_SECRET_PATH: Secret path for Telegram webhook (e.g. /telegram/secret)
- ADMINS: Comma-separated Telegram user IDs with admin rights
- PORT: Port to listen on (Railway provides)
- POSTBACK_TOKEN: Optional token to validate Keitaro postbacks via Authorization header
- YTDLP_COOKIES_PATH: Optional path to cookies.txt in Netscape format for YouTube downloads (если отсутствует — бот работает без авторизации)
- YTDLP_COOKIES: Альтернатива YTDLP_COOKIES_PATH — содержимое файла cookies.txt (многострочная строка)
- YTDLP_COOKIES_B64: То же, что YTDLP_COOKIES, но в base64 (удобно хранить в переменной окружения)
- YTDLP_IDENTITY_TOKEN: (опционально) значение заголовка `X-Youtube-Identity-Token` для аккаунта, если YouTube требует дополнительное подтверждение
- YTDLP_AUTH_USER: (опционально) значение заголовка `X-Goog-AuthUser` — чаще всего `0`, если используется основной профиль

## Orders bot setup
1. Create a second Telegram bot via BotFather and set its token in `ORDERS_BOT_TOKEN`.
2. Decide on a webhook path (or keep the default) and set `ORDERS_WEBHOOK_PATH`.
3. Deploy/restart the app so it registers both webhooks. The orders bot will now handle `/start`, store the user via `tg_users`, and immediately send any pending orders fetched from Underdog.
4. When buyers send `/start` in the orders bot, their username is saved and matched automatically, so future Underdog notifications are delivered privately instead of going to the deposits/admin bot.

## Domain expiry notifications
Trigger Underdog domain reminders (expiring domains with `telegram_notified=0`) via the internal endpoint:

```
POST https://<your-app>/underdog/domains/notify
Authorization: Bearer <POSTBACK_TOKEN>
Content-Type: application/json

{
	"days": 30,        // optional horizon, default 30
	"dry_run": true,   // set false to actually message and mark telegram_sent
	"token": "..."     // optional fallback if you cannot send Authorization header
}
```

The endpoint reuses `POSTBACK_TOKEN` for auth. When `dry_run` is false it sends formatted alerts via the orders bot and PATCH-es each domain as `telegram_notified` in Underdog.

For quick CLI checks you can also run:

```
python -m src.underdog --notify-domains --days 14 --apply
```

Without `--apply` the script prints stats without messaging buyers.

## Run locally (optional)
1. Create virtualenv and install deps
2. Copy `.env.example` to `.env` and fill values
3. Start server

## Keitaro Postback
Set postback URL in Keitaro to:
`https://<your-app>/keitaro/postback`

Send fields like: status, offer/offer_name, country/geo, source/traffic_source, payout, currency, clickid/click_id. If you set POSTBACK_TOKEN, add header `Authorization: Bearer <token>`.

## Telegram Webhook
On startup the app sets webhook to: `${BASE_URL}${WEBHOOK_SECRET_PATH}`. Locally you usually skip webhook; on Railway it's automatically set if BASE_URL is correct.

## Commands (role-aware)
- /listusers — users with roles and teams (admin/head see all, lead sees own team, buyer sees self)
- /listroutes — list routing rules (MVP: visible to all)
- /addrule <user_id> offer=OFF|* country=RU|* source=FB|* priority=0 — add a rule (admin/head)
- /setrole <telegram_id> <buyer|lead|head|admin> — set role (admin)
- /createteam <name> — create team (admin)
- /setteam <telegram_id> <team_id|-> — assign user to team or remove (admin/head)

## Deploy to Railway
- Create a new Railway project (or use existing)
- Use MySQL 8 (or external), set DATABASE_URL in mysql:// format
- Add variables: TELEGRAM_BOT_TOKEN, BASE_URL, WEBHOOK_SECRET_PATH, ADMINS, POSTBACK_TOKEN (optional)
- Railway will detect Procfile and run the app
