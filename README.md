# Keitaro -> Telegram Notifier Bot

A FastAPI + aiogram bot that receives Keitaro S2S postbacks and notifies the appropriate user in Telegram based on routing rules. Supports roles and teams.

## Features
- FastAPI endpoint to receive Keitaro postbacks (form or JSON)
- aiogram v3 bot with webhook
- MySQL 8 via aiomysql (can use your existing DB)
- Roles and teams (buyer/lead/head/admin) with role-based visibility

## Env vars (.env)
- TELEGRAM_BOT_TOKEN: Telegram bot token
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
