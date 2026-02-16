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

## IP expiry notifications
Triggered similarly via:

```
POST https://<your-app>/underdog/ip/notify
Authorization: Bearer <POSTBACK_TOKEN>
Content-Type: application/json

{
	"days": 7,        // optional horizon, default 7
	"dry_run": true,   // set false to actually message and mark telegram_sent
	"token": "..."     // optional fallback if you cannot send Authorization header
}
```

The helper fetches `/api/v2/ip`, filters records with `expires_at <= today + days` and `telegram_sent=0`, notifies matching buyers, then PATCH-es `/api/v2/ip/{id}/telegram-sent`.

CLI example:

```
python -m src.underdog --notify-ips --ip-days 7 --apply
```

## Ticket completion notifications
Triggered via:

```
POST https://<your-app>/underdog/tickets/notify
Authorization: Bearer <POSTBACK_TOKEN>
Content-Type: application/json

{
	"dry_run": true,   // set false to actually message and mark telegram_sent
	"token": "..."     // optional fallback if you cannot send Authorization header
}
```

The helper fetches `/api/v2/tickets`, filters records with `status='completed'` and `telegram_sent=false`, notifies matching buyers, then PATCH-es `/api/v2/tickets/{id}/telegram-sent`.

CLI example:

```
python -m src.underdog --notify-tickets --apply
python -m src.underdog --notify-tickets  # dry-run режим
python -m src.underdog --tickets  # получить список всех тикетов
```

Or use the script:

```
python scripts/notify_tickets.py
python scripts/notify_tickets.py --dry-run  # для тестирования
```

For Railway scheduled task (every 5 minutes):

1. **Using Railway Scheduler** (recommended):
   - Create a new service in Railway
   - Select "Scheduler" as the service type
   - Set the command: `python -m src.underdog --notify-tickets --apply`
   - Configure schedule: `*/5 * * * *` (every 5 minutes)

2. **Alternative**: Use the script:
   - Command: `python scripts/notify_tickets.py`
   - Schedule: `*/5 * * * *` (every 5 minutes)

Note: The `tickets` process in Procfile is for a persistent worker, not for scheduled tasks. Use Railway Scheduler for cron-like behavior.

## Design bot (заказы на дизайн: креативы и PWA)
Отдельный бот (`DESIGN_BOT_TOKEN`) получает заказы типов `pwaDesign` и `creative` из Underdog API и рассылает их всем, кто нажал `/start` в design-боте. В уведомлении передаётся статус заказа (обработка, выполнен, в работе, на правках, отдано на апрув, возвращено на доработку).

**Cron (например, Railway Scheduler):**
```bash
python -m src.underdog --notify-design --apply
```
Без `--apply` — dry-run (статистика без отправки).

**Проверка ответа API локально:**
```bash
# из корня проекта, с активированным venv и .env (UNDERDOG_EMAIL, UNDERDOG_PASSWORD)
python -m src.underdog --orders-design
```
Выведет JSON с заказами (pwaDesign + creative). То же для orders-бота: `python -m src.underdog --orders`.

**HTTP endpoint:** `POST /underdog/design/notify` (как у domains/notify: `dry_run`, `token`/Authorization).

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
