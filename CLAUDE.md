# TG Bot — заметки по кодбазе

## Стек
Python 3.12+, FastAPI (uvicorn, `Procfile`: `uvicorn src.app:app`), aiogram 3.13 (webhook-режим), aiomysql (MySQL 8), loguru, httpx, pydantic-settings-подобный `src/config.py`. Деплой — Railway (push в `main` → автодеплой). Тесты — `unittest` в `tests/`, запуск `python -m pytest -q` (pytest — только dev, не в requirements).

## Три бота (`src/app.py`)
| Бот | Объекты | Webhook-путь (env) | Назначение |
|---|---|---|---|
| Основной | `dispatcher.py`: `bot`, `dp` | `WEBHOOK_SECRET_PATH` (дефолт `/telegram/webhook-secret`) | депозиты, меню, отчёты, админка; все хендлеры в `src/handlers/` |
| Orders | `orders_bot.py`: `orders_bot`, `orders_dp` | `ORDERS_WEBHOOK_PATH` (`/telegram/orders-webhook`) | Underdog: заказы, домены, IP, тикеты |
| Design | `design_bot.py`: `design_bot`, `design_dp` | `DESIGN_WEBHOOK_PATH` (`/telegram/design-webhook`) | Underdog design-таски: назначение/выполнение/SLA 24h/48h |

## Пайплайн депозита
Keitaro S2S → `POST|GET /keitaro/postback` (`_authorize_postback` по `POSTBACK_TOKEN`) → `BackgroundTasks` → `_run_keitaro_postback_job` → `_process_keitaro_postback`: dedupe продаж (`claim_keitaro_sale_postback`, `tg_keitaro_sale_dedupe`) → `db.log_event` (`tg_events`) → роутинг (alias из sub_id → `tg_aliases`/`tg_routes`, fallback) → fan-out через `dispatcher.notify_buyer`: байер, alias lead, лиды команды (кроме депозитов менторов), менторы команды (`tg_mentor_teams`), все head, хелперы байера (`tg_helper_buyer`). Текст — `services/keitaro_postbacks.py`, дневные счётчики/доход — `_resolve_daily_counter`/`_resolve_daily_revenue`.

## Модули
- `src/app.py` — FastAPI: postback, webhook'и, внутренние `/underdog/*/notify`, `/reports/daily-revenue` (auth `_require_internal_token`), фоновые циклы на startup (design notify, keitaro sync, new-admin sync, daily revenue).
- `src/db.py` — пул, `SCHEMA_SQL` + миграции через `SHOW COLUMNS`, все запросы.
- `src/underdog.py` — клиент Underdog API + нотифаеры (orders/design/domains/ips/tickets), CLI `python -m src.underdog`. Design-нотифаеры помечают sent через `_finish_design_delivery`: если доставлено хоть одному получателю; иначе не помечают и пишут warning через `admin_notify_throttle`.
- `src/handlers/` — хендлеры основного бота; регистрация импортом в `handlers/__init__.py`. `pending.py` (catch-all `@dp.message()`) импортируется последним. `fb.py` — загрузка FB CSV (pending `fb:await_csv`, `@dp.message(F.document)`) и drill-down колбэки `fbua:` (кабинет из загрузки) / `fbar:` (кабинет из отчёта `report:fb:accounts`), данные в `tg_ui_cache`.
- `src/services/` — `fb_uploads.py` (обработка CSV, флаги), `daily_revenue.py`, `keitaro_postbacks.py`, `campaigns.py`, `youtube.py`, `ads_workspace.py`.
- `src/utils/formatting.py` — единственный модуль форматирования (`fmt_money`, `fmt_percent`, `month_label_ru`, `as_decimal`, `format_flag_label`, `format_flag_decision`, `format_buyer_label`, `chunk_lines`). `src/utils/domain.py` — домены/алиасы.
- `src/fb_csv.py` — парсер выгрузки Ads Manager, `decide_flag`. `src/keitaro.py`, `keitaro_sync.py` — Keitaro Admin API. `new_admin_sync.py` — синк сотрудников из new Admin API. `telegram_rate_limit.py` — `limited_send_message` (429 backoff).

## Таблицы
`tg_users`, `tg_teams`, `tg_team_leads_extra`, `tg_mentor_teams`, `tg_helper_buyer`, `tg_routes`, `tg_aliases`, `tg_events`, `tg_keitaro_sale_dedupe`, `tg_pending_actions`, `tg_ui_cache`, `tg_report_filters`, `tg_kpi`, `tg_admin_notify_throttle`, `tg_design_bot_chats`, `tg_design_assignment_sent`, `tg_design_completion_sent`, `tg_design_sla_*`, `tg_design_not_in_progress_*`, `tg_underdog_contractor_telegram`, `keitaro_campaigns`. FB: `fb_*` (DDL — `sql/2025-11-03_fb_tables.sql`, не в `SCHEMA_SQL`).

## Конвенции
- loguru: только `{}`-плейсхолдеры (`logger.info("x={}", x)`), никаких `%s`; литеральные `{`/`}` в строке с аргументами — `{{ }}`. Не передавать `exc_info=` (в loguru это kwarg формата); в `except` — `logger.exception(...)`.
- Все боты с `parse_mode=HTML` по умолчанию → любой внешний текст (имена, username, alias, домены, Underdog-поля, `str(e)`) через `html.escape`.
- Роли: `buyer`, `lead`, `head`, `admin`, `mentor`, `helper` (`tg_users.role`); админ = `ADMIN_IDS` (env `ADMINS`), в reports дополнительно DB `role='admin'`.
- Pending-флоу: `db.set_pending_action(user_id, "<action>", payload)` → обработка в `handlers/pending.py` (текст) или профильном хендлере (документ — `handlers/fb.py`).
- Сообщения >4096 — резать `chunk_lines` (лимит 3500).
- Мёртвый код удалять, не комментировать. Новые env — в README.
