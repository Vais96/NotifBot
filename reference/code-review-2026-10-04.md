# Code review TG Bot — 2026-10-04

Scope: src/ (21k lines). Line refs = state of the tree on 2026-10-04.

## P0 — ломает работу сейчас
1. **bot.py (1691 строк) — мёртвый код.** `app.py:14 from . import handlers` резолвится в пакет `handlers/`, модуль `src/handlers.py` (единственный импортёр `bot.py`) никем не импортируется. Следствия: нет `F.document`-хендлера → загрузка FB CSV из меню (`menu:uploadcsv` → pending `fb:await_csv`) упирается в `pending.py:14` и отвечает «Пришлите CSV файлом» на сам файл; кнопки `fbua:`/`fbar:` (fb_uploads.py:767) без обработчика → вечный спиннер. Действие: перенести `on_document_upload` + `fbua/fbar` в `handlers/fb.py`, удалить `bot.py`, `handlers.py`, `utils/formatting.py` или `services/formatting.py` (дубли).
2. **underdog.py:2230** loguru + `%s` + `{id}` в строке → `KeyError('id')` при неудачном PATCH; исключение прерывает цикл по остальным IP того же хендла → они не помечаются → повторные уведомления.
3. **Design 48h reminder spam-loop**: `DesignNotInProgress48hNotifier` (underdog.py:1841-1850) помечает sent только если дизайнер получил; если дизайнер не резолвится — рассылка во все чаты и админам каждый цикл. Остальные design-нотифаеры наоборот помечают sent даже когда все отправки упали.

## P1 — надёжность доставки депозитов
4. **Постбек подтверждается до обработки и живёт только в памяти.** `app.py:751` BackgroundTasks → 200 Keitaro → обработка в процессе. Рестарт/деплой Railway в этот момент = депозит потерян без ретрая (Keitaro видел 200). Плюс `claim_keitaro_sale_postback` пишется ДО отправки → если упало после claim, ретрай Keitaro отбросится как дубль. Решение: таблица `tg_inbound_postbacks(id, fingerprint, raw JSON, status pending/done/failed, attempts, created_at)`; в запросе — только INSERT; воркер обрабатывает; на старте добираем pending; `/admin/replay` по id/дате — закрывает кейс ручной переотправки 14 депозитов.
5. **Webhook Telegram обрабатывается inline** (`app.py:882 await dp.feed_update`) без `dp.errors()`. Долгие хендлеры (yt-dlp, keitaro sync, FB-отчёты) держат ответ → Telegram ретраит → дубли; исключение в хендлере = спиннер без ответа. Решение: `create_task(feed_update)` + глобальный errors-handler, который делает `call.answer("Ошибка")`.
6. **MySQL session tz не задан** (db.py:289). `TIMESTAMP`-колонки читаются в tz сервера, Python сравнивает с UTC — дневные счётчики, окно daily revenue, SLA 24/48h сдвинутся, если сервер не в UTC. Fix: `init_command="SET time_zone='+00:00'"`, плюс `pool_recycle=3600` (сейчас «MySQL server has gone away» после простоя).
7. **Underdog send-then-mark без локальной идемпотентности** (orders/domains/ips/tickets): любой сбой PATCH = повтор сообщения. `UnderdogClient.request` без retry на 5xx/timeout; TransportError не оборачивается. Нотифай-эндпоинты (`app.py:782-808`) выполняются inline, без lock → параллельные cron-запуски дублируют.
8. **Гонки SELECT-then-INSERT**: `admin_notify_throttle_allow_send` db.py:386-411 (при IntegrityError «allowing send» — троттл не троттлит), `set_alias` 1726. → `INSERT ... ON DUPLICATE KEY UPDATE` + rowcount.
9. **`upsert_user` реактивирует деактивированных** (db.py:671 `is_active=1`) при любом `/start` в любом из трёх ботов; `/unsubscribe` orders-бота деактивирует в основном. `design_bot.py:270` в группе пишет chat_id как user.
10. Rate limiter покрывает только underdog; fan-out депозитов (`dispatcher.notify_buyer`) и 55 других `send_message` — без лимита; `notify_buyer` глотает ошибки → daily report всегда «sent».

## P2 — корректность
- `log_event` db.py:1295: `float("1.5 USD")` → ValueError → событие не записано; `payout or revenue` теряет 0.
- 7 копий списка sale-статусов (db.py 1257/1305/1487/1657/2390/2477/2577), расходятся (`ftd`, `purchased`).
- Деньги: float + `int(round())` в daily_revenue/keitaro_postbacks (копейки, banker's rounding); FB-путь на Decimal — ок.
- `fb_csv._parse_decimal` ломает `"1,234.56"` → spend 0; `_detect_geo` ловит `PWA/CPA/USD`.
- Sync: username-матчинг с `@`/пробелами не совпадает (db.py 885 vs 927); stale `tg_team_leads_extra` не чистятся (C2); переименование команды = удаление + потеря mentor-подписок; `tg_team_leads_extra.team_id` PK → один extra lead на команду.
- HTML не экранируется (parse_mode=HTML): full_name, alias, domain, YouTube title, Underdog name/owner, текст исключений → `can't parse entities`, отправка падает. Нужен `safe_html()`.
- callback_data `alias:setbuyer:{alias}` без ограничения длины → `BUTTON_DATA_INVALID`, экран алиасов перестаёт открываться; split-unpack без проверок в users/mentors/helpers/reports.
- Нет индексов `tg_events(clickid, routed_user_id, created_at)` — dedupe и счётчики full scan. `VALUES()` в ON DUPLICATE KEY deprecated в MySQL 8.0.20+.
- fb_* таблицы нигде не создаются (SCHEMA_SQL их нет).
- Config: webhook path по умолчанию `/telegram/webhook`, `set_webhook` без `secret_token`; `POSTBACK_TOKEN=""` = постбеки без auth.
- Админ ≠ админ: 63 места проверяют `ADMIN_IDS` (env), reports.py:98 ещё и DB role admin. FB-отчёты без скоупа ролей. `/listteams` без гейта.

## P3 — структура
- db.py → пакет `db/` (pool, users, teams+sync, routing, sales_stats, aliases, design, fb_analytics…) + обёртки `fetch_one/fetch_all/execute/transaction` (~95 повторов boilerplate).
- underdog.py → `underdog/` (client, messages, stats, notifiers/{base,orders,design,domains,ips,tickets}, cli); 4 нотифаера дублируют `_alert_admins/_notify_admins_*`; `_is_*_sent` ×3 идентичны.
- handlers: `get_actor()` + `RoleFilter`, `Role(StrEnum)`, aiogram `CallbackData` вместо split, один `send_long()` вместо 6 чанкеров, pending-строки → FSM/enum, reports.py → period/fb/filters/kpi.
- Мёртвое: `replace_keitaro_campaigns`, `user_has_lead_privileges`, `fetch_orders_for_date`, `_build_design_order_message`, `teams.py` 14 недостижимых колбэков и pending-ветки `team:new/setlead`, `campaigns._lookup_inferred_buyer`.
- Нет файла памяти кодбазы (AGENTS.md пустой) — завести `CLAUDE.md`/`AGENTS.md`: стек, 3 бота, пайплайн депозита, таблицы, конвенции.
