# Задача: привести бота в порядок по результатам ревью

Ты работаешь в репозитории TG Bot (FastAPI + aiogram 3 + aiomysql, деплой на Railway, три Telegram-бота: основной/депозиты, orders, design). Полный список находок с номерами строк — `reference/code-review-2026-10-04.md`. Прочитай его целиком перед началом. Номера строк актуальны на 2026-10-04; после твоих правок они сдвинутся — ориентируйся на имена функций.

## Правила работы

- Экономь токены: не пересказывай код, не делай полных перезаписей файлов там, где хватает точечного diff; отчёт после каждой фазы — 5–10 строк.
- Сначала создай `CLAUDE.md` в корне (если нет): стек, три бота и их webhook-пути, пайплайн депозита (Keitaro → `/postback` → `_process_keitaro_postback` → fan-out), список таблиц `tg_*`, конвенции (loguru только с `{}`-плейсхолдерами, `html.escape` для любого внешнего текста, `parse_mode=HTML`, роли). Обновляй его в конце каждой фазы — он должен отражать новую структуру модулей.
- Фазы выполняй строго по порядку, каждую заканчивай отдельным коммитом (`git add -A && git commit`). Не пушь — пуш делаю я.
- После каждой фазы: `python -m py_compile $(git ls-files '*.py')` и `pytest -q`. Падения чини до перехода дальше. Если тест ломается из-за изменённого поведения — обнови тест и объясни в отчёте почему.
- Поведение, видимое пользователям в Telegram (тексты, кнопки, кто что получает), не меняй без явного указания ниже. Если для фикса нужно решение по бизнес-логике — остановись и спроси, не угадывай.
- Не трогай `.env`, не меняй значения переменных окружения; новые переменные добавляй в `.env.example` и README.
- Мёртвый код удаляй, а не комментируй. Перед удалением убедись `grep -rn` по всему репо (включая `scripts/`, `tests/`, `*.md`), что нет ссылок.
- Каждую фазу завершай коротким списком: что изменено, что проверено, что требует ручной проверки на проде.

## Фаза 0 — P0, точечные фиксы

1. `src/underdog.py` ~2230: loguru-вызов с `%s` и `{id}` в одной строке → `KeyError`. Переписать на `{}`-плейсхолдеры. Затем `grep -rn 'logger\.\(info\|warning\|error\|debug\|exception\)(.*%s' src/` и починить все аналогичные места (как минимум `keitaro_sync.py` ~114, `db.py` ~2757).
2. Design-нотифаеры в `underdog.py`: `DesignAssignmentNotifier`, `DesignCompletionNotifier`, `DesignSLA24hNotifier` помечают `mark_*_sent` безусловно; `DesignNotInProgress48hNotifier` — только при доставке дизайнеру (spam-loop, если дизайнер не резолвится). Унифицировать: помечать sent, если доставлено хотя бы одному получателю (дизайнер, чат рассылки или админ); если никому — не помечать, но залогировать warning один раз через `admin_notify_throttle` с ключом по order_id.
3. Восстановить функционал, потерянный из-за мёртвого `src/bot.py`:
   - создать `src/handlers/fb.py`, перенести туда `on_document_upload` (`@dp.message(F.document)`, парсинг CSV через `asyncio.to_thread`) и обработчики колбэков `fbua:` и `fbar:` из `bot.py` (~192–300). Привязать к pending-action `fb:await_csv` так же, как это делает `pending.py` для остальных действий;
   - зарегистрировать в `src/handlers/__init__.py`;
   - сравнить `_send_fb_account_report` в `bot.py` (~707, есть drill-down кнопки и чанкинг) с живой версией в `handlers/reports.py` (~543, без них) и оставить в `reports.py` объединённую версию с чанкингом;
   - удалить `src/bot.py`, `src/handlers.py`; из `src/utils/formatting.py` и `src/services/formatting.py` оставить один модуль (`src/utils/formatting.py`), перевести на него `reports.py` (там третья копия `_fmt_money/_month_label_ru/...`). Проверить `grep -rn "from .bot\|src.bot\|services.formatting"`.
   - добавить тест `tests/test_fb_document_handler.py`: хендлер документа зарегистрирован в `dp`, pending `fb:await_csv` обрабатывается.

## Фаза 1 — надёжность депозитов и webhook

1. **Очередь входящих постбеков.** Новая таблица `tg_inbound_postbacks(id BIGINT AUTO_INCREMENT PK, fingerprint VARCHAR(64) NULL, raw JSON NOT NULL, status ENUM('pending','done','failed','duplicate'), attempts TINYINT DEFAULT 0, error TEXT NULL, created_at, processed_at)` + индекс по `(status, created_at)`. Эндпоинты `/postback` (POST и GET) в `app.py`: только валидация токена + INSERT + `return 200`, затем `asyncio.create_task(process_inbound(id))`. `_process_keitaro_postback` получает строку из очереди, при успехе — `done`, при исключении — `failed` с текстом ошибки. На старте (`startup`) добирать все `pending` и `failed` с `attempts < 3`. Dedupe-claim (`claim_keitaro_sale_postback`) перенести так, чтобы он выполнялся внутри обработки, а не до постановки в очередь, и помечать такие строки `duplicate`.
2. Эндпоинт `POST /admin/postbacks/replay` (auth через `_require_internal_token`): параметры `from`/`to` (UTC datetime) или список `ids`, `dry_run`. Переводит строки в `pending` и запускает обработку; ответ — счётчики. Задокументировать в README.
3. **Webhook Telegram**: во всех трёх `*_telegram_webhook` заменить `await dp.feed_update(...)` на `asyncio.create_task(...)` с удержанием ссылки на таск (set + `add_done_callback(discard)`), ответ 200 сразу. Добавить `@dp.errors()` (и для `orders_dp`, `design_dp`): логировать `logger.exception`, для `CallbackQuery` вызвать `call.answer("Ошибка, попробуйте ещё раз", show_alert=False)`, для `Message` — короткий ответ пользователю.
4. **MySQL**: в `db.init_pool` добавить `init_command="SET time_zone='+00:00'"`, `pool_recycle=3600`, `connect_timeout=10`; обернуть инициализацию в `asyncio.Lock`; при ошибке DDL сбрасывать `_pool = None`. Добавить миграции-индексы через существующий механизм `SHOW COLUMNS`/`SHOW INDEX`: `tg_events(clickid)`, `tg_events(created_at)`, `tg_events(routed_user_id, created_at)`.
5. **Гонки**: `admin_notify_throttle_allow_send` и `set_alias` переписать на один `INSERT ... ON DUPLICATE KEY UPDATE` с проверкой `rowcount`, без SELECT-then-INSERT. `get_contractor_telegram_id` — не вызывать `find_user_by_username` внутри уже захваченного соединения.
6. **Underdog**: в `UnderdogClient.request` — retry с экспоненциальной паузой (3 попытки) на 5xx, 429 и `httpx.TransportError`; все транспортные ошибки оборачивать в `UnderdogAPIError`. Нотифай-эндпоинты `/underdog/*/notify` выполнять через `create_task` под `asyncio.Lock` на каждый тип (как `keitaro_sync._sync_lock`); при занятой блокировке отвечать `{"ok": false, "busy": true}`. Таблица `tg_underdog_sent(kind ENUM('order','domain','ip','ticket'), external_id VARCHAR(64), chat_id BIGINT, sent_at, PK(kind, external_id, chat_id))`: писать перед PATCH в Underdog, проверять перед отправкой — защита от дублей, когда PATCH упал.
7. `upsert_user`: не поднимать `is_active=1` при `ON DUPLICATE KEY` — реактивация только явным действием (`set_active` / sync). `/unsubscribe` в orders-боте — отдельный флаг `orders_opt_out`, а не `is_active=0`. `design_bot` в группах не должен писать `chat_id` в `tg_users`.
8. `dispatcher.notify_buyer`: отправка через `limited_send_message` из `telegram_rate_limit`, исключение пробрасывать наверх (fan-out в `app.py` и `daily_revenue_report` уже ловят и считают). `telegram_rate_limit._ensure_status_capturing_session` убрать — сессию со статусом настраивать при создании `Bot(...)` в `dispatcher.py`/`orders_bot.py`/`design_bot.py`.

## Фаза 2 — корректность

1. `src/utils/html.py`: `safe(s) -> str` (= `html.escape(str(s or ""))`) и `user_label(row)`. Применить ко всем f-строкам с внешним текстом: `full_name`, `username`, alias, domain, YouTube title, Underdog `name/owner/domain/label`, `str(e)` в сообщениях об ошибке. Список файлов — в ревью, раздел «HTML». Добавить тест: имя с `<b>&` не ломает рендер сообщения.
2. `src/constants.py`: `SALE_STATUSES` (единый набор, включить `ftd` и `purchased`, согласовать с бизнес-логикой — если сомнение, спроси), `class Role(StrEnum)`, префиксы callback. Заменить все 7 копий списков sale-статусов в `db.py` на хелпер `_sale_status_sql()`.
3. `db.log_event`: безопасный парсер payout (`Decimal`, регулярка на число, при ошибке — `NULL` + warning, но событие записывается); `payout`/`revenue` проверять через `is not None`, а не `or`.
4. Деньги: в `daily_revenue._fmt_amount` и `keitaro_postbacks._format_payout` — `Decimal` + `ROUND_HALF_UP`; формат вывода не менять.
5. `fb_csv._parse_decimal`: корректно разбирать `1,234.56`, `1 234,56`, `1.234,56`; `_detect_geo` — исключить `PWA/CPA/USD/EUR/RUB` и проверять по списку ISO-кодов стран.
6. Алиасы: callback_data `alias:setbuyer:{alias}` → индекс/ID (через `tg_ui_cache` как в reports или `tg_aliases.id`). Все `call.data.split(":")` распаковки — через aiogram `CallbackData`-фабрики или проверку длины с `call.answer("Устаревшая кнопка")`.
7. Сообщения > 4096: один `send_long(bot, chat_id, lines_or_text, reply_markup=None)` в `utils/formatting.py`, заменить шесть локальных чанкеров (`utils.chunk_lines`, `users._chunk_text_lines`, `orders_bot._chunk_lines`, `reports._chunk_lines`, `reports._send_long_html`, `fb_uploads._notify_flag_updates`).
8. Роли: единая `is_admin(user_id, row=None)` — `ADMIN_IDS` ИЛИ DB `role='admin'` (это текущее поведение reports; подтвердить у меня перед применением). FB-отчёты (`report:fb:*`) пропускать через `_resolve_scope_user_ids`. `/listteams` — только lead/head/admin. `cb_set_role` — валидировать роль по `Role`.
9. Sync (`new_admin_sync` + `db.sync_employee_directory`): нормализовать username одинаково (`strip().lstrip("@").lower()`) и в ключах, и в lookups; удалять `tg_team_leads_extra` для пользователей, у которых больше нет observer-команд; при дубликате username в `tg_users` — warning с обоими telegram_id и пропуск, а не «последний победил». Переименование команды пока не трогать — опиши риск в отчёте.
10. `config.py`: `webhook_secret_path` — один дефолт; `set_webhook(..., secret_token=...)` + проверка заголовка `X-Telegram-Bot-Api-Secret-Token` в webhook-хендлерах (новая переменная `TELEGRAM_WEBHOOK_SECRET`, если пустая — поведение как сейчас); пустой `POSTBACK_TOKEN` — `logger.warning` на старте; `int(os.getenv(...))` через хелпер с именем переменной в ошибке; токены/пароли как `SecretStr`.
11. `db.py`: все `VALUES(col)` в `ON DUPLICATE KEY UPDATE` → алиас строки `INSERT ... AS new ... = new.col` (MySQL 8.0.20+). Многошаговые функции (`remove_helper_and_promote_to_buyer`, `deactivate_user`, `set_ui_cache_list` через `executemany`) — в транзакцию.
12. `fb_*` таблицы: если DDL есть вне репо — вынести в `SCHEMA_SQL`; если нет — спроси меня, где он.

## Фаза 3 — структура (только после фаз 0–2, каждый пункт — отдельный коммит)

1. `src/db.py` → пакет `src/db/`: `pool.py` (+ обёртки `fetch_one/fetch_all/execute/transaction`), `users.py`, `teams.py`, `directory_sync.py`, `routing.py`, `sales_stats.py`, `aliases.py`, `design.py`, `pending.py`, `mentors.py`, `ui_cache.py`, `keitaro_campaigns.py`, `fb_analytics.py`, `inbound.py`. `src/db/__init__.py` реэкспортирует всё, чтобы `from . import db` и `db.func()` продолжали работать. Boilerplate `pool.acquire/cursor` заменить на обёртки. `sync_employee_directory` и `aggregate_sales` разбить на шаги.
2. `src/underdog.py` → пакет `src/underdog/`: `client.py`, `common.py`, `messages.py`, `stats.py`, `telegram_confirm.py`, `notifiers/base.py` (общие `_send_to_admins`, `_alert_admins_digest`, `_resolve_recipients`, `_send_and_confirm`), `notifiers/{orders,design,domains,ips,tickets}.py`, `cli.py` (`python -m src.underdog` должен работать как раньше). Три идентичных `_is_*_sent` → один; 4 копии `_alert_admins`/`_notify_admins_*` → базовый класс; 8 обёрток `notify_*` → `_run_notifier`.
3. handlers: `handlers/common.py` с `get_actor(user_id) -> Actor(role, is_admin, team_ids, lead_team_ids)` и `RoleFilter`; `reports.py` → `handlers/reports/{period,fb,filters,kpi}.py`; `cb_report_today/yesterday/week` + `on_today/...` → один `_run_period_report`; pending-строки → `PendingAction(StrEnum)` + реестр обработчиков вместо `if action ==` лестницы (FSM aiogram — только если не ломает текущие тесты).
4. Удалить мёртвое после `grep`: `replace_keitaro_campaigns`, `user_has_lead_privileges`, `list_telegram_ids_tg_users`, `set_contractor_telegram`, `fetch_fb_monthly_summary`, `fetch_orders_for_date`, `fetch_yesterday_orders`, `_build_design_order_message`, `services/campaigns._lookup_inferred_buyer`, недостижимые колбэки и `_team_*_picker_kb` в `teams.py`, pending-ветки `team:new/team:setlead/myteam:add`. Убрать debug `logger.info` спам в `_send_period_report`.
5. Унифицировать тексты ошибок прав («Нет прав») и токены отмены (`-`, `отмена`, `cancel`) во всех pending-флоу; дополнить `/help`.

## Критерии готовности

- `pytest -q` зелёный, `py_compile` без ошибок, `grep -rn "src.bot\|from .bot"` пусто.
- Загрузка CSV из меню работает (хендлер документа зарегистрирован), кнопки `fbua:`/`fbar:` обрабатываются.
- Постбек при `kill -9` процесса между 200 и отправкой переживает рестарт (строка в `tg_inbound_postbacks` остаётся `pending` и добирается на старте) — покрыть тестом на уровне функций.
- `CLAUDE.md` описывает итоговую структуру модулей и конвенции.
