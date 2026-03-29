# Рассылка «истекающие IP» — выжимка для передачи коллеге

Файл сгенерирован для обмена без всего репозитория. Актуальная реализация живёт в **`src/underdog.py`** (класс `IPNotifier`). Отправка в Telegram с лимитами — **`src/telegram_rate_limit.py`** (`limited_send_message`).

## Как передать человеку

1. **Отправь только этот файл**  
   Путь в проекте: `reference/ip-notify-handoff.md`  
   Можно приложить к письму / в мессенджер или положить в общий диск.

2. **Или заархивировать папку `reference/`**  
   ```bash
   cd "/path/to/TG Bot" && zip -r ip-notify-handoff.zip reference/ip-notify-handoff.md
   ```

3. **Или дать доступ к репо** и указать строки в `underdog.py` (номера ниже могут сдвинуться после правок — ориентир по именам методов).

## Проверки до отправки пользователю (кратко)

| Условие | Действие |
|--------|----------|
| Пустой список с API | `return`, рассылки нет |
| `_is_ip_sent(ip_entry)` | Пропуск (уже помечено в Underdog) |
| Нет Telegram-ника у владельца (`not handle`) | Учёт в stats, алерт админам, без TG |
| Нет пользователя в БД бота (`not user`) | То же |
| `dry_run=True` | Только лог и preview в stats, **без** `limited_send_message` |

## После отправки

- Лог round-trip: `_log_telegram_send_roundtrip`
- Если `_telegram_message_confirmed(msg)` — считаем успех → `_mark_ip_entries_in_underdog` (PATCH `telegram-sent` по каждому IP)
- Иначе — ошибка в stats, алерт админам, PATCH не делаем
- Исключения Telegram — `except Forbidden/BadRequest` и общий `Exception`

---

## Код: пометка в Underdog после успешного TG

Источник: `src/underdog.py`, метод `IPNotifier._mark_ip_entries_in_underdog` (ориентировочно строки 1800–1885).

```python
    async def _mark_ip_entries_in_underdog(
        self,
        *,
        handle: str,
        telegram_id: int,
        ip_entries: List[Dict[str, Any]],
        stats: IPNotifierStats,
    ) -> None:
        """
        После успешной отправки в TG — PATCH telegram-sent для каждого IP.
        Ошибки только в stats.underdog_mark_failures; наружу не пробрасываем (иначе ложный send_failures).
        """
        try:
            for idx, entry in enumerate(ip_entries):
                raw = entry.get("raw") if isinstance(entry, dict) else None
                if not isinstance(raw, dict):
                    stats.errors += 1
                    stats.underdog_mark_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "ip_id": None,
                            "exc_type": "ValueError",
                            "error": "запись IP без dict raw — нет id для PATCH",
                        }
                    )
                    logger.warning(
                        "IP notify skip PATCH: raw не dict",
                        handle=handle,
                        entry_index=idx,
                    )
                    continue
                ip_id = _extract_ip_record_id(raw)
                if ip_id is None:
                    stats.errors += 1
                    stats.underdog_mark_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "ip_id": None,
                            "exc_type": "ValueError",
                            "error": "в объекте IP нет поля id/Id/ip_id",
                        }
                    )
                    logger.warning(
                        "IP notify skip PATCH: не извлечён id",
                        handle=handle,
                        raw_keys=list(raw.keys())[:40],
                    )
                    continue
                try:
                    await self.underdog.mark_ip_telegram_sent(ip_id)
                except Exception as exc:  # pragma: no cover
                    stats.errors += 1
                    stats.underdog_mark_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "ip_id": ip_id,
                            "exc_type": type(exc).__name__,
                            "error": str(exc),
                        }
                    )
                    logger.warning(
                        "Failed to mark IP telegram_sent: ip_id=%s path=PATCH /api/v2/ip/{id}/telegram-sent error=%s",
                        ip_id,
                        exc,
                    )
                if idx < len(ip_entries) - 1:
                    await asyncio.sleep(0.35)
        except Exception as exc:  # pragma: no cover
            stats.errors += 1
            stats.underdog_mark_failures.append(
                {
                    "handle": handle,
                    "telegram_id": telegram_id,
                    "ip_id": None,
                    "exc_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            logger.opt(exception=exc).error(
                "Unexpected error while marking IPs in Underdog after TG send",
                handle=handle,
                telegram_id=telegram_id,
            )
```

---

## Код: основной поток `notify_expiring_ips`

Источник: `src/underdog.py`, метод `IPNotifier.notify_expiring_ips` (ориентировочно строки 1887–2114).

Зависимости в том же файле: `_is_ip_sent`, `_resolve_owner_fields`, `_parse_date`, `_build_ip_notification`, `_telegram_message_confirmed`, `_log_telegram_send_roundtrip`, `_log_ip_notify_delivery_report`; модуль `db`; импорт `limited_send_message` из `telegram_rate_limit`.

```python
    async def notify_expiring_ips(self, *, dry_run: bool = True, days: int = 7) -> IPNotifierStats:
        """
        Список IP приходит с Underdog API уже отфильтрованным (кому нужно уведомление).
        Клиентский фильтр по горизонту дат не применяем; параметр days оставлен для совместимости API/CLI.
        """
        _ = days  # совместимость с POST /underdog/ip/notify и CLI --ip-days
        ips = await self.underdog.fetch_ips()
        stats = IPNotifierStats(total_ips=len(ips))
        if not ips:
            logger.info("IP notify: список IP пуст, рассылка не требуется")
            return stats

        logger.info(
            "IP notify",
            ips_count=len(ips),
            bot_orders_bot=bool(settings.orders_bot_token),
            note="список IP с API — без доп. фильтра по дате на нашей стороне",
        )
        per_handle: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        today = datetime.now(timezone.utc).date()

        for ip_entry in ips:
            if _is_ip_sent(ip_entry):
                continue
            handle, raw_handle, owner_name = _resolve_owner_fields(ip_entry)
            expires_at = _parse_date(
                ip_entry.get("expires_at")
                or ip_entry.get("expires")
                or ip_entry.get("expiration")
            )
            days_left = (expires_at - today).days if expires_at else None
            if not handle:
                stats.missing_contact += 1
                stats.unknown_items.append(
                    {
                        "ip": ip_entry.get("ip") or ip_entry.get("address"),
                        "expires_at": expires_at.isoformat() if expires_at else None,
                        "owner": owner_name,
                    }
                )
                await self._notify_admins_missing_ip(
                    handle=None,
                    entries=[{
                        "raw": ip_entry,
                        "expires_at": expires_at,
                        "days_left": days_left,
                        "display_handle": raw_handle,
                        "owner_name": owner_name,
                    }],
                    dry_run=dry_run,
                )
                continue
            per_handle[handle].append(
                {
                    "raw": ip_entry,
                    "expires_at": expires_at,
                    "days_left": days_left,
                    "display_handle": raw_handle,
                    "owner_name": owner_name,
                }
            )

        if not per_handle:
            _log_ip_notify_delivery_report(stats, dry_run=dry_run)
            return stats

        user_map = await db.fetch_users_by_usernames(list(per_handle.keys()))

        for handle, ip_entries in per_handle.items():
            user = user_map.get(handle)
            if not user:
                stats.unknown_user += len(ip_entries)
                stats.unknown_items.extend(
                    {
                        "ip": entry["raw"].get("ip") or entry["raw"].get("address"),
                        "expires_at": entry["expires_at"].isoformat() if entry["expires_at"] else None,
                        "handle": handle,
                    }
                    for entry in ip_entries
                )
                await self._notify_admins_missing_ip(
                    handle=handle,
                    entries=ip_entries,
                    dry_run=dry_run,
                )
                continue

            stats.matched_users += 1
            text = _build_ip_notification(ip_entries)
            if dry_run:
                logger.info(
                    "Dry-run: would notify about expiring IPs",
                    handle=handle,
                    telegram_id=user.get("telegram_id"),
                )
                stats.notified_users += 1
                stats.notified_ips += len(ip_entries)
                stats.dry_run_preview.append(
                    {
                        "handle": handle,
                        "telegram_id": user.get("telegram_id"),
                        "ips_count": len(ip_entries),
                        "ips": [
                            e["raw"].get("ip") or e["raw"].get("address")
                            for e in ip_entries
                        ],
                    }
                )
                continue

            try:
                telegram_id = int(user["telegram_id"])
                msg = await limited_send_message(self.bot, telegram_id, text=text)
                _log_telegram_send_roundtrip(
                    context="orders_bot_ip_expiration",
                    chat_id=telegram_id,
                    text=text,
                    msg=msg,
                    extra={
                        "handle": handle,
                        "ips_count": len(ip_entries),
                        "ips": [
                            e["raw"].get("ip") or e["raw"].get("address")
                            for e in ip_entries
                        ],
                    },
                )
                # В Underdog помечаем telegram_sent только если в ответе API ok === true (есть message_id)
                if not _telegram_message_confirmed(msg):
                    stats.errors += 1
                    stats.send_failures.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "exc_type": "TelegramResponse",
                            "error": "ответ без message_id, Underdog не обновлён",
                        }
                    )
                    logger.error(
                        "Telegram returned message without message_id — не помечаем IP telegram_sent в Underdog",
                        handle=handle,
                        telegram_id=telegram_id,
                    )
                    await self._notify_admins_ip_delivery_error(
                        handle=handle,
                        entries=ip_entries,
                        error_text="Telegram: ok!==true или нет message_id, Underdog не обновлён",
                        dry_run=dry_run,
                    )
                else:
                    stats.notified_users += 1
                    stats.notified_ips += len(ip_entries)
                    stats.delivered.append(
                        {
                            "handle": handle,
                            "telegram_id": telegram_id,
                            "message_id": getattr(msg, "message_id", None),
                            "ips_count": len(ip_entries),
                            "ips": [
                                e["raw"].get("ip") or e["raw"].get("address")
                                for e in ip_entries
                            ],
                        }
                    )
                    await self._mark_ip_entries_in_underdog(
                        handle=handle,
                        telegram_id=telegram_id,
                        ip_entries=ip_entries,
                        stats=stats,
                    )
            except (TelegramForbiddenError, TelegramBadRequest) as exc:
                stats.errors += 1
                stats.send_failures.append(
                    {
                        "handle": handle,
                        "telegram_id": user.get("telegram_id"),
                        "exc_type": type(exc).__name__,
                        "error": str(exc),
                        "ips": [
                            e["raw"].get("ip") or e["raw"].get("address")
                            for e in ip_entries
                        ],
                    }
                )
                logger.warning(
                    "IP notification not delivered (user blocked bot or chat not found)",
                    handle=handle,
                    telegram_id=user.get("telegram_id"),
                    error=str(exc),
                )
                await self._notify_admins_ip_delivery_error(
                    handle=handle,
                    entries=ip_entries,
                    error_text=str(exc),
                    dry_run=dry_run,
                )
            except Exception as exc:  # pragma: no cover
                stats.errors += 1
                stats.send_failures.append(
                    {
                        "handle": handle,
                        "telegram_id": user.get("telegram_id"),
                        "exc_type": type(exc).__name__,
                        "error": str(exc),
                        "ips": [
                            e["raw"].get("ip") or e["raw"].get("address")
                            for e in ip_entries
                        ],
                    }
                )
                logger.warning(
                    "Failed to send IP expiration message",
                    handle=handle,
                    exc_type=type(exc).__name__,
                    error=str(exc),
                )
                await self._notify_admins_ip_delivery_error(
                    handle=handle,
                    entries=ip_entries,
                    error_text=str(exc),
                    dry_run=dry_run,
                )

        if stats.unknown_items and dry_run:
            await self._alert_admins(stats.unknown_items)

        _log_ip_notify_delivery_report(stats, dry_run=dry_run)
        return stats
```
