#!/usr/bin/env python3
"""Скрипт для отправки уведомлений о завершенных тикетах."""

import asyncio
import sys
import json
from pathlib import Path

# Добавляем корневую директорию в путь
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

from src import underdog
from loguru import logger


async def main():
    """Запускает отправку уведомлений о завершенных тикетах."""
    dry_run = "--dry-run" in sys.argv or "-d" in sys.argv
    
    try:
        stats = await underdog.notify_completed_tickets(dry_run=dry_run)
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        
        if not dry_run:
            print(f"\n✅ Отправлено уведомлений: {stats.get('notified_users', 0)}")
            print(f"📊 Всего тикетов: {stats.get('completed_tickets', 0)}")
        else:
            print(f"\n🔍 Dry-run режим: было бы отправлено {stats.get('notified_users', 0)} уведомлений")
    except Exception as e:
        logger.exception("Ошибка при отправке уведомлений о тикетах")
        print(f"❌ Ошибка: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
