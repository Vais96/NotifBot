import asyncio
from datetime import datetime, timezone

_daily_counter_lock = asyncio.Lock()
_daily_counter_cache = {}

async def _resolve_daily_counter(user_id: int, db_value: int | None) -> int:
    today = datetime.now(timezone.utc).date()
    base_value = db_value or 0
    async with _daily_counter_lock:
        cached = _daily_counter_cache.get(user_id)
        if not cached or cached[0] != today:
            display = base_value if base_value > 0 else 1
        else:
            _, last_value = cached
            if base_value > last_value:
                display = base_value
            else:
                display = last_value
        _daily_counter_cache[user_id] = (today, display)
    return display

async def main():
    uid = 42
    print('First call with db_value=5 -> expect 5')
    print(await _resolve_daily_counter(uid, 5))
    print('Second call with db_value=5 -> expect 5 (no increment)')
    print(await _resolve_daily_counter(uid, 5))
    print('Third call with db_value=4 -> expect 5 (DB stale)')
    print(await _resolve_daily_counter(uid, 4))
    print('Fourth call with db_value=6 -> expect 6 (DB increased)')
    print(await _resolve_daily_counter(uid, 6))

if __name__ == '__main__':
    asyncio.run(main())
