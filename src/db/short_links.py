"""Short link persistence (tg_short_links) for built-in /r/<slug> redirects."""

from typing import Optional

from .pool import cursor, execute, fetch_one


async def create_short_link(slug: str, target_url: str, created_by: Optional[int]) -> bool:
    """Atomically insert a new short link. Returns False if slug already exists."""
    async with cursor() as cur:
        await cur.execute(
            "INSERT IGNORE INTO tg_short_links(slug, target_url, created_by) VALUES(%s, %s, %s)",
            (slug, target_url, created_by),
        )
        return cur.rowcount > 0


async def get_short_link(slug: str) -> Optional[str]:
    """Return target_url for slug, or None if not found."""
    row = await fetch_one("SELECT target_url FROM tg_short_links WHERE slug=%s", (slug,))
    return str(row[0]) if row and row[0] else None


async def increment_short_link_clicks(slug: str) -> None:
    """Increment click counter for a short link."""
    await execute("UPDATE tg_short_links SET clicks = clicks + 1 WHERE slug=%s", (slug,))
