"""Short link & domain persistence (tg_short_links, tg_short_domains)."""

from typing import Any, Dict, List, Optional, Tuple

from .pool import cursor, execute, fetch_all, fetch_one, transaction


async def create_short_link(
    slug: str,
    target_url: str,
    created_by: Optional[int],
    domain: str = "",
) -> bool:
    """Atomically insert a new short link for (domain, slug). Returns False if already exists."""
    async with cursor() as cur:
        await cur.execute(
            "INSERT IGNORE INTO tg_short_links(domain, slug, target_url, created_by) VALUES(%s, %s, %s, %s)",
            (domain, slug, target_url, created_by),
        )
        return cur.rowcount > 0


async def resolve_short_link(slug: str, host: str = "") -> Optional[Tuple[str, str]]:
    """Return (matched_domain, target_url) for slug, prioritizing exact host match then legacy '' domain."""
    norm_slug = (slug or "").strip().lower()
    if not norm_slug:
        return None
    norm_host = (host or "").strip().lower()
    candidates: List[str] = []
    if norm_host:
        candidates.append(norm_host)
        if norm_host.startswith("www.") and len(norm_host) > 4:
            candidates.append(norm_host[4:])
    if "" not in candidates:
        candidates.append("")

    placeholders = ",".join(["%s"] * len(candidates))
    rows = await fetch_all(
        f"SELECT domain, target_url FROM tg_short_links WHERE slug=%s AND domain IN ({placeholders})",
        (norm_slug, *candidates),
    )
    by_domain = {str(r[0] or "").lower(): str(r[1]) for r in rows if r and r[1]}
    for cand in candidates:
        if cand in by_domain:
            return cand, by_domain[cand]
    return None


async def get_short_link(slug: str, domain: str = "") -> Optional[str]:
    """Return target_url for slug (optionally scoped by host), or None if not found."""
    resolved = await resolve_short_link(slug, domain)
    return resolved[1] if resolved else None


async def increment_short_link_clicks(slug: str, domain: str = "") -> None:
    """Increment click counter for a short link."""
    await execute(
        "UPDATE tg_short_links SET clicks = clicks + 1 WHERE domain=%s AND slug=%s",
        (domain, slug),
    )


async def list_short_domains_for_user(user_id: int, *, include_all: bool = False) -> List[Dict[str, Any]]:
    """List active short-link domains visible to user_id (shared + own, or all for admin)."""
    if include_all:
        return await fetch_all(
            "SELECT domain, owner_id, is_shared, is_active, created_at "
            "FROM tg_short_domains WHERE is_active=1 ORDER BY is_shared DESC, domain ASC",
            dict_rows=True,
        )
    return await fetch_all(
        "SELECT domain, owner_id, is_shared, is_active, created_at "
        "FROM tg_short_domains WHERE is_active=1 AND (is_shared=1 OR owner_id=%s) "
        "ORDER BY (owner_id=%s) DESC, is_shared DESC, domain ASC",
        (user_id, user_id),
        dict_rows=True,
    )


async def get_short_domain(domain: str) -> Optional[Dict[str, Any]]:
    return await fetch_one(
        "SELECT domain, owner_id, is_shared, is_active, created_at FROM tg_short_domains WHERE domain=%s",
        (domain.strip().lower(),),
        dict_rows=True,
    )


async def add_short_domain(
    domain: str,
    owner_id: Optional[int],
    *,
    is_shared: bool = False,
    allow_takeover: bool = False,
) -> str:
    """Register or reactivate a short-link domain.

    Returns 'added', 'exists', or 'taken' (owned by another user).
    """
    norm = domain.strip().lower()
    async with transaction(dict_rows=True) as cur:
        await cur.execute(
            "SELECT domain, owner_id, is_shared, is_active FROM tg_short_domains WHERE domain=%s FOR UPDATE",
            (norm,),
        )
        row = await cur.fetchone()
        if not row:
            await cur.execute(
                "INSERT INTO tg_short_domains(domain, owner_id, is_shared, is_active) VALUES(%s, %s, %s, 1)",
                (norm, owner_id, 1 if is_shared else 0),
            )
            return "added"
        existing_owner = row.get("owner_id")
        is_active = bool(row.get("is_active"))
        same_owner = existing_owner is not None and owner_id is not None and int(existing_owner) == int(owner_id)
        if is_active:
            if same_owner or allow_takeover:
                if allow_takeover and is_shared != bool(row.get("is_shared")):
                    await cur.execute(
                        "UPDATE tg_short_domains SET is_shared=%s WHERE domain=%s",
                        (1 if is_shared else 0, norm),
                    )
                return "exists"
            return "taken"
        await cur.execute(
            "UPDATE tg_short_domains SET owner_id=%s, is_shared=%s, is_active=1 WHERE domain=%s",
            (owner_id, 1 if is_shared else 0, norm),
        )
        return "added"


async def set_short_domain_shared(domain: str, is_shared: bool) -> None:
    await execute(
        "UPDATE tg_short_domains SET is_shared=%s WHERE domain=%s",
        (1 if is_shared else 0, domain.strip().lower()),
    )


async def deactivate_short_domain(domain: str) -> None:
    await execute(
        "UPDATE tg_short_domains SET is_active=0 WHERE domain=%s",
        (domain.strip().lower(),),
    )
