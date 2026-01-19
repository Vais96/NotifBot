"""Domain-related utilities."""

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from .. import db
from ..keitaro import normalize_domain, parse_campaign_name

_DOMAIN_SPLIT_RE = re.compile(r"[\s,;]+")
MAX_DOMAINS_PER_REQUEST = 10

# Alias overrides - moved from services/campaigns.py to avoid circular import
ALIAS_OVERRIDES = {
    "ars": "arseny",
}


def canonical_alias_key(value: Optional[str]) -> Optional[str]:
    """Normalize alias key with overrides."""
    if not value:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    return ALIAS_OVERRIDES.get(normalized, normalized)


def extract_domains(raw_text: str) -> tuple[list[str], list[str]]:
    """Extract and normalize domains from text."""
    tokens = [t.strip() for t in _DOMAIN_SPLIT_RE.split(raw_text or "") if t.strip()]
    seen: set[str] = set()
    domains: list[str] = []
    invalid: list[str] = []
    for token in tokens:
        normalized = normalize_domain(token)
        if not normalized:
            invalid.append(token)
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        domains.append(normalized)
        if len(domains) >= MAX_DOMAINS_PER_REQUEST:
            break
    return domains, invalid


async def resolve_campaign_assignments(campaign_names: Set[str]) -> Dict[str, Dict[str, Any]]:
    """Resolve campaign assignments (buyer_id, alias_key, etc.) for given campaign names."""
    alias_keys: Dict[str, Optional[str]] = {}
    for name in campaign_names:
        meta = parse_campaign_name(name or "")
        alias_key = canonical_alias_key(meta.get("alias_key"))
        if not alias_key and name:
            fallback = name.split("_", 1)[0].strip() if "_" in name else name
            alias_key = canonical_alias_key(fallback)
        alias_keys[name] = alias_key
    alias_values = [val for val in alias_keys.values() if val]
    alias_map = await db.fetch_alias_map(alias_values)
    identifiers: Set[str] = set()
    for name in campaign_names:
        if name:
            identifiers.add(name)
    identifiers.update(alias_values)
    inferred = await db.infer_campaign_buyers(identifiers)
    result: Dict[str, Dict[str, Any]] = {}
    for name in campaign_names:
        alias_key = alias_keys.get(name)
        alias_row = alias_map.get(alias_key) if alias_key else None
        buyer_id: Optional[int] = None
        alias_lead_id: Optional[int] = None
        if alias_row:
            buyer_raw = alias_row.get("buyer_id")
            if buyer_raw is not None:
                try:
                    buyer_id = int(buyer_raw)
                except Exception:
                    buyer_id = None
            lead_raw = alias_row.get("lead_id")
            if lead_raw is not None:
                try:
                    alias_lead_id = int(lead_raw)
                except Exception:
                    alias_lead_id = None
        if buyer_id is None:
            buyer_id = _lookup_inferred_buyer(name, alias_key, inferred)
        result[name] = {
            "buyer_id": buyer_id,
            "alias_key": alias_key,
            "alias_lead_id": alias_lead_id,
            "alias_row": alias_row,
        }
    return result


def _lookup_inferred_buyer(campaign_name: Optional[str], alias_key: Optional[str], inferred: Dict[str, int]) -> Optional[int]:
    """Look up inferred buyer from campaign name or alias key."""
    for key in (campaign_name, alias_key):
        if not key:
            continue
        candidate = inferred.get(key.strip().lower())
        if candidate is not None:
            try:
                return int(candidate)
            except Exception:
                continue
    return None


async def render_domain_block(
    domain: str,
    alias_cache: dict[str, dict | None],
    user_cache: dict[int, dict | None]
) -> str:
    """Render domain block with campaign information."""
    rows = await db.find_campaigns_by_domain(domain)
    if not rows:
        return f"Кампании для домена <code>{domain}</code>:\n\nНе найдено."
    lines: list[str] = []
    for row in rows[:20]:
        # Всегда пытаемся извлечь алиас из названия кампании (часть до первого _)
        # Это более надежно, чем полагаться на alias_key из базы данных
        campaign_name = row.get("name") or ""
        alias_key = None
        if "_" in campaign_name:
            alias_key = campaign_name.split("_", 1)[0].strip()
        elif campaign_name:
            # Если нет подчеркивания, используем весь name как алиас
            alias_key = campaign_name.strip()
        
        # Если не удалось извлечь из названия, используем alias_key из базы
        if not alias_key:
            alias_key = row.get("alias_key")
        
        # Нормализуем алиас (уже в нижнем регистре)
        alias_key = canonical_alias_key(alias_key) if alias_key else None
        
        prefix = row.get("prefix") or alias_key or (row.get("name") or "-")
        alias_info = None
        if alias_key:
            # alias_key уже нормализован и в нижнем регистре
            if alias_key not in alias_cache:
                alias_cache[alias_key] = await db.find_alias(alias_key)
            alias_info = alias_cache[alias_key]
        mention = None
        if alias_info:
            # Используем buyer_id в первую очередь, так как это основной владелец кампании
            target_id = alias_info.get("buyer_id") or alias_info.get("lead_id")
            if target_id:
                tid = int(target_id)
                if tid not in user_cache:
                    user_cache[tid] = await db.get_user(tid)
                user = user_cache[tid]
                if user:
                    username = user.get("username")
                    fullname = user.get("full_name")
                    if username:
                        mention = f"@{username}"
                    elif fullname:
                        mention = str(fullname)
        if not mention:
            mention = prefix
        header = prefix if mention == prefix else f"{prefix} — {mention}"
        display_domain = row.get("source_domain") or domain
        lines.append(f"{header}\n{display_domain}")
    if len(rows) > 20:
        lines.append(f"… и ещё {len(rows) - 20}")
    return f"Кампании для домена <code>{domain}</code>:\n\n" + "\n\n".join(lines)


async def lookup_domains_text(raw_text: str) -> str:
    """Look up domains and return formatted text."""
    domains, invalid = extract_domains(raw_text)
    if not domains:
        if invalid:
            return f"Не удалось распознать домены: {', '.join(invalid[:5])}"
        return "Не найдено доменов в сообщении"
    alias_cache: dict[str, dict | None] = {}
    user_cache: dict[int, dict | None] = {}
    blocks: list[str] = []
    for domain in domains:
        block = await render_domain_block(domain, alias_cache, user_cache)
        blocks.append(block)
    message = "\n\n".join(blocks)
    if invalid:
        message += f"\n\n⚠️ Не удалось распознать: {', '.join(invalid[:5])}"
    return message
