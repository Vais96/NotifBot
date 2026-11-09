"""Campaign ownership helpers (FB CSV uploads)."""

from typing import Any, Dict, Optional, Set

from .. import db
from ..keitaro import parse_campaign_name

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
