"""Shared helpers for Keitaro- and domain-related workflows."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from .. import db, fb_csv
from ..keitaro import normalize_domain, parse_campaign_name

FLAG_CODE_LABELS = {
    "GREEN": "🟢 Зелёный",
    "YELLOW": "🟡 Жёлтый",
    "RED": "🔴 Красный",
}

FLAG_REASON_OVERRIDES = {
    "Spend ≥ $200 и FTD = 0": "🟥 Красный флаг",
    "CTR < 0.7%": "⚠️ Жёлтый флаг",
}

# ALIAS_OVERRIDES and canonical_alias_key moved to utils/domain.py to avoid circular import
# Re-export for backward compatibility
from ..utils.domain import ALIAS_OVERRIDES, canonical_alias_key

_DOMAIN_SPLIT_RE = re.compile(r"[\s,;]+")
MAX_DOMAINS_PER_REQUEST = 10


def format_flag_label(flag_id: Any, flags_by_id: Dict[int, Dict[str, Any]]) -> str:
    if flag_id is None:
        return "—"
    try:
        fid = int(flag_id)
    except Exception:
        return str(flag_id)
    row = flags_by_id.get(fid)
    if not row:
        return str(fid)
    code = (row.get("code") or "").upper()
    if code and code in FLAG_CODE_LABELS:
        return FLAG_CODE_LABELS[code]
    title = row.get("title")
    if title:
        return str(title)
    return code or str(fid)


def format_flag_decision(decision: Optional[fb_csv.FlagDecision]) -> str:
    if not decision:
        return "—"
    reasons = decision.reasons or []
    override_reason = next((reason for reason in reasons if reason in FLAG_REASON_OVERRIDES), None)
    label = FLAG_REASON_OVERRIDES.get(override_reason)
    if not label:
        label = FLAG_CODE_LABELS.get((decision.code or "").upper(), decision.code)
    if reasons:
        return f"{label} ({'; '.join(reasons)})"
    return label or "—"


def _lookup_inferred_buyer(
    campaign_name: Optional[str],
    alias_key: Optional[str],
    inferred: Dict[str, int],
) -> Optional[int]:
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


# resolve_campaign_assignments moved to utils/domain.py
# Import moved to avoid circular dependency - use direct import from utils.domain where needed


def extract_domains(raw_text: str, *, limit: int = MAX_DOMAINS_PER_REQUEST) -> Tuple[List[str], List[str]]:
    tokens = [t.strip() for t in _DOMAIN_SPLIT_RE.split(raw_text or "") if t.strip()]
    seen: Set[str] = set()
    domains: List[str] = []
    invalid: List[str] = []
    for token in tokens:
        normalized = normalize_domain(token)
        if not normalized:
            invalid.append(token)
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        domains.append(normalized)
        if len(domains) >= limit:
            break
    return domains, invalid


__all__ = [
    "ALIAS_OVERRIDES",
    "FLAG_CODE_LABELS",
    "FLAG_REASON_OVERRIDES",
    "MAX_DOMAINS_PER_REQUEST",
    "canonical_alias_key",  # Re-exported from utils.domain
    "extract_domains",
    "format_flag_decision",
    "format_flag_label",
    # resolve_campaign_assignments moved to utils.domain - import directly from there
]
