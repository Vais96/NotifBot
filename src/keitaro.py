import re
from typing import Dict, Optional
from urllib.parse import urlparse

_BRACKET_RE = re.compile(r"\[([^\]]+)\]")
_ARROW_SPLIT = re.compile(r"\s*(?:->|→|➡|=>)\s*")


def normalize_domain(value: str) -> str:
    """Normalize user-provided domain/url to bare host."""
    raw = (value or "").strip()
    if not raw:
        return ""
    probe = raw
    if not (probe.startswith("http://") or probe.startswith("https://")):
        probe = "http://" + probe
    parsed = urlparse(probe)
    host = parsed.netloc or parsed.path
    host = host.strip().lower()
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    host = host.strip()
    return host


def parse_campaign_name(name: str) -> Dict[str, Optional[str]]:
    """Extract prefix and domains from Keitaro campaign name."""
    value = name or ""
    before_bracket = value.split("[", 1)[0].strip()
    prefix = before_bracket.rstrip("_").strip() or None
    alias_key = None
    if prefix:
        alias_key = prefix.split("_", 1)[0].strip() or prefix
    alias_key = (alias_key or None)

    source_domain: Optional[str] = None
    target_domain: Optional[str] = None
    match = _BRACKET_RE.search(value)
    if match:
        inner = match.group(1)
        parts = _ARROW_SPLIT.split(inner)
        if parts:
            source_domain = normalize_domain(parts[0]) if parts[0] else ""
            if source_domain == "":
                source_domain = None
        if len(parts) > 1:
            target_domain = normalize_domain(parts[1]) if parts[1] else ""
            if target_domain == "":
                target_domain = None
    result: Dict[str, Optional[str]] = {
        "prefix": prefix,
        "alias_key": alias_key.lower() if alias_key else None,
        "source_domain": source_domain,
        "target_domain": target_domain,
    }
    return result
