"""Comment link builder (Binom sub1..subN + fbp) and built-in custom-slug shortener."""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

from loguru import logger

from .. import db
from ..config import settings
from ..utils.html import safe

_MACRO_RE = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")
_SUB_KEY_RE = re.compile(r"^sub(\d+)$", re.IGNORECASE)
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,62}[a-z0-9]$|^[a-z0-9]{2}$")
_DOMAIN_RE = re.compile(r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9-]{2,63}$")
_SKIP_TOKENS = {".", "skip", "пропуск", "пропустить", "none", "нет"}
_RANDOM_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789"
RESERVED_SLUGS = frozenset({
    "r",
    "health",
    "db",
    "keitaro",
    "admin",
    "underdog",
    "reports",
    "telegram",
    "api",
    "docs",
    "redoc",
    "openapi",
})

# Macro name (inside {{...}}) -> canonical field name
_MACRO_TO_FIELD: Dict[str, str] = {
    "campaign.name": "campaign_name",
    "campaign_name": "campaign_name",
    "adset.name": "adset_name",
    "adset_name": "adset_name",
    "ad.name": "ad_name",
    "ad_name": "ad_name",
    "account.id": "account_id",
    "account_id": "account_id",
    "accountid": "account_id",
    "cab.id": "account_id",
    "cab_id": "account_id",
    "campaign.id": "campaign_id",
    "campaign_id": "campaign_id",
    "adset.id": "adset_id",
    "adset_id": "adset_id",
    "ad.id": "ad_id",
    "ad_id": "ad_id",
    "placement": "placement",
    "site_source_name": "site_source_name",
    "pixel": "pixel",
    "fbp": "pixel",
}

# Canonical field -> default Binom query parameter name
FIELD_TO_PARAM: Dict[str, str] = {
    "sub1": "sub1",
    "campaign_name": "sub2",
    "ad_name": "sub3",
    "geo": "sub4",
    "adset_name": "sub5",
    "account_id": "sub6",
    "campaign_id": "sub7",
    "adset_id": "sub8",
    "ad_id": "sub9",
    "placement": "sub10",
    "site_source_name": "sub11",
    "pixel": "fbp",
}

PARAM_LABELS_RU: Dict[str, str] = {
    "sub1": "Байер (sub1)",
    "sub2": "Кампания (sub2)",
    "sub3": "Объявление (sub3)",
    "sub4": "Гео (sub4)",
    "sub5": "Адсет (sub5)",
    "sub6": "ID каба (sub6)",
    "sub7": "Campaign ID (sub7)",
    "sub8": "Adset ID (sub8)",
    "sub9": "Ad ID (sub9)",
    "sub10": "Плейсмент (sub10)",
    "sub11": "Источник (sub11)",
    "fbp": "Пиксель (fbp)",
    "pixel": "Пиксель (pixel)",
}

GUIDED_FIELDS: Tuple[str, ...] = (
    "campaign_name",
    "adset_name",
    "ad_name",
    "account_id",
    "pixel",
)

GUIDED_PROMPTS: Dict[str, Tuple[str, str]] = {
    "campaign_name": (
        "Название кампании",
        "Например: <code>77_TH_1740624179668966_15.11</code>",
    ),
    "adset_name": (
        "Название адсета",
        "Например: <code>adset_1</code>",
    ),
    "ad_name": (
        "Название объявления (крео)",
        "Например: <code>creo_1</code>",
    ),
    "account_id": (
        "ID рекламного кабинета",
        "Например: <code>123456789012345</code>",
    ),
    "pixel": (
        "ID пикселя (fbp)",
        "Например: <code>1491853589548065</code>",
    ),
}

_KEY_ALIASES: Dict[str, str] = {
    "кампания": "campaign_name",
    "название кампании": "campaign_name",
    "campaign": "campaign_name",
    "campaign_name": "campaign_name",
    "campaign.name": "campaign_name",
    "cmp": "campaign_name",
    "адсет": "adset_name",
    "название адсета": "adset_name",
    "группа": "adset_name",
    "adset": "adset_name",
    "adset_name": "adset_name",
    "adset.name": "adset_name",
    "объявление": "ad_name",
    "название объявления": "ad_name",
    "крео": "ad_name",
    "креатив": "ad_name",
    "ad": "ad_name",
    "ad_name": "ad_name",
    "ad.name": "ad_name",
    "creo": "ad_name",
    "каб": "account_id",
    "id каба": "account_id",
    "айди каба": "account_id",
    "аккаунт": "account_id",
    "id аккаунта": "account_id",
    "account": "account_id",
    "account_id": "account_id",
    "accountid": "account_id",
    "account.id": "account_id",
    "cab": "account_id",
    "cab_id": "account_id",
    "пиксель": "pixel",
    "id пикселя": "pixel",
    "айди пикселя": "pixel",
    "pixel": "pixel",
    "pixel_id": "pixel",
    "fbp": "pixel",
    "байер": "sub1",
    "алиас": "sub1",
    "buyer": "sub1",
    "alias": "sub1",
    "гео": "geo",
    "страна": "geo",
    "geo": "geo",
    "country": "geo",
    "id кампании": "campaign_id",
    "айди кампании": "campaign_id",
    "campaign_id": "campaign_id",
    "campaign.id": "campaign_id",
    "id адсета": "adset_id",
    "айди адсета": "adset_id",
    "adset_id": "adset_id",
    "adset.id": "adset_id",
    "id объявления": "ad_id",
    "айди объявления": "ad_id",
    "ad_id": "ad_id",
    "ad.id": "ad_id",
    "плейсмент": "placement",
    "placement": "placement",
    "источник": "site_source_name",
    "source": "site_source_name",
    "site_source_name": "site_source_name",
    "slug": "custom_slug",
    "слаг": "custom_slug",
    "название": "custom_slug",
    "название ссылки": "custom_slug",
    "короткая ссылка": "custom_slug",
    "домен": "short_domain",
    "domain": "short_domain",
    "короткий домен": "short_domain",
}


class InvalidBaseLinkError(ValueError):
    """Raised when the user's base link cannot be parsed as an HTTP(S) URL."""


class InvalidSlugError(ValueError):
    """Raised when a custom short-link slug has invalid syntax."""


class InvalidShortDomainError(ValueError):
    """Raised when a custom short-link domain has invalid syntax."""


class ShortLinkTakenError(RuntimeError):
    """Raised when the requested custom slug already exists for that domain."""


class LinkShortenError(RuntimeError):
    """Raised when saving a short link fails unexpectedly."""


@dataclass(frozen=True)
class ParsedBaseLink:
    normalized_url: str
    host: str
    params: Tuple[Tuple[str, str], ...]
    detected_static: Dict[str, str] = field(default_factory=dict)
    has_pixel: bool = False
    pixel_value: Optional[str] = None


@dataclass(frozen=True)
class BuiltLink:
    long_url: str
    params: Tuple[Tuple[str, str], ...]
    has_pixel: bool


def _clean_url_token(raw: str) -> str:
    value = (raw or "").strip()
    if value.startswith("<") and value.endswith(">"):
        value = value[1:-1].strip()
    return value.strip("`\"'")


def parse_base_link(raw_url: str) -> ParsedBaseLink:
    """Validate and parse a campaign/tracker URL."""
    cleaned = _clean_url_token(raw_url)
    if not cleaned:
        raise InvalidBaseLinkError("Ссылка пустая. Пришлите корректный URL, например https://luckytop.online/?sub1=77...")
    if any(ch.isspace() for ch in cleaned):
        raise InvalidBaseLinkError("В ссылке не должно быть пробелов или переносов строк.")

    probe = cleaned
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", probe):
        probe = "https://" + probe

    parsed = urlparse(probe)
    if parsed.scheme.lower() not in ("http", "https"):
        raise InvalidBaseLinkError("Поддерживаются только http:// и https:// ссылки.")
    host = (parsed.netloc or "").strip().lower()
    if not host or "." not in host or host.startswith(".") or host.endswith("."):
        raise InvalidBaseLinkError("Не удалось распознать домен в ссылке. Проверьте URL и попробуйте ещё раз.")

    path = parsed.path or "/"
    normalized_url = urlunparse((parsed.scheme.lower(), parsed.netloc, path, parsed.params, parsed.query, ""))
    params = tuple(parse_qsl(parsed.query, keep_blank_values=True))

    detected_static: Dict[str, str] = {}
    pixel_value: Optional[str] = None
    for key, val in params:
        v = val.strip()
        if not v or _is_macro_value(v):
            continue
        detected_static[key] = v
        if key.lower() in ("fbp", "pixel") and not pixel_value:
            pixel_value = v

    return ParsedBaseLink(
        normalized_url=normalized_url,
        host=host,
        params=params,
        detected_static=detected_static,
        has_pixel=bool(pixel_value),
        pixel_value=pixel_value,
    )


def _is_macro_value(value: str) -> bool:
    stripped = (value or "").strip()
    return bool(_MACRO_RE.search(stripped)) or stripped.lower() == "accountid"


def extract_first_line_url_and_rest(raw_text: str) -> Tuple[str, str]:
    """Split '<url>\\n<params...>' when user pastes both in a single message."""
    lines = (raw_text or "").splitlines()
    if not lines:
        return "", ""
    first = lines[0].strip()
    rest = "\n".join(lines[1:]).strip()
    return first, rest


def parse_fields_input(raw_text: str) -> Tuple[Dict[str, str], bool]:
    """Parse multi-line or key-value fields.

    Returns (fields_dict, is_structured):
    - is_structured=True when the user supplied key:value / key=value pairs or 2+ positional lines.
    - is_structured=False when it's a single plain value (used for step-by-step flow).
    """
    raw_lines = [line.strip() for line in (raw_text or "").splitlines()]
    while raw_lines and not raw_lines[0]:
        raw_lines.pop(0)
    while raw_lines and not raw_lines[-1]:
        raw_lines.pop()
    if not raw_lines:
        return {}, False

    kv_fields: Dict[str, str] = {}
    matched_kv = 0
    for line in raw_lines:
        if not line:
            continue
        sep_match = re.match(r"^([^:=]+?)\s*[:=]\s*(.*)$", line)
        if not sep_match:
            continue
        raw_key = sep_match.group(1).strip().lower()
        raw_val = sep_match.group(2).strip()
        canonical = _KEY_ALIASES.get(raw_key)
        if not canonical and _SUB_KEY_RE.fullmatch(raw_key):
            canonical = raw_key
        if not canonical:
            continue
        matched_kv += 1
        if raw_val and raw_val.lower() not in _SKIP_TOKENS and raw_val != "-":
            kv_fields[canonical] = raw_val

    if matched_kv > 0:
        return kv_fields, True

    if len(raw_lines) >= 2:
        positional_order = ("campaign_name", "adset_name", "ad_name", "account_id", "pixel")
        pos_fields: Dict[str, str] = {}
        for idx, line in enumerate(raw_lines[: len(positional_order)]):
            if not line or line == "-" or line.lower() in _SKIP_TOKENS:
                continue
            pos_fields[positional_order[idx]] = line
        return pos_fields, True

    return {}, False


def _replace_macros(value: str, fields: Mapping[str, str], placed: set[str]) -> Optional[str]:
    """Replace {{...}} macros inside a query value.

    Returns None if the whole value was an unexpanded macro for an unprovided field.
    """
    stripped = value.strip()
    if stripped.lower() == "accountid":
        if fields.get("account_id"):
            placed.add("account_id")
            return fields["account_id"]
        return None

    missing_macro = False

    def _sub(match: re.Match[str]) -> str:
        nonlocal missing_macro
        macro_name = match.group(1).strip().lower()
        field_name = _MACRO_TO_FIELD.get(macro_name)
        if field_name and fields.get(field_name):
            placed.add(field_name)
            return fields[field_name]
        missing_macro = True
        return match.group(0)

    replaced = _MACRO_RE.sub(_sub, value)
    if missing_macro and _MACRO_RE.fullmatch(replaced.strip()):
        return None
    if _MACRO_RE.search(replaced):
        cleaned = _MACRO_RE.sub("", replaced).strip("_- ")
        return cleaned or None
    return replaced


def _next_free_sub(used_lower: set[str], start: int = 5) -> str:
    idx = start
    while f"sub{idx}" in used_lower:
        idx += 1
    return f"sub{idx}"


def _param_sort_key(item: Tuple[str, str]) -> Tuple[int, int, str]:
    key = item[0]
    m = _SUB_KEY_RE.fullmatch(key)
    if m:
        return (0, int(m.group(1)), key.lower())
    if key.lower() in ("fbp", "pixel"):
        return (1, 0, key.lower())
    return (2, 0, key.lower())


def extract_campaign_metadata(campaign_name: str) -> Dict[str, str]:
    """Extract buyer alias (sub1), 2-letter geo (sub4), and numeric account_id (sub6)
    from campaign names like '77_TH_1740624179668966_15.11'.
    """
    raw = (campaign_name or "").strip()
    if not raw:
        return {}
    parts = [p.strip() for p in raw.split("_") if p.strip()]
    if len(parts) < 2:
        return {}

    geo: Optional[str] = None
    if re.fullmatch(r"[A-Za-z]{2}", parts[1]):
        geo = parts[1].upper()

    account_id: Optional[str] = None
    for part in parts[1:]:
        if re.fullmatch(r"\d{6,20}", part):
            account_id = part
            break

    extracted: Dict[str, str] = {}
    # Only treat parts[0] as buyer alias when the name matches the <alias>_<GEO|account_id>... pattern.
    if (geo or account_id) and not re.fullmatch(r"\d{1,2}\.\d{1,2}(?:\.\d{2,4})?", parts[0]):
        extracted["sub1"] = parts[0]
    if geo:
        extracted["geo"] = geo
    if account_id:
        extracted["account_id"] = account_id
    return extracted


def build_comment_link(base_url: str, fields: Mapping[str, str]) -> BuiltLink:
    """Substitute macros, add missing Binom sub params + fbp, and return the built URL."""
    parsed_base = parse_base_link(base_url)
    explicit_fields: Dict[str, str] = {
        k: str(v).strip()
        for k, v in fields.items()
        if k not in ("custom_slug", "short_domain")
        and v is not None
        and str(v).strip()
        and str(v).strip().lower() not in _SKIP_TOKENS
    }
    auto_fields: Dict[str, str] = {}
    if explicit_fields.get("campaign_name"):
        for auto_key, auto_val in extract_campaign_metadata(explicit_fields["campaign_name"]).items():
            if auto_key not in explicit_fields:
                auto_fields[auto_key] = auto_val

    cleaned_fields: Dict[str, str] = {**auto_fields, **explicit_fields}

    placed: set[str] = set()
    updated_params: List[Tuple[str, str]] = []

    param_to_field = {param: field for field, param in FIELD_TO_PARAM.items()}
    param_to_field["pixel"] = "pixel"

    for key, val in parsed_base.params:
        lower_key = key.lower()
        if lower_key in explicit_fields and _SUB_KEY_RE.fullmatch(lower_key):
            updated_params.append((key, explicit_fields[lower_key]))
            placed.add(lower_key)
            continue

        new_val = _replace_macros(val, cleaned_fields, placed)
        if new_val is None:
            continue

        mapped_field = param_to_field.get(lower_key)
        if mapped_field and mapped_field in cleaned_fields and mapped_field not in placed:
            if not new_val.strip() or _is_macro_value(val) or (mapped_field == "pixel" and mapped_field in explicit_fields):
                new_val = cleaned_fields[mapped_field]
                placed.add(mapped_field)
            elif mapped_field in auto_fields:
                # Base URL already provides a static value for this slot; don't duplicate auto-extracted metadata into another subN.
                placed.add(mapped_field)

        updated_params.append((key, new_val))

    used_keys_lower = {k.lower() for k, _ in updated_params}

    for key, val in cleaned_fields.items():
        if _SUB_KEY_RE.fullmatch(key) and key.lower() not in placed:
            if key.lower() in used_keys_lower:
                if key in explicit_fields:
                    updated_params = [(k, val if k.lower() == key.lower() else v) for k, v in updated_params]
            else:
                updated_params.append((key.lower(), val))
                used_keys_lower.add(key.lower())
            placed.add(key.lower())

    ordered_fields = (
        "sub1",
        "campaign_name",
        "ad_name",
        "geo",
        "adset_name",
        "account_id",
        "campaign_id",
        "adset_id",
        "ad_id",
        "placement",
        "site_source_name",
        "pixel",
    )
    for field_name in ordered_fields:
        if field_name in placed or field_name not in cleaned_fields:
            continue
        val = cleaned_fields[field_name]
        target_param = FIELD_TO_PARAM[field_name]
        if target_param == "fbp":
            if "fbp" in used_keys_lower:
                updated_params = [(k, val if k.lower() == "fbp" else v) for k, v in updated_params]
            elif "pixel" in used_keys_lower:
                updated_params = [(k, val if k.lower() == "pixel" else v) for k, v in updated_params]
            else:
                updated_params.append(("fbp", val))
                used_keys_lower.add("fbp")
        else:
            if target_param in used_keys_lower:
                target_param = _next_free_sub(used_keys_lower)
            updated_params.append((target_param, val))
            used_keys_lower.add(target_param)
        placed.add(field_name)

    sorted_params = tuple(sorted(updated_params, key=_param_sort_key))
    parsed = urlparse(parsed_base.normalized_url)
    query = urlencode(sorted_params, quote_via=quote, safe="_-.,~")
    long_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", query, ""))
    has_pixel = any(k.lower() in ("fbp", "pixel") and v.strip() for k, v in sorted_params)
    return BuiltLink(long_url=long_url, params=sorted_params, has_pixel=has_pixel)


def normalize_slug(raw_slug: str) -> str:
    """Validate and normalize a custom short-link slug (2..64 chars of a-z, 0-9, -, _)."""
    cleaned = re.sub(r"\s+", "-", (raw_slug or "").strip().lower()).strip("-_")
    if not cleaned or not _SLUG_RE.fullmatch(cleaned):
        raise InvalidSlugError(
            "Название должно быть от 2 до 64 символов: латиница (a-z), цифры, «-» или «_» "
            "(например: thai-slot-1)."
        )
    if cleaned in RESERVED_SLUGS:
        raise InvalidSlugError("Это название зарезервировано системой, выберите другое.")
    return cleaned


def normalize_short_domain(raw_domain: str) -> str:
    """Normalize and validate a short-link domain name (e.g. 'https://Go.Example.com/' -> 'go.example.com')."""
    cleaned = _clean_url_token(raw_domain)
    if not cleaned:
        raise InvalidShortDomainError("Домен пустой. Пришлите, например, <code>myshort.online</code>.")
    if any(ch.isspace() for ch in cleaned):
        raise InvalidShortDomainError("В домене не должно быть пробелов.")
    probe = cleaned if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", cleaned) else f"https://{cleaned}"
    parsed = urlparse(probe)
    host = (parsed.netloc or "").strip().lower()
    if ":" in host:
        host = host.split(":", 1)[0].strip()
    if not host or not _DOMAIN_RE.fullmatch(host):
        raise InvalidShortDomainError(
            "Некорректный домен. Укажите домен вида <code>myshort.online</code> или <code>go.myshort.online</code>."
        )
    return host


def extract_request_host(
    x_forwarded_host: Optional[str],
    host_header: Optional[str],
    x_short_host: Optional[str] = None,
) -> str:
    """Extract lowercase hostname without port from X-Short-Host, X-Forwarded-Host, or Host header."""
    raw = (
        (x_short_host or "").strip()
        or (x_forwarded_host or "").split(",", 1)[0].strip()
        or (host_header or "").strip()
    )
    if ":" in raw:
        raw = raw.split(":", 1)[0].strip()
    return raw.lower()


def generate_random_slug(length: int = 6) -> str:
    while True:
        candidate = "".join(secrets.choice(_RANDOM_ALPHABET) for _ in range(length))
        if candidate not in RESERVED_SLUGS:
            return candidate


def default_short_domain() -> str:
    """Default hostname used for short links (SHORT_LINK_BASE_URL if set, else BASE_URL)."""
    raw = (settings.short_link_base_url or settings.base_url or "").strip()
    if not raw:
        return ""
    try:
        return normalize_short_domain(raw)
    except InvalidShortDomainError:
        return ""


def bot_cname_target() -> str:
    """Public Railway/server host of the bot for CNAME instructions."""
    raw = (settings.base_url or "").strip()
    if not raw:
        return ""
    try:
        return normalize_short_domain(raw)
    except InvalidShortDomainError:
        return ""


def short_link_base(domain: Optional[str] = None) -> str:
    """Origin used for short links (explicit domain, else SHORT_LINK_BASE_URL or BASE_URL)."""
    if domain:
        norm = normalize_short_domain(domain)
        return f"https://{norm}"
    raw = (settings.short_link_base_url or settings.base_url or "").strip().rstrip("/")
    if raw and not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw):
        raw = "https://" + raw
    return raw


def short_link_url(slug: str, domain: Optional[str] = None) -> str:
    base = short_link_base(domain)
    return f"{base}/{slug}" if base else f"/{slug}"


async def list_available_domains(user_id: int, *, include_all: bool = False) -> List[Dict[str, Any]]:
    """Return active short domains for user_id plus configured env/bot domain fallback."""
    rows = list(await db.list_short_domains_for_user(user_id, include_all=include_all))
    seen = {str(r.get("domain") or "").lower() for r in rows}

    env_domain = default_short_domain()
    if env_domain and env_domain not in seen:
        rows.append({
            "domain": env_domain,
            "owner_id": None,
            "is_shared": 1,
            "is_active": 1,
            "is_builtin": True,
        })
        seen.add(env_domain)

    bot_domain = bot_cname_target()
    if bot_domain and bot_domain not in seen:
        rows.append({
            "domain": bot_domain,
            "owner_id": None,
            "is_shared": 1,
            "is_active": 1,
            "is_builtin": True,
        })
    return rows


def merge_redirect_query(target_url: str, incoming_params: Iterable[Tuple[str, str]]) -> str:
    """Forward incoming query params (e.g. fbclid from Facebook) onto target_url without clobbering existing keys."""
    incoming = [(k, v) for k, v in incoming_params if k]
    if not incoming:
        return target_url
    parsed = urlparse(target_url)
    existing = parse_qsl(parsed.query, keep_blank_values=True)
    existing_keys = {k.lower() for k, _ in existing}
    merged = list(existing)
    for key, val in incoming:
        if key.lower() not in existing_keys:
            merged.append((key, val))
            existing_keys.add(key.lower())
    query = urlencode(merged, quote_via=quote, safe="_-.,~")
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path or "/", parsed.params, query, parsed.fragment))


async def create_short_link_for_url(
    long_url: str,
    created_by: Optional[int],
    custom_slug: Optional[str] = None,
    domain: Optional[str] = None,
) -> str:
    """Persist a short link in DB for (domain, slug) and return its public https://<domain>/<slug> URL."""
    resolved_domain = normalize_short_domain(domain) if domain and domain.strip() else default_short_domain()

    if custom_slug is not None and custom_slug.strip():
        slug = normalize_slug(custom_slug)
        try:
            created = await db.create_short_link(slug, long_url, created_by, domain=resolved_domain)
        except Exception as exc:
            logger.exception("Failed to save short link domain={} slug={}", resolved_domain, slug)
            raise LinkShortenError("Ошибка базы данных при сохранении короткой ссылки") from exc
        if not created:
            raise ShortLinkTakenError(slug)
        return short_link_url(slug, resolved_domain or None)

    for _ in range(6):
        slug = generate_random_slug()
        try:
            if await db.create_short_link(slug, long_url, created_by, domain=resolved_domain):
                return short_link_url(slug, resolved_domain or None)
        except Exception as exc:
            logger.exception("Failed to save generated short link domain={}", resolved_domain)
            raise LinkShortenError("Ошибка базы данных при создании короткой ссылки") from exc
    raise LinkShortenError("Не удалось подобрать свободный код, попробуйте ещё раз")


def format_result_message(
    built: BuiltLink,
    short_url: Optional[str] = None,
    shorten_error: Optional[str] = None,
) -> str:
    """Format Telegram HTML message with optional short URL, full URL, and parameter breakdown."""
    lines: List[str] = ["✅ <b>Ссылка для комментария готова</b>"]

    if short_url:
        lines.append("")
        lines.append("🔗 <b>Короткая ссылка:</b>")
        lines.append(f"<code>{safe(short_url)}</code>")
    elif shorten_error:
        lines.append("")
        lines.append(f"⚠️ <b>Короткая ссылка не создана:</b> {safe(shorten_error)}")
        lines.append("Используйте полную ссылку ниже — она уже полностью рабочая.")

    lines.append("")
    lines.append("📎 <b>Полная ссылка с сабами:</b>")
    lines.append(f"<code>{safe(built.long_url)}</code>")

    if built.params:
        lines.append("")
        lines.append("📋 <b>Параметры:</b>")
        for key, val in built.params:
            label = PARAM_LABELS_RU.get(key.lower(), key)
            lines.append(f"• {safe(label)}: <code>{safe(val)}</code>")

    if not built.has_pixel:
        lines.append("")
        lines.append("⚠️ <i>Пиксель (fbp) не указан — события могут не отбиваться в Ads Manager.</i>")

    return "\n".join(lines)
