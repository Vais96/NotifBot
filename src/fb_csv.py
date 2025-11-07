import csv
import io
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Set


HEADER_ALIASES: Dict[str, List[str]] = {
    "account_name": ["названиеаккаунта", "идентификатораккаунта", "accountname", "account"],
    "campaign_name": ["названиекампании", "campaignname", "campaign"],
    "adset_name": ["названиегруппыобъявлений", "adsetname", "группаобъявлений", "adset"],
    "ad_name": ["названиеобъявления", "adname", "объявление", "ad"],
    "day_date": ["day", "день", "date", "reportingstarts", "date_start"],
    "currency": ["валюта", "currency", "accountcurrency"],
    "spend": ["суммазатратusd", "суммазатрат", "amountspend", "amountspent", "amountspentusd", "spend"],
    "impressions": ["показы", "impressions"],
    "clicks": ["кликитвсе", "клики", "clicks", "clicksall", "кликивсе"],
    "leads": ["лиды", "leads"],
    "registrations": ["завершенныерегистрации", "registrations", "complete_registration", "completedregistration"],
    "ctr": ["ctr", "ctrвсе", "ctral"],
    "cpc": ["cpc", "cpcвсе", "cpcall"],
}

REQUIRED_COLUMNS = ["account_name", "campaign_name", "day_date", "spend"]


@dataclass
class ParsedFbCsv:
    raw_rows: List[Dict[str, Any]]
    daily_rows: List[Dict[str, Any]]
    period_start: Optional[date]
    period_end: Optional[date]
    campaign_names: Set[str]
    account_names: Set[str]
    latest_day_by_campaign: Dict[str, date]
    has_totals: bool


@dataclass
class FlagDecision:
    code: str
    reasons: List[str] = field(default_factory=list)

    @property
    def primary_reason(self) -> Optional[str]:
        if not self.reasons:
            return None
        return self.reasons[0]


def _normalize_header_name(name: Optional[str]) -> str:
    text = (name or "").lower()
    return "".join(ch for ch in text if ch.isalnum())


def _build_column_map(fieldnames: List[str]) -> Dict[str, Optional[str]]:
    normalized: Dict[str, str] = {}
    for original in fieldnames:
        norm = _normalize_header_name(original)
        if norm and norm not in normalized:
            normalized[norm] = original

    column_map: Dict[str, Optional[str]] = {}
    for key, aliases in HEADER_ALIASES.items():
        resolved = None
        for alias in aliases:
            alias_norm = _normalize_header_name(alias)
            if alias_norm in normalized:
                resolved = normalized[alias_norm]
                break
        column_map[key] = resolved
    return column_map


def parse_fb_csv(content: bytes) -> ParsedFbCsv:
    text = content.decode("utf-8-sig", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("Не удалось прочитать заголовки CSV")

    column_map = _build_column_map(reader.fieldnames)
    missing = [key for key in REQUIRED_COLUMNS if not column_map.get(key)]
    if missing:
        raise ValueError(f"В CSV отсутствуют обязательные столбцы: {', '.join(missing)}")

    def get_value(row: Dict[str, Any], key: str) -> Optional[str]:
        column = column_map.get(key)
        if not column:
            return None
        return row.get(column)

    raw_rows: List[Dict[str, Any]] = []
    daily_rows: List[Dict[str, Any]] = []
    campaign_names: Set[str] = set()
    account_names: Set[str] = set()
    latest_day: Dict[str, date] = {}
    has_totals = False
    period_start: Optional[date] = None
    period_end: Optional[date] = None

    for source_row in reader:
        if not source_row:
            continue

        row = {k: (v.strip() if isinstance(v, str) else v) for k, v in source_row.items()}
        if not any(value for value in row.values()):
            continue

        account_name = _normalize_str(get_value(row, "account_name"))
        campaign_name = _normalize_str(get_value(row, "campaign_name"))
        adset_name = _normalize_str(get_value(row, "adset_name"))
        ad_name = _normalize_str(get_value(row, "ad_name"))
        day_date = _parse_date(get_value(row, "day_date"))
        geo = _detect_geo(campaign_name)
        currency_raw = get_value(row, "currency")
        currency = currency_raw.upper() if currency_raw else "USD"
        spend = _parse_decimal(get_value(row, "spend"))
        impressions = _parse_int(get_value(row, "impressions"))
        clicks = _parse_int(get_value(row, "clicks"))
        leads = _parse_int(get_value(row, "leads"))
        registrations = _parse_int(get_value(row, "registrations"))
        ctr_csv = _parse_decimal(get_value(row, "ctr"))
        cpc_csv = _parse_decimal(get_value(row, "cpc"))

        if not any([day_date, spend, impressions, clicks, leads, registrations]):
            continue

        is_total = day_date is None
        if is_total:
            has_totals = True
            # Ignore pre-aggregated totals/averages rows (usually the second line) and derive aggregates ourselves.
            continue
        if not campaign_name:
            # региональные сводные строки иногда без названия кампании; пропускаем
            continue

        if account_name:
            account_names.add(account_name)
        campaign_names.add(campaign_name)
        if day_date:
            latest_day_value = latest_day.get(campaign_name)
            if latest_day_value is None or day_date > latest_day_value:
                latest_day[campaign_name] = day_date
        if day_date:
            if period_start is None or day_date < period_start:
                period_start = day_date
            if period_end is None or day_date > period_end:
                period_end = day_date

        if spend is None and any(value is not None for value in (impressions, clicks, leads, registrations)):
            spend = Decimal("0")

        raw_rows.append(
            {
                "account_name": account_name,
                "campaign_name": campaign_name,
                "adset_name": adset_name,
                "ad_name": ad_name,
                "day_date": day_date,
                "currency": currency,
                "geo": geo,
                "spend": spend,
                "impressions": impressions,
                "clicks": clicks,
                "leads": leads,
                "registrations": registrations,
                "ctr": ctr_csv,
                "cpc": cpc_csv,
                "is_total": is_total,
            }
        )

        if day_date and campaign_name:
            ctr_value = _compute_ctr(clicks, impressions, ctr_csv)
            cpc_value = _compute_cpc(spend, clicks, cpc_csv)
            daily_rows.append(
                {
                    "account_name": account_name,
                    "campaign_name": campaign_name,
                    "adset_name": adset_name,
                    "ad_name": ad_name,
                    "day_date": day_date,
                    "currency": currency,
                    "geo": geo,
                    "spend": spend,
                    "impressions": impressions,
                    "clicks": clicks,
                    "leads": leads,
                    "registrations": registrations,
                    "ctr": ctr_value,
                    "cpc": cpc_value,
                }
            )

    return ParsedFbCsv(
        raw_rows=raw_rows,
        daily_rows=daily_rows,
        period_start=period_start,
        period_end=period_end,
        campaign_names=campaign_names,
        account_names=account_names,
        latest_day_by_campaign=latest_day,
        has_totals=has_totals,
    )


def decide_flag(spend: Optional[Decimal], ctr: Optional[Decimal], roi: Optional[Decimal], ftd: int) -> FlagDecision:
    spend_value = float(spend) if spend is not None else 0.0
    ctr_value = float(ctr) if ctr is not None else None
    roi_value = float(roi) if roi is not None else None

    severity_rank = {"GREEN": 0, "YELLOW": 1, "RED": 2}
    records: List[tuple[str, str]] = []
    final_code = "GREEN"

    def register(candidate_code: str, reason: Optional[str]) -> None:
        nonlocal final_code
        if not reason:
            return
        normalized_code = candidate_code.upper()
        records.append((normalized_code, reason))
        if severity_rank.get(normalized_code, 0) > severity_rank.get(final_code, 0):
            final_code = normalized_code

    if spend_value >= 200 and ftd == 0:
        register("RED", "Spend ≥ $200 и FTD = 0")

    if roi_value is not None:
        if roi_value < -30:
            register("RED", "ROI < -30%")
        elif roi_value > 30:
            register("GREEN", "ROI > 30%")
        else:
            register("YELLOW", "ROI в диапазоне -30%…30%")
    else:
        register("GREEN", "ROI не рассчитан")

    if ctr_value is not None and ctr_value < 0.7:
        register("YELLOW", "CTR < 0.7%")

    if not records:
        register("GREEN", "Показателей недостаточно")

    records.sort(key=lambda item: severity_rank.get(item[0], 0), reverse=True)

    ordered_reasons: List[str] = []
    seen: Set[str] = set()
    for _, reason in records:
        if reason not in seen:
            ordered_reasons.append(reason)
            seen.add(reason)

    return FlagDecision(code=final_code, reasons=ordered_reasons)


def _normalize_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    text = value.replace("\u00a0", " ").replace(" ", "")
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    dec_value = _parse_decimal(value)
    if dec_value is None:
        return None
    try:
        return int(dec_value)
    except (ValueError, InvalidOperation):
        return None


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _compute_ctr(clicks: Optional[int], impressions: Optional[int], fallback: Optional[Decimal]) -> Optional[Decimal]:
    if impressions and impressions > 0 and clicks is not None:
        return (Decimal(clicks) / Decimal(impressions)) * Decimal(100)
    return fallback


def _compute_cpc(spend: Optional[Decimal], clicks: Optional[int], fallback: Optional[Decimal]) -> Optional[Decimal]:
    if spend is not None and clicks and clicks > 0:
        return spend / Decimal(clicks)
    return fallback


def _detect_geo(campaign_name: Optional[str]) -> Optional[str]:
    if not campaign_name:
        return None
    tokens = campaign_name.replace("-", "_").split("_")
    for token in tokens:
        cleaned = "".join(ch for ch in token if ch.isalpha())
        if 2 <= len(cleaned) <= 3 and cleaned.isupper():
            return cleaned
    return None
