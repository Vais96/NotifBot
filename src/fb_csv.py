import csv
import io
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional, Set


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
    reason: str


def parse_fb_csv(content: bytes) -> ParsedFbCsv:
    text = content.decode("utf-8-sig", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    raw_rows: List[Dict[str, Any]] = []
    daily_rows: List[Dict[str, Any]] = []
    campaign_names: Set[str] = set()
    account_names: Set[str] = set()
    latest_day: Dict[str, date] = {}
    has_totals = False
    period_start: Optional[date] = None
    period_end: Optional[date] = None

    for source_row in reader:
        row = {k: (v.strip() if isinstance(v, str) else v) for k, v in (source_row or {}).items()}
        account_name = _normalize_str(row.get("Название аккаунта"))
        campaign_name = _normalize_str(row.get("Название кампании"))
        adset_name = _normalize_str(row.get("Название группы объявлений"))
        ad_name = _normalize_str(row.get("Название объявления"))
        day_date = _parse_date(row.get("День"))
        geo = _detect_geo(campaign_name)
        currency = "USD"
        spend = _parse_decimal(row.get("Сумма затрат (USD)"))
        impressions = _parse_int(row.get("Показы"))
        clicks = _parse_int(row.get("Клики (все)"))
        leads = _parse_int(row.get("Лиды"))
        registrations = _parse_int(row.get("Завершенные регистрации"))
        ctr_csv = _parse_decimal(row.get("CTR (все)"))
        cpc_csv = _parse_decimal(row.get("CPC (все)"))

        is_total = day_date is None
        if is_total:
            has_totals = True
        if account_name:
            account_names.add(account_name)
        if campaign_name:
            campaign_names.add(campaign_name)
        if day_date:
            latest_day_value = latest_day.get(campaign_name)
            if latest_day_value is None or day_date > latest_day_value:
                latest_day[campaign_name] = day_date
            period_start = min(filter(None, [period_start, day_date])) if period_start else day_date
            period_end = max(filter(None, [period_end, day_date])) if period_end else day_date

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

    if spend_value >= 200 and ftd == 0:
        return FlagDecision(code="RED", reason="Spend ≥ $200 и FTD = 0")

    if roi_value is not None:
        if roi_value < -30:
            base = FlagDecision(code="RED", reason="ROI < -30%")
        elif roi_value > 30:
            base = FlagDecision(code="GREEN", reason="ROI > 30%")
        else:
            base = FlagDecision(code="YELLOW", reason="ROI в диапазоне -30%…30%")
    else:
        base = FlagDecision(code="GREEN", reason="ROI не рассчитан")

    if base.code != "RED" and ctr_value is not None and ctr_value < 0.7:
        return FlagDecision(code="YELLOW", reason="CTR < 0.7%")

    return base


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
