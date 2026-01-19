from __future__ import annotations

import html
import json
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from loguru import logger

from .. import db, fb_csv
from .campaigns import format_flag_decision, format_flag_label
from ..utils.domain import resolve_campaign_assignments
from .formatting import fmt_money, fmt_percent, month_label_ru

NotifyAdminsFunc = Callable[[str, Exception, Optional[List[str]]], Awaitable[None]]

MAX_CSV_FILE_SIZE_BYTES = 8 * 1024 * 1024
CSV_ALLOWED_MIME_TYPES = {"text/csv", "application/csv", "text/plain", "application/vnd.ms-excel"}

_fmt_money = fmt_money
_fmt_percent = fmt_percent
_month_label_ru = month_label_ru


async def process_fb_csv_upload(
    *,
    bot: Bot,
    message: Message,
    filename: str,
    parsed: fb_csv.ParsedFbCsv,
    status_msg: Message,
    admin_ids: Iterable[int],
    notify_admins: NotifyAdminsFunc,
) -> bool:
    user_id = message.from_user.id
    campaign_meta: Dict[str, Dict[str, Any]] = {}
    assigned_buyers: Set[int] = set()
    upload_id: Optional[int] = None
    upload_buyer_id: Optional[int] = None
    try:
        campaign_meta = await resolve_campaign_assignments(parsed.campaign_names)
        assigned_buyers = {
            int(meta.get("buyer_id"))
            for meta in campaign_meta.values()
            if meta.get("buyer_id") is not None
        }
        upload_buyer_id = next(iter(assigned_buyers)) if len(assigned_buyers) == 1 else None
        upload_id = await db.create_fb_csv_upload(
            uploaded_by=user_id,
            buyer_id=upload_buyer_id,
            original_filename=filename,
            period_start=parsed.period_start,
            period_end=parsed.period_end,
            row_count=len(parsed.raw_rows),
            has_totals=parsed.has_totals,
        )
        await db.bulk_insert_fb_csv_rows(upload_id, parsed.raw_rows)
    except Exception as exc:
        logger.exception("Failed to persist FB CSV upload", exc_info=exc)
        await status_msg.edit_text("Не удалось сохранить CSV в базе. Сообщите админу.")
        await notify_admins(
            "Ошибка при сохранении CSV",
            exc,
            [
                f"User ID: {message.from_user.id}",
                f"Filename: {filename}",
            ],
        )
        return False

    if upload_id is None:
        await status_msg.edit_text("Сохранение CSV не завершено. Попробуйте ещё раз позже.")
        return False

    if not parsed.daily_rows:
        await status_msg.edit_text(
            "CSV сохранён, но в файле нет строк с датой. Проверьте, что выгрузка сделана с разбивкой по дням."
        )
        # Обновим владельцев аккаунтов даже если нет дневных строк
        await _update_fb_accounts(parsed, campaign_meta)
        return True

    try:
        keitaro_stats = await db.fetch_keitaro_campaign_stats(
            parsed.campaign_names,
            parsed.period_start,
            parsed.period_end,
        )
        flag_rows = await db.list_fb_flags()
        flag_by_code = {row["code"].upper(): row for row in flag_rows}
        flag_id_to_title = {row["id"]: row["title"] for row in flag_rows}
        flag_id_to_code = {row["id"]: row["code"] for row in flag_rows}
        flags_by_id = {int(row["id"]): row for row in flag_rows if row.get("id") is not None}
        state_map = await db.fetch_fb_campaign_state(parsed.campaign_names)
        latest_day = parsed.latest_day_by_campaign
        daily_stats = keitaro_stats.get("daily", {})

        def as_decimal(value: Any) -> Decimal:
            if value is None:
                return Decimal("0")
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))

        daily_records: list[dict[str, Any]] = []
        per_campaign_info: dict[str, dict[str, Any]] = {}
        upload_spend = Decimal("0")
        account_buyers: Dict[str, Set[int]] = defaultdict(set)
        aggregated_daily: Dict[tuple[str, date], Dict[str, Any]] = {}

        for row in parsed.daily_rows:
            campaign = row.get("campaign_name")
            day = row.get("day_date")
            if not campaign or not day:
                continue
            meta = campaign_meta.get(campaign, {})
            campaign_buyer_id = meta.get("buyer_id")
            spend_raw = row.get("spend")
            spend = as_decimal(spend_raw) if spend_raw is not None else Decimal("0")
            impressions = row.get("impressions") or 0
            clicks = row.get("clicks") or 0
            leads = row.get("leads")
            registrations = row.get("registrations")
            geo = row.get("geo")
            state = state_map.get(campaign) or {}
            status_id = state.get("status_id")
            account_name = row.get("account_name")
            if account_name and campaign_buyer_id is not None:
                try:
                    account_buyers[account_name].add(int(campaign_buyer_id))
                except Exception:
                    pass

            key = (campaign, day)
            entry = aggregated_daily.setdefault(
                key,
                {
                    "campaign_name": campaign,
                    "day_date": day,
                    "account_name": account_name,
                    "buyer_id": campaign_buyer_id,
                    "geo": geo,
                    "spend": Decimal("0"),
                    "impressions": 0,
                    "clicks": 0,
                    "registrations": 0,
                    "leads": 0,
                    "status_id": status_id,
                },
            )
            entry["spend"] += spend
            entry["impressions"] += int(impressions or 0)
            entry["clicks"] += int(clicks or 0)
            if registrations is not None:
                entry["registrations"] += int(registrations)
            if leads is not None:
                entry["leads"] += int(leads)
            if account_name and not entry.get("account_name"):
                entry["account_name"] = account_name
            if campaign_buyer_id is not None and entry.get("buyer_id") is None:
                entry["buyer_id"] = campaign_buyer_id
            if geo and not entry.get("geo"):
                entry["geo"] = geo
            if status_id is not None:
                entry["status_id"] = status_id

            upload_spend += spend

        for (campaign, day), entry in aggregated_daily.items():
            meta = campaign_meta.get(campaign, {})
            stats = daily_stats.get((campaign, day), {})
            ftd = int(stats.get("ftd") or 0)
            revenue = as_decimal(stats.get("revenue"))
            spend_total = entry["spend"]
            impressions_total = entry["impressions"]
            clicks_total = entry["clicks"]
            registrations_total = entry["registrations"]
            leads_total = entry["leads"]
            ctr = (Decimal(clicks_total) / Decimal(impressions_total) * Decimal(100)) if impressions_total else None
            cpc = (spend_total / Decimal(clicks_total)) if clicks_total else None
            roi = ((revenue - spend_total) / spend_total * Decimal(100)) if spend_total else None
            ftd_rate = (
                (Decimal(ftd) / Decimal(registrations_total) * Decimal(100))
                if registrations_total
                else None
            )
            flag_decision = fb_csv.decide_flag(spend_total, ctr, roi, ftd)
            flag_row = flag_by_code.get(flag_decision.code.upper())
            flag_id = flag_row.get("id") if flag_row else None
            daily_records.append(
                {
                    "campaign_name": campaign,
                    "day_date": day,
                    "account_name": entry.get("account_name"),
                    "buyer_id": entry.get("buyer_id"),
                    "geo": entry.get("geo"),
                    "spend": spend_total,
                    "impressions": impressions_total,
                    "clicks": clicks_total,
                    "registrations": registrations_total,
                    "leads": leads_total,
                    "ftd": ftd,
                    "revenue": revenue,
                    "ctr": ctr,
                    "cpc": cpc,
                    "roi": roi,
                    "ftd_rate": ftd_rate,
                    "status_id": entry.get("status_id"),
                    "flag_id": flag_id,
                    "upload_id": upload_id,
                }
            )
            if latest_day.get(campaign) == day:
                per_campaign_info[campaign] = {
                    "status_id": entry.get("status_id"),
                    "buyer_id": entry.get("buyer_id"),
                    "alias_key": meta.get("alias_key"),
                    "alias_lead_id": meta.get("alias_lead_id"),
                    "day": day,
                }

        if daily_records:
            await db.upsert_fb_campaign_daily(daily_records)

        period_text = "—"
        if parsed.period_start and parsed.period_end:
            if parsed.period_start == parsed.period_end:
                period_text = parsed.period_start.isoformat()
            else:
                period_text = f"{parsed.period_start.isoformat()} — {parsed.period_end.isoformat()}"

        summary_main: list[str] = [
            f"<b>Файл:</b> {html.escape(filename)}",
            f"<b>Период:</b> {html.escape(period_text)}",
            f"<b>Кампаний:</b> {len(parsed.campaign_names)}",
            f"<b>Строк:</b> {len(parsed.raw_rows)}",
            f"<b>Spend за загрузку:</b> {_fmt_money(upload_spend)}",
            f"<b>Байеров в загрузке:</b> {len(assigned_buyers)}",
        ]

        unresolved = sorted(
            campaign for campaign, meta in campaign_meta.items() if meta.get("buyer_id") is None
        )
        if unresolved:
            shown = unresolved[:5]
            suffix = " …" if len(unresolved) > 5 else ""
            summary_main.append(
                "Нет привязки к байеру для: " + ", ".join(html.escape(c) for c in shown) + suffix
            )

        target_month_start: Optional[date] = None
        if parsed.period_end:
            target_month_start = parsed.period_end.replace(day=1)
        elif parsed.period_start:
            target_month_start = parsed.period_start.replace(day=1)
        else:
            target_month_start = date.today().replace(day=1)

        month_summary: Optional[Dict[str, Any]] = None
        month_campaign_rows: List[Dict[str, Any]] = []
        if target_month_start:
            try:
                month_campaign_rows = await db.fetch_fb_campaign_month_report(target_month_start)
            except Exception as exc:
                logger.warning("Failed to fetch FB campaign month report", exc_info=exc)

        def _should_skip_month_campaign(name: Any) -> bool:
            if not name:
                return True
            return "," in str(name)

        aggregated_month_campaigns: Dict[str, Dict[str, Any]] = {}
        aggregated_account_names: Set[str] = set()
        if target_month_start:
            for daily_row in parsed.daily_rows:
                day_value = daily_row.get("day_date")
                if not day_value or day_value.replace(day=1) != target_month_start:
                    continue
                campaign_name = daily_row.get("campaign_name")
                if _should_skip_month_campaign(campaign_name):
                    continue
                entry = aggregated_month_campaigns.setdefault(
                    campaign_name,
                    {
                        "spend": Decimal("0"),
                        "impressions": 0,
                        "clicks": 0,
                        "registrations": 0,
                        "ftd": 0,
                        "revenue": Decimal("0"),
                        "account_name": daily_row.get("account_name"),
                        "flag_id": None,
                    },
                )
                entry["spend"] += as_decimal(daily_row.get("spend"))
                entry["impressions"] += int(daily_row.get("impressions") or 0)
                entry["clicks"] += int(daily_row.get("clicks") or 0)
                entry["registrations"] += int(daily_row.get("registrations") or 0)
                account_name = daily_row.get("account_name")
                if account_name:
                    aggregated_account_names.add(str(account_name))
                    if not entry.get("account_name"):
                        entry["account_name"] = account_name

        month_campaign_lookup: Dict[str, Dict[str, Any]] = {}
        for row in month_campaign_rows:
            campaign_name = row.get("campaign_name")
            if _should_skip_month_campaign(campaign_name):
                continue
            if campaign_name:
                month_campaign_lookup[str(campaign_name)] = row

        for campaign_name, entry in aggregated_month_campaigns.items():
            db_row = month_campaign_lookup.get(campaign_name)
            if not db_row:
                continue
            entry["ftd"] = int(db_row.get("ftd") or entry.get("ftd") or 0)
            entry["revenue"] = as_decimal(db_row.get("revenue"))
            flag_id = db_row.get("curr_flag_id") or db_row.get("state_flag_id") or db_row.get("prev_flag_id")
            if flag_id is not None:
                entry["flag_id"] = flag_id
            account_name = db_row.get("account_name")
            if account_name:
                aggregated_account_names.add(str(account_name))
                if not entry.get("account_name"):
                    entry["account_name"] = account_name

        def _update_month_flag_info(
            campaign: str,
            spend_value: Decimal,
            revenue_value: Decimal,
            impressions_value: int,
            clicks_value: int,
            registrations_value: int,
            ftd_value: int,
            account_name_value: Optional[str],
        ) -> Dict[str, Any]:
            state = state_map.get(campaign) or {}
            meta = campaign_meta.get(campaign, {})
            existing = per_campaign_info.get(campaign, {})
            buyer_id = existing.get("buyer_id", meta.get("buyer_id"))
            alias_key = existing.get("alias_key", meta.get("alias_key"))
            alias_lead_id = existing.get("alias_lead_id", meta.get("alias_lead_id"))
            status_id = existing.get("status_id", state.get("status_id"))
            day_value = latest_day.get(campaign)
            ctr_value = (
                (Decimal(clicks_value) / Decimal(impressions_value) * Decimal(100))
                if impressions_value
                else None
            )
            roi_value = ((revenue_value - spend_value) / spend_value * Decimal(100)) if spend_value else None
            ftd_rate_value = (
                (Decimal(ftd_value) / Decimal(registrations_value) * Decimal(100))
                if registrations_value
                else None
            )
            flag_decision = fb_csv.decide_flag(spend_value, ctr_value, roi_value, ftd_value)
            flag_row = flag_by_code.get(flag_decision.code.upper())
            new_flag_id = flag_row.get("id") if flag_row else None
            reason_text = "; ".join(flag_decision.reasons)
            info = {
                "decision": flag_decision,
                "flag_id": new_flag_id,
                "old_flag_id": state.get("flag_id"),
                "status_id": status_id,
                "roi": roi_value,
                "spend": spend_value,
                "ftd": ftd_value,
                "revenue": revenue_value,
                "ctr": ctr_value,
                "ftd_rate": ftd_rate_value,
                "reason": reason_text,
                "buyer_id": buyer_id,
                "alias_key": alias_key,
                "alias_lead_id": alias_lead_id,
                "day": day_value,
                "account_name": account_name_value,
            }
            per_campaign_info[campaign] = info
            return info

        account_section: list[str] = []
        account_cache_values: List[str] = []
        account_keyboard_rows: List[List[InlineKeyboardButton]] = []
        account_keyboard_markup: Optional[InlineKeyboardMarkup] = None
        account_summaries: Dict[str, Dict[str, Any]] = {}
        cache_kind = f"fbua:{upload_id}"

        def _account_label(raw_name: Optional[str]) -> str:
            text = (raw_name or "").strip()
            return text or "Без кабинета"

        def _add_campaign_to_account(
            account_name_value: Optional[str],
            spend_value: Decimal,
            revenue_value: Decimal,
            ftd_value: int,
            impressions_value: int,
            clicks_value: int,
            registrations_value: int,
            campaign_line: str,
        ) -> None:
            account_label = _account_label(account_name_value)
            summary = account_summaries.setdefault(
                account_label,
                {
                    "spend": Decimal("0"),
                    "revenue": Decimal("0"),
                    "ftd": 0,
                    "campaign_count": 0,
                    "impressions": 0,
                    "clicks": 0,
                    "registrations": 0,
                    "campaign_lines": [],
                },
            )
            summary["spend"] += spend_value
            summary["revenue"] += revenue_value
            summary["ftd"] += ftd_value
            summary["campaign_count"] += 1
            summary["impressions"] += int(impressions_value or 0)
            summary["clicks"] += int(clicks_value or 0)
            summary["registrations"] += int(registrations_value or 0)
            summary["campaign_lines"].append(campaign_line)

        if aggregated_month_campaigns and target_month_start:
            account_names_for_summary = set(aggregated_account_names)
            spend_total = sum((entry["spend"] for entry in aggregated_month_campaigns.values()), Decimal("0"))
            revenue_total = sum(
                (entry.get("revenue", Decimal("0")) for entry in aggregated_month_campaigns.values()),
                Decimal("0"),
            )
            impressions_total = sum(entry["impressions"] for entry in aggregated_month_campaigns.values())
            clicks_total = sum(entry["clicks"] for entry in aggregated_month_campaigns.values())
            registrations_total = sum(entry["registrations"] for entry in aggregated_month_campaigns.values())
            ftd_total = sum(int(entry.get("ftd") or 0) for entry in aggregated_month_campaigns.values())
            for entry in aggregated_month_campaigns.values():
                account_name = entry.get("account_name")
                if account_name:
                    account_names_for_summary.add(str(account_name))
            month_summary = {
                "month_start": target_month_start,
                "spend": spend_total,
                "revenue": revenue_total,
                "impressions": impressions_total,
                "clicks": clicks_total,
                "registrations": registrations_total,
                "ftd": ftd_total,
                "campaign_count": len(aggregated_month_campaigns),
                "account_count": len(account_names_for_summary),
            }
            sorted_items = sorted(
                aggregated_month_campaigns.items(),
                key=lambda item: float(item[1]["spend"]),
                reverse=True,
            )
            for name, stats in sorted_items:
                spend_value = stats["spend"]
                revenue_value = stats.get("revenue", Decimal("0"))
                ftd_value = int(stats.get("ftd") or 0)
                impressions_value = int(stats.get("impressions") or 0)
                clicks_value = int(stats.get("clicks") or 0)
                registrations_value = int(stats.get("registrations") or 0)
                account_name = stats.get("account_name")
                info = _update_month_flag_info(
                    name,
                    spend_value,
                    revenue_value,
                    impressions_value,
                    clicks_value,
                    registrations_value,
                    ftd_value,
                    account_name,
                )
                flag_label = format_flag_decision(info.get("decision"))
                roi_value = info.get("roi")
                campaign_line = (
                    "• "
                    + html.escape(str(name))
                    + " — "
                    + html.escape(str(flag_label))
                    + f". Spend {_fmt_money(spend_value)} | FTD {ftd_value} | Rev {_fmt_money(revenue_value)} | ROI {_fmt_percent(roi_value)}"
                )
                _add_campaign_to_account(
                    account_name,
                    spend_value,
                    revenue_value,
                    ftd_value,
                    impressions_value,
                    clicks_value,
                    registrations_value,
                    campaign_line,
                )
        elif month_campaign_rows and target_month_start:
            spend_total = Decimal("0")
            revenue_total = Decimal("0")
            impressions_total = 0
            clicks_total = 0
            registrations_total = 0
            ftd_total = 0
            account_names: Set[str] = set()
            valid_campaign_count = 0
            sorted_rows = sorted(
                month_campaign_rows,
                key=lambda row: float(row.get("spend") or 0),
                reverse=True,
            )
            for row in sorted_rows:
                name = row.get("campaign_name")
                if _should_skip_month_campaign(name):
                    continue
                spend_value = as_decimal(row.get("spend"))
                revenue_value = as_decimal(row.get("revenue"))
                impressions_value = int(row.get("impressions") or 0)
                clicks_value = int(row.get("clicks") or 0)
                registrations_value = int(row.get("registrations") or 0)
                ftd_value = int(row.get("ftd") or 0)
                account_name = row.get("account_name")
                info = _update_month_flag_info(
                    str(name),
                    spend_value,
                    revenue_value,
                    impressions_value,
                    clicks_value,
                    registrations_value,
                    ftd_value,
                    account_name,
                )
                flag_label = format_flag_decision(info.get("decision"))
                campaign_line = (
                    "• "
                    + html.escape(str(name))
                    + " — "
                    + html.escape(str(flag_label))
                    + f". Spend {_fmt_money(spend_value)} | FTD {ftd_value} | Rev {_fmt_money(revenue_value)} | ROI {_fmt_percent(info.get('roi'))}"
                )
                _add_campaign_to_account(
                    account_name,
                    spend_value,
                    revenue_value,
                    ftd_value,
                    impressions_value,
                    clicks_value,
                    registrations_value,
                    campaign_line,
                )
                spend_total += spend_value
                revenue_total += revenue_value
                impressions_total += impressions_value
                clicks_total += clicks_value
                registrations_total += registrations_value
                ftd_total += ftd_value
                account_names.add(_account_label(account_name))
                valid_campaign_count += 1
            month_summary = {
                "month_start": target_month_start,
                "spend": spend_total,
                "revenue": revenue_total,
                "impressions": impressions_total,
                "clicks": clicks_total,
                "registrations": registrations_total,
                "ftd": ftd_total,
                "campaign_count": valid_campaign_count,
                "account_count": len(account_names),
            }

        for summary in account_summaries.values():
            spend_value = summary.get("spend", Decimal("0"))
            revenue_value = summary.get("revenue", Decimal("0"))
            ftd_value = int(summary.get("ftd") or 0)
            impressions_total = int(summary.get("impressions") or 0)
            clicks_total = int(summary.get("clicks") or 0)
            registrations_total = int(summary.get("registrations") or 0)
            ctr_value = (
                (Decimal(clicks_total) / Decimal(impressions_total) * Decimal(100))
                if impressions_total
                else None
            )
            roi_value = (
                (revenue_value - spend_value) / spend_value * Decimal(100)
                if spend_value
                else None
            )
            ftd_rate_value = (
                (Decimal(ftd_value) / Decimal(registrations_total) * Decimal(100))
                if registrations_total
                else None
            )
            decision = fb_csv.decide_flag(spend_value, ctr_value, roi_value, ftd_value)
            summary["ctr"] = ctr_value
            summary["roi"] = roi_value
            summary["ftd_rate"] = ftd_rate_value
            summary["decision"] = decision
            summary["flag_label"] = format_flag_decision(decision)

        per_campaign_info = {
            campaign: info for campaign, info in per_campaign_info.items() if info.get("decision")
        }

        states_to_upsert: list[dict[str, Any]] = []
        history_entries: list[dict[str, Any]] = []
        flag_changes: list[dict[str, Any]] = []

        for campaign, info in per_campaign_info.items():
            new_flag_id = info.get("flag_id")
            state_raw = state_map.get(campaign)
            state = state_raw or {}
            old_flag_id = info.get("old_flag_id")
            status_id = info.get("status_id")
            buyer_comment = state.get("buyer_comment")
            lead_comment = state.get("lead_comment")
            if state_raw is None or new_flag_id != old_flag_id:
                states_to_upsert.append(
                    {
                        "campaign_name": campaign,
                        "status_id": status_id,
                        "flag_id": new_flag_id,
                        "buyer_comment": buyer_comment,
                        "lead_comment": lead_comment,
                        "updated_by": user_id,
                    }
                )
            if state_raw is not None or new_flag_id is not None or old_flag_id is not None:
                history_entries.append(
                    {
                        "campaign_name": campaign,
                        "changed_by": user_id,
                        "old_status_id": status_id,
                        "new_status_id": status_id,
                        "old_flag_id": old_flag_id,
                        "new_flag_id": new_flag_id,
                        "note": info.get("reason"),
                    }
                )
            if new_flag_id != old_flag_id:
                old_label = format_flag_label(old_flag_id, flags_by_id)
                new_label = format_flag_decision(info.get("decision"))
                flag_changes.append(
                    {
                        "campaign": campaign,
                        "old": old_label,
                        "new": new_label,
                        "old_flag_id": old_flag_id,
                        "new_flag_id": new_flag_id,
                        "reason": info.get("reason", ""),
                        "buyer_id": info.get("buyer_id"),
                        "alias_key": info.get("alias_key"),
                        "alias_lead_id": info.get("alias_lead_id"),
                        "spend": info.get("spend"),
                        "revenue": info.get("revenue"),
                        "ftd": info.get("ftd"),
                        "roi": info.get("roi"),
                        "ctr": info.get("ctr"),
                        "ftd_rate": info.get("ftd_rate"),
                        "day": info.get("day"),
                    }
                )

        if states_to_upsert:
            await db.upsert_fb_campaign_state(states_to_upsert)
        if history_entries:
            await db.log_fb_campaign_history(history_entries)

        await db.recompute_fb_campaign_totals(parsed.campaign_names)
        await _update_fb_accounts(parsed, campaign_meta, account_buyers)

        flag_section: list[str] = []
        if flag_changes:
            max_flag_rows = 30
            flag_section.append("<b>Изменения флагов:</b>")
            for change in flag_changes[:max_flag_rows]:
                flag_section.append(
                    "• "
                    + html.escape(change["campaign"])
                    + ": "
                    + html.escape(change["old"] or "—")
                    + " → "
                    + html.escape(change["new"] or "—")
                    + " ("
                    + html.escape(change.get("reason") or "")
                    + ")"
                )
            remaining = len(flag_changes) - max_flag_rows
            if remaining > 0:
                flag_section.append(f"• ещё {remaining} изменений …")
            summary_main.append("<b>Изменения флагов:</b> см. ниже")
        else:
            summary_main.append("Изменения флагов: нет")

        month_section: list[str] = []
        if month_summary and target_month_start:
            month_label = _month_label_ru(target_month_start)
            spend_total = as_decimal(month_summary.get("spend"))
            revenue_total = as_decimal(month_summary.get("revenue"))
            ftd_total = int(month_summary.get("ftd") or 0)
            registrations_total = int(month_summary.get("registrations") or 0)
            roi_total = ((revenue_total - spend_total) / spend_total * Decimal(100)) if spend_total else None
            ftd_rate_total = (Decimal(ftd_total) / Decimal(registrations_total) * Decimal(100)) if registrations_total else None
            campaign_count = int(month_summary.get("campaign_count") or 0)
            account_count = int(month_summary.get("account_count") or 0)
            month_section.append("<b>" + html.escape(month_label) + "</b>")
            month_section.append(
                f"Spend {_fmt_money(spend_total)} | Rev {_fmt_money(revenue_total)} | ROI {_fmt_percent(roi_total)} | FTD {ftd_total} (FTD rate {_fmt_percent(ftd_rate_total)}) | Кампаний {campaign_count} | Кабинетов {account_count}"
            )

        max_accounts_in_text = 20
        max_accounts_in_keyboard = 12
        if account_summaries:
            period_label = _month_label_ru(target_month_start) if target_month_start else None
            if period_label:
                account_section.append(
                    "<b>Кабинеты за " + html.escape(period_label) + ":</b>"
                )
            else:
                account_section.append("<b>Кабинеты:</b>")
            account_section.append("Нажми кнопку ниже, чтобы посмотреть кампании кабинета.")
            sorted_accounts = sorted(
                account_summaries.items(),
                key=lambda item: float(item[1]["spend"]),
                reverse=True,
            )
            for idx, (account_name, summary) in enumerate(sorted_accounts):
                spend_value = summary["spend"]
                revenue_value = summary["revenue"]
                ftd_value = summary["ftd"]
                campaign_count = summary["campaign_count"]
                roi_value = summary.get("roi")
                flag_label = summary.get("flag_label") or "—"
                line = (
                    "• "
                    + html.escape(account_name)
                    + " — "
                    + html.escape(flag_label)
                    + f". Spend {_fmt_money(spend_value)} | FTD {ftd_value} | Rev {_fmt_money(revenue_value)} | ROI {_fmt_percent(roi_value)} | Кампаний {campaign_count}"
                )
                if idx < max_accounts_in_text:
                    account_section.append(line)
                    if idx < min(max_accounts_in_text, len(sorted_accounts)) - 1:
                        account_section.append("")
                payload = {
                    "account_name": account_name,
                    "flag_label": flag_label,
                    "spend": str(spend_value),
                    "revenue": str(revenue_value),
                    "roi": str(roi_value) if roi_value is not None else None,
                    "ftd": ftd_value,
                    "campaign_count": campaign_count,
                    "campaign_lines": summary["campaign_lines"],
                    "ctr": str(summary.get("ctr")) if summary.get("ctr") is not None else None,
                    "ftd_rate": str(summary.get("ftd_rate")) if summary.get("ftd_rate") is not None else None,
                }
                account_cache_values.append(json.dumps(payload))
                if idx < max_accounts_in_keyboard:
                    flag_icon = flag_label.split(" ", 1)[0] if flag_label else "—"
                    short_name = account_name
                    if len(short_name) > 28:
                        short_name = short_name[:27] + "…"
                    button_text = f"{idx + 1}. {flag_icon} {short_name}".strip()
                    if len(button_text) > 64:
                        button_text = button_text[:63] + "…"
                    account_keyboard_rows.append(
                        [InlineKeyboardButton(text=button_text, callback_data=f"fbua:{upload_id}:{idx}")]
                    )
            remaining_accounts = len(account_summaries) - max_accounts_in_text
            if remaining_accounts > 0:
                account_section.append(f"… и ещё {remaining_accounts} кабинетов")
            if account_keyboard_rows:
                account_keyboard_markup = InlineKeyboardMarkup(inline_keyboard=account_keyboard_rows)
        else:
            account_section.append("Кабинеты в этой загрузке не найдены.")

        try:
            await db.set_ui_cache_list(user_id, cache_kind, account_cache_values)
        except Exception as exc:
            logger.warning("Failed to update FB account cache", exc_info=exc)
            account_keyboard_markup = None

        missing_keitaro = sorted(
            c for c in parsed.campaign_names if c not in keitaro_stats.get("totals", {}) and "," not in c
        )
        missing_section: list[str] = []
        if missing_keitaro:
            shown = missing_keitaro[:5]
            suffix = " …" if len(missing_keitaro) > 5 else ""
            missing_section.append(
                "Внимание: нет данных Keitaro для: " + ", ".join(html.escape(c) for c in shown) + suffix
            )

        def build_messages(sections: list[list[str]], max_length: int = 3500) -> list[str]:
            lines: list[str] = []
            for section in sections:
                if not section:
                    continue
                if lines:
                    lines.append("")
                lines.extend(section)
            messages: list[str] = []
            current_lines: list[str] = []
            current_len = 0
            for line in lines:
                appended_len = len(line) + 1
                if appended_len > max_length:
                    # hard truncate overly long lines to fit Telegram limit
                    trimmed = line[: max_length - 4] + " …"
                    line = trimmed
                    appended_len = len(line) + 1
                if current_lines and current_len + appended_len > max_length:
                    messages.append("\n".join(current_lines))
                    current_lines = []
                    current_len = 0
                current_lines.append(line)
                current_len += appended_len
            if current_lines:
                messages.append("\n".join(current_lines))
            return messages or [""]

        sections = [summary_main, flag_section, month_section, account_section, missing_section]
        message_chunks = build_messages(sections)
        if account_keyboard_markup:
            await status_msg.edit_text(
                message_chunks[0],
                parse_mode=ParseMode.HTML,
                reply_markup=account_keyboard_markup,
            )
        else:
            await status_msg.edit_text(message_chunks[0], parse_mode=ParseMode.HTML)
        for extra_text in message_chunks[1:]:
            await bot.send_message(message.chat.id, extra_text, parse_mode=ParseMode.HTML)
        if flag_changes:
            await _notify_flag_updates(bot, admin_ids, filename, flag_changes)
        return True
    except Exception as exc:
        logger.exception("Failed to process FB CSV data", exc_info=exc)
        await status_msg.edit_text("Во время расчётов произошла ошибка. Сообщите админу.")
        await notify_admins(
            "Ошибка при обработке CSV",
            exc,
            [
                f"User ID: {message.from_user.id}",
                f"Filename: {filename}",
                f"Upload ID: {upload_id or '—'}",
            ],
        )
        return False

async def _update_fb_accounts(
    parsed: fb_csv.ParsedFbCsv,
    campaign_meta: Dict[str, Dict[str, Any]],
    account_buyers: Optional[Dict[str, Set[int]]] = None,
) -> None:
    buyers_map: Dict[str, Set[int]] = {}
    if account_buyers:
        for account, buyers in account_buyers.items():
            buyers_map[account] = {int(b) for b in buyers if b is not None}
    else:
        temp: Dict[str, Set[int]] = defaultdict(set)
        for row in parsed.raw_rows:
            account = row.get("account_name")
            campaign = row.get("campaign_name")
            if not account or not campaign:
                continue
            meta = campaign_meta.get(campaign) or {}
            buyer_id = meta.get("buyer_id")
            if buyer_id is None:
                continue
            try:
                temp.setdefault(account, set()).add(int(buyer_id))
            except Exception:
                continue
        buyers_map = temp
    all_accounts: Set[str] = set()
    all_accounts.update(buyers_map.keys())
    all_accounts.update(name for name in parsed.account_names if name)
    payload: List[Dict[str, Any]] = []
    for account in sorted(all_accounts):
        buyers = buyers_map.get(account, set())
        buyer_id: Optional[int] = None
        if len(buyers) == 1:
            buyer_id = next(iter(buyers))
        payload.append(
            {
                "account_name": account,
                "buyer_id": buyer_id,
                "owner_since": parsed.period_start,
            }
        )
    if not payload:
        return
    try:
        await db.upsert_fb_accounts(payload)
    except Exception as exc:
        logger.warning("Failed to upsert FB accounts", exc_info=exc)

async def _notify_flag_updates(
    bot: Bot,
    admin_ids: Iterable[int],
    filename: str,
    flag_changes: List[Dict[str, Any]],
) -> None:
    if not flag_changes:
        return
    try:
        flag_rows = await db.list_fb_flags()
    except Exception as exc:
        logger.warning("Failed to fetch flags for notifications", exc_info=exc)
        flag_rows = []
    red_flag_ids: Set[int] = set()
    for row in flag_rows or []:
        code = (row.get("code") or "").upper()
        identifier = row.get("id")
        if code != "RED" or identifier is None:
            continue
        try:
            red_flag_ids.add(int(identifier))
        except Exception:
            continue

    def _is_red_related(change: Dict[str, Any]) -> bool:
        new_id = change.get("new_flag_id")
        old_id = change.get("old_flag_id")
        for value in (new_id, old_id):
            if value is None:
                continue
            try:
                if red_flag_ids and int(value) in red_flag_ids:
                    return True
            except Exception:
                continue
        if not red_flag_ids:
            for label in (change.get("new"), change.get("old")):
                if isinstance(label, str) and "🔴" in label:
                    return True
        return False

    filtered_changes = [change for change in flag_changes if _is_red_related(change)]
    if not filtered_changes:
        return
    try:
        users = await db.list_users()
    except Exception as exc:
        logger.warning("Failed to fetch users for flag notifications", exc_info=exc)
        return
    user_map: Dict[int, Dict[str, Any]] = {}
    for row in users:
        telegram_id = row.get("telegram_id")
        if telegram_id is None:
            continue
        try:
            user_map[int(telegram_id)] = row
        except Exception:
            continue
    admins_from_db = {uid for uid, row in user_map.items() if row.get("role") == "admin" and row.get("is_active")}
    heads = {uid for uid, row in user_map.items() if row.get("role") == "head" and row.get("is_active")}
    env_admins: Set[int] = set()
    for aid in admin_ids:
        try:
            env_admins.add(int(aid))
        except Exception:
            continue
    admin_recipients = admins_from_db | env_admins
    team_leads_cache: Dict[int, List[int]] = {}
    team_mentors_cache: Dict[int, List[int]] = {}
    recipient_messages: Dict[int, List[str]] = defaultdict(list)

    async def get_team_leads_cached(team_id: int) -> List[int]:
        if team_id not in team_leads_cache:
            try:
                team_leads_cache[team_id] = await db.list_team_leads(team_id)
            except Exception as exc:
                logger.warning("Failed to fetch team leads", team_id=team_id, exc_info=exc)
                team_leads_cache[team_id] = []
        return team_leads_cache[team_id]

    async def get_team_mentors_cached(team_id: int) -> List[int]:
        if team_id not in team_mentors_cache:
            try:
                team_mentors_cache[team_id] = await db.list_team_mentors(team_id)
            except Exception as exc:
                logger.warning("Failed to fetch team mentors", team_id=team_id, exc_info=exc)
                team_mentors_cache[team_id] = []
        return team_mentors_cache[team_id]

    def format_user_label(user_id: Optional[int]) -> str:
        if user_id is None:
            return ""
        try:
            uid = int(user_id)
        except Exception:
            return str(user_id)
        info = user_map.get(uid)
        if not info:
            return str(uid)
        username = info.get("username")
        if username:
            return f"@{username}"
        full_name = info.get("full_name")
        if full_name:
            return str(full_name)
        return str(uid)

    for change in filtered_changes:
        recipients: Set[int] = set(admin_recipients) | set(heads)
        buyer_id = change.get("buyer_id")
        alias_lead_id = change.get("alias_lead_id")
        if buyer_id is not None:
            try:
                recipients.add(int(buyer_id))
            except Exception:
                pass
        if alias_lead_id is not None:
            try:
                recipients.add(int(alias_lead_id))
            except Exception:
                pass
        team_id: Optional[int] = None
        if buyer_id is not None:
            buyer_row = user_map.get(int(buyer_id))
            if buyer_row:
                tid = buyer_row.get("team_id")
                if tid is not None:
                    try:
                        team_id = int(tid)
                    except Exception:
                        team_id = None
        if team_id is not None:
            leads = await get_team_leads_cached(team_id)
            for lid in leads:
                try:
                    recipients.add(int(lid))
                except Exception:
                    continue
            mentors = await get_team_mentors_cached(team_id)
            for mid in mentors:
                try:
                    recipients.add(int(mid))
                except Exception:
                    continue
        recipients = {
            rid for rid in recipients
            if rid in env_admins
            or rid not in user_map
            or user_map.get(rid, {}).get("is_active", 1)
        }
        campaign = html.escape(change.get("campaign") or "—")
        old_label = html.escape(change.get("old") or "—")
        new_label = html.escape(change.get("new") or "—")
        alias_key = change.get("alias_key")
        alias_text = f"Алиас: {html.escape(alias_key)}" if alias_key else None
        day = change.get("day")
        day_text = None
        if day:
            try:
                day_text = f"Дата: {html.escape(day.isoformat())}"
            except Exception:
                day_text = None
        buyer_label = format_user_label(buyer_id)
        buyer_text = f"Байер: {html.escape(buyer_label)}" if buyer_label else None
        metrics = (
            f"Spend {_fmt_money(change.get('spend'))}, "
            f"FTD {change.get('ftd') or 0}, "
            f"Rev {_fmt_money(change.get('revenue'))}, "
            f"ROI {_fmt_percent(change.get('roi'))}"
        )
        extras: List[str] = []
        ctr_value = change.get("ctr")
        if ctr_value is not None:
            extras.append(f"CTR {_fmt_percent(ctr_value)}")
        ftd_rate_value = change.get("ftd_rate")
        if ftd_rate_value is not None:
            extras.append(f"FTD rate {_fmt_percent(ftd_rate_value)}")
        reason = change.get("reason") or ""
        parts = [f"<b>{campaign}</b>: {old_label} → {new_label}"]
        if day_text:
            parts.append(day_text)
        parts.append(metrics)
        if extras:
            parts.append("; ".join(extras))
        if buyer_text:
            parts.append(buyer_text)
        if alias_text:
            parts.append(alias_text)
        if reason:
            parts.append("Причина: " + html.escape(str(reason)))
        line = "\n".join(parts)
        for rid in recipients:
            recipient_messages[rid].append(line)

    if not recipient_messages:
        return
    header = f"<b>Обновления флагов</b> из {html.escape(filename)}"
    for rid, lines in recipient_messages.items():
        message_text = header + "\n\n" + "\n\n".join(lines)
        try:
            await bot.send_message(rid, message_text, parse_mode=ParseMode.HTML)
        except Exception as exc:
            logger.warning("Failed to send flag notification", user_id=rid, exc_info=exc)
