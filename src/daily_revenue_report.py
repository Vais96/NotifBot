"""End-of-day revenue summary: per-team and per-buyer deposit totals for leads, mentors and heads."""

from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from loguru import logger

from . import db
from .config import settings
from .dispatcher import notify_buyer
from .services.daily_revenue import SaleStat, build_report_messages, resolve_scope


def report_timezone() -> ZoneInfo:
    try:
        return ZoneInfo(settings.daily_revenue_report_tz or "UTC")
    except Exception:
        logger.warning(
            "Unknown DAILY_REVENUE_REPORT_TZ, falling back to UTC",
            tz=settings.daily_revenue_report_tz,
        )
        return ZoneInfo("UTC")


def parse_report_time(value: str | None) -> time | None:
    """Parse ``HH:MM``; empty or invalid values disable the report."""
    if not value:
        return None
    try:
        hours, minutes = value.strip().split(":", 1)
        return time(hour=int(hours), minute=int(minutes))
    except (TypeError, ValueError):
        logger.warning("Invalid DAILY_REVENUE_REPORT_TIME, report disabled", value=value)
        return None


def next_run_at(now: datetime, at: time, tz: ZoneInfo) -> datetime:
    """Next wall-clock occurrence of ``at`` in ``tz`` strictly after ``now``."""
    local_now = now.astimezone(tz)
    candidate = datetime.combine(local_now.date(), at, tzinfo=tz)
    if candidate <= local_now:
        candidate = datetime.combine(local_now.date() + timedelta(days=1), at, tzinfo=tz)
    return candidate


def day_window(now: datetime, tz: ZoneInfo) -> tuple[datetime, datetime, datetime]:
    """Return (local day start, UTC window start, UTC window end) for the day containing ``now``."""
    local_now = now.astimezone(tz)
    local_start = datetime.combine(local_now.date(), time(0, 0), tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    to_utc = lambda value: value.astimezone(timezone.utc).replace(tzinfo=None)  # noqa: E731
    return local_start, to_utc(local_start), to_utc(local_end)


async def build_daily_revenue_messages(now: datetime | None = None) -> Dict[int, List[str]]:
    """Build the summary text for every recipient (telegram_id -> messages)."""
    tz = report_timezone()
    now = now or datetime.now(timezone.utc)
    local_start, start_utc, end_utc = day_window(now, tz)

    rows = await db.sales_by_user_between(start_utc, end_utc)
    sales = [SaleStat(user_id=r["user_id"], count=r["count"], revenue=r["revenue"]) for r in rows]
    users = await db.list_users()
    teams = {int(t["id"]): str(t["name"]) for t in await db.list_teams()}

    result: Dict[int, List[str]] = {}
    for user in users:
        if not user.get("is_active"):
            continue
        role = user.get("role")
        recipient_id = int(user["telegram_id"])
        extra_lead_teams: List[int] = []
        mentor_teams: List[int] = []
        alias_buyers: List[int] = []
        try:
            if role == "lead":
                extra_lead_teams = await db.list_extra_lead_teams(recipient_id)
                alias_buyers = await db.list_alias_lead_buyers(recipient_id)
            elif role == "mentor":
                mentor_teams = await db.list_mentor_teams(recipient_id)
        except Exception as exc:
            logger.warning("Failed to resolve report scope", user_id=recipient_id, error=str(exc))
        scope = resolve_scope(
            user,
            users,
            extra_lead_teams=extra_lead_teams,
            mentor_teams=mentor_teams,
            alias_buyers=alias_buyers,
        )
        if scope is None:
            continue
        result[recipient_id] = build_report_messages(local_start.date(), sales, scope, users, teams)
    return result


async def send_daily_revenue_report(*, dry_run: bool = False) -> Dict[str, Any]:
    """Send (or preview) the end-of-day summary to every lead, mentor and head."""
    messages = await build_daily_revenue_messages()
    stats: Dict[str, Any] = {
        "dry_run": dry_run,
        "recipients": len(messages),
        "sent": 0,
        "failed": 0,
    }
    if dry_run:
        stats["preview"] = {str(uid): texts for uid, texts in messages.items()}
        return stats
    for recipient_id, texts in messages.items():
        try:
            for text in texts:
                await notify_buyer(recipient_id, text)
            stats["sent"] += 1
        except Exception as exc:
            stats["failed"] += 1
            logger.warning("Daily revenue report delivery failed", user_id=recipient_id, error=str(exc))
    return stats


async def daily_revenue_report_loop(at: time, tz: ZoneInfo) -> None:
    """Sleep until the configured local time every day, then send the summary."""
    while True:
        now = datetime.now(timezone.utc)
        run_at = next_run_at(now, at, tz)
        delay = max(1.0, (run_at - now).total_seconds())
        logger.info("Daily revenue report scheduled", run_at=run_at.isoformat(), delay_seconds=int(delay))
        await asyncio.sleep(delay)
        try:
            stats = await send_daily_revenue_report(dry_run=False)
            logger.info("Daily revenue report sent", **stats)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Daily revenue report failed", error=str(exc))
        # Guard against re-running within the same minute if the clock is slightly early.
        await asyncio.sleep(61)


__all__ = [
    "build_daily_revenue_messages",
    "daily_revenue_report_loop",
    "day_window",
    "next_run_at",
    "parse_report_time",
    "report_timezone",
    "send_daily_revenue_report",
]
