"""Per-notifier run statistics returned by the notify endpoints."""

from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from loguru import logger


@dataclass(slots=True)
class NotificationStats:
    total_orders: int = 0
    missing_contact: int = 0
    unknown_user: int = 0
    no_recipient_mapping: int = 0
    matched_users: int = 0
    notified: int = 0
    errors: int = 0
    delivery_failed: int = 0
    unknown_orders: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        payload = {
            "total_orders": self.total_orders,
            "missing_contact": self.missing_contact,
            "unknown_user": self.unknown_user,
            "no_recipient_mapping": self.no_recipient_mapping,
            "matched_users": self.matched_users,
            "notified": self.notified,
            "errors": self.errors,
            "delivery_failed": self.delivery_failed,
            "unknown_orders": self.unknown_orders,
        }
        if dry_run is not None:
            payload["dry_run"] = dry_run
        return payload


@dataclass(slots=True)
class DomainNotifierStats:
    total_domains: int = 0
    expiring_domains: int = 0
    matched_users: int = 0
    notified_users: int = 0
    notified_domains: int = 0
    missing_contact: int = 0
    unknown_user: int = 0
    no_recipient_mapping: int = 0
    errors: int = 0
    delivery_failed: int = 0
    unknown_items: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        payload = {
            "total_domains": self.total_domains,
            "expiring_domains": self.expiring_domains,
            "matched_users": self.matched_users,
            "notified_users": self.notified_users,
            "notified_domains": self.notified_domains,
            "missing_contact": self.missing_contact,
            "unknown_user": self.unknown_user,
            "no_recipient_mapping": self.no_recipient_mapping,
            "errors": self.errors,
            "delivery_failed": self.delivery_failed,
            "unknown_items": self.unknown_items,
        }
        if dry_run is not None:
            payload["dry_run"] = dry_run
        return payload


@dataclass(slots=True)
class IPNotifierStats:
    total_ips: int = 0
    matched_users: int = 0
    notified_users: int = 0
    notified_ips: int = 0
    missing_contact: int = 0
    unknown_user: int = 0
    no_recipient_mapping: int = 0
    errors: int = 0
    unknown_items: List[Dict[str, Any]] = field(default_factory=list)
    # Детализация для логов и ответа API: кому ушло, кому нет, почему
    delivered: List[Dict[str, Any]] = field(default_factory=list)
    send_failures: List[Dict[str, Any]] = field(default_factory=list)
    underdog_mark_failures: List[Dict[str, Any]] = field(default_factory=list)
    dry_run_preview: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        payload = {
            "total_ips": self.total_ips,
            "matched_users": self.matched_users,
            "notified_users": self.notified_users,
            "notified_ips": self.notified_ips,
            "missing_contact": self.missing_contact,
            "unknown_user": self.unknown_user,
            "no_recipient_mapping": self.no_recipient_mapping,
            "errors": self.errors,
            "delivery_failed": len(self.send_failures),
            "unknown_items": self.unknown_items,
            "delivered": self.delivered,
            "send_failures": self.send_failures,
            "underdog_mark_failures": self.underdog_mark_failures,
            "dry_run_preview": self.dry_run_preview,
        }
        if dry_run is not None:
            payload["dry_run"] = dry_run
        return payload


def _log_ip_notify_delivery_report(stats: IPNotifierStats, *, dry_run: bool) -> None:
    """Сводка в лог: кому ушло, кому нет, ошибки Telegram и PATCH Underdog."""
    if dry_run:
        logger.info(
            "IP notify (dry-run): план — {} получателей (preview)",
            len(stats.dry_run_preview),
        )
        if stats.dry_run_preview:
            logger.info(
                "IP notify dry-run preview:\n{}",
                json.dumps(stats.dry_run_preview, ensure_ascii=False, indent=2),
            )
        if stats.unknown_user or stats.missing_contact:
            logger.info(
                "IP notify dry-run: без рассылки — unknown_user={}, missing_contact={}, см. unknown_items",
                stats.unknown_user,
                stats.missing_contact,
            )
        return

    logger.info(
        "IP notify итог: доставлено записей={}, ошибок отправки в TG={}, сбоев пометки в Underdog={}",
        len(stats.delivered),
        len(stats.send_failures),
        len(stats.underdog_mark_failures),
    )
    if stats.unknown_user or stats.missing_contact:
        logger.info(
            "IP notify без рассылки: unknown_user={} (нет в tg_users), missing_contact={} (нет username у IP)",
            stats.unknown_user,
            stats.missing_contact,
        )
    if stats.unknown_items:
        preview = stats.unknown_items[:50]
        extra = len(stats.unknown_items) - len(preview)
        logger.info(
            "IP notify — не рассылали по этим IP (нет контакта / нет в БД), записей={}{}:\n{}",
            len(stats.unknown_items),
            f", показано {len(preview)}" if extra > 0 else "",
            json.dumps(preview, ensure_ascii=False, indent=2),
        )
        if extra > 0:
            logger.info("IP notify — … и ещё {} записей в unknown_items", extra)
    if stats.delivered:
        logger.info(
            "IP notify — кому отправилось:\n{}",
            json.dumps(stats.delivered, ensure_ascii=False, indent=2),
        )
    if stats.send_failures:
        logger.warning(
            "IP notify — кому НЕ отправилось (ошибка Telegram):\n{}",
            json.dumps(stats.send_failures, ensure_ascii=False, indent=2),
        )
    if stats.underdog_mark_failures:
        logger.warning(
            "IP notify — в TG ушло, но PATCH telegram-sent в Underdog не удался:\n{}",
            json.dumps(stats.underdog_mark_failures, ensure_ascii=False, indent=2),
        )


@dataclass(slots=True)
class TicketNotifierStats:
    total_tickets: int = 0
    completed_tickets: int = 0
    matched_users: int = 0
    notified_users: int = 0
    notified_tickets: int = 0
    missing_contact: int = 0
    unknown_user: int = 0
    no_recipient_mapping: int = 0
    errors: int = 0
    delivery_failed: int = 0
    unknown_items: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        payload = {
            "total_tickets": self.total_tickets,
            "completed_tickets": self.completed_tickets,
            "matched_users": self.matched_users,
            "notified_users": self.notified_users,
            "notified_tickets": self.notified_tickets,
            "missing_contact": self.missing_contact,
            "unknown_user": self.unknown_user,
            "no_recipient_mapping": self.no_recipient_mapping,
            "errors": self.errors,
            "delivery_failed": self.delivery_failed,
            "unknown_items": self.unknown_items,
        }
        if dry_run is not None:
            payload["dry_run"] = dry_run
        return payload
