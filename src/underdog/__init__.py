"""Underdog API integration: client, message builders and Telegram notifiers (orders, design, domains, IPs, tickets).

Everything is re-exported here so `from . import underdog; underdog.notify_*()` keeps working.
"""

from .. import db  # noqa: F401 — tests patch src.underdog.db.<func>
from .client import (  # noqa: F401
    LOGIN_PATH,
    ORDERS_PATH,
    DOMAINS_PATH,
    IPS_PATH,
    TICKETS_PATH,
    DEFAULT_TIMEOUT,
    REQUEST_ATTEMPTS,
    REQUEST_BACKOFF_SECONDS,
    _TokenCache,
    _extract_token,
    UnderdogClient,
)
from .common import (  # noqa: F401
    UnderdogAuthError,
    UnderdogAPIError,
    _extract_items,
    _extract_domains,
    _extract_ips,
    _log_underdog_raw_json_response,
    _extract_ip_record_id,
    _extract_tickets,
    _normalize_handle,
    _parse_telegram_id,
    _resolve_corporate_owner_fields,
    _corp_handle_admin_line,
    resolve_underdog_notify_admin_ids,
    _resolve_order_owner_handle,
    _entry_ids,
    _drop_locally_sent,
    _to_utc_aware,
    _parse_date,
    _is_telegram_sent,
)
from .messages import (  # noqa: F401
    ORDER_STATUS_TEXTS,
    _order_status_text,
    _build_order_message,
    _build_design_assignment_message,
    _format_duration_ru,
    _build_design_completion_message,
    _build_design_sla_warning_message,
    _build_design_not_in_progress_48h_message,
    _build_domain_notification,
    _build_ip_notification,
    _get_ticket_type_name,
    _build_ticket_notification,
)
from .stats import (  # noqa: F401
    NotificationStats,
    DomainNotifierStats,
    IPNotifierStats,
    _log_ip_notify_delivery_report,
    TicketNotifierStats,
)
from .telegram_confirm import (  # noqa: F401
    _truncate_log_text,
    _chat_to_log_dict,
    _telegram_api_response_dict,
    _log_telegram_send_roundtrip,
    _telegram_message_confirmed,
    _telegram_http_ok_for_underdog,
    _telegram_underdog_send_confirmed,
)
from .notifiers.orders import (  # noqa: F401
    OrderNotifier,
)
from .notifiers.design import (  # noqa: F401
    _finish_design_delivery,
    _resolve_designer_telegram_id_from_order,
    _is_design_order_completed,
    _is_design_order_taken_in_progress,
    _is_design_order_awaiting_take_in_progress,
    DesignAssignmentNotifier,
    DesignCompletionNotifier,
    DesignSLA24hNotifier,
    DesignNotInProgress48hNotifier,
)
from .notifiers.domains import (  # noqa: F401
    DomainNotifier,
)
from .notifiers.ips import (  # noqa: F401
    IPNotifier,
)
from .notifiers.tickets import (  # noqa: F401
    TicketNotifier,
)
from .run import (  # noqa: F401
    _create_bot,
    _orders_and_main_bots_differ,
    _create_main_bot,
    _create_design_bot,
    notify_ready_orders,
    notify_design_assignments,
    notify_design_completions,
    notify_design_sla_24h,
    notify_design_not_in_progress_48h,
    notify_expiring_domains,
    notify_expiring_ips,
    notify_completed_tickets,
)
