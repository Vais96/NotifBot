"""MySQL access layer. `from . import db; db.func()` keeps working: every public function is re-exported here."""

from .pool import (  # noqa: F401
    _parse_mysql_dsn,
    init_pool,
    close_pool,
    cursor,
    execute,
    fetch_one,
    fetch_all,
    transaction,
)
from .schema import (  # noqa: F401
    SCHEMA_SQL,
)
from .users import (  # noqa: F401
    upsert_user,
    list_users,
    get_user,
    set_user_role,
    set_orders_opt_out,
    set_user_active,
    get_helper_buyer,
    set_helper_buyer,
    remove_helper_and_promote_to_buyer,
    deactivate_user,
    list_helpers_by_buyer,
    list_helpers_with_buyers,
    list_users_as_buyer_candidates,
    fetch_users_by_usernames,
    find_user_by_username,
)
from .teams import (  # noqa: F401
    list_teams,
    set_team_lead_override,
    clear_team_lead_override,
    list_team_leads,
    list_user_lead_teams,
    get_primary_lead_team,
    list_extra_lead_teams,
)
from .directory_sync import (  # noqa: F401
    sync_employee_directory,
)
from .routing import (  # noqa: F401
    add_route,
    list_routes,
    find_user_for_postback,
    claim_keitaro_sale_postback,
    _parse_payout,
    log_event,
)
from .inbound import (  # noqa: F401
    enqueue_inbound_postback,
    claim_inbound_postback,
    finish_inbound_postback,
    list_inbound_postbacks_for_retry,
    requeue_inbound_postbacks,
)
from .sales_stats import (  # noqa: F401
    count_today_user_sales,
    sum_today_user_profit,
    today_alias_sales,
    sales_by_user_between,
    get_kpi,
    set_kpi,
    aggregate_sales,
    trend_daily_sales,
    get_report_filter,
    set_report_filter,
    clear_report_filter,
    list_offers_for_users,
    list_creatives_for_users,
)
from .aliases import (  # noqa: F401
    list_alias_lead_buyers,
    find_alias,
    find_campaign_name,
    _UNSET,
    set_alias,
    list_aliases,
    fetch_alias_map,
    delete_alias,
)
from .keitaro_campaigns import (  # noqa: F401
    upsert_keitaro_campaigns,
    find_campaigns_by_domain,
    infer_campaign_buyers,
    fetch_keitaro_campaign_stats,
)
from .design import (  # noqa: F401
    add_design_bot_subscriber,
    list_design_bot_subscribers,
    is_design_assignment_sent,
    mark_design_assignment_sent,
    is_design_completion_sent,
    mark_design_completion_sent,
    get_design_assignment_sent_at,
    list_design_assignments_pending_take_in_progress_reminder,
    find_telegram_id_among_subscribers_by_username,
    is_design_sla_24h_alert_sent,
    mark_design_sla_24h_alert_sent,
    is_design_not_in_progress_48h_sent,
    mark_design_not_in_progress_48h_sent,
    get_contractor_telegram_id,
)
from .notify_state import (  # noqa: F401
    admin_notify_throttle_allow_send,
    admin_notify_throttle_clear,
    underdog_sent_ids,
    mark_underdog_sent,
)
from .ui_cache import (  # noqa: F401
    set_ui_cache_list,
    get_ui_cache_value,
)
from .pending import (  # noqa: F401
    set_pending_action,
    get_pending_action,
    clear_pending_action,
)
from .mentors import (  # noqa: F401
    add_mentor_team,
    remove_mentor_team,
    list_mentor_teams,
    list_team_mentors,
)
from .fb_analytics import (  # noqa: F401
    create_fb_csv_upload,
    bulk_insert_fb_csv_rows,
    upsert_fb_accounts,
    upsert_fb_campaign_daily,
    upsert_fb_campaign_totals,
    fetch_fb_campaign_state,
    upsert_fb_campaign_state,
    log_fb_campaign_history,
    list_fb_flags,
    list_fb_available_months,
    fetch_fb_campaign_month_report,
    recompute_fb_campaign_totals,
    reset_fb_upload_data,
)
