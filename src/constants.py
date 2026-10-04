"""Shared constants: sale statuses and roles."""

from enum import StrEnum

# Keitaro conversion statuses that count as a deposit (postback routing, daily counters, reports)
SALE_STATUSES: tuple[str, ...] = (
    "sale", "approved", "approve", "confirmed", "confirm", "purchase", "purchased", "paid", "success", "ftd",
)


class Role(StrEnum):
    BUYER = "buyer"
    LEAD = "lead"
    HEAD = "head"
    ADMIN = "admin"
    MENTOR = "mentor"
    HELPER = "helper"


class PendingAction(StrEnum):
    """tg_pending_actions.action prefixes; parametrized ones are stored as "<action>:<arg>"."""

    FB_AWAIT_CSV = "fb:await_csv"
    ALIAS_NEW = "alias:new"
    ALIAS_SET_BUYER = "alias:setbuyer"
    ALIAS_SET_LEAD = "alias:setlead"
    DOMAIN_CHECK = "domain:check"
    YOUTUBE_AWAIT_URL = "youtube:await_url"
    MENTOR_ADD = "mentor:add"
    HELPER_ADD = "helper:add"
    KPI_SET = "kpi:set"
