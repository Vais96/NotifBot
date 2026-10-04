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
