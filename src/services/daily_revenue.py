"""Pure helpers for the end-of-day revenue summary sent to leads, mentors and heads.

Scope rules mirror deposit routing in ``app._process_keitaro_postback``:

* ``head``   — every buyer company-wide (plus unrouted deposits).
* ``lead``   — buyers of the team they lead (``tg_users.team_id`` when ``role='lead'``),
  teams granted via ``tg_team_leads_extra`` (Admin observers), and buyers whose alias
  names them as lead. Mentors' own deposits are hidden from leads, as in routing.
* ``mentor`` — buyers of every team in ``tg_mentor_teams``.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Mapping, Sequence

from .formatting import chunk_lines

REPORT_ROLES = frozenset({"lead", "mentor", "head"})
NO_TEAM_LABEL = "Без команды"
UNROUTED_LABEL = "Не распределено (без байера)"


@dataclass(frozen=True)
class SaleStat:
    user_id: int | None
    count: int
    revenue: float


@dataclass
class ReportScope:
    """Which deposits a recipient sees: ``all`` or an explicit buyer set."""

    all: bool = False
    buyer_ids: set[int] = field(default_factory=set)

    def includes(self, user_id: int | None) -> bool:
        if self.all:
            return True
        return user_id is not None and user_id in self.buyer_ids


def resolve_scope(
    recipient: Mapping[str, Any],
    users: Sequence[Mapping[str, Any]],
    *,
    extra_lead_teams: Iterable[int] = (),
    mentor_teams: Iterable[int] = (),
    alias_buyers: Iterable[int] = (),
) -> ReportScope | None:
    """Return the recipient's scope, or None when their role gets no summary."""
    role = recipient.get("role")
    if role not in REPORT_ROLES or not recipient.get("is_active", 1):
        return None
    if role == "head":
        return ReportScope(all=True)

    team_ids: set[int] = set()
    if role == "lead":
        if recipient.get("team_id") is not None:
            team_ids.add(int(recipient["team_id"]))
        team_ids.update(int(t) for t in extra_lead_teams)
    else:  # mentor
        team_ids.update(int(t) for t in mentor_teams)

    buyer_ids: set[int] = set()
    for user in users:
        team_id = user.get("team_id")
        if team_id is None or int(team_id) not in team_ids:
            continue
        if role == "lead" and user.get("role") == "mentor":
            continue
        buyer_ids.add(int(user["telegram_id"]))
    if role == "lead":
        buyer_ids.update(int(b) for b in alias_buyers)
    return ReportScope(all=False, buyer_ids=buyer_ids)


def _fmt_amount(value: float) -> str:
    return f"{int(round(value)):,}".replace(",", " ")


def _display_name(user: Mapping[str, Any] | None, user_id: int) -> str:
    if user:
        full_name = (user.get("full_name") or "").strip()
        username = (user.get("username") or "").strip().lstrip("@")
        if full_name and username:
            return f"{full_name} (@{username})"
        if full_name:
            return full_name
        if username:
            return f"@{username}"
    return f"id {user_id}"


def build_report_messages(
    day: date,
    sales: Sequence[SaleStat],
    scope: ReportScope,
    users: Sequence[Mapping[str, Any]],
    teams: Mapping[int, str],
    *,
    limit: int = 3500,
) -> list[str]:
    """Render the summary as one or more HTML messages (split under Telegram's limit)."""
    users_by_id = {int(u["telegram_id"]): u for u in users}
    visible = [s for s in sales if s.count > 0 and scope.includes(s.user_id)]
    day_label = day.strftime("%d.%m.%Y")
    header = f"📊 <b>ИТОГИ ДНЯ · {day_label}</b>"

    if not visible:
        return [f"{header}\nСегодня депозитов по вашим командам не было."]

    total_count = sum(s.count for s in visible)
    total_revenue = sum(s.revenue for s in visible)

    grouped: dict[int | None, list[SaleStat]] = {}
    unrouted: SaleStat | None = None
    for stat in visible:
        if stat.user_id is None:
            unrouted = stat
            continue
        user = users_by_id.get(stat.user_id)
        team_id = user.get("team_id") if user else None
        grouped.setdefault(int(team_id) if team_id is not None else None, []).append(stat)

    def _team_sort_key(item: tuple[int | None, list[SaleStat]]) -> tuple[int, float]:
        team_id, stats = item
        # teams with a name first, then "no team", ordered by revenue desc
        return (1 if team_id is None else 0, -sum(s.revenue for s in stats))

    lines = [
        header,
        f"💵 Доход: <b>{_fmt_amount(total_revenue)}</b>",
        f"📈 Депозитов: <b>{total_count}</b>",
    ]
    for team_id, stats in sorted(grouped.items(), key=_team_sort_key):
        team_name = teams.get(team_id, f"Команда #{team_id}") if team_id is not None else NO_TEAM_LABEL
        team_revenue = sum(s.revenue for s in stats)
        team_count = sum(s.count for s in stats)
        lines.append("")
        lines.append(
            f"🏷 <b>{html.escape(team_name, quote=False)}</b> — "
            f"{_fmt_amount(team_revenue)} · {team_count} деп."
        )
        for stat in sorted(stats, key=lambda s: (-s.revenue, -s.count)):
            name = _display_name(users_by_id.get(stat.user_id), stat.user_id)  # type: ignore[arg-type]
            lines.append(
                f"  • {html.escape(name, quote=False)} — {_fmt_amount(stat.revenue)} · {stat.count}"
            )
    if unrouted is not None:
        lines.append("")
        lines.append(
            f"⚠️ <b>{UNROUTED_LABEL}</b> — {_fmt_amount(unrouted.revenue)} · {unrouted.count} деп."
        )
    return chunk_lines(lines, limit=limit)


__all__ = [
    "NO_TEAM_LABEL",
    "REPORT_ROLES",
    "ReportScope",
    "SaleStat",
    "UNROUTED_LABEL",
    "build_report_messages",
    "resolve_scope",
]
