"""Atomic sync of the Admin employee directory into users, teams, helpers, observers and aliases."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import aiomysql
from loguru import logger

from ..constants import Role
from .pool import transaction


def _norm_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _is_active(person: Any) -> bool:
    return getattr(person, "is_active", True)


@dataclass
class _SyncState:
    """Everything the steps share inside one transaction."""

    cur: aiomysql.Cursor
    stats: Dict[str, int]
    username_to_id: Dict[str, int] = field(default_factory=dict)
    ambiguous_usernames: set = field(default_factory=set)
    known_ids: set = field(default_factory=set)
    teams: Dict[str, int] = field(default_factory=dict)  # lower(name) -> id
    resolved: List[Tuple[Any, int]] = field(default_factory=list)
    external_id_to_telegram_id: Dict[str, int] = field(default_factory=dict)

    async def team_id_for(self, team_name: str) -> int:
        key = str(team_name).strip().lower()
        team_id = self.teams.get(key)
        if team_id is None:
            await self.cur.execute("INSERT INTO tg_teams(name) VALUES(%s)", (team_name,))
            team_id = int(self.cur.lastrowid)
            self.teams[key] = team_id
            self.stats["teams_created"] += 1
        return team_id


async def _load_users(st: _SyncState) -> None:
    """Username map without ambiguous names: a duplicate username in tg_users must not pick a random user."""
    await st.cur.execute("SELECT telegram_id, username FROM tg_users")
    rows = await st.cur.fetchall() or []
    for row in rows:
        key = _norm_username(row.get("username"))
        if not key:
            continue
        other = st.username_to_id.get(key)
        if other is not None and other != int(row["telegram_id"]):
            logger.warning(
                "Duplicate username in tg_users, not matching it: @{} -> {} and {}",
                key, other, int(row["telegram_id"]),
            )
            st.ambiguous_usernames.add(key)
        st.username_to_id[key] = int(row["telegram_id"])
    for key in st.ambiguous_usernames:
        st.username_to_id.pop(key, None)
    st.known_ids = {int(row["telegram_id"]) for row in rows}


async def _delete_stale_teams(st: _SyncState, employees: List[Any]) -> None:
    """The directory is authoritative for teams; disabled employees do not keep a historic team alive."""
    await st.cur.execute("SELECT id, name FROM tg_teams")
    team_rows = await st.cur.fetchall() or []
    st.teams = {str(row["name"]).strip().lower(): int(row["id"]) for row in team_rows}
    authoritative: set = set()
    for person in employees:
        if not _is_active(person):
            continue
        for raw_name in (getattr(person, "team_name", None), *tuple(getattr(person, "observer_team_names", ()) or ())):
            if raw_name and str(raw_name).strip() != "-":
                authoritative.add(str(raw_name).strip().lower())
    stale_team_ids = [int(row["id"]) for row in team_rows if str(row["name"]).strip().lower() not in authoritative]
    if not stale_team_ids:
        return
    placeholders = ",".join(["%s"] * len(stale_team_ids))
    await st.cur.execute(f"UPDATE tg_users SET team_id=NULL WHERE team_id IN ({placeholders})", tuple(stale_team_ids))
    await st.cur.execute(f"DELETE FROM tg_teams WHERE id IN ({placeholders})", tuple(stale_team_ids))
    st.stats["teams_deleted"] = len(stale_team_ids)
    st.teams = {key: value for key, value in st.teams.items() if value not in stale_team_ids}


async def _resolve_people(st: _SyncState, employees: List[Any]) -> None:
    """A record without a Telegram ID is matched only to an existing local username."""
    for person in employees:
        uid: Optional[int] = getattr(person, "telegram_id", None)
        uid = int(uid) if uid else st.username_to_id.get(_norm_username(getattr(person, "username", None)))
        if uid is None:
            st.stats["skipped"] += 1
            continue
        st.stats["matched"] += 1
        if uid not in st.known_ids:
            await st.cur.execute(
                "INSERT INTO tg_users(telegram_id, username, full_name) VALUES(%s, %s, %s)",
                (uid, getattr(person, "username", None), getattr(person, "full_name", None)),
            )
            st.known_ids.add(uid)
            username = _norm_username(getattr(person, "username", None))
            if username and username not in st.ambiguous_usernames:
                st.username_to_id[username] = uid
        st.resolved.append((person, uid))
        external_id = getattr(person, "external_id", None)
        if external_id:
            st.external_id_to_telegram_id[str(external_id)] = uid


async def _update_users(st: _SyncState) -> None:
    for person, uid in st.resolved:
        team_id = None
        team_name = getattr(person, "team_name", None)
        if team_name and _is_active(person) and str(team_name).strip() != "-":
            team_id = await st.team_id_for(team_name)
        await st.cur.execute(
            "UPDATE tg_users SET username=COALESCE(NULLIF(%s, ''), username), "
            "full_name=COALESCE(NULLIF(%s, ''), full_name), "
            "role=COALESCE(%s, role), team_id=%s WHERE telegram_id=%s",
            (getattr(person, "username", None), getattr(person, "full_name", None),
             getattr(person, "role", None), team_id, uid),
        )
        await st.cur.execute(
            "UPDATE tg_users SET is_active=%s WHERE telegram_id=%s",
            (1 if _is_active(person) else 0, uid),
        )
        st.stats["users_updated"] += 1


async def _sync_helper_links(st: _SyncState) -> None:
    for person, helper_id in st.resolved:
        if getattr(person, "role", None) != Role.HELPER:
            continue
        buyer_id = getattr(person, "helper_for_telegram_id", None)
        if buyer_id is None:
            external_id = getattr(person, "helper_for_external_id", None)
            buyer_id = st.external_id_to_telegram_id.get(str(external_id)) if external_id else None
        if buyer_id is None:
            buyer_id = st.username_to_id.get(_norm_username(getattr(person, "helper_for_username", None)))
        if buyer_id and int(buyer_id) in st.known_ids:
            await st.cur.execute(
                "INSERT INTO tg_helper_buyer(helper_id, buyer_id) VALUES(%s, %s) AS new "
                "ON DUPLICATE KEY UPDATE buyer_id=new.buyer_id",
                (helper_id, int(buyer_id)),
            )
            st.stats["helper_links_updated"] += 1


async def _sync_observer_leads(st: _SyncState) -> None:
    """Admin isObserver memberships become extra leads (team deposits + reports for those teams)."""
    observer_user_ids: set = set()
    desired_extra: Dict[int, int] = {}
    for person, uid in st.resolved:
        if not _is_active(person):
            continue
        for team_name in getattr(person, "observer_team_names", ()) or ():
            key = str(team_name).strip().lower()
            if not key or key == "-":
                continue
            team_id = await st.team_id_for(team_name)
            existing = desired_extra.get(team_id)
            if existing is not None and existing != uid:
                logger.warning(
                    "Multiple observers for team; keeping first extra lead",
                    team_id=team_id,
                    kept_user_id=existing,
                    skipped_user_id=uid,
                )
                continue
            desired_extra[team_id] = uid
            observer_user_ids.add(uid)

    if observer_user_ids:
        placeholders = ",".join(["%s"] * len(observer_user_ids))
        await st.cur.execute(
            f"SELECT team_id, user_id FROM tg_team_leads_extra WHERE user_id IN ({placeholders})",
            tuple(observer_user_ids),
        )
        for row in await st.cur.fetchall() or []:
            team_id, user_id = int(row["team_id"]), int(row["user_id"])
            if desired_extra.get(team_id) != user_id:
                await st.cur.execute(
                    "DELETE FROM tg_team_leads_extra WHERE team_id=%s AND user_id=%s", (team_id, user_id)
                )

    # Directory users without observer teams lose stale extra-lead rows. Mentors keep theirs:
    # cb_set_role(mentor) writes tg_team_leads_extra manually.
    former_observer_ids = sorted(
        uid for person, uid in st.resolved
        if uid not in observer_user_ids and getattr(person, "role", None) != Role.MENTOR
    )
    if former_observer_ids:
        placeholders = ",".join(["%s"] * len(former_observer_ids))
        await st.cur.execute(
            f"DELETE FROM tg_team_leads_extra WHERE user_id IN ({placeholders})", tuple(former_observer_ids)
        )
        if st.cur.rowcount:
            logger.info("Removed {} stale extra-lead rows (no observer teams in Admin)", st.cur.rowcount)

    for team_id, user_id in desired_extra.items():
        await st.cur.execute(
            """
            INSERT INTO tg_team_leads_extra(team_id, user_id)
            VALUES(%s, %s) AS new
            ON DUPLICATE KEY UPDATE user_id=new.user_id, created_at=CURRENT_TIMESTAMP
            """,
            (team_id, user_id),
        )
        st.stats["observer_links_updated"] += 1


def campaign_aliases(person: Any) -> List[str]:
    """Campaign prefixes that route to this person: Admin «Имя в Keitaro» (old campaigns)
    and the Admin employee number (new Keitaro since 10.10.2026, `3_PWAPartners[...]`)."""
    aliases: List[str] = []
    for value in (getattr(person, "keitaro_name", None), getattr(person, "public_id", None)):
        alias = str(value or "").strip().lower()
        if alias and alias not in aliases:
            aliases.append(alias)
    return aliases


async def _sync_aliases(st: _SyncState) -> None:
    """Admin «Имя в Keitaro» and employee number are the campaign prefixes used for deposit routing."""
    seen_aliases: Dict[str, int] = {}
    for person, uid in st.resolved:
        if not _is_active(person):
            continue
        for alias in campaign_aliases(person):
            await _upsert_alias(st, seen_aliases, alias, uid)


async def _sync_campaign_names(st: _SyncState, employees: List[Any]) -> None:
    """Every active employee's campaign prefixes -> Admin name, Telegram or not (tg_aliases needs a tg user)."""
    resolved_ids = {id(person) for person, _ in st.resolved}
    for person in employees:
        if not _is_active(person):
            continue
        aliases = campaign_aliases(person)
        name = str(getattr(person, "full_name", None) or "").strip()
        if not aliases or not name:
            continue
        has_tg = id(person) in resolved_ids
        if not has_tg:
            logger.warning(
                "Admin employee has campaign aliases but no Telegram; deposits go to fallback",
                aliases=aliases, full_name=name, external_id=getattr(person, "external_id", None),
            )
            st.stats["aliases_without_telegram"] += 1
        for alias in aliases:
            await st.cur.execute(
                "INSERT INTO tg_campaign_names(alias, full_name, has_telegram) VALUES(%s, %s, %s) AS new "
                "ON DUPLICATE KEY UPDATE full_name=new.full_name, has_telegram=new.has_telegram",
                (alias, name[:255], 1 if has_tg else 0),
            )


async def _upsert_alias(st: _SyncState, seen_aliases: Dict[str, int], alias: str, uid: int) -> None:
    existing_owner = seen_aliases.get(alias)
    if existing_owner is not None and existing_owner != uid:
        logger.warning(
            "Duplicate Admin campaign alias; keeping first alias owner",
            alias=alias,
            kept_user_id=existing_owner,
            skipped_user_id=uid,
        )
        return
    seen_aliases[alias] = uid
    await st.cur.execute("SELECT buyer_id FROM tg_aliases WHERE alias=%s", (alias,))
    previous = await st.cur.fetchone()
    previous_buyer = previous.get("buyer_id") if previous else None
    if previous_buyer is not None and int(previous_buyer) != uid:
        logger.warning(
            "Admin campaign alias reassigns existing alias",
            alias=alias,
            previous_buyer_id=int(previous_buyer),
            new_buyer_id=uid,
        )
    await st.cur.execute(
        """
        INSERT INTO tg_aliases(alias, buyer_id, lead_id)
        VALUES(%s, %s, NULL) AS new
        ON DUPLICATE KEY UPDATE buyer_id=new.buyer_id
        """,
        (alias, uid),
    )
    st.stats["aliases_upserted"] += 1


async def sync_employee_directory(employees: List[Any]) -> Dict[str, int]:
    """Persist one trusted employee-directory snapshot atomically (all steps in one transaction)."""
    stats = {"received": len(employees), "matched": 0, "skipped": 0, "users_updated": 0,
             "teams_created": 0, "teams_deleted": 0, "helper_links_updated": 0,
             "observer_links_updated": 0, "aliases_upserted": 0,
             "aliases_without_telegram": 0}
    async with transaction(dict_rows=True) as cur:
        st = _SyncState(cur=cur, stats=stats)
        await _load_users(st)
        await _delete_stale_teams(st, employees)
        await _resolve_people(st, employees)
        await _update_users(st)
        await _sync_helper_links(st)
        await _sync_observer_leads(st)
        await _sync_aliases(st)
        await _sync_campaign_names(st, employees)
    return stats
