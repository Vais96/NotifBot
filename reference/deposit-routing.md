# Deposit routing: who receives a Keitaro deposit

Reference map of the code path a Keitaro sale postback takes and every rule that
decides who gets the Telegram message. Written 2026-09-03 while debugging
"Татьяна Русанова не получает депозиты своей команды".

## Pipeline

| Step | Location |
| --- | --- |
| HTTP entry (POST/GET) | [app.py:620](../src/app.py#L620), [app.py:659](../src/app.py#L659) |
| Auth (`POSTBACK_TOKEN`, optional) | [app.py:193](../src/app.py#L193) |
| Backgrounded so Keitaro gets 200 fast | [app.py:251](../src/app.py#L251) |
| Main logic | [app.py:266](../src/app.py#L266) `_process_keitaro_postback` |
| Sale dedupe (SHA-256 of conversion_id or click id) | [keitaro_postbacks.py:51](../src/services/keitaro_postbacks.py#L51), [db.py:1196](../src/db.py#L1196) |
| Message text | [keitaro_postbacks.py:90](../src/services/keitaro_postbacks.py#L90) |

## Step 1 — resolve the buyer

1. `alias_key = campaign_name.split("_", 1)[0]` → `db.find_alias` ([db.py:1751](../src/db.py#L1751)).
   `tg_aliases.buyer_id` wins and is **authoritative** (not role-filtered).
2. Else `db.find_user_for_postback(offer, country, source)` → `tg_routes` ([db.py:1174](../src/db.py#L1174)).
   Result **is** role-filtered: dropped unless role ∈ {buyer, lead, mentor, head} ([app.py:339-346](../src/app.py#L339-L346)).
3. Else fall back to `settings.admins[0]` / first DB admin — but then `routed_id` is
   forced to `None`, so the event is logged unrouted and no daily counter is computed.

## Step 2 — recipient set ([app.py:412-466](../src/app.py#L412-L466))

Always (any postback, sale or not):
- every `tg_users` row with `role='admin' AND is_active=1`
- every id in the `ADMINS` env var

Only when `is_sale` (status ∈ sale/approved/confirmed/purchase/paid/success):
- the buyer themself (`buyer_id`)
- `alias.lead_id`, if the matched alias row has one
- **team leads of the buyer's team** → `db.list_team_leads(buyer.team_id)`, skipped when the
  buyer's own role is `mentor`
- mentors subscribed to that team (`tg_mentor_teams`)
- **all** `role='head' AND is_active=1` users — heads get every deposit company-wide
- helpers linked to the buyer (`tg_helper_buyer`)

Note the team-lead branch runs only `if buyer_user.get("team_id")` — a buyer with
`team_id IS NULL` silently notifies nobody but admins/heads.

## Step 3 — `list_team_leads(team_id)` ([db.py:1073](../src/db.py#L1073))

Union of exactly two sets:
- `tg_users WHERE role='lead' AND team_id=<team> AND is_active=1`
- `tg_team_leads_extra WHERE team_id=<team>` joined to an active user

**There is no other way to receive a team's deposits.** Being named in the team's
name, being `isPrimary`, or being the highest-paid buyer on it changes nothing.

## Source of truth: the Admin API

`tg_users.role` / `team_id` are overwritten hourly by
`GET {NEW_ADMIN_API_URL}/users` (`NEW_ADMIN_SYNC_INTERVAL_SECONDS=3600`, first run at
startup) — [new_admin_sync.py](../src/new_admin_sync.py) → [db.sync_employee_directory](../src/db.py#L868).

Role mapping ([new_admin_sync.py:73](../src/new_admin_sync.py#L73)), in order:
1. `position` contains `assistant`/`помощ` → `helper`
2. **any** `teamMemberships[].isManager == true` → `lead`
3. `position` or `roles[]` looked up in `_ROLE_MAP` (buyer/lead/head/bizdev→head/admin/mentor/helper)
4. otherwise `None`

Writeback semantics — these are the traps:
- `role=COALESCE(%s, role)` — a non-null directory role **overwrites any manual `/setrole`**
  within the hour. Manual role edits only survive when the directory yields `None`.
- `team_id=%s` unconditional — no team in the directory ⇒ `team_id` set to `NULL`.
- Teams absent from the directory are **deleted**, and their members' `team_id` nulled
  ([db.py:893-920](../src/db.py#L893-L920)).
- `isObserver: true` memberships are written into `tg_team_leads_extra` so observers
  receive that team's deposits ([db.py:993-1046](../src/db.py#L993-L1046)).
- ⚠️ `tg_team_leads_extra.team_id` is the **PRIMARY KEY** ([db.py:117-123](../src/db.py#L117-L123)) —
  only **one** extra lead per team. The sync's `ON DUPLICATE KEY UPDATE user_id=VALUES(user_id)`
  overwrites whoever held the slot, and it warns + drops any second observer on a team
  ([db.py:1010-1018](../src/db.py#L1010-L1018)).
- Since dc10df7 the bot's manual "назначить лидом команды" button and `/setteam` are gone —
  team membership and leadership can only be changed in the Admin panel.

## Diagnostic queries

```sql
-- is X a lead anywhere?
SELECT u.telegram_id,u.username,u.full_name,u.role,u.team_id,u.is_active,t.name
FROM tg_users u LEFT JOIN tg_teams t ON t.id=u.team_id WHERE u.full_name LIKE '%…%';
SELECT * FROM tg_team_leads_extra WHERE user_id=<tg_id>;

-- who actually receives team N's deposits?
SELECT telegram_id,full_name FROM tg_users WHERE role='lead' AND team_id=N AND is_active=1
UNION SELECT u.telegram_id,u.full_name FROM tg_team_leads_extra e
  JOIN tg_users u ON u.telegram_id=e.user_id WHERE e.team_id=N AND u.is_active=1;
```

Admin API check: `GET /users` with header `X-API-Key: $NEW_ADMIN_API_KEY`, then read the
person's `position`, `roles`, and `teamMemberships[].isManager / isObserver`.

## Known data hazards (updated 2026-09-03, after the Русанова fix)

Pattern to watch: **a buyer whose team is named after them but who is not `isManager`
of it.** They receive only their own deposits; the team's deposits go to whoever holds
the observer slot. Fixed for Татьяна Русанова; still open for:

| Employee | Team | Deposits/14d | Currently received by |
| --- | --- | --- | --- |
| Олег Синявин `@oleg_underdog` | Команда Олега Синявина | 349 | Владислав Сергиенко (observer) |
| Матвей Яковлев `@matsvei_underdog` | Команда Матвея Яковлева | 240 | Владислав Сергиенко (observer) |
| Лев Ямщиков `@Lev_Underdog` | Команда Льва Ямщикова | 93 | Диана Симич (observer) |
| Арсений Симич `@underdog_headofbuying` | Команда Арсения Симича | 109 | никем как лидом — но он `admin`, а админы получают всё |

Detection script: `scratchpad/eponym.py` pattern — for each ACTIVE user, compare `fullName`
stems against every `teamMemberships[].teamName` and flag matches with `isManager: false`.

Other hazards:
- `tg_users.username='maria_underdog'` exists on two rows (8468335562 Maryia Charniauskaya,
  8688104187 Мария Чернявская). Username→telegram_id matching in the sync is ambiguous here.
- Of the 89 ACTIVE directory records, 45 never resolve to a Telegram ID (no `telegramId`,
  and the handle is absent from `tg_users`), so the sync silently counts them as `skipped`.
  21 of those hold a buying role (buyer/lead/helper/head) — see the HR list. (An earlier
  count of ~101 included FIRED records and overstated this.)
- Orphans exist on the other side too: `tg_users` rows no directory record resolves to,
  e.g. `Vladimir Samarin @vs_underdog` (146 dep/30d) whose Admin handle reads
  `@vladimirs_underdog`. Their directory role and team therefore never apply.
