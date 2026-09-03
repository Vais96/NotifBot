# Second Bot Integration Spec (Teams / Leads / Mentors)

This document defines the minimal data contract for any external bot that must work with the same team-management data as this bot.

## Source of Truth

Use the same MySQL database and these tables:

- `tg_users` - user profile + role + primary team
- `tg_teams` - teams directory
- `tg_team_leads_extra` - extra lead per team (override)
- `tg_mentor_teams` - mentor-to-team mapping (many-to-many)

## Roles and Semantics

Allowed `tg_users.role` values:

- `buyer`
- `lead`
- `head`
- `admin`
- `mentor`
- `helper`

Leader access is resolved from:

1. Primary role-based lead: `tg_users.role in ('lead','head')` with `tg_users.team_id = <team_id>`
2. Extra lead override: `tg_team_leads_extra.team_id = <team_id>`

Mentor assignment is split:

- user role marker: `tg_users.role = 'mentor'`
- actual team links: `tg_mentor_teams(mentor_id, team_id)`

## Required Flows for Second Bot

### 1) Ensure user exists

Always upsert user before role/team/mentor operations.

```sql
INSERT INTO tg_users (telegram_id, username, full_name, role, team_id, is_active)
VALUES (?, ?, ?, 'buyer', NULL, 1)
ON DUPLICATE KEY UPDATE
  username = COALESCE(NULLIF(VALUES(username), ''), username),
  full_name = COALESCE(NULLIF(VALUES(full_name), ''), full_name),
  is_active = 1;
```

### 2) Set primary team

```sql
UPDATE tg_users
SET team_id = ?
WHERE telegram_id = ?;
```

Use `team_id = NULL` to remove from team.

### 3) Set role

```sql
UPDATE tg_users
SET role = ?
WHERE telegram_id = ?;
```

Validate role strictly to allowed enum values.

### 4) Set extra lead for a team

One extra lead per team:

```sql
INSERT INTO tg_team_leads_extra (team_id, user_id)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
  user_id = VALUES(user_id),
  created_at = CURRENT_TIMESTAMP;
```

Clear override:

```sql
DELETE FROM tg_team_leads_extra
WHERE team_id = ?;
```

### 5) Add/remove mentor-team link

Add:

```sql
INSERT INTO tg_mentor_teams (mentor_id, team_id)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
  created_at = CURRENT_TIMESTAMP;
```

Remove:

```sql
DELETE FROM tg_mentor_teams
WHERE mentor_id = ? AND team_id = ?;
```

## Read Queries (Common)

### Teams list

```sql
SELECT id, name, created_at
FROM tg_teams
ORDER BY id DESC;
```

### Users list

```sql
SELECT telegram_id, username, full_name, role, team_id, is_active, created_at
FROM tg_users
ORDER BY created_at DESC;
```

### Teams that user leads

```sql
-- Primary lead/head + extra overrides
SELECT DISTINCT team_id
FROM (
  SELECT u.team_id AS team_id
  FROM tg_users u
  WHERE u.telegram_id = ?
    AND u.is_active = 1
    AND u.role IN ('lead', 'head')
    AND u.team_id IS NOT NULL
  UNION
  SELECT e.team_id
  FROM tg_team_leads_extra e
  JOIN tg_users u ON u.telegram_id = e.user_id
  WHERE e.user_id = ?
    AND u.is_active = 1
) t;
```

### Mentors for team

```sql
SELECT mentor_id
FROM tg_mentor_teams
WHERE team_id = ?;
```

## Consistency Rules

- Never write unknown role values.
- Keep `telegram_id` immutable as primary key.
- If user is disabled in your flow, set `is_active = 0` instead of hard delete.
- Upsert user first, then role/team/link updates.
- Do not assume `username` exists; it may be `NULL`.
- Treat `tg_team_leads_extra` as optional override, not replacement for primary lead role.

## Suggested Transaction Boundaries

For multi-step commands, run in one transaction:

- assign lead: `upsert user -> set role/team -> set/clear extra lead`
- assign mentor: `upsert user -> set role='mentor' -> add mentor-team rows`

## Compatibility Note

Current bot code paths for this data live in `src/db.py` and `src/handlers/teams.py` / `src/handlers/mentors.py`.
If schema changes there, this spec must be updated in lockstep.
