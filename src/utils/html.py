"""HTML-safe text for messages sent with parse_mode=HTML."""

import html
from typing import Any, Mapping, Optional


def safe(value: Any) -> str:
    """Escape any external text (names, usernames, aliases, domains, API fields, exception text)."""
    return html.escape(str(value if value is not None else ""))


def user_label(row: Optional[Mapping[str, Any]], user_id: Any = None) -> str:
    """'@username', else full name, else the id in <code> — already escaped."""
    row = row or {}
    username = str(row.get("username") or "").strip().lstrip("@")
    if username:
        return f"@{safe(username)}"
    full_name = str(row.get("full_name") or "").strip()
    if full_name:
        return safe(full_name)
    uid = row.get("telegram_id", user_id)
    return f"<code>{safe(uid)}</code>" if uid is not None else "—"
