"""Admin alerts and the send+log helper shared by the Underdog notifiers (orders, domains, IPs, tickets)."""

from typing import Any, Callable, Dict, List, Optional

from loguru import logger

from ... import db
from ...telegram_rate_limit import limited_send_message
from ..telegram_confirm import _log_telegram_send_roundtrip

DIGEST_LIMIT = 20


class AdminAlerts:
    """Mixin for notifier dataclasses with `bot`, `admin_ids` and an optional `admin_bot`."""

    __slots__ = ()

    async def _send_admin_dm(self, admin_id: int, text: str) -> None:
        """admin_bot (main bot) first, then bot — alerts reach admins who never opened the orders bot."""
        admin_bot = getattr(self, "admin_bot", None)
        bots = [b for b in (admin_bot, self.bot) if b is not None]
        if admin_bot is self.bot:
            bots = [self.bot]
        last_exc: Optional[BaseException] = None
        for bot in bots:
            try:
                await limited_send_message(bot, int(admin_id), text=text)
                return
            except Exception as exc:
                last_exc = exc
        if last_exc is not None:
            raise last_exc

    async def _send_and_log(self, chat_id: int, text: str, *, context: str, extra: Dict[str, Any]) -> Any:
        """Send to the recipient and log the Bot API round-trip (used to decide whether to mark Underdog)."""
        msg = await limited_send_message(self.bot, chat_id, text=text)
        _log_telegram_send_roundtrip(context=context, chat_id=chat_id, text=text, msg=msg, extra=extra)
        return msg

    async def _send_to_admins(self, text: str, *, what: str, exclude_user_id: Optional[int] = None) -> None:
        for admin_id in self.admin_ids:
            if exclude_user_id is not None and int(admin_id) == exclude_user_id:
                continue  # the recipient is an admin: they already got the message
            try:
                await self._send_admin_dm(int(admin_id), text)
            except Exception as exc:
                logger.warning("Failed to notify admin {} about {}: {}: {}", admin_id, what, type(exc).__name__, exc)

    async def _alert_admins_throttled(self, text: str, *, dedupe_key: str, what: str, dry_run: bool = False) -> None:
        """One alert per key per hour (and silence after 24h) — see db.admin_notify_throttle_allow_send."""
        if not self.admin_ids:
            return
        if dry_run:
            logger.info("Dry-run: would alert admins about {}", what)
            return
        if not await db.admin_notify_throttle_allow_send(dedupe_key):
            return
        await self._send_to_admins(text, what=what)

    async def _alert_admins_digest(
        self, items: List[Any], *, title: str, render: Callable[[Any], str], noun: str, dedupe_key: str
    ) -> None:
        """Throttled list of up to DIGEST_LIMIT undeliverable items."""
        if not self.admin_ids or not items:
            return
        lines = [title, ""] + [render(item) for item in items[:DIGEST_LIMIT]]
        if len(items) > DIGEST_LIMIT:
            lines.append(f"… и ещё {len(items) - DIGEST_LIMIT} {noun}")
        await self._alert_admins_throttled("\n".join(lines), dedupe_key=dedupe_key, what=title)
