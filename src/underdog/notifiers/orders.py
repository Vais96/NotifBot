"""Orders bot: completed Underdog orders -> owner."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Sequence
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from loguru import logger
from ... import db
from ...utils.html import safe
from ..client import UnderdogClient
from ..common import _drop_locally_sent, _resolve_order_owner_handle
from ..messages import _build_order_message
from ..stats import NotificationStats
from ..telegram_confirm import _telegram_api_response_dict, _telegram_message_confirmed, _telegram_underdog_send_confirmed
from .base import AdminAlerts


@dataclass(slots=True)
class OrderNotifier(AdminAlerts):
    underdog: UnderdogClient
    bot: Bot
    admin_ids: Sequence[int]

    async def notify_ready_orders(
        self,
        *,
        dry_run: bool = True,
        limit_user_ids: Optional[Iterable[int]] = None,
    ) -> NotificationStats:
        orders = await self.underdog.fetch_orders_for_orders_bot()
        limit_set: Optional[set[int]] = None
        if limit_user_ids is not None:
            limit_set = {int(uid) for uid in limit_user_ids}
        stats = NotificationStats(total_orders=len(orders) if limit_set is None else 0)
        if not orders:
            return stats

        handles = [_resolve_order_owner_handle(order)[0] for order in orders]
        valid_handles = [h for h in handles if h]
        user_map = await db.fetch_users_by_usernames(valid_handles, orders_recipients=True)

        for order, handle in zip(orders, handles):
            owner = order.get("owner") or {}
            if not handle:
                stats.missing_contact += 1
                logger.warning("Order lacks corporate Telegram handle", order_id=order.get("id"))
                continue
            user = user_map.get(handle)
            if not user:
                stats.unknown_user += 1
                stats.no_recipient_mapping += 1
                stats.unknown_orders.append(
                    {
                        "order_id": order.get("id"),
                        "owner": owner.get("name"),
                        "handle": handle,
                        "total": order.get("total"),
                        "name": order.get("name"),
                    }
                )
                logger.warning(
                    "Telegram handle not found among bot users (corporate @ must match username after /start)",
                    handle=handle,
                    order_id=order.get("id"),
                )
                continue
            db_user = await db.get_user(int(user["telegram_id"]))
            if db_user is not None and not bool(db_user.get("is_active")):
                stats.unknown_user += 1
                stats.no_recipient_mapping += 1
                stats.unknown_orders.append(
                    {
                        "order_id": order.get("id"),
                        "owner": owner.get("name"),
                        "handle": handle,
                        "total": order.get("total"),
                        "name": order.get("name"),
                    }
                )
                logger.warning(
                    "Order recipient is deactivated in tg_users, skip send",
                    telegram_id=user.get("telegram_id"),
                    order_id=order.get("id"),
                )
                continue
            try:
                user_telegram_id = int(user.get("telegram_id"))
            except Exception:
                user_telegram_id = None

            if limit_set is not None and (user_telegram_id not in limit_set):
                continue

            if limit_set is not None:
                stats.total_orders += 1

            stats.matched_users += 1
            message = _build_order_message(order)
            if dry_run:
                logger.info(
                    "Dry-run: would notify order owner",
                    order_id=order.get("id"),
                    telegram_id=user.get("telegram_id"),
                    username=user.get("username"),
                )
                stats.notified += 1
                continue

            try:
                oid = int(order.get("id"))
                chat_id = int(user["telegram_id"])
                if not await _drop_locally_sent("order", chat_id, [order], lambda o: o.get("id"), self.underdog.mark_order_telegram_sent):
                    continue
                msg = await self._send_and_log(chat_id, message, context="orders_bot_order_ready", extra={"order_id": oid, "handle": handle})
                if _telegram_underdog_send_confirmed(msg):
                    await db.mark_underdog_sent("order", [str(oid)], chat_id)
                    await self.underdog.mark_order_telegram_sent(oid)
                    await db.admin_notify_throttle_clear(f"orders:delivery_err:{oid}")
                    stats.notified += 1
                elif not _telegram_message_confirmed(msg):
                    stats.errors += 1
                    stats.delivery_failed += 1
                    logger.error(
                        "Order notify: Telegram ok!==true — Underdog telegram_sent не меняем",
                        order_id=oid,
                        chat_id=chat_id,
                        response=_telegram_api_response_dict(msg),
                    )
                    await self._notify_admins_delivery_error(
                        order=order,
                        error_text="Telegram: ответ без подтверждённого message_id (ok!==true), статус заказа не обновлён",
                    )
                else:
                    stats.errors += 1
                    stats.delivery_failed += 1
                    logger.error(
                        "Order notify: Telegram HTTP !== 200 — Underdog telegram_sent не меняем",
                        order_id=oid,
                        chat_id=chat_id,
                        http_status=getattr(msg, "__tg_http_status__", None),
                        response=_telegram_api_response_dict(msg),
                    )
                    await self._notify_admins_delivery_error(
                        order=order,
                        error_text="Telegram: HTTP статус не 200, статус заказа не обновлён",
                    )
            except (TelegramForbiddenError, TelegramBadRequest) as exc:
                stats.errors += 1
                stats.delivery_failed += 1
                logger.warning(
                    "Failed to deliver notification",
                    order_id=order.get("id"),
                    error=str(exc),
                )
                await self._notify_admins_delivery_error(order=order, error_text=str(exc))
            except Exception as exc:  # pragma: no cover
                stats.errors += 1
                stats.delivery_failed += 1
                logger.exception(
                    "Unexpected error while notifying order",
                    order_id=order.get("id"),
                    error=str(exc),
                )
                await self._notify_admins_delivery_error(order=order, error_text=str(exc))

        if stats.unknown_user > 0 and not dry_run:
            await self._alert_admins(stats.unknown_orders)

        return stats

    async def _alert_admins(self, unknown_orders: Iterable[Dict[str, Any]]) -> None:
        await self._alert_admins_digest(
            list(unknown_orders),
            title="⚠️ Не смог найти пользователей в боте для заказов:",
            render=lambda item: (
                f"ID {safe(item.get('order_id'))}: @{safe(item.get('handle'))} ({safe(item.get('owner'))}) "
                f"— {safe(item.get('name'))} на {safe(item.get('total'))}"
            ),
            noun="заказов",
            dedupe_key="orders:unknown_digest",
        )

    async def _notify_admins_delivery_error(self, *, order: Dict[str, Any], error_text: str) -> None:
        oid = order.get("id")
        owner = order.get("owner") or {}
        corp_handle, _ = _resolve_order_owner_handle(order)
        lines = [
            "⚠️ Ошибка отправки уведомления о заказе",
            f"ID: {order.get('id')}",
            f"Покупатель: {safe(owner.get('name') or '—')}",
            f"Корп. Telegram @: @{safe(corp_handle)}" if corp_handle else "Корп. Telegram: (не указан)",
            f"Сумма: {safe(order.get('total') or order.get('price') or '0')}",
            f"Ошибка: {safe(error_text)}",
        ]
        await self._alert_admins_throttled(
            "\n".join(lines),
            dedupe_key=f"orders:delivery_err:{oid}" if oid is not None else "orders:delivery_err:unknown",
            what=f"order {oid} delivery error",
        )
