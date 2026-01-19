"""Pending action text handlers."""

from aiogram.types import Message
from loguru import logger

from ..dispatcher import dp
from .. import db
from ..utils.domain import lookup_domains_text
from ..handlers.youtube import handle_youtube_download
from ..handlers.users import _resolve_user_id


@dp.message()
async def on_text_fallback(message: Message):
    # Ignore slash commands
    if message.text and message.text.startswith('/'):
        return
    pending = await db.get_pending_action(message.from_user.id)
    if not pending:
        return
    action, _ = pending
    try:
        if action == "fb:await_csv":
            text = (message.text or "").strip()
            if text.lower() in ("-", "стоп", "stop"):
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Загрузка CSV отменена")
            return await message.answer("Пришлите CSV файлом или '-' чтобы отменить ожидание")
        if action == "alias:new":
            alias = message.text.strip()
            await db.set_alias(alias)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Алиас создан. Откройте Алиасы в меню, чтобы назначить buyer/lead")
        if action == "domain:check":
            text = (message.text or "").strip()
            if text.lower() in ("-", "stop", "стоп"):
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Готово. Проверка доменов завершена")
            result = await lookup_domains_text(text)
            await message.answer(result + "\n\nОтправьте следующий домен или '-' чтобы завершить")
            return
        if action == "youtube:await_url":
            if await handle_youtube_download(message):
                return
        if action.startswith("alias:setbuyer:"):
            alias = action.split(":", 2)[2]
            v = message.text.strip()
            if v == '-':
                buyer_id = None
            else:
                try:
                    buyer_id = await _resolve_user_id(v)
                except ValueError:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer(
                        "Не удалось распознать пользователя. Пришлите numeric ID или @username. "
                        "Если пользователь не писал боту, попросите его отправить /start."
                    )
            await db.set_alias(alias, buyer_id=buyer_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Buyer назначен")
        if action.startswith("alias:setlead:"):
            alias = action.split(":", 2)[2]
            v = message.text.strip()
            if v == '-':
                lead_id = None
            else:
                try:
                    lead_id = await _resolve_user_id(v)
                except ValueError:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer(
                        "Не удалось распознать пользователя. Пришлите numeric ID или @username. "
                        "Если пользователь не писал боту, попросите его отправить /start."
                    )
            await db.set_alias(alias, lead_id=lead_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Lead назначен")
        if action == "mentor:add":
            v = message.text.strip()
            try:
                uid = await _resolve_user_id(v)
            except Exception:
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Не удалось распознать пользователя. Пришлите numeric ID или @username.")
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_role(uid, "mentor")
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Назначен ментором")
        if action == "team:new":
            name = message.text.strip()
            tid = await db.create_team(name)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer(f"Команда создана: id={tid}")
        if action.startswith("team:setlead:"):
            team_id = int(action.split(":", 2)[2])
            v = message.text.strip()
            uid = None
            if v.startswith("tg://user?id="):
                try:
                    uid = int(v.split("=", 1)[1])
                except Exception:
                    uid = None
            if uid is None and v.startswith("@"):
                uname = v[1:].strip().lower()
                users = await db.list_users()
                hit = next((u for u in users if (u.get("username") or "").lower() == uname), None)
                if hit:
                    uid = int(hit["telegram_id"])  # type: ignore
            if uid is None:
                try:
                    uid = int(v)
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer(
                        "Не удалось распознать пользователя. Пришлите numeric Telegram ID или @username. "
                        "Если пользователь не писал боту, попросите его отправить /start."
                    )
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_team(uid, team_id)
            user_row = await db.get_user(uid)
            role_before = (user_row or {}).get("role")
            if role_before not in ("mentor", "admin", "head"):
                await db.set_user_role(uid, "lead")
            await db.set_team_lead_override(team_id, uid)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Лид назначен")
        if action.startswith("myteam:add"):
            users = await db.list_users()
            team_id = None
            parts = action.split(":", 2)
            if len(parts) == 3 and parts[2]:
                try:
                    team_id = int(parts[2])
                except Exception:
                    team_id = None
            if team_id is None:
                team_id = await db.get_primary_lead_team(message.from_user.id)
            if team_id is None:
                await db.clear_pending_action(message.from_user.id)
                return await message.answer("Нет прав или команда не найдена")
            v = message.text.strip()
            uid = None
            if v.startswith("tg://user?id="):
                try:
                    uid = int(v.split("=", 1)[1])
                except Exception:
                    uid = None
            if uid is None and v.startswith("@"):
                uname = v[1:].strip().lower()
                hit = next((u for u in users if (u.get("username") or "").lower() == uname), None)
                if hit:
                    uid = int(hit["telegram_id"])  # type: ignore
            if uid is None:
                try:
                    uid = int(v)
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer(
                        "Не удалось распознать пользователя. Пришлите numeric ID или @username. "
                        "Если пользователь не писал боту, попросите его отправить /start."
                    )
            try:
                await db.upsert_user(uid, None, None)
            except Exception:
                pass
            await db.set_user_team(uid, team_id)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("Пользователь добавлен в вашу команду")
        if action.startswith("kpi:set:"):
            which = action.split(":", 2)[2]
            v = message.text.strip()
            goal_val = None
            if v != '-':
                try:
                    goal_val = int(v)
                    if goal_val < 0:
                        goal_val = 0
                except Exception:
                    await db.clear_pending_action(message.from_user.id)
                    return await message.answer("Нужно целое число или '-' для очистки")
            current = await db.get_kpi(message.from_user.id)
            daily = current.get('daily_goal')
            weekly = current.get('weekly_goal')
            if which == 'daily':
                daily = goal_val
            else:
                weekly = goal_val
            await db.set_kpi(message.from_user.id, daily_goal=daily, weekly_goal=weekly)
            await db.clear_pending_action(message.from_user.id)
            return await message.answer("KPI обновлен")
    except Exception as exc:
        logger.exception(exc)
        return await message.answer("Ошибка обработки ввода")
