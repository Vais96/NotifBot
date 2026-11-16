"""Handlers for building and shortening FB comment links (/link, menu:commentlink)."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from loguru import logger

from .. import db
from ..constants import PendingAction
from ..dispatcher import bot, dp
from ..services.comment_links import (
    GUIDED_FIELDS,
    GUIDED_PROMPTS,
    InvalidBaseLinkError,
    InvalidSlugError,
    LinkShortenError,
    ShortLinkTakenError,
    build_comment_link,
    create_short_link_for_url,
    extract_first_line_url_and_rest,
    format_result_message,
    parse_base_link,
    parse_fields_input,
    short_link_url,
)
from ..utils.html import safe

_CACHE_DRAFT_KIND = "clink_draft"
_CACHE_LAST_KIND = "clink_last"


def _cancel_kb(last_base_url: Optional[str] = None) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if last_base_url:
        try:
            host = parse_base_link(last_base_url).host
        except InvalidBaseLinkError:
            host = ""
        label = f"🔁 Взять прошлую базу ({host})" if host else "🔁 Взять прошлую базу"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data="clink:uselast")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="clink:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _params_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Заполнить по шагам", callback_data="clink:guided")],
            [InlineKeyboardButton(text="⚡ Собрать как есть", callback_data="clink:build")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="clink:cancel")],
        ]
    )


def _step_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⏭ Пропустить", callback_data="clink:skip"),
                InlineKeyboardButton(text="✅ Собрать сейчас", callback_data="clink:build"),
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="clink:cancel")],
        ]
    )


def _slug_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Случайное название", callback_data="clink:autoslug")],
            [InlineKeyboardButton(text="⏭ Оставить длинную (без укорочения)", callback_data="clink:noshort")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="clink:cancel")],
        ]
    )


def _result_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Сделать ещё ссылку", callback_data="menu:commentlink")],
            [InlineKeyboardButton(text="🏠 Меню", callback_data="clink:menu")],
        ]
    )


async def _save_draft(user_id: int, draft: Dict[str, Any]) -> None:
    await db.set_ui_cache_list(user_id, _CACHE_DRAFT_KIND, [json.dumps(draft, ensure_ascii=False)])


async def _load_draft(user_id: int) -> Optional[Dict[str, Any]]:
    raw = await db.get_ui_cache_value(user_id, _CACHE_DRAFT_KIND, 0)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


async def _clear_draft(user_id: int) -> None:
    await db.set_ui_cache_list(user_id, _CACHE_DRAFT_KIND, [])


async def _save_last_base(user_id: int, base_url: str) -> None:
    await db.set_ui_cache_list(user_id, _CACHE_LAST_KIND, [base_url])


async def _load_last_base(user_id: int) -> Optional[str]:
    return await db.get_ui_cache_value(user_id, _CACHE_LAST_KIND, 0)


def _next_guided_field(current: Optional[str], has_pixel: bool) -> Optional[str]:
    fields = [f for f in GUIDED_FIELDS if not (f == "pixel" and has_pixel)]
    if not fields:
        return None
    if current is None:
        return fields[0]
    try:
        idx = fields.index(current)
    except ValueError:
        return None
    return fields[idx + 1] if idx + 1 < len(fields) else None


def _format_params_prompt(base_url: str) -> str:
    parsed = parse_base_link(base_url)
    lines: List[str] = [f"✅ База принята: <code>{safe(parsed.host)}</code>"]
    if parsed.detected_static:
        found = ", ".join(f"<code>{safe(k)}={safe(v)}</code>" for k, v in parsed.detected_static.items())
        lines.append(f"Уже в ссылке: {found}")
    lines.extend([
        "",
        "Отправьте данные для подстановки — <b>в одном сообщении</b> или нажмите «По шагам»:",
        "<code>Кампания: название_кампании</code>",
        "<code>Адсет: название_адсета</code>",
        "<code>Объявление: название_объявления</code>",
        "<code>Каб: id_каба</code>",
    ])
    if not parsed.has_pixel:
        lines.append("<code>Пиксель: id_пикселя</code>")
    lines.extend([
        "",
        "Можно также прислать просто 4 строки подряд (кампания, адсет, объявление, ID каба) "
        "или сразу одно название кампании, чтобы продолжить по шагам.",
    ])
    return "\n".join(lines)


async def _send_guided_step(chat_id: int, user_id: int, field_name: str, draft: Dict[str, Any]) -> None:
    title, example = GUIDED_PROMPTS[field_name]
    has_pixel = bool(draft.get("has_pixel"))
    fields = [f for f in GUIDED_FIELDS if not (f == "pixel" and has_pixel)]
    step_num = fields.index(field_name) + 1
    total_steps = len(fields)
    await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:step:{field_name}", None)
    await bot.send_message(
        chat_id,
        f"Шаг {step_num}/{total_steps}. Введите <b>{safe(title)}</b>:\n{example}",
        reply_markup=_step_kb(),
    )


async def _finish_with_result(
    chat_id: int,
    user_id: int,
    draft: Dict[str, Any],
    *,
    short_url: Optional[str] = None,
    shorten_error: Optional[str] = None,
) -> None:
    base_url = str(draft.get("base_url") or "")
    fields = draft.get("fields") or {}
    built = build_comment_link(base_url, fields)

    await db.clear_pending_action(user_id)
    try:
        await _clear_draft(user_id)
        if base_url:
            await _save_last_base(user_id, base_url)
    except Exception as exc:
        logger.warning("Failed to update comment link cache for {}: {}", user_id, exc)

    text = format_result_message(built, short_url, shorten_error)
    await bot.send_message(chat_id, text, reply_markup=_result_kb(), disable_web_page_preview=True)


async def _proceed_after_fields(chat_id: int, user_id: int, draft: Dict[str, Any]) -> None:
    """Build full link and either finalize with custom_slug or prompt user for short-link slug."""
    base_url = str(draft.get("base_url") or "")
    fields = dict(draft.get("fields") or {})
    built = build_comment_link(base_url, fields)

    custom_slug = str(fields.get("custom_slug") or "").strip()
    if custom_slug:
        try:
            short_url = await create_short_link_for_url(built.long_url, user_id, custom_slug=custom_slug)
        except InvalidSlugError as exc:
            await _save_draft(user_id, draft)
            await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:slug", None)
            await bot.send_message(
                chat_id,
                f"✅ <b>Полная ссылка собрана:</b>\n<code>{safe(built.long_url)}</code>\n\n"
                f"⚠️ {safe(str(exc))}\nВведите другое название для короткой ссылки или выберите действие ниже:",
                reply_markup=_slug_kb(),
                disable_web_page_preview=True,
            )
            return
        except ShortLinkTakenError as exc:
            await _save_draft(user_id, draft)
            await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:slug", None)
            await bot.send_message(
                chat_id,
                f"✅ <b>Полная ссылка собрана:</b>\n<code>{safe(built.long_url)}</code>\n\n"
                f"⚠️ Название <code>{safe(str(exc))}</code> уже занято. Введите другое или нажмите «Случайное название»:",
                reply_markup=_slug_kb(),
                disable_web_page_preview=True,
            )
            return
        except LinkShortenError as exc:
            await _finish_with_result(chat_id, user_id, draft, shorten_error=str(exc))
            return

        await _finish_with_result(chat_id, user_id, draft, short_url=short_url)
        return

    await _save_draft(user_id, draft)
    await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:slug", None)
    example_url = short_link_url("thai-slot-1")
    pixel_note = (
        ""
        if built.has_pixel
        else "\n\n⚠️ <i>Пиксель (fbp) не указан — события могут не отбиваться в Ads Manager.</i>"
    )
    await bot.send_message(
        chat_id,
        f"✅ <b>Полная ссылка собрана (уже можно использовать):</b>\n"
        f"<code>{safe(built.long_url)}</code>{pixel_note}\n\n"
        f"Теперь введите <b>своё название</b> для короткой ссылки "
        f"(латиница, цифры, <code>-</code>, <code>_</code>):\n"
        f"Пример: <code>thai-slot-1</code> → <code>{safe(example_url)}</code>",
        reply_markup=_slug_kb(),
        disable_web_page_preview=True,
    )


async def start_comment_link_flow(
    chat_id: int,
    user_id: int,
    username: Optional[str] = None,
    full_name: Optional[str] = None,
) -> None:
    """Start link builder dialog for a user."""
    try:
        await db.upsert_user(user_id, username, full_name)
    except Exception as exc:
        logger.warning("Failed to upsert user {} before comment link flow: {}", user_id, exc)

    last_base = None
    try:
        await _clear_draft(user_id)
        last_base = await _load_last_base(user_id)
    except Exception as exc:
        logger.warning("Failed to load last base link for {}: {}", user_id, exc)

    await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:base", None)
    await bot.send_message(
        chat_id,
        "🔗 <b>Сборка ссылки для комментария</b>\n\n"
        "Пришлите базовую ссылку из Линки групп / Binom.\n"
        "Например:\n"
        "<code>https://luckytop.online/?sub1=77&amp;sub2={{campaign.name}}&amp;sub3={{ad.name}}&amp;sub4=TH&amp;fbp=1491853589548065</code>\n\n"
        "<i>Можно прислать сразу ссылку и параметры с новой строки в одном сообщении. "
        "Для отмены отправьте <code>-</code>.</i>",
        reply_markup=_cancel_kb(last_base),
        disable_web_page_preview=True,
    )


@dp.message(Command("link", "commentlink"))
async def on_link_command(message: Message) -> None:
    """Handle /link command."""
    await start_comment_link_flow(
        message.chat.id,
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name,
    )


async def handle_comment_link_text(message: Message, arg: str) -> None:
    """Handle pending text replies for PendingAction.COMMENT_LINK."""
    user_id = message.from_user.id
    chat_id = message.chat.id
    raw_text = (message.text or "").strip()

    if arg == "base" or not arg:
        first_line, rest = extract_first_line_url_and_rest(raw_text)
        try:
            parsed = parse_base_link(first_line)
        except InvalidBaseLinkError as exc:
            await message.answer(f"{safe(str(exc))}\n\nПопробуйте ещё раз или отправьте <code>-</code> для отмены.")
            return

        draft: Dict[str, Any] = {
            "base_url": parsed.normalized_url,
            "has_pixel": parsed.has_pixel,
            "fields": {},
        }
        if rest:
            fields, is_structured = parse_fields_input(rest)
            if is_structured:
                draft["fields"] = fields
                await _proceed_after_fields(chat_id, user_id, draft)
                return
            draft["fields"]["campaign_name"] = rest
            await _save_draft(user_id, draft)
            next_field = _next_guided_field("campaign_name", parsed.has_pixel)
            if next_field:
                await _send_guided_step(chat_id, user_id, next_field, draft)
                return
            await _proceed_after_fields(chat_id, user_id, draft)
            return

        await _save_draft(user_id, draft)
        await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:params", None)
        await message.answer(
            _format_params_prompt(parsed.normalized_url),
            reply_markup=_params_kb(),
            disable_web_page_preview=True,
        )
        return

    draft = await _load_draft(user_id)
    if not draft or not draft.get("base_url"):
        await db.clear_pending_action(user_id)
        await message.answer("Черновик ссылки не найден или устарел. Запустите заново через /link.")
        return

    if arg == "slug":
        built = build_comment_link(str(draft.get("base_url") or ""), draft.get("fields") or {})
        try:
            short_url = await create_short_link_for_url(built.long_url, user_id, custom_slug=raw_text)
        except InvalidSlugError as exc:
            await message.answer(f"{safe(str(exc))}\nПопробуйте другое название или нажмите кнопку ниже.", reply_markup=_slug_kb())
            return
        except ShortLinkTakenError as exc:
            await message.answer(
                f"Название <code>{safe(str(exc))}</code> уже занято. Введите другое или нажмите «Случайное название».",
                reply_markup=_slug_kb(),
            )
            return
        except LinkShortenError as exc:
            await _finish_with_result(chat_id, user_id, draft, shorten_error=str(exc))
            return

        await _finish_with_result(chat_id, user_id, draft, short_url=short_url)
        return

    has_pixel = bool(draft.get("has_pixel"))
    fields, is_structured = parse_fields_input(raw_text)

    if arg == "params":
        if is_structured:
            draft_fields = dict(draft.get("fields") or {})
            draft_fields.update(fields)
            draft["fields"] = draft_fields
            await _proceed_after_fields(chat_id, user_id, draft)
            return
        draft_fields = dict(draft.get("fields") or {})
        draft_fields["campaign_name"] = raw_text
        draft["fields"] = draft_fields
        await _save_draft(user_id, draft)
        next_field = _next_guided_field("campaign_name", has_pixel)
        if next_field:
            await _send_guided_step(chat_id, user_id, next_field, draft)
            return
        await _proceed_after_fields(chat_id, user_id, draft)
        return

    if arg.startswith("step:"):
        field_name = arg.split(":", 1)[1]
        if field_name not in GUIDED_PROMPTS:
            await db.clear_pending_action(user_id)
            await message.answer("Неизвестный шаг. Начните заново через /link.")
            return
        if is_structured:
            draft_fields = dict(draft.get("fields") or {})
            draft_fields.update(fields)
            draft["fields"] = draft_fields
            await _proceed_after_fields(chat_id, user_id, draft)
            return

        draft_fields = dict(draft.get("fields") or {})
        draft_fields[field_name] = raw_text
        draft["fields"] = draft_fields
        await _save_draft(user_id, draft)
        next_field = _next_guided_field(field_name, has_pixel)
        if next_field:
            await _send_guided_step(chat_id, user_id, next_field, draft)
            return
        await _proceed_after_fields(chat_id, user_id, draft)
        return

    await db.clear_pending_action(user_id)
    await message.answer("Состояние сборки сброшено. Запустите /link заново.")


@dp.callback_query(F.data == "clink:cancel")
async def cb_clink_cancel(call: CallbackQuery) -> None:
    await db.clear_pending_action(call.from_user.id)
    try:
        await _clear_draft(call.from_user.id)
    except Exception:
        pass
    await call.answer("Отменено")
    try:
        await call.message.edit_text("Создание ссылки отменено.")
    except Exception:
        await call.message.answer("Создание ссылки отменено.")


@dp.callback_query(F.data == "clink:uselast")
async def cb_clink_uselast(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    last_base = await _load_last_base(user_id)
    if not last_base:
        await call.answer("Предыдущая база не найдена", show_alert=True)
        return
    try:
        parsed = parse_base_link(last_base)
    except InvalidBaseLinkError:
        await call.answer("Предыдущая база некорректна", show_alert=True)
        return

    draft: Dict[str, Any] = {
        "base_url": parsed.normalized_url,
        "has_pixel": parsed.has_pixel,
        "fields": {},
    }
    await _save_draft(user_id, draft)
    await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:params", None)
    await call.answer()
    await call.message.answer(
        _format_params_prompt(parsed.normalized_url),
        reply_markup=_params_kb(),
        disable_web_page_preview=True,
    )


@dp.callback_query(F.data == "clink:guided")
async def cb_clink_guided(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    draft = await _load_draft(user_id)
    if not draft or not draft.get("base_url"):
        await db.clear_pending_action(user_id)
        await call.answer("Черновик устарел. Нажмите /link", show_alert=True)
        return
    first_field = _next_guided_field(None, bool(draft.get("has_pixel")))
    if not first_field:
        await _proceed_after_fields(call.message.chat.id, user_id, draft)
        await call.answer()
        return
    await call.answer()
    await _send_guided_step(call.message.chat.id, user_id, first_field, draft)


@dp.callback_query(F.data == "clink:skip")
async def cb_clink_skip(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    stored = await db.get_pending_action(user_id)
    draft = await _load_draft(user_id)
    if not stored or not stored[0].startswith(f"{PendingAction.COMMENT_LINK}:step:") or not draft:
        await call.answer("Шаг уже не актуален", show_alert=True)
        return
    current_field = stored[0].split(":step:", 1)[1]
    next_field = _next_guided_field(current_field, bool(draft.get("has_pixel")))
    await call.answer("Пропущено")
    if next_field:
        await _send_guided_step(call.message.chat.id, user_id, next_field, draft)
        return
    await _proceed_after_fields(call.message.chat.id, user_id, draft)


@dp.callback_query(F.data == "clink:build")
async def cb_clink_build(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    draft = await _load_draft(user_id)
    if not draft or not draft.get("base_url"):
        await db.clear_pending_action(user_id)
        await call.answer("Черновик устарел. Нажмите /link", show_alert=True)
        return
    await call.answer()
    await _proceed_after_fields(call.message.chat.id, user_id, draft)


@dp.callback_query(F.data == "clink:autoslug")
async def cb_clink_autoslug(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    draft = await _load_draft(user_id)
    if not draft or not draft.get("base_url"):
        await db.clear_pending_action(user_id)
        await call.answer("Черновик устарел. Нажмите /link", show_alert=True)
        return
    built = build_comment_link(str(draft.get("base_url") or ""), draft.get("fields") or {})
    await call.answer()
    try:
        short_url = await create_short_link_for_url(built.long_url, user_id, custom_slug=None)
    except LinkShortenError as exc:
        await _finish_with_result(call.message.chat.id, user_id, draft, shorten_error=str(exc))
        return
    await _finish_with_result(call.message.chat.id, user_id, draft, short_url=short_url)


@dp.callback_query(F.data == "clink:noshort")
async def cb_clink_noshort(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    draft = await _load_draft(user_id)
    if not draft or not draft.get("base_url"):
        await db.clear_pending_action(user_id)
        await call.answer("Черновик устарел. Нажмите /link", show_alert=True)
        return
    await call.answer()
    await _finish_with_result(call.message.chat.id, user_id, draft)


@dp.callback_query(F.data == "clink:menu")
async def cb_clink_menu(call: CallbackQuery) -> None:
    from .menu import send_user_menu

    await call.answer()
    await send_user_menu(call.message.chat.id, call.from_user.id)
