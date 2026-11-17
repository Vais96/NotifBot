"""Handlers for building and shortening FB comment links (/link, /domains, menu:commentlink)."""

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
from ..services.cloudflare_short_domains import (
    CloudflareProvisionError,
    CloudflareZoneNotFoundError,
    is_cloudflare_configured,
    provision_domain_in_cloudflare,
)
from ..services.comment_links import (
    GUIDED_FIELDS,
    GUIDED_PROMPTS,
    InvalidBaseLinkError,
    InvalidShortDomainError,
    InvalidSlugError,
    LinkShortenError,
    ShortLinkTakenError,
    bot_cname_target,
    build_comment_link,
    create_short_link_for_url,
    default_short_domain,
    extract_campaign_metadata,
    extract_first_line_url_and_rest,
    format_result_message,
    list_available_domains,
    normalize_short_domain,
    parse_base_link,
    parse_fields_input,
    short_link_url,
)
from ..utils.html import safe
from .common import is_admin

_CACHE_DRAFT_KIND = "clink_draft"
_CACHE_LAST_KIND = "clink_last"
_CACHE_LAST_DOMAIN_KIND = "clink_last_dom"
_CACHE_PICK_DOMAINS_KIND = "clink_pickdom"
_CACHE_MANAGE_DOMAINS_KIND = "clink_domlist"


def _cancel_kb(last_base_url: Optional[str] = None) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if last_base_url:
        try:
            host = parse_base_link(last_base_url).host
        except InvalidBaseLinkError:
            host = ""
        label = f"🔁 Взять прошлую базу ({host})" if host else "🔁 Взять прошлую базу"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data="clink:uselast")])
    rows.append([InlineKeyboardButton(text="🌐 Мои домены", callback_data="clink:domains")])
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
            [InlineKeyboardButton(text="🌐 Выбрать / добавить домен", callback_data="clink:pickdomain")],
            [InlineKeyboardButton(text="⏭ Оставить длинную (без укорочения)", callback_data="clink:noshort")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="clink:cancel")],
        ]
    )


def _result_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Сделать ещё ссылку", callback_data="menu:commentlink")],
            [InlineKeyboardButton(text="🌐 Мои домены", callback_data="clink:domains")],
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


async def _save_last_domain(user_id: int, domain: str) -> None:
    if domain:
        await db.set_ui_cache_list(user_id, _CACHE_LAST_DOMAIN_KIND, [domain])


async def _load_last_domain(user_id: int) -> Optional[str]:
    return await db.get_ui_cache_value(user_id, _CACHE_LAST_DOMAIN_KIND, 0)


async def _resolve_user_short_domain(
    user_id: int,
    *,
    draft_domain: Optional[str] = None,
    requested_domain: Optional[str] = None,
) -> str:
    admin = await is_admin(user_id)
    available = await list_available_domains(user_id, include_all=admin)
    avail_names = [str(r["domain"]).lower() for r in available if r.get("domain")]

    if requested_domain and requested_domain.strip():
        norm = normalize_short_domain(requested_domain)
        if norm in avail_names or norm == default_short_domain():
            return norm
        raise InvalidShortDomainError(
            f"Домен <code>{safe(norm)}</code> не добавлен в ваш список. Добавьте его через /domains."
        )

    if draft_domain and draft_domain.lower() in avail_names:
        return draft_domain.lower()

    last_dom = (await _load_last_domain(user_id) or "").strip().lower()
    if last_dom and last_dom in avail_names:
        return last_dom

    for row in available:
        if not row.get("is_builtin") and row.get("domain"):
            return str(row["domain"]).lower()

    return avail_names[0] if avail_names else default_short_domain()


def _active_guided_fields(draft: Dict[str, Any]) -> List[str]:
    has_pixel = bool(draft.get("has_pixel"))
    fields_map = draft.get("fields") or {}
    result: List[str] = []
    for field_name in GUIDED_FIELDS:
        if field_name == "pixel" and has_pixel:
            continue
        if field_name == "account_id" and str(fields_map.get("account_id") or "").strip():
            continue
        result.append(field_name)
    return result


def _next_guided_field(current: Optional[str], draft: Dict[str, Any]) -> Optional[str]:
    has_pixel = bool(draft.get("has_pixel"))
    base_fields = [f for f in GUIDED_FIELDS if not (f == "pixel" and has_pixel)]
    if not base_fields:
        return None
    if current is None:
        active = _active_guided_fields(draft)
        return active[0] if active else None
    try:
        start_idx = base_fields.index(current) + 1
    except ValueError:
        return None
    fields_map = draft.get("fields") or {}
    for candidate in base_fields[start_idx:]:
        if candidate == "account_id" and str(fields_map.get("account_id") or "").strip():
            continue
        return candidate
    return None


def _apply_campaign_autofill(draft_fields: Dict[str, str], campaign_name: str) -> Optional[str]:
    """Merge metadata extracted from campaign_name without overwriting explicit entries."""
    extracted = extract_campaign_metadata(campaign_name)
    for key, val in extracted.items():
        draft_fields.setdefault(key, val)
    parts: List[str] = []
    if extracted.get("account_id"):
        parts.append(f"ID каба <code>{safe(extracted['account_id'])}</code>")
    if extracted.get("sub1"):
        parts.append(f"байер <code>{safe(extracted['sub1'])}</code>")
    if extracted.get("geo"):
        parts.append(f"гео <code>{safe(extracted['geo'])}</code>")
    if not parts:
        return None
    return "✨ Из названия кампании определено: " + ", ".join(parts)


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
        "<code>Каб: id_каба</code> <i>(подтянется сам из 77_TH_1740624179668966_15.11)</i>",
    ])
    if not parsed.has_pixel:
        lines.append("<code>Пиксель: id_пикселя</code>")
    lines.extend([
        "",
        "Можно также прислать просто строки подряд или одно название кампании, чтобы продолжить по шагам.",
    ])
    return "\n".join(lines)


async def _send_guided_step(
    chat_id: int,
    user_id: int,
    field_name: str,
    draft: Dict[str, Any],
    *,
    note: Optional[str] = None,
) -> None:
    title, example = GUIDED_PROMPTS[field_name]
    fields = _active_guided_fields(draft)
    step_num = fields.index(field_name) + 1 if field_name in fields else 1
    total_steps = max(step_num, len(fields))
    await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:step:{field_name}", None)
    prefix = f"{note}\n\n" if note else ""
    await bot.send_message(
        chat_id,
        f"{prefix}Шаг {step_num}/{total_steps}. Введите <b>{safe(title)}</b>:\n{example}",
        reply_markup=_step_kb(),
    )


async def _send_slug_prompt(
    chat_id: int,
    user_id: int,
    draft: Dict[str, Any],
    *,
    error_note: Optional[str] = None,
) -> None:
    base_url = str(draft.get("base_url") or "")
    fields = dict(draft.get("fields") or {})
    built = build_comment_link(base_url, fields)
    domain = await _resolve_user_short_domain(user_id, draft_domain=draft.get("domain"))
    draft["domain"] = domain
    await _save_draft(user_id, draft)
    await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:slug", None)

    example_url = short_link_url("games", domain or None)
    pixel_note = (
        ""
        if built.has_pixel
        else "\n\n⚠️ <i>Пиксель (fbp) не указан — события могут не отбиваться в Ads Manager.</i>"
    )
    err_block = f"\n\n⚠️ {error_note}" if error_note else ""
    domain_line = f"🌐 Домен: <code>{safe(domain)}</code>\n" if domain else ""
    await bot.send_message(
        chat_id,
        f"✅ <b>Полная ссылка собрана (уже можно использовать):</b>\n"
        f"<code>{safe(built.long_url)}</code>{pixel_note}{err_block}\n\n"
        f"{domain_line}"
        f"Введите <b>своё название</b> для короткой ссылки "
        f"(латиница, цифры, <code>-</code>, <code>_</code>):\n"
        f"Пример: <code>games</code> → <code>{safe(example_url)}</code>",
        reply_markup=_slug_kb(),
        disable_web_page_preview=True,
    )


async def _finish_with_result(
    chat_id: int,
    user_id: int,
    draft: Dict[str, Any],
    *,
    short_url: Optional[str] = None,
    shorten_error: Optional[str] = None,
    used_domain: Optional[str] = None,
) -> None:
    base_url = str(draft.get("base_url") or "")
    fields = draft.get("fields") or {}
    built = build_comment_link(base_url, fields)

    await db.clear_pending_action(user_id)
    try:
        await _clear_draft(user_id)
        if base_url:
            await _save_last_base(user_id, base_url)
        if used_domain:
            await _save_last_domain(user_id, used_domain)
    except Exception as exc:
        logger.warning("Failed to update comment link cache for {}: {}", user_id, exc)

    text = format_result_message(built, short_url, shorten_error)
    await bot.send_message(chat_id, text, reply_markup=_result_kb(), disable_web_page_preview=True)


async def _proceed_after_fields(chat_id: int, user_id: int, draft: Dict[str, Any]) -> None:
    """Build full link and either finalize with custom_slug or prompt user for short-link slug."""
    base_url = str(draft.get("base_url") or "")
    fields = dict(draft.get("fields") or {})
    built = build_comment_link(base_url, fields)

    requested_domain = str(fields.get("short_domain") or "").strip() or None
    try:
        domain = await _resolve_user_short_domain(
            user_id,
            draft_domain=draft.get("domain"),
            requested_domain=requested_domain,
        )
    except InvalidShortDomainError as exc:
        await _send_slug_prompt(chat_id, user_id, draft, error_note=str(exc))
        return

    draft["domain"] = domain
    custom_slug = str(fields.get("custom_slug") or "").strip()
    if custom_slug:
        try:
            short_url = await create_short_link_for_url(
                built.long_url,
                user_id,
                custom_slug=custom_slug,
                domain=domain,
            )
        except InvalidSlugError as exc:
            await _send_slug_prompt(chat_id, user_id, draft, error_note=safe(str(exc)))
            return
        except ShortLinkTakenError as exc:
            await _send_slug_prompt(
                chat_id,
                user_id,
                draft,
                error_note=f"Название <code>{safe(str(exc))}</code> уже занято на домене <code>{safe(domain)}</code>.",
            )
            return
        except LinkShortenError as exc:
            await _finish_with_result(chat_id, user_id, draft, shorten_error=str(exc))
            return

        await _finish_with_result(chat_id, user_id, draft, short_url=short_url, used_domain=domain)
        return

    await _send_slug_prompt(chat_id, user_id, draft)


async def _try_provision_cloudflare(domain: str) -> str:
    """Provision Cloudflare DNS + Worker route when CLOUDFLARE_* is configured; returns status note."""
    if not is_cloudflare_configured():
        cname = bot_cname_target()
        if cname:
            return (
                f"\nЗаведите домен в Cloudflare, добавьте проксируемый CNAME → <code>{safe(cname)}</code> "
                f"и подключите маршрут Worker <code>{safe(domain)}/*</code>."
            )
        return ""
    try:
        res = await provision_domain_in_cloudflare(domain)
    except CloudflareZoneNotFoundError:
        return (
            f"\n⚠️ Домен <code>{safe(domain)}</code> пока не найден в вашем Cloudflare. "
            "Добавьте сайт в Cloudflare, смените NS у регистратора и нажмите ☁️ в /domains."
        )
    except CloudflareProvisionError as exc:
        return f"\n⚠️ Не удалось настроить Cloudflare автоматически: {safe(str(exc))}"

    parts: List[str] = []
    if res.dns_created:
        parts.append("DNS создан")
    if res.routes_created:
        parts.append("Worker route подключён")
    if not parts:
        parts.append("DNS и Worker route уже настроены")
    status_hint = " (зона ждёт смены NS)" if res.zone_status != "active" else ""
    return f"\n☁️ Cloudflare: {', '.join(parts)}{safe(status_hint)}."


async def send_domains_menu(chat_id: int, user_id: int) -> None:
    """Show user's and shared short-link domains with DNS setup instructions."""
    admin = await is_admin(user_id)
    rows = await list_available_domains(user_id, include_all=admin)
    db_domains = [r for r in rows if not r.get("is_builtin")]
    await db.set_ui_cache_list(user_id, _CACHE_MANAGE_DOMAINS_KIND, [str(r["domain"]) for r in db_domains])

    lines: List[str] = ["🌐 <b>Домены для коротких ссылок</b>"]
    cname = bot_cname_target()
    if cname:
        lines.append(f"CNAME для подключения своего домена: <code>{safe(cname)}</code>")
    lines.append("")

    if not rows:
        lines.append("Доменов пока нет.")
    else:
        for r in rows:
            dom = str(r.get("domain") or "")
            if r.get("is_builtin"):
                badge = "базовый"
            elif r.get("is_shared"):
                badge = "общий"
            elif r.get("owner_id") and int(r["owner_id"]) == int(user_id):
                badge = "мой"
            else:
                badge = f"id:{r.get('owner_id')}"
            lines.append(f"• <code>{safe(dom)}</code> — <i>{safe(badge)}</i>")

    buttons: List[List[InlineKeyboardButton]] = []
    cf_enabled = is_cloudflare_configured()
    for idx, r in enumerate(db_domains):
        dom = str(r["domain"])
        is_owner = r.get("owner_id") is not None and int(r["owner_id"]) == int(user_id)
        row_btns: List[InlineKeyboardButton] = []
        if admin:
            share_label = f"👤 Скрыть {dom}" if r.get("is_shared") else f"🌐 Общий: {dom}"
            row_btns.append(InlineKeyboardButton(text=share_label[:36], callback_data=f"clink:domshare:{idx}"))
        if cf_enabled and (admin or is_owner):
            row_btns.append(InlineKeyboardButton(text=f"☁️ CF: {dom}"[:24], callback_data=f"clink:domcf:{idx}"))
        if admin or is_owner:
            row_btns.append(InlineKeyboardButton(text=f"🗑 {dom}"[:24], callback_data=f"clink:domdel:{idx}"))
        if row_btns:
            buttons.append(row_btns)

    buttons.append([InlineKeyboardButton(text="➕ Добавить домен", callback_data="clink:adddomain")])
    buttons.append([InlineKeyboardButton(text="🔗 Собрать ссылку", callback_data="menu:commentlink")])
    buttons.append([InlineKeyboardButton(text="🏠 Меню", callback_data="clink:menu")])

    await bot.send_message(
        chat_id,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
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


@dp.message(Command("domains"))
async def on_domains_command(message: Message) -> None:
    """Handle /domains command."""
    try:
        await db.upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    except Exception:
        pass
    await send_domains_menu(message.chat.id, message.from_user.id)


async def handle_comment_link_text(message: Message, arg: str) -> None:
    """Handle pending text replies for PendingAction.COMMENT_LINK."""
    user_id = message.from_user.id
    chat_id = message.chat.id
    raw_text = (message.text or "").strip()

    if arg in ("add_domain", "add_domain_flow"):
        try:
            domain = normalize_short_domain(raw_text)
        except InvalidShortDomainError as exc:
            await message.answer(f"{str(exc)}\nПопробуйте ещё раз или отправьте <code>-</code> для отмены.")
            return
        admin = await is_admin(user_id)
        status = await db.add_short_domain(domain, user_id, is_shared=admin, allow_takeover=admin)
        if status == "taken":
            await message.answer(
                f"Домен <code>{safe(domain)}</code> уже привязан к другому пользователю. Пришлите другой домен или <code>-</code>."
            )
            return
        try:
            await _save_last_domain(user_id, domain)
        except Exception:
            pass
        cf_note = await _try_provision_cloudflare(domain)
        scope_note = " (общий для команды; в /domains можно скрыть)" if admin else ""

        if arg == "add_domain_flow":
            draft = await _load_draft(user_id)
            if draft and draft.get("base_url"):
                draft["domain"] = domain
                await _save_draft(user_id, draft)
                await message.answer(f"✅ Домен <code>{safe(domain)}</code> добавлен{scope_note}.{cf_note}")
                await _send_slug_prompt(chat_id, user_id, draft)
                return

        await db.clear_pending_action(user_id)
        await message.answer(f"✅ Домен <code>{safe(domain)}</code> добавлен{scope_note}.{cf_note}")
        await send_domains_menu(chat_id, user_id)
        return

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
            note = _apply_campaign_autofill(draft["fields"], rest)
            await _save_draft(user_id, draft)
            next_field = _next_guided_field("campaign_name", draft)
            if next_field:
                await _send_guided_step(chat_id, user_id, next_field, draft, note=note)
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
        domain = await _resolve_user_short_domain(user_id, draft_domain=draft.get("domain"))
        try:
            short_url = await create_short_link_for_url(
                built.long_url,
                user_id,
                custom_slug=raw_text,
                domain=domain,
            )
        except InvalidSlugError as exc:
            await message.answer(
                f"{safe(str(exc))}\nПопробуйте другое название или нажмите кнопку ниже.",
                reply_markup=_slug_kb(),
            )
            return
        except ShortLinkTakenError as exc:
            await message.answer(
                f"Название <code>{safe(str(exc))}</code> уже занято на домене <code>{safe(domain)}</code>. "
                "Введите другое или нажмите «Случайное название».",
                reply_markup=_slug_kb(),
            )
            return
        except LinkShortenError as exc:
            await _finish_with_result(chat_id, user_id, draft, shorten_error=str(exc))
            return

        await _finish_with_result(chat_id, user_id, draft, short_url=short_url, used_domain=domain)
        return

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
        note = _apply_campaign_autofill(draft_fields, raw_text)
        draft["fields"] = draft_fields
        await _save_draft(user_id, draft)
        next_field = _next_guided_field("campaign_name", draft)
        if next_field:
            await _send_guided_step(chat_id, user_id, next_field, draft, note=note)
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
        note = _apply_campaign_autofill(draft_fields, raw_text) if field_name == "campaign_name" else None
        draft["fields"] = draft_fields
        await _save_draft(user_id, draft)
        next_field = _next_guided_field(field_name, draft)
        if next_field:
            await _send_guided_step(chat_id, user_id, next_field, draft, note=note)
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
    first_field = _next_guided_field(None, draft)
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
    next_field = _next_guided_field(current_field, draft)
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
    domain = await _resolve_user_short_domain(user_id, draft_domain=draft.get("domain"))
    await call.answer()
    try:
        short_url = await create_short_link_for_url(built.long_url, user_id, custom_slug=None, domain=domain)
    except LinkShortenError as exc:
        await _finish_with_result(call.message.chat.id, user_id, draft, shorten_error=str(exc))
        return
    await _finish_with_result(call.message.chat.id, user_id, draft, short_url=short_url, used_domain=domain)


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


@dp.callback_query(F.data == "clink:pickdomain")
async def cb_clink_pickdomain(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    draft = await _load_draft(user_id)
    if not draft or not draft.get("base_url"):
        await call.answer("Черновик устарел. Нажмите /link", show_alert=True)
        return
    admin = await is_admin(user_id)
    rows = await list_available_domains(user_id, include_all=admin)
    domains = [str(r["domain"]) for r in rows if r.get("domain")]
    await db.set_ui_cache_list(user_id, _CACHE_PICK_DOMAINS_KIND, domains)

    current = str(draft.get("domain") or "").lower()
    buttons: List[List[InlineKeyboardButton]] = []
    for idx, row in enumerate(rows):
        dom = str(row["domain"])
        mark = "✅ " if dom.lower() == current else ""
        tag = " (общий)" if row.get("is_shared") and not row.get("is_builtin") else (" (базовый)" if row.get("is_builtin") else "")
        buttons.append([InlineKeyboardButton(text=f"{mark}{dom}{tag}"[:64], callback_data=f"clink:setdom:{idx}")])
    buttons.append([InlineKeyboardButton(text="➕ Добавить свой домен", callback_data="clink:adddomain_in_flow")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="clink:backtoslug")])

    await call.answer()
    await call.message.answer(
        "Выберите домен для короткой ссылки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@dp.callback_query(F.data.startswith("clink:setdom:"))
async def cb_clink_setdom(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    try:
        idx = int(call.data.split(":", 2)[2])
    except Exception:
        await call.answer("Некорректный выбор", show_alert=True)
        return
    chosen = await db.get_ui_cache_value(user_id, _CACHE_PICK_DOMAINS_KIND, idx)
    draft = await _load_draft(user_id)
    if not chosen or not draft:
        await call.answer("Список устарел", show_alert=True)
        return
    draft["domain"] = chosen.lower()
    await _save_draft(user_id, draft)
    await _save_last_domain(user_id, chosen.lower())
    await call.answer(f"Домен: {chosen}")
    await _send_slug_prompt(call.message.chat.id, user_id, draft)


@dp.callback_query(F.data == "clink:backtoslug")
async def cb_clink_backtoslug(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    draft = await _load_draft(user_id)
    if not draft or not draft.get("base_url"):
        await call.answer("Черновик устарел. Нажмите /link", show_alert=True)
        return
    await call.answer()
    await _send_slug_prompt(call.message.chat.id, user_id, draft)


@dp.callback_query(F.data == "clink:domains")
async def cb_clink_domains(call: CallbackQuery) -> None:
    await call.answer()
    await send_domains_menu(call.message.chat.id, call.from_user.id)


@dp.callback_query(F.data.in_(("clink:adddomain", "clink:adddomain_in_flow")))
async def cb_clink_adddomain(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    action_arg = "add_domain_flow" if call.data == "clink:adddomain_in_flow" else "add_domain"
    await db.set_pending_action(user_id, f"{PendingAction.COMMENT_LINK}:{action_arg}", None)
    cname = bot_cname_target()
    hint = f"\nCNAME для привязки в DNS: <code>{safe(cname)}</code>" if cname else ""
    await call.answer()
    await call.message.answer(
        f"Пришлите свой домен для коротких ссылок (например <code>myshort.online</code>).{hint}\n\n"
        "Для отмены отправьте <code>-</code>."
    )


@dp.callback_query(F.data.startswith("clink:domshare:"))
async def cb_clink_domshare(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    if not await is_admin(user_id):
        await call.answer("Нет прав", show_alert=True)
        return
    try:
        idx = int(call.data.split(":", 2)[2])
    except Exception:
        await call.answer("Некорректный выбор", show_alert=True)
        return
    dom = await db.get_ui_cache_value(user_id, _CACHE_MANAGE_DOMAINS_KIND, idx)
    row = await db.get_short_domain(dom) if dom else None
    if not row:
        await call.answer("Список устарел", show_alert=True)
        return
    new_shared = not bool(row.get("is_shared"))
    await db.set_short_domain_shared(dom, new_shared)
    await call.answer("Общий для всех" if new_shared else "Скрыт из общих")
    await send_domains_menu(call.message.chat.id, user_id)


@dp.callback_query(F.data.startswith("clink:domcf:"))
async def cb_clink_domcf(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    try:
        idx = int(call.data.split(":", 2)[2])
    except Exception:
        await call.answer("Некорректный выбор", show_alert=True)
        return
    dom = await db.get_ui_cache_value(user_id, _CACHE_MANAGE_DOMAINS_KIND, idx)
    row = await db.get_short_domain(dom) if dom else None
    if not row:
        await call.answer("Список устарел", show_alert=True)
        return
    admin = await is_admin(user_id)
    is_owner = row.get("owner_id") is not None and int(row["owner_id"]) == int(user_id)
    if not (admin or is_owner):
        await call.answer("Нет прав", show_alert=True)
        return
    await call.answer("Настраиваю Cloudflare…")
    cf_note = await _try_provision_cloudflare(dom)
    await call.message.answer(f"☁️ <b>{safe(dom)}</b>{cf_note}")


@dp.callback_query(F.data.startswith("clink:domdel:"))
async def cb_clink_domdel(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    try:
        idx = int(call.data.split(":", 2)[2])
    except Exception:
        await call.answer("Некорректный выбор", show_alert=True)
        return
    dom = await db.get_ui_cache_value(user_id, _CACHE_MANAGE_DOMAINS_KIND, idx)
    row = await db.get_short_domain(dom) if dom else None
    if not row:
        await call.answer("Список устарел", show_alert=True)
        return
    admin = await is_admin(user_id)
    is_owner = row.get("owner_id") is not None and int(row["owner_id"]) == int(user_id)
    if not (admin or is_owner):
        await call.answer("Нет прав", show_alert=True)
        return
    await db.deactivate_short_domain(dom)
    await call.answer(f"Удалён: {dom}")
    await send_domains_menu(call.message.chat.id, user_id)


@dp.callback_query(F.data == "clink:menu")
async def cb_clink_menu(call: CallbackQuery) -> None:
    from .menu import send_user_menu

    await call.answer()
    await send_user_menu(call.message.chat.id, call.from_user.id)
