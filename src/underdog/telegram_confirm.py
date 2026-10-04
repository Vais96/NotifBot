"""Telegram send confirmation: Underdog is marked telegram_sent only after ok + message_id + HTTP 200."""

from __future__ import annotations
import json
from typing import Any, Dict, Optional
from loguru import logger


def _truncate_log_text(text: str, max_len: int = 1500) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _chat_to_log_dict(chat: Any) -> Optional[Dict[str, Any]]:
    if chat is None:
        return None
    out: Dict[str, Any] = {}
    cid = getattr(chat, "id", None)
    if cid is not None:
        out["id"] = cid
    ctype = getattr(chat, "type", None)
    if ctype is not None:
        out["type"] = str(getattr(ctype, "value", ctype))
    title = getattr(chat, "title", None)
    username = getattr(chat, "username", None)
    if title:
        out["title"] = title
    if username:
        out["username"] = username
    return out or None


def _telegram_api_response_dict(msg: Any) -> Dict[str, Any]:
    """
    Похоже на JSON ответа Telegram Bot API: {"ok": true, "result": {...}}.
    Эквивалент проверки на PHP: if ($response['ok'] === true) { ... }
    """
    if msg is None:
        return {"ok": False, "result": None, "description": "empty response"}
    mid = getattr(msg, "message_id", None)
    if mid is None:
        return {
            "ok": False,
            "result": {"_type": type(msg).__name__},
            "description": "missing message_id",
        }
    body: Dict[str, Any] = {
        "message_id": int(mid),
        "date": getattr(msg, "date", None),
        "chat": _chat_to_log_dict(getattr(msg, "chat", None)),
    }
    text = getattr(msg, "text", None) or getattr(msg, "caption", None)
    if text is not None:
        body["text"] = _truncate_log_text(str(text), 800)
    from_user = getattr(msg, "from_user", None) or getattr(msg, "from", None)
    if from_user is not None:
        body["from"] = {
            "id": getattr(from_user, "id", None),
            "username": getattr(from_user, "username", None),
        }
    return {"ok": True, "result": body}


def _log_telegram_send_roundtrip(
    *,
    context: str,
    chat_id: int,
    text: str,
    msg: Any,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Один JSON в лог: тело запроса sendMessage и то, что вернула Telegram (как в Postman)."""
    response_body = _telegram_api_response_dict(msg)
    response_body["http_status"] = getattr(msg, "__tg_http_status__", None)
    line: Dict[str, Any] = {
        "telegram_api_roundtrip": context,
        "request": {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": _truncate_log_text(text),
            "text_full_length": len(text),
            "parse_mode": "HTML",
        },
        "response": response_body,
    }
    if extra:
        line["extra"] = extra
    logger.info("{}", json.dumps(line, ensure_ascii=False, default=str))


def _telegram_message_confirmed(msg: Any) -> bool:
    """True только если в ответе API ok === true (есть message_id)."""
    return _telegram_api_response_dict(msg).get("ok") is True


def _telegram_http_ok_for_underdog(msg: Any) -> bool:
    """HTTP 200 от Bot API — иначе не помечаем telegram_sent в Underdog."""
    return getattr(msg, "__tg_http_status__", None) == 200


def _telegram_underdog_send_confirmed(msg: Any) -> bool:
    """Тело ответа успешно и транспорт HTTP 200 (как в Postman / curl -w %http_code)."""
    return _telegram_message_confirmed(msg) and _telegram_http_ok_for_underdog(msg)
