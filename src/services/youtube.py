"""YouTube download helper utilities used by the bot handlers."""

from __future__ import annotations

import asyncio
import base64
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yt_dlp
from loguru import logger
from yt_dlp.utils import DownloadError

from ..config import settings

TELEGRAM_MAX_VIDEO_BYTES = 48 * 1024 * 1024
YOUTUBE_URL_RE = re.compile(r"^(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/", re.IGNORECASE)


@dataclass
class YoutubeDownloadResult:
    file_path: Path
    title: str
    temp_dir: Path


class YoutubeDownloadError(Exception):
    """Base error for YouTube download flow."""


class YoutubeVideoTooLarge(YoutubeDownloadError):
    def __init__(self, size_bytes: int):
        super().__init__("Video exceeds Telegram upload limit")
        self.size_bytes = size_bytes


_YOUTUBE_COOKIES_CACHE: Optional[Path] = None


def is_youtube_url(value: str) -> bool:
    if not value:
        return False
    return bool(YOUTUBE_URL_RE.match(value.strip()))


def _ensure_url_scheme(value: str) -> str:
    if not value:
        return value
    stripped = value.strip()
    if not re.match(r"^https?://", stripped, re.IGNORECASE):
        return "https://" + stripped
    return stripped


def _build_youtube_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {}
    token = (settings.youtube_identity_token or "").strip()
    if token:
        headers["X-Youtube-Identity-Token"] = token
    auth_user = (settings.youtube_auth_user or "").strip()
    if auth_user:
        headers["X-Goog-AuthUser"] = auth_user
    return headers


def _normalize_cookiefile_content(content: str) -> str:
    normalized: List[str] = []
    modified = False
    for raw in content.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if not raw.strip():
            normalized.append("")
            continue
        if raw.startswith("#"):
            normalized.append(raw)
            continue
        if "\t" not in raw:
            for idx in range(len(normalized) - 1, -1, -1):
                if "\t" in normalized[idx]:
                    normalized[idx] += raw.strip()
                    modified = True
                    break
            else:
                normalized.append(raw.strip())
                modified = True
            continue
        normalized.append(raw)
    if modified:
        logger.debug("Normalized YouTube cookies text before caching")
    return "\n".join(normalized)


def _resolve_youtube_cookies_file() -> Optional[str]:
    global _YOUTUBE_COOKIES_CACHE
    path_setting = settings.youtube_cookies_path
    raw_cookies = settings.youtube_cookies_raw
    encoded_cookies = settings.youtube_cookies_base64
    configured_cookies = bool(path_setting or raw_cookies or encoded_cookies)

    if path_setting:
        try:
            resolved = Path(path_setting).expanduser()
            if resolved.exists():
                return str(resolved.resolve())
            logger.warning("Configured YouTube cookies file not found", path=str(resolved))
        except Exception as exc:
            logger.warning("Failed to resolve YouTube cookies path", error=str(exc))

    def _cache_content(content: str) -> str:
        global _YOUTUBE_COOKIES_CACHE
        if _YOUTUBE_COOKIES_CACHE is None:
            temp_dir = Path(tempfile.mkdtemp(prefix="ytcookies-"))
            _YOUTUBE_COOKIES_CACHE = temp_dir / "cookies.txt"
        try:
            assert _YOUTUBE_COOKIES_CACHE is not None
            if not _YOUTUBE_COOKIES_CACHE.parent.exists():
                _YOUTUBE_COOKIES_CACHE.parent.mkdir(parents=True, exist_ok=True)
            sanitized = _normalize_cookiefile_content(content)
            _YOUTUBE_COOKIES_CACHE.write_text(sanitized, encoding="utf-8")
        except Exception as exc:
            logger.warning("Failed to write YouTube cookies cache", error=str(exc))
        return str(_YOUTUBE_COOKIES_CACHE)

    if raw_cookies:
        return _cache_content(raw_cookies)

    if encoded_cookies:
        try:
            decoded = base64.b64decode(encoded_cookies).decode("utf-8")
        except Exception as exc:
            logger.warning("Failed to decode base64 YouTube cookies", error=str(exc))
        else:
            return _cache_content(decoded)

    if not configured_cookies and _YOUTUBE_COOKIES_CACHE and _YOUTUBE_COOKIES_CACHE.exists():
        try:
            _YOUTUBE_COOKIES_CACHE.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        try:
            if _YOUTUBE_COOKIES_CACHE.parent.exists():
                _YOUTUBE_COOKIES_CACHE.parent.rmdir()
        except Exception:
            pass
        logger.debug("Cleared cached YouTube cookies because no configuration present")
        _YOUTUBE_COOKIES_CACHE = None
        return None

    if _YOUTUBE_COOKIES_CACHE and _YOUTUBE_COOKIES_CACHE.exists():
        return str(_YOUTUBE_COOKIES_CACHE)
    return None


async def download_youtube_video(url: str) -> YoutubeDownloadResult:
    normalized_url = _ensure_url_scheme(url)
    temp_dir = Path(tempfile.mkdtemp(prefix="ytbot-"))

    cookies_file = _resolve_youtube_cookies_file()
    youtube_headers = _build_youtube_headers()

    if cookies_file or youtube_headers:
        client_order: Optional[List[str]] = ["web", "android"]
    else:
        client_order = None

    logger.bind(
        url=url,
        cookies_path=cookies_file,
        headers_present=bool(youtube_headers),
        client_order=client_order,
    ).debug("Preparing YouTube download")

    ffmpeg_path = shutil.which("ffmpeg")

    def _probe_info() -> dict[str, Any]:
        options: dict[str, Any] = {
            "outtmpl": str(temp_dir / "probe"),
            "noplaylist": True,
            "quiet": True,
            "no_color": True,
            "skip_download": True,
        }
        if cookies_file:
            options["cookiefile"] = cookies_file
        options["extractor_args"] = {
            "youtube": {
                "skip": ["dash"],
            }
        }
        if client_order:
            options["extractor_args"]["youtube"]["player_client"] = client_order
        options["extractor_retries"] = 3
        if youtube_headers:
            options["http_headers"] = youtube_headers
        with yt_dlp.YoutubeDL(options) as ydl:
            return ydl.extract_info(normalized_url, download=False)

    try:
        probe = await asyncio.to_thread(_probe_info)
    except Exception as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise YoutubeDownloadError(str(exc)) from exc

    formats = probe.get("formats") or []
    progressive_available = any(
        (fmt.get("acodec") not in (None, "none")) and (fmt.get("vcodec") not in (None, "none"))
        for fmt in formats
    )
    audio_available = any(fmt.get("acodec") not in (None, "none") for fmt in formats)
    video_available = any(fmt.get("vcodec") not in (None, "none") for fmt in formats)
    have_separate_streams = audio_available and video_available

    format_candidates: List[str] = []
    if ffmpeg_path and have_separate_streams:
        format_candidates.extend([
            "bv*+ba/b",
            "bestvideo*+bestaudio/best",
        ])
    if progressive_available:
        format_candidates.extend([
            "best[acodec!=none][vcodec!=none]",
            "best[height<=1080][acodec!=none][vcodec!=none]",
            "best[height<=720][acodec!=none][vcodec!=none]",
            "best[height<=480][acodec!=none][vcodec!=none]",
        ])

    if not format_candidates:
        shutil.rmtree(temp_dir, ignore_errors=True)
        if not ffmpeg_path and have_separate_streams:
            raise YoutubeDownloadError("Для этого видео нужен установленный ffmpeg, т.к. YouTube выдаёт раздельные дорожки")
        raise YoutubeDownloadError("Не удалось подобрать доступный формат видео")

    last_error: Optional[Exception] = None

    def _invoke_download(fmt: str) -> tuple[dict[str, Any], str]:
        options: dict[str, Any] = {
            "outtmpl": str(temp_dir / "%(title)s.%(ext)s"),
            "noplaylist": True,
            "quiet": True,
            "format": fmt,
            "restrictfilenames": True,
            "no_color": True,
            "extractor_args": {
                "youtube": {
                    "skip": ["dash"]
                }
            },
            "extractor_retries": 3,
        }
        if client_order:
            options["extractor_args"]["youtube"]["player_client"] = client_order
        if cookies_file:
            options["cookiefile"] = cookies_file
        if ffmpeg_path:
            options.update(
                {
                    "ffmpeg_location": ffmpeg_path,
                    "merge_output_format": "mp4",
                    "postprocessors": [
                        {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}
                    ],
                }
            )
            if youtube_headers:
                options["http_headers"] = youtube_headers
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(normalized_url, download=True)
            filepath = ydl.prepare_filename(info)
            requested = info.get("requested_downloads") or []
            for item in requested:
                candidate = item.get("filepath")
                if candidate:
                    filepath = candidate
            final_path = info.get("_filename")
            if final_path:
                filepath = final_path
            return info, filepath

    info: Optional[dict[str, Any]] = None
    filepath: Optional[str] = None
    for fmt in format_candidates:
        try:
            info, filepath = await asyncio.to_thread(_invoke_download, fmt)
            break
        except DownloadError as exc:
            last_error = exc
            continue
        except Exception as exc:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise YoutubeDownloadError(str(exc)) from exc
    else:
        shutil.rmtree(temp_dir, ignore_errors=True)
        message = str(last_error) if last_error else "Не удалось подобрать формат"
        if last_error and "ffmpeg" in message.lower() and not ffmpeg_path:
            message = "Требуется установленный ffmpeg для склейки видео и аудио"
        raise YoutubeDownloadError(message)

    if not info or not filepath:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise YoutubeDownloadError("Не удалось скачать видео")

    file_path = Path(filepath)
    if not file_path.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise YoutubeDownloadError("Downloaded file not found")

    size = file_path.stat().st_size
    if size > TELEGRAM_MAX_VIDEO_BYTES:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise YoutubeVideoTooLarge(size)

    title = str(info.get("title") or file_path.stem)
    return YoutubeDownloadResult(file_path=file_path, title=title, temp_dir=temp_dir)
