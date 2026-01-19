"""YouTube video download handlers."""

import shutil
from typing import Optional

from aiogram.types import FSInputFile, Message

from ..dispatcher import dp
from .. import db
from ..services.youtube import (
    download_youtube_video,
    is_youtube_url,
    YoutubeDownloadError,
    YoutubeDownloadResult,
    YoutubeVideoTooLarge,
)
from loguru import logger


async def handle_youtube_download(message: Message) -> bool:
    """Handle YouTube video download. Returns True if handled, False otherwise."""
    pending = await db.get_pending_action(message.from_user.id)
    if not pending:
        return False
    action, _ = pending
    if action != "youtube:await_url":
        return False

    text = (message.text or "").strip()
    lowered = text.lower()
    if lowered in ("-", "stop", "стоп"):
        await db.clear_pending_action(message.from_user.id)
        await message.answer("Скачивание отменено")
        return True

    if not is_youtube_url(text):
        await message.answer("Это не похоже на ссылку YouTube. Пришлите корректный URL или '-' чтобы отменить ожидание")
        return True

    status_msg: Optional[Message] = None
    try:
        status_msg = await message.answer("Скачиваю видео, подождите…")
    except Exception as exc:
        logger.warning("Не удалось отправить статусное сообщение о скачивании", error=str(exc))

    download_result: Optional[YoutubeDownloadResult] = None
    try:
        download_result = await download_youtube_video(text)
    except YoutubeVideoTooLarge as exc:
        size_mb = exc.size_bytes / (1024 * 1024)
        response = (
            f"Видео слишком большое для отправки (~{size_mb:.1f} MB, лимит 48 MB). "
            "Попробуйте ссылку на более короткий ролик или '-' чтобы отменить."
        )
        if status_msg:
            try:
                await status_msg.edit_text(response)
            except Exception:
                await message.answer(response)
        else:
            await message.answer(response)
        return True
    except YoutubeDownloadError as exc:
        logger.warning("Ошибка скачивания видео YouTube", error=str(exc))
        detail = str(exc).strip()
        
        # Если ошибка уже содержит подробное объяснение (например, про возрастные ограничения),
        # используем его напрямую
        if detail and ("возрастных ограничений" in detail or "требует подтверждения" in detail):
            response = detail
        else:
            response = "Не удалось скачать видео. Проверьте ссылку и попробуйте ещё раз либо отправьте '-' чтобы отменить."
            if detail and len(detail) < 200:  # Показываем короткие детали
                response += f"\nПричина: {detail}"
        
        if status_msg:
            try:
                await status_msg.edit_text(response)
            except Exception:
                await message.answer(response)
        else:
            await message.answer(response)
        return True
    except Exception as exc:
        logger.exception("Непредвиденная ошибка при скачивании видео YouTube", exc_info=exc)
        response = "Произошла ошибка при скачивании. Попробуйте позже или отправьте '-' чтобы отменить."
        if status_msg:
            try:
                await status_msg.edit_text(response)
            except Exception:
                await message.answer(response)
        else:
            await message.answer(response)
        return True

    if download_result is None:
        return True

    try:
        if status_msg:
            try:
                await status_msg.edit_text("Отправляю видео…")
            except Exception:
                pass
        caption = download_result.title[:1024] if download_result.title else None
        input_file = FSInputFile(download_result.file_path, filename=download_result.file_path.name)
        await message.answer_video(video=input_file, caption=caption)
    except Exception as exc:
        logger.exception("Не удалось отправить скачанное видео", exc_info=exc)
        error_text = "Не удалось отправить видео. Попробуйте ещё раз позже или отправьте '-' чтобы отменить."
        if status_msg:
            try:
                await status_msg.edit_text(error_text)
            except Exception:
                await message.answer(error_text)
        else:
            await message.answer(error_text)
        return True
    finally:
        if download_result is not None:
            shutil.rmtree(download_result.temp_dir, ignore_errors=True)

    await db.clear_pending_action(message.from_user.id)
    if status_msg:
        try:
            await status_msg.delete()
        except Exception:
            pass
    return True
