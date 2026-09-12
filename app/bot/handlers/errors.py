from __future__ import annotations

import logging

from aiogram.exceptions import TelegramAPIError
from aiogram.types import ErrorEvent, Message

from app.web import WebAuthCoordinator

logger = logging.getLogger(__name__)


async def handle_error(event: ErrorEvent, web_auth: WebAuthCoordinator) -> bool:
    # Never log the update or exception text: either may contain user input.
    logger.error("Unhandled bot update error (%s)", type(event.exception).__name__)

    message = event.update.message
    callback = event.update.callback_query
    user_id = None
    if message is not None and message.from_user is not None:
        user_id = message.from_user.id
    elif callback is not None:
        user_id = callback.from_user.id
    if user_id is not None:
        await web_auth.cancel_owner(user_id)

    target = message
    if (
        target is None
        and callback is not None
        and isinstance(callback.message, Message)
    ):
        target = callback.message
    if target is not None:
        try:
            await target.answer("❌ Что-то пошло не так. Попробуйте ещё раз.")
        except TelegramAPIError:
            pass
    return True
