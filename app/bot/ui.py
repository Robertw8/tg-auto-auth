from __future__ import annotations

from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup, Message


def main_menu_text(*, has_accounts: bool) -> str:
    text = "<b>Главное меню</b>\n\nВыберите, что хотите сделать."
    if not has_accounts:
        text += (
            "\n\nУ вас пока нет подключённых аккаунтов.\n"
            "Сначала добавьте Telegram-аккаунт."
        )
    return text


async def edit_or_answer(
    message: Message,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    """Prefer one tidy bot message, but fall back when Telegram cannot edit it."""
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        await message.answer(text, reply_markup=reply_markup)
