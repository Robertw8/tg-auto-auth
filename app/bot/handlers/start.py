from __future__ import annotations

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from app.bot.keyboards import main_menu_keyboard
from app.bot.ui import main_menu_text
from app.db import Database
from app.web import WebAuthCoordinator

router = Router(name="start")


@router.message(CommandStart())
async def start(
    message: Message,
    web_auth: WebAuthCoordinator,
    db: Database,
    state: FSMContext,
) -> None:
    if message.from_user is None:
        return

    await web_auth.cancel_owner(message.from_user.id)
    await state.clear()

    has_accounts = bool(await db.list_accounts(message.from_user.id))
    await message.answer(
        main_menu_text(has_accounts=has_accounts),
        reply_markup=main_menu_keyboard(has_accounts=has_accounts),
    )
