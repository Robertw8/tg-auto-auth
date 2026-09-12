from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import help_keyboard, main_menu_keyboard
from app.bot.ui import edit_or_answer, main_menu_text
from app.db import Database
from app.web import WebAuthCoordinator

router = Router(name="navigation")

HELP_TEXT = (
    "<b>❓ Помощь</b>\n\n"
    "<b>Как это работает</b>\n"
    "1. Подключите ваш и рабочий Telegram-аккаунты.\n"
    "2. Выберите MRKT или Portals.\n"
    "3. Проверьте данные операции.\n"
    "4. Подтвердите действие.\n\n"
    "В MRKT рабочий аккаунт размещает подарок, а ваш покупает его.\n"
    "В Portals ваш аккаунт отправляет оффер, а рабочий принимает его.\n\n"
    "Отдельное размещение: /mrkt_job. Отдельное принятие оффера: /portals_job.\n\n"
    "Если операция получила статус «Требуется проверка», не повторяйте её до ручной проверки.\n\n"
    "<b>Безопасность</b>\n"
    "Код Telegram и пароль 2FA вводятся только во встроенной форме и не отправляются в чат."
)


@router.message(Command("help"))
async def help_command(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(HELP_TEXT, reply_markup=help_keyboard())


@router.callback_query(F.data == "nav:help")
async def help_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.clear()
    if isinstance(callback.message, Message):
        await edit_or_answer(callback.message, HELP_TEXT, reply_markup=help_keyboard())


@router.callback_query(F.data == "nav:main")
async def back_to_main(
    callback: CallbackQuery,
    state: FSMContext,
    web_auth: WebAuthCoordinator,
    db: Database,
) -> None:
    await callback.answer()
    await state.clear()
    await web_auth.cancel_owner(callback.from_user.id)
    if isinstance(callback.message, Message):
        has_accounts = bool(await db.list_accounts(callback.from_user.id))
        await edit_or_answer(
            callback.message,
            main_menu_text(has_accounts=has_accounts),
            reply_markup=main_menu_keyboard(has_accounts=has_accounts),
        )


@router.callback_query()
async def reject_stale_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Эта кнопка устарела.", show_alert=True)
    await state.clear()
    if isinstance(callback.message, Message):
        await edit_or_answer(
            callback.message,
            "Действие больше не активно. Выберите нужный раздел заново.",
            reply_markup=main_menu_keyboard(),
        )
