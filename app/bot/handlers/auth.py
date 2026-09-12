from __future__ import annotations

from urllib.parse import quote

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import (
    account_role_continue_keyboard,
    add_account_intro_keyboard,
    authorization_keyboard,
    main_menu_keyboard,
)
from app.bot.ui import edit_or_answer
from app.config import Config
from app.web import AttemptConflictError, WebAuthCoordinator

router = Router(name="auth")


@router.callback_query(F.data == "account:add")
async def add_account(
    callback: CallbackQuery,
    web_auth: WebAuthCoordinator,
    state: FSMContext,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    await web_auth.cancel_owner(callback.from_user.id)
    await state.clear()
    await edit_or_answer(
        callback.message,
        "<b>➕ Подключение аккаунта</b>\n\n"
        "Сначала выберите тип аккаунта. Авторизация откроется во встроенном окне Telegram.\n\n"
        "Вам понадобится:\n"
        "• номер телефона;\n"
        "• код от Telegram;\n"
        "• пароль 2FA, если он включён.\n\n"
        "Коды и пароль не отправляются сообщениями в бот.",
        reply_markup=add_account_intro_keyboard(),
    )


@router.callback_query(F.data.startswith("auth:role:"))
async def choose_account_role(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    role = callback.data.rsplit(":", 1)[-1]
    if role not in {"OWNER", "TARGET"}:
        await callback.message.answer("Некорректный тип аккаунта.")
        return
    await state.update_data(account_role=role)
    title = "👤 Мой аккаунт" if role == "OWNER" else "💼 Рабочий аккаунт"
    description = (
        "Он будет храниться постоянно и использоваться для покупок и отправки офферов."
        if role == "OWNER"
        else "Он будет использоваться для размещений и принятия офферов."
    )
    await edit_or_answer(
        callback.message,
        f"<b>{title}</b>\n\n{description}\n\nПродолжить подключение?",
        reply_markup=account_role_continue_keyboard(),
    )


@router.callback_query(F.data == "auth:continue")
async def continue_add_account(
    callback: CallbackQuery,
    config: Config,
    web_auth: WebAuthCoordinator,
    state: FSMContext,
) -> None:
    await callback.answer("Готовим форму…")
    if not isinstance(callback.message, Message):
        return
    data = await state.get_data()
    role = str(data.get("account_role", "TARGET"))
    await state.clear()
    try:
        authorization = await web_auth.create_attempt(
            callback.from_user.id, callback.message.chat.id, role
        )
    except AttemptConflictError:
        await callback.message.answer(
            "Подключение уже выполняется. Завершите его или нажмите «Отмена»."
        )
        return

    # Fragments are not sent to the web server and therefore avoid access-log leaks.
    webapp_url = f"{config.webapp_public_url}#token={quote(authorization.launch_token)}"
    await edit_or_answer(
        callback.message,
        "Откройте форму авторизации и следуйте подсказкам.\n\n"
        "Ссылка действует ограниченное время.",
        reply_markup=authorization_keyboard(webapp_url),
    )


@router.callback_query(F.data == "auth:cancel")
async def cancel_login_callback(
    callback: CallbackQuery, web_auth: WebAuthCoordinator, state: FSMContext
) -> None:
    await callback.answer()
    await web_auth.cancel_owner(callback.from_user.id)
    await state.clear()
    if isinstance(callback.message, Message):
        await edit_or_answer(
            callback.message,
            "Подключение отменено.",
            reply_markup=main_menu_keyboard(),
        )


@router.message(Command("cancel"))
async def cancel_login(
    message: Message, web_auth: WebAuthCoordinator, state: FSMContext
) -> None:
    if message.from_user is None:
        return
    cancelled = await web_auth.cancel_owner(message.from_user.id)
    await state.clear()
    await message.answer(
        "Подключение отменено." if cancelled else "Активного подключения нет.",
        reply_markup=main_menu_keyboard(),
    )
