from __future__ import annotations

import html
import secrets

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from telethon.errors import FloodWaitError, RPCError

from app.bot.keyboards import (
    account_label,
    account_remove_confirmation_keyboard,
    account_sections_keyboard,
    accounts_keyboard,
    back_to_main_keyboard,
    mask_phone,
)
from app.bot.states import AccountStates
from app.bot.ui import edit_or_answer
from app.db import Account, Database
from app.telegram_client import AuthService, SessionService

router = Router(name="accounts")


def _accounts_text(
    accounts: list[Account],
    session_status: dict[int, bool] | None = None,
    role: str | None = None,
) -> str:
    selected_role = role or (accounts[0].role if accounts else "OWNER")
    title = "👤 Мои аккаунты" if selected_role == "OWNER" else "💼 Рабочие аккаунты"
    if not accounts:
        return f"<b>{title}</b>\n\nПодключённых аккаунтов пока нет."

    lines = [(f"<b>{title}</b>\n\nПодключённые Telegram-аккаунты: {len(accounts)}")]
    for account in accounts:
        username = f"@{account.username}" if account.username else "—"
        connected = session_status is None or session_status.get(account.id, False)
        lines.append(
            "\n"
            f"<b>{html.escape(username)}</b>\n"
            f"{html.escape(mask_phone(account.phone))}\n"
            f"{'✅ Подключён' if connected else '⚠️ Авторизация недействительна'}"
        )
    return "\n".join(lines)


async def _show_accounts(
    message: Message,
    owner_id: int,
    db: Database,
    sessions: SessionService,
    state: FSMContext,
    role: str | None = None,
) -> None:
    await state.clear()
    accounts = (
        await db.list_accounts_by_role(owner_id, role)
        if role is not None
        else await db.list_accounts(owner_id)
    )
    nonce = secrets.token_hex(8)
    await state.update_data(account_list_nonce=nonce)
    await state.set_state(AccountStates.viewing)
    session_status = {
        account.id: sessions.exists(account.session_key) for account in accounts
    }
    pair = await db.get_active_account_pair(owner_id)
    active_account_id = (
        pair.owner.id
        if role == "OWNER" and pair.owner is not None
        else pair.target.id
        if role == "TARGET" and pair.target is not None
        else None
    )
    await edit_or_answer(
        message,
        _accounts_text(accounts, session_status, role),
        reply_markup=accounts_keyboard(
            accounts, nonce, session_status, active_account_id
        ),
    )


@router.message(Command("accounts"))
async def list_accounts_command(
    message: Message, db: Database, sessions: SessionService, state: FSMContext
) -> None:
    if message.from_user is not None:
        await state.clear()
        await message.answer(
            "<b>Аккаунты</b>\n\nВыберите тип аккаунтов.",
            reply_markup=account_sections_keyboard(),
        )


@router.callback_query(F.data == "account:list")
async def list_accounts_callback(
    callback: CallbackQuery,
    db: Database,
    sessions: SessionService,
    state: FSMContext,
) -> None:
    await callback.answer()
    if isinstance(callback.message, Message):
        await state.clear()
        await edit_or_answer(
            callback.message,
            "<b>Аккаунты</b>\n\nВыберите тип аккаунтов.",
            reply_markup=account_sections_keyboard(),
        )


@router.callback_query(F.data.startswith("account:list_role:"))
async def list_accounts_by_role_callback(
    callback: CallbackQuery,
    db: Database,
    sessions: SessionService,
    state: FSMContext,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    role = callback.data.rsplit(":", 1)[-1]
    if role not in {"OWNER", "TARGET"}:
        await callback.message.answer("Некорректный тип аккаунта.")
        return
    await _show_accounts(
        callback.message,
        callback.from_user.id,
        db,
        sessions,
        state,
        role,
    )


@router.callback_query(
    AccountStates.viewing,
    F.data.startswith("account:activate:"),
)
async def activate_account(
    callback: CallbackQuery,
    db: Database,
    sessions: SessionService,
    state: FSMContext,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    try:
        _, _, nonce, account_id_raw = callback.data.split(":", maxsplit=3)
        account_id = int(account_id_raw)
    except (ValueError, IndexError):
        await state.clear()
        await edit_or_answer(
            callback.message,
            "Некорректное действие с аккаунтом.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    data = await state.get_data()
    account = await db.get_account(account_id, callback.from_user.id)
    if (
        nonce != data.get("account_list_nonce")
        or account is None
        or not sessions.exists(account.session_key)
    ):
        await state.clear()
        await edit_or_answer(
            callback.message,
            "Список аккаунтов устарел. Откройте раздел заново.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if not await db.set_active_account(
        account.id, callback.from_user.id, account.role
    ):
        await state.clear()
        await edit_or_answer(
            callback.message,
            "Не удалось выбрать аккаунт.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    await _show_accounts(
        callback.message,
        callback.from_user.id,
        db,
        sessions,
        state,
        account.role,
    )


@router.callback_query(
    AccountStates.viewing,
    F.data.startswith("account:remove:"),
)
async def request_remove_account(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return

    try:
        _, _, nonce, account_id_raw = callback.data.split(":", maxsplit=3)
        account_id = int(account_id_raw)
    except (ValueError, IndexError):
        await callback.message.answer("Некорректное действие с аккаунтом.")
        return

    data = await state.get_data()
    account = await db.get_account(account_id, callback.from_user.id)
    if nonce != data.get("account_list_nonce"):
        await state.clear()
        await edit_or_answer(
            callback.message,
            "Список аккаунтов устарел. Откройте раздел заново.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if account is None:
        await state.clear()
        await edit_or_answer(
            callback.message,
            "Аккаунт не найден или уже удалён.",
            reply_markup=back_to_main_keyboard(),
        )
        return

    confirm_nonce = secrets.token_hex(8)
    await state.update_data(
        account_remove_nonce=confirm_nonce,
        account_remove_id=account.id,
    )
    await state.set_state(AccountStates.awaiting_removal)
    await edit_or_answer(
        callback.message,
        "<b>Удалить аккаунт?</b>\n\n"
        f"{html.escape(account_label(account))}\n"
        f"{html.escape(mask_phone(account.phone))}\n\n"
        "После удаления бот больше не сможет использовать этот аккаунт.",
        reply_markup=account_remove_confirmation_keyboard(confirm_nonce, account.id),
    )


@router.callback_query(
    AccountStates.awaiting_removal,
    F.data.startswith("account:remove_confirm:"),
)
async def remove_account(
    callback: CallbackQuery,
    db: Database,
    sessions: SessionService,
    auth_service: AuthService,
    state: FSMContext,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    try:
        _, _, nonce, account_id_raw = callback.data.split(":", maxsplit=3)
        account_id = int(account_id_raw)
    except (ValueError, IndexError):
        await state.clear()
        await callback.message.answer("Некорректное действие с аккаунтом.")
        return
    data = await state.get_data()
    expected_id = data.get("account_remove_id")
    expected_nonce = data.get("account_remove_nonce")
    await state.clear()
    if nonce != expected_nonce or account_id != expected_id:
        await callback.message.answer(
            "Подтверждение удаления устарело.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    account = await db.get_account(account_id, callback.from_user.id)
    if account is None:
        await callback.message.answer(
            "Аккаунт не найден или уже удалён.",
            reply_markup=back_to_main_keyboard(),
        )
        return

    remote_logout_confirmed = False
    try:
        if sessions.exists(account.session_key):
            remote_logout_confirmed = await auth_service.logout(
                sessions.path_for(account.session_key)
            )
    except (FloodWaitError, RPCError, OSError, ValueError):
        # Local removal still prevents this application from reusing the session.
        remote_logout_confirmed = False
    finally:
        local_session_removed = sessions.delete(account.session_key)

    if not local_session_removed:
        await callback.message.answer(
            "❌ Не удалось удалить аккаунт. Попробуйте ещё раз."
        )
        return

    await db.delete_account(account.id, callback.from_user.id)

    result = "✅ Аккаунт удалён."
    if not remote_logout_confirmed:
        result += "\n\nПроверьте список активных устройств в Telegram."
    await callback.message.answer(result, reply_markup=back_to_main_keyboard())
