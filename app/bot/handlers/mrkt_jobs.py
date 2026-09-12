from __future__ import annotations

import html
import logging
import re
import secrets
from decimal import Decimal, InvalidOperation
from typing import Any

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import (
    back_to_main_keyboard,
    mrkt_empty_keyboard,
    mrkt_job_account_keyboard,
    mrkt_job_confirmation_keyboard,
    mrkt_job_gifts_keyboard,
    mrkt_price_keyboard,
)
from app.bot.states import AccountStates, MrktJobStates
from app.bot.ui import edit_or_answer
from app.db import Account, Database
from app.jobs import MrktJobService
from app.telegram_client import (
    MiniAppAccountNotFoundError,
    MiniAppSessionMissingError,
    MiniAppSessionUnauthorizedError,
    MrktApiError,
    MrktAuthenticationError,
    MrktGift,
    MrktNetworkError,
    MrktPriceError,
    MrktService,
)

router = Router(name="mrkt-jobs")
logger = logging.getLogger(__name__)
MAX_GIFT_BUTTONS = 20
PRICE_PATTERN = re.compile(r"^\d+(?:\.\d+)?$")


@router.message(Command("mrkt_job"))
async def start_mrkt_job_command(
    message: Message,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    if message.from_user is not None:
        await _start_mrkt(message, message.from_user.id, db, state, mrkt_service)


@router.callback_query(F.data == "mrktjob:start")
async def start_mrkt_job_callback(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await callback.answer()
    if isinstance(callback.message, Message):
        await _start_mrkt(
            callback.message, callback.from_user.id, db, state, mrkt_service
        )


async def _start_mrkt(
    message: Message,
    owner_id: int,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await state.clear()
    accounts = await db.list_accounts(owner_id)
    if not accounts:
        await edit_or_answer(
            message,
            "<b>🛒 MRKT</b>\n\nСначала подключите Telegram-аккаунт.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if len(accounts) == 1:
        await _load_inventory(message, owner_id, accounts[0], state, mrkt_service)
        return
    await _show_account_choice(message, accounts, state)


async def _show_account_choice(
    message: Message, accounts: list[Account], state: FSMContext
) -> None:
    nonce = secrets.token_hex(8)
    await state.clear()
    await state.update_data(mrkt_account_nonce=nonce)
    await state.set_state(MrktJobStates.awaiting_account)
    await edit_or_answer(
        message,
        "<b>🛒 MRKT</b>\n\nРазмещение подарка на продажу.\n\nВыберите аккаунт:",
        reply_markup=mrkt_job_account_keyboard(accounts, nonce),
    )


@router.callback_query(
    MrktJobStates.awaiting_account,
    F.data.startswith("mrktjob:account:"),
)
async def select_mrkt_account(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    parts = callback.data.split(":")
    data = await state.get_data()
    if len(parts) != 4 or parts[2] != data.get("mrkt_account_nonce"):
        await _stale(callback.message, state, "Выбор аккаунта устарел.")
        return
    try:
        account_id = int(parts[3])
    except ValueError:
        await _stale(callback.message, state, "Не удалось выбрать аккаунт.")
        return
    account = await db.get_account(account_id, callback.from_user.id)
    if account is None:
        await _stale(callback.message, state, "Аккаунт больше недоступен.")
        return
    await _load_inventory(
        callback.message,
        callback.from_user.id,
        account,
        state,
        mrkt_service,
    )


@router.callback_query(
    AccountStates.viewing,
    F.data.startswith("acct:mrkt:"),
)
async def select_mrkt_from_accounts(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    parts = callback.data.split(":")
    data = await state.get_data()
    if len(parts) != 4 or parts[2] != data.get("account_list_nonce"):
        await _stale(callback.message, state, "Список аккаунтов устарел.")
        return
    try:
        account_id = int(parts[3])
    except ValueError:
        await _stale(callback.message, state, "Не удалось выбрать аккаунт.")
        return
    account = await db.get_account(account_id, callback.from_user.id)
    if account is None:
        await _stale(callback.message, state, "Аккаунт больше недоступен.")
        return
    await _load_inventory(
        callback.message,
        callback.from_user.id,
        account,
        state,
        mrkt_service,
    )


async def _load_inventory(
    message: Message,
    owner_id: int,
    account: Account,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await state.clear()
    await edit_or_answer(message, "🔄 Загружаем хранилище MRKT…")
    try:
        inventory = await mrkt_service.get_inventory(
            account.id,
            owner_telegram_id=owner_id,
            is_listed=False,
        )
    except Exception as exc:  # noqa: BLE001 - sanitized UI boundary
        await state.clear()
        await _send_inventory_error(message, account.id, exc)
        return
    eligible = [gift for gift in inventory if gift.eligible]
    await state.update_data(
        mrkt_job_account_id=account.id,
        mrkt_job_account_label=_account_label(account),
        mrkt_job_gifts=eligible[:MAX_GIFT_BUTTONS],
    )
    if not eligible:
        nonce = secrets.token_hex(8)
        await state.update_data(mrkt_refresh_nonce=nonce)
        await state.set_state(MrktJobStates.awaiting_inventory_retry)
        await edit_or_answer(
            message,
            "<b>🛒 MRKT</b>\n\n"
            "Здесь пока нечего размещать.\n\n"
            "В хранилище MRKT нет доступных для продажи подарков.",
            reply_markup=mrkt_empty_keyboard(nonce, account.id),
        )
        return
    if len(eligible) == 1:
        await _select_gift_and_ask_price(message, state, eligible[0], False)
        return
    await _show_gift_choice(message, state, eligible[:MAX_GIFT_BUTTONS])


async def _show_gift_choice(
    message: Message, state: FSMContext, gifts: list[MrktGift]
) -> None:
    nonce = secrets.token_hex(8)
    await state.update_data(mrkt_gift_nonce=nonce, mrkt_job_gifts=gifts)
    await state.set_state(MrktJobStates.awaiting_gift)
    suffix = "\n\nПоказаны первые 20." if len(gifts) >= MAX_GIFT_BUTTONS else ""
    await edit_or_answer(
        message,
        "<b>🛒 MRKT</b>\n\nВыберите подарок:" + suffix,
        reply_markup=mrkt_job_gifts_keyboard(gifts, nonce),
    )


@router.callback_query(
    MrktJobStates.awaiting_gift,
    F.data.startswith("mrktjob:gift:"),
)
async def select_mrkt_gift(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    parts = callback.data.split(":")
    data = await state.get_data()
    gifts = data.get("mrkt_job_gifts")
    if len(parts) != 4 or parts[2] != data.get("mrkt_gift_nonce"):
        await _stale(callback.message, state, "Выбор подарка устарел.")
        return
    try:
        index = int(parts[3])
    except ValueError:
        index = -1
    if not isinstance(gifts, list) or not 0 <= index < len(gifts):
        await _stale(callback.message, state, "Не удалось выбрать подарок.")
        return
    gift = gifts[index]
    if not isinstance(gift, MrktGift) or not gift.eligible:
        await _stale(callback.message, state, "Этот подарок больше нельзя разместить.")
        return
    await _select_gift_and_ask_price(callback.message, state, gift, True)


async def _select_gift_and_ask_price(
    message: Message,
    state: FSMContext,
    gift: MrktGift,
    back_to_gifts: bool,
) -> None:
    await state.update_data(
        mrkt_selected_gift_id=gift.gift_id,
        mrkt_selected_gift_name=gift.display_name,
        mrkt_back_to_gifts=back_to_gifts,
    )
    await state.set_state(MrktJobStates.awaiting_price)
    await edit_or_answer(
        message,
        f"<b>{html.escape(gift.display_name)}</b>\n\n"
        "Введите цену в TON.\n\n"
        "Например:\n<code>1.68</code>\n\n"
        "💡 До 2 знаков после точки.",
        reply_markup=mrkt_price_keyboard(back_to_gifts=back_to_gifts),
    )


@router.message(
    MrktJobStates.awaiting_price,
    F.text,
    ~F.text.startswith("/"),
)
async def receive_mrkt_price(
    message: Message,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    price = (message.text or "").strip()
    validation_error = _price_validation_error(price)
    if validation_error is not None:
        await message.answer(validation_error)
        return
    try:
        mrkt_service.ton_to_nanotons(price)
        formatted = mrkt_service.format_ton(price)
    except (MrktPriceError, ArithmeticError, ValueError):
        await message.answer("Не удалось проверить цену. Введите её ещё раз.")
        return
    data = await state.get_data()
    account_id = data.get("mrkt_job_account_id")
    account_name = data.get("mrkt_job_account_label")
    gift_name = data.get("mrkt_selected_gift_name")
    gift_id: Any = data.get("mrkt_selected_gift_id")
    if (
        not isinstance(account_id, int)
        or not isinstance(account_name, str)
        or not isinstance(gift_name, str)
        or isinstance(gift_id, bool)
        or not isinstance(gift_id, (str, int))
    ):
        await _stale(message, state, "Выбор MRKT устарел.")
        return
    nonce = secrets.token_hex(8)
    await state.update_data(mrkt_job_price=formatted, mrkt_confirm_nonce=nonce)
    await state.set_state(MrktJobStates.awaiting_confirmation)
    await message.answer(
        _mrkt_review_text(
            account_name,
            gift_name,
            formatted,
            dry_run=mrkt_service.dry_run,
        ),
        reply_markup=mrkt_job_confirmation_keyboard(nonce),
    )


def _price_validation_error(price: str) -> str | None:
    if not PRICE_PATTERN.fullmatch(price):
        return "Введите цену числом, например 1.68."
    try:
        value = Decimal(price)
    except InvalidOperation:
        return "Введите цену числом, например 1.68."
    if value <= 0:
        return "Цена должна быть больше 0."
    exponent = value.as_tuple().exponent
    decimals = max(0, -exponent) if isinstance(exponent, int) else 0
    if decimals > 2:
        return "Можно использовать не более 2 знаков после точки."
    return None


def _mrkt_review_text(
    account_name: str,
    gift_name: str,
    price: str,
    *,
    dry_run: bool,
) -> str:
    mode_note = (
        "\n\n<b>🧪 Тестовый режим</b>\nРеальное размещение выполнено не будет."
        if dry_run
        else ""
    )
    return (
        "<b>Проверьте размещение</b>\n\n"
        f"Аккаунт: {html.escape(account_name)}\n"
        f"Подарок: {html.escape(gift_name)}\n"
        f"Цена: <b>{html.escape(price)} TON</b>\n\n"
        "После подтверждения подарок будет выставлен на продажу."
        f"{mode_note}"
    )


@router.callback_query(
    MrktJobStates.awaiting_confirmation,
    F.data.startswith("mrktjob:edit_price:"),
)
async def edit_mrkt_price(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", maxsplit=1)[-1]
    if nonce != data.get("mrkt_confirm_nonce"):
        await _stale(callback.message, state, "Подтверждение устарело.")
        return
    gift_name = data.get("mrkt_selected_gift_name")
    if not isinstance(gift_name, str):
        await _stale(callback.message, state, "Выбор подарка устарел.")
        return
    await state.set_state(MrktJobStates.awaiting_price)
    await edit_or_answer(
        callback.message,
        f"<b>{html.escape(gift_name)}</b>\n\n"
        "Введите новую цену в TON.\n\n"
        "💡 До 2 знаков после точки.",
        reply_markup=mrkt_price_keyboard(
            back_to_gifts=bool(data.get("mrkt_back_to_gifts"))
        ),
    )


@router.callback_query(
    MrktJobStates.awaiting_confirmation,
    F.data.startswith("mrktjob:confirm:"),
)
async def confirm_mrkt_job(
    callback: CallbackQuery,
    state: FSMContext,
    mrkt_jobs: MrktJobService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", maxsplit=1)[-1]
    account_id = data.get("mrkt_job_account_id")
    price = data.get("mrkt_job_price")
    gift_id: Any = data.get("mrkt_selected_gift_id")
    expected_nonce = data.get("mrkt_confirm_nonce")
    await state.clear()
    if (
        nonce != expected_nonce
        or not isinstance(account_id, int)
        or not isinstance(price, str)
        or isinstance(gift_id, bool)
        or not isinstance(gift_id, (str, int))
    ):
        await edit_or_answer(
            callback.message,
            "Подтверждение устарело. Начните заново.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    await edit_or_answer(
        callback.message,
        "🔄 Размещаем подарок…\n\nНе нажимайте кнопку повторно.",
    )
    try:
        await mrkt_jobs.execute_mrkt_job(
            callback.from_user.id,
            account_id,
            price,
            gift_id,
        )
    except Exception as exc:  # noqa: BLE001 - isolate one user's job
        logger.warning(
            "MRKT job dispatch failed account_db_id=%d exception=%s",
            account_id,
            type(exc).__name__,
        )
        await edit_or_answer(
            callback.message,
            "❌ Что-то пошло не так. Попробуйте ещё раз позже.",
            reply_markup=back_to_main_keyboard(),
        )


@router.callback_query(
    MrktJobStates.awaiting_confirmation,
    F.data.startswith("mrktjob:cancel:"),
)
async def cancel_mrkt_job(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    nonce = callback.data.rsplit(":", maxsplit=1)[-1] if callback.data else ""
    await state.clear()
    if isinstance(callback.message, Message):
        text = (
            "Размещение отменено."
            if nonce == data.get("mrkt_confirm_nonce")
            else "Подтверждение устарело."
        )
        await edit_or_answer(
            callback.message, text, reply_markup=back_to_main_keyboard()
        )


@router.callback_query(
    MrktJobStates.awaiting_inventory_retry,
    F.data.startswith("mrktjob:refresh:"),
)
async def refresh_mrkt_inventory(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    parts = callback.data.split(":")
    data = await state.get_data()
    if len(parts) != 4 or parts[2] != data.get("mrkt_refresh_nonce"):
        await _stale(callback.message, state, "Кнопка обновления устарела.")
        return
    try:
        account_id = int(parts[3])
    except ValueError:
        account_id = -1
    account = await db.get_account(account_id, callback.from_user.id)
    if account is None:
        await _stale(callback.message, state, "Аккаунт больше недоступен.")
        return
    await _load_inventory(
        callback.message,
        callback.from_user.id,
        account,
        state,
        mrkt_service,
    )


@router.callback_query(
    MrktJobStates.awaiting_gift,
    F.data == "mrktjob:back_accounts",
)
@router.callback_query(
    MrktJobStates.awaiting_price,
    F.data == "mrktjob:back_accounts",
)
@router.callback_query(
    MrktJobStates.awaiting_inventory_retry,
    F.data == "mrktjob:back_accounts",
)
async def back_to_mrkt_accounts(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    await state.clear()
    accounts = await db.list_accounts(callback.from_user.id)
    if len(accounts) <= 1:
        await edit_or_answer(
            callback.message,
            "<b>🛒 MRKT</b>\n\nВыберите другое действие.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    await _show_account_choice(callback.message, accounts, state)


@router.callback_query(
    MrktJobStates.awaiting_price,
    F.data == "mrktjob:back_gifts",
)
async def back_to_mrkt_gifts(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    data = await state.get_data()
    gifts = data.get("mrkt_job_gifts")
    if not isinstance(gifts, list) or len(gifts) < 2:
        await _stale(callback.message, state, "Список подарков устарел.")
        return
    await _show_gift_choice(callback.message, state, gifts)


async def _send_inventory_error(
    message: Message, account_id: int, exc: Exception
) -> None:
    logger.warning(
        "MRKT inventory load failed account_db_id=%d exception=%s",
        account_id,
        type(exc).__name__,
    )
    if isinstance(
        exc,
        (
            MiniAppAccountNotFoundError,
            MiniAppSessionMissingError,
            MiniAppSessionUnauthorizedError,
        ),
    ):
        text = (
            "<b>⚠️ Авторизация аккаунта больше не действует</b>\n\n"
            "Подключите аккаунт заново."
        )
    elif isinstance(exc, MrktApiError) and exc.status == 429:
        text = "Слишком много запросов. Попробуйте немного позже."
    elif isinstance(exc, MrktNetworkError):
        text = "Сервис временно не отвечает. Попробуйте позже."
    elif isinstance(exc, (MrktApiError, MrktAuthenticationError)):
        text = "MRKT временно недоступен."
    else:
        text = "Что-то пошло не так. Попробуйте ещё раз."
    await edit_or_answer(message, text, reply_markup=back_to_main_keyboard())


async def _stale(message: Message, state: FSMContext, text: str) -> None:
    await state.clear()
    await edit_or_answer(
        message,
        f"{text} Начните заново.",
        reply_markup=back_to_main_keyboard(),
    )


def _account_label(account: Account) -> str:
    if account.username:
        return f"@{account.username}"
    if account.phone:
        normalized = "".join(
            character for character in account.phone if character.isdigit()
        )
        if len(normalized) >= 5:
            return f"+{normalized[:2]}••••{normalized[-3:]}"
    return account.first_name or "Telegram-аккаунт"
