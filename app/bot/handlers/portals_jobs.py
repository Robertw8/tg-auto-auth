from __future__ import annotations

import html
import logging
import secrets

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import (
    back_to_main_keyboard,
    portals_confirmation_keyboard,
    portals_empty_keyboard,
    portals_job_account_keyboard,
    portals_offers_keyboard,
)
from app.bot.states import AccountStates, PortalsJobStates
from app.bot.ui import edit_or_answer
from app.db import Account, Database
from app.jobs import PortalsJobService
from app.telegram_client import (
    MiniAppAccountNotFoundError,
    MiniAppSessionMissingError,
    MiniAppSessionUnauthorizedError,
    PortalsApiError,
    PortalsAuthenticationError,
    PortalsNetworkError,
    PortalsOffer,
    PortalsService,
)

router = Router(name="portals-jobs")
logger = logging.getLogger(__name__)
MAX_OFFER_BUTTONS = 20


@router.message(Command("portals_job"))
async def start_portals_job_command(
    message: Message,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    if message.from_user is not None:
        await _start_portals(message, message.from_user.id, db, state, portals_service)


@router.callback_query(F.data == "portalsjob:start")
async def start_portals_job_callback(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    if isinstance(callback.message, Message):
        await _start_portals(
            callback.message,
            callback.from_user.id,
            db,
            state,
            portals_service,
        )


async def _start_portals(
    message: Message,
    owner_id: int,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await state.clear()
    accounts = await db.list_accounts(owner_id)
    if not accounts:
        await edit_or_answer(
            message,
            "<b>🤝 Portals</b>\n\nСначала подключите Telegram-аккаунт.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if len(accounts) == 1:
        await _load_offers(message, owner_id, accounts[0], state, portals_service)
        return
    await _show_account_choice(message, accounts, state)


async def _show_account_choice(
    message: Message, accounts: list[Account], state: FSMContext
) -> None:
    nonce = secrets.token_hex(8)
    await state.clear()
    await state.update_data(portals_account_nonce=nonce)
    await state.set_state(PortalsJobStates.awaiting_account)
    await edit_or_answer(
        message,
        "<b>🤝 Portals</b>\n\n"
        "Просмотр и принятие полученных офферов.\n\n"
        "Выберите аккаунт:",
        reply_markup=portals_job_account_keyboard(accounts, nonce),
    )


@router.callback_query(
    PortalsJobStates.awaiting_account,
    F.data.startswith("portalsjob:account:"),
)
async def select_portals_account(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    await _select_account_from_callback(
        callback,
        db,
        state,
        portals_service,
        expected_nonce_key="portals_account_nonce",
    )


@router.callback_query(
    AccountStates.viewing,
    F.data.startswith("acct:portals:"),
)
async def select_portals_from_accounts(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    await _select_account_from_callback(
        callback,
        db,
        state,
        portals_service,
        expected_nonce_key="account_list_nonce",
    )


async def _select_account_from_callback(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
    *,
    expected_nonce_key: str,
) -> None:
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    parts = callback.data.split(":")
    data = await state.get_data()
    if len(parts) != 4 or parts[2] != data.get(expected_nonce_key):
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
    await _load_offers(
        callback.message,
        callback.from_user.id,
        account,
        state,
        portals_service,
    )


async def _load_offers(
    message: Message,
    owner_id: int,
    account: Account,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await state.clear()
    await edit_or_answer(message, "🔄 Загружаем офферы Portals…")
    try:
        offers = await portals_service.get_received_offers(
            account.id,
            owner_telegram_id=owner_id,
        )
    except Exception as exc:  # noqa: BLE001 - sanitized UI boundary
        await state.clear()
        await _send_offers_error(message, account.id, exc)
        return
    eligible = [offer for offer in offers if offer.eligible]
    await state.update_data(
        portals_account_id=account.id,
        portals_account_label=_account_label(account),
        portals_offers=eligible[:MAX_OFFER_BUTTONS],
    )
    if not eligible:
        nonce = secrets.token_hex(8)
        await state.update_data(portals_refresh_nonce=nonce)
        await state.set_state(PortalsJobStates.awaiting_offers_retry)
        await edit_or_answer(
            message,
            "<b>🤝 Portals</b>\n\nНовых офферов нет.",
            reply_markup=portals_empty_keyboard(nonce, account.id),
        )
        return
    if len(eligible) == 1:
        await _show_review(
            message,
            state,
            eligible[0],
            portals_service.dry_run,
            back_to_offers=False,
        )
        return
    await _show_offer_choice(message, state, eligible[:MAX_OFFER_BUTTONS])


async def _show_offer_choice(
    message: Message, state: FSMContext, offers: list[PortalsOffer]
) -> None:
    nonce = secrets.token_hex(8)
    await state.update_data(portals_offer_nonce=nonce, portals_offers=offers)
    await state.set_state(PortalsJobStates.awaiting_offer)
    suffix = "\n\nПоказаны первые 20." if len(offers) >= MAX_OFFER_BUTTONS else ""
    await edit_or_answer(
        message,
        "<b>🤝 Portals</b>\n\nВыберите оффер:" + suffix,
        reply_markup=portals_offers_keyboard(offers, nonce),
    )


@router.callback_query(
    PortalsJobStates.awaiting_offer,
    F.data.startswith("portalsjob:offer:"),
)
async def select_portals_offer(
    callback: CallbackQuery,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    parts = callback.data.split(":")
    data = await state.get_data()
    if len(parts) != 4 or parts[2] != data.get("portals_offer_nonce"):
        await _stale(callback.message, state, "Выбор оффера устарел.")
        return
    try:
        index = int(parts[3])
    except ValueError:
        index = -1
    offers = data.get("portals_offers")
    if not isinstance(offers, list) or not 0 <= index < len(offers):
        await _stale(callback.message, state, "Не удалось выбрать оффер.")
        return
    offer = offers[index]
    if not isinstance(offer, PortalsOffer) or not offer.eligible:
        await _stale(callback.message, state, "Этот оффер больше недоступен.")
        return
    await _show_review(
        callback.message,
        state,
        offer,
        portals_service.dry_run,
        back_to_offers=True,
    )


async def _show_review(
    message: Message,
    state: FSMContext,
    offer: PortalsOffer,
    dry_run: bool,
    *,
    back_to_offers: bool,
) -> None:
    nonce = secrets.token_hex(8)
    await state.update_data(
        portals_selected_offer=offer,
        portals_confirm_nonce=nonce,
        portals_back_to_offers=back_to_offers,
    )
    await state.set_state(PortalsJobStates.awaiting_confirmation)
    data = await state.get_data()
    account_name = data.get("portals_account_label")
    if not isinstance(account_name, str):
        await _stale(message, state, "Выбор Portals устарел.")
        return
    await edit_or_answer(
        message,
        _portals_review_text(account_name, offer, dry_run=dry_run),
        reply_markup=portals_confirmation_keyboard(nonce),
    )


def _portals_review_text(
    account_name: str, offer: PortalsOffer, *, dry_run: bool = False
) -> str:
    mode_note = (
        "\n\n<b>🧪 Тестовый режим</b>\nОффер не будет принят." if dry_run else ""
    )
    return (
        "<b>Проверьте оффер</b>\n\n"
        f"Аккаунт: {html.escape(account_name)}\n"
        f"NFT: {html.escape(offer.display_name)}\n"
        f"Сумма: <b>{html.escape(offer.amount_text)} TON</b>\n\n"
        "После подтверждения оффер будет принят."
        f"{mode_note}"
    )


@router.callback_query(
    PortalsJobStates.awaiting_confirmation,
    F.data.startswith("portalsjob:confirm:"),
)
async def confirm_portals_offer(
    callback: CallbackQuery,
    state: FSMContext,
    portals_jobs: PortalsJobService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", maxsplit=1)[-1]
    account_id = data.get("portals_account_id")
    offer = data.get("portals_selected_offer")
    expected_nonce = data.get("portals_confirm_nonce")
    await state.clear()
    if (
        nonce != expected_nonce
        or not isinstance(account_id, int)
        or not isinstance(offer, PortalsOffer)
    ):
        await edit_or_answer(
            callback.message,
            "Подтверждение устарело. Начните заново.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    await edit_or_answer(
        callback.message,
        "🔄 Принимаем оффер…\n\nНе отправляйте действие повторно.",
    )
    try:
        await portals_jobs.execute_portals_job(
            owner_telegram_id=callback.from_user.id,
            account_id=account_id,
            offer_id=offer.offer_id,
        )
    except Exception as exc:  # noqa: BLE001 - isolate one user's job
        logger.warning(
            "Portals job dispatch failed account_db_id=%d exception=%s",
            account_id,
            type(exc).__name__,
        )
        await edit_or_answer(
            callback.message,
            "❌ Что-то пошло не так. Попробуйте ещё раз позже.",
            reply_markup=back_to_main_keyboard(),
        )


@router.callback_query(
    PortalsJobStates.awaiting_confirmation,
    F.data.startswith("portalsjob:cancel:"),
)
async def cancel_portals_offer(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    nonce = callback.data.rsplit(":", maxsplit=1)[-1] if callback.data else ""
    await state.clear()
    if isinstance(callback.message, Message):
        text = (
            "Принятие оффера отменено."
            if nonce == data.get("portals_confirm_nonce")
            else "Подтверждение устарело."
        )
        await edit_or_answer(
            callback.message, text, reply_markup=back_to_main_keyboard()
        )


@router.callback_query(
    PortalsJobStates.awaiting_confirmation,
    F.data.startswith("portalsjob:back:"),
)
async def back_from_portals_review(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", maxsplit=1)[-1]
    if nonce != data.get("portals_confirm_nonce"):
        await _stale(callback.message, state, "Подтверждение устарело.")
        return
    offers = data.get("portals_offers")
    if data.get("portals_back_to_offers") and isinstance(offers, list):
        await _show_offer_choice(callback.message, state, offers)
        return
    await state.clear()
    await edit_or_answer(
        callback.message,
        "<b>🤝 Portals</b>\n\nВыберите другое действие.",
        reply_markup=back_to_main_keyboard(),
    )


@router.callback_query(
    PortalsJobStates.awaiting_offers_retry,
    F.data.startswith("portalsjob:refresh:"),
)
async def refresh_portals_offers(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    parts = callback.data.split(":")
    data = await state.get_data()
    if len(parts) != 4 or parts[2] != data.get("portals_refresh_nonce"):
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
    await _load_offers(
        callback.message,
        callback.from_user.id,
        account,
        state,
        portals_service,
    )


@router.callback_query(
    PortalsJobStates.awaiting_offer,
    F.data == "portalsjob:back_accounts",
)
@router.callback_query(
    PortalsJobStates.awaiting_offers_retry,
    F.data == "portalsjob:back_accounts",
)
async def back_to_portals_accounts(
    callback: CallbackQuery, db: Database, state: FSMContext
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    await state.clear()
    accounts = await db.list_accounts(callback.from_user.id)
    if len(accounts) <= 1:
        await edit_or_answer(
            callback.message,
            "<b>🤝 Portals</b>\n\nВыберите другое действие.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    await _show_account_choice(callback.message, accounts, state)


async def _send_offers_error(message: Message, account_id: int, exc: Exception) -> None:
    logger.warning(
        "Portals bot offer load failed account_db_id=%d exception=%s",
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
    elif isinstance(exc, PortalsApiError) and exc.status == 429:
        text = "Слишком много запросов. Попробуйте немного позже."
    elif isinstance(exc, PortalsNetworkError):
        text = "Сервис временно не отвечает. Попробуйте позже."
    elif isinstance(exc, (PortalsApiError, PortalsAuthenticationError)):
        text = "Portals временно недоступен."
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
