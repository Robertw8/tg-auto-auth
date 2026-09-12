from __future__ import annotations

import hashlib
import html
import logging
import secrets
import time
from datetime import UTC, datetime

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import (
    account_label,
    account_sections_keyboard,
    back_to_main_keyboard,
    portals_cancel_confirmation_keyboard,
    portals_cancel_result_keyboard,
    portals_placed_account_keyboard,
    portals_placed_offers_keyboard,
)
from app.bot.states import PortalsOfferManagementStates
from app.bot.ui import edit_or_answer
from app.db import Account, Database
from app.telegram_client import (
    MiniAppAccountNotFoundError,
    MiniAppSessionMissingError,
    MiniAppSessionUnauthorizedError,
    PortalsApiError,
    PortalsAuthenticationError,
    PortalsCancelStatus,
    PortalsNetworkError,
    PortalsOffer,
    PortalsService,
)

router = Router(name="portals-offer-management")
logger = logging.getLogger(__name__)
MAX_PLACED_OFFERS = 20
CANCEL_CONFIRM_TIMEOUT_SECONDS = 5 * 60


@router.callback_query(F.data == "portalsoffers:start")
async def start_portals_offers(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    await _start(
        callback.message,
        callback.from_user.id,
        db,
        state,
        portals_service,
    )


async def _start(
    message: Message,
    owner_id: int,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await state.clear()
    accounts = await db.list_accounts_by_role(owner_id, "OWNER")
    if not accounts:
        await edit_or_answer(
            message,
            "<b>📨 Мои офферы</b>\n\nСначала подключите ваш аккаунт.",
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
    await state.update_data(portals_placed_account_nonce=nonce)
    await state.set_state(PortalsOfferManagementStates.choosing_account)
    await edit_or_answer(
        message,
        "<b>📨 Мои офферы</b>\n\nВыберите ваш аккаунт:",
        reply_markup=portals_placed_account_keyboard(accounts, nonce),
    )


@router.callback_query(
    PortalsOfferManagementStates.choosing_account,
    F.data.startswith("portalsoffers:account:"),
)
async def select_portals_offers_account(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    try:
        _, _, nonce, account_raw = callback.data.split(":", 3)
        account_id = int(account_raw)
    except (ValueError, IndexError):
        await _stale(callback.message, state)
        return
    account = await db.get_account(account_id, callback.from_user.id)
    if (
        nonce != data.get("portals_placed_account_nonce")
        or account is None
        or account.role != "OWNER"
    ):
        await _stale(callback.message, state)
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
    await edit_or_answer(message, "🔄 Загружаем активные офферы…")
    try:
        offers = await portals_service.get_placed_offers(
            account.id, owner_telegram_id=owner_id
        )
    except Exception as exc:  # noqa: BLE001 - sanitized UI boundary
        await state.clear()
        await _send_error(message, account.id, exc)
        return
    visible = offers[:MAX_PLACED_OFFERS]
    nonce = secrets.token_hex(8)
    await state.clear()
    await state.update_data(
        portals_placed_account_id=account.id,
        portals_placed_account_label=account_label(account),
        portals_placed_offers=visible,
        portals_placed_list_nonce=nonce,
    )
    await state.set_state(PortalsOfferManagementStates.viewing_offers)
    await edit_or_answer(
        message,
        _placed_offers_text(visible),
        reply_markup=portals_placed_offers_keyboard(visible, nonce),
    )


@router.callback_query(
    PortalsOfferManagementStates.viewing_offers,
    F.data.startswith("portalsoffers:open:"),
)
async def open_placed_offer(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    try:
        _, _, nonce, index_raw = callback.data.split(":", 3)
        index = int(index_raw)
        if index < 0:
            raise ValueError("negative index")
        offer = data["portals_placed_offers"][index]
    except (ValueError, IndexError, KeyError, TypeError):
        await _stale(callback.message, state)
        return
    if nonce != data.get("portals_placed_list_nonce") or not isinstance(
        offer, PortalsOffer
    ):
        await _stale(callback.message, state)
        return
    confirm_nonce = secrets.token_hex(8)
    await state.update_data(
        portals_cancel_offer=offer,
        portals_cancel_nonce=confirm_nonce,
        portals_cancel_expires_at=time.monotonic() + CANCEL_CONFIRM_TIMEOUT_SECONDS,
    )
    await state.set_state(PortalsOfferManagementStates.confirming_cancel)
    await edit_or_answer(
        callback.message,
        _placed_offer_detail_text(offer),
        reply_markup=portals_cancel_confirmation_keyboard(confirm_nonce),
    )


@router.callback_query(
    PortalsOfferManagementStates.viewing_offers,
    F.data.startswith("portalsoffers:refresh:"),
)
async def refresh_placed_offers(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", 1)[-1]
    account_id = data.get("portals_placed_account_id")
    if nonce != data.get("portals_placed_list_nonce") or not isinstance(
        account_id, int
    ):
        await _stale(callback.message, state)
        return
    account = await db.get_account(account_id, callback.from_user.id)
    if account is None or account.role != "OWNER":
        await _stale(callback.message, state)
        return
    await _load_offers(
        callback.message,
        callback.from_user.id,
        account,
        state,
        portals_service,
    )


@router.callback_query(
    PortalsOfferManagementStates.viewing_offers,
    F.data == "portalsoffers:accounts",
)
async def back_from_placed_offers(
    callback: CallbackQuery, db: Database, state: FSMContext
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    accounts = await db.list_accounts_by_role(callback.from_user.id, "OWNER")
    if len(accounts) > 1:
        await _show_account_choice(callback.message, accounts, state)
        return
    await state.clear()
    await edit_or_answer(
        callback.message,
        "<b>Аккаунты</b>\n\nВыберите раздел:",
        reply_markup=account_sections_keyboard(),
    )


@router.callback_query(
    PortalsOfferManagementStates.confirming_cancel,
    F.data.startswith("portalsoffers:back:"),
)
async def back_from_cancel_confirmation(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", 1)[-1]
    account_id = data.get("portals_placed_account_id")
    if nonce != data.get("portals_cancel_nonce") or not isinstance(account_id, int):
        await _stale(callback.message, state)
        return
    account = await db.get_account(account_id, callback.from_user.id)
    if account is None or account.role != "OWNER":
        await _stale(callback.message, state)
        return
    await _load_offers(
        callback.message,
        callback.from_user.id,
        account,
        state,
        portals_service,
    )


@router.callback_query(
    PortalsOfferManagementStates.confirming_cancel,
    F.data.startswith("portalsoffers:cancel:"),
)
async def confirm_cancel_placed_offer(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", 1)[-1]
    expected_nonce = data.get("portals_cancel_nonce")
    expires_at = data.get("portals_cancel_expires_at")
    account_id = data.get("portals_placed_account_id")
    offer = data.get("portals_cancel_offer")
    valid = (
        isinstance(expected_nonce, str)
        and secrets.compare_digest(nonce, expected_nonce)
        and isinstance(expires_at, (int, float))
        and time.monotonic() < expires_at
        and isinstance(account_id, int)
        and not isinstance(account_id, bool)
        and isinstance(offer, PortalsOffer)
    )
    await state.clear()
    if not valid:
        await _stale(callback.message, state)
        return
    assert isinstance(account_id, int)
    assert isinstance(offer, PortalsOffer)
    account = await db.get_account(account_id, callback.from_user.id)
    if account is None or account.role != "OWNER":
        await _stale(callback.message, state)
        return

    await edit_or_answer(
        callback.message,
        "🔄 Отзываем оффер…\n\nНе отправляйте действие повторно.",
    )
    try:
        result = await portals_service.cancel_offer(
            owner_telegram_id=callback.from_user.id,
            account_id=account.id,
            offer_id=offer.offer_id,
            nft_id=offer.nft_id,
            amount=offer.amount,
        )
    except Exception as exc:  # noqa: BLE001 - sanitized UI boundary
        logger.warning(
            "Portals offer cancellation failed account_db_id=%d exception=%s",
            account.id,
            type(exc).__name__,
        )
        await edit_or_answer(
            callback.message,
            "<b>❌ Не удалось отозвать оффер.</b>",
            reply_markup=portals_cancel_result_keyboard(),
        )
        return

    await edit_or_answer(
        callback.message,
        _cancel_result_text(result.status, offer),
        reply_markup=portals_cancel_result_keyboard(),
    )


def _placed_offers_text(offers: list[PortalsOffer]) -> str:
    if not offers:
        return "<b>📨 Активные офферы</b>\n\nАктивных офферов нет."
    lines = ["<b>📨 Активные офферы</b>"]
    for index, offer in enumerate(offers, start=1):
        lines.append(
            "\n"
            f"<b>{index}. {html.escape(offer.display_name)}</b>\n"
            f"{html.escape(offer.amount_text)} TON\n"
            f"Статус: {_status_text(offer.status)}"
            + (
                f"\nСоздан: {html.escape(_created_at_text(offer.created_at))}"
                if offer.created_at
                else ""
            )
            + f"\nНомер: #{_offer_reference(offer.offer_id)}"
        )
    if len(offers) >= MAX_PLACED_OFFERS:
        lines.append("\nПоказаны первые 20 активных офферов.")
    return "\n".join(lines)


def _placed_offer_detail_text(offer: PortalsOffer) -> str:
    return (
        "<b>📨 Оффер Portals</b>\n\n"
        f"NFT: {html.escape(offer.display_name)}\n"
        f"Сумма: {html.escape(offer.amount_text)} TON\n"
        f"Статус: {_status_text(offer.status)}\n\n"
        "Отозвать этот оффер?"
    )


def _cancel_result_text(status: PortalsCancelStatus, offer: PortalsOffer) -> str:
    if status is PortalsCancelStatus.SUCCESS:
        return (
            "<b>✅ Оффер отозван</b>\n\n"
            f"NFT: {html.escape(offer.display_name)}\n"
            f"Сумма: {html.escape(offer.amount_text)} TON"
        )
    if status is PortalsCancelStatus.ALREADY_INACTIVE:
        return "<b>ℹ️ Оффер уже не активен.</b>"
    return (
        "<b>⚠️ Требуется проверка</b>\n\n"
        "Не удалось точно подтвердить результат.\n"
        "Проверьте список офферов перед повторной попыткой."
    )


def _offer_reference(offer_id: object) -> str:
    value = f"{type(offer_id).__name__}:{offer_id}"
    return hashlib.sha256(value.encode()).hexdigest()[:8]


def _created_at_text(value: str | None) -> str:
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return "—"
    return parsed.astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")


def _status_text(value: str | None) -> str:
    statuses = {"pending": "ожидает", "active": "активен"}
    return statuses.get(value.lower() if isinstance(value, str) else "", "активен")


async def _send_error(message: Message, account_id: int, exc: Exception) -> None:
    logger.warning(
        "Portals placed-offer load failed account_db_id=%d exception=%s",
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
        text = "Авторизация аккаунта больше не действует. Подключите его заново."
    elif isinstance(exc, PortalsApiError) and exc.status == 429:
        text = "Слишком много запросов. Попробуйте немного позже."
    elif isinstance(exc, PortalsNetworkError):
        text = "Portals временно не отвечает. Попробуйте позже."
    elif isinstance(exc, (PortalsApiError, PortalsAuthenticationError)):
        text = "Portals временно недоступен."
    else:
        text = "Не удалось загрузить активные офферы."
    await edit_or_answer(message, text, reply_markup=back_to_main_keyboard())


async def _stale(message: Message, state: FSMContext) -> None:
    await state.clear()
    await edit_or_answer(
        message,
        "Это действие устарело. Откройте список офферов заново.",
        reply_markup=back_to_main_keyboard(),
    )
