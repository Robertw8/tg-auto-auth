from __future__ import annotations

import html
import logging
import secrets
import time

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import (
    account_label,
    back_to_main_keyboard,
    transfer_accounts_keyboard,
    transfer_assets_keyboard,
    transfer_confirmation_keyboard,
    transfer_input_keyboard,
)
from app.bot.states import TonnelTransferStates
from app.bot.ui import edit_or_answer
from app.db import Account, ActiveTransferConflictError, Database
from app.jobs import (
    TonnelBatchProgress,
    TonnelTransferBatchResult,
    TonnelTransferBatchService,
    TonnelTransferJobService,
)
from app.telegram_client import (
    TonnelGift,
    TonnelInventoryUnavailableError,
    TonnelMarketGift,
    TonnelNetworkError,
    TonnelService,
    TonnelTransferMode,
)

router = Router(name="tonnel_transfers")
logger = logging.getLogger(__name__)


def _inventory_error_text(mode: TonnelTransferMode) -> str:
    if mode in {TonnelTransferMode.MARKET_SALE, TonnelTransferMode.BUY_OFFER}:
        return "Не удалось загрузить инвентарь Tonnel."
    return "Прямая передача требует подключения Tonnel в Telegram Business."


def _empty_inventory_text(mode: TonnelTransferMode) -> str:
    if mode in {TonnelTransferMode.MARKET_SALE, TonnelTransferMode.BUY_OFFER}:
        return "Подарки не найдены. Убедитесь, что подарок передан в @GiftRelayer."
    return "Нет подарков, доступных для операции."


@router.callback_query(F.data == "tonnelx:start")
async def start_tonnel_transfer(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    tonnel_transfer_batches: TonnelTransferBatchService,
) -> None:
    if not isinstance(callback.message, Message):
        await callback.answer()
        return
    await state.clear()
    pair = await db.get_active_account_pair(callback.from_user.id)
    if pair.owner is None:
        await callback.answer()
        await edit_or_answer(
            callback.message,
            "<b>🚇 Tonnel</b>\n\n"
            "Настройте активный аккаунт N1 в разделе «Мои аккаунты».",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if pair.target is None:
        await callback.answer()
        await edit_or_answer(
            callback.message,
            "<b>🚇 Tonnel</b>\n\n"
            "Подключите или выберите активный рабочий аккаунт N2.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    message = callback.message

    async def progress(value: TonnelBatchProgress) -> None:
        await edit_or_answer(message, _tonnel_batch_progress_text(value))

    async def reserved(batch_id: int) -> None:
        del batch_id
        await callback.answer()
        await edit_or_answer(
            message,
            "<b>🚇 Tonnel</b>\n\n"
            "Загружаем все доступные подарки и проверяем баланс…",
        )

    try:
        result = await tonnel_transfer_batches.execute_batch(
            owner_telegram_id=callback.from_user.id,
            owner_account_id=pair.owner.id,
            target_account_id=pair.target.id,
            progress=progress,
            reserved=reserved,
            progress_chat_id=message.chat.id,
            progress_message_id=message.message_id,
        )
    except ActiveTransferConflictError:
        await callback.answer("Передача уже выполняется.")
        return
    except Exception as exc:  # noqa: BLE001 - sanitized bot boundary
        logger.warning("Tonnel batch failed exception=%s", type(exc).__name__)
        await edit_or_answer(
            message,
            "❌ Не удалось запустить передачу Tonnel. Попробуйте позже.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if not await db.claim_tonnel_batch_final_notification(result.batch_id):
        return
    await edit_or_answer(
        message,
        _tonnel_batch_result_text(result),
        reply_markup=back_to_main_keyboard(),
    )


def _tonnel_batch_progress_text(progress: TonnelBatchProgress) -> str:
    processed = (
        progress.success_count
        + progress.failed_count
        + progress.ambiguous_count
        + progress.dry_run_count
    )
    return (
        "<b>🚇 Tonnel</b>\n\n"
        f"Найдено подарков: {progress.total_count}\n"
        f"Обработано: {processed}/{progress.total_count}\n"
        f"Успешно: {progress.success_count}\n"
        f"Ошибок: {progress.failed_count}\n"
        f"Требуют проверки: {progress.ambiguous_count}"
    )


def _tonnel_batch_result_text(result: TonnelTransferBatchResult) -> str:
    progress = result.progress
    if result.error_code == "INSUFFICIENT_BATCH_BALANCE":
        return (
            "<b>❌ Недостаточно баланса N1 для передачи всех подарков.</b>\n\n"
            f"Подарков: {progress.total_count}\n"
            f"Текущий баланс: {_decimal_text(result.buyer_balance)} TON\n"
            f"Требуется: {_decimal_text(result.total_required)} TON"
        )
    if result.status == "EMPTY":
        return "<b>🚇 Tonnel</b>\n\nНет подарков, доступных для передачи."
    if result.status == "SUCCESS":
        title = "✅ Tonnel завершён"
    elif result.status == "DRY_RUN":
        title = "🧪 Тестовый запуск Tonnel завершён"
    elif result.status == "PARTIAL":
        title = "⚠️ Tonnel завершён частично"
    elif result.status == "AMBIGUOUS":
        title = "⚠️ Требуется проверка"
    else:
        title = "❌ Передача Tonnel не выполнена"
    lines = [
        f"<b>{title}</b>",
        "",
        f"Подарков: {progress.total_count}",
        f"Успешно: {progress.success_count}",
        f"Ошибок: {progress.failed_count}",
    ]
    if progress.dry_run_count:
        lines.append(f"Тестовый запуск: {progress.dry_run_count}")
    if progress.ambiguous_count:
        lines.append(f"Требуют проверки: {progress.ambiguous_count}")
    successful = [
        item.display_name
        for item in progress.items
        if item.status == "SUCCESS"
    ]
    dry_run = [item.display_name for item in progress.items if item.status == "DRY_RUN"]
    failed = [
        item
        for item in progress.items
        if item.status == "FAILED"
    ]
    ambiguous = [item for item in progress.items if item.status == "AMBIGUOUS"]
    if successful:
        lines.extend(["", "Успешные подарки:"])
        lines.extend(f"• {html.escape(name)}" for name in successful)
    if failed:
        lines.extend(["", "Ошибки:"])
        lines.extend(
            f"• {html.escape(item.display_name)} — "
            f"{html.escape(_batch_failure_reason(item.safe_reason))}"
            for item in failed
        )
    if dry_run:
        lines.extend(["", "Тестовый запуск:"])
        lines.extend(f"• {html.escape(name)}" for name in dry_run)
    if ambiguous:
        lines.extend(["", "Требуют проверки:"])
        lines.extend(
            f"• {html.escape(item.display_name)} — "
            f"{html.escape(item.safe_reason or 'проверьте операцию')}"
            for item in ambiguous
        )
    return "\n".join(lines)


def _batch_failure_reason(reason: str | None) -> str:
    if reason is None or reason.strip().lower() in {"success", "accepted", "created"}:
        return "операция не подтверждена"
    return reason


@router.callback_query(
    TonnelTransferStates.choosing_target, F.data.startswith("tonnelx:target:")
)
async def choose_tonnel_target(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    tonnel_service: TonnelService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    account = await _account_from_callback(callback, db, state, "TARGET")
    if account is None:
        return
    owners = await db.list_accounts_by_role(callback.from_user.id, "OWNER")
    await _after_target(
        callback.message,
        callback.from_user.id,
        account,
        owners,
        state,
        tonnel_service,
    )


async def _after_target(
    message: Message,
    user_id: int,
    target: Account,
    owners: list[Account],
    state: FSMContext,
    tonnel_service: TonnelService,
) -> None:
    await state.update_data(
        target_account_id=target.id, target_account_label=account_label(target)
    )
    if not owners:
        await state.clear()
        await edit_or_answer(
            message,
            "Мой аккаунт больше не доступен. Подключите его заново.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if len(owners) == 1:
        await state.update_data(
            owner_account_id=owners[0].id,
            owner_account_label=account_label(owners[0]),
        )
        await _show_gifts(message, user_id, state, tonnel_service)
        return
    await state.set_state(TonnelTransferStates.choosing_owner)
    nonce = str((await state.get_data())["transfer_nonce"])
    await edit_or_answer(
        message,
        "<b>🚇 Tonnel</b>\n\nВыберите мой аккаунт — получателя подарка.",
        reply_markup=transfer_accounts_keyboard(
            owners, market="tonnel", kind="owner", nonce=nonce
        ),
    )


@router.callback_query(
    TonnelTransferStates.choosing_owner, F.data.startswith("tonnelx:owner:")
)
async def choose_tonnel_owner(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    tonnel_service: TonnelService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    account = await _account_from_callback(callback, db, state, "OWNER")
    if account is None:
        return
    await state.update_data(
        owner_account_id=account.id, owner_account_label=account_label(account)
    )
    await _show_gifts(callback.message, callback.from_user.id, state, tonnel_service)


async def _show_gifts(
    message: Message,
    user_id: int,
    state: FSMContext,
    tonnel_service: TonnelService,
) -> None:
    data = await state.get_data()
    try:
        gifts = [
            gift
            for gift in await tonnel_service.get_transfer_inventory(
                int(data["target_account_id"]), owner_telegram_id=user_id
            )
            if gift.eligible
        ]
    except Exception as exc:  # noqa: BLE001 - sanitized user boundary
        await state.clear()
        reason = (
            exc.reason
            if isinstance(exc, TonnelInventoryUnavailableError)
            else "network_error"
            if isinstance(exc, TonnelNetworkError)
            else "unexpected_error"
        )
        logger.warning(
            "Tonnel inventory load failed account_db_id=%s exception=%s "
            "reason=%s http_status=%s",
            data.get("target_account_id", "unknown"),
            type(exc).__name__,
            reason,
            getattr(exc, "http_status", None),
        )
        await edit_or_answer(
            message,
            _inventory_error_text(tonnel_service.transfer_mode),
            reply_markup=back_to_main_keyboard(),
        )
        return
    if not gifts:
        await state.clear()
        await edit_or_answer(
            message,
            f"<b>🚇 Tonnel</b>\n\n{_empty_inventory_text(tonnel_service.transfer_mode)}",
            reply_markup=back_to_main_keyboard(),
        )
        return
    await state.update_data(transfer_assets=gifts)
    if len(gifts) == 1:
        await _after_gift_selected(message, gifts[0], state, tonnel_service)
        return
    await state.set_state(TonnelTransferStates.choosing_asset)
    await edit_or_answer(
        message,
        "<b>🚇 Tonnel</b>\n\nВыберите точный подарок.",
        reply_markup=transfer_assets_keyboard(
            gifts, market="tonnel", nonce=str(data["transfer_nonce"])
        ),
    )


@router.callback_query(
    TonnelTransferStates.choosing_asset, F.data.startswith("tonnelx:asset:")
)
async def choose_tonnel_gift(
    callback: CallbackQuery, state: FSMContext, tonnel_service: TonnelService
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    try:
        _, _, nonce, index_raw = callback.data.split(":", 3)
        index = int(index_raw)
        gifts = data["transfer_assets"]
        gift = gifts[index]
        if index < 0:
            raise ValueError("negative index")
    except (ValueError, IndexError, KeyError, TypeError):
        await _stale(callback.message, state)
        return
    if nonce != data.get("transfer_nonce") or not isinstance(
        gift, (TonnelGift, TonnelMarketGift)
    ):
        await _stale(callback.message, state)
        return
    await _after_gift_selected(callback.message, gift, state, tonnel_service)


async def _after_gift_selected(
    message: Message,
    gift: TonnelGift | TonnelMarketGift,
    state: FSMContext,
    tonnel_service: TonnelService,
) -> None:
    await state.update_data(
        asset_id=gift.gift_id,
        asset_name=gift.display_name,
        transfer_assets=None,
    )
    if tonnel_service.transfer_mode in {
        TonnelTransferMode.MARKET_SALE,
        TonnelTransferMode.BUY_OFFER,
    }:
        await state.set_state(TonnelTransferStates.entering_amount)
        amount_name = (
            "сумму прямого оффера"
            if tonnel_service.transfer_mode is TonnelTransferMode.BUY_OFFER
            else "цену продавца"
        )
        await edit_or_answer(
            message,
            f"<b>🚇 Tonnel</b>\n\nПодарок: {html.escape(gift.display_name)}\n\n"
            f"Введите {amount_name} в TON, например <code>5</code> или "
            "<code>3.52</code>.",
            reply_markup=transfer_input_keyboard(market="tonnel"),
        )
        return
    assert isinstance(gift, TonnelGift)
    await _show_direct_review(message, gift, state)


@router.message(TonnelTransferStates.entering_amount)
async def enter_tonnel_price(
    message: Message,
    state: FSMContext,
    tonnel_service: TonnelService,
    db: Database,
    tonnel_transfer_jobs: TonnelTransferJobService,
) -> None:
    if message.from_user is None or not message.text:
        return
    try:
        price = tonnel_service.format_market_price(message.text)
    except (ValueError, ArithmeticError):
        value_name = (
            "сумму"
            if tonnel_transfer_jobs.transfer_mode is TonnelTransferMode.BUY_OFFER
            else "цену"
        )
        await message.answer(
            f"Введите {value_name} от 0.5 до 20000 TON, "
            "не более трёх знаков после точки."
        )
        return
    data = await state.get_data()
    owner = await db.get_account(int(data["owner_account_id"]), message.from_user.id)
    target = await db.get_account(int(data["target_account_id"]), message.from_user.id)
    if owner is None or target is None:
        await state.clear()
        await message.answer(
            "Один из аккаунтов больше не доступен.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    is_buy_offer = (
        tonnel_transfer_jobs.transfer_mode is TonnelTransferMode.BUY_OFFER
    )
    nonce = secrets.token_hex(8)
    await state.update_data(
        confirm_nonce=nonce,
        amount=price,
        confirm_expires_at=time.monotonic() + 300,
    )
    await state.set_state(TonnelTransferStates.confirming)
    if is_buy_offer:
        offer_quote = tonnel_service.buy_offer_quote(price)
        review = (
            "<b>🚇 Tonnel — Прямой оффер</b>\n\n"
            f"Источник N2: {html.escape(account_label(target))}\n"
            f"Получатель N1: {html.escape(account_label(owner))}\n"
            f"Подарок: {html.escape(str(data['asset_name']))}\n"
            f"Сумма оффера: {html.escape(price)} TON\n"
            f"N2 получит: "
            f"{html.escape(_decimal_text(offer_quote.seller_proceeds))} TON\n"
            f"Комиссия с выплаты N2: "
            f"{html.escape(_decimal_text(offer_quote.seller_fee))} TON (0.5%)\n"
            f"Комиссия создания: "
            f"{html.escape(_decimal_text(offer_quote.create_fee))} TON"
            + (
                "\n\n🧪 Тестовый режим: оффер не создаётся и не принимается."
                if tonnel_transfer_jobs.dry_run
                else "\n\nПубличное размещение подарка не создаётся."
            )
        )
    else:
        market_quote = tonnel_service.market_quote(price)
        review = (
            "<b>🚇 Tonnel</b>\n\n"
            f"Продавец: {html.escape(account_label(target))}\n"
            f"Покупатель: {html.escape(account_label(owner))}\n"
            f"Подарок: {html.escape(str(data['asset_name']))}\n"
            f"Цена продавца: {html.escape(price)} TON\n"
            f"Максимальная сумма покупателя: "
            f"{html.escape(_decimal_text(market_quote.buyer_price))} TON\n"
            f"Комиссия покупателя: до "
            f"{html.escape(_decimal_text(market_quote.fee_rate * 100))}%"
            + (
                "\n\n🧪 Тестовый режим: размещение и покупка не выполняются."
                if tonnel_transfer_jobs.dry_run
                else "\n\nРазмещение будет публичным до завершения покупки."
            )
        )
    await message.answer(
        review,
        reply_markup=transfer_confirmation_keyboard(market="tonnel", nonce=nonce),
    )


async def _show_direct_review(
    message: Message, gift: TonnelGift, state: FSMContext
) -> None:
    data = await state.get_data()
    nonce = secrets.token_hex(8)
    await state.update_data(
        asset_id=gift.gift_id,
        asset_name=gift.display_name,
        transfer_assets=None,
        confirm_nonce=nonce,
        confirm_expires_at=time.monotonic() + 300,
    )
    await state.set_state(TonnelTransferStates.confirming)
    await edit_or_answer(
        message,
        "<b>🚇 Tonnel</b>\n\n"
        f"Источник: {html.escape(str(data['target_account_label']))}\n"
        f"Получатель: {html.escape(str(data['owner_account_label']))}\n"
        f"Подарок: {html.escape(gift.display_name)}\n\n"
        "Режим Telegram Business: подарок будет отправлен напрямую выбранному "
        "получателю без публичного размещения.",
        reply_markup=transfer_confirmation_keyboard(market="tonnel", nonce=nonce),
    )


@router.callback_query(
    TonnelTransferStates.confirming, F.data.startswith("tonnelx:confirm:")
)
async def confirm_tonnel_transfer(
    callback: CallbackQuery,
    state: FSMContext,
    tonnel_transfer_jobs: TonnelTransferJobService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", 1)[-1]
    stored_nonce = data.get("confirm_nonce")
    valid = (
        isinstance(stored_nonce, str)
        and secrets.compare_digest(nonce, stored_nonce)
        and time.monotonic() < data.get("confirm_expires_at", 0)
    )
    if not valid:
        await _stale(callback.message, state)
        return
    message = callback.message
    await state.clear()

    async def progress(text: str) -> None:
        await edit_or_answer(message, f"<b>🚇 Tonnel</b>\n\n{text}")

    job = await tonnel_transfer_jobs.execute_tonnel_transfer(
        callback.from_user.id,
        int(data["owner_account_id"]),
        int(data["target_account_id"]),
        data["asset_id"],
        str(data.get("amount", "")),
        confirmation_nonce=nonce,
        progress=progress,
    )
    if job.status == "SUCCESS":
        text = "<b>✅ Подарок передан</b>\n\nПолучателем стал выбранный мой аккаунт."
    elif job.status == "DRY_RUN":
        text = (
            "<b>✅ Тестовый запуск завершён</b>\n\n"
            + (
                "Оффер не создавался и не принимался."
                if tonnel_transfer_jobs.transfer_mode
                is TonnelTransferMode.BUY_OFFER
                else (
                    "Размещение и покупка не выполнялись."
                    if tonnel_transfer_jobs.transfer_mode
                    is TonnelTransferMode.MARKET_SALE
                    else "Подарок не передавался."
                )
            )
        )
    elif job.status == "AMBIGUOUS":
        text = "<b>⚠️ Требуется проверка</b>\n\n" + html.escape(
            job.error_message or "Проверьте владельца подарка вручную."
        )
    else:
        text = "<b>❌ Передача не выполнена</b>\n\n" + html.escape(
            job.error_message or "Повторите операцию позже."
        )
    await edit_or_answer(
        message,
        text,
        reply_markup=back_to_main_keyboard(),
    )


def _decimal_text(value: object) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


async def _account_from_callback(
    callback: CallbackQuery, db: Database, state: FSMContext, role: str
) -> Account | None:
    if callback.data is None or not isinstance(callback.message, Message):
        return None
    data = await state.get_data()
    try:
        _, _, nonce, account_raw = callback.data.split(":", 3)
        account_id = int(account_raw)
    except (ValueError, IndexError):
        await _stale(callback.message, state)
        return None
    account = await db.get_account(account_id, callback.from_user.id)
    if nonce != data.get("transfer_nonce") or account is None or account.role != role:
        await _stale(callback.message, state)
        return None
    return account


async def _stale(message: Message, state: FSMContext) -> None:
    await state.clear()
    await edit_or_answer(
        message,
        "Это действие устарело. Начните операцию заново.",
        reply_markup=back_to_main_keyboard(),
    )
