from __future__ import annotations

import html
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
from app.bot.states import MrktTransferStates
from app.bot.ui import edit_or_answer
from app.db import Account, Database
from app.jobs import MrktTransferJobService
from app.live_verify import russian_summary
from app.telegram_client import MrktGift, MrktPriceError, MrktService

router = Router(name="mrkt_transfers")

_FAILED_PHASES = {
    "VALIDATING": "проверка аккаунтов и подарка",
    "LISTING": "размещение",
    "LOCATING_LISTING": "поиск размещения",
    "BUYING": "покупка",
}


@router.callback_query(F.data == "mrktx:start")
async def start_mrkt_transfer(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    await state.clear()
    targets = await db.list_accounts_by_role(callback.from_user.id, "TARGET")
    owners = await db.list_accounts_by_role(callback.from_user.id, "OWNER")
    if not owners:
        await edit_or_answer(
            callback.message,
            "<b>🛍 MRKT</b>\n\nСначала добавьте аккаунт в разделе «Мои аккаунты».",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if not targets:
        await edit_or_answer(
            callback.message,
            "<b>🛍 MRKT</b>\n\nСначала добавьте рабочий аккаунт.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    nonce = secrets.token_hex(8)
    await state.update_data(transfer_nonce=nonce)
    if len(targets) == 1:
        await _after_target(
            callback.message,
            callback.from_user.id,
            targets[0],
            owners,
            state,
            mrkt_service,
        )
        return
    await state.set_state(MrktTransferStates.choosing_target)
    await edit_or_answer(
        callback.message,
        "<b>🛍 MRKT</b>\n\nВыберите рабочий аккаунт — продавца подарка.",
        reply_markup=transfer_accounts_keyboard(
            targets, market="mrkt", kind="target", nonce=nonce
        ),
    )


@router.callback_query(
    MrktTransferStates.choosing_target, F.data.startswith("mrktx:target:")
)
async def choose_mrkt_target(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
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
        mrkt_service,
    )


async def _after_target(
    message: Message,
    user_id: int,
    target: Account,
    owners: list[Account],
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await state.update_data(target_account_id=target.id)
    if not owners:
        await state.clear()
        await edit_or_answer(
            message,
            "Мой аккаунт больше не доступен. Подключите его заново.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if len(owners) == 1:
        await state.update_data(owner_account_id=owners[0].id)
        await _show_gifts(message, user_id, state, mrkt_service)
        return
    await state.set_state(MrktTransferStates.choosing_owner)
    nonce = str((await state.get_data())["transfer_nonce"])
    await edit_or_answer(
        message,
        "<b>🛍 MRKT</b>\n\nВыберите мой аккаунт — покупателя подарка.",
        reply_markup=transfer_accounts_keyboard(
            owners, market="mrkt", kind="owner", nonce=nonce
        ),
    )


@router.callback_query(
    MrktTransferStates.choosing_owner, F.data.startswith("mrktx:owner:")
)
async def choose_mrkt_owner(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    account = await _account_from_callback(callback, db, state, "OWNER")
    if account is None:
        return
    await state.update_data(owner_account_id=account.id)
    await _show_gifts(callback.message, callback.from_user.id, state, mrkt_service)


async def _show_gifts(
    message: Message,
    user_id: int,
    state: FSMContext,
    mrkt_service: MrktService,
) -> None:
    data = await state.get_data()
    target_id = int(data["target_account_id"])
    try:
        gifts = [
            gift
            for gift in await mrkt_service.get_inventory(
                target_id, owner_telegram_id=user_id, is_listed=False
            )
            if gift.eligible
        ]
    except Exception:  # noqa: BLE001 - user receives a sanitized message
        await state.clear()
        await edit_or_answer(
            message,
            "Не удалось загрузить подарки. Проверьте авторизацию аккаунта и повторите позже.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if not gifts:
        await state.clear()
        await edit_or_answer(
            message,
            "<b>🛍 MRKT</b>\n\nВ рабочем аккаунте нет подарков, доступных для размещения.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    await state.update_data(transfer_assets=gifts)
    if len(gifts) == 1:
        await _ask_mrkt_price(message, gifts[0], state)
        return
    await state.set_state(MrktTransferStates.choosing_asset)
    nonce = str(data["transfer_nonce"])
    await edit_or_answer(
        message,
        "<b>🛍 MRKT</b>\n\nВыберите подарок.",
        reply_markup=transfer_assets_keyboard(gifts, market="mrkt", nonce=nonce),
    )


@router.callback_query(
    MrktTransferStates.choosing_asset, F.data.startswith("mrktx:asset:")
)
async def choose_mrkt_gift(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    try:
        _, _, nonce, index_raw = callback.data.split(":", 3)
        index = int(index_raw)
        if index < 0:
            raise ValueError("Negative selection index")
        gifts = data["transfer_assets"]
        gift = gifts[index]
    except (ValueError, IndexError, KeyError, TypeError):
        await _stale(callback.message, state)
        return
    if nonce != data.get("transfer_nonce") or not isinstance(gift, MrktGift):
        await _stale(callback.message, state)
        return
    await _ask_mrkt_price(callback.message, gift, state)


async def _ask_mrkt_price(message: Message, gift: MrktGift, state: FSMContext) -> None:
    await state.update_data(
        asset_id=gift.gift_id,
        asset_name=gift.display_name,
        transfer_assets=None,
    )
    await state.set_state(MrktTransferStates.entering_amount)
    await edit_or_answer(
        message,
        f"<b>🛍 MRKT</b>\n\nПодарок: {html.escape(gift.display_name)}\n\n"
        "Введите точную цену в TON, например <code>1.68</code>.",
        reply_markup=transfer_input_keyboard(market="mrkt"),
    )


@router.message(MrktTransferStates.entering_amount)
async def enter_mrkt_price(
    message: Message,
    state: FSMContext,
    mrkt_service: MrktService,
    db: Database,
    mrkt_transfer_jobs: MrktTransferJobService,
) -> None:
    if message.from_user is None or not message.text:
        return
    try:
        mrkt_service.ton_to_nanotons(message.text)
        price = mrkt_service.format_ton(message.text)
    except (MrktPriceError, ValueError, ArithmeticError):
        await message.answer(
            "Введите положительную цену в TON, не более двух знаков после точки."
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
    nonce = secrets.token_hex(8)
    await state.update_data(
        confirm_nonce=nonce, amount=price, confirm_expires_at=time.monotonic() + 300
    )
    await state.set_state(MrktTransferStates.confirming)
    await message.answer(
        "<b>🛍 MRKT</b>\n\n"
        f"Продавец: {html.escape(account_label(target))}\n"
        f"Покупатель: {html.escape(account_label(owner))}\n"
        f"Подарок: {html.escape(str(data['asset_name']))}\n"
        f"Цена: {html.escape(price)} TON"
        + (
            "\n\n🧪 Тестовый режим: размещение и покупка не выполняются."
            if mrkt_transfer_jobs.dry_run
            else ""
        ),
        reply_markup=transfer_confirmation_keyboard(market="mrkt", nonce=nonce),
    )


@router.callback_query(
    MrktTransferStates.confirming, F.data.startswith("mrktx:confirm:")
)
async def confirm_mrkt_transfer(
    callback: CallbackQuery,
    state: FSMContext,
    mrkt_transfer_jobs: MrktTransferJobService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", 1)[-1]
    stored_nonce = data.get("confirm_nonce")
    nonce_valid = (
        isinstance(stored_nonce, str)
        and secrets.compare_digest(nonce, stored_nonce)
        and time.monotonic() < data.get("confirm_expires_at", 0)
    )
    if not nonce_valid:
        await _stale(callback.message, state)
        return
    message = callback.message
    await state.clear()

    async def progress(text: str) -> None:
        await edit_or_answer(message, f"<b>🛍 MRKT</b>\n\n{text}")

    job = await mrkt_transfer_jobs.execute_mrkt_transfer(
        callback.from_user.id,
        int(data["owner_account_id"]),
        int(data["target_account_id"]),
        data["asset_id"],
        str(data["amount"]),
        confirmation_nonce=nonce,
        progress=progress,
    )
    if job.status == "SUCCESS":
        text = "<b>✅ Операция завершена</b>\n\nПодарок выставлен рабочим аккаунтом\nи куплен вашим аккаунтом."
    elif job.status == "DRY_RUN":
        text = (
            "<b>✅ Тестовый запуск завершён</b>\n\nРазмещение и покупка не выполнялись."
        )
    elif job.status == "AMBIGUOUS":
        text = "<b>⚠️ Требуется проверка</b>\n\n" + html.escape(
            job.error_message or "Проверьте MRKT вручную."
        )
    elif job.error_code == "GIFT_INTERCEPTED_OR_SOLD_EXTERNALLY":
        text = (
            "<b>❌ Подарок успел уйти другому покупателю.</b>\n\n"
            "Размещение было создано, но наш аккаунт не успел выполнить покупку."
        )
    elif job.error_code == "SPECULATIVE_BUY_TOO_EARLY":
        text = (
            "<b>❌ Покупка началась слишком рано.</b>\n\n"
            "Подарок остался у рабочего аккаунта или в размещении. "
            "Автоматическая повторная покупка не выполнялась."
        )
    else:
        phase = _FAILED_PHASES.get(job.phase, "выполнение операции")
        text = f"<b>❌ Ошибка: {phase}</b>\n\n" + html.escape(
            job.error_message or "Операция не выполнена."
        )
    await edit_or_answer(
        message,
        text + russian_summary(job.result_metadata),
        reply_markup=back_to_main_keyboard(),
    )


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
