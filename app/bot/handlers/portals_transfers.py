from __future__ import annotations

import asyncio
import html
import logging
import secrets

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import (
    account_label,
    back_to_main_keyboard,
    portals_batch_assets_keyboard,
    transfer_accounts_keyboard,
)
from app.bot.states import PortalsTransferStates
from app.bot.ui import edit_or_answer
from app.db import Account, ActiveTransferConflictError, Database, TransferJob
from app.jobs import (
    PortalsBatchProgress,
    PortalsTransferBatchService,
)
from app.telegram_client import PortalsNft, PortalsService

router = Router(name="portals_transfers")
logger = logging.getLogger(__name__)

@router.callback_query(F.data == "portalsx:start")
async def start_portals_transfer(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
    portals_transfer_batches: PortalsTransferBatchService,
) -> None:
    logger.info("PORTALS_FLOW start")
    await callback.answer()
    if not isinstance(callback.message, Message):
        logger.info("PORTALS_FLOW stopped reason=missing_message")
        return
    await state.clear()
    pair = await db.get_active_account_pair(callback.from_user.id)
    if pair.owner is None:
        logger.info("PORTALS_FLOW stopped reason=owner_account_missing")
        await edit_or_answer(
            callback.message,
            "<b>🌀 Portals</b>\n\n"
            "Настройте активный аккаунт N1 в разделе «Мои аккаунты».",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if pair.target is None:
        logger.info("PORTALS_FLOW stopped reason=target_account_missing")
        await edit_or_answer(
            callback.message,
            "<b>🌀 Portals</b>\n\n"
            "Подключите или выберите активный рабочий аккаунт N2.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    owner = pair.owner
    target = pair.target
    try:
        nfts, active_ids = await asyncio.gather(
            portals_service.get_owned_nfts(
                target.id, owner_telegram_id=callback.from_user.id
            ),
            db.list_active_transfer_asset_ids(
                owner_telegram_id=callback.from_user.id,
                market="portals",
                owner_account_id=owner.id,
                target_account_id=target.id,
            ),
        )
    except Exception as exc:  # noqa: BLE001 - sanitized UI boundary
        logger.warning(
            "PORTALS_FLOW stopped reason=inventory_load_failed exception=%s",
            type(exc).__name__,
        )
        await edit_or_answer(
            callback.message,
            "<b>🌀 Portals</b>\n\nНе удалось загрузить подарки. Попробуйте позже.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    active = {(type(value), value) for value in active_ids}
    eligible = [
        nft
        for nft in nfts
        if nft.eligible and (type(nft.nft_id), nft.nft_id) not in active
    ]
    if not eligible:
        await edit_or_answer(
            callback.message,
            "<b>🌀 Portals</b>\n\nНет подарков, доступных для передачи.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    message = callback.message

    async def progress(value: PortalsBatchProgress) -> None:
        await edit_or_answer(message, _batch_progress_text(value))

    await edit_or_answer(
        message,
        _batch_progress_text(
            PortalsBatchProgress(
                total_count=len(eligible),
                success_count=0,
                failed_count=0,
                ambiguous_count=0,
                running_count=0,
                queued_count=len(eligible),
                items=(),
            )
        ),
    )
    nonce = secrets.token_urlsafe(24)
    try:
        batch = await portals_transfer_batches.execute_batch(
            owner_telegram_id=callback.from_user.id,
            owner_account_id=owner.id,
            target_account_id=target.id,
            nfts=eligible,
            confirmation_nonce=nonce,
            progress=progress,
        )
    except ActiveTransferConflictError:
        await edit_or_answer(
            message,
            "Передача уже выполняется.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    except Exception as exc:  # noqa: BLE001 - safe bot boundary
        logger.warning("PORTALS_BATCH stopped exception=%s", type(exc).__name__)
        await edit_or_answer(
            message,
            "❌ Не удалось запустить передачу. Попробуйте позже.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    finally:
        del nonce
    children = await db.list_transfer_jobs_by_batch(batch.id)
    await edit_or_answer(
        message,
        _batch_result_text(batch.status, children),
        reply_markup=back_to_main_keyboard(),
    )


@router.callback_query(
    PortalsTransferStates.choosing_target, F.data.startswith("portalsx:target:")
)
async def choose_portals_target(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
    portals_transfer_batches: PortalsTransferBatchService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    target = await _account_from_callback(callback, db, state, "TARGET")
    if target is None:
        return
    owners = await db.list_accounts_by_role(callback.from_user.id, "OWNER")
    await _after_target(
        callback.message,
        callback.from_user.id,
        target,
        owners,
        state,
        portals_service,
        db,
        portals_transfer_batches,
    )


async def _after_target(
    message: Message,
    user_id: int,
    target: Account,
    owners: list[Account],
    state: FSMContext,
    portals_service: PortalsService,
    db: Database,
    portals_transfer_batches: PortalsTransferBatchService,
) -> None:
    await state.update_data(target_account_id=target.id)
    logger.info("PORTALS_FLOW target_selected")
    if not owners:
        logger.info("PORTALS_FLOW stopped reason=owner_account_missing")
        await state.clear()
        await edit_or_answer(
            message,
            "Мой аккаунт больше не доступен. Подключите его заново.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if len(owners) == 1:
        await state.update_data(owner_account_id=owners[0].id)
        logger.info("PORTALS_FLOW owner_selected")
        await _show_nfts(
            message, user_id, state, portals_service, db, portals_transfer_batches
        )
        return
    await state.set_state(PortalsTransferStates.choosing_owner)
    nonce = str((await state.get_data())["transfer_nonce"])
    await edit_or_answer(
        message,
        "<b>🤝 Portals</b>\n\nВыберите ваш аккаунт для отправки оффера.",
        reply_markup=transfer_accounts_keyboard(
            owners, market="portals", kind="owner", nonce=nonce
        ),
    )


@router.callback_query(
    PortalsTransferStates.choosing_owner, F.data.startswith("portalsx:owner:")
)
async def choose_portals_owner(
    callback: CallbackQuery,
    db: Database,
    state: FSMContext,
    portals_service: PortalsService,
    portals_transfer_batches: PortalsTransferBatchService,
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    owner = await _account_from_callback(callback, db, state, "OWNER")
    if owner is None:
        return
    await state.update_data(owner_account_id=owner.id)
    logger.info("PORTALS_FLOW owner_selected")
    await _show_nfts(
        callback.message,
        callback.from_user.id,
        state,
        portals_service,
        db,
        portals_transfer_batches,
    )


async def _show_nfts(
    message: Message,
    user_id: int,
    state: FSMContext,
    portals_service: PortalsService,
    db: Database,
    portals_transfer_batches: PortalsTransferBatchService,
) -> None:
    data = await state.get_data()
    try:
        nfts = [
            nft
            for nft in await portals_service.get_owned_nfts(
                int(data["target_account_id"]), owner_telegram_id=user_id
            )
            if nft.eligible
        ]
    except Exception:  # noqa: BLE001 - sanitized UI boundary
        logger.info("PORTALS_FLOW stopped reason=nft_load_failed")
        await state.clear()
        await edit_or_answer(
            message,
            "Не удалось загрузить NFT. Проверьте авторизацию аккаунта и повторите позже.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    if not nfts:
        logger.info("PORTALS_FLOW stopped reason=empty_nft_inventory")
        await state.clear()
        await edit_or_answer(
            message,
            "<b>🤝 Portals</b>\n\nВ рабочем аккаунте нет NFT, доступных для оффера.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    await state.update_data(transfer_assets=nfts, selected_asset_indices=[])
    await state.set_state(PortalsTransferStates.choosing_asset)
    owner, target = await asyncio.gather(
        db.get_account(int(data["owner_account_id"]), user_id),
        db.get_account(int(data["target_account_id"]), user_id),
    )
    if owner is None or target is None:
        await _stale(message, state, reason="account_unavailable")
        return
    await edit_or_answer(
        message,
        _selection_text(owner, target),
        reply_markup=portals_batch_assets_keyboard(
            nfts, selected=set(), nonce=str(data["transfer_nonce"])
        ),
    )


@router.callback_query(
    PortalsTransferStates.choosing_asset, F.data.startswith("portalsx:toggle:")
)
async def toggle_portals_nft(callback: CallbackQuery, state: FSMContext) -> None:
    if not isinstance(callback.message, Message) or callback.data is None:
        await callback.answer()
        return
    data = await state.get_data()
    try:
        _, _, nonce, index_raw = callback.data.split(":", 3)
        index = int(index_raw)
        assets = data["transfer_assets"]
        if nonce != data.get("transfer_nonce") or not 0 <= index < len(assets):
            raise ValueError("Stale NFT selection")
        if not isinstance(assets[index], PortalsNft):
            raise TypeError("Invalid NFT selection")
        selected = _selected_indices(data, len(assets))
    except (ValueError, KeyError, TypeError):
        await callback.answer("Эта кнопка устарела.", show_alert=True)
        await _stale(callback.message, state)
        return
    if index in selected:
        selected.remove(index)
    else:
        selected.add(index)
    await state.update_data(selected_asset_indices=sorted(selected))
    await callback.answer()
    await callback.message.edit_reply_markup(
        reply_markup=portals_batch_assets_keyboard(
            assets, selected=selected, nonce=nonce
        )
    )


@router.callback_query(
    PortalsTransferStates.choosing_asset,
    F.data.startswith("portalsx:select_all:"),
)
async def select_all_portals_nfts(
    callback: CallbackQuery, state: FSMContext
) -> None:
    await _set_all_selection(callback, state, select_all=True)


@router.callback_query(
    PortalsTransferStates.choosing_asset, F.data.startswith("portalsx:clear:")
)
async def clear_portals_nfts(callback: CallbackQuery, state: FSMContext) -> None:
    await _set_all_selection(callback, state, select_all=False)


async def _set_all_selection(
    callback: CallbackQuery, state: FSMContext, *, select_all: bool
) -> None:
    if not isinstance(callback.message, Message) or callback.data is None:
        await callback.answer()
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", 1)[-1]
    assets = data.get("transfer_assets")
    if nonce != data.get("transfer_nonce") or not isinstance(assets, list):
        await callback.answer("Эта кнопка устарела.", show_alert=True)
        await _stale(callback.message, state)
        return
    selected = set(range(len(assets))) if select_all else set()
    await state.update_data(selected_asset_indices=sorted(selected))
    await callback.answer()
    await callback.message.edit_reply_markup(
        reply_markup=portals_batch_assets_keyboard(
            assets, selected=selected, nonce=nonce
        )
    )


@router.callback_query(
    PortalsTransferStates.choosing_asset, F.data.startswith("portalsx:launch:")
)
async def launch_portals_batch(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
    portals_transfer_batches: PortalsTransferBatchService,
) -> None:
    if not isinstance(callback.message, Message) or callback.data is None:
        await callback.answer()
        return
    data = await state.get_data()
    nonce = callback.data.rsplit(":", 1)[-1]
    assets = data.get("transfer_assets")
    try:
        if nonce != data.get("transfer_nonce") or not isinstance(assets, list):
            raise ValueError("Stale batch confirmation")
        selected_indices = _selected_indices(data, len(assets))
        selected = [assets[index] for index in sorted(selected_indices)]
        if not selected:
            await callback.answer(
                "Выберите хотя бы один подарок.", show_alert=True
            )
            return
        if not all(isinstance(nft, PortalsNft) for nft in selected):
            raise TypeError("Invalid NFT selection")
        owner_account_id = int(data["owner_account_id"])
        target_account_id = int(data["target_account_id"])
    except (ValueError, KeyError, TypeError):
        await callback.answer("Эта кнопка устарела.", show_alert=True)
        await _stale(callback.message, state)
        return
    await callback.answer()
    await state.clear()
    message = callback.message

    async def progress(value: PortalsBatchProgress) -> None:
        await edit_or_answer(message, _batch_progress_text(value))

    await edit_or_answer(
        message,
        _batch_progress_text(
            PortalsBatchProgress(
                total_count=len(selected),
                success_count=0,
                failed_count=0,
                ambiguous_count=0,
                running_count=0,
                queued_count=len(selected),
                items=(),
            )
        ),
    )
    try:
        batch = await portals_transfer_batches.execute_batch(
            owner_telegram_id=callback.from_user.id,
            owner_account_id=owner_account_id,
            target_account_id=target_account_id,
            nfts=selected,
            confirmation_nonce=nonce,
            progress=progress,
        )
    except ActiveTransferConflictError:
        await edit_or_answer(
            message,
            "⚠️ Один из выбранных подарков уже участвует в незавершённой операции.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    except Exception as exc:  # noqa: BLE001 - safe bot boundary
        logger.warning(
            "PORTALS_BATCH stopped exception=%s", type(exc).__name__
        )
        await edit_or_answer(
            message,
            "❌ Не удалось запустить передачу. Попробуйте позже.",
            reply_markup=back_to_main_keyboard(),
        )
        return
    children = await db.list_transfer_jobs_by_batch(batch.id)
    await edit_or_answer(
        message,
        _batch_result_text(batch.status, children),
        reply_markup=back_to_main_keyboard(),
    )


def _selection_text(owner: Account, target: Account) -> str:
    return (
        "<b>🤝 Portals</b>\n\n"
        "Рабочий аккаунт:\n"
        f"{html.escape(account_label(target))}\n\n"
        "Мой аккаунт:\n"
        f"{html.escape(account_label(owner))}\n\n"
        "Выберите подарки:"
    )


def _selected_indices(data: dict[str, object], asset_count: int) -> set[int]:
    raw = data.get("selected_asset_indices", [])
    if not isinstance(raw, list) or any(
        not isinstance(index, int)
        or isinstance(index, bool)
        or not 0 <= index < asset_count
        for index in raw
    ):
        raise ValueError("Invalid stored selection")
    return set(raw)


def _batch_progress_text(progress: PortalsBatchProgress) -> str:
    completed = (
        progress.success_count + progress.failed_count + progress.ambiguous_count
    )
    lines = [
        "<b>🤝 Передача подарков Portals</b>",
        "",
        f"Всего: {progress.total_count}",
        f"✅ Готово: {completed}",
        f"⏳ Выполняется: {progress.running_count}",
        f"🕓 В очереди: {progress.queued_count}",
        f"❌ Ошибок: {progress.failed_count}",
    ]
    if progress.ambiguous_count:
        lines.append(f"⚠️ Требуют проверки: {progress.ambiguous_count}")
    if progress.items:
        icons = {
            "QUEUED": "🕓",
            "RUNNING": "⏳",
            "SUCCESS": "✅",
            "DRY_RUN": "🧪",
            "FAILED": "❌",
            "AMBIGUOUS": "⚠️",
        }
        lines.extend(
            [""]
            + [
                f"{icons.get(item.status, '•')} {html.escape(item.display_name)}"
                for item in progress.items
            ]
        )
    return "\n".join(lines)


def _batch_result_text(status: str, children: list[TransferJob]) -> str:
    success = sum(job.status == "SUCCESS" for job in children)
    dry_run = sum(job.status == "DRY_RUN" for job in children)
    failed = sum(job.status == "FAILED" for job in children)
    ambiguous = sum(job.status == "AMBIGUOUS" for job in children)
    if status == "SUCCESS":
        title = "✅ Передача завершена"
    elif status == "DRY_RUN":
        title = "🧪 Тестовый запуск завершён"
    elif status == "PARTIAL":
        title = "⚠️ Передача завершена частично"
    elif status == "AMBIGUOUS":
        title = "⚠️ Требуется проверка"
    else:
        title = "❌ Передача не выполнена"
    lines = [
        f"<b>{title}</b>",
        "",
        f"Всего: {len(children)}",
        f"Успешно: {success}",
        f"Ошибок: {failed}",
    ]
    if ambiguous:
        lines.append(f"Требуют проверки: {ambiguous}")
    if dry_run:
        lines.append(f"Тестовых запусков: {dry_run}")
    icons = {
        "SUCCESS": "✅",
        "DRY_RUN": "🧪",
        "FAILED": "❌",
        "AMBIGUOUS": "⚠️",
    }
    lines.extend(
        [""]
        + [
            f"{html.escape(job.display_name or 'Подарок')} — "
            f"{icons.get(job.status, '•')}"
            for job in children
        ]
    )
    return "\n".join(lines)


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


async def _stale(
    message: Message, state: FSMContext, *, reason: str = "stale_action"
) -> None:
    logger.info("PORTALS_FLOW stopped reason=%s", reason)
    await state.clear()
    await edit_or_answer(
        message,
        "Это действие устарело. Начните операцию заново.",
        reply_markup=back_to_main_keyboard(),
    )
