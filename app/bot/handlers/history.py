from __future__ import annotations

import html
import math
import secrets
from datetime import UTC, datetime

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import history_keyboard
from app.bot.states import HistoryStates
from app.bot.ui import edit_or_answer
from app.db import Account, Database, OperationHistoryItem

router = Router(name="history")
PAGE_SIZE = 10

STATUS_LABELS = {
    "QUEUED": "В очереди",
    "PENDING": "Ожидает",
    "RUNNING": "Выполняется",
    "SUCCESS": "Успешно",
    "FAILED": "Ошибка",
    "AMBIGUOUS": "Требуется проверка",
    "DRY_RUN": "Тестовый запуск",
}
STATUS_ICONS = {
    "QUEUED": "🕓",
    "PENDING": "⏳",
    "RUNNING": "🔄",
    "SUCCESS": "✅",
    "FAILED": "❌",
    "AMBIGUOUS": "⚠️",
    "DRY_RUN": "🧪",
}


def _history_text(items: list[OperationHistoryItem], accounts: list[Account]) -> str:
    if not items:
        return "<b>📜 История операций</b>\n\nОпераций пока нет."
    account_labels = {account.id: _account_label(account) for account in accounts}
    blocks = ["<b>📜 История операций</b>"]
    for item in items:
        if item.market.startswith("mrkt"):
            market = "MRKT"
        elif item.market.startswith("tonnel"):
            market = "Tonnel"
        else:
            market = "Portals"
        label = account_labels.get(item.account_id, "Удалённый аккаунт")
        target = item.display_name or (
            "Подарок"
            if item.market.startswith(("mrkt", "tonnel"))
            else "NFT"
        )
        value = f"{item.value_text} TON" if item.value_text else "—"
        status = STATUS_LABELS.get(item.status, "Неизвестно")
        icon = STATUS_ICONS.get(item.status, "•")
        blocks.append(
            "\n"
            f"<b>{icon} {status} · {market}</b>\n"
            f"{html.escape(target)}\n"
            f"{html.escape(value)}\n"
            f"👤 {html.escape(label)}\n"
            f"{html.escape(_format_history_time(item.timestamp))}"
        )
    return "\n".join(blocks)


def _format_history_time(value: str, *, now: datetime | None = None) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace(" ", "T"))
    except ValueError:
        return value[:19]
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    parsed = parsed.astimezone(UTC)
    current = current.astimezone(UTC)
    if parsed.date() == current.date():
        return f"Сегодня, {parsed:%H:%M} UTC"
    if (current.date() - parsed.date()).days == 1:
        return f"Вчера, {parsed:%H:%M} UTC"
    return f"{parsed:%d.%m.%Y, %H:%M} UTC"


@router.callback_query(F.data == "history:list")
async def show_history(
    callback: CallbackQuery, db: Database, state: FSMContext
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message):
        return
    nonce = secrets.token_hex(8)
    await state.clear()
    await state.update_data(history_nonce=nonce)
    await state.set_state(HistoryStates.viewing)
    await _show_history_page(
        callback.message,
        callback.from_user.id,
        db,
        page=0,
        nonce=nonce,
    )


@router.callback_query(
    HistoryStates.viewing,
    F.data.startswith("history:page:"),
)
async def show_history_page(
    callback: CallbackQuery, db: Database, state: FSMContext
) -> None:
    await callback.answer()
    if not isinstance(callback.message, Message) or callback.data is None:
        return
    parts = callback.data.split(":")
    data = await state.get_data()
    if len(parts) != 4 or parts[2] != data.get("history_nonce"):
        await state.clear()
        await edit_or_answer(
            callback.message,
            "История обновилась. Откройте её заново.",
        )
        return
    try:
        page = max(0, int(parts[3]))
    except ValueError:
        page = 0
    await _show_history_page(
        callback.message,
        callback.from_user.id,
        db,
        page=page,
        nonce=parts[2],
    )


@router.callback_query(F.data == "history:noop")
async def history_noop(callback: CallbackQuery) -> None:
    await callback.answer()


async def _show_history_page(
    message: Message,
    owner_id: int,
    db: Database,
    *,
    page: int,
    nonce: str,
) -> None:
    total = await db.count_operation_history(owner_id)
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    safe_page = min(page, total_pages - 1)
    items = await db.list_operation_history(
        owner_id, limit=PAGE_SIZE, offset=safe_page * PAGE_SIZE
    )
    accounts = await db.list_accounts(owner_id)
    await edit_or_answer(
        message,
        _history_text(items, accounts),
        reply_markup=history_keyboard(
            nonce=nonce,
            page=safe_page,
            total_pages=total_pages,
            has_previous=safe_page > 0,
            has_next=safe_page + 1 < total_pages,
        ),
    )


def _account_label(account: Account) -> str:
    if account.username:
        return f"@{account.username}"
    if account.phone:
        digits = "".join(
            character for character in account.phone if character.isdigit()
        )
        if len(digits) >= 5:
            return f"+{digits[:2]}••••{digits[-3:]}"
    return account.first_name or "Telegram-аккаунт"
