from __future__ import annotations

from collections.abc import Sequence

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.bot.keyboards.accounts import account_label
from app.db import Account


def transfer_accounts_keyboard(
    accounts: list[Account], *, market: str, kind: str, nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.button(
            text=account_label(account),
            callback_data=f"{market}x:{kind}:{nonce}:{account.id}",
        )
    builder.button(text="⬅️ Назад", callback_data="nav:main")
    builder.adjust(1)
    return builder.as_markup()


def transfer_assets_keyboard(
    assets: Sequence[object], *, market: str, nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, asset in enumerate(assets):
        label = str(getattr(asset, "display_name", "Объект"))[:60]
        builder.button(
            text=label,
            callback_data=f"{market}x:asset:{nonce}:{index}",
        )
    builder.button(text="⬅️ Назад", callback_data=f"{market}x:start")
    builder.button(text="Отмена", callback_data="nav:main")
    builder.adjust(1)
    return builder.as_markup()


def portals_batch_assets_keyboard(
    assets: Sequence[object], *, selected: set[int], nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, asset in enumerate(assets):
        label = str(getattr(asset, "display_name", "Подарок"))[:54]
        marker = "✅" if index in selected else "☐"
        builder.button(
            text=f"{marker} {label}",
            callback_data=f"portalsx:toggle:{nonce}:{index}",
        )
    builder.button(
        text="✅ Выбрать все", callback_data=f"portalsx:select_all:{nonce}"
    )
    builder.button(
        text="❌ Снять выбор", callback_data=f"portalsx:clear:{nonce}"
    )
    count = len(selected)
    builder.button(
        text=f"🚀 Передать {count} {gift_word(count)}",
        callback_data=f"portalsx:launch:{nonce}",
    )
    builder.button(text="⬅️ Назад", callback_data="portalsx:start")
    builder.button(text="Отмена", callback_data="nav:main")
    builder.adjust(*([1] * len(assets)), 2, 1, 2)
    return builder.as_markup()


def gift_word(count: int) -> str:
    remainder_100 = count % 100
    remainder_10 = count % 10
    if 11 <= remainder_100 <= 14:
        return "подарков"
    if remainder_10 == 1:
        return "подарок"
    if 2 <= remainder_10 <= 4:
        return "подарка"
    return "подарков"


def transfer_input_keyboard(*, market: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{market}x:start")],
            [InlineKeyboardButton(text="Отмена", callback_data="nav:main")],
        ]
    )


def transfer_confirmation_keyboard(*, market: str, nonce: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="▶️ Запустить", callback_data=f"{market}x:confirm:{nonce}"
                )
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="nav:main")],
        ]
    )
