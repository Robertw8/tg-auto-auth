from __future__ import annotations

from collections.abc import Mapping, Sequence

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.db import Account


def main_menu_keyboard(*, has_accounts: bool = True) -> InlineKeyboardMarkup:
    account_rows = [
        [
            InlineKeyboardButton(
                text="👤 Мои аккаунты", callback_data="account:list_role:OWNER"
            ),
            InlineKeyboardButton(
                text="💼 Рабочие аккаунты", callback_data="account:list_role:TARGET"
            ),
        ]
    ]
    if not has_accounts:
        account_rows.insert(
            0,
            [
                InlineKeyboardButton(
                    text="➕ Добавить аккаунт", callback_data="account:add"
                )
            ],
        )
    return InlineKeyboardMarkup(
        inline_keyboard=account_rows
        + [
            [
                InlineKeyboardButton(text="🌀 Portals", callback_data="portalsx:start"),
                InlineKeyboardButton(text="🚇 Tonnel", callback_data="tonnelx:start"),
            ],
            [
                InlineKeyboardButton(text="📜 История", callback_data="history:list"),
                InlineKeyboardButton(text="❓ Помощь", callback_data="nav:help"),
            ],
        ]
    )


def back_to_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")]
        ]
    )


def add_account_intro_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Мой аккаунт", callback_data="auth:role:OWNER"
                ),
                InlineKeyboardButton(
                    text="💼 Рабочий аккаунт", callback_data="auth:role:TARGET"
                ),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:main")],
        ]
    )


def account_role_continue_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Продолжить", callback_data="auth:continue")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="account:add")],
            [InlineKeyboardButton(text="Отмена", callback_data="nav:main")],
        ]
    )


def account_sections_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Мои аккаунты", callback_data="account:list_role:OWNER"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💼 Рабочие аккаунты",
                    callback_data="account:list_role:TARGET",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📨 Мои офферы", callback_data="portalsoffers:start"
                )
            ],
            [
                InlineKeyboardButton(
                    text="➕ Добавить аккаунт", callback_data="account:add"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )


def authorization_keyboard(webapp_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔐 Открыть авторизацию",
                    web_app=WebAppInfo(url=webapp_url),
                )
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="auth:cancel")],
        ]
    )


def account_connected_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🌀 Запустить Portals", callback_data="portalsx:start"
                ),
                InlineKeyboardButton(
                    text="🚇 Запустить Tonnel", callback_data="tonnelx:start"
                ),
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )


def mask_phone(phone: str | None) -> str:
    if not phone:
        return "—"
    normalized = "".join(character for character in phone if character.isdigit())
    if len(normalized) < 5:
        return "••••"
    prefix = "+" if phone.strip().startswith("+") else ""
    return f"{prefix}{normalized[:2]}••••{normalized[-3:]}"


def account_label(account: Account) -> str:
    if account.username:
        return f"@{account.username}"
    if account.phone:
        return mask_phone(account.phone)
    if account.first_name:
        return account.first_name[:40]
    return "Подключённый аккаунт"


def accounts_keyboard(
    accounts: list[Account],
    nonce: str,
    session_status: Mapping[int, bool] | None = None,
    active_account_id: int | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        connected = session_status is None or session_status.get(account.id, False)
        label = account_label(account)[:22]
        if connected:
            builder.row(
                InlineKeyboardButton(
                    text=(
                        f"✅ Активен · {label}"
                        if account.id == active_account_id
                        else f"Выбрать · {label}"
                    ),
                    callback_data=f"account:activate:{nonce}:{account.id}",
                )
            )
            builder.row(
                InlineKeyboardButton(
                    text=f"🗑 Удалить · {label}",
                    callback_data=f"account:remove:{nonce}:{account.id}",
                ),
            )
        else:
            builder.row(
                InlineKeyboardButton(
                    text=f"🔄 Подключить заново · {label}",
                    callback_data="account:add",
                ),
                InlineKeyboardButton(
                    text=f"🗑 Удалить · {label}",
                    callback_data=f"account:remove:{nonce}:{account.id}",
                ),
            )
    builder.row(
        InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="account:add")
    )
    builder.row(InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main"))
    return builder.as_markup()


def account_remove_confirmation_keyboard(
    nonce: str, account_id: int
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить",
                    callback_data=f"account:remove_confirm:{nonce}:{account_id}",
                )
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="nav:main")],
        ]
    )


def mrkt_job_account_keyboard(
    accounts: list[Account], nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.button(
            text=account_label(account),
            callback_data=f"mrktjob:account:{nonce}:{account.id}",
        )
    builder.button(text="⬅️ Назад", callback_data="nav:main")
    builder.adjust(1)
    return builder.as_markup()


def mrkt_job_confirmation_keyboard(nonce: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Разместить",
                    callback_data=f"mrktjob:confirm:{nonce}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Изменить цену",
                    callback_data=f"mrktjob:edit_price:{nonce}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"mrktjob:cancel:{nonce}",
                )
            ],
        ]
    )


def mrkt_job_gifts_keyboard(
    gifts: Sequence[object], nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, gift in enumerate(gifts):
        label = getattr(gift, "display_name", "Подарок Telegram")
        builder.button(
            text=str(label)[:60],
            callback_data=f"mrktjob:gift:{nonce}:{index}",
        )
    builder.button(text="⬅️ Назад", callback_data="mrktjob:back_accounts")
    builder.button(text="🏠 Главное меню", callback_data="nav:main")
    builder.adjust(*([1] * (len(gifts) + 2)))
    return builder.as_markup()


def mrkt_price_keyboard(*, back_to_gifts: bool) -> InlineKeyboardMarkup:
    back_callback = "mrktjob:back_gifts" if back_to_gifts else "mrktjob:back_accounts"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)],
            [InlineKeyboardButton(text="Отмена", callback_data="nav:main")],
        ]
    )


def mrkt_empty_keyboard(nonce: str, account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Обновить",
                    callback_data=f"mrktjob:refresh:{nonce}:{account_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад", callback_data="mrktjob:back_accounts"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )


def portals_job_account_keyboard(
    accounts: list[Account], nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.button(
            text=account_label(account),
            callback_data=f"portalsjob:account:{nonce}:{account.id}",
        )
    builder.button(text="⬅️ Назад", callback_data="nav:main")
    builder.adjust(1)
    return builder.as_markup()


def portals_offers_keyboard(
    offers: Sequence[object], nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, offer in enumerate(offers):
        name = str(getattr(offer, "display_name", "NFT"))
        amount = str(getattr(offer, "amount_text", "—"))
        builder.button(
            text=f"{name[:35]} — {amount[:18]} TON",
            callback_data=f"portalsjob:offer:{nonce}:{index}",
        )
    builder.button(text="⬅️ Назад", callback_data="portalsjob:back_accounts")
    builder.button(text="🏠 Главное меню", callback_data="nav:main")
    builder.adjust(*([1] * (len(offers) + 2)))
    return builder.as_markup()


def portals_confirmation_keyboard(nonce: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Принять оффер",
                    callback_data=f"portalsjob:confirm:{nonce}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data=f"portalsjob:back:{nonce}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"portalsjob:cancel:{nonce}",
                )
            ],
        ]
    )


def portals_empty_keyboard(nonce: str, account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Обновить",
                    callback_data=f"portalsjob:refresh:{nonce}:{account_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад", callback_data="portalsjob:back_accounts"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )


def portals_placed_account_keyboard(
    accounts: list[Account], nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.button(
            text=account_label(account),
            callback_data=f"portalsoffers:account:{nonce}:{account.id}",
        )
    builder.button(text="⬅️ Назад", callback_data="account:list")
    builder.button(text="🏠 Главное меню", callback_data="nav:main")
    builder.adjust(1)
    return builder.as_markup()


def portals_placed_offers_keyboard(
    offers: Sequence[object], nonce: str
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, offer in enumerate(offers):
        name = str(getattr(offer, "display_name", "NFT"))[:35]
        builder.button(
            text=f"Открыть · {name}",
            callback_data=f"portalsoffers:open:{nonce}:{index}",
        )
    builder.button(text="🔄 Обновить", callback_data=f"portalsoffers:refresh:{nonce}")
    builder.button(text="⬅️ Назад", callback_data="portalsoffers:accounts")
    builder.button(text="🏠 Главное меню", callback_data="nav:main")
    builder.adjust(1)
    return builder.as_markup()


def portals_cancel_confirmation_keyboard(nonce: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Отозвать оффер",
                    callback_data=f"portalsoffers:cancel:{nonce}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад", callback_data=f"portalsoffers:back:{nonce}"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )


def portals_cancel_result_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Обновить", callback_data="portalsoffers:start"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )


def help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Аккаунты", callback_data="account:list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )


def history_keyboard(
    *,
    nonce: str,
    page: int,
    total_pages: int,
    has_previous: bool,
    has_next: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if has_previous or has_next:
        rows.append(
            [
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=(
                        f"history:page:{nonce}:{page - 1}"
                        if has_previous
                        else "history:noop"
                    ),
                ),
                InlineKeyboardButton(
                    text=f"{page + 1} / {total_pages}", callback_data="history:noop"
                ),
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=(
                        f"history:page:{nonce}:{page + 1}"
                        if has_next
                        else "history:noop"
                    ),
                ),
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🔄 Обновить",
                    callback_data=f"history:page:{nonce}:{page}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def mrkt_result_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🛒 Разместить ещё", callback_data="mrktjob:start"
                )
            ],
            [InlineKeyboardButton(text="📜 История", callback_data="history:list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )


def portals_result_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🤝 Посмотреть офферы", callback_data="portalsjob:start"
                )
            ],
            [InlineKeyboardButton(text="📜 История", callback_data="history:list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:main")],
        ]
    )
