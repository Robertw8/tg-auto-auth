from __future__ import annotations

import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from app.bot.handlers.history import _format_history_time, _history_text
from app.bot.handlers.mrkt_jobs import (
    _load_inventory,
    _mrkt_review_text,
    _price_validation_error,
)
from app.bot.handlers.portals_jobs import _load_offers, _portals_review_text
from app.bot.keyboards import (
    account_remove_confirmation_keyboard,
    add_account_intro_keyboard,
    back_to_main_keyboard,
    history_keyboard,
    main_menu_keyboard,
    mrkt_job_confirmation_keyboard,
    portals_confirmation_keyboard,
)
from app.bot.states import MrktJobStates, PortalsJobStates
from app.db import Account, OperationHistoryItem
from app.telegram_client import MrktGift, MrktService, PortalsOffer, PortalsService

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _Message:
    def __init__(self) -> None:
        self.texts: list[str] = []
        self.markups: list[object] = []

    async def edit_text(self, text: str, **kwargs: object) -> None:
        self.texts.append(text)
        self.markups.append(kwargs.get("reply_markup"))

    async def answer(self, text: str, **kwargs: object) -> None:
        self.texts.append(text)
        self.markups.append(kwargs.get("reply_markup"))


class _State:
    def __init__(self) -> None:
        self.data: dict[str, object] = {}
        self.current: object | None = None

    async def clear(self) -> None:
        self.data.clear()
        self.current = None

    async def update_data(self, **kwargs: object) -> None:
        self.data.update(kwargs)

    async def get_data(self) -> dict[str, object]:
        return dict(self.data)

    async def set_state(self, state: object) -> None:
        self.current = state

    async def get_state(self) -> str | None:
        value = getattr(self.current, "state", self.current)
        return value if isinstance(value, str) else None


class _MrktService:
    def __init__(self, gifts: list[MrktGift]) -> None:
        self.gifts = gifts
        self.dry_run = True

    async def get_inventory(self, *args: object, **kwargs: object) -> list[MrktGift]:
        del args, kwargs
        return self.gifts


class _PortalsService:
    def __init__(self, offers: list[PortalsOffer]) -> None:
        self.offers = offers
        self.dry_run = True

    async def get_received_offers(
        self, *args: object, **kwargs: object
    ) -> list[PortalsOffer]:
        del args, kwargs
        return self.offers


def _account() -> Account:
    return Account(
        id=1,
        owner_telegram_id=10,
        telegram_account_id=99,
        session_key="internal",
        username="owner",
        first_name="Имя",
        phone="+380123456789",
        created_at="2026-09-02",
    )


def _gift(identifier: str = "gift") -> MrktGift:
    return MrktGift(
        gift_id=identifier,
        display_name=f"Подарок {identifier}",
        eligible=True,
        eligibility_reason=None,
        sale_price_nanotons=None,
    )


def _offer(identifier: str = "offer") -> PortalsOffer:
    return PortalsOffer(
        offer_id=identifier,
        nft_id=f"nft-{identifier}",
        amount="12.5",
        display_name=f"NFT {identifier}",
        amount_text="12.5",
        eligible=True,
        eligibility_reason=None,
    )


class UxPolishTests(unittest.IsolatedAsyncioTestCase):
    def test_add_account_delete_and_navigation_copy(self) -> None:
        intro = add_account_intro_keyboard().inline_keyboard
        self.assertEqual(
            [button.text for button in intro[0]],
            ["👤 Мой аккаунт", "💼 Рабочий аккаунт"],
        )
        self.assertEqual(intro[1][0].text, "⬅️ Назад")
        remove = account_remove_confirmation_keyboard("n", 1).inline_keyboard
        self.assertEqual(remove[0][0].text, "🗑 Да, удалить")
        self.assertEqual(remove[1][0].text, "Отмена")
        self.assertEqual(
            back_to_main_keyboard().inline_keyboard[0][0].text,
            "🏠 Главное меню",
        )

    def test_zero_account_menu_prioritizes_add(self) -> None:
        rows = main_menu_keyboard(has_accounts=False).inline_keyboard
        self.assertEqual(len(rows[0]), 1)
        self.assertEqual(rows[0][0].text, "➕ Добавить аккаунт")

    async def test_mrkt_empty_one_and_multiple_gift_states(self) -> None:
        message = _Message()
        state = _State()
        await _load_inventory(
            cast(Message, message),
            10,
            _account(),
            cast(FSMContext, state),
            cast(MrktService, _MrktService([])),
        )
        self.assertIn("Здесь пока нечего размещать", message.texts[-1])
        self.assertEqual(state.current, MrktJobStates.awaiting_inventory_retry)

        await _load_inventory(
            cast(Message, message),
            10,
            _account(),
            cast(FSMContext, state),
            cast(MrktService, _MrktService([_gift()])),
        )
        self.assertIn("Введите цену в TON", message.texts[-1])
        self.assertEqual(state.current, MrktJobStates.awaiting_price)

        await _load_inventory(
            cast(Message, message),
            10,
            _account(),
            cast(FSMContext, state),
            cast(MrktService, _MrktService([_gift("a"), _gift("b")])),
        )
        self.assertIn("Выберите подарок", message.texts[-1])
        self.assertEqual(state.current, MrktJobStates.awaiting_gift)

    def test_mrkt_review_dry_run_and_price_errors(self) -> None:
        text = _mrkt_review_text("@owner", "Подарок", "1.68", dry_run=True)
        self.assertIn("Проверьте размещение", text)
        self.assertIn("🧪 Тестовый режим", text)
        labels = [
            button.text
            for row in mrkt_job_confirmation_keyboard("n").inline_keyboard
            for button in row
        ]
        self.assertEqual(labels, ["✅ Разместить", "✏️ Изменить цену", "Отмена"])
        self.assertIn("числом", _price_validation_error("abc") or "")
        self.assertIn("больше 0", _price_validation_error("0") or "")
        self.assertIn("не более 2", _price_validation_error("1.234") or "")

    async def test_portals_empty_one_and_multiple_offer_states(self) -> None:
        message = _Message()
        state = _State()
        await _load_offers(
            cast(Message, message),
            10,
            _account(),
            cast(FSMContext, state),
            cast(PortalsService, _PortalsService([])),
        )
        self.assertIn("Новых офферов нет", message.texts[-1])
        self.assertEqual(state.current, PortalsJobStates.awaiting_offers_retry)

        await _load_offers(
            cast(Message, message),
            10,
            _account(),
            cast(FSMContext, state),
            cast(PortalsService, _PortalsService([_offer()])),
        )
        self.assertIn("Проверьте оффер", message.texts[-1])
        self.assertEqual(state.current, PortalsJobStates.awaiting_confirmation)

        await _load_offers(
            cast(Message, message),
            10,
            _account(),
            cast(FSMContext, state),
            cast(PortalsService, _PortalsService([_offer("a"), _offer("b")])),
        )
        self.assertIn("Выберите оффер", message.texts[-1])
        self.assertEqual(state.current, PortalsJobStates.awaiting_offer)

    def test_portals_review_and_confirmation_copy(self) -> None:
        text = _portals_review_text("@owner", _offer(), dry_run=True)
        self.assertIn("Проверьте оффер", text)
        self.assertIn("🧪 Тестовый режим", text)
        labels = [
            button.text
            for row in portals_confirmation_keyboard("n").inline_keyboard
            for button in row
        ]
        self.assertEqual(labels, ["✅ Принять оффер", "⬅️ Назад", "Отмена"])

    def test_history_is_compact_human_readable_and_paginated(self) -> None:
        self.assertEqual(
            _format_history_time(
                "2026-09-02 14:32:00",
                now=datetime(2026, 9, 2, 16, 0, tzinfo=UTC),
            ),
            "Сегодня, 14:32 UTC",
        )
        history_text = _history_text(
            [
                OperationHistoryItem(
                    operation_id=91,
                    market="mrkt",
                    account_id=1,
                    display_name="Подарок",
                    value_text="1.68",
                    status="SUCCESS",
                    timestamp="2026-09-02 14:32:00",
                )
            ],
            [_account()],
        )
        self.assertNotIn("#91", history_text)
        self.assertIn("✅ Успешно · MRKT", history_text)
        keyboard = history_keyboard(
            nonce="n",
            page=1,
            total_pages=3,
            has_previous=True,
            has_next=True,
        )
        self.assertEqual(keyboard.inline_keyboard[0][1].text, "2 / 3")

    def test_web_app_loading_validation_password_and_theme(self) -> None:
        html_source = (PROJECT_ROOT / "app/web/static/auth.html").read_text()
        js_source = (PROJECT_ROOT / "app/web/static/auth.js").read_text()
        css_source = (PROJECT_ROOT / "app/web/static/auth.css").read_text()
        app_source = (PROJECT_ROOT / "app/web/app.py").read_text()
        self.assertIn('data-loading-label="Отправляем код…"', html_source)
        self.assertIn('id="toggle-password"', html_source)
        self.assertIn("setLoading(button, true)", js_source)
        self.assertIn('telegram.onEvent("themeChanged", applyTheme)', js_source)
        self.assertIn(':root[data-theme="dark"]', css_source)
        self.assertIn("Неверный код. Проверьте его", app_source)
        self.assertIn("Код больше не действует", app_source)
        self.assertIn("Неверный пароль 2FA", app_source)

    def test_no_raw_technical_errors_in_bot_copy(self) -> None:
        sources = "\n".join(
            (PROJECT_ROOT / path).read_text()
            for path in (
                "app/bot/handlers/accounts.py",
                "app/bot/handlers/mrkt_jobs.py",
                "app/bot/handlers/portals_jobs.py",
                "app/bot/handlers/history.py",
                "app/bot/handlers/navigation.py",
            )
        )
        for raw_code in ("SALE_REJECTED", "MRKT_AUTH_FAILED", "ACCEPT_AMBIGUOUS"):
            self.assertNotIn(raw_code, sources)
