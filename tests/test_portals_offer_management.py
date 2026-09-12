from __future__ import annotations

import tempfile
import time
import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, patch

from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Chat, InlineKeyboardMarkup, Message, User

from app.bot.handlers.portals_offers import (
    _cancel_result_text,
    _placed_offer_detail_text,
    _placed_offers_text,
    confirm_cancel_placed_offer,
)
from app.bot.keyboards import (
    account_sections_keyboard,
    portals_cancel_confirmation_keyboard,
    portals_placed_offers_keyboard,
)
from app.bot.states import PortalsOfferManagementStates
from app.db import Database
from app.telegram_client import (
    PortalsCancelResult,
    PortalsCancelStatus,
    PortalsOffer,
    PortalsService,
)
from tests.test_ux_polish import _State


def offer(identifier: str = "private-offer-id") -> PortalsOffer:
    return PortalsOffer(
        offer_id=identifier,
        nft_id="private-nft-id",
        amount="0.53",
        display_name="Lol Pop",
        amount_text="0.53",
        eligible=True,
        eligibility_reason=None,
        status="pending",
        created_at="2026-09-07T12:10:48.845999Z",
    )


def callback(data: str, user_id: int = 100) -> CallbackQuery:
    return CallbackQuery(
        id="query",
        from_user=User(id=user_id, is_bot=False, first_name="Оператор"),
        chat_instance="chat",
        message=Message(
            message_id=1,
            date=datetime.now(UTC),
            chat=Chat(id=user_id, type="private"),
        ),
        data=data,
    )


class PortalsOfferManagementTests(unittest.IsolatedAsyncioTestCase):
    def test_empty_one_and_multiple_offer_ui(self) -> None:
        self.assertIn("Активных офферов нет", _placed_offers_text([]))
        one = _placed_offers_text([offer()])
        multiple = _placed_offers_text([offer("first"), offer("second")])
        self.assertIn("Lol Pop", one)
        self.assertIn("0.53 TON", one)
        self.assertIn("Статус: ожидает", one)
        self.assertNotIn("private-offer-id", one)
        self.assertIn("1. Lol Pop", multiple)
        self.assertIn("2. Lol Pop", multiple)

    def test_russian_keyboards_and_confirmation(self) -> None:
        sections = account_sections_keyboard().inline_keyboard
        self.assertTrue(
            any(button.text == "📨 Мои офферы" for row in sections for button in row)
        )
        listing = portals_placed_offers_keyboard([offer()], "nonce")
        labels = [button.text for row in listing.inline_keyboard for button in row]
        self.assertIn("Открыть · Lol Pop", labels)
        self.assertIn("🔄 Обновить", labels)
        self.assertIn("⬅️ Назад", labels)
        confirmation = portals_cancel_confirmation_keyboard("nonce")
        self.assertEqual(confirmation.inline_keyboard[0][0].text, "🗑 Отозвать оффер")
        detail = _placed_offer_detail_text(offer())
        self.assertIn("Отозвать этот оффер?", detail)
        self.assertNotIn("private-offer-id", detail)
        self.assertIn(
            "Оффер уже не активен",
            _cancel_result_text(PortalsCancelStatus.ALREADY_INACTIVE, offer()),
        )
        self.assertIn(
            "Требуется проверка",
            _cancel_result_text(PortalsCancelStatus.AMBIGUOUS, offer()),
        )

    async def test_stale_callback_never_calls_cancel(self) -> None:
        state = _State()
        state.current = PortalsOfferManagementStates.confirming_cancel
        state.data.update(
            portals_cancel_nonce="expected",
            portals_cancel_expires_at=time.monotonic() + 300,
            portals_placed_account_id=1,
            portals_cancel_offer=offer(),
        )
        service = AsyncMock(spec=PortalsService)
        with (
            patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
            patch(
                "app.bot.handlers.portals_offers.edit_or_answer",
                new_callable=AsyncMock,
            ),
        ):
            await confirm_cancel_placed_offer(
                callback("portalsoffers:cancel:stale"),
                AsyncMock(spec=Database),
                cast(FSMContext, state),
                service,
            )
        service.cancel_offer.assert_not_awaited()
        self.assertIsNone(state.current)

    async def test_account_ownership_is_rechecked_before_cancel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = Database(Path(directory) / "app.db")
            await db.initialize()
            await db.save_account(
                owner_telegram_id=200,
                telegram_account_id=300,
                session_key="other-owner-session",
                username="other",
                first_name=None,
                phone=None,
                role="OWNER",
            )
            state = _State()
            state.current = PortalsOfferManagementStates.confirming_cancel
            state.data.update(
                portals_cancel_nonce="nonce",
                portals_cancel_expires_at=time.monotonic() + 300,
                portals_placed_account_id=1,
                portals_cancel_offer=offer(),
            )
            service = AsyncMock(spec=PortalsService)
            with (
                patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
                patch(
                    "app.bot.handlers.portals_offers.edit_or_answer",
                    new_callable=AsyncMock,
                ),
            ):
                await confirm_cancel_placed_offer(
                    callback("portalsoffers:cancel:nonce", user_id=100),
                    db,
                    cast(FSMContext, state),
                    service,
                )
            service.cancel_offer.assert_not_awaited()

    async def test_valid_confirmation_is_single_use(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = Database(Path(directory) / "app.db")
            await db.initialize()
            await db.save_account(
                owner_telegram_id=100,
                telegram_account_id=300,
                session_key="owner-session",
                username="owner",
                first_name=None,
                phone=None,
                role="OWNER",
            )
            state = _State()
            state.current = PortalsOfferManagementStates.confirming_cancel
            selected = offer()
            state.data.update(
                portals_cancel_nonce="nonce",
                portals_cancel_expires_at=time.monotonic() + 300,
                portals_placed_account_id=1,
                portals_cancel_offer=selected,
            )
            service = AsyncMock(spec=PortalsService)
            service.cancel_offer.return_value = PortalsCancelResult(
                status=PortalsCancelStatus.SUCCESS,
                display_name="Lol Pop",
                amount_text="0.53",
                cancel_request_sent=True,
                response_status=204,
                response_fields=(),
            )
            query = callback("portalsoffers:cancel:nonce")
            with (
                patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
                patch(
                    "app.bot.handlers.portals_offers.edit_or_answer",
                    new_callable=AsyncMock,
                ) as render,
            ):
                await confirm_cancel_placed_offer(
                    query,
                    db,
                    cast(FSMContext, state),
                    service,
                )
                await confirm_cancel_placed_offer(
                    query,
                    db,
                    cast(FSMContext, state),
                    service,
                )

        service.cancel_offer.assert_awaited_once_with(
            owner_telegram_id=100,
            account_id=1,
            offer_id=selected.offer_id,
            nft_id=selected.nft_id,
            amount=selected.amount,
        )
        rendered_text = "\n".join(
            str(call.args[1]) for call in render.await_args_list if len(call.args) > 1
        )
        self.assertIn("✅ Оффер отозван", rendered_text)
        self.assertNotIn("private-offer-id", rendered_text)
        self.assertTrue(
            all(
                isinstance(call.kwargs.get("reply_markup"), InlineKeyboardMarkup)
                or "Отзываем" in str(call.args[1])
                for call in render.await_args_list
                if len(call.args) > 1
            )
        )


if __name__ == "__main__":
    unittest.main()
