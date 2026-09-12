from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Chat, Message, User

from app.bot.handlers.mrkt_transfers import _after_target as mrkt_after_target
from app.bot.handlers.mrkt_transfers import confirm_mrkt_transfer
from app.bot.handlers.portals_transfers import _after_target as portals_after_target
from app.bot.states import MrktTransferStates, PortalsTransferStates
from app.db import Database
from app.telegram_client import MrktService, PortalsService
from tests.test_ux_polish import _account, _Message, _State


class TransferUiTests(unittest.IsolatedAsyncioTestCase):
    async def test_valid_mrkt_confirmation_passes_nonce_once_and_clears_state(
        self,
    ) -> None:
        nonce = "fresh-confirmation"
        state = _State()
        state.data.update(
            confirm_nonce=nonce,
            confirm_expires_at=10**12,
            owner_account_id=1,
            target_account_id=2,
            asset_id="gift-7",
            amount="1.68",
        )
        state.current = MrktTransferStates.confirming
        callback = CallbackQuery(
            id="q",
            from_user=User(id=100, is_bot=False, first_name="Имя"),
            chat_instance="c",
            message=Message(
                message_id=1,
                date=datetime.now(UTC),
                chat=Chat(id=100, type="private"),
            ),
            data=f"mrktx:confirm:{nonce}",
        )
        jobs = AsyncMock()
        jobs.execute_mrkt_transfer.return_value = SimpleNamespace(
            status="DRY_RUN",
            error_code=None,
            error_message=None,
            result_metadata={},
        )
        with (
            patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
            patch(
                "app.bot.handlers.mrkt_transfers.edit_or_answer",
                new_callable=AsyncMock,
            ),
        ):
            await confirm_mrkt_transfer(callback, cast(FSMContext, state), jobs)
            await confirm_mrkt_transfer(callback, cast(FSMContext, state), jobs)

        jobs.execute_mrkt_transfer.assert_awaited_once()
        self.assertEqual(
            jobs.execute_mrkt_transfer.await_args.kwargs["confirmation_nonce"],
            nonce,
        )
        self.assertEqual(state.data, {})
        self.assertIsNone(state.current)

    async def test_stale_and_expired_confirmation_never_execute_job(self) -> None:
        for stored_nonce in ("stale", "current"):
            with self.subTest(nonce=stored_nonce):
                state = _State()
                state.data.update(confirm_nonce=stored_nonce, confirm_expires_at=0)
                state.current = MrktTransferStates.confirming
                user = User(id=100, is_bot=False, first_name="Имя")
                message = Message(
                    message_id=1,
                    date=datetime.now(UTC),
                    chat=Chat(id=100, type="private"),
                )
                callback = CallbackQuery(
                    id="q",
                    from_user=user,
                    chat_instance="c",
                    message=message,
                    data="mrktx:confirm:current",
                )
                jobs = AsyncMock()
                with (
                    patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
                    patch(
                        "app.bot.handlers.mrkt_transfers.edit_or_answer",
                        new_callable=AsyncMock,
                    )
                ):
                    await confirm_mrkt_transfer(
                        callback, cast(FSMContext, state), jobs
                    )
                self.assertEqual(jobs.mock_calls, [])
                self.assertIsNone(state.current)

    async def test_single_owner_auto_selected_multiple_owners_require_selection(
        self,
    ) -> None:
        for market, handler, states, loader, service in (
            (
                "mrkt",
                mrkt_after_target,
                MrktTransferStates,
                "_show_gifts",
                cast(MrktService, None),
            ),
            (
                "portals",
                portals_after_target,
                PortalsTransferStates,
                "_show_nfts",
                cast(PortalsService, None),
            ),
        ):
            with self.subTest(market=market):
                message, state = _Message(), _State()
                owner = replace(_account(), role="OWNER", id=1)
                target = replace(_account(), role="TARGET", id=2)
                state.data["transfer_nonce"] = "nonce"
                with patch(
                    f"app.bot.handlers.{market}_transfers.{loader}",
                    new_callable=AsyncMock,
                ) as load:
                    extra = (
                        (cast(Database, AsyncMock()), cast(Any, AsyncMock()))
                        if market == "portals"
                        else ()
                    )
                    await handler(
                        cast(Message, message),
                        10,
                        target,
                        [owner],
                        cast(FSMContext, state),
                        cast(Any, service),
                        *extra,
                    )
                    self.assertEqual(state.data["owner_account_id"], 1)
                    load.assert_awaited_once()
                    load.reset_mock()
                    await handler(
                        cast(Message, message),
                        10,
                        target,
                        [owner, replace(owner, id=3)],
                        cast(FSMContext, state),
                        cast(Any, service),
                        *extra,
                    )
                    self.assertEqual(state.current, states.choosing_owner)
                    load.assert_not_awaited()

    async def test_owner_removed_during_selection_stops_flow(self) -> None:
        message, state = _Message(), _State()
        state.data["transfer_nonce"] = "nonce"
        await mrkt_after_target(
            cast(Message, message),
            10,
            _account(),
            [],
            cast(FSMContext, state),
            cast(MrktService, None),
        )
        self.assertIn("больше не доступен", message.texts[-1])
        self.assertIsNone(state.current)
