from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, patch

from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Chat, Message, User

from app.bot.handlers.portals_transfers import start_portals_transfer
from app.bot.handlers.tonnel_transfers import start_tonnel_transfer
from app.bot.ui import edit_or_answer
from app.db import Database, PortalsTransferBatch
from app.jobs import (
    PortalsTransferBatchService,
    TonnelBatchProgress,
    TonnelTransferBatchResult,
    TonnelTransferBatchService,
    TonnelTransferJobService,
)
from app.telegram_client import PortalsNft, PortalsService, TonnelService
from tests.test_tonnel_batches import _ChildRunner, _TonnelPreflight, gift
from tests.test_ux_polish import _State


def callback(data: str) -> CallbackQuery:
    return CallbackQuery(
        id=data,
        from_user=User(id=100, is_bot=False, first_name="Имя"),
        chat_instance="chat",
        message=Message(
            message_id=1,
            date=datetime.now(UTC),
            chat=Chat(id=100, type="private"),
        ),
        data=data,
    )


class OneClickTransferTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "app.db"
        self.db = Database(self.path)
        await self.db.initialize()
        await self.add_account(1001, "OWNER", "owner")
        await self.add_account(1002, "TARGET", "target")

    async def asyncTearDown(self) -> None:
        self.temporary.cleanup()

    async def add_account(self, telegram_id: int, role: str, key: str) -> None:
        await self.db.save_account(
            owner_telegram_id=100,
            telegram_account_id=telegram_id,
            session_key=key,
            username=key,
            first_name=None,
            phone=None,
            role=role,
        )

    async def test_newly_connected_target_becomes_active_immediately(self) -> None:
        await self.add_account(1003, "TARGET", "new-target")
        pair = await self.db.get_active_account_pair(100)
        self.assertIsNotNone(pair.owner)
        self.assertIsNotNone(pair.target)
        assert pair.target is not None
        self.assertEqual(pair.target.telegram_account_id, 1003)

        restarted = Database(self.path)
        await restarted.initialize()
        persisted = await restarted.get_active_account_pair(100)
        assert persisted.target is not None
        self.assertEqual(persisted.target.telegram_account_id, 1003)

    async def test_unchanged_progress_edit_does_not_send_duplicate_message(self) -> None:
        class _NotModified(Exception):
            pass

        message = AsyncMock()
        message.edit_text.side_effect = _NotModified("message is not modified")
        with patch("app.bot.ui.TelegramBadRequest", _NotModified):
            await edit_or_answer(cast(Message, message), "Без изменений")
        message.answer.assert_not_awaited()

    async def test_tonnel_button_starts_batch_without_picker_or_price(self) -> None:
        state = _State()
        service = AsyncMock()
        progress = TonnelBatchProgress(3, 0, 0, 0, 3, 0, 0, ())
        service.execute_batch.return_value = TonnelTransferBatchResult(
            batch_id=99,
            status="DRY_RUN",
            children=(),
            progress=progress,
            batch_reference="safe",
            offer_amount=Decimal("4.5"),
            per_gift_required=Decimal("4.51"),
            total_required=Decimal("13.53"),
            buyer_balance=Decimal(20),
        )
        with (
            patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
            patch.object(
                self.db,
                "claim_tonnel_batch_final_notification",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "app.bot.handlers.tonnel_transfers.edit_or_answer",
                new_callable=AsyncMock,
            ) as render,
        ):
            await start_tonnel_transfer(
                callback("tonnelx:start"),
                self.db,
                cast(FSMContext, state),
                cast(TonnelTransferBatchService, service),
            )

        service.execute_batch.assert_awaited_once()
        self.assertIsNone(state.current)
        rendered = "\n".join(str(call.args[1]) for call in render.await_args_list)
        self.assertNotIn("Выберите точный подарок", rendered)
        self.assertNotIn("Введите", rendered)
        self.assertIn("Тестовый запуск", rendered)

    async def test_three_rapid_tonnel_presses_share_one_message_and_final(self) -> None:
        tonnel = _TonnelPreflight([gift(1), gift(2)])
        runner = _ChildRunner(self.db, delay=0.03)
        service = TonnelTransferBatchService(
            self.db,
            cast(TonnelService, tonnel),
            cast(TonnelTransferJobService, runner),
            offer_amount=Decimal(2),
        )
        callbacks = [callback("tonnelx:start") for _ in range(3)]
        states = [cast(FSMContext, _State()) for _ in range(3)]

        with (
            patch.object(CallbackQuery, "answer", new_callable=AsyncMock) as answer,
            patch(
                "app.bot.handlers.tonnel_transfers.edit_or_answer",
                new_callable=AsyncMock,
            ) as render,
        ):
            await asyncio.gather(
                *(
                    start_tonnel_transfer(item, self.db, state, service)
                    for item, state in zip(callbacks, states, strict=True)
                )
            )

        rendered = [str(call.args[1]) for call in render.await_args_list]
        self.assertEqual(sum("Загружаем все" in text for text in rendered), 1)
        self.assertEqual(sum("Tonnel завершён</b>" in text for text in rendered), 1)
        self.assertTrue(all(call.args[0].message_id == 1 for call in render.await_args_list))
        self.assertEqual(sum(runner.create_counts.values()), 2)
        self.assertEqual(sum(runner.accept_counts.values()), 2)
        self.assertEqual(
            sum(
                bool(call.args) and call.args[0] == "Передача уже выполняется."
                for call in answer.await_args_list
            ),
            2,
        )
        connection = sqlite3.connect(self.path)
        try:
            row = connection.execute(
                """
                SELECT COUNT(*), progress_chat_id, progress_message_id,
                       final_notification_sent
                FROM tonnel_transfer_batches
                """
            ).fetchone()
            assert row is not None
            self.assertEqual(row, (1, 100, 1, 1))
        finally:
            connection.close()

    async def test_portals_button_starts_all_gifts_without_selection(self) -> None:
        state = _State()
        portals = AsyncMock()
        portals.get_owned_nfts.return_value = [
            PortalsNft("Первый", True, None, "nft-1"),
            PortalsNft("Второй", True, None, "nft-2"),
        ]
        batch_service = AsyncMock()
        batch_service.execute_batch.return_value = PortalsTransferBatch(
            id=99,
            owner_telegram_id=100,
            owner_account_id=1,
            target_account_id=2,
            status="DRY_RUN",
            total_count=2,
            success_count=0,
            failed_count=0,
            ambiguous_count=0,
            created_at="2026-09-11",
            started_at=None,
            finished_at=None,
            result_metadata={},
        )
        with (
            patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
            patch(
                "app.bot.handlers.portals_transfers.edit_or_answer",
                new_callable=AsyncMock,
            ) as render,
        ):
            await start_portals_transfer(
                callback("portalsx:start"),
                self.db,
                cast(FSMContext, state),
                cast(PortalsService, portals),
                cast(PortalsTransferBatchService, batch_service),
            )

        batch_service.execute_batch.assert_awaited_once()
        selected = batch_service.execute_batch.await_args.kwargs["nfts"]
        self.assertEqual([nft.nft_id for nft in selected], ["nft-1", "nft-2"])
        rendered = "\n".join(str(call.args[1]) for call in render.await_args_list)
        self.assertNotIn("Выберите подарки", rendered)
        self.assertNotIn("Введите", rendered)

    async def test_missing_or_ambiguous_pair_never_starts_batch(self) -> None:
        for missing_role in ("OWNER", "TARGET"):
            with self.subTest(missing_role=missing_role):
                local = tempfile.TemporaryDirectory()
                try:
                    db = Database(Path(local.name) / "app.db")
                    await db.initialize()
                    role = "TARGET" if missing_role == "OWNER" else "OWNER"
                    await db.save_account(
                        owner_telegram_id=100,
                        telegram_account_id=9001,
                        session_key=f"only-{role}",
                        username=None,
                        first_name=None,
                        phone=None,
                        role=role,
                    )
                    state = _State()
                    service = AsyncMock()
                    with (
                        patch.object(
                            CallbackQuery, "answer", new_callable=AsyncMock
                        ),
                        patch(
                            "app.bot.handlers.tonnel_transfers.edit_or_answer",
                            new_callable=AsyncMock,
                        ) as render,
                    ):
                        await start_tonnel_transfer(
                            callback("tonnelx:start"),
                            db,
                            cast(FSMContext, state),
                            cast(TonnelTransferBatchService, service),
                        )
                    service.execute_batch.assert_not_awaited()
                    text = "\n".join(
                        str(call.args[1]) for call in render.await_args_list
                    )
                    self.assertIn(f"N{1 if missing_role == 'OWNER' else 2}", text)
                finally:
                    local.cleanup()

        await self.add_account(1003, "TARGET", "target-2")
        connection = sqlite3.connect(self.path)
        try:
            with connection:
                connection.execute(
                    "UPDATE active_account_pairs SET target_account_id = NULL "
                    "WHERE owner_telegram_id = 100"
                )
        finally:
            connection.close()
        pair = await self.db.get_active_account_pair(100)
        self.assertIsNone(pair.target)
        self.assertEqual(pair.target_count, 2)


if __name__ == "__main__":
    unittest.main()
