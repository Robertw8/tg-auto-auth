from __future__ import annotations

import asyncio
import tempfile
import unittest
from collections import Counter
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, patch

from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Chat, Message, User

from app.bot.handlers.portals_transfers import (
    _batch_progress_text,
    _batch_result_text,
    clear_portals_nfts,
    launch_portals_batch,
    select_all_portals_nfts,
    toggle_portals_nft,
)
from app.bot.keyboards import portals_batch_assets_keyboard
from app.bot.states import PortalsTransferStates
from app.db import ActiveTransferConflictError, Database, TransferJob
from app.jobs import PortalsTransferBatchService, PortalsTransferJobService
from app.telegram_client import PortalsNft
from tests.test_ux_polish import _State


def nft(index: int) -> PortalsNft:
    return PortalsNft(
        nft_id=f"nft-{index}",
        display_name=f"Подарок {index}",
        eligible=True,
        eligibility_reason=None,
    )


class _ChildRunner:
    def __init__(
        self,
        db: Database,
        *,
        statuses: dict[str, str] | None = None,
        rate_limited_assets: set[str] | None = None,
        delay: float = 0.01,
    ) -> None:
        self.db = db
        self.statuses = statuses or {}
        self.rate_limited_assets = rate_limited_assets or set()
        self.delay = delay
        self.calls: list[str] = []
        self.create_counts: Counter[str] = Counter()
        self.accept_counts: Counter[str] = Counter()
        self.running = 0
        self.max_running = 0
        self.batch_scope_entries = 0
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def batch_session_scope(self, *args: object):
        del args
        self.batch_scope_entries += 1
        yield

    async def execute_reserved_portals_transfer(
        self, job_id: int, *, progress: object = None
    ) -> TransferJob:
        del progress
        job = await self.db.claim_transfer_job(job_id)
        if job is None:
            current = await self.db.get_transfer_job(job_id)
            assert current is not None
            return current
        asset = str(job.asset_id)
        async with self._lock:
            self.calls.append(asset)
            self.running += 1
            self.max_running = max(self.max_running, self.running)
            self.create_counts[asset] += 1
        try:
            await asyncio.sleep(self.delay)
            status = self.statuses.get(asset, "SUCCESS")
            if status != "FAILED":
                self.accept_counts[asset] += 1
            mutation_counts = {"portals_create_offer": 1}
            if status != "FAILED":
                mutation_counts["portals_accept"] = 1
            return await self.db.finish_transfer_job(
                job.id,
                status=status,
                phase="COMPLETED",
                error_code=(
                    "RATE_LIMITED"
                    if asset in self.rate_limited_assets
                    else status if status in {"FAILED", "AMBIGUOUS"} else None
                ),
                error_message=None,
                result_metadata={
                    "live_verify": {"mutation_counts": mutation_counts}
                },
            )
        finally:
            async with self._lock:
                self.running -= 1


class PortalsBatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temporary.name) / "app.db")
        await self.db.initialize()
        await self.db.save_account(
            owner_telegram_id=100,
            telegram_account_id=1001,
            session_key="owner",
            username="owner",
            first_name=None,
            phone=None,
            role="OWNER",
        )
        await self.db.save_account(
            owner_telegram_id=100,
            telegram_account_id=1002,
            session_key="target",
            username="target",
            first_name=None,
            phone=None,
            role="TARGET",
        )

    async def asyncTearDown(self) -> None:
        self.temporary.cleanup()

    def service(
        self,
        runner: _ChildRunner,
        *,
        max_concurrency: int = 5,
    ) -> PortalsTransferBatchService:
        return PortalsTransferBatchService(
            self.db,
            cast(PortalsTransferJobService, runner),
            max_concurrency=max_concurrency,
        )

    async def execute(
        self,
        service: PortalsTransferBatchService,
        count: int,
        *,
        nonce: str = "nonce",
    ):
        return await service.execute_batch(
            owner_telegram_id=100,
            owner_account_id=1,
            target_account_id=2,
            nfts=[nft(index) for index in range(count)],
            confirmation_nonce=nonce,
        )

    async def test_one_child_per_nft_and_duplicate_nonce_is_idempotent(self) -> None:
        runner = _ChildRunner(self.db)
        service = self.service(runner)
        first = await self.execute(service, 3)
        second = await self.execute(service, 3)
        children = await self.db.list_transfer_jobs_by_batch(first.id)

        self.assertEqual(first.id, second.id)
        self.assertEqual(len(children), 3)
        self.assertEqual(runner.calls, ["nft-0", "nft-1", "nft-2"])
        self.assertTrue(all(job.amount_text == "0.53" for job in children))
        self.assertTrue(all(job.batch_id == first.id for job in children))
        self.assertEqual(sum(runner.create_counts.values()), 3)
        self.assertEqual(sum(runner.accept_counts.values()), 3)
        self.assertEqual(runner.batch_scope_entries, 1)
        self.assertEqual(first.result_metadata["create_mutation_count"], 3)
        self.assertEqual(first.result_metadata["accept_mutation_count"], 3)

    async def test_three_and_five_run_concurrently(self) -> None:
        for count in (3, 5):
            with self.subTest(count=count):
                temporary = tempfile.TemporaryDirectory()
                try:
                    db = Database(Path(temporary.name) / "app.db")
                    await db.initialize()
                    for account_id, role in ((1, "OWNER"), (2, "TARGET")):
                        await db.save_account(
                            owner_telegram_id=100,
                            telegram_account_id=account_id,
                            session_key=f"session-{account_id}",
                            username=None,
                            first_name=None,
                            phone=None,
                            role=role,
                        )
                    runner = _ChildRunner(db, delay=0.03)
                    service = PortalsTransferBatchService(
                        db, cast(PortalsTransferJobService, runner)
                    )
                    await service.execute_batch(
                        owner_telegram_id=100,
                        owner_account_id=1,
                        target_account_id=2,
                        nfts=[nft(index) for index in range(count)],
                        confirmation_nonce=f"nonce-{count}",
                    )
                    self.assertEqual(runner.max_running, count)
                finally:
                    temporary.cleanup()

    async def test_six_and_ten_never_exceed_five_and_queue_drains(self) -> None:
        for count in (6, 10):
            with self.subTest(count=count):
                runner = _ChildRunner(self.db, delay=0.01)
                service = self.service(runner)
                result = await self.execute(service, count, nonce=f"n-{count}")
                self.assertEqual(result.status, "SUCCESS")
                self.assertEqual(runner.max_running, 5)
                self.assertEqual(len(runner.calls), count)
                self.assertEqual(
                    result.result_metadata["queued_count"], count - 5
                )

    async def test_configured_limit_below_five_is_enforced(self) -> None:
        runner = _ChildRunner(self.db, delay=0.02)
        result = await self.execute(self.service(runner, max_concurrency=2), 5)
        self.assertEqual(runner.max_running, 2)
        self.assertEqual(result.result_metadata["max_concurrency_observed"], 2)

    async def test_double_click_same_pair_does_not_launch_second_batch(self) -> None:
        runner = _ChildRunner(self.db, delay=0.05)
        service = self.service(runner)
        first = asyncio.create_task(self.execute(service, 3, nonce="first"))
        while not runner.calls:
            await asyncio.sleep(0)
        with self.assertRaises(ActiveTransferConflictError):
            await self.execute(service, 3, nonce="second")
        await first
        self.assertEqual(sum(runner.create_counts.values()), 3)
        self.assertEqual(sum(runner.accept_counts.values()), 3)

    async def test_partial_failure_and_ambiguous_children_do_not_stop_others(
        self,
    ) -> None:
        runner = _ChildRunner(
            self.db,
            statuses={"nft-1": "FAILED", "nft-2": "AMBIGUOUS"},
        )
        result = await self.execute(self.service(runner), 4)
        children = await self.db.list_transfer_jobs_by_batch(result.id)

        self.assertEqual(result.status, "PARTIAL")
        self.assertEqual(result.success_count, 2)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.ambiguous_count, 1)
        self.assertEqual(len(runner.calls), 4)
        self.assertEqual(Counter(job.status for job in children)["SUCCESS"], 2)
        self.assertEqual(runner.create_counts["nft-2"], 1)

    async def test_same_pair_allows_distinct_active_nfts_but_not_same_nft(
        self,
    ) -> None:
        first = await self.db.create_transfer_job(
            owner_telegram_id=100,
            market="portals",
            owner_account_id=1,
            target_account_id=2,
            asset_id="nft-a",
            amount_text="0.53",
        )
        await self.db.claim_transfer_job(first.id)
        second = await self.db.create_transfer_job(
            owner_telegram_id=100,
            market="portals",
            owner_account_id=1,
            target_account_id=2,
            asset_id="nft-b",
            amount_text="0.53",
        )
        duplicate = await self.db.create_transfer_job(
            owner_telegram_id=100,
            market="portals",
            owner_account_id=1,
            target_account_id=2,
            asset_id="nft-a",
            amount_text="0.53",
        )
        self.assertNotEqual(first.id, second.id)
        self.assertEqual(duplicate.id, first.id)

    async def test_existing_active_nft_blocks_batch_atomically(
        self,
    ) -> None:
        job = await self.db.create_transfer_job(
            owner_telegram_id=100,
            market="portals",
            owner_account_id=1,
            target_account_id=2,
            asset_id="nft-1",
            amount_text="0.53",
        )
        await self.db.claim_transfer_job(job.id)
        with self.assertRaises(ActiveTransferConflictError):
            await self.execute(self.service(_ChildRunner(self.db)), 3)
        self.assertIsNone(await self.db.get_portals_transfer_batch(1))

    async def test_offer_ids_and_mutation_counts_stay_child_local(self) -> None:
        runner = _ChildRunner(self.db)
        result = await self.execute(self.service(runner), 5)
        children = await self.db.list_transfer_jobs_by_batch(result.id)
        self.assertEqual(set(runner.create_counts), {f"nft-{i}" for i in range(5)})
        self.assertTrue(all(count == 1 for count in runner.create_counts.values()))
        self.assertTrue(all(count == 1 for count in runner.accept_counts.values()))
        self.assertEqual({job.asset_id for job in children}, set(runner.calls))

    async def test_history_keeps_each_child_job(self) -> None:
        result = await self.execute(self.service(_ChildRunner(self.db)), 3)
        history = await self.db.list_operation_history(100)
        child_ids = {
            job.id for job in await self.db.list_transfer_jobs_by_batch(result.id)
        }
        self.assertEqual(
            {item.operation_id for item in history if item.market == "portals_transfer"},
            child_ids,
        )

    async def test_rate_limit_stops_new_queued_children_without_retry(self) -> None:
        runner = _ChildRunner(
            self.db,
            statuses={"nft-0": "FAILED"},
            rate_limited_assets={"nft-0"},
        )
        result = await self.execute(
            self.service(runner, max_concurrency=1), 4
        )
        children = await self.db.list_transfer_jobs_by_batch(result.id)
        self.assertEqual(runner.calls, ["nft-0"])
        self.assertEqual(runner.create_counts["nft-0"], 1)
        self.assertTrue(result.result_metadata["rate_limited"])
        self.assertEqual(
            [job.error_code for job in children[1:]],
            ["BATCH_RATE_LIMITED"] * 3,
        )

    async def test_batch_metadata_redacts_nonce_assets_and_credentials(self) -> None:
        secret_nonce = "secret-nonce-value"
        result = await self.execute(
            self.service(_ChildRunner(self.db)), 2, nonce=secret_nonce
        )
        serialized = repr(result.result_metadata)
        self.assertNotIn(secret_nonce, serialized)
        self.assertNotIn("nft-0", serialized)
        self.assertNotIn("Authorization", serialized)
        self.assertNotIn("initData", serialized)

    async def test_cancellation_marks_running_and_queued_children_terminal(
        self,
    ) -> None:
        runner = _ChildRunner(self.db, delay=1)
        service = self.service(runner, max_concurrency=2)
        task = asyncio.create_task(self.execute(service, 5))
        while runner.running < 2:
            await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        batch = await self.db.get_portals_transfer_batch(1)
        children = await self.db.list_transfer_jobs_by_batch(1)
        self.assertIsNotNone(batch)
        self.assertNotIn(batch.status if batch else None, {"PENDING", "RUNNING"})
        self.assertTrue(
            all(job.status in {"AMBIGUOUS", "FAILED"} for job in children)
        )

    async def test_restart_recovers_running_and_queued_batch_state(self) -> None:
        batch, children, created = await self.db.create_portals_transfer_batch(
            owner_telegram_id=100,
            owner_account_id=1,
            target_account_id=2,
            assets=[("nft-a", "A"), ("nft-b", "B")],
            amount_text="0.53",
            confirmation_key="safe-hash",
        )
        self.assertTrue(created)
        await self.db.claim_portals_transfer_batch(batch.id)
        await self.db.claim_transfer_job(children[0].id)

        restarted = Database(Path(self.temporary.name) / "app.db")
        await restarted.initialize()
        self.assertEqual(await restarted.recover_running_transfer_jobs(), 2)
        self.assertEqual(await restarted.recover_portals_transfer_batches(), 1)
        recovered_batch = await restarted.get_portals_transfer_batch(batch.id)
        recovered_children = await restarted.list_transfer_jobs_by_batch(batch.id)
        self.assertEqual(recovered_batch.status if recovered_batch else None, "AMBIGUOUS")
        self.assertEqual(
            [job.status for job in recovered_children], ["AMBIGUOUS", "FAILED"]
        )

    async def test_empty_selection_and_invalid_limit_are_rejected(self) -> None:
        service = self.service(_ChildRunner(self.db))
        with self.assertRaises(ValueError):
            await service.execute_batch(
                owner_telegram_id=100,
                owner_account_id=1,
                target_account_id=2,
                nfts=[],
                confirmation_nonce="n",
            )
        with self.assertRaises(ValueError):
            self.service(_ChildRunner(self.db), max_concurrency=6)

    async def test_account_roles_and_control_user_ownership_are_enforced(self) -> None:
        service = self.service(_ChildRunner(self.db))
        for owner_user, owner_id, target_id in (
            (999, 1, 2),
            (100, 2, 1),
        ):
            with self.subTest(owner_user=owner_user), self.assertRaises(ValueError):
                await service.execute_batch(
                    owner_telegram_id=owner_user,
                    owner_account_id=owner_id,
                    target_account_id=target_id,
                    nfts=[nft(1)],
                    confirmation_nonce=f"nonce-{owner_user}",
                )
        self.assertIsNone(await self.db.get_portals_transfer_batch(1))

    async def test_multi_select_select_all_and_clear_selection(self) -> None:
        state = _State()
        state.current = PortalsTransferStates.choosing_asset
        state.data.update(
            transfer_nonce="nonce",
            transfer_assets=[nft(0), nft(1), nft(2)],
            selected_asset_indices=[],
        )

        async def invoke(data: str, handler: object) -> None:
            callback = CallbackQuery(
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
            with (
                patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
                patch.object(Message, "edit_reply_markup", new_callable=AsyncMock),
            ):
                await handler(callback, cast(FSMContext, state))  # type: ignore[operator]

        await invoke("portalsx:toggle:nonce:0", toggle_portals_nft)
        await invoke("portalsx:toggle:nonce:2", toggle_portals_nft)
        self.assertEqual(state.data["selected_asset_indices"], [0, 2])
        await invoke("portalsx:select_all:nonce", select_all_portals_nfts)
        self.assertEqual(state.data["selected_asset_indices"], [0, 1, 2])
        await invoke("portalsx:clear:nonce", clear_portals_nfts)
        self.assertEqual(state.data["selected_asset_indices"], [])

    async def test_zero_selection_cannot_launch(self) -> None:
        state = _State()
        state.current = PortalsTransferStates.choosing_asset
        state.data.update(
            transfer_nonce="nonce",
            transfer_assets=[nft(0)],
            selected_asset_indices=[],
            owner_account_id=1,
            target_account_id=2,
        )
        callback = CallbackQuery(
            id="launch",
            from_user=User(id=100, is_bot=False, first_name="Имя"),
            chat_instance="chat",
            message=Message(
                message_id=1,
                date=datetime.now(UTC),
                chat=Chat(id=100, type="private"),
            ),
            data="portalsx:launch:nonce",
        )
        service = AsyncMock()
        with patch.object(
            CallbackQuery, "answer", new_callable=AsyncMock
        ) as answer:
            await launch_portals_batch(
                callback,
                cast(FSMContext, state),
                self.db,
                cast(PortalsTransferBatchService, service),
            )
        service.execute_batch.assert_not_awaited()
        answer.assert_awaited_once_with(
            "Выберите хотя бы один подарок.", show_alert=True
        )

    async def test_valid_multi_selection_launches_one_batch(self) -> None:
        state = _State()
        state.current = PortalsTransferStates.choosing_asset
        state.data.update(
            transfer_nonce="nonce",
            transfer_assets=[nft(0), nft(1), nft(2)],
            selected_asset_indices=[0, 2],
            owner_account_id=1,
            target_account_id=2,
        )
        callback = CallbackQuery(
            id="launch",
            from_user=User(id=100, is_bot=False, first_name="Имя"),
            chat_instance="chat",
            message=Message(
                message_id=1,
                date=datetime.now(UTC),
                chat=Chat(id=100, type="private"),
            ),
            data="portalsx:launch:nonce",
        )
        runner = _ChildRunner(self.db)
        service = self.service(runner)
        with (
            patch.object(CallbackQuery, "answer", new_callable=AsyncMock),
            patch(
                "app.bot.handlers.portals_transfers.edit_or_answer",
                new_callable=AsyncMock,
            ) as render,
        ):
            await launch_portals_batch(
                callback,
                cast(FSMContext, state),
                self.db,
                service,
            )
        batch = await self.db.get_portals_transfer_batch(1)
        children = await self.db.list_transfer_jobs_by_batch(1)
        self.assertIsNotNone(batch)
        self.assertEqual([job.asset_id for job in children], ["nft-0", "nft-2"])
        self.assertEqual(runner.calls, ["nft-0", "nft-2"])
        self.assertIsNone(state.current)
        self.assertTrue(
            any(
                "Передача завершена" in str(call.args[1])
                for call in render.await_args_list
                if len(call.args) > 1
            )
        )

    def test_batch_keyboard_and_status_text_are_russian(self) -> None:
        keyboard = portals_batch_assets_keyboard(
            [nft(0), nft(1)], selected={0}, nonce="nonce"
        )
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertIn("✅ Подарок 0", labels)
        self.assertIn("☐ Подарок 1", labels)
        self.assertIn("✅ Выбрать все", labels)
        self.assertIn("❌ Снять выбор", labels)
        self.assertIn("🚀 Передать 1 подарок", labels)
        progress = PortalsTransferBatchService._progress_snapshot(
            {1: "SUCCESS", 2: "RUNNING", 3: "QUEUED"},
            {1: "Один", 2: "Два", 3: "Три"},
        )
        self.assertIn("Передача подарков Portals", _batch_progress_text(progress))
        children = [
            TransferJob(
                id=index,
                owner_telegram_id=100,
                market="portals",
                owner_account_id=1,
                target_account_id=2,
                asset_id=f"nft-{index}",
                display_name=f"Подарок {index}",
                amount_text="0.53",
                amount_atomic=None,
                external_ref=None,
                status=status,
                phase="COMPLETED",
                created_at="2026-09-08",
                started_at=None,
                finished_at=None,
                error_code=None,
                error_message=None,
                result_metadata={},
            )
            for index, status in ((1, "SUCCESS"), (2, "FAILED"))
        ]
        result_text = _batch_result_text("PARTIAL", children)
        self.assertIn("Передача завершена частично", result_text)
        self.assertIn("Подарок 1 — ✅", result_text)
        self.assertIn("Подарок 2 — ❌", result_text)


if __name__ == "__main__":
    unittest.main()
