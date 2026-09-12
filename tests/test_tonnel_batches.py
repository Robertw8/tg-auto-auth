from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from collections import Counter
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import cast

from app.db import ActiveTransferConflictError, Database, TransferJob
from app.jobs import TonnelTransferBatchService, TonnelTransferJobService
from app.telegram_client import (
    TonnelAuthenticationError,
    TonnelBalance,
    TonnelMarketGift,
    TonnelMarketGiftIdentity,
    TonnelService,
    TonnelTransferMode,
)


class _BatchAuth:
    def __init__(self, n1_account_id: int, n2_account_id: int) -> None:
        self.n1_account_id = n1_account_id
        self.n2_account_id = n2_account_id
        self.child_count = 0

    def set_child_count(self, child_count: int) -> None:
        self.child_count = child_count

    def safe_metadata(self) -> dict[str, object]:
        return {
            "n1_authenticated_once": True,
            "n2_authenticated_once": True,
            "n1_account_id": self.n1_account_id,
            "n2_account_id": self.n2_account_id,
            "child_count": self.child_count,
            "telethon_launch_count_n1": 1,
            "telethon_launch_count_n2": 1,
        }


def gift(index: int, *, source_id: int | None = None) -> TonnelMarketGift:
    exact_id = source_id if source_id is not None else 100 + index
    return TonnelMarketGift(
        display_name=f"Подарок {index}",
        identity=TonnelMarketGiftIdentity(
            source_gift_id=exact_id,
            sale_id=f"sale-{index}",
            collectible_slug=f"gift-{index}",
        ),
        eligible=True,
        eligibility_reason=None,
        seller_telegram_id=1002,
    )


class _TonnelPreflight:
    def __init__(
        self,
        gifts: list[TonnelMarketGift],
        *,
        balance: Decimal = Decimal(100),
        auth_failure: bool = False,
    ) -> None:
        self.gifts = gifts
        self.balance = balance
        self.inventory_calls = 0
        self.balance_calls = 0
        self.offer_accept_delay_ms = 5000
        self.auth_failure = auth_failure
        self.n1_auth_calls = 0
        self.n2_auth_calls = 0
        self.auth_context_active = False

    @asynccontextmanager
    async def batch_auth_context(
        self,
        *,
        owner_telegram_id: int,
        n1_account_id: int,
        n2_account_id: int,
    ):
        del owner_telegram_id
        self.n1_auth_calls += 1
        if self.auth_failure:
            raise TonnelAuthenticationError("Безопасная ошибка авторизации")
        self.n2_auth_calls += 1
        self.auth_context_active = True
        try:
            yield _BatchAuth(n1_account_id, n2_account_id)
        finally:
            self.auth_context_active = False

    async def get_market_inventory(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[TonnelMarketGift]:
        del account_id, owner_telegram_id
        assert self.auth_context_active
        self.inventory_calls += 1
        return list(self.gifts)

    async def get_balance(
        self, account_id: int, *, owner_telegram_id: int
    ) -> TonnelBalance:
        del account_id, owner_telegram_id
        assert self.auth_context_active
        self.balance_calls += 1
        return TonnelBalance(self.balance, Decimal(0), Decimal(0), None)

    @staticmethod
    def normalize_market_price(value: str | Decimal) -> Decimal:
        return TonnelService.normalize_market_price(value)

    @staticmethod
    def format_market_price(value: str | Decimal) -> str:
        return TonnelService.format_market_price(value)

    @staticmethod
    def buy_offer_quote(value: str | Decimal):
        return TonnelService.buy_offer_quote(value)


class _ChildRunner:
    transfer_mode = TonnelTransferMode.BUY_OFFER

    def __init__(
        self,
        db: Database,
        *,
        statuses: dict[int, str] | None = None,
        delay: float = 0.0,
    ) -> None:
        self.db = db
        self.statuses = statuses or {}
        self.delay = delay
        self.calls: list[int] = []
        self.create_counts: Counter[int] = Counter()
        self.accept_counts: Counter[int] = Counter()
        self.running = 0
        self.max_running = 0
        self.offer_accept_delay_ms = 5000
        self.observed_accept_delays: list[int] = []
        self.observed_batch_auth: list[dict[str, object]] = []
        self._lock = asyncio.Lock()

    async def execute_reserved_tonnel_transfer(
        self,
        job_id: int,
        *,
        progress: object = None,
        batch_auth_metadata: dict[str, object] | None = None,
    ) -> TransferJob:
        del progress
        job = await self.db.claim_transfer_job(job_id)
        assert job is not None
        assert isinstance(job.asset_id, int)
        asset = job.asset_id
        async with self._lock:
            self.calls.append(asset)
            self.observed_accept_delays.append(self.offer_accept_delay_ms)
            self.observed_batch_auth.append(dict(batch_auth_metadata or {}))
            self.running += 1
            self.max_running = max(self.max_running, self.running)
            if self.statuses.get(asset) != "DRY_RUN":
                self.create_counts[asset] += 1
        try:
            await asyncio.sleep(self.delay)
            status = self.statuses.get(asset, "SUCCESS")
            if status == "SUCCESS":
                self.accept_counts[asset] += 1
            return await self.db.finish_transfer_job(
                job.id,
                status=status,
                phase="COMPLETED",
                error_code=status if status in {"FAILED", "AMBIGUOUS"} else None,
                error_message=(
                    "Безопасная ошибка" if status in {"FAILED", "AMBIGUOUS"} else None
                ),
                result_metadata={
                    "batch_auth": dict(batch_auth_metadata or {}),
                    "mutation_counts": {
                        "tonnel_offer_create": self.create_counts[asset],
                        "tonnel_offer_accept": self.accept_counts[asset],
                    }
                },
            )
        finally:
            async with self._lock:
                self.running -= 1


class TonnelBatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "app.db"
        self.db = Database(self.path)
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
        tonnel: _TonnelPreflight,
        runner: _ChildRunner,
        *,
        max_concurrency: int = 5,
    ) -> TonnelTransferBatchService:
        return TonnelTransferBatchService(
            self.db,
            cast(TonnelService, tonnel),
            cast(TonnelTransferJobService, runner),
            offer_amount=Decimal(2),
            max_concurrency=max_concurrency,
        )

    async def execute(self, service: TonnelTransferBatchService):
        return await service.execute_batch(
            owner_telegram_id=100,
            owner_account_id=1,
            target_account_id=2,
        )

    def job_count(self) -> int:
        connection = sqlite3.connect(self.path)
        try:
            row = connection.execute(
                "SELECT COUNT(*) FROM transfer_jobs WHERE market = 'tonnel'"
            ).fetchone()
            assert row is not None
            return int(row[0])
        finally:
            connection.close()

    async def test_three_gifts_create_three_exact_independent_jobs(self) -> None:
        tonnel = _TonnelPreflight([gift(1), gift(2), gift(3)])
        runner = _ChildRunner(self.db)
        result = await self.execute(self.service(tonnel, runner))

        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual([job.asset_id for job in result.children], [101, 102, 103])
        self.assertEqual({job.amount_text for job in result.children}, {"2"})
        self.assertEqual({job.amount_atomic for job in result.children}, {2_000_000_000})
        self.assertEqual(sum(runner.create_counts.values()), 3)
        self.assertEqual(sum(runner.accept_counts.values()), 3)
        self.assertEqual(tonnel.inventory_calls, 1)
        self.assertEqual(tonnel.balance_calls, 1)
        self.assertEqual(tonnel.n1_auth_calls, 1)
        self.assertEqual(tonnel.n2_auth_calls, 1)
        self.assertEqual(
            [metadata["child_count"] for metadata in runner.observed_batch_auth],
            [3, 3, 3],
        )
        self.assertTrue(
            all(
                metadata["telethon_launch_count_n1"] == 1
                and metadata["telethon_launch_count_n2"] == 1
                for metadata in runner.observed_batch_auth
            )
        )
        for child in result.children:
            self.assertEqual(child.batch_id, result.batch_id)
            self.assertEqual(child.result_metadata["batch_auth"]["child_count"], 3)
            rendered = repr(child.result_metadata["batch_auth"]).lower()
            for secret in ("authdata", "initdata", "token", "secret-token"):
                self.assertNotIn(secret, rendered)
        batch = await self.db.get_tonnel_transfer_batch(result.batch_id, 100)
        self.assertIsNotNone(batch)
        assert batch is not None
        self.assertEqual(batch.status, "SUCCESS")
        self.assertEqual(batch.total_count, 3)

    async def test_failure_and_ambiguity_do_not_stop_other_gifts(self) -> None:
        tonnel = _TonnelPreflight([gift(1), gift(2), gift(3), gift(4)])
        runner = _ChildRunner(
            self.db, statuses={102: "FAILED", 103: "AMBIGUOUS"}
        )
        result = await self.execute(self.service(tonnel, runner))

        self.assertEqual(result.status, "PARTIAL")
        self.assertEqual(result.progress.success_count, 2)
        self.assertEqual(result.progress.failed_count, 1)
        self.assertEqual(result.progress.ambiguous_count, 1)
        self.assertEqual(len(runner.calls), 4)
        self.assertEqual(
            result.progress.total_count,
            result.progress.success_count
            + result.progress.dry_run_count
            + result.progress.failed_count
            + result.progress.ambiguous_count,
        )

    async def test_duplicate_identity_and_active_gift_are_excluded(self) -> None:
        await self.db.create_transfer_job(
            owner_telegram_id=100,
            market="tonnel",
            owner_account_id=1,
            target_account_id=2,
            asset_id=103,
            amount_text="2",
            confirmation_key="active",
        )
        duplicate_a = gift(1, source_id=101)
        duplicate_b = TonnelMarketGift(
            display_name="Дубликат",
            identity=TonnelMarketGiftIdentity(101, "other-sale", "duplicate"),
            eligible=True,
            eligibility_reason=None,
            seller_telegram_id=1002,
        )
        tonnel = _TonnelPreflight(
            [duplicate_a, duplicate_b, gift(2), gift(3)]
        )
        runner = _ChildRunner(self.db)
        result = await self.execute(self.service(tonnel, runner))

        self.assertEqual([job.asset_id for job in result.children], [102])
        self.assertEqual(runner.calls, [102])

    async def test_insufficient_balance_starts_zero_jobs_or_mutations(self) -> None:
        tonnel = _TonnelPreflight(
            [gift(1), gift(2)], balance=Decimal("4.01")
        )
        runner = _ChildRunner(self.db)
        result = await self.execute(self.service(tonnel, runner))

        self.assertEqual(result.error_code, "INSUFFICIENT_BATCH_BALANCE")
        self.assertEqual(result.total_required, Decimal("4.02"))
        self.assertEqual(self.job_count(), 0)
        self.assertEqual(runner.calls, [])

    async def test_batch_auth_failure_starts_zero_jobs_or_mutations(self) -> None:
        tonnel = _TonnelPreflight([gift(1), gift(2)], auth_failure=True)
        runner = _ChildRunner(self.db)

        with self.assertRaises(TonnelAuthenticationError):
            await self.execute(self.service(tonnel, runner))

        self.assertEqual(tonnel.n1_auth_calls, 1)
        self.assertEqual(tonnel.n2_auth_calls, 0)
        self.assertEqual(self.job_count(), 0)
        self.assertEqual(runner.calls, [])

    async def test_exact_balance_starts_complete_batch(self) -> None:
        tonnel = _TonnelPreflight(
            [gift(1), gift(2)], balance=Decimal("4.02")
        )
        runner = _ChildRunner(self.db)
        result = await self.execute(self.service(tonnel, runner))
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(len(result.children), 2)

    async def test_max_concurrency_is_respected(self) -> None:
        tonnel = _TonnelPreflight([gift(i) for i in range(8)])
        runner = _ChildRunner(self.db, delay=0.02)
        await self.execute(self.service(tonnel, runner, max_concurrency=3))
        self.assertEqual(runner.max_running, 3)

    async def test_each_batch_job_uses_configured_accept_delay(self) -> None:
        tonnel = _TonnelPreflight([gift(1), gift(2), gift(3)])
        runner = _ChildRunner(self.db)
        service = self.service(tonnel, runner)
        await self.execute(service)

        self.assertEqual(service.offer_accept_delay_ms, 5000)
        self.assertEqual(runner.observed_accept_delays, [5000, 5000, 5000])

    async def test_double_click_does_not_launch_second_batch(self) -> None:
        tonnel = _TonnelPreflight([gift(1), gift(2), gift(3)])
        runner = _ChildRunner(self.db, delay=0.05)
        service = self.service(tonnel, runner)
        first = asyncio.create_task(self.execute(service))
        while not runner.calls:
            await asyncio.sleep(0)
        with self.assertRaises(ActiveTransferConflictError):
            await self.execute(service)
        await first
        self.assertEqual(sum(runner.create_counts.values()), 3)
        self.assertEqual(sum(runner.accept_counts.values()), 3)

    async def test_three_rapid_launches_use_one_persisted_batch(self) -> None:
        tonnel = _TonnelPreflight([gift(1), gift(2)])
        runner = _ChildRunner(self.db, delay=0.05)
        services = [self.service(tonnel, runner) for _ in range(3)]

        first = asyncio.create_task(self.execute(services[0]))
        while not runner.calls:
            await asyncio.sleep(0)
        duplicates = await asyncio.gather(
            self.execute(services[1]),
            self.execute(services[2]),
            return_exceptions=True,
        )
        result = await first

        self.assertTrue(
            all(isinstance(item, ActiveTransferConflictError) for item in duplicates)
        )
        self.assertEqual(self.job_count(), 2)
        self.assertEqual(sum(runner.create_counts.values()), 2)
        self.assertEqual(sum(runner.accept_counts.values()), 2)
        connection = sqlite3.connect(self.path)
        try:
            row = connection.execute(
                "SELECT COUNT(*) FROM tonnel_transfer_batches"
            ).fetchone()
            assert row is not None
            self.assertEqual(int(row[0]), 1)
        finally:
            connection.close()
        self.assertTrue(
            await self.db.claim_tonnel_batch_final_notification(result.batch_id)
        )
        self.assertFalse(
            await self.db.claim_tonnel_batch_final_notification(result.batch_id)
        )

    async def test_dry_run_multiple_gifts_has_zero_mutations(self) -> None:
        tonnel = _TonnelPreflight([gift(1), gift(2), gift(3)])
        runner = _ChildRunner(
            self.db,
            statuses={101: "DRY_RUN", 102: "DRY_RUN", 103: "DRY_RUN"},
        )
        result = await self.execute(self.service(tonnel, runner))

        self.assertEqual(result.status, "DRY_RUN")
        self.assertEqual(result.progress.dry_run_count, 3)
        self.assertEqual(sum(runner.create_counts.values()), 0)
        self.assertEqual(sum(runner.accept_counts.values()), 0)


if __name__ == "__main__":
    unittest.main()
