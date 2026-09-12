from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import cast

from app.db import Database
from app.jobs import MrktJobErrorCode, MrktJobService, MrktJobStatus
from app.telegram_client import (
    MiniAppSessionUnauthorizedError,
    MrktApiError,
    MrktGift,
    MrktGiftUnavailableError,
    MrktListingResult,
    MrktListingStatus,
    MrktService,
)

OWNER_ID = 100
ACCOUNT_ID = 1
GIFT_ID = "opaque-gift"


def gift(gift_id: str | int = GIFT_ID, *, eligible: bool = True) -> MrktGift:
    return MrktGift(
        gift_id=gift_id,
        display_name=f"Gift {gift_id}",
        eligible=eligible,
        eligibility_reason=None if eligible else "Gift is transfer-locked.",
        sale_price_nanotons=None,
    )


def listing_result(status: MrktListingStatus) -> MrktListingResult:
    sent = status not in {MrktListingStatus.DRY_RUN}
    return MrktListingResult(
        status=status,
        display_name="Gift",
        price_ton="1.68",
        price_nanotons=1_680_000_000,
        sale_request_sent=sent,
        response_status=200 if status is MrktListingStatus.SUCCESS else None,
        response_fields=("ids",) if status is MrktListingStatus.SUCCESS else (),
        message="safe result",
    )


class FakeMrktService:
    ton_to_nanotons = staticmethod(MrktService.ton_to_nanotons)
    format_ton = staticmethod(MrktService.format_ton)

    def __init__(self) -> None:
        self.inventories: list[list[MrktGift] | Exception] = []
        self.execution_result = listing_result(MrktListingStatus.SUCCESS)
        self.execution_error: Exception | None = None
        self.execute_calls = 0
        self.execute_gift_ids: list[str | int] = []
        self.active = 0
        self.max_active = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.block_once = False

    async def get_inventory(
        self,
        account_id: int,
        *,
        owner_telegram_id: int,
        is_listed: bool = False,
    ) -> list[MrktGift]:
        del account_id, owner_telegram_id, is_listed
        value = self.inventories.pop(0) if self.inventories else []
        if isinstance(value, Exception):
            raise value
        return value

    async def execute_job_listing(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        gift_id: str | int,
        display_name: str,
        price_ton: str,
    ) -> MrktListingResult:
        del owner_telegram_id, account_id, display_name, price_ton
        self.execute_calls += 1
        self.execute_gift_ids.append(gift_id)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.block_once:
                self.block_once = False
                self.started.set()
                await self.release.wait()
            if self.execution_error is not None:
                raise self.execution_error
            return self.execution_result
        finally:
            self.active -= 1


class MrktJobTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp_dir.name) / "app.db")
        await self.db.initialize()
        await self.db.save_account(
            owner_telegram_id=OWNER_ID,
            telegram_account_id=9001,
            session_key="internal-session",
            username="owner",
            first_name="Owner",
            phone=None,
        )
        self.fake = FakeMrktService()
        self.notifications: list[tuple[int, str]] = []

        async def notify(owner_id: int, text: str) -> None:
            self.notifications.append((owner_id, text))

        self.jobs = MrktJobService(
            self.db,
            cast(MrktService, self.fake),
            notifier=notify,
        )

    async def asyncTearDown(self) -> None:
        await self.jobs.shutdown()
        self.temp_dir.cleanup()

    async def test_zero_one_and_multiple_gift_selection(self) -> None:
        self.fake.inventories = [[], [gift()], [gift("a"), gift("b")]]

        empty = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1")
        one = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1")
        multiple = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1")

        self.assertEqual(empty.error_code, MrktJobErrorCode.EMPTY_STORAGE)
        self.assertEqual(one.status, MrktJobStatus.SUCCESS)
        self.assertEqual(one.gift_id, GIFT_ID)
        self.assertEqual(multiple.error_code, MrktJobErrorCode.AMBIGUOUS_GIFT)
        self.assertEqual(self.fake.execute_calls, 1)

    async def test_explicit_gift_id_is_exact_and_opaque(self) -> None:
        self.fake.inventories = [[gift(7)], [gift(7)]]

        mismatch = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1", "7")
        match = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1", 7)

        self.assertEqual(mismatch.error_code, MrktJobErrorCode.GIFT_NOT_FOUND)
        self.assertEqual(match.status, MrktJobStatus.SUCCESS)
        self.assertEqual(match.gift_id, 7)
        self.assertIs(type(match.gift_id), int)

    async def test_ineligible_explicit_gift(self) -> None:
        self.fake.inventories = [[gift(eligible=False)]]

        result = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1", GIFT_ID)

        self.assertEqual(result.error_code, MrktJobErrorCode.GIFT_NOT_ELIGIBLE)
        self.assertEqual(self.fake.execute_calls, 0)

    async def test_revoked_session(self) -> None:
        self.fake.inventories = [MiniAppSessionUnauthorizedError("revoked-secret")]

        result = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1")

        self.assertEqual(result.error_code, MrktJobErrorCode.SESSION_REVOKED)
        self.assertNotIn("revoked-secret", result.error_message or "")

    async def test_price_conversion_and_invalid_price(self) -> None:
        self.fake.inventories = [[gift()]]

        valid = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1.68")
        invalid = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1.001")
        zero = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "0")

        self.assertEqual(valid.price_nanotons, 1_680_000_000)
        self.assertEqual(invalid.error_code, MrktJobErrorCode.INVALID_PRICE)
        self.assertEqual(zero.error_code, MrktJobErrorCode.INVALID_PRICE)

    async def test_dry_run_persists_without_sale(self) -> None:
        self.fake.inventories = [[gift()]]
        self.fake.execution_result = listing_result(MrktListingStatus.DRY_RUN)

        result = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1.68")

        self.assertEqual(result.status, MrktJobStatus.DRY_RUN)
        self.assertEqual(result.error_code, MrktJobErrorCode.SUCCESS)
        self.assertFalse(result.result_metadata["sale_request_sent"])

    async def test_successful_sale_and_persistence(self) -> None:
        self.fake.inventories = [[gift()]]

        result = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1.68")
        reopened = Database(Path(self.temp_dir.name) / "app.db")
        await reopened.initialize()
        persisted = await reopened.get_mrkt_job(result.id, OWNER_ID)

        self.assertEqual(result.status, MrktJobStatus.SUCCESS)
        self.assertEqual(result.error_code, MrktJobErrorCode.SUCCESS)
        self.assertEqual(self.fake.execute_calls, 1)
        self.assertIsNotNone(persisted)
        assert persisted is not None
        self.assertEqual(persisted.result_metadata["response_fields"], ["ids"])
        self.assertEqual(len(self.notifications), 1)

    async def test_interrupted_running_job_is_recovered_as_ambiguous(self) -> None:
        pending = await self.db.create_mrkt_job(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id=GIFT_ID,
            price_ton="1.68",
        )
        claimed = await self.db.claim_mrkt_job(pending.id)
        self.assertIsNotNone(claimed)

        recovered_count = await self.db.recover_running_mrkt_jobs()
        recovered = await self.db.get_mrkt_job(pending.id, OWNER_ID)

        self.assertEqual(recovered_count, 1)
        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(recovered.status, MrktJobStatus.AMBIGUOUS)
        self.assertEqual(recovered.error_code, MrktJobErrorCode.SALE_AMBIGUOUS)

    async def test_duplicate_execution_runs_sale_once(self) -> None:
        self.fake.inventories = [[gift()]]
        self.fake.block_once = True
        pending = await self.db.create_mrkt_job(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id=GIFT_ID,
            price_ton="1.68",
        )
        first = asyncio.create_task(
            self.jobs.execute_existing_job(pending.id, OWNER_ID)
        )
        await asyncio.wait_for(self.fake.started.wait(), timeout=1)
        second = asyncio.create_task(
            self.jobs.execute_existing_job(pending.id, OWNER_ID)
        )
        self.fake.release.set()

        first_result, second_result = await asyncio.gather(first, second)

        self.assertEqual(first_result.status, MrktJobStatus.SUCCESS)
        self.assertEqual(second_result.id, first_result.id)
        self.assertEqual(self.fake.execute_calls, 1)

    async def test_concurrent_jobs_for_same_account_are_serialized(self) -> None:
        self.fake.inventories = [[gift("a")], [gift("b")]]
        self.fake.block_once = True
        first_job = await self.db.create_mrkt_job(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id="a",
            price_ton="1",
        )
        second_job = await self.db.create_mrkt_job(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id="b",
            price_ton="1",
        )
        first = asyncio.create_task(
            self.jobs.execute_existing_job(first_job.id, OWNER_ID)
        )
        await asyncio.wait_for(self.fake.started.wait(), timeout=1)
        second = asyncio.create_task(
            self.jobs.execute_existing_job(second_job.id, OWNER_ID)
        )
        await asyncio.sleep(0)
        self.assertEqual(self.fake.execute_calls, 1)
        self.fake.release.set()

        await asyncio.gather(first, second)

        self.assertEqual(self.fake.execute_calls, 2)
        self.assertEqual(self.fake.max_active, 1)

    async def test_gift_disappears_during_final_revalidation(self) -> None:
        self.fake.inventories = [[gift()]]
        self.fake.execution_error = MrktGiftUnavailableError("raw detail")

        result = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1")

        self.assertEqual(result.error_code, MrktJobErrorCode.GIFT_NOT_FOUND)
        self.assertEqual(self.fake.execute_calls, 1)

    async def test_known_sale_rejection(self) -> None:
        self.fake.inventories = [[gift()]]
        self.fake.execution_error = MrktApiError(409, "GIFT_ALREADY_LISTED")

        result = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1")

        self.assertEqual(result.error_code, MrktJobErrorCode.SALE_REJECTED)
        self.assertEqual(self.fake.execute_calls, 1)

    async def test_timeout_resolution_success_and_unresolved_ambiguity(self) -> None:
        self.fake.inventories = [[gift()], [gift()]]
        self.fake.execution_result = listing_result(MrktListingStatus.SUCCESS)
        resolved = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1.68")
        self.fake.execution_result = listing_result(MrktListingStatus.AMBIGUOUS)
        unresolved = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1.68")

        self.assertEqual(resolved.status, MrktJobStatus.SUCCESS)
        self.assertEqual(unresolved.status, MrktJobStatus.AMBIGUOUS)
        self.assertEqual(unresolved.error_code, MrktJobErrorCode.SALE_AMBIGUOUS)
        self.assertEqual(self.fake.execute_calls, 2)

    async def test_secret_exception_is_not_logged_or_persisted(self) -> None:
        secret = "secret-init-data-and-token"
        self.fake.inventories = [RuntimeError(secret)]

        with self.assertLogs("app.jobs.mrkt_jobs", level="WARNING") as captured:
            result = await self.jobs.execute_mrkt_job(OWNER_ID, ACCOUNT_ID, "1")

        output = "\n".join(captured.output) + (result.error_message or "")
        self.assertNotIn(secret, output)
        self.assertEqual(result.error_code, MrktJobErrorCode.SERVER_ERROR)
