from __future__ import annotations

import asyncio
import logging
import tempfile
import unittest
from pathlib import Path
from typing import cast

from app.db import Database
from app.jobs import PortalsJobErrorCode, PortalsJobService, PortalsJobStatus
from app.telegram_client import (
    MiniAppSessionUnauthorizedError,
    PortalsAcceptResult,
    PortalsAcceptStatus,
    PortalsApiError,
    PortalsOffer,
    PortalsOfferChangedError,
    PortalsOfferNotFoundError,
    PortalsService,
)

OWNER_ID = 100
ACCOUNT_ID = 1
OFFER_ID = "opaque-offer"
NFT_ID = "opaque-nft"


def offer(
    offer_id: str | int = OFFER_ID,
    *,
    nft_id: str | int = NFT_ID,
    amount: str | float = "1.25",
    eligible: bool = True,
) -> PortalsOffer:
    return PortalsOffer(
        offer_id=offer_id,
        nft_id=nft_id,
        amount=amount,
        display_name=f"NFT {nft_id}",
        amount_text=str(amount),
        eligible=eligible,
        eligibility_reason=None if eligible else "Оффер заблокирован.",
    )


def accept_result(status: PortalsAcceptStatus) -> PortalsAcceptResult:
    sent = status is not PortalsAcceptStatus.DRY_RUN
    return PortalsAcceptResult(
        status=status,
        display_name="NFT",
        amount_text="1.25",
        accept_request_sent=sent,
        response_status=200 if status is PortalsAcceptStatus.SUCCESS else None,
        response_fields=("success",) if status is PortalsAcceptStatus.SUCCESS else (),
        verification_fields=(
            ("received_offers_refreshed",)
            if status is PortalsAcceptStatus.AMBIGUOUS
            else ()
        ),
        message="Безопасный результат.",
    )


class FakePortalsService:
    def __init__(self) -> None:
        self.offer_batches: list[list[PortalsOffer] | Exception] = []
        self.result = accept_result(PortalsAcceptStatus.SUCCESS)
        self.accept_error: Exception | None = None
        self.accept_calls = 0
        self.accepted_ids: list[str | int] = []
        self.active = 0
        self.max_active = 0
        self.block_once = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def get_received_offers(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[PortalsOffer]:
        del account_id, owner_telegram_id
        value = self.offer_batches.pop(0) if self.offer_batches else []
        if isinstance(value, Exception):
            raise value
        return value

    async def accept_offer(
        self,
        *,
        owner_telegram_id: int,
        account_id: int,
        offer_id: object,
        nft_id: object,
        amount: object,
        display_name: str,
    ) -> PortalsAcceptResult:
        del owner_telegram_id, account_id, nft_id, amount, display_name
        self.accept_calls += 1
        assert isinstance(offer_id, (str, int)) and not isinstance(offer_id, bool)
        self.accepted_ids.append(offer_id)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.block_once:
                self.block_once = False
                self.started.set()
                await self.release.wait()
            if self.accept_error is not None:
                raise self.accept_error
            return self.result
        finally:
            self.active -= 1


class PortalsJobTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp_dir.name) / "app.db")
        await self.db.initialize()
        await self.db.save_account(
            owner_telegram_id=OWNER_ID,
            telegram_account_id=9001,
            session_key="internal-session",
            username="owner",
            first_name="Владелец",
            phone=None,
        )
        self.fake = FakePortalsService()
        self.notifications: list[tuple[int, str]] = []

        async def notify(owner_id: int, text: str) -> None:
            self.notifications.append((owner_id, text))

        self.jobs = PortalsJobService(
            self.db,
            cast(PortalsService, self.fake),
            notifier=notify,
        )

    async def asyncTearDown(self) -> None:
        await self.jobs.shutdown()
        self.temp_dir.cleanup()

    async def test_zero_one_and_multiple_offer_selection(self) -> None:
        self.fake.offer_batches = [[], [offer()], [offer("a"), offer("b")]]

        empty = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)
        one = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)
        multiple = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)

        self.assertEqual(empty.error_code, PortalsJobErrorCode.EMPTY_OFFERS)
        self.assertEqual(one.status, PortalsJobStatus.SUCCESS)
        self.assertEqual(one.offer_id, OFFER_ID)
        self.assertEqual(multiple.error_code, PortalsJobErrorCode.AMBIGUOUS_OFFER)
        self.assertEqual(self.fake.accept_calls, 1)

    async def test_explicit_offer_id_is_exact_and_missing_is_safe(self) -> None:
        self.fake.offer_batches = [[offer(7)], [offer(7)], [offer("other")]]

        type_mismatch = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID, "7")
        exact = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID, 7)
        missing = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID, OFFER_ID)

        self.assertEqual(type_mismatch.error_code, PortalsJobErrorCode.OFFER_NOT_FOUND)
        self.assertEqual(exact.status, PortalsJobStatus.SUCCESS)
        self.assertIs(type(exact.offer_id), int)
        self.assertEqual(missing.error_code, PortalsJobErrorCode.OFFER_NOT_FOUND)

    async def test_offer_disappears_changes_or_becomes_ineligible(self) -> None:
        failures = (
            (PortalsOfferNotFoundError("missing"), PortalsJobErrorCode.OFFER_NOT_FOUND),
            (PortalsOfferChangedError("changed"), PortalsJobErrorCode.OFFER_CHANGED),
        )
        for error, code in failures:
            with self.subTest(code=code):
                self.fake.offer_batches = [[offer()]]
                self.fake.accept_error = error
                result = await self.jobs.execute_portals_job(
                    OWNER_ID, ACCOUNT_ID, OFFER_ID
                )
                self.assertEqual(result.error_code, code)
        self.fake.accept_error = None
        self.fake.offer_batches = [[offer(eligible=False)]]
        ineligible = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID, OFFER_ID)
        self.assertEqual(ineligible.error_code, PortalsJobErrorCode.OFFER_NOT_ELIGIBLE)

    async def test_revoked_session_and_secret_redaction(self) -> None:
        self.fake.offer_batches = [
            MiniAppSessionUnauthorizedError("auth-key-secret-value")
        ]
        with self.assertLogs("app.jobs.portals_jobs", logging.WARNING) as logs:
            result = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)

        self.assertEqual(result.error_code, PortalsJobErrorCode.SESSION_REVOKED)
        combined = " ".join(logs.output) + (result.error_message or "")
        self.assertNotIn("auth-key-secret-value", combined)

    async def test_dry_run_success_and_ambiguous_are_persisted(self) -> None:
        self.fake.offer_batches = [[offer()], [offer()], [offer()]]
        self.fake.result = accept_result(PortalsAcceptStatus.DRY_RUN)
        dry_run = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)
        self.fake.result = accept_result(PortalsAcceptStatus.SUCCESS)
        success = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)
        self.fake.result = accept_result(PortalsAcceptStatus.AMBIGUOUS)
        ambiguous = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)

        self.assertEqual(dry_run.status, PortalsJobStatus.DRY_RUN)
        self.assertFalse(dry_run.result_metadata["accept_request_sent"])
        self.assertEqual(success.status, PortalsJobStatus.SUCCESS)
        self.assertEqual(ambiguous.status, PortalsJobStatus.AMBIGUOUS)
        self.assertEqual(ambiguous.error_code, PortalsJobErrorCode.ACCEPT_AMBIGUOUS)
        persisted = await self.db.get_portals_job(success.id, OWNER_ID)
        self.assertIsNotNone(persisted)
        self.assertEqual(persisted.status if persisted else None, "SUCCESS")

    async def test_duplicate_execution_sends_one_accept(self) -> None:
        self.fake.offer_batches = [[offer()]]
        pending = await self.db.create_portals_job(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            offer_id=OFFER_ID,
        )

        first, second = await asyncio.gather(
            self.jobs.execute_existing_job(pending.id, OWNER_ID),
            self.jobs.execute_existing_job(pending.id, OWNER_ID),
        )

        self.assertEqual(first.status, PortalsJobStatus.SUCCESS)
        self.assertEqual(second.status, PortalsJobStatus.SUCCESS)
        self.assertEqual(self.fake.accept_calls, 1)

    async def test_concurrent_jobs_for_same_account_are_serialized(self) -> None:
        self.fake.offer_batches = [[offer("one")], [offer("two")]]
        self.fake.block_once = True

        first_task = asyncio.create_task(
            self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID, "one")
        )
        await self.fake.started.wait()
        second_task = asyncio.create_task(
            self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID, "two")
        )
        await asyncio.sleep(0)
        self.fake.release.set()
        first, second = await asyncio.gather(first_task, second_task)

        self.assertEqual(first.status, PortalsJobStatus.SUCCESS)
        self.assertEqual(second.status, PortalsJobStatus.SUCCESS)
        self.assertEqual(self.fake.max_active, 1)

    async def test_server_rejection_and_rate_limit(self) -> None:
        self.fake.offer_batches = [[offer()], [offer()]]
        self.fake.accept_error = PortalsApiError(400)
        rejected = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)
        self.fake.accept_error = PortalsApiError(429)
        limited = await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)

        self.assertEqual(rejected.error_code, PortalsJobErrorCode.ACCEPT_REJECTED)
        self.assertEqual(limited.error_code, PortalsJobErrorCode.RATE_LIMITED)

    async def test_interrupted_running_job_is_never_replayed(self) -> None:
        pending = await self.db.create_portals_job(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            offer_id=OFFER_ID,
        )
        claimed = await self.db.claim_portals_job(pending.id)
        self.assertIsNotNone(claimed)

        recovered_count = await self.db.recover_running_portals_jobs()
        recovered = await self.jobs.execute_existing_job(pending.id, OWNER_ID)

        self.assertEqual(recovered_count, 1)
        self.assertEqual(recovered.status, PortalsJobStatus.AMBIGUOUS)
        self.assertEqual(recovered.error_code, PortalsJobErrorCode.ACCEPT_AMBIGUOUS)
        self.assertEqual(self.fake.accept_calls, 0)

    async def test_notification_is_russian(self) -> None:
        self.fake.offer_batches = [[offer()]]
        await self.jobs.execute_portals_job(OWNER_ID, ACCOUNT_ID)
        text = self.notifications[-1][1]
        self.assertIn("✅ Оффер принят", text)
