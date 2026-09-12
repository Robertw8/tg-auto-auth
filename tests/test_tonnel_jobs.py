from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from app.db import Database
from app.jobs import TonnelTransferJobService
from app.telegram_client import (
    TonnelBuyOfferTransferResult,
    TonnelExistingOfferDiagnosticResult,
    TonnelGift,
    TonnelGiftIdentity,
    TonnelMarketGift,
    TonnelMarketGiftIdentity,
    TonnelMarketTransferResult,
    TonnelMutationResult,
    TonnelPreflightFailure,
    TonnelService,
    TonnelTransferResult,
    TonnelTransferStatus,
    TonnelTransferStrategy,
)

CONTROL_USER = 100


class FakeTonnelService:
    def __init__(self) -> None:
        self.transfer_calls: list[dict[str, Any]] = []
        self.result_status = TonnelTransferStatus.DRY_RUN
        self.create_request_metadata: dict[str, object] = {}
        self.accept_request_metadata: dict[str, object] = {}
        self.ownership_verification: dict[str, object] = {}
        self.market_inventory_error: Exception | None = None
        self.gifts = [
            TonnelGift(
                display_name="Lol Pop #17",
                identity=TonnelGiftIdentity(91, "LolPop-17"),
                eligible=True,
                eligibility_reason=None,
            )
        ]
        self.market_gifts = [
            TonnelMarketGift(
                display_name="Lol Pop #17",
                identity=TonnelMarketGiftIdentity(91, "91", "LolPop-17"),
                eligible=True,
                eligibility_reason=None,
                seller_telegram_id=302,
            )
        ]

    async def get_inventory(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[TonnelGift]:
        del account_id, owner_telegram_id
        return list(self.gifts)

    async def transfer_exact_gift(self, **kwargs: Any) -> TonnelTransferResult:
        self.transfer_calls.append(dict(kwargs))
        status = self.result_status
        return TonnelTransferResult(
            status=status,
            display_name="Lol Pop #17",
            strategy=TonnelTransferStrategy.DIRECT_RECIPIENT,
            mutation_sent=status is not TonnelTransferStatus.DRY_RUN,
            response_status=200 if status is not TonnelTransferStatus.DRY_RUN else None,
            response_fields=("status",),
            source_owns_after=status is not TonnelTransferStatus.SUCCESS,
            destination_owns_after=status is TonnelTransferStatus.SUCCESS,
            message="готово",
        )

    async def get_market_inventory(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[TonnelMarketGift]:
        del account_id, owner_telegram_id
        if self.market_inventory_error is not None:
            raise self.market_inventory_error
        return list(self.market_gifts)

    @staticmethod
    def ton_to_nanotons(value: str) -> int:
        return TonnelService.ton_to_nanotons(value)

    @staticmethod
    def format_market_price(value: str) -> str:
        return TonnelService.format_market_price(value)

    async def transfer_market_sale(self, **kwargs: Any) -> TonnelMarketTransferResult:
        self.transfer_calls.append(dict(kwargs))
        return TonnelMarketTransferResult(
            status=TonnelTransferStatus.DRY_RUN,
            display_name="Lol Pop #17",
            sale_id="91",
            seller_price=Decimal(5),
            buyer_price=Decimal("5.025"),
            seller_price_nanotons=5_000_000_000,
            buyer_price_nanotons=5_025_000_000,
            commission_percent=Decimal("0.5"),
            listing_sent=False,
            buy_sent=False,
            listing_status=None,
            buy_status=None,
            listing_response_fields=(),
            buy_response_fields=(),
            source_owns_after=True,
            destination_owns_after=False,
            listing_still_active=False,
            error_code=None,
            timings_ms={
                "preparation_ms": 1,
                "listing_request_ms": None,
                "sale_id_known_ms": None,
                "sale_id_known_to_buy_start_ms": None,
                "buy_request_ms": None,
                "total_job_ms": 1,
            },
            message="готово",
        )

    async def transfer_buy_offer(self, **kwargs: Any) -> TonnelBuyOfferTransferResult:
        self.transfer_calls.append(dict(kwargs))
        quote = TonnelService.buy_offer_quote(str(kwargs["offer_amount"]))
        empty = TonnelMutationResult(
            request_sent=False,
            http_status=None,
            body_type="none",
            status=None,
            message=None,
            safe_field_names=(),
            duration_ms=None,
        )
        return TonnelBuyOfferTransferResult(
            status=TonnelTransferStatus.DRY_RUN,
            display_name="Lol Pop #17",
            offer_id=None,
            gift_id=91,
            quote=quote,
            create_result=empty,
            accept_result=empty,
            source_owns_after=True,
            destination_owns_after=False,
            offer_active_after=False,
            error_code=None,
            timings_ms={
                "preparation_ms": 1,
                "offer_create_request_ms": None,
                "offer_id_known_ms": None,
                "offer_id_known_to_accept_start_ms": None,
                "n2_offer_confirmed_ms": None,
                "accept_delay_ms": 1000,
                "n2_offer_confirmed_to_accept_start_ms": None,
                "offer_accept_request_ms": None,
                "total_job_ms": 1,
            },
            message="Тестовый запуск завершён.",
            offer_correlation={
                "pre_create_offer_count": 3,
                "post_create_offer_count": 4,
                "new_offer_count": 1,
                "new_offer_id_fingerprint": "safe-fingerprint",
                "new_offer_id_type": "str",
                "unique_new_offer_proven": True,
                "semantic_match_count": 1,
            },
            ownership_verification=self.ownership_verification,
            create_request_metadata=self.create_request_metadata,
            accept_request_metadata=self.accept_request_metadata,
        )

    async def accept_existing_buy_offer_diagnostic(
        self, **kwargs: Any
    ) -> TonnelExistingOfferDiagnosticResult:
        self.transfer_calls.append(dict(kwargs))
        confirmed = bool(kwargs["confirm"])
        accept = TonnelMutationResult(
            request_sent=confirmed,
            http_status=200 if confirmed else None,
            body_type="mapping" if confirmed else "none",
            status="success" if confirmed else None,
            message="accepted" if confirmed else None,
            safe_field_names=("message", "status") if confirmed else (),
            duration_ms=25 if confirmed else None,
        )
        return TonnelExistingOfferDiagnosticResult(
            status=(
                TonnelTransferStatus.SUCCESS
                if confirmed
                else TonnelTransferStatus.DRY_RUN
            ),
            display_name="Lol Pop #17",
            gift_id=91,
            offer_id="offer-1",
            offer_age_ms=120_000,
            candidate_count=1,
            validation={
                "candidate_count": 1,
                "gift_match": True,
                "amount_match": True,
                "asset_match": True,
                "buyer_match": True,
                "seller_match": True,
                "status": "pending",
            },
            accept_result=accept,
            accept_request_metadata={
                "api_origin": "https://gifts.coffin.meme",
                "endpoint": "/api/buyOffer/acceptBuyOffer",
                "offer_id_type": "str",
                "offer_id_fingerprint": "7c838c66a52b",
                "identity_match": True,
                "auth_context_created_at": "2026-09-11T12:00:00.000Z",
                "cookies_sent": False,
                "origin_set": True,
                "referer_set": True,
            },
            source_owns_after=not confirmed,
            destination_owns_after=confirmed,
            offer_active_after=not confirmed,
            error_code=None,
            message="готово",
        )


class TonnelTransferJobTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "app.db")
        await self.db.initialize()
        await self.db.save_account(
            owner_telegram_id=CONTROL_USER,
            telegram_account_id=300,
            session_key="owner-session",
            username="owner",
            first_name="Owner",
            phone=None,
            role="OWNER",
        )
        await self.db.save_account(
            owner_telegram_id=CONTROL_USER,
            telegram_account_id=302,
            session_key="target-session",
            username="target",
            first_name="Target",
            phone=None,
            role="TARGET",
        )
        accounts = await self.db.list_accounts(CONTROL_USER)
        self.owner_id = next(
            account.id for account in accounts if account.role == "OWNER"
        )
        self.target_id = next(
            account.id for account in accounts if account.role == "TARGET"
        )
        self.tonnel = FakeTonnelService()

    async def asyncTearDown(self) -> None:
        self.tempdir.cleanup()

    def jobs(self, *, dry_run: bool = True) -> TonnelTransferJobService:
        return TonnelTransferJobService(
            self.db, cast(TonnelService, self.tonnel), dry_run=dry_run
        )

    async def test_dry_run_persists_zero_mutations(self) -> None:
        job = await self.jobs().execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            confirmation_nonce="nonce-one",
        )
        self.assertEqual(job.market, "tonnel")
        self.assertEqual(job.status, "DRY_RUN")
        self.assertEqual(job.result_metadata["mutation_count"], 0)

    async def test_same_confirmation_replay_reuses_job(self) -> None:
        service = self.jobs()
        first = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            confirmation_nonce="same-nonce",
        )
        second = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            confirmation_nonce="same-nonce",
        )
        self.assertEqual(first.id, second.id)
        self.assertEqual(len(self.tonnel.transfer_calls), 1)

    async def test_fresh_confirmation_after_terminal_creates_new_job(self) -> None:
        service = self.jobs()
        first = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            confirmation_nonce="old-nonce",
        )
        second = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            confirmation_nonce="fresh-nonce",
        )
        self.assertGreater(second.id, first.id)
        self.assertEqual(len(self.tonnel.transfer_calls), 2)

    async def test_active_same_gift_collision_reuses_active_job(self) -> None:
        first = await self.db.create_transfer_job(
            owner_telegram_id=CONTROL_USER,
            market="tonnel",
            owner_account_id=self.owner_id,
            target_account_id=self.target_id,
            asset_id=91,
            amount_text="",
            confirmation_key=hashlib.sha256(b"active").hexdigest(),
        )
        claimed = await self.db.claim_transfer_job(first.id)
        self.assertIsNotNone(claimed)
        second = await self.db.create_transfer_job(
            owner_telegram_id=CONTROL_USER,
            market="tonnel",
            owner_account_id=self.owner_id,
            target_account_id=self.target_id,
            asset_id=91,
            amount_text="",
            confirmation_key=hashlib.sha256(b"fresh").hexdigest(),
        )
        self.assertEqual(first.id, second.id)

    async def test_raw_nonce_is_absent_from_database(self) -> None:
        raw_nonce = "raw-secret-confirmation"
        await self.jobs().execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            confirmation_nonce=raw_nonce,
        )
        with sqlite3.connect(Path(self.tempdir.name) / "app.db") as connection:
            stored = connection.execute(
                "SELECT confirmation_key FROM transfer_jobs WHERE market='tonnel'"
            ).fetchone()
        self.assertIsNotNone(stored)
        self.assertNotEqual(stored[0], raw_nonce)
        self.assertEqual(stored[0], hashlib.sha256(raw_nonce.encode()).hexdigest())

    async def test_success_records_one_mutation(self) -> None:
        self.tonnel.result_status = TonnelTransferStatus.SUCCESS
        job = await self.jobs(dry_run=False).execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            confirmation_nonce="live-nonce",
        )
        self.assertEqual(job.status, "SUCCESS")
        self.assertEqual(job.result_metadata["mutation_count"], 1)
        self.assertEqual(len(self.tonnel.transfer_calls), 1)

    async def test_market_dry_run_persists_price_and_zero_mutations(self) -> None:
        service = TonnelTransferJobService(
            self.db,
            cast(TonnelService, self.tonnel),
            dry_run=True,
            transfer_mode="MARKET_SALE",
        )
        job = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            "5",
            confirmation_nonce="market-nonce",
        )
        self.assertEqual(job.status, "DRY_RUN")
        self.assertEqual(job.amount_text, "5")
        self.assertEqual(job.amount_atomic, 5_000_000_000)
        self.assertEqual(
            job.result_metadata["mutation_counts"],
            {"tonnel_listing": 0, "tonnel_buy": 0},
        )
        self.assertEqual(job.external_ref, "91")

    async def test_buy_offer_dry_run_persists_safe_quote_and_zero_mutations(
        self,
    ) -> None:
        service = TonnelTransferJobService(
            self.db,
            cast(TonnelService, self.tonnel),
            dry_run=True,
            transfer_mode="BUY_OFFER",
        )
        job = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            "5",
            confirmation_nonce="buy-offer-nonce",
        )
        self.assertEqual(job.status, "DRY_RUN")
        self.assertEqual(job.amount_atomic, 5_000_000_000)
        self.assertEqual(job.result_metadata["strategy"], "BUY_OFFER")
        self.assertEqual(job.result_metadata["seller_proceeds"], "4.975")
        self.assertEqual(job.result_metadata["buyer_balance_required"], "5.01")
        self.assertEqual(
            job.result_metadata["mutation_counts"],
            {"tonnel_offer_create": 0, "tonnel_offer_accept": 0},
        )
        self.assertEqual(job.result_metadata["timings"]["accept_delay_ms"], 1000)
        self.assertIsNone(job.result_metadata["offer_id_fingerprint"])
        self.assertEqual(job.result_metadata["offer_correlation"]["new_offer_count"], 1)
        self.assertTrue(
            job.result_metadata["offer_correlation"]["unique_new_offer_proven"]
        )

    async def test_buy_offer_persists_only_safe_accept_request_metadata(self) -> None:
        self.tonnel.create_request_metadata = {
            "endpoint": "/api/buyOffer/create",
            "body_fields": ["authData", "gift_id", "amount", "asset"],
            "api_origin": "https://rs-api.tonnel.network",
        }
        self.tonnel.accept_request_metadata = {
            "endpoint": "/api/buyOffer/acceptBuyOffer",
            "body_fields": ["authData", "offer_id"],
            "offer_id_type": "str",
            "offer_id_fingerprint": "d04b21572d43",
            "identity_match": True,
            "cookies_sent": False,
        }
        self.tonnel.ownership_verification = {
            "timeout_ms": 15000,
            "probe_count": 3,
            "first_probe_ms": 0,
            "confirmed_ms": 2001,
            "source_owns_after": False,
            "destination_owns_after": True,
        }
        service = TonnelTransferJobService(
            self.db,
            cast(TonnelService, self.tonnel),
            dry_run=True,
            transfer_mode="BUY_OFFER",
        )

        job = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            "5",
            confirmation_nonce="accept-diagnostics",
        )

        stored = job.result_metadata["tonnel_offer_accept_request"]
        self.assertEqual(stored, self.tonnel.accept_request_metadata)
        self.assertEqual(
            job.result_metadata["tonnel_offer_create_request"],
            self.tonnel.create_request_metadata,
        )
        self.assertEqual(
            job.result_metadata["ownership_verification"],
            self.tonnel.ownership_verification,
        )
        self.assertNotIn("token", repr(stored).lower())
        self.assertNotIn("initdata", repr(stored).lower())

    async def test_buy_offer_persists_safe_preflight_failure_metadata(self) -> None:
        diagnostic: dict[str, object] = {
            "operation": "page_gifts",
            "api_origin": "https://gifts2.tonnel.network",
            "hostname": "gifts2.tonnel.network",
            "endpoint": "/api/pageGifts",
            "method": "POST",
            "started_at": "2026-09-11T12:00:00.000Z",
            "duration_ms": 321,
            "http_status": None,
            "exception_type": "ClientConnectorDNSError",
            "message": "Не удалось определить адрес сервера Tonnel.",
            "failure_kind": "dns",
        }
        self.tonnel.market_inventory_error = TonnelPreflightFailure(
            "Не удалось определить адрес сервера Tonnel.",
            error_code="NETWORK_ERROR",
            diagnostic=diagnostic,
        )
        service = TonnelTransferJobService(
            self.db,
            cast(TonnelService, self.tonnel),
            dry_run=False,
            transfer_mode="BUY_OFFER",
        )

        job = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            "5",
            confirmation_nonce="preflight-failure",
        )

        self.assertEqual(job.status, "FAILED")
        self.assertEqual(job.error_code, "NETWORK_ERROR")
        self.assertEqual(job.result_metadata["mutation_count"], 0)
        self.assertEqual(job.result_metadata["tonnel_preflight_error"], diagnostic)
        self.assertNotIn("authdata", repr(job.result_metadata).lower())
        self.assertEqual(len(self.tonnel.transfer_calls), 0)

    async def test_buy_offer_confirmation_replay_and_fresh_confirmation(self) -> None:
        service = TonnelTransferJobService(
            self.db,
            cast(TonnelService, self.tonnel),
            dry_run=True,
            transfer_mode="BUY_OFFER",
        )
        first = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            "5",
            confirmation_nonce="buy-offer-same",
        )
        replay = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            "5",
            confirmation_nonce="buy-offer-same",
        )
        fresh = await service.execute_tonnel_transfer(
            CONTROL_USER,
            self.owner_id,
            self.target_id,
            91,
            "5",
            confirmation_nonce="buy-offer-fresh",
        )
        self.assertEqual(first.id, replay.id)
        self.assertGreater(fresh.id, first.id)
        self.assertEqual(len(self.tonnel.transfer_calls), 2)

    async def test_existing_offer_diagnostic_persists_safe_single_shot_metadata(
        self,
    ) -> None:
        service = TonnelTransferJobService(
            self.db,
            cast(TonnelService, self.tonnel),
            dry_run=False,
            transfer_mode="BUY_OFFER",
        )

        job = await service.execute_existing_offer_diagnostic(
            owner_telegram_id=CONTROL_USER,
            owner_account_id=self.owner_id,
            target_account_id=self.target_id,
            gift_id=91,
            offer_amount="5",
            expected_offer_fingerprint="7c838c66a52b",
            confirm=True,
        )

        self.assertEqual(job.status, "SUCCESS")
        self.assertEqual(
            job.result_metadata["strategy"],
            "ACCEPT_EXISTING_OFFER_DIAGNOSTIC",
        )
        self.assertEqual(
            job.result_metadata["mutation_counts"],
            {"tonnel_offer_create": 0, "tonnel_offer_accept": 1},
        )
        self.assertEqual(job.result_metadata["candidate_count"], 1)
        self.assertEqual(job.result_metadata["offer_age_ms"], 120_000)
        self.assertNotIn("authdata", repr(job.result_metadata).lower())
        self.assertNotIn("token", repr(job.result_metadata).lower())


if __name__ == "__main__":
    unittest.main()
