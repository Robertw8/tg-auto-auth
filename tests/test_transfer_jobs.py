from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import tempfile
import unittest
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from app.db import Database
from app.jobs import MrktTransferJobService, PortalsTransferJobService
from app.live_verify import offer_checks
from app.telegram_client import (
    MrktFastTransferResult,
    MrktFastTransferStatus,
    MrktGift,
    MrktGiftUnavailableError,
    MrktListingResult,
    MrktListingStatus,
    MrktPurchaseResult,
    MrktPurchaseStatus,
    MrktService,
    MrktTransferMode,
    PortalsAcceptResult,
    PortalsAcceptStatus,
    PortalsCreateOfferResult,
    PortalsCreateOfferUnconfirmedError,
    PortalsNft,
    PortalsOffer,
    PortalsOfferChangedError,
    PortalsOfferNotFoundError,
    PortalsService,
)


class FakePortalsService:
    def __init__(self) -> None:
        self.nfts = [self.nft()]
        self.offers: list[PortalsOffer] = []
        self.placed_offers = [self.offer()]
        self.nft_offers = [self.offer()]
        self.create_error: Exception | None = None
        self.accept_status = PortalsAcceptStatus.SUCCESS
        self.accepted = False
        self.create_calls: list[dict[str, object]] = []
        self.accept_calls: list[dict[str, object]] = []

    @staticmethod
    def nft(nft_id: Any = "nft-7") -> PortalsNft:
        return PortalsNft(
            nft_id=nft_id,
            display_name="NFT #7",
            eligible=True,
            eligibility_reason=None,
        )

    @asynccontextmanager
    async def transfer_session_scope(self, *args: object) -> Any:
        del args
        yield

    @staticmethod
    def offer(
        *,
        offer_id: Any = "offer-9",
        nft_id: Any = "nft-7",
        amount: Any = "0.53",
        sender_id: Any = "sender-1",
    ) -> PortalsOffer:
        return PortalsOffer(
            offer_id=offer_id,
            nft_id=nft_id,
            amount=amount,
            sender_id=sender_id,
            display_name="NFT #7",
            amount_text=str(amount),
            eligible=True,
            eligibility_reason=None,
        )

    async def get_owned_nfts(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[PortalsNft]:
        del owner_telegram_id
        if self.accepted:
            return list(self.nfts) if account_id == 1 else []
        return list(self.nfts)

    async def create_offer(self, **kwargs: object) -> PortalsCreateOfferResult:
        self.create_calls.append(dict(kwargs))
        if self.create_error is not None:
            raise self.create_error
        return PortalsCreateOfferResult(
            offer_id=None if kwargs["dry_run"] else "offer-9",
            nft_id=kwargs["nft_id"],
            amount=str(kwargs["amount"]),
            request_sent=not bool(kwargs["dry_run"]),
            response_status=None if kwargs["dry_run"] else 201,
            response_fields=(),
            sender_id="sender-1",
        )

    async def get_received_offers(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[PortalsOffer]:
        del account_id, owner_telegram_id
        return list(self.offers)

    async def accept_offer(self, **kwargs: object) -> PortalsAcceptResult:
        self.accept_calls.append(dict(kwargs))
        self.accepted = self.accept_status is PortalsAcceptStatus.SUCCESS
        return PortalsAcceptResult(
            status=self.accept_status,
            display_name="NFT #7",
            amount_text="0.53",
            accept_request_sent=True,
            response_status=None,
            response_fields=(),
            verification_fields=(),
            message="готово",
        )

    async def accept_transfer_offer(self, **kwargs: object) -> PortalsAcceptResult:
        offer_id = kwargs["offer_id"]
        nft_id = kwargs["nft_id"]
        amount = kwargs["amount"]
        if not any(nft.nft_id == nft_id for nft in self.nfts):
            raise PortalsOfferNotFoundError("missing nft")
        for candidates in (self.placed_offers, self.nft_offers):
            exact = [offer for offer in candidates if offer.offer_id == offer_id]
            if len(exact) != 1:
                raise PortalsOfferNotFoundError("missing offer")
            offer = exact[0]
            if offer.nft_id != nft_id or Decimal(str(offer.amount)) != Decimal(
                str(amount)
            ):
                raise PortalsOfferChangedError("changed offer")
            if not offer.eligible:
                raise PortalsOfferChangedError("inactive offer")
            expected_sender = kwargs.get("expected_sender_id")
            if (
                offer.sender_id is not None
                and expected_sender is not None
                and offer.sender_id != expected_sender
            ):
                raise PortalsOfferChangedError("changed sender")
        current = self.nft_offers[0]
        offer_checks(
            "portals_job_before_accept",
            actual_id=current.offer_id,
            expected_id=offer_id,
            actual_nft=current.nft_id,
            expected_nft=nft_id,
            actual_amount=current.amount,
            expected_amount=amount,
            actual_sender=current.sender_id,
            expected_sender=kwargs.get("expected_sender_id"),
            match_count=1,
            placed_match=True,
            nft_offer_match=True,
            received_match=False,
            n2_owns_nft=True,
        )
        return await self.accept_offer(**kwargs)


class FakeMrktService:
    def __init__(self) -> None:
        self.gifts = [self.gift()]
        self.listed_gifts = [self.gift(listed=True, seller_id="seller-2")]
        self.market_gift: MrktGift | None = self.gift(listed=True, seller_id="seller-2")
        self.listing_status = MrktListingStatus.SUCCESS
        self.purchase_status = MrktPurchaseStatus.SUCCESS
        self.listing_calls: list[dict[str, object]] = []
        self.purchase_calls: list[dict[str, object]] = []
        self.external_sale = False
        self.final_unavailable = False

    @staticmethod
    def gift(
        gift_id: Any = "gift-7",
        *,
        listed: bool = False,
        seller_id: Any = None,
    ) -> MrktGift:
        return MrktGift(
            gift_id=gift_id,
            display_name="Подарок #7",
            eligible=True,
            eligibility_reason=None,
            sale_price_nanotons=1_680_000_000 if listed else None,
            seller_id=seller_id,
        )

    @staticmethod
    def ton_to_nanotons(value: str) -> int:
        return MrktService.ton_to_nanotons(value)

    @staticmethod
    def format_ton(value: str) -> str:
        return MrktService.format_ton(value)

    async def get_inventory(
        self, account_id: int, *, owner_telegram_id: int, is_listed: bool = False
    ) -> list[MrktGift]:
        del account_id, owner_telegram_id
        return list(self.listed_gifts if is_listed else self.gifts)

    async def execute_job_listing(self, **kwargs: object) -> MrktListingResult:
        self.listing_calls.append(dict(kwargs))
        dry_run = bool(kwargs["dry_run_override"])
        return MrktListingResult(
            status=MrktListingStatus.DRY_RUN if dry_run else self.listing_status,
            display_name="Подарок #7",
            price_ton="1.68",
            price_nanotons=1_680_000_000,
            sale_request_sent=not dry_run,
            response_status=None,
            response_fields=(),
            message="готово",
        )

    async def get_marketplace_gift_by_id(
        self, *args: object, **kwargs: object
    ) -> MrktGift | None:
        del args, kwargs
        return self.market_gift

    async def buy_listing(self, **kwargs: object) -> MrktPurchaseResult:
        self.purchase_calls.append(dict(kwargs))
        return MrktPurchaseResult(
            status=self.purchase_status,
            display_name="Подарок #7",
            price_nanotons=1_680_000_000,
            buy_request_sent=True,
            response_status=None,
            response_fields=(),
        )

    async def execute_fast_transfer(
        self, **kwargs: object
    ) -> MrktFastTransferResult:
        gift_id = kwargs["gift_id"]
        if gift_id is None:
            eligible = [gift for gift in self.gifts if gift.eligible]
            if len(eligible) != 1:
                raise MrktGiftUnavailableError("missing or ambiguous")
            selected = eligible[0]
        else:
            matches = [
                gift
                for gift in self.gifts
                if gift.eligible
                and type(gift.gift_id) is type(gift_id)
                and gift.gift_id == gift_id
            ]
            if len(matches) != 1:
                raise MrktGiftUnavailableError("missing or ambiguous")
            selected = matches[0]
        dry_run = bool(kwargs["dry_run"])
        listing_call = {
            "account_id": kwargs["seller_account_id"],
            "gift_id": selected.gift_id,
            "price_nanotons": kwargs["price_nanotons"],
            "dry_run": dry_run,
            "speculative_buy": kwargs.get("speculative_buy", False),
            "speculative_buy_delay_ms": kwargs.get(
                "speculative_buy_delay_ms", 40
            ),
            "observe_listing_activation": kwargs.get(
                "observe_listing_activation", False
            ),
            "transfer_mode": str(kwargs.get("transfer_mode", "")),
            "listed_trigger_max_wait_ms": kwargs.get(
                "listed_trigger_max_wait_ms", 1000
            ),
            "listed_trigger_poll_interval_ms": kwargs.get(
                "listed_trigger_poll_interval_ms", 10
            ),
        }
        self.listing_calls.append(listing_call)
        listing_sent = not dry_run
        buy_sent = False
        verification: dict[str, object] = {"outcome": "not_run"}
        if dry_run:
            status = MrktFastTransferStatus.DRY_RUN
        elif self.listing_status is MrktListingStatus.AMBIGUOUS:
            status = MrktFastTransferStatus.LISTING_AMBIGUOUS
            verification = {"outcome": "inconclusive"}
        elif self.listing_status is not MrktListingStatus.SUCCESS:
            status = MrktFastTransferStatus.LISTING_REJECTED
        else:
            buy_sent = True
            buyer_price_nanotons = MrktService.seller_to_buyer_price_nanotons(
                cast(int, kwargs["price_nanotons"])
            )
            self.purchase_calls.append(
                {
                    "account_id": kwargs["buyer_account_id"],
                    "gift_id": selected.gift_id,
                    "expected_price_nanotons": buyer_price_nanotons,
                }
            )
            if self.external_sale:
                status = MrktFastTransferStatus.EXTERNAL_SALE
                verification = {
                    "outcome": "external_sale",
                    "buyer_unlisted_present": False,
                    "buyer_listed_present": False,
                    "seller_unlisted_present": False,
                    "seller_listed_present": False,
                }
            elif self.purchase_status is MrktPurchaseStatus.SUCCESS:
                status = MrktFastTransferStatus.SUCCESS
                verification = {"outcome": "consistent"}
            elif self.purchase_status is MrktPurchaseStatus.REJECTED:
                status = MrktFastTransferStatus.BUY_REJECTED
                verification = {"outcome": "listing_still_active"}
            else:
                status = MrktFastTransferStatus.BUY_AMBIGUOUS
                verification = {"outcome": "unavailable"}
        return MrktFastTransferResult(
            status=status,
            display_name=selected.display_name,
            gift_id=selected.gift_id,
            price_nanotons=cast(int, kwargs["price_nanotons"]),
            buyer_price_nanotons=MrktService.seller_to_buyer_price_nanotons(
                cast(int, kwargs["price_nanotons"])
            ),
            commission_percent=2,
            listing_request_sent=listing_sent,
            buy_request_sent=buy_sent,
            listing_response_status=200 if listing_sent else None,
            listing_response_fields=("ids",) if listing_sent else (),
            buy_response_status=200 if buy_sent else None,
            buy_response_fields=("ids",) if buy_sent else (),
            verification=verification,
            listing_observation={
                "enabled": bool(kwargs.get("observe_listing_activation", False)),
                "observation_stopped_reason": "TEST",
                "samples": [],
            },
            listed_trigger={
                "enabled": kwargs.get("transfer_mode")
                is MrktTransferMode.LISTED_TRIGGERED,
                "mode": str(kwargs.get("transfer_mode", "")),
                "probe_count": 0,
                "probes": [],
                "probe_rtts_ms": [],
                "stopped_reason": "TEST",
            },
            timings={
                "mrkt_auth_n1_ms": 1.0,
                "mrkt_auth_n2_ms": 1.0,
                "mrkt_preparation_ms": 2.0,
                "mrkt_listing_request_ms": 1.0 if listing_sent else None,
                "mrkt_listing_to_buy_start_ms": 0.1 if buy_sent else None,
                "mrkt_buy_request_ms": 1.0 if buy_sent else None,
                "mrkt_final_verification_ms": 1.0 if buy_sent else None,
                "mrkt_total_job_ms": 4.0,
            },
        )


class TransferJobTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temporary.name) / "app.db")
        await self.db.initialize()
        await self.db.save_account(
            owner_telegram_id=100,
            telegram_account_id=1001,
            session_key="owner-session",
            username="owner",
            first_name=None,
            phone=None,
            role="OWNER",
        )
        await self.db.save_account(
            owner_telegram_id=100,
            telegram_account_id=1002,
            session_key="target-session",
            username="target",
            first_name=None,
            phone=None,
            role="TARGET",
        )

    async def asyncTearDown(self) -> None:
        self.temporary.cleanup()

    def reset_jobs(self) -> None:
        connection = sqlite3.connect(Path(self.temporary.name) / "app.db")
        try:
            with connection:
                connection.execute("DELETE FROM transfer_jobs")
        finally:
            connection.close()

    async def test_roles_are_owner_scoped_and_filterable(self) -> None:
        await self.db.save_account(
            owner_telegram_id=200,
            telegram_account_id=2002,
            session_key="other-target",
            username="other",
            first_name=None,
            phone=None,
            role="TARGET",
        )

        owners = await self.db.list_accounts_by_role(100, "OWNER")
        targets = await self.db.list_accounts_by_role(100, "TARGET")

        self.assertEqual([account.id for account in owners], [1])
        self.assertEqual([account.id for account in targets], [2])
        self.assertIsNone(await self.db.get_account(3, 100))

    async def test_roles_persist_and_legacy_accounts_default_to_target(self) -> None:
        restarted = Database(Path(self.temporary.name) / "app.db")
        await restarted.initialize()
        owners = await restarted.list_accounts_by_role(100, "OWNER")
        self.assertEqual([a.id for a in owners], [1])
        legacy_path = Path(self.temporary.name) / "legacy.db"
        connection = sqlite3.connect(legacy_path)
        try:
            with connection:
                connection.execute("""CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY, owner_telegram_id INTEGER,
                    telegram_account_id INTEGER, session_key TEXT UNIQUE,
                    username TEXT, first_name TEXT, phone TEXT,
                    created_at TEXT,
                    UNIQUE(owner_telegram_id, telegram_account_id)
                )""")
                connection.execute(
                    "INSERT INTO accounts VALUES (1,100,1002,'internal',NULL,NULL,NULL,'2026-09-06')"
                )
        finally:
            connection.close()
        migrated = Database(legacy_path)
        await migrated.initialize()
        targets = await migrated.list_accounts_by_role(100, "TARGET")
        self.assertEqual([a.telegram_account_id for a in targets], [1002])

    async def test_portals_job6_shape_empty_received_is_accepted_once(self) -> None:
        portals = FakePortalsService()
        self.assertEqual(portals.offers, [])
        service = PortalsTransferJobService(
            self.db, cast(PortalsService, portals), dry_run=False
        )

        job = await service.execute_portals_transfer(100, 1, 2, "nft-7")

        self.assertEqual(job.status, "SUCCESS", job)
        self.assertEqual(job.external_ref, "offer-9")
        self.assertEqual(len(portals.create_calls), 1)
        self.assertEqual(portals.create_calls[0]["amount"], "0.53")
        self.assertEqual(len(portals.accept_calls), 1)
        self.assertEqual(portals.accept_calls[0]["offer_id"], "offer-9")
        self.assertEqual(portals.accept_calls[0]["nft_id"], "nft-7")

    async def test_portals_mismatch_aborts_accept(self) -> None:
        variants = (
            FakePortalsService.offer(sender_id="wrong-sender"),
            FakePortalsService.offer(nft_id="wrong-nft"),
            FakePortalsService.offer(amount="2.00"),
        )
        for offer in variants:
            with self.subTest(offer=offer):
                self.reset_jobs()
                portals = FakePortalsService()
                portals.nft_offers = [offer]
                service = PortalsTransferJobService(
                    self.db, cast(PortalsService, portals), dry_run=False
                )
                job = await service.execute_portals_transfer(100, 1, 2, "nft-7")
                self.assertEqual(job.status, "FAILED")
                self.assertEqual(job.error_code, "OFFER_CHANGED")
                self.assertEqual(portals.accept_calls, [])

    async def test_portals_zero_and_multiple_nfts_do_not_create_offer(self) -> None:
        for nfts, expected in (
            ([], "EMPTY_NFTS"),
            (
                [FakePortalsService.nft("nft-1"), FakePortalsService.nft("nft-2")],
                "AMBIGUOUS_NFT",
            ),
        ):
            with self.subTest(expected=expected):
                portals = FakePortalsService()
                portals.nfts = nfts
                service = PortalsTransferJobService(
                    self.db, cast(PortalsService, portals), dry_run=False
                )
                job = await service.execute_portals_transfer(100, 1, 2, None)
                self.assertEqual(job.error_code, expected)
                self.assertEqual(portals.create_calls, [])

    async def test_portals_ambiguous_create_stops_before_accept(self) -> None:
        portals = FakePortalsService()
        portals.create_error = PortalsCreateOfferUnconfirmedError("secret")
        service = PortalsTransferJobService(
            self.db, cast(PortalsService, portals), dry_run=False
        )

        job = await service.execute_portals_transfer(100, 1, 2, "nft-7")

        self.assertEqual(job.status, "AMBIGUOUS")
        self.assertEqual(job.error_code, "CREATE_OFFER_UNCONFIRMED")
        self.assertEqual(len(portals.create_calls), 1)
        self.assertEqual(portals.accept_calls, [])

    async def test_portals_create_transition_reason_is_persisted(self) -> None:
        portals = FakePortalsService()
        portals.create_error = PortalsCreateOfferUnconfirmedError(
            "secret", "CREATE_OFFER_CANCELLED"
        )
        service = PortalsTransferJobService(
            self.db, cast(PortalsService, portals), dry_run=False
        )

        job = await service.execute_portals_transfer(100, 1, 2, "nft-7")

        self.assertEqual(job.status, "AMBIGUOUS")
        self.assertEqual(job.error_code, "CREATE_OFFER_CANCELLED")
        self.assertIn("отменён", job.error_message or "")
        self.assertEqual(len(portals.create_calls), 1)
        self.assertEqual(portals.accept_calls, [])

    async def test_portals_ambiguous_accept_is_not_retried(self) -> None:
        portals = FakePortalsService()
        portals.accept_status = PortalsAcceptStatus.AMBIGUOUS
        service = PortalsTransferJobService(
            self.db, cast(PortalsService, portals), dry_run=False
        )

        job = await service.execute_portals_transfer(100, 1, 2, "nft-7")

        self.assertEqual(job.status, "AMBIGUOUS")
        self.assertEqual(job.error_code, "ACCEPT_AMBIGUOUS")
        self.assertEqual(len(portals.accept_calls), 1)

    async def test_portals_dry_run_sends_no_mutation(self) -> None:
        portals = FakePortalsService()
        service = PortalsTransferJobService(
            self.db, cast(PortalsService, portals), dry_run=True
        )

        job = await service.execute_portals_transfer(100, 1, 2, "nft-7")

        self.assertEqual(job.status, "DRY_RUN")
        self.assertTrue(portals.create_calls[0]["dry_run"])
        self.assertEqual(portals.accept_calls, [])

    async def test_mrkt_lists_then_buys_exact_gift_once(self) -> None:
        mrkt = FakeMrktService()
        service = MrktTransferJobService(
            self.db, cast(MrktService, mrkt), dry_run=False
        )

        job = await service.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )

        self.assertEqual(job.status, "SUCCESS")
        self.assertEqual(len(mrkt.listing_calls), 1)
        self.assertEqual(len(mrkt.purchase_calls), 1)
        self.assertEqual(mrkt.listing_calls[0]["account_id"], 2)
        self.assertEqual(mrkt.purchase_calls[0]["account_id"], 1)
        self.assertEqual(mrkt.purchase_calls[0]["gift_id"], "gift-7")
        self.assertEqual(
            mrkt.purchase_calls[0]["expected_price_nanotons"], 1_713_600_000
        )
        self.assertEqual(job.result_metadata["seller_price_nanotons"], 1_680_000_000)
        self.assertEqual(job.result_metadata["buyer_price_nanotons"], 1_713_600_000)
        self.assertEqual(job.result_metadata["commission_percent"], 2)
        self.assertTrue(job.result_metadata["fast_path"])
        self.assertEqual(
            job.result_metadata["mrkt_listing_to_buy_start_ms"], 0.1
        )

    async def test_mrkt_speculative_mode_is_explicitly_forwarded(self) -> None:
        mrkt = FakeMrktService()
        service = MrktTransferJobService(
            self.db,
            cast(MrktService, mrkt),
            dry_run=False,
            speculative_buy=True,
            speculative_buy_delay_ms=35,
        )

        job = await service.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )

        self.assertTrue(mrkt.listing_calls[0]["speculative_buy"])
        self.assertEqual(mrkt.listing_calls[0]["speculative_buy_delay_ms"], 35)
        self.assertEqual(job.result_metadata["transfer_mode"], "SPECULATIVE")
        self.assertEqual(job.result_metadata["speculative_buy_delay_ms"], 35)

    async def test_mrkt_live_verify_enables_listing_observer_metadata(self) -> None:
        mrkt = FakeMrktService()
        service = MrktTransferJobService(
            self.db,
            cast(MrktService, mrkt),
            dry_run=False,
            live_verify=True,
            speculative_buy=True,
        )

        job = await service.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )

        self.assertTrue(mrkt.listing_calls[0]["observe_listing_activation"])
        self.assertIn("mrkt_listing_observation", job.result_metadata)

    async def test_mrkt_listed_triggered_mode_and_bounds_are_forwarded(self) -> None:
        mrkt = FakeMrktService()
        service = MrktTransferJobService(
            self.db,
            cast(MrktService, mrkt),
            dry_run=False,
            transfer_mode=MrktTransferMode.LISTED_TRIGGERED,
            listed_trigger_max_wait_ms=900,
            listed_trigger_poll_interval_ms=12,
        )

        job = await service.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )

        call = mrkt.listing_calls[0]
        self.assertEqual(call["transfer_mode"], "LISTED_TRIGGERED")
        self.assertEqual(call["listed_trigger_max_wait_ms"], 900)
        self.assertEqual(call["listed_trigger_poll_interval_ms"], 12)
        self.assertEqual(job.result_metadata["transfer_mode"], "LISTED_TRIGGERED")
        self.assertIn("mrkt_listed_trigger", job.result_metadata)

    async def test_mrkt_fresh_confirmation_ignores_terminal_job_and_replay_reuses(
        self,
    ) -> None:
        for old_status, old_phase in (
            ("FAILED", "BUYING"),
            ("SUCCESS", "COMPLETED"),
            ("AMBIGUOUS", "BUYING"),
        ):
            with self.subTest(old_status=old_status):
                self.reset_jobs()
                old = await self.db.create_transfer_job(
                    owner_telegram_id=100,
                    market="mrkt",
                    owner_account_id=1,
                    target_account_id=2,
                    asset_id="gift-7",
                    amount_text="1.68",
                    amount_atomic=1_680_000_000,
                    confirmation_key=f"old-{old_status}",
                )
                claimed = await self.db.claim_transfer_job(old.id)
                self.assertIsNotNone(claimed)
                await self.db.finish_transfer_job(
                    old.id,
                    status=old_status,
                    phase=old_phase,
                    error_code="OLD_RESULT" if old_status != "SUCCESS" else None,
                    error_message=None,
                    result_metadata={},
                )
                mrkt = FakeMrktService()
                service = MrktTransferJobService(
                    self.db, cast(MrktService, mrkt), dry_run=True
                )
                raw_nonce = f"fresh-confirmation-{old_status}"
                with (
                    patch.object(
                        self.db,
                        "claim_transfer_job",
                        new=AsyncMock(return_value=None),
                    ),
                    patch("app.jobs.mrkt_transfer_jobs.logger") as safe_logger,
                ):
                    fresh = await service.execute_mrkt_transfer(
                        100,
                        1,
                        2,
                        "gift-7",
                        "1.68",
                        confirmation_nonce=raw_nonce,
                    )
                    replay = await service.execute_mrkt_transfer(
                        100,
                        1,
                        2,
                        "gift-7",
                        "1.68",
                        confirmation_nonce=raw_nonce,
                    )

                self.assertEqual(fresh.id, old.id + 1)
                self.assertEqual(fresh.status, "PENDING")
                self.assertEqual(replay.id, fresh.id)
                connection = sqlite3.connect(Path(self.temporary.name) / "app.db")
                try:
                    row = connection.execute(
                        "SELECT confirmation_key FROM transfer_jobs WHERE id = ?",
                        (fresh.id,),
                    ).fetchone()
                    count = connection.execute(
                        "SELECT COUNT(*) FROM transfer_jobs"
                    ).fetchone()
                finally:
                    connection.close()
                self.assertIsNotNone(row)
                self.assertEqual(
                    row[0], hashlib.sha256(raw_nonce.encode()).hexdigest()
                )
                self.assertNotEqual(row[0], raw_nonce)
                self.assertEqual(count[0], 2)
                self.assertNotIn(raw_nonce, repr(safe_logger.mock_calls))
                self.assertEqual(mrkt.listing_calls, [])

    async def test_mrkt_active_same_gift_blocks_fresh_nonce_but_other_gift_starts(
        self,
    ) -> None:
        active = await self.db.create_transfer_job(
            owner_telegram_id=100,
            market="mrkt",
            owner_account_id=1,
            target_account_id=2,
            asset_id="gift-7",
            amount_text="1.68",
            amount_atomic=1_680_000_000,
            confirmation_key="active-key",
        )
        claimed = await self.db.claim_transfer_job(active.id)
        self.assertIsNotNone(claimed)
        mrkt = FakeMrktService()
        service = MrktTransferJobService(
            self.db, cast(MrktService, mrkt), dry_run=True
        )

        same = await service.execute_mrkt_transfer(
            100,
            1,
            2,
            "gift-7",
            "1.68",
            confirmation_nonce="fresh-but-colliding",
        )

        self.assertEqual(same.id, active.id)
        self.assertEqual(same.status, "RUNNING")
        self.assertEqual(mrkt.listing_calls, [])

        mrkt.gifts = [mrkt.gift("gift-8")]
        different = await service.execute_mrkt_transfer(
            100,
            1,
            2,
            "gift-8",
            "1.68",
            confirmation_nonce="fresh-other-gift",
        )
        self.assertGreater(different.id, active.id)
        self.assertEqual(different.status, "DRY_RUN")
        self.assertEqual(len(mrkt.listing_calls), 1)

    async def test_mrkt_exact_gift_mismatch_stops_before_mutation(self) -> None:
        mrkt = FakeMrktService()
        mrkt.gifts = [mrkt.gift("different-gift")]
        service = MrktTransferJobService(
            self.db, cast(MrktService, mrkt), dry_run=False
        )

        job = await service.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )

        self.assertEqual(job.error_code, "AMBIGUOUS_LISTING")
        self.assertEqual(mrkt.listing_calls, [])
        self.assertEqual(mrkt.purchase_calls, [])

    async def test_mrkt_already_sold_and_ambiguous_purchase_are_single_shot(
        self,
    ) -> None:
        for purchase_status, expected_status, expected_code in (
            (MrktPurchaseStatus.REJECTED, "FAILED", "BUY_FAILED"),
            (MrktPurchaseStatus.AMBIGUOUS, "AMBIGUOUS", "BUY_AMBIGUOUS"),
        ):
            with self.subTest(purchase_status=purchase_status):
                self.reset_jobs()
                mrkt = FakeMrktService()
                mrkt.purchase_status = purchase_status
                service = MrktTransferJobService(
                    self.db, cast(MrktService, mrkt), dry_run=False
                )
                job = await service.execute_mrkt_transfer(
                    100,
                    1,
                    2,
                    "gift-7",
                    "1.68",
                    confirmation_nonce="confirm-1",
                )
                self.assertEqual(job.status, expected_status)
                self.assertEqual(job.error_code, expected_code)
                self.assertEqual(len(mrkt.purchase_calls), 1)

    async def test_mrkt_dry_run_sends_no_mutation(self) -> None:
        mrkt = FakeMrktService()
        service = MrktTransferJobService(self.db, cast(MrktService, mrkt), dry_run=True)

        job = await service.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )

        self.assertEqual(job.status, "DRY_RUN")
        self.assertTrue(mrkt.listing_calls[0]["dry_run"])
        self.assertEqual(mrkt.purchase_calls, [])

    async def test_interrupted_composite_jobs_become_ambiguous(self) -> None:
        job = await self.db.create_transfer_job(
            owner_telegram_id=100,
            market="mrkt",
            owner_account_id=1,
            target_account_id=2,
            asset_id="gift-7",
            amount_text="1.68",
            amount_atomic=1_680_000_000,
        )
        await self.db.claim_transfer_job(job.id)

        recovered = await self.db.recover_running_transfer_jobs()
        current = await self.db.get_transfer_job(job.id)

        self.assertEqual(recovered, 1)
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.status, "AMBIGUOUS")
        self.assertEqual(current.error_code, "INTERRUPTED_MUTATION")

    async def test_duplicate_and_concurrent_transfers_do_not_repeat_mutations(
        self,
    ) -> None:
        for market in ("mrkt", "portals"):
            with self.subTest(market=market):
                self.reset_jobs()
                mrkt = FakeMrktService()
                portals = FakePortalsService()
                mrkt_jobs = MrktTransferJobService(
                    self.db, cast(MrktService, mrkt), dry_run=False
                )
                portals_jobs = PortalsTransferJobService(
                    self.db, cast(PortalsService, portals), dry_run=False
                )

                async def execute(
                    market: str = market,
                    mrkt_jobs: MrktTransferJobService = mrkt_jobs,
                    portals_jobs: PortalsTransferJobService = portals_jobs,
                ) -> object:
                    if market == "mrkt":
                        return await mrkt_jobs.execute_mrkt_transfer(
                            100,
                            1,
                            2,
                            "gift-7",
                            "1.68",
                            confirmation_nonce="same-confirmation",
                        )
                    return await portals_jobs.execute_portals_transfer(
                        100, 1, 2, "nft-7"
                    )

                await asyncio.gather(execute(), execute())
                await execute()
                if market == "mrkt":
                    self.assertEqual(len(mrkt.listing_calls), 1)
                    self.assertEqual(len(mrkt.purchase_calls), 1)
                else:
                    self.assertEqual(len(portals.create_calls), 1)
                    self.assertEqual(len(portals.accept_calls), 1)

    async def test_mrkt_external_sale_has_distinct_safe_result(self) -> None:
        mrkt = FakeMrktService()
        mrkt.external_sale = True
        service = MrktTransferJobService(
            self.db, cast(MrktService, mrkt), dry_run=False
        )

        result = await service.execute_mrkt_transfer(
            100,
            1,
            2,
            "gift-7",
            "1.68",
            confirmation_nonce="confirm-1",
        )

        self.assertEqual(result.status, "FAILED")
        self.assertEqual(result.error_code, "GIFT_INTERCEPTED_OR_SOLD_EXTERNALLY")
        self.assertIn("другому покупателю", result.error_message or "")
        self.assertEqual(len(mrkt.purchase_calls), 1)

    async def test_mrkt_ambiguous_listing_blocks_buy_and_replay(self) -> None:
        mrkt = FakeMrktService()
        mrkt.listing_status = MrktListingStatus.AMBIGUOUS
        service = MrktTransferJobService(
            self.db, cast(MrktService, mrkt), dry_run=False
        )
        first = await service.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="same-confirmation"
        )
        again = await service.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "2.00", confirmation_nonce="same-confirmation"
        )
        self.assertEqual(first.status, "AMBIGUOUS")
        self.assertEqual(first.id, again.id)
        self.assertEqual(len(mrkt.listing_calls), 1)
        self.assertEqual(mrkt.purchase_calls, [])

    async def test_role_and_ownership_fail_before_marketplace_access(self) -> None:
        portals = FakePortalsService()
        service = PortalsTransferJobService(
            self.db, cast(PortalsService, portals), dry_run=False
        )
        result = await service.execute_portals_transfer(999, 1, 2, "nft-7")
        self.assertEqual(result.error_code, "NO_OWNER_ACCOUNT")
        result = await service.execute_portals_transfer(100, 2, 1, "nft-7")
        self.assertEqual(result.error_code, "NO_OWNER_ACCOUNT")
        self.assertEqual(portals.create_calls, [])

    async def test_composite_history_is_owner_scoped(self) -> None:
        service = PortalsTransferJobService(
            self.db, cast(PortalsService, FakePortalsService())
        )
        await service.execute_portals_transfer(100, 1, 2, "nft-7")
        self.assertEqual(await self.db.count_operation_history(999), 0)
        history = await self.db.list_operation_history(100)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].market, "portals_transfer")
        self.assertEqual(history[0].status, "DRY_RUN")
