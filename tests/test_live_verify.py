from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from app.config import Config
from app.db import Database, TransferJob
from app.jobs import MrktTransferJobService, PortalsTransferJobService
from app.live_verify import (
    created_offer,
    final_verification,
    gift_snapshot,
    identity,
    mutation_started,
    offer_checks,
    offer_observation,
    response_metadata,
    russian_summary,
    verification_scope,
)
from app.telegram_client import (
    MiniAppService,
    MrktGift,
    MrktMutationNetworkError,
    MrktPurchaseStatus,
    MrktService,
    PortalsAcceptResult,
    PortalsCreateOfferResult,
    PortalsCreateOfferUnconfirmedError,
    PortalsMutationNetworkError,
    PortalsNft,
    PortalsOffer,
    PortalsService,
)
from app.telegram_client.mrkt_service import MrktHttpResponse
from app.telegram_client.portals_service import PortalsHttpResponse
from tests.test_mrkt_service import (
    GIFT_ID,
    gift,
)
from tests.test_mrkt_service import (
    FakeMiniAppService as MrktMiniApp,
)
from tests.test_mrkt_service import (
    FakeTransport as MrktTransport,
)
from tests.test_portals_service import (
    AMOUNT,
    NFT_ID,
    OFFER_ID,
    CountingMiniAppService,
    detail_body,
    nft_offer_body,
    placed_body,
    received_body,
)
from tests.test_portals_service import (
    FakeMiniAppService as PortalsMiniApp,
)
from tests.test_portals_service import (
    FakeTransport as PortalsTransport,
)
from tests.test_transfer_jobs import FakeMrktService, FakePortalsService


def job(market: str = "mrkt", identifier: int = 1) -> TransferJob:
    return TransferJob(
        id=identifier,
        owner_telegram_id=100,
        market=market,
        owner_account_id=1,
        target_account_id=2,
        asset_id="gift-7",
        display_name="Подарок",
        amount_text="1.68",
        amount_atomic=1680000000,
        external_ref=None,
        status="RUNNING",
        phase="BUYING",
        created_at="2026-09-06",
        started_at=None,
        finished_at=None,
        error_code=None,
        error_message=None,
        result_metadata={},
    )


class ObservedMrkt(FakeMrktService):
    def __init__(self) -> None:
        super().__init__()
        self.purchased = False
        self.final_calls = 0
        self.final_error = False

    async def execute_fast_transfer(self, **kwargs: object) -> Any:
        if not kwargs["dry_run"]:
            mutation_started("mrkt_listing")
            mutation_started("mrkt_buy", emit_event=False)
            response_metadata("mrkt_listing", 200, {"ids": ["gift-7"]})
            response_metadata(
                "mrkt_buy", 200, {"ids": ["gift-7"], "token": "NEVER-LOG"}
            )
            self.purchased = True
            self.final_calls = 4
        result = await super().execute_fast_transfer(**kwargs)
        if self.final_error and not kwargs["dry_run"]:
            return replace(result, verification={"outcome": "unavailable"})
        return result

    async def get_inventory(
        self, account_id: int, *, owner_telegram_id: int, is_listed: bool = False
    ) -> list[MrktGift]:
        if self.purchased:
            self.final_calls += 1
            if self.final_error:
                raise RuntimeError("initData=NEVER-LOG")
            return [self.gift()] if account_id == 1 and not is_listed else []
        return await super().get_inventory(
            account_id, owner_telegram_id=owner_telegram_id, is_listed=is_listed
        )


class ObservedPortals(FakePortalsService):
    def __init__(self) -> None:
        super().__init__()
        self.accepted = False

    async def create_offer(self, **kwargs: object) -> PortalsCreateOfferResult:
        result = await super().create_offer(**kwargs)
        if not kwargs["dry_run"]:
            mutation_started("portals_create_offer")
            response_metadata("portals_create_offer", 201, {"offer": {"id": "offer-9"}})
            created_offer(result.offer_id)
        return result

    async def accept_offer(self, **kwargs: object) -> PortalsAcceptResult:
        mutation_started("portals_accept")
        response_metadata("portals_accept", 200, {"success": True})
        self.accepted = True
        return await super().accept_offer(**kwargs)

    async def get_owned_nfts(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[PortalsNft]:
        if self.accepted:
            return [self.nft()] if account_id == 1 else []
        return await super().get_owned_nfts(
            account_id, owner_telegram_id=owner_telegram_id
        )

    async def get_received_offers(
        self, account_id: int, *, owner_telegram_id: int
    ) -> list[PortalsOffer]:
        if self.accepted:
            return []
        return await super().get_received_offers(
            account_id, owner_telegram_id=owner_telegram_id
        )


class LiveVerifyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp.name) / "test.db")
        await self.db.initialize()
        for account_id, role in ((1, "OWNER"), (2, "TARGET")):
            await self.db.save_account(
                owner_telegram_id=100,
                telegram_account_id=1000 + account_id,
                session_key=f"internal-{account_id}",
                username=None,
                first_name=None,
                phone=None,
                role=role,
            )

    async def asyncTearDown(self) -> None:
        self.temp.cleanup()

    def test_config_is_disabled_by_default_and_strict(self) -> None:
        env = {
            "BOT_TOKEN": "123456:" + "A" * 35,
            "TELEGRAM_API_ID": "123",
            "TELEGRAM_API_HASH": "a" * 32,
            "WEBAPP_PUBLIC_URL": "https://example.com/auth",
        }
        with patch("app.config.load_dotenv"), patch.dict(os.environ, env, clear=True):
            self.assertFalse(Config.load().live_verify)
            with patch.dict(os.environ, {"LIVE_VERIFY": "true"}):
                self.assertTrue(Config.load().live_verify)
                self.assertTrue(Config.load().mrkt_transfer_dry_run)
            with (
                patch.dict(os.environ, {"LIVE_VERIFY": "invalid-secret"}),
                self.assertRaisesRegex(
                    RuntimeError, "LIVE_VERIFY must be true or false"
                ),
            ):
                Config.load()

    async def test_redaction_in_logs_and_persistable_report(self) -> None:
        secret = "SECRET-session-initData-token-cookie"
        test_job = replace(job(), asset_id=secret)
        with (
            self.assertLogs("app.live_verify", level="INFO") as logs,
            verification_scope(test_job, True),
        ):
            response_metadata(
                "mrkt_buy",
                200,
                {"token": secret, secret: secret, "data": {secret: secret}},
            )
            gift_snapshot(
                "mrkt_market",
                {
                    "id": secret,
                    "sellerId": secret,
                    "salePrice": 1680000000,
                    "isMine": False,
                },
                secret,
            )
            report = await final_verification(
                test_job, cast(MrktService, None), "DRY_RUN"
            )
        encoded = json.dumps(report) + str(logs.output)
        self.assertNotIn(secret, encoded)
        self.assertIn('"omitted_field_count": 1', encoded)
        self.assertIn('"price_nanotons": 1680000000', encoded)
        self.assertNotEqual(identity(77), identity("77"))

    async def test_disabled_mode_adds_no_events_or_reads(self) -> None:
        service = AsyncMock()
        with verification_scope(job(), False):
            mutation_started("mrkt_buy")
            response_metadata("mrkt_buy", 200, {"token": "secret"})
            report = await final_verification(job(), service, "SUCCESS")
        self.assertIsNone(report)
        self.assertEqual(service.mock_calls, [])

    async def test_concurrent_scopes_are_isolated_and_reset(self) -> None:
        async def run(identifier: int) -> dict[str, Any] | None:
            test_job = job(identifier=identifier)
            with verification_scope(test_job, True):
                await asyncio.sleep(0)
                response_metadata("mrkt_buy", 200, {"ids": []})
                return await final_verification(
                    test_job, cast(MrktService, None), "DRY_RUN"
                )

        reports = await asyncio.gather(run(11), run(22))
        for identifier, report in zip((11, 22), reports, strict=True):
            assert report is not None
            self.assertEqual({e["job_id"] for e in report["events"]}, {identifier})
        self.assertIsNone(
            await final_verification(job(), cast(MrktService, None), "SUCCESS")
        )

    async def test_mrkt_report_is_saved_without_changing_success_or_post_counts(
        self,
    ) -> None:
        service = ObservedMrkt()
        jobs = MrktTransferJobService(
            self.db, cast(MrktService, service), dry_run=False, live_verify=True
        )
        result = await jobs.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )
        report = result.result_metadata["live_verify"]
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(report["mutation_counts"], {"mrkt_listing": 1, "mrkt_buy": 1})
        self.assertEqual(report["final_read"]["outcome"], "consistent")
        self.assertEqual(service.final_calls, 4)
        persisted = await self.db.get_transfer_job(result.id, 100)
        assert persisted is not None
        self.assertEqual(persisted.result_metadata, result.result_metadata)
        self.assertNotIn("NEVER-LOG", json.dumps(persisted.result_metadata))

    async def test_ambiguous_result_keeps_unavailable_read_state(self) -> None:
        service = ObservedMrkt()
        service.purchase_status = MrktPurchaseStatus.AMBIGUOUS
        jobs = MrktTransferJobService(
            self.db, cast(MrktService, service), dry_run=False, live_verify=True
        )
        result = await jobs.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )
        self.assertEqual(result.status, "AMBIGUOUS")
        self.assertEqual(
            result.result_metadata["live_verify"]["final_read"]["outcome"],
            "unavailable",
        )
        self.assertEqual(len(service.purchase_calls), 1)

    async def test_failed_read_does_not_change_success_or_retry_purchase(self) -> None:
        service = ObservedMrkt()
        service.final_error = True
        jobs = MrktTransferJobService(
            self.db, cast(MrktService, service), dry_run=False, live_verify=True
        )
        result = await jobs.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(
            result.result_metadata["live_verify"]["final_read"]["outcome"],
            "unavailable",
        )
        self.assertNotIn("NEVER-LOG", json.dumps(result.result_metadata))
        self.assertEqual(len(service.purchase_calls), 1)
        self.assertIn("Нужна ручная проверка", russian_summary(result.result_metadata))

    async def test_portals_correlation_and_final_ownership(self) -> None:
        service = ObservedPortals()
        jobs = PortalsTransferJobService(
            self.db, cast(PortalsService, service), dry_run=False, live_verify=True
        )
        result = await jobs.execute_portals_transfer(100, 1, 2, "nft-7")
        report = result.result_metadata["live_verify"]
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(
            report["mutation_counts"], {"portals_create_offer": 1, "portals_accept": 1}
        )
        checks = next(e for e in report["events"] if e["event"] == "correlation")
        for key in ("offer_id_match", "nft_id_match", "amount_match", "sender_match"):
            self.assertTrue(checks[key])
        self.assertEqual(report["final_read"]["outcome"], "consistent")

    async def test_real_portals_service_reuses_auth_exits_early_and_times_stages(
        self,
    ) -> None:
        amount = "0.53"
        miniapp = CountingMiniAppService()
        source = PortalsTransport()
        source.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
        ]
        source.responses_by_path = {
            "/offers/placed": [
                PortalsHttpResponse(200, placed_body(amount=amount)),
                PortalsHttpResponse(200, placed_body(amount=amount)),
            ],
            "/nfts/owned": [
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                )
            ],
        }
        target = PortalsTransport()
        target.responses = [
            PortalsHttpResponse(200, {"user_id": 2}),
            PortalsHttpResponse(200, {"success": True}),
        ]
        target.responses_by_path = {
            "/nfts/owned": [
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                ),
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                ),
                PortalsHttpResponse(200, {"nfts": [], "total_count": 0}),
            ],
            f"/offers/nft/{NFT_ID}": [
                PortalsHttpResponse(200, nft_offer_body(amount=amount)),
                PortalsHttpResponse(200, nft_offer_body(amount=amount)),
            ],
        }
        transports = iter((source, target))
        portals = PortalsService(
            cast(MiniAppService, miniapp),
            transport_factory=lambda: next(transports),
            reconciliation_delays=(0.0,) * 7,
        )
        jobs = PortalsTransferJobService(
            self.db, portals, dry_run=False, live_verify=True
        )

        result = await jobs.execute_portals_transfer(100, 1, 2, NFT_ID)

        self.assertEqual(result.status, "SUCCESS")
        report = result.result_metadata["live_verify"]
        self.assertEqual(
            report["mutation_counts"],
            {"portals_create_offer": 1, "portals_accept": 1},
        )
        self.assertEqual(report["final_read"]["outcome"], "consistent")
        self.assertEqual(miniapp.calls, 2)
        self.assertEqual(
            [path for path, _ in source.get_calls].count("/offers/placed"), 2
        )
        timing_stages = {
            event["stage"]
            for event in report["events"]
            if event["event"] == "stage_timing"
        }
        self.assertTrue(
            {
                "portals_n1_auth",
                "portals_n2_auth",
                "portals_create_offer",
                "portals_first_reconciliation",
                "portals_reconciliation_total",
                "portals_pre_accept_verification",
                "portals_accept_request",
                "portals_final_ownership",
                "portals_total_job",
            }.issubset(timing_stages)
        )

    async def test_dry_run_with_live_verify_still_sends_no_mutation(self) -> None:
        service = ObservedPortals()
        jobs = PortalsTransferJobService(
            self.db, cast(PortalsService, service), dry_run=True, live_verify=True
        )
        result = await jobs.execute_portals_transfer(100, 1, 2, "nft-7")
        self.assertEqual(result.status, "DRY_RUN")
        self.assertEqual(result.result_metadata["live_verify"]["mutation_counts"], {})
        self.assertEqual(service.accept_calls, [])

    async def test_service_hooks_capture_actual_portals_responses(self) -> None:
        transport = PortalsTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(201, {"offer": {"id": OFFER_ID}, "token": "secret"}),
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, received_body()),
            PortalsHttpResponse(200, detail_body()),
            PortalsHttpResponse(200, {"success": True}),
        ]
        service = PortalsService(
            cast(MiniAppService, PortalsMiniApp()),
            transport_factory=lambda: transport,
            reconciliation_delays=(0.0,),
        )
        with verification_scope(replace(job("portals"), asset_id=NFT_ID), True):
            await service.create_offer(
                owner_telegram_id=100,
                account_id=1,
                nft_id=NFT_ID,
                amount=AMOUNT,
                dry_run=False,
            )
            await service.accept_offer(
                owner_telegram_id=100,
                account_id=2,
                nft_id=NFT_ID,
                offer_id=OFFER_ID,
                amount=AMOUNT,
                display_name="NFT",
                dry_run=False,
            )
            report = await final_verification(
                job("portals"), cast(PortalsService, ObservedPortals()), "DRY_RUN"
            )
        assert report is not None
        responses = [e for e in report["events"] if e["event"] == "response"]
        self.assertEqual([e["http_status"] for e in responses], [201, 200])
        self.assertEqual(len(transport.post_calls), 2)

    async def test_live_verify_records_204_reconciliation_metadata(self) -> None:
        transport = PortalsTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, placed_body()),
        ]
        service = PortalsService(
            cast(MiniAppService, PortalsMiniApp()),
            transport_factory=lambda: transport,
            reconciliation_delays=(0.0,),
        )
        test_job = replace(job("portals"), asset_id=NFT_ID)
        with verification_scope(test_job, True):
            created = await service.create_offer(
                owner_telegram_id=100,
                account_id=1,
                nft_id=NFT_ID,
                amount=AMOUNT,
                dry_run=False,
            )
            report = await final_verification(
                test_job, cast(PortalsService, None), "DRY_RUN"
            )

        assert report is not None
        self.assertEqual(created.offer_id, OFFER_ID)
        create_response = next(
            event
            for event in report["events"]
            if event.get("stage") == "portals_create_offer"
            and event["event"] == "response"
        )
        self.assertEqual(create_response["http_status"], 204)
        reconciliation = next(
            event
            for event in report["events"]
            if event["event"] == "offer_reconciliation"
        )
        self.assertEqual(reconciliation["attempt"], 1)
        self.assertEqual(reconciliation["placed_result_count"], 1)
        self.assertIsNone(reconciliation["nft_result_count"])
        self.assertEqual(reconciliation["exact_match_count"], 1)
        self.assertEqual(reconciliation["active_match_count"], 1)
        self.assertTrue(reconciliation["offer_id_resolved"])
        self.assertIsInstance(reconciliation["elapsed_ms"], int)

    async def test_offer_observation_keeps_only_sanitized_lifecycle_metadata(
        self,
    ) -> None:
        test_job = replace(job("portals"), asset_id=NFT_ID)
        with verification_scope(test_job, True):
            offer_observation(
                attempt=2,
                endpoint="n1_placed",
                offer_id="private-offer-id",
                status="cancelled",
                previous_status="pending",
                created_at="2026-09-06T12:00:00Z",
                updated_at="2026-09-06T12:00:01Z",
                expires_at="2026-09-07T12:00:00Z",
                appeared=False,
                disappeared=True,
                created_in_window=True,
            )
            report = await final_verification(
                test_job, cast(PortalsService, None), "DRY_RUN"
            )

        assert report is not None
        observation = next(
            event for event in report["events"] if event["event"] == "offer_observation"
        )
        self.assertEqual(observation["endpoint"], "n1_placed")
        self.assertEqual(observation["status"], "cancelled")
        self.assertEqual(observation["previous_status"], "pending")
        self.assertTrue(observation["status_changed"])
        self.assertTrue(observation["disappeared"])
        self.assertEqual(observation["created_at"], "2026-09-06T12:00:00Z")
        self.assertNotIn("private-offer-id", repr(report))

    async def test_reconciliation_trace_records_terminal_status_transition(
        self,
    ) -> None:
        created_at = datetime.now(timezone.utc).isoformat()
        updated_at = datetime.now(timezone.utc).isoformat()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        first_placed = placed_body(status="pending")
        first_placed["offers"][0].update(
            created_at=created_at,
            updated_at=created_at,
            expires_at=expires_at,
        )
        cancelled_placed = placed_body(status="cancelled")
        cancelled_placed["offers"][0].update(
            created_at=created_at,
            updated_at=updated_at,
            expires_at=expires_at,
        )
        transport = PortalsTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, first_placed),
            PortalsHttpResponse(200, cancelled_placed),
        ]
        service = PortalsService(
            cast(MiniAppService, PortalsMiniApp()),
            transport_factory=lambda: transport,
            reconciliation_delays=(0.0, 0.0),
        )
        test_job = replace(job("portals"), asset_id=NFT_ID)
        with verification_scope(test_job, True):
            with self.assertRaises(PortalsCreateOfferUnconfirmedError) as context:
                await service.create_offer(
                    owner_telegram_id=100,
                    account_id=1,
                    nft_id=NFT_ID,
                    amount=AMOUNT,
                    dry_run=False,
                )
            report = await final_verification(
                test_job, cast(PortalsService, None), "DRY_RUN"
            )

        assert report is not None
        self.assertEqual(context.exception.reason, "CREATE_OFFER_CANCELLED")
        observations = [
            event
            for event in report["events"]
            if event["event"] == "offer_observation"
            and event["endpoint"] == "n1_placed"
        ]
        self.assertEqual(
            [event["status"] for event in observations], ["pending", "cancelled"]
        )
        self.assertTrue(observations[0]["appeared"])
        self.assertTrue(observations[1]["status_changed"])
        self.assertEqual(observations[1]["updated_at"], updated_at)
        self.assertNotIn(OFFER_ID, repr(report))

    async def test_service_hooks_capture_actual_mrkt_responses_and_price(self) -> None:
        listed = dict(gift(listed=True, price=1680000000), sellerId="seller")
        transport = MrktTransport(
            inventories=[[gift()]],
            market_items=[listed],
            buy_response=MrktHttpResponse(200, {"ids": [GIFT_ID]}),
        )
        service = MrktService(
            cast(MiniAppService, MrktMiniApp()), transport_factory=lambda: transport
        )
        with verification_scope(replace(job(), asset_id=GIFT_ID), True):
            await service.execute_job_listing(
                owner_telegram_id=100,
                account_id=2,
                gift_id=GIFT_ID,
                display_name="Подарок",
                price_ton="1.68",
                dry_run_override=False,
            )
            await service.buy_listing(
                owner_telegram_id=100,
                account_id=1,
                gift_id=GIFT_ID,
                expected_price_nanotons=1680000000,
                expected_seller_id="seller",
                dry_run=False,
            )
            report = await final_verification(
                job(), cast(MrktService, ObservedMrkt()), "DRY_RUN"
            )
        assert report is not None
        responses = {
            e["stage"]: e for e in report["events"] if e["event"] == "response"
        }
        self.assertEqual(responses["mrkt_buy"]["http_status"], 200)
        self.assertEqual(responses["mrkt_listing"]["http_status"], 200)
        snapshot = next(
            e
            for e in report["events"]
            if e["event"] == "gift_snapshot" and e["stage"] == "mrkt_market"
        )
        self.assertEqual(snapshot["price_nanotons"], 1680000000)

    async def test_unknown_sender_is_not_reported_as_match(self) -> None:
        with verification_scope(job("portals"), True):
            offer_checks(
                "check",
                actual_id="a",
                expected_id="a",
                actual_nft="n",
                expected_nft="n",
                actual_amount="1",
                expected_amount="1",
                actual_sender=None,
                expected_sender=None,
            )
            report = await final_verification(
                job("portals"), cast(PortalsService, None), "DRY_RUN"
            )
        assert report is not None
        checks = next(e for e in report["events"] if e["event"] == "correlation")
        self.assertIsNone(checks["sender_match"])

    async def test_preaccept_evidence_sources_are_recorded(self) -> None:
        with verification_scope(job("portals"), True):
            offer_checks(
                "portals_job_before_accept",
                actual_id="offer",
                expected_id="offer",
                actual_nft=7,
                expected_nft=7,
                actual_amount="0.53",
                expected_amount="0.530",
                actual_sender=None,
                expected_sender=1,
                placed_match=True,
                nft_offer_match=True,
                received_match=False,
                n2_owns_nft=True,
            )
            report = await final_verification(
                job("portals"), cast(PortalsService, None), "DRY_RUN"
            )
        assert report is not None
        checks = next(e for e in report["events"] if e["event"] == "correlation")
        self.assertTrue(checks["placed_match"])
        self.assertTrue(checks["nft_offer_match"])
        self.assertFalse(checks["received_match"])
        self.assertTrue(checks["n2_owns_nft"])
        self.assertIsNone(checks["sender_match"])

    async def test_live_verify_mrkt_timeout_has_no_retry_or_fabricated_status(
        self,
    ) -> None:
        transport = MrktTransport(
            market_items=[dict(gift(listed=True, price=1680000000), sellerId="seller")],
            buy_failure=MrktMutationNetworkError("NEVER-LOG-token"),
        )
        service = MrktService(
            cast(MiniAppService, MrktMiniApp()), transport_factory=lambda: transport
        )
        with verification_scope(replace(job(), asset_id=GIFT_ID), True):
            with self.assertRaises(MrktMutationNetworkError):
                await service.buy_listing(
                    owner_telegram_id=100,
                    account_id=1,
                    gift_id=GIFT_ID,
                    expected_price_nanotons=1680000000,
                    expected_seller_id="seller",
                    dry_run=False,
                )
            report = await final_verification(
                job(), cast(MrktService, None), "AMBIGUOUS"
            )
        assert report is not None
        self.assertEqual(report["mutation_counts"], {"mrkt_buy": 1})
        self.assertFalse(
            any(
                e["event"] == "response" and e["stage"] == "mrkt_buy"
                for e in report["events"]
            )
        )
        self.assertNotIn("NEVER-LOG", json.dumps(report))
        self.assertTrue(transport.closed)

    async def test_live_verify_portals_create_timeout_never_accepts_or_retries(
        self,
    ) -> None:
        transport = PortalsTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsMutationNetworkError("NEVER-LOG-cookie"),
        ]
        service = PortalsService(
            cast(MiniAppService, PortalsMiniApp()), transport_factory=lambda: transport
        )
        with verification_scope(replace(job("portals"), asset_id=NFT_ID), True):
            with self.assertRaises(PortalsMutationNetworkError):
                await service.create_offer(
                    owner_telegram_id=100,
                    account_id=1,
                    nft_id=NFT_ID,
                    amount=AMOUNT,
                    dry_run=False,
                )
            report = await final_verification(
                job("portals"), cast(PortalsService, None), "AMBIGUOUS"
            )
        assert report is not None
        self.assertEqual(report["mutation_counts"], {"portals_create_offer": 1})
        self.assertEqual(len(transport.post_calls), 1)
        self.assertFalse(any(e["event"] == "response" for e in report["events"]))
        self.assertNotIn("NEVER-LOG", json.dumps(report))
        self.assertTrue(transport.closed)

    async def test_mrkt_dry_run_stays_read_only_with_live_verify(self) -> None:
        service = ObservedMrkt()
        jobs = MrktTransferJobService(
            self.db, cast(MrktService, service), dry_run=True, live_verify=True
        )
        result = await jobs.execute_mrkt_transfer(
            100, 1, 2, "gift-7", "1.68", confirmation_nonce="confirm-1"
        )
        self.assertEqual(result.status, "DRY_RUN")
        self.assertEqual(result.result_metadata["live_verify"]["mutation_counts"], {})
        self.assertEqual(service.purchase_calls, [])
