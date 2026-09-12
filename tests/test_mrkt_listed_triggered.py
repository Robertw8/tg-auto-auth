from __future__ import annotations

import asyncio
import unittest
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import patch

from app.telegram_client.miniapp_service import MiniAppService
from app.telegram_client.mrkt_service import (
    MRKT_AUTH_PATH,
    MRKT_BUY_PATH,
    MRKT_MARKET_BY_IDS_PATH,
    MRKT_SALE_PATH,
    MrktFastTransferStatus,
    MrktHttpResponse,
    MrktService,
    MrktTransferMode,
    _MrktAuthSession,
)
from tests.test_mrkt_activation_observer import FakeClock
from tests.test_mrkt_service import (
    GIFT_ID,
    OWNER_ID,
    DelayedSaleTransport,
    FakeMiniAppService,
    FakeTransport,
    TransportQueue,
    gift,
)

SELLER_PRICE_NANOTONS = 530_000_000
BUYER_PRICE_NANOTONS = 540_600_000


class ListedTriggeredWatcherTests(unittest.IsolatedAsyncioTestCase):
    def service(self) -> MrktService:
        return MrktService(cast(MiniAppService, FakeMiniAppService()))

    async def watch(
        self,
        lookup: FakeTransport,
        *,
        sale_result: tuple[MrktHttpResponse | None, Exception | None, int] | None = None,
        start_ms: int = 0,
        max_wait_ms: int = 100,
        interval_ms: int = 10,
        expected_seller_id: Any = None,
        on_start_buy: Callable[[], None] | None = None,
    ) -> tuple[Any, int, asyncio.Task[Any]]:
        clock = FakeClock(start_ms)
        release = asyncio.Event()

        async def sale() -> tuple[MrktHttpResponse | None, Exception | None, int]:
            if sale_result is not None:
                return sale_result
            await release.wait()
            return MrktHttpResponse(200, {"ids": [GIFT_ID]}), None, clock()

        listing_task = asyncio.create_task(sale())
        await asyncio.sleep(0)
        buy_calls = 0

        def start_buy() -> tuple[asyncio.Task[Any], int]:
            nonlocal buy_calls
            buy_calls += 1
            if on_start_buy is not None:
                on_start_buy()

            async def completed() -> tuple[MrktHttpResponse, None, int, int]:
                return MrktHttpResponse(200, {"ids": [GIFT_ID]}), None, clock(), clock()

            return asyncio.create_task(completed()), clock()

        result = await self.service()._watch_for_exact_listing(
            lookup_session=_MrktAuthSession(lookup, "token", None),
            listing_task=listing_task,
            gift_id=GIFT_ID,
            expected_seller_price_nanotons=SELLER_PRICE_NANOTONS,
            expected_buyer_price_nanotons=BUYER_PRICE_NANOTONS,
            expected_seller_id=expected_seller_id,
            sale_started_ns=0,
            max_wait_ms=max_wait_ms,
            poll_interval_ms=interval_ms,
            start_buy=start_buy,
            clock_ns=clock,
            sleeper=clock.sleep,
        )
        return result, buy_calls, listing_task

    async def finish_pending(self, task: asyncio.Task[Any]) -> None:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_listing_on_third_probe_starts_one_buy(self) -> None:
        lookup = FakeTransport(
            market_item_batches=[
                [],
                [],
                [gift(listed=True, price=BUYER_PRICE_NANOTONS)],
            ]
        )
        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["probe_count"], 3)
        self.assertEqual(result.metadata["stopped_reason"], "LISTING_CONFIRMED")
        self.assertEqual(buy_calls, 1)
        self.assertIsNotNone(result.buy_task)
        self.assertEqual(
            [call[0] for call in lookup.calls],
            [MRKT_MARKET_BY_IDS_PATH] * 3,
        )

    async def test_first_positive_probe_starts_buy_and_stops(self) -> None:
        lookup = FakeTransport(
            market_item_batches=[[gift(listed=True, price=BUYER_PRICE_NANOTONS)]]
        )
        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["probe_count"], 1)
        self.assertEqual(len(lookup.calls), 1)
        self.assertEqual(buy_calls, 1)

    async def test_job19_shape_triggers_on_second_probe_at_public_price(self) -> None:
        listing = gift(listed=True, price=BUYER_PRICE_NANOTONS)
        listing.update(price=SELLER_PRICE_NANOTONS, referencePrice=999_000_000)
        lookup = FakeTransport(market_item_batches=[[], [listing]])

        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["probe_count"], 2)
        self.assertFalse(
            result.metadata["probes"][0]["exact_listing_present"]
        )
        self.assertTrue(result.metadata["probes"][1]["exact_listing_present"])
        self.assertTrue(result.metadata["probes"][1]["exact_price_match"])
        self.assertEqual(len(lookup.calls), 2)
        self.assertEqual(buy_calls, 1)
        price = result.metadata["mrkt_price_correlation"]
        self.assertEqual(price["seller_price_nanotons"], SELLER_PRICE_NANOTONS)
        self.assertEqual(price["buyer_price_nanotons"], BUYER_PRICE_NANOTONS)
        self.assertEqual(
            price["observed_listing_price_nanotons"], BUYER_PRICE_NANOTONS
        )
        self.assertEqual(price["selected_field"], "salePrice")
        self.assertTrue(price["exact_price_match"])

    async def test_integer_string_public_price_is_normalized_exactly(self) -> None:
        listing = gift(listed=True)
        listing["salePrice"] = str(BUYER_PRICE_NANOTONS)
        lookup = FakeTransport(market_items=[listing])

        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(buy_calls, 1)
        self.assertTrue(result.metadata["probes"][0]["exact_price_match"])

    async def test_seller_and_reference_prices_never_replace_public_price(self) -> None:
        listing = gift(listed=True, price=SELLER_PRICE_NANOTONS)
        listing.update(price=BUYER_PRICE_NANOTONS, referencePrice=BUYER_PRICE_NANOTONS)
        lookup = FakeTransport(market_items=[listing])

        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(buy_calls, 0)
        self.assertTrue(result.metadata["probes"][0]["exact_listing_present"])
        self.assertFalse(result.metadata["probes"][0]["exact_price_match"])

    async def test_malformed_public_price_never_triggers_buy(self) -> None:
        listing = gift(listed=True)
        listing["salePrice"] = float(BUYER_PRICE_NANOTONS)
        lookup = FakeTransport(market_items=[listing])

        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(buy_calls, 0)
        self.assertFalse(result.metadata["probes"][0]["exact_price_match"])

    async def test_wrong_id_and_same_name_never_trigger_buy(self) -> None:
        wrong = gift(gift_id="different", listed=True, price=BUYER_PRICE_NANOTONS)
        wrong["title"] = "Plush Pepe"
        lookup = FakeTransport(market_items=[wrong])
        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["stopped_reason"], "TIMEOUT")
        self.assertEqual(buy_calls, 0)

    async def test_wrong_price_waits_for_exact_price(self) -> None:
        lookup = FakeTransport(
            market_item_batches=[
                [gift(listed=True, price=540_000_000)],
                [gift(listed=True, price=BUYER_PRICE_NANOTONS)],
            ]
        )
        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["probe_count"], 2)
        self.assertFalse(result.metadata["probes"][0]["exact_price_match"])
        self.assertTrue(result.metadata["probes"][1]["exact_price_match"])
        self.assertEqual(buy_calls, 1)

    async def test_seller_mismatch_is_ambiguous_and_never_buys(self) -> None:
        listing = dict(
            gift(listed=True, price=BUYER_PRICE_NANOTONS), sellerId="other-seller"
        )
        lookup = FakeTransport(market_items=[listing])
        result, buy_calls, sale_task = await self.watch(
            lookup, expected_seller_id="expected-seller"
        )
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["stopped_reason"], "AMBIGUOUS")
        self.assertEqual(buy_calls, 0)

    async def test_multiple_exact_results_are_ambiguous_and_never_buy(self) -> None:
        listing = gift(listed=True, price=BUYER_PRICE_NANOTONS)
        lookup = FakeTransport(market_items=[listing, dict(listing)])
        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["stopped_reason"], "AMBIGUOUS")
        self.assertEqual(buy_calls, 0)

    async def test_pending_sale_does_not_block_positive_listing(self) -> None:
        lookup = FakeTransport(
            market_items=[gift(listed=True, price=BUYER_PRICE_NANOTONS)]
        )
        result, buy_calls, sale_task = await self.watch(lookup)

        self.assertFalse(sale_task.done())
        self.assertEqual(buy_calls, 1)
        self.assertIsNotNone(result.buy_task)
        await self.finish_pending(sale_task)

    async def test_explicit_sale_failure_stops_without_probe_or_buy(self) -> None:
        lookup = FakeTransport(
            market_items=[gift(listed=True, price=BUYER_PRICE_NANOTONS)]
        )
        result, buy_calls, sale_task = await self.watch(
            lookup,
            sale_result=(MrktHttpResponse(400, {"code": "REJECTED"}), None, 0),
        )

        self.assertTrue(sale_task.done())
        self.assertEqual(result.metadata["stopped_reason"], "SALE_FAILED")
        self.assertEqual(buy_calls, 0)
        self.assertEqual(lookup.calls, [])

    async def test_timeout_never_buys(self) -> None:
        result, buy_calls, sale_task = await self.watch(FakeTransport())
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["stopped_reason"], "TIMEOUT")
        self.assertEqual(buy_calls, 0)
        self.assertLessEqual(result.metadata["probe_count"], 64)

    async def test_http_429_stops_after_one_probe(self) -> None:
        lookup = FakeTransport(
            market_responses=[MrktHttpResponse(429, {"code": "RATE_LIMIT"})]
        )
        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["stopped_reason"], "RATE_LIMITED")
        self.assertEqual(result.metadata["probe_count"], 1)
        self.assertEqual(buy_calls, 0)

    async def test_read_error_is_terminal_sanitized_and_never_buys(self) -> None:
        lookup = FakeTransport(unexpected_failure=RuntimeError("secret-token"))
        result, buy_calls, sale_task = await self.watch(lookup)
        await self.finish_pending(sale_task)

        self.assertEqual(result.metadata["stopped_reason"], "READ_ERROR")
        self.assertEqual(buy_calls, 0)
        self.assertNotIn("secret-token", str(result.metadata))

    async def test_buy_submission_precedes_diagnostic_formatting(self) -> None:
        events: list[str] = []
        lookup = FakeTransport(
            market_items=[gift(listed=True, price=BUYER_PRICE_NANOTONS)]
        )
        service = self.service()
        original = service._listed_trigger_probe_metadata

        def formatted(**kwargs: Any) -> dict[str, Any]:
            events.append("metadata")
            return original(**kwargs)

        with patch.object(service, "_listed_trigger_probe_metadata", new=formatted):
            clock = FakeClock(0)
            release = asyncio.Event()

            async def sale() -> tuple[MrktHttpResponse, None, int]:
                await release.wait()
                return MrktHttpResponse(200, {"ids": [GIFT_ID]}), None, clock()

            listing_task = asyncio.create_task(sale())

            def start_buy() -> tuple[asyncio.Task[Any], int]:
                events.append("buy_submitted")

                async def done() -> tuple[MrktHttpResponse, None, int, int]:
                    return MrktHttpResponse(200, []), None, clock(), clock()

                return asyncio.create_task(done()), clock()

            await service._watch_for_exact_listing(
                lookup_session=_MrktAuthSession(lookup, "token", None),
                listing_task=listing_task,
                gift_id=GIFT_ID,
                expected_seller_price_nanotons=SELLER_PRICE_NANOTONS,
                expected_buyer_price_nanotons=BUYER_PRICE_NANOTONS,
                expected_seller_id=None,
                sale_started_ns=0,
                max_wait_ms=100,
                poll_interval_ms=10,
                start_buy=start_buy,
                clock_ns=clock,
                sleeper=clock.sleep,
            )
            await self.finish_pending(listing_task)

        self.assertEqual(events[:2], ["buy_submitted", "metadata"])


class ListedTriggeredIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def execute(
        self,
        *,
        buy_response: MrktHttpResponse,
        buyer_inventories: list[list[dict[str, Any]]],
        seller_inventories: list[list[dict[str, Any]]],
        lookup_batches: list[list[dict[str, Any]]],
        sale_delay: float = 0.05,
    ) -> tuple[Any, FakeTransport, FakeTransport, FakeTransport, list[str]]:
        events: list[str] = []
        buyer = FakeTransport(
            inventories=buyer_inventories,
            buy_response=buy_response,
            events=events,
        )
        seller = DelayedSaleTransport(
            sale_delay,
            inventories=seller_inventories,
            events=events,
        )
        lookup = FakeTransport(market_item_batches=lookup_batches, events=events)
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService(events)),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller, lookup]),
        )
        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=530_000_000,
            dry_run=False,
            transfer_mode=MrktTransferMode.LISTED_TRIGGERED,
            listed_trigger_max_wait_ms=500,
            listed_trigger_poll_interval_ms=1,
        )
        return result, buyer, seller, lookup, events

    async def test_exact_third_probe_buys_once_before_sale_response(self) -> None:
        result, buyer, seller, lookup, events = await self.execute(
            buy_response=MrktHttpResponse(200, {"ids": [GIFT_ID]}),
            buyer_inventories=[[gift()], []],
            seller_inventories=[[gift()], [], []],
            lookup_batches=[
                [],
                [],
                [gift(listed=True, price=BUYER_PRICE_NANOTONS)],
            ],
        )

        self.assertEqual(result.status, MrktFastTransferStatus.SUCCESS)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 1)
        buy_payload = next(c[1] for c in buyer.calls if c[0] == MRKT_BUY_PATH)
        self.assertEqual(
            buy_payload,
            {"ids": [GIFT_ID], "prices": {GIFT_ID: BUYER_PRICE_NANOTONS}},
        )
        self.assertEqual(
            [c[0] for c in lookup.calls].count(MRKT_MARKET_BY_IDS_PATH), 3
        )
        self.assertGreater(
            result.timings["mrkt_sale_response_relative_to_buy_start_ms"] or 0,
            0,
        )
        self.assertEqual(result.listed_trigger["probe_count"], 3)
        sale_index = events.index(MRKT_SALE_PATH)
        auth_indices = [
            index for index, event in enumerate(events) if event == MRKT_AUTH_PATH
        ]
        self.assertEqual(len(auth_indices), 3)
        self.assertTrue(all(index < sale_index for index in auth_indices))

    async def test_http_200_empty_buy_is_not_assumed_success(self) -> None:
        result, buyer, seller, _, _ = await self.execute(
            buy_response=MrktHttpResponse(200, []),
            buyer_inventories=[[], []],
            seller_inventories=[
                [gift()],
                [],
                [gift(listed=True, price=BUYER_PRICE_NANOTONS)],
            ],
            lookup_batches=[[gift(listed=True, price=BUYER_PRICE_NANOTONS)]],
        )

        self.assertEqual(result.status, MrktFastTransferStatus.BUY_REJECTED)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 1)

    async def test_complete_absence_is_external_sale(self) -> None:
        result, buyer, seller, _, _ = await self.execute(
            buy_response=MrktHttpResponse(200, []),
            buyer_inventories=[[], []],
            seller_inventories=[[gift()], [], []],
            lookup_batches=[[gift(listed=True, price=BUYER_PRICE_NANOTONS)]],
        )

        self.assertEqual(result.status, MrktFastTransferStatus.EXTERNAL_SALE)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 1)

    async def test_timeout_returns_listing_not_observed_without_buy(self) -> None:
        buyer = FakeTransport(inventories=[[], []])
        seller = FakeTransport(inventories=[[gift()], [gift()], []])
        lookup = FakeTransport()
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller, lookup]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=530_000_000,
            dry_run=False,
            transfer_mode=MrktTransferMode.LISTED_TRIGGERED,
            listed_trigger_max_wait_ms=100,
            listed_trigger_poll_interval_ms=10,
        )

        self.assertEqual(result.status, MrktFastTransferStatus.LISTING_NOT_OBSERVED)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 0)

    async def test_watcher_429_returns_rate_limited_without_buy(self) -> None:
        buyer = FakeTransport(inventories=[[], []])
        seller = FakeTransport(inventories=[[gift()], [gift()], []])
        lookup = FakeTransport(
            market_responses=[MrktHttpResponse(429, {"code": "RATE_LIMIT"})]
        )
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller, lookup]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=530_000_000,
            dry_run=False,
            transfer_mode=MrktTransferMode.LISTED_TRIGGERED,
            listed_trigger_max_wait_ms=100,
            listed_trigger_poll_interval_ms=10,
        )

        self.assertEqual(result.status, MrktFastTransferStatus.LISTING_RATE_LIMITED)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 0)

    async def test_explicit_sale_failure_returns_rejected_without_buy(self) -> None:
        buyer = FakeTransport(inventories=[[], []])
        seller = FakeTransport(
            inventories=[[gift()], [gift()], []],
            sale_response=MrktHttpResponse(400, {"code": "REJECTED"}),
        )
        lookup = FakeTransport()
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller, lookup]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=530_000_000,
            dry_run=False,
            transfer_mode=MrktTransferMode.LISTED_TRIGGERED,
            listed_trigger_max_wait_ms=100,
            listed_trigger_poll_interval_ms=10,
        )

        self.assertEqual(result.status, MrktFastTransferStatus.LISTING_REJECTED)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 0)
