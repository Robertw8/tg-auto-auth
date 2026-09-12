from __future__ import annotations

import asyncio
import threading
import time
import unittest
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import urlencode

from app.telegram_client.miniapp_service import MiniAppLaunchResult, MiniAppService
from app.telegram_client.mrkt_service import (
    MRKT_AUTH_PATH,
    MRKT_BUY_PATH,
    MRKT_INVENTORY_PATH,
    MRKT_MARKET_BY_IDS_PATH,
    MRKT_SALE_PATH,
    MrktApiError,
    MrktConfirmationUsedError,
    MrktFastTransferStatus,
    MrktGiftUnavailableError,
    MrktHttpResponse,
    MrktListingInFlightError,
    MrktListingStatus,
    MrktMutationNetworkError,
    MrktNetworkError,
    MrktPriceError,
    MrktPurchaseStatus,
    MrktService,
    MrktServiceError,
    _KeepAliveMrktTransport,
)

OWNER_ID = 100
ACCOUNT_ID = 7
GIFT_ID = "opaque-gift-id"
SECRET_INIT_DATA = "query_id=secret-query&" + urlencode(
    {
        "user": '{"id":100}',
        "auth_date": "1720000000",
        "hash": "secret-init-hash",
    }
)
SECRET_TOKEN = "secret-mrkt-token"
WEBVIEW_URL = "https://cdn.tgmrkt.io/index.html#" + urlencode(
    {"tgWebAppData": SECRET_INIT_DATA}
)


def launch_result() -> MiniAppLaunchResult:
    return MiniAppLaunchResult(
        account_id=ACCOUNT_ID,
        bot_username="@mrkt",
        resolved_bot_id=1,
        webview_obtained=True,
        init_data_obtained=True,
        init_data_fingerprint="12345678",
        init_data_fields=frozenset({"auth_date", "hash", "user"}),
        webview_url=WEBVIEW_URL,
        init_data=SECRET_INIT_DATA,
        query_id=1,
    )


class FakeMiniAppService:
    def __init__(self, events: list[str] | None = None) -> None:
        self.calls: list[tuple[int, str, int]] = []
        self.events = events

    async def open_miniapp(
        self,
        account_id: int,
        bot_username: str,
        *,
        owner_telegram_id: int,
    ) -> MiniAppLaunchResult:
        self.calls.append((account_id, bot_username, owner_telegram_id))
        if self.events is not None:
            self.events.append(f"webview:{account_id}")
        return launch_result()


class FakeTransport:
    def __init__(
        self,
        *,
        inventories: list[list[dict[str, Any]]] | None = None,
        sale_response: MrktHttpResponse | None = None,
        sale_failure: Exception | None = None,
        unexpected_failure: Exception | None = None,
        market_items: list[dict[str, Any]] | None = None,
        market_item_batches: list[list[dict[str, Any]]] | None = None,
        market_responses: list[MrktHttpResponse] | None = None,
        buy_response: MrktHttpResponse | None = None,
        buy_failure: Exception | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.inventories = list(inventories or [])
        self.sale_response = sale_response or MrktHttpResponse(
            status=200,
            body={"ids": [GIFT_ID]},
        )
        self.sale_failure = sale_failure
        self.unexpected_failure = unexpected_failure
        self.market_items = list(market_items or [])
        self.market_item_batches = list(market_item_batches or [])
        self.market_responses = list(market_responses or [])
        self.buy_response = buy_response or MrktHttpResponse(
            status=200,
            body={"ids": [GIFT_ID]},
        )
        self.buy_failure = buy_failure
        self.calls: list[tuple[str, dict[str, Any], str | None]] = []
        self.closed = False
        self.events = events

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> MrktHttpResponse:
        if self.unexpected_failure is not None:
            raise self.unexpected_failure
        self.calls.append((path, deepcopy(dict(payload)), authorization))
        if self.events is not None:
            self.events.append(path)
        if path == MRKT_AUTH_PATH:
            return MrktHttpResponse(
                status=200,
                body={"token": SECRET_TOKEN, "isFirstTime": False},
            )
        if path == MRKT_INVENTORY_PATH:
            gifts = self.inventories.pop(0) if self.inventories else []
            return MrktHttpResponse(
                status=200,
                body={"cursor": "", "gifts": gifts, "total": len(gifts)},
            )
        if path == MRKT_SALE_PATH:
            if self.sale_failure is not None:
                raise self.sale_failure
            return self.sale_response
        if path == MRKT_MARKET_BY_IDS_PATH:
            if self.market_responses:
                return self.market_responses.pop(0)
            items = (
                self.market_item_batches.pop(0)
                if self.market_item_batches
                else self.market_items
            )
            return MrktHttpResponse(
                status=200,
                body={"gifts": items},
            )
        if path == MRKT_BUY_PATH:
            if self.buy_failure is not None:
                raise self.buy_failure
            return self.buy_response
        raise AssertionError(f"Unexpected path: {path}")

    def close(self) -> None:
        self.closed = True

    @property
    def successful_requests(self) -> int:
        return len(self.calls)

    @property
    def connection_reused(self) -> bool:
        return len(self.calls) > 1


class TransportQueue:
    def __init__(self, transports: list[FakeTransport]) -> None:
        self.transports = transports
        self.created: list[FakeTransport] = []

    def __call__(self) -> FakeTransport:
        if not self.transports:
            raise AssertionError("No fake transport remaining")
        transport = self.transports.pop(0)
        self.created.append(transport)
        return transport


class BlockingSaleTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__(inventories=[[gift()]])
        self.sale_started = threading.Event()
        self.release_sale = threading.Event()

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> MrktHttpResponse:
        if path == MRKT_SALE_PATH:
            self.calls.append((path, dict(payload), authorization))
            self.sale_started.set()
            if not self.release_sale.wait(timeout=5):
                raise AssertionError("Timed out waiting to release fake sale")
            return self.sale_response
        return super().post_json(path, payload, authorization=authorization)


class DelayedSaleTransport(FakeTransport):
    def __init__(self, delay_seconds: float, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.delay_seconds = delay_seconds

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> MrktHttpResponse:
        if path == MRKT_SALE_PATH:
            self.calls.append((path, deepcopy(dict(payload)), authorization))
            if self.events is not None:
                self.events.append(path)
            time.sleep(self.delay_seconds)
            if self.sale_failure is not None:
                raise self.sale_failure
            return self.sale_response
        return super().post_json(path, payload, authorization=authorization)


def gift(
    *,
    gift_id: Any = GIFT_ID,
    listed: bool = False,
    price: int | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "id": gift_id,
        "title": "Plush Pepe",
        "number": 42,
        "isLockedForSale": False,
    }
    if listed:
        value["isListed"] = True
    if price is not None:
        value["salePrice"] = price
    return value


class MrktPriceTests(unittest.TestCase):
    def test_decimal_ton_conversion(self) -> None:
        self.assertEqual(MrktService.ton_to_nanotons("1.68"), 1_680_000_000)
        self.assertEqual(MrktService.ton_to_nanotons("1"), 1_000_000_000)
        self.assertEqual(MrktService.ton_to_nanotons("0.01"), 10_000_000)
        self.assertEqual(
            MrktService.ton_to_nanotons(Decimal("1.68")),
            1_680_000_000,
        )

    def test_invalid_decimals_are_rejected(self) -> None:
        for value in ("1.001", "1.000", "1e2", "abc", "", ".5", "1."):
            with self.subTest(value=value), self.assertRaises(MrktPriceError):
                MrktService.ton_to_nanotons(value)

    def test_negative_and_zero_prices_are_rejected(self) -> None:
        for value in ("0", "0.00", "-1", "-0.01"):
            with self.subTest(value=value), self.assertRaises(MrktPriceError):
                MrktService.ton_to_nanotons(value)

    def test_seller_price_to_public_buyer_price_is_exact(self) -> None:
        self.assertEqual(
            MrktService.seller_to_buyer_price_nanotons(3_520_000_000),
            3_590_400_000,
        )
        self.assertEqual(
            MrktService.seller_to_buyer_price_nanotons(5_000_000_000),
            5_100_000_000,
        )
        self.assertEqual(
            MrktService.seller_to_buyer_price_nanotons(3_500_000_000),
            3_570_000_000,
        )

    def test_seller_price_conversion_rejects_non_integral_nanotons(self) -> None:
        for value in (True, 0, -1, 1):
            with self.subTest(value=value), self.assertRaises(MrktPriceError):
                MrktService.seller_to_buyer_price_nanotons(value)

    def test_listing_price_parser_is_strict(self) -> None:
        self.assertEqual(
            MrktService.extract_mrkt_listing_price_nanotons(
                {"salePrice": 3_590_400_000}
            ),
            3_590_400_000,
        )
        self.assertEqual(
            MrktService.extract_mrkt_listing_price_nanotons(
                {"salePrice": "3590400000"}
            ),
            3_590_400_000,
        )
        for value in (True, 3_590_400_000.0, "3.5904", " 3590400000 ", None):
            with self.subTest(value=value):
                self.assertIsNone(
                    MrktService.extract_mrkt_listing_price_nanotons(
                        {"salePrice": value}
                    )
                )


class MrktServiceTests(unittest.IsolatedAsyncioTestCase):
    def service(
        self,
        transports: list[FakeTransport],
        *,
        dry_run: bool,
    ) -> tuple[MrktService, TransportQueue]:
        queue = TransportQueue(transports)
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=dry_run,
            transport_factory=queue,
        )
        return service, queue

    async def confirmation(self, service: MrktService) -> str:
        confirmation = await service.prepare_confirmation(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id=GIFT_ID,
            display_name="Plush Pepe #42",
            price_ton="1.68",
        )
        return confirmation.token

    async def test_empty_inventory(self) -> None:
        transport = FakeTransport(inventories=[[]])
        service, _ = self.service([transport], dry_run=True)

        inventory = await service.get_inventory(
            ACCOUNT_ID,
            owner_telegram_id=OWNER_ID,
        )

        self.assertEqual(inventory, [])
        self.assertTrue(transport.closed)

    def test_transport_reuses_connection_and_cookies(self) -> None:
        connection = MagicMock()
        first = MagicMock(status=200)
        first.read.return_value = b'{"token":"secret"}'
        first.getheaders.return_value = [("Set-Cookie", "sid=opaque; HttpOnly")]
        second = MagicMock(status=200)
        second.read.return_value = b'{"gifts":[]}'
        second.getheaders.return_value = []
        connection.getresponse.side_effect = [first, second]
        with patch(
            "app.telegram_client.mrkt_service.http.client.HTTPSConnection",
            return_value=connection,
        ) as factory:
            transport = _KeepAliveMrktTransport()
            transport.post_json(MRKT_AUTH_PATH, {"data": "opaque"})
            transport.post_json(
                MRKT_INVENTORY_PATH,
                {"isListed": False},
                authorization="opaque-token",
            )
            transport.close()

        factory.assert_called_once()
        self.assertEqual(connection.request.call_count, 2)
        second_headers = connection.request.call_args_list[1].kwargs["headers"]
        self.assertEqual(second_headers["Cookie"], "sid=opaque")
        connection.close.assert_called_once()

    async def test_job10_fast_path_has_no_read_between_listing_and_buy(self) -> None:
        events: list[str] = []
        miniapp = FakeMiniAppService(events)
        buyer = FakeTransport(
            inventories=[[gift(gift_id=163035)], []],
            buy_response=MrktHttpResponse(200, {"ids": ["163035"]}),
            events=events,
        )
        seller = FakeTransport(
            inventories=[[gift(gift_id=163035)], [], []],
            sale_response=MrktHttpResponse(200, {"ids": [163035]}),
            events=events,
        )
        service = MrktService(
            cast(MiniAppService, miniapp),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=163035,
            price_nanotons=530_000_000,
            dry_run=False,
        )

        self.assertEqual(result.status, MrktFastTransferStatus.SUCCESS)
        listing_index = events.index(MRKT_SALE_PATH)
        buy_index = events.index(MRKT_BUY_PATH)
        self.assertEqual(events[listing_index : buy_index + 1], [MRKT_SALE_PATH, MRKT_BUY_PATH])
        self.assertEqual(events.count(MRKT_SALE_PATH), 1)
        self.assertEqual(events.count(MRKT_BUY_PATH), 1)
        self.assertEqual(len(miniapp.calls), 2)
        self.assertTrue(
            all(
                index < listing_index
                for index, event in enumerate(events)
                if event.startswith("webview:") or event == MRKT_AUTH_PATH
            )
        )
        buy_call = next(call for call in buyer.calls if call[0] == MRKT_BUY_PATH)
        self.assertEqual(
            buy_call[1],
            {"ids": ["163035"], "prices": {"163035": 540_600_000}},
        )
        sale_call = next(call for call in seller.calls if call[0] == MRKT_SALE_PATH)
        self.assertEqual(sale_call[1], {"ids": [163035], "price": 530_000_000})
        self.assertIsNotNone(result.timings["mrkt_listing_to_buy_start_ms"])

    async def test_fast_path_listing_failure_or_timeout_never_buys(self) -> None:
        cases = (
            MrktHttpResponse(400, {"code": "REJECTED"}),
            MrktHttpResponse(500, {"code": "TEMPORARY"}),
        )
        for listing_response in cases:
            with self.subTest(status=listing_response.status):
                buyer = FakeTransport()
                seller = FakeTransport(
                    inventories=[[gift()], [], []],
                    sale_response=listing_response,
                )
                service = MrktService(
                    cast(MiniAppService, FakeMiniAppService()),
                    dry_run=False,
                    transport_factory=TransportQueue([buyer, seller]),
                )
                result = await service.execute_fast_transfer(
                    owner_telegram_id=OWNER_ID,
                    buyer_account_id=1,
                    seller_account_id=2,
                    gift_id=GIFT_ID,
                    price_nanotons=1_680_000_000,
                    dry_run=False,
                )
                self.assertIn(
                    result.status,
                    {
                        MrktFastTransferStatus.LISTING_REJECTED,
                        MrktFastTransferStatus.LISTING_AMBIGUOUS,
                    },
                )
                self.assertEqual(
                    [path for path, _, _ in buyer.calls].count(MRKT_BUY_PATH), 0
                )

        buyer = FakeTransport()
        seller = FakeTransport(
            inventories=[[gift()], [], []],
            sale_failure=MrktMutationNetworkError("secret"),
        )
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )
        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=1_680_000_000,
            dry_run=False,
        )
        self.assertEqual(result.status, MrktFastTransferStatus.LISTING_AMBIGUOUS)
        self.assertEqual([p for p, _, _ in seller.calls].count(MRKT_SALE_PATH), 1)
        self.assertEqual([p for p, _, _ in buyer.calls].count(MRKT_BUY_PATH), 0)

    async def test_fast_path_buy_timeout_is_single_shot_and_verified(self) -> None:
        buyer = FakeTransport(
            inventories=[[], []],
            buy_failure=MrktMutationNetworkError("secret"),
        )
        seller = FakeTransport(inventories=[[gift()], [], []])
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=1_680_000_000,
            dry_run=False,
        )

        self.assertEqual(result.status, MrktFastTransferStatus.EXTERNAL_SALE)
        self.assertEqual([p for p, _, _ in buyer.calls].count(MRKT_BUY_PATH), 1)

    async def test_fast_path_rejects_different_gift_before_mutation(self) -> None:
        buyer = FakeTransport()
        seller = FakeTransport(inventories=[[gift(gift_id="different")]])
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        with self.assertRaises(MrktGiftUnavailableError):
            await service.execute_fast_transfer(
                owner_telegram_id=OWNER_ID,
                buyer_account_id=1,
                seller_account_id=2,
                gift_id=GIFT_ID,
                price_nanotons=1_680_000_000,
                dry_run=False,
            )

        self.assertNotIn(MRKT_SALE_PATH, [path for path, _, _ in seller.calls])
        self.assertNotIn(MRKT_BUY_PATH, [path for path, _, _ in buyer.calls])

    async def test_fast_path_does_not_sleep_between_mutations(self) -> None:
        events: list[str] = []
        buyer = FakeTransport(inventories=[[gift()], []], events=events)
        seller = FakeTransport(inventories=[[gift()], [], []], events=events)
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService(events)),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        with patch(
            "app.telegram_client.mrkt_service.asyncio.sleep", new=AsyncMock()
        ) as sleep:
            await service.execute_fast_transfer(
                owner_telegram_id=OWNER_ID,
                buyer_account_id=1,
                seller_account_id=2,
                gift_id=GIFT_ID,
                price_nanotons=1_680_000_000,
                dry_run=False,
            )

        sleep.assert_not_awaited()

    async def test_speculative_sale_failure_before_delay_never_buys(self) -> None:
        buyer = FakeTransport()
        seller = FakeTransport(
            inventories=[[gift()], [], []],
            sale_response=MrktHttpResponse(400, {"code": "REJECTED"}),
        )
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=530_000_000,
            dry_run=False,
            speculative_buy=True,
            speculative_buy_delay_ms=10,
        )

        self.assertEqual(result.status, MrktFastTransferStatus.LISTING_REJECTED)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 0)

    async def test_speculative_buy_overlaps_sale_once_with_prepared_values(self) -> None:
        events: list[str] = []
        buyer = FakeTransport(
            inventories=[[gift(gift_id=163035)], []],
            buy_response=MrktHttpResponse(200, {"ids": ["163035"]}),
            events=events,
        )
        seller = DelayedSaleTransport(
            0.05,
            inventories=[[gift(gift_id=163035)], [], []],
            sale_response=MrktHttpResponse(200, {"ids": [163035]}),
            events=events,
        )
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService(events)),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=163035,
            price_nanotons=530_000_000,
            dry_run=False,
            speculative_buy=True,
            speculative_buy_delay_ms=5,
        )

        self.assertEqual(result.status, MrktFastTransferStatus.SUCCESS)
        self.assertEqual(events.count(MRKT_SALE_PATH), 1)
        self.assertEqual(events.count(MRKT_BUY_PATH), 1)
        sale_index = events.index(MRKT_SALE_PATH)
        buy_index = events.index(MRKT_BUY_PATH)
        self.assertEqual(events[sale_index : buy_index + 1], [MRKT_SALE_PATH, MRKT_BUY_PATH])
        self.assertGreater(
            result.timings["mrkt_sale_response_relative_to_buy_start_ms"] or 0,
            0,
        )
        self.assertIsNotNone(result.timings["mrkt_sale_start_to_buy_start_ms"])
        self.assertIsNotNone(result.timings["mrkt_sale_start_to_sale_response_ms"])
        self.assertIsNotNone(result.timings["mrkt_buy_start_to_buy_response_ms"])
        self.assertLess(
            result.timings["mrkt_listing_to_buy_start_ms"] or 0,
            0,
        )
        sale_payload = next(c[1] for c in seller.calls if c[0] == MRKT_SALE_PATH)
        buy_payload = next(c[1] for c in buyer.calls if c[0] == MRKT_BUY_PATH)
        self.assertEqual(sale_payload, {"ids": [163035], "price": 530_000_000})
        self.assertEqual(
            buy_payload,
            {"ids": ["163035"], "prices": {"163035": 540_600_000}},
        )
        reuse = result.verification["connection_reuse"]
        self.assertIsInstance(reuse, dict)
        assert isinstance(reuse, dict)
        self.assertTrue(reuse["n1_prepared_before_sale"])
        self.assertTrue(reuse["n2_prepared_before_sale"])
        self.assertTrue(reuse["n1_reused_for_buy"])
        self.assertTrue(reuse["n2_reused_for_sale"])

    async def test_speculative_too_early_buy_is_not_retried(self) -> None:
        buyer = FakeTransport(
            inventories=[[], []],
            buy_response=MrktHttpResponse(404, {"code": "NOT_FOUND"}),
        )
        seller = DelayedSaleTransport(
            0.03,
            inventories=[[gift()], [], [gift(listed=True, price=540_600_000)]],
        )
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=530_000_000,
            dry_run=False,
            speculative_buy=True,
            speculative_buy_delay_ms=0,
        )

        self.assertEqual(
            result.status, MrktFastTransferStatus.SPECULATIVE_BUY_TOO_EARLY
        )
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 1)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)

    async def test_speculative_ambiguous_buy_is_never_retried(self) -> None:
        buyer = FakeTransport(
            inventories=[[], []],
            buy_failure=MrktMutationNetworkError("sanitized"),
        )
        seller = DelayedSaleTransport(
            0.02,
            inventories=[[gift()], [], [gift(listed=True, price=540_600_000)]],
        )
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        result = await service.execute_fast_transfer(
            owner_telegram_id=OWNER_ID,
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            price_nanotons=530_000_000,
            dry_run=False,
            speculative_buy=True,
            speculative_buy_delay_ms=0,
        )

        self.assertEqual(result.status, MrktFastTransferStatus.BUY_AMBIGUOUS)
        self.assertEqual([c[0] for c in buyer.calls].count(MRKT_BUY_PATH), 1)
        self.assertEqual([c[0] for c in seller.calls].count(MRKT_SALE_PATH), 1)

    async def test_read_latency_benchmark_calls_inventory_only(self) -> None:
        transport = FakeTransport(inventories=[[], [], []])
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            transport_factory=TransportQueue([transport]),
        )
        with patch(
            "app.telegram_client.mrkt_service.asyncio.sleep", new=AsyncMock()
        ) as sleep:
            result = await service.benchmark_read_latency(
                ACCOUNT_ID,
                owner_telegram_id=OWNER_ID,
                samples=3,
                spacing_ms=100,
            )

        self.assertEqual(result.sample_count, 3)
        self.assertEqual([c[0] for c in transport.calls].count(MRKT_INVENTORY_PATH), 3)
        self.assertNotIn(MRKT_SALE_PATH, [c[0] for c in transport.calls])
        self.assertNotIn(MRKT_BUY_PATH, [c[0] for c in transport.calls])
        self.assertEqual(sleep.await_count, 2)

    def test_benchmark_script_has_no_mutation_endpoint(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "benchmark_mrkt_latency.py"
        ).read_text()
        self.assertNotIn("/gifts/sale", source)
        self.assertNotIn("/gifts/buy", source)
        self.assertIn("benchmark_read_latency", source)

    async def test_gift_disappears_before_confirmation(self) -> None:
        transport = FakeTransport(inventories=[[]])
        service, _ = self.service([transport], dry_run=False)
        token = await self.confirmation(service)

        with self.assertRaises(MrktGiftUnavailableError):
            await service.confirm_listing(token, owner_telegram_id=OWNER_ID)

        self.assertNotIn(MRKT_SALE_PATH, [call[0] for call in transport.calls])

    async def test_duplicate_confirm_click_is_single_use(self) -> None:
        transport = FakeTransport(inventories=[[gift()]])
        service, _ = self.service([transport], dry_run=False)
        token = await self.confirmation(service)

        first = await service.confirm_listing(token, owner_telegram_id=OWNER_ID)
        with self.assertRaises(MrktConfirmationUsedError):
            await service.confirm_listing(token, owner_telegram_id=OWNER_ID)

        self.assertEqual(first.status, MrktListingStatus.SUCCESS)
        self.assertEqual(
            [call[0] for call in transport.calls].count(MRKT_SALE_PATH),
            1,
        )

    async def test_purchase_uses_exact_frontend_contract_once(self) -> None:
        listed = gift(listed=True, price=1_680_000_000)
        listed["sellerId"] = "seller-2"
        transport = FakeTransport(
            market_items=[listed],
            buy_response=MrktHttpResponse(status=200, body={"ids": [GIFT_ID]}),
        )
        service, _ = self.service([transport], dry_run=False)

        result = await service.buy_listing(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id=GIFT_ID,
            expected_price_nanotons=1_680_000_000,
            expected_seller_id="seller-2",
            dry_run=False,
        )

        self.assertEqual(result.status, MrktPurchaseStatus.SUCCESS)
        buy_calls = [call for call in transport.calls if call[0] == MRKT_BUY_PATH]
        self.assertEqual(
            buy_calls,
            [
                (
                    MRKT_BUY_PATH,
                    {
                        "ids": [GIFT_ID],
                        "prices": {GIFT_ID: 1_680_000_000},
                    },
                    SECRET_TOKEN,
                )
            ],
        )

    async def test_purchase_rejects_changed_price_seller_and_ambiguous_match(
        self,
    ) -> None:
        cases = (
            ([gift(listed=True, price=2_000_000_000)], "seller-2"),
            (
                [dict(gift(listed=True, price=1_680_000_000), sellerId="wrong")],
                "seller-2",
            ),
            (
                [
                    gift(listed=True, price=1_680_000_000),
                    gift(listed=True, price=1_680_000_000),
                ],
                None,
            ),
        )
        for market_items, seller_id in cases:
            with self.subTest(items=market_items):
                transport = FakeTransport(market_items=market_items)
                service, _ = self.service([transport], dry_run=False)
                with self.assertRaises(MrktGiftUnavailableError):
                    await service.buy_listing(
                        owner_telegram_id=OWNER_ID,
                        account_id=ACCOUNT_ID,
                        gift_id=GIFT_ID,
                        expected_price_nanotons=1_680_000_000,
                        expected_seller_id=seller_id,
                        dry_run=False,
                    )
                self.assertNotIn(
                    MRKT_BUY_PATH,
                    [path for path, _, _ in transport.calls],
                )

    async def test_purchase_network_failure_is_not_retried(self) -> None:
        listed = dict(gift(listed=True, price=1_680_000_000), sellerId="seller-2")
        transport = FakeTransport(
            market_items=[listed],
            buy_failure=MrktMutationNetworkError("secret-token"),
        )
        service, _ = self.service([transport], dry_run=False)

        with self.assertRaises(MrktMutationNetworkError):
            await service.buy_listing(
                owner_telegram_id=OWNER_ID,
                account_id=ACCOUNT_ID,
                gift_id=GIFT_ID,
                expected_price_nanotons=1_680_000_000,
                expected_seller_id="seller-2",
                dry_run=False,
            )

        self.assertEqual(
            [path for path, _, _ in transport.calls].count(MRKT_BUY_PATH),
            1,
        )

    async def test_purchase_unknown_response_is_ambiguous(self) -> None:
        transport = FakeTransport(
            market_items=[gift(listed=True, price=1_680_000_000)],
            buy_response=MrktHttpResponse(200, {"success": True}),
        )
        service, _ = self.service([transport], dry_run=False)
        result = await service.buy_listing(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id=GIFT_ID,
            expected_price_nanotons=1_680_000_000,
            dry_run=False,
        )
        self.assertEqual(result.status, MrktPurchaseStatus.AMBIGUOUS)
        self.assertEqual([p for p, _, _ in transport.calls].count(MRKT_BUY_PATH), 1)

    async def test_purchase_unexpected_exception_is_sanitized_and_not_retried(
        self,
    ) -> None:
        transport = FakeTransport(
            market_items=[gift(listed=True, price=1_680_000_000)],
            buy_failure=ValueError("authorization=SECRET-EXCEPTION"),
        )
        service, _ = self.service([transport], dry_run=False)
        with (
            self.assertLogs(
                "app.telegram_client.mrkt_service", level="WARNING"
            ) as logs,
            self.assertRaises(MrktMutationNetworkError) as caught,
        ):
            await service.buy_listing(
                owner_telegram_id=OWNER_ID,
                account_id=ACCOUNT_ID,
                gift_id=GIFT_ID,
                expected_price_nanotons=1_680_000_000,
                dry_run=False,
            )
        self.assertNotIn("SECRET-EXCEPTION", str(caught.exception) + str(logs.output))
        self.assertEqual([p for p, _, _ in transport.calls].count(MRKT_BUY_PATH), 1)

    async def test_account_in_flight_lock_consumes_second_confirmation(self) -> None:
        transport = BlockingSaleTransport()
        service, _ = self.service([transport], dry_run=False)
        first_token = await self.confirmation(service)
        first_task = asyncio.create_task(
            service.confirm_listing(first_token, owner_telegram_id=OWNER_ID)
        )
        started = await asyncio.to_thread(transport.sale_started.wait, 2)
        self.assertTrue(started)

        second_token = await self.confirmation(service)
        with self.assertRaises(MrktListingInFlightError):
            await service.confirm_listing(
                second_token,
                owner_telegram_id=OWNER_ID,
            )
        with self.assertRaises(MrktConfirmationUsedError):
            await service.confirm_listing(
                second_token,
                owner_telegram_id=OWNER_ID,
            )

        transport.release_sale.set()
        first_result = await first_task
        self.assertEqual(first_result.status, MrktListingStatus.SUCCESS)
        self.assertEqual(
            [call[0] for call in transport.calls].count(MRKT_SALE_PATH),
            1,
        )

    async def test_successful_sale_uses_exact_opaque_id_and_nanotons(self) -> None:
        opaque_id = {"server": "value", "revision": 3}
        transport = FakeTransport(
            inventories=[[gift(gift_id=opaque_id)]],
            sale_response=MrktHttpResponse(status=200, body={"ids": [opaque_id]}),
        )
        service, _ = self.service([transport], dry_run=False)
        confirmation = await service.prepare_confirmation(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id=opaque_id,
            display_name="Plush Pepe #42",
            price_ton="1.68",
        )

        result = await service.confirm_listing(
            confirmation.token,
            owner_telegram_id=OWNER_ID,
        )

        sale_calls = [call for call in transport.calls if call[0] == MRKT_SALE_PATH]
        self.assertEqual(len(sale_calls), 1)
        self.assertEqual(
            sale_calls[0][1],
            {"ids": [opaque_id], "price": 1_680_000_000},
        )
        self.assertEqual(result.status, MrktListingStatus.SUCCESS)

    async def test_known_api_error_is_surfaced_without_retry(self) -> None:
        transport = FakeTransport(
            inventories=[[gift()]],
            sale_response=MrktHttpResponse(
                status=409,
                body={"code": "GIFT_ALREADY_LISTED"},
            ),
        )
        service, _ = self.service([transport], dry_run=False)
        token = await self.confirmation(service)

        with self.assertRaises(MrktApiError) as raised:
            await service.confirm_listing(token, owner_telegram_id=OWNER_ID)

        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(
            [call[0] for call in transport.calls].count(MRKT_SALE_PATH),
            1,
        )

    async def test_network_timeout_never_retries_sale(self) -> None:
        sale_transport = FakeTransport(
            inventories=[[gift()]],
            sale_failure=MrktNetworkError("sanitized timeout"),
        )
        verify_transport = FakeTransport(
            inventories=[[gift(listed=True, price=1_713_600_000)]]
        )
        service, queue = self.service(
            [sale_transport, verify_transport],
            dry_run=False,
        )
        token = await self.confirmation(service)

        result = await service.confirm_listing(token, owner_telegram_id=OWNER_ID)

        total_sale_calls = sum(
            [call[0] for call in transport.calls].count(MRKT_SALE_PATH)
            for transport in queue.created
        )
        self.assertEqual(total_sale_calls, 1)
        self.assertEqual(result.status, MrktListingStatus.SUCCESS)
        self.assertTrue(result.sale_request_sent)

    async def test_secrets_are_redacted_from_logs_repr_and_errors(self) -> None:
        secret = SECRET_INIT_DATA + SECRET_TOKEN
        transport = FakeTransport(unexpected_failure=RuntimeError(secret))
        service, _ = self.service([transport], dry_run=True)

        with (
            self.assertLogs(
                "app.telegram_client.mrkt_service", level="WARNING"
            ) as captured,
            self.assertRaises(MrktServiceError) as raised,
        ):
            await service.get_inventory(
                ACCOUNT_ID,
                owner_telegram_id=OWNER_ID,
            )

        output = "\n".join(captured.output) + str(raised.exception)
        self.assertNotIn(secret, output)
        self.assertNotIn(SECRET_TOKEN, repr(MrktHttpResponse(200, {"token": secret})))

    async def test_dry_run_never_posts_sale(self) -> None:
        transport = FakeTransport(inventories=[[gift()]])
        service, _ = self.service([transport], dry_run=True)
        token = await self.confirmation(service)

        result = await service.confirm_listing(token, owner_telegram_id=OWNER_ID)

        self.assertEqual(result.status, MrktListingStatus.DRY_RUN)
        self.assertFalse(result.sale_request_sent)
        self.assertEqual(result.price_nanotons, 1_680_000_000)
        self.assertNotIn(MRKT_SALE_PATH, [call[0] for call in transport.calls])

    async def test_job_listing_uses_normal_final_revalidation_and_sale(self) -> None:
        transport = FakeTransport(inventories=[[gift()]])
        service, _ = self.service([transport], dry_run=False)

        result = await service.execute_job_listing(
            owner_telegram_id=OWNER_ID,
            account_id=ACCOUNT_ID,
            gift_id=GIFT_ID,
            display_name="Plush Pepe #42",
            price_ton="1.68",
        )

        sale_calls = [call for call in transport.calls if call[0] == MRKT_SALE_PATH]
        self.assertEqual(result.status, MrktListingStatus.SUCCESS)
        self.assertEqual(len(sale_calls), 1)
        self.assertEqual(
            sale_calls[0][1],
            {"ids": [GIFT_ID], "price": 1_680_000_000},
        )
