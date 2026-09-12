from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from app.telegram_client.miniapp_service import MiniAppService
from app.telegram_client.mrkt_service import (
    MRKT_BUY_PATH,
    MRKT_SALE_PATH,
    MrktApiError,
    MrktFastTransferStatus,
    MrktHttpResponse,
    MrktService,
    _MrktAuthSession,
    _MrktPresenceSnapshot,
)
from tests.test_mrkt_service import (
    GIFT_ID,
    OWNER_ID,
    DelayedSaleTransport,
    FakeMiniAppService,
    FakeTransport,
    TransportQueue,
    gift,
)


class FakeClock:
    def __init__(self, milliseconds: int) -> None:
        self.nanoseconds = milliseconds * 1_000_000

    def __call__(self) -> int:
        return self.nanoseconds

    async def sleep(self, seconds: float) -> None:
        self.nanoseconds += round(seconds * 1_000_000_000)


def presence(
    *,
    seller_unlisted: bool = False,
    seller_listed: bool = False,
    buyer_unlisted: bool = False,
    buyer_listed: bool = False,
) -> _MrktPresenceSnapshot:
    return _MrktPresenceSnapshot(
        seller_unlisted_present=seller_unlisted,
        seller_listed_present=seller_listed,
        buyer_unlisted_present=buyer_unlisted,
        buyer_listed_present=buyer_listed,
    )


class MrktActivationObserverTests(unittest.IsolatedAsyncioTestCase):
    def service(self) -> MrktService:
        return MrktService(cast(MiniAppService, FakeMiniAppService()))

    async def observe(
        self,
        values: list[_MrktPresenceSnapshot | Exception],
        *,
        start_ms: int = 60,
        schedule_ms: tuple[int, ...] = (0, 25, 50),
    ) -> tuple[dict[str, Any], int, int]:
        clock = FakeClock(start_ms)
        calls = 0

        async def reader() -> _MrktPresenceSnapshot:
            nonlocal calls
            value = values[calls]
            calls += 1
            if isinstance(value, Exception):
                raise value
            return value

        result, started = await self.service()._observe_listing_activation(
            buyer_session=cast(_MrktAuthSession, None),
            seller_session=cast(_MrktAuthSession, None),
            buyer_account_id=1,
            seller_account_id=2,
            gift_id=GIFT_ID,
            sale_started_ns=0,
            observation_started_ns=start_ms * 1_000_000,
            schedule_ms=schedule_ms,
            clock_ns=clock,
            sleeper=clock.sleep,
            sample_reader=reader,
        )
        return result, started, calls

    async def test_transition_records_polling_bounds(self) -> None:
        result, _, _ = await self.observe(
            [
                presence(seller_unlisted=True),
                presence(seller_unlisted=True),
                presence(seller_listed=True),
            ]
        )

        self.assertEqual(result["last_observed_seller_unlisted_ms"], 85.0)
        self.assertEqual(result["first_observed_seller_listed_ms"], 110.0)
        self.assertEqual(result["listing_transition_lower_bound_ms"], 85.0)
        self.assertEqual(result["listing_transition_upper_bound_ms"], 110.0)
        self.assertEqual(result["observation_stopped_reason"], "LISTED_OBSERVED")

    async def test_first_sample_listed_is_upper_bound_only(self) -> None:
        result, _, calls = await self.observe([presence(seller_listed=True)])

        self.assertEqual(calls, 1)
        self.assertTrue(result["first_snapshot_is_upper_bound"])
        self.assertIsNone(result["listing_transition_lower_bound_ms"])
        self.assertEqual(result["listing_transition_upper_bound_ms"], 60.0)

    async def test_unlisted_for_entire_window_times_out(self) -> None:
        result, _, calls = await self.observe(
            [presence(seller_unlisted=True)] * 3
        )

        self.assertEqual(calls, 3)
        self.assertEqual(result["observation_stopped_reason"], "TIMEOUT")
        self.assertIsNone(result["first_observed_seller_listed_ms"])

    async def test_buyer_ownership_stops_observer(self) -> None:
        result, _, calls = await self.observe([presence(buyer_unlisted=True)])

        self.assertEqual(calls, 1)
        self.assertEqual(result["observation_stopped_reason"], "BUYER_OWNS")

    async def test_complete_absence_stops_observer(self) -> None:
        result, _, calls = await self.observe([presence()])

        self.assertEqual(calls, 1)
        self.assertEqual(result["observation_stopped_reason"], "GIFT_ABSENT")

    async def test_read_error_is_safe_and_stops(self) -> None:
        result, _, calls = await self.observe([RuntimeError("secret")])

        self.assertEqual(calls, 1)
        self.assertEqual(result["observation_stopped_reason"], "READ_ERROR")
        self.assertFalse(result["samples"][0]["read_ok"])
        self.assertNotIn("secret", str(result))

    async def test_rate_limit_stops_without_further_polling(self) -> None:
        result, _, calls = await self.observe(
            [MrktApiError(429), presence(seller_unlisted=True)]
        )

        self.assertEqual(calls, 1)
        self.assertEqual(result["observation_stopped_reason"], "RATE_LIMITED")

    async def test_observer_starts_after_buy_without_delaying_buy_or_mutations(
        self,
    ) -> None:
        events: list[str] = []
        buyer = FakeTransport(
            inventories=[[], []],
            buy_response=MrktHttpResponse(404, {"code": "NOT_FOUND"}),
            events=events,
        )
        seller = DelayedSaleTransport(
            0.05,
            inventories=[[gift()], [], [gift(listed=True)]],
            events=events,
        )
        observer_buyer = _MrktAuthSession(FakeTransport(), "token", None)
        observer_seller = _MrktAuthSession(FakeTransport(), "token", None)
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService(events)),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )

        async def fake_observer(**kwargs: object) -> tuple[dict[str, Any], int]:
            events.append("observer")
            return service._empty_listing_observation(True), cast(
                int, kwargs["observation_started_ns"]
            )

        with (
            patch.object(
                service,
                "_prepare_listing_observer_sessions",
                new=AsyncMock(return_value=(observer_buyer, observer_seller)),
            ),
            patch.object(service, "_observe_listing_activation", new=fake_observer),
        ):
            result = await service.execute_fast_transfer(
                owner_telegram_id=OWNER_ID,
                buyer_account_id=1,
                seller_account_id=2,
                gift_id=GIFT_ID,
                price_nanotons=530_000_000,
                dry_run=False,
                speculative_buy=True,
                speculative_buy_delay_ms=5,
                observe_listing_activation=True,
            )

        self.assertEqual(events.count(MRKT_SALE_PATH), 1)
        self.assertEqual(events.count(MRKT_BUY_PATH), 1)
        self.assertGreater(events.index("observer"), events.index(MRKT_BUY_PATH))
        self.assertLess(result.timings["mrkt_sale_start_to_buy_start_ms"] or 999, 30)
        self.assertTrue(
            result.listing_observation[
                "observation_started_before_sale_response"
            ]
        )

    async def test_final_unlisted_state_is_not_listing_still_active(self) -> None:
        buyer = FakeTransport(
            inventories=[[], []],
            buy_response=MrktHttpResponse(404, {"code": "NOT_FOUND"}),
        )
        seller = FakeTransport(inventories=[[gift()], [gift()], []])
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
        )

        self.assertEqual(result.verification["outcome"], "listing_not_active")
        self.assertNotEqual(result.status, MrktFastTransferStatus.SPECULATIVE_BUY_TOO_EARLY)

    async def test_fast_confirmed_does_not_prepare_or_run_observer(self) -> None:
        buyer = FakeTransport(inventories=[[gift()], []])
        seller = FakeTransport(inventories=[[gift()], [], []])
        service = MrktService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=TransportQueue([buyer, seller]),
        )
        with patch.object(
            service, "_prepare_listing_observer_sessions", new=AsyncMock()
        ) as prepare:
            result = await service.execute_fast_transfer(
                owner_telegram_id=OWNER_ID,
                buyer_account_id=1,
                seller_account_id=2,
                gift_id=GIFT_ID,
                price_nanotons=530_000_000,
                dry_run=False,
                observe_listing_activation=True,
            )

        prepare.assert_not_awaited()
        self.assertFalse(result.listing_observation["enabled"])
        self.assertEqual(
            result.listing_observation["observation_stopped_reason"], "DISABLED"
        )

    def test_report_script_is_read_only(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "report_mrkt_activation_timing.py"
        ).read_text()
        for mutation in ("INSERT", "UPDATE", "DELETE", "/gifts/sale", "/gifts/buy"):
            self.assertNotIn(mutation, source)
