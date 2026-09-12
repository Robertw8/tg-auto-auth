from __future__ import annotations

import asyncio
import time
import unittest
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from app.telegram_client import (
    MiniAppLaunchResult,
    MiniAppService,
    PortalsAcceptStatus,
    PortalsApiError,
    PortalsCancelStatus,
    PortalsMutationNetworkError,
    PortalsOfferChangedError,
    PortalsOfferNotFoundError,
    PortalsOfferUnavailableError,
    PortalsService,
    PortalsServiceError,
)
from app.telegram_client.portals_service import (
    PORTALS_CREATE_CLOCK_SKEW_SECONDS,
    PORTALS_CREATE_OFFER_PATH,
    PORTALS_PLACED_OFFERS_PATH,
    PortalsHttpResponse,
)

INIT_DATA = "auth_date=1&hash=secret-hash&user=%7B%22id%22%3A1%7D"
OFFER_ID = "opaque-offer"
NFT_ID = 77
AMOUNT = "1.25"


def received_body(
    *,
    offer_id: str | int = OFFER_ID,
    nft_id: str | int = NFT_ID,
    amount: str | float = AMOUNT,
) -> dict[str, Any]:
    return {
        "top_offers": [
            {
                "offer": {
                    "id": offer_id,
                    "amount": amount,
                    "status": "active",
                },
                "nft": {"id": nft_id, "name": "Safe NFT"},
            }
        ],
        "total_count": 1,
    }


def detail_body(
    *, offer_id: str | int = OFFER_ID, amount: str | float = AMOUNT
) -> dict[str, Any]:
    return {"offers": [{"id": offer_id, "amount": amount, "status": "active"}]}


def placed_body(
    *,
    offer_id: str | int = OFFER_ID,
    nft_id: str | int = NFT_ID,
    amount: str | float = AMOUNT,
    status: str = "pending",
    sender_id: str | int | None = 1,
    created_at: str | None = None,
) -> dict[str, Any]:
    if created_at is None:
        created_at = datetime.now(timezone.utc).isoformat()
    offer: dict[str, Any] = {
        "id": offer_id,
        "nft_id": nft_id,
        "amount": amount,
        "status": status,
        "created_at": created_at,
    }
    if sender_id is not None:
        offer["sender_id"] = sender_id
    return {"offers": [offer], "total_count": 1, "total_amount": str(amount)}


def nft_offer_body(
    *,
    offer_id: str | int = OFFER_ID,
    nft_id: str | int = NFT_ID,
    amount: str | float = AMOUNT,
    status: str = "pending",
) -> dict[str, Any]:
    return {
        "nft": {"id": nft_id},
        "offers": [
            {
                "id": offer_id,
                "nft_id": nft_id,
                "amount": amount,
                "status": status,
            }
        ],
    }


class FakeMiniAppService:
    async def open_miniapp(
        self,
        account_id: int,
        bot_username: str,
        *,
        owner_telegram_id: int,
    ) -> MiniAppLaunchResult:
        del owner_telegram_id
        return MiniAppLaunchResult(
            account_id=account_id,
            bot_username=bot_username,
            resolved_bot_id=123,
            webview_obtained=True,
            init_data_obtained=True,
            init_data_fingerprint="abcdef12",
            init_data_fields=frozenset({"auth_date", "hash", "user"}),
            webview_url=("https://portal-market.com/#tgWebAppData=redacted"),
            init_data=INIT_DATA,
            query_id=None,
        )


class CountingMiniAppService(FakeMiniAppService):
    def __init__(self) -> None:
        self.calls = 0

    async def open_miniapp(
        self,
        account_id: int,
        bot_username: str,
        *,
        owner_telegram_id: int,
    ) -> MiniAppLaunchResult:
        self.calls += 1
        return await super().open_miniapp(
            account_id,
            bot_username,
            owner_telegram_id=owner_telegram_id,
        )


class FakeTransport:
    def __init__(self) -> None:
        self.responses: list[PortalsHttpResponse | Exception] = []
        self.responses_by_path: dict[
            str, list[PortalsHttpResponse | Exception]
        ] = {}
        self.delays_by_path: dict[str, float] = {}
        self.get_calls: list[tuple[str, dict[str, str | int]]] = []
        self.post_calls: list[tuple[str, dict[str, Any] | None]] = []
        self.closed = False

    def get_json(
        self,
        path: str,
        *,
        query: Mapping[str, str | int] | None,
        authorization: str,
    ) -> PortalsHttpResponse:
        self._assert_authorization(authorization)
        self.get_calls.append((path, dict(query or {})))
        if delay := self.delays_by_path.get(path):
            time.sleep(delay)
        routed = self.responses_by_path.get(path)
        if routed:
            value = routed.pop(0)
            if isinstance(value, Exception):
                raise value
            return value
        return self._next()

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any] | None,
        *,
        authorization: str,
    ) -> PortalsHttpResponse:
        self._assert_authorization(authorization)
        self.post_calls.append(
            (path, deepcopy(dict(payload)) if payload is not None else None)
        )
        return self._next()

    def _next(self) -> PortalsHttpResponse:
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    @staticmethod
    def _assert_authorization(value: str) -> None:
        if value != f"tma {INIT_DATA}":
            raise AssertionError("unexpected authorization")

    def close(self) -> None:
        self.closed = True


class PortalsServiceTests(unittest.IsolatedAsyncioTestCase):
    def service(
        self,
        transport: FakeTransport,
        *,
        dry_run: bool = False,
        reconciliation_delays: tuple[float, ...] = (0.0,),
        cancel_verification_delays: tuple[float, ...] = (0.0, 0.0),
    ) -> PortalsService:
        return PortalsService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=dry_run,
            transport_factory=lambda: transport,
            reconciliation_delays=reconciliation_delays,
            cancel_verification_delays=cancel_verification_delays,
        )

    @staticmethod
    def transfer_service(
        source_transport: FakeTransport,
        target_transport: FakeTransport,
    ) -> PortalsService:
        transports = iter((source_transport, target_transport))
        return PortalsService(
            cast(MiniAppService, FakeMiniAppService()),
            dry_run=False,
            transport_factory=lambda: next(transports),
            reconciliation_delays=(0.0,),
        )

    @staticmethod
    def base_responses() -> list[PortalsHttpResponse | Exception]:
        return [
            PortalsHttpResponse(200, {"user_id": 1, "token": "secret"}),
            PortalsHttpResponse(200, received_body()),
            PortalsHttpResponse(200, detail_body()),
        ]

    async def test_dry_run_builds_but_never_posts(self) -> None:
        transport = FakeTransport()
        transport.responses = self.base_responses()
        service = self.service(transport, dry_run=True)

        result = await service.accept_offer(
            owner_telegram_id=1,
            account_id=2,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
            display_name="Safe NFT",
        )

        self.assertEqual(result.status, PortalsAcceptStatus.DRY_RUN)
        self.assertFalse(result.accept_request_sent)
        self.assertEqual(transport.post_calls, [])
        self.assertTrue(transport.closed)

    async def test_successful_accept_preserves_exact_amount_and_id(self) -> None:
        transport = FakeTransport()
        transport.responses = self.base_responses() + [
            PortalsHttpResponse(200, {"success": True})
        ]
        service = self.service(transport)

        result = await service.accept_offer(
            owner_telegram_id=1,
            account_id=2,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
            display_name="Safe NFT",
        )

        self.assertEqual(result.status, PortalsAcceptStatus.SUCCESS)
        self.assertEqual(
            transport.post_calls,
            [(f"/offers/{OFFER_ID}/accept", {"amount": AMOUNT})],
        )

    async def test_transfer_preaccept_uses_placed_and_nft_when_received_empty(
        self,
    ) -> None:
        source = FakeTransport()
        source.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, placed_body()),
        ]
        target = FakeTransport()
        target.responses = [
            PortalsHttpResponse(200, {"user_id": 2}),
            PortalsHttpResponse(200, {"success": True}),
        ]
        target.responses_by_path = {
            "/offers/received": [
                PortalsHttpResponse(200, {"top_offers": [], "total_count": 0})
            ],
            "/nfts/owned": [
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                )
            ],
            f"/offers/nft/{NFT_ID}": [PortalsHttpResponse(200, nft_offer_body())],
        }

        result = await self.transfer_service(source, target).accept_transfer_offer(
            owner_telegram_id=1,
            source_account_id=10,
            target_account_id=20,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
            display_name="Safe NFT",
            expected_sender_id=1,
        )

        self.assertIs(result.status, PortalsAcceptStatus.SUCCESS)
        self.assertEqual(source.post_calls, [])
        self.assertEqual(
            target.post_calls,
            [(f"/offers/{OFFER_ID}/accept", {"amount": AMOUNT})],
        )

    async def test_transfer_preaccept_rejects_inconsistent_fresh_reads(self) -> None:
        cases = (
            ("wrong_offer", nft_offer_body(offer_id="other"), True, placed_body()),
            ("wrong_nft", nft_offer_body(nft_id=88), True, placed_body()),
            ("wrong_amount", nft_offer_body(amount="2.00"), True, placed_body()),
            (
                "inactive",
                nft_offer_body(status="cancelled"),
                True,
                placed_body(),
            ),
            ("not_owned", nft_offer_body(), False, placed_body()),
        )
        for name, nft_body, owns_nft, placed in cases:
            with self.subTest(case=name):
                source = FakeTransport()
                source.responses = [
                    PortalsHttpResponse(200, {"user_id": 1}),
                    PortalsHttpResponse(200, placed),
                ]
                owned_body = (
                    {"nfts": [{"id": NFT_ID}], "total_count": 1}
                    if owns_nft
                    else {"nfts": [], "total_count": 0}
                )
                target = FakeTransport()
                target.responses = [PortalsHttpResponse(200, {"user_id": 2})]
                target.responses_by_path = {
                    "/offers/received": [
                        PortalsHttpResponse(
                            200, {"top_offers": [], "total_count": 0}
                        )
                    ],
                    "/nfts/owned": [PortalsHttpResponse(200, owned_body)],
                    f"/offers/nft/{NFT_ID}": [
                        PortalsHttpResponse(200, nft_body)
                    ],
                }
                with self.assertRaises(
                    (
                        PortalsOfferNotFoundError,
                        PortalsOfferChangedError,
                        PortalsOfferUnavailableError,
                    )
                ):
                    await self.transfer_service(
                        source, target
                    ).accept_transfer_offer(
                        owner_telegram_id=1,
                        source_account_id=10,
                        target_account_id=20,
                        offer_id=OFFER_ID,
                        nft_id=NFT_ID,
                        amount=AMOUNT,
                        display_name="Safe NFT",
                        expected_sender_id=1,
                    )
                self.assertEqual(source.post_calls, [])
                self.assertEqual(target.post_calls, [])

    async def test_transfer_preaccept_rejects_multiple_exact_candidates(self) -> None:
        placed = placed_body()
        placed["offers"].append(
            {
                "id": "other-offer",
                "nft_id": NFT_ID,
                "amount": AMOUNT,
                "status": "pending",
            }
        )
        source = FakeTransport()
        source.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, placed),
        ]
        target = FakeTransport()
        target.responses = [PortalsHttpResponse(200, {"user_id": 2})]
        target.responses_by_path = {
            "/offers/received": [
                PortalsHttpResponse(200, {"top_offers": [], "total_count": 0})
            ],
            "/nfts/owned": [
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                )
            ],
            f"/offers/nft/{NFT_ID}": [PortalsHttpResponse(200, nft_offer_body())],
        }

        with self.assertRaises(PortalsOfferChangedError):
            await self.transfer_service(source, target).accept_transfer_offer(
                owner_telegram_id=1,
                source_account_id=10,
                target_account_id=20,
                offer_id=OFFER_ID,
                nft_id=NFT_ID,
                amount=AMOUNT,
                display_name="Safe NFT",
                expected_sender_id=1,
            )

        self.assertEqual(source.post_calls, [])
        self.assertEqual(target.post_calls, [])

    async def test_transfer_accept_timeout_is_never_retried(self) -> None:
        source = FakeTransport()
        source.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, placed_body()),
        ]
        target = FakeTransport()
        target.responses = [
            PortalsHttpResponse(200, {"user_id": 2}),
            PortalsMutationNetworkError("secret"),
            PortalsHttpResponse(200, {"top_offers": [], "total_count": 0}),
            PortalsHttpResponse(200, {"offers": []}),
        ]
        target.responses_by_path = {
            "/offers/received": [
                PortalsHttpResponse(200, {"top_offers": [], "total_count": 0})
            ],
            "/nfts/owned": [
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                )
            ],
            f"/offers/nft/{NFT_ID}": [PortalsHttpResponse(200, nft_offer_body())],
        }

        result = await self.transfer_service(source, target).accept_transfer_offer(
            owner_telegram_id=1,
            source_account_id=10,
            target_account_id=20,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
            display_name="Safe NFT",
            expected_sender_id=1,
        )

        self.assertIs(result.status, PortalsAcceptStatus.AMBIGUOUS)
        self.assertEqual(len(target.post_calls), 1)

    async def test_transfer_mandatory_reads_run_concurrently(self) -> None:
        source = FakeTransport()
        source.responses = [PortalsHttpResponse(200, {"user_id": 1})]
        source.responses_by_path = {
            "/offers/placed": [PortalsHttpResponse(200, placed_body())]
        }
        source.delays_by_path["/offers/placed"] = 0.05
        target = FakeTransport()
        target.responses = [
            PortalsHttpResponse(200, {"user_id": 2}),
            PortalsHttpResponse(200, {"success": True}),
        ]
        target.responses_by_path = {
            "/nfts/owned": [
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                )
            ],
            f"/offers/nft/{NFT_ID}": [PortalsHttpResponse(200, nft_offer_body())],
        }
        target.delays_by_path = {
            "/nfts/owned": 0.05,
            f"/offers/nft/{NFT_ID}": 0.05,
        }

        started_at = time.monotonic()
        result = await self.transfer_service(source, target).accept_transfer_offer(
            owner_telegram_id=1,
            source_account_id=10,
            target_account_id=20,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
            display_name="Safe NFT",
            expected_sender_id=1,
        )
        elapsed = time.monotonic() - started_at

        self.assertIs(result.status, PortalsAcceptStatus.SUCCESS)
        self.assertLess(elapsed, 0.12)
        self.assertNotIn("/offers/received", [path for path, _ in target.get_calls])

    async def test_transfer_scope_reuses_one_auth_per_account(self) -> None:
        miniapp = CountingMiniAppService()
        source = FakeTransport()
        source.responses = [PortalsHttpResponse(200, {"user_id": 1})]
        source.responses_by_path = {
            "/offers/placed": [
                PortalsHttpResponse(200, placed_body()),
                PortalsHttpResponse(200, placed_body()),
            ]
        }
        target = FakeTransport()
        target.responses = [PortalsHttpResponse(200, {"user_id": 2})]
        target.responses_by_path = {
            "/nfts/owned": [
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1
                }),
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1
                }),
            ]
        }
        transports = iter((source, target))
        service = PortalsService(
            cast(MiniAppService, miniapp), transport_factory=lambda: next(transports)
        )

        async with service.transfer_session_scope(1, 10, 20):
            await service.get_placed_offers(10, owner_telegram_id=1)
            await service.get_placed_offers(10, owner_telegram_id=1)
            await service.get_owned_nfts(20, owner_telegram_id=1)
            await service.get_owned_nfts(20, owner_telegram_id=1)

        self.assertEqual(miniapp.calls, 2)
        self.assertTrue(source.closed)
        self.assertTrue(target.closed)

    async def test_nested_concurrent_transfer_scopes_reuse_batch_auth(self) -> None:
        miniapp = CountingMiniAppService()
        source = FakeTransport()
        source.responses = [PortalsHttpResponse(200, {"user_id": 1})]
        source.responses_by_path = {
            "/offers/placed": [
                PortalsHttpResponse(200, placed_body()),
                PortalsHttpResponse(200, placed_body()),
            ]
        }
        target = FakeTransport()
        target.responses = [PortalsHttpResponse(200, {"user_id": 2})]
        target.responses_by_path = {
            "/nfts/owned": [
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                ),
                PortalsHttpResponse(
                    200, {"nfts": [{"id": NFT_ID}], "total_count": 1}
                ),
            ]
        }
        transports = iter((source, target))
        service = PortalsService(
            cast(MiniAppService, miniapp), transport_factory=lambda: next(transports)
        )

        async def child() -> None:
            async with service.transfer_session_scope(1, 10, 20):
                await asyncio.gather(
                    service.get_placed_offers(10, owner_telegram_id=1),
                    service.get_owned_nfts(20, owner_telegram_id=1),
                )

        async with service.transfer_session_scope(1, 10, 20):
            await asyncio.gather(child(), child())

        self.assertEqual(miniapp.calls, 2)
        self.assertTrue(source.closed)
        self.assertTrue(target.closed)

    async def test_offer_disappears_before_mutation(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, {"top_offers": [], "total_count": 0}),
        ]
        service = self.service(transport)

        with self.assertRaises(PortalsOfferNotFoundError):
            await service.accept_offer(
                owner_telegram_id=1,
                account_id=2,
                offer_id=OFFER_ID,
                nft_id=NFT_ID,
                amount=AMOUNT,
                display_name="NFT",
            )
        self.assertEqual(transport.post_calls, [])

    async def test_amount_or_nft_change_aborts_before_post(self) -> None:
        for changed_body in (
            received_body(amount="2.00"),
            received_body(nft_id=88),
        ):
            with self.subTest(body=changed_body):
                transport = FakeTransport()
                transport.responses = [
                    PortalsHttpResponse(200, {"user_id": 1}),
                    PortalsHttpResponse(200, changed_body),
                ]
                service = self.service(transport)
                with self.assertRaises(PortalsOfferChangedError):
                    await service.accept_offer(
                        owner_telegram_id=1,
                        account_id=2,
                        offer_id=OFFER_ID,
                        nft_id=NFT_ID,
                        amount=AMOUNT,
                        display_name="NFT",
                    )
                self.assertEqual(transport.post_calls, [])

    async def test_server_rejection_is_not_retried(self) -> None:
        transport = FakeTransport()
        transport.responses = self.base_responses() + [
            PortalsHttpResponse(400, {"code": "INVALID_OFFER"})
        ]
        service = self.service(transport)

        with self.assertRaises(PortalsApiError) as context:
            await service.accept_offer(
                owner_telegram_id=1,
                account_id=2,
                offer_id=OFFER_ID,
                nft_id=NFT_ID,
                amount=AMOUNT,
                display_name="NFT",
            )
        self.assertEqual(context.exception.status, 400)
        self.assertEqual(len(transport.post_calls), 1)

    async def test_network_timeout_posts_once_and_remains_ambiguous(self) -> None:
        transport = FakeTransport()
        transport.responses = self.base_responses() + [
            PortalsMutationNetworkError("token=secret"),
            PortalsHttpResponse(200, {"top_offers": [], "total_count": 0}),
            PortalsHttpResponse(200, {"offers": []}),
        ]
        service = self.service(transport)

        result = await service.accept_offer(
            owner_telegram_id=1,
            account_id=2,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
            display_name="NFT",
        )

        self.assertEqual(result.status, PortalsAcceptStatus.AMBIGUOUS)
        self.assertTrue(result.accept_request_sent)
        self.assertEqual(len(transport.post_calls), 1)
        self.assertIn("received_offers_refreshed", result.verification_fields)
        self.assertNotIn("secret", repr(result))

    async def test_received_offer_parsing_preserves_types(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(
                200,
                received_body(offer_id=7, nft_id="77", amount=3),
            ),
        ]
        service = self.service(transport)

        offers = await service.get_received_offers(2, owner_telegram_id=1)

        self.assertEqual(len(offers), 1)
        self.assertIs(type(offers[0].offer_id), int)
        self.assertIs(type(offers[0].nft_id), str)
        self.assertIs(type(offers[0].amount), int)
        self.assertNotIn(INIT_DATA, repr(offers[0]))

    async def test_create_offer_uses_exact_frontend_contract_once(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 10}),
            PortalsHttpResponse(
                201,
                {"offer": {"id": "created-7", "sender_id": 10}},
            ),
        ]
        service = self.service(transport)

        result = await service.create_offer(
            owner_telegram_id=1,
            account_id=2,
            nft_id="opaque-nft",
            amount="10.00",
            dry_run=False,
        )

        self.assertEqual(result.offer_id, "created-7")
        self.assertEqual(result.amount, "10")
        self.assertEqual(
            transport.post_calls,
            [
                (
                    PORTALS_CREATE_OFFER_PATH,
                    {
                        "offer": {
                            "nft_id": "opaque-nft",
                            "offer_price": "10",
                        }
                    },
                )
            ],
        )

    async def test_create_offer_dry_run_sends_no_mutation(self) -> None:
        transport = FakeTransport()
        transport.responses = [PortalsHttpResponse(200, {"user_id": 10})]
        service = self.service(transport)

        result = await service.create_offer(
            owner_telegram_id=1,
            account_id=2,
            nft_id=77,
            amount="1.25",
            dry_run=True,
        )

        self.assertFalse(result.request_sent)
        self.assertEqual(transport.post_calls, [])

    async def test_create_204_resolves_immediately_from_placed_offer(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, placed_body()),
        ]

        result = await self.service(transport).create_offer(
            owner_telegram_id=1,
            account_id=2,
            nft_id=NFT_ID,
            amount=AMOUNT,
            dry_run=False,
        )

        self.assertEqual(result.offer_id, OFFER_ID)
        self.assertEqual(result.response_status, 204)
        self.assertEqual(len(transport.post_calls), 1)
        self.assertEqual(
            [path for path, _ in transport.get_calls],
            ["/users/auth", PORTALS_PLACED_OFFERS_PATH],
        )

    async def test_job_3_clock_skew_shape_resolves_from_n1_placed(self) -> None:
        created_at = (datetime.now(timezone.utc) - timedelta(seconds=8)).isoformat()
        source = FakeTransport()
        source.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            *[
                PortalsHttpResponse(
                    200,
                    placed_body(sender_id=None, created_at=created_at),
                )
                for _ in range(7)
            ],
        ]
        target = FakeTransport()
        target.responses = [PortalsHttpResponse(200, {"user_id": 2})]
        target.responses_by_path = {
            f"/offers/nft/{NFT_ID}": [
                PortalsHttpResponse(403, {"detail": "forbidden"}) for _ in range(7)
            ],
            "/offers/received": [
                PortalsHttpResponse(200, {"top_offers": [], "total_count": 0})
                for _ in range(7)
            ],
        }
        transports = iter((source, target))
        service = PortalsService(
            cast(MiniAppService, FakeMiniAppService()),
            transport_factory=lambda: next(transports),
            reconciliation_delays=(0.0,) * 7,
        )

        result = await service.create_offer(
            owner_telegram_id=1,
            account_id=1,
            nft_id=NFT_ID,
            amount=AMOUNT,
            dry_run=False,
            reconciliation_account_id=2,
        )

        self.assertEqual(result.offer_id, OFFER_ID)
        self.assertEqual(len(source.post_calls), 1)
        self.assertEqual(
            [path for path, _ in source.get_calls],
            ["/users/auth", *[PORTALS_PLACED_OFFERS_PATH] * 7],
        )
        target_paths = [path for path, _ in target.get_calls]
        self.assertEqual(target_paths[0], "/users/auth")
        self.assertEqual(target_paths.count(f"/offers/nft/{NFT_ID}"), 7)
        self.assertEqual(target_paths.count("/offers/received"), 0)
        self.assertEqual(target.post_calls, [])

    async def test_offer_older_than_clock_skew_window_is_rejected(self) -> None:
        from app.telegram_client import PortalsCreateOfferUnconfirmedError

        created_at = (
            datetime.now(timezone.utc)
            - timedelta(seconds=PORTALS_CREATE_CLOCK_SKEW_SECONDS + 5)
        ).isoformat()
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, placed_body(created_at=created_at)),
        ]

        with self.assertRaises(PortalsCreateOfferUnconfirmedError):
            await self.service(transport).create_offer(
                owner_telegram_id=1,
                account_id=1,
                nft_id=NFT_ID,
                amount=AMOUNT,
                dry_run=False,
            )

        self.assertEqual(len(transport.post_calls), 1)

    async def test_create_204_resolves_after_delayed_read(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, {"offers": [], "total_count": 0}),
            PortalsHttpResponse(200, placed_body()),
        ]

        with patch(
            "app.telegram_client.portals_service.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            result = await self.service(
                transport, reconciliation_delays=(0.0, 0.4)
            ).create_offer(
                owner_telegram_id=1,
                account_id=2,
                nft_id=NFT_ID,
                amount=AMOUNT,
                dry_run=False,
            )

        self.assertEqual(result.offer_id, OFFER_ID)
        sleep.assert_awaited_once_with(0.4)
        self.assertEqual(len(transport.post_calls), 1)

    async def test_create_204_without_offer_stays_unconfirmed(self) -> None:
        from app.telegram_client import PortalsCreateOfferUnconfirmedError

        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, {"offers": [], "total_count": 0}),
        ]
        with self.assertRaises(PortalsCreateOfferUnconfirmedError):
            await self.service(transport).create_offer(
                owner_telegram_id=1,
                account_id=2,
                nft_id=NFT_ID,
                amount=AMOUNT,
                dry_run=False,
            )
        self.assertEqual(len(transport.post_calls), 1)

    async def test_create_204_multiple_matches_stays_unconfirmed(self) -> None:
        from app.telegram_client import PortalsCreateOfferUnconfirmedError

        body = placed_body()
        body["offers"].append(
            {
                "id": "other-offer",
                "nft_id": NFT_ID,
                "amount": AMOUNT,
                "status": "active",
                "sender_id": 1,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, body),
        ]
        with self.assertRaises(PortalsCreateOfferUnconfirmedError):
            await self.service(transport).create_offer(
                owner_telegram_id=1,
                account_id=2,
                nft_id=NFT_ID,
                amount=AMOUNT,
                dry_run=False,
            )
        self.assertEqual(len(transport.post_calls), 1)

    async def test_create_204_rejects_wrong_or_closed_offer(self) -> None:
        from app.telegram_client import PortalsCreateOfferUnconfirmedError

        old_offer = placed_body()
        old_offer["offers"][0]["created_at"] = "2020-01-01T00:00:00Z"
        cases = (
            placed_body(nft_id=str(NFT_ID)),
            placed_body(amount="9.99"),
            placed_body(status="cancelled"),
            old_offer,
        )
        for body in cases:
            with self.subTest(body=body):
                transport = FakeTransport()
                transport.responses = [
                    PortalsHttpResponse(200, {"user_id": 1}),
                    PortalsHttpResponse(204, None),
                    PortalsHttpResponse(200, body),
                ]
                with self.assertRaises(PortalsCreateOfferUnconfirmedError):
                    await self.service(transport).create_offer(
                        owner_telegram_id=1,
                        account_id=2,
                        nft_id=NFT_ID,
                        amount=AMOUNT,
                        dry_run=False,
                    )
                self.assertEqual(len(transport.post_calls), 1)

    async def test_resolved_204_offer_is_accepted_exactly_once(self) -> None:
        source = FakeTransport()
        source.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, placed_body()),
        ]
        reconciliation_target = FakeTransport()
        reconciliation_target.responses = [PortalsHttpResponse(200, {"user_id": 2})]
        reconciliation_target.responses_by_path = {
            f"/offers/nft/{NFT_ID}": [PortalsHttpResponse(200, nft_offer_body())],
            "/offers/received": [PortalsHttpResponse(200, received_body())],
        }
        accept_target = FakeTransport()
        accept_target.responses = [
            PortalsHttpResponse(200, {"user_id": 2}),
            PortalsHttpResponse(200, received_body()),
            PortalsHttpResponse(200, detail_body()),
            PortalsHttpResponse(200, {"success": True}),
        ]
        transports = iter((source, reconciliation_target, accept_target))
        service = PortalsService(
            cast(MiniAppService, FakeMiniAppService()),
            transport_factory=lambda: next(transports),
            reconciliation_delays=(0.0,) * 7,
        )

        created = await service.create_offer(
            owner_telegram_id=1,
            account_id=1,
            nft_id=NFT_ID,
            amount=AMOUNT,
            dry_run=False,
            reconciliation_account_id=2,
        )
        accepted = await service.accept_offer(
            owner_telegram_id=1,
            account_id=2,
            offer_id=created.offer_id,
            nft_id=created.nft_id,
            amount=created.amount,
            display_name="NFT",
            expected_sender_id=created.sender_id,
            dry_run=False,
        )

        self.assertEqual(accepted.status, PortalsAcceptStatus.SUCCESS)
        self.assertEqual(len(source.post_calls), 1)
        self.assertEqual(
            [path for path, _ in source.get_calls].count(PORTALS_PLACED_OFFERS_PATH),
            1,
        )
        self.assertEqual(reconciliation_target.post_calls, [])
        self.assertEqual(
            {path for path, _ in reconciliation_target.get_calls},
            {"/users/auth", f"/offers/nft/{NFT_ID}"},
        )
        self.assertEqual(len(accept_target.post_calls), 1)

    async def test_create_204_records_terminal_transition_and_stops_accept(
        self,
    ) -> None:
        from app.telegram_client import PortalsCreateOfferUnconfirmedError

        for status, reason in (
            ("cancelled", "CREATE_OFFER_CANCELLED"),
            ("expired", "CREATE_OFFER_EXPIRED"),
            ("rejected", "CREATE_OFFER_REJECTED"),
        ):
            with self.subTest(status=status):
                transport = FakeTransport()
                transport.responses = [
                    PortalsHttpResponse(200, {"user_id": 1}),
                    PortalsHttpResponse(204, None),
                    PortalsHttpResponse(200, placed_body(status="pending")),
                    PortalsHttpResponse(200, placed_body(status=status)),
                ]
                with self.assertRaises(PortalsCreateOfferUnconfirmedError) as context:
                    await self.service(
                        transport, reconciliation_delays=(0.0, 0.0)
                    ).create_offer(
                        owner_telegram_id=1,
                        account_id=2,
                        nft_id=NFT_ID,
                        amount=AMOUNT,
                        dry_run=False,
                    )
                self.assertEqual(context.exception.reason, reason)
                self.assertEqual(len(transport.post_calls), 1)

    async def test_create_204_records_offer_disappearance(self) -> None:
        from app.telegram_client import PortalsCreateOfferUnconfirmedError

        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(204, None),
            PortalsHttpResponse(200, placed_body()),
            PortalsHttpResponse(200, {"offers": [], "total_count": 0}),
        ]
        with self.assertRaises(PortalsCreateOfferUnconfirmedError) as context:
            await self.service(
                transport, reconciliation_delays=(0.0, 0.0)
            ).create_offer(
                owner_telegram_id=1,
                account_id=2,
                nft_id=NFT_ID,
                amount=AMOUNT,
                dry_run=False,
            )
        self.assertEqual(context.exception.reason, "CREATE_OFFER_DISAPPEARED")
        self.assertEqual(len(transport.post_calls), 1)

    async def test_create_offer_network_failure_is_not_retried(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 10}),
            PortalsMutationNetworkError("secret-token"),
        ]
        service = self.service(transport)

        with self.assertRaises(PortalsMutationNetworkError):
            await service.create_offer(
                owner_telegram_id=1,
                account_id=2,
                nft_id=77,
                amount="1.25",
                dry_run=False,
            )

        self.assertEqual(len(transport.post_calls), 1)

    async def test_accept_rechecks_sender_before_post(self) -> None:
        transport = FakeTransport()
        body = received_body()
        body["top_offers"][0]["offer"]["sender_id"] = "someone-else"
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, body),
        ]
        with self.assertRaises(PortalsOfferChangedError):
            await self.service(transport).accept_offer(
                owner_telegram_id=1,
                account_id=2,
                offer_id=OFFER_ID,
                nft_id=NFT_ID,
                amount=AMOUNT,
                display_name="NFT",
                expected_sender_id="expected-sender",
                dry_run=False,
            )
        self.assertEqual(transport.post_calls, [])

    async def test_create_unknown_response_and_5xx_are_ambiguous(self) -> None:
        from app.telegram_client import PortalsCreateOfferUnconfirmedError

        for response in (
            PortalsHttpResponse(200, {}),
            PortalsHttpResponse(503, {"error": "secret"}),
        ):
            with self.subTest(status=response.status):
                transport = FakeTransport()
                transport.responses = [
                    PortalsHttpResponse(200, {"user_id": 1}),
                    response,
                ]
                with self.assertRaises(PortalsCreateOfferUnconfirmedError):
                    await self.service(transport).create_offer(
                        owner_telegram_id=1,
                        account_id=2,
                        nft_id=NFT_ID,
                        amount=AMOUNT,
                        dry_run=False,
                    )
                self.assertEqual(len(transport.post_calls), 1)

    async def test_invalid_owned_inventory_is_not_silently_auto_selected(self) -> None:
        for body in ({"nfts": [{"id": "a"}, {}]}, {"nfts": [{"id": "a"}, {"id": "a"}]}):
            transport = FakeTransport()
            transport.responses = [
                PortalsHttpResponse(200, {"user_id": 1}),
                PortalsHttpResponse(200, body),
            ]
            with self.assertRaises(PortalsServiceError):
                await self.service(transport).get_owned_nfts(2, owner_telegram_id=1)
            self.assertEqual(transport.post_calls, [])

    def test_offer_amount_validation_matches_ui_contract(self) -> None:
        self.assertEqual(PortalsService.normalize_offer_amount("1.00"), "1")
        self.assertEqual(PortalsService.normalize_offer_amount("10"), "10")
        for value in ("0", "0.49", "-1", "abc"):
            with self.subTest(value=value), self.assertRaises(PortalsServiceError):
                PortalsService.normalize_offer_amount(value)

    async def test_get_placed_offers_empty(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, {"offers": [], "total_count": 0}),
        ]

        offers = await self.service(transport).get_placed_offers(2, owner_telegram_id=1)

        self.assertEqual(offers, [])
        self.assertEqual(transport.post_calls, [])

    async def test_get_placed_offers_one_active_and_filters_inactive(self) -> None:
        body = placed_body()
        body["offers"].append(
            {
                "id": "cancelled-offer",
                "nft_id": 91,
                "amount": "0.75",
                "status": "cancelled",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, body),
        ]

        offers = await self.service(transport).get_placed_offers(2, owner_telegram_id=1)

        self.assertEqual(len(offers), 1)
        self.assertEqual(offers[0].offer_id, OFFER_ID)

    async def test_get_placed_offers_multiple(self) -> None:
        body = placed_body()
        body["offers"].append(
            {
                "id": "second-offer",
                "nft_id": 91,
                "amount": "0.75",
                "status": "active",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, body),
        ]

        offers = await self.service(transport).get_placed_offers(2, owner_telegram_id=1)

        self.assertEqual(
            [offer.offer_id for offer in offers], [OFFER_ID, "second-offer"]
        )

    async def test_cancel_uses_exact_selected_offer_once(self) -> None:
        body = placed_body()
        body["offers"].append(
            {
                "id": "selected-offer",
                "nft_id": 91,
                "amount": "0.53",
                "status": "pending",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, body),
            PortalsHttpResponse(204, None),
        ]

        result = await self.service(transport).cancel_offer(
            owner_telegram_id=1,
            account_id=2,
            offer_id="selected-offer",
            nft_id=91,
            amount="0.53",
        )

        self.assertIs(result.status, PortalsCancelStatus.SUCCESS)
        self.assertEqual(
            transport.post_calls,
            [("/offers/selected-offer/cancel", None)],
        )

    async def test_cancel_offer_disappears_before_confirmation(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, {"offers": [], "total_count": 0}),
        ]

        result = await self.service(transport).cancel_offer(
            owner_telegram_id=1,
            account_id=2,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
        )

        self.assertIs(result.status, PortalsCancelStatus.ALREADY_INACTIVE)
        self.assertEqual(transport.post_calls, [])

    async def test_cancel_rejects_changed_offer_without_post(self) -> None:
        for body in (placed_body(nft_id=91), placed_body(amount="0.53")):
            with self.subTest(body=body):
                transport = FakeTransport()
                transport.responses = [
                    PortalsHttpResponse(200, {"user_id": 1}),
                    PortalsHttpResponse(200, body),
                ]
                with self.assertRaises(PortalsOfferChangedError):
                    await self.service(transport).cancel_offer(
                        owner_telegram_id=1,
                        account_id=2,
                        offer_id=OFFER_ID,
                        nft_id=NFT_ID,
                        amount=AMOUNT,
                    )
                self.assertEqual(transport.post_calls, [])

    async def test_cancel_server_rejection_is_not_retried(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, placed_body()),
            PortalsHttpResponse(403, {"detail": "forbidden"}),
        ]

        with self.assertRaises(PortalsApiError):
            await self.service(transport).cancel_offer(
                owner_telegram_id=1,
                account_id=2,
                offer_id=OFFER_ID,
                nft_id=NFT_ID,
                amount=AMOUNT,
            )

        self.assertEqual(len(transport.post_calls), 1)

    async def test_ambiguous_cancel_does_not_retry_when_offer_remains(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, placed_body()),
            PortalsMutationNetworkError("timeout"),
            PortalsHttpResponse(200, placed_body()),
            PortalsHttpResponse(200, placed_body()),
        ]

        result = await self.service(transport).cancel_offer(
            owner_telegram_id=1,
            account_id=2,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
        )

        self.assertIs(result.status, PortalsCancelStatus.AMBIGUOUS)
        self.assertEqual(len(transport.post_calls), 1)

    async def test_ambiguous_cancel_is_success_after_two_absent_reads(self) -> None:
        transport = FakeTransport()
        empty = {"offers": [], "total_count": 0}
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1}),
            PortalsHttpResponse(200, placed_body()),
            PortalsMutationNetworkError("timeout"),
            PortalsHttpResponse(200, deepcopy(empty)),
            PortalsHttpResponse(200, deepcopy(empty)),
        ]

        result = await self.service(transport).cancel_offer(
            owner_telegram_id=1,
            account_id=2,
            offer_id=OFFER_ID,
            nft_id=NFT_ID,
            amount=AMOUNT,
        )

        self.assertIs(result.status, PortalsCancelStatus.SUCCESS)
        self.assertEqual(len(transport.post_calls), 1)

    async def test_cancel_logs_and_result_redact_secrets(self) -> None:
        transport = FakeTransport()
        transport.responses = [
            PortalsHttpResponse(200, {"user_id": 1, "token": "private-token"}),
            PortalsHttpResponse(200, placed_body()),
            PortalsHttpResponse(200, {"success": True, "token": "private-token"}),
        ]
        with self.assertLogs(
            "app.telegram_client.portals_service", level="INFO"
        ) as captured:
            result = await self.service(transport).cancel_offer(
                owner_telegram_id=1,
                account_id=2,
                offer_id=OFFER_ID,
                nft_id=NFT_ID,
                amount=AMOUNT,
            )

        output = "\n".join(captured.output)
        self.assertNotIn(INIT_DATA, output)
        self.assertNotIn("private-token", output)
        self.assertNotIn(OFFER_ID, repr(result))
        self.assertNotIn(str(NFT_ID), repr(result))
