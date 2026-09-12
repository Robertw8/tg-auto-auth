from __future__ import annotations

import json
import socket
import ssl
import unittest
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, call, patch

from app.db import Account, Database
from app.telegram_client import (
    MiniAppLaunchResult,
    MiniAppService,
    TonnelAuthenticationError,
    TonnelDuplicateMutationError,
    TonnelGiftUnavailableError,
    TonnelHttpResponse,
    TonnelHttpTransport,
    TonnelMarketGiftIdentity,
    TonnelMutationNetworkError,
    TonnelNetworkError,
    TonnelPreflightFailure,
    TonnelService,
    TonnelTransferStatus,
)
from app.telegram_client.tonnel_service import (
    TONNEL_API_ORIGIN,
    TONNEL_FRONTEND_ORIGIN,
    TONNEL_FRONTEND_REFERER,
    TONNEL_REGIONAL_API_ORIGIN,
    TONNEL_REGIONAL_DATA_ORIGIN,
    _AiohttpTonnelTransport,
    _network_failure_kind,
    _TonnelSession,
)

CONTROL_USER = 100


def account(account_id: int, telegram_id: int, role: str) -> Account:
    return Account(
        id=account_id,
        owner_telegram_id=CONTROL_USER,
        telegram_account_id=telegram_id,
        session_key=f"session-{account_id}",
        username=f"user{account_id}",
        first_name="User",
        phone=None,
        created_at="2026-09-10 00:00:00",
        role=role,
    )


class FakeDatabase:
    def __init__(self) -> None:
        self.accounts = {
            1: account(1, 300, "OWNER"),
            2: account(2, 302, "TARGET"),
        }

    async def get_account(
        self, account_id: int, owner_telegram_id: int
    ) -> Account | None:
        item = self.accounts.get(account_id)
        return item if item and item.owner_telegram_id == owner_telegram_id else None


class FakeMiniApps:
    async def open_miniapp(
        self, account_id: int, bot_username: str, *, owner_telegram_id: int
    ) -> MiniAppLaunchResult:
        del bot_username, owner_telegram_id
        return MiniAppLaunchResult(
            account_id=account_id,
            bot_username="@Tonnel_Network_bot",
            resolved_bot_id=777,
            webview_obtained=True,
            init_data_obtained=True,
            init_data_fingerprint=f"launch-{account_id}",
            init_data_fields=frozenset({"auth_date", "hash", "user"}),
            webview_url="https://redacted.invalid",
            init_data=f"secret-init-{account_id}",
            query_id=None,
        )


def raw_gift(*, seller: int = 302, gift_id: int = 91) -> dict[str, Any]:
    return {
        "gift_id": gift_id,
        "gift_num": 17,
        "name": "Lol Pop",
        "model": "Blue (1%)",
        "symbol": "Star (1%)",
        "backdrop": "Navy (1%)",
        "seller": seller,
        "asset": "TON",
        "status": "owned",
    }


class FakeBuyOfferTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], bool]] = []
        self.events: list[str] = []
        self.closed = False
        self.buyer_balance = "100"
        self.source_owns = True
        self.destination_owns = False
        self.offer_exists = False
        self.offer_status = "pending"
        self.offer_id: str | int = 700
        self.offer_gift_id: str | int = 91
        self.source_gift_id = 91
        self.offer_price: str = "5"
        self.offer_buyer: int | None = 300
        self.offer_seller: int | None = 302
        self.extra_exact_offer = False
        self.preexisting_offer_ids: list[str | int] = []
        self.create_adds_offer = True
        self.create_response = TonnelHttpResponse(
            200, {"status": "success", "message": "created", "offer_id": 700}
        )
        self.accept_response = TonnelHttpResponse(
            200, {"status": "success", "message": "accepted"}
        )
        self.create_ambiguous = False
        self.accept_ambiguous = False
        self.complete_ambiguous_accept = False
        self.fail_reads_after_accept = False
        self.accept_attempted = False
        self.accept_application_success = False
        self.accept_success_without_transfer = False
        self.ownership_converges_after_probe: int | None = 1
        self.ownership_probe_count = 0
        self._ownership_page_call_count = 0
        self.reads_unavailable = False
        self.page_gifts_failure: TonnelHttpResponse | None = None
        self.get_offers_failure: TonnelHttpResponse | None = None
        self.get_my_offers_failure: TonnelHttpResponse | None = None
        self.auth_user_ids = {1: 300, 2: 302}
        self.complementary_identity_fields = False

    def offer(self, offer_id: str | int | None = None) -> dict[str, Any]:
        item: dict[str, Any] = {
            "offer_id": self.offer_id if offer_id is None else offer_id,
            "gift_id": self.offer_gift_id,
            "price": self.offer_price,
            "asset": "TON",
            "status": self.offer_status,
            "createdAt": "2026-09-10T12:00:00.000Z",
        }
        if self.offer_buyer is not None:
            item["buyer"] = self.offer_buyer
        if self.offer_seller is not None:
            item["seller"] = self.offer_seller
        return item

    async def post_json(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        mutation: bool = False,
    ) -> TonnelHttpResponse:
        self.calls.append((url, dict(payload), mutation))
        self.events.append(url)
        if url.endswith("/api/auth/telegram/miniapp/session"):
            account_id = int(str(payload["initData"]).rsplit("-", 1)[-1])
            telegram_id = self.auth_user_ids[account_id]
            return TonnelHttpResponse(
                200,
                {
                    "status": "success",
                    "token": f"secret-token-{account_id}",
                    "user": {"id": telegram_id},
                },
            )
        if self.reads_unavailable and not mutation:
            raise TonnelNetworkError("regional read unavailable")
        if url.endswith("/api/balance/info"):
            buyer = str(payload["authData"]).endswith("-1")
            return TonnelHttpResponse(
                200,
                {
                    "status": "success",
                    "balance": self.buyer_balance if buyer else "0",
                    "tonnelBalance": "0",
                    "usdtBalance": "0",
                    "transferGift": False,
                },
            )
        if url.endswith("/api/pageGifts"):
            if self.page_gifts_failure is not None:
                return self.page_gifts_failure
            if self.accept_attempted and self.fail_reads_after_accept:
                return TonnelHttpResponse(503, {"status": "error"})
            filters = json.loads(str(payload["filter"]))
            seller = filters["seller"]
            if self.accept_attempted and self.accept_application_success:
                probe = self._ownership_page_call_count // 2 + 1
                self._ownership_page_call_count += 1
                self.ownership_probe_count = max(self.ownership_probe_count, probe)
                if (
                    not self.accept_success_without_transfer
                    and self.ownership_converges_after_probe is not None
                    and probe >= self.ownership_converges_after_probe
                ):
                    self.source_owns = False
                    self.destination_owns = True
            if seller == 302 and self.source_owns:
                return TonnelHttpResponse(
                    200, [raw_gift(gift_id=self.source_gift_id)]
                )
            if seller == 300 and self.destination_owns:
                return TonnelHttpResponse(
                    200, [raw_gift(seller=300, gift_id=self.source_gift_id)]
                )
            return TonnelHttpResponse(200, [])
        if url.endswith("/api/fetchMangedGifts"):
            return TonnelHttpResponse(
                200,
                {"status": "error", "message": "Business connection not found"},
            )
        if url.endswith("/api/buyOffer/getOffers"):
            if self.get_offers_failure is not None:
                return self.get_offers_failure
            if self.accept_attempted and self.fail_reads_after_accept:
                return TonnelHttpResponse(503, {"status": "error"})
            offers = [self.offer(offer_id) for offer_id in self.preexisting_offer_ids]
            if self.offer_exists:
                offers.append(self.offer())
            if self.offer_exists and self.extra_exact_offer:
                offers.append(self.offer(701))
            if self.complementary_identity_fields:
                for offer in offers:
                    offer.pop("buyer", None)
            return TonnelHttpResponse(200, {"status": "success", "offers": offers})
        if url.endswith("/api/buyOffer/getMyOffers"):
            if self.get_my_offers_failure is not None:
                return self.get_my_offers_failure
            offers = [self.offer(offer_id) for offer_id in self.preexisting_offer_ids]
            if self.offer_exists:
                offers.append(self.offer())
            if self.offer_exists and self.extra_exact_offer:
                offers.append(self.offer(701))
            if self.complementary_identity_fields:
                for offer in offers:
                    offer.pop("seller", None)
            return TonnelHttpResponse(200, {"status": "success", "offers": offers})
        if url.endswith("/api/buyOffer/create"):
            self.offer_exists = self.create_adds_offer
            if self.create_ambiguous:
                raise TonnelMutationNetworkError("ambiguous")
            return self.create_response
        if url.endswith("/api/buyOffer/acceptBuyOffer"):
            self.accept_attempted = True
            if (
                self.accept_response.body
                == {"status": "success", "message": "accepted"}
            ):
                self.accept_application_success = True
                if not self.accept_success_without_transfer:
                    self.offer_status = "accepted"
                    self.offer_exists = False
                if (
                    not self.accept_success_without_transfer
                    and self.ownership_converges_after_probe == 0
                ):
                    self.source_owns = False
                    self.destination_owns = True
            if self.accept_ambiguous:
                if not self.complete_ambiguous_accept:
                    self.accept_application_success = False
                    self.source_owns = True
                    self.destination_owns = False
                    self.offer_exists = True
                    self.offer_status = "pending"
                raise TonnelMutationNetworkError("ambiguous")
            return self.accept_response
        raise AssertionError(f"unexpected URL {url}")

    async def close(self) -> None:
        self.closed = True


class TonnelBuyOfferTests(unittest.IsolatedAsyncioTestCase):
    def service(
        self,
        transport: FakeBuyOfferTransport,
        *,
        api_origin: str = TONNEL_API_ORIGIN,
        offer_accept_delay_ms: int = 0,
        ownership_verify_timeout_ms: int = 15_000,
    ) -> TonnelService:
        return TonnelService(
            cast(MiniAppService, FakeMiniApps()),
            cast(Database, FakeDatabase()),
            transfer_mode="BUY_OFFER",
            api_origin=api_origin,
            offer_accept_delay_ms=offer_accept_delay_ms,
            ownership_verify_timeout_ms=ownership_verify_timeout_ms,
            transport_factory=lambda: cast(TonnelHttpTransport, transport),
        )

    async def transfer(
        self,
        transport: FakeBuyOfferTransport,
        *,
        amount: str = "5",
        dry_run: bool = False,
        api_origin: str = TONNEL_API_ORIGIN,
        offer_accept_delay_ms: int = 0,
        ownership_verify_timeout_ms: int = 15_000,
    ) -> Any:
        with patch("app.telegram_client.tonnel_service.asyncio.sleep", new=AsyncMock()):
            return await self.service(
                transport,
                api_origin=api_origin,
                offer_accept_delay_ms=offer_accept_delay_ms,
                ownership_verify_timeout_ms=ownership_verify_timeout_ms,
            ).transfer_buy_offer(
                owner_telegram_id=CONTROL_USER,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelMarketGiftIdentity(91, "91", "LolPop-17"),
                offer_amount=amount,
                dry_run=dry_run,
            )

    async def test_unlisted_gift_transfers_without_business_or_public_sale(
        self,
    ) -> None:
        transport = FakeBuyOfferTransport()
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        urls = [call[0] for call in transport.calls]
        self.assertFalse(any("fetchMangedGifts" in url for url in urls))
        self.assertFalse(any("listForSale" in url for url in urls))
        self.assertFalse(any("buyGift/" in url for url in urls))

    async def test_create_and_accept_use_exact_official_payloads(self) -> None:
        transport = FakeBuyOfferTransport()
        await self.transfer(transport)
        create = next(call for call in transport.calls if call[0].endswith("/create"))
        accept = next(
            call for call in transport.calls if call[0].endswith("/acceptBuyOffer")
        )
        get_offers = next(
            call for call in transport.calls if call[0].endswith("/getOffers")
        )
        get_my_offers = next(
            call for call in transport.calls if call[0].endswith("/getMyOffers")
        )
        self.assertEqual(
            create[1],
            {
                "authData": "secret-token-1",
                "gift_id": 91,
                "amount": Decimal(5),
                "asset": "TON",
            },
        )
        self.assertEqual(accept[1], {"authData": "secret-token-2", "offer_id": 700})
        self.assertEqual(get_offers[1]["gift_id"], "91")
        self.assertIs(type(get_offers[1]["gift_id"]), str)
        self.assertEqual(get_my_offers[1]["pageSize"], 50)
        self.assertEqual(get_my_offers[1]["filter"], {})
        self.assertIsInstance(get_my_offers[1]["tillTime"], str)
        self.assertNotIn("timestamp", create[1])
        self.assertNotIn("wtf", create[1])
        self.assertNotIn("timestamp", accept[1])
        self.assertNotIn("wtf", accept[1])

    async def test_two_child_scopes_transmit_one_create_for_each_exact_gift(
        self,
    ) -> None:
        transmissions: dict[int, int] = {}
        for child_job_id, gift_id in ((44, 91), (45, 92)):
            transport = FakeBuyOfferTransport()
            transport.source_gift_id = gift_id
            transport.offer_gift_id = gift_id
            with patch(
                "app.telegram_client.tonnel_service.asyncio.sleep", new=AsyncMock()
            ):
                await self.service(transport).transfer_buy_offer(
                    owner_telegram_id=CONTROL_USER,
                    source_account_id=2,
                    destination_account_id=1,
                    identity=TonnelMarketGiftIdentity(
                        gift_id, str(gift_id), "LolPop-17"
                    ),
                    offer_amount="2",
                    dry_run=False,
                    mutation_guard_key=f"transfer-job:{child_job_id}",
                    child_job_id=child_job_id,
                )
            transmissions[gift_id] = self.mutations(transport, "/create")

        self.assertEqual(transmissions, {91: 1, 92: 1})

    async def test_duplicate_child_create_is_blocked_before_second_transmission(
        self,
    ) -> None:
        transport = FakeBuyOfferTransport()
        service = self.service(transport)

        async def execute_once() -> Any:
            return await service.transfer_buy_offer(
                owner_telegram_id=CONTROL_USER,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelMarketGiftIdentity(91, "91", "LolPop-17"),
                offer_amount="2",
                dry_run=False,
                mutation_guard_key="transfer-job:45",
                child_job_id=45,
            )

        with patch(
            "app.telegram_client.tonnel_service.asyncio.sleep", new=AsyncMock()
        ):
            first = await execute_once()
            transport.source_owns = True
            transport.destination_owns = False
            transport.offer_exists = False
            transport.offer_status = "pending"
            with self.assertRaises(TonnelDuplicateMutationError):
                await execute_once()

        self.assertEqual(self.mutations(transport, "/create"), 1)
        audit = first.create_request_metadata
        self.assertEqual(audit["child_job_id"], 45)
        self.assertEqual(audit["gift_id"], 91)
        self.assertEqual(audit["amount"], "2")
        self.assertEqual(audit["http_transmission_count"], 1)
        self.assertIsNotNone(audit["request_started_at"])
        self.assertIsNotNone(audit["response_received_at"])
        self.assertNotIn("secret-token", repr(audit))

    async def test_action_origin_does_not_override_offer_read_shards(self) -> None:
        transport = FakeBuyOfferTransport()

        result = await self.transfer(
            transport,
            api_origin=TONNEL_REGIONAL_API_ORIGIN,
        )

        action_calls = [url for url, _, mutation in transport.calls if mutation]
        self.assertTrue(action_calls)
        self.assertTrue(
            all(url.startswith(TONNEL_REGIONAL_API_ORIGIN) for url in action_calls)
        )
        offer_read_calls = [
            url
            for url, _, mutation in transport.calls
            if not mutation
            if any(
                url.endswith(path)
                for path in (
                    "/api/buyOffer/getMyOffers",
                    "/api/buyOffer/getOffers",
                )
            )
        ]
        self.assertIn(
            f"{TONNEL_REGIONAL_DATA_ORIGIN}/api/buyOffer/getOffers",
            offer_read_calls,
        )
        self.assertIn(
            f"{TONNEL_REGIONAL_DATA_ORIGIN}/api/buyOffer/getMyOffers",
            offer_read_calls,
        )
        self.assertTrue(
            all(url.startswith(TONNEL_REGIONAL_DATA_ORIGIN) for url in offer_read_calls)
        )
        page_gifts_urls = [
            url for url, _, _ in transport.calls if url.endswith("/api/pageGifts")
        ]
        self.assertIn("https://gifts2.tonnel.network/api/pageGifts", page_gifts_urls)
        self.assertFalse(
            any(url.startswith(TONNEL_REGIONAL_API_ORIGIN) for url in page_gifts_urls)
        )
        balance_urls = [
            url for url, _, _ in transport.calls if url.endswith("/api/balance/info")
        ]
        self.assertEqual(
            balance_urls,
            ["https://gifts3.tonnel.network/api/balance/info"],
        )
        self.assertEqual(
            result.create_request_metadata["api_origin"],
            TONNEL_REGIONAL_API_ORIGIN,
        )
        self.assertEqual(
            result.accept_request_metadata["api_origin"],
            TONNEL_REGIONAL_API_ORIGIN,
        )
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_default_action_origin_remains_without_mutation_fallback(
        self,
    ) -> None:
        transport = FakeBuyOfferTransport()

        await self.transfer(transport, api_origin=TONNEL_API_ORIGIN)

        mutation_calls = [url for url, _, mutation in transport.calls if mutation]
        self.assertTrue(
            all(url.startswith(TONNEL_API_ORIGIN) for url in mutation_calls)
        )
        self.assertFalse(
            any(url.startswith(TONNEL_REGIONAL_API_ORIGIN) for url in mutation_calls)
        )
        read_urls = [url for url, _, mutation in transport.calls if not mutation]
        self.assertIn("https://gifts2.tonnel.network/api/buyOffer/getOffers", read_urls)
        self.assertIn(
            "https://gifts3.tonnel.network/api/buyOffer/getMyOffers", read_urls
        )
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    def test_api_origin_rejects_unknown_hosts(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported Tonnel API origin"):
            self.service(
                FakeBuyOfferTransport(),
                api_origin="https://untrusted.example",
            )

    async def test_regional_read_failure_stops_before_any_mutation(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.reads_unavailable = True

        with self.assertRaises(TonnelPreflightFailure) as captured:
            await self.transfer(
                transport,
                api_origin=TONNEL_REGIONAL_API_ORIGIN,
            )

        self.assertEqual(captured.exception.diagnostic["operation"], "page_gifts")
        self.assertEqual(captured.exception.diagnostic["failure_kind"], "network")
        self.assertFalse(any(mutation for _, _, mutation in transport.calls))

    async def test_page_gifts_failure_is_not_gift_relayer_guidance(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.page_gifts_failure = TonnelHttpResponse(
            503,
            {"status": "error", "message": "Business connection not found"},
        )

        with self.assertRaises(TonnelPreflightFailure) as captured:
            await self.service(transport).get_market_inventory(
                2, owner_telegram_id=CONTROL_USER
            )

        diagnostic = captured.exception.diagnostic
        self.assertEqual(captured.exception.error_code, "INVENTORY_UNAVAILABLE")
        self.assertEqual(diagnostic["operation"], "page_gifts")
        self.assertEqual(diagnostic["hostname"], "gifts2.tonnel.network")
        self.assertEqual(diagnostic["endpoint"], "/api/pageGifts")
        self.assertEqual(diagnostic["method"], "POST")
        self.assertEqual(diagnostic["http_status"], 503)
        self.assertEqual(diagnostic["failure_kind"], "http")
        self.assertNotIn("GiftRelayer", str(captured.exception))
        self.assertFalse(any(mutation for _, _, mutation in transport.calls))

    async def test_balance_failure_has_safe_operation_diagnostic(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.reads_unavailable = True

        with self.assertRaises(TonnelPreflightFailure) as captured:
            await self.service(transport).get_balance(1, owner_telegram_id=CONTROL_USER)

        diagnostic = captured.exception.diagnostic
        self.assertEqual(diagnostic["operation"], "balance_info")
        self.assertEqual(diagnostic["hostname"], "gifts3.tonnel.network")
        self.assertEqual(diagnostic["endpoint"], "/api/balance/info")
        self.assertIsNone(diagnostic["http_status"])
        self.assertNotIn("secret-token", repr(diagnostic))

    async def test_offer_read_failures_are_distinguished(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.reads_unavailable = True
        service = self.service(transport)

        with self.assertRaises(TonnelPreflightFailure) as gift_error:
            await service.get_buy_offers_for_gift(2, 91, owner_telegram_id=CONTROL_USER)
        with self.assertRaises(TonnelPreflightFailure) as mine_error:
            await service.get_my_buy_offers(1, owner_telegram_id=CONTROL_USER)

        self.assertEqual(
            gift_error.exception.diagnostic["operation"], "buy_offer_get_offers"
        )
        self.assertEqual(
            gift_error.exception.diagnostic["hostname"], "gifts2.tonnel.network"
        )
        self.assertEqual(
            mine_error.exception.diagnostic["operation"],
            "buy_offer_get_my_offers",
        )
        self.assertEqual(
            mine_error.exception.diagnostic["hostname"], "gifts3.tonnel.network"
        )
        self.assertFalse(any(mutation for _, _, mutation in transport.calls))

    async def test_offer_read_404_diagnostics_preserve_route(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.get_offers_failure = TonnelHttpResponse(404, None)
        transport.get_my_offers_failure = TonnelHttpResponse(404, None)
        service = self.service(transport)

        with self.assertRaises(TonnelPreflightFailure) as gift_error:
            await service.get_buy_offers_for_gift(
                2, "91", owner_telegram_id=CONTROL_USER
            )
        with self.assertRaises(TonnelPreflightFailure) as mine_error:
            await service.get_my_buy_offers(1, owner_telegram_id=CONTROL_USER)

        gift_diagnostic = gift_error.exception.diagnostic
        mine_diagnostic = mine_error.exception.diagnostic
        self.assertEqual(gift_diagnostic["operation"], "buy_offer_get_offers")
        self.assertEqual(gift_diagnostic["endpoint"], "/api/buyOffer/getOffers")
        self.assertEqual(gift_diagnostic["http_status"], 404)
        self.assertEqual(gift_diagnostic["failure_kind"], "http")
        self.assertEqual(mine_diagnostic["operation"], "buy_offer_get_my_offers")
        self.assertEqual(mine_diagnostic["endpoint"], "/api/buyOffer/getMyOffers")
        self.assertEqual(mine_diagnostic["http_status"], 404)
        self.assertEqual(mine_diagnostic["failure_kind"], "http")
        self.assertFalse(any(mutation for _, _, mutation in transport.calls))

    def test_network_failure_categories_are_specific(self) -> None:
        self.assertEqual(_network_failure_kind(TimeoutError()), "timeout")
        self.assertEqual(_network_failure_kind(socket.gaierror()), "dns")
        self.assertEqual(_network_failure_kind(ssl.SSLError()), "tls")
        self.assertEqual(_network_failure_kind(ConnectionRefusedError()), "connect")

    async def test_successful_empty_inventory_is_not_an_api_failure(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.source_owns = False

        gifts = await self.service(transport).get_market_inventory(
            2, owner_telegram_id=CONTROL_USER
        )

        self.assertEqual(gifts, [])
        self.assertFalse(any(mutation for _, _, mutation in transport.calls))

    async def test_exact_typed_gift_identity_is_required(self) -> None:
        transport = FakeBuyOfferTransport()
        with (
            self.assertRaises(TonnelGiftUnavailableError),
            patch("app.telegram_client.tonnel_service.asyncio.sleep", new=AsyncMock()),
        ):
            await self.service(transport).transfer_buy_offer(
                owner_telegram_id=CONTROL_USER,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelMarketGiftIdentity("91", "91", "LolPop-17"),
                offer_amount="5",
                dry_run=False,
            )
        self.assertFalse(any(call[2] for call in transport.calls))

    async def test_similar_display_identity_does_not_match_exact_gift(self) -> None:
        transport = FakeBuyOfferTransport()
        with (
            self.assertRaises(TonnelGiftUnavailableError),
            patch("app.telegram_client.tonnel_service.asyncio.sleep", new=AsyncMock()),
        ):
            await self.service(transport).transfer_buy_offer(
                owner_telegram_id=CONTROL_USER,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelMarketGiftIdentity(91, "91", "LolPop-18"),
                offer_amount="5",
                dry_run=False,
            )
        self.assertFalse(any(call[2] for call in transport.calls))

    async def test_insufficient_balance_sends_no_mutation(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.buyer_balance = "5"
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.error_code, "INSUFFICIENT_BALANCE")
        self.assertFalse(any(call[2] for call in transport.calls))

    async def test_http_200_application_error_is_not_success(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.create_response = TonnelHttpResponse(
            200, {"status": "error", "message": "offer rejected"}
        )
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.create_result.message, "offer rejected")
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_wrong_offer_fields_never_reach_accept(self) -> None:
        for attribute, value in (
            ("offer_buyer", 999),
            ("offer_seller", 999),
            ("offer_gift_id", 92),
            ("offer_price", "4.999"),
            ("offer_status", "cancelled"),
        ):
            with self.subTest(attribute=attribute):
                transport = FakeBuyOfferTransport()
                setattr(transport, attribute, value)
                result = await self.transfer(transport)
                self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
                self.assertEqual(self.mutations(transport, "/create"), 1)
                self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_multiple_exact_offers_are_ambiguous(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.extra_exact_offer = True
        transport.create_response = TonnelHttpResponse(
            200, {"status": "success", "message": "created"}
        )
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_offer_correlation_proves_single_new_id(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.create_response = TonnelHttpResponse(
            200, {"status": "success", "message": "created"}
        )

        result = await self.transfer(transport)

        self.assertEqual(result.offer_correlation["pre_create_offer_count"], 0)
        self.assertEqual(result.offer_correlation["post_create_offer_count"], 1)
        self.assertEqual(result.offer_correlation["new_offer_count"], 1)
        self.assertEqual(result.offer_correlation["semantic_match_count"], 1)
        self.assertTrue(result.offer_correlation["unique_new_offer_proven"])
        self.assertEqual(result.offer_correlation["new_offer_id_type"], "int")
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_old_identical_offer_is_excluded_by_set_difference(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.preexisting_offer_ids = [699]
        transport.create_response = TonnelHttpResponse(
            200, {"status": "success", "message": "created"}
        )

        result = await self.transfer(transport)

        accept = next(
            call for call in transport.calls if call[0].endswith("/acceptBuyOffer")
        )
        self.assertEqual(accept[1]["offer_id"], 700)
        self.assertEqual(result.offer_correlation["pre_create_offer_count"], 1)
        self.assertEqual(result.offer_correlation["post_create_offer_count"], 2)
        self.assertEqual(result.offer_correlation["new_offer_count"], 1)
        self.assertTrue(result.offer_correlation["unique_new_offer_proven"])

    async def test_several_old_identical_offers_do_not_hide_unique_new_id(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.preexisting_offer_ids = [698, 699]
        transport.create_response = TonnelHttpResponse(
            200, {"status": "success", "message": "created"}
        )

        result = await self.transfer(transport)

        accept = next(
            call for call in transport.calls if call[0].endswith("/acceptBuyOffer")
        )
        self.assertEqual(accept[1]["offer_id"], 700)
        self.assertEqual(result.offer_correlation["pre_create_offer_count"], 2)
        self.assertEqual(result.offer_correlation["new_offer_count"], 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_no_new_offer_id_never_reaches_accept(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.preexisting_offer_ids = [699]
        transport.create_adds_offer = False
        transport.create_response = TonnelHttpResponse(
            200, {"status": "success", "message": "created"}
        )

        result = await self.transfer(transport)

        self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
        self.assertEqual(result.offer_correlation["new_offer_count"], 0)
        self.assertFalse(result.offer_correlation["unique_new_offer_proven"])
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_two_new_exact_ids_are_ambiguous_without_accept(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.extra_exact_offer = True
        transport.create_response = TonnelHttpResponse(
            200, {"status": "success", "message": "created"}
        )

        result = await self.transfer(transport)

        self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
        self.assertEqual(result.offer_correlation["new_offer_count"], 2)
        self.assertEqual(result.offer_correlation["semantic_match_count"], 2)
        self.assertFalse(result.offer_correlation["unique_new_offer_proven"])
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_create_network_ambiguity_is_reconciled_without_retry(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.create_ambiguous = True
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_accept_network_ambiguity_is_never_retried(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.accept_ambiguous = True
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.error_code, "OFFER_NOT_ACCEPTED")
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_ambiguous_accept_can_be_proven_by_exact_ownership(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.accept_ambiguous = True
        transport.complete_ambiguous_accept = True
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_incomplete_final_reads_remain_ambiguous(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.fail_reads_after_accept = True
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
        self.assertEqual(result.error_code, "OWNERSHIP_VERIFICATION_TIMEOUT")
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_accept_request_metadata_is_structural_and_n2_bound(self) -> None:
        transport = FakeBuyOfferTransport()
        result = await self.transfer(transport)

        metadata = result.accept_request_metadata
        self.assertEqual(metadata["endpoint"], "/api/buyOffer/acceptBuyOffer")
        self.assertEqual(metadata["method"], "POST")
        self.assertEqual(metadata["body_fields"], ["authData", "offer_id"])
        self.assertEqual(metadata["offer_id_type"], "int")
        self.assertTrue(metadata["identity_match"])
        self.assertTrue(metadata["context_prepared_for_n2"])
        self.assertFalse(metadata["context_shared_with_n1"])
        self.assertEqual(metadata["credentials_mode"], "same-origin")
        self.assertFalse(metadata["cookies_sent"])
        self.assertNotIn("secret-token", repr(metadata))
        self.assertNotIn("secret-init", repr(metadata))

    async def test_string_offer_id_preserves_read_api_type_and_serialization(
        self,
    ) -> None:
        transport = FakeBuyOfferTransport()
        transport.offer_id = "offer-700"
        transport.create_response = TonnelHttpResponse(
            200,
            {"status": "success", "message": "created", "offer_id": "offer-700"},
        )

        result = await self.transfer(transport)

        accept = next(
            call for call in transport.calls if call[0].endswith("/acceptBuyOffer")
        )
        self.assertEqual(accept[1]["offer_id"], "offer-700")
        self.assertIsInstance(accept[1]["offer_id"], str)
        self.assertEqual(result.accept_request_metadata["offer_id_type"], "str")
        self.assertEqual(
            result.accept_request_metadata["offer_id_frontend_expected_type"],
            "str",
        )

    async def test_wrong_accept_identity_is_rejected_before_mutation(self) -> None:
        transport = FakeBuyOfferTransport()
        session = _TonnelSession(
            account=account(2, 302, "TARGET"),
            transport=cast(TonnelHttpTransport, transport),
            auth_data="secret-token-2",
            authenticated_telegram_id=300,
            auth_created_at="2026-09-10T12:00:00.000Z",
        )

        with self.assertRaises(TonnelAuthenticationError):
            TonnelService._assert_accept_context(session)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_transport_matches_official_browser_request_context(self) -> None:
        transport = _AiohttpTonnelTransport()
        try:
            self.assertEqual(
                transport._session.headers["Origin"], TONNEL_FRONTEND_ORIGIN
            )
            self.assertEqual(
                transport._session.headers["Referer"], TONNEL_FRONTEND_REFERER
            )
            self.assertEqual(transport._session.headers["Sec-Fetch-Dest"], "empty")
            self.assertEqual(transport._session.headers["Sec-Fetch-Mode"], "cors")
            self.assertEqual(transport._session.headers["Sec-Fetch-Site"], "cross-site")
            self.assertEqual(
                type(transport._session.cookie_jar).__name__, "DummyCookieJar"
            )
        finally:
            await transport.close()

    async def test_accept_http_200_application_error_is_preserved(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.accept_response = TonnelHttpResponse(
            200, {"status": "error", "message": "Please try again later!"}
        )

        result = await self.transfer(transport)

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.accept_result.http_status, 200)
        self.assertEqual(result.accept_result.status, "error")
        self.assertEqual(result.accept_result.message, "Please try again later!")
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_accept_success_still_requires_ownership_verification(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.accept_success_without_transfer = True

        result = await self.transfer(transport)

        self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
        self.assertEqual(result.error_code, "OWNERSHIP_VERIFICATION_TIMEOUT")
        self.assertEqual(result.accept_result.status, "success")
        self.assertEqual(
            result.message,
            "Принятие подтверждено Tonnel, владение ещё не успело обновиться.",
        )
        self.assertEqual(result.ownership_verification["timeout_ms"], 15000)
        self.assertEqual(result.ownership_verification["probe_count"], 16)
        self.assertIsNone(result.ownership_verification["confirmed_ms"])
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_accept_success_and_immediate_ownership_is_success(self) -> None:
        transport = FakeBuyOfferTransport()

        result = await self.transfer(transport)

        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(result.ownership_verification["probe_count"], 1)
        self.assertIsNotNone(result.ownership_verification["confirmed_ms"])
        self.assertFalse(result.ownership_verification["source_owns_after"])
        self.assertTrue(result.ownership_verification["destination_owns_after"])

    async def test_accept_success_converges_after_three_read_only_probes(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.ownership_converges_after_probe = 3

        result = await self.transfer(transport)

        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(result.ownership_verification["probe_count"], 3)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)
        accept_index = next(
            index
            for index, item in enumerate(transport.calls)
            if item[0].endswith("/acceptBuyOffer")
        )
        self.assertTrue(
            all(not mutation for _, _, mutation in transport.calls[accept_index + 1 :])
        )

    async def test_accept_success_converges_on_last_timeout_probe(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.ownership_converges_after_probe = 4

        result = await self.transfer(
            transport,
            ownership_verify_timeout_ms=3_000,
        )

        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(result.ownership_verification["timeout_ms"], 3_000)
        self.assertEqual(result.ownership_verification["probe_count"], 4)
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_configured_delay_occurs_after_exact_n2_correlation(self) -> None:
        transport = FakeBuyOfferTransport()

        async def sleep(seconds: float) -> None:
            transport.events.append(f"sleep:{seconds}")

        with patch("app.telegram_client.tonnel_service.asyncio.sleep", sleep):
            result = await self.service(
                transport,
                offer_accept_delay_ms=1000,
            ).transfer_buy_offer(
                owner_telegram_id=CONTROL_USER,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelMarketGiftIdentity(91, "91", "LolPop-17"),
                offer_amount="5",
                dry_run=False,
            )

        create_index = next(
            index
            for index, event in enumerate(transport.events)
            if event.endswith("/api/buyOffer/create")
        )
        sleep_index = transport.events.index("sleep:1.0")
        accept_index = next(
            index
            for index, event in enumerate(transport.events)
            if event.endswith("/api/buyOffer/acceptBuyOffer")
        )
        correlated_get_offers = max(
            index
            for index, event in enumerate(transport.events[:sleep_index])
            if event.endswith("/api/buyOffer/getOffers")
        )
        correlated_get_mine = max(
            index
            for index, event in enumerate(transport.events[:sleep_index])
            if event.endswith("/api/buyOffer/getMyOffers")
        )
        self.assertLess(create_index, correlated_get_offers)
        self.assertLess(create_index, correlated_get_mine)
        self.assertLess(correlated_get_offers, sleep_index)
        self.assertLess(correlated_get_mine, sleep_index)
        self.assertLess(sleep_index, accept_index)
        self.assertEqual(result.timings_ms["accept_delay_ms"], 1000)
        self.assertIsNotNone(result.timings_ms["n2_offer_confirmed_ms"])
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_measured_confirmation_to_accept_timing_includes_delay(self) -> None:
        transport = FakeBuyOfferTransport()

        result = await self.service(
            transport,
            offer_accept_delay_ms=20,
        ).transfer_buy_offer(
            owner_telegram_id=CONTROL_USER,
            source_account_id=2,
            destination_account_id=1,
            identity=TonnelMarketGiftIdentity(91, "91", "LolPop-17"),
            offer_amount="5",
            dry_run=False,
        )

        elapsed = result.timings_ms["n2_offer_confirmed_to_accept_start_ms"]
        self.assertIsNotNone(elapsed)
        assert elapsed is not None
        self.assertGreaterEqual(elapsed, 15)
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_accept_delay_is_not_used_without_exact_correlation(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.offer_price = "4"
        sleep_mock = AsyncMock()

        with patch(
            "app.telegram_client.tonnel_service.asyncio.sleep",
            new=sleep_mock,
        ):
            result = await self.service(
                transport,
                offer_accept_delay_ms=1000,
            ).transfer_buy_offer(
                owner_telegram_id=CONTROL_USER,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelMarketGiftIdentity(91, "91", "LolPop-17"),
                offer_amount="5",
                dry_run=False,
            )

        self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
        self.assertNotIn(call(1.0), sleep_mock.await_args_list)
        self.assertEqual(self.mutations(transport, "/create"), 1)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    def test_offer_accept_delay_validation(self) -> None:
        for invalid in (-1, 5001, True):
            with self.subTest(delay=invalid), self.assertRaises(ValueError):
                self.service(
                    FakeBuyOfferTransport(),
                    offer_accept_delay_ms=invalid,
                )

    def test_ownership_verify_timeout_validation(self) -> None:
        for invalid in (-1, 60_001, True):
            with self.subTest(timeout=invalid), self.assertRaises(ValueError):
                self.service(
                    FakeBuyOfferTransport(),
                    ownership_verify_timeout_ms=invalid,
                )

    async def test_dry_run_proves_reads_and_sends_zero_mutations(self) -> None:
        transport = FakeBuyOfferTransport()
        result = await self.transfer(transport, dry_run=True)
        self.assertIs(result.status, TonnelTransferStatus.DRY_RUN)
        self.assertFalse(any(call[2] for call in transport.calls))
        self.assertTrue(any(call[0].endswith("/getOffers") for call in transport.calls))
        self.assertTrue(
            any(call[0].endswith("/getMyOffers") for call in transport.calls)
        )

    def test_quote_uses_decimal_atomic_values_and_current_fees(self) -> None:
        quote = TonnelService.buy_offer_quote("5")
        self.assertEqual(quote.offer_amount, Decimal(5))
        self.assertEqual(quote.seller_proceeds, Decimal("4.975"))
        self.assertEqual(quote.seller_fee, Decimal("0.025"))
        self.assertEqual(quote.create_fee, Decimal("0.01"))
        self.assertEqual(quote.required_buyer_balance, Decimal("5.01"))
        self.assertEqual(quote.offer_amount_nanotons, 5_000_000_000)
        self.assertEqual(quote.seller_proceeds_nanotons, 4_975_000_000)

    def test_safe_mutation_diagnostics_redact_credentials(self) -> None:
        result = TonnelService._mutation_result(
            True,
            TonnelHttpResponse(
                200,
                {
                    "status": "error",
                    "message": "token=abc authData=secret-value-long-enough-123456789",
                    "token": "must-not-be-copied",
                },
            ),
            1.0,
            1.1,
        )
        self.assertEqual(result.status, "error")
        self.assertNotIn("secret-value", result.message or "")
        self.assertNotIn("must-not-be-copied", repr(result))

    @staticmethod
    def mutations(transport: FakeBuyOfferTransport, suffix: str) -> int:
        return sum(call[2] and call[0].endswith(suffix) for call in transport.calls)


class TonnelExistingOfferDiagnosticTests(unittest.IsolatedAsyncioTestCase):
    def service(
        self,
        transport: FakeBuyOfferTransport,
        *,
        offer_accept_delay_ms: int = 0,
    ) -> TonnelService:
        return TonnelService(
            cast(MiniAppService, FakeMiniApps()),
            cast(Database, FakeDatabase()),
            transfer_mode="BUY_OFFER",
            api_origin=TONNEL_API_ORIGIN,
            offer_accept_delay_ms=offer_accept_delay_ms,
            transport_factory=lambda: cast(TonnelHttpTransport, transport),
        )

    async def diagnose(
        self,
        transport: FakeBuyOfferTransport,
        *,
        amount: str = "5",
        expected_fingerprint: str | None = None,
        confirm: bool = False,
    ) -> Any:
        transport.offer_exists = True
        if expected_fingerprint is None:
            expected_fingerprint = TonnelService.fingerprint(
                f"{type(transport.offer_id).__name__}:{transport.offer_id}"
            )
        return await self.service(
            transport,
            offer_accept_delay_ms=1000,
        ).accept_existing_buy_offer_diagnostic(
            owner_telegram_id=CONTROL_USER,
            source_account_id=2,
            destination_account_id=1,
            gift_id=91,
            offer_amount=amount,
            expected_offer_fingerprint=expected_fingerprint,
            confirm=confirm,
        )

    async def test_default_mode_is_read_only(self) -> None:
        transport = FakeBuyOfferTransport()

        result = await self.diagnose(transport)

        self.assertIs(result.status, TonnelTransferStatus.DRY_RUN)
        self.assertEqual(self.mutations(transport, "/create"), 0)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)
        self.assertEqual(result.candidate_count, 1)

    async def test_real_schema_matches_principal_not_seller_proceeds(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.offer_price = "4.5"
        transport.complementary_identity_fields = True

        result = await self.diagnose(transport, amount="4.5")

        self.assertIs(result.status, TonnelTransferStatus.DRY_RUN)
        self.assertEqual(result.candidate_count, 1)
        self.assertEqual(
            TonnelService.buy_offer_quote("4.5").seller_proceeds,
            Decimal("4.478"),
        )
        self.assertEqual(result.validation["match_diagnostics"]["amount"], "matched")

    async def test_seller_proceeds_is_never_used_as_principal(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.offer_price = "4.5"
        transport.complementary_identity_fields = True

        result = await self.diagnose(transport, amount="4.478", confirm=True)

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.candidate_count, 0)
        self.assertEqual(
            result.validation["match_diagnostics"]["amount"],
            "field_present_but_mismatch",
        )
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_complementary_n1_n2_identity_fields_correlate(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.complementary_identity_fields = True

        result = await self.diagnose(transport)

        diagnostics = result.validation["match_diagnostics"]
        self.assertIs(result.status, TonnelTransferStatus.DRY_RUN)
        self.assertEqual(result.candidate_count, 1)
        self.assertEqual(diagnostics["buyer"], "matched")
        self.assertEqual(diagnostics["seller"], "matched")

    async def test_exact_candidate_with_confirmation_accepts_once(self) -> None:
        transport = FakeBuyOfferTransport()

        result = await self.diagnose(transport, confirm=True)

        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(self.mutations(transport, "/create"), 0)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)
        self.assertFalse(result.source_owns_after)
        self.assertTrue(result.destination_owns_after)
        self.assertFalse(result.offer_active_after)

    async def test_zero_candidate_never_accepts(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.offer_price = "4"

        result = await self.diagnose(transport, confirm=True)

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.candidate_count, 0)
        self.assertFalse(result.validation["amount_match"])
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_multiple_candidates_never_accepts(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.extra_exact_offer = True

        result = await self.diagnose(transport, confirm=True)

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.candidate_count, 2)
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_wrong_fingerprint_never_accepts(self) -> None:
        transport = FakeBuyOfferTransport()

        result = await self.diagnose(
            transport,
            expected_fingerprint="000000000000",
            confirm=True,
        )

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertFalse(result.validation["fingerprint_match"])
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_wrong_amount_never_accepts(self) -> None:
        transport = FakeBuyOfferTransport()

        result = await self.diagnose(transport, amount="4.5", confirm=True)

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertFalse(result.validation["amount_match"])
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_wrong_buyer_or_seller_never_accepts(self) -> None:
        for field in ("offer_buyer", "offer_seller"):
            with self.subTest(field=field):
                transport = FakeBuyOfferTransport()
                setattr(transport, field, 999)

                result = await self.diagnose(transport, confirm=True)

                self.assertIs(result.status, TonnelTransferStatus.FAILED)
                self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_non_pending_offer_never_accepts(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.offer_status = "rejected"

        result = await self.diagnose(transport, confirm=True)

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.validation["status"], "rejected")
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_fresh_n2_identity_mismatch_never_accepts(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.auth_user_ids[2] = 999

        with self.assertRaises(TonnelAuthenticationError):
            await self.diagnose(transport, confirm=True)

        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 0)

    async def test_application_error_is_preserved(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.accept_response = TonnelHttpResponse(
            200, {"status": "error", "message": "Please try again later!"}
        )

        result = await self.diagnose(transport, confirm=True)

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.accept_result.http_status, 200)
        self.assertEqual(result.accept_result.status, "error")
        self.assertEqual(result.accept_result.message, "Please try again later!")
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_success_response_still_requires_ownership_verification(
        self,
    ) -> None:
        transport = FakeBuyOfferTransport()
        transport.accept_success_without_transfer = True

        result = await self.diagnose(transport, confirm=True)

        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.error_code, "OFFER_NOT_ACCEPTED")
        self.assertEqual(self.mutations(transport, "/acceptBuyOffer"), 1)

    async def test_accept_has_no_delay_retry_or_host_fallback(self) -> None:
        transport = FakeBuyOfferTransport()
        transport.accept_ambiguous = True
        sleep_mock = AsyncMock()

        with patch(
            "app.telegram_client.tonnel_service.asyncio.sleep",
            new=sleep_mock,
        ):
            result = await self.diagnose(transport, confirm=True)

        accept_calls = [
            call
            for call in transport.calls
            if call[0].endswith("/acceptBuyOffer")
        ]
        self.assertEqual(len(accept_calls), 1)
        self.assertTrue(accept_calls[0][0].startswith(TONNEL_API_ORIGIN))
        self.assertNotIn(call(1.0), sleep_mock.await_args_list)
        self.assertEqual(self.mutations(transport, "/create"), 0)
        self.assertIn(
            result.status,
            {TonnelTransferStatus.FAILED, TonnelTransferStatus.AMBIGUOUS},
        )

    @staticmethod
    def mutations(transport: FakeBuyOfferTransport, suffix: str) -> int:
        return sum(call[2] and call[0].endswith(suffix) for call in transport.calls)


if __name__ == "__main__":
    unittest.main()
