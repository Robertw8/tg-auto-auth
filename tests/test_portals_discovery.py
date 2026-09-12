from __future__ import annotations

import unittest
from collections.abc import Mapping
from typing import Any

from app.telegram_client.miniapp_service import MiniAppLaunchResult
from app.telegram_client.portals_discovery import (
    PORTALS_AUTH_PATH,
    PORTALS_INVENTORY_PATH,
    PORTALS_NFT_OFFERS_PATH,
    PORTALS_RECEIVED_OFFERS_PATH,
    PortalsDiscoveryAuthError,
    PortalsDiscoveryError,
    PortalsDiscoveryInputError,
    PortalsDiscoveryService,
    PortalsHttpResponse,
)

SECRET_INIT_DATA = "query_id=secret-query&auth_date=1720000000&hash=secret-hash"
SECRET_RESPONSE_TOKEN = "secret-portals-response-token"


def launch_result(
    *,
    bot_username: str = "@portals",
    webview_url: str | None = "https://portal-market.com/#tgWebAppData=redacted",
    init_data: str | None = SECRET_INIT_DATA,
) -> MiniAppLaunchResult:
    return MiniAppLaunchResult(
        account_id=7,
        bot_username=bot_username,
        resolved_bot_id=123,
        webview_obtained=webview_url is not None,
        init_data_obtained=init_data is not None,
        init_data_fingerprint="abcdef12",
        init_data_fields=frozenset({"auth_date", "hash"}),
        webview_url=webview_url,
        init_data=init_data,
        query_id=1,
    )


class FakePortalsTransport:
    def __init__(
        self,
        responses: list[PortalsHttpResponse] | None = None,
        *,
        failure: Exception | None = None,
    ) -> None:
        self.responses = list(responses or [])
        self.failure = failure
        self.calls: list[tuple[str, dict[str, str | int], str]] = []
        self.closed = False

    def get_json(
        self,
        path: str,
        *,
        query: Mapping[str, str | int] | None = None,
        authorization: str,
    ) -> PortalsHttpResponse:
        if self.failure is not None:
            raise self.failure
        self.calls.append((path, dict(query or {}), authorization))
        if not self.responses:
            raise AssertionError("No fake Portals response remaining")
        return self.responses.pop(0)

    def close(self) -> None:
        self.closed = True


def response(status: int, body: Any) -> PortalsHttpResponse:
    return PortalsHttpResponse(
        status=status,
        body=body,
        cookie_names=("portal_session",),
        cookie_attribute_names=("httponly", "secure"),
    )


class PortalsDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_auth_and_read_only_response_parsing(self) -> None:
        transport = FakePortalsTransport(
            [
                response(
                    200,
                    {
                        "user_id": 1,
                        "username": "owner",
                        "token": SECRET_RESPONSE_TOKEN,
                    },
                ),
                response(
                    200,
                    {
                        "nfts": [{"id": 101, "name": "Gift"}],
                        "total_count": 1,
                    },
                ),
                response(
                    200,
                    {
                        "top_offers": [
                            {
                                "offer": {
                                    "id": "offer-1",
                                    "amount": "1.50",
                                },
                                "nft": {"id": 101, "name": "Gift"},
                            }
                        ],
                        "total_amount": "1.50",
                        "total_count": 1,
                    },
                ),
                response(200, {"offers": [{"id": "offer-1"}]}),
            ]
        )
        service = PortalsDiscoveryService(transport_factory=lambda: transport)

        result = await service.inspect(launch_result())

        self.assertEqual(result.authorization_scheme, "tma")
        self.assertTrue(result.init_data_sent_directly)
        self.assertTrue(result.auth_response_token_present)
        self.assertEqual(result.inventory_item_id_kind, "integer")
        self.assertEqual(result.offer_id_kind, "opaque-string")
        self.assertEqual(result.nft_id_kind, "integer")
        self.assertTrue(result.deal_view_requested)
        self.assertEqual(result.deal_view_response_fields, ("offers",))
        self.assertEqual(result.cookie_names, ("portal_session",))
        self.assertTrue(transport.closed)
        self.assertEqual(
            [call[0] for call in transport.calls],
            [
                PORTALS_AUTH_PATH,
                PORTALS_INVENTORY_PATH,
                PORTALS_RECEIVED_OFFERS_PATH,
                "/offers/nft/101",
            ],
        )
        self.assertTrue(
            all(call[2] == f"tma {SECRET_INIT_DATA}" for call in transport.calls)
        )
        self.assertEqual(result.traces[-1].path, PORTALS_NFT_OFFERS_PATH)
        self.assertNotIn(SECRET_INIT_DATA, repr(result))
        self.assertNotIn(SECRET_RESPONSE_TOKEN, repr(result))

    async def test_empty_read_only_responses_are_parsed(self) -> None:
        transport = FakePortalsTransport(
            [
                response(200, {"user_id": 1}),
                response(200, {"nfts": [], "total_count": 0}),
                response(
                    200,
                    {"top_offers": [], "total_amount": "0", "total_count": 0},
                ),
            ]
        )
        result = await PortalsDiscoveryService(
            transport_factory=lambda: transport
        ).inspect(launch_result())

        self.assertEqual(result.inventory_item_count, 0)
        self.assertEqual(result.received_item_count, 0)
        self.assertFalse(result.deal_view_requested)
        self.assertEqual(len(result.traces), 3)

    async def test_auth_rejection_is_sanitized(self) -> None:
        transport = FakePortalsTransport([response(401, {"message": "secret"})])

        with self.assertRaises(PortalsDiscoveryAuthError) as raised:
            await PortalsDiscoveryService(transport_factory=lambda: transport).inspect(
                launch_result()
            )

        self.assertNotIn("secret", str(raised.exception))
        self.assertTrue(transport.closed)

    async def test_missing_or_invalid_launch_data_is_rejected(self) -> None:
        invalid_launches = (
            launch_result(bot_username="@mrkt"),
            launch_result(webview_url=None),
            launch_result(webview_url="https://example.com/"),
            launch_result(init_data=None),
            launch_result(init_data="auth_date=1720000000"),
        )
        for invalid in invalid_launches:
            with (
                self.subTest(invalid=invalid),
                self.assertRaises(PortalsDiscoveryInputError),
            ):
                await PortalsDiscoveryService().inspect(invalid)

    async def test_secret_failure_is_redacted_from_logs_and_error(self) -> None:
        secret = SECRET_INIT_DATA + SECRET_RESPONSE_TOKEN
        transport = FakePortalsTransport(failure=RuntimeError(secret))

        with (
            self.assertLogs(
                "app.telegram_client.portals_discovery", level="WARNING"
            ) as captured,
            self.assertRaises(PortalsDiscoveryError) as raised,
        ):
            await PortalsDiscoveryService(transport_factory=lambda: transport).inspect(
                launch_result()
            )

        output = "\n".join(captured.output) + str(raised.exception)
        self.assertNotIn(secret, output)
        self.assertNotIn(SECRET_INIT_DATA, output)
        self.assertNotIn(SECRET_RESPONSE_TOKEN, output)

    def test_frontend_api_and_contract_detection(self) -> None:
        source = """
        const api = "https://portal-market.com/api";
        const games = "https://backend.portal-market.com";
        baseApiRequest({url:"/users/auth",method:"GET"});
        baseApiRequest({url:"/nfts/owned",method:"GET"});
        baseApiRequest({url:"/offers/received",method:"GET"});
        baseApiRequest({url:`/offers/nft/${nftId}`,method:"GET"});
        baseApiRequest({url:`/offers/${offerId}/accept`,method:"POST",data:body});
        accept({data:{amount:offer.amount}});
        """

        contract = PortalsDiscoveryService.inspect_frontend_source(source)

        self.assertIn("https://portal-market.com/api", contract.api_origins)
        self.assertIn("https://backend.portal-market.com", contract.api_origins)
        self.assertTrue(contract.auth_path_detected)
        self.assertTrue(contract.inventory_path_detected)
        self.assertTrue(contract.received_offers_path_detected)
        self.assertTrue(contract.nft_offers_path_detected)
        self.assertTrue(contract.accept_path_detected)
        self.assertEqual(contract.accept_payload_fields, ("amount",))

    def test_empty_frontend_source_is_invalid(self) -> None:
        with self.assertRaises(PortalsDiscoveryInputError):
            PortalsDiscoveryService.inspect_frontend_source("   ")
