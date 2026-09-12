from __future__ import annotations

import unittest
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlencode

from app.telegram_client.miniapp_service import MiniAppLaunchResult
from app.telegram_client.mrkt_discovery import (
    DiscoveryHttpResponse,
    MrktDiscoveryInputError,
    MrktDiscoveryService,
)

RAW_INIT_DATA = "query_id=secret-query&" + urlencode(
    {
        "user": '{"id":100,"photo_url":"https://example.test/private"}',
        "auth_date": "1720000000",
        "hash": "secret-init-hash",
    }
)
WEBVIEW_URL = "https://cdn.tgmrkt.io/index.html#" + urlencode(
    {"tgWebAppData": RAW_INIT_DATA}
)
SECRET_TOKEN = "secret-mrkt-token"


class FakeTransport:
    def __init__(
        self,
        *,
        inventory_id: Any = "a" * 8 + "-" + "b" * 27,
        failure: Exception | None = None,
    ) -> None:
        self.inventory_id = inventory_id
        self.failure = failure
        self.calls: list[tuple[str, dict[str, Any], str | None]] = []
        self.closed = False

    def post_json(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        authorization: str | None = None,
    ) -> DiscoveryHttpResponse:
        if self.failure is not None:
            raise self.failure
        self.calls.append((path, dict(payload), authorization))
        if path == "/api/v1/auth":
            return DiscoveryHttpResponse(
                status=200,
                body={"token": SECRET_TOKEN, "isFirstTime": False},
                cookie_names=("access_token",),
                cookie_attribute_names=("httponly", "samesite", "secure"),
            )
        if path == "/api/v1/gifts":
            return DiscoveryHttpResponse(
                status=200,
                body={
                    "cursor": "",
                    "gifts": [{"id": self.inventory_id, "title": "Gift"}],
                    "total": 1,
                },
            )
        raise AssertionError(f"Unexpected path: {path}")

    def close(self) -> None:
        self.closed = True


def launch_result(
    *,
    bot_username: str = "@mrkt",
    init_data: str | None = RAW_INIT_DATA,
    webview_url: str | None = WEBVIEW_URL,
) -> MiniAppLaunchResult:
    return MiniAppLaunchResult(
        account_id=1,
        bot_username=bot_username,
        resolved_bot_id=10,
        webview_obtained=webview_url is not None,
        init_data_obtained=init_data is not None,
        init_data_fingerprint="12345678" if init_data else None,
        init_data_fields=frozenset({"auth_date", "hash", "user"}),
        webview_url=webview_url,
        init_data=init_data,
        query_id=1,
    )


class MrktDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_calls_only_auth_and_inventory(self) -> None:
        transport = FakeTransport()
        service = MrktDiscoveryService(transport_factory=lambda: transport)

        result = await service.inspect(launch_result())

        self.assertEqual(
            [call[0] for call in transport.calls],
            ["/api/v1/auth", "/api/v1/gifts"],
        )
        self.assertIsNone(transport.calls[0][2])
        self.assertEqual(transport.calls[1][2], SECRET_TOKEN)
        self.assertFalse(transport.calls[1][1]["isListed"])
        self.assertEqual(transport.calls[1][1]["ordering"], "None")
        self.assertEqual(result.inventory_item_id_kind, "uuid-like-string")
        self.assertTrue(transport.closed)

    async def test_probe_uses_expected_auth_payload(self) -> None:
        transport = FakeTransport()
        service = MrktDiscoveryService(transport_factory=lambda: transport)

        await service.inspect(launch_result())

        auth_payload = transport.calls[0][1]
        self.assertEqual(auth_payload["data"], RAW_INIT_DATA)
        self.assertIsNone(auth_payload["appId"])
        self.assertEqual(auth_payload["photo"], "https://example.test/private")

    async def test_invalid_launch_is_rejected_before_network_use(self) -> None:
        transport = FakeTransport()
        service = MrktDiscoveryService(transport_factory=lambda: transport)

        with self.assertRaises(MrktDiscoveryInputError):
            await service.inspect(launch_result(bot_username="@portals"))
        with self.assertRaises(MrktDiscoveryInputError):
            await service.inspect(launch_result(init_data=None))
        with self.assertRaises(MrktDiscoveryInputError):
            await service.inspect(
                launch_result(webview_url="https://attacker.example/app")
            )

        self.assertEqual(transport.calls, [])

    async def test_logs_and_result_repr_do_not_expose_credentials(self) -> None:
        transport = FakeTransport()
        service = MrktDiscoveryService(transport_factory=lambda: transport)

        with self.assertLogs(
            "app.telegram_client.mrkt_discovery", level="INFO"
        ) as captured:
            result = await service.inspect(launch_result())

        output = "\n".join(captured.output) + repr(result)
        self.assertNotIn(RAW_INIT_DATA, output)
        self.assertNotIn(SECRET_TOKEN, output)
        self.assertNotIn("secret-query", output)
        self.assertNotIn("secret-init-hash", output)
        self.assertNotIn(WEBVIEW_URL, output)

        auth_response = DiscoveryHttpResponse(
            status=200,
            body={"token": SECRET_TOKEN},
        )
        self.assertNotIn(SECRET_TOKEN, repr(auth_response))

    async def test_integer_item_identifier_is_classified_without_value(self) -> None:
        transport = FakeTransport(inventory_id=987654321)
        result = await MrktDiscoveryService(
            transport_factory=lambda: transport
        ).inspect(launch_result())

        self.assertEqual(result.inventory_item_id_kind, "integer")
        self.assertNotIn("987654321", repr(result))

    async def test_unexpected_failure_is_sanitized(self) -> None:
        secret = RAW_INIT_DATA + SECRET_TOKEN
        transport = FakeTransport(failure=RuntimeError(secret))
        service = MrktDiscoveryService(transport_factory=lambda: transport)

        with (
            self.assertLogs(
                "app.telegram_client.mrkt_discovery", level="WARNING"
            ) as captured,
            self.assertRaisesRegex(RuntimeError, "RuntimeError") as raised,
        ):
            await service.inspect(launch_result())

        self.assertNotIn(secret, str(raised.exception))
        self.assertNotIn(secret, "\n".join(captured.output))
        self.assertTrue(transport.closed)
