from __future__ import annotations

import hashlib
import unittest
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

from telethon.tl import functions, types

from app.db import Account, Database
from app.telegram_client import (
    MiniAppService,
    MiniAppSessionConnectionError,
    MiniAppSessionIdentityMismatchError,
    MiniAppSessionUnauthorizedError,
    SessionService,
    UnsupportedMiniAppBotError,
)
from app.telegram_client.miniapp_service import ClientFactory

OWNER_ID = 100
ACCOUNT_ID = 7
RAW_INIT_DATA = (
    "query_id=secret-query&user=%7B%22id%22%3A100%7D&auth_date=1720000000&"
    "hash=secret-init-hash"
)
WEBVIEW_URL = "https://miniapp.example/app#" + urlencode(
    {
        "tgWebAppData": RAW_INIT_DATA,
        "tgWebAppVersion": "9.1",
        "tgWebAppPlatform": "android",
    }
)


class FakeDatabase:
    def __init__(self, account: Account | None) -> None:
        self.account = account

    async def get_account(
        self, account_id: int, owner_telegram_id: int
    ) -> Account | None:
        if (
            self.account is not None
            and self.account.id == account_id
            and self.account.owner_telegram_id == owner_telegram_id
        ):
            return self.account
        return None


class FakeSessions:
    def __init__(self, exists: bool = True) -> None:
        self.session_exists = exists

    def exists(self, session_key: str) -> bool:
        return self.session_exists

    def path_for(self, session_key: str) -> Path:
        return Path("/private/test-sessions") / session_key


class EmptyWebViewResponse:
    pass


class FakeClient:
    def __init__(
        self,
        *,
        authorized: bool = True,
        response: Any = None,
        telegram_id: int = 999,
    ) -> None:
        self.authorized = authorized
        self.response = response or types.WebViewResultUrl(
            url=WEBVIEW_URL, query_id=987654321
        )
        self.telegram_id = telegram_id
        self.connected = False
        self.disconnected = False
        self.resolved_username: str | None = None
        self.requests: list[Any] = []

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnected = True

    async def is_user_authorized(self) -> bool:
        return self.authorized

    async def get_me(self) -> types.User:
        return types.User(id=self.telegram_id, first_name="Account")

    async def get_entity(self, entity: str) -> types.User:
        self.resolved_username = entity
        bot_id = {"@mrkt": 111, "@portals": 222, "@Tonnel_Network_bot": 333}[entity]
        return types.User(
            id=bot_id,
            access_hash=bot_id * 10,
            bot=True,
            bot_has_main_app=True,
            username=entity.removeprefix("@"),
        )

    async def __call__(self, request: Any) -> Any:
        self.requests.append(request)
        return self.response


class SensitiveFailureClient(FakeClient):
    async def __call__(self, request: Any) -> Any:
        self.requests.append(request)
        raise RuntimeError(WEBVIEW_URL)


class MiniAppServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.account = Account(
            id=ACCOUNT_ID,
            owner_telegram_id=OWNER_ID,
            telegram_account_id=999,
            session_key=f"{OWNER_ID}_{'a' * 32}",
            username="owner",
            first_name="Owner",
            phone="48123456789",
            created_at="2026-08-31 00:00:00",
        )

    def service(
        self,
        client: FakeClient,
        *,
        account: Account | None = None,
    ) -> MiniAppService:
        database = FakeDatabase(self.account if account is None else account)
        sessions = FakeSessions()

        def client_factory(session_path: Path) -> FakeClient:
            self.assertEqual(
                session_path,
                Path("/private/test-sessions") / self.account.session_key,
            )
            return client

        return MiniAppService(
            123,
            "0" * 32,
            cast(Database, database),
            cast(SessionService, sessions),
            client_factory=cast(ClientFactory, client_factory),
        )

    async def test_authorized_session_launches_and_disconnects(self) -> None:
        client = FakeClient()
        result = await self.service(client).open_miniapp(
            ACCOUNT_ID, "@mrkt", owner_telegram_id=OWNER_ID
        )

        self.assertTrue(client.connected)
        self.assertTrue(client.disconnected)
        self.assertTrue(result.webview_obtained)
        self.assertTrue(result.init_data_obtained)
        self.assertEqual(result.query_id, 987654321)

    async def test_revoked_session_is_rejected_and_disconnected(self) -> None:
        client = FakeClient(authorized=False)

        with self.assertRaises(MiniAppSessionUnauthorizedError):
            await self.service(client).open_miniapp(
                ACCOUNT_ID, "@mrkt", owner_telegram_id=OWNER_ID
            )

        self.assertTrue(client.disconnected)
        self.assertEqual(client.requests, [])

    async def test_session_identity_mismatch_is_rejected_before_webview(self) -> None:
        client = FakeClient(telegram_id=123456)

        with self.assertRaises(MiniAppSessionIdentityMismatchError):
            await self.service(client).open_miniapp(
                ACCOUNT_ID, "@mrkt", owner_telegram_id=OWNER_ID
            )

        self.assertTrue(client.disconnected)
        self.assertEqual(client.requests, [])

    async def test_connect_failure_has_specific_safe_error(self) -> None:
        client = FakeClient()

        async def fail_connect() -> None:
            raise ConnectionError("secret-network-detail")

        client.connect = fail_connect  # type: ignore[method-assign]
        with self.assertRaises(MiniAppSessionConnectionError) as captured:
            await self.service(client).open_miniapp(
                ACCOUNT_ID, "@mrkt", owner_telegram_id=OWNER_ID
            )

        self.assertNotIn("secret-network-detail", str(captured.exception))
        self.assertTrue(client.disconnected)

    async def test_unknown_bot_is_rejected_before_session_use(self) -> None:
        client = FakeClient()

        with self.assertRaises(UnsupportedMiniAppBotError):
            await self.service(client).open_miniapp(
                ACCOUNT_ID, "@unknown_bot", owner_telegram_id=OWNER_ID
            )

        self.assertFalse(client.connected)

    async def test_mrkt_launch_uses_main_webview(self) -> None:
        client = FakeClient()
        result = await self.service(client).open_miniapp(
            ACCOUNT_ID, "mrkt", owner_telegram_id=OWNER_ID
        )

        self.assertEqual(client.resolved_username, "@mrkt")
        self.assertEqual(result.resolved_bot_id, 111)
        self._assert_main_webview_request(client)

    async def test_portals_launch_uses_main_webview(self) -> None:
        client = FakeClient()
        result = await self.service(client).open_miniapp(
            ACCOUNT_ID, "@Portals", owner_telegram_id=OWNER_ID
        )

        self.assertEqual(client.resolved_username, "@portals")
        self.assertEqual(result.resolved_bot_id, 222)
        self._assert_main_webview_request(client)

    async def test_tonnel_launch_uses_named_gift_app(self) -> None:
        client = FakeClient()
        result = await self.service(client).open_miniapp(
            ACCOUNT_ID, "tonnel", owner_telegram_id=OWNER_ID
        )

        self.assertEqual(client.resolved_username, "@Tonnel_Network_bot")
        self.assertEqual(result.resolved_bot_id, 333)
        self.assertEqual(len(client.requests), 1)
        request = client.requests[0]
        self.assertIsInstance(request, functions.messages.RequestAppWebViewRequest)
        self.assertIsInstance(request.peer, types.InputPeerSelf)
        self.assertEqual(request.app.short_name, "gift")
        self.assertEqual(request.platform, "android")

        canonical_client = FakeClient()
        await self.service(canonical_client).open_miniapp(
            ACCOUNT_ID,
            "@Tonnel_Network_bot",
            owner_telegram_id=OWNER_ID,
        )
        self.assertEqual(canonical_client.resolved_username, "@Tonnel_Network_bot")

    async def test_missing_webview_or_init_data_is_reported_safely(self) -> None:
        missing_webview = FakeClient(response=EmptyWebViewResponse())
        no_webview_result = await self.service(missing_webview).open_miniapp(
            ACCOUNT_ID, "@mrkt", owner_telegram_id=OWNER_ID
        )
        self.assertFalse(no_webview_result.webview_obtained)
        self.assertFalse(no_webview_result.init_data_obtained)

        missing_init_data = FakeClient(
            response=types.WebViewResultUrl(url="https://miniapp.example/app")
        )
        no_init_result = await self.service(missing_init_data).open_miniapp(
            ACCOUNT_ID, "@portals", owner_telegram_id=OWNER_ID
        )
        self.assertTrue(no_init_result.webview_obtained)
        self.assertFalse(no_init_result.init_data_obtained)

    async def test_logs_and_result_repr_redact_launch_credentials(self) -> None:
        client = FakeClient()
        with self.assertLogs(
            "app.telegram_client.miniapp_service", level="INFO"
        ) as captured:
            result = await self.service(client).open_miniapp(
                ACCOUNT_ID, "@mrkt", owner_telegram_id=OWNER_ID
            )

        logs = "\n".join(captured.output)
        fingerprint = hashlib.sha256(RAW_INIT_DATA.encode()).hexdigest()[:8]
        self.assertIn(fingerprint, logs)
        self.assertNotIn(RAW_INIT_DATA, logs)
        self.assertNotIn(WEBVIEW_URL, logs)
        self.assertNotIn("secret-query", logs)
        self.assertNotIn(RAW_INIT_DATA, repr(result))
        self.assertNotIn(WEBVIEW_URL, repr(result))

        failure_client = SensitiveFailureClient()
        with (
            self.assertLogs(
                "app.telegram_client.miniapp_service", level="WARNING"
            ) as failure_logs,
            self.assertRaises(RuntimeError),
        ):
            await self.service(failure_client).open_miniapp(
                ACCOUNT_ID, "@mrkt", owner_telegram_id=OWNER_ID
            )
        failure_output = "\n".join(failure_logs.output)
        self.assertIn("RuntimeError", failure_output)
        self.assertNotIn(WEBVIEW_URL, failure_output)
        self.assertNotIn(RAW_INIT_DATA, failure_output)

    def _assert_main_webview_request(self, client: FakeClient) -> None:
        self.assertEqual(len(client.requests), 1)
        request = client.requests[0]
        self.assertIsInstance(request, functions.messages.RequestMainWebViewRequest)
        self.assertIsInstance(request.peer, types.InputPeerEmpty)
        self.assertEqual(request.platform, "android")
