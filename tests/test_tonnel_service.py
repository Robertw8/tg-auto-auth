from __future__ import annotations

import asyncio
import sqlite3
import unittest
from decimal import Decimal
from typing import Any, cast

from app.db import Account, Database
from app.telegram_client import (
    MiniAppLaunchResult,
    MiniAppService,
    MiniAppSessionConnectionError,
    TonnelAuthenticationError,
    TonnelGiftIdentity,
    TonnelGiftUnavailableError,
    TonnelHttpResponse,
    TonnelHttpTransport,
    TonnelInsufficientBalanceError,
    TonnelPreflightFailure,
    TonnelService,
    TonnelTransferStatus,
    TonnelTransferStrategy,
)

OWNER_TELEGRAM_ID = 100


def account(account_id: int, telegram_id: int, role: str) -> Account:
    return Account(
        id=account_id,
        owner_telegram_id=OWNER_TELEGRAM_ID,
        telegram_account_id=telegram_id,
        session_key=f"session-{account_id}",
        username=f"user{account_id}",
        first_name="User",
        phone=None,
        created_at="2026-09-09 00:00:00",
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
        if item and item.owner_telegram_id == owner_telegram_id:
            return item
        return None


class FakeMiniApps:
    async def open_miniapp(
        self, account_id: int, bot_username: str, *, owner_telegram_id: int
    ) -> MiniAppLaunchResult:
        del owner_telegram_id
        return MiniAppLaunchResult(
            account_id=account_id,
            bot_username=bot_username,
            resolved_bot_id=777,
            webview_obtained=True,
            init_data_obtained=True,
            init_data_fingerprint=f"launch-{account_id}",
            init_data_fields=frozenset({"auth_date", "hash", "user"}),
            webview_url="https://redacted.invalid",
            init_data=f"secret-init-{account_id}",
            query_id=None,
        )


class CountingMiniApps(FakeMiniApps):
    def __init__(self) -> None:
        self.calls: list[int] = []
        self.running = 0
        self.max_running = 0

    async def open_miniapp(
        self, account_id: int, bot_username: str, *, owner_telegram_id: int
    ) -> MiniAppLaunchResult:
        self.calls.append(account_id)
        self.running += 1
        self.max_running = max(self.max_running, self.running)
        try:
            await asyncio.sleep(0)
            return await super().open_miniapp(
                account_id,
                bot_username,
                owner_telegram_id=owner_telegram_id,
            )
        finally:
            self.running -= 1


class FailingMiniApps:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def open_miniapp(
        self, account_id: int, bot_username: str, *, owner_telegram_id: int
    ) -> MiniAppLaunchResult:
        del account_id, bot_username, owner_telegram_id
        raise self.error


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], bool]] = []
        self.closed = False
        self.moved = False
        self.balance = "1.25"
        self.inventory_error_after_mutation = False
        self.mutation_response = TonnelHttpResponse(
            200, {"status": "success", "message": "ok"}
        )

    async def post_json(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        mutation: bool = False,
    ) -> TonnelHttpResponse:
        self.calls.append((url, dict(payload), mutation))
        if url.endswith("/api/auth/telegram/miniapp/session"):
            account_id = int(str(payload["initData"]).rsplit("-", 1)[-1])
            user_id = 300 if account_id == 1 else 302
            return TonnelHttpResponse(
                200,
                {
                    "status": "success",
                    "token": f"secret-token-{account_id}",
                    "user": {"id": user_id},
                },
            )
        if url.endswith("/api/userInfo"):
            return TonnelHttpResponse(
                200,
                {
                    "status": "success",
                    "data": {"userId": int(payload["user"]), "name": "Owner"},
                },
            )
        if url.endswith("/api/returnGiftToUser"):
            self.moved = True
            return self.mutation_response
        if url.endswith("/api/fetchMangedGifts"):
            if self.moved and self.inventory_error_after_mutation:
                return TonnelHttpResponse(
                    200, {"status": "error", "message": "temporarily unavailable"}
                )
            token = str(payload["authData"])
            source = token.endswith("-2")
            items = []
            if (source and not self.moved) or (not source and self.moved):
                items = [raw_gift(91 if source else 92)]
            return TonnelHttpResponse(200, {"status": "success", "data": items})
        if url.endswith("/api/balance/info"):
            return TonnelHttpResponse(
                200,
                {
                    "status": "success",
                    "balance": self.balance,
                    "tonnelBalance": 2,
                    "usdtBalance": "3.5",
                    "transferGift": True,
                },
            )
        if url.endswith("/api/returnGiftStats"):
            return TonnelHttpResponse(
                200, {"status": "success", "data": {"totalReturns": 3}}
            )
        raise AssertionError(f"unexpected URL {url}")

    async def close(self) -> None:
        self.closed = True


def raw_gift(owned_id: int = 91) -> dict[str, Any]:
    return {
        "owned_gift_id": owned_id,
        "next_transfer_date": 0,
        "is_private": False,
        "gift": {
            "number": 17,
            "base_name": "Lol Pop",
            "name": "Lol Pop",
            "model": {"name": "Blue"},
        },
    }


class TonnelServiceTests(unittest.IsolatedAsyncioTestCase):
    def service(self, transport: FakeTransport) -> TonnelService:
        return TonnelService(
            cast(MiniAppService, FakeMiniApps()),
            cast(Database, FakeDatabase()),
            transport_factory=lambda: cast(TonnelHttpTransport, transport),
        )

    async def test_auth_and_balance_parsing(self) -> None:
        transport = FakeTransport()
        balance = await self.service(transport).get_balance(
            1, owner_telegram_id=OWNER_TELEGRAM_ID
        )
        self.assertEqual(balance.ton, Decimal("1.25"))
        self.assertEqual(balance.tonnel, Decimal(2))
        self.assertEqual(balance.usdt, Decimal("3.5"))
        self.assertTrue(balance.direct_transfer_enabled)
        self.assertTrue(transport.closed)

    async def test_batch_auth_opens_each_telegram_session_once_sequentially(
        self,
    ) -> None:
        transport = FakeTransport()
        miniapps = CountingMiniApps()
        service = TonnelService(
            cast(MiniAppService, miniapps),
            cast(Database, FakeDatabase()),
            transfer_mode="BUY_OFFER",
            offer_accept_delay_ms=5000,
            transport_factory=lambda: cast(TonnelHttpTransport, transport),
        )

        async with service.batch_auth_context(
            owner_telegram_id=OWNER_TELEGRAM_ID,
            n1_account_id=1,
            n2_account_id=2,
        ) as context:
            borrowed = await asyncio.gather(
                service.authenticate(1, owner_telegram_id=OWNER_TELEGRAM_ID),
                service.authenticate(2, owner_telegram_id=OWNER_TELEGRAM_ID),
                service.authenticate(2, owner_telegram_id=OWNER_TELEGRAM_ID),
            )
            context.set_child_count(3)
            metadata = context.safe_metadata()
            self.assertEqual([item.account.id for item in borrowed], [1, 2, 2])
            self.assertEqual(metadata["child_count"], 3)
            rendered = repr(metadata).lower()
            for secret in ("authdata", "initdata", "token", "secret-token"):
                self.assertNotIn(secret, rendered)

        self.assertEqual(miniapps.calls, [1, 2])
        self.assertEqual(miniapps.max_running, 1)
        self.assertTrue(transport.closed)

    async def test_direct_transfer_fee_uses_official_threshold(self) -> None:
        transport = FakeTransport()
        fee = await self.service(transport).get_direct_transfer_fee(
            2, owner_telegram_id=OWNER_TELEGRAM_ID
        )
        self.assertEqual(fee, Decimal("0.3"))

    async def test_auth_rejects_wrong_telegram_user(self) -> None:
        transport = FakeTransport()

        async def wrong_post(
            url: str, payload: dict[str, Any], *, mutation: bool = False
        ) -> TonnelHttpResponse:
            del url, payload, mutation
            return TonnelHttpResponse(
                200, {"status": "success", "token": "secret", "user": {"id": 999}}
            )

        transport.post_json = wrong_post  # type: ignore[method-assign]
        with self.assertRaises(TonnelAuthenticationError) as captured:
            await self.service(transport).authenticate(
                1, owner_telegram_id=OWNER_TELEGRAM_ID
            )
        diagnostic = captured.exception.diagnostic
        self.assertIsNotNone(diagnostic)
        assert diagnostic is not None
        self.assertEqual(diagnostic["operation"], "session_auth")
        self.assertEqual(diagnostic["http_status"], 200)
        self.assertEqual(diagnostic["failure_kind"], "application")
        self.assertEqual(diagnostic["failure_code"], "TONNEL_AUTH_APPLICATION_ERROR")
        self.assertNotIn("secret", repr(diagnostic).lower())

    async def test_missing_account_reports_local_stage_without_http(self) -> None:
        transport = FakeTransport()
        with self.assertRaises(TonnelAuthenticationError) as captured:
            await self.service(transport).authenticate(
                999, owner_telegram_id=OWNER_TELEGRAM_ID
            )

        diagnostic = captured.exception.diagnostic
        self.assertIsNotNone(diagnostic)
        assert diagnostic is not None
        self.assertEqual(diagnostic["stage"], "account_lookup")
        self.assertEqual(diagnostic["failure_code"], "TELEGRAM_ACCOUNT_NOT_FOUND")
        self.assertEqual(transport.calls, [])

    async def test_telegram_connect_failure_has_specific_diagnostic(self) -> None:
        transport = FakeTransport()
        service = TonnelService(
            cast(
                MiniAppService,
                FailingMiniApps(MiniAppSessionConnectionError("safe wrapper")),
            ),
            cast(Database, FakeDatabase()),
            transport_factory=lambda: cast(TonnelHttpTransport, transport),
        )

        with self.assertRaises(TonnelPreflightFailure) as captured:
            await service.authenticate(1, owner_telegram_id=OWNER_TELEGRAM_ID)

        self.assertEqual(
            captured.exception.diagnostic["failure_code"],
            "TELEGRAM_CONNECT_FAILED",
        )
        self.assertEqual(captured.exception.diagnostic["stage"], "telegram_connection")
        self.assertEqual(transport.calls, [])

    async def test_sqlite_operational_error_has_safe_underlying_diagnostic(
        self,
    ) -> None:
        root = sqlite3.OperationalError("database is locked")
        wrapped = MiniAppSessionConnectionError("safe wrapper")
        wrapped.__cause__ = root
        service = TonnelService(
            cast(MiniAppService, FailingMiniApps(wrapped)),
            cast(Database, FakeDatabase()),
            transport_factory=lambda: cast(TonnelHttpTransport, FakeTransport()),
        )

        with self.assertRaises(TonnelPreflightFailure) as captured:
            await service.authenticate(1, owner_telegram_id=OWNER_TELEGRAM_ID)

        diagnostic = captured.exception.diagnostic
        self.assertEqual(diagnostic["exception_type"], "OperationalError")
        self.assertEqual(diagnostic["failure_kind"], "sqlite_session")
        self.assertEqual(
            diagnostic["underlying_exception_category"], "sqlite_operational"
        )
        self.assertEqual(
            diagnostic["underlying_exception_message"], "database is locked"
        )

    async def test_auth_http_error_has_specific_diagnostic(self) -> None:
        transport = FakeTransport()

        async def rejected_post(
            url: str, payload: dict[str, Any], *, mutation: bool = False
        ) -> TonnelHttpResponse:
            del url, payload, mutation
            return TonnelHttpResponse(401, None)

        transport.post_json = rejected_post  # type: ignore[method-assign]
        with self.assertRaises(TonnelAuthenticationError) as captured:
            await self.service(transport).authenticate(
                1, owner_telegram_id=OWNER_TELEGRAM_ID
            )

        diagnostic = captured.exception.diagnostic
        self.assertIsNotNone(diagnostic)
        assert diagnostic is not None
        self.assertEqual(diagnostic["http_status"], 401)
        self.assertEqual(diagnostic["failure_code"], "TONNEL_AUTH_HTTP_ERROR")

    async def test_inventory_preserves_exact_typed_identity(self) -> None:
        transport = FakeTransport()
        gifts = await self.service(transport).get_inventory(
            2, owner_telegram_id=OWNER_TELEGRAM_ID
        )
        self.assertEqual(len(gifts), 1)
        self.assertIs(type(gifts[0].gift_id), int)
        self.assertEqual(gifts[0].gift_id, 91)
        self.assertEqual(gifts[0].identity.collectible_slug, "LolPop-17")

    async def test_wrong_typed_gift_id_is_rejected_before_mutation(self) -> None:
        transport = FakeTransport()
        with self.assertRaises(TonnelGiftUnavailableError):
            await self.service(transport).transfer_exact_gift(
                owner_telegram_id=OWNER_TELEGRAM_ID,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelGiftIdentity("91", "LolPop-17"),
                dry_run=False,
            )
        self.assertEqual(sum(call[2] for call in transport.calls), 0)

    async def test_direct_dry_run_performs_zero_mutations(self) -> None:
        transport = FakeTransport()
        result = await self.service(transport).transfer_exact_gift(
            owner_telegram_id=OWNER_TELEGRAM_ID,
            source_account_id=2,
            destination_account_id=1,
            identity=TonnelGiftIdentity(91, "LolPop-17"),
            dry_run=True,
        )
        self.assertIs(result.status, TonnelTransferStatus.DRY_RUN)
        self.assertFalse(result.mutation_sent)
        self.assertEqual(sum(call[2] for call in transport.calls), 0)

    async def test_direct_transfer_mutates_once_and_verifies_owner(self) -> None:
        transport = FakeTransport()
        result = await self.service(transport).transfer_exact_gift(
            owner_telegram_id=OWNER_TELEGRAM_ID,
            source_account_id=2,
            destination_account_id=1,
            identity=TonnelGiftIdentity(91, "LolPop-17"),
            dry_run=False,
        )
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(sum(call[2] for call in transport.calls), 1)
        mutation = next(call for call in transport.calls if call[2])
        self.assertEqual(
            set(mutation[1]), {"authData", "gift_id", "receiver", "anonymous"}
        )
        self.assertEqual(mutation[1]["gift_id"], 91)
        self.assertEqual(mutation[1]["receiver"], 300)

    async def test_insufficient_fee_balance_stops_before_mutation(self) -> None:
        transport = FakeTransport()
        transport.balance = "0"
        with self.assertRaises(TonnelInsufficientBalanceError):
            await self.service(transport).transfer_exact_gift(
                owner_telegram_id=OWNER_TELEGRAM_ID,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelGiftIdentity(91, "LolPop-17"),
                dry_run=False,
            )
        self.assertEqual(sum(call[2] for call in transport.calls), 0)

    async def test_ambiguous_final_reads_do_not_retry_mutation(self) -> None:
        transport = FakeTransport()
        transport.inventory_error_after_mutation = True
        result = await self.service(transport).transfer_exact_gift(
            owner_telegram_id=OWNER_TELEGRAM_ID,
            source_account_id=2,
            destination_account_id=1,
            identity=TonnelGiftIdentity(91, "LolPop-17"),
            dry_run=False,
        )
        self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
        self.assertEqual(sum(call[2] for call in transport.calls), 1)

    def test_decimal_commission_and_strategy_priority(self) -> None:
        self.assertEqual(TonnelService.purchase_price("3.52"), Decimal("3.53760"))
        self.assertEqual(TonnelService.seller_proceeds("3.52"), Decimal("3.52"))
        self.assertIs(
            TonnelService.select_transfer_strategy(
                direct_recipient=True,
                private_reserved=True,
                listing_bound=True,
                offer=True,
                public_listing=True,
            ),
            TonnelTransferStrategy.DIRECT_RECIPIENT,
        )

    async def test_logs_do_not_expose_credentials(self) -> None:
        transport = FakeTransport()
        with self.assertLogs(
            "app.telegram_client.tonnel_service", level="INFO"
        ) as logs:
            await self.service(transport).get_balance(
                1, owner_telegram_id=OWNER_TELEGRAM_ID
            )
        output = "\n".join(logs.output)
        self.assertNotIn("secret-init", output)
        self.assertNotIn("secret-token", output)


if __name__ == "__main__":
    unittest.main()
