from __future__ import annotations

import json
import unittest
from decimal import Decimal
from typing import Any, cast

from app.db import Account, Database
from app.telegram_client import (
    MiniAppLaunchResult,
    MiniAppService,
    TonnelGiftUnavailableError,
    TonnelHttpResponse,
    TonnelHttpTransport,
    TonnelInventoryUnavailableError,
    TonnelMarketGiftIdentity,
    TonnelMutationNetworkError,
    TonnelService,
    TonnelTransferStatus,
)
from app.telegram_client.tonnel_service import _exact_json_dumps, _exact_json_loads

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


def raw_market_gift(*, listed: bool = False, seller: int = 302) -> dict[str, Any]:
    item: dict[str, Any] = {
        "gift_id": 91,
        "gift_num": 17,
        "name": "Lol Pop",
        "model": "Blue (1%)",
        "symbol": "Star (1%)",
        "backdrop": "Navy (1%)",
        "seller": seller,
        "asset": "TON",
        "status": "forsale" if listed else "owned",
    }
    if listed:
        item["price"] = "5"
    return item


class FakeMarketTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], bool]] = []
        self.listed = False
        self.bought = False
        self.external_sale = False
        self.external_sold = False
        self.list_ambiguous = False
        self.buy_ambiguous = False
        self.list_response = TonnelHttpResponse(
            200, {"status": "success", "message": "listed"}
        )
        self.buy_response = TonnelHttpResponse(
            200, {"status": "success", "message": "bought"}
        )
        self.owner_balance = "100"
        self.delivery_to_inventory = True
        self.closed = False

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
        if url.endswith("/api/balance/info"):
            is_owner = str(payload["authData"]).endswith("-1")
            return TonnelHttpResponse(
                200,
                {
                    "status": "success",
                    "balance": self.owner_balance if is_owner else "0",
                    "tonnelBalance": "0",
                    "usdtBalance": "0",
                    "transferGift": False,
                },
            )
        if url.endswith("/api/pageGifts"):
            filters = json.loads(str(payload["filter"]))
            listed_query = filters.get("price") == {"$exists": True}
            is_owner = str(payload["user_auth"]).endswith("-1")
            items: list[dict[str, Any]] = []
            if listed_query and not is_owner and self.listed and not self.bought:
                items = [raw_market_gift(listed=True)]
            elif not listed_query:
                if (
                    is_owner
                    and self.bought
                    and not self.external_sale
                    and self.delivery_to_inventory
                ):
                    items = [raw_market_gift(seller=300)]
                elif (
                    not is_owner
                    and not self.listed
                    and not self.bought
                    and not self.external_sold
                ):
                    items = [raw_market_gift()]
            return TonnelHttpResponse(200, items)
        if url.endswith("/api/fetchMangedGifts"):
            return TonnelHttpResponse(
                200,
                {"status": "error", "message": "Business connection not found"},
            )
        if "/api/giftData/" in url:
            if self.bought and not self.external_sale:
                sold = raw_market_gift(listed=True)
                sold["buyer"] = 300
                sold["status"] = "sold"
                return TonnelHttpResponse(200, sold)
            return TonnelHttpResponse(
                200,
                raw_market_gift(listed=True)
                if self.listed and not self.bought
                else {"status": "error", "message": "not found"},
            )
        if url.endswith("/api/listForSale"):
            self.listed = True
            if self.list_ambiguous:
                raise TonnelMutationNetworkError("ambiguous")
            return self.list_response
        if "/api/buyGift/" in url:
            if self.external_sale:
                self.listed = False
                self.external_sold = True
            elif self.buy_response.body == {"status": "success", "message": "bought"}:
                self.bought = True
                self.listed = False
            if self.buy_ambiguous:
                raise TonnelMutationNetworkError("ambiguous")
            return self.buy_response
        raise AssertionError(f"unexpected URL {url}")

    async def close(self) -> None:
        self.closed = True


class TonnelMarketTests(unittest.IsolatedAsyncioTestCase):
    def service(self, transport: FakeMarketTransport) -> TonnelService:
        return TonnelService(
            cast(MiniAppService, FakeMiniApps()),
            cast(Database, FakeDatabase()),
            transfer_mode="MARKET_SALE",
            transport_factory=lambda: cast(TonnelHttpTransport, transport),
        )

    async def transfer(
        self, transport: FakeMarketTransport, *, dry_run: bool = False
    ) -> Any:
        return await self.service(transport).transfer_market_sale(
            owner_telegram_id=CONTROL_USER,
            source_account_id=2,
            destination_account_id=1,
            identity=TonnelMarketGiftIdentity(91, "91", "LolPop-17"),
            seller_price="5",
            dry_run=dry_run,
        )

    async def test_standard_inventory_works_without_business(self) -> None:
        transport = FakeMarketTransport()
        gifts = await self.service(transport).get_market_inventory(
            2, owner_telegram_id=CONTROL_USER
        )
        self.assertEqual(len(gifts), 1)
        self.assertEqual(gifts[0].gift_id, 91)
        self.assertEqual(gifts[0].sale_id, "91")
        self.assertFalse(any("fetchMangedGifts" in call[0] for call in transport.calls))

    async def test_missing_business_only_blocks_direct_inventory(self) -> None:
        transport = FakeMarketTransport()
        with self.assertRaisesRegex(
            TonnelInventoryUnavailableError, "Список подарков Tonnel недоступен"
        ):
            await self.service(transport).get_inventory(
                2, owner_telegram_id=CONTROL_USER
            )
        gifts = await self.service(transport).get_market_inventory(
            2, owner_telegram_id=CONTROL_USER
        )
        self.assertEqual(len(gifts), 1)

    async def test_exact_typed_gift_selection(self) -> None:
        transport = FakeMarketTransport()
        with self.assertRaises(TonnelGiftUnavailableError):
            await self.service(transport).transfer_market_sale(
                owner_telegram_id=CONTROL_USER,
                source_account_id=2,
                destination_account_id=1,
                identity=TonnelMarketGiftIdentity("91", "91", "LolPop-17"),
                seller_price="5",
                dry_run=False,
            )
        self.assertFalse(any(call[2] for call in transport.calls))

    async def test_list_success_uses_preexisting_exact_sale_id_for_buy(self) -> None:
        transport = FakeMarketTransport()
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        mutations = [call for call in transport.calls if call[2]]
        self.assertEqual(len(mutations), 2)
        self.assertTrue(mutations[0][0].endswith("/api/listForSale"))
        self.assertTrue(mutations[1][0].endswith("/api/buyGift/91"))
        self.assertEqual(mutations[0][1]["gift_id"], "91")
        self.assertEqual(mutations[1][1]["price"], Decimal(5))

    async def test_redundant_matching_sale_id_is_accepted(self) -> None:
        transport = FakeMarketTransport()
        transport.list_response = TonnelHttpResponse(
            200, {"status": "success", "data": {"sale_id": 91}}
        )
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(sum("/api/buyGift/" in call[0] for call in transport.calls), 1)

    async def test_mismatched_returned_sale_id_stops_before_buy(self) -> None:
        transport = FakeMarketTransport()
        transport.list_response = TonnelHttpResponse(
            200, {"status": "success", "sale_id": 92}
        )
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.AMBIGUOUS)
        self.assertEqual(result.error_code, "LISTING_ID_MISMATCH")
        self.assertFalse(any("/api/buyGift/" in call[0] for call in transport.calls))

    async def test_no_public_lookup_between_successful_list_and_buy(self) -> None:
        transport = FakeMarketTransport()
        await self.transfer(transport)
        list_index = next(
            index
            for index, call in enumerate(transport.calls)
            if call[0].endswith("/api/listForSale")
        )
        buy_index = next(
            index for index, call in enumerate(transport.calls) if "/api/buyGift/" in call[0]
        )
        self.assertEqual(buy_index, list_index + 1)

    async def test_sale_id_to_buy_gap_is_measured(self) -> None:
        result = await self.transfer(FakeMarketTransport())
        gap = result.timings_ms["sale_id_known_to_buy_start_ms"]
        self.assertIsInstance(gap, int)
        self.assertLessEqual(cast(int, gap), 5)

    async def test_ambiguous_listing_is_reconciled_without_second_listing(self) -> None:
        transport = FakeMarketTransport()
        transport.list_ambiguous = True
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(
            sum(call[0].endswith("/api/listForSale") for call in transport.calls), 1
        )
        self.assertTrue(any("/api/giftData/91" in call[0] for call in transport.calls))

    async def test_ambiguous_buy_is_not_retried_and_uses_final_reads(self) -> None:
        transport = FakeMarketTransport()
        transport.buy_ambiguous = True
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertEqual(sum("/api/buyGift/" in call[0] for call in transport.calls), 1)

    async def test_exact_sale_buyer_is_final_ownership_evidence(self) -> None:
        transport = FakeMarketTransport()
        transport.delivery_to_inventory = False
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.SUCCESS)
        self.assertFalse(result.source_owns_after)
        self.assertTrue(result.destination_owns_after)

    async def test_external_buyer_is_not_success(self) -> None:
        transport = FakeMarketTransport()
        transport.external_sale = True
        transport.buy_response = TonnelHttpResponse(
            200, {"status": "error", "message": "already sold"}
        )
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.error_code, "EXTERNAL_SALE")

    async def test_listing_remaining_is_buy_failed(self) -> None:
        transport = FakeMarketTransport()
        transport.buy_response = TonnelHttpResponse(
            200, {"status": "error", "message": "not purchased"}
        )
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.error_code, "BUY_FAILED")

    async def test_insufficient_buyer_balance_prevents_listing(self) -> None:
        transport = FakeMarketTransport()
        transport.owner_balance = "5"
        result = await self.transfer(transport)
        self.assertIs(result.status, TonnelTransferStatus.FAILED)
        self.assertEqual(result.error_code, "INSUFFICIENT_BALANCE")
        self.assertFalse(result.balance_sufficient)
        self.assertFalse(any(call[2] for call in transport.calls))

    async def test_dry_run_has_zero_mutations(self) -> None:
        transport = FakeMarketTransport()
        result = await self.transfer(transport, dry_run=True)
        self.assertIs(result.status, TonnelTransferStatus.DRY_RUN)
        self.assertFalse(any(call[2] for call in transport.calls))

    def test_exact_decimal_price_and_commission(self) -> None:
        quote = TonnelService.market_quote("5", "0")
        self.assertEqual(quote.seller_price, Decimal(5))
        self.assertEqual(quote.buyer_price, Decimal("5.025"))
        self.assertEqual(quote.seller_proceeds, Decimal(5))
        self.assertEqual(quote.seller_price_nanotons, 5_000_000_000)
        self.assertEqual(quote.buyer_price_nanotons, 5_025_000_000)
        discounted = TonnelService.market_quote("5", "669")
        self.assertEqual(discounted.buyer_price, Decimal("5.0225"))

    def test_market_price_validation(self) -> None:
        for invalid in ("0", "-1", "0.49", "1.2345", "nan", "20000.001"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                TonnelService.normalize_market_price(invalid)

    def test_exact_json_money_never_uses_float(self) -> None:
        encoded = _exact_json_dumps({"price": Decimal("3.520"), "count": 1})
        self.assertEqual(encoded, '{"price":3.520,"count":1}')
        decoded = _exact_json_loads('{"price":3.520}')
        self.assertEqual(decoded["price"], Decimal("3.520"))
        self.assertIs(type(decoded["price"]), Decimal)

    def test_integer_prices_keep_trailing_zeroes(self) -> None:
        self.assertEqual(TonnelService.format_market_price("10"), "10")


if __name__ == "__main__":
    unittest.main()
