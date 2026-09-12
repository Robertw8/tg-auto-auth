"""Read-only Tonnel discovery using an existing authorized account."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import Config
from app.db import Database
from app.telegram_client import (
    MiniAppService,
    SessionService,
    TonnelService,
)


async def run(
    account_id: int,
    owner_telegram_id: int,
    destination_account_id: int | None,
    price_ton: str,
) -> int:
    config = Config.load()
    database = Database(config.database_path)
    sessions = SessionService(config.sessions_dir)
    miniapps = MiniAppService(
        config.telegram_api_id,
        config.telegram_api_hash,
        database,
        sessions,
    )
    tonnel = TonnelService(miniapps, database, dry_run=True, transfer_mode="BUY_OFFER")
    failed = False
    print("BUSINESS DIRECT")
    try:
        fee = await tonnel.get_direct_transfer_fee(
            account_id, owner_telegram_id=owner_telegram_id
        )
        business_gifts = await tonnel.get_inventory(
            account_id, owner_telegram_id=owner_telegram_id
        )
        print("Доступно: да")
        print(f"Управляемых подарков: {len(business_gifts)}")
        print(f"Комиссия прямой передачи: {fee} TON")
    except Exception as exc:  # noqa: BLE001 - developer tool prints sanitized type
        print("Доступно: нет")
        print(f"Причина: {type(exc).__name__}")

    print("\nORDINARY INVENTORY / BUY OFFER")
    try:
        balance = await tonnel.get_balance(
            account_id, owner_telegram_id=owner_telegram_id
        )
        market_gifts = await tonnel.get_market_inventory(
            account_id, owner_telegram_id=owner_telegram_id
        )
        sellable = [gift for gift in market_gifts if gift.eligible]
        print("Авторизация: успешно")
        print("Доступно: да")
        print(f"Обычных подарков: {len(market_gifts)}")
        print(f"Доступно для оффера: {len(sellable)}")
        print(f"Баланс TON: {balance.ton}")
        print("API точного оффера известен: да")
        print("Публичное размещение требуется: нет")
        print("Telegram Business требуется: нет")
        if sellable:
            sample = sellable[0]
            print(f"Пример: {sample.display_name}")
            print(f"Тип исходного ID: {type(sample.gift_id).__name__}")
            print(f"Тип sale_id: {type(sample.sale_id).__name__}")
            if destination_account_id is not None:
                offer_result = await tonnel.transfer_buy_offer(
                    owner_telegram_id=owner_telegram_id,
                    source_account_id=account_id,
                    destination_account_id=destination_account_id,
                    identity=sample.identity,
                    offer_amount=price_ton,
                    dry_run=True,
                )
                print(f"Результат BUY_OFFER dry-run: {offer_result.status.value}")
                print(f"Сумма оффера: {offer_result.quote.offer_amount} TON")
                print(
                    f"Ожидаемая выплата N2: "
                    f"{offer_result.quote.seller_proceeds} TON"
                )
                print(
                    f"Требуемый баланс N1: "
                    f"{offer_result.quote.required_buyer_balance} TON"
                )
                print(f"Баланс N1: {offer_result.quote.buyer_balance} TON")
                print("Отправлено мутаций: 0")
    except Exception as exc:  # noqa: BLE001 - developer tool prints sanitized type
        failed = True
        print("Доступно: нет")
        print(f"Причина: {type(exc).__name__}")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Tonnel Mini App capability discovery."
    )
    parser.add_argument("--account-id", type=int, required=True)
    parser.add_argument("--owner-telegram-id", type=int, required=True)
    parser.add_argument("--destination-account-id", type=int)
    parser.add_argument("--price-ton", default="5")
    arguments = parser.parse_args()
    return asyncio.run(
        run(
            arguments.account_id,
            arguments.owner_telegram_id,
            arguments.destination_account_id,
            arguments.price_ton,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
