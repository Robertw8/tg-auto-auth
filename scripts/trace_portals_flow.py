from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import Database
from app.telegram_client import (
    MiniAppService,
    PortalsDiscoveryService,
    SessionService,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run sanitized, read-only Portals auth/inventory/offer discovery. "
            "The accept endpoint is never called."
        )
    )
    parser.add_argument("--account-id", type=int, required=True)
    parser.add_argument("--owner-telegram-id", type=int, required=True)
    parser.add_argument(
        "--frontend-bundle",
        type=Path,
        help="Optional local Portals JavaScript bundle for static contract checks.",
    )
    return parser.parse_args()


def load_api_credentials() -> tuple[int, str]:
    load_dotenv(PROJECT_ROOT / ".env")
    api_id_value = os.getenv("TELEGRAM_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
    if not api_id_value or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")
    try:
        api_id = int(api_id_value)
    except ValueError as exc:
        raise RuntimeError("TELEGRAM_API_ID must be an integer") from exc
    if api_id <= 0:
        raise RuntimeError("TELEGRAM_API_ID must be positive")
    return api_id, api_hash


async def run() -> None:
    args = parse_args()
    if args.account_id <= 0 or args.owner_telegram_id <= 0:
        raise RuntimeError("Account and owner IDs must be positive")

    api_id, api_hash = load_api_credentials()
    database = Database(PROJECT_ROOT / "data" / "app.db")
    await database.initialize()
    launch = await MiniAppService(
        api_id,
        api_hash,
        database,
        SessionService(PROJECT_ROOT / "data" / "sessions"),
    ).open_miniapp(
        args.account_id,
        "@portals",
        owner_telegram_id=args.owner_telegram_id,
    )
    discovery = PortalsDiscoveryService()
    result = await discovery.inspect(launch)

    print(f"frontend origin: {result.frontend_origin}")
    print(f"API origins: {', '.join(result.api_origins)}")
    print(f"authorization scheme: {result.authorization_scheme}")
    print(f"initData sent directly: {result.init_data_sent_directly}")
    print(f"auth response fields: {','.join(result.auth_response_fields)}")
    print(f"auth token field present: {result.auth_response_token_present}")
    print(f"cookie names: {', '.join(result.cookie_names) or 'none'}")
    print(f"cookie attributes: {', '.join(result.cookie_attribute_names) or 'none'}")
    for trace in result.traces:
        print(
            f"{trace.method} {trace.hostname}{trace.path} -> {trace.status}; "
            f"query fields={','.join(trace.query_fields)}; "
            f"response fields={','.join(trace.response_fields)}"
        )
    print(f"inventory item count: {result.inventory_item_count}")
    print(f"inventory item fields: {','.join(result.inventory_item_fields) or 'none'}")
    print(f"inventory item ID kind: {result.inventory_item_id_kind or 'unknown'}")
    print(f"received item count: {result.received_item_count}")
    print(f"received item fields: {','.join(result.received_item_fields) or 'none'}")
    print(f"offer ID kind: {result.offer_id_kind or 'unknown'}")
    print(f"NFT ID kind: {result.nft_id_kind or 'unknown'}")
    print(f"deal view requested: {result.deal_view_requested}")

    if args.frontend_bundle is not None:
        source = args.frontend_bundle.read_text(encoding="utf-8")
        contract = discovery.inspect_frontend_source(source)
        source = ""
        print(f"detected API origins: {', '.join(contract.api_origins)}")
        print(f"auth path detected: {contract.auth_path_detected}")
        print(f"inventory path detected: {contract.inventory_path_detected}")
        print(
            f"received offers path detected: {contract.received_offers_path_detected}"
        )
        print(f"deal view path detected: {contract.nft_offers_path_detected}")
        print(f"accept path detected: {contract.accept_path_detected}")
        print(
            "accept payload fields: "
            f"{','.join(contract.accept_payload_fields) or 'unknown'}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("telethon").setLevel(logging.CRITICAL)
    asyncio.run(run())
