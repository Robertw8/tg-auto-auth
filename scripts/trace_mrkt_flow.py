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
    MrktDiscoveryService,
    SessionService,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the sanitized MRKT auth/inventory discovery probe. "
            "No listing endpoint is called."
        )
    )
    parser.add_argument("--account-id", type=int, required=True)
    parser.add_argument("--owner-telegram-id", type=int, required=True)
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
    sessions = SessionService(PROJECT_ROOT / "data" / "sessions")
    launch = await MiniAppService(
        api_id,
        api_hash,
        database,
        sessions,
    ).open_miniapp(
        args.account_id,
        "@mrkt",
        owner_telegram_id=args.owner_telegram_id,
    )
    result = await MrktDiscoveryService().inspect(launch)

    print(f"frontend origin: {result.frontend_origin}")
    print(f"API origin: {result.api_origin}")
    print(f"auth token obtained: {result.auth_token_obtained}")
    print(f"cookie names: {', '.join(result.cookie_names) or 'none'}")
    print(f"cookie attributes: {', '.join(result.cookie_attribute_names) or 'none'}")
    for trace in result.traces:
        print(
            f"{trace.method} {trace.hostname}{trace.path} -> {trace.status}; "
            f"request fields={','.join(trace.request_fields)}; "
            f"response fields={','.join(trace.response_fields)}"
        )
    print(f"inventory item count: {result.inventory_item_count}")
    print(f"inventory item fields: {','.join(result.inventory_item_fields) or 'none'}")
    print(f"inventory item id kind: {result.inventory_item_id_kind or 'unknown'}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(run())
