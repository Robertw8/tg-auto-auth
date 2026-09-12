from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import Database
from app.telegram_client import MiniAppService, MrktService, SessionService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure safe authenticated MRKT inventory latency."
    )
    parser.add_argument("--account-id", type=int, required=True)
    parser.add_argument("--owner-telegram-id", type=int, required=True)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--spacing-ms", type=int, default=250)
    return parser.parse_args()


def load_api_credentials() -> tuple[int, str]:
    load_dotenv(PROJECT_ROOT / ".env")
    raw_api_id = os.getenv("TELEGRAM_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
    if not raw_api_id or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")
    try:
        api_id = int(raw_api_id)
    except ValueError as exc:
        raise RuntimeError("TELEGRAM_API_ID must be an integer") from exc
    if api_id <= 0:
        raise RuntimeError("TELEGRAM_API_ID must be positive")
    return api_id, api_hash


def percentile_95(samples: tuple[float, ...]) -> float:
    if len(samples) == 1:
        return samples[0]
    return statistics.quantiles(samples, n=100, method="inclusive")[94]


async def run() -> None:
    args = parse_args()
    if args.account_id <= 0 or args.owner_telegram_id <= 0:
        raise RuntimeError("Account and owner IDs must be positive")
    api_id, api_hash = load_api_credentials()
    database = Database(PROJECT_ROOT / "data" / "app.db")
    sessions = SessionService(PROJECT_ROOT / "data" / "sessions")
    miniapp = MiniAppService(api_id, api_hash, database, sessions)
    result = await MrktService(miniapp).benchmark_read_latency(
        args.account_id,
        owner_telegram_id=args.owner_telegram_id,
        samples=args.samples,
        spacing_ms=args.spacing_ms,
    )
    values = result.samples_ms
    print("MRKT latency benchmark")
    print(f"auth/connect/TLS preparation: {result.auth_and_connection_ms:.3f} ms")
    print(f"connection reused: {result.connection_reused}")
    print(f"samples: {result.sample_count}")
    print(f"min: {min(values):.3f} ms")
    print(f"p50: {statistics.median(values):.3f} ms")
    print(f"p95: {percentile_95(values):.3f} ms")
    print(f"max: {max(values):.3f} ms")


if __name__ == "__main__":
    asyncio.run(run())
