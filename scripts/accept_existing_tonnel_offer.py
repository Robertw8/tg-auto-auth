from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import Config
from app.db import Database
from app.jobs import TonnelTransferJobService
from app.telegram_client import MiniAppService, SessionService, TonnelService
from app.telegram_client.tonnel_service import TONNEL_API_ORIGIN

_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{12}$")


def _opaque_id(value: str) -> str | int:
    try:
        parsed: Any = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(
            "gift ID must be a JSON string or integer"
        ) from exc
    if isinstance(parsed, bool) or not isinstance(parsed, (str, int)):
        raise argparse.ArgumentTypeError("gift ID must be a JSON string or integer")
    return parsed


def _fingerprint(value: str) -> str:
    normalized = value.strip().lower()
    if not _FINGERPRINT_RE.fullmatch(normalized):
        raise argparse.ArgumentTypeError(
            "expected offer fingerprint must be 12 lowercase hexadecimal characters"
        )
    return normalized


async def _run(args: argparse.Namespace) -> int:
    config = Config.load()
    if config.tonnel_api_origin != TONNEL_API_ORIGIN:
        raise RuntimeError(
            "TONNEL_API_ORIGIN must be https://gifts.coffin.meme for this diagnostic"
        )
    database = Database(config.database_path)
    await database.initialize()
    sessions = SessionService(config.sessions_dir)
    miniapps = MiniAppService(
        config.telegram_api_id,
        config.telegram_api_hash,
        database,
        sessions,
    )
    tonnel = TonnelService(
        miniapps,
        database,
        dry_run=not args.confirm,
        transfer_mode="BUY_OFFER",
        api_origin=TONNEL_API_ORIGIN,
        offer_accept_delay_ms=0,
    )
    jobs = TonnelTransferJobService(
        database,
        tonnel,
        dry_run=not args.confirm,
        transfer_mode="BUY_OFFER",
    )
    job = await jobs.execute_existing_offer_diagnostic(
        owner_telegram_id=args.control_user_id,
        owner_account_id=args.n1_account_id,
        target_account_id=args.n2_account_id,
        gift_id=args.gift_id,
        offer_amount=args.amount,
        expected_offer_fingerprint=args.expected_offer_fingerprint,
        confirm=args.confirm,
    )
    metadata = job.result_metadata
    print(
        json.dumps(
            {
                "job_id": job.id,
                "mode": "CONFIRM" if args.confirm else "READ_ONLY",
                "status": job.status,
                "error_code": job.error_code,
                "message": job.error_message,
                "strategy": metadata.get("strategy"),
                "candidate_count": metadata.get("candidate_count"),
                "offer_age_ms": metadata.get("offer_age_ms"),
                "offer_id_type": metadata.get("offer_id_type"),
                "offer_id_fingerprint": metadata.get("offer_id_fingerprint"),
                "gift_match": metadata.get("gift_match"),
                "amount_match": metadata.get("amount_match"),
                "asset_match": metadata.get("asset_match"),
                "buyer_match": metadata.get("buyer_match"),
                "seller_match": metadata.get("seller_match"),
                "match_diagnostics": metadata.get("match_diagnostics"),
                "offer_status": metadata.get("status"),
                "accept_result": metadata.get("tonnel_offer_accept_result"),
                "mutation_counts": metadata.get("mutation_counts"),
                "source_owns_after": metadata.get("source_owns_after"),
                "destination_owns_after": metadata.get(
                    "destination_owns_after"
                ),
                "offer_active_after": metadata.get("offer_active_after"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if job.status in {"DRY_RUN", "SUCCESS"} else 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Validate one existing Tonnel BUY_OFFER and optionally accept it once"
        )
    )
    parser.add_argument("--control-user-id", type=int, required=True)
    parser.add_argument("--n1-account-id", type=int, required=True)
    parser.add_argument("--n2-account-id", type=int, required=True)
    parser.add_argument("--gift-id", type=_opaque_id, required=True)
    parser.add_argument("--amount", required=True)
    parser.add_argument(
        "--expected-offer-fingerprint", type=_fingerprint, required=True
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="send at most one ACCEPT after every exact validation succeeds",
    )
    try:
        exit_code = asyncio.run(_run(parser.parse_args()))
    except Exception as exc:  # noqa: BLE001 - class-only secret-free fallback
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "exception_class": type(exc).__name__,
                    "mutation_counts": {
                        "tonnel_offer_create": 0,
                        "tonnel_offer_accept": 0,
                    },
                },
                sort_keys=True,
            )
        )
        raise SystemExit(1) from None
    raise SystemExit(exit_code)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("telethon").setLevel(logging.CRITICAL)
    main()
