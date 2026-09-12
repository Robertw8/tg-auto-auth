from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print read-only MRKT listing activation diagnostics."
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=PROJECT_ROOT / "data" / "app.db",
    )
    parser.add_argument("--limit", type=int, default=10)
    return parser.parse_args()


def number(value: object) -> str:
    return f"{value:g}" if isinstance(value, (int, float)) else "—"


def metadata_object(value: object) -> dict[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def main() -> None:
    args = parse_args()
    if not 1 <= args.limit <= 100:
        raise SystemExit("--limit must be between 1 and 100")
    database = args.database.resolve()
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT id, display_name, result_metadata
            FROM transfer_jobs
            WHERE market = 'mrkt'
            ORDER BY id DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
    finally:
        connection.close()

    print(
        "job | gift | mode | sale | probes | listed | buy | confirmed->buy | "
        "buy RTT | seller price | buyer price | observed price | price match | outcome"
    )
    for row in rows:
        metadata = metadata_object(row["result_metadata"])
        trigger = metadata.get("mrkt_listed_trigger")
        if not isinstance(trigger, dict):
            trigger = {}
        final_read = metadata.get("live_verify", {})
        if isinstance(final_read, dict):
            final_read = final_read.get("final_read", {})
        if not isinstance(final_read, dict):
            final_read = {}
        price_correlation = trigger.get("mrkt_price_correlation", {})
        if not isinstance(price_correlation, dict):
            price_correlation = {}
        print(
            f"#{row['id']} | {row['display_name'] or '—'} | "
            f"{metadata.get('transfer_mode', '—')} | "
            f"{number(metadata.get('mrkt_sale_start_to_sale_response_ms'))}ms | "
            f"{number(trigger.get('probe_count'))} | "
            f"{number(trigger.get('listing_confirmed_ms'))}ms | "
            f"{number(trigger.get('buy_started_ms'))}ms | "
            f"{number(trigger.get('confirm_to_buy_start_ms'))}ms | "
            f"{number(metadata.get('mrkt_buy_start_to_buy_response_ms'))}ms | "
            f"{number(metadata.get('seller_price_nanotons'))} | "
            f"{number(metadata.get('buyer_price_nanotons'))} | "
            f"{number(price_correlation.get('observed_listing_price_nanotons'))} | "
            f"{price_correlation.get('exact_price_match', '—')} | "
            f"{final_read.get('outcome', '—')}"
        )


if __name__ == "__main__":
    main()
