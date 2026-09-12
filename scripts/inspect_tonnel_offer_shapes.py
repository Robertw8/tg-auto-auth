from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import Mapping
from contextlib import AsyncExitStack
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import Config
from app.db import Database
from app.telegram_client import MiniAppService, SessionService, TonnelService
from app.telegram_client.tonnel_service import (
    TONNEL_BUY_OFFER_GET_MINE_PATH,
    TONNEL_BUY_OFFER_GET_PATH,
)

_MONEY_PARTS = ("amount", "price", "value", "proceed", "fee")
_TIME_PARTS = ("created", "updated", "responded", "timestamp", "expires")


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


def _field_paths(value: Any, prefix: str = "") -> list[str]:
    paths: list[str] = []
    if isinstance(value, Mapping):
        for key, nested in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            paths.append(path)
            paths.extend(_field_paths(nested, path))
    elif isinstance(value, list):
        for nested in value[:20]:
            paths.extend(_field_paths(nested, f"{prefix}[]"))
    return sorted(set(paths))


def _identity(value: Any, *, n1_id: int, n2_id: int) -> dict[str, object]:
    scalar = value if isinstance(value, (str, int)) and not isinstance(value, bool) else None
    return {
        "type": type(value).__name__,
        "fingerprint": (
            TonnelService.fingerprint(f"{type(value).__name__}:{value}")
            if scalar is not None
            else None
        ),
        "matches_n1": scalar is not None and str(scalar) == str(n1_id),
        "matches_n2": scalar is not None and str(scalar) == str(n2_id),
    }


def _safe_relevant_fields(
    value: Any,
    *,
    n1_id: int,
    n2_id: int,
    prefix: str = "",
) -> dict[str, object]:
    result: dict[str, object] = {}
    if not isinstance(value, Mapping):
        return result
    for raw_key, nested in value.items():
        key = str(raw_key)
        lowered = key.lower()
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(nested, Mapping):
            result.update(
                _safe_relevant_fields(
                    nested,
                    n1_id=n1_id,
                    n2_id=n2_id,
                    prefix=path,
                )
            )
            continue
        if isinstance(nested, list):
            for index, item in enumerate(nested[:20]):
                result.update(
                    _safe_relevant_fields(
                        item,
                        n1_id=n1_id,
                        n2_id=n2_id,
                        prefix=f"{path}[{index}]",
                    )
                )
            continue
        if "offer_id" in lowered:
            result[path] = {
                "type": type(nested).__name__,
                "fingerprint": (
                    TonnelService.fingerprint(
                        f"{type(nested).__name__}:{nested}"
                    )
                    if isinstance(nested, (str, int))
                    and not isinstance(nested, bool)
                    else None
                ),
            }
        elif lowered in {"gift_id", "sale_id"} or lowered.endswith(
            (".gift_id", ".sale_id")
        ):
            result[path] = {
                "type": type(nested).__name__,
                "value": nested
                if isinstance(nested, (str, int)) and not isinstance(nested, bool)
                else None,
            }
        elif any(part in lowered for part in _MONEY_PARTS):
            result[path] = {
                "type": type(nested).__name__,
                "value": nested
                if isinstance(nested, (str, int, float, Decimal))
                and not isinstance(nested, bool)
                else None,
            }
        elif lowered in {"asset", "status"}:
            result[path] = {
                "type": type(nested).__name__,
                "value": nested if isinstance(nested, str) else None,
            }
        elif "buyer" in lowered or "seller" in lowered:
            result[path] = _identity(nested, n1_id=n1_id, n2_id=n2_id)
        elif any(part in lowered for part in _TIME_PARTS):
            result[path] = {
                "type": type(nested).__name__,
                "value": nested
                if isinstance(nested, (str, int, float, Decimal))
                and not isinstance(nested, bool)
                else None,
            }
    return result


def _offer_objects(body: Any) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []

    def visit(value: Any, key: str | None = None) -> None:
        if isinstance(value, Mapping):
            for child_key, child in value.items():
                visit(child, str(child_key))
        elif isinstance(value, list):
            if key and "offer" in key.lower():
                found.extend(item for item in value if isinstance(item, Mapping))
            else:
                for item in value:
                    visit(item, key)

    visit(body)
    return found


def _sanitize_response(
    *,
    endpoint: str,
    status: int,
    body: Any,
    n1_id: int,
    n2_id: int,
) -> dict[str, object]:
    offers = _offer_objects(body)
    return {
        "endpoint": endpoint,
        "http_status": status,
        "body_type": type(body).__name__,
        "all_field_paths": _field_paths(body),
        "offer_count": len(offers),
        "offers": [
            {
                "all_field_paths": _field_paths(offer),
                "relevant_fields": _safe_relevant_fields(
                    offer, n1_id=n1_id, n2_id=n2_id
                ),
            }
            for offer in offers
        ],
    }


async def _run(args: argparse.Namespace) -> int:
    config = Config.load()
    database = Database(config.database_path)
    await database.initialize()
    n1_account, n2_account = await asyncio.gather(
        database.get_account(args.n1_account_id, args.control_user_id),
        database.get_account(args.n2_account_id, args.control_user_id),
    )
    if n1_account is None or n1_account.role != "OWNER":
        raise RuntimeError("N1 owner account was not found")
    if n2_account is None or n2_account.role != "TARGET":
        raise RuntimeError("N2 target account was not found")
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
        dry_run=True,
        transfer_mode="BUY_OFFER",
        api_origin=config.tonnel_api_origin,
    )
    n1 = None
    n2 = None
    n1_error: str | None = None
    n2_error: str | None = None
    try:
        n1 = await tonnel.authenticate(
            args.n1_account_id, owner_telegram_id=args.control_user_id
        )
    except Exception as exc:  # noqa: BLE001 - safe class-only read diagnostic
        n1_error = type(exc).__name__
    try:
        n2 = await tonnel.authenticate(
            args.n2_account_id, owner_telegram_id=args.control_user_id
        )
    except Exception as exc:  # noqa: BLE001 - safe class-only read diagnostic
        n2_error = type(exc).__name__
    if n1 is None and n2 is None:
        raise RuntimeError("Neither read-only Tonnel session is available")
    async with AsyncExitStack() as stack:
        if n1 is not None:
            stack.push_async_callback(n1.transport.close)
        if n2 is not None:
            stack.push_async_callback(n2.transport.close)
        gifts = (
            await tonnel._get_market_inventory(n2, listed=False)
            if n2 is not None
            else []
        )
        exact = [
            gift
            for gift in gifts
            if type(gift.gift_id) is type(args.gift_id)
            and gift.gift_id == args.gift_id
        ]
        selected_gift = exact[0] if len(exact) == 1 else None
        sale_id = selected_gift.sale_id if selected_gift is not None else args.gift_id
        n1_response = None
        n2_response = None
        if n1 is not None:
            n1_url = tonnel._buy_offer_read_url(
                n1.account.telegram_account_id, TONNEL_BUY_OFFER_GET_MINE_PATH
            )
            n1_response = await n1.transport.post_json(
                n1_url,
                {
                    "authData": n1.auth_data,
                    "pageSize": 50,
                    "filter": {},
                    "tillTime": tonnel._javascript_date_now(),
                },
            )
        if n2 is not None:
            n2_url = tonnel._buy_offer_read_url(
                n2.account.telegram_account_id, TONNEL_BUY_OFFER_GET_PATH
            )
            n2_response = await n2.transport.post_json(
                n2_url,
                {"authData": n2.auth_data, "gift_id": sale_id},
            )
    output = {
        "n2_selected_gift": {
            "currently_owned_by_n2": selected_gift is not None,
            "gift_id_type": type(args.gift_id).__name__,
            "gift_id_value": args.gift_id,
            "sale_id_type": type(sale_id).__name__,
            "seller_field_present": (
                selected_gift is not None
                and selected_gift.seller_telegram_id is not None
            ),
            "seller_matches_n2": (
                selected_gift.seller_telegram_id == n2_account.telegram_account_id
                if selected_gift is not None
                and selected_gift.seller_telegram_id is not None
                else None
            ),
        },
        "n1_get_my_offers": (
            _sanitize_response(
                endpoint=TONNEL_BUY_OFFER_GET_MINE_PATH,
                status=n1_response.status,
                body=n1_response.body,
                n1_id=n1_account.telegram_account_id,
                n2_id=n2_account.telegram_account_id,
            )
            if n1_response is not None
            else {"status": "UNAVAILABLE", "exception_class": n1_error}
        ),
        "n2_get_offers": (
            _sanitize_response(
                endpoint=TONNEL_BUY_OFFER_GET_PATH,
                status=n2_response.status,
                body=n2_response.body,
                n1_id=n1_account.telegram_account_id,
                n2_id=n2_account.telegram_account_id,
            )
            if n2_response is not None
            else {"status": "UNAVAILABLE", "exception_class": n2_error}
        ),
        "mutations_sent": 0,
    }
    print(
        json.dumps(
            output,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            default=lambda value: (
                format(value, "f") if isinstance(value, Decimal) else str(value)
            ),
        )
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect safe current Tonnel buy-offer response shapes"
    )
    parser.add_argument("--control-user-id", type=int, required=True)
    parser.add_argument("--n1-account-id", type=int, required=True)
    parser.add_argument("--n2-account-id", type=int, required=True)
    parser.add_argument("--gift-id", type=_opaque_id, required=True)
    try:
        exit_code = asyncio.run(_run(parser.parse_args()))
    except Exception as exc:  # noqa: BLE001 - class-only secret-free fallback
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "exception_class": type(exc).__name__,
                    "mutations_sent": 0,
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
