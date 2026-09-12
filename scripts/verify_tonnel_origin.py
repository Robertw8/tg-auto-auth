from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from telethon import TelegramClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import Config
from app.db import Database
from app.telegram_client import (
    MiniAppService,
    SessionService,
    TonnelAuthenticationError,
    TonnelBuyOffer,
    TonnelHttpResponse,
    TonnelPreflightFailure,
    TonnelService,
)
from app.telegram_client.tonnel_service import _AiohttpTonnelTransport

logger = logging.getLogger(__name__)
_SAFE_RESPONSE_TEXT = re.compile(r"^[A-Za-zА-Яа-яЁё0-9 .,!?_():-]{1,160}$")


def _check_readable(path: Path) -> None:
    with path.open("rb"):
        pass


def _safe_response_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    return value if _SAFE_RESPONSE_TEXT.fullmatch(value) else None


class _ReadOnlyObservingTransport:
    def __init__(self, observations: list[dict[str, object]]) -> None:
        self._inner = _AiohttpTonnelTransport()
        self._observations = observations

    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        mutation: bool = False,
    ) -> TonnelHttpResponse:
        if mutation:
            raise RuntimeError("read-only diagnostic blocked a mutation")
        started = time.monotonic()
        parsed = urlsplit(url)
        try:
            response = await self._inner.post_json(url, payload)
        except Exception as exc:
            self._observations.append(
                {
                    "url": f"{parsed.scheme}://{parsed.netloc}{parsed.path}",
                    "hostname": parsed.hostname or "unknown",
                    "endpoint": parsed.path,
                    "method": "POST",
                    "duration_ms": round((time.monotonic() - started) * 1000),
                    "http_status": None,
                    "exception_class": type(exc).__name__,
                }
            )
            raise
        observation: dict[str, object] = {
            "url": f"{parsed.scheme}://{parsed.netloc}{parsed.path}",
            "hostname": parsed.hostname or "unknown",
            "endpoint": parsed.path,
            "method": "POST",
            "duration_ms": round((time.monotonic() - started) * 1000),
            "http_status": response.status,
            "body_type": (
                "list"
                if isinstance(response.body, list)
                else "mapping"
                if isinstance(response.body, Mapping)
                else type(response.body).__name__
            ),
        }
        if isinstance(response.body, Mapping):
            observation["response_field_names"] = sorted(
                str(key) for key in response.body
            )
        if parsed.path == "/api/auth/telegram/miniapp/session" and isinstance(
            response.body, Mapping
        ):
            status = _safe_response_text(response.body.get("status"))
            message = _safe_response_text(response.body.get("message"))
            user = response.body.get("user")
            if status is not None:
                observation["application_status"] = status
            if message is not None:
                observation["application_message"] = message
            if isinstance(user, Mapping):
                observation["user_field_names"] = sorted(str(key) for key in user)
                user_id = user.get("id")
                if isinstance(user_id, int) and not isinstance(user_id, bool):
                    observation["returned_telegram_user_id"] = user_id
        self._observations.append(observation)
        return response

    async def close(self) -> None:
        await self._inner.close()


class _SafeDiagnosticFailure(RuntimeError):
    def __init__(self, diagnostic: Mapping[str, object]) -> None:
        self.diagnostic = dict(diagnostic)
        super().__init__("Safe read-only diagnostic failure")


def _opaque_id(value: str) -> str | int:
    parsed: Any = json.loads(value)
    if isinstance(parsed, bool) or not isinstance(parsed, (str, int)):
        raise argparse.ArgumentTypeError("gift ID must be a JSON string or integer")
    return parsed


async def _safe_check(
    name: str,
    action: Callable[[], Awaitable[dict[str, object]]],
) -> dict[str, object]:
    started = time.monotonic()
    try:
        details = await action()
    except TonnelPreflightFailure as exc:
        return {
            "check": name,
            "result": "FAIL",
            "duration_ms": round((time.monotonic() - started) * 1000),
            "error": dict(exc.diagnostic),
        }
    except TonnelAuthenticationError as exc:
        diagnostic = exc.diagnostic or {
            "stage": "tonnel_authentication",
            "exception_type": type(exc).__name__,
            "failure_kind": "application",
            "failure_code": "TONNEL_AUTH_APPLICATION_ERROR",
            "message": "Tonnel authentication failed.",
        }
        return {
            "check": name,
            "result": "FAIL",
            "duration_ms": round((time.monotonic() - started) * 1000),
            "error": dict(diagnostic),
        }
    except _SafeDiagnosticFailure as exc:
        return {
            "check": name,
            "result": "FAIL",
            "duration_ms": round((time.monotonic() - started) * 1000),
            "error": dict(exc.diagnostic),
        }
    except Exception as exc:  # noqa: BLE001 - class-only fallback avoids secret text
        return {
            "check": name,
            "result": "FAIL",
            "duration_ms": round((time.monotonic() - started) * 1000),
            "error": {
                "exception_type": type(exc).__name__,
                "message": "Read-only diagnostic failed before completion.",
                "failure_kind": "unknown",
            },
        }
    return {
        "check": name,
        "result": "PASS",
        "duration_ms": round((time.monotonic() - started) * 1000),
        **details,
    }


async def _run(args: argparse.Namespace) -> int:
    config = Config.load()
    if config.tonnel_transfer_mode != "BUY_OFFER":
        raise RuntimeError("TONNEL_TRANSFER_MODE must be BUY_OFFER")

    database = Database(config.database_path)
    await database.initialize()
    sessions = SessionService(config.sessions_dir)
    miniapps = MiniAppService(
        config.telegram_api_id,
        config.telegram_api_hash,
        database,
        sessions,
    )
    observations: list[dict[str, object]] = []
    tonnel = TonnelService(
        miniapps,
        database,
        dry_run=True,
        transfer_mode="BUY_OFFER",
        api_origin=config.tonnel_api_origin,
        transport_factory=lambda: _ReadOnlyObservingTransport(observations),
    )
    gift_offer_id: str | int = args.gift_id

    async def account_identity(account_id: int) -> int | None:
        item = await database.get_account(account_id, args.control_user_id)
        return item.telegram_account_id if item is not None else None

    async def safe_offers(
        offers: list[TonnelBuyOffer], *, observed_via: str
    ) -> list[dict[str, object]]:
        buyer_id, seller_id = await asyncio.gather(
            account_identity(args.n1_account_id),
            account_identity(args.n2_account_id),
        )
        return [
            {
                "offer_id_fingerprint": TonnelService.fingerprint(
                    f"{type(offer.offer_id).__name__}:{offer.offer_id}"
                ),
                "offer_id_type": type(offer.offer_id).__name__,
                "gift_id_type": type(offer.gift_id).__name__,
                "gift_id_matches": str(offer.gift_id) == str(gift_offer_id),
                "amount": format(offer.amount, "f"),
                "asset": offer.asset,
                "status": offer.status,
                "buyer_identity_match": (
                    offer.buyer_telegram_id == buyer_id
                    if offer.buyer_telegram_id is not None and buyer_id is not None
                    else None
                ),
                "seller_identity_match": (
                    offer.seller_telegram_id == seller_id
                    if offer.seller_telegram_id is not None and seller_id is not None
                    else None
                ),
                "created_at": offer.created_at,
                "observed_via": observed_via,
            }
            for offer in offers
            if str(offer.gift_id) == str(gift_offer_id)
        ]

    async def telegram_session(account_id: int) -> dict[str, object]:
        account = await database.get_account(account_id, args.control_user_id)
        if account is None:
            raise _SafeDiagnosticFailure(
                {
                    "stage": "account_lookup",
                    "account_id": account_id,
                    "exception_type": "TonnelAuthenticationError",
                    "failure_kind": "local_state",
                    "failure_code": "TELEGRAM_ACCOUNT_NOT_FOUND",
                    "safe_message": "Account record is missing or stale.",
                }
            )
        session_base = sessions.path_for(account.session_key)
        session_file = Path(f"{session_base}.session")
        if not session_file.is_file():
            raise _SafeDiagnosticFailure(
                {
                    "stage": "telegram_session",
                    "account_id": account_id,
                    "session_exists": False,
                    "exception_type": "MiniAppSessionMissingError",
                    "failure_kind": "local_state",
                    "failure_code": "TELEGRAM_SESSION_MISSING",
                    "safe_message": "Saved Telegram session file is missing.",
                }
            )
        try:
            await asyncio.to_thread(_check_readable, session_file)
        except OSError as exc:
            raise _SafeDiagnosticFailure(
                {
                    "stage": "telegram_session",
                    "account_id": account_id,
                    "session_exists": True,
                    "session_opened": False,
                    "exception_type": type(exc).__name__,
                    "failure_kind": "local_io",
                    "failure_code": "TELEGRAM_SESSION_MISSING",
                    "safe_message": "Saved Telegram session file cannot be opened.",
                }
            ) from exc

        client = TelegramClient(
            str(session_base), config.telegram_api_id, config.telegram_api_hash
        )
        connected = False
        try:
            try:
                await client.connect()
                connected = True
            except Exception as exc:
                raise _SafeDiagnosticFailure(
                    {
                        "stage": "telegram_connection",
                        "account_id": account_id,
                        "session_exists": True,
                        "session_opened": True,
                        "telegram_connected": False,
                        "exception_type": type(exc).__name__,
                        "failure_kind": "network",
                        "failure_code": "TELEGRAM_CONNECT_FAILED",
                        "safe_message": "Telegram connection failed.",
                    }
                ) from exc
            try:
                authorized = await client.is_user_authorized()
            except Exception as exc:
                raise _SafeDiagnosticFailure(
                    {
                        "stage": "telegram_authorization",
                        "account_id": account_id,
                        "session_exists": True,
                        "session_opened": True,
                        "telegram_connected": True,
                        "exception_type": type(exc).__name__,
                        "failure_kind": "telegram_rpc",
                        "failure_code": "TELEGRAM_CONNECT_FAILED",
                        "safe_message": "Telegram authorization check failed.",
                    }
                ) from exc
            if not authorized:
                raise _SafeDiagnosticFailure(
                    {
                        "stage": "telegram_authorization",
                        "account_id": account_id,
                        "session_exists": True,
                        "session_opened": True,
                        "telegram_connected": True,
                        "authorized": False,
                        "exception_type": "MiniAppSessionUnauthorizedError",
                        "failure_kind": "telegram_authorization",
                        "failure_code": "TELEGRAM_SESSION_UNAUTHORIZED",
                        "safe_message": "Saved Telegram session is not authorized.",
                    }
                )
            try:
                me = await client.get_me()
            except Exception as exc:
                raise _SafeDiagnosticFailure(
                    {
                        "stage": "telegram_identity",
                        "account_id": account_id,
                        "session_exists": True,
                        "session_opened": True,
                        "telegram_connected": True,
                        "authorized": True,
                        "exception_type": type(exc).__name__,
                        "failure_kind": "telegram_rpc",
                        "failure_code": "TELEGRAM_CONNECT_FAILED",
                        "safe_message": "Telegram identity lookup failed.",
                    }
                ) from exc
            telegram_id = getattr(me, "id", None)
            identity_match = (
                isinstance(telegram_id, int)
                and not isinstance(telegram_id, bool)
                and telegram_id == account.telegram_account_id
            )
            if not identity_match:
                raise _SafeDiagnosticFailure(
                    {
                        "stage": "telegram_identity",
                        "account_id": account_id,
                        "session_exists": True,
                        "session_opened": True,
                        "telegram_connected": True,
                        "authorized": True,
                        "telegram_account_id": telegram_id,
                        "expected_telegram_account_id": account.telegram_account_id,
                        "identity_match": False,
                        "exception_type": "MiniAppSessionIdentityMismatchError",
                        "failure_kind": "local_state",
                        "failure_code": "TELEGRAM_IDENTITY_MISMATCH",
                        "safe_message": "Telegram identity does not match the database record.",
                    }
                )
            return {
                "account_id": account_id,
                "session_exists": True,
                "session_opened": True,
                "telegram_connected": True,
                "authorized": True,
                "telegram_account_id": telegram_id,
                "identity_match": True,
            }
        finally:
            if connected:
                try:
                    await client.disconnect()
                except Exception as exc:  # noqa: BLE001 - diagnostic cleanup only
                    logger.warning(
                        "Telegram diagnostic disconnect failed account_db_id=%d "
                        "exception=%s",
                        account_id,
                        type(exc).__name__,
                    )

    async def auth(account_id: int) -> dict[str, object]:
        session = await tonnel.authenticate(
            account_id, owner_telegram_id=args.control_user_id
        )
        await session.transport.close()
        return {"session_authenticated": True}

    async def inventory() -> dict[str, object]:
        nonlocal gift_offer_id
        gifts = await tonnel.get_market_inventory(
            args.n2_account_id, owner_telegram_id=args.control_user_id
        )
        exact = [
            gift
            for gift in gifts
            if type(gift.gift_id) is type(args.gift_id) and gift.gift_id == args.gift_id
        ]
        if len(exact) == 1:
            gift_offer_id = exact[0].sale_id
        return {
            "result_count": len(gifts),
            "exact_gift_count": len(exact),
            "exact_gift_id_type": type(args.gift_id).__name__,
            "exact_gift_fingerprint": TonnelService.fingerprint(
                f"{type(args.gift_id).__name__}:{args.gift_id}"
            ),
        }

    async def balance(account_id: int) -> dict[str, object]:
        result = await tonnel.get_balance(
            account_id, owner_telegram_id=args.control_user_id
        )
        return {"balance_read": result.ton >= 0}

    async def my_offers() -> dict[str, object]:
        offers = await tonnel.get_my_buy_offers(
            args.n1_account_id, owner_telegram_id=args.control_user_id
        )
        return {
            "result_count": len(offers),
            "offer_id_types": sorted(
                {type(offer.offer_id).__name__ for offer in offers}
            ),
            "exact_gift_offers": await safe_offers(
                offers, observed_via="n1_get_my_offers"
            ),
        }

    async def gift_offers() -> dict[str, object]:
        offers = await tonnel.get_buy_offers_for_gift(
            args.n2_account_id,
            gift_offer_id,
            owner_telegram_id=args.control_user_id,
        )
        return {
            "result_count": len(offers),
            "request_gift_id_type": type(gift_offer_id).__name__,
            "offer_id_types": sorted(
                {type(offer.offer_id).__name__ for offer in offers}
            ),
            "exact_gift_offers": await safe_offers(
                offers, observed_via="n2_get_offers"
            ),
        }

    checks: list[dict[str, object]] = []
    checks.append(
        await _safe_check(
            "n1_telegram_session", lambda: telegram_session(args.n1_account_id)
        )
    )
    checks.append(
        await _safe_check(
            "n2_telegram_session", lambda: telegram_session(args.n2_account_id)
        )
    )
    checks.append(
        await _safe_check("n1_auth_session", lambda: auth(args.n1_account_id))
    )
    checks.append(
        await _safe_check("n2_auth_session", lambda: auth(args.n2_account_id))
    )
    checks.append(await _safe_check("n2_page_gifts", inventory))
    if not args.inventory_only:
        checks.append(
            await _safe_check("n1_balance_info", lambda: balance(args.n1_account_id))
        )
        checks.append(
            await _safe_check("n2_balance_info", lambda: balance(args.n2_account_id))
        )
        checks.append(await _safe_check("n1_get_my_offers", my_offers))
        checks.append(await _safe_check("n2_get_offers", gift_offers))

    for check in checks:
        print(json.dumps(check, ensure_ascii=False, sort_keys=True))
    all_passed = all(check["result"] == "PASS" for check in checks)
    print(
        json.dumps(
            {
                "summary": "PASS" if all_passed else "FAIL",
                "api_origin": tonnel.api_origin,
                "http_observations": observations,
                "mutations_sent": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if all_passed else 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only Tonnel BUY_OFFER preflight diagnostic"
    )
    parser.add_argument("--control-user-id", type=int, required=True)
    parser.add_argument("--n1-account-id", type=int, required=True)
    parser.add_argument("--n2-account-id", type=int, required=True)
    parser.add_argument("--gift-id", type=_opaque_id, required=True)
    parser.add_argument(
        "--inventory-only",
        action="store_true",
        help="check auth/session and the exact N2 pageGifts endpoint only",
    )
    try:
        exit_code = asyncio.run(_run(parser.parse_args()))
    except Exception as exc:  # noqa: BLE001 - class-only and secret-free fallback
        print(
            json.dumps(
                {
                    "summary": "FAIL",
                    "exception_class": type(exc).__name__,
                    "mutations_sent": 0,
                },
                sort_keys=True,
            )
        )
        raise SystemExit(1) from None
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
