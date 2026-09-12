from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlsplit

from aiogram.utils.token import TokenValidationError, validate_token
from dotenv import load_dotenv

API_HASH_PATTERN = re.compile(r"^[0-9a-fA-F]{32}$")
DEFAULT_TONNEL_API_ORIGIN = "https://gifts.coffin.meme"
ALLOWED_TONNEL_API_ORIGINS = frozenset(
    {
        DEFAULT_TONNEL_API_ORIGIN,
        "https://rs-api.tonnel.network",
    }
)


@dataclass(frozen=True, slots=True)
class Config:
    bot_token: str
    telegram_api_id: int
    telegram_api_hash: str
    webapp_public_url: str
    web_host: str
    web_port: int
    mrkt_dry_run: bool
    portals_dry_run: bool
    mrkt_transfer_dry_run: bool
    portals_transfer_dry_run: bool
    tonnel_transfer_dry_run: bool
    tonnel_transfer_mode: str
    tonnel_api_origin: str
    tonnel_offer_accept_delay_ms: int
    tonnel_ownership_verify_timeout_ms: int
    max_portals_concurrent_jobs: int
    max_tonnel_concurrent_jobs: int
    portals_auto_offer_amount: Decimal
    tonnel_auto_offer_amount: Decimal
    database_path: Path
    sessions_dir: Path
    live_verify: bool = False
    mrkt_speculative_buy: bool = False
    mrkt_speculative_buy_delay_ms: int = 40
    mrkt_transfer_mode: str = "FAST_CONFIRMED"
    mrkt_listed_trigger_max_wait_ms: int = 1000
    mrkt_listed_trigger_poll_interval_ms: int = 10

    @classmethod
    def load(cls) -> Config:
        project_root = Path(__file__).resolve().parent.parent
        load_dotenv(project_root / ".env")

        bot_token = os.getenv("BOT_TOKEN", "").strip()
        api_id_value = os.getenv("TELEGRAM_API_ID", "").strip()
        api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
        webapp_public_url = os.getenv("WEBAPP_PUBLIC_URL", "").strip().rstrip("/")
        web_host = os.getenv("WEB_HOST", "127.0.0.1").strip()
        web_port_value = os.getenv("WEB_PORT", "8000").strip()
        mrkt_dry_run_value = os.getenv("MRKT_DRY_RUN", "true").strip().lower()
        portals_dry_run_value = os.getenv("PORTALS_DRY_RUN", "true").strip().lower()
        mrkt_transfer_dry_run_value = (
            os.getenv("MRKT_TRANSFER_DRY_RUN", "true").strip().lower()
        )
        portals_transfer_dry_run_value = (
            os.getenv("PORTALS_TRANSFER_DRY_RUN", "true").strip().lower()
        )
        tonnel_transfer_dry_run_value = (
            os.getenv("TONNEL_TRANSFER_DRY_RUN", "true").strip().lower()
        )
        tonnel_transfer_mode_value = (
            os.getenv("TONNEL_TRANSFER_MODE", "BUY_OFFER").strip().upper()
        )
        tonnel_api_origin = os.getenv(
            "TONNEL_API_ORIGIN", DEFAULT_TONNEL_API_ORIGIN
        ).strip().rstrip("/")
        tonnel_offer_accept_delay_value = os.getenv(
            "TONNEL_OFFER_ACCEPT_DELAY_MS", "5000"
        ).strip()
        tonnel_ownership_verify_timeout_value = os.getenv(
            "TONNEL_OWNERSHIP_VERIFY_TIMEOUT_MS", "15000"
        ).strip()
        max_portals_concurrent_jobs_value = os.getenv(
            "MAX_PORTALS_CONCURRENT_JOBS", "5"
        ).strip()
        max_tonnel_concurrent_jobs_value = os.getenv(
            "MAX_TONNEL_CONCURRENT_JOBS", "5"
        ).strip()
        portals_auto_offer_amount_value = os.getenv(
            "PORTALS_AUTO_OFFER_AMOUNT", "0.53"
        ).strip()
        tonnel_auto_offer_amount_value = os.getenv(
            "TONNEL_AUTO_OFFER_AMOUNT", "2"
        ).strip()
        live_verify_value = os.getenv("LIVE_VERIFY", "false").strip().lower()
        mrkt_speculative_buy_value = (
            os.getenv("MRKT_SPECULATIVE_BUY", "false").strip().lower()
        )
        mrkt_speculative_buy_delay_value = os.getenv(
            "MRKT_SPECULATIVE_BUY_DELAY_MS", "40"
        ).strip()
        mrkt_transfer_mode_value = os.getenv("MRKT_TRANSFER_MODE", "").strip().upper()
        mrkt_listed_trigger_max_wait_value = os.getenv(
            "MRKT_LISTED_TRIGGER_MAX_WAIT_MS", "1000"
        ).strip()
        mrkt_listed_trigger_poll_interval_value = os.getenv(
            "MRKT_LISTED_TRIGGER_POLL_INTERVAL_MS", "10"
        ).strip()

        missing = [
            name
            for name, value in (
                ("BOT_TOKEN", bot_token),
                ("TELEGRAM_API_ID", api_id_value),
                ("TELEGRAM_API_HASH", api_hash),
                ("WEBAPP_PUBLIC_URL", webapp_public_url),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        try:
            api_id = int(api_id_value)
        except ValueError as exc:
            raise RuntimeError("TELEGRAM_API_ID must be an integer") from exc

        if api_id <= 0:
            raise RuntimeError("TELEGRAM_API_ID must be a positive integer")

        try:
            validate_token(bot_token)
        except TokenValidationError as exc:
            raise RuntimeError("BOT_TOKEN has an invalid format") from exc

        if not API_HASH_PATTERN.fullmatch(api_hash):
            raise RuntimeError("TELEGRAM_API_HASH must be 32 hexadecimal characters")

        public_url = urlsplit(webapp_public_url)
        insecure_localhost = public_url.scheme == "http" and public_url.hostname in {
            "127.0.0.1",
            "localhost",
            "::1",
        }
        if public_url.scheme != "https" and not insecure_localhost:
            raise RuntimeError(
                "WEBAPP_PUBLIC_URL must use HTTPS (HTTP is allowed only for localhost development)"
            )
        if (
            not public_url.netloc
            or public_url.path != "/auth"
            or public_url.query
            or public_url.fragment
        ):
            raise RuntimeError(
                "WEBAPP_PUBLIC_URL must be an absolute URL ending in /auth without a query or fragment"
            )
        if not web_host:
            raise RuntimeError("WEB_HOST must not be empty")
        try:
            web_port = int(web_port_value)
        except ValueError as exc:
            raise RuntimeError("WEB_PORT must be an integer") from exc
        if not 1 <= web_port <= 65535:
            raise RuntimeError("WEB_PORT must be between 1 and 65535")
        if mrkt_dry_run_value not in {"true", "false"}:
            raise RuntimeError("MRKT_DRY_RUN must be true or false")
        if portals_dry_run_value not in {"true", "false"}:
            raise RuntimeError("PORTALS_DRY_RUN must be true or false")
        if mrkt_transfer_dry_run_value not in {"true", "false"}:
            raise RuntimeError("MRKT_TRANSFER_DRY_RUN must be true or false")
        if portals_transfer_dry_run_value not in {"true", "false"}:
            raise RuntimeError("PORTALS_TRANSFER_DRY_RUN must be true or false")
        if tonnel_transfer_dry_run_value not in {"true", "false"}:
            raise RuntimeError("TONNEL_TRANSFER_DRY_RUN must be true or false")
        if tonnel_transfer_mode_value not in {
            "BUY_OFFER",
            "MARKET_SALE",
            "DIRECT_RECIPIENT",
        }:
            raise RuntimeError(
                "TONNEL_TRANSFER_MODE must be BUY_OFFER, MARKET_SALE, or "
                "DIRECT_RECIPIENT"
            )
        if tonnel_api_origin not in ALLOWED_TONNEL_API_ORIGINS:
            raise RuntimeError(
                "TONNEL_API_ORIGIN must be an approved HTTPS Tonnel API origin"
            )
        try:
            tonnel_offer_accept_delay_ms = int(tonnel_offer_accept_delay_value)
        except ValueError as exc:
            raise RuntimeError(
                "TONNEL_OFFER_ACCEPT_DELAY_MS must be an integer"
            ) from exc
        if not 0 <= tonnel_offer_accept_delay_ms <= 5_000:
            raise RuntimeError(
                "TONNEL_OFFER_ACCEPT_DELAY_MS must be between 0 and 5000"
            )
        try:
            tonnel_ownership_verify_timeout_ms = int(
                tonnel_ownership_verify_timeout_value
            )
        except ValueError as exc:
            raise RuntimeError(
                "TONNEL_OWNERSHIP_VERIFY_TIMEOUT_MS must be an integer"
            ) from exc
        if not 0 <= tonnel_ownership_verify_timeout_ms <= 60_000:
            raise RuntimeError(
                "TONNEL_OWNERSHIP_VERIFY_TIMEOUT_MS must be between 0 and 60000"
            )
        if live_verify_value not in {"true", "false"}:
            raise RuntimeError("LIVE_VERIFY must be true or false")
        if mrkt_speculative_buy_value not in {"true", "false"}:
            raise RuntimeError("MRKT_SPECULATIVE_BUY must be true or false")
        try:
            mrkt_speculative_buy_delay_ms = int(mrkt_speculative_buy_delay_value)
        except ValueError as exc:
            raise RuntimeError(
                "MRKT_SPECULATIVE_BUY_DELAY_MS must be an integer"
            ) from exc
        if not 0 <= mrkt_speculative_buy_delay_ms <= 250:
            raise RuntimeError(
                "MRKT_SPECULATIVE_BUY_DELAY_MS must be between 0 and 250"
            )
        supported_modes = {"FAST_CONFIRMED", "SPECULATIVE", "LISTED_TRIGGERED"}
        if mrkt_transfer_mode_value and mrkt_transfer_mode_value not in supported_modes:
            raise RuntimeError(
                "MRKT_TRANSFER_MODE must be FAST_CONFIRMED, SPECULATIVE, or LISTED_TRIGGERED"
            )
        mrkt_transfer_mode = mrkt_transfer_mode_value or (
            "SPECULATIVE"
            if mrkt_speculative_buy_value == "true"
            else "FAST_CONFIRMED"
        )
        try:
            mrkt_listed_trigger_max_wait_ms = int(
                mrkt_listed_trigger_max_wait_value
            )
        except ValueError as exc:
            raise RuntimeError(
                "MRKT_LISTED_TRIGGER_MAX_WAIT_MS must be an integer"
            ) from exc
        if not 100 <= mrkt_listed_trigger_max_wait_ms <= 5_000:
            raise RuntimeError(
                "MRKT_LISTED_TRIGGER_MAX_WAIT_MS must be between 100 and 5000"
            )
        try:
            mrkt_listed_trigger_poll_interval_ms = int(
                mrkt_listed_trigger_poll_interval_value
            )
        except ValueError as exc:
            raise RuntimeError(
                "MRKT_LISTED_TRIGGER_POLL_INTERVAL_MS must be an integer"
            ) from exc
        if not 1 <= mrkt_listed_trigger_poll_interval_ms <= 250:
            raise RuntimeError(
                "MRKT_LISTED_TRIGGER_POLL_INTERVAL_MS must be between 1 and 250"
            )
        if mrkt_listed_trigger_poll_interval_ms > mrkt_listed_trigger_max_wait_ms:
            raise RuntimeError(
                "MRKT_LISTED_TRIGGER_POLL_INTERVAL_MS must not exceed the maximum wait"
            )
        try:
            max_portals_concurrent_jobs = int(max_portals_concurrent_jobs_value)
        except ValueError as exc:
            raise RuntimeError(
                "MAX_PORTALS_CONCURRENT_JOBS must be an integer"
            ) from exc
        if not 1 <= max_portals_concurrent_jobs <= 5:
            raise RuntimeError(
                "MAX_PORTALS_CONCURRENT_JOBS must be between 1 and 5"
            )
        try:
            max_tonnel_concurrent_jobs = int(max_tonnel_concurrent_jobs_value)
        except ValueError as exc:
            raise RuntimeError(
                "MAX_TONNEL_CONCURRENT_JOBS must be an integer"
            ) from exc
        if not 1 <= max_tonnel_concurrent_jobs <= 5:
            raise RuntimeError(
                "MAX_TONNEL_CONCURRENT_JOBS must be between 1 and 5"
            )
        portals_auto_offer_amount = cls._offer_amount(
            "PORTALS_AUTO_OFFER_AMOUNT",
            portals_auto_offer_amount_value,
            max_decimal_places=9,
        )
        if portals_auto_offer_amount < Decimal("0.5"):
            raise RuntimeError("PORTALS_AUTO_OFFER_AMOUNT must be at least 0.5")
        tonnel_auto_offer_amount = cls._offer_amount(
            "TONNEL_AUTO_OFFER_AMOUNT",
            tonnel_auto_offer_amount_value,
            max_decimal_places=3,
        )
        if not Decimal(2) <= tonnel_auto_offer_amount <= Decimal(20000):
            raise RuntimeError(
                "TONNEL_AUTO_OFFER_AMOUNT must be between 2 and 20000"
            )

        return cls(
            bot_token=bot_token,
            telegram_api_id=api_id,
            telegram_api_hash=api_hash,
            webapp_public_url=webapp_public_url,
            web_host=web_host,
            web_port=web_port,
            mrkt_dry_run=mrkt_dry_run_value == "true",
            portals_dry_run=portals_dry_run_value == "true",
            mrkt_transfer_dry_run=mrkt_transfer_dry_run_value == "true",
            portals_transfer_dry_run=portals_transfer_dry_run_value == "true",
            tonnel_transfer_dry_run=tonnel_transfer_dry_run_value == "true",
            tonnel_transfer_mode=tonnel_transfer_mode_value,
            tonnel_api_origin=tonnel_api_origin,
            tonnel_offer_accept_delay_ms=tonnel_offer_accept_delay_ms,
            tonnel_ownership_verify_timeout_ms=(
                tonnel_ownership_verify_timeout_ms
            ),
            max_portals_concurrent_jobs=max_portals_concurrent_jobs,
            max_tonnel_concurrent_jobs=max_tonnel_concurrent_jobs,
            portals_auto_offer_amount=portals_auto_offer_amount,
            tonnel_auto_offer_amount=tonnel_auto_offer_amount,
            database_path=project_root / "data" / "app.db",
            sessions_dir=project_root / "data" / "sessions",
            live_verify=live_verify_value == "true",
            mrkt_speculative_buy=mrkt_speculative_buy_value == "true",
            mrkt_speculative_buy_delay_ms=mrkt_speculative_buy_delay_ms,
            mrkt_transfer_mode=mrkt_transfer_mode,
            mrkt_listed_trigger_max_wait_ms=mrkt_listed_trigger_max_wait_ms,
            mrkt_listed_trigger_poll_interval_ms=(
                mrkt_listed_trigger_poll_interval_ms
            ),
        )

    @staticmethod
    def _offer_amount(
        name: str, value: str, *, max_decimal_places: int
    ) -> Decimal:
        try:
            amount = Decimal(value)
        except InvalidOperation as exc:
            raise RuntimeError(f"{name} must be a decimal number") from exc
        if not amount.is_finite() or amount <= 0:
            raise RuntimeError(f"{name} must be positive")
        exponent = amount.as_tuple().exponent
        if not isinstance(exponent, int) or max(0, -exponent) > max_decimal_places:
            raise RuntimeError(
                f"{name} must have at most {max_decimal_places} decimal places"
            )
        return amount
