from __future__ import annotations

import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import patch

from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.bot.handlers.accounts import _accounts_text
from app.bot.handlers.history import STATUS_LABELS, _history_text
from app.bot.handlers.mrkt_jobs import _mrkt_review_text, _send_inventory_error
from app.bot.handlers.navigation import reject_stale_callback
from app.bot.handlers.portals_jobs import _portals_review_text
from app.bot.keyboards import (
    main_menu_keyboard,
    mask_phone,
    mrkt_job_confirmation_keyboard,
    portals_confirmation_keyboard,
)
from app.config import Config
from app.db import Account, Database
from app.telegram_client import MiniAppSessionUnauthorizedError, PortalsOffer

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _FakeState:
    def __init__(self) -> None:
        self.cleared = False

    async def clear(self) -> None:
        self.cleared = True


class _FakeCallback:
    def __init__(self) -> None:
        self.message = None
        self.answers: list[tuple[str | None, bool]] = []

    async def answer(
        self, text: str | None = None, *, show_alert: bool = False
    ) -> None:
        self.answers.append((text, show_alert))


class _FakeMessage:
    def __init__(self) -> None:
        self.answers: list[str] = []

    async def answer(self, text: str, **kwargs: object) -> None:
        del kwargs
        self.answers.append(text)

    async def edit_text(self, text: str, **kwargs: object) -> None:
        del kwargs
        self.answers.append(text)


class ClientMvpTests(unittest.IsolatedAsyncioTestCase):
    def test_final_main_menu_and_confirmation_buttons(self) -> None:
        menu = main_menu_keyboard().inline_keyboard
        self.assertEqual(
            [[button.text for button in row] for row in menu],
            [
                ["👤 Мои аккаунты", "💼 Рабочие аккаунты"],
                ["🌀 Portals", "🚇 Tonnel"],
                ["📜 История", "❓ Помощь"],
            ],
        )
        self.assertEqual(
            mrkt_job_confirmation_keyboard("n").inline_keyboard[0][0].text,
            "✅ Разместить",
        )
        self.assertEqual(
            portals_confirmation_keyboard("n").inline_keyboard[0][0].text,
            "✅ Принять оффер",
        )

    def test_accounts_are_masked_and_show_status(self) -> None:
        account = Account(
            id=1,
            owner_telegram_id=10,
            telegram_account_id=99,
            session_key="internal",
            username="owner",
            first_name="Имя",
            phone="+48123456789",
            created_at="2026-09-02",
        )
        text = _accounts_text([account], {1: True})
        self.assertIn("@owner", text)
        self.assertIn("+48••••789", text)
        self.assertIn("✅ Подключён", text)
        self.assertNotIn("+48123456789", text)
        self.assertNotIn("telegram_account_id", text)
        self.assertEqual(mask_phone("+48123456789"), "+48••••789")

    def test_final_mrkt_and_portals_review_text(self) -> None:
        mrkt = _mrkt_review_text("@owner", "Подарок", "1.68", dry_run=True)
        portals = _portals_review_text(
            "@owner",
            PortalsOffer(
                offer_id="hidden",
                nft_id="hidden-nft",
                amount="2.5",
                display_name="Коллекционный NFT",
                amount_text="2.5",
                eligible=True,
                eligibility_reason=None,
            ),
        )
        self.assertIn("Аккаунт: @owner", mrkt)
        self.assertIn("Подарок: Подарок", mrkt)
        self.assertIn("Цена: <b>1.68 TON</b>", mrkt)
        self.assertIn("Аккаунт: @owner", portals)
        self.assertIn("NFT: Коллекционный NFT", portals)
        self.assertIn("Сумма: <b>2.5 TON</b>", portals)
        self.assertNotIn("hidden", portals)

    async def test_history_is_owner_scoped_and_human_readable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = Database(Path(directory) / "app.db")
            await db.initialize()
            await db.save_account(
                owner_telegram_id=1,
                telegram_account_id=101,
                session_key="owner-one",
                username="first",
                first_name=None,
                phone=None,
            )
            self.assertIsNotNone(await db.get_account(1, 1))
            self.assertIsNone(await db.get_account(1, 2))
            await db.save_account(
                owner_telegram_id=2,
                telegram_account_id=202,
                session_key="owner-two",
                username="second",
                first_name=None,
                phone=None,
            )
            first = await db.create_mrkt_job(
                owner_telegram_id=1,
                account_id=1,
                gift_id="secret-gift-id",
                price_ton="1.68",
            )
            await db.claim_mrkt_job(first.id)
            await db.update_mrkt_job_target(
                first.id,
                gift_id="secret-gift-id",
                price_ton="1.68",
                price_nanotons=1_680_000_000,
                display_name="Безопасный подарок",
            )
            await db.finish_mrkt_job(
                first.id,
                status="SUCCESS",
                error_code="SUCCESS",
                error_message="ok",
                result_metadata={},
            )
            second = await db.create_portals_job(
                owner_telegram_id=2,
                account_id=2,
                offer_id="other-owner-offer",
            )
            await db.claim_portals_job(second.id)
            await db.update_portals_job_target(
                second.id,
                offer_id="other-owner-offer",
                nft_id="other-nft",
                amount="4",
                display_name="Чужой NFT",
            )
            await db.finish_portals_job(
                second.id,
                status="SUCCESS",
                error_code="SUCCESS",
                error_message="ok",
                result_metadata={},
            )

            history = await db.list_operation_history(1)
            accounts = await db.list_accounts(1)
            text = _history_text(history, accounts)

        self.assertEqual(len(history), 1)
        self.assertIn("Безопасный подарок", text)
        self.assertIn("✅ Успешно · MRKT", text)
        self.assertNotIn("Чужой NFT", text)
        self.assertNotIn("secret-gift-id", text)

    async def test_stale_callback_is_rejected(self) -> None:
        callback = _FakeCallback()
        state = _FakeState()
        await reject_stale_callback(
            cast(CallbackQuery, callback), cast(FSMContext, state)
        )
        self.assertTrue(state.cleared)
        self.assertEqual(callback.answers, [("Эта кнопка устарела.", True)])

    async def test_revoked_account_error_is_sanitized(self) -> None:
        message = _FakeMessage()
        await _send_inventory_error(
            cast(Message, message),
            1,
            MiniAppSessionUnauthorizedError("secret-session-value"),
        )
        self.assertEqual(
            message.answers,
            [
                (
                    "<b>⚠️ Авторизация аккаунта больше не действует</b>\n\n"
                    "Подключите аккаунт заново."
                )
            ],
        )
        self.assertNotIn("secret-session-value", " ".join(message.answers))

    def test_empty_history_and_status_labels(self) -> None:
        self.assertIn("Операций пока нет", _history_text([], []))
        self.assertEqual(STATUS_LABELS["QUEUED"], "В очереди")
        self.assertEqual(STATUS_LABELS["PENDING"], "Ожидает")
        self.assertEqual(STATUS_LABELS["RUNNING"], "Выполняется")
        self.assertEqual(STATUS_LABELS["SUCCESS"], "Успешно")
        self.assertEqual(STATUS_LABELS["FAILED"], "Ошибка")
        self.assertEqual(STATUS_LABELS["AMBIGUOUS"], "Требуется проверка")
        self.assertEqual(STATUS_LABELS["DRY_RUN"], "Тестовый запуск")

    def test_config_and_docker_smoke(self) -> None:
        environment = {
            "BOT_TOKEN": f"123456:{'A' * 35}",
            "TELEGRAM_API_ID": "123456",
            "TELEGRAM_API_HASH": "a" * 32,
            "WEBAPP_PUBLIC_URL": "https://auth.example.com/auth",
            "WEB_HOST": "0.0.0.0",
            "WEB_PORT": "8000",
            "MRKT_DRY_RUN": "true",
            "PORTALS_DRY_RUN": "true",
        }
        with (
            patch("app.config.load_dotenv"),
            patch.dict(os.environ, environment, clear=True),
        ):
            config = Config.load()
        self.assertTrue(config.mrkt_dry_run)
        self.assertTrue(config.portals_dry_run)
        self.assertTrue(config.mrkt_transfer_dry_run)
        self.assertTrue(config.portals_transfer_dry_run)
        self.assertTrue(config.tonnel_transfer_dry_run)
        self.assertEqual(config.tonnel_transfer_mode, "BUY_OFFER")
        self.assertEqual(config.tonnel_api_origin, "https://gifts.coffin.meme")
        self.assertEqual(config.tonnel_offer_accept_delay_ms, 5000)
        self.assertEqual(config.tonnel_ownership_verify_timeout_ms, 15000)
        self.assertFalse(config.mrkt_speculative_buy)
        self.assertEqual(config.mrkt_speculative_buy_delay_ms, 40)
        self.assertEqual(config.mrkt_transfer_mode, "FAST_CONFIRMED")
        self.assertEqual(config.mrkt_listed_trigger_max_wait_ms, 1000)
        self.assertEqual(config.mrkt_listed_trigger_poll_interval_ms, 10)
        self.assertEqual(config.max_portals_concurrent_jobs, 5)
        self.assertEqual(config.max_tonnel_concurrent_jobs, 5)
        self.assertEqual(config.portals_auto_offer_amount, Decimal("0.53"))
        self.assertEqual(config.tonnel_auto_offer_amount, Decimal(2))
        dockerfile = (PROJECT_ROOT / "Dockerfile").read_text()
        compose = (PROJECT_ROOT / "docker-compose.yml").read_text()
        self.assertIn("python:3.12-slim", dockerfile)
        self.assertIn("/healthz", dockerfile)
        self.assertIn("tg_auto_auth_data:/app/data", compose)
        self.assertIn("restart: unless-stopped", compose)
        self.assertIn("stop_grace_period: 30s", compose)
        with (
            patch("app.config.load_dotenv"),
            patch.dict(
                os.environ,
                {
                    **environment,
                    "TONNEL_API_ORIGIN": "https://rs-api.tonnel.network/",
                    "TONNEL_OFFER_ACCEPT_DELAY_MS": "0",
                    "TONNEL_OWNERSHIP_VERIFY_TIMEOUT_MS": "1000",
                },
                clear=True,
            ),
        ):
            regional = Config.load()
        self.assertEqual(
            regional.tonnel_api_origin, "https://rs-api.tonnel.network"
        )
        self.assertEqual(regional.tonnel_offer_accept_delay_ms, 0)
        self.assertEqual(regional.tonnel_ownership_verify_timeout_ms, 1000)
        for invalid_origin in (
            "http://rs-api.tonnel.network",
            "https://rs-api.tonnel.network/api",
            "https://untrusted.example",
        ):
            with (
                self.subTest(tonnel_api_origin=invalid_origin),
                patch("app.config.load_dotenv"),
                patch.dict(
                    os.environ,
                    {**environment, "TONNEL_API_ORIGIN": invalid_origin},
                    clear=True,
                ),
                self.assertRaisesRegex(RuntimeError, "TONNEL_API_ORIGIN"),
            ):
                Config.load()
        for invalid_timeout in ("-1", "60001", "not-a-number"):
            with (
                self.subTest(tonnel_ownership_verify_timeout_ms=invalid_timeout),
                patch("app.config.load_dotenv"),
                patch.dict(
                    os.environ,
                    {
                        **environment,
                        "TONNEL_OWNERSHIP_VERIFY_TIMEOUT_MS": invalid_timeout,
                    },
                    clear=True,
                ),
                self.assertRaisesRegex(
                    RuntimeError, "TONNEL_OWNERSHIP_VERIFY_TIMEOUT_MS"
                ),
            ):
                Config.load()
        for invalid_delay in ("-1", "5001", "not-a-number"):
            with (
                self.subTest(tonnel_offer_accept_delay_ms=invalid_delay),
                patch("app.config.load_dotenv"),
                patch.dict(
                    os.environ,
                    {**environment, "TONNEL_OFFER_ACCEPT_DELAY_MS": invalid_delay},
                    clear=True,
                ),
                self.assertRaisesRegex(
                    RuntimeError, "TONNEL_OFFER_ACCEPT_DELAY_MS"
                ),
            ):
                Config.load()
        with (
            patch("app.config.load_dotenv"),
            patch.dict(
                os.environ,
                {**environment, "MAX_PORTALS_CONCURRENT_JOBS": "6"},
                clear=True,
            ),
            self.assertRaisesRegex(RuntimeError, "between 1 and 5"),
        ):
            Config.load()
        for name, value in (
            ("MAX_TONNEL_CONCURRENT_JOBS", "0"),
            ("MAX_TONNEL_CONCURRENT_JOBS", "6"),
            ("PORTALS_AUTO_OFFER_AMOUNT", "0.49"),
            ("PORTALS_AUTO_OFFER_AMOUNT", "bad"),
            ("TONNEL_AUTO_OFFER_AMOUNT", "1.999"),
            ("TONNEL_AUTO_OFFER_AMOUNT", "4.5001"),
        ):
            with (
                self.subTest(name=name, value=value),
                patch("app.config.load_dotenv"),
                patch.dict(os.environ, {**environment, name: value}, clear=True),
                self.assertRaises(RuntimeError),
            ):
                Config.load()
        with (
            patch("app.config.load_dotenv"),
            patch.dict(
                os.environ,
                {
                    **environment,
                    "MRKT_SPECULATIVE_BUY": "true",
                    "MRKT_TRANSFER_MODE": "LISTED_TRIGGERED",
                },
                clear=True,
            ),
        ):
            explicit_mode = Config.load()
        self.assertEqual(explicit_mode.mrkt_transfer_mode, "LISTED_TRIGGERED")
        with (
            patch("app.config.load_dotenv"),
            patch.dict(
                os.environ,
                {**environment, "MRKT_SPECULATIVE_BUY": "true"},
                clear=True,
            ),
        ):
            legacy_mode = Config.load()
        self.assertEqual(legacy_mode.mrkt_transfer_mode, "SPECULATIVE")
        for name, value in (
            ("TONNEL_TRANSFER_MODE", "UNKNOWN"),
            ("MRKT_TRANSFER_MODE", "UNKNOWN"),
            ("MRKT_LISTED_TRIGGER_MAX_WAIT_MS", "99"),
            ("MRKT_LISTED_TRIGGER_MAX_WAIT_MS", "5001"),
            ("MRKT_LISTED_TRIGGER_MAX_WAIT_MS", "invalid"),
            ("MRKT_LISTED_TRIGGER_POLL_INTERVAL_MS", "0"),
            ("MRKT_LISTED_TRIGGER_POLL_INTERVAL_MS", "251"),
            ("MRKT_LISTED_TRIGGER_POLL_INTERVAL_MS", "invalid"),
        ):
            with (
                self.subTest(name=name, value=value),
                patch("app.config.load_dotenv"),
                patch.dict(os.environ, {**environment, name: value}, clear=True),
                self.assertRaises(RuntimeError),
            ):
                Config.load()
        with (
            patch("app.config.load_dotenv"),
            patch.dict(
                os.environ,
                {
                    **environment,
                    "MRKT_LISTED_TRIGGER_MAX_WAIT_MS": "100",
                    "MRKT_LISTED_TRIGGER_POLL_INTERVAL_MS": "101",
                },
                clear=True,
            ),
            self.assertRaisesRegex(RuntimeError, "must not exceed"),
        ):
            Config.load()
        for invalid_delay in ("-1", "251", "not-a-number"):
            with (
                patch("app.config.load_dotenv"),
                patch.dict(
                    os.environ,
                    {
                        **environment,
                        "MRKT_SPECULATIVE_BUY_DELAY_MS": invalid_delay,
                    },
                    clear=True,
                ),
                self.assertRaisesRegex(
                    RuntimeError,
                    "MRKT_SPECULATIVE_BUY_DELAY_MS",
                ),
            ):
                Config.load()
        with (
            patch("app.config.load_dotenv"),
            patch.dict(
                os.environ,
                {**environment, "MRKT_SPECULATIVE_BUY": "yes"},
                clear=True,
            ),
            self.assertRaisesRegex(RuntimeError, "MRKT_SPECULATIVE_BUY must"),
        ):
            Config.load()
