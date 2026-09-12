from __future__ import annotations

import unittest
from pathlib import Path

from app.bot.handlers.accounts import _accounts_text
from app.bot.keyboards import (
    accounts_keyboard,
    authorization_keyboard,
    main_menu_keyboard,
    mrkt_job_confirmation_keyboard,
    portals_confirmation_keyboard,
)
from app.db import Account

PROJECT_ROOT = Path(__file__).resolve().parents[1]
USER_INTERFACE_FILES = (
    PROJECT_ROOT / "app/bot/handlers/start.py",
    PROJECT_ROOT / "app/bot/handlers/accounts.py",
    PROJECT_ROOT / "app/bot/handlers/auth.py",
    PROJECT_ROOT / "app/bot/handlers/errors.py",
    PROJECT_ROOT / "app/bot/handlers/mrkt_jobs.py",
    PROJECT_ROOT / "app/bot/handlers/mrkt_transfers.py",
    PROJECT_ROOT / "app/bot/handlers/history.py",
    PROJECT_ROOT / "app/bot/handlers/navigation.py",
    PROJECT_ROOT / "app/bot/handlers/portals_jobs.py",
    PROJECT_ROOT / "app/bot/handlers/portals_transfers.py",
    PROJECT_ROOT / "app/bot/keyboards/accounts.py",
    PROJECT_ROOT / "app/bot/keyboards/transfers.py",
    PROJECT_ROOT / "app/jobs/mrkt_jobs.py",
    PROJECT_ROOT / "app/jobs/mrkt_transfer_jobs.py",
    PROJECT_ROOT / "app/jobs/portals_jobs.py",
    PROJECT_ROOT / "app/jobs/portals_transfer_batches.py",
    PROJECT_ROOT / "app/jobs/portals_transfer_jobs.py",
    PROJECT_ROOT / "app/telegram_client/mrkt_service.py",
    PROJECT_ROOT / "app/web/app.py",
    PROJECT_ROOT / "app/web/auth_flow.py",
    PROJECT_ROOT / "app/web/static/auth.html",
    PROJECT_ROOT / "app/web/static/auth.js",
)
OBVIOUS_ENGLISH_UI = (
    "Add account",
    "My accounts",
    "Open Portals",
    "Open secure authorization",
    "Confirm listing",
    "Send code",
    "Статус запуска Mini App",
    "Выбрано неподдерживаемое Mini App",
    "безопасном Web App",
    "WebView получен",
    "initData получен",
    "Sign in",
    "Account successfully connected",
    "Phone number",
    "Telegram login code",
    "First name",
    "No connected accounts",
    "Invalid account action",
    "Select the connected account",
    "An unexpected error occurred",
    "Mini App launch status",
    "Account connected:",
    "Dry run successful",
    "Listing cancelled",
    "Requesting a login code",
    "Checking the login code",
    "Authorization complete",
    "The request failed",
    "Login timed out",
)


class RussianUiTests(unittest.TestCase):
    def test_primary_keyboard_labels_are_russian(self) -> None:
        main_labels = [
            button.text
            for row in main_menu_keyboard().inline_keyboard
            for button in row
        ]
        self.assertEqual(
            main_labels,
            [
                "👤 Мои аккаунты",
                "💼 Рабочие аккаунты",
                "🌀 Portals",
                "🚇 Tonnel",
                "📜 История",
                "❓ Помощь",
            ],
        )
        self.assertEqual(
            authorization_keyboard("https://example.com/auth")
            .inline_keyboard[0][0]
            .text,
            "🔐 Открыть авторизацию",
        )
        mrkt_buttons = mrkt_job_confirmation_keyboard("token").inline_keyboard
        self.assertEqual(mrkt_buttons[0][0].text, "✅ Разместить")
        self.assertEqual(mrkt_buttons[2][0].text, "Отмена")
        portals_buttons = portals_confirmation_keyboard("token").inline_keyboard
        self.assertEqual(portals_buttons[0][0].text, "✅ Принять оффер")
        self.assertEqual(portals_buttons[2][0].text, "Отмена")

    def test_account_list_and_removal_are_russian(self) -> None:
        account = Account(
            id=1,
            owner_telegram_id=2,
            telegram_account_id=3,
            session_key="internal",
            username="owner",
            first_name="Имя",
            phone=None,
            created_at="2026-08-31",
        )
        text = _accounts_text([account])
        self.assertIn("Подключённые Telegram-аккаунты", text)
        labels = [
            button.text
            for row in accounts_keyboard([account], "nonce").inline_keyboard
            for button in row
        ]
        self.assertTrue(any(label.startswith("🗑 Удалить") for label in labels))

    def test_authorization_web_app_is_russian(self) -> None:
        html_source = (PROJECT_ROOT / "app/web/static/auth.html").read_text()
        js_source = (PROJECT_ROOT / "app/web/static/auth.js").read_text()
        self.assertIn('<html lang="ru">', html_source)
        self.assertIn("Подключение Telegram", html_source)
        self.assertIn("Получить код", html_source)
        self.assertIn("Введите номер", js_source)
        self.assertIn("Аккаунт подключён", js_source)

    def test_portals_dry_run_is_safe_by_default(self) -> None:
        env_example = (PROJECT_ROOT / ".env.example").read_text()
        self.assertIn("PORTALS_DRY_RUN=true", env_example)
        self.assertIn("MRKT_TRANSFER_DRY_RUN=true", env_example)
        self.assertIn("PORTALS_TRANSFER_DRY_RUN=true", env_example)

    def test_portals_transfer_has_no_amount_prompt(self) -> None:
        source = (PROJECT_ROOT / "app/bot/handlers/portals_transfers.py").read_text()
        states = (PROJECT_ROOT / "app/bot/states/transfers.py").read_text()
        self.assertNotIn("Введите точную сумму оффера", source)
        portals_states = states.split("class PortalsTransferStates", 1)[1].split(
            "class TonnelTransferStates", 1
        )[0]
        self.assertNotIn("entering_amount", portals_states)
        self.assertIn('Decimal("0.53")', (PROJECT_ROOT / "app/jobs/portals_transfer_jobs.py").read_text())

    def test_mrkt_external_sale_message_is_russian(self) -> None:
        source = (PROJECT_ROOT / "app/bot/handlers/mrkt_transfers.py").read_text()
        self.assertIn("Подарок успел уйти другому покупателю", source)
        self.assertIn(
            "наш аккаунт не успел выполнить покупку",
            source,
        )
        self.assertIn("Покупка началась слишком рано", source)

    def test_no_known_english_user_interface_phrases_remain(self) -> None:
        combined = "\n".join(path.read_text() for path in USER_INTERFACE_FILES)
        for phrase in OBVIOUS_ENGLISH_UI:
            with self.subTest(phrase=phrase):
                self.assertNotIn(phrase, combined)
