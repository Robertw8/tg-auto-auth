from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path

from app.bot.handlers.history import _history_text
from app.bot.handlers.tonnel_transfers import (
    _empty_inventory_text,
    _inventory_error_text,
    _tonnel_batch_result_text,
)
from app.bot.keyboards import main_menu_keyboard, transfer_confirmation_keyboard
from app.db import OperationHistoryItem
from app.jobs import (
    TonnelBatchItemProgress,
    TonnelBatchProgress,
    TonnelTransferBatchResult,
)
from app.telegram_client import TonnelTransferMode


class TonnelUiTests(unittest.TestCase):
    @staticmethod
    def batch_result(*statuses: str) -> TonnelTransferBatchResult:
        progress = TonnelBatchProgress(
            total_count=len(statuses),
            success_count=statuses.count("SUCCESS"),
            failed_count=statuses.count("FAILED"),
            ambiguous_count=statuses.count("AMBIGUOUS"),
            dry_run_count=statuses.count("DRY_RUN"),
            running_count=0,
            queued_count=0,
            items=tuple(
                TonnelBatchItemProgress(
                    display_name=f"Подарок {index}",
                    status=status,
                    safe_reason="success" if status == "FAILED" else None,
                )
                for index, status in enumerate(statuses, start=1)
            ),
        )
        return TonnelTransferBatchResult(
            batch_id=1,
            status="SUCCESS" if set(statuses) == {"SUCCESS"} else "PARTIAL",
            children=(),
            progress=progress,
            batch_reference="safe",
            offer_amount=Decimal(2),
            per_gift_required=Decimal("2.01"),
            total_required=Decimal("4.02"),
            buyer_balance=Decimal(10),
        )

    def test_main_menu_contains_russian_tonnel_entry(self) -> None:
        markup = main_menu_keyboard()
        buttons = [button for row in markup.inline_keyboard for button in row]
        tonnel = next(button for button in buttons if button.callback_data == "tonnelx:start")
        self.assertEqual(tonnel.text, "🚇 Tonnel")

    def test_tonnel_confirmation_is_explicit_and_russian(self) -> None:
        markup = transfer_confirmation_keyboard(market="tonnel", nonce="safe")
        texts = [button.text for row in markup.inline_keyboard for button in row]
        callbacks = [
            button.callback_data for row in markup.inline_keyboard for button in row
        ]
        self.assertIn("▶️ Запустить", texts)
        self.assertIn("Отмена", texts)
        self.assertIn("tonnelx:confirm:safe", callbacks)

    def test_tonnel_history_uses_market_name_and_gift_label(self) -> None:
        text = _history_text(
            [
                OperationHistoryItem(
                    operation_id=1,
                    market="tonnel_transfer",
                    account_id=2,
                    display_name="Lol Pop #17",
                    value_text="",
                    status="DRY_RUN",
                    timestamp="2026-09-09 12:00:00",
                )
            ],
            [],
        )
        self.assertIn("Tonnel", text)
        self.assertIn("Lol Pop #17", text)
        self.assertIn("Тестовый запуск", text)

    def test_tonnel_user_messages_do_not_contain_obvious_english_labels(self) -> None:
        source = Path("app/bot/handlers/tonnel_transfers.py").read_text()
        self.assertIn("сумму прямого оффера", source)
        self.assertIn("Прямой оффер", source)
        self.assertIn("Публичное размещение подарка не создаётся", source)
        self.assertIn("Максимальная сумма покупателя", source)
        self.assertIn("Размещение будет публичным", source)
        for label in ("Select gift", "Confirm", "Cancel", "Success", "Failed"):
            self.assertNotIn(f'"{label}', source)

    def test_inventory_failure_does_not_claim_gift_is_missing(self) -> None:
        text = _inventory_error_text(TonnelTransferMode.BUY_OFFER)
        self.assertEqual(text, "Не удалось загрузить инвентарь Tonnel.")
        self.assertNotIn("GiftRelayer", text)

    def test_successful_empty_inventory_can_show_gift_relayer_guidance(self) -> None:
        text = _empty_inventory_text(TonnelTransferMode.BUY_OFFER)
        self.assertIn("@GiftRelayer", text)

    def test_two_successes_are_never_rendered_as_unfinished(self) -> None:
        text = _tonnel_batch_result_text(
            self.batch_result("SUCCESS", "SUCCESS")
        )
        self.assertIn("Успешно: 2", text)
        self.assertIn("Ошибок: 0", text)
        self.assertNotIn("Не завершены", text)
        self.assertNotIn("Требуют проверки:", text)

    def test_success_and_failure_have_exact_disjoint_counts(self) -> None:
        text = _tonnel_batch_result_text(self.batch_result("SUCCESS", "FAILED"))
        self.assertIn("Успешно: 1", text)
        self.assertIn("Ошибок: 1", text)
        self.assertIn("Ошибки:", text)
        self.assertIn("Подарок 2 — операция не подтверждена", text)
        self.assertNotIn("Подарок 1 —", text)

    def test_ambiguous_child_appears_only_under_requires_verification(self) -> None:
        text = _tonnel_batch_result_text(
            self.batch_result("SUCCESS", "AMBIGUOUS")
        )
        self.assertIn("Успешно: 1", text)
        self.assertIn("Ошибок: 0", text)
        self.assertIn("Требуют проверки:", text)
        self.assertIn("Подарок 2 — проверьте операцию", text)
        self.assertNotIn("Ошибки:\n• Подарок 2", text)


if __name__ == "__main__":
    unittest.main()
