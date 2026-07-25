from __future__ import annotations

import ast
import re
import time
import types
import unittest
from pathlib import Path


def _load_delivery_helpers():
    path = Path(__file__).resolve().parents[1] / "cores" / "personal_core.py"
    names = {
        "_row_merge_preserving_id",
        "_delivery_dedupe_key",
        "_delivery_status_rank",
        "_merge_delivery_timestamps",
        "_dedupe_deliveries",
        "_repair_delivery_row_fields",
        "_delivery_is_stale",
        "_cleanup_deliveries",
    }
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    selected = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
    ]
    if len(selected) != len(names):
        found = {node.name for node in selected}
        raise RuntimeError(f"Missing delivery helpers: {sorted(names - found)}")

    def as_float(value, default=0.0, *, minimum=None, maximum=None):
        try:
            result = float(value)
        except (TypeError, ValueError):
            result = float(default)
        if minimum is not None:
            result = max(float(minimum), result)
        if maximum is not None:
            result = min(float(maximum), result)
        return result

    def text(value):
        return str(value or "").strip()

    def normalize_reference(value):
        return re.sub(r"\s+", "", text(value)).strip("#.,:;()[]{}<>")

    module = types.ModuleType("test_personal_delivery_helpers")
    module.__dict__.update(
        {
            "time": time,
            "_as_float": as_float,
            "_text": text,
            "_slug": lambda value, default="": text(value).lower().replace(" ", "_") or default,
            "_normalize_tracking_id": normalize_reference,
            "_normalize_order_number": normalize_reference,
            "_looks_like_order_reference": lambda _value: False,
            "_looks_like_tracking_reference": lambda _value: False,
            "_clean_text_blob": lambda value, max_chars=160: text(value)[:max_chars],
            "_extract_delivery_item_from_text": lambda *_values: "",
            "_dedupe_token": lambda value: re.sub(r"[^a-z0-9]+", "_", text(value).lower()).strip("_"),
            "_DELIVERY_DELIVERED_RETENTION_SECONDS": 7 * 86400,
            "_DELIVERY_OPEN_RETENTION_SECONDS": 45 * 86400,
            "_DELIVERY_PAST_ETA_GRACE_SECONDS": 3 * 86400,
        }
    )
    module_tree = ast.Module(
        body=[
            ast.ImportFrom(
                module="__future__",
                names=[ast.alias(name="annotations")],
                level=0,
            ),
            *selected,
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(module_tree)
    exec(compile(module_tree, str(path), "exec"), module.__dict__)
    return module


class PersonalDeliveryRetentionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.core = _load_delivery_helpers()
        cls.now = 2_000_000_000.0

    @staticmethod
    def _row(**overrides):
        row = {
            "id": "delivery-1",
            "tracking_id": "TRACK123456",
            "status": "in_transit",
            "confidence": 0.8,
            "eta_ts": 0.0,
        }
        row.update(overrides)
        return row

    def test_open_delivery_expires_after_45_days_without_an_update(self) -> None:
        rows = self.core._cleanup_deliveries(
            [self._row(first_seen_ts=self.now - 50 * 86400, last_seen_ts=self.now - 46 * 86400)],
            max_items=500,
            now_ts=self.now,
        )
        self.assertEqual(rows, [])

    def test_recent_open_delivery_is_kept(self) -> None:
        rows = self.core._cleanup_deliveries(
            [self._row(first_seen_ts=self.now - 30 * 86400, last_seen_ts=self.now - 2 * 86400)],
            max_items=500,
            now_ts=self.now,
        )
        self.assertEqual(len(rows), 1)

    def test_old_eta_expires_when_no_later_update_arrived(self) -> None:
        eta_ts = self.now - 4 * 86400
        rows = self.core._cleanup_deliveries(
            [self._row(eta_ts=eta_ts, first_seen_ts=eta_ts - 4 * 86400, last_seen_ts=eta_ts)],
            max_items=500,
            now_ts=self.now,
        )
        self.assertEqual(rows, [])

    def test_update_after_old_eta_keeps_delayed_delivery(self) -> None:
        eta_ts = self.now - 30 * 86400
        rows = self.core._cleanup_deliveries(
            [self._row(status="exception", eta_ts=eta_ts, first_seen_ts=eta_ts - 3 * 86400, last_seen_ts=self.now - 86400)],
            max_items=500,
            now_ts=self.now,
        )
        self.assertEqual(len(rows), 1)

    def test_delivered_delivery_is_removed_after_seven_days(self) -> None:
        rows = self.core._cleanup_deliveries(
            [self._row(status="delivered", first_seen_ts=self.now - 20 * 86400, last_seen_ts=self.now - 8 * 86400)],
            max_items=500,
            now_ts=self.now,
        )
        self.assertEqual(rows, [])

    def test_legacy_delivery_without_dates_gets_a_retention_anchor(self) -> None:
        rows = self.core._cleanup_deliveries(
            [self._row()],
            max_items=500,
            now_ts=self.now,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["first_seen_ts"], self.now)
        self.assertEqual(rows[0]["last_seen_ts"], self.now)

    def test_delivered_update_wins_over_prior_exception_and_preserves_lifecycle(self) -> None:
        rows = self.core._dedupe_deliveries(
            [
                self._row(
                    id="original",
                    status="exception",
                    first_seen_ts=self.now - 10 * 86400,
                    last_seen_ts=self.now - 2 * 86400,
                ),
                self._row(
                    id="delivered-email",
                    status="delivered",
                    first_seen_ts=self.now - 86400,
                    last_seen_ts=self.now - 86400,
                ),
            ],
            max_items=500,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "original")
        self.assertEqual(rows[0]["status"], "delivered")
        self.assertEqual(rows[0]["first_seen_ts"], self.now - 10 * 86400)
        self.assertEqual(rows[0]["last_seen_ts"], self.now - 86400)


if __name__ == "__main__":
    unittest.main()
