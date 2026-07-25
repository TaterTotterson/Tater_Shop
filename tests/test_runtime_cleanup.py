from __future__ import annotations

import asyncio
import ast
import json
import threading
import time
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock


def _load_core(name: str):
    path = Path(__file__).resolve().parents[1] / "cores" / f"{name}.py"
    function_name = {
        "awareness_core": "_trim_events_for_source",
        "rss_core": "_interruptible_poll_sleep",
    }[name]
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    selected = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == function_name
    ]
    if not selected:
        raise RuntimeError(f"Unable to find {function_name} in {path}")

    module = types.ModuleType(f"test_{name}")
    module.__dict__.update(
        {
            "asyncio": asyncio,
            "datetime": datetime,
            "timedelta": timedelta,
            "json": json,
            "logger": mock.Mock(),
            "redis_client": None,
        }
    )
    if name == "awareness_core":
        module._events_retention_seconds = lambda _client: 7 * 86400
        module._event_key = lambda source: f"tater:automations:events:{source}"

        def _parse_iso(value):
            try:
                return datetime.fromisoformat(str(value))
            except Exception:
                return None

        module._parse_iso = _parse_iso
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
    code = compile(module_tree, str(path), "exec")
    exec(code, module.__dict__)
    return module


class _EventRedis:
    def __init__(self, rows: list[str]):
        self.rows = rows
        self.trim_calls: list[tuple[str, int, int]] = []
        self.delete_calls: list[str] = []

    def lrange(self, _key: str, _start: int, _end: int) -> list[str]:
        return list(self.rows)

    def ltrim(self, key: str, start: int, end: int) -> None:
        self.trim_calls.append((key, start, end))

    def delete(self, key: str) -> None:
        self.delete_calls.append(key)


class AwarenessRetentionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.core = _load_core("awareness_core")

    def test_retention_trims_expired_tail_without_rewriting_kept_events(self) -> None:
        now = datetime.now()
        rows = [
            json.dumps({"ha_time": now.isoformat()}),
            json.dumps({"ha_time": (now - timedelta(days=10)).isoformat()}),
            json.dumps({"ha_time": (now - timedelta(days=11)).isoformat()}),
        ]
        fake = _EventRedis(rows)
        with mock.patch.object(self.core, "_events_retention_seconds", return_value=7 * 86400):
            self.core._trim_events_for_source(fake, "camera")
        self.assertEqual(
            fake.trim_calls,
            [("tater:automations:events:camera", 0, 0)],
        )
        self.assertEqual(fake.delete_calls, [])

    def test_retention_does_not_write_when_nothing_expired(self) -> None:
        now = datetime.now()
        fake = _EventRedis(
            [
                json.dumps({"ha_time": now.isoformat()}),
                json.dumps({"ha_time": (now - timedelta(minutes=5)).isoformat()}),
            ]
        )
        with mock.patch.object(self.core, "_events_retention_seconds", return_value=7 * 86400):
            self.core._trim_events_for_source(fake, "camera")
        self.assertEqual(fake.trim_calls, [])
        self.assertEqual(fake.delete_calls, [])


class RssShutdownTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.core = _load_core("rss_core")

    async def test_poll_sleep_stops_promptly(self) -> None:
        stop_event = threading.Event()
        started = time.monotonic()
        task = asyncio.create_task(self.core._interruptible_poll_sleep(stop_event, 60))
        await asyncio.sleep(0.05)
        stop_event.set()
        await asyncio.wait_for(task, timeout=1.0)
        self.assertLess(time.monotonic() - started, 1.0)


if __name__ == "__main__":
    unittest.main()
