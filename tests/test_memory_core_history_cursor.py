import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORE_PATH = ROOT / "cores" / "memory_core.py"


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value
        return True

    def llen(self, key):
        return len(self.lists.get(key, []))

    def lrange(self, key, start, end):
        rows = list(self.lists.get(key, []))
        length = len(rows)
        if start < 0:
            start = max(0, length + start)
        if end < 0:
            end = length + end
        if not rows or start >= length or end < start:
            return []
        return rows[start : min(length, end + 1)]


def _load_memory_core(redis):
    helpers = types.ModuleType("helpers")
    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    helpers.redis_client = redis

    pandas = types.ModuleType("pandas")
    pandas.DataFrame = type("DataFrame", (), {})
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None

    previous_helpers = sys.modules.get("helpers")
    previous_pandas = sys.modules.get("pandas")
    previous_dotenv = sys.modules.get("dotenv")
    sys.modules["helpers"] = helpers
    sys.modules["pandas"] = pandas
    sys.modules["dotenv"] = dotenv
    try:
        spec = importlib.util.spec_from_file_location("memory_core_history_cursor_test", CORE_PATH)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        return module
    finally:
        if previous_helpers is None:
            sys.modules.pop("helpers", None)
        else:
            sys.modules["helpers"] = previous_helpers
        if previous_pandas is None:
            sys.modules.pop("pandas", None)
        else:
            sys.modules["pandas"] = previous_pandas
        if previous_dotenv is None:
            sys.modules.pop("dotenv", None)
        else:
            sys.modules["dotenv"] = previous_dotenv


def _history_row(index):
    return json.dumps(
        {
            "role": "user",
            "user_id": "fred",
            "username": "Fred",
            "content": f"message {index}",
            "timestamp": 1_700_000_000 + index,
        },
        separators=(",", ":"),
    )


class MemoryCoreHistoryCursorTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.core = _load_memory_core(self.redis)
        self.extraction_calls = []

        def extract(_llm_client, **kwargs):
            self.extraction_calls.append(list(kwargs["messages"]))
            return {"user": [], "room": [], "user_removals": [], "room_removals": []}

        self.core._llm_extract_observations = extract
        self.settings = {
            "lookback_limit": 10,
            "min_confidence": 0.65,
            "write_user_memory": True,
            "write_room_memory": True,
            "use_people_identities": False,
        }
        self.history_key = "tater:channel:room-1:history"
        self.scope_id = "room-1"

    def process(self):
        return self.core._process_scope(
            llm_client=object(),
            platform="discord",
            history_key=self.history_key,
            scope_id=self.scope_id,
            settings=self.settings,
        )

    def test_capped_history_resumes_after_the_last_recognized_row(self):
        self.redis.lists[self.history_key] = [_history_row(index) for index in range(5)]

        self.assertEqual(self.process()["processed_messages"], 5)
        cursor_key = self.core.cursor_key("discord", self.scope_id)
        initial_cursor = json.loads(self.redis.get(cursor_key))
        self.assertEqual(initial_cursor["version"], 2)
        self.assertEqual(initial_cursor["position"], 5)

        self.redis.lists[self.history_key] = self.redis.lists[self.history_key][1:] + [_history_row(5)]
        self.assertEqual(self.process()["processed_messages"], 1)
        self.assertEqual(self.extraction_calls[-1][0]["text"], "message 5")

        call_count = len(self.extraction_calls)
        self.assertEqual(self.process()["processed_messages"], 0)
        self.assertEqual(len(self.extraction_calls), call_count)

    def test_legacy_numeric_cursor_recovers_once_then_becomes_stable(self):
        self.redis.lists[self.history_key] = [_history_row(index) for index in range(5)]
        cursor_key = self.core.cursor_key("discord", self.scope_id)
        self.redis.set(cursor_key, "5")

        self.assertEqual(self.process()["processed_messages"], 5)
        migrated_cursor = json.loads(self.redis.get(cursor_key))
        self.assertEqual(migrated_cursor["version"], 2)
        self.assertEqual(migrated_cursor["position"], 5)

        call_count = len(self.extraction_calls)
        self.assertEqual(self.process()["processed_messages"], 0)
        self.assertEqual(len(self.extraction_calls), call_count)

    def test_complete_rollover_recovers_the_newest_bounded_window(self):
        self.redis.lists[self.history_key] = [_history_row(index) for index in range(5)]
        self.assertEqual(self.process()["processed_messages"], 5)

        self.redis.lists[self.history_key] = [_history_row(index) for index in range(20, 25)]
        self.assertEqual(self.process()["processed_messages"], 5)
        self.assertEqual(
            [row["text"] for row in self.extraction_calls[-1]],
            [f"message {index}" for index in range(20, 25)],
        )

    def test_failed_extraction_does_not_advance_the_cursor(self):
        self.redis.lists[self.history_key] = [_history_row(index) for index in range(3)]
        cursor_key = self.core.cursor_key("discord", self.scope_id)
        successful_extract = self.core._llm_extract_observations
        self.core._llm_extract_observations = lambda *_args, **_kwargs: None

        self.assertEqual(self.process()["processed_messages"], 0)
        self.assertIsNone(self.redis.get(cursor_key))

        self.core._llm_extract_observations = successful_extract
        self.assertEqual(self.process()["processed_messages"], 3)

    def test_processing_continues_in_bounded_chunks(self):
        self.redis.lists[self.history_key] = [_history_row(index) for index in range(12)]

        self.assertEqual(self.process()["processed_messages"], 10)
        self.assertEqual(self.process()["processed_messages"], 2)
        self.assertEqual([row["text"] for row in self.extraction_calls[-1]], ["message 10", "message 11"])

    def test_message_id_stays_stable_when_a_row_changes_list_position(self):
        raw_row = _history_row(7)
        first = self.core._normalize_history_entry(
            platform="discord",
            scope_id=self.scope_id,
            index=7,
            raw_entry=raw_row,
            now_ts=1_700_000_100,
        )
        shifted = self.core._normalize_history_entry(
            platform="discord",
            scope_id=self.scope_id,
            index=2,
            raw_entry=raw_row,
            now_ts=1_700_000_100,
        )

        self.assertEqual(first["message_id"], shifted["message_id"])
        self.assertNotIn("_7", first["message_id"])


if __name__ == "__main__":
    unittest.main()
