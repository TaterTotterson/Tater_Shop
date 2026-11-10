import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


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

    def delete(self, *keys):
        deleted = 0
        for key in keys:
            if key in self.values:
                deleted += 1
                self.values.pop(key, None)
        return deleted

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
    async def llm_structured_chat(client, *, messages, **kwargs):
        return await client.chat(messages=messages, **kwargs)

    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    helpers.llm_structured_chat = llm_structured_chat
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


def _irc_history_row(username, content):
    return json.dumps(
        {
            "role": "user",
            "username": username,
            "content": content,
        },
        separators=(",", ":"),
    )


class MemoryCoreHistoryCursorTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.core = _load_memory_core(self.redis)
        self.real_extract = self.core._llm_extract_observations
        self.extraction_calls = []

        def extract(_llm_client, **kwargs):
            self.extraction_calls.append(list(kwargs["messages"]))
            return {"user": [], "room": [], "user_removals": [], "room_removals": []}

        self.core._llm_extract_observations = extract
        self.settings = {
            "interval_seconds": 30,
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

        with patch.object(self.core.time, "time", return_value=100.0):
            first = self.process()
        self.assertEqual(first["processed_messages"], 0)
        self.assertEqual(first["failed_scopes"], 1)
        self.assertIsNone(self.redis.get(cursor_key))

        self.core._llm_extract_observations = successful_extract
        with patch.object(self.core.time, "time", return_value=110.0):
            deferred = self.process()
        self.assertEqual(deferred["processed_messages"], 0)
        self.assertEqual(deferred["deferred_scopes"], 1)

        with patch.object(self.core.time, "time", return_value=131.0):
            self.assertEqual(self.process()["processed_messages"], 3)
        self.assertIsNone(
            self.redis.get(self.core._memory_extraction_failure_key("discord", self.scope_id))
        )

    def test_repeated_failure_is_quarantined_after_three_attempts(self):
        self.redis.lists[self.history_key] = [_history_row(index) for index in range(2)]
        self.core._llm_extract_observations = lambda *_args, **_kwargs: None

        for now_value in (100.0, 131.0, 192.0):
            with patch.object(self.core.time, "time", return_value=now_value):
                self.assertEqual(self.process()["failed_scopes"], 1)

        state = self.core._load_memory_extraction_failure("discord", self.scope_id)
        self.assertEqual(state["attempts"], 3)
        self.assertTrue(state["quarantined"])
        self.assertEqual(state["delay_seconds"], 21_600)

        with patch.object(self.core.time, "time", return_value=1_000.0):
            self.assertEqual(self.process()["deferred_scopes"], 1)

    def test_new_batch_bypasses_an_old_batch_cooldown(self):
        self.redis.lists[self.history_key] = [_history_row(index) for index in range(2)]
        successful_extract = self.core._llm_extract_observations
        self.core._llm_extract_observations = lambda *_args, **_kwargs: None

        with patch.object(self.core.time, "time", return_value=100.0):
            self.assertEqual(self.process()["failed_scopes"], 1)

        self.core._llm_extract_observations = successful_extract
        self.redis.lists[self.history_key].append(_history_row(2))
        with patch.object(self.core.time, "time", return_value=101.0):
            self.assertEqual(self.process()["processed_messages"], 3)

    def test_pending_signature_ignores_a_batch_during_failure_cooldown(self):
        self.redis.lists[self.history_key] = [_history_row(index) for index in range(2)]
        signature = self.core._memory_extraction_batch_signature(self.redis.lists[self.history_key])
        self.core._record_memory_extraction_failure(
            "discord",
            self.scope_id,
            signature,
            interval_seconds=30,
            error="schema mismatch",
            now_ts=100.0,
        )
        self.core._resolve_enabled_platforms = lambda: ["discord"]
        self.core._discover_scopes = lambda _platform: [(self.history_key, self.scope_id)]

        with patch.object(self.core.time, "time", return_value=110.0):
            self.assertEqual(self.core._pending_memory_history_signature(lookback_limit=10), ())
        with patch.object(self.core.time, "time", return_value=131.0):
            self.assertEqual(len(self.core._pending_memory_history_signature(lookback_limit=10)), 1)

    def test_irc_status_and_ctcp_noise_advance_without_using_the_llm(self):
        history_key = "tater:irc:taters:history"
        scope_id = "taters"
        self.redis.lists[history_key] = [
            _irc_history_row("*status", "Connected!"),
            _irc_history_row("DM", "\x01VERSION\x01"),
            _irc_history_row("*status", "IRC connection timed out. Reconnecting..."),
        ]

        self.core._resolve_enabled_platforms = lambda: ["irc"]
        self.core._discover_scopes = lambda _platform: [(history_key, scope_id)]
        self.assertEqual(self.core._pending_memory_history_signature(lookback_limit=10), ())

        result = self.core._process_scope(
            llm_client=object(),
            platform="irc",
            history_key=history_key,
            scope_id=scope_id,
            settings=self.settings,
        )

        self.assertEqual(result["processed_messages"], 0)
        self.assertEqual(self.extraction_calls, [])
        cursor = json.loads(self.redis.get(self.core.cursor_key("irc", scope_id)))
        self.assertEqual(cursor["position"], 3)

    def test_irc_mixed_batch_only_sends_real_conversation_to_the_llm(self):
        history_key = "tater:irc:taters:history"
        scope_id = "taters"
        self.redis.lists[history_key] = [
            _irc_history_row("*status", "Connected!"),
            _irc_history_row("DM", "\x01VERSION\x01"),
            _irc_history_row("SpudLord", "remember that my favorite color is orange"),
        ]

        result = self.core._process_scope(
            llm_client=object(),
            platform="irc",
            history_key=history_key,
            scope_id=scope_id,
            settings=self.settings,
        )

        self.assertEqual(result["processed_messages"], 1)
        self.assertEqual(
            [row["text"] for row in self.extraction_calls[-1]],
            ["remember that my favorite color is orange"],
        )
        cursor = json.loads(self.redis.get(self.core.cursor_key("irc", scope_id)))
        self.assertEqual(cursor["position"], 3)

    def test_irc_noise_does_not_reset_the_quiet_period_for_real_chat(self):
        history_key = "tater:irc:taters:history"
        scope_id = "taters"
        self.redis.lists[history_key] = [
            _irc_history_row("SpudLord", "remember the garage code changed"),
            _irc_history_row("*status", "Connected!"),
        ]
        self.core._resolve_enabled_platforms = lambda: ["irc"]
        self.core._discover_scopes = lambda _platform: [(history_key, scope_id)]

        first = self.core._pending_memory_history_signature(lookback_limit=10)
        self.redis.lists[history_key].extend(
            [
                _irc_history_row("DM", "\x01VERSION\x01"),
                _irc_history_row("*status", "Disconnected from IRC. Reconnecting..."),
            ]
        )
        second = self.core._pending_memory_history_signature(lookback_limit=10)

        self.assertEqual(first, second)
        self.assertEqual(len(second), 1)

    def test_extraction_schema_accepts_omitted_optional_sections_and_fields(self):
        captured = {}

        async def structured_chat(_client, **kwargs):
            captured["schema"] = kwargs["schema"]
            return {"message": {"content": '{"user_observations":[]}'}}

        self.core.llm_structured_chat = structured_chat
        self.core._llm_extract_observations = self.real_extract
        result = self.core._llm_extract_observations(
            object(),
            platform="discord",
            scope_id=self.scope_id,
            messages=[
                {
                    "role": "user",
                    "user_id": "fred",
                    "user_name": "Fred",
                    "text": "hello",
                    "timestamp": 100.0,
                    "message_id": "m1",
                }
            ],
        )

        self.assertEqual(result["user"], [])
        self.assertEqual(result["user_episodes"], [])
        self.assertEqual(captured["schema"]["required"], [])
        user_required = captured["schema"]["properties"]["user_observations"]["items"]["required"]
        self.assertEqual(user_required, ["user_id", "candidate_key", "value"])

    def test_cycle_stops_after_interactive_request_preempts_extraction(self):
        second_history_key = "tater:channel:room-2:history"
        self.redis.lists[self.history_key] = [_history_row(1)]
        self.redis.lists[second_history_key] = [_history_row(2)]
        extraction_calls = []

        def preempted_extract(_llm_client, **kwargs):
            extraction_calls.append(kwargs["scope_id"])
            return self.core._MemoryExtractionFailure(
                "A newer interactive request replaced this model task.",
                preempted=True,
            )

        self.core._llm_extract_observations = preempted_extract
        self.core._resolve_enabled_platforms = lambda: ["discord"]
        self.core._discover_scopes = lambda _platform: [
            (self.history_key, self.scope_id),
            (second_history_key, "room-2"),
        ]

        stats = self.core._run_cycle(object(), self.settings)

        self.assertEqual(stats["preempted"], 1)
        self.assertEqual(stats["scanned_scopes"], 1)
        self.assertEqual(extraction_calls, [self.scope_id])

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
