import importlib.util
import asyncio
import json
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class FakeRedis:
    def __init__(self, values=None):
        self.values = values or {}
        self.hashes = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value

    def hgetall(self, key):
        return dict(self.hashes.get(key) or {})

    def hset(self, key, mapping=None, **_kwargs):
        self.hashes.setdefault(key, {}).update(mapping or {})


def load_tater_tube_core():
    helpers = types.ModuleType("helpers")
    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    helpers.redis_client = FakeRedis()
    sys.modules["helpers"] = helpers
    requests = types.ModuleType("requests")
    requests.request = lambda *_args, **_kwargs: None
    requests.get = lambda *_args, **_kwargs: None
    sys.modules["requests"] = requests

    path = Path(__file__).resolve().parents[1] / "cores" / "tater_tube_core.py"
    spec = importlib.util.spec_from_file_location("tater_tube_core_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class TaterTubeCoreAssistantNameTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_tater_tube_core()

    def test_reads_only_the_configured_first_name(self):
        client = FakeRedis({"tater:first_name": "  Totty Totterson  "})
        self.assertEqual(self.core._assistant_first_name(client), "Totty")

    def test_defaults_to_tater(self):
        self.assertEqual(self.core._assistant_first_name(FakeRedis()), "Tater")

    def test_local_moment_includes_time_day_and_holiday_context(self):
        moment = self.core._local_moment(
            datetime(2026, 12, 20, 19, 30, tzinfo=timezone.utc)
        )
        self.assertEqual(moment["weekday"], "Sunday")
        self.assertEqual(moment["time_of_day"], "evening")
        self.assertEqual(moment["day_kind"], "weekend")
        self.assertFalse(moment["weekend_morning"])
        self.assertEqual(moment["season"], "winter")
        self.assertEqual(moment["nearby_occasion"], "winter holiday season")

    def test_local_moment_marks_weekend_mornings(self):
        moment = self.core._local_moment(
            datetime(2026, 9, 13, 8, 30, tzinfo=timezone.utc)
        )
        self.assertEqual(moment["weekday"], "Sunday")
        self.assertEqual(moment["time_of_day"], "morning")
        self.assertTrue(moment["weekend_morning"])

    def test_sends_a_url_encoded_unicode_name_header(self):
        response = types.SimpleNamespace(
            ok=True,
            status_code=200,
            json=lambda: {"data": {"ok": True}},
        )
        client = FakeRedis({"tater:first_name": "José Totterson"})
        with patch.object(self.core.requests, "request", return_value=response) as request:
            self.core._api_request(
                "GET",
                "tater/core/context",
                server_url="https://tatertube.example",
                token="secret",
                redis_obj=client,
            )
        headers = request.call_args.kwargs["headers"]
        self.assertEqual(headers["X-Tater-Assistant-Name"], "Jos%C3%A9")

    def test_reloads_the_name_for_each_server_request(self):
        response = types.SimpleNamespace(
            ok=True,
            status_code=200,
            json=lambda: {"data": {"ok": True}},
        )
        client = FakeRedis({"tater:first_name": "Tater"})
        with patch.object(self.core.requests, "request", return_value=response) as request:
            self.core._api_request(
                "GET",
                "tater/core/context",
                server_url="https://tatertube.example",
                token="secret",
                redis_obj=client,
            )
            client.values["tater:first_name"] = "Totty"
            self.core._api_request(
                "GET",
                "tater/core/context",
                server_url="https://tatertube.example",
                token="secret",
                redis_obj=client,
            )
        self.assertEqual(
            request.call_args_list[-1].kwargs["headers"]["X-Tater-Assistant-Name"],
            "Totty",
        )

    def test_combines_server_and_music_activity_in_recency_order(self):
        client = FakeRedis(
            {
                self.core.SHARED_ACTIVITY_KEY: json.dumps(
                    [
                        {
                            "source": "music_core",
                            "media_id": "song-1",
                            "media_type": "music",
                            "title": "Three Little Birds",
                            "occurred_at": 200,
                            "metadata": {"artist": "Bob Marley", "action": "played"},
                        }
                    ]
                )
            }
        )
        context = {
            "events": [
                {
                    "source": "game_center",
                    "media_id": "game-1",
                    "media_type": "game",
                    "title": "Super Mario 64",
                    "occurred_at": "1970-01-01T00:01:40Z",
                }
            ]
        }
        rows = self.core._combined_activity(context, client)
        self.assertEqual([row["title"] for row in rows], ["Three Little Birds", "Super Mario 64"])

    def test_extracts_only_safe_module_capabilities(self):
        rows = [
            {
                "media_type": "session",
                "metadata_json": json.dumps(
                    {
                        "modules": [
                            {"id": "com.240mp.retro", "name": "Game Center", "configured": True}
                        ],
                        "token": "must-not-escape",
                    }
                ),
            }
        ]
        self.assertEqual(
            self.core._available_modules(rows),
            [{"id": "com.240mp.retro", "name": "Game Center", "configured": True}],
        )

    def test_main_menu_fallback_prefers_global_activity_over_tater_picks(self):
        message, suggestion = self.core._main_menu_fallback(
            [
                {
                    "source": "game_center",
                    "media_type": "game",
                    "title": "Super Mario 64",
                }
            ],
            [{"title": "Server Movie", "media_type": "movie"}],
            [],
        )
        self.assertEqual(suggestion["title"], "Super Mario 64")
        self.assertEqual(suggestion["kind"], "play")
        self.assertIn("Super Mario 64", message)

    def test_global_main_menu_message_is_separate_from_server_picks(self):
        client = FakeRedis()
        client.hset(
            self.core.SETTINGS_KEY,
            mapping={"server_url": "http://tube.local", "token": "secret"},
        )
        client.set(
            self.core.CONTEXT_KEY,
            json.dumps(
                {
                    "events": [
                        {
                            "source": "local_media",
                            "media_id": "movie-history-1",
                            "media_type": "movie",
                            "title": "Server Movie",
                            "state": "completed",
                            "occurred_at": "2026-08-09T12:00:00Z",
                        },
                        {
                            "source": "game_center",
                            "media_id": "game-1",
                            "media_type": "game",
                            "title": "Super Mario 64",
                            "state": "started",
                            "occurred_at": "2026-08-10T12:00:00Z",
                            "metadata_json": json.dumps({"action": "launched"}),
                        }
                    ]
                }
            ),
        )

        class FakeLLM:
            def __init__(self):
                self.payloads = []

            async def chat(self, **kwargs):
                payload = json.loads(kwargs["messages"][1]["content"])
                self.payloads.append(payload)
                if "server_catalog_candidates" in payload:
                    body = {
                        "summary": "I made a fresh server-library mix.",
                        "picks_briefing": (
                            "You have been returning to easygoing movies, so I grouped "
                            "together another relaxed set for your next watch."
                        ),
                        "items": [
                            {"candidate_id": "movie-1", "reason": "A good next watch."}
                        ],
                    }
                else:
                    body = {
                        "message": (
                            "You have been enjoying Super Mario 64. "
                            "My global suggestion is to play Super Mario 64 again."
                        ),
                        "suggestion": {
                            "title": "Super Mario 64",
                            "kind": "play",
                            "source": "game_center",
                        },
                    }
                return {
                    "message": {
                        "content": json.dumps(body)
                    }
                }

        published_payload = {}

        def fake_api(method, path, **kwargs):
            if path.startswith("tater/core/candidates"):
                return {
                    "candidates": [
                        {
                            "id": "movie-1",
                            "title": "A Movie",
                            "media_type": "movie",
                            "source": "local_media",
                        }
                    ]
                }
            if path == "tater/core/recommendations":
                published_payload.update(kwargs["payload"])
                return {"batch_id": "batch-1"}
            raise AssertionError((method, path))

        loop = asyncio.new_event_loop()
        llm = FakeLLM()
        try:
            with patch.object(self.core, "_api_request", side_effect=fake_api):
                result = self.core._generate_recommendations_impl(loop, llm, client)
        finally:
            loop.close()

        self.assertIn("Super Mario 64", result["boot_summary"])
        self.assertEqual(published_payload["boot_summary"], result["boot_summary"])
        self.assertEqual(
            published_payload["picks_briefing"], result["picks_briefing"]
        )
        self.assertIn("easygoing movies", result["picks_briefing"])
        self.assertNotEqual(result["picks_briefing"], result["summary"])
        self.assertEqual(len(llm.payloads), 2)
        self.assertIn("local_moment", llm.payloads[0])
        self.assertIn("weekday", llm.payloads[0]["local_moment"])
        pick_titles = [row["title"] for row in llm.payloads[0]["recent_server_viewing"]]
        global_titles = [row["title"] for row in llm.payloads[1]["recent_global_activity"]]
        self.assertEqual(pick_titles, ["Server Movie"])
        self.assertIn("Super Mario 64", global_titles)
        self.assertIn("Server Movie", global_titles)
        saved_main_menu = json.loads(client.get(self.core.MAIN_MENU_MESSAGE_KEY))
        self.assertEqual(saved_main_menu["suggestion"]["kind"], "play")

    def test_recommendations_rotate_previous_picks_and_request_weekend_balance(self):
        client = FakeRedis()
        client.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "server_url": "http://tube.local",
                "token": "secret",
                "recommendation_count": "1",
            },
        )
        client.set(self.core.CONTEXT_KEY, json.dumps({"events": []}))
        client.set(
            self.core.RECOMMENDATIONS_KEY,
            json.dumps(
                {
                    "items": [
                        {
                            "candidate_id": "old-movie",
                            "title": "Yesterday's Pick",
                            "media_type": "movie",
                        }
                    ]
                }
            ),
        )

        class FakeLLM:
            def __init__(self):
                self.calls = []

            async def chat(self, **kwargs):
                system = kwargs["messages"][0]["content"]
                payload = json.loads(kwargs["messages"][1]["content"])
                self.calls.append((system, payload))
                if "server_catalog_candidates" in payload:
                    return {
                        "message": {
                            "content": json.dumps(
                                {
                                    "summary": "A colorful mix is ready.",
                                    "picks_briefing": "These animated adventures and comedies make a playful mix.",
                                    "items": [
                                        {
                                            "candidate_id": "new-series",
                                            "reason": "A lively animated adventure.",
                                        }
                                    ],
                                }
                            )
                        }
                    }
                return {
                    "message": {
                        "content": json.dumps(
                            {"message": "Try New Series.", "suggestion": {"title": "New Series"}}
                        )
                    }
                }

        candidate_paths = []

        def fake_api(method, path, **kwargs):
            if path.startswith("tater/core/candidates"):
                candidate_paths.append(path)
                return {
                    "candidates": [
                        {
                            "id": "new-series",
                            "title": "New Series",
                            "media_type": "series",
                            "source": "local_media",
                            "genres": ["Animation", "Family"],
                        },
                        {
                            "id": "old-movie",
                            "title": "Yesterday's Pick",
                            "media_type": "movie",
                            "source": "local_media",
                            "genres": ["Comedy"],
                        },
                    ]
                }
            if path == "tater/core/recommendations":
                return {"batch_id": "batch-2"}
            raise AssertionError((method, path, kwargs))

        weekend_morning = {
            "local_datetime": "2026-09-13T08:30-05:00",
            "weekday": "Sunday",
            "time_of_day": "morning",
            "day_kind": "weekend",
            "weekend_morning": True,
            "season": "fall",
            "nearby_occasion": "",
        }
        loop = asyncio.new_event_loop()
        llm = FakeLLM()
        try:
            with patch.object(self.core, "_api_request", side_effect=fake_api), patch.object(
                self.core, "_local_moment", return_value=weekend_morning
            ):
                self.core._generate_recommendations_impl(loop, llm, client)
        finally:
            loop.close()

        self.assertEqual(len(candidate_paths), 1)
        self.assertIn("weekend_morning=1", candidate_paths[0])
        self.assertIn("exclude_ids=old-movie", candidate_paths[0])
        pick_system, pick_payload = llm.calls[0]
        self.assertIn("cartoons, animation, family", pick_system)
        self.assertIn("never mention a weekday", pick_system)
        self.assertEqual(
            pick_payload["previous_recommendation_picks"][0]["candidate_id"],
            "old-movie",
        )
        self.assertEqual(
            [row["id"] for row in pick_payload["server_catalog_candidates"]],
            ["new-series"],
        )
        self.assertEqual(
            pick_payload["server_catalog_candidates"][0]["genres"],
            ["Animation", "Family"],
        )

    def test_recommendation_generation_requires_a_distinct_picks_briefing(self):
        client = FakeRedis()
        client.hset(
            self.core.SETTINGS_KEY,
            mapping={"server_url": "http://tube.local", "token": "secret"},
        )
        client.set(self.core.CONTEXT_KEY, json.dumps({"events": []}))

        def fake_api(method, path, **_kwargs):
            if path.startswith("tater/core/candidates"):
                return {
                    "candidates": [
                        {
                            "id": "movie-1",
                            "title": "A Movie",
                            "media_type": "movie",
                            "source": "local_media",
                        }
                    ]
                }
            raise AssertionError((method, path))

        model_result = {
            "summary": "A fresh mix is ready.",
            "items": [{"candidate_id": "movie-1", "reason": "A good next watch."}],
        }
        loop = asyncio.new_event_loop()
        try:
            with patch.object(self.core, "_api_request", side_effect=fake_api), patch.object(
                self.core, "_llm_json", return_value=model_result
            ):
                with self.assertRaisesRegex(RuntimeError, "group briefing"):
                    self.core._generate_recommendations_impl(loop, object(), client)
        finally:
            loop.close()

    def test_core_ui_has_a_dedicated_main_menu_message_section(self):
        client = FakeRedis(
            {
                self.core.MAIN_MENU_MESSAGE_KEY: json.dumps(
                    {
                        "generated_at": 100,
                        "message": "Try Super Mario 64 next.",
                        "suggestion": {
                            "title": "Super Mario 64",
                            "kind": "play",
                            "source": "game_center",
                        },
                    }
                )
            }
        )
        data = self.core.get_htmlui_tab_data(redis_client=client)
        tabs = data["ui"]["manager_tabs"]
        self.assertIn("main_menu", [row["key"] for row in tabs])
        forms = data["ui"]["item_forms"]
        message = next(row for row in forms if row["id"] == "main_menu:current")
        self.assertEqual(message["detail"], "Try Super Mario 64 next.")
        self.assertTrue(any(row["id"] == "main_menu:context" for row in forms))


if __name__ == "__main__":
    unittest.main()
