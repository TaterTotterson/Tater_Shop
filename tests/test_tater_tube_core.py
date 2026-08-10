import importlib.util
import asyncio
import json
import sys
import types
import unittest
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

    def test_recommendation_publish_includes_dedicated_boot_summary(self):
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
            async def chat(self, **_kwargs):
                return {
                    "message": {
                        "content": json.dumps(
                            {
                                "summary": "I made a fresh screen-time mix.",
                                "boot_summary": (
                                    "You have been enjoying Super Mario 64. "
                                    "A Movie could be a fun change of pace next."
                                ),
                                "items": [
                                    {"candidate_id": "movie-1", "reason": "A good next watch."}
                                ],
                            }
                        )
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
        try:
            with patch.object(self.core, "_api_request", side_effect=fake_api):
                result = self.core._generate_recommendations_impl(loop, FakeLLM(), client)
        finally:
            loop.close()

        self.assertIn("Super Mario 64", result["boot_summary"])
        self.assertEqual(published_payload["boot_summary"], result["boot_summary"])


if __name__ == "__main__":
    unittest.main()
