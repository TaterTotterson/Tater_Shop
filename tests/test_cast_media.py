from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "verba" / "cast_media.py"


def _action_success(**kwargs):
    return {"ok": True, **kwargs}


def _action_failure(*, code, message, **kwargs):
    return {"ok": False, "error": {"code": code, "message": message}, **kwargs}


def _load_module():
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})

    verba_result = types.ModuleType("verba_result")
    verba_result.action_success = _action_success
    verba_result.action_failure = _action_failure

    helpers = types.ModuleType("helpers")
    helpers.redis_blob_client = SimpleNamespace(get=lambda _key: None)

    registry = types.ModuleType("integration_registry")
    registry.get_integration_devices_by_capability = lambda *_args, **_kwargs: []
    registry.get_integration_room_preferred_media_player = lambda *_args, **_kwargs: {}

    spec = importlib.util.spec_from_file_location("cast_media_test_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {
            "helpers": helpers,
            "integration_registry": registry,
            "verba_base": verba_base,
            "verba_result": verba_result,
        },
    ):
        spec.loader.exec_module(module)
    return module


class CastMediaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_module()

    def setUp(self):
        self.plugin = self.module.CastMediaPlugin()
        self.office_tv = {
            "integration_id": "google_cast",
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "Office TV",
            "room": "Office",
            "actions": ["play_media", "play_url"],
        }
        self.living_tv = {
            "integration_id": "google_cast",
            "id": "22222222-2222-2222-2222-222222222222",
            "name": "Living Room TV",
            "room": "Living Room",
            "actions": ["play_media", "play_url"],
        }

    def test_metadata_routes_tv_playback_and_media_chaining(self):
        self.assertTrue(self.plugin.when_to_use.startswith("Play media on a TV or cast"))
        self.assertIn("after another Verba creates or returns media", self.plugin.description)
        self.assertIn('"query":"Play the generated song on the office TV"', self.plugin.usage)
        self.assertIn('"artifact_id"', self.plugin.usage)
        self.assertEqual(self.plugin.argument_schema["required"], ["query"])
        self.assertEqual(self.plugin.version, "1.0.1")
        self.assertEqual(self.plugin.min_tater_version, "197")

    def test_generated_audio_artifact_is_played_on_named_tv(self):
        playback = Mock(return_value={"ok": True, "sent_count": 1})
        media_playback = types.ModuleType("media_playback")
        media_playback.play_media_url_targets = playback
        args = {
            "query": "Play the song on the office TV at 35 percent",
            "artifact_id": "att7",
            "origin": {
                "available_artifacts": [
                    {
                        "artifact_id": "att7",
                        "type": "audio",
                        "name": "ace_song.mp3",
                        "mimetype": "audio/mpeg",
                        "blob_key": "tater:blob:song",
                    }
                ]
            },
        }

        with patch.object(
            self.module,
            "get_integration_devices_by_capability",
            return_value=[self.office_tv, self.living_tv],
        ), patch.object(
            self.module.redis_blob_client,
            "get",
            return_value=b"generated-song",
        ), patch.dict(sys.modules, {"media_playback": media_playback}):
            result = asyncio.run(self.plugin.handle_webui(args, None))

        self.assertTrue(result["ok"])
        self.assertEqual(result["facts"]["device_name"], "Office TV")
        self.assertEqual(result["facts"]["artifact_id"], "att7")
        playback.assert_called_once()
        self.assertEqual(
            playback.call_args.args[0],
            "integration:google_cast:11111111-1111-1111-1111-111111111111",
        )
        self.assertEqual(playback.call_args.kwargs["audio_bytes"], b"generated-song")
        self.assertEqual(playback.call_args.kwargs["volume_percent"], 35)

    def test_previous_verba_media_url_is_used_when_no_artifact_exists(self):
        playback = Mock(return_value={"ok": True, "sent_count": 1})
        media_playback = types.ModuleType("media_playback")
        media_playback.play_media_url_targets = playback
        args = {
            "query": "Cast the new song to the office TV",
            "origin": {
                "tool_results_full": [
                    {
                        "ok": True,
                        "facts": {
                            "media_url": "http://192.168.1.50:8188/view?filename=ace_song.mp3"
                        },
                    }
                ]
            },
        }

        with patch.object(
            self.module,
            "get_integration_devices_by_capability",
            return_value=[self.office_tv],
        ), patch.dict(sys.modules, {"media_playback": media_playback}):
            result = asyncio.run(self.plugin.handle_webui(args, None))

        self.assertTrue(result["ok"])
        self.assertEqual(result["facts"]["resolved_from"], "previous_tool_result")
        self.assertEqual(
            playback.call_args.args[1],
            "http://192.168.1.50:8188/view?filename=ace_song.mp3",
        )

    def test_latest_playable_artifact_is_used_without_explicit_artifact_id(self):
        playback = Mock(return_value={"ok": True, "sent_count": 1})
        media_playback = types.ModuleType("media_playback")
        media_playback.play_media_url_targets = playback
        args = {
            "query": "Play the song you just made on the office TV",
            "origin": {
                "available_artifacts": [
                    {
                        "artifact_id": "att9",
                        "type": "audio",
                        "name": "latest_song.mp3",
                        "mimetype": "audio/mpeg",
                        "blob_key": "tater:blob:latest-song",
                    }
                ],
                "tool_results_full": [
                    {
                        "ok": True,
                        "facts": {
                            "media_url": "http://127.0.0.1:8188/view?filename=latest_song.mp3"
                        },
                    }
                ],
            },
        }

        with patch.object(
            self.module,
            "get_integration_devices_by_capability",
            return_value=[self.office_tv],
        ), patch.object(
            self.module.redis_blob_client,
            "get",
            return_value=b"latest-generated-song",
        ), patch.dict(sys.modules, {"media_playback": media_playback}):
            result = asyncio.run(self.plugin.handle_webui(args, None))

        self.assertTrue(result["ok"])
        self.assertEqual(result["facts"]["artifact_id"], "att9")
        self.assertEqual(result["facts"]["resolved_from"], "artifact")
        self.assertEqual(playback.call_args.args[1], "")
        self.assertEqual(playback.call_args.kwargs["audio_bytes"], b"latest-generated-song")

    def test_room_preferred_cast_target_wins_when_room_has_multiple_devices(self):
        preferred = {
            "room_id": "office",
            "target": "integration:google_cast:11111111-1111-1111-1111-111111111111",
        }
        with patch.object(
            self.module,
            "get_integration_room_preferred_media_player",
            return_value=preferred,
        ):
            selected, error = self.plugin._select_device(
                [self.office_tv, {**self.living_tv, "name": "Office Speaker", "room": "Office"}],
                args={"target": "office"},
                context=None,
                query="Play it in the office",
            )

        self.assertEqual(error, "")
        self.assertEqual(selected["name"], "Office TV")

    def test_ambiguous_room_returns_device_choices(self):
        office_speaker = {**self.living_tv, "name": "Office Speaker", "room": "Office"}
        selected, error = self.plugin._select_device(
            [self.office_tv, office_speaker],
            args={"target": "office"},
            context=None,
            query="Play it in the office",
        )

        self.assertIsNone(selected)
        self.assertIn("ambiguous", error.lower())
        self.assertIn("Office TV", error)
        self.assertIn("Office Speaker", error)

    def test_missing_media_fails_before_cast_discovery(self):
        inventory = Mock(return_value=[self.office_tv])
        with patch.object(self.module, "get_integration_devices_by_capability", inventory):
            result = asyncio.run(
                self.plugin.handle_webui(
                    {"query": "Play it on the office TV", "origin": {"available_artifacts": []}},
                    None,
                )
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "missing_media")
        inventory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
