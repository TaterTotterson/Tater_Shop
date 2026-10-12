from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


def _load_broadcast_module():
    previous = {
        name: sys.modules.get(name)
        for name in (
            "announcement_targets",
            "dotenv",
            "helpers",
            "speech_settings",
            "speech_tts",
            "tateros",
            "tateros.integration_store",
            "verba_base",
            "verba_result",
        )
    }
    announcement_targets = types.ModuleType("announcement_targets")
    announcement_targets.VOICE_CORE_TARGET_PREFIX = "voice_core:"
    announcement_targets.build_announcement_target_options = lambda: []
    announcement_targets.get_voice_core_satellite_target_options = lambda: []
    announcement_targets.normalize_announcement_targets = lambda value: list(value or [])
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None
    helpers = types.ModuleType("helpers")
    helpers.get_tater_name = lambda: ("Tater", "Tot")
    helpers.redis_client = object()
    speech_settings = types.ModuleType("speech_settings")
    speech_settings.get_speech_settings = lambda: {}
    speech_tts = types.ModuleType("speech_tts")
    speech_tts.speak_announcement_targets = None
    tateros = types.ModuleType("tateros")
    integration_store = types.ModuleType("tateros.integration_store")
    integration_store.integration_module = lambda _name: None
    tateros.integration_store = integration_store
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = object
    verba_result = types.ModuleType("verba_result")
    verba_result.action_failure = lambda **kwargs: kwargs
    verba_result.action_success = lambda **kwargs: kwargs
    stubs = {
        "announcement_targets": announcement_targets,
        "dotenv": dotenv,
        "helpers": helpers,
        "speech_settings": speech_settings,
        "speech_tts": speech_tts,
        "tateros": tateros,
        "tateros.integration_store": integration_store,
        "verba_base": verba_base,
        "verba_result": verba_result,
    }
    sys.modules.update(stubs)
    try:
        path = Path(__file__).resolve().parents[1] / "verba" / "broadcast.py"
        spec = importlib.util.spec_from_file_location("test_broadcast_audio_scene_module", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        for name, module in previous.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


class BroadcastAudioSceneTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.broadcast = _load_broadcast_module()

    def test_preserves_every_background_audio_control(self) -> None:
        scene = self.broadcast.BroadcastPlugin._normalize_audio_scene(
            {
                "background": {
                    "url": "https://example.test/morning.mp3",
                    "loop": False,
                    "volume_percent": 72,
                },
                "foreground": {"start_delay_ms": 2250},
                "ducking": {
                    "target_percent": 28,
                    "attack_ms": 90,
                    "release_ms": 420,
                },
                "finish": {"fade_ms": 725},
            }
        )

        self.assertEqual(scene["background"]["volume_percent"], 72)
        self.assertFalse(scene["background"]["loop"])
        self.assertEqual(scene["foreground"]["start_delay_ms"], 2250)
        self.assertEqual(scene["ducking"]["target_percent"], 28)
        self.assertEqual(scene["ducking"]["attack_ms"], 90)
        self.assertEqual(scene["ducking"]["release_ms"], 420)
        self.assertEqual(scene["finish"]["fade_ms"], 725)

    def test_lead_in_is_declared_and_clamped(self) -> None:
        foreground_schema = self.broadcast.BroadcastPlugin.argument_schema["properties"][
            "audio_scene"
        ]["properties"]["foreground"]["properties"]["start_delay_ms"]
        self.assertEqual(foreground_schema["maximum"], 30000)

        scene = self.broadcast.BroadcastPlugin._normalize_audio_scene(
            {
                "background_audio_url": "https://example.test/morning.mp3",
                "tts_start_delay_ms": 45000,
            }
        )
        self.assertEqual(scene["foreground"]["start_delay_ms"], 30000)


if __name__ == "__main__":
    unittest.main()
