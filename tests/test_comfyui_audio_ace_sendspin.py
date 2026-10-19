from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "verba" / "comfyui_audio_ace.py"


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
    async def llm_structured_chat(client, *, messages, **kwargs):
        return await client.chat(messages=messages, **kwargs)

    helpers.redis_client = SimpleNamespace(
        hgetall=lambda _key: {},
        hget=lambda _key, _field: None,
    )
    helpers.run_comfy_prompt = lambda *_args, **_kwargs: None
    helpers.get_llm_client_from_env = lambda **_kwargs: None
    helpers.get_primary_llm_client_from_env = lambda **_kwargs: None
    helpers.llm_structured_chat = llm_structured_chat
    tateros = types.ModuleType("tateros")
    tateros.integration_store = SimpleNamespace(
        integration_module=lambda _integration_id: None,
    )

    spec = importlib.util.spec_from_file_location("comfyui_audio_ace_sendspin_test_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {
            "verba_base": verba_base,
            "verba_result": verba_result,
            "helpers": helpers,
            "tateros": tateros,
        },
    ):
        spec.loader.exec_module(module)
    return module


class _BackgroundClient:
    async def __aenter__(self):
        return SimpleNamespace(chat=AsyncMock())

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


class ComfyUIAudioAceSendspinTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_module()

    def setUp(self):
        self.plugin = self.module.ComfyUIAudioAcePlugin()

    async def test_ai_presentation_supplies_title_and_cover_prompt(self):
        llm = SimpleNamespace(
            chat=AsyncMock(
                return_value={
                    "message": {
                        "content": (
                            '{"title":"Neon Rain","album_art_prompt":'
                            '"A lone chrome umbrella under electric blue rain, square album cover, no text."}'
                        )
                    }
                }
            )
        )

        result = await self.plugin.generate_song_presentation(
            "a hopeful synth song about rain",
            "synthwave, hopeful",
            "[verse]\nRain turns into light",
            llm,
        )

        self.assertEqual(result["title"], "Neon Rain")
        self.assertIn("umbrella", result["album_art_prompt"])

    async def test_no_screen_skips_album_cover_generation(self):
        native_satellite = types.ModuleType("tater_voice.native_satellite")
        native_satellite.client_has_capability = AsyncMock(return_value=False)
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite

        with patch.dict(
            sys.modules,
            {
                "tater_voice": tater_voice,
                "tater_voice.native_satellite": native_satellite,
            },
        ), patch.object(self.plugin, "_installed_image_verba") as installed:
            result = await self.plugin._maybe_generate_album_cover(
                "square cover",
                requesting_selector="native:kitchen-dot",
            )

        self.assertEqual(result, (None, ""))
        installed.assert_not_called()

    async def test_missing_image_verba_is_optional_on_screen_satellite(self):
        native_satellite = types.ModuleType("tater_voice.native_satellite")
        native_satellite.client_has_capability = AsyncMock(return_value=True)
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite

        with patch.dict(
            sys.modules,
            {
                "tater_voice": tater_voice,
                "tater_voice.native_satellite": native_satellite,
            },
        ), patch.object(self.plugin, "_installed_image_verba", return_value=None):
            result = await self.plugin._maybe_generate_album_cover(
                "square cover",
                requesting_selector="native:kitchen-show",
            )

        self.assertEqual(result, (None, ""))

    async def test_screen_satellite_gets_optional_cover_and_sendspin_presentation(self):
        playback = Mock(return_value={"ok": True, "sent_count": 1})

        def play_media_url_targets(targets, source_url, **kwargs):
            return playback(targets, source_url, **kwargs)

        media_playback = types.ModuleType("media_playback")
        media_playback.play_media_url_targets = play_media_url_targets

        with patch.dict(sys.modules, {"media_playback": media_playback}), patch.object(
            self.plugin,
            "_background_llm_client",
            return_value=_BackgroundClient(),
        ), patch.object(
            self.plugin,
            "generate_tags_and_lyrics",
            new=AsyncMock(return_value=("dream pop", "[verse]\nmoonlight")),
        ), patch.object(
            self.plugin,
            "generate_song_presentation",
            new=AsyncMock(
                return_value={
                    "title": "Moon Garden",
                    "artist": "Tater AI",
                    "album": "Audio ACE Originals",
                    "album_art_prompt": "A luminous moon garden, square cover, no text.",
                }
            ),
        ), patch.object(
            self.plugin,
            "process_prompt_sync",
            return_value=("https://comfy.test/song.mp3", b"song"),
        ), patch.object(
            self.plugin,
            "_maybe_generate_album_cover",
            new=AsyncMock(return_value=(b"cover", "image/png")),
        ) as cover:
            await self.plugin._bg_generate_and_play_target(
                "a moon garden song",
                None,
                "voice_core:native:kitchen-show",
                requesting_selector="native:kitchen-show",
            )

        cover.assert_awaited_once()
        self.assertEqual(
            cover.await_args.kwargs["requesting_selector"],
            "native:kitchen-show",
        )
        kwargs = playback.call_args.kwargs
        self.assertEqual(kwargs["source_owner"], "music_core")
        self.assertEqual(kwargs["title"], "Moon Garden")
        self.assertEqual(kwargs["artwork_bytes"], b"cover")
        self.assertNotIn("primary_color", kwargs)
        self.assertNotIn("accent_color", kwargs)


if __name__ == "__main__":
    unittest.main()
