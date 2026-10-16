from __future__ import annotations

import base64
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "verba" / "comfyui_image_plugin.py"


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
    helpers.redis_client = SimpleNamespace(hgetall=lambda _key: {})
    helpers.run_comfy_prompt = lambda *_args, **_kwargs: None

    spec = importlib.util.spec_from_file_location("comfyui_image_satellite_test_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {
            "verba_base": verba_base,
            "verba_result": verba_result,
            "helpers": helpers,
        },
    ):
        spec.loader.exec_module(module)
    return module


class ComfyUIImageSatelliteTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_module()

    def setUp(self):
        self.plugin = self.module.ComfyUIImagePlugin()

    def test_voice_core_is_an_advertised_platform(self):
        self.assertEqual("1.1.0", self.plugin.version)
        self.assertIn("voice_core", self.plugin.platforms)
        self.assertIn("tater_open_webui", self.plugin.platforms)

    async def test_screen_satellite_receives_generated_image_notification(self):
        with patch.object(
            self.plugin,
            "_screen_notifications_supported",
            new=AsyncMock(return_value=True),
        ), patch.object(
            self.plugin,
            "process_prompt",
            return_value=b"original-image",
        ), patch.object(
            self.plugin,
            "_prepare_satellite_image",
            return_value=(b"display-image", "image/jpeg"),
        ), patch.object(
            self.plugin,
            "_send_to_satellite_screen",
            new=AsyncMock(),
        ) as send_image:
            result = await self.plugin.handle_voice_core(
                {"prompt": "a potato astronaut"},
                None,
                context={"satellite_selector": "native:kitchen-show"},
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["facts"]["displayed_on_satellite"])
        send_image.assert_awaited_once()
        selector, image_bytes, content_type, prompt = send_image.await_args.args
        self.assertEqual("native:kitchen-show", selector)
        self.assertEqual(b"display-image", image_bytes)
        self.assertEqual("image/jpeg", content_type)
        self.assertIn("a potato astronaut", prompt)

    async def test_speaker_only_satellite_does_not_start_generation(self):
        with patch.object(
            self.plugin,
            "_screen_notifications_supported",
            new=AsyncMock(return_value=False),
        ), patch.object(self.plugin, "process_prompt") as generate:
            result = await self.plugin.handle_voice_core(
                {"prompt": "a potato astronaut"},
                None,
                context={"satellite_selector": "native:kitchen-dot"},
            )

        self.assertFalse(result["ok"])
        self.assertEqual("satellite_screen_required", result["error"]["code"])
        generate.assert_not_called()

    async def test_satellite_payload_uses_extensible_display_contract(self):
        native_satellite = types.ModuleType("tater_voice.native_satellite")
        native_satellite.send_command = AsyncMock(return_value={"ok": True})
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite

        with patch.dict(
            sys.modules,
            {
                "tater_voice": tater_voice,
                "tater_voice.native_satellite": native_satellite,
            },
        ):
            await self.plugin._send_to_satellite_screen(
                "native:kitchen-show",
                b"jpeg-data",
                "image/jpeg",
                "a potato astronaut",
            )

        selector, message_type, payload = native_satellite.send_command.await_args.args
        self.assertEqual("native:kitchen-show", selector)
        self.assertEqual("display.notification", message_type)
        self.assertEqual("image", payload["kind"])
        self.assertEqual("Generated Image", payload["display_label"])
        self.assertEqual("contain", payload["image_fit"])
        self.assertEqual("ComfyUI", payload["source"])
        self.assertEqual(b"jpeg-data", base64.b64decode(payload["image_data_b64"]))
        self.assertGreater(payload["expires_at"], payload["created_at"])


if __name__ == "__main__":
    unittest.main()
