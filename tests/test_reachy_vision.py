from __future__ import annotations

import asyncio
import base64
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def _action_success(**kwargs):
    return {"ok": True, **kwargs}


def _action_failure(*, code, message, **kwargs):
    return {"ok": False, "error": {"code": code, "message": message}, **kwargs}


def load_reachy_vision():
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})
    verba_result = types.ModuleType("verba_result")
    verba_result.action_success = _action_success
    verba_result.action_failure = _action_failure

    path = Path(__file__).resolve().parents[1] / "verba" / "reachy_vision.py"
    spec = importlib.util.spec_from_file_location("reachy_vision_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {"verba_base": verba_base, "verba_result": verba_result},
    ):
        spec.loader.exec_module(module)
    return module


class ReachyVisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_reachy_vision()
        cls.plugin = cls.module.ReachyVisionPlugin()

    def test_prefers_requesting_reachy_then_same_room(self):
        candidates = [
            {
                "selector": "native:office-reachy",
                "device_id": "office-reachy",
                "room": "office",
                "last_seen_ts": 20.0,
            },
            {
                "selector": "native:kitchen-reachy",
                "device_id": "kitchen-reachy",
                "room": "kitchen",
                "last_seen_ts": 10.0,
            },
        ]

        selected, reason = self.plugin._select_reachy(
            candidates,
            {"device_id": "native:kitchen-reachy", "area_name": "office"},
        )
        self.assertEqual(selected["selector"], "native:kitchen-reachy")
        self.assertEqual(reason, "requesting_reachy")

        selected, reason = self.plugin._select_reachy(
            candidates,
            {"device_id": "native:voice-pe", "area_name": "office"},
        )
        self.assertEqual(selected["selector"], "native:office-reachy")
        self.assertEqual(reason, "same_room")

    def test_non_reachy_satellite_uses_a_connected_reachy_and_builtin_vision(self):
        calls = []
        jpeg = b"\xff\xd8current-camera-frame\xff\xd9"

        native_satellite = types.ModuleType("tater_voice.native_satellite")

        async def status():
            return {
                "clients": {
                    "native:voice-pe": {
                        "connected": True,
                        "selector": "native:voice-pe",
                        "device_id": "voice-pe",
                        "board": "voice_pe",
                        "room": "kitchen",
                        "capabilities": {"speaker": True},
                    },
                    "native:kitchen-reachy": {
                        "connected": True,
                        "selector": "native:kitchen-reachy",
                        "device_id": "kitchen-reachy",
                        "device_name": "Kitchen Reachy",
                        "board": "reachy_mini",
                        "room": "kitchen",
                        "last_seen_ts": 12.0,
                        "capabilities": {"camera_snapshot": True},
                    },
                }
            }

        async def send_request(selector, message_type, payload, timeout_s=0):
            calls.append((selector, message_type, payload, timeout_s))
            return {
                "ok": True,
                "content_type": "image/jpeg",
                "image_base64": base64.b64encode(jpeg).decode("ascii"),
            }

        native_satellite.status = status
        native_satellite.send_request = send_request
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite

        kernel_tools = types.ModuleType("kernel_tools")

        def image_describe(**kwargs):
            image_ref = kwargs["image_ref"]
            self.assertEqual(image_ref["bytes"], jpeg)
            self.assertIn("How do I look?", kwargs["prompt"])
            return {
                "ok": True,
                "data": {
                    "description": "Your blue jacket looks sharp and fits the casual outfit well.",
                    "model": "vision-model",
                },
            }

        kernel_tools.image_describe = image_describe
        helpers = types.ModuleType("helpers")
        helpers.redis_client = types.SimpleNamespace(hgetall=lambda _key: {})

        with patch.dict(
            sys.modules,
            {
                "tater_voice": tater_voice,
                "tater_voice.native_satellite": native_satellite,
                "kernel_tools": kernel_tools,
                "helpers": helpers,
            },
        ):
            result = asyncio.run(
                self.plugin.handle_voice_core(
                    {
                        "query": "How do I look?",
                        "origin": {
                            "platform": "voice_core",
                            "device_id": "native:voice-pe",
                            "area_name": "kitchen",
                        },
                    }
                )
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["summary_for_user"],
            "Your blue jacket looks sharp and fits the casual outfit well.",
        )
        self.assertEqual(result["facts"]["selection_reason"], "same_room")
        self.assertEqual(calls[0][0], "native:kitchen-reachy")
        self.assertEqual(calls[0][1], "camera.snapshot")

    def test_returns_safe_failure_when_no_reachy_is_available(self):
        native_satellite = types.ModuleType("tater_voice.native_satellite")

        async def status():
            return {"clients": {}}

        native_satellite.status = status
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite
        helpers = types.ModuleType("helpers")
        helpers.redis_client = types.SimpleNamespace(hgetall=lambda _key: {})

        with patch.dict(
            sys.modules,
            {
                "tater_voice": tater_voice,
                "tater_voice.native_satellite": native_satellite,
                "helpers": helpers,
            },
        ):
            result = asyncio.run(
                self.plugin.handle_voice_core(
                    {
                        "query": "Look at this.",
                        "origin": {
                            "platform": "voice_core",
                            "device_id": "native:voice-pe",
                        },
                    }
                )
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "no_reachy_camera")


if __name__ == "__main__":
    unittest.main()
