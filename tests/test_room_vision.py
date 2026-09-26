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


def load_room_vision():
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})
    verba_result = types.ModuleType("verba_result")
    verba_result.action_success = _action_success
    verba_result.action_failure = _action_failure
    path = Path(__file__).resolve().parents[1] / "verba" / "room_vision.py"
    spec = importlib.util.spec_from_file_location("room_vision_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(sys.modules, {"verba_base": verba_base, "verba_result": verba_result}):
        spec.loader.exec_module(module)
    return module


class RoomVisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_room_vision()
        cls.plugin = cls.module.RoomVisionPlugin()

    def test_selects_requesting_show_then_same_room_and_never_other_room(self):
        candidates = [
            {"selector": "native:office-show", "device_id": "office-show", "room": "Office"},
            {"selector": "native:kitchen-show", "device_id": "kitchen-show", "room": "Kitchen"},
        ]
        selected, reason = self.plugin._select_camera(
            candidates, {"device_id": "native:kitchen-show", "area_name": "Office"}
        )
        self.assertEqual(selected["selector"], "native:kitchen-show")
        self.assertEqual(reason, "requesting_camera_satellite")

        selected, reason = self.plugin._select_camera(
            candidates, {"device_id": "native:voice-pe", "area_name": "Office"}
        )
        self.assertEqual(selected["selector"], "native:office-show")
        self.assertEqual(reason, "same_room")

        selected, reason = self.plugin._select_camera(
            candidates, {"device_id": "native:voice-pe", "area_name": "Garage"}
        )
        self.assertIsNone(selected)
        self.assertEqual(reason, "no_same_room_camera")

    def test_filters_reachy_disconnected_and_non_camera_satellites(self):
        rows = self.plugin._camera_satellites({"clients": {
            "native:show": {
                "connected": True, "board": "checkers", "room": "Office",
                "capabilities": {"camera_snapshot": True},
            },
            "native:reachy": {
                "connected": True, "board": "reachy_mini", "room": "Office",
                "capabilities": {"camera_snapshot": True},
            },
            "native:offline": {
                "connected": False, "board": "checkers", "room": "Office",
                "capabilities": {"camera_snapshot": True},
            },
            "native:voice-pe": {
                "connected": True, "board": "voice_pe", "room": "Office",
                "capabilities": {"speaker": True},
            },
        }})
        self.assertEqual([row["selector"] for row in rows], ["native:show"])

    def test_same_room_show_captures_and_answers_with_weather_context(self):
        jpeg = b"\xff\xd8fresh-room-frame\xff\xd9"
        calls = []
        native_satellite = types.ModuleType("tater_voice.native_satellite")

        async def status():
            return {"clients": {
                "native:office-speaker": {
                    "connected": True, "selector": "native:office-speaker", "device_id": "office-speaker",
                    "board": "voice_pe", "room": "Office", "capabilities": {"speaker": True},
                },
                "native:office-show": {
                    "connected": True, "selector": "native:office-show", "device_id": "office-show",
                    "device_name": "Office Show", "board": "checkers", "room": "Office",
                    "capabilities": {"camera_snapshot": True},
                },
            }}

        async def send_request(selector, message_type, payload, timeout_s=0):
            calls.append((selector, message_type, payload, timeout_s))
            return {
                "ok": True,
                "content_type": "image/jpeg",
                "image_base64": base64.b64encode(jpeg).decode("ascii"),
            }

        native_satellite.status = status
        native_satellite.send_request = send_request
        display_feed = types.ModuleType("tater_voice.display_feed")
        display_feed.build_weather_summary = lambda selector="": {
            "available": True, "temperature_text": "38° F", "feels_like_text": "Feels like 31°",
            "condition": "Cloudy", "wind_text": "NW 12 mph",
        }
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite
        tater_voice.display_feed = display_feed
        kernel_tools = types.ModuleType("kernel_tools")

        def image_describe(**kwargs):
            self.assertEqual(kwargs["image_ref"]["bytes"], jpeg)
            self.assertIn("Am I dressed warmly enough outside?", kwargs["prompt"])
            self.assertIn("temperature: 38° F", kwargs["prompt"])
            return {"ok": True, "data": {"description": "Add a warmer coat before heading outside."}}

        kernel_tools.image_describe = image_describe
        with patch.dict(sys.modules, {
            "tater_voice": tater_voice,
            "tater_voice.native_satellite": native_satellite,
            "tater_voice.display_feed": display_feed,
            "kernel_tools": kernel_tools,
        }):
            result = asyncio.run(self.plugin.handle_voice_core({
                "query": "Am I dressed warmly enough outside?",
                "origin": {
                    "platform": "homeassistant", "entrypoint": "voice_core",
                    "device_id": "native:office-speaker", "area_name": "Office",
                },
            }))

        self.assertTrue(result["ok"])
        self.assertEqual(result["summary_for_user"], "Add a warmer coat before heading outside.")
        self.assertEqual(result["facts"]["selection_reason"], "same_room")
        self.assertEqual(calls[0][0:2], ("native:office-show", "camera.snapshot"))

    def test_returns_safe_failure_without_same_room_camera(self):
        native_satellite = types.ModuleType("tater_voice.native_satellite")

        async def status():
            return {"clients": {
                "native:kitchen-show": {
                    "connected": True, "board": "checkers", "room": "Kitchen",
                    "capabilities": {"camera_snapshot": True},
                }
            }}

        native_satellite.status = status
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite
        with patch.dict(sys.modules, {
            "tater_voice": tater_voice,
            "tater_voice.native_satellite": native_satellite,
        }):
            result = asyncio.run(self.plugin.handle_voice_core({
                "query": "What is this?",
                "origin": {"platform": "voice_core", "device_id": "native:voice-pe", "area_name": "Office"},
            }))
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "no_room_camera")


if __name__ == "__main__":
    unittest.main()
