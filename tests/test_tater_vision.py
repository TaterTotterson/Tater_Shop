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


def load_tater_vision():
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})
    verba_result = types.ModuleType("verba_result")
    verba_result.action_success = _action_success
    verba_result.action_failure = _action_failure
    path = Path(__file__).resolve().parents[1] / "verba" / "tater_vision.py"
    spec = importlib.util.spec_from_file_location("tater_vision_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(sys.modules, {"verba_base": verba_base, "verba_result": verba_result}):
        spec.loader.exec_module(module)
    return module


class TaterVisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_tater_vision()
        cls.plugin = cls.module.TaterVisionPlugin()

    def test_metadata_routes_all_current_visual_questions_to_tater_vision(self):
        self.assertEqual(self.plugin.name, "tater_vision")
        self.assertEqual(self.plugin.verba_name, "Tater Vision")
        self.assertEqual(self.plugin.version, "1.1.2")
        description = self.plugin.description.lower()
        for phrase in (
            "tater's visual-question tool",
            "what is this?",
            "how do i look?",
            "are there any dogs in the backyard?",
            "what did you see when you looked?",
            "derives the asking satellite's room",
        ):
            self.assertIn(phrase, description)
        self.assertNotIn("camera control", description)

        routing = self.plugin.when_to_use.lower()
        self.assertIn("named locations", routing)
        self.assertIn("follow-ups", routing)
        self.assertIn("weather-only or other nonvisual questions", routing)

        shop_description = self.plugin.verba_dec.lower()
        for phrase in (
            "give tater eyes",
            "camera-equipped rooms and areas",
            "what you're holding",
            "how an outfit looks",
            "a pet is nearby",
            "automatically chooses a camera",
        ):
            self.assertIn(phrase, shop_description)
        self.assertNotIn("use when", shop_description)

    def test_selects_every_camera_in_named_or_asking_room_and_never_other_room(self):
        candidates = [
            {
                "source": "native_satellite", "selector": "native:office-show",
                "device_id": "office-show", "name": "Office Show", "room": "Office",
            },
            {
                "source": "integration", "integration_id": "protect", "device_id": "office-cam",
                "name": "Office Camera", "room": "Office",
            },
            {
                "source": "native_satellite", "selector": "native:kitchen-show",
                "device_id": "kitchen-show", "name": "Kitchen Show", "room": "Kitchen",
            },
        ]
        selected, reason, room = self.plugin._select_cameras(
            candidates,
            {"device_id": "native:kitchen-show", "area_name": "Office"},
            "What is happening in the office?",
            "Office",
            ["Office", "Kitchen"],
        )
        self.assertEqual({row["device_id"] for row in selected}, {"office-show", "office-cam"})
        self.assertEqual(reason, "named_area")
        self.assertEqual(room, "Office")

        selected, reason, room = self.plugin._select_cameras(
            candidates,
            {"device_id": "native:voice-pe", "area_name": "Office"},
            "What am I holding?",
            "Office",
            ["Office", "Kitchen"],
        )
        self.assertEqual({row["device_id"] for row in selected}, {"office-show", "office-cam"})
        self.assertEqual(reason, "asking_room")
        self.assertEqual(room, "Office")

        selected, reason, room = self.plugin._select_cameras(
            candidates,
            {"device_id": "native:voice-pe", "area_name": "Garage"},
            "What is this?",
            "Garage",
            ["Office", "Kitchen", "Garage"],
        )
        self.assertEqual(selected, [])
        self.assertEqual(reason, "asking_room")
        self.assertEqual(room, "Garage")

    def test_filters_disconnected_and_non_camera_satellites(self):
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
        self.assertEqual([row["selector"] for row in rows], ["native:reachy", "native:show"])

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
                    "device_id": "native:office-speaker",
                },
            }))

        self.assertTrue(result["ok"])
        self.assertEqual(result["summary_for_user"], "Add a warmer coat before heading outside.")
        self.assertEqual(result["facts"]["selection_reason"], "asking_room")
        self.assertEqual(result["facts"]["target_room"], "Office")
        self.assertEqual(calls[0][0:2], ("native:office-show", "camera.snapshot"))

    def test_named_backyard_uses_all_integrated_cameras_instead_of_asking_room_show(self):
        jpeg = b"\xff\xd8camera-frame\xff\xd9"
        captured = []
        native_satellite = types.ModuleType("tater_voice.native_satellite")

        async def status():
            return {"clients": {
                "native:game-speaker": {
                    "connected": True, "device_id": "game-speaker", "room": "Game Room",
                    "board": "voice_pe", "capabilities": {"speaker": True},
                },
                "native:game-show": {
                    "connected": True, "device_id": "game-show", "device_name": "Game Room Show",
                    "room": "Game Room", "board": "checkers", "capabilities": {"camera_snapshot": True},
                },
            }}

        async def send_request(*_args, **_kwargs):
            self.fail("The asking-room Show must not be used for an explicit backyard request")

        native_satellite.status = status
        native_satellite.send_request = send_request
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite
        integration_registry = types.ModuleType("integration_registry")
        integration_registry.get_integration_devices_by_capability = lambda _capability: [
            {"integration_id": "protect", "id": "back-east", "name": "Back East", "room": "Back Yard"},
            {"integration_id": "protect", "id": "back-west", "name": "Back West", "room": "Back Yard"},
            {"integration_id": "protect", "id": "front", "name": "Front Door", "room": "Front Porch"},
        ]

        def run_action(integration_id, action, device_id, payload):
            captured.append((integration_id, action, device_id, payload))
            return {"ok": True, "bytes": jpeg, "content_type": "image/jpeg"}

        integration_registry.run_integration_device_action = run_action
        kernel_tools = types.ModuleType("kernel_tools")

        def image_describe(**kwargs):
            camera = kwargs["image_ref"]["device_name"]
            return {"ok": True, "data": {"description": f"No dog is visible from {camera}."}}

        kernel_tools.image_describe = image_describe
        with patch.dict(sys.modules, {
            "tater_voice": tater_voice,
            "tater_voice.native_satellite": native_satellite,
            "integration_registry": integration_registry,
            "kernel_tools": kernel_tools,
        }):
            result = asyncio.run(self.plugin.handle_voice_core({
                "query": "Are there any dogs in the backyard?",
                "origin": {"platform": "voice_core", "device_id": "native:game-speaker"},
            }))

        self.assertTrue(result["ok"])
        self.assertEqual(result["facts"]["selection_reason"], "named_area")
        self.assertEqual(result["facts"]["target_room"], "Back Yard")
        self.assertEqual(result["facts"]["vision_count"], 2)
        self.assertEqual({call[2] for call in captured}, {"back-east", "back-west"})
        self.assertNotIn("front", {call[2] for call in captured})
        self.assertIn("Back East", result["summary_for_user"])
        self.assertIn("Back West", result["summary_for_user"])

    def test_named_room_without_camera_never_falls_back_to_asking_room_camera(self):
        candidates = [
            {
                "source": "native_satellite", "selector": "native:game-show",
                "device_id": "game-show", "name": "Game Room Show", "room": "Game Room",
            }
        ]
        selected, reason, room = self.plugin._select_cameras(
            candidates,
            {"device_id": "native:game-speaker"},
            "Is anyone in the garage?",
            "Game Room",
            ["Game Room", "Garage"],
        )
        self.assertEqual(selected, [])
        self.assertEqual(reason, "named_area")
        self.assertEqual(room, "Garage")

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
