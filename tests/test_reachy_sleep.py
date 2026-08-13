from __future__ import annotations

import asyncio
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


def load_reachy_sleep():
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})
    verba_result = types.ModuleType("verba_result")
    verba_result.action_success = _action_success
    verba_result.action_failure = _action_failure

    path = Path(__file__).resolve().parents[1] / "verba" / "reachy_sleep.py"
    spec = importlib.util.spec_from_file_location("reachy_sleep_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {"verba_base": verba_base, "verba_result": verba_result},
    ):
        spec.loader.exec_module(module)
    return module


class ReachySleepTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_reachy_sleep()
        cls.plugin = cls.module.ReachySleepPlugin()

    def test_prefers_requested_then_requesting_then_same_room_reachy(self):
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
            requested_target="office-reachy",
        )
        self.assertEqual(selected["selector"], "native:office-reachy")
        self.assertEqual(reason, "requested_target")

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

    def test_non_reachy_satellite_can_put_same_room_reachy_to_sleep(self):
        calls = []
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
                        "capabilities": {"sleep_until_wake": True},
                    },
                }
            }

        async def send_request(selector, message_type, payload, timeout_s=0):
            calls.append((selector, message_type, payload, timeout_s))
            return {
                "ok": True,
                "already_sleeping": False,
                "wake_word": "hey reachy",
            }

        native_satellite.status = status
        native_satellite.send_request = send_request
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
                        "origin": {
                            "platform": "voice_core",
                            "device_id": "native:voice-pe",
                            "area_name": "kitchen",
                        },
                    }
                )
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["facts"]["selection_reason"], "same_room")
        self.assertIn('"hey reachy"', result["summary_for_user"])
        self.assertIn("music", result["summary_for_user"])
        self.assertTrue(result["data"]["music_wakes_reachy"])
        self.assertEqual(calls[0][0], "native:kitchen-reachy")
        self.assertEqual(calls[0][1], "reachy.sleep")
        self.assertEqual(calls[0][2], {"reason": "explicit_verba_request"})
        self.assertEqual(calls[0][3], 8.0)

    def test_ignores_reachys_without_sleep_capability(self):
        status = {
            "clients": {
                "native:old-reachy": {
                    "connected": True,
                    "board": "reachy_mini",
                    "capabilities": {"camera_snapshot": True},
                }
            }
        }

        self.assertEqual(self.plugin._sleep_reachys(status), [])


if __name__ == "__main__":
    unittest.main()
