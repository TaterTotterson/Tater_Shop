from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import sys
import types
import unittest


class _ToolVerba:
    pass


class _VoiceCoreEntityClient:
    @staticmethod
    def selector_from_context(context):
        return str((context or {}).get("satellite_selector") or "")


def _success(**values):
    return {"ok": True, **values}


def _failure(**values):
    return {"ok": False, **values}


verba_base = types.ModuleType("verba_base")
verba_base.ToolVerba = _ToolVerba
verba_result = types.ModuleType("verba_result")
verba_result.action_success = _success
verba_result.action_failure = _failure
voice_core = types.ModuleType("voice_core_entities")
voice_core.VoiceCoreEntityClient = _VoiceCoreEntityClient
sys.modules.setdefault("verba_base", verba_base)
sys.modules.setdefault("verba_result", verba_result)
sys.modules.setdefault("voice_core_entities", voice_core)

MODULE_PATH = Path(__file__).resolve().parents[1] / "verba/ble_device_enrollment.py"
SPEC = importlib.util.spec_from_file_location("test_ble_device_enrollment_verba", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class _Enrollment:
    def __init__(self):
        self.started = None

    async def start(self, **values):
        self.started = values
        return {
            "session": {
                "display_name": "Tater Enroll 1234",
                "satellite": {"name": "Kitchen"},
            }
        }

    async def snapshot(self):
        return {"active": None}

    async def cancel(self):
        return {"session": {"status": "cancelled"}}


class BleDeviceEnrollmentVerbaTests(unittest.TestCase):
    def setUp(self):
        self.enrollment = _Enrollment()
        package = types.ModuleType("tater_voice")
        package.ble_enrollment = self.enrollment
        sys.modules["tater_voice"] = package
        self.plugin = module.BleDeviceEnrollmentPlugin()

    def test_voice_request_targets_speaking_satellite(self):
        result = asyncio.run(
            self.plugin.handle_voice_core(
                {"query": "enroll my watch"},
                context={"satellite_selector": "native:kitchen", "user_name": "Alex"},
            )
        )
        self.assertTrue(result["ok"])
        self.assertEqual(self.enrollment.started["selector"], "native:kitchen")
        self.assertEqual(self.enrollment.started["category"], "watch")
        self.assertEqual(self.enrollment.started["name"], "My watch")
        self.assertEqual(self.enrollment.started["owner"], "Alex")

    def test_cancel_and_status_are_deterministic(self):
        self.assertEqual(self.plugin._action({"query": "cancel device enrollment"}), "cancel")
        self.assertEqual(self.plugin._action({"query": "is pairing still running"}), "status")


if __name__ == "__main__":
    unittest.main()
