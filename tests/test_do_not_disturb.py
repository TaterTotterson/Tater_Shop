from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


class ToolVerba:
    pass


fake_verba_base = types.ModuleType("verba_base")
fake_verba_base.ToolVerba = ToolVerba
sys.modules.setdefault("verba_base", fake_verba_base)

spec = importlib.util.spec_from_file_location("shop_do_not_disturb", ROOT / "verba" / "do_not_disturb.py")
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)
DoNotDisturbPlugin = module.DoNotDisturbPlugin


class DoNotDisturbVerbaTests(unittest.TestCase):
    def test_understands_common_actions(self) -> None:
        self.assertEqual(DoNotDisturbPlugin._action({"query": "enable DND"}), "on")
        self.assertEqual(DoNotDisturbPlugin._action({"query": "turn off do not disturb"}), "off")
        self.assertEqual(DoNotDisturbPlugin._action({"query": "is DND active"}), "status")

    def test_enables_the_satellite_from_voice_context(self) -> None:
        plugin = DoNotDisturbPlugin()
        status = {
            "clients": {
                "native:rook": {
                    "connected": True,
                    "device_name": "Bedroom Rook",
                    "room": "Bedroom",
                    "board": "rook",
                }
            }
        }
        fake_live_settings = types.ModuleType("tater_voice.native_live_settings")
        fake_live_settings.dnd_active = lambda *_args, **_kwargs: False
        fake_satellite = types.ModuleType("tater_voice.native_satellite")
        fake_satellite.status = mock.AsyncMock(return_value=status)
        fake_satellite.save_live_settings = mock.AsyncMock(return_value={"ok": True})
        fake_tater_voice = types.ModuleType("tater_voice")
        fake_tater_voice.native_live_settings = fake_live_settings
        fake_tater_voice.native_satellite = fake_satellite
        with mock.patch.dict(
            sys.modules,
            {
                "tater_voice": fake_tater_voice,
                "tater_voice.native_live_settings": fake_live_settings,
                "tater_voice.native_satellite": fake_satellite,
            },
        ):
            result = asyncio.run(
                plugin.handle_voice_core(
                    {"action": "on"},
                    context={"satellite_selector": "native:rook"},
                )
            )
        self.assertIn("now on", result)
        fake_satellite.save_live_settings.assert_awaited_once_with(
            {"dnd_enabled": True},
            selector="native:rook",
            push=True,
        )


if __name__ == "__main__":
    unittest.main()
