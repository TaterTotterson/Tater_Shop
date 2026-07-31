from __future__ import annotations

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path


SHOP_ROOT = Path(__file__).resolve().parents[1]
REMOVED_CONTROL_VERBAS = {
    "camera_control",
    "climate_control",
    "cover_control",
    "fan_control",
    "garage_door_control",
    "light_control",
    "lock_control",
    "plug_control",
    "remote_control",
    "scene_control",
    "script_control",
    "switch_control",
}


class StubCategoryDeviceControlBase:
    pass


category_module = types.ModuleType("category_device_control")
category_module.CategoryDeviceControlBase = StubCategoryDeviceControlBase
sys.modules.setdefault("category_device_control", category_module)


def _load_device_control_module():
    path = SHOP_ROOT / "verba" / "device_control.py"
    spec = importlib.util.spec_from_file_location("shop_device_control", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load device_control.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


device_control_module = _load_device_control_module()


class DeviceControlVerbaTests(unittest.TestCase):
    def test_unified_verba_owns_specialized_control_actions(self) -> None:
        plugin = device_control_module.DeviceControlPlugin()

        self.assertEqual(plugin.name, "device_control")
        self.assertEqual(plugin.inventory_scope, "all")
        self.assertEqual(
            plugin.description,
            "Use for any request to control or check smart-home devices across integrations, "
            "including lights, switches, plugs, fans, covers, garage doors, locks, thermostats, "
            "cameras, media players, remotes, scenes, and scripts.",
        )
        self.assertIn("do not guess", plugin.when_to_use)
        for action in (
            "turn_on",
            "set_brightness",
            "set_percentage",
            "set_position",
            "set_temperature",
            "lock",
            "camera_snapshot",
        ):
            self.assertIn(action, plugin.allowed_actions)

    def test_manifest_facing_metadata_is_explicit(self) -> None:
        plugin = device_control_module.DeviceControlPlugin()

        self.assertEqual(plugin.version, "1.0.0")
        self.assertEqual(plugin.min_tater_version, "98.4")
        self.assertIn("voice_core", plugin.platforms)
        self.assertIn("webui", plugin.platforms)
        self.assertEqual(plugin.settings_category, "Device Control")

    def test_specialized_control_verbas_are_removed(self) -> None:
        for verba_id in REMOVED_CONTROL_VERBAS:
            self.assertFalse((SHOP_ROOT / "verba" / f"{verba_id}.py").exists())

        manifest = json.loads((SHOP_ROOT / "manifest.json").read_text(encoding="utf-8"))
        catalog_ids = {str(row.get("id") or "") for row in manifest.get("verbas", [])}
        self.assertIn("device_control", catalog_ids)
        self.assertTrue(REMOVED_CONTROL_VERBAS.isdisjoint(catalog_ids))


if __name__ == "__main__":
    unittest.main()
