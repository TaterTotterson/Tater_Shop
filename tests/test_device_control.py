from __future__ import annotations

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


SHOP_ROOT = Path(__file__).resolve().parents[1]
CATEGORY_DEVICE_VERBAS = {
    "battery_status",
    "camera_control",
    "device_control",
    "energy_status",
    "entry_sensor_status",
    "humidity_status",
    "illuminance_status",
    "leak_status",
    "motion_status",
    "network_device_status",
    "presence_status",
    "sensor_status",
    "temperature_status",
}
REMOVED_CONTROL_VERBAS = {
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
START_MARKER = "# BEGIN EMBEDDED DEVICE VERBA RUNTIME"
END_MARKER = "# END EMBEDDED DEVICE VERBA RUNTIME"


def _extract_json(text: object) -> str:
    value = str(text or "")
    start = value.find("{")
    end = value.rfind("}")
    return value[start : end + 1] if start >= 0 and end >= start else ""


helpers_stub = types.ModuleType("helpers")
helpers_stub.extract_json = _extract_json
helpers_stub.redis_client = None
sys.modules["helpers"] = helpers_stub

registry_stub = types.ModuleType("integration_registry")
registry_stub.get_integration_device_registry = lambda: []
registry_stub.get_integration_devices_by_capability = lambda _category: []
registry_stub.run_integration_device_action = lambda *_args, **_kwargs: {}
sys.modules["integration_registry"] = registry_stub

base_stub = types.ModuleType("verba_base")


class StubToolVerba:
    pass


base_stub.ToolVerba = StubToolVerba
sys.modules["verba_base"] = base_stub

result_stub = types.ModuleType("verba_result")
result_stub.action_failure = lambda message, **kwargs: {"ok": False, "message": message, **kwargs}
result_stub.action_success = lambda message, **kwargs: {"ok": True, "message": message, **kwargs}
sys.modules["verba_result"] = result_stub


def _load_verba_module(verba_id: str):
    path = SHOP_ROOT / "verba" / f"{verba_id}.py"
    spec = importlib.util.spec_from_file_location(f"shop_{verba_id}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path.name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


device_control_module = _load_verba_module("device_control")
camera_control_module = _load_verba_module("camera_control")


class StandaloneCategoryVerbaTests(unittest.TestCase):
    def test_every_category_verba_is_a_complete_single_file_artifact(self) -> None:
        runtime = (SHOP_ROOT / "tools" / "device_verba_runtime.py").read_text(
            encoding="utf-8"
        ).rstrip()

        sys.modules.pop("category_device_control", None)
        for verba_id in sorted(CATEGORY_DEVICE_VERBAS):
            with self.subTest(verba_id=verba_id):
                path = SHOP_ROOT / "verba" / f"{verba_id}.py"
                source = path.read_text(encoding="utf-8")
                self.assertNotIn("from category_device_control import", source)
                self.assertEqual(source.count(START_MARKER), 1)
                self.assertEqual(source.count(END_MARKER), 1)
                embedded = source.split(START_MARKER, 1)[1].split(END_MARKER, 1)[0].strip()
                self.assertEqual(embedded, runtime)

                module = _load_verba_module(verba_id)
                self.assertEqual(module.verba.name, verba_id)
                self.assertNotIn("category_device_control", sys.modules)

    def test_manifest_points_to_the_standalone_plugins(self) -> None:
        manifest = json.loads((SHOP_ROOT / "manifest.json").read_text(encoding="utf-8"))
        catalog = {
            str(row.get("id") or ""): row
            for row in manifest.get("verbas", [])
        }

        self.assertNotIn("category_device_control", catalog)
        self.assertNotIn("_device_verba_runtime", catalog)
        for verba_id in CATEGORY_DEVICE_VERBAS:
            with self.subTest(verba_id=verba_id):
                row = catalog[verba_id]
                self.assertEqual(row["entry"], f"verba/{verba_id}.py")
                self.assertEqual(
                    row["version"],
                    {
                        "camera_control": "1.0.3",
                        "device_control": "1.0.3",
                    }.get(verba_id, "1.0.1"),
                )
                self.assertEqual(row["min_tater_version"], "98.4")
                self.assertEqual(len(row["sha256"]), 64)


class DeviceControlVerbaTests(unittest.TestCase):
    def test_unified_verba_owns_specialized_control_actions(self) -> None:
        plugin = device_control_module.DeviceControlPlugin()

        self.assertEqual(plugin.name, "device_control")
        self.assertEqual(plugin.inventory_scope, "all")
        self.assertIn("Use Camera Control for cameras and snapshots", plugin.description)
        self.assertIn("do not guess", plugin.when_to_use)
        for action in (
            "turn_on",
            "set_brightness",
            "set_percentage",
            "set_position",
            "set_temperature",
            "lock",
        ):
            self.assertIn(action, plugin.allowed_actions)
        self.assertNotIn("camera_snapshot", plugin.allowed_actions)

    def test_manifest_facing_metadata_is_explicit(self) -> None:
        plugin = device_control_module.DeviceControlPlugin()

        self.assertEqual(plugin.version, "1.0.3")
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
        self.assertIn("camera_control", catalog_ids)
        self.assertTrue(REMOVED_CONTROL_VERBAS.isdisjoint(catalog_ids))


class CameraControlVerbaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plugin = camera_control_module.CameraControlPlugin()

    def test_camera_verba_owns_snapshot_requests(self) -> None:
        self.assertEqual(self.plugin.name, "camera_control")
        self.assertEqual(self.plugin.version, "1.0.3")
        self.assertEqual(self.plugin.min_tater_version, "98.4")
        self.assertEqual(self.plugin.category_id, "camera")
        self.assertEqual(
            self.plugin.allowed_actions,
            {"list", "status", "camera_snapshot"},
        )
        self.assertEqual(
            self.plugin.verba_dec,
            "Use for requests about cameras, doorbells, snapshots, camera images, or current visible activity in a "
            "named camera-covered area, including questions like 'what's happening in the front yard?', "
            "'who is at the door?', or 'is anyone on the porch?'",
        )
        self.assertEqual(
            self.plugin._normalize_action("", "show me a snapshot from the front door camera"),
            "camera_snapshot",
        )
        self.assertEqual(
            self.plugin._normalize_action("", "what is happening in the front yard?"),
            "camera_snapshot",
        )
        self.assertEqual(
            self.plugin._clean_target("what is happening in the front yard?"),
            "front yard",
        )

    def test_snapshot_bytes_become_an_image_artifact(self) -> None:
        self.plugin._store_snapshot_blob = lambda _raw: "tater:blob:camera_snapshot:test"
        artifact = self.plugin._camera_snapshot_artifact(
            result={"bytes": b"camera-image", "content_type": "image/jpeg"},
            device={
                "integration_id": "homeassistant",
                "id": "camera.front_door",
                "name": "Front Door",
            },
            index=0,
        )

        self.assertIsNotNone(artifact)
        self.assertEqual(artifact["type"], "image")
        self.assertEqual(artifact["mimetype"], "image/jpeg")
        self.assertEqual(artifact["source"], "camera_control")
        self.assertEqual(artifact["device_id"], "camera.front_door")
        self.assertEqual(artifact["blob_key"], "tater:blob:camera_snapshot:test")

    def test_snapshot_uses_taters_configured_vision_tool(self) -> None:
        calls = []
        kernel_tools_stub = types.ModuleType("kernel_tools")

        def image_describe(**kwargs):
            calls.append(kwargs)
            return {
                "ok": True,
                "data": {"description": "A person is walking up the driveway.", "model": "vision-model"},
                "summary_for_user": "A person is walking up the driveway.",
            }

        kernel_tools_stub.image_describe = image_describe
        artifact = {
            "type": "image",
            "name": "Driveway-snapshot.jpg",
            "mimetype": "image/jpeg",
            "device_name": "Driveway",
            "blob_key": "tater:blob:camera_snapshot:test",
        }
        with patch.dict(sys.modules, {"kernel_tools": kernel_tools_stub}):
            result = self.plugin._describe_snapshot_artifact(
                artifact,
                "what is happening in the driveway?",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["image_ref"], artifact)
        self.assertIn("what is happening in the driveway", calls[0]["prompt"])


class CameraVisionWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_description_is_returned_to_the_assistant(self) -> None:
        plugin = camera_control_module.CameraControlPlugin()
        snapshot_result = {
            "ok": True,
            "facts": {"action": "camera_snapshot", "artifact_count": 1},
            "data": {"intent": {"action": "camera_snapshot"}},
            "summary_for_user": "Captured 1 camera snapshot for Front Yard.",
            "say_hint": "",
            "artifacts": [
                {
                    "type": "image",
                    "name": "Front-Yard-snapshot.jpg",
                    "mimetype": "image/jpeg",
                    "device_name": "Front Yard",
                    "blob_key": "tater:blob:camera_snapshot:test",
                }
            ],
        }
        vision_result = {
            "ok": True,
            "data": {
                "description": "A delivery van is parked by the curb.",
                "model": "vision-model",
            },
            "summary_for_user": "A delivery van is parked by the curb.",
        }

        with patch.object(
            camera_control_module._DeviceVerbaRuntime,
            "_handle",
            new=AsyncMock(return_value=snapshot_result),
        ), patch.object(
            plugin,
            "_describe_snapshot_artifact",
            return_value=vision_result,
        ) as describe:
            result = await plugin._handle(
                {"query": "what is happening in the front yard?"},
                llm_client=None,
            )

        describe.assert_called_once()
        self.assertEqual(result["facts"]["vision_count"], 1)
        self.assertEqual(
            result["data"]["vision_descriptions"][0]["description"],
            "A delivery van is parked by the curb.",
        )
        self.assertEqual(
            result["summary_for_user"],
            "Front Yard: A delivery van is parked by the curb.",
        )


class UnifiedDeviceSelectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.plugin = device_control_module.DeviceControlPlugin()
        self.tree_plug = {
            "integration_id": "shelly",
            "id": "relay.tree",
            "name": "Christmas Tree Plug",
            "aliases": ["Christmas tree lights", "Tree lights"],
            "room": "Living Room",
            "capabilities": ["plug", "switch"],
            "category_ids": ["plug", "switch"],
            "actions": ["turn_on", "turn_off", "toggle"],
        }
        self.floor_light = {
            "integration_id": "hue",
            "id": "light.floor",
            "name": "Floor Lamp",
            "room": "Living Room",
            "capabilities": ["light", "dimmable"],
            "category_ids": ["light"],
            "actions": ["turn_on", "turn_off", "set_brightness"],
        }
        self.ceiling_fan = {
            "integration_id": "homeassistant",
            "id": "fan.ceiling",
            "name": "Ceiling Fan",
            "room": "Living Room",
            "capabilities": ["fan"],
            "category_ids": ["fan"],
            "actions": ["turn_on", "turn_off", "set_percentage"],
        }

    async def _resolve(self, query: str, llm_client=None):
        intent = await self.plugin._interpret_query({}, query, None)
        selected, needs = await self.plugin._select_devices(
            devices=[self.tree_plug, self.floor_light, self.ceiling_fan],
            payload={},
            query=query,
            intent=intent,
            llm_client=llm_client,
        )
        return intent, selected, needs

    async def test_light_word_can_resolve_to_plug_alias(self) -> None:
        intent, selected, needs = await self._resolve("Turn on the Christmas tree lights")

        self.assertEqual(intent["action"], "turn_on")
        self.assertEqual([row["id"] for row in selected], ["relay.tree"])
        self.assertEqual(needs, [])

    async def test_room_light_group_excludes_non_lighting_fan(self) -> None:
        intent, selected, needs = await self._resolve("Turn off the living room lights")

        self.assertEqual(intent["action"], "turn_off")
        self.assertEqual(
            {row["id"] for row in selected},
            {"relay.tree", "light.floor"},
        )
        self.assertEqual(needs, [])

    async def test_brightness_filters_to_dimmable_device(self) -> None:
        intent, selected, needs = await self._resolve("Set the living room lights to 30 percent")

        self.assertEqual(intent["action"], "set_brightness")
        self.assertEqual(intent["brightness_pct"], 30)
        self.assertEqual([row["id"] for row in selected], ["light.floor"])
        self.assertEqual(needs, [])

    async def test_fuzzy_score_cannot_select_without_ai(self) -> None:
        intent, selected, needs = await self._resolve("Turn on Christmas")

        self.assertEqual(intent["action"], "turn_on")
        self.assertEqual(selected, [])
        self.assertTrue(any("could not confidently match" in item.lower() for item in needs))

    async def test_ai_selects_every_non_exact_device_match(self) -> None:
        class PickingLlm:
            def __init__(self) -> None:
                self.calls = 0

            async def chat(self, **_kwargs):
                self.calls += 1
                return {
                    "message": {
                        "content": json.dumps({"device_id": "relay.tree"}),
                    }
                }

        llm_client = PickingLlm()
        intent, selected, needs = await self._resolve(
            "Turn on Christmas",
            llm_client=llm_client,
        )

        self.assertEqual(intent["action"], "turn_on")
        self.assertEqual(llm_client.calls, 1)
        self.assertEqual([row["id"] for row in selected], ["relay.tree"])
        self.assertEqual(needs, [])

    def test_overlapping_action_words_use_device_semantics(self) -> None:
        cases = {
            "set the office fan to 40 percent": "set_percentage",
            "close the bedroom blinds": "close",
            "set the thermostat to 70 degrees": "set_temperature",
            "open lock on the front door": "unlock",
            "mute the living room TV": "mute",
            "press mute on the den remote": "send_command",
            "activate the movie scene": "activate",
            "turn on the cleanup script": "run",
        }

        for query, expected in cases.items():
            with self.subTest(query=query):
                self.assertEqual(
                    self.plugin._normalize_action("", query),
                    expected,
                )


if __name__ == "__main__":
    unittest.main()
