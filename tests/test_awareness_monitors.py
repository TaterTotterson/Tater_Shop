import fnmatch
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.values = {}
        self.lists = {}

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hset(self, key, field=None, value=None, mapping=None):
        target = self.hashes.setdefault(key, {})
        if mapping:
            target.update(mapping)
        elif field is not None:
            target[field] = value
        return 1

    def hdel(self, key, field):
        target = self.hashes.get(key, {})
        existed = field in target
        target.pop(field, None)
        return int(existed)

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None, nx=False):
        del ex
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    def setex(self, key, _seconds, value):
        self.values[key] = value
        return True

    def incr(self, key):
        value = int(self.values.get(key, 0)) + 1
        self.values[key] = value
        return value

    def delete(self, key):
        self.values.pop(key, None)
        self.lists.pop(key, None)
        return 1

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)
        return len(self.lists[key])

    def rpop(self, key):
        rows = self.lists.get(key, [])
        return rows.pop() if rows else None

    def llen(self, key):
        return len(self.lists.get(key, []))

    def lrange(self, key, start, end):
        rows = self.lists.get(key, [])
        return rows[start:] if end < 0 else rows[start : end + 1]

    def ltrim(self, key, start, end):
        rows = self.lists.get(key, [])
        self.lists[key] = rows[start:] if end < 0 else rows[start : end + 1]

    def lset(self, key, index, value):
        self.lists[key][index] = value
        return True

    def scan_iter(self, match="*"):
        keys = [*self.hashes, *self.values, *self.lists]
        return iter(key for key in dict.fromkeys(keys) if fnmatch.fnmatch(key, match))


def sample_registry():
    camera = {
        "integration_id": "unifi_protect",
        "integration_name": "UniFi Protect",
        "id": "cam-front",
        "ref": "camera:cam-front",
        "name": "Front Camera",
        "room": "Front Yard",
        "category_ids": ["camera"],
        "capabilities": ["camera", "snapshot", "video_clip"],
        "actions": ["camera_snapshot", "camera_clip"],
        "event_sources": [
            {"type": "motion", "ref": "binary_sensor.unifi_cam-front_motion"},
            {"type": "smart_person", "ref": "binary_sensor.unifi_cam-front_smart_person"},
            {"type": "smart_animal", "ref": "binary_sensor.unifi_cam-front_smart_animal"},
            {"type": "doorbell", "ref": "binary_sensor.unifi_cam-front_doorbell"},
        ],
    }
    doorbell = {
        "integration_id": "unifi_protect",
        "integration_name": "UniFi Protect",
        "id": "doorbell-front",
        "ref": "camera:doorbell-front",
        "name": "Front Doorbell",
        "room": "Front Door",
        "category_ids": ["device"],
        "type": "doorbell_camera",
        "capabilities": ["camera", "snapshot", "video_clip", "doorbell"],
        "actions": ["camera_snapshot", "camera_clip"],
        "event_sources": [
            {"type": "motion", "ref": "binary_sensor.unifi_doorbell-front_motion", "state_on": "on", "state_off": "off"},
            {"type": "doorbell", "ref": "event.unifi_doorbell-front_doorbell", "state_on": "on", "state_off": "off"},
        ],
    }
    sensor = {
        "integration_id": "homeassistant",
        "integration_name": "Home Assistant",
        "id": "binary_sensor.back_door",
        "ref": "binary_sensor.back_door",
        "name": "Back Door",
        "room": "Back Entry",
        "category_ids": ["entry_sensor"],
        "event_sources": [{"type": "contact", "ref": "binary_sensor.back_door"}],
    }
    unifi_sensor = {
        "integration_id": "unifi_protect",
        "integration_name": "UniFi Protect",
        "id": "sensor-back-door",
        "ref": "binary_sensor.unifi_sensor_sensor-back-door",
        "name": "Back Door",
        "room": "Back Yard",
        "type": "entry_sensor",
        "category_ids": ["entry_sensor"],
        "capabilities": ["entry_sensor", "door", "motion"],
        "status": "online",
        "state": "closed",
        "event_sources": [
            {
                "type": "door",
                "ref": "binary_sensor.unifi_sensor_sensor-back-door",
                "state_on": "open",
                "state_off": "closed",
            },
            {
                "type": "motion",
                "ref": "binary_sensor.unifi_sensor_sensor-back-door",
                "state_on": "motion",
                "state_off": "clear",
            },
        ],
    }
    hue_sensor = {
        "integration_id": "hue",
        "integration_name": "Philips Hue",
        "id": "hue-motion-hall",
        "ref": "binary_sensor.hue_motion_hall",
        "name": "Hall Motion",
        "room": "Hall",
        "type": "sensor",
        "category_ids": ["motion"],
        "state": "clear",
        "event_sources": [
            {
                "type": "motion",
                "ref": "binary_sensor.hue_motion_hall",
                "state_on": "motion",
                "state_off": "clear",
            }
        ],
    }
    unsupported_sensor = {
        "integration_id": "unifi_protect",
        "integration_name": "UniFi Protect",
        "id": "capability-only-sensor",
        "ref": "binary_sensor.capability_only",
        "name": "Capability Only",
        "room": "Lab",
        "type": "sensor",
        "category_ids": ["motion"],
        "capabilities": ["motion"],
        "state": "clear",
        "event_sources": [],
    }
    return {
        "devices": [camera, doorbell, sensor, unifi_sensor, hue_sensor, unsupported_sensor],
        "categories": [],
        "rooms": [],
    }


def load_awareness_core():
    fake_redis = FakeRedis()
    injected = (
        "announcement_targets",
        "helpers",
        "notify",
        "speech_settings",
        "speech_tts",
        "tateros",
        "tateros.integration_store",
        "vision_settings",
    )
    previous = {name: sys.modules.get(name) for name in injected}

    helpers = types.ModuleType("helpers")
    helpers.redis_client = fake_redis
    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda **_kwargs: None
    helpers.get_primary_llm_client_from_env = lambda **_kwargs: None
    sys.modules["helpers"] = helpers

    notify = types.ModuleType("notify")
    notify.dispatch_notification = lambda **_kwargs: None
    notify.notifier_destination_catalog = lambda **_kwargs: {"platforms": []}
    sys.modules["notify"] = notify

    speech_settings = types.ModuleType("speech_settings")
    speech_settings.get_speech_settings = lambda: {}
    sys.modules["speech_settings"] = speech_settings

    speech_tts = types.ModuleType("speech_tts")
    speech_tts.speak_announcement_targets = lambda **_kwargs: None
    sys.modules["speech_tts"] = speech_tts

    vision_settings = types.ModuleType("vision_settings")
    vision_settings.get_vision_settings = lambda **_kwargs: {}
    sys.modules["vision_settings"] = vision_settings

    announcement_targets = types.ModuleType("announcement_targets")
    announcement_targets.build_announcement_target_options = lambda **_kwargs: []
    sys.modules["announcement_targets"] = announcement_targets

    integration_store = types.ModuleType("tateros.integration_store")
    integration_store.integration_module = lambda _integration_id: None
    integration_store.get_integration_enabled = lambda _integration_id: True
    tateros = types.ModuleType("tateros")
    tateros.integration_store = integration_store
    sys.modules["tateros"] = tateros
    sys.modules["tateros.integration_store"] = integration_store

    path = Path(__file__).resolve().parents[1] / "cores" / "awareness_core.py"
    try:
        spec = importlib.util.spec_from_file_location("awareness_monitor_test_module", path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
    finally:
        for name, old_module in previous.items():
            if old_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_module
    return module, fake_redis


class AwarenessMonitorTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.core, cls.redis = load_awareness_core()

    def setUp(self):
        self.redis.hashes.clear()
        self.redis.values.clear()
        self.redis.lists.clear()
        # Historical face and event fixtures use fixed timestamps; retention
        # behavior is covered separately and must not make these tests expire.
        self.redis.hashes["awareness_core_settings"] = {"events_retention": "forever"}

    def _add_monitor(
        self,
        kind,
        device,
        area,
        trigger_events=None,
        face_id_enabled=None,
        description_mode=None,
        linked_camera="",
        linked_camera_description_mode="image",
        notifications_enabled=None,
        notification_destinations=None,
    ):
        values = {"kind": kind, "device": device, "area": area, "enabled": True}
        if trigger_events is not None:
            values["trigger_events"] = trigger_events
        if face_id_enabled is not None:
            values["face_id_enabled"] = face_id_enabled
        if description_mode is not None:
            values["description_mode"] = description_mode
        if notifications_enabled is not None:
            values["notifications_enabled"] = notifications_enabled
        if notification_destinations is not None:
            values["notification_destinations"] = notification_destinations
        if linked_camera:
            provider = linked_camera.split("|", 1)[0]
            values.update(
                {
                    "linked_camera_integration": f"camera::{provider}",
                    "linked_camera": linked_camera,
                    "linked_camera_description_mode": linked_camera_description_mode,
                }
            )
        with patch.object(self.core, "_monitor_registry", return_value=sample_registry()):
            result = self.core.handle_htmlui_tab_action(
                action="awareness_add_monitor",
                payload={"values": values},
                redis_client=self.redis,
            )
        return self.core._get_monitor(self.redis, result["id"])

    def test_add_monitor_uses_device_event_sources_without_rules(self):
        monitor = self._add_monitor("camera", "unifi_protect|cam-front", "Front Yard")
        self.assertEqual(monitor["kind"], "camera")
        self.assertEqual(monitor["device_ref"], "camera:cam-front")
        self.assertIn("binary_sensor.unifi_cam-front_smart_person", monitor["event_refs"])
        self.assertTrue(monitor["face_id_enabled"])
        self.assertEqual(monitor["description_mode"], "image")
        self.assertFalse(monitor["notifications_enabled"])
        self.assertEqual(monitor["notification_destinations"], [])
        self.assertEqual(self.redis.hgetall("awareness:rules"), {})

    def test_notification_delivery_requires_at_least_one_destination(self):
        with (
            patch.object(self.core, "_monitor_registry", return_value=sample_registry()),
            self.assertRaisesRegex(ValueError, "Choose at least one destination"),
        ):
            self.core.handle_htmlui_tab_action(
                action="awareness_add_monitor",
                payload={
                    "values": {
                        "kind": "sensor",
                        "device": "homeassistant|binary_sensor.back_door",
                        "area": "Back Door",
                        "enabled": True,
                        "notifications_enabled": True,
                    }
                },
                redis_client=self.redis,
            )

    def test_awareness_ui_lists_connected_notification_destinations(self):
        destination = self.core._encode_notification_destination(
            "little_spud",
            {"device_id": "spud-phone", "device_name": "Spud Phone"},
        )
        catalog = {
            "platforms": [
                {
                    "platform": "little_spud",
                    "label": "Little Spud",
                    "requires_target": True,
                    "destinations": [
                        {
                            "label": "Spud Phone",
                            "targets": {
                                "device_id": "spud-phone",
                                "device_name": "Spud Phone",
                            },
                        }
                    ],
                }
            ]
        }
        with (
            patch.object(self.core, "_monitor_registry", return_value=sample_registry()),
            patch.object(self.core, "notifier_destination_catalog", return_value=catalog),
        ):
            manager = self.core._awareness_manager_ui(self.redis)
        fields = manager["add_form"]["fields"]
        enabled = next(field for field in fields if field.get("key") == "notifications_enabled")
        targets = next(field for field in fields if field.get("key") == "notification_destinations")
        self.assertFalse(enabled["value"])
        self.assertEqual(targets["show_when"], {"source_key": "notifications_enabled", "equals": True})
        self.assertIn(
            {"value": destination, "label": "Little Spud: Spud Phone"},
            targets["options"],
        )

    def test_face_id_can_be_disabled_per_camera_without_disabling_monitoring(self):
        monitor = self._add_monitor(
            "camera",
            "unifi_protect|cam-front",
            "Front Yard",
            face_id_enabled=False,
        )

        self.assertTrue(monitor["enabled"])
        self.assertFalse(monitor["face_id_enabled"])

        with patch.object(self.core, "_monitor_registry", return_value=sample_registry()):
            self.core.handle_htmlui_tab_action(
                action="awareness_save_monitor",
                payload={
                    "id": monitor["id"],
                    "values": {
                        "kind": "camera",
                        "device": "unifi_protect|cam-front",
                        "area": "Front Yard",
                        "enabled": True,
                        "face_id_enabled": True,
                    },
                },
                redis_client=self.redis,
            )

        saved = self.core._get_monitor(self.redis, monitor["id"])
        self.assertTrue(saved["enabled"])
        self.assertTrue(saved["face_id_enabled"])

    def test_legacy_camera_monitors_keep_face_id_enabled(self):
        monitor = self.core._normalize_monitor(
            {
                "kind": "camera",
                "provider": "unifi_protect",
                "device_id": "cam-front",
                "device_ref": "camera:cam-front",
                "name": "Front Camera",
            }
        )
        sensor = self.core._normalize_monitor(
            {
                "kind": "sensor",
                "provider": "homeassistant",
                "device_id": "binary_sensor.back_door",
                "device_ref": "binary_sensor.back_door",
                "name": "Back Door",
                "face_id_enabled": True,
            }
        )

        self.assertTrue(monitor["face_id_enabled"])
        self.assertFalse(sensor["face_id_enabled"])
        self.assertEqual(monitor["description_mode"], "image")
        self.assertEqual(sensor["description_mode"], "")

    def test_same_device_cannot_be_monitored_twice(self):
        self._add_monitor("camera", "unifi_protect|cam-front", "Front Yard")
        with self.assertRaisesRegex(ValueError, "already being monitored"):
            self._add_monitor("camera", "unifi_protect|cam-front", "Front Door")

    def test_camera_monitor_matches_active_detection_but_not_clear(self):
        monitor = self._add_monitor("camera", "unifi_protect|cam-front", "Front Yard")
        active = self.core._monitor_matches_event(
            monitor,
            provider="unifi_protect",
            entity_id="binary_sensor.unifi_cam-front_smart_person",
            new_state={"state": "on"},
            old_state={"state": "off"},
        )
        clear = self.core._monitor_matches_event(
            monitor,
            provider="unifi_protect",
            entity_id="binary_sensor.unifi_cam-front_smart_person",
            new_state={"state": "off"},
            old_state={"state": "on"},
        )
        self.assertTrue(active)
        self.assertFalse(clear)

    def test_camera_monitor_only_captures_selected_detection_types(self):
        monitor = self._add_monitor(
            "camera",
            "unifi_protect|cam-front",
            "Front Yard",
            ["person", "animal"],
        )
        self.assertEqual(monitor["trigger_events"], ["person", "animal"])
        self.assertFalse(
            self.core._monitor_matches_event(
                monitor,
                provider="unifi_protect",
                entity_id="binary_sensor.unifi_cam-front_motion",
                new_state={"state": "on"},
                old_state={"state": "off"},
            )
        )
        self.assertTrue(
            self.core._monitor_matches_event(
                monitor,
                provider="unifi_protect",
                entity_id="binary_sensor.unifi_cam-front_smart_animal",
                new_state={"state": "on"},
                old_state={"state": "off"},
            )
        )

    def test_motion_only_camera_exposes_only_motion_capture(self):
        device = {
            "integration_id": "homeassistant",
            "id": "camera.garage",
            "ref": "camera.garage",
            "type": "camera",
            "category_ids": ["camera"],
            "event_sources": [
                {"type": "motion", "ref": "binary_sensor.garage_motion", "state_on": "on", "state_off": "off"}
            ],
        }

        self.assertEqual(self.core._monitor_trigger_values_for_device(device), ["motion"])

    def test_video_descriptions_are_only_offered_for_clip_capable_cameras(self):
        registry = sample_registry()
        options, dependency = self.core._monitor_description_mode_dependency(
            registry,
            current_device="unifi_protect|cam-front",
        )
        self.assertEqual([row["value"] for row in options], ["image", "video"])
        door_sensor = dependency["options_by_source"].get("unifi_protect|sensor-back-door")
        self.assertIsNone(door_sensor)

        image_only = {
            "integration_id": "homeassistant",
            "id": "camera.image_only",
            "ref": "camera.image_only",
            "type": "camera",
            "category_ids": ["camera"],
            "capabilities": ["camera", "snapshot"],
            "actions": ["camera_snapshot"],
            "event_sources": [{"type": "motion", "ref": "binary_sensor.image_only_motion"}],
        }
        self.assertFalse(self.core._monitor_device_supports_description_mode(image_only, "video"))
        self.assertTrue(self.core._monitor_device_supports_description_mode(image_only, "image"))

    def test_face_burst_gate_keeps_motion_and_uses_vision_for_smart_nonperson_events(self):
        self.assertTrue(self.core._face_burst_should_run("motion", "A quiet driveway."))
        self.assertTrue(self.core._face_burst_should_run("person", "A figure is near the porch."))
        self.assertTrue(self.core._face_burst_should_run("doorbell", "The porch is visible."))
        self.assertTrue(
            self.core._face_burst_should_run(
                "animal",
                "A delivery driver is walking across the yard.",
            )
        )
        self.assertTrue(
            self.core._face_burst_should_run(
                "vehicle",
                "Two people stand next to a parked truck.",
            )
        )
        self.assertFalse(self.core._face_burst_should_run("animal", "A dog runs across the lawn."))
        self.assertFalse(self.core._face_burst_should_run("package", "A box is on the porch."))

    def test_capture_events_use_only_integration_declared_event_sources(self):
        device = {
            "type": "entry_sensor",
            "category_ids": ["entry_sensor", "motion"],
            "capabilities": ["door", "motion", "person"],
            "features": ["animal"],
            "event_sources": [
                {
                    "type": "door",
                    "ref": "binary_sensor.unifi_front_door",
                    "state_on": "open",
                    "state_off": "closed",
                },
                {
                    "type": "motion",
                    "ref": "binary_sensor.unifi_front_door",
                    "state_on": "motion",
                    "state_off": "clear",
                },
            ],
        }
        capability_only = {**device, "event_sources": []}

        self.assertEqual(
            self.core._monitor_trigger_values_for_device(device),
            ["opens", "closes", "motion"],
        )
        self.assertEqual(self.core._monitor_trigger_values_for_device(capability_only), [])

    def test_sensor_monitor_can_capture_open_without_capture_on_close(self):
        monitor = self._add_monitor(
            "sensor",
            "homeassistant|binary_sensor.back_door",
            "Back Door",
            ["opens"],
        )
        self.assertTrue(
            self.core._monitor_matches_event(
                monitor,
                provider="homeassistant",
                entity_id="binary_sensor.back_door",
                new_state={"state": "on"},
                old_state={"state": "off"},
            )
        )
        self.assertFalse(
            self.core._monitor_matches_event(
                monitor,
                provider="homeassistant",
                entity_id="binary_sensor.back_door",
                new_state={"state": "off"},
                old_state={"state": "on"},
            )
        )

    def test_sensor_monitor_can_optionally_link_a_capability_reported_camera(self):
        monitor = self._add_monitor(
            "sensor",
            "homeassistant|binary_sensor.back_door",
            "Back Door",
            ["opens"],
            linked_camera="unifi_protect|cam-front",
            linked_camera_description_mode="video",
        )

        self.assertEqual(monitor["linked_camera_provider"], "unifi_protect")
        self.assertEqual(monitor["linked_camera_device_id"], "cam-front")
        self.assertEqual(monitor["linked_camera_device_ref"], "camera:cam-front")
        self.assertEqual(monitor["linked_camera_name"], "Front Camera")
        self.assertEqual(monitor["linked_camera_description_mode"], "video")

    async def test_linked_camera_adds_snapshot_and_description_to_sensor_event(self):
        monitor = self._add_monitor(
            "sensor",
            "homeassistant|binary_sensor.back_door",
            "Back Door",
            ["opens"],
            linked_camera="unifi_protect|cam-front",
        )
        event = {
            "entity_id": "binary_sensor.back_door",
            "new_state": {"state": "on"},
            "old_state": {"state": "off"},
        }
        with (
            patch.object(
                self.core,
                "_capture_camera_snapshot",
                new=AsyncMock(return_value=(b"linked-jpeg", "image/jpeg")),
            ),
            patch.object(
                self.core,
                "_vision_describe",
                new=AsyncMock(return_value="A person is walking through the doorway."),
            ),
        ):
            result = await self.core._execute_sensor_monitor(monitor, event)

        self.assertIn("Back Door opened.", result["summary"])
        self.assertIn("A person is walking through the doorway.", result["summary"])
        stored = json.loads(self.redis.lists["tater:automations:events:back_door"][0])
        self.assertEqual(stored["data"]["description_media"], "image")
        self.assertEqual(stored["data"]["camera_entity"], "cam-front")
        self.assertTrue(stored["snapshot_id"])

    async def test_linked_camera_video_is_stored_on_the_sensor_event(self):
        monitor = self._add_monitor(
            "sensor",
            "homeassistant|binary_sensor.back_door",
            "Back Door",
            ["opens"],
            linked_camera="unifi_protect|cam-front",
            linked_camera_description_mode="video",
        )
        event = {
            "entity_id": "binary_sensor.back_door",
            "new_state": {"state": "on", "last_changed": "2026-08-20T21:00:00Z"},
            "old_state": {"state": "off"},
        }
        with (
            patch.object(
                self.core,
                "_capture_camera_snapshot",
                new=AsyncMock(return_value=(b"linked-jpeg", "image/jpeg")),
            ),
            patch.object(
                self.core,
                "_capture_camera_clip",
                new=AsyncMock(
                    return_value=(
                        b"linked-video",
                        "video/mp4",
                        {"duration_seconds": 8},
                    )
                ),
            ),
            patch.object(
                self.core,
                "_video_describe",
                new=AsyncMock(return_value="A person opens the door and walks inside."),
            ),
        ):
            result = await self.core._execute_sensor_monitor(monitor, event)

        stored = json.loads(self.redis.lists["tater:automations:events:back_door"][0])
        self.assertEqual(result["description_mode"], "video")
        self.assertEqual(stored["data"]["description_media"], "video")
        self.assertTrue(stored["clip_id"])
        self.assertEqual(stored["data"]["clip_duration_seconds"], 8)

    async def test_linked_camera_failure_does_not_drop_the_sensor_event(self):
        monitor = self._add_monitor(
            "sensor",
            "homeassistant|binary_sensor.back_door",
            "Back Door",
            ["opens"],
            linked_camera="unifi_protect|cam-front",
        )
        event = {
            "entity_id": "binary_sensor.back_door",
            "new_state": {"state": "on"},
            "old_state": {"state": "off"},
        }
        with patch.object(
            self.core,
            "_capture_camera_snapshot",
            new=AsyncMock(side_effect=RuntimeError("camera offline")),
        ):
            result = await self.core._execute_sensor_monitor(monitor, event)

        self.assertEqual(result["summary"], "Back Door opened.")
        stored = json.loads(self.redis.lists["tater:automations:events:back_door"][0])
        self.assertEqual(stored["message"], "Back Door opened.")
        self.assertIn("camera offline", stored["data"]["capture_error"])
        self.assertEqual(stored["data"]["snapshot_status"], "capture_failed")

    def test_unifi_door_source_uses_open_and_close_events(self):
        monitor = self._add_monitor(
            "sensor",
            "unifi_protect|sensor-back-door",
            "Back Door",
            ["opens"],
        )

        self.assertTrue(
            self.core._monitor_matches_event(
                monitor,
                provider="unifi_protect",
                entity_id="binary_sensor.unifi_sensor_sensor-back-door",
                new_state={"state": "open"},
                old_state={"state": "closed"},
            )
        )
        self.assertFalse(
            self.core._monitor_matches_event(
                monitor,
                provider="unifi_protect",
                entity_id="binary_sensor.unifi_sensor_sensor-back-door",
                new_state={"state": "closed"},
                old_state={"state": "open"},
            )
        )

    def test_doorbell_capture_does_not_treat_motion_as_a_button_press(self):
        monitor = self._add_monitor(
            "camera",
            "unifi_protect|doorbell-front",
            "Front Door",
            ["doorbell"],
        )
        self.assertFalse(
            self.core._monitor_matches_event(
                monitor,
                provider="unifi_protect",
                entity_id="binary_sensor.unifi_doorbell-front_motion",
                new_state={"state": "on"},
                old_state={"state": "off"},
            )
        )
        self.assertTrue(
            self.core._monitor_matches_event(
                monitor,
                provider="unifi_protect",
                entity_id="event.unifi_doorbell-front_doorbell",
                new_state={"state": "on"},
                old_state={"state": "off"},
            )
        )

    async def test_camera_monitor_uses_the_matching_event_type_and_stores_snapshot(self):
        monitor = self._add_monitor("camera", "unifi_protect|cam-front", "Front Yard")
        event = {
            "entity_id": "binary_sensor.unifi_cam-front_smart_person",
            "new_state": {"state": "on"},
            "old_state": {"state": "off"},
        }
        with (
            patch.object(
                self.core,
                "_capture_camera_snapshot",
                new=AsyncMock(return_value=(b"jpeg-bytes", "image/jpeg")),
            ),
            patch.object(
                self.core,
                "_vision_describe",
                new=AsyncMock(return_value="A person is walking toward the front door."),
            ),
            patch.object(self.core, "_schedule_face_burst", return_value="face-image") as schedule,
        ):
            result = await self.core._execute_camera_monitor(monitor, event)
        self.assertEqual(result["event_type"], "person")
        self.assertEqual(schedule.call_args.kwargs["video_bytes"], b"")
        stored = json.loads(self.redis.lists["tater:automations:events:front_yard"][0])
        self.assertEqual(stored["data"]["event_type"], "person")
        self.assertTrue(stored["snapshot_id"])

    async def test_video_camera_monitor_analyzes_clip_and_keeps_snapshot_for_history(self):
        monitor = self._add_monitor(
            "camera",
            "unifi_protect|cam-front",
            "Front Yard",
            description_mode="video",
        )
        event = {
            "entity_id": "binary_sensor.unifi_cam-front_smart_person",
            "new_state": {
                "state": "on",
                "attributes": {"event_id": "evt-1", "event_start": 1_786_486_900_000},
            },
            "old_state": {"state": "off"},
        }
        with (
            patch.object(
                self.core,
                "_capture_camera_snapshot",
                new=AsyncMock(return_value=(b"jpeg-bytes", "image/jpeg")),
            ),
            patch.object(
                self.core,
                "_capture_camera_clip",
                new=AsyncMock(
                    return_value=(
                        b"video-bytes",
                        "video/mp4",
                        {"event_id": "evt-1", "duration_seconds": 8},
                    )
                ),
            ) as capture_clip,
            patch.object(
                self.core,
                "_video_describe",
                new=AsyncMock(return_value="A person walks up and leaves a package."),
            ),
            patch.object(self.core, "_vision_describe", new=AsyncMock()) as image_describe,
            patch.object(self.core, "_schedule_face_burst", return_value="face-video") as schedule,
        ):
            result = await self.core._execute_camera_monitor(monitor, event)

        self.assertEqual(result["description_mode"], "video")
        image_describe.assert_not_awaited()
        self.assertEqual(schedule.call_args.kwargs["video_bytes"], b"video-bytes")
        self.assertEqual(schedule.call_args.kwargs["video_content_type"], "video/mp4")
        self.assertEqual(schedule.call_args.kwargs["video_duration_seconds"], 8.0)
        self.assertEqual(capture_clip.await_args.args[2]["event_id"], "evt-1")
        stored = json.loads(self.redis.lists["tater:automations:events:front_yard"][0])
        self.assertEqual(stored["data"]["description_mode"], "video")
        self.assertEqual(stored["data"]["description_media"], "video")
        self.assertEqual(stored["data"]["clip_bytes"], len(b"video-bytes"))
        self.assertTrue(stored["clip_id"])
        self.assertEqual(stored["data"]["clip_id"], stored["clip_id"])
        self.assertEqual(stored["data"]["clip_stored_bytes"], len(b"video-bytes"))
        self.assertTrue(stored["snapshot_id"])
        media = self.core.get_htmlui_tab_media(
            media_id=stored["clip_id"],
            redis_client=self.redis,
        )
        self.assertEqual(media["bytes"], b"video-bytes")
        self.assertEqual(media["content_type"], "video/mp4")
        card = self.core._event_forms_from_events(self.redis, [stored], list_view=False)[0]
        video_field = next(field for field in card["fields"] if field.get("type") == "video")
        self.assertEqual(video_field["src"], f"/api/cores/awareness_core/media/{stored['clip_id']}")
        self.assertTrue(video_field["poster"].startswith("data:image/jpeg;base64,"))
        self.assertTrue(video_field["reset_to_poster"])
        self.assertFalse(any(field.get("type") == "image" for field in card["fields"]))

    async def test_notification_reuses_saved_description_media_and_face_id_results(self):
        destination = self.core._encode_notification_destination(
            "little_spud",
            {"device_id": "spud-phone"},
        )
        monitor = self._add_monitor(
            "camera",
            "unifi_protect|cam-front",
            "Front Yard",
            notifications_enabled=True,
            notification_destinations=[destination],
        )
        snapshot = self.core._store_event_snapshot(
            self.redis,
            b"saved-jpeg",
            content_type="image/jpeg",
        )
        self.core._save_face_identity(
            self.redis,
            {"id": "face-fred", "name": "Fred"},
        )
        self.core._save_face_session(
            self.redis,
            {
                "id": "face-session",
                "event_id": "event-with-face",
                "status": "complete",
                "identity_ids": ["face-fred"],
            },
        )
        event = {
            "id": "event-with-face",
            "source": "front_yard",
            "title": "Front Yard Camera",
            "type": "camera_event",
            "message": "A person walked up and left a package.",
            "ha_time": "2026-08-21T10:00:00",
            "snapshot_id": snapshot["snapshot_id"],
            "data": {
                "area": "Front Yard",
                "snapshot_id": snapshot["snapshot_id"],
                "face_session_id": "face-session",
            },
        }
        self.core._append_event(self.redis, source="Front Yard", payload=event)
        dispatch = AsyncMock(return_value="Queued notification for Little Spud")

        with patch.object(self.core, "dispatch_notification", new=dispatch):
            result = await self.core._dispatch_awareness_event_notification(monitor, event)

        self.assertTrue(result["ok"])
        self.assertEqual(result["sent_count"], 1)
        request = dispatch.await_args.kwargs
        self.assertEqual(request["content"], "A person walked up and left a package.\n\nFace ID: Fred recognized.")
        self.assertEqual(request["targets"], {"device_id": "spud-phone"})
        self.assertEqual(request["attachments"][0]["type"], "image")
        self.assertEqual(request["attachments"][0]["bytes"], b"saved-jpeg")
        stored = json.loads(self.redis.lists["tater:automations:events:front_yard"][0])
        self.assertEqual(stored["data"]["notification_status"], "sent")
        self.assertEqual(stored["data"]["notification_sent_count"], 1)

    async def test_notification_failure_does_not_remove_or_fail_the_awareness_event(self):
        destination = self.core._encode_notification_destination("ntfy", {"topic": "home"})
        monitor = self._add_monitor(
            "sensor",
            "homeassistant|binary_sensor.back_door",
            "Back Door",
            notifications_enabled=True,
            notification_destinations=[destination],
        )
        event = {
            "id": "sensor-event",
            "source": "back_door",
            "title": "Back Door",
            "type": "door_sensor_opened",
            "message": "Back Door opened.",
            "ha_time": "2026-08-21T10:00:00",
            "data": {"area": "Back Door"},
        }
        self.core._append_event(self.redis, source="Back Door", payload=event)

        with patch.object(
            self.core,
            "dispatch_notification",
            new=AsyncMock(side_effect=RuntimeError("push service offline")),
        ):
            result = await self.core._dispatch_awareness_event_notification(monitor, event)

        self.assertFalse(result["ok"])
        self.assertEqual(result["sent_count"], 0)
        stored = json.loads(self.redis.lists["tater:automations:events:back_door"][0])
        self.assertEqual(stored["message"], "Back Door opened.")
        self.assertEqual(stored["data"]["notification_status"], "failed")
        self.assertIn("push service offline", stored["data"]["notification_errors"])

    def test_event_clip_storage_is_bounded(self):
        with patch.object(self.core, "_clip_max_bytes", return_value=4):
            stored = self.core._store_event_clip(
                self.redis,
                b"video-bytes",
                content_type="video/mp4",
            )

        self.assertFalse(stored["stored"])
        self.assertEqual(stored["reason"], "too_large")
        self.assertEqual(stored["max_bytes"], 4)
        self.assertFalse(
            any(key.startswith("awareness:event_clip:") for key in self.redis.values)
        )

    def test_notification_prefers_the_saved_clip_over_the_snapshot(self):
        snapshot = self.core._store_event_snapshot(self.redis, b"poster", content_type="image/jpeg")
        clip = self.core._store_event_clip(self.redis, b"clip-bytes", content_type="video/mp4")
        event = {
            "id": "video-event",
            "snapshot_id": snapshot["snapshot_id"],
            "clip_id": clip["clip_id"],
            "data": {},
        }

        attachments = self.core._notification_attachments_for_event(self.redis, event)

        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0]["type"], "video")
        self.assertEqual(attachments[0]["mimetype"], "video/mp4")
        self.assertEqual(attachments[0]["bytes"], b"clip-bytes")

    def test_event_clip_is_remuxed_for_fast_start_playback(self):
        def mp4_box(box_type, payload=b""):
            return (8 + len(payload)).to_bytes(4, "big") + box_type + payload

        source = mp4_box(b"ftyp", b"isom") + mp4_box(b"mdat", b"video") + mp4_box(b"moov", b"index")
        prepared = mp4_box(b"ftyp", b"isom") + mp4_box(b"moov", b"index") + mp4_box(b"mdat", b"video")

        def fake_run(command, **_kwargs):
            Path(command[-1]).write_bytes(prepared)
            return types.SimpleNamespace(returncode=0, stderr=b"")

        with (
            patch.object(self.core, "_event_clip_ffmpeg_path", return_value="/usr/bin/ffmpeg"),
            patch.object(self.core.subprocess, "run", side_effect=fake_run) as run,
        ):
            result, content_type, metadata = self.core._prepare_event_clip_for_playback(
                source,
                "video/mp4",
            )

        self.assertEqual(result, prepared)
        self.assertEqual(content_type, "video/mp4")
        self.assertTrue(metadata["playback_fast_start"])
        self.assertTrue(metadata["playback_prepared"])
        self.assertEqual(metadata["playback_original_bytes"], len(source))
        self.assertIn("+faststart", run.call_args.args[0])

    def test_event_clip_keeps_original_when_ffmpeg_is_unavailable(self):
        def mp4_box(box_type, payload=b""):
            return (8 + len(payload)).to_bytes(4, "big") + box_type + payload

        source = mp4_box(b"ftyp", b"isom") + mp4_box(b"mdat", b"video") + mp4_box(b"moov", b"index")
        with patch.object(self.core, "_event_clip_ffmpeg_path", return_value=""):
            result, content_type, metadata = self.core._prepare_event_clip_for_playback(
                source,
                "video/mp4",
            )

        self.assertEqual(result, source)
        self.assertEqual(content_type, "video/mp4")
        self.assertFalse(metadata["playback_fast_start"])
        self.assertFalse(metadata["playback_prepared"])

    def test_face_id_frames_are_sampled_across_the_event_clip(self):
        def fake_run(command, **_kwargs):
            output_pattern = command[-1]
            for index in range(1, 6):
                Path(output_pattern.replace("%03d", f"{index:03d}")).write_bytes(
                    f"frame-{index}".encode()
                )
            return types.SimpleNamespace(returncode=0, stderr=b"")

        with (
            patch.object(self.core, "_event_clip_ffmpeg_path", return_value="/usr/bin/ffmpeg"),
            patch.object(self.core.subprocess, "run", side_effect=fake_run) as run,
        ):
            frames = self.core._extract_face_frames_from_clip(
                b"video-bytes",
                "video/mp4",
                duration_seconds=8,
                frame_count=5,
            )

        self.assertEqual(frames, [f"frame-{index}".encode() for index in range(1, 6)])
        command = run.call_args.args[0]
        self.assertEqual(command[command.index("-vf") + 1], "fps=0.625000")
        self.assertEqual(command[command.index("-frames:v") + 1], "5")

    async def test_camera_with_face_id_disabled_does_not_schedule_a_face_burst(self):
        monitor = self._add_monitor(
            "camera",
            "unifi_protect|cam-front",
            "Front Yard",
            face_id_enabled=False,
        )
        event = {
            "entity_id": "binary_sensor.unifi_cam-front_smart_person",
            "new_state": {"state": "on"},
            "old_state": {"state": "off"},
        }
        with (
            patch.object(
                self.core,
                "_capture_camera_snapshot",
                new=AsyncMock(return_value=(b"jpeg-bytes", "image/jpeg")),
            ),
            patch.object(
                self.core,
                "_vision_describe",
                new=AsyncMock(return_value="A person is walking toward the front door."),
            ),
            patch.object(self.core, "_schedule_face_burst", return_value="unexpected") as schedule,
        ):
            result = await self.core._execute_camera_monitor(monitor, event)

        schedule.assert_not_called()
        self.assertEqual(result["face_session_id"], "")
        stored = json.loads(self.redis.lists["tater:automations:events:front_yard"][0])
        self.assertNotIn("face_session_id", stored["data"])

    async def test_selected_sensor_event_is_queued_and_stored(self):
        monitor = self._add_monitor("sensor", "homeassistant|binary_sensor.back_door", "Back Door")
        await self.core._handle_trigger_state_change(
            provider="homeassistant",
            entity_id="binary_sensor.back_door",
            new_state={"state": "on", "attributes": {"friendly_name": "Back Door"}},
            old_state={"state": "off", "attributes": {"friendly_name": "Back Door"}},
        )
        job = self.core._dequeue_execution(self.redis)
        self.assertEqual(job["monitor_id"], monitor["id"])
        await self.core._execute_monitor(monitor, job["event"])
        rows = self.redis.lists["tater:automations:events:back_door"]
        event = json.loads(rows[0])
        self.assertEqual(event["type"], "door_sensor_open")
        self.assertEqual(event["message"], "Back Door opened.")

    async def test_sensor_monitor_delivers_its_completed_event_when_notifications_are_enabled(self):
        destination = self.core._encode_notification_destination("little_spud", {"device_id": "phone"})
        monitor = self._add_monitor(
            "sensor",
            "homeassistant|binary_sensor.back_door",
            "Back Door",
            notifications_enabled=True,
            notification_destinations=[destination],
        )
        deliver = AsyncMock(return_value={"ok": True, "sent_count": 1})
        with patch.object(self.core, "_deliver_awareness_event_notification", new=deliver):
            result = await self.core._execute_sensor_monitor(
                monitor,
                {
                    "entity_id": "binary_sensor.back_door",
                    "new_state": {"state": "on"},
                    "old_state": {"state": "off"},
                },
            )

        self.assertTrue(result["ok"])
        delivered_monitor, delivered_event = deliver.await_args.args
        self.assertEqual(delivered_monitor["id"], monitor["id"])
        self.assertEqual(delivered_event["message"], "Back Door opened.")
        stored = json.loads(self.redis.lists["tater:automations:events:back_door"][0])
        self.assertEqual(stored["id"], delivered_event["id"])

    def test_new_event_returns_history_to_the_latest_page(self):
        self.core._runtime_set(self.redis, events_page=4)
        self.core._append_event(
            self.redis,
            source="Back Yard",
            payload={
                "id": "new-event",
                "ha_time": "2026-08-13T11:30:00",
                "type": "camera_event",
                "message": "A person is in the back yard.",
            },
        )
        self.assertEqual(self.core._runtime_get(self.redis)["events_page"], 1.0)
        stored = json.loads(self.redis.lists["tater:automations:events:back_yard"][0])
        self.assertEqual(stored["id"], "new-event")

    def test_ui_is_a_card_based_monitor_picker(self):
        with patch.object(self.core, "_monitor_registry", return_value=sample_registry()):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)
        ui = payload["ui"]
        self.assertEqual([tab["key"] for tab in ui["manager_tabs"]], ["events", "faces", "monitors", "add"])
        self.assertEqual(ui["default_tab"], "events")
        self.assertEqual(ui["appearance"], "awareness")
        fields = {field.get("key"): field for field in ui["add_form"]["fields"] if field.get("key")}
        self.assertEqual(fields["kind"]["presentation"], "cards")
        self.assertEqual(fields["integration"]["presentation"], "cards")
        self.assertEqual(fields["integration"]["dependent_options"]["source_key"], "kind")
        self.assertEqual(fields["device"]["presentation"], "cards")
        self.assertEqual(fields["device"]["dependent_options"]["source_key"], "integration")
        self.assertEqual(fields["trigger_events"]["type"], "multiselect")
        self.assertEqual(fields["trigger_events"]["presentation"], "cards")
        self.assertEqual(fields["trigger_events"]["dependent_options"]["source_key"], "device")
        self.assertEqual(fields["description_mode"]["presentation"], "cards")
        self.assertEqual(fields["description_mode"]["dependent_options"]["source_key"], "device")
        camera_media = fields["description_mode"]["dependent_options"]["options_by_source"][
            "unifi_protect|cam-front"
        ]
        self.assertEqual([row["value"] for row in camera_media], ["image", "video"])
        self.assertEqual(fields["linked_camera_integration"]["options"][0]["value"], "")
        self.assertEqual(fields["linked_camera_integration"]["options"][0]["label"], "No camera")
        self.assertEqual(
            fields["linked_camera"]["dependent_options"]["source_key"],
            "linked_camera_integration",
        )
        linked_unifi_cameras = fields["linked_camera"]["dependent_options"]["options_by_source"][
            "camera::unifi_protect"
        ]
        self.assertIn("unifi_protect|cam-front", [row["value"] for row in linked_unifi_cameras])
        self.assertEqual(
            fields["linked_camera_description_mode"]["dependent_options"]["source_key"],
            "linked_camera",
        )
        linked_camera_modes = fields["linked_camera_description_mode"]["dependent_options"][
            "options_by_source"
        ]["unifi_protect|cam-front"]
        self.assertEqual([row["value"] for row in linked_camera_modes], ["image", "video"])
        doorbell_options = fields["trigger_events"]["dependent_options"]["options_by_source"][
            "unifi_protect|doorbell-front"
        ]
        self.assertEqual([row["value"] for row in doorbell_options], ["motion", "doorbell"])
        camera_values = {
            row["value"]
            for row in fields["device"]["dependent_options"]["options_by_source"][
                "camera::unifi_protect"
            ]
        }
        self.assertIn("unifi_protect|doorbell-front", camera_values)
        sensor_integrations = fields["integration"]["dependent_options"]["options_by_source"]["sensor"]
        self.assertEqual(
            [row["label"] for row in sensor_integrations],
            ["Home Assistant", "Philips Hue", "UniFi Protect"],
        )
        unifi_sensors = fields["device"]["dependent_options"]["options_by_source"][
            "sensor::unifi_protect"
        ]
        self.assertEqual([row["value"] for row in unifi_sensors], ["unifi_protect|sensor-back-door"])
        self.assertEqual(unifi_sensors[0]["description"], "Door sensor • Back Yard • UniFi Protect")
        self.assertEqual(unifi_sensors[0]["meta"], "closed")
        unifi_sensor_events = fields["trigger_events"]["dependent_options"]["options_by_source"][
            "unifi_protect|sensor-back-door"
        ]
        self.assertEqual(
            [row["value"] for row in unifi_sensor_events],
            ["opens", "closes", "motion"],
        )
        hue_sensors = fields["device"]["dependent_options"]["options_by_source"]["sensor::hue"]
        self.assertEqual([row["value"] for row in hue_sensors], ["hue|hue-motion-hall"])
        self.assertEqual(hue_sensors[0]["description"], "Motion sensor • Hall • Philips Hue")
        self.assertNotIn("trigger_entities", fields)
        self.assertNotIn("notification_targets", fields)
        self.assertNotIn("presentation", fields["enabled"])
        self.assertNotIn("presentation", fields["face_id_enabled"])
        self.assertEqual(fields["face_id_enabled"]["show_when"], {"source_key": "kind", "equals": "camera"})
        self.assertEqual(ui["add_form"]["action"], "awareness_add_monitor")

    def test_event_list_view_uses_compact_rows_instead_of_event_cards(self):
        snapshot_id = "snapshot-list-view"
        self.redis.values[self.core._event_snapshot_key(snapshot_id)] = json.dumps(
            {
                "content_type": "image/jpeg",
                "bytes": 4,
                "data_b64": "dGVzdA==",
            }
        )
        event = {
            "id": "event-list-view",
            "source": "back_yard",
            "type": "camera_person",
            "entity_id": "camera.back_yard",
            "ha_time": "2026-08-12T12:00:00",
            "message": "A person is walking across the back yard.",
            "snapshot_id": snapshot_id,
            "data": {"area": "Back Yard"},
        }

        card_item = self.core._event_forms_from_events(self.redis, [event], list_view=False)[0]
        list_item = self.core._event_forms_from_events(self.redis, [event], list_view=True)[0]

        self.assertEqual(card_item["group"], "event")
        self.assertTrue(card_item["fields"])
        self.assertEqual(list_item["group"], "event_list")
        self.assertEqual(list_item["card_variant"], "event_list")
        self.assertEqual(list_item["detail"], event["message"])
        self.assertTrue(list_item["hero_image_src"].startswith("data:image/jpeg;base64,"))
        self.assertEqual(list_item["fields"], [])

        self.core._runtime_set(self.redis, events_list_view=True)
        with patch.object(self.core, "_monitor_registry", return_value=sample_registry()):
            ui = self.core.get_htmlui_tab_data(redis_client=self.redis)["ui"]
        events_tab = next(tab for tab in ui["manager_tabs"] if tab["key"] == "events")
        self.assertEqual(events_tab["item_group"], "event_list")

    def test_face_id_tab_explains_that_the_model_must_be_enabled(self):
        with patch.object(self.core, "_monitor_registry", return_value=sample_registry()):
            ui = self.core.get_htmlui_tab_data(redis_client=self.redis)["ui"]
        face_tab = next(tab for tab in ui["manager_tabs"] if tab["key"] == "faces")
        self.assertEqual(face_tab["label"], "Face ID")
        face_card = next(item for item in ui["item_forms"] if item.get("group") == "face_person")
        self.assertEqual(face_card["title"], "Face ID needs to be enabled")

    def test_face_identity_name_is_resolved_into_historical_event_context(self):
        detection = {
            "embedding": [1.0, 0.0, 0.0],
            "facial_area": {"w": 120, "h": 120},
            "confidence": 0.98,
            "crop_b64": "ZmFjZQ==",
            "crop_content_type": "image/jpeg",
        }
        identity = self.core._record_face_detection(
            self.redis,
            detection,
            event_id="event-fred",
            seen_at="2026-08-13T09:00:00",
        )
        second = self.core._record_face_detection(
            self.redis,
            {**detection, "embedding": [0.999, 0.001, 0.0]},
            event_id="event-fred-2",
            seen_at="2026-08-13T09:05:00",
        )
        self.assertEqual(second["id"], identity["id"])

        self.core._save_face_session(
            self.redis,
            {"id": "session-fred", "status": "complete", "identity_ids": [identity["id"]]},
        )
        event = {
            "id": "event-fred",
            "source": "back_yard",
            "ha_time": "2026-08-13T09:00:00",
            "data": {"area": "Back Yard", "face_session_id": "session-fred"},
        }
        self.redis.lpush("tater:automations:events:back_yard", json.dumps(event))
        self.core.handle_htmlui_tab_action(
            action="awareness_save_face_identity",
            payload={"id": identity["id"], "values": {"name": "Fred", "merge_into": ""}},
            redis_client=self.redis,
        )
        context = self.core._face_event_context(self.redis, event)
        compact = self.core._events_query_compact_event_for_llm(event, self.redis)
        stored = json.loads(self.redis.lists["tater:automations:events:back_yard"][0])
        self.assertEqual(context["known_people"], ["Fred"])
        self.assertEqual(compact["data"]["known_people"], ["Fred"])
        self.assertEqual(stored["data"]["known_people"], ["Fred"])

    async def test_face_burst_captures_five_frames_before_analyzing(self):
        runtime = types.SimpleNamespace(
            MATCH_THRESHOLD=0.30,
            analyze_image=lambda _image, _client: [],
        )
        session = {
            "id": "burst-session",
            "event_id": "burst-event",
            "status": "pending",
            "identity_ids": [],
        }
        capture = AsyncMock(return_value=(b"next-frame", "image/jpeg"))
        with (
            patch.object(self.core, "_face_id_enabled", return_value=True),
            patch.object(self.core, "_face_id_runtime", runtime),
            patch.object(self.core, "_FACE_BURST_INTERVAL_SECONDS", 0.001),
            patch.object(self.core, "_capture_camera_snapshot", new=capture),
        ):
            await self.core._run_face_burst(
                session=session,
                provider="unifi_protect",
                camera_target="cam-front",
                initial_image=b"first-frame",
                initial_content_type="image/jpeg",
            )
        saved = self.core._load_face_session(self.redis, "burst-session")
        self.assertEqual(capture.await_count, 4)
        self.assertEqual(saved["frames_captured"], 5)
        self.assertEqual(saved["frames_checked"], 5)
        self.assertEqual(saved["status"], "no_faces")

    async def test_face_burst_sends_enabled_notification_after_event_enrichment(self):
        destination = self.core._encode_notification_destination("little_spud", {"device_id": "phone"})
        monitor = self._add_monitor(
            "camera",
            "unifi_protect|cam-front",
            "Front Yard",
            notifications_enabled=True,
            notification_destinations=[destination],
        )
        event = {
            "id": "face-notification-event",
            "source": "front_yard",
            "title": "Front Yard Camera",
            "type": "camera_event",
            "message": "A person is at the front door.",
            "ha_time": "2026-08-21T10:00:00",
            "data": {
                "area": "Front Yard",
                "monitor_id": monitor["id"],
                "face_session_id": "face-notification-session",
                "face_status": "pending",
            },
        }
        self.core._append_event(self.redis, source="Front Yard", payload=event)
        session = {
            "id": "face-notification-session",
            "event_id": "face-notification-event",
            "monitor_id": monitor["id"],
            "area": "Front Yard",
            "status": "pending",
            "identity_ids": [],
        }
        deliver = AsyncMock(return_value={"ok": True, "sent_count": 1})
        runtime = types.SimpleNamespace(analyze_image=lambda _image, _client: [])
        with (
            patch.object(self.core, "_face_id_enabled", return_value=True),
            patch.object(self.core, "_face_id_runtime", runtime),
            patch.object(self.core, "_FACE_BURST_FRAME_COUNT", 1),
            patch.object(self.core, "_dispatch_awareness_event_notification", new=deliver),
        ):
            await self.core._run_face_burst(
                session=session,
                provider="unifi_protect",
                camera_target="cam-front",
                initial_image=b"frame",
                initial_content_type="image/jpeg",
            )

        delivered_event = deliver.await_args.args[1]
        self.assertEqual(delivered_event["data"]["face_status"], "no_faces")
        self.assertEqual(delivered_event["data"]["face_count"], 0)

    async def test_face_id_uses_clip_frames_without_capturing_a_snapshot_burst(self):
        analyzed = []
        runtime = types.SimpleNamespace(
            MATCH_THRESHOLD=0.30,
            analyze_image=lambda image, _client: analyzed.append(image) or [],
        )
        session = {
            "id": "clip-session",
            "event_id": "clip-event",
            "status": "pending",
            "identity_ids": [],
        }
        frames = [f"clip-frame-{index}".encode() for index in range(5)]
        capture = AsyncMock()
        with (
            patch.object(self.core, "_face_id_enabled", return_value=True),
            patch.object(self.core, "_face_id_runtime", runtime),
            patch.object(self.core, "_extract_face_frames_from_clip", return_value=frames) as extract,
            patch.object(self.core, "_capture_camera_snapshot", new=capture),
        ):
            await self.core._run_face_burst(
                session=session,
                provider="unifi_protect",
                camera_target="cam-front",
                initial_image=b"poster-frame",
                initial_content_type="image/jpeg",
                video_bytes=b"video-bytes",
                video_content_type="video/mp4",
                video_duration_seconds=8,
            )

        extract.assert_called_once_with(
            b"video-bytes",
            "video/mp4",
            duration_seconds=8,
            frame_count=5,
        )
        capture.assert_not_awaited()
        self.assertEqual(analyzed, frames)
        saved = self.core._load_face_session(self.redis, "clip-session")
        self.assertEqual(saved["frame_source"], "video_clip")
        self.assertEqual(saved["frames_captured"], 5)
        self.assertEqual(saved["frames_checked"], 5)
        self.assertEqual(saved["status"], "no_faces")

    async def test_face_id_clip_failure_falls_back_to_snapshot_burst(self):
        runtime = types.SimpleNamespace(
            MATCH_THRESHOLD=0.30,
            analyze_image=lambda _image, _client: [],
        )
        session = {
            "id": "clip-fallback-session",
            "event_id": "clip-fallback-event",
            "status": "pending",
            "identity_ids": [],
        }
        capture = AsyncMock(return_value=(b"next-frame", "image/jpeg"))
        with (
            patch.object(self.core, "_face_id_enabled", return_value=True),
            patch.object(self.core, "_face_id_runtime", runtime),
            patch.object(self.core, "_FACE_BURST_FRAME_COUNT", 2),
            patch.object(self.core, "_FACE_BURST_INTERVAL_SECONDS", 0.001),
            patch.object(
                self.core,
                "_extract_face_frames_from_clip",
                side_effect=RuntimeError("clip decode failed"),
            ),
            patch.object(self.core, "_capture_camera_snapshot", new=capture),
        ):
            await self.core._run_face_burst(
                session=session,
                provider="unifi_protect",
                camera_target="cam-front",
                initial_image=b"poster-frame",
                initial_content_type="image/jpeg",
                video_bytes=b"bad-video",
            )

        self.assertEqual(capture.await_count, 1)
        saved = self.core._load_face_session(self.redis, "clip-fallback-session")
        self.assertEqual(saved["frame_source"], "snapshot_burst")
        self.assertEqual(saved["frames_captured"], 2)
        self.assertEqual(saved["frames_checked"], 2)
        self.assertEqual(saved["clip_frame_error"], "clip decode failed")
        self.assertEqual(saved["status"], "no_faces")

    async def test_face_burst_emits_linked_person_event_for_automation(self):
        self.core._save_face_identity(
            self.redis,
            {
                "id": "face_fred",
                "name": "Fred",
                "person_id": "person_fred",
                "person_name": "Fred",
                "centroid": [1.0, 0.0],
                "centroid_count": 1,
                "reference_centroids": [[1.0, 0.0]],
                "observation_count": 1,
                "event_count": 1,
            },
        )
        runtime = types.SimpleNamespace(
            MATCH_THRESHOLD=0.30,
            analyze_image=lambda _image, _client: [
                {
                    "embedding": [1.0, 0.0],
                    "facial_area": {"w": 120, "h": 120},
                    "confidence": 0.99,
                    "crop_b64": "ZmFjZQ==",
                }
            ],
        )
        session = {
            "id": "burst-fred",
            "event_id": "front-event",
            "provider": "unifi_protect",
            "camera_target": "cam-front",
            "area": "Front Door",
            "status": "pending",
            "identity_ids": [],
        }
        with (
            patch.object(self.core, "_face_id_enabled", return_value=True),
            patch.object(self.core, "_face_id_runtime", runtime),
            patch.object(self.core, "_FACE_BURST_FRAME_COUNT", 1),
            patch.object(self.core, "_people_person_name", return_value="Fred"),
        ):
            await self.core._run_face_burst(
                session=session,
                provider="unifi_protect",
                camera_target="cam-front",
                initial_image=b"frame",
                initial_content_type="image/jpeg",
            )

        emitted = json.loads(self.redis.lists[self.core._INTEGRATION_RUNTIME_EVENTS_KEY][0])
        saved = self.core._load_face_session(self.redis, "burst-fred")
        self.assertEqual(emitted["provider"], "awareness")
        self.assertEqual(emitted["kind"], "recognized_person")
        self.assertEqual(emitted["payload"]["person_id"], "person_fred")
        self.assertEqual(emitted["payload"]["camera_target"], "cam-front")
        self.assertEqual(saved["recognized_person_ids"], ["person_fred"])
        self.assertEqual(saved["automation_events_emitted"], 1)

    def test_unknown_face_can_be_manually_sorted_into_a_known_person(self):
        base = {
            "facial_area": {"w": 100, "h": 100},
            "confidence": 0.95,
            "crop_b64": "ZmFjZQ==",
        }
        known = self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [1.0, 0.0]},
            event_id="known-event",
            seen_at="2026-08-13T08:00:00",
        )
        unknown = self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [0.0, 1.0]},
            event_id="unknown-event",
            seen_at="2026-08-13T08:05:00",
        )
        self.core.handle_htmlui_tab_action(
            action="awareness_save_face_identity",
            payload={"id": known["id"], "values": {"name": "Fred", "merge_into": ""}},
            redis_client=self.redis,
        )
        self.core._save_face_session(
            self.redis,
            {"id": "unknown-session", "status": "complete", "identity_ids": [unknown["id"]]},
        )
        self.core.handle_htmlui_tab_action(
            action="awareness_save_face_identity",
            payload={"id": unknown["id"], "values": {"name": "", "merge_into": known["id"]}},
            redis_client=self.redis,
        )
        session = self.core._load_face_session(self.redis, "unknown-session")
        identities = self.core._face_identity_rows(self.redis)
        self.assertEqual(session["identity_ids"], [known["id"]])
        self.assertNotIn(unknown["id"], identities)
        self.assertEqual(identities[known["id"]]["name"], "Fred")

    def test_face_person_ui_is_a_compact_clickable_gallery_with_inline_name(self):
        detection = {
            "embedding": [1.0, 0.0, 0.0],
            "facial_area": {"w": 120, "h": 120},
            "confidence": 0.98,
            "crop_b64": "ZmFjZQ==",
            "crop_content_type": "image/jpeg",
        }
        identity = self.core._record_face_detection(
            self.redis,
            detection,
            event_id="gallery-event-1",
            seen_at="2026-08-13T09:00:00",
        )
        self.core._record_face_detection(
            self.redis,
            {**detection, "embedding": [0.999, 0.001, 0.0]},
            event_id="gallery-event-2",
            seen_at="2026-08-13T09:05:00",
        )
        with (
            patch.object(self.core, "_face_runtime_status", return_value={"enabled": True, "state": "ready"}),
            patch.object(self.core, "_monitor_registry", return_value=sample_registry()),
        ):
            ui = self.core.get_htmlui_tab_data(redis_client=self.redis)["ui"]

        face = next(item for item in ui["item_forms"] if item.get("id") == identity["id"])
        fields = {field["key"]: field for field in face["fields"]}
        face_tab = next(tab for tab in ui["manager_tabs"] if tab["key"] == "faces")
        self.assertEqual(face["group"], "face_person")
        self.assertEqual(face["card_variant"], "face_person")
        self.assertFalse(face["selectable"])
        self.assertTrue(face["click_opens_fields"])
        self.assertFalse(face["fields_popup"])
        self.assertEqual(fields["name"]["type"], "text")
        self.assertEqual(fields["person_id"]["type"], "select")
        self.assertEqual(fields["observation_ids"]["type"], "image_checklist")
        self.assertEqual(len(fields["observation_ids"]["options"]), 2)
        self.assertTrue(all(row["selectable"] for row in fields["observation_ids"]["options"]))
        self.assertEqual(fields["target_identity_id"]["type"], "select")
        self.assertEqual(face["actions"][0]["action"], "awareness_move_face_images")
        self.assertEqual(face["actions"][1]["action"], "awareness_remove_face_images")
        self.assertEqual(face["actions"][1]["label"], "Remove Selected Images")
        self.assertNotIn("remove_action", face)
        self.assertEqual(face["run_action"], "awareness_remove_face_identity")
        self.assertEqual(face["run_label"], "Remove Person")
        self.assertEqual(face_tab["item_group"], "face_person")
        self.assertNotIn("bulk_actions", face_tab)

    def test_face_identity_links_to_people_api_and_enriches_events(self):
        people = [{"id": "person_fred", "display_name": "Fred", "aliases": []}]
        identity = self.core._record_face_detection(
            self.redis,
            {
                "embedding": [1.0, 0.0, 0.0],
                "facial_area": {"w": 120, "h": 120},
                "confidence": 0.98,
                "crop_b64": "ZmFjZQ==",
            },
            event_id="event-fred-link",
            seen_at="2026-08-13T09:00:00",
        )
        self.core._save_face_session(
            self.redis,
            {
                "id": "session-fred-link",
                "event_id": "event-fred-link",
                "status": "complete",
                "identity_ids": [identity["id"]],
            },
        )
        self.redis.lpush(
            "tater:automations:events:front_door",
            json.dumps(
                {
                    "id": "event-fred-link",
                    "source": "front_door",
                    "ha_time": "2026-08-13T09:00:00",
                    "data": {"face_session_id": "session-fred-link"},
                }
            ),
        )

        with (
            patch.object(self.core, "_people_person_rows", return_value=people),
            patch.object(self.core, "_people_attach_face_identity") as attach_face,
            patch.object(self.core, "_face_runtime_status", return_value={"enabled": True, "state": "ready"}),
        ):
            saved = self.core.handle_htmlui_tab_action(
                action="awareness_save_face_identity",
                payload={
                    "id": identity["id"],
                    "values": {"name": "Temporary", "person_id": "person_fred"},
                },
                redis_client=self.redis,
            )
            linked = self.core._face_identity_rows(self.redis)[identity["id"]]
            stored_event = json.loads(self.redis.lists["tater:automations:events:front_door"][0])
            context = self.core._face_event_context(self.redis, stored_event)
            with patch.object(self.core, "_monitor_registry", return_value=sample_registry()):
                ui = self.core.get_htmlui_tab_data(redis_client=self.redis)["ui"]

        self.assertEqual(saved["person_id"], "person_fred")
        self.assertEqual(linked["person_id"], "person_fred")
        self.assertEqual(linked["name"], "Fred")
        attach_face.assert_called_once_with(
            self.redis,
            person_id="person_fred",
            identity_id=identity["id"],
            label="Fred",
        )
        self.assertEqual(context["known_people"], ["Fred"])
        self.assertEqual(context["recognized_people"], ["Fred"])
        self.assertEqual(context["recognized_person_ids"], ["person_fred"])
        self.assertEqual(stored_event["data"]["recognized_person_ids"], ["person_fred"])
        face = next(item for item in ui["item_forms"] if item.get("id") == identity["id"])
        fields = {field["key"]: field for field in face["fields"]}
        self.assertEqual(fields["person_id"]["value"], "person_fred")
        self.assertEqual(fields["person_id"]["options"][1]["label"], "Fred")

    def test_legacy_images_without_face_vectors_are_hidden(self):
        legacy = {
            "id": "face_legacy",
            "name": "Fred",
            "face_b64": "b2xkLWZ1bGwtc25hcHNob3Q=",
            "face_content_type": "image/jpeg",
            "centroid": [1.0, 0.0],
            "observation_count": 5,
        }

        self.assertEqual(self.core._face_identity_gallery(self.redis, legacy), [])

    def test_selected_face_images_can_move_to_an_existing_person(self):
        base = {
            "facial_area": {"w": 100, "h": 100},
            "confidence": 0.95,
            "crop_b64": "ZmFjZQ==",
        }
        known = self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [1.0, 0.0]},
            event_id="known-event",
            seen_at="2026-08-13T08:00:00",
        )
        self.core.handle_htmlui_tab_action(
            action="awareness_save_face_identity",
            payload={"id": known["id"], "values": {"name": "Fred"}},
            redis_client=self.redis,
        )
        unknown = self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [0.0, 1.0]},
            event_id="unknown-event-1",
            seen_at="2026-08-13T08:05:00",
        )
        self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [0.001, 0.999]},
            event_id="unknown-event-2",
            seen_at="2026-08-13T08:06:00",
        )
        for suffix in ("1", "2"):
            session_id = f"unknown-session-{suffix}"
            event_id = f"unknown-event-{suffix}"
            self.core._save_face_session(
                self.redis,
                {"id": session_id, "event_id": event_id, "status": "complete", "identity_ids": [unknown["id"]]},
            )
            self.redis.lpush(
                "tater:automations:events:back_yard",
                json.dumps(
                    {
                        "id": event_id,
                        "source": "back_yard",
                        "ha_time": f"2026-08-13T08:0{4 + int(suffix)}:00",
                        "data": {"face_session_id": session_id},
                    }
                ),
            )

        source = self.core._face_identity_rows(self.redis)[unknown["id"]]
        first = next(row for row in source["observations"] if row["event_id"] == "unknown-event-1")
        moved = self.core.handle_htmlui_tab_action(
            action="awareness_move_face_images",
            payload={
                "id": unknown["id"],
                "values": {"observation_ids": [first["id"]], "target_identity_id": known["id"]},
            },
            redis_client=self.redis,
        )
        identities = self.core._face_identity_rows(self.redis)
        self.assertEqual(moved["moved"], 1)
        self.assertIn(unknown["id"], identities)
        self.assertEqual(len(identities[known["id"]]["observations"]), 2)
        self.assertEqual(len(identities[unknown["id"]]["observations"]), 1)
        self.assertEqual(
            self.core._load_face_session(self.redis, "unknown-session-1")["identity_ids"],
            [known["id"]],
        )

        last = identities[unknown["id"]]["observations"][0]
        moved_last = self.core.handle_htmlui_tab_action(
            action="awareness_move_face_images",
            payload={
                "id": unknown["id"],
                "values": {"observation_ids": [last["id"]], "target_identity_id": known["id"]},
            },
            redis_client=self.redis,
        )
        identities = self.core._face_identity_rows(self.redis)
        self.assertTrue(moved_last["source_removed"])
        self.assertNotIn(unknown["id"], identities)
        self.assertEqual(len(identities[known["id"]]["observations"]), 3)
        self.assertEqual(
            self.core._load_face_session(self.redis, "unknown-session-2")["identity_ids"],
            [known["id"]],
        )

    def test_selected_face_images_can_be_removed_without_removing_the_person(self):
        base = {
            "facial_area": {"w": 100, "h": 100},
            "confidence": 0.95,
            "crop_b64": "ZmFjZQ==",
        }
        identity = self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [1.0, 0.0]},
            event_id="clear-event",
            seen_at="2026-08-13T08:00:00",
        )
        identity = self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [0.9, 0.435]},
            event_id="blurry-event",
            seen_at="2026-08-13T08:05:00",
        )
        self.core.handle_htmlui_tab_action(
            action="awareness_save_face_identity",
            payload={"id": identity["id"], "values": {"name": "Fred"}},
            redis_client=self.redis,
        )
        for event_id in ("clear-event", "blurry-event"):
            session_id = f"session-{event_id}"
            self.core._save_face_session(
                self.redis,
                {
                    "id": session_id,
                    "event_id": event_id,
                    "status": "complete",
                    "identity_ids": [identity["id"]],
                },
            )
            self.redis.lpush(
                "tater:automations:events:front_yard",
                json.dumps(
                    {
                        "id": event_id,
                        "source": "front_yard",
                        "ha_time": "2026-08-13T08:05:00",
                        "data": {"face_session_id": session_id},
                    }
                ),
            )

        blurry = next(
            row
            for row in identity["observations"]
            if row.get("event_id") == "blurry-event"
        )
        result = self.core.handle_htmlui_tab_action(
            action="awareness_remove_face_images",
            payload={
                "id": identity["id"],
                "values": {"observation_ids": [blurry["id"]]},
            },
            redis_client=self.redis,
        )

        saved = self.core._face_identity_rows(self.redis)[identity["id"]]
        self.assertEqual(result["removed"], 1)
        self.assertEqual(saved["name"], "Fred")
        self.assertEqual(len(saved["observations"]), 1)
        self.assertEqual(saved["observations"][0]["event_id"], "clear-event")
        self.assertEqual(saved["observation_count"], 1)
        self.assertEqual(saved["event_count"], 1)
        self.assertEqual(
            self.core._load_face_session(self.redis, "session-blurry-event")["identity_ids"],
            [],
        )
        self.assertEqual(
            self.core._load_face_session(self.redis, "session-clear-event")["identity_ids"],
            [identity["id"]],
        )
        stored_events = [
            json.loads(row)
            for row in self.redis.lists["tater:automations:events:front_yard"]
        ]
        stored_blurry = next(row for row in stored_events if row["id"] == "blurry-event")
        self.assertEqual(stored_blurry["data"]["known_people"], [])

        remaining_id = saved["observations"][0]["id"]
        self.core.handle_htmlui_tab_action(
            action="awareness_remove_face_images",
            payload={
                "id": identity["id"],
                "values": {"observation_ids": [remaining_id]},
            },
            redis_client=self.redis,
        )
        saved_without_images = self.core._face_identity_rows(self.redis)[identity["id"]]
        self.assertEqual(saved_without_images["name"], "Fred")
        self.assertEqual(saved_without_images["observations"], [])
        self.assertEqual(saved_without_images["observation_count"], 0)
        self.assertEqual(saved_without_images["event_count"], 0)

    def test_bulk_merge_and_selected_image_unmerge_preserve_event_links(self):
        base = {
            "facial_area": {"w": 100, "h": 100},
            "confidence": 0.95,
            "crop_b64": "ZmFjZQ==",
        }
        known = self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [1.0, 0.0]},
            event_id="known-event",
            seen_at="2026-08-13T08:00:00",
        )
        self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [0.999, 0.001]},
            event_id="known-event-2",
            seen_at="2026-08-13T08:01:00",
        )
        unknown = self.core._record_face_detection(
            self.redis,
            {**base, "embedding": [0.0, 1.0]},
            event_id="unknown-event",
            seen_at="2026-08-13T08:05:00",
        )
        self.core.handle_htmlui_tab_action(
            action="awareness_save_face_identity",
            payload={"id": known["id"], "values": {"name": "Fred"}},
            redis_client=self.redis,
        )
        self.core._save_face_session(
            self.redis,
            {"id": "unknown-session", "event_id": "unknown-event", "status": "complete", "identity_ids": [unknown["id"]]},
        )
        merged = self.core.handle_htmlui_tab_action(
            action="awareness_merge_face_identities",
            payload={"values": {"identity_ids": [known["id"], unknown["id"]]}},
            redis_client=self.redis,
        )
        self.assertEqual(merged["id"], known["id"])
        combined = self.core._face_identity_rows(self.redis)[known["id"]]
        self.assertEqual(len(combined["observations"]), 3)
        selected = next(
            row["id"] for row in combined["observations"] if row.get("event_id") == "unknown-event"
        )

        split = self.core.handle_htmlui_tab_action(
            action="awareness_unmerge_face_observations",
            payload={"id": known["id"], "values": {"observation_ids": [selected]}},
            redis_client=self.redis,
        )
        identities = self.core._face_identity_rows(self.redis)
        self.assertIn(split["id"], identities)
        self.assertEqual(identities[split["id"]]["name"], "")
        self.assertEqual(len(identities[known["id"]]["observations"]), 2)
        session = self.core._load_face_session(self.redis, "unknown-session")
        self.assertEqual(session["identity_ids"], [split["id"]])

    def test_manual_merge_keeps_distinct_face_profiles_for_future_matching(self):
        target = {
            "id": "face_named",
            "name": "Fred",
            "centroid": [1.0, 0.0, 0.0],
            "centroid_count": 5,
            "observation_count": 5,
            "event_count": 2,
        }
        source = {
            "id": "face_angle",
            "name": "",
            "centroid": [0.70, 0.714, 0.0],
            "centroid_count": 5,
            "observation_count": 5,
            "event_count": 1,
            "observations": [
                {
                    "id": "observation-angle",
                    "event_id": "event-angle",
                    "seen_at": "2026-08-13T12:00:00",
                    "embedding": [0.70, 0.714, 0.0],
                    "face_b64": "ZmFjZQ==",
                }
            ],
        }
        self.core._save_face_identity(self.redis, target)
        self.core._save_face_identity(self.redis, source)
        merged = self.core._merge_face_identities(self.redis, source["id"], target["id"])

        references = self.core._face_reference_embeddings(merged)
        self.assertGreaterEqual(len(references), 2)
        matched_id, distance = self.core._face_match_identity(
            {merged["id"]: merged},
            [0.69, 0.724, 0.0],
            threshold=0.30,
        )
        self.assertEqual(matched_id, target["id"])
        self.assertLess(distance, 0.01)

    def test_face_reference_set_rotates_to_clearer_new_profiles(self):
        dimensions = 27

        def profile(index, strength=0.20):
            embedding = [0.0] * dimensions
            embedding[0] = 1.0
            embedding[index + 1] = strength
            return embedding

        observations = [
            {
                "id": f"observation-{index}",
                "seen_at": f"2026-08-13T12:{index:02d}:00",
                "embedding": profile(index),
                "quality": 0.8,
            }
            for index in range(25)
        ]
        clearer_new_view = profile(0, 0.21)
        observations.insert(
            0,
            {
                "id": "observation-new-clear",
                "seen_at": "2026-08-13T13:00:00",
                "embedding": clearer_new_view,
                "quality": 2.8,
            },
        )
        references = self.core._curate_face_reference_embeddings(
            {
                "centroid": [1.0] + ([0.0] * (dimensions - 1)),
                "observations": observations,
            }
        )

        self.assertEqual(len(references), self.core._FACE_REFERENCE_LIMIT)
        self.assertIn(clearer_new_view, references)
        self.assertNotIn(profile(0), references)

    def test_face_reference_set_balances_quality_with_different_angles(self):
        clear_front = [1.0, 0.0, 0.0]
        similar_front = [0.98, 0.20, 0.0]
        different_angle = [0.70, 0.714, 0.0]
        references = self.core._curate_face_reference_embeddings(
            {
                "observations": [
                    {"id": "front", "embedding": clear_front, "quality": 3.0},
                    {"id": "similar", "embedding": similar_front, "quality": 2.9},
                    {"id": "angle", "embedding": different_angle, "quality": 1.0},
                ]
            },
            limit=2,
        )

        self.assertEqual(references, [clear_front, different_angle])

    def test_legacy_rule_actions_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "moved to Automation Core"):
            self.core.handle_htmlui_tab_action(
                action="awareness_add_rule",
                payload={"values": {}},
                redis_client=self.redis,
            )

    def test_legacy_rules_are_not_migrated_or_shown(self):
        self.redis.hset(
            "awareness:rules",
            "old-rule",
            json.dumps({"id": "old-rule", "kind": "camera", "name": "Old Camera Rule"}),
        )
        with patch.object(self.core, "_monitor_registry", return_value=sample_registry()):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)
        self.assertEqual(payload["stats"][0]["value"], 0)
        self.assertFalse(any(item.get("title") == "Old Camera Rule" for item in payload["ui"]["item_forms"]))


if __name__ == "__main__":
    unittest.main()
