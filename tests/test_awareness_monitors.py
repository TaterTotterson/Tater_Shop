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
        "actions": ["camera_snapshot"],
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
    return {
        "devices": [camera, doorbell, sensor],
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

    def _add_monitor(self, kind, device, area, trigger_events=None):
        values = {"kind": kind, "device": device, "area": area, "enabled": True}
        if trigger_events is not None:
            values["trigger_events"] = trigger_events
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
        self.assertEqual(self.redis.hgetall("awareness:rules"), {})

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
        ):
            result = await self.core._execute_camera_monitor(monitor, event)
        self.assertEqual(result["event_type"], "person")
        stored = json.loads(self.redis.lists["tater:automations:events:front_yard"][0])
        self.assertEqual(stored["data"]["event_type"], "person")
        self.assertTrue(stored["snapshot_id"])

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

    def test_ui_is_a_card_based_monitor_picker(self):
        with patch.object(self.core, "_monitor_registry", return_value=sample_registry()):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)
        ui = payload["ui"]
        self.assertEqual([tab["key"] for tab in ui["manager_tabs"]], ["events", "monitors", "add"])
        self.assertEqual(ui["default_tab"], "events")
        self.assertEqual(ui["appearance"], "awareness")
        fields = {field.get("key"): field for field in ui["add_form"]["fields"] if field.get("key")}
        self.assertEqual(fields["kind"]["presentation"], "cards")
        self.assertEqual(fields["device"]["presentation"], "cards")
        self.assertEqual(fields["trigger_events"]["type"], "multiselect")
        self.assertEqual(fields["trigger_events"]["presentation"], "cards")
        self.assertEqual(fields["trigger_events"]["dependent_options"]["source_key"], "device")
        doorbell_options = fields["trigger_events"]["dependent_options"]["options_by_source"][
            "unifi_protect|doorbell-front"
        ]
        self.assertEqual([row["value"] for row in doorbell_options], ["motion", "doorbell"])
        device_values = {
            row["value"]
            for row in fields["device"]["dependent_options"]["options_by_source"]["camera"]
        }
        self.assertIn("unifi_protect|doorbell-front", device_values)
        self.assertNotIn("trigger_entities", fields)
        self.assertNotIn("notification_targets", fields)
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
