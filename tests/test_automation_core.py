import base64
import importlib.util
import io
import json
import sys
import tempfile
import types
import unittest
import wave
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

    def hlen(self, key):
        return len(self.hashes.get(key, {}))

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None, nx=False):
        del ex
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    def incr(self, key):
        value = int(self.values.get(key, 0)) + 1
        self.values[key] = value
        return value

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)
        return len(self.lists[key])

    def rpop(self, key):
        rows = self.lists.get(key, [])
        return rows.pop() if rows else None

    def lrange(self, key, start, end):
        rows = self.lists.get(key, [])
        return rows[start:] if end < 0 else rows[start : end + 1]

    def ltrim(self, key, start, end):
        rows = self.lists.get(key, [])
        self.lists[key] = rows[start:] if end < 0 else rows[start : end + 1]

    def llen(self, key):
        return len(self.lists.get(key, []))


def load_automation_core():
    fake_redis = FakeRedis()
    injected_modules = (
        "announcement_targets",
        "helpers",
        "integration_registry",
        "notify",
        "speech_settings",
        "speech_tts",
        "vision_settings",
    )
    previous_modules = {name: sys.modules.get(name) for name in injected_modules}

    announcement_targets = types.ModuleType("announcement_targets")
    announcement_targets.build_announcement_target_options = lambda **_kwargs: [
        {"value": "voice_core:sat-kitchen", "label": "Kitchen satellite"}
    ]
    sys.modules["announcement_targets"] = announcement_targets

    helpers = types.ModuleType("helpers")
    helpers.redis_client = fake_redis
    helpers.describe_image_with_local_llm = lambda **_kwargs: {
        "ok": True,
        "description": "A person is standing by the front door.",
    }
    helpers.resolve_hydra_base_servers = lambda **_kwargs: []
    sys.modules["helpers"] = helpers

    integration_registry = types.ModuleType("integration_registry")
    integration_registry.get_integration_device_registry = lambda *_args, **_kwargs: {
        "devices": [],
        "categories": [],
        "rooms": [],
    }
    integration_registry.run_integration_device_action = lambda *_args, **_kwargs: {"ok": True}
    sys.modules["integration_registry"] = integration_registry

    notify = types.ModuleType("notify")

    async def dispatch_notification(**_kwargs):
        return "Queued notification"

    notify.dispatch_notification = dispatch_notification
    notify.notifier_destination_catalog = lambda **_kwargs: {"platforms": []}
    sys.modules["notify"] = notify

    speech_settings = types.ModuleType("speech_settings")
    speech_settings.get_speech_settings = lambda: {"tts_backend": "wyoming"}
    sys.modules["speech_settings"] = speech_settings

    speech_tts = types.ModuleType("speech_tts")

    async def speak_announcement_targets(**_kwargs):
        return {"ok": True, "sent_count": 1}

    speech_tts.speak_announcement_targets = speak_announcement_targets
    sys.modules["speech_tts"] = speech_tts

    vision_settings = types.ModuleType("vision_settings")
    vision_settings.get_vision_settings = lambda **_kwargs: {
        "mode": "dedicated",
        "provider": "llama_cpp",
        "model": "vision-model",
        "api_base": "",
        "api_key": "",
    }
    sys.modules["vision_settings"] = vision_settings

    path = Path(__file__).resolve().parents[1] / "cores" / "automation_core.py"
    try:
        spec = importlib.util.spec_from_file_location("automation_core_test_module", path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
    finally:
        for name, previous in previous_modules.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
    return module, fake_redis


def sample_registry():
    camera = {
        "integration_id": "unifi_protect",
        "integration_name": "UniFi Protect",
        "id": "cam-front",
        "ref": "camera:cam-front",
        "name": "Front Yard",
        "room": "Front Yard",
        "category_ids": ["camera"],
        "actions": ["camera_snapshot"],
        "event_sources": [
            {"type": "motion", "ref": "binary_sensor.unifi_cam-front_motion"},
            {"type": "smart_person", "ref": "binary_sensor.unifi_cam-front_smart_person"},
            {"type": "smart_animal", "ref": "binary_sensor.unifi_cam-front_smart_animal"},
        ],
    }
    doorbell = {
        "integration_id": "unifi_protect",
        "integration_name": "UniFi Protect",
        "id": "doorbell-front",
        "ref": "camera:doorbell-front",
        "name": "Front Doorbell",
        "room": "Front Door",
        "type": "camera",
        "category_ids": ["camera"],
        "capabilities": ["camera", "motion", "doorbell"],
        "actions": ["camera_snapshot"],
        "event_sources": [
            {
                "type": "motion",
                "ref": "binary_sensor.unifi_doorbell-front_motion",
                "state_on": "on",
                "state_off": "off",
            },
            {
                "type": "doorbell",
                "ref": "event.unifi_doorbell-front_doorbell",
                "state_on": "on",
                "state_off": "off",
            },
        ],
    }
    light = {
        "integration_id": "homeassistant",
        "integration_name": "Home Assistant",
        "id": "light.kitchen",
        "ref": "light.kitchen",
        "name": "Kitchen Lights",
        "room": "Kitchen",
        "category_ids": ["light"],
        "actions": ["turn_on", "turn_off", "set_brightness"],
    }
    entry = {
        "integration_id": "homeassistant",
        "integration_name": "Home Assistant",
        "id": "binary_sensor.front_door",
        "ref": "binary_sensor.front_door",
        "name": "Front Door",
        "room": "Entry",
        "category_ids": ["entry_sensor"],
        "actions": [],
        "status": "online",
        "state": "closed",
        "event_sources": [
            {
                "type": "contact",
                "ref": "binary_sensor.front_door",
                "state_on": "open",
                "state_off": "closed",
            }
        ],
    }
    return {
        "devices": [camera, doorbell, light, entry],
        "categories": [
            {"id": "light", "name": "Lights", "order": 10, "devices": [light]},
            {"id": "entry_sensor", "name": "Door & Window Sensors", "order": 50, "devices": [entry]},
            {"id": "camera", "name": "Cameras", "order": 70, "devices": [camera, doorbell]},
        ],
        "rooms": [
            {"id": "kitchen", "name": "Kitchen"},
            {"id": "front_yard", "name": "Front Yard"},
            {"id": "entry", "name": "Entry"},
        ],
    }


def multi_integration_registry():
    registry = sample_registry()
    garage_camera = {
        "integration_id": "homeassistant",
        "integration_name": "Home Assistant",
        "id": "camera.garage",
        "ref": "camera.garage",
        "name": "Garage Camera",
        "room": "Garage",
        "category_ids": ["camera"],
        "actions": ["camera_snapshot"],
        "event_sources": [{"type": "motion", "ref": "binary_sensor.garage_motion"}],
    }
    hue_light = {
        "integration_id": "hue",
        "integration_name": "Philips Hue",
        "id": "light.porch",
        "ref": "light.porch",
        "name": "Porch Light",
        "room": "Porch",
        "category_ids": ["light"],
        "actions": ["turn_on", "turn_off"],
    }
    registry["devices"].extend([garage_camera, hue_light])
    next(row for row in registry["categories"] if row["id"] == "camera")["devices"].append(garage_camera)
    next(row for row in registry["categories"] if row["id"] == "light")["devices"].append(hue_light)
    return registry


class AutomationCoreTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.core, cls.redis = load_automation_core()

    def setUp(self):
        self.redis.hashes.clear()
        self.redis.values.clear()
        self.redis.lists.clear()

    def _tts_rule(self, **overrides):
        payload = {
            "id": "rule-1",
            "name": "Test",
            "enabled": True,
            "trigger_category": "camera",
            "trigger_event": "person",
            "cooldown_seconds": 0,
            "action_type": "tts",
            "tts_text": "{device} detected a person.",
            "tts_targets": ["voice_core:sat-kitchen"],
        }
        payload.update(overrides)
        rule = self.core._normalize_rule(payload)
        self.assertIsNotNone(rule)
        return rule

    def test_matches_unifi_person_event_to_selected_camera(self):
        rule = self._tts_rule(trigger_device="unifi_protect|cam-front")
        event = {
            "seq": 11,
            "provider": "unifi_protect",
            "kind": "protect_event",
            "payload": {
                "type": "cameraSmartDetectZone",
                "camera": "cam-front",
                "smartDetectTypes": ["person"],
            },
        }
        matched, context = self.core._event_match(rule, event, sample_registry())
        self.assertTrue(matched)
        self.assertEqual(context["device"], "Front Yard")
        self.assertEqual(context["device_target"], "unifi_protect|cam-front")

    def test_matches_linked_person_recognition_to_selected_camera_and_person(self):
        rule = self._tts_rule(
            trigger_device="unifi_protect|cam-front",
            trigger_event="recognized_person",
            trigger_person_id="person_fred",
            tts_text="{person} was recognized at {device}.",
        )
        event = {
            "seq": 12,
            "provider": "awareness",
            "kind": "recognized_person",
            "payload": {
                "state": "recognized",
                "person_id": "person_fred",
                "person_name": "Fred",
                "face_identity_ids": ["face_fred"],
                "camera_provider": "unifi_protect",
                "camera_target": "cam-front",
                "camera_id": "cam-front",
            },
        }

        matched, context = self.core._event_match(rule, event, sample_registry())
        self.assertTrue(matched)
        self.assertEqual(context["device"], "Front Yard")
        self.assertEqual(context["person"], "Fred")
        self.assertEqual(context["person_id"], "person_fred")
        self.assertEqual(
            self.core._render_template(rule["tts_text"], context),
            "Fred was recognized at Front Yard.",
        )

        wrong_person = {**event, "payload": {**event["payload"], "person_id": "person_alex"}}
        self.assertFalse(self.core._event_match(rule, wrong_person, sample_registry())[0])

    def test_integration_events_keep_sequences_above_one_million(self):
        self.redis.lists[self.core._INTEGRATION_EVENTS_KEY] = [
            json.dumps({"seq": 1_358_240, "provider": "unifi_protect", "kind": "newer"}),
            json.dumps({"seq": 1_358_239, "provider": "unifi_protect", "kind": "older"}),
        ]

        events = self.core._integration_events(self.redis, 1_358_238)

        self.assertEqual([event["seq"] for event in events], [1_358_239, 1_358_240])

    def test_legacy_capped_cursor_recovers_at_live_sequence(self):
        self.assertEqual(
            self.core._resolve_event_cursor("1000000", "1358240"),
            (1_358_240, True),
        )
        self.assertEqual(
            self.core._resolve_event_cursor("1358239", "1358240"),
            (1_358_239, False),
        )

    def test_sequence_parser_preserves_64_bit_integer_precision(self):
        self.assertEqual(
            self.core._sequence("9007199254740993"),
            9_007_199_254_740_993,
        )

    def test_ignores_terminal_unifi_detection_update(self):
        rule = self._tts_rule(trigger_device="unifi_protect|cam-front")
        event = {
            "seq": 12,
            "provider": "unifi_protect",
            "kind": "protect_event",
            "payload": {
                "type": "cameraSmartDetectZone",
                "camera": "cam-front",
                "smartDetectTypes": ["person"],
                "__ws_action": "update",
                "end": 123456,
            },
        }
        matched, _context = self.core._event_match(rule, event, sample_registry())
        self.assertFalse(matched)

    def test_ignores_motion_sensor_clear_event(self):
        rule = self._tts_rule(
            trigger_category="motion",
            trigger_device="homeassistant|binary_sensor.front_door",
            trigger_event="motion",
        )
        event = {
            "seq": 13,
            "provider": "homeassistant",
            "kind": "state_changed",
            "payload": {
                "entity_id": "binary_sensor.front_door",
                "old_state": {"state": "on"},
                "new_state": {
                    "state": "off",
                    "attributes": {"device_class": "motion"},
                },
            },
        }
        matched, _context = self.core._event_match(rule, event, sample_registry())
        self.assertFalse(matched)

    def test_matches_homeassistant_light_turning_on(self):
        rule = self._tts_rule(
            trigger_category="light",
            trigger_device="homeassistant|light.kitchen",
            trigger_event="turns_on",
        )
        event = {
            "seq": 12,
            "provider": "homeassistant",
            "kind": "state_changed",
            "payload": {
                "entity_id": "light.kitchen",
                "old_state": {"state": "off"},
                "new_state": {"state": "on"},
            },
        }
        matched, context = self.core._event_match(rule, event, sample_registry())
        self.assertTrue(matched)
        self.assertEqual(context["state"], "on")
        self.assertEqual(context["old_state"], "off")

    def test_category_action_targets_respect_room_and_capability(self):
        rule = self.core._normalize_rule(
            {
                "trigger_category": "camera",
                "trigger_event": "person",
                "action_type": "device",
                "action_category": "light",
                "action_scope": "category",
                "action_room": "kitchen",
                "action_operation": "turn_on",
            }
        )
        self.assertIsNotNone(rule)
        targets = self.core._action_targets(rule, sample_registry())
        self.assertEqual([item["id"] for item in targets], ["light.kitchen"])

    def test_category_action_targets_respect_selected_integration(self):
        rule = self.core._normalize_rule(
            {
                "trigger_category": "camera",
                "trigger_event": "person",
                "action_type": "device",
                "action_category": "light",
                "action_integration": "light::hue",
                "action_scope": "category",
                "action_operation": "turn_on",
            }
        )
        self.assertIsNotNone(rule)

        targets = self.core._action_targets(rule, multi_integration_registry())

        self.assertEqual([item["id"] for item in targets], ["light.porch"])

    def test_guided_form_builds_default_announcement_rule(self):
        values = {
            "name": "Front yard person",
            "trigger_category": "camera",
            "trigger_device": "unifi_protect|cam-front",
            "trigger_event": "person",
            "action_type": "tts",
            "tts_mode": "default",
            "tts_targets": ["voice_core:sat-kitchen"],
            "cooldown_seconds": 45,
        }
        rule = self.core._rule_from_form(values, {"values": values})
        self.assertEqual(rule["trigger_category"], "camera")
        self.assertEqual(rule["trigger_event"], "person")
        self.assertEqual(rule["action_type"], "tts")
        self.assertEqual(rule["tts_mode"], "default")
        self.assertEqual(rule["tts_text"], "")

    async def test_default_announcement_uses_trigger_specific_words(self):
        rule = self._tts_rule(tts_mode="default", tts_text="")
        with patch.object(
            self.core,
            "speak_announcement_targets",
            new=AsyncMock(return_value={"ok": True, "sent_count": 1}),
        ) as speak:
            await self.core._execute_tts(rule, {"device": "Front Yard"})
        self.assertEqual(speak.await_args.kwargs["text"], "A person was detected at Front Yard.")

    async def test_announcement_passes_background_audio_scene_to_tater_satellite(self):
        rule = self._tts_rule(
            tts_audio_scene={
                "background": {
                    "url": "https://example.test/morning.wav",
                    "loop": True,
                    "volume_percent": 60,
                },
                "ducking": {"target_percent": 35, "attack_ms": 150, "release_ms": 350},
                "finish": {"fade_ms": 500},
            }
        )
        with patch.object(
            self.core,
            "speak_announcement_targets",
            new=AsyncMock(
                return_value={
                    "ok": True,
                    "sent_count": 1,
                    "audio_scene_sent_count": 1,
                    "audio_scene_fallback_count": 0,
                }
            ),
        ) as speak:
            result = await self.core._execute_tts(rule, {"device": "Front Yard"})

        scene = speak.await_args.kwargs["audio_scene"]
        self.assertEqual(scene["background"]["url"], "https://example.test/morning.wav")
        self.assertEqual(scene["ducking"]["target_percent"], 35)
        self.assertEqual(result["audio_scene_sent_count"], 1)

    async def test_announcement_reports_audio_scene_fallback(self):
        rule = self._tts_rule(
            tts_audio_scene={"background": {"url": "https://example.test/morning.wav"}}
        )
        with patch.object(
            self.core,
            "speak_announcement_targets",
            new=AsyncMock(
                return_value={
                    "ok": True,
                    "sent_count": 1,
                    "audio_scene_sent_count": 0,
                    "audio_scene_fallback_count": 1,
                }
            ),
        ):
            result = await self.core._execute_tts(rule, {"device": "Front Yard"})

        self.assertIn("played TTS without background audio", result["summary"])

    def test_announcement_form_builds_and_persists_background_audio_preset(self):
        values = {
            "name": "Morning announcement",
            "trigger_category": "camera",
            "trigger_device": "unifi_protect|cam-front",
            "trigger_event": "person",
            "action_type": "tts",
            "tts_mode": "default",
            "tts_targets": ["voice_core:sat-kitchen"],
            "tts_audio_enabled": "enabled",
            "tts_background_audio_source": "preset:morning_glow",
            "tts_background_volume_percent": 55,
            "tts_ducking_target_percent": 30,
            "tts_ducking_attack_ms": 125,
            "tts_ducking_release_ms": 325,
            "tts_background_fade_ms": 450,
            "tts_background_loop": "enabled",
        }
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            self.core,
            "_background_audio_root",
            return_value=Path(temp_dir) / "background_audio",
        ):
            rule = self.core._rule_from_form(values, {"values": values})
            generated = sorted((Path(temp_dir) / "background_audio" / "presets").glob("*.wav"))

        scene = rule["tts_audio_scene"]
        self.assertTrue(scene["background"]["url"].endswith("/presets/morning_glow.wav"))
        self.assertEqual(scene["background"]["volume_percent"], 55)
        self.assertEqual(scene["ducking"]["target_percent"], 30)
        self.assertEqual(len(generated), 4)

    def test_announcement_form_stores_uploaded_background_audio(self):
        output = io.BytesIO()
        with wave.open(output, "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(8000)
            wav_file.writeframes(b"\x00\x00" * 800)
        audio = output.getvalue()
        values = {
            "name": "Uploaded bed",
            "trigger_category": "camera",
            "trigger_event": "person",
            "action_type": "tts",
            "tts_targets": ["voice_core:sat-kitchen"],
            "tts_audio_enabled": "enabled",
            "tts_background_audio_source": "upload",
            "tts_background_audio_upload": {
                "filename": "custom-bed.wav",
                "content_type": "audio/wav",
                "data_b64": base64.b64encode(audio).decode("ascii"),
            },
            "tts_background_loop": "enabled",
        }
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            self.core,
            "_background_audio_root",
            return_value=Path(temp_dir) / "background_audio",
        ):
            rule = self.core._rule_from_form(values, {"values": values})
            uploads = list((Path(temp_dir) / "background_audio" / "uploads").glob("*.wav"))

        self.assertEqual(len(uploads), 1)
        self.assertIn("/api/ai-tasks/background-audio/uploads/", rule["tts_audio_scene"]["background"]["url"])

    def test_selected_camera_exposes_only_reported_trigger_events(self):
        options, dependency = self.core._trigger_event_dependency(
            sample_registry(),
            current_device="unifi_protect|cam-front",
        )
        values = [row["value"] for row in options]
        self.assertEqual(values, ["motion", "person", "recognized_person", "animal"])
        self.assertEqual(dependency["source_key"], "trigger_device")

    def test_integration_step_groups_trigger_and_action_devices(self):
        registry = multi_integration_registry()

        trigger_integrations, trigger_dependency = self.core._integration_dependency(
            registry,
            current_category="camera",
            source_key="trigger_category",
            triggerable_only=True,
        )
        trigger_devices, device_dependency = self.core._device_dependency(
            registry,
            current_integration="camera::homeassistant",
            source_key="trigger_integration",
            triggerable_only=True,
        )
        action_integrations, _action_integration_dependency = self.core._integration_dependency(
            registry,
            current_category="light",
            source_key="action_category",
            actionable_only=True,
        )
        hue_actions, action_dependency = self.core._action_dependency(
            registry,
            current_integration="light::hue",
        )

        self.assertEqual(
            [row["value"] for row in trigger_integrations],
            ["camera::homeassistant", "camera::unifi_protect"],
        )
        self.assertEqual(trigger_dependency["source_key"], "trigger_category")
        self.assertEqual([row["value"] for row in trigger_devices], ["homeassistant|camera.garage"])
        self.assertEqual(device_dependency["source_key"], "trigger_integration")
        self.assertEqual(
            [row["value"] for row in action_integrations],
            ["light::homeassistant", "light::hue"],
        )
        self.assertEqual([row["value"] for row in hue_actions], ["turn_off", "turn_on"])
        self.assertEqual(action_dependency["source_key"], "action_integration")

    def test_normalized_rules_derive_integrations_from_saved_devices(self):
        rule = self.core._normalize_rule(
            {
                "trigger_category": "camera",
                "trigger_device": "unifi_protect|cam-front",
                "trigger_event": "person",
                "action_type": "device",
                "action_category": "light",
                "action_scope": "devices",
                "action_devices": ["homeassistant|light.kitchen"],
                "action_operation": "turn_on",
            }
        )

        self.assertIsNotNone(rule)
        self.assertEqual(rule["trigger_integration"], "camera::unifi_protect")
        self.assertEqual(rule["action_integration"], "light::homeassistant")

    def test_legacy_multi_integration_action_keeps_its_unscoped_targets(self):
        rule = self.core._normalize_rule(
            {
                "trigger_category": "camera",
                "trigger_event": "person",
                "action_type": "device",
                "action_category": "light",
                "action_scope": "devices",
                "action_devices": ["homeassistant|light.kitchen", "hue|light.porch"],
                "action_operation": "turn_on",
            }
        )

        self.assertIsNotNone(rule)
        self.assertEqual(rule["action_integration"], "")
        self.assertEqual(
            [device["id"] for device in self.core._action_targets(rule, multi_integration_registry())],
            ["light.kitchen", "light.porch"],
        )

    def test_motion_only_camera_exposes_only_motion_trigger(self):
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

        self.assertEqual(self.core._trigger_event_values_for_device(device), ["motion", "recognized_person"])

    def test_trigger_events_use_only_integration_declared_event_sources(self):
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
                }
            ],
        }
        capability_only = {**device, "event_sources": []}

        self.assertEqual(
            self.core._trigger_event_values_for_device(device),
            ["opens", "closes"],
        )
        self.assertEqual(self.core._trigger_event_values_for_device(capability_only), [])

    def test_homeassistant_enum_sensor_exposes_reported_state_events(self):
        washer = {
            "integration_id": "homeassistant",
            "integration_name": "Home Assistant",
            "id": "sensor.washer_state",
            "ref": "sensor.washer_state",
            "name": "Washer State",
            "type": "sensor",
            "category_ids": ["sensor"],
            "state": "washing",
            "event_sources": [
                {
                    "type": "enum",
                    "ref": "sensor.washer_state",
                    "trigger_events": ["changed", "equals"],
                    "state_options": ["inactive", "washing", "rinsing", "wash_done"],
                }
            ],
        }
        registry = {
            "devices": [washer],
            "categories": [{"id": "sensor", "name": "Sensors", "devices": [washer]}],
        }

        options, _dependency = self.core._trigger_event_dependency(
            registry,
            current_device="homeassistant|sensor.washer_state",
        )

        self.assertEqual([row["value"] for row in options], ["changed", "equals"])
        equals = next(row for row in options if row["value"] == "equals")
        self.assertIn("wash_done", equals["description"])

    def test_homeassistant_enum_sensor_matches_exact_reported_state(self):
        washer = {
            "integration_id": "homeassistant",
            "id": "sensor.washer_state",
            "ref": "sensor.washer_state",
            "name": "Washer State",
            "type": "sensor",
            "category_ids": ["sensor"],
            "event_sources": [
                {
                    "type": "enum",
                    "ref": "sensor.washer_state",
                    "trigger_events": ["changed", "equals"],
                    "state_options": ["inactive", "washing", "wash_done"],
                }
            ],
        }
        registry = {
            "devices": [washer],
            "categories": [{"id": "sensor", "name": "Sensors", "devices": [washer]}],
        }
        rule = self._tts_rule(
            trigger_category="sensor",
            trigger_device="homeassistant|sensor.washer_state",
            trigger_event="equals",
            trigger_value="wash_done",
        )
        event = {
            "provider": "homeassistant",
            "kind": "state_changed",
            "payload": {
                "entity_id": "sensor.washer_state",
                "old_state": {"state": "washing"},
                "new_state": {"state": "wash_done"},
            },
        }

        self.assertTrue(self.core._event_match(rule, event, registry)[0])

    def test_device_card_prefers_sensor_state_over_connection_status(self):
        entry = next(
            device
            for device in sample_registry()["devices"]
            if device.get("id") == "binary_sensor.front_door"
        )

        self.assertEqual(self.core._device_option(entry)["meta"], "closed")

    def test_doorbell_and_sensor_expose_device_specific_trigger_events(self):
        doorbell_options, _dependency = self.core._trigger_event_dependency(
            sample_registry(),
            current_device="unifi_protect|doorbell-front",
        )
        sensor_options, _dependency = self.core._trigger_event_dependency(
            sample_registry(),
            current_device="homeassistant|binary_sensor.front_door",
        )
        self.assertEqual(
            [row["value"] for row in doorbell_options],
            ["motion", "recognized_person", "doorbell"],
        )
        self.assertEqual([row["value"] for row in sensor_options], ["opens", "closes"])

    def test_doorbell_rule_does_not_treat_doorbell_camera_motion_as_a_press(self):
        rule = self._tts_rule(
            trigger_device="unifi_protect|doorbell-front",
            trigger_event="doorbell",
        )
        motion_event = {
            "provider": "unifi_protect",
            "kind": "protect_event",
            "payload": {"type": "cameraMotion", "device": "doorbell-front"},
        }
        press_event = {
            "provider": "unifi_protect",
            "kind": "protect_event",
            "payload": {"type": "ring", "device": "doorbell-front"},
        }

        self.assertFalse(self.core._event_match(rule, motion_event, sample_registry())[0])
        self.assertTrue(self.core._event_match(rule, press_event, sample_registry())[0])

    def test_unifi_back_door_open_state_change_matches_selected_sensor(self):
        device_id = "66097deb01fbe603e405bbf6"
        back_door = {
            "integration_id": "unifi_protect",
            "integration_name": "UniFi Protect",
            "id": device_id,
            "ref": f"sensor:{device_id}",
            "name": "Back Door",
            "room": "Back Yard",
            "type": "entry_sensor",
            "category_ids": ["entry_sensor"],
            "event_sources": [
                {
                    "type": "contact",
                    "ref": device_id,
                    "state_on": "open",
                    "state_off": "closed",
                }
            ],
        }
        registry = {
            "devices": [back_door],
            "categories": [
                {"id": "entry_sensor", "name": "Door & Window Sensors", "devices": [back_door]}
            ],
        }
        rule = self._tts_rule(
            trigger_category="entry_sensor",
            trigger_integration="entry_sensor::unifi_protect",
            trigger_device=f"unifi_protect|{device_id}",
            trigger_event="opens",
        )
        event = {
            "seq": 1_358_104,
            "provider": "unifi_protect",
            "kind": "state_changed",
            "payload": {
                "device_id": device_id,
                "friendly_name": "Back Door",
                "old_state": {"state": "closed"},
                "new_state": {"state": "open"},
            },
        }

        matched, context = self.core._event_match(rule, event, registry)

        self.assertTrue(matched)
        self.assertEqual(context["device"], "Back Door")
        self.assertEqual(context["state"], "open")
        self.assertEqual(context["old_state"], "closed")
        self.assertEqual(context["event_seq"], 1_358_104)

    def test_awareness_import_action_is_not_supported(self):
        with self.assertRaises(KeyError):
            self.core.handle_htmlui_tab_action(
                action="automation_import_awareness",
                payload={},
                redis_client=self.redis,
            )

    async def test_camera_ai_uses_snapshot_vision_and_tts(self):
        rule = self.core._normalize_rule(
            {
                "id": "camera-ai",
                "name": "Camera AI",
                "trigger_category": "camera",
                "trigger_event": "person",
                "action_type": "camera_ai",
                "camera_source": "selected",
                "camera_device": "unifi_protect|cam-front",
                "camera_tts_text": "{vision}",
                "camera_tts_targets": ["voice_core:sat-kitchen"],
            }
        )
        self.assertIsNotNone(rule)
        with (
            patch.object(self.core, "_registry", return_value=sample_registry()),
            patch.object(
                self.core,
                "run_integration_device_action",
                return_value={"ok": True, "bytes": b"jpeg", "content_type": "image/jpeg"},
            ),
            patch.object(
                self.core,
                "_describe_snapshot_sync",
                return_value="A person is standing by the door.",
            ),
            patch.object(
                self.core,
                "_execute_tts",
                new=AsyncMock(return_value={"ok": True, "summary": "Spoke to kitchen."}),
            ) as speak,
        ):
            result = await self.core._execute_camera_ai(rule, {"device": "Front Yard"})
        self.assertTrue(result["ok"])
        self.assertIn("Spoke to kitchen", result["summary"])
        self.assertEqual(speak.await_args.args[0]["tts_mode"], "custom")
        self.assertEqual(speak.await_args.args[0]["tts_text"], "{vision}")
        self.assertEqual(speak.await_args.args[1]["vision"], "A person is standing by the door.")

    def test_camera_ai_base_vision_uses_active_base_model(self):
        with (
            patch.object(
                self.core,
                "get_vision_settings",
                return_value={
                    "mode": "base",
                    "provider": "hf_transformers",
                    "model": "stale-dedicated-model",
                    "api_base": "",
                    "api_key": "",
                },
            ),
            patch.object(
                self.core,
                "resolve_hydra_base_servers",
                return_value=[{"provider": "llama_cpp", "model": "active-base-model"}],
            ),
            patch.object(
                self.core,
                "describe_image_with_local_llm",
                return_value={"description": "A visitor is at the door."},
            ) as describe,
        ):
            result = self.core._describe_snapshot_sync(b"jpeg", "image/jpeg", "Describe it")

        self.assertEqual(result, "A visitor is at the door.")
        self.assertEqual(describe.call_args.kwargs["provider"], "llama_cpp")
        self.assertEqual(describe.call_args.kwargs["model"], "active-base-model")

    def test_ui_removes_quick_start_and_exposes_guided_builder(self):
        with (
            patch.object(self.core, "_registry", return_value=sample_registry()),
            patch.object(
                self.core,
                "_announcement_options",
                return_value=[{"value": "voice_core:sat-kitchen", "label": "Kitchen"}],
            ),
            patch.object(self.core, "_notification_options", return_value=[]),
        ):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)
        tabs = [item["key"] for item in payload["ui"]["manager_tabs"]]
        self.assertNotIn("starters", tabs)
        self.assertIn("rules", tabs)
        self.assertIn("create", tabs)
        self.assertEqual(payload["ui"]["default_tab"], "create")
        self.assertFalse(
            any(item.get("group") == "starters" for item in payload["ui"]["item_forms"])
        )
        fields = payload["ui"]["add_form"]["fields"]
        by_key = {item.get("key"): item for item in fields if item.get("key")}
        self.assertEqual(by_key["trigger_category"]["type"], "select")
        self.assertEqual(by_key["trigger_integration"]["type"], "select")
        self.assertEqual(by_key["trigger_device"]["type"], "select")
        self.assertEqual(by_key["trigger_event"]["type"], "select")
        self.assertEqual(by_key["trigger_person_id"]["type"], "select")
        self.assertEqual(
            by_key["trigger_person_id"]["show_when"],
            {"source_key": "trigger_event", "equals": "recognized_person"},
        )
        self.assertEqual(by_key["action_type"]["type"], "select")
        self.assertEqual(by_key["tts_targets"]["type"], "multiselect")
        for key in (
            "trigger_category",
            "trigger_integration",
            "trigger_device",
            "trigger_event",
            "action_type",
            "action_integration",
            "tts_targets",
        ):
            self.assertEqual(by_key[key]["presentation"], "cards")
        self.assertEqual(
            by_key["trigger_device"]["dependent_options"]["source_key"],
            "trigger_integration",
        )
        self.assertEqual(
            by_key["action_devices"]["dependent_options"]["source_key"],
            "action_integration",
        )
        self.assertEqual(
            by_key["action_operation"]["dependent_options"]["source_key"],
            "action_integration",
        )
        self.assertEqual(by_key["tts_mode"]["value"], "default")
        self.assertEqual(by_key["tts_audio_enabled"]["type"], "select")
        self.assertEqual(by_key["tts_audio_enabled"]["presentation"], "cards")
        self.assertEqual(
            by_key["tts_background_audio_source"]["show_when_all"],
            [
                {"source_key": "action_type", "equals": "tts"},
                {"source_key": "tts_audio_enabled", "equals": "enabled"},
            ],
        )
        self.assertEqual(by_key["tts_background_audio_upload"]["max_bytes"], 16 * 1024 * 1024)
        trigger_values = [
            row["value"]
            for row in by_key["trigger_event"]["dependent_options"]["options_by_source"][
                "unifi_protect|cam-front"
            ]
        ]
        self.assertEqual(trigger_values, ["motion", "person", "recognized_person", "animal"])


if __name__ == "__main__":
    unittest.main()
