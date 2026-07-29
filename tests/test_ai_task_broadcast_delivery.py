from __future__ import annotations

import ast
import asyncio
import base64
import io
import json
import sys
import tempfile
import types
import unittest
import wave
from pathlib import Path
from typing import Any, Dict, List


def _normalize_targets(value: Any) -> List[str]:
    rows = list(value or []) if isinstance(value, (list, tuple, set)) else [value]
    out: List[str] = []
    for item in rows:
        token = str(item or "").strip()
        if not token:
            continue
        if not token.startswith(("voice_core:", "ha:", "sonos:", "unifi:", "integration:")):
            token = f"voice_core:{token}"
        if token not in out:
            out.append(token)
    return out


def _load_helpers():
    path = Path(__file__).resolve().parents[1] / "cores" / "ai_task_core.py"
    names = {
        "_delivery_clamped_int",
        "_normalize_audio_scene",
        "_normalize_delivery",
        "_is_broadcast_delivery",
        "_deliver_scheduled_broadcast",
        "_ai_tasks_ui_clean_text",
        "_ai_tasks_ui_clean_targets_dict",
        "_ai_tasks_ui_encode_destination_value",
        "_ai_tasks_ui_broadcast_destination_options",
        "_ai_tasks_ui_delivery_destination_value",
        "_ai_tasks_ui_broadcast_audio_fields",
    }
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    selected = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
    ]
    if len(selected) != len(names):
        found = {node.name for node in selected}
        raise RuntimeError(f"Missing AI Task helpers: {sorted(names - found)}")

    module = types.ModuleType("test_ai_task_broadcast_helpers")
    module.__dict__.update(
        {
            "Any": Any,
            "Dict": Dict,
            "List": List,
            "json": json,
            "BROADCAST_DELIVERY_TYPE": "broadcast",
            "normalize_announcement_targets": _normalize_targets,
            "get_voice_core_satellite_target_options": lambda **_kwargs: [
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                }
            ],
            "pr": types.SimpleNamespace(get_verba_registry_snapshot=lambda: {}),
            "get_plugin_enabled": lambda _name: True,
        }
    )
    compiled = ast.Module(body=selected, type_ignores=[])
    ast.fix_missing_locations(compiled)
    exec(compile(compiled, str(path), "exec"), module.__dict__)
    return module


class AiTaskBroadcastDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.core = _load_helpers()

    def test_normalizes_selected_scene_and_clamps_values(self) -> None:
        delivery = self.core._normalize_delivery(
            {
                "type": "broadcast",
                "scope": "selected",
                "targets": ["native:kitchen"],
                "audio_scene": {
                    "background": {
                        "url": "https://example.test/morning.mp3",
                        "loop": True,
                        "volume_percent": 140,
                    },
                    "ducking": {
                        "target_percent": -5,
                        "attack_ms": 20000,
                    },
                },
            }
        )

        self.assertEqual(delivery["scope"], "selected")
        self.assertEqual(delivery["targets"], ["voice_core:native:kitchen"])
        self.assertEqual(delivery["audio_scene"]["background"]["volume_percent"], 100)
        self.assertEqual(delivery["audio_scene"]["ducking"]["target_percent"], 0)
        self.assertEqual(delivery["audio_scene"]["ducking"]["attack_ms"], 10000)
        self.assertEqual(delivery["audio_scene"]["ducking"]["release_ms"], 350)

    def test_selected_without_target_is_rejected(self) -> None:
        delivery = self.core._normalize_delivery(
            {"type": "broadcast", "scope": "selected", "targets": []}
        )
        self.assertEqual(delivery, {})

    def test_destination_options_include_everywhere_and_satellite(self) -> None:
        options = self.core._ai_tasks_ui_broadcast_destination_options()
        labels = [row["label"] for row in options]
        self.assertIn("Broadcast: Everywhere", labels)
        self.assertIn("Broadcast: Tater Satellite: Kitchen", labels)

        selected_platform, selected_targets = self._decode(options[1]["value"])
        self.assertEqual(selected_platform, "broadcast")
        self.assertEqual(selected_targets["target"], "voice_core:native:kitchen")

    @staticmethod
    def _decode(value: str):
        parsed = json.loads(value)
        return parsed["platform"], parsed["targets"]

    def test_delivery_uses_prepared_text_without_a_second_rewrite(self) -> None:
        calls: List[Dict[str, Any]] = []

        class FakeBroadcast:
            async def deliver_prepared_announcement(self, announcement: str, **kwargs):
                calls.append({"announcement": announcement, **kwargs})
                return {"ok": True}

        self.core.pr = types.SimpleNamespace(
            get_verba_registry_snapshot=lambda: {"broadcast": FakeBroadcast()}
        )
        self.core.get_plugin_enabled = lambda _name: True

        result = asyncio.run(
            self.core._deliver_scheduled_broadcast(
                announcement="Good morning. Today will be sunny.",
                delivery={
                    "type": "broadcast",
                    "scope": "selected",
                    "targets": ["voice_core:native:kitchen"],
                },
            )
        )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0]["announcement"], "Good morning. Today will be sunny.")
        self.assertEqual(calls[0]["targets"], ["voice_core:native:kitchen"])


class FakeRedis:
    def __init__(self):
        self.values: Dict[str, Any] = {}
        self.due: Dict[str, float] = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value
        return True

    def zadd(self, _key, values):
        self.due.update({str(key): float(value) for key, value in values.items()})
        return len(values)

    def zrem(self, _key, value):
        self.due.pop(str(value), None)
        return 1

    def hget(self, _key, _field):
        return None

    def hgetall(self, _key):
        return {}


def _load_full_core(fake_redis: FakeRedis):
    injected = {
        "verba_registry",
        "verba_base",
        "verba_kernel",
        "helpers",
        "hydra",
        "notify",
        "notify.queue",
        "dotenv",
        "announcement_targets",
    }
    previous = {name: sys.modules.get(name) for name in injected}

    registry = types.ModuleType("verba_registry")
    registry.get_verba_registry_snapshot = lambda: {}
    sys.modules["verba_registry"] = registry

    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})
    sys.modules["verba_base"] = verba_base

    verba_kernel = types.ModuleType("verba_kernel")
    verba_kernel.verba_supports_platform = lambda *_args, **_kwargs: True
    sys.modules["verba_kernel"] = verba_kernel

    helpers = types.ModuleType("helpers")
    helpers.redis_client = fake_redis
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    sys.modules["helpers"] = helpers

    hydra = types.ModuleType("hydra")

    async def run_hydra_turn(**_kwargs):
        return {"text": ""}

    hydra.run_hydra_turn = run_hydra_turn
    hydra.resolve_agent_limits = lambda _redis: (4, 8)
    sys.modules["hydra"] = hydra

    notify = types.ModuleType("notify")
    notify.__path__ = []

    async def dispatch_notification(**_kwargs):
        return "queued"

    notify.dispatch_notification = dispatch_notification
    notify.notifier_destination_catalog = lambda **_kwargs: {
        "platforms": [
            {
                "platform": "webui",
                "label": "Web UI",
                "requires_target": False,
                "destinations": [],
            }
        ]
    }
    sys.modules["notify"] = notify

    notify_queue = types.ModuleType("notify.queue")
    notify_queue.ALLOWED_PLATFORMS = ("webui", "homeassistant")
    notify_queue.load_default_targets = lambda *_args, **_kwargs: {}
    notify_queue.normalize_origin = lambda value: dict(value or {})
    notify_queue.normalize_platform = lambda value: str(value or "").strip().lower()
    notify_queue.resolve_targets = lambda _platform, targets, _origin, _defaults: (dict(targets or {}), None)
    sys.modules["notify.queue"] = notify_queue

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None
    sys.modules["dotenv"] = dotenv

    announcement_targets = types.ModuleType("announcement_targets")
    announcement_targets.normalize_announcement_targets = _normalize_targets
    announcement_targets.get_voice_core_satellite_target_options = lambda **_kwargs: [
        {
            "value": "voice_core:native:kitchen",
            "label": "Tater Satellite: Kitchen",
        }
    ]
    sys.modules["announcement_targets"] = announcement_targets

    path = Path(__file__).resolve().parents[1] / "cores" / "ai_task_core.py"
    try:
        module = types.ModuleType("ai_task_core_broadcast_test_module")
        module.__file__ = str(path)
        source = "from __future__ import annotations\n" + path.read_text(encoding="utf-8")
        exec(compile(source, str(path), "exec"), module.__dict__)
    finally:
        for name, old_module in previous.items():
            if old_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_module
    return module


class AiTaskBroadcastFormTests(unittest.TestCase):
    @staticmethod
    def _wav_upload_payload(filename: str = "custom-bed.wav") -> Dict[str, Any]:
        output = io.BytesIO()
        with wave.open(output, "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(8000)
            wav_file.writeframes(b"\x00\x00" * 800)
        data = output.getvalue()
        return {
            "filename": filename,
            "content_type": "audio/wav",
            "size": len(data),
            "data_b64": base64.b64encode(data).decode("ascii"),
        }

    def test_audio_controls_only_show_for_broadcast_destinations(self) -> None:
        redis = FakeRedis()
        core = _load_full_core(redis)
        broadcast_values = [
            str(row.get("value") or "")
            for row in core._ai_tasks_ui_broadcast_destination_options()
        ]
        fields = core._ai_tasks_ui_broadcast_audio_fields(
            destination_values=broadcast_values
        )
        by_key = {
            str(field.get("key") or ""): field
            for field in fields
            if isinstance(field, dict) and field.get("key")
        }

        audio_toggle = by_key["broadcast_audio_enabled"]
        self.assertEqual(audio_toggle["show_when"]["source_key"], "destination")
        broadcast_values = list(audio_toggle["show_when"]["any_of"])
        self.assertGreaterEqual(len(broadcast_values), 2)
        self.assertTrue(
            all(json.loads(value)["platform"] == "broadcast" for value in broadcast_values)
        )

        source = by_key["background_audio_source"]
        self.assertEqual(
            source["show_when_all"][1],
            {"source_key": "broadcast_audio_enabled", "equals": True},
        )
        source_values = [str(row.get("value") or "") for row in source["options"]]
        self.assertEqual(
            source_values[:4],
            [
                "preset:morning_glow",
                "preset:calm_focus",
                "preset:gentle_rain",
                "preset:bright_pulse",
            ],
        )
        self.assertIn("upload", source_values)
        self.assertIn("custom", source_values)

    def test_uploaded_audio_is_stored_in_agent_lab_audio_folder(self) -> None:
        redis = FakeRedis()
        core = _load_full_core(redis)
        core._ai_tasks_ui_parse_schedule_input = lambda _text: (
            {
                "next_run_ts": 2_000_000_000.0,
                "interval_sec": 86400.0,
                "recurrence": {
                    "kind": "daily_local_time",
                    "hour": 7,
                    "minute": 0,
                    "second": 0,
                },
            },
            "",
        )
        destination = core._ai_tasks_ui_encode_destination_value(
            "broadcast",
            {
                "scope": "selected",
                "target": "voice_core:native:kitchen",
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            audio_root = Path(temp_dir) / "ai_task" / "background_audio"
            core._background_audio_root = lambda: audio_root
            result = core.handle_htmlui_tab_action(
                action="ai_tasks_add_schedule",
                payload={
                    "values": {
                        "task_prompt": "Create a short morning announcement.",
                        "schedule_text": "every day at 7am",
                        "destination": destination,
                        "broadcast_audio_enabled": True,
                        "background_audio_source": "upload",
                        "background_audio_upload": self._wav_upload_payload(),
                        "enabled": True,
                    }
                },
                redis_client=redis,
            )

            reminder = json.loads(redis.get(f"reminders:{result['id']}"))
            audio_url = reminder["delivery"]["audio_scene"]["background"]["url"]
            self.assertIn("/api/ai-tasks/background-audio/uploads/", audio_url)
            uploads = list((audio_root / "uploads").glob("*.wav"))
            self.assertEqual(len(uploads), 1)
            self.assertGreater(uploads[0].stat().st_size, 44)

    def test_uploaded_audio_rejects_mismatched_file_content(self) -> None:
        redis = FakeRedis()
        core = _load_full_core(redis)
        upload = self._wav_upload_payload(filename="pretend.mp3")
        with self.assertRaisesRegex(ValueError, "does not match"):
            core._store_background_audio_upload(upload)

    def test_default_presets_are_generated_as_valid_wav_files(self) -> None:
        redis = FakeRedis()
        core = _load_full_core(redis)
        with tempfile.TemporaryDirectory() as temp_dir:
            audio_root = Path(temp_dir) / "ai_task" / "background_audio"
            core._background_audio_root = lambda: audio_root
            preset_url = core._background_audio_preset_url("morning_glow")

            self.assertTrue(preset_url.endswith("/presets/morning_glow.wav"))
            generated = sorted((audio_root / "presets").glob("*.wav"))
            self.assertEqual(len(generated), 4)
            for path in generated:
                with wave.open(str(path), "rb") as wav_file:
                    self.assertEqual(wav_file.getnchannels(), 1)
                    self.assertEqual(wav_file.getframerate(), 24000)
                    self.assertEqual(wav_file.getnframes(), 12 * 24000)

    def test_add_form_persists_broadcast_delivery_separately_from_task_prompt(self) -> None:
        redis = FakeRedis()
        core = _load_full_core(redis)
        core._ai_tasks_ui_parse_schedule_input = lambda _text: (
            {
                "next_run_ts": 2_000_000_000.0,
                "interval_sec": 86400.0,
                "recurrence": {
                    "kind": "daily_local_time",
                    "hour": 7,
                    "minute": 0,
                    "second": 0,
                },
            },
            "",
        )
        destination = core._ai_tasks_ui_encode_destination_value(
            "broadcast",
            {
                "scope": "selected",
                "target": "voice_core:native:kitchen",
            },
        )

        result = core.handle_htmlui_tab_action(
            action="ai_tasks_add_schedule",
            payload={
                "values": {
                    "title": "Morning wake-up",
                    "task_prompt": "Fetch today's weather and create a motivating wake-up message.",
                    "schedule_text": "every day at 7am",
                    "destination": destination,
                    "broadcast_audio_enabled": True,
                    "background_audio_url": "https://example.test/morning.mp3",
                    "background_volume_percent": 60,
                    "ducking_target_percent": 35,
                    "ducking_attack_ms": 150,
                    "ducking_release_ms": 350,
                    "background_fade_ms": 500,
                    "background_loop": True,
                    "enabled": True,
                }
            },
            redis_client=redis,
        )

        reminder = json.loads(redis.get(f"reminders:{result['id']}"))
        self.assertEqual(reminder["platform"], "webui")
        self.assertNotIn("broadcast", reminder["task_prompt"].lower())
        self.assertEqual(reminder["delivery"]["type"], "broadcast")
        self.assertEqual(reminder["delivery"]["scope"], "selected")
        self.assertEqual(
            reminder["delivery"]["targets"],
            ["voice_core:native:kitchen"],
        )
        self.assertEqual(
            reminder["delivery"]["audio_scene"]["ducking"]["target_percent"],
            35,
        )


if __name__ == "__main__":
    unittest.main()
