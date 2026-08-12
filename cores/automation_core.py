"""General event-to-action automations for Tater integrations and voice targets."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import math
import os
import re
import struct
import threading
import time
import uuid
import wave
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

from announcement_targets import build_announcement_target_options
import requests

from helpers import describe_image_with_local_llm, redis_client
from integration_registry import get_integration_device_registry, run_integration_device_action
from notify import dispatch_notification, notifier_destination_catalog
from speech_settings import get_speech_settings
from speech_tts import speak_announcement_targets
from vision_settings import get_vision_settings
try:
    from tater_paths import agent_lab_path as _tater_agent_lab_path
except Exception:  # pragma: no cover - compatibility with older Tater runtimes.
    _tater_agent_lab_path = None


__version__ = "1.3.3"
MIN_TATER_VERSION = "98"
CORE_DESCRIPTION = (
    "Build simple event-to-action automations from Tater's shared integration categories, "
    "device actions, notifications, and announcement targets."
)
TAGS = ["automation", "integrations", "smart-home", "tts", "rules"]

CORE_SETTINGS = {
    "category": "Automation Core Settings",
    "hydra_tools_require_running": False,
    "required": {},
}

CORE_WEBUI_TAB = {
    "label": "Automations",
    "order": 24,
    "requires_running": True,
}

logger = logging.getLogger("automation_core")
logger.setLevel(logging.INFO)

_RULES_KEY = "automation:rules"
_QUEUE_KEY = "automation:queue"
_HISTORY_KEY = "automation:history"
_RUNTIME_KEY = "automation:runtime"
_CURSOR_KEY = "automation:integration_runtime:last_seq"
_INTEGRATION_EVENTS_KEY = "tater:integration_runtime:events"
_INTEGRATION_EVENT_SEQ_KEY = "tater:integration_runtime:event_seq"
_EVENT_SEQUENCE_MAX = 9_223_372_036_854_775_807
_LEGACY_CURSOR_CLAMP = 1_000_000
_HISTORY_LIMIT = 200
_WORKER_COUNT = 4
_BACKGROUND_AUDIO_MAX_UPLOAD_BYTES = 16 * 1024 * 1024
_BACKGROUND_AUDIO_PRESET_SECONDS = 12
_BACKGROUND_AUDIO_PRESET_SAMPLE_RATE = 24000
_BACKGROUND_AUDIO_PRESETS: Tuple[Dict[str, str], ...] = (
    {
        "id": "morning_glow",
        "label": "Morning Glow",
        "description": "Warm, optimistic synth chords for weather and wake-up announcements.",
    },
    {
        "id": "calm_focus",
        "label": "Calm Focus",
        "description": "A soft, steady ambient bed for reminders and status updates.",
    },
    {
        "id": "gentle_rain",
        "label": "Gentle Rain",
        "description": "A light, seamless rain-like texture with subtle tonal movement.",
    },
    {
        "id": "bright_pulse",
        "label": "Bright Pulse",
        "description": "A quiet rhythmic pulse for upbeat announcements.",
    },
)
_background_audio_preset_lock = threading.Lock()

_TRUE = {"1", "true", "yes", "on", "enabled", "y"}
_FALSE = {"0", "false", "no", "off", "disabled", "n"}
_ON_STATES = {
    "on",
    "open",
    "opened",
    "active",
    "detected",
    "motion",
    "occupied",
    "connected",
    "online",
    "home",
    "present",
    "wet",
    "alarm",
    "tamper",
    "true",
    "1",
}
_OFF_STATES = {
    "off",
    "closed",
    "close",
    "inactive",
    "clear",
    "idle",
    "unoccupied",
    "disconnected",
    "offline",
    "away",
    "not_present",
    "dry",
    "false",
    "0",
}

_EVENT_OPTIONS = [
    {"value": "changed", "label": "Changes"},
    {"value": "turns_on", "label": "Turns on / becomes active"},
    {"value": "turns_off", "label": "Turns off / becomes inactive"},
    {"value": "opens", "label": "Opens"},
    {"value": "closes", "label": "Closes"},
    {"value": "motion", "label": "Detects motion"},
    {"value": "person", "label": "Detects a person"},
    {"value": "vehicle", "label": "Detects a vehicle"},
    {"value": "animal", "label": "Detects an animal"},
    {"value": "package", "label": "Detects a package"},
    {"value": "face", "label": "Detects a face"},
    {"value": "license_plate", "label": "Detects a license plate"},
    {"value": "doorbell", "label": "Doorbell is pressed"},
    {"value": "connects", "label": "Connects / comes online"},
    {"value": "disconnects", "label": "Disconnects / goes offline"},
    {"value": "equals", "label": "State or value equals…"},
    {"value": "contains", "label": "Event contains text…"},
    {"value": "above", "label": "Numeric value rises above…"},
    {"value": "below", "label": "Numeric value falls below…"},
]

_EVENT_ICONS = {
    "changed": "↻",
    "turns_on": "●",
    "turns_off": "○",
    "opens": "↗",
    "closes": "↘",
    "motion": "⌁",
    "person": "♟",
    "vehicle": "◆",
    "animal": "♣",
    "package": "▣",
    "face": "◎",
    "license_plate": "▤",
    "doorbell": "◉",
    "connects": "⌁",
    "disconnects": "×",
    "equals": "=",
    "contains": "≋",
    "above": "↑",
    "below": "↓",
}

_CATEGORY_ICONS = {
    "light": "☀",
    "switch": "⏻",
    "plug": "⌁",
    "fan": "✣",
    "garage_door": "▥",
    "cover": "▤",
    "entry_sensor": "↔",
    "lock": "◆",
    "motion": "⌁",
    "camera": "◉",
    "doorbell": "◉",
    "leak": "◒",
    "climate": "◐",
    "temperature": "°",
    "humidity": "◔",
    "illuminance": "☼",
    "energy": "ϟ",
    "battery": "▰",
    "media_player": "♪",
    "presence": "◎",
    "network_device": "⌘",
    "remote": "⌁",
    "scene": "✦",
    "script": "▶",
    "sensor": "◇",
    "device": "◆",
}

_ACTION_LABELS = {
    "turn_on": "Turn on",
    "turn_off": "Turn off",
    "toggle": "Toggle",
    "set_brightness": "Set brightness",
    "set_color": "Set color",
    "open": "Open",
    "close": "Close",
    "stop": "Stop",
    "set_position": "Set position",
    "lock": "Lock",
    "unlock": "Unlock",
    "set_temperature": "Set temperature",
    "set_hvac_mode": "Set HVAC mode",
    "play": "Play",
    "pause": "Pause",
    "playpause": "Play / pause",
    "next": "Next",
    "previous": "Previous",
    "set_volume": "Set volume",
    "volume_up": "Volume up",
    "volume_down": "Volume down",
    "mute": "Mute",
    "unmute": "Unmute",
    "play_media": "Play media",
    "play_url": "Play URL",
    "announce": "Announce",
    "activate": "Activate",
    "run": "Run",
}

def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore").strip()
    return str(value or "").strip()


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in _TRUE:
        return True
    if token in _FALSE:
        return False
    return bool(default)


def _int(value: Any, default: int = 0, *, minimum: int = 0, maximum: int = 1_000_000) -> int:
    try:
        parsed = int(float(_text(value)))
    except Exception:
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _sequence(value: Any, default: int = 0) -> int:
    text = _text(value)
    try:
        parsed = int(text)
    except Exception:
        try:
            parsed = int(float(text))
        except Exception:
            parsed = int(default)
    return max(0, min(_EVENT_SEQUENCE_MAX, parsed))


def _background_audio_root() -> Path:
    # Tater exposes this shared audio-scene asset folder through its existing
    # /api/ai-tasks/background-audio route, even when AI Task Core is disabled.
    if callable(_tater_agent_lab_path):
        return Path(_tater_agent_lab_path("ai_task", "background_audio")).resolve()
    configured = _text(os.getenv("TATER_AGENT_ROOT"))
    base = Path(configured).expanduser() if configured else Path.cwd() / "agent_lab"
    return (base / "ai_task" / "background_audio").resolve()


def _background_audio_base_url() -> str:
    port = _int(os.getenv("HTMLUI_PORT"), 8501, minimum=1, maximum=65535)
    return f"http://127.0.0.1:{port}/api/ai-tasks/background-audio"


def _background_audio_file_url(kind: str, filename: str) -> str:
    clean_kind = _text(kind).lower()
    clean_filename = Path(_text(filename)).name
    if clean_kind not in {"presets", "uploads"} or not clean_filename:
        return ""
    return f"{_background_audio_base_url()}/{clean_kind}/{quote(clean_filename)}"


def _background_audio_periodic_frequency(frequency: float) -> float:
    duration = float(_BACKGROUND_AUDIO_PRESET_SECONDS)
    return max(1.0 / duration, round(float(frequency) * duration) / duration)


def _background_audio_preset_sample(preset_id: str, sample_time: float) -> float:
    tau = math.tau

    def tone(frequency: float, phase: float = 0.0) -> float:
        return math.sin(tau * _background_audio_periodic_frequency(frequency) * sample_time + phase)

    if preset_id == "morning_glow":
        breath = 0.72 + (0.18 * math.sin(tau * sample_time / 6.0))
        pad = (tone(261.63) + tone(329.63, 0.7) + tone(392.0, 1.4)) / 3.0
        shimmer_gate = (0.5 + (0.5 * math.sin(tau * sample_time / 3.0))) ** 6
        return (0.25 * breath * pad) + (0.055 * tone(659.25, 0.3) * shimmer_gate)
    if preset_id == "calm_focus":
        breath = 0.68 + (0.2 * math.sin(tau * sample_time / 12.0))
        low = (tone(130.81) + tone(196.0, 0.8)) * 0.5
        air = (tone(261.63, 1.1) + tone(293.66, 2.0)) * 0.5
        return (0.22 * breath * low) + (0.07 * air)
    if preset_id == "gentle_rain":
        texture = 0.0
        for index, frequency in enumerate((487.0, 613.0, 743.0, 887.0, 1061.0, 1229.0, 1451.0, 1693.0)):
            texture += tone(frequency, 0.73 * index) * (0.7 / math.sqrt(index + 2.0))
        drift = tone(174.61, 0.5) * (0.5 + (0.5 * math.sin(tau * sample_time / 6.0)))
        return (0.04 * texture) + (0.045 * drift)
    pulse = (0.5 + (0.5 * math.sin(tau * sample_time / 1.5))) ** 5
    chord = (tone(220.0) + tone(277.18, 0.6) + tone(329.63, 1.2)) / 3.0
    upper = tone(554.37, 0.9) * (0.5 + (0.5 * math.sin(tau * sample_time / 3.0)))
    return (0.2 * chord * (0.35 + (0.65 * pulse))) + (0.045 * upper)


def _write_background_audio_preset(path: Path, preset_id: str) -> None:
    sample_rate = int(_BACKGROUND_AUDIO_PRESET_SAMPLE_RATE)
    frame_count = sample_rate * int(_BACKGROUND_AUDIO_PRESET_SECONDS)
    pcm = bytearray(frame_count * 2)
    for index in range(frame_count):
        sample = _background_audio_preset_sample(preset_id, index / sample_rate)
        struct.pack_into("<h", pcm, index * 2, int(max(-0.92, min(0.92, sample)) * 32767.0))
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with wave.open(str(temp_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(bytes(pcm))
        os.replace(temp_path, path)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _ensure_background_audio_presets() -> Dict[str, Path]:
    root = _background_audio_root() / "presets"
    out: Dict[str, Path] = {}
    with _background_audio_preset_lock:
        for preset in _BACKGROUND_AUDIO_PRESETS:
            preset_id = _text(preset.get("id"))
            if not preset_id:
                continue
            path = root / f"{preset_id}.wav"
            if not path.is_file() or path.stat().st_size <= 44:
                _write_background_audio_preset(path, preset_id)
            out[preset_id] = path
    return out


def _background_audio_preset_url(preset_id: Any) -> str:
    clean_id = _text(preset_id).lower()
    if clean_id not in {_text(row.get("id")) for row in _BACKGROUND_AUDIO_PRESETS}:
        raise ValueError("Choose a valid background audio preset.")
    path = _ensure_background_audio_presets().get(clean_id)
    if not path or not path.is_file():
        raise ValueError("The selected background audio preset could not be created.")
    return _background_audio_file_url("presets", path.name)


def _background_audio_detect_extension(filename: Any, content_type: Any, data: bytes) -> str:
    suffix = Path(_text(filename)).suffix.lower()
    content = _text(content_type).split(";", 1)[0].strip().lower()
    if data.startswith(b"RIFF") and len(data) >= 12 and data[8:12] == b"WAVE":
        detected = ".wav"
    elif data.startswith(b"fLaC"):
        detected = ".flac"
    elif data.startswith(b"ID3") or any(
        data[index] == 0xFF and (data[index + 1] & 0xE0) == 0xE0
        for index in range(max(0, min(len(data) - 1, 4096)))
    ):
        detected = ".mp3"
    else:
        raise ValueError("Uploaded background audio must be a valid WAV, MP3, or FLAC file.")
    content_extensions = {
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/flac": ".flac",
        "audio/x-flac": ".flac",
    }
    claimed = suffix or content_extensions.get(content, "")
    if claimed and claimed not in {".wav", ".mp3", ".flac"}:
        raise ValueError("Uploaded background audio must use a .wav, .mp3, or .flac filename.")
    if claimed and claimed != detected:
        raise ValueError("Uploaded background audio content does not match its filename.")
    if detected == ".wav":
        try:
            with wave.open(io.BytesIO(data), "rb") as wav_file:
                channels = int(wav_file.getnchannels())
                sample_width = int(wav_file.getsampwidth())
                sample_rate = int(wav_file.getframerate())
                frame_count = int(wav_file.getnframes())
        except Exception as exc:
            raise ValueError("Uploaded WAV background audio is invalid or unsupported.") from exc
        if channels not in {1, 2} or sample_width != 2 or not 8000 <= sample_rate <= 96000 or frame_count <= 0:
            raise ValueError("Uploaded WAV background audio must be 16-bit mono or stereo at 8–96 kHz.")
    return detected


def _store_background_audio_upload(raw: Any) -> str:
    upload = raw if isinstance(raw, dict) else {}
    encoded = _text(upload.get("data_b64"))
    if not encoded:
        raise ValueError("Choose a WAV, MP3, or FLAC file to upload.")
    try:
        data = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ValueError("The uploaded background audio could not be decoded.") from exc
    if not data:
        raise ValueError("The uploaded background audio is empty.")
    if len(data) > _BACKGROUND_AUDIO_MAX_UPLOAD_BYTES:
        raise ValueError("Uploaded background audio must be 16 MB or smaller.")
    extension = _background_audio_detect_extension(upload.get("filename"), upload.get("content_type"), data)
    source_stem = Path(_text(upload.get("filename")) or "background-audio").stem
    safe_stem = re.sub(r"[^a-zA-Z0-9_-]+", "-", source_stem).strip("-_").lower() or "background-audio"
    digest = hashlib.sha256(data).hexdigest()[:12]
    filename = f"{safe_stem[:48]}-{digest}{extension}"
    root = _background_audio_root() / "uploads"
    root.mkdir(parents=True, exist_ok=True)
    path = root / filename
    if not path.is_file() or path.stat().st_size != len(data):
        temp_path = root / f".{filename}.{uuid.uuid4().hex}.tmp"
        try:
            temp_path.write_bytes(data)
            os.replace(temp_path, path)
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass
    return _background_audio_file_url("uploads", filename)


def _background_audio_source_from_url(url: Any) -> str:
    text = _text(url)
    for preset in _BACKGROUND_AUDIO_PRESETS:
        preset_id = _text(preset.get("id"))
        if text.endswith(f"/presets/{preset_id}.wav"):
            return f"preset:{preset_id}"
    if "/api/ai-tasks/background-audio/uploads/" in text:
        return "upload"
    return "custom"


def _normalize_tts_audio_scene(raw: Any) -> Dict[str, Any]:
    scene = raw if isinstance(raw, dict) else {}
    background = scene.get("background") if isinstance(scene.get("background"), dict) else {}
    ducking = scene.get("ducking") if isinstance(scene.get("ducking"), dict) else {}
    finish = scene.get("finish") if isinstance(scene.get("finish"), dict) else {}
    background_url = _text(background.get("url") or scene.get("background_url"))
    if not background_url:
        return {}
    return {
        "background": {
            "url": background_url,
            "loop": _bool(background.get("loop"), True),
            "volume_percent": _int(background.get("volume_percent"), 60, maximum=100),
        },
        "ducking": {
            "target_percent": _int(ducking.get("target_percent"), 35, maximum=100),
            "attack_ms": _int(ducking.get("attack_ms"), 150, maximum=10000),
            "release_ms": _int(ducking.get("release_ms"), 350, maximum=10000),
        },
        "finish": {
            "fade_ms": _int(finish.get("fade_ms"), 500, maximum=10000),
        },
    }


def _float(value: Any) -> Optional[float]:
    try:
        return float(_text(value))
    except Exception:
        return None


def _token(value: Any) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", _text(value).lower().replace("-", "_")).strip("_")


def _list(value: Any) -> List[str]:
    raw = value
    if isinstance(raw, str):
        token = raw.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                raw = json.loads(token)
            except Exception:
                raw = token
    if isinstance(raw, (tuple, set)):
        raw = list(raw)
    if not isinstance(raw, list):
        raw = [] if raw in (None, "") else [raw]
    out: List[str] = []
    seen: set[str] = set()
    for item in raw:
        if isinstance(item, dict):
            item = item.get("value") or item.get("id") or item.get("key") or item.get("target")
        text = _text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    token = _text(value)
    if not token:
        return {}
    try:
        parsed = json.loads(token)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_record(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(_text(value))
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _now_label(ts: Any) -> str:
    value = _float(ts)
    if not value:
        return "never"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def _encode_device(provider: Any, device_id: Any) -> str:
    left = _text(provider)
    right = _text(device_id)
    return f"{left}|{right}" if left and right else ""


def _decode_device(value: Any) -> Tuple[str, str]:
    token = _text(value)
    if "|" not in token:
        return "", token
    provider, device_id = token.split("|", 1)
    return _text(provider), _text(device_id)


def _device_id(device: Dict[str, Any]) -> str:
    return _text(device.get("id") or device.get("ref"))


def _device_ref(device: Dict[str, Any]) -> str:
    return _text(device.get("ref") or device.get("id"))


def _integration_value(category: Any, provider: Any) -> str:
    category_token = _token(category)
    provider_token = _token(provider)
    if not category_token or not provider_token:
        return ""
    return f"{category_token}::{provider_token}"


def _integration_provider(value: Any) -> str:
    token = _text(value)
    if "::" not in token:
        return ""
    _category, provider = token.split("::", 1)
    return _token(provider)


def _integration_from_devices(category: Any, values: Any) -> str:
    integrations: set[str] = set()
    for value in _list(values):
        provider, _device = _decode_device(value)
        integration = _integration_value(category, provider)
        if integration:
            integrations.add(integration)
    return next(iter(integrations)) if len(integrations) == 1 else ""


def _device_categories(device: Dict[str, Any]) -> set[str]:
    values = [
        *(device.get("category_ids") or []),
        *(device.get("capabilities") or []),
        device.get("type"),
    ]
    return {_token(item) for item in values if _token(item)}


def _device_actions(device: Dict[str, Any]) -> List[str]:
    return [_token(item) for item in (device.get("actions") or []) if _token(item)]


def _device_room(device: Dict[str, Any]) -> str:
    return _token(device.get("room") or device.get("area") or "unassigned")


def _registry(client: Any = None, *, refresh: bool = False) -> Dict[str, Any]:
    try:
        result = get_integration_device_registry(client or redis_client, refresh=refresh)
    except Exception:
        logger.debug("[automation] device registry unavailable", exc_info=True)
        return {"devices": [], "categories": [], "rooms": [], "category_definitions": []}
    return result if isinstance(result, dict) else {"devices": [], "categories": [], "rooms": []}


def _category_rows(
    registry: Dict[str, Any],
    *,
    actionable_only: bool = False,
    triggerable_only: bool = False,
) -> List[Dict[str, Any]]:
    definitions = {
        _token(item.get("id")): item
        for item in registry.get("category_definitions") or []
        if isinstance(item, dict) and _token(item.get("id"))
    }
    rows: List[Dict[str, Any]] = []
    for category in registry.get("categories") or []:
        if not isinstance(category, dict):
            continue
        devices = [item for item in category.get("devices") or [] if isinstance(item, dict)]
        if triggerable_only:
            devices = [item for item in devices if _trigger_event_values_for_device(item)]
        if actionable_only:
            devices = [item for item in devices if _device_actions(item)]
        if not devices:
            continue
        category_id = _token(category.get("id"))
        if not category_id:
            continue
        rows.append(
            {
                "id": category_id,
                "name": _text(category.get("name")) or category_id.replace("_", " ").title(),
                "description": _text(
                    category.get("description") or (definitions.get(category_id) or {}).get("description")
                ),
                "devices": devices,
                "order": _int(category.get("order"), 1000),
            }
        )
    rows.sort(key=lambda item: (item["order"], item["name"].casefold()))
    return rows


def _category_options(
    registry: Dict[str, Any],
    *,
    actionable_only: bool = False,
    triggerable_only: bool = False,
) -> List[Dict[str, Any]]:
    return [
        {
            "value": row["id"],
            "label": row["name"],
            "description": row["description"] or f"{len(row['devices'])} available",
            "meta": f"{len(row['devices'])} device{'s' if len(row['devices']) != 1 else ''}",
            "icon": _CATEGORY_ICONS.get(row["id"], "◆"),
        }
        for row in _category_rows(
            registry,
            actionable_only=actionable_only,
            triggerable_only=triggerable_only,
        )
    ]


def _device_option(device: Dict[str, Any]) -> Dict[str, Any]:
    provider = _text(device.get("integration_id"))
    device_id = _device_id(device)
    name = _text(device.get("name")) or device_id
    room = _text(device.get("room") or device.get("area"))
    integration = _text(device.get("integration_name")) or provider
    categories = sorted(_device_categories(device))
    primary_category = categories[0] if categories else "device"
    details = " • ".join(item for item in (room, integration) if item)
    return {
        "value": _encode_device(provider, device_id),
        "label": name,
        "description": details,
        "meta": _text(device.get("state") or device.get("status")),
        "icon": _CATEGORY_ICONS.get(primary_category, "◆"),
    }


def _integration_dependency(
    registry: Dict[str, Any],
    *,
    current_category: str = "",
    source_key: str,
    actionable_only: bool = False,
    triggerable_only: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    options_by_source: Dict[str, List[Dict[str, Any]]] = {}
    all_options: List[Dict[str, Any]] = []
    for category in _category_rows(
        registry,
        actionable_only=actionable_only,
        triggerable_only=triggerable_only,
    ):
        providers: Dict[str, Dict[str, Any]] = {}
        for device in category["devices"]:
            provider = _token(device.get("integration_id"))
            if not provider:
                continue
            row = providers.setdefault(
                provider,
                {
                    "value": _integration_value(category["id"], provider),
                    "label": _text(device.get("integration_name")) or provider.replace("_", " ").title(),
                    "count": 0,
                    "icon": _CATEGORY_ICONS.get(category["id"], "◆"),
                },
            )
            row["count"] = _int(row.get("count"), 0, minimum=0) + 1
        rows: List[Dict[str, Any]] = []
        for provider_row in providers.values():
            row = dict(provider_row)
            count = _int(row.pop("count", 0), 0, minimum=0)
            row["description"] = f"{count} compatible device{'s' if count != 1 else ''} available"
            rows.append(row)
        rows.sort(key=lambda item: (_text(item.get("label")).casefold(), _text(item.get("value"))))
        options_by_source[category["id"]] = rows
        all_options.extend(dict(row) for row in rows)
    selected = [dict(row) for row in options_by_source.get(_token(current_category), [])]
    return selected, {
        "source_key": source_key,
        "options_by_source": options_by_source,
        "default_options": all_options,
    }


def _device_dependency(
    registry: Dict[str, Any],
    *,
    current_integration: str = "",
    source_key: str,
    current_values: Any = None,
    multiple: bool = False,
    allow_any: bool = False,
    actionable_only: bool = False,
    triggerable_only: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    current = _list(current_values)
    options_by_source: Dict[str, List[Dict[str, Any]]] = {}
    all_options: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for category in _category_rows(
        registry,
        actionable_only=actionable_only,
        triggerable_only=triggerable_only,
    ):
        devices_by_integration: Dict[str, List[Dict[str, Any]]] = {}
        for device in category["devices"]:
            integration = _integration_value(category["id"], device.get("integration_id"))
            if integration:
                devices_by_integration.setdefault(integration, []).append(device)
        for integration, devices in devices_by_integration.items():
            rows = [_device_option(device) for device in devices]
            rows.sort(key=lambda item: (_text(item.get("label")).casefold(), _text(item.get("value"))))
            if not multiple and allow_any:
                rows = [
                    {
                        "value": "",
                        "label": "Any device",
                        "description": "Run when any compatible device from this integration matches.",
                        "icon": "✦",
                    },
                    *rows,
                ]
            options_by_source[integration] = rows
            for row in rows:
                if not row["value"] or row["value"] in seen:
                    continue
                seen.add(row["value"])
                all_options.append(row)
    selected_rows = [dict(row) for row in options_by_source.get(_text(current_integration), [])]
    for value in current:
        if value and not any(row.get("value") == value for row in selected_rows):
            selected_rows.append({"value": value, "label": f"{value} (saved)"})
    return selected_rows, {
        "source_key": source_key,
        "options_by_source": options_by_source,
        "default_options": all_options,
    }


def _event_option(value: Any) -> Dict[str, Any]:
    token = _token(value)
    row = next((item for item in _EVENT_OPTIONS if item["value"] == token), None)
    return {
        "value": token,
        "label": _text((row or {}).get("label")) or token.replace("_", " ").title(),
        "icon": _EVENT_ICONS.get(token, "◆"),
    }


def _trigger_event_values_for_device(device: Dict[str, Any]) -> List[str]:
    found: set[str] = set()

    def add(*values: str) -> None:
        found.update(_token(value) for value in values if _token(value))

    for source in device.get("event_sources") or []:
        if not isinstance(source, dict):
            continue
        explicit_events = _list(source.get("trigger_events") or source.get("events"))
        if explicit_events:
            add(*explicit_events)
            continue
        source_type = _token(source.get("type"))
        detected_event = source_type.removeprefix("smart_")
        if detected_event == "licenseplate":
            detected_event = "license_plate"
        if detected_event in {
            "license_plate",
            "person",
            "vehicle",
            "animal",
            "package",
            "face",
            "doorbell",
            "motion",
        }:
            add(detected_event)
        elif source_type in {
            "contact",
            "door",
            "window",
            "entry",
            "entry_sensor",
            "door_window",
            "open_close",
            "garage",
            "garage_door",
            "cover",
        }:
            add("opens", "closes")
        elif source_type in {"connectivity", "network", "network_device"}:
            add("connects", "disconnects")
        elif source_type in {
            "occupancy",
            "presence",
            "switch",
            "light",
            "input",
            "power",
            "power_meter",
            "relay",
            "leak",
            "tamper",
        }:
            add("turns_on", "turns_off")
        elif source_type in {
            "temperature",
            "humidity",
            "relative_humidity",
            "illuminance",
            "light_level",
            "energy",
            "battery",
            "meter",
            "sensor",
            "thermostat",
            "value",
        }:
            add("changed", "above", "below")
    return [row["value"] for row in _EVENT_OPTIONS if row["value"] in found]


def _trigger_state_options_for_device(device: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for source in device.get("event_sources") or []:
        if not isinstance(source, dict):
            continue
        for value in _list(source.get("state_options") or source.get("options")):
            token = _text(value)
            folded = token.casefold()
            if not token or folded in seen:
                continue
            seen.add(folded)
            out.append(token)
    return out


def _trigger_event_dependency(
    registry: Dict[str, Any],
    *,
    current_device: Any = "",
    current_event: Any = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    options_by_source: Dict[str, List[Dict[str, Any]]] = {}
    all_values: set[str] = set()
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        encoded = _encode_device(device.get("integration_id"), _device_id(device))
        if not encoded:
            continue
        values = _trigger_event_values_for_device(device)
        all_values.update(values)
        state_options = _trigger_state_options_for_device(device)
        rows: List[Dict[str, Any]] = []
        for value in values:
            option = _event_option(value)
            if value == "equals":
                option["description"] = (
                    f"Reported states: {', '.join(state_options[:8])}"
                    if state_options
                    else "Enter the exact state value reported by the integration."
                )
            rows.append(option)
        options_by_source[encoded] = rows
    default_options = [_event_option(row["value"]) for row in _EVENT_OPTIONS if row["value"] in all_values]
    selected = list(options_by_source.get(_text(current_device), default_options))
    saved = _token(current_event)
    if saved and not any(row.get("value") == saved for row in selected):
        selected.append({**_event_option(saved), "meta": "Saved setting"})
    return selected, {
        "source_key": "trigger_device",
        "options_by_source": options_by_source,
        "default_options": default_options,
    }


def _action_dependency(
    registry: Dict[str, Any],
    *,
    current_integration: str = "",
    current_action: str = "",
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    options_by_source: Dict[str, List[Dict[str, str]]] = {}
    all_actions: set[str] = set()
    for category in _category_rows(registry, actionable_only=True):
        actions_by_integration: Dict[str, set[str]] = {}
        for device in category["devices"]:
            integration = _integration_value(category["id"], device.get("integration_id"))
            if integration:
                actions_by_integration.setdefault(integration, set()).update(_device_actions(device))
        for integration, integration_actions in actions_by_integration.items():
            actions = sorted(integration_actions)
            all_actions.update(actions)
            options_by_source[integration] = [
                {"value": action, "label": _ACTION_LABELS.get(action, action.replace("_", " ").title())}
                for action in actions
            ]
    default_options = [
        {"value": action, "label": _ACTION_LABELS.get(action, action.replace("_", " ").title())}
        for action in sorted(all_actions)
    ]
    selected = list(options_by_source.get(_text(current_integration), default_options))
    if current_action and not any(row["value"] == current_action for row in selected):
        selected.append({"value": current_action, "label": f"{current_action.replace('_', ' ').title()} (saved)"})
    return selected, {
        "source_key": "action_integration",
        "options_by_source": options_by_source,
        "default_options": default_options,
    }


def _room_options(registry: Dict[str, Any]) -> List[Dict[str, str]]:
    rows = [{"value": "", "label": "Any room"}]
    seen = {""}
    for room in registry.get("rooms") or []:
        if not isinstance(room, dict):
            continue
        value = _token(room.get("id") or room.get("name"))
        if not value or value in seen:
            continue
        seen.add(value)
        rows.append({"value": value, "label": _text(room.get("name")) or value.replace("_", " ").title()})
    return rows


def _devices_for_category_options(
    registry: Dict[str, Any],
    category_id: str,
    *,
    require_actions: Sequence[str] = (),
    placeholder: str = "",
) -> List[Dict[str, str]]:
    required = {_token(item) for item in require_actions if _token(item)}
    rows: List[Dict[str, str]] = []
    if placeholder:
        rows.append({"value": "", "label": placeholder})
    seen: set[str] = set()
    for category in _category_rows(registry):
        if category["id"] != _token(category_id):
            continue
        for device in category["devices"]:
            actions = set(_device_actions(device))
            if required and not required.intersection(actions):
                continue
            option = _device_option(device)
            if option["value"] in seen:
                continue
            seen.add(option["value"])
            rows.append(option)
    return rows


def _homeassistant_config() -> Dict[str, str]:
    try:
        from tateros import integration_store as integration_store_module

        module = integration_store_module.integration_module("homeassistant")
        if module is not None:
            result = module.load_homeassistant_config(required=False)
            if isinstance(result, dict):
                return {"base": _text(result.get("base")), "token": _text(result.get("token"))}
    except Exception:
        pass
    return {"base": "", "token": ""}


def _announcement_options(current_values: Any = None) -> List[Dict[str, str]]:
    ha = _homeassistant_config()
    try:
        return [
            {
                **dict(row),
                "description": _text(row.get("description")) or "Speaker, media player, or Tater satellite",
                "icon": _text(row.get("icon")) or "♪",
            }
            for row in build_announcement_target_options(
                homeassistant_base_url=ha["base"],
                homeassistant_token=ha["token"],
                include_homeassistant=bool(ha["base"] and ha["token"]),
                include_sonos=True,
                include_unifi_protect=True,
                include_voice_core=True,
                include_integrations=True,
                current_values=current_values,
            )
            if isinstance(row, dict)
        ]
    except Exception:
        logger.debug("[automation] announcement target discovery failed", exc_info=True)
        return []


def _encode_notification_target(platform: Any, targets: Any = None) -> str:
    platform_name = _text(platform).lower()
    if not platform_name:
        return ""
    payload = targets if isinstance(targets, dict) else {}
    return json.dumps({"platform": platform_name, "targets": payload}, sort_keys=True, separators=(",", ":"))


def _decode_notification_target(value: Any) -> Optional[Dict[str, Any]]:
    payload = _json_object(value)
    platform = _text(payload.get("platform")).lower()
    if not platform:
        return None
    return {
        "platform": platform,
        "targets": dict(payload.get("targets") or {}) if isinstance(payload.get("targets"), dict) else {},
    }


def _notification_label(platform: str, targets: Dict[str, Any]) -> str:
    for key in (
        "label",
        "channel",
        "channel_id",
        "room_alias",
        "room_id",
        "chat_id",
        "device_name",
        "device_id",
        "service",
        "device_service",
        "scope",
    ):
        value = _text(targets.get(key))
        if value:
            return value
    return "Defaults"


def _notification_options(client: Any, current_values: Any = None) -> List[Dict[str, str]]:
    try:
        catalog = notifier_destination_catalog(redis_client=client, limit=250)
    except Exception:
        catalog = {"platforms": []}
    rows: List[Dict[str, str]] = []
    seen: set[str] = set()
    for platform_row in catalog.get("platforms") or []:
        if not isinstance(platform_row, dict):
            continue
        platform = _text(platform_row.get("platform")).lower()
        platform_label = _text(platform_row.get("label")) or platform.replace("_", " ").title()
        if not platform:
            continue
        if not _bool(platform_row.get("requires_target"), False):
            value = _encode_notification_target(platform, {})
            rows.append({"value": value, "label": f"{platform_label}: defaults"})
            seen.add(value)
        for destination in platform_row.get("destinations") or []:
            if not isinstance(destination, dict):
                continue
            targets = destination.get("targets") if isinstance(destination.get("targets"), dict) else {}
            value = _encode_notification_target(platform, targets)
            if not value or value in seen:
                continue
            seen.add(value)
            label = _text(destination.get("label")) or _notification_label(platform, targets)
            rows.append({"value": value, "label": f"{platform_label}: {label}"})
    for saved in _list(current_values):
        if saved and saved not in seen:
            rows.append({"value": saved, "label": f"{saved} (saved)"})
    return rows


def _normalize_rule(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    trigger_category = _token(raw.get("trigger_category"))
    trigger_event = _token(raw.get("trigger_event") or "changed")
    action_type = _token(raw.get("action_type") or "device")
    raw_tts_text = _text(raw.get("tts_text"))
    tts_mode = _token(raw.get("tts_mode") or ("custom" if raw_tts_text else "default"))
    if tts_mode not in {"default", "custom"}:
        tts_mode = "custom" if raw_tts_text else "default"
    if not trigger_category or trigger_event not in {row["value"] for row in _EVENT_OPTIONS}:
        return None
    if action_type not in {"device", "tts", "notification", "camera_ai"}:
        return None
    now = time.time()
    trigger_device = _text(raw.get("trigger_device"))
    trigger_integration = _text(raw.get("trigger_integration")) or _integration_from_devices(
        trigger_category,
        trigger_device,
    )
    action_category = _token(raw.get("action_category"))
    action_devices = _list(raw.get("action_devices"))
    action_integration = _text(raw.get("action_integration")) or _integration_from_devices(
        action_category,
        action_devices,
    )
    rule = {
        "id": _text(raw.get("id")) or str(uuid.uuid4()),
        "name": _text(raw.get("name")) or "New automation",
        "enabled": _bool(raw.get("enabled"), True),
        "preset": _token(raw.get("preset") or "custom"),
        "trigger_category": trigger_category,
        "trigger_integration": trigger_integration,
        "trigger_device": trigger_device,
        "trigger_room": _token(raw.get("trigger_room")),
        "trigger_event": trigger_event,
        "trigger_attribute": _text(raw.get("trigger_attribute")),
        "trigger_value": _text(raw.get("trigger_value")),
        "cooldown_seconds": _int(raw.get("cooldown_seconds"), 30, minimum=0, maximum=86400),
        "action_type": action_type,
        "action_category": action_category,
        "action_integration": action_integration,
        "action_scope": _token(raw.get("action_scope") or "devices"),
        "action_devices": action_devices,
        "action_room": _token(raw.get("action_room")),
        "action_operation": _token(raw.get("action_operation")),
        "action_value": _text(raw.get("action_value")),
        "action_mode": _text(raw.get("action_mode")),
        "action_text": _text(raw.get("action_text")),
        "action_payload_json": _text(raw.get("action_payload_json")),
        "tts_mode": tts_mode,
        "tts_text": raw_tts_text,
        "tts_targets": _list(raw.get("tts_targets")),
        "tts_audio_scene": _normalize_tts_audio_scene(raw.get("tts_audio_scene")),
        "notification_title": _text(raw.get("notification_title") or "Tater Automation"),
        "notification_message": _text(raw.get("notification_message")),
        "notification_targets": _list(raw.get("notification_targets")),
        "notification_priority": "high"
        if _token(raw.get("notification_priority")) in {"high", "critical"}
        else "normal",
        "camera_source": _token(raw.get("camera_source") or "trigger"),
        "camera_device": _text(raw.get("camera_device")),
        "vision_prompt": _text(raw.get("vision_prompt") or "Briefly describe the important activity in this image."),
        "vision_fallback": _text(raw.get("vision_fallback") or "Activity was detected."),
        "camera_tts_text": _text(raw.get("camera_tts_text") or "{vision}"),
        "camera_tts_targets": _list(raw.get("camera_tts_targets")),
        "camera_notification_title": _text(raw.get("camera_notification_title") or "Camera Activity"),
        "camera_notification_message": _text(raw.get("camera_notification_message") or "{vision}"),
        "camera_notification_targets": _list(raw.get("camera_notification_targets")),
        "camera_notification_priority": "high"
        if _token(raw.get("camera_notification_priority")) in {"high", "critical"}
        else "normal",
        "created_at": _float(raw.get("created_at")) or now,
        "updated_at": _float(raw.get("updated_at")) or now,
        "last_run_ts": _float(raw.get("last_run_ts")) or 0.0,
        "last_status": _text(raw.get("last_status")),
        "last_summary": _text(raw.get("last_summary")),
        "last_error": _text(raw.get("last_error")),
        "run_count": _int(raw.get("run_count"), 0, minimum=0),
        "error_count": _int(raw.get("error_count"), 0, minimum=0),
        "source_core": _token(raw.get("source_core")),
        "source_rule_id": _text(raw.get("source_rule_id")),
    }
    if rule["action_scope"] not in {"category", "devices"}:
        rule["action_scope"] = "devices"
    if action_type == "device":
        if not rule["action_category"] or not rule["action_operation"]:
            return None
        if rule["action_scope"] == "devices" and not rule["action_devices"]:
            return None
    elif action_type == "tts":
        if not rule["tts_targets"]:
            return None
        if rule["tts_mode"] == "custom" and not rule["tts_text"]:
            return None
    elif action_type == "notification":
        if not rule["notification_message"] or not rule["notification_targets"]:
            return None
    elif action_type == "camera_ai":
        if rule["camera_source"] not in {"trigger", "selected"}:
            rule["camera_source"] = "trigger"
        if rule["camera_source"] == "selected" and not rule["camera_device"]:
            return None
        if not rule["camera_tts_targets"] and not rule["camera_notification_targets"]:
            return None
    return rule


def _load_rules(client: Any) -> Dict[str, Dict[str, Any]]:
    raw = client.hgetall(_RULES_KEY) or {}
    if not isinstance(raw, dict):
        return {}
    rules: Dict[str, Dict[str, Any]] = {}
    for field, value in raw.items():
        payload = _json_record(value)
        if not payload:
            continue
        payload.setdefault("id", _text(field))
        rule = _normalize_rule(payload)
        if rule:
            rules[rule["id"]] = rule
    return rules


def _get_rule(client: Any, rule_id: Any) -> Optional[Dict[str, Any]]:
    token = _text(rule_id)
    if not token:
        return None
    payload = _json_record(client.hget(_RULES_KEY, token))
    if not payload:
        return None
    payload.setdefault("id", token)
    return _normalize_rule(payload)


def _save_rule(client: Any, rule: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_rule(rule)
    if not normalized:
        raise ValueError("The automation is missing a required trigger or action setting.")
    client.hset(_RULES_KEY, normalized["id"], json.dumps(normalized, separators=(",", ":"), default=str))
    return normalized


def _runtime_set(client: Any, **fields: Any) -> None:
    payload = {key: json.dumps(value) if isinstance(value, (dict, list)) else str(value) for key, value in fields.items()}
    payload["updated_at"] = str(time.time())
    try:
        client.hset(_RUNTIME_KEY, mapping=payload)
    except Exception:
        logger.debug("[automation] runtime update failed", exc_info=True)


def _runtime_get(client: Any) -> Dict[str, Any]:
    raw = client.hgetall(_RUNTIME_KEY) or {}
    return {_text(key): _text(value) for key, value in raw.items()} if isinstance(raw, dict) else {}


def _append_history(client: Any, row: Dict[str, Any]) -> None:
    client.lpush(_HISTORY_KEY, json.dumps(row, separators=(",", ":"), default=str))
    client.ltrim(_HISTORY_KEY, 0, _HISTORY_LIMIT - 1)


def _history(client: Any, limit: int = 50) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in client.lrange(_HISTORY_KEY, 0, max(0, limit - 1)) or []:
        row = _json_record(raw)
        if row:
            rows.append(row)
    return rows


def _integration_events(client: Any, after_seq: int, limit: int = 200) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    max_rows = max(1, min(1000, int(limit or 200)))
    page_size = min(50, max_rows)
    for start in range(0, 1000, page_size):
        raw_rows = client.lrange(
            _INTEGRATION_EVENTS_KEY,
            start,
            min(999, start + page_size - 1),
        ) or []
        reached_cursor = False
        for raw in raw_rows:
            event = _json_record(raw)
            if not event:
                continue
            seq = _sequence(event.get("seq"), 0)
            if seq <= after_seq:
                reached_cursor = True
                continue
            event["seq"] = seq
            rows.append(event)
        if reached_cursor or len(raw_rows) < page_size:
            break
    rows.sort(key=lambda item: _sequence(item.get("seq"), 0))
    return rows[:max_rows]


def _resolve_event_cursor(stored: Any, current: Any) -> Tuple[int, bool]:
    current_seq = _sequence(current, 0)
    if stored is None:
        return current_seq, False
    stored_seq = _sequence(stored, 0)
    if stored_seq == _LEGACY_CURSOR_CLAMP and current_seq > _LEGACY_CURSOR_CLAMP:
        return current_seq, True
    return stored_seq, False


def _walk_values(value: Any, *, depth: int = 0) -> Iterable[Tuple[str, Any]]:
    if depth > 5:
        return
    if isinstance(value, dict):
        for key, child in value.items():
            yield _text(key), child
            yield from _walk_values(child, depth=depth + 1)
    elif isinstance(value, list):
        for child in value[:100]:
            yield "", child
            yield from _walk_values(child, depth=depth + 1)


def _path_value(payload: Dict[str, Any], path: str) -> Any:
    token = _text(path)
    if not token:
        return None
    current: Any = payload
    for part in token.split("."):
        if not isinstance(current, dict):
            return None
        if part in current:
            current = current[part]
            continue
        lowered = part.lower()
        matched = next((key for key in current if _text(key).lower() == lowered), None)
        if matched is None:
            return None
        current = current[matched]
    return current


def _event_refs(event: Dict[str, Any]) -> set[str]:
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    refs: set[str] = set()
    id_keys = {
        "entity_id",
        "device_id",
        "deviceid",
        "id",
        "ref",
        "resource_ref",
        "device_ref",
        "camera",
        "camera_id",
        "cameraid",
        "sensor",
        "sensor_id",
        "sensorid",
        "thermostat_id",
    }
    for key, value in _walk_values(payload):
        if _token(key) not in id_keys or isinstance(value, (dict, list)):
            continue
        text = _text(value).lower()
        if text:
            refs.add(text)
    resource_type = _text(payload.get("resource_type")).lower()
    resource_id = _text(payload.get("id")).lower()
    if resource_type and resource_id:
        refs.add(f"{resource_type}:{resource_id}")
    return refs


def _token_variants(value: Any) -> set[str]:
    text = _text(value).lower()
    if not text:
        return set()
    variants = {text}
    for delimiter in (".", ":", "/"):
        if delimiter in text:
            variants.add(text.rsplit(delimiter, 1)[-1])
    variants.add(re.sub(r"[^a-z0-9]+", "", text))
    return {item for item in variants if item}


def _device_tokens(device: Dict[str, Any]) -> set[str]:
    values: List[Any] = [
        device.get("id"),
        device.get("ref"),
        device.get("name"),
    ]
    details = device.get("details") if isinstance(device.get("details"), dict) else {}
    values.extend(
        details.get(key)
        for key in ("id", "device_id", "entity_id", "resource_id", "camera_id", "sensor_id", "serial", "mac")
    )
    for source in device.get("event_sources") or []:
        if isinstance(source, dict):
            values.extend([source.get("id"), source.get("ref"), source.get("resource_ref")])
    out: set[str] = set()
    for value in values:
        out.update(_token_variants(value))
    return out


def _matching_devices(event: Dict[str, Any], registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    provider = _text(event.get("provider")).lower()
    refs = _event_refs(event)
    ref_variants: set[str] = set()
    for ref in refs:
        ref_variants.update(_token_variants(ref))
    matches: List[Dict[str, Any]] = []
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        device_provider = _text(device.get("integration_id")).lower()
        if provider and device_provider and provider != device_provider:
            continue
        if ref_variants.intersection(_device_tokens(device)):
            matches.append(device)
    return matches


def _event_source_types(event: Dict[str, Any], devices: Sequence[Dict[str, Any]]) -> set[str]:
    event_variants: set[str] = set()
    for ref in _event_refs(event):
        event_variants.update(_token_variants(ref))
    source_types: set[str] = set()
    for device in devices:
        for source in device.get("event_sources") or []:
            if not isinstance(source, dict):
                continue
            source_variants: set[str] = set()
            for ref_key in ("ref", "resource_ref", "id"):
                source_variants.update(_token_variants(source.get(ref_key)))
            if event_variants.intersection(source_variants):
                source_type = _token(source.get("type"))
                if source_type:
                    source_types.add(source_type)
    return source_types


def _event_signal_haystack(event: Dict[str, Any], devices: Sequence[Dict[str, Any]], state: str) -> str:
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    signal_keys = {
        "type",
        "event_type",
        "eventtype",
        "detection_type",
        "detectiontype",
        "smart_detect_types",
        "smartdetecttypes",
        "device_class",
        "deviceclass",
        "resource_type",
        "resourcetype",
        "action",
        "state",
        "status",
    }
    signals = [_text(event.get("kind")), state, *_event_source_types(event, devices)]
    for key, value in _walk_values(payload):
        if _token(key) not in signal_keys:
            continue
        if isinstance(value, (dict, list)):
            signals.append(json.dumps(value, default=str))
        else:
            signals.append(_text(value))
    return " ".join(item.lower().replace("-", "_") for item in signals if _text(item))


def _heuristic_categories(event: Dict[str, Any]) -> set[str]:
    provider = _text(event.get("provider")).lower()
    kind = _text(event.get("kind")).lower()
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    entity_id = _text(payload.get("entity_id")).lower()
    resource_type = _token(payload.get("resource_type"))
    haystack = f"{provider} {kind} {entity_id} {resource_type} {json.dumps(payload, default=str)[:12000]}".lower()
    categories: set[str] = set()
    domain = entity_id.split(".", 1)[0] if "." in entity_id else resource_type
    domain_map = {
        "light": "light",
        "switch": "switch",
        "fan": "fan",
        "lock": "lock",
        "cover": "cover",
        "climate": "climate",
        "camera": "camera",
        "media_player": "media_player",
        "sensor": "sensor",
        "binary_sensor": "sensor",
    }
    if domain in domain_map:
        categories.add(domain_map[domain])
    if any(word in haystack for word in ("camera", "smartdetect", "doorbell", "ring_event")):
        categories.add("camera")
    if "motion" in haystack:
        categories.add("motion")
    if any(word in haystack for word in ("door", "window", "contact", "sensoropen", "sensorclosed")):
        categories.add("entry_sensor")
    if any(word in haystack for word in ("client_connected", "client_disconnected", "network_snapshot")):
        categories.update({"presence", "network_device"})
    if "thermostat" in haystack:
        categories.update({"climate", "temperature"})
    return categories


def _event_states(event: Dict[str, Any]) -> Tuple[str, str]:
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    new_state = payload.get("new_state") if isinstance(payload.get("new_state"), dict) else {}
    old_state = payload.get("old_state") if isinstance(payload.get("old_state"), dict) else {}
    resource = payload.get("resource") if isinstance(payload.get("resource"), dict) else {}
    previous = payload.get("previous") if isinstance(payload.get("previous"), dict) else {}
    state = _text(
        new_state.get("state")
        or payload.get("state")
        or payload.get("status")
        or resource.get("state")
    ).lower()
    old = _text(
        old_state.get("state")
        or payload.get("old_state_value")
        or payload.get("previous_state")
        or previous.get("state")
    ).lower()
    kind = _text(event.get("kind")).lower()
    if not state:
        if any(word in kind for word in ("connected", "seen")):
            state = "connected"
        elif any(word in kind for word in ("disconnected", "missing")):
            state = "disconnected"
    return state, old


def _event_is_terminal(event: Dict[str, Any]) -> bool:
    if _text(event.get("provider")).lower() != "unifi_protect":
        return False
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    action = _token(payload.get("__ws_action") or payload.get("action"))
    if action in {"update", "remove", "delete", "deleted"}:
        return True
    for key in (
        "end",
        "endTime",
        "end_time",
        "endedAt",
        "ended_at",
        "stop",
        "stoppedAt",
        "completedAt",
    ):
        value = payload.get(key)
        if value not in (None, "", 0, False):
            return True
    for key in ("state", "status", "eventState", "event_state", "lifecycle", "stage"):
        if _token(payload.get(key)) in {
            "idle",
            "end",
            "ended",
            "complete",
            "completed",
            "done",
            "closed",
            "inactive",
            "stop",
            "stopped",
            "finished",
            "off",
        }:
            return True
    return False


def _event_match(rule: Dict[str, Any], event: Dict[str, Any], registry: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    devices = _matching_devices(event, registry)
    categories = set(_heuristic_categories(event))
    for device in devices:
        categories.update(_device_categories(device))
    category = _token(rule.get("trigger_category"))
    if category not in categories:
        return False, {}

    selected_provider, selected_id = _decode_device(rule.get("trigger_device"))
    if selected_id:
        selected_variants = _token_variants(selected_id)
        devices = [
            device
            for device in devices
            if (not selected_provider or _text(device.get("integration_id")).lower() == selected_provider.lower())
            and selected_variants.intersection(_device_tokens(device))
        ]
        if not devices:
            return False, {}
    else:
        integration_provider = _integration_provider(rule.get("trigger_integration"))
        if integration_provider:
            devices = [
                device
                for device in devices
                if _token(device.get("integration_id")) == integration_provider
            ]
            if not devices:
                return False, {}

    room = _token(rule.get("trigger_room"))
    if room and not any(_device_room(device) == room for device in devices):
        return False, {}

    state, old_state = _event_states(event)
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    kind = _text(event.get("kind")).lower()
    haystack = f"{kind} {state} {json.dumps(payload, default=str)[:20000]}".lower()
    signal_haystack = _event_signal_haystack(event, devices, state)
    trigger = _token(rule.get("trigger_event"))
    expected = _text(rule.get("trigger_value")).lower()
    attribute = _text(rule.get("trigger_attribute"))
    attribute_value = _path_value(payload, attribute) if attribute else None
    compared_value = attribute_value if attribute_value is not None else state

    matched = False
    if trigger == "changed":
        matched = state != old_state if (state or old_state) else bool(kind)
    elif trigger == "turns_on":
        matched = state in _ON_STATES and old_state not in _ON_STATES
    elif trigger == "turns_off":
        matched = state in _OFF_STATES and old_state not in _OFF_STATES
    elif trigger == "opens":
        matched = state in {"open", "opened", "on"} and old_state not in {"open", "opened", "on"}
    elif trigger == "closes":
        matched = state in {"closed", "close", "off"} and old_state not in {"closed", "close", "off"}
    elif trigger == "connects":
        matched = "connected" in kind or state in {"connected", "online", "home", "present"}
    elif trigger == "disconnects":
        matched = "disconnected" in kind or "missing" in kind or state in {"disconnected", "offline", "away"}
    elif trigger == "doorbell":
        matched = not _event_is_terminal(event) and any(
            word in signal_haystack for word in ("doorbell", "ring", "pressed", "button_press")
        )
    elif trigger in {"motion", "person", "vehicle", "animal", "package", "face", "license_plate"}:
        needles = {trigger}
        if trigger == "license_plate":
            needles.update({"licenseplate", "plate"})
        matched = (
            not _event_is_terminal(event)
            and state not in _OFF_STATES
            and any(needle in signal_haystack for needle in needles)
        )
    elif trigger == "equals":
        matched = _text(compared_value).lower() == expected
    elif trigger == "contains":
        matched = bool(expected and expected in haystack)
    elif trigger in {"above", "below"}:
        actual = _float(compared_value)
        threshold = _float(expected)
        if actual is not None and threshold is not None:
            matched = actual > threshold if trigger == "above" else actual < threshold

    if not matched:
        return False, {}
    first_device = devices[0] if devices else {}
    return True, {
        "provider": _text(event.get("provider")),
        "event": kind,
        "state": state,
        "old_state": old_state,
        "value": _text(compared_value),
        "category": category,
        "device": _text(first_device.get("name")) or selected_id or category.replace("_", " "),
        "device_target": _encode_device(
            first_device.get("integration_id"),
            _device_id(first_device),
        ),
        "room": _text(first_device.get("room") or first_device.get("area")),
        "event_seq": _sequence(event.get("seq"), 0),
    }


def _render_template(value: Any, context: Dict[str, Any]) -> str:
    text = _text(value)
    for key in (
        "device",
        "room",
        "event",
        "state",
        "old_state",
        "value",
        "category",
        "provider",
        "vision",
    ):
        text = text.replace("{" + key + "}", _text(context.get(key)))
    return text


def _default_tts_template(trigger_event: Any) -> str:
    event = _token(trigger_event)
    return {
        "person": "A person was detected at {device}.",
        "vehicle": "A vehicle was detected at {device}.",
        "animal": "An animal was detected at {device}.",
        "package": "A package was detected at {device}.",
        "face": "A face was detected at {device}.",
        "license_plate": "A license plate was detected at {device}.",
        "motion": "Motion was detected at {device}.",
        "doorbell": "Someone pressed the doorbell at {device}.",
        "opens": "{device} opened.",
        "closes": "{device} closed.",
        "turns_on": "{device} turned on.",
        "turns_off": "{device} turned off.",
        "connects": "{device} connected.",
        "disconnects": "{device} disconnected.",
        "above": "{device} rose above the configured value.",
        "below": "{device} fell below the configured value.",
        "equals": "{device} reached the configured value.",
        "contains": "{device} reported the configured text.",
    }.get(event, "{device} changed.")


def _action_payload(rule: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    payload = _json_object(rule.get("action_payload_json"))
    action = _token(rule.get("action_operation"))
    value_text = _render_template(rule.get("action_value"), context)
    text_value = _render_template(rule.get("action_text"), context)
    mode = _render_template(rule.get("action_mode"), context)
    number = _float(value_text)
    if number is not None:
        if action == "set_brightness":
            payload.update({"brightness": number, "brightness_pct": number, "level": number, "percent": number})
        elif action == "set_position":
            payload.update({"position": number, "position_pct": number, "percent": number})
        elif action == "set_temperature":
            payload.update({"temperature": number, "target_temperature": number})
        elif action == "set_volume":
            payload.update({"volume": number, "volume_level": number, "percent": number})
        else:
            payload.setdefault("value", number)
    if mode:
        payload.setdefault("mode", mode)
        payload.setdefault("hvac_mode", mode)
    if text_value:
        payload.setdefault("text", text_value)
        payload.setdefault("message", text_value)
        payload.setdefault("url", text_value)
        payload.setdefault("media_url", text_value)
        payload.setdefault("uri", text_value)
    return payload


def _action_targets(rule: Dict[str, Any], registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    category = _token(rule.get("action_category"))
    integration_provider = _integration_provider(rule.get("action_integration"))
    room = _token(rule.get("action_room"))
    operation = _token(rule.get("action_operation"))
    selected = {_decode_device(value) for value in _list(rule.get("action_devices"))}
    targets: List[Dict[str, Any]] = []
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        provider = _text(device.get("integration_id"))
        device_id = _device_id(device)
        if category not in _device_categories(device) or operation not in _device_actions(device):
            continue
        if integration_provider and _token(provider) != integration_provider:
            continue
        if room and _device_room(device) != room:
            continue
        if _token(rule.get("action_scope")) == "devices" and (provider, device_id) not in selected:
            continue
        targets.append(device)
    return targets


async def _execute_device_action(rule: Dict[str, Any], context: Dict[str, Any], registry: Dict[str, Any]) -> Dict[str, Any]:
    targets = _action_targets(rule, registry)
    if not targets:
        raise ValueError("No currently connected devices support this automation action.")
    operation = _token(rule.get("action_operation"))
    payload = _action_payload(rule, context)
    succeeded = 0
    errors: List[str] = []
    for device in targets:
        provider = _text(device.get("integration_id"))
        device_id = _device_id(device)
        try:
            result = await asyncio.to_thread(
                run_integration_device_action,
                provider,
                operation,
                device_id,
                payload,
            )
            if isinstance(result, dict) and result.get("ok") is False:
                errors.append(_text(result.get("error") or result.get("message")) or f"{device_id} rejected the action")
            else:
                succeeded += 1
        except Exception as exc:
            errors.append(f"{_text(device.get('name')) or device_id}: {exc}")
    if succeeded <= 0:
        raise RuntimeError(errors[0] if errors else "The device action failed.")
    summary = f"{_ACTION_LABELS.get(operation, operation.replace('_', ' ').title())} sent to {succeeded} device"
    if succeeded != 1:
        summary += "s"
    return {"ok": True, "summary": summary + ".", "succeeded": succeeded, "errors": errors}


def _speech_settings() -> Dict[str, Any]:
    shared = get_speech_settings()
    return {
        "backend": _text(shared.get("announcement_tts_backend") or shared.get("tts_backend") or "wyoming"),
        "model": _text(shared.get("announcement_tts_model")),
        "voice": _text(shared.get("announcement_tts_voice")),
        "wyoming_host": _text(shared.get("wyoming_tts_host")),
        "wyoming_port": shared.get("wyoming_tts_port"),
        "wyoming_voice": _text(shared.get("wyoming_tts_voice")),
        "voice_core_backend": _text(shared.get("tts_backend")),
        "voice_core_model": _text(shared.get("tts_model")),
        "voice_core_voice": _text(shared.get("tts_voice")),
        "voice_core_wyoming_host": _text(shared.get("wyoming_tts_host")),
        "voice_core_wyoming_port": shared.get("wyoming_tts_port"),
        "voice_core_wyoming_voice": _text(shared.get("wyoming_tts_voice")),
    }


async def _execute_tts(rule: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    template = (
        rule.get("tts_text")
        if _token(rule.get("tts_mode")) == "custom" and _text(rule.get("tts_text"))
        else _default_tts_template(rule.get("trigger_event"))
    )
    message = _render_template(template, context)
    if not message:
        raise ValueError("The TTS message is empty.")
    settings = _speech_settings()
    ha = _homeassistant_config()
    result = await speak_announcement_targets(
        text=message,
        backend=settings["backend"],
        ha_base=ha["base"],
        token=ha["token"],
        targets=_list(rule.get("tts_targets")),
        model=settings["model"],
        voice=settings["voice"],
        wyoming_host=settings["wyoming_host"],
        wyoming_port=settings["wyoming_port"],
        wyoming_voice=settings["wyoming_voice"],
        voice_core_backend=settings["voice_core_backend"],
        voice_core_model=settings["voice_core_model"],
        voice_core_voice=settings["voice_core_voice"],
        voice_core_wyoming_host=settings["voice_core_wyoming_host"],
        voice_core_wyoming_port=settings["voice_core_wyoming_port"],
        voice_core_wyoming_voice=settings["voice_core_wyoming_voice"],
        default_backend=settings["backend"],
        audio_scene=_normalize_tts_audio_scene(rule.get("tts_audio_scene")),
    )
    if isinstance(result, dict) and result.get("ok") is False:
        raise RuntimeError(_text(result.get("error")) or "TTS delivery failed.")
    count = _int((result or {}).get("sent_count") if isinstance(result, dict) else 0, 0)
    scene_sent = _int((result or {}).get("audio_scene_sent_count") if isinstance(result, dict) else 0, 0)
    scene_fallback = _int((result or {}).get("audio_scene_fallback_count") if isinstance(result, dict) else 0, 0)
    summary = f'Spoke “{message[:120]}” to {count or len(_list(rule.get("tts_targets")))} target(s).'
    if scene_fallback:
        summary += f" {scene_fallback} target(s) played TTS without background audio."
    return {
        "ok": True,
        "summary": summary,
        "audio_scene_sent_count": scene_sent,
        "audio_scene_fallback_count": scene_fallback,
    }


async def _execute_notification(rule: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    title = _render_template(rule.get("notification_title"), context) or "Tater Automation"
    message = _render_template(rule.get("notification_message"), context)
    sent = 0
    errors: List[str] = []
    for encoded in _list(rule.get("notification_targets")):
        destination = _decode_notification_target(encoded)
        if not destination:
            continue
        try:
            result = await dispatch_notification(
                platform=destination["platform"],
                title=title,
                content=message,
                targets=destination["targets"],
                origin={"platform": "automation_core", "scope": _text(rule.get("id"))},
                meta={"priority": _text(rule.get("notification_priority") or "normal")},
            )
            result_text = _text(result)
            if not result_text or result_text.lower().startswith("queued notification"):
                sent += 1
            else:
                errors.append(result_text)
        except Exception as exc:
            errors.append(str(exc))
    if sent <= 0:
        raise RuntimeError(errors[0] if errors else "No notification destination accepted the message.")
    return {"ok": True, "summary": f"Notification sent to {sent} destination(s).", "errors": errors}


def _find_device(registry: Dict[str, Any], encoded: Any) -> Optional[Dict[str, Any]]:
    provider, device_id = _decode_device(encoded)
    variants = _token_variants(device_id)
    if not device_id:
        return None
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        if provider and _text(device.get("integration_id")).lower() != provider.lower():
            continue
        if variants.intersection(_device_tokens(device)):
            return device
    return None


def _camera_device_for_rule(
    rule: Dict[str, Any],
    context: Dict[str, Any],
    registry: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if _token(rule.get("camera_source")) == "selected":
        encoded = rule.get("camera_device")
    else:
        encoded = context.get("device_target") or rule.get("trigger_device") or rule.get("camera_device")
    device = _find_device(registry, encoded)
    if device and "camera" in _device_categories(device):
        return device
    return None


def _snapshot_result_bytes(result: Any) -> Tuple[bytes, str]:
    if isinstance(result, tuple) and len(result) >= 1:
        content = result[0]
        content_type = _text(result[1]) if len(result) > 1 else "image/jpeg"
    elif isinstance(result, dict):
        content = result.get("bytes") or result.get("content") or result.get("image")
        content_type = _text(result.get("content_type") or result.get("mimetype") or "image/jpeg")
        if not content and _text(result.get("base64")):
            content = base64.b64decode(_text(result.get("base64")))
    else:
        content = result
        content_type = "image/jpeg"
    if isinstance(content, str):
        try:
            content = base64.b64decode(content)
        except Exception:
            content = b""
    if not isinstance(content, (bytes, bytearray)) or not content:
        raise RuntimeError("The camera integration returned no snapshot image.")
    return bytes(content), content_type or "image/jpeg"


def _describe_snapshot_sync(image_bytes: bytes, content_type: str, prompt: str) -> str:
    settings = get_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    provider = _token(settings.get("provider") or "openai_compatible")
    model = _text(settings.get("model") or "qwen2.5-vl-7b-instruct")
    if provider in {"hf", "huggingface", "hugging_face", "hf_transformers"}:
        provider = "hf_transformers"
    elif provider in {"llama", "llamacpp", "llama_cpp", "llama_cpp_python"}:
        provider = "llama_cpp"
    elif provider in {"mlx", "mlx_lm", "apple_mlx"}:
        provider = "mlx_lm"
    if provider in {"hf_transformers", "llama_cpp", "mlx_lm"}:
        result = describe_image_with_local_llm(
            provider=provider,
            model=model,
            image_bytes=image_bytes,
            filename="tater-automation-camera.jpg",
            prompt=prompt,
            timeout=90.0,
        )
        description = _text((result or {}).get("description"))
        if not description:
            raise RuntimeError("The local vision model returned no description.")
        return description

    api_base = _text(settings.get("api_base") or "http://127.0.0.1:1234").rstrip("/")
    api_key = _text(settings.get("api_key"))
    b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:{content_type or 'image/jpeg'};base64,{b64}"
    body = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Describe only visible, relevant facts for a short home automation alert. "
                    "Do not invent identity, intent, or details that are not visible."
                ),
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ],
        "temperature": 0.2,
        "max_tokens": 160,
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    response = requests.post(
        f"{api_base}/v1/chat/completions",
        headers=headers,
        data=json.dumps(body),
        timeout=45,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Vision HTTP {response.status_code}: {response.text[:200]}")
    payload = response.json() or {}
    description = _text(((payload.get("choices") or [{}])[0].get("message") or {}).get("content"))
    if not description:
        raise RuntimeError("The vision model returned no description.")
    return description


async def _execute_camera_ai(rule: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    registry = _registry(redis_client)
    camera = _camera_device_for_rule(rule, context, registry)
    if not camera:
        raise ValueError(
            "Tater could not identify the triggering camera. Select a specific camera in this automation."
        )
    provider = _text(camera.get("integration_id"))
    device_id = _device_id(camera)
    snapshot_action = next(
        (action for action in ("camera_snapshot", "snapshot") if action in _device_actions(camera)),
        "",
    )
    if not snapshot_action:
        raise ValueError("The selected camera integration does not expose snapshots.")
    snapshot_result = await asyncio.to_thread(
        run_integration_device_action,
        provider,
        snapshot_action,
        device_id,
        {},
    )
    image_bytes, content_type = _snapshot_result_bytes(snapshot_result)
    next_context = dict(context)
    next_context["device"] = _text(camera.get("name")) or context.get("device") or "camera"
    next_context["device_target"] = _encode_device(provider, device_id)
    prompt = _render_template(rule.get("vision_prompt"), next_context)
    vision_error = ""
    try:
        description = await asyncio.to_thread(
            _describe_snapshot_sync,
            image_bytes,
            content_type,
            prompt,
        )
    except Exception as exc:
        vision_error = str(exc)
        logger.warning("[automation] camera vision failed for %s: %s", device_id, exc)
        description = _render_template(rule.get("vision_fallback"), next_context) or "Camera activity was detected."
    next_context["vision"] = description
    results: List[str] = []
    errors: List[str] = []
    tts_targets = _list(rule.get("camera_tts_targets"))
    if tts_targets:
        try:
            tts_result = await _execute_tts(
                {
                    "tts_text": _text(rule.get("camera_tts_text") or "{vision}"),
                    "tts_targets": tts_targets,
                },
                next_context,
            )
            results.append(_text(tts_result.get("summary")))
        except Exception as exc:
            errors.append(str(exc))
    notification_targets = _list(rule.get("camera_notification_targets"))
    if notification_targets:
        try:
            notification_result = await _execute_notification(
                {
                    "id": rule.get("id"),
                    "notification_title": rule.get("camera_notification_title"),
                    "notification_message": rule.get("camera_notification_message") or "{vision}",
                    "notification_targets": notification_targets,
                    "notification_priority": rule.get("camera_notification_priority"),
                },
                next_context,
            )
            results.append(_text(notification_result.get("summary")))
        except Exception as exc:
            errors.append(str(exc))
    if not results:
        raise RuntimeError(errors[0] if errors else "No camera announcement destination was selected.")
    return {
        "ok": True,
        "summary": " ".join(item for item in results if item),
        "vision": description,
        "vision_warning": vision_error,
        "errors": errors,
    }


async def _execute_rule(rule: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    action_type = _token(rule.get("action_type"))
    if action_type == "tts":
        return await _execute_tts(rule, context)
    if action_type == "notification":
        return await _execute_notification(rule, context)
    if action_type == "camera_ai":
        return await _execute_camera_ai(rule, context)
    return await _execute_device_action(rule, context, _registry(redis_client))


def _enqueue(client: Any, rule: Dict[str, Any], context: Dict[str, Any], reason: str) -> None:
    client.lpush(
        _QUEUE_KEY,
        json.dumps(
            {
                "rule_id": rule["id"],
                "context": context,
                "reason": reason,
                "queued_at": time.time(),
            },
            separators=(",", ":"),
            default=str,
        ),
    )
    _runtime_set(client, queue_depth=_int(client.llen(_QUEUE_KEY), 0))


def _dequeue(client: Any) -> Optional[Dict[str, Any]]:
    raw = client.rpop(_QUEUE_KEY)
    return _json_record(raw) if raw else None


def _acquire_cooldown(client: Any, rule: Dict[str, Any]) -> bool:
    seconds = _int(rule.get("cooldown_seconds"), 30, minimum=0, maximum=86400)
    if seconds <= 0:
        return True
    key = f"automation:cooldown:{_text(rule.get('id'))}"
    return client.set(key, "1", ex=max(1, seconds), nx=True) is not None


async def _process_event(client: Any, event: Dict[str, Any]) -> int:
    registry = _registry(client)
    matched_count = 0
    for rule in _load_rules(client).values():
        if not _bool(rule.get("enabled"), True):
            continue
        matched, context = _event_match(rule, event, registry)
        if not matched or not _acquire_cooldown(client, rule):
            continue
        _enqueue(client, rule, context, "trigger")
        matched_count += 1
    return matched_count


async def _event_loop(stop_event: Optional[object]) -> None:
    stored = redis_client.get(_CURSOR_KEY)
    current = redis_client.get(_INTEGRATION_EVENT_SEQ_KEY)
    last_seq, recovered = _resolve_event_cursor(stored, current)
    if stored is None or recovered:
        redis_client.set(_CURSOR_KEY, str(last_seq))
    if recovered:
        logger.warning(
            "[automation] recovered capped event cursor %s at live sequence %s",
            _LEGACY_CURSOR_CLAMP,
            last_seq,
        )
        _runtime_set(
            redis_client,
            last_event_seq=last_seq,
            cursor_recovered_from=_LEGACY_CURSOR_CLAMP,
        )
    while not (stop_event and stop_event.is_set()):
        events = _integration_events(redis_client, last_seq)
        if not events:
            await asyncio.sleep(0.25)
            continue
        for event in events:
            seq = _sequence(event.get("seq"), last_seq)
            try:
                await _process_event(redis_client, event)
            except Exception as exc:
                logger.warning("[automation] event %s failed: %s", seq, exc)
                _runtime_set(redis_client, last_error=str(exc))
            finally:
                last_seq = max(last_seq, seq)
                redis_client.set(_CURSOR_KEY, str(last_seq))
                _runtime_set(redis_client, last_event_seq=last_seq, last_event_ts=event.get("ts") or time.time())
        await asyncio.sleep(0)


async def _worker_loop(stop_event: Optional[object], worker_id: int) -> None:
    while not (stop_event and stop_event.is_set()):
        job = _dequeue(redis_client)
        if not job:
            await asyncio.sleep(0.2)
            continue
        rule = _get_rule(redis_client, job.get("rule_id"))
        if not rule:
            continue
        reason = _text(job.get("reason") or "trigger")
        context = job.get("context") if isinstance(job.get("context"), dict) else {}
        started = time.time()
        status = "ok"
        summary = ""
        error = ""
        try:
            result = await _execute_rule(rule, context)
            summary = _text(result.get("summary")) if isinstance(result, dict) else "Automation completed."
        except Exception as exc:
            status = "error"
            error = str(exc)
            logger.warning("[automation] rule %s failed: %s", rule["id"], exc)
        rule["last_run_ts"] = started
        rule["last_status"] = status
        rule["last_summary"] = summary
        rule["last_error"] = error
        rule["run_count"] = _int(rule.get("run_count"), 0) + 1
        if error:
            rule["error_count"] = _int(rule.get("error_count"), 0) + 1
        rule["updated_at"] = time.time()
        _save_rule(redis_client, rule)
        history_row = {
            "id": str(uuid.uuid4()),
            "rule_id": rule["id"],
            "rule_name": rule["name"],
            "status": status,
            "summary": summary,
            "error": error,
            "reason": reason,
            "context": context,
            "started_at": started,
            "duration_ms": round((time.time() - started) * 1000, 1),
            "worker": worker_id,
        }
        _append_history(redis_client, history_row)
        _runtime_set(
            redis_client,
            queue_depth=_int(redis_client.llen(_QUEUE_KEY), 0),
            last_run_ts=started,
            last_status=status,
            last_summary=summary,
            last_error=error,
        )


async def _main(stop_event: Optional[object]) -> None:
    _runtime_set(
        redis_client,
        started_at=time.time(),
        running=True,
        worker_count=_WORKER_COUNT,
        queue_depth=_int(redis_client.llen(_QUEUE_KEY), 0),
        last_error="",
    )
    tasks = [asyncio.create_task(_event_loop(stop_event))]
    tasks.extend(asyncio.create_task(_worker_loop(stop_event, index + 1)) for index in range(_WORKER_COUNT))
    logger.info("[automation] core started v%s with %d workers", __version__, _WORKER_COUNT)
    try:
        while not (stop_event and stop_event.is_set()):
            await asyncio.sleep(0.5)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        _runtime_set(redis_client, running=False)
        logger.info("[automation] core stopped")


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def _value(values: Dict[str, Any], payload: Dict[str, Any], key: str, default: Any = "") -> Any:
    value = values[key] if key in values else payload.get(key, default)
    if isinstance(value, dict):
        for inner in ("value", "id", "key", "target"):
            if inner in value:
                return value[inner]
    return value


def _tts_audio_scene_from_form(
    values: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    existing: Any = None,
) -> Dict[str, Any]:
    current = _normalize_tts_audio_scene(existing)
    current_background = (
        current.get("background") if isinstance(current.get("background"), dict) else {}
    )
    current_ducking = current.get("ducking") if isinstance(current.get("ducking"), dict) else {}
    current_finish = current.get("finish") if isinstance(current.get("finish"), dict) else {}
    audio_enabled = _bool(
        _value(values, payload, "tts_audio_enabled", bool(current_background.get("url"))),
        bool(current_background.get("url")),
    )
    if not audio_enabled:
        return {}

    existing_url = _text(
        _value(values, payload, "tts_background_audio_existing_url", current_background.get("url"))
    )
    custom_url = _text(_value(values, payload, "tts_background_audio_url"))
    uploaded_audio = _value(values, payload, "tts_background_audio_upload")
    source = _text(_value(values, payload, "tts_background_audio_source")).lower()
    if not source:
        if isinstance(uploaded_audio, dict) and uploaded_audio.get("data_b64"):
            source = "upload"
        elif custom_url:
            source = "custom"
        elif existing_url:
            source = _background_audio_source_from_url(existing_url)
        else:
            source = "preset:morning_glow"

    if source.startswith("preset:"):
        background_url = _background_audio_preset_url(source.split(":", 1)[1])
    elif source == "upload":
        if isinstance(uploaded_audio, dict) and uploaded_audio.get("data_b64"):
            background_url = _store_background_audio_upload(uploaded_audio)
        elif existing_url and "/api/ai-tasks/background-audio/uploads/" in existing_url:
            background_url = existing_url
        else:
            raise ValueError("Choose a background audio file to upload.")
    elif source == "custom":
        background_url = custom_url
        if not background_url and existing_url and _background_audio_source_from_url(existing_url) == "custom":
            background_url = existing_url
        if not background_url.lower().startswith(("http://", "https://")):
            raise ValueError("Background Audio URL must start with http:// or https://.")
    else:
        raise ValueError("Choose a valid background audio source.")

    return _normalize_tts_audio_scene(
        {
            "background": {
                "url": background_url,
                "loop": _bool(
                    _value(values, payload, "tts_background_loop", current_background.get("loop", True)),
                    True,
                ),
                "volume_percent": _int(
                    _value(
                        values,
                        payload,
                        "tts_background_volume_percent",
                        current_background.get("volume_percent"),
                    ),
                    60,
                    maximum=100,
                ),
            },
            "ducking": {
                "target_percent": _int(
                    _value(
                        values,
                        payload,
                        "tts_ducking_target_percent",
                        current_ducking.get("target_percent"),
                    ),
                    35,
                    maximum=100,
                ),
                "attack_ms": _int(
                    _value(values, payload, "tts_ducking_attack_ms", current_ducking.get("attack_ms")),
                    150,
                    maximum=10000,
                ),
                "release_ms": _int(
                    _value(
                        values,
                        payload,
                        "tts_ducking_release_ms",
                        current_ducking.get("release_ms"),
                    ),
                    350,
                    maximum=10000,
                ),
            },
            "finish": {
                "fade_ms": _int(
                    _value(values, payload, "tts_background_fade_ms", current_finish.get("fade_ms")),
                    500,
                    maximum=10000,
                ),
            },
        }
    )


def _rule_from_form(
    values: Dict[str, Any],
    payload: Dict[str, Any],
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    previous = existing if isinstance(existing, dict) else {}
    now = time.time()
    fields = (
        "preset",
        "name",
        "enabled",
        "trigger_category",
        "trigger_integration",
        "trigger_device",
        "trigger_room",
        "trigger_event",
        "trigger_attribute",
        "trigger_value",
        "cooldown_seconds",
        "action_type",
        "action_category",
        "action_integration",
        "action_scope",
        "action_devices",
        "action_room",
        "action_operation",
        "action_value",
        "action_mode",
        "action_text",
        "action_payload_json",
        "tts_mode",
        "tts_text",
        "tts_targets",
        "tts_audio_scene",
        "notification_title",
        "notification_message",
        "notification_targets",
        "notification_priority",
        "camera_source",
        "camera_device",
        "vision_prompt",
        "vision_fallback",
        "camera_tts_text",
        "camera_tts_targets",
        "camera_notification_title",
        "camera_notification_message",
        "camera_notification_targets",
        "camera_notification_priority",
    )
    rule = dict(previous)
    for field in fields:
        rule[field] = _value(values, payload, field, previous.get(field, ""))
    audio_form_keys = {
        "tts_audio_enabled",
        "tts_background_audio_source",
        "tts_background_audio_upload",
        "tts_background_audio_url",
        "tts_background_loop",
    }
    if audio_form_keys.intersection(values) or audio_form_keys.intersection(payload):
        rule["tts_audio_scene"] = _tts_audio_scene_from_form(
            values,
            payload,
            existing=previous.get("tts_audio_scene"),
        )
    rule.update(
        {
            "id": _text(previous.get("id")) or str(uuid.uuid4()),
            "created_at": _float(previous.get("created_at")) or now,
            "updated_at": now,
            "last_run_ts": _float(previous.get("last_run_ts")) or 0.0,
            "last_status": _text(previous.get("last_status")),
            "last_summary": _text(previous.get("last_summary")),
            "last_error": _text(previous.get("last_error")),
            "run_count": _int(previous.get("run_count"), 0),
            "error_count": _int(previous.get("error_count"), 0),
        }
    )
    normalized = _normalize_rule(rule)
    if normalized:
        return normalized
    action_type = _token(rule.get("action_type") or "device")
    if not _token(rule.get("trigger_category")):
        raise ValueError("Choose a trigger category.")
    if action_type == "device":
        if not _token(rule.get("action_category")):
            raise ValueError("Choose the category Tater should control.")
        if not _token(rule.get("action_operation")):
            raise ValueError("Choose a device action.")
        if _token(rule.get("action_scope")) == "devices" and not _list(rule.get("action_devices")):
            raise ValueError("Choose at least one action device.")
    elif action_type == "tts":
        if not _list(rule.get("tts_targets")):
            raise ValueError("Choose at least one announcement target.")
        if _token(rule.get("tts_mode")) == "custom" and not _text(rule.get("tts_text")):
            raise ValueError("Enter the words Tater should speak.")
    elif action_type == "notification":
        if not _text(rule.get("notification_message")):
            raise ValueError("Enter a notification message.")
        if not _list(rule.get("notification_targets")):
            raise ValueError("Choose at least one notification destination.")
    elif action_type == "camera_ai":
        if _token(rule.get("camera_source")) == "selected" and not _text(rule.get("camera_device")):
            raise ValueError("Choose the camera this automation should describe.")
        if not _list(rule.get("camera_tts_targets")) and not _list(rule.get("camera_notification_targets")):
            raise ValueError("Choose at least one announcement or notification destination.")
    raise ValueError("Complete the required automation fields.")


def _announcement_audio_fields(rule: Dict[str, Any], show_tts: Dict[str, Any]) -> List[Dict[str, Any]]:
    scene = _normalize_tts_audio_scene(rule.get("tts_audio_scene"))
    background = scene.get("background") if isinstance(scene.get("background"), dict) else {}
    ducking = scene.get("ducking") if isinstance(scene.get("ducking"), dict) else {}
    finish = scene.get("finish") if isinstance(scene.get("finish"), dict) else {}
    background_url = _text(background.get("url"))
    source = _background_audio_source_from_url(background_url) if background_url else "preset:morning_glow"
    show_for_audio = [show_tts, {"source_key": "tts_audio_enabled", "equals": "enabled"}]
    source_options = [
        {
            "value": f"preset:{preset['id']}",
            "label": preset["label"],
            "description": preset["description"],
            "icon": "♪",
        }
        for preset in _BACKGROUND_AUDIO_PRESETS
    ]
    source_options.extend(
        [
            {
                "value": "upload",
                "label": "Upload Audio",
                "description": "Use your own WAV, MP3, or FLAC file.",
                "icon": "↑",
            },
            {
                "value": "custom",
                "label": "Audio URL",
                "description": "Use a stable HTTP(S) audio URL.",
                "icon": "⌁",
            },
        ]
    )
    return [
        {
            "key": "tts_audio_enabled",
            "label": "Background Audio",
            "type": "select",
            "presentation": "cards",
            "options": [
                {
                    "value": "disabled",
                    "label": "No Background Audio",
                    "description": "Play the announcement by itself.",
                    "icon": "○",
                },
                {
                    "value": "enabled",
                    "label": "Play Background Audio",
                    "description": "Mix a looping audio bed underneath TTS on compatible Tater satellites.",
                    "icon": "♪",
                },
            ],
            "value": "enabled" if background_url else "disabled",
            "show_when": show_tts,
            "full_width": True,
        },
        {
            "key": "tts_background_audio_source",
            "label": "Background Track",
            "type": "select",
            "presentation": "cards",
            "options": source_options,
            "value": source,
            "show_when_all": show_for_audio,
            "full_width": True,
        },
        {
            "key": "tts_background_audio_upload",
            "label": "Upload Background Audio",
            "type": "file",
            "accept": ".wav,.mp3,.flac,audio/wav,audio/mpeg,audio/flac",
            "file_encoding": "base64",
            "max_bytes": _BACKGROUND_AUDIO_MAX_UPLOAD_BYTES,
            "description": "WAV, MP3, or FLAC up to 16 MB. Tater stores it in Agent Lab.",
            "value": "",
            "show_when_all": [
                *show_for_audio,
                {"source_key": "tts_background_audio_source", "equals": "upload"},
            ],
            "full_width": True,
        },
        {
            "key": "tts_background_audio_existing_url",
            "label": "Existing Background Audio URL",
            "type": "hidden",
            "value": background_url,
        },
        {
            "key": "tts_background_audio_url",
            "label": "Background Audio URL",
            "type": "text",
            "description": "A stable HTTP(S) URL for WAV, MP3, or FLAC audio.",
            "value": background_url if source == "custom" else "",
            "show_when_all": [
                *show_for_audio,
                {"source_key": "tts_background_audio_source", "equals": "custom"},
            ],
            "full_width": True,
        },
        {
            "key": "tts_background_volume_percent",
            "label": "Background Volume (%)",
            "type": "number",
            "min": 0,
            "max": 100,
            "value": _int(background.get("volume_percent"), 60, maximum=100),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_ducking_target_percent",
            "label": "Volume During Speech (%)",
            "type": "number",
            "min": 0,
            "max": 100,
            "description": "Percentage of the background volume retained while Tater is speaking.",
            "value": _int(ducking.get("target_percent"), 35, maximum=100),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_ducking_attack_ms",
            "label": "Duck Attack (ms)",
            "type": "number",
            "min": 0,
            "max": 10000,
            "value": _int(ducking.get("attack_ms"), 150, maximum=10000),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_ducking_release_ms",
            "label": "Duck Release (ms)",
            "type": "number",
            "min": 0,
            "max": 10000,
            "value": _int(ducking.get("release_ms"), 350, maximum=10000),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_background_fade_ms",
            "label": "Final Fade-Out (ms)",
            "type": "number",
            "min": 0,
            "max": 10000,
            "value": _int(finish.get("fade_ms"), 500, maximum=10000),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_background_loop",
            "label": "Track Playback",
            "type": "select",
            "presentation": "cards",
            "options": [
                {
                    "value": "enabled",
                    "label": "Loop Until Finished",
                    "description": "Repeat the track until the announcement ends.",
                    "icon": "↻",
                },
                {
                    "value": "disabled",
                    "label": "Play Once",
                    "description": "Do not restart the track if it ends first.",
                    "icon": "▶",
                },
            ],
            "value": "enabled" if _bool(background.get("loop"), True) else "disabled",
            "show_when_all": show_for_audio,
            "full_width": True,
        },
    ]


def _editor_fields(
    rule: Dict[str, Any],
    registry: Dict[str, Any],
    client: Any,
    *,
    announcement_catalog: Optional[List[Dict[str, Any]]] = None,
    notification_catalog: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    trigger_category = _token(rule.get("trigger_category"))
    action_category = _token(rule.get("action_category"))
    trigger_integration = _text(rule.get("trigger_integration")) or _integration_from_devices(
        trigger_category,
        rule.get("trigger_device"),
    )
    trigger_integration_options, trigger_integration_dependency = _integration_dependency(
        registry,
        current_category=trigger_category,
        source_key="trigger_category",
        triggerable_only=True,
    )
    if not trigger_integration and trigger_integration_options:
        trigger_integration = _text(trigger_integration_options[0].get("value"))
    trigger_device_options, trigger_device_dependency = _device_dependency(
        registry,
        current_integration=trigger_integration,
        source_key="trigger_integration",
        current_values=rule.get("trigger_device"),
        multiple=False,
        allow_any=bool(rule.get("id") and not _text(rule.get("trigger_device"))),
        triggerable_only=True,
    )
    current_trigger_device = _text(rule.get("trigger_device"))
    if not current_trigger_device and trigger_device_options:
        current_trigger_device = _text(trigger_device_options[0].get("value"))
    trigger_event_options, trigger_event_dependency = _trigger_event_dependency(
        registry,
        current_device=current_trigger_device,
        current_event=rule.get("trigger_event"),
    )
    action_integration = _text(rule.get("action_integration")) or _integration_from_devices(
        action_category,
        rule.get("action_devices"),
    )
    action_integration_options, action_integration_dependency = _integration_dependency(
        registry,
        current_category=action_category,
        source_key="action_category",
        actionable_only=True,
    )
    if not action_integration and action_integration_options:
        action_integration = _text(action_integration_options[0].get("value"))
    action_device_options, action_device_dependency = _device_dependency(
        registry,
        current_integration=action_integration,
        source_key="action_integration",
        current_values=rule.get("action_devices"),
        multiple=True,
        actionable_only=True,
    )
    action_options, action_dependency = _action_dependency(
        registry,
        current_integration=action_integration,
        current_action=_token(rule.get("action_operation")),
    )
    announcement_options = (
        [dict(row) for row in announcement_catalog if isinstance(row, dict)]
        if announcement_catalog is not None
        else _announcement_options(
            [*_list(rule.get("tts_targets")), *_list(rule.get("camera_tts_targets"))]
        )
    )
    notification_options = (
        [dict(row) for row in notification_catalog if isinstance(row, dict)]
        if notification_catalog is not None
        else [
            {
                **row,
                "description": _text(row.get("description")) or "Notification destination",
                "icon": _text(row.get("icon")) or "◉",
            }
            for row in _notification_options(
                client,
                [*_list(rule.get("notification_targets")), *_list(rule.get("camera_notification_targets"))],
            )
        ]
    )
    show_device_action = {"source_key": "action_type", "equals": "device"}
    show_tts = {"source_key": "action_type", "equals": "tts"}
    show_notification = {"source_key": "action_type", "equals": "notification"}
    show_camera_ai = {"source_key": "action_type", "equals": "camera_ai"}
    show_trigger_value = {"source_key": "trigger_event", "any_of": ["equals", "contains", "above", "below"]}
    show_numeric_action = {
        "source_key": "action_operation",
        "any_of": ["set_brightness", "set_position", "set_temperature", "set_volume"],
    }
    show_action_text = {
        "source_key": "action_operation",
        "any_of": ["set_color", "play_media", "play_url", "announce"],
    }
    def tts_mode_options(event: Any) -> List[Dict[str, Any]]:
        return [
            {
                "value": "default",
                "label": "Use Tater's Default",
                "description": _default_tts_template(event),
                "icon": "✦",
            },
            {
                "value": "custom",
                "label": "Write My Own",
                "description": "Type exactly what the selected speakers should say.",
                "icon": "✎",
            },
        ]

    tts_mode_options_by_event = {
        row["value"]: tts_mode_options(row["value"])
        for row in _EVENT_OPTIONS
    }
    return [
        {
            "type": "heading",
            "label": "1. Trigger",
            "description": "Choose the device and the exact event that starts this automation.",
        },
        {
            "key": "trigger_category",
            "label": "Device Category",
            "type": "select",
            "presentation": "cards",
            "options": _category_options(registry, triggerable_only=True),
            "value": trigger_category,
            "full_width": True,
        },
        {
            "key": "trigger_integration",
            "label": "Integration",
            "type": "select",
            "presentation": "cards",
            "options": trigger_integration_options,
            "dependent_options": trigger_integration_dependency,
            "value": trigger_integration,
            "description": "Choose the enabled integration that reports this trigger.",
            "full_width": True,
        },
        {
            "key": "trigger_device",
            "label": "Which Device?",
            "type": "select",
            "presentation": "cards",
            "options": trigger_device_options,
            "dependent_options": trigger_device_dependency,
            "value": current_trigger_device,
            "description": "Only compatible devices from the selected integration are shown.",
            "full_width": True,
        },
        {
            "key": "trigger_event",
            "label": "What Should Happen?",
            "type": "select",
            "presentation": "cards",
            "options": trigger_event_options,
            "dependent_options": trigger_event_dependency,
            "value": _token(rule.get("trigger_event") or "changed"),
            "description": "Only events explicitly reported by the selected device's integration are shown.",
            "full_width": True,
        },
        {
            "key": "trigger_attribute",
            "label": "Value To Watch",
            "type": "text",
            "placeholder": "new_state.attributes.temperature",
            "value": _text(rule.get("trigger_attribute")),
            "description": "Optional nested value for equals, contains, above, or below.",
            "show_when": show_trigger_value,
        },
        {
            "key": "trigger_value",
            "label": "Comparison Value",
            "type": "text",
            "value": _text(rule.get("trigger_value")),
            "show_when": show_trigger_value,
        },
        {
            "type": "heading",
            "label": "2. Action",
            "description": "Choose what Tater should do when the trigger occurs.",
        },
        {
            "key": "action_type",
            "label": "What Should Tater Do?",
            "type": "select",
            "presentation": "cards",
            "options": [
                {
                    "value": "tts",
                    "label": "Announcement",
                    "description": "Speak on a Sonos, media player, or Tater satellite.",
                    "icon": "♪",
                },
                {
                    "value": "device",
                    "label": "Control a Device",
                    "description": "Turn something on, change a setting, or run a scene.",
                    "icon": "⏻",
                },
                {
                    "value": "notification",
                    "label": "Send a Notification",
                    "description": "Send a message through a configured notification integration.",
                    "icon": "◉",
                },
                {
                    "value": "camera_ai",
                    "label": "Describe a Camera",
                    "description": "Capture a camera image, describe it, then announce or notify.",
                    "icon": "◎",
                },
            ],
            "value": _token(rule.get("action_type") or "tts"),
            "full_width": True,
        },
        {
            "key": "tts_targets",
            "label": "Where Should It Play?",
            "type": "multiselect",
            "presentation": "cards",
            "options": announcement_options,
            "value": _list(rule.get("tts_targets")),
            "description": "Choose one or more available speakers, media players, or satellites.",
            "show_when": show_tts,
            "full_width": True,
        },
        {
            "key": "tts_mode",
            "label": "Announcement Words",
            "type": "select",
            "presentation": "cards",
            "options": tts_mode_options(rule.get("trigger_event") or "changed"),
            "dependent_options": {
                "source_key": "trigger_event",
                "options_by_source": tts_mode_options_by_event,
                "default_options": tts_mode_options("changed"),
            },
            "value": _token(rule.get("tts_mode") or ("custom" if _text(rule.get("tts_text")) else "default")),
            "show_when": show_tts,
            "full_width": True,
        },
        {
            "key": "tts_text",
            "label": "Custom Announcement",
            "type": "textarea",
            "placeholder": "A person was detected in the front yard.",
            "value": _text(rule.get("tts_text")),
            "description": "Optional variables: {device}, {room}, {event}, {state}, {value}, {category}, and {provider}.",
            "show_when_all": [show_tts, {"source_key": "tts_mode", "equals": "custom"}],
            "full_width": True,
        },
        *_announcement_audio_fields(rule, show_tts),
        {
            "key": "action_category",
            "label": "Device Category",
            "type": "select",
            "presentation": "cards",
            "options": _category_options(registry, actionable_only=True),
            "value": action_category,
            "show_when": show_device_action,
            "full_width": True,
        },
        {
            "key": "action_integration",
            "label": "Integration",
            "type": "select",
            "presentation": "cards",
            "options": action_integration_options,
            "dependent_options": action_integration_dependency,
            "value": action_integration,
            "description": "Choose the enabled integration Tater should use for this action.",
            "show_when": show_device_action,
            "full_width": True,
        },
        {
            "key": "action_scope",
            "label": "Which Ones?",
            "type": "select",
            "presentation": "cards",
            "options": [
                {
                    "value": "devices",
                    "label": "Selected Devices",
                    "description": "Choose exactly which devices this automation controls.",
                    "icon": "◆",
                },
                {
                    "value": "category",
                    "label": "Every Compatible Device",
                    "description": "Apply the action to every compatible device from this integration.",
                    "icon": "✦",
                },
            ],
            "value": _token(rule.get("action_scope") or "devices"),
            "show_when": show_device_action,
            "full_width": True,
        },
        {
            "key": "action_devices",
            "label": "Which Devices?",
            "type": "multiselect",
            "presentation": "cards",
            "options": action_device_options,
            "dependent_options": action_device_dependency,
            "value": _list(rule.get("action_devices")),
            "show_when_all": [
                show_device_action,
                {"source_key": "action_scope", "equals": "devices"},
            ],
            "full_width": True,
        },
        {
            "key": "action_operation",
            "label": "What Should They Do?",
            "type": "select",
            "presentation": "cards",
            "options": action_options,
            "dependent_options": action_dependency,
            "value": _token(rule.get("action_operation")),
            "show_when": show_device_action,
            "full_width": True,
        },
        {
            "key": "action_value",
            "label": "Value",
            "type": "number",
            "placeholder": "0–100 or target temperature",
            "value": _text(rule.get("action_value")),
            "show_when_all": [show_device_action, show_numeric_action],
        },
        {
            "key": "action_mode",
            "label": "Mode",
            "type": "text",
            "placeholder": "heat, cool, auto…",
            "value": _text(rule.get("action_mode")),
            "show_when_all": [
                show_device_action,
                {"source_key": "action_operation", "equals": "set_hvac_mode"},
            ],
        },
        {
            "key": "action_text",
            "label": "Color, Text, or URL",
            "type": "text",
            "value": _text(rule.get("action_text")),
            "show_when_all": [show_device_action, show_action_text],
            "full_width": True,
        },
        {
            "key": "notification_targets",
            "label": "Where Should It Send?",
            "type": "multiselect",
            "presentation": "cards",
            "options": notification_options,
            "value": _list(rule.get("notification_targets")),
            "show_when": show_notification,
            "full_width": True,
        },
        {
            "key": "notification_title",
            "label": "Notification Title",
            "type": "text",
            "value": _text(rule.get("notification_title") or "Tater Automation"),
            "show_when": show_notification,
        },
        {
            "key": "notification_priority",
            "label": "Priority",
            "type": "select",
            "presentation": "cards",
            "options": [
                {"value": "normal", "label": "Normal", "icon": "○"},
                {"value": "high", "label": "High", "icon": "!"},
            ],
            "value": _text(rule.get("notification_priority") or "normal"),
            "show_when": show_notification,
        },
        {
            "key": "notification_message",
            "label": "Message",
            "type": "textarea",
            "value": _text(rule.get("notification_message")),
            "description": "You can use the same variables available for announcements.",
            "show_when": show_notification,
            "full_width": True,
        },
        {
            "key": "camera_source",
            "label": "Which Camera?",
            "type": "select",
            "presentation": "cards",
            "options": [
                {
                    "value": "trigger",
                    "label": "Triggering Camera",
                    "description": "Use the same camera selected in the trigger.",
                    "icon": "↻",
                },
                {
                    "value": "selected",
                    "label": "Another Camera",
                    "description": "Always capture a separately selected camera.",
                    "icon": "◎",
                },
            ],
            "value": _token(rule.get("camera_source") or "trigger"),
            "show_when": show_camera_ai,
            "full_width": True,
        },
        {
            "key": "camera_device",
            "label": "Camera",
            "type": "select",
            "presentation": "cards",
            "options": _devices_for_category_options(
                registry,
                "camera",
                require_actions=("camera_snapshot", "snapshot"),
            ),
            "value": _text(rule.get("camera_device")),
            "show_when_all": [
                show_camera_ai,
                {"source_key": "camera_source", "equals": "selected"},
            ],
            "full_width": True,
        },
        {
            "key": "vision_prompt",
            "label": "What Should Vision Describe?",
            "type": "textarea",
            "value": _text(
                rule.get("vision_prompt")
                or "Briefly describe the important activity in this image. Do not invent details."
            ),
            "show_when": show_camera_ai,
            "full_width": True,
        },
        {
            "key": "vision_fallback",
            "label": "Fallback Message",
            "type": "text",
            "value": _text(rule.get("vision_fallback") or "Camera activity was detected."),
            "description": "Used if the camera or vision model is unavailable.",
            "show_when": show_camera_ai,
            "full_width": True,
        },
        {
            "key": "camera_tts_targets",
            "label": "Announcement Speakers",
            "type": "multiselect",
            "presentation": "cards",
            "options": announcement_options,
            "value": _list(rule.get("camera_tts_targets")),
            "description": "Optional. Choose speakers if Tater should announce the description.",
            "show_when": show_camera_ai,
            "full_width": True,
        },
        {
            "key": "camera_tts_text",
            "label": "Camera Announcement",
            "type": "textarea",
            "value": _text(rule.get("camera_tts_text") or "{vision}"),
            "description": "Use {vision} to insert the camera description.",
            "show_when": show_camera_ai,
            "full_width": True,
        },
        {
            "key": "camera_notification_targets",
            "label": "Notification Destinations",
            "type": "multiselect",
            "presentation": "cards",
            "options": notification_options,
            "value": _list(rule.get("camera_notification_targets")),
            "description": "Optional. Announce, notify, or do both.",
            "show_when": show_camera_ai,
            "full_width": True,
        },
        {
            "key": "camera_notification_title",
            "label": "Camera Notification Title",
            "type": "text",
            "value": _text(rule.get("camera_notification_title") or "Camera Activity"),
            "show_when": show_camera_ai,
        },
        {
            "key": "camera_notification_message",
            "label": "Camera Notification Message",
            "type": "textarea",
            "value": _text(rule.get("camera_notification_message") or "{vision}"),
            "show_when": show_camera_ai,
            "full_width": True,
        },
        {
            "type": "heading",
            "label": "3. Save",
            "description": "Give the automation a clear name and choose how quickly it may run again.",
        },
        {
            "key": "name",
            "label": "Automation Name",
            "type": "text",
            "required": True,
            "placeholder": "Front yard person announcement",
            "value": _text(rule.get("name")),
            "full_width": True,
        },
        {
            "key": "enabled",
            "label": "Enabled",
            "type": "checkbox",
            "value": _bool(rule.get("enabled"), True),
        },
        {
            "key": "cooldown_seconds",
            "label": "Wait Before Running Again",
            "type": "number",
            "min": 0,
            "max": 86400,
            "value": _int(rule.get("cooldown_seconds"), 30),
            "description": "Seconds to ignore duplicate events after this automation runs.",
        },
    ]


def _rule_form(
    rule: Dict[str, Any],
    registry: Dict[str, Any],
    client: Any,
    *,
    announcement_catalog: Optional[List[Dict[str, Any]]] = None,
    notification_catalog: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    status = _text(rule.get("last_status")) or "not run"
    trigger_device = _find_device(registry, rule.get("trigger_device"))
    trigger_name = _text((trigger_device or {}).get("name")) or _text(rule.get("trigger_category")).replace("_", " ").title()
    trigger_event = _event_option(rule.get("trigger_event"))["label"]
    action_type = _token(rule.get("action_type"))
    if action_type == "tts":
        target_count = len(_list(rule.get("tts_targets")))
        action_summary = f"Announce on {target_count} speaker{'s' if target_count != 1 else ''}"
    elif action_type == "device":
        device_count = len(_list(rule.get("action_devices")))
        operation = _ACTION_LABELS.get(
            _token(rule.get("action_operation")),
            _text(rule.get("action_operation")).replace("_", " ").title(),
        )
        action_summary = f"{operation} on {device_count or 'compatible'} device{'s' if device_count != 1 else ''}"
    elif action_type == "notification":
        target_count = len(_list(rule.get("notification_targets")))
        action_summary = f"Notify {target_count} destination{'s' if target_count != 1 else ''}"
    else:
        action_summary = "Describe camera and deliver the result"
    enabled = _bool(rule.get("enabled"), True)
    return {
        "id": rule["id"],
        "group": "rules",
        "title": _text(rule.get("name")) or "Automation",
        "detail": f"Last {status}: {_now_label(rule.get('last_run_ts'))}",
        "hero_badges": [
            {"label": "Enabled" if enabled else "Disabled", "tone": "running" if enabled else "muted"},
            {"label": status.title(), "tone": "running" if status == "ok" else "muted"},
        ],
        "summary_rows": [
            {"label": "Trigger", "value": f"{trigger_name} · {trigger_event}"},
            {"label": "Action", "value": action_summary},
        ],
        "save_action": "automation_save_rule",
        "run_action": "automation_run_now",
        "run_label": "Test Now",
        "remove_action": "automation_remove_rule",
        "remove_confirm": "Remove this automation?",
        "fields": _editor_fields(
            rule,
            registry,
            client,
            announcement_catalog=announcement_catalog,
            notification_catalog=notification_catalog,
        ),
        "sections": [
            {
                "label": "Last Run",
                "fields": [
                    {"key": "last_status", "label": "Status", "type": "text", "read_only": True, "value": status},
                    {
                        "key": "last_summary",
                        "label": "Result",
                        "type": "textarea",
                        "read_only": True,
                        "value": _text(rule.get("last_summary") or rule.get("last_error")),
                    },
                    {
                        "key": "run_count",
                        "label": "Runs",
                        "type": "number",
                        "read_only": True,
                        "value": _int(rule.get("run_count"), 0),
                    },
                ],
            }
        ],
    }


def _history_form(row: Dict[str, Any]) -> Dict[str, Any]:
    context = row.get("context") if isinstance(row.get("context"), dict) else {}
    status = _text(row.get("status") or "unknown")
    return {
        "id": _text(row.get("id")) or str(uuid.uuid4()),
        "group": "history",
        "title": _text(row.get("rule_name")) or "Automation run",
        "subtitle": f"{status.title()} • {_now_label(row.get('started_at'))} • {_text(row.get('duration_ms'))} ms",
        "fields": [
            {"key": "status", "label": "Status", "type": "text", "read_only": True, "value": status},
            {
                "key": "result",
                "label": "Result",
                "type": "textarea",
                "read_only": True,
                "value": _text(row.get("summary") or row.get("error")),
            },
            {
                "key": "trigger",
                "label": "Trigger",
                "type": "text",
                "read_only": True,
                "value": " • ".join(
                    item
                    for item in (
                        _text(context.get("device")),
                        _text(context.get("event")),
                        _text(context.get("state")),
                    )
                    if item
                )
                or _text(row.get("reason")),
            },
        ],
    }



def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    if client is None:
        raise ValueError("Redis connection is unavailable.")
    registry = _registry(client)
    rules = _load_rules(client)
    history = _history(client, 50)
    runtime = _runtime_get(client)
    enabled_count = sum(1 for rule in rules.values() if _bool(rule.get("enabled"), True))
    success_count = sum(1 for row in history if _text(row.get("status")) == "ok")
    error_count = sum(1 for row in history if _text(row.get("status")) == "error")
    default_trigger_category = next(
        (row["value"] for row in _category_options(registry, triggerable_only=True)),
        "",
    )
    default_action_category = next(
        (row["value"] for row in _category_options(registry, actionable_only=True)),
        "",
    )
    default_trigger_integrations, _trigger_integration_dependency = _integration_dependency(
        registry,
        current_category=default_trigger_category,
        source_key="trigger_category",
        triggerable_only=True,
    )
    default_trigger_integration = (
        _text(default_trigger_integrations[0].get("value")) if default_trigger_integrations else ""
    )
    default_action_integrations, _action_integration_dependency = _integration_dependency(
        registry,
        current_category=default_action_category,
        source_key="action_category",
        actionable_only=True,
    )
    default_action_integration = (
        _text(default_action_integrations[0].get("value")) if default_action_integrations else ""
    )
    default_action_options, _dependency = _action_dependency(
        registry,
        current_integration=default_action_integration,
    )
    default_trigger_devices, _trigger_device_dependency = _device_dependency(
        registry,
        current_integration=default_trigger_integration,
        source_key="trigger_integration",
        multiple=False,
        triggerable_only=True,
    )
    default_trigger_device = _text(default_trigger_devices[0].get("value")) if default_trigger_devices else ""
    default_trigger_events, _trigger_event_options_dependency = _trigger_event_dependency(
        registry,
        current_device=default_trigger_device,
    )
    default_action_devices, _action_device_dependency = _device_dependency(
        registry,
        current_integration=default_action_integration,
        source_key="action_integration",
        multiple=True,
        actionable_only=True,
    )
    blank = {
        "name": "",
        "enabled": True,
        "trigger_category": default_trigger_category,
        "trigger_integration": default_trigger_integration,
        "trigger_device": default_trigger_device,
        "trigger_event": default_trigger_events[0]["value"] if default_trigger_events else "changed",
        "cooldown_seconds": 30,
        "action_type": "tts",
        "action_category": default_action_category,
        "action_integration": default_action_integration,
        "action_scope": "devices",
        "action_devices": [default_action_devices[0]["value"]] if default_action_devices else [],
        "action_operation": default_action_options[0]["value"] if default_action_options else "",
        "tts_mode": "default",
        "notification_title": "Tater Automation",
        "notification_priority": "normal",
    }
    saved_rules = list(rules.values())
    announcement_catalog = _announcement_options(
        [
            target
            for rule in saved_rules
            for target in [*_list(rule.get("tts_targets")), *_list(rule.get("camera_tts_targets"))]
        ]
    )
    notification_catalog = [
        {
            **row,
            "description": _text(row.get("description")) or "Notification destination",
            "icon": _text(row.get("icon")) or "◉",
        }
        for row in _notification_options(
            client,
            [
                target
                for rule in saved_rules
                for target in [
                    *_list(rule.get("notification_targets")),
                    *_list(rule.get("camera_notification_targets")),
                ]
            ],
        )
    ]
    forms = [_history_form(row) for row in history]
    forms.extend(
        _rule_form(
            rule,
            registry,
            client,
            announcement_catalog=announcement_catalog,
            notification_catalog=notification_catalog,
        )
        for rule in sorted(rules.values(), key=lambda item: (_text(item.get("name")).casefold(), item["id"]))
    )
    last_run = _text(runtime.get("last_run_ts"))
    return {
        "summary": "Create simple “when this happens, do that” rules across every enabled Tater integration.",
        "stats": [
            {"label": "Automations", "value": len(rules)},
            {"label": "Enabled", "value": enabled_count},
            {"label": "Successful Runs", "value": success_count},
            {"label": "Errors", "value": error_count},
            {"label": "Queue", "value": _int(runtime.get("queue_depth"), 0)},
            {"label": "Last Run", "value": _now_label(last_run)},
        ],
        "items": [],
        "empty_message": "No automations configured yet.",
        "ui": {
            "kind": "settings_manager",
            "appearance": "automation",
            "title": "Tater Automations",
            "empty_message": "No automations configured yet.",
            "stats_refresh_button": True,
            "stats_refresh_label": "Refresh devices",
            "stats_refresh_action": "automation_refresh_devices",
            "item_fields_dropdown": True,
            "item_fields_dropdown_label": "Automation Settings",
            "item_fields_popup": True,
            "item_fields_popup_label": "Edit Automation",
            "item_sections_in_dropdown": True,
            "default_tab": "rules" if rules else "create",
            "manager_tabs": [
                {
                    "key": "rules",
                    "label": "Automations",
                    "source": "items",
                    "item_group": "rules",
                    "selector": False,
                    "empty_message": "No automations configured.",
                },
                {"key": "create", "label": "Create Automation", "source": "add_form"},
                {
                    "key": "history",
                    "label": "Run History",
                    "source": "items",
                    "item_group": "history",
                    "selector": False,
                    "empty_message": "No automation runs recorded yet.",
                },
            ],
            "add_form": {
                "action": "automation_add_rule",
                "submit_label": "Create Automation",
                "fields": _editor_fields(
                    blank,
                    registry,
                    client,
                    announcement_catalog=announcement_catalog,
                    notification_catalog=notification_catalog,
                ),
            },
            "item_forms": forms,
        },
    }


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    if client is None:
        raise ValueError("Redis connection is unavailable.")
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)
    action_name = _token(action)
    if action_name == "automation_refresh_devices":
        _registry(client, refresh=True)
        return {"ok": True, "message": "Integration devices refreshed."}
    if action_name == "automation_add_rule":
        rule = _rule_from_form(values, body)
        _save_rule(client, rule)
        return {"ok": True, "id": rule["id"], "message": "Automation created."}
    if action_name == "automation_save_rule":
        rule_id = _text(body.get("id"))
        existing = _get_rule(client, rule_id)
        if not existing:
            raise KeyError("Automation not found.")
        rule = _rule_from_form(values, body, existing)
        _save_rule(client, rule)
        return {"ok": True, "id": rule_id, "message": "Automation saved."}
    if action_name == "automation_remove_rule":
        rule_id = _text(body.get("id"))
        if not rule_id or not client.hdel(_RULES_KEY, rule_id):
            raise KeyError("Automation not found.")
        return {"ok": True, "id": rule_id, "message": "Automation removed."}
    if action_name == "automation_run_now":
        rule_id = _text(body.get("id"))
        rule = _get_rule(client, rule_id)
        if not rule:
            raise KeyError("Automation not found.")
        context = {
            "device": "test trigger",
            "room": _text(rule.get("trigger_room")),
            "event": "manual test",
            "state": "detected",
            "old_state": "",
            "value": _text(rule.get("trigger_value")),
            "category": _text(rule.get("trigger_category")),
            "provider": "automation_core",
        }
        _enqueue(client, rule, context, "manual")
        return {"ok": True, "id": rule_id, "message": "Automation queued for a test run."}
    raise KeyError(f"Unsupported Automation Core UI action: {action}")


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    del platform
    return [
        {
            "id": "automation_status",
            "description": "List configured Tater automations and their latest status without changing them.",
            "usage": '{"function":"automation_status","arguments":{}}',
        }
    ]


async def run_hydra_kernel_tool(
    *,
    tool_id: str,
    args: Optional[Dict[str, Any]] = None,
    platform: str = "",
    scope: str = "",
    origin: Optional[Dict[str, Any]] = None,
    llm_client: Any = None,
    redis_client: Any = None,
    **_kwargs,
) -> Optional[Dict[str, Any]]:
    del args, platform, scope, origin, llm_client
    if _token(tool_id) != "automation_status":
        return None
    client = redis_client or globals().get("redis_client")
    rules = _load_rules(client)
    return {
        "tool": "automation_status",
        "ok": True,
        "automation_count": len(rules),
        "enabled_count": sum(1 for rule in rules.values() if _bool(rule.get("enabled"), True)),
        "automations": [
            {
                "id": rule["id"],
                "name": rule["name"],
                "enabled": rule["enabled"],
                "trigger_category": rule["trigger_category"],
                "trigger_event": rule["trigger_event"],
                "action_type": rule["action_type"],
                "last_status": rule["last_status"],
                "last_run": _now_label(rule["last_run_ts"]),
            }
            for rule in sorted(rules.values(), key=lambda item: item["name"].casefold())
        ],
        "summary_for_user": f"{len(rules)} automations are configured.",
    }


def run(stop_event=None) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_main(stop_event))
    except asyncio.CancelledError:
        logger.info("[automation] core cancelled")
    except KeyboardInterrupt:
        logger.info("[automation] core interrupted")
    except Exception:
        logger.exception("[automation] core crashed")
        raise
    finally:
        loop.close()
