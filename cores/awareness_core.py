"""Observe selected cameras and sensors and keep a queryable home-activity history."""

import asyncio
import ast
import base64
import hashlib
import importlib.util
import json
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv

from helpers import extract_json, get_llm_client_from_env, redis_client
try:
    from helpers import redis_blob_client as _shared_redis_blob_client
except Exception:  # pragma: no cover - compatibility with older Tater runtimes and test harnesses.
    _shared_redis_blob_client = None
try:
    from helpers import get_primary_llm_client_from_env as _get_primary_llm_client_from_env
except Exception:  # pragma: no cover - compatibility with older Tater runtimes.
    _get_primary_llm_client_from_env = get_llm_client_from_env
try:
    from helpers import (
        _is_local_hydra_llm_provider as _shared_is_local_hydra_llm_provider,
        _normalize_hydra_llm_provider as _shared_normalize_hydra_llm_provider,
        describe_image_with_local_llm as _shared_describe_image_with_local_llm,
        resolve_hydra_base_servers as _shared_resolve_hydra_base_servers,
    )
except Exception:  # pragma: no cover - keeps older Tater runtimes from failing import.
    _shared_is_local_hydra_llm_provider = None
    _shared_normalize_hydra_llm_provider = None
    _shared_describe_image_with_local_llm = None
    _shared_resolve_hydra_base_servers = None
from tateros import integration_store as integration_store_module
from vision_settings import get_vision_settings as get_shared_vision_settings
try:
    from notify import dispatch_notification, notifier_destination_catalog
except Exception:  # pragma: no cover - compatibility with Tater versions before shared notifications.
    dispatch_notification = None
    notifier_destination_catalog = None
try:
    import face_id_runtime as _face_id_runtime
except Exception:  # pragma: no cover - compatibility with Tater versions before Face ID.
    _face_id_runtime = None
try:
    from kernel_tools import video_analyze as _shared_video_analyze
except Exception:  # pragma: no cover - compatibility with Tater versions before video understanding.
    _shared_video_analyze = None

__version__ = "4.9.0"
CORE_DESCRIPTION = (
    "Choose which cameras and sensors Tater should observe, describe camera events from images or short video clips, "
    "optionally pair sensors with cameras, retain their bounded event history, snapshots, and playable clips, "
    "answer questions about past activity, and optionally deliver the completed event with media and Face ID context. "
    "Use Automation Core for custom notification text, announcements, and device actions."
)
TAGS = ["awareness", "cameras", "sensors", "event-history", "vision", "video", "notifications"]

load_dotenv()

logger = logging.getLogger("awareness_core")
logger.setLevel(logging.INFO)


def _awareness_normalize_llm_provider(value: Any) -> str:
    if callable(_shared_normalize_hydra_llm_provider):
        try:
            return str(_shared_normalize_hydra_llm_provider(value) or "openai_compatible")
        except Exception:
            pass
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if token in {"hf", "huggingface", "hugging_face", "transformers", "hf_transformers", "local_transformers"}:
        return "hf_transformers"
    if token in {"llama", "llamacpp", "llama_cpp", "llama.cpp", "gguf", "llama_cpp_python", "llama-cpp-python"}:
        return "llama_cpp"
    if token in {"mlx", "mlx_lm", "mlx-lm", "apple_mlx", "apple_silicon", "mlxlm"}:
        return "mlx_lm"
    return "openai_compatible"


def _awareness_is_local_llm_provider(provider: Any) -> bool:
    if callable(_shared_is_local_hydra_llm_provider):
        try:
            return bool(_shared_is_local_hydra_llm_provider(provider))
        except Exception:
            pass
    return _awareness_normalize_llm_provider(provider) in {"hf_transformers", "llama_cpp", "mlx_lm"}


def _integration_module(integration_id: str):
    return integration_store_module.integration_module(integration_id)


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> Dict[str, str]:
    module = _integration_module("homeassistant")
    if module is not None:
        return module.load_homeassistant_config(required=required, client=client)
    if required:
        raise ValueError("Home Assistant integration is not enabled.")
    return {"base": "", "token": ""}


def _unifi_request(*args, **kwargs):
    module = _integration_module("unifi_protect")
    if module is None:
        raise RuntimeError("UniFi Protect integration is not enabled.")
    return module.unifi_protect_request(*args, **kwargs)

CORE_SETTINGS = {
    "category": "Awareness Core Settings",
    # Keep events_query kernel tool callable even if the background listener loop is stopped.
    "hydra_tools_require_running": False,
    "required": {
        "events_retention": {
            "label": "Events Retention",
            "type": "select",
            "options": ["2d", "7d", "30d", "forever"],
            "default": "7d",
            "description": "How long to retain awareness events written to Redis.",
        },
        "store_event_snapshots": {
            "label": "Store Event Snapshots",
            "type": "checkbox",
            "default": True,
            "description": "Store camera, doorbell, and sensor-linked snapshots for Event History.",
        },
        "event_snapshot_max_kb": {
            "label": "Snapshot Max Size (KB)",
            "type": "number",
            "default": 768,
            "description": "Maximum JPEG size to store per event snapshot.",
        },
        "store_event_clips": {
            "label": "Store Event Clips",
            "type": "checkbox",
            "default": True,
            "description": "Keep successful camera and sensor-linked clips so they can be played from Event History.",
        },
        "event_clip_max_mb": {
            "label": "Event Clip Max Size (MB)",
            "type": "number",
            "default": 32,
            "description": "Maximum video size to retain for each Awareness event.",
        },
        "camera_monitor_cooldown_seconds": {
            "label": "Camera Event Cooldown (sec)",
            "type": "number",
            "default": 30,
            "description": "Minimum time between snapshot and vision checks for each monitored camera.",
        },
        "camera_event_clip_seconds": {
            "label": "Camera Event Clip Length (sec)",
            "type": "number",
            "default": 8,
            "description": "Length of short camera clips sent to the configured Video Understanding model.",
        },
    },
}

CORE_WEBUI_TAB = {
    "label": "Awareness",
    "order": 20,
    "requires_running": True,
}

_MONITORS_KEY = "awareness:monitors"
_EXEC_QUEUE_KEY = "awareness:monitor_queue"
_RUNTIME_KEY = "awareness:runtime"
_EVENTS_PREFIX = "tater:automations:events:"
_EVENT_SNAPSHOT_PREFIX = "awareness:event_snapshot:"
_EVENT_CLIP_PREFIX = "awareness:event_clip:"
_EVENT_CLIP_META_PREFIX = "awareness:event_clip_meta:"
_FACE_IDENTITIES_KEY = "awareness:face_identities"
_FACE_SESSION_PREFIX = "awareness:face_session:"
_FACE_BURST_FRAME_COUNT = 5
_FACE_BURST_INTERVAL_SECONDS = 1.0
_FACE_OBSERVATION_LIMIT = 500
_FACE_REFERENCE_LIMIT = 24
_FACE_NEW_UNKNOWN_TARGET = "__new_unknown__"
_FACE_BURST_TASKS: set[Any] = set()
_FACE_BURST_BY_CAMERA: Dict[str, Any] = {}
_FACE_IDENTITY_LOCK = threading.Lock()
_FACE_ALWAYS_BURST_EVENTS = {"activity", "motion", "person", "face", "doorbell"}
_FACE_VISION_GATED_EVENTS = {"animal", "vehicle", "package", "license_plate"}
_FACE_HUMAN_SUMMARY_RE = re.compile(
    r"\b(?:person|people|man|men|woman|women|boy|girl|child|children|adult|human|someone|"
    r"visitors?|guests?|couriers?|drivers?|workers?|pedestrians?|residents?|homeowners?|figures?)\b",
    re.IGNORECASE,
)
_AWARENESS_WORKER_COUNT = 4

_TRUE_TOKENS = {"1", "true", "yes", "on", "enabled", "y"}
_FALSE_TOKENS = {"0", "false", "no", "off", "disabled", "n"}
_SUPPORTED_EVENT_PROVIDERS = {
    "all",
    "homeassistant",
    "unifi_protect",
    "unifi_network",
    "hue",
    "ecobee_homekit",
    "aladdin",
    "sonos",
}
_MONITOR_SENSOR_CATEGORIES = {
    "entry_sensor",
    "garage_door",
    "motion",
    "presence",
    "leak",
    "temperature",
    "humidity",
    "illuminance",
    "energy",
    "battery",
    "sensor",
}
_MONITOR_ACTIVE_STATES = {
    "on",
    "open",
    "opened",
    "active",
    "detected",
    "motion",
    "occupied",
    "present",
    "connected",
    "online",
    "wet",
    "alarm",
    "tamper",
    "ringing",
    "pressed",
    "true",
    "1",
}
_MONITOR_INACTIVE_STATES = {
    "off",
    "closed",
    "inactive",
    "clear",
    "idle",
    "unoccupied",
    "away",
    "disconnected",
    "offline",
    "dry",
    "false",
    "0",
}
_MONITOR_TRIGGER_OPTIONS = [
    {"value": "changed", "label": "Changes", "icon": "↻"},
    {"value": "turns_on", "label": "Becomes active", "icon": "●"},
    {"value": "turns_off", "label": "Becomes inactive", "icon": "○"},
    {"value": "opens", "label": "Opens", "icon": "↗"},
    {"value": "closes", "label": "Closes", "icon": "↘"},
    {"value": "motion", "label": "Detects motion", "icon": "⌁"},
    {"value": "person", "label": "Detects a person", "icon": "♟"},
    {"value": "vehicle", "label": "Detects a vehicle", "icon": "◆"},
    {"value": "animal", "label": "Detects an animal", "icon": "♣"},
    {"value": "package", "label": "Detects a package", "icon": "▣"},
    {"value": "face", "label": "Detects a face", "icon": "◎"},
    {"value": "license_plate", "label": "Detects a license plate", "icon": "▤"},
    {"value": "doorbell", "label": "Doorbell is pressed", "icon": "◉"},
    {"value": "connects", "label": "Connects / comes online", "icon": "⌁"},
    {"value": "disconnects", "label": "Disconnects / goes offline", "icon": "×"},
]
_MONITOR_TRIGGER_VALUES = {row["value"] for row in _MONITOR_TRIGGER_OPTIONS}
_MONITOR_DESCRIPTION_MODES = {"image", "video"}
_MONITOR_DESCRIPTION_MODE_OPTIONS = [
    {
        "value": "image",
        "label": "Image description",
        "description": "Describe one snapshot. Faster and supported by every compatible camera.",
        "icon": "▧",
    },
    {
        "value": "video",
        "label": "Video description",
        "description": "Analyze a short clip to understand actions, changes, and sequence.",
        "icon": "▶",
    },
]
_UNIFI_SMART_TYPE_ALIASES = {
    "people": "person",
    "human": "person",
    "humans": "person",
    "persons": "person",
    "vehicles": "vehicle",
    "car": "vehicle",
    "cars": "vehicle",
    "auto": "vehicle",
    "autos": "vehicle",
    "animals": "animal",
    "pet": "animal",
    "pets": "animal",
    "packages": "package",
    "parcel": "package",
    "parcels": "package",
    "delivery": "package",
    "deliveries": "package",
    "faces": "face",
    "licenseplate": "license_plate",
    "licenseplates": "license_plate",
    "plate": "license_plate",
    "plates": "license_plate",
}
_UNIFI_SMART_TYPE_LABELS = {
    "person": "Person",
    "vehicle": "Vehicle",
    "animal": "Animal",
    "package": "Package",
    "face": "Face",
    "license_plate": "License Plate",
}
_EVENT_FILTER_RUNTIME_KEYS = {
    "camera": "events_filter_camera",
    "doorbell": "events_filter_doorbell",
    "sensor": "events_filter_sensor",
}
_EVENT_LIST_VIEW_RUNTIME_KEY = "events_list_view"
_EVENT_PAGE_RUNTIME_KEY = "events_page"
_EVENT_PAGE_SIZE_DEFAULT = 24
_EVENT_PAGE_SIZE_MAX = 100
_EVENT_FILTER_DEFAULTS = {
    "camera": True,
    "doorbell": True,
    "sensor": True,
}
_UNIFI_SENSOR_EVENT_LOCK = threading.Lock()
_UNIFI_SENSOR_LAST_EVENT: Dict[str, Tuple[str, float, str]] = {}
_UNIFI_CAMERA_EVENT_LOCK = threading.Lock()
_UNIFI_CAMERA_LAST_EVENT: Dict[str, Tuple[float, str]] = {}
_EVENTS_QUERY_MAX_EVENTS_PER_SOURCE = 1000
_EVENTS_QUERY_MAX_CANDIDATE_EVENTS_FOR_LLM = 120
_EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER = 40
_EVENTS_QUERY_INPUT_TOKEN_BUDGET = 12_000
_EVENTS_QUERY_RETRY_TOKEN_BUDGET = 6_000
_EVENTS_QUERY_CHARS_PER_TOKEN_ESTIMATE = 3
_EVENTS_QUERY_MAX_TITLE_CHARS = 160
_EVENTS_QUERY_MAX_MESSAGE_CHARS = 360
_EVENTS_QUERY_MAX_DATA_TEXT_CHARS = 160
_EVENTS_QUERY_MAX_ROLLUP_SAMPLES = 6
_EVENTS_QUERY_IMMEDIATE_WINDOW_MINUTES = 10
_EVENTS_QUERY_IMMEDIATE_RE = re.compile(
    r"\b(?:right\s+now|just\s+now|currently|at\s+the\s+moment|at\s+present)\b",
    re.IGNORECASE,
)
_EVENTS_QUERY_SAFE_DATA_FIELDS = {
    "area",
    "camera_entity",
    "confidence",
    "detected_object",
    "detected_objects",
    "description_media",
    "description_mode",
    "event_type",
    "face_count",
    "face_identity_ids",
    "known_people",
    "recognized_people",
    "recognized_person_ids",
    "new_state",
    "object_type",
    "object_types",
    "old_state",
    "provider",
    "reason",
    "sensor_type",
    "smart_detect_type",
    "smart_detect_types",
    "state",
    "trigger_entity",
}
_INTEGRATION_RUNTIME_EVENTS_KEY = "tater:integration_runtime:events"
_INTEGRATION_RUNTIME_EVENT_SEQ_KEY = "tater:integration_runtime:event_seq"
_INTEGRATION_RUNTIME_STATUS_KEY = "tater:integration_runtime:status"
_INTEGRATION_RUNTIME_STATES_KEY = "tater:integration_runtime:states"
_AWARENESS_RUNTIME_SEQ_KEY = "awareness:integration_runtime:last_seq"


def _text(value: Any) -> str:
    return str(value or "").strip()




def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    return bool(default)


def _as_int(value: Any, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    if maximum is not None:
        out = min(int(maximum), out)
    return out


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _slug(value: str) -> str:
    text = _text(value).lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_:-]", "", text)
    return text or "unknown"


def _category_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", _text(value).lower()).strip("_")


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _parse_iso(value: Any) -> Optional[datetime]:
    raw = _text(value)
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        try:
            dt = datetime.fromisoformat(raw)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except Exception:
            return None


def _fmt_ts(ts: Any) -> str:
    try:
        val = float(ts)
    except Exception:
        return "n/a"
    if val <= 0:
        return "n/a"
    return datetime.fromtimestamp(val).strftime("%Y-%m-%d %H:%M:%S")


def _settings(client: Any) -> Dict[str, str]:
    redis_obj = client or redis_client
    data = redis_obj.hgetall("awareness_core_settings") if redis_obj else {}
    return data if isinstance(data, dict) else {}


def _setting_int(client: Any, key: str, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    return _as_int(_settings(client).get(key), default, minimum=minimum, maximum=maximum)



def _normalize_event_provider(value: Any) -> str:
    token = _text(value).lower()
    if token in {"", "runtime", "integrations", "integration_runtime"}:
        return "all"
    if token in {"unifi", "protect"}:
        token = "unifi_protect"
    if token in {"network", "unifi_network"}:
        token = "unifi_network"
    if token in {"philips_hue", "phillips_hue"}:
        token = "hue"
    if token in {"ecobee", "homekit", "ecobee_homekit"}:
        token = "ecobee_homekit"
    if token not in _SUPPORTED_EVENT_PROVIDERS:
        if re.fullmatch(r"[a-z0-9_]+", token) and integration_store_module.get_integration_enabled(token):
            return token
        return "all"
    return token


def _provider_ref(provider: Any, value: Any) -> str:
    token = _text(value)
    if not token:
        return ""
    provider_token = _normalize_event_provider(provider)
    if provider_token == "all":
        return token
    return f"{provider_token}|{token}"


def _split_provider_ref(value: Any, fallback_provider: Any = "all") -> Tuple[str, str]:
    token = _text(value)
    if "|" in token:
        left, right = token.split("|", 1)
        provider = _normalize_event_provider(left)
        if provider != "all" and _text(right):
            return provider, _text(right)
    return _normalize_event_provider(fallback_provider), token


def _entity_object_id(entity_id: str) -> str:
    _provider, raw_entity = _split_provider_ref(entity_id, "")
    token = _text(raw_entity or entity_id).lower()
    if "." in token:
        return token.split(".", 1)[1]
    return token



def _provider_label(provider: str) -> str:
    token = _normalize_event_provider(provider)
    if token == "all":
        return "Integrated Devices"
    if token == "unifi_protect":
        return "UniFi Protect"
    if token == "unifi_network":
        return "UniFi Network"
    if token == "hue":
        return "Philips Hue"
    if token == "ecobee_homekit":
        return "Ecobee HomeKit"
    if token == "aladdin":
        return "Aladdin Connect"
    if token == "sonos":
        return "Sonos"
    if token != "all":
        try:
            from integration_registry import get_integration_catalog

            for row in get_integration_catalog():
                if _text(row.get("id")).lower() == token:
                    return _text(row.get("name")) or token.replace("_", " ").title()
        except Exception:
            pass
        return token.replace("_", " ").title()
    return "Home Assistant"


def _events_retention_seconds(client: Any) -> Optional[int]:
    token = _text(_settings(client).get("events_retention") or "7d").lower()
    mapping = {
        "2d": 2 * 24 * 60 * 60,
        "7d": 7 * 24 * 60 * 60,
        "30d": 30 * 24 * 60 * 60,
        "forever": None,
    }
    return mapping.get(token, mapping["7d"])


def _runtime_set(client: Any, **fields: Any) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    payload: Dict[str, Any] = {"updated_at": time.time()}
    payload.update(fields)
    clean = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in payload.items()}
    try:
        redis_obj.hset(_RUNTIME_KEY, mapping=clean)
    except Exception:
        logger.debug("[awareness] failed to update runtime state", exc_info=True)


def _runtime_get(client: Any) -> Dict[str, Any]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {}
    raw = redis_obj.hgetall(_RUNTIME_KEY) or {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Any] = {}
    for key, value in raw.items():
        token = _text(value)
        if not token:
            out[key] = ""
            continue
        try:
            out[key] = float(token)
            continue
        except Exception:
            pass
        low = token.lower()
        if low in _TRUE_TOKENS:
            out[key] = True
        elif low in _FALSE_TOKENS:
            out[key] = False
        else:
            out[key] = token
    return out


def _redis_json_object(raw: Any) -> Optional[Dict[str, Any]]:
    token = _text(raw)
    if not token:
        return None
    try:
        data = json.loads(token)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _integration_runtime_status(client: Any) -> Dict[str, Any]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {}
    raw = redis_obj.hgetall(_INTEGRATION_RUNTIME_STATUS_KEY) or {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Any] = {}
    for key, value in raw.items():
        key_text = _text(key)
        token = _text(value)
        low = token.lower()
        if low in _TRUE_TOKENS:
            out[key_text] = True
            continue
        if low in _FALSE_TOKENS:
            out[key_text] = False
            continue
        try:
            out[key_text] = float(token)
            continue
        except Exception:
            out[key_text] = token
    return out


def _integration_runtime_current_seq(client: Any) -> int:
    redis_obj = client or redis_client
    if redis_obj is None:
        return 0
    return _as_int(redis_obj.get(_INTEGRATION_RUNTIME_EVENT_SEQ_KEY), 0, minimum=0)


def _integration_runtime_events(client: Any, *, after_seq: int, limit: int = 100) -> List[Dict[str, Any]]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return []
    max_rows = max(1, min(1000, int(limit or 100)))
    events: List[Dict[str, Any]] = []
    page_size = min(50, max_rows)
    try:
        for start in range(0, 1000, page_size):
            raw_rows = redis_obj.lrange(
                _INTEGRATION_RUNTIME_EVENTS_KEY,
                start,
                min(999, start + page_size - 1),
            ) or []
            reached_cursor = False
            for raw in raw_rows:
                event = _redis_json_object(raw)
                if not event:
                    continue
                seq = _as_int(event.get("seq"), 0, minimum=0)
                if seq <= after_seq:
                    reached_cursor = True
                    continue
                event["seq"] = seq
                events.append(event)
            if reached_cursor or len(raw_rows) < page_size:
                break
    except Exception:
        logger.debug("[awareness] failed to read integration runtime events", exc_info=True)
        return []
    events.sort(key=lambda item: _as_int(item.get("seq"), 0, minimum=0))
    return events[:max_rows]


def _publish_automation_event(client: Any, *, kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {}
    now_ts = time.time()
    seq = _as_int(redis_obj.incr(_INTEGRATION_RUNTIME_EVENT_SEQ_KEY), 0, minimum=0)
    record = {
        "seq": seq,
        "ts": now_ts,
        "provider": "awareness",
        "kind": _text(kind),
        "payload": payload if isinstance(payload, dict) else {},
    }
    redis_obj.lpush(
        _INTEGRATION_RUNTIME_EVENTS_KEY,
        json.dumps(record, separators=(",", ":"), default=str),
    )
    redis_obj.ltrim(_INTEGRATION_RUNTIME_EVENTS_KEY, 0, 999)
    return record


def _recognized_people_for_identities(client: Any, identity_ids: List[str]) -> List[Dict[str, Any]]:
    identities = _face_identity_rows(client)
    recognized: List[Dict[str, Any]] = []
    seen_people: set[str] = set()
    for identity_id in identity_ids:
        identity = identities.get(_text(identity_id)) or {}
        person_id = _text(identity.get("person_id"))
        person_name = _people_person_name(client, person_id) if person_id else ""
        if not person_id or not person_name or person_id in seen_people:
            continue
        seen_people.add(person_id)
        recognized.append(
            {
                "person_id": person_id,
                "person_name": person_name,
                "face_identity_ids": [
                    _text(candidate_id)
                    for candidate_id in identity_ids
                    if _text((identities.get(_text(candidate_id)) or {}).get("person_id")) == person_id
                ],
            }
        )
    return recognized


def _publish_recognized_person_events(client: Any, session: Dict[str, Any]) -> List[Dict[str, Any]]:
    identity_ids = [_text(value) for value in session.get("identity_ids") or [] if _text(value)]
    recognized = _recognized_people_for_identities(client, identity_ids)
    events: List[Dict[str, Any]] = []
    for row in recognized:
        events.append(
            _publish_automation_event(
                client,
                kind="recognized_person",
                payload={
                    "state": "recognized",
                    "event_type": "recognized_person",
                    "event_id": _text(session.get("event_id")),
                    "face_session_id": _text(session.get("id")),
                    "person_id": _text(row.get("person_id")),
                    "person_name": _text(row.get("person_name")),
                    "face_identity_ids": list(row.get("face_identity_ids") or []),
                    "camera_provider": _text(session.get("provider")),
                    "camera_target": _text(session.get("camera_target")),
                    "camera_id": _text(session.get("camera_target")),
                    "device_ref": _text(session.get("camera_target")),
                    "area": _text(session.get("area")),
                    "recognized_at": _text(session.get("completed_at")) or _now_iso(),
                },
            )
        )
    return [event for event in events if event]




def _integration_runtime_connected(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _bool(value, False)


def _sync_integration_runtime_status(client: Any) -> None:
    status = _integration_runtime_status(client)
    if not status:
        _runtime_set(
            client,
            ws_connected=False,
            unifi_connected=False,
            unifi_ws_connected=False,
            integration_runtime_connected=False,
        )
        return
    last_error = (
        _text(status.get("last_error"))
        or _text(status.get("homeassistant_last_error"))
        or _text(status.get("unifi_protect_last_error"))
    )
    _runtime_set(
        client,
        integration_runtime_connected=_integration_runtime_connected(status.get("running")),
        integration_runtime_last_event_seq=_as_int(status.get("last_event_seq"), 0, minimum=0),
        ws_connected=_integration_runtime_connected(status.get("homeassistant_ws_connected")),
        ws_url=_text(status.get("homeassistant_ws_url")),
        unifi_connected=_integration_runtime_connected(status.get("unifi_protect_connected")),
        unifi_ws_connected=_integration_runtime_connected(status.get("unifi_protect_ws_connected")),
        unifi_ws_url=_text(status.get("unifi_protect_ws_url")),
        last_error=last_error,
    )


def _event_key(source: str) -> str:
    return f"{_EVENTS_PREFIX}{_slug(source or 'general')}"


def _event_snapshot_key(snapshot_id: str) -> str:
    return f"{_EVENT_SNAPSHOT_PREFIX}{_text(snapshot_id)}"


def _event_clip_key(clip_id: str) -> str:
    return f"{_EVENT_CLIP_PREFIX}{_text(clip_id)}"


def _event_clip_meta_key(clip_id: str) -> str:
    return f"{_EVENT_CLIP_META_PREFIX}{_text(clip_id)}"


def _event_clip_blob_client(client: Any) -> Any:
    redis_obj = client or redis_client
    if _shared_redis_blob_client is not None and (client is None or redis_obj is redis_client):
        return _shared_redis_blob_client
    return redis_obj


def _snapshot_storage_enabled(client: Any) -> bool:
    return _bool(_settings(client).get("store_event_snapshots"), True)


def _snapshot_max_bytes(client: Any) -> int:
    kb = _setting_int(client, "event_snapshot_max_kb", 768, minimum=64, maximum=8192)
    return int(kb) * 1024


def _store_event_snapshot(client: Any, image_bytes: bytes, *, content_type: str = "image/jpeg") -> Dict[str, Any]:
    redis_obj = client or redis_client
    size = len(image_bytes or b"")
    if redis_obj is None:
        return {"stored": False, "reason": "redis_unavailable", "bytes": size}
    if not image_bytes:
        return {"stored": False, "reason": "empty_image", "bytes": size}
    if not _snapshot_storage_enabled(redis_obj):
        return {"stored": False, "reason": "disabled", "bytes": size}
    max_bytes = _snapshot_max_bytes(redis_obj)
    if size > max_bytes:
        return {
            "stored": False,
            "reason": "too_large",
            "bytes": size,
            "max_bytes": max_bytes,
        }

    snapshot_id = uuid.uuid4().hex
    payload = {
        "id": snapshot_id,
        "content_type": _text(content_type) or "image/jpeg",
        "encoding": "base64",
        "bytes": size,
        "created_at": _now_iso(),
        "data_b64": base64.b64encode(image_bytes).decode("ascii"),
    }
    key = _event_snapshot_key(snapshot_id)
    try:
        retention = _events_retention_seconds(redis_obj)
        if retention is None:
            redis_obj.set(key, json.dumps(payload))
        else:
            redis_obj.setex(key, max(60, int(retention)), json.dumps(payload))
    except Exception:
        logger.warning("[awareness] failed to store snapshot %s", snapshot_id, exc_info=True)
        return {"stored": False, "reason": "store_failed", "bytes": size}

    return {
        "stored": True,
        "snapshot_id": snapshot_id,
        "bytes": size,
        "content_type": payload["content_type"],
    }


def _clip_storage_enabled(client: Any) -> bool:
    return _bool(_settings(client).get("store_event_clips"), True)


def _clip_max_bytes(client: Any) -> int:
    mb = _setting_int(client, "event_clip_max_mb", 32, minimum=1, maximum=256)
    return int(mb) * 1024 * 1024


def _mp4_top_level_boxes(video_bytes: bytes) -> Dict[str, int]:
    content = bytes(video_bytes or b"")
    total = len(content)
    cursor = 0
    boxes: Dict[str, int] = {}
    while cursor + 8 <= total:
        size = int.from_bytes(content[cursor : cursor + 4], "big")
        box_type = content[cursor + 4 : cursor + 8].decode("ascii", "ignore")
        header_size = 8
        if size == 1:
            if cursor + 16 > total:
                break
            size = int.from_bytes(content[cursor + 8 : cursor + 16], "big")
            header_size = 16
        elif size == 0:
            size = total - cursor
        if size < header_size or cursor + size > total:
            break
        if box_type and box_type not in boxes:
            boxes[box_type] = cursor
        cursor += size
    return boxes


def _mp4_is_fast_start(video_bytes: bytes) -> bool:
    boxes = _mp4_top_level_boxes(video_bytes)
    moov_offset = boxes.get("moov", -1)
    mdat_offset = boxes.get("mdat", -1)
    return moov_offset >= 0 and (mdat_offset < 0 or moov_offset < mdat_offset)


def _event_clip_ffmpeg_path() -> str:
    configured = _text(os.getenv("TATER_FFMPEG_PATH") or os.getenv("FFMPEG_PATH"))
    if configured:
        resolved = shutil.which(configured)
        if resolved:
            return resolved
        candidate = Path(configured).expanduser()
        if candidate.is_file():
            return str(candidate)
    resolved = shutil.which("ffmpeg")
    if resolved:
        return resolved
    try:
        import imageio_ffmpeg

        bundled = _text(imageio_ffmpeg.get_ffmpeg_exe())
    except Exception:
        bundled = ""
    return bundled if bundled and Path(bundled).is_file() else ""


def _prepare_event_clip_for_playback(
    video_bytes: bytes,
    content_type: str,
) -> Tuple[bytes, str, Dict[str, Any]]:
    content = bytes(video_bytes or b"")
    media_type = _text(content_type).split(";", 1)[0].strip().lower() or "video/mp4"
    looks_like_mp4 = media_type in {"video/mp4", "video/m4v"} or content[4:8] == b"ftyp"
    if not content or not looks_like_mp4:
        return content, media_type, {"playback_fast_start": False, "playback_prepared": False}
    if _mp4_is_fast_start(content):
        return content, "video/mp4", {"playback_fast_start": True, "playback_prepared": False}

    ffmpeg = _event_clip_ffmpeg_path()
    if not ffmpeg:
        return content, media_type, {"playback_fast_start": False, "playback_prepared": False}

    try:
        with tempfile.TemporaryDirectory(prefix="tater-awareness-clip-") as temp_dir:
            source_path = Path(temp_dir) / "source.mp4"
            output_path = Path(temp_dir) / "playback.mp4"
            source_path.write_bytes(content)
            completed = subprocess.run(
                [
                    ffmpeg,
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-y",
                    "-i",
                    str(source_path),
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a?",
                    "-c",
                    "copy",
                    "-movflags",
                    "+faststart",
                    str(output_path),
                ],
                capture_output=True,
                timeout=30,
                check=False,
            )
            if completed.returncode != 0 or not output_path.is_file():
                logger.debug(
                    "[awareness] event clip fast-start remux failed: %s",
                    _compact(completed.stderr, limit=240),
                )
                return content, media_type, {
                    "playback_fast_start": False,
                    "playback_prepared": False,
                }
            prepared = output_path.read_bytes()
    except Exception as exc:
        logger.debug("[awareness] event clip fast-start remux failed: %s", exc)
        return content, media_type, {"playback_fast_start": False, "playback_prepared": False}

    if not prepared or not _mp4_is_fast_start(prepared):
        return content, media_type, {"playback_fast_start": False, "playback_prepared": False}
    return prepared, "video/mp4", {
        "playback_fast_start": True,
        "playback_prepared": True,
        "playback_original_bytes": len(content),
    }


def _extract_face_frames_from_clip(
    video_bytes: bytes,
    content_type: str,
    *,
    duration_seconds: float = 0,
    frame_count: int = _FACE_BURST_FRAME_COUNT,
) -> List[bytes]:
    content = bytes(video_bytes or b"")
    target_count = max(1, min(12, int(frame_count or _FACE_BURST_FRAME_COUNT)))
    if not content:
        raise ValueError("The event clip was empty.")
    ffmpeg = _event_clip_ffmpeg_path()
    if not ffmpeg:
        raise RuntimeError("FFmpeg is unavailable for Face ID clip frames.")

    media_type = _text(content_type).split(";", 1)[0].strip().lower()
    suffix = {
        "video/webm": ".webm",
        "video/quicktime": ".mov",
        "video/x-matroska": ".mkv",
        "video/mpeg": ".mpeg",
        "video/x-msvideo": ".avi",
    }.get(media_type, ".mp4")
    duration = max(0.0, _as_float(duration_seconds, 0.0))
    sample_rate = (target_count / duration) if duration > 0 else 1.0

    with tempfile.TemporaryDirectory(prefix="tater-awareness-face-clip-") as temp_dir:
        source_path = Path(temp_dir) / f"source{suffix}"
        output_pattern = Path(temp_dir) / "face-frame-%03d.jpg"
        source_path.write_bytes(content)
        completed = subprocess.run(
            [
                ffmpeg,
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(source_path),
                "-an",
                "-vf",
                f"fps={sample_rate:.6f}",
                "-frames:v",
                str(target_count),
                "-q:v",
                "2",
                str(output_pattern),
            ],
            capture_output=True,
            timeout=30,
            check=False,
        )
        frame_paths = sorted(Path(temp_dir).glob("face-frame-*.jpg"))
        frames = [path.read_bytes() for path in frame_paths if path.stat().st_size > 0]
        if completed.returncode != 0 or not frames:
            detail = _compact(completed.stderr, limit=240)
            raise RuntimeError(detail or "FFmpeg did not extract Face ID frames from the event clip.")
        return frames[:target_count]


def _store_event_clip(client: Any, clip_bytes: bytes, *, content_type: str = "video/mp4") -> Dict[str, Any]:
    redis_obj = client or redis_client
    blob_obj = _event_clip_blob_client(redis_obj)
    size = len(clip_bytes or b"")
    if redis_obj is None or blob_obj is None:
        return {"stored": False, "reason": "redis_unavailable", "bytes": size}
    if not clip_bytes:
        return {"stored": False, "reason": "empty_video", "bytes": size}
    if not _clip_storage_enabled(redis_obj):
        return {"stored": False, "reason": "disabled", "bytes": size}
    max_bytes = _clip_max_bytes(redis_obj)
    if size > max_bytes:
        return {
            "stored": False,
            "reason": "too_large",
            "bytes": size,
            "max_bytes": max_bytes,
        }

    clip_id = uuid.uuid4().hex
    media_type = _text(content_type).split(";", 1)[0].strip().lower()
    if not media_type.startswith("video/"):
        media_type = "video/mp4"
    metadata = {
        "id": clip_id,
        "content_type": media_type,
        "bytes": size,
        "created_at": _now_iso(),
    }
    clip_key = _event_clip_key(clip_id)
    meta_key = _event_clip_meta_key(clip_id)
    try:
        retention = _events_retention_seconds(redis_obj)
        if retention is None:
            blob_obj.set(clip_key, bytes(clip_bytes))
            redis_obj.set(meta_key, json.dumps(metadata))
        else:
            ttl = max(60, int(retention))
            blob_obj.setex(clip_key, ttl, bytes(clip_bytes))
            redis_obj.setex(meta_key, ttl, json.dumps(metadata))
    except Exception:
        logger.warning("[awareness] failed to store event clip %s", clip_id, exc_info=True)
        try:
            blob_obj.delete(clip_key)
            redis_obj.delete(meta_key)
        except Exception:
            pass
        return {"stored": False, "reason": "store_failed", "bytes": size}

    return {
        "stored": True,
        "clip_id": clip_id,
        "bytes": size,
        "content_type": media_type,
    }


_PEOPLE_API_MODULE: Any = None
_PEOPLE_API_UNAVAILABLE = False
_FACE_PEOPLE_ALIAS_PLATFORM = "face_id"


def _people_api_module() -> Any:
    global _PEOPLE_API_MODULE, _PEOPLE_API_UNAVAILABLE
    if _PEOPLE_API_MODULE is not None:
        return _PEOPLE_API_MODULE
    if _PEOPLE_API_UNAVAILABLE:
        return None
    try:
        import people as people_module  # type: ignore

        _PEOPLE_API_MODULE = people_module
        return _PEOPLE_API_MODULE
    except Exception:
        pass
    try:
        candidate = Path(__file__).resolve().parents[2] / "Tater" / "people.py"
        if candidate.exists():
            spec = importlib.util.spec_from_file_location("tater_people_api_for_awareness", candidate)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                _PEOPLE_API_MODULE = module
                return _PEOPLE_API_MODULE
    except Exception:
        pass
    _PEOPLE_API_UNAVAILABLE = True
    return None


def _people_person_rows(client: Any) -> List[Dict[str, Any]]:
    module = _people_api_module()
    load_store = getattr(module, "load_store", None) if module is not None else None
    if not callable(load_store):
        return []
    try:
        store = load_store(client or redis_client)
    except Exception:
        return []
    people = list(store.get("people") or []) if isinstance(store, dict) else []
    rows = [dict(row) for row in people if isinstance(row, dict)]
    rows.sort(key=lambda row: (_text(row.get("display_name")).casefold(), _text(row.get("id"))))
    return rows


def _people_person_name(client: Any, person_id: Any) -> str:
    wanted = _text(person_id)
    if not wanted:
        return ""
    for person in _people_person_rows(client):
        if _text(person.get("id")) == wanted:
            return _text(person.get("display_name"))
    return ""


def _people_person_options(client: Any) -> List[Dict[str, str]]:
    options = [{"value": "", "label": "Not linked to a Tater Person"}]
    for person in _people_person_rows(client):
        person_id = _text(person.get("id"))
        display_name = _text(person.get("display_name"))
        if person_id and display_name:
            options.append({"value": person_id, "label": display_name})
    return options


def _people_attach_face_identity(client: Any, *, person_id: str, identity_id: str, label: str) -> None:
    module = _people_api_module()
    attach_alias = getattr(module, "attach_alias", None) if module is not None else None
    if not callable(attach_alias):
        raise ValueError("The People API is unavailable.")
    attach_alias(
        person_id=person_id,
        platform=_FACE_PEOPLE_ALIAS_PLATFORM,
        external_id=identity_id,
        label=label or identity_id,
        kind="face_identity",
        redis_client=client or redis_client,
    )


def _people_detach_face_identity(client: Any, *, person_id: str, identity_id: str) -> None:
    module = _people_api_module()
    detach_alias = getattr(module, "detach_alias", None) if module is not None else None
    if not callable(detach_alias) or not person_id or not identity_id:
        return
    try:
        detach_alias(
            person_id=person_id,
            platform=_FACE_PEOPLE_ALIAS_PLATFORM,
            external_id=identity_id,
            redis_client=client or redis_client,
        )
    except KeyError:
        pass


def _face_identity_display_name(client: Any, identity: Dict[str, Any]) -> str:
    person_id = _text(identity.get("person_id"))
    linked_name = _people_person_name(client, person_id) if person_id else ""
    return linked_name or _text(identity.get("name") or identity.get("person_name"))


def _face_runtime_status(client: Any) -> Dict[str, Any]:
    if _face_id_runtime is None:
        return {
            "enabled": False,
            "loaded": False,
            "loading": False,
            "state": "unavailable",
            "error": "This Tater version does not provide the Face ID runtime.",
            "model": "Facenet512",
        }
    try:
        return dict(_face_id_runtime.status(client) or {})
    except Exception as exc:
        return {
            "enabled": False,
            "loaded": False,
            "loading": False,
            "state": "error",
            "error": str(exc),
            "model": "Facenet512",
        }


def _face_id_enabled(client: Any) -> bool:
    return bool(_face_runtime_status(client).get("enabled"))


def _face_identity_rows(client: Any, *, cleanup: bool = False) -> Dict[str, Dict[str, Any]]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {}
    try:
        raw_rows = redis_obj.hgetall(_FACE_IDENTITIES_KEY) or {}
    except Exception:
        return {}
    rows: Dict[str, Dict[str, Any]] = {}
    for raw_id, raw_payload in raw_rows.items():
        identity_id = _text(raw_id)
        try:
            payload = json.loads(raw_payload) if isinstance(raw_payload, (str, bytes, bytearray)) else raw_payload
        except Exception:
            continue
        if identity_id and isinstance(payload, dict):
            payload["id"] = identity_id
            rows[identity_id] = payload

    if cleanup:
        retention = _events_retention_seconds(redis_obj)
        if retention is not None:
            cutoff = datetime.now() - timedelta(seconds=max(60, int(retention)))
            for identity_id, payload in list(rows.items()):
                if _text(payload.get("name")):
                    continue
                last_seen = _parse_iso(payload.get("last_seen"))
                if last_seen is not None and last_seen < cutoff:
                    try:
                        redis_obj.hdel(_FACE_IDENTITIES_KEY, identity_id)
                    except Exception:
                        pass
                    rows.pop(identity_id, None)
    return rows


def _save_face_identity(client: Any, identity: Dict[str, Any]) -> Dict[str, Any]:
    redis_obj = client or redis_client
    identity_id = _text(identity.get("id"))
    if redis_obj is None or not identity_id:
        raise ValueError("Face identity cannot be stored.")
    payload = dict(identity)
    payload["id"] = identity_id
    redis_obj.hset(_FACE_IDENTITIES_KEY, identity_id, json.dumps(payload))
    return payload


def _face_session_key(session_id: str) -> str:
    return f"{_FACE_SESSION_PREFIX}{_text(session_id)}"


def _save_face_session(client: Any, session: Dict[str, Any]) -> Dict[str, Any]:
    redis_obj = client or redis_client
    session_id = _text(session.get("id"))
    if redis_obj is None or not session_id:
        return session
    payload = dict(session)
    payload["id"] = session_id
    retention = _events_retention_seconds(redis_obj)
    try:
        if retention is None:
            redis_obj.set(_face_session_key(session_id), json.dumps(payload))
        else:
            redis_obj.setex(_face_session_key(session_id), max(60, int(retention)), json.dumps(payload))
    except Exception:
        logger.warning("[awareness] failed to store Face ID session %s", session_id, exc_info=True)
    return payload


def _load_face_session(client: Any, session_id: Any) -> Dict[str, Any]:
    redis_obj = client or redis_client
    token = _text(session_id)
    if redis_obj is None or not token:
        return {}
    try:
        raw = redis_obj.get(_face_session_key(token))
        payload = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _face_cosine_distance(left: List[float], right: List[float]) -> float:
    if not left or not right or len(left) != len(right):
        return float("inf")
    dot = sum(float(a) * float(b) for a, b in zip(left, right))
    left_norm = math.sqrt(sum(float(value) * float(value) for value in left))
    right_norm = math.sqrt(sum(float(value) * float(value) for value in right))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return float("inf")
    return 1.0 - (dot / (left_norm * right_norm))


def _face_valid_embedding(raw: Any, dimensions: int = 0) -> List[float]:
    if not isinstance(raw, list) or not raw:
        return []
    try:
        embedding = [float(value) for value in raw]
    except (TypeError, ValueError):
        return []
    if dimensions and len(embedding) != dimensions:
        return []
    return embedding


def _face_reference_embeddings(identity: Dict[str, Any]) -> List[List[float]]:
    """Return the already-curated match set, with legacy identity fallbacks."""
    stored = identity.get("reference_centroids")
    references: List[List[float]] = []
    if isinstance(stored, list):
        for raw in stored:
            embedding = _face_valid_embedding(raw, len(references[0]) if references else 0)
            if not embedding:
                continue
            if any(_face_cosine_distance(embedding, existing) < 0.0001 for existing in references):
                continue
            references.append(embedding)
            if len(references) >= _FACE_REFERENCE_LIMIT:
                break
    if references:
        return references

    # Older identities predate curated references. Keep them matchable until the
    # next accepted sighting migrates them to the rotating reference set.
    fallback: List[Any] = []
    anchors = identity.get("anchor_references")
    if isinstance(anchors, list):
        fallback.extend(anchors)
    centroid = identity.get("centroid")
    if isinstance(centroid, list):
        fallback.append(centroid)
    fallback.extend(
        row.get("embedding")
        for row in _face_observations(identity)
        if isinstance(row.get("embedding"), list)
    )
    for raw in fallback:
        embedding = _face_valid_embedding(raw, len(references[0]) if references else 0)
        if not embedding:
            continue
        if any(_face_cosine_distance(embedding, existing) < 0.0001 for existing in references):
            continue
        references.append(embedding)
        if len(references) >= _FACE_REFERENCE_LIMIT:
            break
    return references


def _curate_face_reference_embeddings(
    identity: Dict[str, Any],
    *,
    extra_references: Optional[List[List[float]]] = None,
    limit: int = _FACE_REFERENCE_LIMIT,
) -> List[List[float]]:
    """Choose a bounded set of clear, visually distinct face profiles."""
    maximum = max(1, int(limit))
    best_quality = max(1.0, _as_float(identity.get("best_quality"), 0.0))
    candidates: List[Dict[str, Any]] = []

    def add(raw: Any, *, quality: float, seen_at: Any = "", anchor: bool = False) -> None:
        embedding = _face_valid_embedding(raw, len(candidates[0]["embedding"]) if candidates else 0)
        if not embedding:
            return
        candidate = {
            "embedding": embedding,
            "quality": max(0.0, float(quality)),
            "seen_at": _text(seen_at),
            "anchor": bool(anchor),
        }
        for index, existing in enumerate(candidates):
            if _face_cosine_distance(embedding, existing["embedding"]) >= 0.005:
                continue
            # A clearer capture replaces a redundant older profile. Preserve the
            # anchor flag when either copy represents a confirmed legacy view.
            candidate["anchor"] = bool(candidate["anchor"] or existing["anchor"])
            if (candidate["quality"], candidate["seen_at"]) >= (
                existing["quality"],
                existing["seen_at"],
            ):
                candidates[index] = candidate
            else:
                existing["anchor"] = candidate["anchor"]
            return
        candidates.append(candidate)

    for raw in identity.get("anchor_references") or []:
        add(raw, quality=best_quality, anchor=True)
    for raw in extra_references or []:
        add(raw, quality=1.0)
    centroid = identity.get("centroid")
    if isinstance(centroid, list):
        add(centroid, quality=1.0)
    for row in _face_observations(identity):
        add(
            row.get("embedding"),
            quality=_as_float(row.get("quality"), 0.0),
            seen_at=row.get("seen_at"),
        )

    if len(candidates) <= maximum:
        return [row["embedding"] for row in candidates]

    anchors = [row for row in candidates if row["anchor"]]
    seed_pool = anchors or candidates
    seed = max(seed_pool, key=lambda row: (row["quality"], row["seen_at"]))
    selected = [seed]
    remaining = [row for row in candidates if row is not seed]
    while remaining and len(selected) < maximum:
        def selection_score(row: Dict[str, Any]) -> Tuple[float, float, str]:
            diversity = min(
                _face_cosine_distance(row["embedding"], chosen["embedding"])
                for chosen in selected
            )
            quality = min(1.0, max(0.0, row["quality"] / 3.0))
            return ((0.70 * min(1.0, diversity)) + (0.30 * quality), row["quality"], row["seen_at"])

        chosen = max(remaining, key=selection_score)
        selected.append(chosen)
        remaining.remove(chosen)
    return [row["embedding"] for row in selected]


def _face_match_identity(
    identities: Dict[str, Dict[str, Any]],
    embedding: List[float],
    *,
    threshold: float,
) -> Tuple[str, float]:
    best_id = ""
    best_distance = float("inf")
    for identity_id, identity in identities.items():
        references = _face_reference_embeddings(identity)
        distance = min(
            (_face_cosine_distance(embedding, reference) for reference in references),
            default=float("inf"),
        )
        if distance < best_distance:
            best_id = identity_id
            best_distance = distance
    if not best_id or best_distance > float(threshold):
        return "", best_distance
    return best_id, best_distance


def _face_observations(identity: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in identity.get("observations") or []:
        if not isinstance(raw, dict):
            continue
        observation_id = _text(raw.get("id"))
        if not observation_id or observation_id in seen:
            continue
        seen.add(observation_id)
        row = dict(raw)
        row["id"] = observation_id
        rows.append(row)
    rows.sort(key=lambda row: (_text(row.get("seen_at")), _text(row.get("id"))), reverse=True)
    return rows[:_FACE_OBSERVATION_LIMIT]


def _face_detection_observation(
    detection: Dict[str, Any],
    *,
    embedding: List[float],
    event_id: str,
    seen_at: str,
    quality: float,
) -> Dict[str, Any]:
    area = detection.get("facial_area") if isinstance(detection.get("facial_area"), dict) else {}
    return {
        "id": f"observation_{uuid.uuid4().hex[:20]}",
        "event_id": _text(event_id),
        "seen_at": _text(seen_at) or _now_iso(),
        "embedding": [float(value) for value in embedding],
        "confidence": round(_as_float(detection.get("confidence"), 0.0), 6),
        "quality": round(max(0.0, float(quality)), 6),
        "facial_area": {
            "x": _as_int(area.get("x"), 0, minimum=0),
            "y": _as_int(area.get("y"), 0, minimum=0),
            "w": _as_int(area.get("w"), 0, minimum=0),
            "h": _as_int(area.get("h"), 0, minimum=0),
        },
        "face_b64": _text(detection.get("crop_b64")),
        "face_content_type": _text(detection.get("crop_content_type") or "image/jpeg"),
    }


def _rebuild_face_identity_from_observations(
    identity: Dict[str, Any],
    observations: List[Dict[str, Any]],
    *,
    keep_name: bool,
) -> Dict[str, Any]:
    payload = dict(identity)
    normalized = _face_observations({"observations": observations})
    payload["observations"] = normalized
    payload["observation_count"] = len(normalized)
    embeddings = [
        [float(value) for value in row.get("embedding")]
        for row in normalized
        if isinstance(row.get("embedding"), list) and row.get("embedding")
    ]
    dimensions = len(embeddings[0]) if embeddings else 0
    embeddings = [row for row in embeddings if len(row) == dimensions]
    if embeddings and dimensions:
        payload["centroid"] = [
            sum(row[index] for row in embeddings) / len(embeddings)
            for index in range(dimensions)
        ]
        payload["centroid_count"] = len(embeddings)
        retained_references = payload.get("anchor_references") if keep_name else []
        payload["reference_centroids"] = _curate_face_reference_embeddings(
            {
                "anchor_references": retained_references,
                "centroid": payload["centroid"],
                "observations": normalized,
                "best_quality": payload.get("best_quality"),
            }
        )
    elif keep_name and isinstance(payload.get("anchor_references"), list):
        payload["reference_centroids"] = _curate_face_reference_embeddings(
            {
                "anchor_references": payload.get("anchor_references"),
                "best_quality": payload.get("best_quality"),
            }
        )
    event_ids = {_text(row.get("event_id")) for row in normalized if _text(row.get("event_id"))}
    payload["event_count"] = len(event_ids)
    if normalized:
        chronological = sorted(normalized, key=lambda row: (_text(row.get("seen_at")), _text(row.get("id"))))
        payload["first_seen"] = _text(chronological[0].get("seen_at"))
        payload["last_seen"] = _text(chronological[-1].get("seen_at"))
        payload["last_event_id"] = _text(chronological[-1].get("event_id"))
        best = max(normalized, key=lambda row: _as_float(row.get("quality"), 0.0))
        if _text(best.get("face_b64")):
            payload["best_quality"] = _as_float(best.get("quality"), 0.0)
            payload["face_b64"] = _text(best.get("face_b64"))
            payload["face_content_type"] = _text(best.get("face_content_type") or "image/jpeg")
    if not keep_name:
        payload["name"] = ""
    payload["updated_at"] = _now_iso()
    return payload


def _record_face_detection(
    client: Any,
    detection: Dict[str, Any],
    *,
    event_id: str,
    seen_at: str,
) -> Dict[str, Any]:
    embedding_raw = detection.get("embedding")
    if not isinstance(embedding_raw, list) or not embedding_raw:
        raise ValueError("Face result did not include an embedding.")
    embedding = [float(value) for value in embedding_raw]
    area = detection.get("facial_area") if isinstance(detection.get("facial_area"), dict) else {}
    confidence = _as_float(detection.get("confidence"), 0.0)
    area_pixels = max(1, _as_int(area.get("w"), 1, minimum=1) * _as_int(area.get("h"), 1, minimum=1))
    quality = max(0.0, confidence) + min(2.0, area_pixels / 100_000.0)
    threshold = _as_float(getattr(_face_id_runtime, "MATCH_THRESHOLD", 0.30), 0.30)

    with _FACE_IDENTITY_LOCK:
        identities = _face_identity_rows(client, cleanup=True)
        identity_id, distance = _face_match_identity(identities, embedding, threshold=threshold)
        identity = dict(identities.get(identity_id) or {})
        created_identity = not identity_id
        if not identity_id:
            identity_id = f"face_{uuid.uuid4().hex[:16]}"
            identity = {
                "id": identity_id,
                "name": "",
                "created_at": seen_at,
                "first_seen": seen_at,
                "observation_count": 0,
                "event_count": 0,
                "centroid": embedding,
                "centroid_count": 0,
                "reference_centroids": [embedding],
                "best_quality": 0.0,
            }
            distance = 0.0

        existing_observations = _face_observations(identity)
        if not created_identity and not existing_observations and not identity.get("anchor_references"):
            legacy_references = _face_reference_embeddings(identity)
            if legacy_references:
                identity["anchor_references"] = legacy_references

        centroid = identity.get("centroid") if isinstance(identity.get("centroid"), list) else embedding
        centroid_count = _as_int(identity.get("centroid_count"), 0, minimum=0)
        if centroid_count <= 0 or len(centroid) != len(embedding):
            next_centroid = embedding
            next_count = 1
        else:
            next_count = centroid_count + 1
            next_centroid = [
                ((float(old) * centroid_count) + float(new)) / next_count
                for old, new in zip(centroid, embedding)
            ]
        identity["centroid"] = next_centroid
        identity["centroid_count"] = next_count
        identity["observation_count"] = _as_int(identity.get("observation_count"), 0, minimum=0) + 1
        if _text(identity.get("last_event_id")) != event_id:
            identity["event_count"] = _as_int(identity.get("event_count"), 0, minimum=0) + 1
            identity["last_event_id"] = event_id
        identity["last_seen"] = seen_at
        identity["last_distance"] = round(max(0.0, float(distance)), 5)
        observations = existing_observations
        observations.insert(
            0,
            _face_detection_observation(
                detection,
                embedding=embedding,
                event_id=event_id,
                seen_at=seen_at,
                quality=quality,
            ),
        )
        identity["observations"] = observations[:_FACE_OBSERVATION_LIMIT]
        if quality >= _as_float(identity.get("best_quality"), 0.0) and _text(detection.get("crop_b64")):
            identity["best_quality"] = round(quality, 5)
            identity["face_b64"] = _text(detection.get("crop_b64"))
            identity["face_content_type"] = _text(detection.get("crop_content_type") or "image/jpeg")
        identity["reference_centroids"] = _curate_face_reference_embeddings(identity)
        return _save_face_identity(client, identity)


def _face_event_context(client: Any, event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    session_id = _text(data.get("face_session_id"))
    if not session_id:
        return {}
    session = _load_face_session(client, session_id)
    identity_ids = [
        token
        for token in [_text(value) for value in session.get("identity_ids") or []]
        if token
    ]
    identities = _face_identity_rows(client)
    known_people: List[str] = []
    recognized_people: List[str] = []
    recognized_person_ids: List[str] = []
    unknown_count = 0
    for identity_id in identity_ids:
        identity = identities.get(identity_id) or {}
        person_id = _text(identity.get("person_id"))
        linked_name = _people_person_name(client, person_id) if person_id else ""
        name = linked_name or _text(identity.get("name") or identity.get("person_name"))
        if name and name.casefold() not in {item.casefold() for item in known_people}:
            known_people.append(name)
        elif not name:
            unknown_count += 1
        if person_id and linked_name and person_id not in recognized_person_ids:
            recognized_person_ids.append(person_id)
            recognized_people.append(linked_name)
    return {
        "face_session_id": session_id,
        "face_status": _text(session.get("status") or "pending"),
        "face_count": len(identity_ids),
        "face_identity_ids": identity_ids,
        "known_people": known_people,
        "recognized_people": recognized_people,
        "recognized_person_ids": recognized_person_ids,
        "unknown_face_count": unknown_count,
        "face_error": _text(session.get("error")),
    }


def _refresh_stored_face_events(client: Any, *, event_id: str = "") -> int:
    redis_obj = client or redis_client
    if redis_obj is None:
        return 0
    try:
        keys = list(redis_obj.scan_iter(match=f"{_EVENTS_PREFIX}*"))
    except Exception:
        return 0
    updated = 0
    for raw_key in keys:
        key = _text(raw_key)
        try:
            rows = redis_obj.lrange(key, 0, -1) or []
        except Exception:
            continue
        for index, raw_row in enumerate(rows):
            try:
                event = json.loads(raw_row) if isinstance(raw_row, (str, bytes, bytearray)) else raw_row
            except Exception:
                continue
            if not isinstance(event, dict):
                continue
            if event_id and _text(event.get("id")) != event_id:
                continue
            data = event.get("data") if isinstance(event.get("data"), dict) else {}
            if not _text(data.get("face_session_id")):
                continue
            context = _face_event_context(redis_obj, event)
            if not context:
                continue
            next_data = dict(data)
            for field in (
                "face_status",
                "face_count",
                "face_identity_ids",
                "known_people",
                "recognized_people",
                "recognized_person_ids",
                "unknown_face_count",
            ):
                next_data[field] = context.get(field)
            if context.get("face_error"):
                next_data["face_error"] = context.get("face_error")
            else:
                next_data.pop("face_error", None)
            event["data"] = next_data
            try:
                redis_obj.lset(key, index, json.dumps(event))
            except Exception:
                continue
            updated += 1
            if event_id:
                return updated
    return updated


async def _run_face_burst(
    *,
    session: Dict[str, Any],
    provider: str,
    camera_target: str,
    initial_image: bytes,
    initial_content_type: str,
    video_bytes: bytes = b"",
    video_content_type: str = "video/mp4",
    video_duration_seconds: float = 0,
) -> None:
    del initial_content_type
    identity_ids: List[str] = []
    errors: List[str] = []
    frames_checked = 0
    faces_detected = 0
    frames: List[bytes] = []
    if video_bytes:
        session["status"] = "extracting"
        session["frame_source"] = "video_clip"
        _save_face_session(redis_client, session)
        try:
            frames = await asyncio.to_thread(
                _extract_face_frames_from_clip,
                video_bytes,
                video_content_type,
                duration_seconds=video_duration_seconds,
                frame_count=_FACE_BURST_FRAME_COUNT,
            )
        except Exception as exc:
            errors.append(_compact(str(exc), limit=180))
            session["clip_frame_error"] = errors[-1]
        session["frames_captured"] = len(frames)
        _save_face_session(redis_client, session)

    if not frames:
        session["status"] = "capturing"
        session["frame_source"] = "snapshot_burst"
        frames = [initial_image] if initial_image else []
        session["frames_captured"] = len(frames)
        _save_face_session(redis_client, session)

        capture_started = time.monotonic()
        for frame_index in range(len(frames), _FACE_BURST_FRAME_COUNT):
            if not _face_id_enabled(redis_client):
                session["status"] = "disabled"
                session["error"] = "Face ID was disabled before analysis completed."
                break
            capture_at = capture_started + (frame_index * _FACE_BURST_INTERVAL_SECONDS)
            await asyncio.sleep(max(0.0, capture_at - time.monotonic()))
            try:
                image_bytes, _content_type = await _capture_camera_snapshot(provider, camera_target)
                if image_bytes:
                    frames.append(image_bytes)
            except Exception as exc:
                errors.append(_compact(str(exc), limit=180))
            session["frames_captured"] = len(frames)
            _save_face_session(redis_client, session)

    if session.get("status") != "disabled":
        session["status"] = "analyzing"
        session["frames_captured"] = len(frames)
        _save_face_session(redis_client, session)

    for image_bytes in frames:
        if not _face_id_enabled(redis_client):
            session["status"] = "disabled"
            session["error"] = "Face ID was disabled before analysis completed."
            break
        frames_checked += 1
        try:
            detections = await asyncio.to_thread(_face_id_runtime.analyze_image, image_bytes, redis_client)
        except Exception as exc:
            errors.append(_compact(str(exc), limit=180))
            continue
        for detection in detections or []:
            if not isinstance(detection, dict):
                continue
            try:
                identity = _record_face_detection(
                    redis_client,
                    detection,
                    event_id=_text(session.get("event_id")),
                    seen_at=_now_iso(),
                )
            except Exception as exc:
                errors.append(_compact(str(exc), limit=180))
                continue
            faces_detected += 1
            identity_id = _text(identity.get("id"))
            if identity_id and identity_id not in identity_ids:
                identity_ids.append(identity_id)
        session.update(
            {
                "identity_ids": identity_ids,
                "frames_checked": frames_checked,
                "faces_detected": faces_detected,
                "frames_total": len(frames),
            }
        )
        _save_face_session(redis_client, session)

    if session.get("status") != "disabled":
        session["status"] = "complete" if identity_ids else ("error" if errors and frames_checked == 0 else "no_faces")
    session["identity_ids"] = identity_ids
    session["frames_checked"] = frames_checked
    session["faces_detected"] = faces_detected
    session["completed_at"] = _now_iso()
    if errors:
        session["error"] = errors[-1]
        session["error_count"] = len(errors)
    recognized = _recognized_people_for_identities(redis_client, identity_ids)
    session["recognized_person_ids"] = [_text(row.get("person_id")) for row in recognized]
    session["recognized_people"] = [_text(row.get("person_name")) for row in recognized]
    _save_face_session(redis_client, session)
    _refresh_stored_face_events(redis_client, event_id=_text(session.get("event_id")))
    if session.get("status") == "complete" and recognized:
        emitted = _publish_recognized_person_events(redis_client, session)
        session["automation_events_emitted"] = len(emitted)
        _save_face_session(redis_client, session)
    await _dispatch_face_session_notification(session)


async def _dispatch_face_session_notification(session: Dict[str, Any]) -> None:
    monitor_id = _text(session.get("monitor_id"))
    if not monitor_id:
        return
    try:
        monitor = _get_monitor(redis_client, monitor_id)
        event = _load_stored_event_by_id(
            redis_client,
            source=_text(session.get("area")),
            event_id=_text(session.get("event_id")),
        )
        if monitor and event:
            await _deliver_awareness_event_notification(monitor, event)
    except Exception:
        logger.exception(
            "[awareness] failed to deliver the post-Face-ID notification for monitor %s",
            monitor_id,
        )


def _schedule_face_burst(
    *,
    event_id: str,
    provider: str,
    camera_target: str,
    area: str,
    initial_image: bytes,
    initial_content_type: str,
    video_bytes: bytes = b"",
    video_content_type: str = "video/mp4",
    video_duration_seconds: float = 0,
    monitor_id: str = "",
) -> str:
    if (not initial_image and not video_bytes) or not _face_id_enabled(redis_client) or _face_id_runtime is None:
        return ""
    camera_key = f"{provider}:{camera_target}"
    active = _FACE_BURST_BY_CAMERA.get(camera_key)
    if active is not None and not active.done():
        return ""
    session_id = event_id or uuid.uuid4().hex
    session = {
        "id": session_id,
        "event_id": event_id,
        "monitor_id": _text(monitor_id),
        "area": area,
        "provider": provider,
        "camera_target": camera_target,
        "status": "pending",
        "identity_ids": [],
        "frames_checked": 0,
        "frames_total": _FACE_BURST_FRAME_COUNT,
        "frame_source": "video_clip" if video_bytes else "snapshot_burst",
        "created_at": _now_iso(),
    }
    _save_face_session(redis_client, session)
    task = asyncio.create_task(
        _run_face_burst(
            session=session,
            provider=provider,
            camera_target=camera_target,
            initial_image=initial_image,
            initial_content_type=initial_content_type,
            video_bytes=video_bytes,
            video_content_type=video_content_type,
            video_duration_seconds=video_duration_seconds,
        )
    )
    _FACE_BURST_TASKS.add(task)
    _FACE_BURST_BY_CAMERA[camera_key] = task

    def _done(completed: Any) -> None:
        _FACE_BURST_TASKS.discard(completed)
        if _FACE_BURST_BY_CAMERA.get(camera_key) is completed:
            _FACE_BURST_BY_CAMERA.pop(camera_key, None)
        try:
            completed.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.exception("[awareness] Face ID burst failed for %s", camera_key)
            failed_session = _load_face_session(redis_client, session_id) or dict(session)
            failed_session["status"] = "error"
            failed_session["error"] = _compact(str(exc), limit=180) or "Face ID analysis failed."
            failed_session["completed_at"] = _now_iso()
            _save_face_session(redis_client, failed_session)
            _refresh_stored_face_events(redis_client, event_id=event_id)
            fallback = asyncio.create_task(_dispatch_face_session_notification(failed_session))
            _FACE_BURST_TASKS.add(fallback)
            fallback.add_done_callback(_FACE_BURST_TASKS.discard)

    task.add_done_callback(_done)
    return session_id


def _face_burst_should_run(event_kind: Any, vision_summary: Any) -> bool:
    kind = _text(event_kind).lower()
    if kind in _FACE_ALWAYS_BURST_EVENTS:
        return True
    describes_person = bool(_FACE_HUMAN_SUMMARY_RE.search(_text(vision_summary)))
    if kind in _FACE_VISION_GATED_EVENTS:
        return describes_person
    return describes_person


def _trim_events_for_source(client: Any, source: str) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    retention = _events_retention_seconds(redis_obj)
    if retention is None:
        return
    cutoff = datetime.now() - timedelta(seconds=retention)
    key = _event_key(source)
    try:
        raw = redis_obj.lrange(key, 0, -1) or []
    except Exception:
        return

    keep_through = -1
    for index, row in enumerate(raw):
        try:
            payload = json.loads(row)
            ts = _parse_iso(payload.get("ha_time"))
            if ts and ts >= cutoff:
                keep_through = index
        except Exception:
            # Preserve malformed rows rather than deleting potentially useful
            # event data during a retention pass.
            keep_through = index

    if keep_through >= len(raw) - 1:
        return

    try:
        if keep_through < 0:
            redis_obj.delete(key)
        else:
            # Events are LPUSHed newest-first, so expired rows accumulate at
            # the tail. LTRIM removes that tail without rewriting every
            # retained event into Redis's append-only log.
            redis_obj.ltrim(key, 0, keep_through)
    except Exception:
        logger.debug("[awareness] failed to trim events for %s", key, exc_info=True)


def _append_event(client: Any, *, source: str, payload: Dict[str, Any]) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    key = _event_key(source)
    try:
        redis_obj.lpush(key, json.dumps(payload))
    except Exception:
        logger.warning("[awareness] failed to append event for %s", key, exc_info=True)
        return
    # A newly recorded event belongs at the front of history. Do not leave the
    # manager parked on an older persisted page where the event looks missing.
    _runtime_set(redis_obj, events_page=1)
    _trim_events_for_source(redis_obj, source)


def _monitor_string_list(value: Any) -> List[str]:
    raw = value
    if isinstance(raw, str):
        token = raw.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                raw = json.loads(token)
            except Exception:
                raw = [token]
        elif token:
            raw = [token]
        else:
            raw = []
    if isinstance(raw, (tuple, set)):
        raw = list(raw)
    if not isinstance(raw, list):
        raw = [] if raw in (None, "") else [raw]
    out: List[str] = []
    seen: set[str] = set()
    for item in raw:
        token = _text(item)
        if not token or token.casefold() in seen:
            continue
        seen.add(token.casefold())
        out.append(token)
    return out


def _encode_notification_destination(platform: Any, targets: Any = None) -> str:
    platform_name = _text(platform).strip().lower()
    if not platform_name:
        return ""
    target_map = targets if isinstance(targets, dict) else {}
    cleaned_targets = {
        _text(key): _text(value)
        for key, value in target_map.items()
        if _text(key) and _text(value)
    }
    return json.dumps(
        {"platform": platform_name, "targets": cleaned_targets},
        sort_keys=True,
        separators=(",", ":"),
    )


def _decode_notification_destination(value: Any) -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(_text(value))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    platform = _text(payload.get("platform")).strip().lower()
    if not platform:
        return None
    raw_targets = payload.get("targets") if isinstance(payload.get("targets"), dict) else {}
    targets = {
        _text(key): _text(item)
        for key, item in raw_targets.items()
        if _text(key) and _text(item)
    }
    return {"platform": platform, "targets": targets}


def _normalize_notification_destinations(value: Any) -> List[str]:
    rows: List[str] = []
    seen: set[str] = set()
    for raw in _monitor_string_list(value):
        destination = _decode_notification_destination(raw)
        if not destination:
            continue
        encoded = _encode_notification_destination(
            destination.get("platform"),
            destination.get("targets"),
        )
        if not encoded or encoded in seen:
            continue
        seen.add(encoded)
        rows.append(encoded)
    return rows


def _notification_destination_label(platform: str, targets: Dict[str, Any]) -> str:
    for key in (
        "label",
        "device_name",
        "device_id",
        "channel",
        "channel_id",
        "room_alias",
        "room_id",
        "chat_id",
        "service",
        "device_service",
        "scope",
    ):
        value = _text(targets.get(key))
        if value:
            return value
    return "Defaults"


def _notification_destination_options(client: Any, current_values: Any = None) -> List[Dict[str, str]]:
    if not callable(notifier_destination_catalog):
        return []
    try:
        catalog = notifier_destination_catalog(redis_client=client or redis_client, limit=250)
    except Exception:
        logger.debug("[awareness] notification destination discovery failed", exc_info=True)
        catalog = {"platforms": []}
    options: List[Dict[str, str]] = []
    seen: set[str] = set()
    for platform_row in catalog.get("platforms") or []:
        if not isinstance(platform_row, dict):
            continue
        platform = _text(platform_row.get("platform")).strip().lower()
        platform_label = _text(platform_row.get("label")) or platform.replace("_", " ").title()
        if not platform:
            continue
        if not _bool(platform_row.get("requires_target"), False):
            value = _encode_notification_destination(platform, {})
            options.append({"value": value, "label": f"{platform_label}: defaults"})
            seen.add(value)
        for destination in platform_row.get("destinations") or []:
            if not isinstance(destination, dict):
                continue
            targets = destination.get("targets") if isinstance(destination.get("targets"), dict) else {}
            value = _encode_notification_destination(platform, targets)
            if not value or value in seen:
                continue
            label = _text(destination.get("label")) or _notification_destination_label(platform, targets)
            options.append({"value": value, "label": f"{platform_label}: {label}"})
            seen.add(value)
    for saved in _normalize_notification_destinations(current_values):
        if saved in seen:
            continue
        destination = _decode_notification_destination(saved) or {}
        platform = _text(destination.get("platform"))
        label = _notification_destination_label(
            platform,
            destination.get("targets") if isinstance(destination.get("targets"), dict) else {},
        )
        options.append(
            {
                "value": saved,
                "label": f"{platform.replace('_', ' ').title()}: {label} (saved)",
            }
        )
        seen.add(saved)
    return options


def _normalize_monitor(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    kind = _text(raw.get("kind")).lower()
    if kind not in {"camera", "sensor"}:
        return None
    provider = _normalize_event_provider(raw.get("provider"))
    if provider == "all":
        return None
    device_id = _text(raw.get("device_id"))
    device_ref = _text(raw.get("device_ref") or device_id)
    if not device_id or not device_ref:
        return None
    now_ts = time.time()
    area = " ".join((_text(raw.get("area")) or _text(raw.get("name")) or kind).split())
    name = _text(raw.get("name")) or area or f"Monitored {kind}"
    event_refs = _monitor_string_list(raw.get("event_refs"))
    event_sources: List[Dict[str, str]] = []
    for source in raw.get("event_sources") or []:
        if not isinstance(source, dict):
            continue
        source_ref = _text(source.get("ref"))
        source_type = _category_token(source.get("type"))
        if source_ref:
            source_row = {"ref": source_ref, "type": source_type}
            for state_key in ("state_on", "state_off"):
                state_value = _text(source.get(state_key))
                if state_value:
                    source_row[state_key] = state_value
            event_sources.append(source_row)
    categories = [_category_token(item) for item in _monitor_string_list(raw.get("categories"))]
    categories = [item for item in categories if item]
    event_types = [_category_token(item) for item in _monitor_string_list(raw.get("event_types"))]
    event_types = [item for item in event_types if item]
    trigger_events = [
        _monitor_trigger_token(item)
        for item in _monitor_string_list(raw.get("trigger_events"))
    ]
    trigger_events = [item for item in trigger_events if item in _MONITOR_TRIGGER_VALUES]
    if not trigger_events:
        trigger_events = _monitor_trigger_values_for_device(
            {
                "type": kind,
                "category_ids": categories,
                "event_sources": event_sources,
            }
        )
    description_mode = _text(raw.get("description_mode") or "image").lower()
    if description_mode not in _MONITOR_DESCRIPTION_MODES:
        description_mode = "image"
    linked_camera_provider = _normalize_event_provider(raw.get("linked_camera_provider"))
    linked_camera_device_id = _text(raw.get("linked_camera_device_id"))
    linked_camera_device_ref = _text(raw.get("linked_camera_device_ref") or linked_camera_device_id)
    linked_camera_name = _text(raw.get("linked_camera_name"))
    linked_camera_description_mode = _text(
        raw.get("linked_camera_description_mode") or "image"
    ).lower()
    if linked_camera_description_mode not in _MONITOR_DESCRIPTION_MODES:
        linked_camera_description_mode = "image"
    if (
        kind != "sensor"
        or linked_camera_provider == "all"
        or not linked_camera_device_id
        or not linked_camera_device_ref
    ):
        linked_camera_provider = ""
        linked_camera_device_id = ""
        linked_camera_device_ref = ""
        linked_camera_name = ""
        linked_camera_description_mode = ""
    return {
        "id": _text(raw.get("id")) or str(uuid.uuid4()),
        "kind": kind,
        "provider": provider,
        "device_id": device_id,
        "device_ref": device_ref,
        "selected_device": _provider_ref(provider, device_id),
        "event_refs": event_refs,
        "event_sources": event_sources,
        "event_types": event_types,
        "trigger_events": trigger_events,
        "categories": categories,
        "name": name,
        "area": area or kind,
        "enabled": _bool(raw.get("enabled"), True),
        "notifications_enabled": _bool(raw.get("notifications_enabled"), False),
        "notification_destinations": _normalize_notification_destinations(
            raw.get("notification_destinations")
        ),
        "description_mode": description_mode if kind == "camera" else "",
        "linked_camera_provider": linked_camera_provider,
        "linked_camera_device_id": linked_camera_device_id,
        "linked_camera_device_ref": linked_camera_device_ref,
        "linked_camera_name": linked_camera_name,
        "linked_camera_description_mode": linked_camera_description_mode,
        # Preserve the behavior of camera monitors created before this setting
        # existed. Sensors can never schedule Face ID work.
        "face_id_enabled": kind == "camera" and _bool(raw.get("face_id_enabled"), True),
        "created_at": _as_float(raw.get("created_at"), now_ts),
        "updated_at": _as_float(raw.get("updated_at"), now_ts),
        "last_event_ts": _as_float(raw.get("last_event_ts"), 0.0),
        "last_status": _text(raw.get("last_status")),
        "last_summary": _text(raw.get("last_summary")),
        "last_error": _text(raw.get("last_error")),
    }


def _load_monitors(client: Any) -> Dict[str, Dict[str, Any]]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {}
    try:
        raw_rows = redis_obj.hgetall(_MONITORS_KEY) or {}
    except Exception:
        logger.debug("[awareness] failed to load monitors", exc_info=True)
        return {}
    monitors: Dict[str, Dict[str, Any]] = {}
    for field, value in raw_rows.items():
        try:
            payload = value if isinstance(value, dict) else json.loads(_text(value))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        payload.setdefault("id", _text(field))
        monitor = _normalize_monitor(payload)
        if monitor:
            monitors[monitor["id"]] = monitor
    return monitors


def _get_monitor(client: Any, monitor_id: Any) -> Optional[Dict[str, Any]]:
    redis_obj = client or redis_client
    mid = _text(monitor_id)
    if redis_obj is None or not mid:
        return None
    try:
        raw = redis_obj.hget(_MONITORS_KEY, mid)
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = raw if isinstance(raw, dict) else json.loads(_text(raw))
    except Exception:
        return None
    if isinstance(payload, dict):
        payload.setdefault("id", mid)
    return _normalize_monitor(payload)


def _save_monitor(client: Any, monitor: Dict[str, Any]) -> Dict[str, Any]:
    redis_obj = client or redis_client
    if redis_obj is None:
        raise ValueError("Redis connection is unavailable.")
    normalized = _normalize_monitor(monitor)
    if not normalized:
        raise ValueError("Invalid Awareness monitor.")
    redis_obj.hset(_MONITORS_KEY, normalized["id"], json.dumps(normalized))
    return normalized


def _remove_monitor(client: Any, monitor_id: Any) -> bool:
    redis_obj = client or redis_client
    mid = _text(monitor_id)
    if redis_obj is None or not mid:
        return False
    return bool(redis_obj.hdel(_MONITORS_KEY, mid))


def _monitor_registry(client: Any, *, refresh: bool = False) -> Dict[str, Any]:
    try:
        from integration_registry import get_integration_device_registry

        registry = get_integration_device_registry(client or redis_client, refresh=refresh)
    except Exception:
        logger.debug("[awareness] integration device registry unavailable", exc_info=True)
        return {"devices": [], "categories": [], "rooms": []}
    return registry if isinstance(registry, dict) else {"devices": [], "categories": [], "rooms": []}


def _monitor_device_categories(device: Dict[str, Any]) -> set[str]:
    values = [
        *(device.get("category_ids") or []),
        *(device.get("capabilities") or []),
        device.get("type"),
    ]
    return {_category_token(item) for item in values if _category_token(item)}


def _monitor_device_kind(device: Dict[str, Any]) -> str:
    categories = _monitor_device_categories(device)
    if "camera" in categories or "doorbell" in categories:
        return "camera"
    if categories.intersection(_MONITOR_SENSOR_CATEGORIES):
        return "sensor"
    event_corpus = " ".join(
        _text(value).lower()
        for source in device.get("event_sources") or []
        if isinstance(source, dict)
        for value in (source.get("type"), source.get("ref"))
        if _text(value)
    )
    feature_corpus = " ".join(
        _text(value).lower()
        for value in [
            device.get("type"),
            *(device.get("features") or []),
            *(device.get("actions") or []),
        ]
        if _text(value)
    )
    if any(token in f"{event_corpus} {feature_corpus}" for token in ("camera", "doorbell", "snapshot")):
        return "camera"
    if event_corpus:
        return "sensor"
    return ""


def _monitor_trigger_token(value: Any) -> str:
    token = _category_token(value)
    aliases = {
        "smart_person": "person",
        "smart_vehicle": "vehicle",
        "smart_animal": "animal",
        "smart_package": "package",
        "smart_face": "face",
        "smart_license_plate": "license_plate",
        "licenseplate": "license_plate",
        "doorbell_pressed": "doorbell",
        "ring": "doorbell",
        "open": "opens",
        "close": "closes",
    }
    return aliases.get(token, token)


def _monitor_trigger_values_for_device(device: Dict[str, Any]) -> List[str]:
    found: set[str] = set()

    def add(*values: str) -> None:
        for value in values:
            token = _monitor_trigger_token(value)
            if token in _MONITOR_TRIGGER_VALUES:
                found.add(token)

    for source in device.get("event_sources") or []:
        if not isinstance(source, dict):
            continue
        explicit_events = _monitor_string_list(
            source.get("trigger_events") or source.get("events")
        )
        if explicit_events:
            add(*explicit_events)
            continue
        source_type = _category_token(source.get("type"))
        event_type = _monitor_trigger_token(source_type)
        if event_type in {
            "license_plate",
            "person",
            "vehicle",
            "animal",
            "package",
            "face",
            "doorbell",
            "motion",
        }:
            add(event_type)
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
            add("changed")
    return [row["value"] for row in _MONITOR_TRIGGER_OPTIONS if row["value"] in found]


def _monitor_trigger_option(value: Any) -> Dict[str, Any]:
    token = _monitor_trigger_token(value)
    row = next((item for item in _MONITOR_TRIGGER_OPTIONS if item["value"] == token), None)
    if row:
        return dict(row)
    return {"value": token, "label": token.replace("_", " ").title(), "icon": "◆"}


def _monitor_trigger_dependency(
    registry: Dict[str, Any],
    *,
    current_device: Any = "",
    current_events: Any = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    options_by_source: Dict[str, List[Dict[str, Any]]] = {}
    all_values: set[str] = set()
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        encoded = _monitor_device_value(device)
        if not encoded or not _monitor_device_kind(device):
            continue
        values = _monitor_trigger_values_for_device(device)
        all_values.update(values)
        options_by_source[encoded] = [_monitor_trigger_option(value) for value in values]
    default_options = [
        _monitor_trigger_option(row["value"])
        for row in _MONITOR_TRIGGER_OPTIONS
        if row["value"] in all_values
    ]
    selected = list(options_by_source.get(_text(current_device), default_options))
    for saved in _monitor_string_list(current_events):
        token = _monitor_trigger_token(saved)
        if token and not any(_text(row.get("value")) == token for row in selected):
            selected.append({**_monitor_trigger_option(token), "meta": "Saved setting"})
    return selected, {
        "source_key": "device",
        "options_by_source": options_by_source,
        "default_options": default_options,
    }


def _monitor_device_value(device: Dict[str, Any]) -> str:
    provider = _normalize_event_provider(device.get("integration_id"))
    device_id = _text(device.get("id") or device.get("ref"))
    return _provider_ref(provider, device_id) if provider != "all" and device_id else ""


def _monitor_integration_value(kind: Any, provider: Any) -> str:
    kind_token = _text(kind).lower()
    provider_token = _normalize_event_provider(provider)
    if kind_token not in {"camera", "sensor"} or provider_token == "all":
        return ""
    return f"{kind_token}::{provider_token}"


def _monitor_device_type_label(device: Dict[str, Any]) -> str:
    kind = _monitor_device_kind(device)
    details = device.get("details") if isinstance(device.get("details"), dict) else {}
    corpus = " ".join(
        _category_token(value)
        for value in [
            device.get("type"),
            details.get("sensor_kind"),
            details.get("mount_type"),
            *(device.get("category_ids") or []),
            *(device.get("capabilities") or []),
            *(device.get("features") or []),
            *[
                source.get("type")
                for source in device.get("event_sources") or []
                if isinstance(source, dict)
            ],
        ]
        if _category_token(value)
    )
    if kind == "camera":
        return "Doorbell camera" if "doorbell" in corpus else "Camera"
    if "window" in corpus:
        return "Window sensor"
    if any(token in corpus for token in ("entry_sensor", "contact", "door", "open_close")):
        return "Door sensor"
    if "motion" in corpus:
        return "Motion sensor"
    if any(token in corpus for token in ("occupancy", "presence")):
        return "Presence sensor"
    if "leak" in corpus:
        return "Leak sensor"
    if "temperature" in corpus and "humidity" in corpus:
        return "Climate sensor"
    if "temperature" in corpus:
        return "Temperature sensor"
    if "humidity" in corpus:
        return "Humidity sensor"
    if "illuminance" in corpus or "light_level" in corpus:
        return "Light sensor"
    if "battery" in corpus:
        return "Battery sensor"
    return "Sensor"


def _monitor_device_icon(device: Dict[str, Any]) -> str:
    label = _monitor_device_type_label(device)
    return {
        "Doorbell camera": "◉",
        "Camera": "◎",
        "Door sensor": "↔",
        "Window sensor": "▤",
        "Motion sensor": "⌁",
        "Presence sensor": "◇",
        "Leak sensor": "◒",
        "Temperature sensor": "°",
        "Humidity sensor": "◔",
        "Climate sensor": "◐",
        "Light sensor": "☼",
        "Battery sensor": "▰",
    }.get(label, "◇")


def _find_monitor_device(registry: Dict[str, Any], selected_device: Any) -> Optional[Dict[str, Any]]:
    provider, device_id = _split_provider_ref(selected_device, "")
    wanted = _text(device_id).casefold()
    if provider == "all" or not wanted:
        return None
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        if _normalize_event_provider(device.get("integration_id")) != provider:
            continue
        identities = {
            _text(device.get("id")).casefold(),
            _text(device.get("ref")).casefold(),
        }
        if wanted in identities:
            return device
    return None


def _monitor_device_option(device: Dict[str, Any]) -> Dict[str, Any]:
    value = _monitor_device_value(device)
    provider_name = _text(device.get("integration_name")) or _provider_label(device.get("integration_id"))
    room = _text(device.get("room") or device.get("area"))
    state = _text(device.get("state") or device.get("status"))
    type_label = _monitor_device_type_label(device)
    return {
        "value": value,
        "label": _text(device.get("name")) or _text(device.get("id") or device.get("ref")),
        "description": " • ".join(item for item in (type_label, room, provider_name) if item),
        "meta": state,
        "icon": _monitor_device_icon(device),
    }


def _monitor_integration_options(
    registry: Dict[str, Any],
    *,
    current_kind: str = "camera",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    provider_rows: Dict[str, Dict[str, Dict[str, Any]]] = {"camera": {}, "sensor": {}}
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        if not _monitor_trigger_values_for_device(device):
            continue
        kind = _monitor_device_kind(device)
        provider = _normalize_event_provider(device.get("integration_id"))
        if kind not in provider_rows or provider == "all":
            continue
        value = _monitor_integration_value(kind, provider)
        row = provider_rows[kind].setdefault(
            provider,
            {
                "value": value,
                "label": _text(device.get("integration_name")) or _provider_label(provider),
                "count": 0,
                "icon": "◎" if kind == "camera" else "◇",
            },
        )
        row["count"] = _as_int(row.get("count"), 0, minimum=0) + 1
    options_by_kind: Dict[str, List[Dict[str, Any]]] = {"camera": [], "sensor": []}
    for kind, providers in provider_rows.items():
        for row in providers.values():
            count = _as_int(row.pop("count", 0), 0, minimum=0)
            noun = "camera" if kind == "camera" else "sensor"
            row["description"] = f"{count} {noun}{'' if count == 1 else 's'} available"
            options_by_kind[kind].append(row)
        rows = options_by_kind[kind]
        rows.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("value"))))
    kind = current_kind if current_kind in options_by_kind else "camera"
    selected = [dict(row) for row in options_by_kind[kind]]
    return selected, {
        "source_key": "kind",
        "options_by_source": options_by_kind,
        "default_options": [*options_by_kind["camera"], *options_by_kind["sensor"]],
    }


def _monitor_device_options(
    registry: Dict[str, Any],
    *,
    current_integration: str = "",
    current_device: str = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    options_by_integration: Dict[str, List[Dict[str, Any]]] = {}
    seen: set[str] = set()
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        if not _monitor_trigger_values_for_device(device):
            continue
        kind = _monitor_device_kind(device)
        provider = _normalize_event_provider(device.get("integration_id"))
        integration_value = _monitor_integration_value(kind, provider)
        option = _monitor_device_option(device)
        value = _text(option.get("value"))
        if not integration_value or not value or value in seen:
            continue
        seen.add(value)
        options_by_integration.setdefault(integration_value, []).append(option)
    for rows in options_by_integration.values():
        rows.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("value"))))
    selected = [dict(row) for row in options_by_integration.get(_text(current_integration), [])]
    current = _text(current_device)
    if current and not any(_text(row.get("value")) == current for row in selected):
        selected.append({"value": current, "label": f"{current} (saved)", "icon": "◆"})
    return selected, {
        "source_key": "integration",
        "options_by_source": options_by_integration,
        "default_options": [
            dict(row)
            for key in sorted(options_by_integration)
            for row in options_by_integration[key]
        ],
    }


def _monitor_device_supports_description_mode(device: Dict[str, Any], mode: Any) -> bool:
    mode_token = _text(mode).lower()
    if _monitor_device_kind(device) != "camera" or mode_token not in _MONITOR_DESCRIPTION_MODES:
        return False
    actions = {_category_token(value) for value in device.get("actions") or [] if _category_token(value)}
    capabilities = {
        _category_token(value)
        for value in [*(device.get("capabilities") or []), *(device.get("features") or [])]
        if _category_token(value)
    }
    if mode_token == "video":
        return bool(actions.intersection({"camera_clip", "video_clip", "clip"})) or bool(
            capabilities.intersection({"camera_clip", "video_clip", "clip"})
        )
    return bool(actions.intersection({"camera_snapshot", "snapshot"})) or "snapshot" in capabilities


def _monitor_description_mode_dependency(
    registry: Dict[str, Any],
    *,
    current_device: Any = "",
    current_mode: Any = "image",
    source_key: str = "device",
    include_default_options: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    options_by_source: Dict[str, List[Dict[str, Any]]] = {}
    all_values: set[str] = set()
    for device in registry.get("devices") or []:
        if not isinstance(device, dict) or _monitor_device_kind(device) != "camera":
            continue
        encoded = _monitor_device_value(device)
        if not encoded:
            continue
        rows = [
            dict(option)
            for option in _MONITOR_DESCRIPTION_MODE_OPTIONS
            if _monitor_device_supports_description_mode(device, option["value"])
        ]
        if rows:
            options_by_source[encoded] = rows
            all_values.update(_text(row.get("value")) for row in rows)
    default_options = [
        dict(option)
        for option in _MONITOR_DESCRIPTION_MODE_OPTIONS
        if _text(option.get("value")) in all_values
    ]
    selected_default = default_options if include_default_options else []
    selected = [dict(row) for row in options_by_source.get(_text(current_device), selected_default)]
    saved_mode = _text(current_mode).lower()
    if (include_default_options or _text(current_device)) and saved_mode in _MONITOR_DESCRIPTION_MODES and not any(
        _text(row.get("value")) == saved_mode for row in selected
    ):
        saved = next(
            (dict(row) for row in _MONITOR_DESCRIPTION_MODE_OPTIONS if row["value"] == saved_mode),
            {"value": saved_mode, "label": saved_mode.title(), "icon": "◆"},
        )
        saved["meta"] = "Saved setting; currently unavailable"
        selected.append(saved)
    return selected, {
        "source_key": _text(source_key) or "device",
        "options_by_source": options_by_source,
        "default_options": default_options if include_default_options else [],
    }


def _monitor_linked_camera_devices(registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for device in registry.get("devices") or []:
        if not isinstance(device, dict) or _monitor_device_kind(device) != "camera":
            continue
        if not any(
            _monitor_device_supports_description_mode(device, mode)
            for mode in _MONITOR_DESCRIPTION_MODES
        ):
            continue
        value = _monitor_device_value(device)
        if not value or value in seen:
            continue
        seen.add(value)
        rows.append(device)
    return rows


def _monitor_linked_camera_integration_options(
    registry: Dict[str, Any],
    *,
    current_provider: Any = "",
) -> List[Dict[str, Any]]:
    providers: Dict[str, Dict[str, Any]] = {}
    for device in _monitor_linked_camera_devices(registry):
        provider = _normalize_event_provider(device.get("integration_id"))
        if provider == "all":
            continue
        row = providers.setdefault(
            provider,
            {
                "value": _monitor_integration_value("camera", provider),
                "label": _text(device.get("integration_name")) or _provider_label(provider),
                "count": 0,
                "icon": "◎",
            },
        )
        row["count"] = _as_int(row.get("count"), 0, minimum=0) + 1
    options = [
        {
            "value": "",
            "label": "No camera",
            "description": "Save the sensor event without visual context.",
            "icon": "◇",
        }
    ]
    for row in sorted(
        providers.values(),
        key=lambda item: (_text(item.get("label")).casefold(), _text(item.get("value"))),
    ):
        count = _as_int(row.pop("count", 0), 0, minimum=0)
        row["description"] = f"{count} camera{'' if count == 1 else 's'} available"
        options.append(row)
    current = _normalize_event_provider(current_provider)
    current_value = _monitor_integration_value("camera", current)
    if current_value and not any(_text(row.get("value")) == current_value for row in options):
        options.append(
            {
                "value": current_value,
                "label": f"{_provider_label(current)} (saved)",
                "description": "The saved camera integration is currently unavailable.",
                "icon": "◆",
            }
        )
    return options


def _monitor_linked_camera_device_options(
    registry: Dict[str, Any],
    *,
    current_integration: Any = "",
    current_device: Any = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    options_by_integration: Dict[str, List[Dict[str, Any]]] = {}
    for device in _monitor_linked_camera_devices(registry):
        provider = _normalize_event_provider(device.get("integration_id"))
        integration_value = _monitor_integration_value("camera", provider)
        if not integration_value:
            continue
        options_by_integration.setdefault(integration_value, []).append(
            _monitor_device_option(device)
        )
    for rows in options_by_integration.values():
        rows.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("value"))))
    selected = [dict(row) for row in options_by_integration.get(_text(current_integration), [])]
    saved_device = _text(current_device)
    if saved_device and not any(_text(row.get("value")) == saved_device for row in selected):
        selected.append(
            {
                "value": saved_device,
                "label": f"{saved_device} (saved)",
                "description": "This saved camera is currently unavailable.",
                "icon": "◆",
            }
        )
    return selected, {
        "source_key": "linked_camera_integration",
        "options_by_source": options_by_integration,
        "default_options": [],
    }


def _build_monitor_from_values(
    *,
    values: Dict[str, Any],
    payload: Dict[str, Any],
    client: Any,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    previous = existing if isinstance(existing, dict) else {}
    kind = _text(_value(values, payload, "kind", previous.get("kind") or "camera")).lower()
    if kind not in {"camera", "sensor"}:
        raise ValueError("Choose Camera or Sensor.")
    selected_integration = _text(
        _value(
            values,
            payload,
            "integration",
            _monitor_integration_value(kind, previous.get("provider")),
        )
    )
    selected = _text(
        _value(values, payload, "device", previous.get("selected_device") or _provider_ref(previous.get("provider"), previous.get("device_id")))
    )
    registry = _monitor_registry(client)
    device = _find_monitor_device(registry, selected)
    if not device:
        raise ValueError("Choose an available device from a connected integration.")
    actual_kind = _monitor_device_kind(device)
    if actual_kind != kind:
        raise ValueError(f"The selected device is not available as a {kind} monitor.")
    provider = _normalize_event_provider(device.get("integration_id"))
    actual_integration = _monitor_integration_value(actual_kind, provider)
    if selected_integration and selected_integration != actual_integration:
        raise ValueError("The selected device is not from the chosen integration.")
    device_id = _text(device.get("id") or device.get("ref"))
    device_ref = _text(device.get("ref") or device_id)
    for saved in _load_monitors(client).values():
        if _text(saved.get("id")) == _text(previous.get("id")):
            continue
        if (
            _normalize_event_provider(saved.get("provider")) == provider
            and _text(saved.get("device_id")).casefold() == device_id.casefold()
        ):
            raise ValueError("That device is already being monitored.")
    event_refs: List[str] = []
    event_types: List[str] = []
    event_sources: List[Dict[str, str]] = []
    for source in device.get("event_sources") or []:
        if not isinstance(source, dict):
            continue
        ref = _text(source.get("ref"))
        source_type = _category_token(source.get("type"))
        if ref and ref not in event_refs:
            event_refs.append(ref)
        if source_type and source_type not in event_types:
            event_types.append(source_type)
        if ref:
            source_row = {"ref": ref, "type": source_type}
            for state_key in ("state_on", "state_off"):
                state_value = _text(source.get(state_key))
                if state_value:
                    source_row[state_key] = state_value
            event_sources.append(source_row)
    if kind == "sensor" or not event_refs:
        for ref in (device_ref, device_id):
            if ref and ref not in event_refs:
                event_refs.append(ref)
    if kind == "sensor" and provider == "unifi_protect":
        sensor_alias = _unifi_sensor_entity(device_id)
        if sensor_alias not in event_refs:
            event_refs.append(sensor_alias)
    available_trigger_events = _monitor_trigger_values_for_device(device)
    raw_trigger_events = _value(
        values,
        payload,
        "trigger_events",
        previous.get("trigger_events") or available_trigger_events,
    )
    requested_trigger_events = [
        _monitor_trigger_token(item)
        for item in _monitor_string_list(raw_trigger_events)
    ]
    trigger_events: List[str] = []
    for item in requested_trigger_events:
        if item in available_trigger_events and item not in trigger_events:
            trigger_events.append(item)
    if not trigger_events:
        raise ValueError("Choose at least one event that should trigger this monitored source.")
    description_mode = _text(
        _value(values, payload, "description_mode", previous.get("description_mode") or "image")
    ).lower()
    if kind == "camera":
        if description_mode not in _MONITOR_DESCRIPTION_MODES:
            raise ValueError("Choose image descriptions or video descriptions for this camera.")
        if not _monitor_device_supports_description_mode(device, description_mode):
            media_label = "short video clips" if description_mode == "video" else "snapshots"
            raise ValueError(
                f"The selected camera integration does not report support for {media_label}."
            )
    else:
        description_mode = ""
    linked_camera_provider = ""
    linked_camera_device_id = ""
    linked_camera_device_ref = ""
    linked_camera_name = ""
    linked_camera_description_mode = ""
    if kind == "sensor":
        selected_linked_integration = _text(
            _value(
                values,
                payload,
                "linked_camera_integration",
                _monitor_integration_value("camera", previous.get("linked_camera_provider")),
            )
        )
        selected_linked_camera = _text(
            _value(
                values,
                payload,
                "linked_camera",
                _provider_ref(
                    previous.get("linked_camera_provider"),
                    previous.get("linked_camera_device_id"),
                )
                if _text(previous.get("linked_camera_device_id"))
                else "",
            )
        )
        if not selected_linked_integration:
            selected_linked_camera = ""
        if selected_linked_camera:
            linked_device = _find_monitor_device(registry, selected_linked_camera)
            if not linked_device or _monitor_device_kind(linked_device) != "camera":
                raise ValueError("Choose an available camera to add visual context to this sensor.")
            linked_camera_provider = _normalize_event_provider(linked_device.get("integration_id"))
            actual_linked_integration = _monitor_integration_value(
                "camera",
                linked_camera_provider,
            )
            if (
                selected_linked_integration
                and selected_linked_integration != actual_linked_integration
            ):
                raise ValueError("The linked camera is not from the chosen camera integration.")
            linked_camera_device_id = _text(linked_device.get("id") or linked_device.get("ref"))
            linked_camera_device_ref = _text(
                linked_device.get("ref") or linked_camera_device_id
            )
            linked_camera_name = _text(linked_device.get("name")) or linked_camera_device_id
            linked_camera_description_mode = _text(
                _value(
                    values,
                    payload,
                    "linked_camera_description_mode",
                    previous.get("linked_camera_description_mode") or "image",
                )
            ).lower()
            if linked_camera_description_mode not in _MONITOR_DESCRIPTION_MODES:
                linked_camera_description_mode = next(
                    (
                        mode
                        for mode in ("image", "video")
                        if _monitor_device_supports_description_mode(linked_device, mode)
                    ),
                    "",
                )
            if not linked_camera_description_mode:
                raise ValueError("The linked camera does not report snapshot or clip support.")
            if not _monitor_device_supports_description_mode(
                linked_device,
                linked_camera_description_mode,
            ):
                media_label = (
                    "short video clips"
                    if linked_camera_description_mode == "video"
                    else "snapshots"
                )
                raise ValueError(
                    f"The linked camera integration does not report support for {media_label}."
                )
    notifications_enabled = _bool(
        _value(
            values,
            payload,
            "notifications_enabled",
            previous.get("notifications_enabled", False),
        ),
        False,
    )
    notification_destinations = _normalize_notification_destinations(
        _value(
            values,
            payload,
            "notification_destinations",
            previous.get("notification_destinations") or [],
        )
    )
    if notifications_enabled and not notification_destinations:
        raise ValueError("Choose at least one destination for Awareness notifications.")
    now_ts = time.time()
    default_name = _text(device.get("name")) or device_id
    default_area = _text(device.get("room") or device.get("area")) or default_name
    monitor = {
        **previous,
        "id": _text(previous.get("id")) or str(uuid.uuid4()),
        "kind": kind,
        "provider": provider,
        "device_id": device_id,
        "device_ref": device_ref,
        "event_refs": event_refs,
        "event_sources": event_sources,
        "event_types": event_types,
        "trigger_events": trigger_events,
        "categories": sorted(_monitor_device_categories(device)),
        "name": _text(_value(values, payload, "name", previous.get("name") or default_name)) or default_name,
        "area": _text(_value(values, payload, "area", previous.get("area") or default_area)) or default_area,
        "enabled": _bool(_value(values, payload, "enabled", previous.get("enabled", True)), True),
        "notifications_enabled": notifications_enabled,
        "notification_destinations": notification_destinations,
        "description_mode": description_mode,
        "linked_camera_provider": linked_camera_provider,
        "linked_camera_device_id": linked_camera_device_id,
        "linked_camera_device_ref": linked_camera_device_ref,
        "linked_camera_name": linked_camera_name,
        "linked_camera_description_mode": linked_camera_description_mode,
        "face_id_enabled": kind == "camera" and _bool(
            _value(values, payload, "face_id_enabled", previous.get("face_id_enabled", True)),
            True,
        ),
        "created_at": _as_float(previous.get("created_at"), now_ts),
        "updated_at": now_ts,
    }
    normalized = _normalize_monitor(monitor)
    if not normalized:
        raise ValueError("Could not create that Awareness monitor.")
    return normalized


def _unwrap_form_value(value: Any) -> Any:
    if isinstance(value, dict):
        for key in ("value", "id", "entity_id", "service", "target", "key", "name"):
            if key in value:
                candidate = value.get(key)
                if candidate not in (None, ""):
                    return _unwrap_form_value(candidate)
        if "checked" in value and isinstance(value.get("checked"), bool):
            return bool(value.get("checked"))
        if len(value) == 1:
            try:
                only_value = next(iter(value.values()))
            except Exception:
                only_value = None
            if only_value not in (None, ""):
                return _unwrap_form_value(only_value)
        return value
    if isinstance(value, list):
        return [_unwrap_form_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_unwrap_form_value(item) for item in value)
    if isinstance(value, set):
        return {_unwrap_form_value(item) for item in value}
    return value



def _queue_depth(client: Any) -> int:
    redis_obj = client or redis_client
    try:
        return int(redis_obj.llen(_EXEC_QUEUE_KEY) or 0)
    except Exception:
        return 0



def _dequeue_execution(client: Any) -> Optional[Dict[str, Any]]:
    redis_obj = client or redis_client
    raw = redis_obj.rpop(_EXEC_QUEUE_KEY)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _ha_config() -> Dict[str, str]:
    return load_homeassistant_config(required=True)


def _unifi_camera_id_from_entity(camera_entity: str) -> str:
    object_id = _entity_object_id(camera_entity)
    if object_id.startswith("unifi_"):
        return object_id[len("unifi_") :]
    return object_id






def _unifi_camera_motion_trigger(camera_id: str) -> str:
    return f"binary_sensor.unifi_{_text(camera_id).lower()}_motion"


def _unifi_camera_doorbell_trigger(camera_id: str) -> str:
    return f"binary_sensor.unifi_{_text(camera_id).lower()}_doorbell"


def _unifi_sensor_entity(sensor_id: str) -> str:
    return f"binary_sensor.unifi_sensor_{_text(sensor_id).lower()}"




def _unifi_normalize_smart_type(raw_type: Any) -> str:
    token = _slug(_text(raw_type))
    if not token:
        return ""
    token = token.replace("smart_detect_", "").replace("smart_", "")
    return _UNIFI_SMART_TYPE_ALIASES.get(token, token)


def _unifi_smart_type_label(smart_type: str) -> str:
    token = _unifi_normalize_smart_type(smart_type)
    if not token:
        return "Smart Detect"
    label = _UNIFI_SMART_TYPE_LABELS.get(token)
    if label:
        return label
    return " ".join(part.capitalize() for part in token.split("_"))


def _unifi_smart_type_variants(smart_type: str) -> set[str]:
    token = _unifi_normalize_smart_type(smart_type)
    if not token:
        return set()
    variants: set[str] = {token, token.replace("_", "")}
    alias_variants = [key for key, value in _UNIFI_SMART_TYPE_ALIASES.items() if value == token]
    for alias in alias_variants:
        alias_token = _slug(alias)
        if not alias_token:
            continue
        variants.add(alias_token)
        variants.add(alias_token.replace("_", ""))
    if token.endswith("s"):
        variants.add(token[:-1])
    else:
        variants.add(f"{token}s")
    return {item for item in variants if item}


def _unifi_matches_smart_type_text(raw_text: Any, smart_type: str) -> bool:
    token = _slug(_text(raw_text))
    if not token:
        return False
    compact = token.replace("_", "")
    for variant in _unifi_smart_type_variants(smart_type):
        if variant and (variant in token or variant in compact):
            return True
    return False


def _unifi_marker_token(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return _text(value)
    if isinstance(value, dict):
        for key in ("eventId", "event_id", "id", "timestamp", "time", "at", "ts", "start", "startTime"):
            marker = _unifi_marker_token(value.get(key))
            if marker:
                return f"{key}:{marker}"
        parts: List[str] = []
        for key in sorted(value.keys(), key=lambda item: _text(item)):
            marker = _unifi_marker_token(value.get(key))
            if not marker:
                continue
            parts.append(f"{_text(key)}:{marker}")
            if len(parts) >= 6:
                break
        return "|".join(parts)
    if isinstance(value, list):
        parts: List[str] = []
        for item in value[:6]:
            marker = _unifi_marker_token(item)
            if marker:
                parts.append(marker)
        return ",".join(parts)
    return _text(value)




def _unifi_event_matches_smart_type(event_obj: Dict[str, Any], smart_type: str) -> bool:
    keys = (
        "type",
        "types",
        "smartDetectType",
        "smartDetectTypes",
        "detectionType",
        "detectionTypes",
        "eventType",
        "eventTypes",
        "objectType",
        "objectTypes",
        "class",
        "classes",
        "label",
        "labels",
    )
    for key in keys:
        raw_value = event_obj.get(key)
        if isinstance(raw_value, (list, tuple, set)):
            if any(_unifi_matches_smart_type_text(item, smart_type) for item in raw_value):
                return True
            continue
        if _unifi_matches_smart_type_text(raw_value, smart_type):
            return True
    return False












def _unifi_camera_smart_trigger(camera_id: str, smart_type: str) -> str:
    token = _unifi_normalize_smart_type(smart_type) or "smart_detect"
    return f"binary_sensor.unifi_{_text(camera_id).lower()}_smart_{token}"





def _entry_state_action(state_value: Any) -> Tuple[str, str]:
    state = _text(state_value).lower()
    if state in {"on", "open", "opening", "unlocked"}:
        return "opened", "open"
    if state in {"off", "closed", "closing", "locked"}:
        return "closed", "closed"
    return f"changed to {state or 'unknown'}", "changed"


def _friendly_entity_name(entity_id: str, state_obj: Dict[str, Any]) -> str:
    attrs = state_obj.get("attributes") if isinstance(state_obj, dict) else {}
    if not isinstance(attrs, dict):
        attrs = {}
    friendly = _text(attrs.get("friendly_name"))
    if friendly:
        return friendly
    token = _entity_object_id(entity_id).replace("_", " ").strip()
    return token or entity_id


def _acquire_cooldown(key: str, cooldown_seconds: int) -> bool:
    seconds = max(0, int(cooldown_seconds))
    if seconds <= 0:
        return True
    try:
        token = str(int(time.time()))
        return bool(redis_client.set(key, token, ex=seconds, nx=True))
    except Exception:
        # Fail-open so transient Redis issues do not block alerts entirely.
        logger.debug("[awareness] failed to acquire cooldown key %s", key, exc_info=True)
        return True

def _clear_cooldown(key: str) -> None:
    try:
        redis_client.delete(key)
    except Exception:
        logger.debug("[awareness] failed to clear cooldown key %s", key, exc_info=True)


def _compact(text: str, limit: int = 220) -> str:
    out = re.sub(r"\s+", " ", _text(text))
    if len(out) <= limit:
        return out
    cut = out[:limit]
    if " " in cut[40:]:
        cut = cut[: cut.rfind(" ")]
    return cut.rstrip(".,;: ") + "..."


def _is_nothing_notable_summary(summary: Any) -> bool:
    token = _text(summary).lower()
    if not token:
        return False
    normalized = re.sub(r"[^a-z]+", " ", token).strip()
    if not normalized:
        return False
    return normalized.startswith("nothing notable") or normalized in {
        "nothing notable",
        "nothing of note",
        "no notable activity",
    }



def _camera_snapshot_sync(ha_base: str, token: str, camera_entity: str) -> bytes:
    url = f"{ha_base}/api/camera_proxy/{quote(camera_entity, safe='')}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=12)
    if resp.status_code >= 400:
        raise RuntimeError(f"camera_proxy HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.content


async def _camera_snapshot(ha_base: str, token: str, camera_entity: str) -> bytes:
    return await asyncio.to_thread(_camera_snapshot_sync, ha_base, token, camera_entity)


def _unifi_camera_snapshot_sync(camera_id: str) -> bytes:
    camera_token = _text(camera_id).lower()
    if not camera_token:
        raise ValueError("UniFi camera id is required.")
    candidates = [
        f"/proxy/protect/integration/v1/cameras/{camera_token}/snapshot",
        f"/proxy/protect/integration/v1/cameras/{camera_token}/snapshot.jpg",
        f"/proxy/protect/integration/v1/cameras/{camera_token}/snapshot?format=jpeg",
        f"/proxy/protect/integration/v1/cameras/{camera_token}/snapshot?force=true",
        f"/proxy/protect/integration/v1/cameras/{camera_token}/snapshot?force=true&format=jpeg",
    ]
    last_error: Optional[Exception] = None
    for path in candidates:
        try:
            content, _headers = _unifi_request(
                "GET",
                path,
                headers={"Accept": "image/jpeg,image/png,image/*,*/*"},
                stream=True,
            )
            if isinstance(content, (bytes, bytearray)) and len(content) > 1000:
                return bytes(content)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"UniFi snapshot unavailable for {camera_token}: {last_error}")


async def _unifi_camera_snapshot(camera_id: str) -> bytes:
    return await asyncio.to_thread(_unifi_camera_snapshot_sync, camera_id)


def _integration_camera_snapshot_sync(provider: str, camera_ref: str) -> Tuple[bytes, str]:
    from integration_registry import run_integration_device_action

    provider_token = _normalize_event_provider(provider)
    device_ref = _text(camera_ref)
    if device_ref.startswith("camera."):
        device_ref = _unifi_camera_id_from_entity(device_ref) if provider_token == "unifi_protect" else device_ref
    elif device_ref.startswith("camera:"):
        device_ref = _text(device_ref.split(":", 1)[1])
    result = run_integration_device_action(provider_token, "camera_snapshot", device_ref, {})
    if isinstance(result, tuple) and len(result) >= 1:
        content = result[0]
        content_type = _text(result[1] if len(result) > 1 else "image/jpeg") or "image/jpeg"
        if isinstance(content, (bytes, bytearray)):
            return bytes(content), content_type
    if isinstance(result, dict):
        content = result.get("bytes") or result.get("content") or result.get("image_bytes")
        content_type = _text(result.get("content_type") or result.get("mime_type") or "image/jpeg")
        if isinstance(content, str) and content.startswith("data:") and "," in content:
            header, payload = content.split(",", 1)
            content_type = header[5:].split(";", 1)[0] or content_type
            content = base64.b64decode(payload)
        if isinstance(content, (bytes, bytearray)):
            return bytes(content), content_type or "image/jpeg"
    raise RuntimeError(f"{_provider_label(provider_token)} did not return snapshot bytes for {camera_ref}.")


async def _integration_camera_snapshot(provider: str, camera_ref: str) -> Tuple[bytes, str]:
    return await asyncio.to_thread(_integration_camera_snapshot_sync, provider, camera_ref)


async def _capture_camera_snapshot(provider: str, camera_target: str) -> Tuple[bytes, str]:
    provider_token = _normalize_event_provider(provider)
    if provider_token == "homeassistant":
        ha = _ha_config()
        return await _camera_snapshot(ha["base"], ha["token"], camera_target), "image/jpeg"
    if provider_token == "unifi_protect":
        return await _unifi_camera_snapshot(_unifi_camera_id_from_entity(camera_target)), "image/jpeg"
    return await _integration_camera_snapshot(provider_token, camera_target)


def _integration_camera_clip_sync(
    provider: str,
    camera_ref: str,
    payload: Dict[str, Any],
) -> Tuple[bytes, str, Dict[str, Any]]:
    from integration_registry import run_integration_device_action

    provider_token = _normalize_event_provider(provider)
    device_ref = _text(camera_ref)
    if device_ref.startswith("camera:"):
        device_ref = _text(device_ref.split(":", 1)[1])
    result = run_integration_device_action(
        provider_token,
        "camera_clip",
        device_ref,
        dict(payload or {}),
    )
    if isinstance(result, tuple) and result:
        content = result[0]
        content_type = _text(result[1] if len(result) > 1 else "video/mp4") or "video/mp4"
        if isinstance(content, (bytes, bytearray)):
            prepared, prepared_type, playback = _prepare_event_clip_for_playback(
                bytes(content),
                content_type,
            )
            return prepared, prepared_type, playback
    if isinstance(result, dict):
        content = result.get("bytes") or result.get("content") or result.get("video_bytes")
        content_type = _text(result.get("content_type") or result.get("mime_type") or "video/mp4")
        if isinstance(content, str) and content.startswith("data:") and "," in content:
            header, encoded = content.split(",", 1)
            content_type = header[5:].split(";", 1)[0] or content_type
            content = base64.b64decode(encoded)
        if isinstance(content, (bytes, bytearray)):
            metadata = {
                key: value
                for key, value in result.items()
                if key not in {"bytes", "content", "video_bytes"}
            }
            prepared, prepared_type, playback = _prepare_event_clip_for_playback(
                bytes(content),
                content_type or "video/mp4",
            )
            return prepared, prepared_type, {**metadata, **playback}
    raise RuntimeError(f"{_provider_label(provider_token)} did not return clip bytes for {camera_ref}.")


async def _capture_camera_clip(
    provider: str,
    camera_target: str,
    payload: Dict[str, Any],
) -> Tuple[bytes, str, Dict[str, Any]]:
    return await asyncio.to_thread(
        _integration_camera_clip_sync,
        provider,
        camera_target,
        payload,
    )


def _vision_describe_prompts(*, query: str, ignore_vehicles: bool, mode: str) -> Tuple[str, str]:
    if mode == "doorbell":
        prompt = (
            "Write one spoken doorbell sentence. Start with 'Someone is at the door'. "
            "If a person is visible, mention count/clothing/package. "
            "If no person is visible, still start that way and describe what is visible in the scene. "
            "Do not list absences (for example, do not say 'no people/animals/vehicles visible')."
        )
    else:
        prompt = (
            "Write one short sentence describing this camera snapshot. "
            "Keep it general and focus on the most important visible activity or subjects "
            "(people, animals, vehicles, packages, or notable movement). "
            "If no people or animals are visible, reply exactly: Nothing notable. "
            "If this appears to be a delivery, name the company when clearly visible "
            "(UPS, FedEx, USPS, Amazon); otherwise say 'delivery driver'. "
            "Mention counts only when clear and avoid guessing uncertain details. "
            "Always describe what is present in frame and never list what is missing. "
            "Do not say phrases like 'no people, animals, or vehicles are visible'. "
            "If the scene is calm, describe the visible setting briefly."
        )
        if _text(query):
            prompt += f" Additional context: {_text(query)}"
        if ignore_vehicles:
            prompt += (
                " HARD RULE: do not mention or imply vehicles in any way "
                "(car, truck, van, SUV, bike, motorcycle, bus, parked/driving traffic). "
                "Describe only non-vehicle details that are visible in frame."
            )
    system_prompt = (
        "You are a concise vision assistant. Describe what is visible. "
        "Never list absent objects or use 'no X visible' phrasing. "
        "For camera mode when there are no people or animals, output exactly: Nothing notable."
    )
    return system_prompt, prompt


def _video_describe_prompt(*, mode: str, query: str = "") -> str:
    if mode == "doorbell":
        prompt = (
            "Write one short spoken sentence describing this doorbell event clip. "
            "Start with 'Someone is at the door'. Describe the important visible action in order, "
            "including a person, clothing, or package when clear. Do not list absent objects."
        )
    else:
        prompt = (
            "Write one short sentence describing this camera event clip. Focus on the most important "
            "visible action, subjects, movement, and sequence. Mention people, animals, vehicles, or "
            "packages only when visible, avoid guessing, and never list what is absent. "
            "If there is no notable activity, reply exactly: Nothing notable."
        )
    if _text(query):
        prompt += f" Additional context: {_text(query)}"
    return prompt


def _video_describe_sync(
    *,
    video_bytes: bytes,
    content_type: str,
    mode: str,
    query: str = "",
) -> str:
    if not callable(_shared_video_analyze):
        raise RuntimeError("Video Understanding is unavailable in this Tater runtime.")
    extension = {
        "video/webm": "webm",
        "video/quicktime": "mov",
        "video/x-matroska": "mkv",
    }.get(_text(content_type).lower(), "mp4")
    result = _shared_video_analyze(
        media_ref={
            "bytes": bytes(video_bytes or b""),
            "name": f"awareness-camera.{extension}",
            "mimetype": _text(content_type) or "video/mp4",
        },
        prompt=_video_describe_prompt(mode=mode, query=query),
    )
    if not isinstance(result, dict) or not result.get("ok"):
        error = result.get("error") if isinstance(result, dict) else {}
        message = _text(error.get("message")) if isinstance(error, dict) else ""
        raise RuntimeError(message or "Video Understanding could not analyze the camera clip.")
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    return _text(data.get("description") or data.get("text") or result.get("summary_for_user")).strip()


async def _video_describe(
    *,
    video_bytes: bytes,
    content_type: str,
    mode: str,
    query: str = "",
) -> str:
    return await asyncio.to_thread(
        _video_describe_sync,
        video_bytes=video_bytes,
        content_type=content_type,
        mode=mode,
        query=query,
    )


def _vision_describe_openai_sync(
    *,
    image_bytes: bytes,
    api_base: str,
    model: str,
    api_key: str,
    system_prompt: str,
    prompt: str,
) -> str:
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:image/jpeg;base64,{b64}"
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
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
        "max_tokens": 120,
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    url = f"{api_base.rstrip('/')}/v1/chat/completions"
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=35)
    if resp.status_code >= 400:
        raise RuntimeError(f"Vision HTTP {resp.status_code}: {resp.text[:200]}")
    body = resp.json() or {}
    return _text(((body.get("choices") or [{}])[0].get("message") or {}).get("content"))


def _vision_describe_local_sync(
    *,
    image_bytes: bytes,
    provider: str,
    model: str,
    prompt: str,
    mode: str,
) -> str:
    if not callable(_shared_describe_image_with_local_llm):
        raise RuntimeError("Local vision support is unavailable in this Tater runtime.")
    filename = "awareness-doorbell.jpg" if mode == "doorbell" else "awareness-camera.jpg"
    result = _shared_describe_image_with_local_llm(
        provider=provider,
        model=model,
        image_bytes=image_bytes,
        filename=filename,
        prompt=prompt,
        timeout=90.0,
    )
    return _text((result or {}).get("description")).strip()


def _vision_base_local_target() -> Tuple[str, str]:
    if not callable(_shared_resolve_hydra_base_servers):
        return "", ""
    try:
        rows = _shared_resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    except Exception:
        logger.exception("[awareness] failed to read base LLM settings for vision routing")
        return "", ""
    row = dict(rows[0]) if rows and isinstance(rows[0], dict) else {}
    provider = _awareness_normalize_llm_provider(row.get("provider"))
    model = _text(row.get("model"))
    return provider, model


def _vision_describe_sync(
    *,
    image_bytes: bytes,
    api_base: str,
    model: str,
    api_key: str,
    query: str,
    ignore_vehicles: bool,
    mode: str,
    vision_mode: str,
    vision_provider: str,
) -> str:
    system_prompt, prompt = _vision_describe_prompts(
        query=query,
        ignore_vehicles=ignore_vehicles,
        mode=mode,
    )
    routing_mode = _text(vision_mode).strip().lower() or "api"
    if routing_mode not in {"api", "auto", "base", "dedicated"}:
        routing_mode = "api"
    provider = _awareness_normalize_llm_provider(vision_provider)

    if routing_mode == "dedicated" and _awareness_is_local_llm_provider(provider):
        return _vision_describe_local_sync(
            image_bytes=image_bytes,
            provider=provider,
            model=model,
            prompt=prompt,
            mode=mode,
        )

    if routing_mode in {"auto", "base"}:
        base_provider, base_model = _vision_base_local_target()
        if _awareness_is_local_llm_provider(base_provider) and base_model:
            try:
                return _vision_describe_local_sync(
                    image_bytes=image_bytes,
                    provider=base_provider,
                    model=base_model,
                    prompt=prompt,
                    mode=mode,
                )
            except Exception:
                if routing_mode == "base":
                    raise
                logger.exception("[awareness] local base vision failed; falling back to configured vision API")
        elif routing_mode == "base":
            raise RuntimeError("Vision is set to use the base model, but the base LLM is not a local provider.")

    return _vision_describe_openai_sync(
        image_bytes=image_bytes,
        api_base=api_base,
        model=model,
        api_key=api_key,
        system_prompt=system_prompt,
        prompt=prompt,
    )


async def _vision_describe(
    *,
    image_bytes: bytes,
    api_base: str,
    model: str,
    api_key: str,
    query: str,
    ignore_vehicles: bool,
    mode: str,
    vision_mode: str,
    vision_provider: str,
) -> str:
    return await asyncio.to_thread(
        _vision_describe_sync,
        image_bytes=image_bytes,
        api_base=api_base,
        model=model,
        api_key=api_key,
        query=query,
        ignore_vehicles=ignore_vehicles,
        mode=mode,
        vision_mode=vision_mode,
        vision_provider=vision_provider,
    )




def _discover_event_sources(client: Any) -> List[str]:
    redis_obj = client or redis_client
    out: List[str] = []
    try:
        for key in redis_obj.scan_iter(match=f"{_EVENTS_PREFIX}*", count=500):
            src = str(key).split(":", maxsplit=3)[-1]
            if src and src not in out:
                out.append(src)
    except Exception:
        return []
    return out


def _load_events_for_sources(
    client: Any,
    sources: List[str],
    start: datetime,
    end: datetime,
    limit_per_source: int = 200,
) -> List[Dict[str, Any]]:
    redis_obj = client or redis_client
    events: List[Dict[str, Any]] = []
    end_index = -1
    try:
        parsed_limit = int(limit_per_source)
        if parsed_limit > 0:
            end_index = max(1, parsed_limit) - 1
    except Exception:
        end_index = -1
    for src in sources:
        try:
            rows = redis_obj.lrange(_event_key(src), 0, end_index) or []
        except Exception:
            continue
        for row in rows:
            try:
                payload = json.loads(row)
            except Exception:
                continue
            ts = _parse_iso(payload.get("ha_time"))
            if ts is None or ts < start or ts > end:
                continue
            payload.setdefault("source", src)
            events.append(payload)
    events.sort(key=lambda item: _text(item.get("ha_time")), reverse=True)
    return events


def _events_query_source_to_area(source: Any) -> str:
    text = _text(source).lower().replace("_", " ")
    return " ".join(text.split())


def _events_query_event_dt(event: Dict[str, Any]) -> Optional[datetime]:
    parsed = _parse_iso(event.get("ha_time"))
    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _events_query_event_id(event: Dict[str, Any]) -> str:
    explicit_id = _text(event.get("id"))
    if explicit_id:
        return explicit_id
    src = _text(event.get("source"))
    ha_time = _text(event.get("ha_time"))
    title = _text(event.get("title"))
    message = _text(event.get("message"))
    entity = _text(event.get("entity_id"))
    seed = "|".join([src, ha_time, title, message, entity])
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    return f"ev_{digest[:16]}"


def _events_query_compact_data(data_payload: Dict[str, Any]) -> Dict[str, Any]:
    compact: Dict[str, Any] = {}
    for key in sorted(_EVENTS_QUERY_SAFE_DATA_FIELDS):
        if key not in data_payload:
            continue
        value = data_payload.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, str):
            text = _compact(value, limit=_EVENTS_QUERY_MAX_DATA_TEXT_CHARS)
            if text:
                compact[key] = text
            continue
        if isinstance(value, (bool, int, float)):
            compact[key] = value
            continue
        if isinstance(value, (list, tuple, set)):
            items: List[Any] = []
            for item in value:
                if isinstance(item, str):
                    text = _compact(item, limit=_EVENTS_QUERY_MAX_DATA_TEXT_CHARS)
                    if text:
                        items.append(text)
                elif isinstance(item, (bool, int, float)):
                    items.append(item)
                if len(items) >= 8:
                    break
            if items:
                compact[key] = items
    return compact


def _events_query_compact_event_for_llm(event: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    source = _text(event.get("source"))
    data_payload = dict(event.get("data")) if isinstance(event.get("data"), dict) else {}
    face_context_resolver = globals().get("_face_event_context")
    if callable(face_context_resolver):
        try:
            data_payload.update(face_context_resolver(client or globals().get("redis_client"), event) or {})
        except Exception:
            pass
    area = _events_query_source_to_area(source) or _text(data_payload.get("area"))
    compact: Dict[str, Any] = {
        "event_id": _events_query_event_id(event),
        "source": _compact(source, limit=120),
        "area": _compact(area, limit=120),
        "ha_time": _text(event.get("ha_time")),
        "title": _compact(event.get("title"), limit=_EVENTS_QUERY_MAX_TITLE_CHARS),
        "message": _compact(event.get("message"), limit=_EVENTS_QUERY_MAX_MESSAGE_CHARS),
        "type": _compact(event.get("type"), limit=120),
        "entity_id": _compact(event.get("entity_id"), limit=180),
        "level": _compact(event.get("level"), limit=40),
    }
    compact_data = _events_query_compact_data(data_payload)
    if compact_data:
        compact["data"] = compact_data
    return compact


def _events_query_estimate_tokens(value: Any) -> int:
    try:
        serialized = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        serialized = _text(value)
    chars_per_token = max(1, int(_EVENTS_QUERY_CHARS_PER_TOKEN_ESTIMATE))
    return max(1, (len(serialized) + chars_per_token - 1) // chars_per_token)


def _events_query_budget_rows(
    rows: List[Dict[str, Any]],
    *,
    token_budget: int = _EVENTS_QUERY_INPUT_TOKEN_BUDGET,
    max_rows: int = _EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER,
) -> Tuple[List[Dict[str, Any]], int, int]:
    if not rows:
        return [], 0, 0
    budget = max(256, int(token_budget))
    limit = max(1, int(max_rows))
    selected_reversed: List[Dict[str, Any]] = []
    tokens_used = 0
    for row in reversed(rows):
        if len(selected_reversed) >= limit:
            break
        row_tokens = _events_query_estimate_tokens(row)
        if selected_reversed and tokens_used + row_tokens > budget:
            break
        selected_reversed.append(row)
        tokens_used += row_tokens
        if tokens_used >= budget:
            break
    selected = list(reversed(selected_reversed))
    return selected, max(0, len(rows) - len(selected)), tokens_used


def _events_query_rollup_events(candidate_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not candidate_events:
        return []
    dated: List[datetime] = []
    for item in candidate_events:
        if not isinstance(item, dict):
            continue
        event_dt = _events_query_event_dt(item)
        if event_dt is not None:
            dated.append(event_dt)
    span_seconds = 0.0
    if dated:
        span_seconds = max(0.0, (max(dated) - min(dated)).total_seconds())
    if span_seconds <= 2 * 86400:
        bucket_seconds = 3600
    elif span_seconds <= 14 * 86400:
        bucket_seconds = 6 * 3600
    else:
        bucket_seconds = 86400

    buckets: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for event in candidate_events:
        if not isinstance(event, dict):
            continue
        event_dt = _events_query_event_dt(event)
        if event_dt is not None:
            bucket_epoch = int(event_dt.timestamp()) // bucket_seconds * bucket_seconds
            bucket_token = datetime.fromtimestamp(bucket_epoch).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            bucket_token = _text(event.get("ha_time"))[:13]
        source = _text(event.get("source"))
        event_type = _text(event.get("type"))
        entity_id = _text(event.get("entity_id"))
        key = (bucket_token, source, event_type, entity_id)
        row = buckets.get(key)
        if row is None:
            row = {
                "kind": "event_rollup",
                "source": source,
                "area": _text(event.get("area")),
                "type": event_type,
                "entity_id": entity_id,
                "first_time": _text(event.get("ha_time")),
                "last_time": _text(event.get("ha_time")),
                "event_count": 0,
                "sample_titles": [],
                "sample_messages": [],
            }
            buckets[key] = row
        row["event_count"] = int(row.get("event_count") or 0) + 1
        row["last_time"] = _text(event.get("ha_time")) or row.get("last_time")
        for field, sample_field in (("title", "sample_titles"), ("message", "sample_messages")):
            value = _text(event.get(field))
            samples = row[sample_field]
            if value and value not in samples and len(samples) < _EVENTS_QUERY_MAX_ROLLUP_SAMPLES:
                samples.append(value)

    return list(buckets.values())


def _events_query_is_immediate_query(user_query: Any) -> bool:
    return bool(_EVENTS_QUERY_IMMEDIATE_RE.search(_text(user_query)))


def _events_query_is_context_limit_error(error: Any) -> bool:
    text = _text(error).lower()
    return bool(
        "context size" in text
        or "context length" in text
        or "maximum context" in text
        or "too many tokens" in text
    )


def _events_query_deterministic_summary(
    *,
    interpretation: Dict[str, Any],
    relevant_events: List[Dict[str, Any]],
    omitted_count: int = 0,
) -> str:
    time_label = _text(interpretation.get("time_label")) or "the requested period"
    if not relevant_events:
        return f"No matching awareness events were recorded during {time_label}."

    represented_count = sum(
        max(1, int(item.get("event_count") or 1))
        for item in relevant_events
        if isinstance(item, dict)
    )
    highlights: List[str] = []
    for item in relevant_events[-3:]:
        if not isinstance(item, dict):
            continue
        count = max(1, int(item.get("event_count") or 1))
        first_time = _text(item.get("first_time") or item.get("ha_time"))
        last_time = _text(item.get("last_time") or item.get("ha_time"))
        time_text = first_time[11:16] if len(first_time) >= 16 else first_time
        if last_time and last_time != first_time and len(last_time) >= 16:
            time_text = f"{time_text}-{last_time[11:16]}" if time_text else last_time[11:16]
        messages = item.get("sample_messages") if isinstance(item.get("sample_messages"), list) else []
        detail = _text(messages[-1] if messages else item.get("message") or item.get("title") or item.get("type"))
        if not detail:
            continue
        count_text = f" ({count} events)" if count > 1 else ""
        highlights.append(f"{time_text}: {detail}{count_text}" if time_text else f"{detail}{count_text}")

    summary = f"Awareness recorded {represented_count} matching event{'s' if represented_count != 1 else ''} during {time_label}."
    if highlights:
        summary += " Latest highlights: " + "; ".join(highlights) + "."
    if omitted_count > 0:
        summary += " This summary is based on the latest bounded event evidence."
    return summary


def _events_query_query_from_args(args: Dict[str, Any], origin: Optional[Dict[str, Any]] = None) -> str:
    payload = args if isinstance(args, dict) else {}
    for key in ("query", "request", "question", "user_query", "prompt", "text", "content", "message"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    arg_origin = payload.get("origin")
    if isinstance(arg_origin, dict):
        for key in ("request_text", "query", "question", "text", "content", "message"):
            value = arg_origin.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    if isinstance(origin, dict):
        for key in ("request_text", "query", "question", "text", "content", "message", "raw_message", "body"):
            value = origin.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _events_query_parse_local_iso(value: Any) -> Optional[datetime]:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _json_object_from_text(text: Any) -> Dict[str, Any]:
    raw = _text(text)
    if not raw:
        return {}
    candidates: List[str] = []

    def _add(candidate: Any) -> None:
        token = _text(candidate).strip()
        if token and token not in candidates:
            candidates.append(token)

    _add(raw)
    if "<|message|>" in raw:
        _add(raw.rsplit("<|message|>", 1)[-1])
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        _add("\n".join(lines))
    _add(extract_json(raw))

    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        _add(raw[start : end + 1])

    for candidate in candidates:
        repaired = re.sub(r",\s*([}\]])", r"\1", candidate)
        for token in (candidate, repaired):
            try:
                payload = json.loads(token)
                if isinstance(payload, dict):
                    return payload
            except Exception:
                pass
            try:
                payload = ast.literal_eval(token)
                if isinstance(payload, dict):
                    return payload
            except Exception:
                pass
    return {}




async def _events_query_llm_json_object(
    *,
    llm_client: Any,
    system_prompt: str,
    user_payload: Dict[str, Any],
    max_tokens: Optional[int] = None,
    temperature: float = 0.0,
) -> Tuple[Optional[Dict[str, Any]], str]:
    if llm_client is None:
        return None, "LLM client is unavailable."
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            temperature=temperature,
            max_tokens=None if max_tokens is None else max(80, int(max_tokens)),
            timeout_ms=45_000,
        )
    except Exception as exc:
        return None, f"LLM request failed: {exc}"

    raw = _text((response.get("message") or {}).get("content"))
    obj = _json_object_from_text(raw)
    if not obj:
        logger.warning("[awareness] events_query LLM returned invalid JSON: %r", raw[:500])
        return None, "Could not parse LLM JSON."
    if not isinstance(obj, dict):
        return None, "LLM did not return a JSON object."
    return obj, ""


async def _events_query_interpret_query(
    *,
    llm_client: Any,
    user_query: str,
    sources: List[str],
    now_local: datetime,
) -> Tuple[Optional[Dict[str, Any]], str]:
    source_rows = [{"source_id": source, "area_name": _events_query_source_to_area(source)} for source in sources]
    system_prompt = (
        "You interpret natural-language event-history requests.\n"
        "Return exactly one strict JSON object with this schema:\n"
        "{"
        "\"query_type\":\"summary|presence|count|semantic_search|timeline\","
        "\"search_scope\":\"selected_sources|all_sources\","
        "\"source_ids\":[\"<source_id>\"],"
        "\"time_window\":{\"start_local\":\"YYYY-MM-DDTHH:MM:SS\",\"end_local\":\"YYYY-MM-DDTHH:MM:SS\",\"label\":\"...\"},"
        "\"semantic_focus\":[\"...\"],"
        "\"response_mode\":\"summary|presence|count|matches\""
        "}\n"
        "Rules:\n"
        "- Use only source_ids from the provided source catalog.\n"
        "- If the user asks broadly (for example around the house/outside), use search_scope=all_sources.\n"
        "- time_window must always include both start_local and end_local in local naive ISO.\n"
        f"- For right now, just now, currently, or at the moment, use only the last {_EVENTS_QUERY_IMMEDIATE_WINDOW_MINUTES} minutes ending at now; never expand those phrases to the start of today.\n"
        "- Preserve user intent including area, timeframe, and semantic details (people/clothing/vehicles/packages/animals/unusual activity).\n"
        "- Do not answer the user.\n"
        "- Do not invent sources that are not in the catalog.\n"
    )
    payload = {
        "user_query": user_query,
        "now_local": now_local.strftime("%Y-%m-%dT%H:%M:%S"),
        "available_sources": source_rows,
    }
    return await _events_query_llm_json_object(
        llm_client=llm_client,
        system_prompt=system_prompt,
        user_payload=payload,
        max_tokens=None,
        temperature=0.0,
    )


def _events_query_normalize_interpretation(
    *,
    interpretation: Dict[str, Any],
    sources_catalog: List[str],
    now_local: datetime,
    user_query: str = "",
) -> Tuple[Optional[Dict[str, Any]], str]:
    catalog = set(sources_catalog)
    query_type = _text(interpretation.get("query_type")).lower()
    if query_type not in {"summary", "presence", "count", "semantic_search", "timeline"}:
        query_type = "summary"

    response_mode = _text(interpretation.get("response_mode")).lower()
    if response_mode not in {"summary", "presence", "count", "matches"}:
        response_mode = "summary"

    search_scope = _text(interpretation.get("search_scope")).lower()
    source_ids_raw = interpretation.get("source_ids") if isinstance(interpretation.get("source_ids"), list) else []
    source_ids = [str(item).strip() for item in source_ids_raw if str(item).strip() in catalog]

    if search_scope == "all_sources":
        selected_sources = list(sources_catalog)
    else:
        selected_sources = sorted(set(source_ids))
    if not selected_sources:
        return None, "Could not resolve relevant event sources from request interpretation."

    time_window = interpretation.get("time_window") if isinstance(interpretation.get("time_window"), dict) else {}
    start_local = _events_query_parse_local_iso(time_window.get("start_local"))
    end_local = _events_query_parse_local_iso(time_window.get("end_local"))
    label = _text(time_window.get("label")) or "requested timeframe"
    if start_local is None or end_local is None:
        return None, "Could not resolve a valid timeframe from request interpretation."
    if end_local < start_local:
        return None, "Interpreted timeframe end is earlier than start."
    if end_local > now_local and response_mode == "presence":
        end_local = now_local
    if _events_query_is_immediate_query(user_query):
        end_local = now_local
        start_local = now_local - timedelta(minutes=_EVENTS_QUERY_IMMEDIATE_WINDOW_MINUTES)
        label = f"the last {_EVENTS_QUERY_IMMEDIATE_WINDOW_MINUTES} minutes"

    focus_raw = interpretation.get("semantic_focus") if isinstance(interpretation.get("semantic_focus"), list) else []
    semantic_focus = [str(item).strip() for item in focus_raw if str(item).strip()][:24]
    broad_summary = bool(
        query_type in {"summary", "timeline"}
        and response_mode == "summary"
        and not semantic_focus
    )
    return (
        {
            "query_type": query_type,
            "response_mode": response_mode,
            "search_scope": search_scope,
            "selected_sources": selected_sources,
            "time_label": label,
            "time_start": start_local,
            "time_end": end_local,
            "semantic_focus": semantic_focus,
            "broad_summary": broad_summary,
        },
        "",
    )


async def _events_query_select_relevant_event_ids(
    *,
    llm_client: Any,
    user_query: str,
    interpretation: Dict[str, Any],
    candidate_events: List[Dict[str, Any]],
) -> Tuple[Optional[List[str]], str]:
    if not candidate_events:
        return [], ""

    system_prompt = (
        "You are selecting relevant home events for a user question.\n"
        "Return exactly one strict JSON object:\n"
        "{"
        "\"relevant_event_ids\":[\"ev_...\"],"
        "\"confidence\":\"high|medium|low\""
        "}\n"
        "Rules:\n"
        "- Select only event_ids that are directly relevant to the user's request.\n"
        "- Use only event_ids from the provided candidate list.\n"
        "- If none are relevant, return an empty list.\n"
        f"- Return at most {_EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER} event_ids. If more match, choose the most relevant and most recent.\n"
        "- Do not invent events.\n"
    )
    payload = {
        "user_query": user_query,
        "interpreted_request": {
            "query_type": interpretation.get("query_type"),
            "response_mode": interpretation.get("response_mode"),
            "time_label": interpretation.get("time_label"),
            "semantic_focus": interpretation.get("semantic_focus"),
            "broad_summary": bool(interpretation.get("broad_summary")),
        },
        "candidate_events": candidate_events,
    }
    obj, err = await _events_query_llm_json_object(
        llm_client=llm_client,
        system_prompt=system_prompt,
        user_payload=payload,
        max_tokens=None,
        temperature=0.0,
    )
    if obj is None:
        return None, err or "Could not determine relevant events."
    relevant_raw = obj.get("relevant_event_ids") if isinstance(obj.get("relevant_event_ids"), list) else []
    valid_ids = {str(item.get("event_id") or "").strip() for item in candidate_events if isinstance(item, dict)}
    selected = [str(item).strip() for item in relevant_raw if str(item).strip() in valid_ids]
    deduped = list(dict.fromkeys(selected))
    return deduped, ""


async def _events_query_compose_final_answer(
    *,
    llm_client: Any,
    user_query: str,
    interpretation: Dict[str, Any],
    relevant_events: List[Dict[str, Any]],
    candidate_count: int,
    prior_omitted_count: int = 0,
) -> Tuple[Optional[str], str]:
    bounded_events, omitted_count, estimated_tokens = _events_query_budget_rows(
        relevant_events,
        token_budget=_EVENTS_QUERY_INPUT_TOKEN_BUDGET,
        max_rows=_EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER,
    )
    omitted_count += max(0, int(prior_omitted_count or 0))
    fallback_text = _events_query_deterministic_summary(
        interpretation=interpretation,
        relevant_events=bounded_events,
        omitted_count=omitted_count,
    )
    if llm_client is None:
        return fallback_text, ""
    system_prompt = (
        "You answer a homeowner's event-history question using only provided events.\n"
        "Rules:\n"
        "- Base the answer only on relevant_events.\n"
        "- Rows with kind=event_rollup summarize repeated events; respect event_count and the first/last timestamps.\n"
        "- If evidence is missing, say so clearly and do not guess.\n"
        "- If evidence_truncated is true, make clear that the answer is based on the supplied latest evidence.\n"
        "- Be concise and conversational.\n"
        "- Mention area/time naturally when useful.\n"
        "- For count questions, provide the count from evidence.\n"
        "- For presence questions, answer yes/no with evidence confidence from data.\n"
        "- Do not mention internal tools or prompts.\n"
    )

    def _payload_for(events: List[Dict[str, Any]], omitted: int, token_estimate: int) -> Dict[str, Any]:
        represented_count = sum(
            max(1, int(item.get("event_count") or 1))
            for item in events
            if isinstance(item, dict)
        )
        return {
            "user_query": user_query,
            "interpreted_request": {
                "query_type": interpretation.get("query_type"),
                "response_mode": interpretation.get("response_mode"),
                "time_label": interpretation.get("time_label"),
                "semantic_focus": interpretation.get("semantic_focus"),
                "sources": interpretation.get("selected_sources"),
            },
            "candidate_event_count": int(candidate_count),
            "represented_event_count": int(represented_count),
            "evidence_row_count": int(len(events)),
            "evidence_omitted_row_count": int(omitted),
            "evidence_truncated": bool(omitted),
            "estimated_evidence_tokens": int(token_estimate),
            "relevant_events": events,
        }

    payload = _payload_for(bounded_events, omitted_count, estimated_tokens)
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0.15,
            max_tokens=None,
            timeout_ms=45_000,
        )
    except Exception as exc:
        if not _events_query_is_context_limit_error(exc) or len(bounded_events) <= 1:
            logger.warning("[awareness] events_query final answer failed; using bounded fallback: %s", exc)
            return fallback_text, ""
        retry_events, retry_omitted, retry_tokens = _events_query_budget_rows(
            bounded_events,
            token_budget=_EVENTS_QUERY_RETRY_TOKEN_BUDGET,
            max_rows=max(1, _EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER // 2),
        )
        retry_omitted += omitted_count
        logger.warning(
            "[awareness] events_query final prompt exceeded context; retrying rows=%s omitted=%s estimated_tokens=%s",
            len(retry_events),
            retry_omitted,
            retry_tokens,
        )
        payload = _payload_for(retry_events, retry_omitted, retry_tokens)
        try:
            response = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=0.15,
                max_tokens=None,
                timeout_ms=45_000,
            )
        except Exception as retry_exc:
            logger.warning(
                "[awareness] events_query smaller final-answer retry failed; using bounded fallback: %s",
                retry_exc,
            )
            return _events_query_deterministic_summary(
                interpretation=interpretation,
                relevant_events=retry_events,
                omitted_count=retry_omitted,
            ), ""

    text = _text((response.get("message") or {}).get("content"))
    if not text:
        logger.warning("[awareness] events_query final answer was empty; using bounded fallback")
        return fallback_text, ""
    return text, ""


async def _events_query_kernel(
    *,
    args: Optional[Dict[str, Any]],
    llm_client: Any,
    origin: Optional[Dict[str, Any]],
    redis_obj: Any,
) -> Dict[str, Any]:
    query = _events_query_query_from_args(args or {}, origin=origin)
    if not query:
        return {
            "tool": "events_query",
            "ok": False,
            "error": "missing_query",
            "summary_for_user": "I need a natural-language query to search event history.",
            "needs": ["query"],
        }

    sources_catalog = _discover_event_sources(redis_obj)
    if not sources_catalog:
        return {
            "tool": "events_query",
            "ok": False,
            "error": "events_sources_missing",
            "summary_for_user": "No awareness event sources are available yet.",
        }

    now_local = datetime.now()
    interpretation_obj, interpretation_err = await _events_query_interpret_query(
        llm_client=llm_client,
        user_query=query,
        sources=sources_catalog,
        now_local=now_local,
    )
    if interpretation_obj is None:
        return {
            "tool": "events_query",
            "ok": False,
            "error": "interpretation_failed",
            "summary_for_user": "I couldn't interpret that event-history request. Try rephrasing with area/time details.",
            "details": interpretation_err or "unknown error",
        }

    interpreted, interpreted_err = _events_query_normalize_interpretation(
        interpretation=interpretation_obj,
        sources_catalog=sources_catalog,
        now_local=now_local,
        user_query=query,
    )
    if interpreted is None:
        return {
            "tool": "events_query",
            "ok": False,
            "error": "interpretation_invalid",
            "summary_for_user": "I couldn't resolve a valid area/time window for that request.",
            "details": interpreted_err,
        }

    selected_sources = interpreted["selected_sources"]
    start_dt = interpreted["time_start"]
    end_dt = interpreted["time_end"]
    logger.info(
        "[awareness] events_query interpreted query_type=%s response_mode=%s sources=%s window=%s..%s label=%s broad_summary=%s",
        interpreted.get("query_type"),
        interpreted.get("response_mode"),
        ",".join(selected_sources),
        start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        interpreted.get("time_label"),
        bool(interpreted.get("broad_summary")),
    )

    fetched = _load_events_for_sources(
        redis_obj,
        selected_sources,
        start_dt,
        end_dt,
        limit_per_source=_EVENTS_QUERY_MAX_EVENTS_PER_SOURCE,
    )
    fetched_sorted = sorted(fetched, key=lambda item: _events_query_event_dt(item) or datetime.min)
    all_compact_events = [_events_query_compact_event_for_llm(item, redis_obj) for item in fetched_sorted]
    compact_events = all_compact_events[-_EVENTS_QUERY_MAX_CANDIDATE_EVENTS_FOR_LLM:]
    source_candidate_omitted = max(0, len(all_compact_events) - len(compact_events))
    relevant_events: List[Dict[str, Any]] = []
    evidence_omitted = 0
    evidence_tokens = 0

    if bool(interpreted.get("broad_summary")):
        rollups = _events_query_rollup_events(all_compact_events)
        relevant_events, evidence_omitted, evidence_tokens = _events_query_budget_rows(
            rollups,
            token_budget=_EVENTS_QUERY_INPUT_TOKEN_BUDGET,
            max_rows=_EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER,
        )
        logger.info(
            "[awareness] events_query broad summary aggregated events=%s rollups=%s evidence_rows=%s omitted_rows=%s estimated_tokens=%s",
            len(all_compact_events),
            len(rollups),
            len(relevant_events),
            evidence_omitted,
            evidence_tokens,
        )
    else:
        compact_events, candidate_omitted, candidate_tokens = _events_query_budget_rows(
            compact_events,
            token_budget=_EVENTS_QUERY_INPUT_TOKEN_BUDGET,
            max_rows=_EVENTS_QUERY_MAX_CANDIDATE_EVENTS_FOR_LLM,
        )
        candidate_omitted += source_candidate_omitted
        logger.info(
            "[awareness] events_query relevance candidates=%s omitted_rows=%s estimated_tokens=%s",
            len(compact_events),
            candidate_omitted,
            candidate_tokens,
        )
        relevant_ids, relevance_err = await _events_query_select_relevant_event_ids(
            llm_client=llm_client,
            user_query=query,
            interpretation=interpreted,
            candidate_events=compact_events,
        )
        if relevant_ids is None:
            return {
                "tool": "events_query",
                "ok": False,
                "error": "relevance_selection_failed",
                "summary_for_user": "I couldn't determine which events were relevant. Please try that request again.",
                "details": relevance_err or "unknown error",
            }
        event_by_id = {str(item.get("event_id") or ""): item for item in compact_events}
        selected_events = [event_by_id[event_id] for event_id in relevant_ids if event_id in event_by_id]
        relevant_events, evidence_omitted, evidence_tokens = _events_query_budget_rows(
            selected_events,
            token_budget=_EVENTS_QUERY_INPUT_TOKEN_BUDGET,
            max_rows=_EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER,
        )
        evidence_omitted += candidate_omitted

    logger.info(
        "[awareness] events_query fetched_events=%s candidate_events=%s relevant_events=%s omitted_rows=%s estimated_tokens=%s",
        len(fetched_sorted),
        len(all_compact_events),
        len(relevant_events),
        evidence_omitted,
        evidence_tokens,
    )

    final_text, final_err = await _events_query_compose_final_answer(
        llm_client=llm_client,
        user_query=query,
        interpretation=interpreted,
        relevant_events=relevant_events,
        candidate_count=len(all_compact_events),
        prior_omitted_count=evidence_omitted,
    )
    if final_text is None:
        return {
            "tool": "events_query",
            "ok": False,
            "error": "final_answer_failed",
            "summary_for_user": "I couldn't finish the event-history answer this time.",
            "details": final_err or "unknown error",
        }

    return {
        "tool": "events_query",
        "ok": True,
        "query": query,
        "intent": interpreted.get("query_type"),
        "response_mode": interpreted.get("response_mode"),
        "timeframe": interpreted.get("time_label"),
        "sources": list(selected_sources),
        "candidate_event_count": int(len(all_compact_events)),
        "relevant_event_count": int(
            sum(max(1, int(item.get("event_count") or 1)) for item in relevant_events)
        ),
        "evidence_row_count": int(len(relevant_events)),
        "evidence_omitted_row_count": int(evidence_omitted),
        "time_window": {
            "start_local": start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "end_local": end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "label": interpreted.get("time_label"),
        },
        "semantic_focus": list(interpreted.get("semantic_focus") or []),
        "summary_for_user": final_text,
    }


def _event_time_display(value: Any) -> str:
    parsed = _parse_iso(value)
    if parsed is not None:
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    raw = _text(value)
    return raw or "n/a"


def _load_event_snapshot_payload(client: Any, snapshot_id: str) -> Optional[Dict[str, Any]]:
    sid = _text(snapshot_id)
    if not sid:
        return None
    redis_obj = client or redis_client
    if redis_obj is None:
        return None
    try:
        raw = redis_obj.get(_event_snapshot_key(sid))
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _load_event_clip_payload(client: Any, clip_id: str) -> Optional[Dict[str, Any]]:
    cid = _text(clip_id).lower()
    if not re.fullmatch(r"[a-f0-9]{32}", cid):
        return None
    redis_obj = client or redis_client
    blob_obj = _event_clip_blob_client(redis_obj)
    if redis_obj is None or blob_obj is None:
        return None
    try:
        raw_meta = redis_obj.get(_event_clip_meta_key(cid))
        raw_clip = blob_obj.get(_event_clip_key(cid))
    except Exception:
        return None
    if not raw_meta or raw_clip is None:
        return None
    try:
        metadata = json.loads(raw_meta)
    except Exception:
        return None
    if not isinstance(metadata, dict):
        return None
    if isinstance(raw_clip, bytearray):
        clip_bytes = bytes(raw_clip)
    elif isinstance(raw_clip, bytes):
        clip_bytes = raw_clip
    else:
        return None
    if not clip_bytes:
        return None
    return {**metadata, "bytes_data": clip_bytes}


def _load_stored_event_by_id(
    client: Any,
    *,
    source: Any,
    event_id: Any,
) -> Optional[Dict[str, Any]]:
    redis_obj = client or redis_client
    target_id = _text(event_id)
    if redis_obj is None or not target_id:
        return None
    keys: List[str] = []
    source_name = _text(source)
    if source_name:
        keys.append(_event_key(source_name))
    try:
        discovered = list(redis_obj.scan_iter(match=f"{_EVENTS_PREFIX}*"))
    except Exception:
        discovered = []
    for raw_key in discovered:
        key = _text(raw_key)
        if key and key not in keys:
            keys.append(key)
    for key in keys:
        try:
            rows = redis_obj.lrange(key, 0, -1) or []
        except Exception:
            continue
        for row in rows:
            try:
                event = json.loads(row) if isinstance(row, (str, bytes, bytearray)) else row
            except Exception:
                continue
            if isinstance(event, dict) and _text(event.get("id")) == target_id:
                return event
    return None


def _update_stored_event_notification(
    client: Any,
    event: Dict[str, Any],
    *,
    status: str,
    sent_count: int,
    errors: List[str],
) -> None:
    redis_obj = client or redis_client
    if redis_obj is None or not isinstance(event, dict):
        return
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    next_data = dict(data)
    next_data.update(
        {
            "notification_status": _text(status),
            "notification_sent_count": max(0, int(sent_count or 0)),
            "notification_at": _now_iso(),
        }
    )
    cleaned_errors = [_compact(error, limit=180) for error in errors if _text(error)]
    if cleaned_errors:
        next_data["notification_errors"] = cleaned_errors
    else:
        next_data.pop("notification_errors", None)
    event["data"] = next_data

    event_id = _text(event.get("id"))
    source = _text(event.get("source") or next_data.get("area"))
    if not event_id or not source:
        return
    key = _event_key(source)
    try:
        rows = redis_obj.lrange(key, 0, -1) or []
    except Exception:
        return
    for index, row in enumerate(rows):
        try:
            stored = json.loads(row) if isinstance(row, (str, bytes, bytearray)) else row
        except Exception:
            continue
        if not isinstance(stored, dict) or _text(stored.get("id")) != event_id:
            continue
        stored["data"] = next_data
        try:
            redis_obj.lset(key, index, json.dumps(stored))
        except Exception:
            logger.debug("[awareness] failed to save notification status for %s", event_id, exc_info=True)
        return


def _notification_media_extension(content_type: Any, *, fallback: str) -> str:
    media_type = _text(content_type).split(";", 1)[0].strip().lower()
    extensions = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/gif": "gif",
        "video/mp4": "mp4",
        "video/webm": "webm",
        "video/quicktime": "mov",
        "video/x-matroska": "mkv",
        "video/mpeg": "mpeg",
        "video/x-msvideo": "avi",
    }
    return extensions.get(media_type, fallback)


def _notification_attachments_for_event(client: Any, event: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    event_id = _text(event.get("id")) or "event"
    clip_id = _text(event.get("clip_id") or data.get("clip_id"))
    clip = _load_event_clip_payload(client, clip_id) if clip_id else None
    if clip:
        content_type = _text(clip.get("content_type") or "video/mp4")
        return [
            {
                "type": "video",
                "name": f"awareness-{event_id}.{_notification_media_extension(content_type, fallback='mp4')}",
                "mimetype": content_type,
                "bytes": clip["bytes_data"],
            }
        ]

    snapshot_id = _text(event.get("snapshot_id") or data.get("snapshot_id"))
    snapshot = _load_event_snapshot_payload(client, snapshot_id) if snapshot_id else None
    if not snapshot:
        return []
    content_type = _text(snapshot.get("content_type") or "image/jpeg")
    try:
        image_bytes = base64.b64decode(_text(snapshot.get("data_b64")), validate=True)
    except Exception:
        return []
    if not image_bytes:
        return []
    return [
        {
            "type": "image",
            "name": f"awareness-{event_id}.{_notification_media_extension(content_type, fallback='jpg')}",
            "mimetype": content_type,
            "bytes": image_bytes,
        }
    ]


def _notification_face_line(client: Any, event: Dict[str, Any]) -> str:
    context = _face_event_context(client, event)
    names: List[str] = []
    seen: set[str] = set()
    for value in context.get("known_people") or []:
        name = " ".join(_text(value).split())
        if not name or name.casefold() in seen:
            continue
        seen.add(name.casefold())
        names.append(name)
    unknown_count = _as_int(context.get("unknown_face_count"), 0, minimum=0)
    parts: List[str] = []
    if names:
        if len(names) == 1:
            people_text = names[0]
        elif len(names) == 2:
            people_text = f"{names[0]} and {names[1]}"
        else:
            people_text = f"{', '.join(names[:-1])}, and {names[-1]}"
        parts.append(f"{people_text} recognized")
    if unknown_count:
        parts.append(
            f"{unknown_count} unknown {'person' if unknown_count == 1 else 'people'} detected"
        )
    return f"Face ID: {'; '.join(parts)}." if parts else ""


async def _dispatch_awareness_event_notification(
    monitor: Dict[str, Any],
    event: Dict[str, Any],
) -> Dict[str, Any]:
    if not _bool(monitor.get("notifications_enabled"), False):
        return {"ok": True, "skipped": "disabled", "sent_count": 0}

    destinations = _normalize_notification_destinations(monitor.get("notification_destinations"))
    if not callable(dispatch_notification) or not destinations:
        reason = (
            "Shared notification delivery is unavailable."
            if not callable(dispatch_notification)
            else "No notification destination is configured."
        )
        _update_stored_event_notification(
            redis_client,
            event,
            status="failed",
            sent_count=0,
            errors=[reason],
        )
        logger.warning("[awareness] %s", reason)
        return {"ok": False, "sent_count": 0, "errors": [reason]}

    content = _compact(event.get("message"), limit=1200) or "Awareness recorded an event."
    face_line = _notification_face_line(redis_client, event)
    if face_line:
        content = f"{content}\n\n{face_line}"
    attachments = _notification_attachments_for_event(redis_client, event)
    title = _compact(event.get("title"), limit=120) or "Tater Awareness"
    event_id = _text(event.get("id"))
    monitor_id = _text(monitor.get("id"))
    event_type = _text(event.get("type") or "event")
    sent = 0
    errors: List[str] = []
    for encoded in destinations:
        destination = _decode_notification_destination(encoded)
        if not destination:
            continue
        try:
            result = await dispatch_notification(
                platform=destination["platform"],
                title=title,
                content=content,
                targets=destination["targets"],
                origin={
                    "platform": "awareness_core",
                    "source": "awareness_core",
                    "scope": monitor_id,
                    "monitor_id": monitor_id,
                    "event_id": event_id,
                },
                meta={"priority": "normal", "tags": ["awareness", event_type]},
                attachments=attachments or None,
            )
            result_text = _text(result)
            if not result_text or result_text.lower().startswith("queued notification"):
                sent += 1
            else:
                errors.append(result_text)
        except Exception as exc:
            errors.append(_compact(str(exc), limit=180) or "Notification delivery failed.")

    status = "sent" if sent else "failed"
    _update_stored_event_notification(
        redis_client,
        event,
        status=status,
        sent_count=sent,
        errors=errors,
    )
    if errors:
        logger.warning(
            "[awareness] notification delivery completed with %s error(s) for event %s",
            len(errors),
            event_id,
        )
    return {"ok": sent > 0, "sent_count": sent, "errors": errors}


async def _deliver_awareness_event_notification(
    monitor: Dict[str, Any],
    event: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        return await _dispatch_awareness_event_notification(monitor, event)
    except Exception as exc:
        reason = _compact(str(exc), limit=180) or "Notification delivery failed."
        logger.exception(
            "[awareness] notification delivery failed without affecting event %s",
            _text(event.get("id")),
        )
        _update_stored_event_notification(
            redis_client,
            event,
            status="failed",
            sent_count=0,
            errors=[reason],
        )
        return {"ok": False, "sent_count": 0, "errors": [reason]}


def get_htmlui_tab_media(
    *,
    media_id: str,
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    payload = _load_event_clip_payload(redis_client or globals().get("redis_client"), media_id)
    if payload is None:
        raise KeyError("Awareness event clip not found.")
    content_type = _text(payload.get("content_type") or "video/mp4")
    extension = {
        "video/webm": "webm",
        "video/quicktime": "mov",
        "video/x-matroska": "mkv",
        "video/mpeg": "mpeg",
        "video/x-msvideo": "avi",
    }.get(content_type.lower(), "mp4")
    return {
        "bytes": payload["bytes_data"],
        "content_type": content_type,
        "filename": f"awareness-event-{_text(media_id)}.{extension}",
    }


def _event_snapshot_preview(client: Any, event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    snapshot_id = _text(event.get("snapshot_id") or data.get("snapshot_id"))
    if not snapshot_id:
        return {}
    payload = _load_event_snapshot_payload(client, snapshot_id)
    if payload is None:
        return {
            "snapshot_id": snapshot_id,
            "bytes": _as_int(data.get("snapshot_bytes"), 0, minimum=0),
            "content_type": _text(data.get("snapshot_content_type") or "image/jpeg"),
            "status": "missing",
        }
    content_type = _text(payload.get("content_type") or "image/jpeg")
    byte_count = _as_int(payload.get("bytes"), 0, minimum=0)
    data_b64 = _text(payload.get("data_b64"))
    preview: Dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "bytes": byte_count,
        "content_type": content_type,
    }
    if data_b64:
        preview["data_url"] = f"data:{content_type};base64,{data_b64}"
    return preview


def _event_clip_preview(event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    clip_id = _text(event.get("clip_id") or data.get("clip_id")).lower()
    if not re.fullmatch(r"[a-f0-9]{32}", clip_id):
        return {}
    return {
        "clip_id": clip_id,
        "bytes": _as_int(data.get("clip_stored_bytes") or data.get("clip_bytes"), 0, minimum=0),
        "content_type": _text(data.get("clip_content_type") or "video/mp4"),
        "url": f"/api/cores/awareness_core/media/{quote(clip_id)}",
    }


def _event_type_filters(client: Any) -> Dict[str, bool]:
    runtime = _runtime_get(client)
    return {
        key: _bool(runtime.get(runtime_key), _EVENT_FILTER_DEFAULTS.get(key, True))
        for key, runtime_key in _EVENT_FILTER_RUNTIME_KEYS.items()
    }


def _event_list_view_enabled(client: Any) -> bool:
    runtime = _runtime_get(client)
    return _bool(runtime.get(_EVENT_LIST_VIEW_RUNTIME_KEY), False)


def _event_type_bucket(event: Dict[str, Any]) -> str:
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    event_type = _text(event.get("type")).lower()
    if event_type == "doorbell":
        return "doorbell"
    if event_type.startswith("camera"):
        return "camera"
    if "_sensor_" in event_type or _text(data.get("sensor_type")):
        return "sensor"
    entity_id = _text(event.get("entity_id")).lower()
    if entity_id.startswith("camera."):
        return "camera"
    if entity_id.startswith(("binary_sensor.", "sensor.", "cover.")):
        return "sensor"
    return "other"


def _event_allowed_by_filter(filters: Dict[str, bool], event_type: str) -> bool:
    if event_type not in filters:
        return True
    return bool(filters.get(event_type))




def _event_source_lengths(client: Any, sources: List[str]) -> Dict[str, int]:
    redis_obj = client or redis_client
    lengths: Dict[str, int] = {}
    if redis_obj is None:
        return {src: 0 for src in sources}
    for src in sources:
        try:
            lengths[src] = max(0, int(redis_obj.llen(_event_key(src)) or 0))
        except Exception:
            lengths[src] = 0
    return lengths


def _event_sort_dt(event: Dict[str, Any]) -> datetime:
    return _parse_iso(event.get("ha_time")) or datetime.min


def _load_filtered_event_prefix(
    client: Any,
    *,
    sources: List[str],
    source_lengths: Dict[str, int],
    filters: Dict[str, bool],
    read_per_source: int,
) -> List[Dict[str, Any]]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return []
    read_count = max(0, int(read_per_source))
    if read_count <= 0:
        return []
    start = datetime(1970, 1, 1)
    end = datetime.now() + timedelta(days=1)
    events: List[Dict[str, Any]] = []
    for src in sources:
        source_len = max(0, int(source_lengths.get(src, 0)))
        if source_len <= 0:
            continue
        end_index = min(read_count, source_len) - 1
        try:
            rows = redis_obj.lrange(_event_key(src), 0, end_index) or []
        except Exception:
            continue
        for row in rows:
            try:
                payload = json.loads(row)
            except Exception:
                continue
            ts = _parse_iso(payload.get("ha_time"))
            if ts is None or ts < start or ts > end:
                continue
            payload.setdefault("source", src)
            event_bucket = _event_type_bucket(payload)
            if not _event_allowed_by_filter(filters, event_bucket):
                continue
            events.append(payload)
    events.sort(
        key=lambda item: (
            _event_sort_dt(item),
            _text(item.get("id")),
            _text(item.get("entity_id")),
        ),
        reverse=True,
    )
    return events


def _event_forms_from_events(
    client: Any,
    events: List[Dict[str, Any]],
    *,
    list_view: bool,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for idx, event in enumerate(events):
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        face_context = _face_event_context(client, event)
        event_time = _event_time_display(event.get("ha_time"))
        source = _text(event.get("source"))
        area = _text(data.get("area")) or source
        event_type = _text(event.get("type"))
        entity_id = _text(event.get("entity_id"))
        title = _text(event.get("title"))
        if not title and event_type:
            title = event_type.replace("_", " ").title()
        if not title:
            title = "Awareness Event"
        subtitle_parts = [event_time]
        if area:
            subtitle_parts.append(f"Area: {area}")
        if entity_id:
            subtitle_parts.append(f"Entity: {entity_id}")
        known_people = [name for name in face_context.get("known_people") or [] if _text(name)]
        unknown_face_count = _as_int(face_context.get("unknown_face_count"), 0, minimum=0)
        if known_people:
            subtitle_parts.append(f"People: {', '.join(known_people)}")
        elif unknown_face_count:
            subtitle_parts.append(f"Unknown faces: {unknown_face_count}")
        description = _text(event.get("message"))
        subtitle = " • ".join([part for part in subtitle_parts if part])

        fields: List[Dict[str, Any]] = []
        snapshot = _event_snapshot_preview(client, event)
        clip = _event_clip_preview(event)
        snapshot_id = _text(snapshot.get("snapshot_id"))
        if (not list_view) and clip.get("url"):
            duration = _as_int(data.get("clip_duration_seconds"), 0, minimum=0)
            fields.append(
                {
                    "key": f"clip_{idx}",
                    "label": "Event Clip",
                    "type": "video",
                    "src": _text(clip.get("url")),
                    "content_type": _text(clip.get("content_type") or "video/mp4"),
                    "poster": _text(snapshot.get("data_url")),
                    "caption": f"{duration}-second event clip" if duration else "Camera event clip",
                    "preload": "metadata",
                    "controls": True,
                    "reset_to_poster": True,
                    "hide_label": True,
                }
            )
        elif (not list_view) and snapshot.get("data_url"):
            fields.append(
                {
                    "key": f"snapshot_{idx}",
                    "label": "Snapshot",
                    "type": "image",
                    "src": _text(snapshot.get("data_url")),
                    "alt": f"{title} snapshot",
                    "hide_label": True,
                }
            )
        elif snapshot_id and not list_view:
            fields.append(
                {
                    "key": f"snapshot_status_{idx}",
                    "label": "Snapshot",
                    "type": "text",
                    "value": f"Stored snapshot unavailable ({snapshot_id})",
                    "read_only": True,
                }
            )

        if description and not list_view:
            fields.append(
                {
                    "key": f"description_{idx}",
                    "label": "",
                    "type": "textarea",
                    "value": description,
                    "read_only": True,
                    "hide_label": True,
                }
            )

        face_status = _text(face_context.get("face_status"))
        if not list_view and (
            known_people or unknown_face_count or face_status in {"pending", "running", "capturing", "analyzing", "error"}
        ):
            if known_people:
                face_value = ", ".join(known_people)
            elif unknown_face_count:
                face_value = f"{unknown_face_count} unknown face{'s' if unknown_face_count != 1 else ''}"
            elif face_status in {"pending", "running", "capturing", "analyzing"}:
                face_value = "Face check in progress"
            else:
                face_value = _text(face_context.get("face_error")) or "Face check failed"
            fields.append(
                {
                    "key": f"people_{idx}",
                    "label": "People",
                    "type": "text",
                    "value": face_value,
                    "read_only": True,
                }
            )

        item_id = _text(event.get("id")) or f"event_{idx}_{_slug(_text(event.get('ha_time')) or str(idx))}"
        items.append(
            {
                "id": item_id,
                "group": "event_list" if list_view else "event",
                "card_variant": "event_list" if list_view else "",
                "title": title,
                "subtitle": subtitle,
                "detail": _compact(description, limit=240) if list_view else "",
                "hero_image_src": _text(snapshot.get("data_url")) if list_view else "",
                "hero_image_alt": f"{title} thumbnail" if list_view else "",
                "fields_popup": False,
                "fields_dropdown": False,
                "sections_in_dropdown": False,
                "fields": fields,
            }
        )
    return items


def _event_page_for_ui(
    client: Any,
    *,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
) -> Dict[str, Any]:
    filters = _event_type_filters(client)
    list_view = _event_list_view_enabled(client)
    runtime = _runtime_get(client)
    current_page = _as_int(
        page if page is not None else runtime.get(_EVENT_PAGE_RUNTIME_KEY),
        1,
        minimum=1,
    )
    current_page_size = _as_int(
        page_size if page_size is not None else _EVENT_PAGE_SIZE_DEFAULT,
        _EVENT_PAGE_SIZE_DEFAULT,
        minimum=1,
        maximum=_EVENT_PAGE_SIZE_MAX,
    )
    sources = _discover_event_sources(client)
    source_lengths = _event_source_lengths(client, sources)
    raw_total = sum(source_lengths.values())
    if not sources or raw_total <= 0:
        return {
            "items": [],
            "page": 1,
            "page_size": current_page_size,
            "page_count": 1,
            "total": 0,
        }

    max_source_len = max(source_lengths.values()) if source_lengths else 0
    offset = (current_page - 1) * current_page_size
    needed = offset + current_page_size
    read_per_source = min(max_source_len, max(current_page_size, needed))
    read_step = max(current_page_size * 4, 100)
    events: List[Dict[str, Any]] = []

    while True:
        events = _load_filtered_event_prefix(
            client,
            sources=sources,
            source_lengths=source_lengths,
            filters=filters,
            read_per_source=read_per_source,
        )
        exhausted = read_per_source >= max_source_len
        if len(events) >= needed or exhausted:
            break
        next_read = min(max_source_len, max(read_per_source + read_step, int(read_per_source * 1.5)))
        if next_read <= read_per_source:
            break
        read_per_source = next_read

    exact_total = read_per_source >= max_source_len
    total_for_pages = len(events) if exact_total else raw_total
    page_count = max(1, (max(0, total_for_pages) + current_page_size - 1) // current_page_size)
    if current_page > page_count:
        current_page = page_count
        offset = (current_page - 1) * current_page_size

    page_events = events[offset : offset + current_page_size]
    return {
        "items": _event_forms_from_events(client, page_events, list_view=list_view),
        "page": current_page,
        "page_size": current_page_size,
        "page_count": page_count,
        "total": max(0, total_for_pages),
    }




def _event_stats_for_ui(client: Any) -> Dict[str, Any]:
    counts = {
        "total": 0,
        "camera": 0,
        "doorbell": 0,
        "sensor": 0,
        "other": 0,
    }
    sources = _discover_event_sources(client)
    if not sources:
        return {"counts": counts, "source_count": 0, "last_event": "n/a"}
    events = _load_events_for_sources(
        client,
        sources=sources,
        start=datetime(1970, 1, 1),
        end=datetime.now() + timedelta(days=1),
        limit_per_source=0,
    )
    for event in events:
        counts["total"] += 1
        bucket = _event_type_bucket(event)
        if bucket in {"camera", "doorbell", "sensor"}:
            counts[bucket] += 1
        else:
            counts["other"] += 1
    last_event = _event_time_display(events[0].get("ha_time")) if events else "n/a"
    return {
        "counts": counts,
        "source_count": len(sources),
        "last_event": last_event,
    }


def _monitor_event_source(monitor: Dict[str, Any], entity_id: Any) -> Dict[str, Any]:
    entity_token = _text(entity_id).casefold()
    for source in monitor.get("event_sources") or []:
        if not isinstance(source, dict):
            continue
        _provider, raw_ref = _split_provider_ref(source.get("ref"), monitor.get("provider"))
        if _text(raw_ref or source.get("ref")).casefold() == entity_token:
            return source
    return {}


def _monitor_event_trigger(
    monitor: Dict[str, Any],
    entity_id: Any,
    new_state: Dict[str, Any],
    old_state: Dict[str, Any],
) -> str:
    attrs = new_state.get("attributes") if isinstance(new_state, dict) else {}
    if not isinstance(attrs, dict):
        attrs = {}
    source = _monitor_event_source(monitor, entity_id)
    matched_source_type = _text(source.get("type"))
    new_value = _text((new_state or {}).get("state")).lower()
    old_value = _text((old_state or {}).get("state")).lower()
    state_on = _text(source.get("state_on")).lower()
    state_off = _text(source.get("state_off")).lower()
    attribute_corpus = " ".join(
        _text(value).lower().replace("-", "_")
        for value in (
            attrs.get("event_type"),
            attrs.get("detection_type"),
            attrs.get("device_class"),
            attrs.get("resource_type"),
        )
        if _text(value)
    )
    corpus = " ".join(
        item
        for item in (
            matched_source_type.lower().replace("-", "_"),
            attribute_corpus,
            _text(entity_id).lower().replace("-", "_") if not matched_source_type else "",
        )
        if item
    )
    if "doorbell" in corpus or "ring" in corpus:
        return "" if new_value in _MONITOR_INACTIVE_STATES else "doorbell"
    if "license_plate" in corpus or "licenseplate" in corpus:
        return "" if new_value in _MONITOR_INACTIVE_STATES else "license_plate"
    for token in ("person", "vehicle", "animal", "package", "face"):
        if token in corpus:
            return "" if new_value in _MONITOR_INACTIVE_STATES else token
    if "motion" in corpus:
        active = new_value == state_on if state_on else new_value in _MONITOR_ACTIVE_STATES
        return "motion" if active else ""
    if any(
        token in corpus
        for token in (
            "contact",
            "entry",
            "door",
            "window",
            "open_close",
            "garage",
            "cover",
        )
    ):
        if (state_on and new_value == state_on) or new_value in {"open", "opened", "on", "no_contact"}:
            return "opens"
        if (state_off and new_value == state_off) or new_value in {"closed", "close", "off", "contact"}:
            return "closes"
        return "changed" if new_value != old_value else ""
    if any(token in corpus for token in ("connectivity", "online", "network")):
        if (state_on and new_value == state_on) or new_value in {"connected", "online", "home", "present"}:
            return "connects"
        if (state_off and new_value == state_off) or new_value in {"disconnected", "offline", "away"}:
            return "disconnects"
    if state_on and new_value == state_on and new_value != old_value:
        return "turns_on"
    if state_off and new_value == state_off and new_value != old_value:
        return "turns_off"
    if new_value in _MONITOR_ACTIVE_STATES and old_value not in _MONITOR_ACTIVE_STATES:
        return "turns_on"
    if new_value in _MONITOR_INACTIVE_STATES and old_value not in _MONITOR_INACTIVE_STATES:
        return "turns_off"
    return "changed" if new_value != old_value or new_state.get("attributes") != old_state.get("attributes") else ""


def _monitor_event_type(monitor: Dict[str, Any], entity_id: Any, new_state: Dict[str, Any], old_state: Dict[str, Any]) -> str:
    trigger = _monitor_event_trigger(monitor, entity_id, new_state, old_state)
    if trigger in {"doorbell", "license_plate", "person", "vehicle", "animal", "package", "face", "motion"}:
        return trigger
    return "activity" if _text(monitor.get("kind")) == "camera" else (trigger or "changed")


def _monitor_camera_target(monitor: Dict[str, Any]) -> str:
    target = _text(monitor.get("device_ref") or monitor.get("device_id"))
    if _normalize_event_provider(monitor.get("provider")) == "unifi_protect" and target.startswith("camera:"):
        return _text(target.split(":", 1)[1])
    return target


def _monitor_linked_camera_target(monitor: Dict[str, Any]) -> str:
    return _monitor_camera_target(
        {
            "provider": monitor.get("linked_camera_provider"),
            "device_ref": monitor.get("linked_camera_device_ref"),
            "device_id": monitor.get("linked_camera_device_id"),
        }
    )


def _monitor_snapshot_fields(event_payload: Dict[str, Any], snapshot_store: Dict[str, Any]) -> None:
    data = event_payload.get("data") if isinstance(event_payload.get("data"), dict) else {}
    if snapshot_store.get("stored"):
        snapshot_id = _text(snapshot_store.get("snapshot_id"))
        event_payload["snapshot_id"] = snapshot_id
        data["snapshot_id"] = snapshot_id
        data["snapshot_content_type"] = _text(snapshot_store.get("content_type") or "image/jpeg")
        data["snapshot_bytes"] = _as_int(snapshot_store.get("bytes"), 0, minimum=0)
    elif snapshot_store.get("reason"):
        data["snapshot_status"] = _text(snapshot_store.get("reason"))
        data["snapshot_bytes"] = _as_int(snapshot_store.get("bytes"), 0, minimum=0)
    event_payload["data"] = data


def _monitor_clip_fields(event_payload: Dict[str, Any], clip_store: Dict[str, Any]) -> None:
    data = event_payload.get("data") if isinstance(event_payload.get("data"), dict) else {}
    if clip_store.get("stored"):
        clip_id = _text(clip_store.get("clip_id"))
        event_payload["clip_id"] = clip_id
        data["clip_id"] = clip_id
        data["clip_content_type"] = _text(clip_store.get("content_type") or "video/mp4")
        data["clip_stored_bytes"] = _as_int(clip_store.get("bytes"), 0, minimum=0)
    elif clip_store.get("reason"):
        data["clip_storage_status"] = _text(clip_store.get("reason"))
        data["clip_stored_bytes"] = 0
    event_payload["data"] = data


def _monitor_camera_clip_payload(event: Dict[str, Any], *, duration_seconds: int) -> Dict[str, Any]:
    new_state = event.get("new_state") if isinstance(event.get("new_state"), dict) else {}
    attrs = new_state.get("attributes") if isinstance(new_state.get("attributes"), dict) else {}
    event_start = (
        attrs.get("event_start")
        or attrs.get("event_ts")
        or new_state.get("last_changed")
        or new_state.get("last_updated")
        or event.get("event_start")
    )
    event_end = attrs.get("event_end") or event.get("event_end")
    event_id = attrs.get("event_id") or event.get("event_id")
    return {
        "duration_seconds": max(1, min(30, int(duration_seconds or 8))),
        "pre_event_seconds": 2,
        "post_event_seconds": 4,
        "event_id": _text(event_id),
        "event_start": event_start,
        "event_end": event_end,
    }


async def _execute_camera_monitor(monitor: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    provider = _normalize_event_provider(monitor.get("provider"))
    camera_target = _monitor_camera_target(monitor)
    area = _text(monitor.get("area") or monitor.get("name") or "camera")
    entity_id = _text(event.get("entity_id"))
    new_state = event.get("new_state") if isinstance(event.get("new_state"), dict) else {}
    old_state = event.get("old_state") if isinstance(event.get("old_state"), dict) else {}
    event_kind = _monitor_event_type(monitor, entity_id, new_state, old_state)
    cooldown_seconds = _setting_int(
        redis_client,
        "camera_monitor_cooldown_seconds",
        30,
        minimum=0,
        maximum=86400,
    )
    cooldown_key = f"awareness:monitor:camera_cooldown:{_text(monitor.get('id'))}"
    if not _acquire_cooldown(cooldown_key, cooldown_seconds):
        return {"ok": True, "summary": "Camera event cooldown active.", "skipped": "cooldown"}

    requested_description_mode = _text(monitor.get("description_mode") or "image").lower()
    if requested_description_mode not in _MONITOR_DESCRIPTION_MODES:
        requested_description_mode = "image"
    actual_description_mode = requested_description_mode
    snapshot_store: Dict[str, Any] = {}
    clip_store: Dict[str, Any] = {}
    jpeg: bytes = b""
    content_type = "image/jpeg"
    clip_bytes: bytes = b""
    clip_content_type = "video/mp4"
    clip_metadata: Dict[str, Any] = {}
    clip_duration_seconds = 0.0
    errors: List[str] = []
    summary = ""
    try:
        jpeg, content_type = await _capture_camera_snapshot(provider, camera_target)
    except Exception as exc:
        errors.append(f"snapshot: {exc}")
        logger.warning("[awareness] monitored camera snapshot failed for %s: %s", camera_target, exc)
        snapshot_store = {"stored": False, "reason": "capture_failed", "bytes": 0}

    if requested_description_mode == "video":
        try:
            clip_seconds = _setting_int(
                redis_client,
                "camera_event_clip_seconds",
                8,
                minimum=1,
                maximum=30,
            )
            clip_payload = _monitor_camera_clip_payload(event, duration_seconds=clip_seconds)
            clip_bytes, clip_content_type, clip_metadata = await _capture_camera_clip(
                provider,
                camera_target,
                clip_payload,
            )
            clip_duration_seconds = _as_float(
                clip_metadata.get("duration_seconds"),
                float(clip_seconds),
            )
            summary = await _video_describe(
                video_bytes=clip_bytes,
                content_type=clip_content_type,
                query="doorbell alert" if event_kind == "doorbell" else "",
                mode="doorbell" if event_kind == "doorbell" else "camera",
            )
        except Exception as exc:
            errors.append(f"video: {exc}")
            logger.warning("[awareness] monitored camera video failed for %s: %s", camera_target, exc)
            actual_description_mode = "image"

    if requested_description_mode == "image" or (actual_description_mode == "image" and not summary):
        try:
            if not jpeg:
                raise RuntimeError("No camera snapshot was available.")
            vision = get_shared_vision_settings(
                default_api_base="http://127.0.0.1:1234",
                default_model="qwen2.5-vl-7b-instruct",
            )
            summary = await _vision_describe(
                image_bytes=jpeg,
                api_base=_text(vision.get("api_base")),
                model=_text(vision.get("model")),
                api_key=_text(vision.get("api_key")),
                query="doorbell alert" if event_kind == "doorbell" else "",
                ignore_vehicles=False,
                mode="doorbell" if event_kind == "doorbell" else "camera",
                vision_mode=_text(vision.get("mode")),
                vision_provider=_text(vision.get("provider")),
            )
        except Exception as exc:
            errors.append(f"image: {exc}")
            logger.warning("[awareness] monitored camera image description failed for %s: %s", camera_target, exc)

    summary = _compact(summary, limit=180) or "Nothing notable."
    error_text = _compact("; ".join(errors), limit=300)

    if _is_nothing_notable_summary(summary) and event_kind in {"activity", "motion"}:
        _clear_cooldown(cooldown_key)
        return {"ok": True, "summary": summary, "skipped": "nothing_notable"}
    if not summary or _is_nothing_notable_summary(summary):
        if event_kind == "doorbell":
            summary = f"The doorbell was pressed at {area}."
        else:
            summary = f"{event_kind.replace('_', ' ').title()} activity was detected at {area}."
    if jpeg:
        snapshot_store = _store_event_snapshot(redis_client, jpeg, content_type=content_type)
    if clip_bytes:
        clip_store = _store_event_clip(
            redis_client,
            clip_bytes,
            content_type=clip_content_type,
        )

    event_id = uuid.uuid4().hex
    event_payload: Dict[str, Any] = {
        "id": event_id,
        "source": _slug(area),
        "title": "Doorbell" if event_kind == "doorbell" else f"{area} Camera",
        "type": "doorbell" if event_kind == "doorbell" else "camera_event",
        "message": summary,
        "entity_id": _text(monitor.get("device_ref") or monitor.get("device_id")),
        "ha_time": _now_iso(),
        "level": "info",
        "data": {
            "area": area,
            "provider": provider,
            "monitor_id": _text(monitor.get("id")),
            "event_type": event_kind,
            "trigger_entity": entity_id,
            "new_state": _text(new_state.get("state")),
            "old_state": _text(old_state.get("state")),
            "description_mode": requested_description_mode,
            "description_media": actual_description_mode,
        },
    }
    if clip_bytes:
        event_payload["data"]["clip_content_type"] = clip_content_type
        event_payload["data"]["clip_bytes"] = len(clip_bytes)
        for key in ("event_id", "start", "end", "duration_seconds"):
            value = clip_metadata.get(key)
            if value not in (None, ""):
                event_payload["data"][f"clip_{key}"] = value
    if error_text:
        event_payload["data"]["capture_error"] = _compact(error_text, limit=180)
    _monitor_snapshot_fields(event_payload, snapshot_store)
    _monitor_clip_fields(event_payload, clip_store)
    face_session_id = ""
    if _bool(monitor.get("face_id_enabled"), True) and _face_burst_should_run(event_kind, summary):
        face_session_id = _schedule_face_burst(
            event_id=event_id,
            provider=provider,
            camera_target=camera_target,
            area=area,
            initial_image=jpeg,
            initial_content_type=content_type,
            video_bytes=clip_bytes if requested_description_mode == "video" else b"",
            video_content_type=clip_content_type,
            video_duration_seconds=clip_duration_seconds,
            monitor_id=_text(monitor.get("id")),
        )
    if face_session_id:
        event_payload["data"]["face_session_id"] = face_session_id
        event_payload["data"]["face_status"] = "pending"
    _append_event(redis_client, source=area, payload=event_payload)
    if not face_session_id:
        await _deliver_awareness_event_notification(monitor, event_payload)
    return {
        "ok": True,
        "summary": summary,
        "event_type": event_kind,
        "warning": error_text,
        "face_session_id": face_session_id,
        "description_mode": actual_description_mode,
        "clip_id": _text(clip_store.get("clip_id")),
    }


def _monitor_sensor_type(monitor: Dict[str, Any]) -> str:
    corpus = " ".join(
        [
            *[_text(item).lower() for item in monitor.get("categories") or []],
            _text(monitor.get("name")).lower(),
            _text(monitor.get("area")).lower(),
        ]
    )
    for token in ("window", "garage", "door", "motion", "presence", "leak", "temperature", "humidity"):
        if token in corpus:
            return token
    return "device"


async def _capture_sensor_linked_camera(
    monitor: Dict[str, Any],
    event: Dict[str, Any],
    *,
    sensor_summary: str,
) -> Dict[str, Any]:
    provider = _normalize_event_provider(monitor.get("linked_camera_provider"))
    camera_target = _monitor_linked_camera_target(monitor)
    if provider == "all" or not camera_target:
        return {"configured": False}

    requested_mode = _text(
        monitor.get("linked_camera_description_mode") or "image"
    ).lower()
    if requested_mode not in _MONITOR_DESCRIPTION_MODES:
        requested_mode = "image"
    actual_mode = requested_mode
    camera_summary = ""
    jpeg: bytes = b""
    image_content_type = "image/jpeg"
    clip_bytes: bytes = b""
    clip_content_type = "video/mp4"
    clip_metadata: Dict[str, Any] = {}
    snapshot_store: Dict[str, Any] = {}
    clip_store: Dict[str, Any] = {}
    errors: List[str] = []
    description_completed = False
    context = f"This camera was captured because this sensor event occurred: {sensor_summary}"

    try:
        jpeg, image_content_type = await _capture_camera_snapshot(provider, camera_target)
    except Exception as exc:
        errors.append(f"snapshot: {exc}")
        logger.warning(
            "[awareness] linked sensor camera snapshot failed for %s: %s",
            camera_target,
            exc,
        )
        snapshot_store = {"stored": False, "reason": "capture_failed", "bytes": 0}

    if requested_mode == "video":
        try:
            clip_seconds = _setting_int(
                redis_client,
                "camera_event_clip_seconds",
                8,
                minimum=1,
                maximum=30,
            )
            clip_payload = _monitor_camera_clip_payload(event, duration_seconds=clip_seconds)
            clip_bytes, clip_content_type, clip_metadata = await _capture_camera_clip(
                provider,
                camera_target,
                clip_payload,
            )
            camera_summary = await _video_describe(
                video_bytes=clip_bytes,
                content_type=clip_content_type,
                query=context,
                mode="camera",
            )
            description_completed = True
        except Exception as exc:
            errors.append(f"video: {exc}")
            logger.warning(
                "[awareness] linked sensor camera video failed for %s: %s",
                camera_target,
                exc,
            )
            actual_mode = "image"

    if requested_mode == "image" or (actual_mode == "image" and not camera_summary):
        try:
            if not jpeg:
                raise RuntimeError("No linked camera snapshot was available.")
            vision = get_shared_vision_settings(
                default_api_base="http://127.0.0.1:1234",
                default_model="qwen2.5-vl-7b-instruct",
            )
            camera_summary = await _vision_describe(
                image_bytes=jpeg,
                api_base=_text(vision.get("api_base")),
                model=_text(vision.get("model")),
                api_key=_text(vision.get("api_key")),
                query=context,
                ignore_vehicles=False,
                mode="camera",
                vision_mode=_text(vision.get("mode")),
                vision_provider=_text(vision.get("provider")),
            )
            description_completed = True
        except Exception as exc:
            errors.append(f"image: {exc}")
            logger.warning(
                "[awareness] linked sensor camera image description failed for %s: %s",
                camera_target,
                exc,
            )

    camera_summary = _compact(camera_summary, limit=180)
    if description_completed and not camera_summary:
        camera_summary = "Nothing notable."
    if jpeg:
        snapshot_store = _store_event_snapshot(
            redis_client,
            jpeg,
            content_type=image_content_type,
        )
    if clip_bytes:
        clip_store = _store_event_clip(
            redis_client,
            clip_bytes,
            content_type=clip_content_type,
        )
    return {
        "configured": True,
        "provider": provider,
        "camera_target": camera_target,
        "camera_name": _text(monitor.get("linked_camera_name")) or camera_target,
        "requested_mode": requested_mode,
        "actual_mode": actual_mode,
        "summary": camera_summary,
        "snapshot_store": snapshot_store,
        "clip_store": clip_store,
        "clip_bytes": len(clip_bytes),
        "clip_content_type": clip_content_type,
        "clip_metadata": clip_metadata,
        "warning": _compact("; ".join(errors), limit=300),
    }


async def _execute_sensor_monitor(monitor: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    entity_id = _text(event.get("entity_id") or monitor.get("device_ref") or monitor.get("device_id"))
    new_state = event.get("new_state") if isinstance(event.get("new_state"), dict) else {}
    old_state = event.get("old_state") if isinstance(event.get("old_state"), dict) else {}
    new_value = _text(new_state.get("state"))
    old_value = _text(old_state.get("state"))
    sensor_type = _monitor_sensor_type(monitor)
    name = _text(monitor.get("name")) or _friendly_entity_name(entity_id, new_state or old_state)
    area = _text(monitor.get("area")) or name
    action_label, action_token = _entry_state_action(new_value)
    if sensor_type in {"door", "window", "garage"}:
        summary = f"{name} {action_label}."
    elif sensor_type in {"motion", "presence", "leak"} and new_value.lower() in _MONITOR_ACTIVE_STATES:
        summary = f"{name} detected {sensor_type}."
        action_token = "detected"
    elif sensor_type in {"motion", "presence", "leak"} and new_value.lower() in _MONITOR_INACTIVE_STATES:
        summary = f"{name} is clear."
        action_token = "clear"
    else:
        summary = f"{name} changed to {new_value or 'unknown'}."
        action_token = "changed"
    sensor_summary = _compact(summary, limit=180)
    camera_media = await _capture_sensor_linked_camera(
        monitor,
        event,
        sensor_summary=sensor_summary,
    )
    camera_summary = _text(camera_media.get("summary"))
    summary = sensor_summary
    if camera_media.get("configured") and camera_summary:
        summary = _compact(f"{sensor_summary} Camera: {camera_summary}", limit=360)
    event_payload: Dict[str, Any] = {
        "id": uuid.uuid4().hex,
        "source": _slug(area),
        "title": name,
        "type": f"{sensor_type}_sensor_{action_token}",
        "message": summary,
        "entity_id": entity_id,
        "ha_time": _now_iso(),
        "level": "info",
        "data": {
            "area": area,
            "provider": _normalize_event_provider(monitor.get("provider")),
            "monitor_id": _text(monitor.get("id")),
            "sensor_type": sensor_type,
            "trigger_entity": entity_id,
            "new_state": new_value,
            "old_state": old_value,
        },
    }
    if camera_media.get("configured"):
        event_payload["data"].update(
            {
                "camera_entity": _text(camera_media.get("camera_target")),
                "camera_provider": _text(camera_media.get("provider")),
                "camera_name": _text(camera_media.get("camera_name")),
                "description_mode": _text(camera_media.get("requested_mode")),
                "description_media": _text(camera_media.get("actual_mode")),
            }
        )
        clip_bytes = _as_int(camera_media.get("clip_bytes"), 0, minimum=0)
        clip_metadata = (
            camera_media.get("clip_metadata")
            if isinstance(camera_media.get("clip_metadata"), dict)
            else {}
        )
        if clip_bytes:
            event_payload["data"]["clip_content_type"] = _text(
                camera_media.get("clip_content_type") or "video/mp4"
            )
            event_payload["data"]["clip_bytes"] = clip_bytes
            for key in ("event_id", "start", "end", "duration_seconds"):
                value = clip_metadata.get(key)
                if value not in (None, ""):
                    event_payload["data"][f"clip_{key}"] = value
        warning = _text(camera_media.get("warning"))
        if warning:
            event_payload["data"]["capture_error"] = warning
        _monitor_snapshot_fields(
            event_payload,
            camera_media.get("snapshot_store")
            if isinstance(camera_media.get("snapshot_store"), dict)
            else {},
        )
        _monitor_clip_fields(
            event_payload,
            camera_media.get("clip_store")
            if isinstance(camera_media.get("clip_store"), dict)
            else {},
        )
    _append_event(redis_client, source=area, payload=event_payload)
    await _deliver_awareness_event_notification(monitor, event_payload)
    return {
        "ok": True,
        "summary": summary,
        "event_type": action_token,
        "warning": _text(camera_media.get("warning")),
        "description_mode": _text(camera_media.get("actual_mode")),
        "clip_id": _text(
            (camera_media.get("clip_store") or {}).get("clip_id")
            if isinstance(camera_media.get("clip_store"), dict)
            else ""
        ),
    }


async def _execute_monitor(monitor: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    if _text(monitor.get("kind")) == "camera":
        return await _execute_camera_monitor(monitor, event)
    return await _execute_sensor_monitor(monitor, event)



def _monitor_form(
    monitor: Dict[str, Any],
    registry: Dict[str, Any],
    client: Any = None,
) -> Dict[str, Any]:
    kind = _text(monitor.get("kind") or "camera")
    selected_device = _provider_ref(monitor.get("provider"), monitor.get("device_id"))
    selected_integration = _monitor_integration_value(kind, monitor.get("provider"))
    integration_options, integration_dependency = _monitor_integration_options(
        registry,
        current_kind=kind,
    )
    device_options, device_dependency = _monitor_device_options(
        registry,
        current_integration=selected_integration,
        current_device=selected_device,
    )
    trigger_options, trigger_dependency = _monitor_trigger_dependency(
        registry,
        current_device=selected_device,
        current_events=monitor.get("trigger_events"),
    )
    description_mode = _text(monitor.get("description_mode") or "image").lower()
    description_options, description_dependency = _monitor_description_mode_dependency(
        registry,
        current_device=selected_device,
        current_mode=description_mode,
    )
    linked_camera_provider = _text(monitor.get("linked_camera_provider"))
    linked_camera_integration = _monitor_integration_value(
        "camera",
        linked_camera_provider,
    )
    linked_camera_device = (
        _provider_ref(linked_camera_provider, monitor.get("linked_camera_device_id"))
        if _text(monitor.get("linked_camera_device_id"))
        else ""
    )
    linked_camera_integration_options = _monitor_linked_camera_integration_options(
        registry,
        current_provider=linked_camera_provider,
    )
    linked_camera_options, linked_camera_dependency = _monitor_linked_camera_device_options(
        registry,
        current_integration=linked_camera_integration,
        current_device=linked_camera_device,
    )
    linked_camera_description_mode = _text(
        monitor.get("linked_camera_description_mode") or "image"
    ).lower()
    linked_description_options, linked_description_dependency = (
        _monitor_description_mode_dependency(
            registry,
            current_device=linked_camera_device,
            current_mode=linked_camera_description_mode,
            source_key="linked_camera",
            include_default_options=False,
        )
    )
    notification_destinations = _normalize_notification_destinations(
        monitor.get("notification_destinations")
    )
    notification_options = _notification_destination_options(
        client or redis_client,
        notification_destinations,
    )
    trigger_labels = [
        _text(_monitor_trigger_option(value).get("label"))
        for value in monitor.get("trigger_events") or []
        if _text(value)
    ]
    enabled_label = "Monitoring" if _bool(monitor.get("enabled"), True) else "Paused"
    face_id_label = "Face ID on" if _bool(monitor.get("face_id_enabled"), True) else "Face ID off"
    notification_label = (
        "Notifications on"
        if _bool(monitor.get("notifications_enabled"), False)
        else "Notifications off"
    )
    description_label = "Video descriptions" if description_mode == "video" else "Image descriptions"
    linked_camera_label = ""
    if linked_camera_device:
        linked_mode_label = (
            "video" if linked_camera_description_mode == "video" else "image"
        )
        linked_camera_label = (
            f"Camera: {_text(monitor.get('linked_camera_name')) or linked_camera_device} "
            f"({linked_mode_label}) • "
        )
    last_event = _fmt_ts(monitor.get("last_event_ts"))
    return {
        "id": monitor["id"],
        "group": "monitors",
        "title": _text(monitor.get("name")) or _text(monitor.get("area")) or "Monitored source",
        "subtitle": (
            f"{enabled_label} • {kind.title()} • {_provider_label(monitor.get('provider'))} • "
            f"{', '.join(trigger_labels) or 'No triggers'} • "
            f"{f'{description_label} • {face_id_label} • ' if kind == 'camera' else linked_camera_label}"
            f"{notification_label} • last event: {last_event}"
        ),
        "save_action": "awareness_save_monitor",
        "remove_action": "awareness_remove_monitor",
        "remove_confirm": "Stop monitoring this source? Stored event history will be kept.",
        "fields": [
            {
                "key": "kind",
                "label": "Source Type",
                "type": "select",
                "presentation": "cards",
                "options": [
                    {
                        "value": "camera",
                        "label": "Camera",
                        "description": "Store notable camera activity with snapshots and vision descriptions.",
                        "icon": "◎",
                    },
                    {
                        "value": "sensor",
                        "label": "Sensor",
                        "description": "Store state changes from doors, motion, presence, climate, and other sensors.",
                        "icon": "◇",
                    },
                ],
                "value": kind,
                "full_width": True,
            },
            {
                "key": "integration",
                "label": "Integration",
                "type": "select",
                "presentation": "cards",
                "options": integration_options,
                "dependent_options": integration_dependency,
                "value": selected_integration,
                "description": "Choose the integration that provides this camera or sensor.",
                "full_width": True,
            },
            {
                "key": "device",
                "label": "Device",
                "type": "select",
                "presentation": "cards",
                "options": device_options,
                "dependent_options": device_dependency,
                "value": selected_device,
                "description": "Only compatible cameras, doorbells, and sensors from enabled integrations are shown.",
                "full_width": True,
            },
            {
                "key": "trigger_events",
                "label": "Capture Events When",
                "type": "multiselect",
                "presentation": "cards",
                "options": trigger_options,
                "dependent_options": trigger_dependency,
                "value": list(monitor.get("trigger_events") or []),
                "description": "Select one or more events reported by this device. Only those events will be stored in Awareness history.",
                "full_width": True,
            },
            {
                "key": "description_mode",
                "label": "Describe Camera Events With",
                "type": "select",
                "presentation": "cards",
                "options": description_options,
                "dependent_options": description_dependency,
                "value": description_mode,
                "description": (
                    "Video is offered only when the selected integration reports that this camera can provide clips."
                ),
                "full_width": True,
                "show_when": {"source_key": "kind", "equals": "camera"},
            },
            {
                "type": "heading",
                "label": "Optional Camera Context",
                "description": (
                    "Pair this sensor with a camera to add a snapshot or clip and a visual description "
                    "to each captured sensor event."
                ),
                "show_when": {"source_key": "kind", "equals": "sensor"},
            },
            {
                "key": "linked_camera_integration",
                "label": "Camera Integration",
                "type": "select",
                "presentation": "cards",
                "options": linked_camera_integration_options,
                "value": linked_camera_integration,
                "description": "Choose No camera to keep sensor events text-only.",
                "full_width": True,
                "show_when": {"source_key": "kind", "equals": "sensor"},
            },
            {
                "key": "linked_camera",
                "label": "Camera",
                "type": "select",
                "presentation": "cards",
                "options": linked_camera_options,
                "dependent_options": linked_camera_dependency,
                "value": linked_camera_device,
                "description": "The selected camera captures when this sensor event fires.",
                "full_width": True,
                "show_when": {"source_key": "kind", "equals": "sensor"},
            },
            {
                "key": "linked_camera_description_mode",
                "label": "Describe Sensor Events With",
                "type": "select",
                "presentation": "cards",
                "options": linked_description_options,
                "dependent_options": linked_description_dependency,
                "value": linked_camera_description_mode if linked_camera_device else "",
                "description": (
                    "Video appears only when the linked camera's integration reports clip support."
                ),
                "full_width": True,
                "show_when": {"source_key": "kind", "equals": "sensor"},
            },
            {
                "key": "area",
                "label": "Area",
                "type": "text",
                "placeholder": "Back Yard, Front Door, Garage…",
                "value": _text(monitor.get("area")),
                "description": "Events are grouped under this area in Awareness history.",
            },
            {
                "key": "name",
                "label": "Display Name",
                "type": "text",
                "value": _text(monitor.get("name")),
            },
            {
                "key": "enabled",
                "label": "Monitor This Source",
                "type": "checkbox",
                "value": _bool(monitor.get("enabled"), True),
            },
            {
                "key": "face_id_enabled",
                "label": "Use Face ID On This Camera",
                "type": "checkbox",
                "value": _bool(monitor.get("face_id_enabled"), True),
                "description": (
                    "Run face burst snapshots and recognition for this camera. "
                    "The Face ID model must also be enabled in Settings › Models."
                ),
                "show_when": {"source_key": "kind", "equals": "camera"},
            },
            {
                "type": "heading",
                "label": "Optional Notifications",
                "description": (
                    "Send the completed Awareness event using its existing description and media. "
                    "Face ID results are included when Face ID runs for the event."
                ),
            },
            {
                "key": "notifications_enabled",
                "label": "Notify For This Source",
                "type": "checkbox",
                "value": _bool(monitor.get("notifications_enabled"), False),
            },
            {
                "key": "notification_destinations",
                "label": "Send Notifications To",
                "type": "multiselect",
                "presentation": "cards",
                "options": notification_options,
                "value": notification_destinations,
                "description": "Choose one or more notification destinations already connected to Tater.",
                "full_width": True,
                "show_when": {"source_key": "notifications_enabled", "equals": True},
            },
        ],
    }


def _rewrite_face_session_identity(client: Any, source_id: str, target_id: str = "") -> None:
    redis_obj = client or redis_client
    if redis_obj is None or not source_id:
        return
    try:
        keys = list(redis_obj.scan_iter(match=f"{_FACE_SESSION_PREFIX}*"))
    except Exception:
        return
    for raw_key in keys:
        key = _text(raw_key)
        try:
            raw = redis_obj.get(key)
            session = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        except Exception:
            continue
        if not isinstance(session, dict):
            continue
        current = [_text(value) for value in session.get("identity_ids") or []]
        if source_id not in current:
            continue
        updated: List[str] = []
        for identity_id in current:
            next_id = target_id if identity_id == source_id else identity_id
            if next_id and next_id not in updated:
                updated.append(next_id)
        session["identity_ids"] = updated
        _save_face_session(redis_obj, session)


def _merge_face_identities(client: Any, source_id: str, target_id: str) -> Dict[str, Any]:
    if not source_id or not target_id or source_id == target_id:
        rows = _face_identity_rows(client)
        identity = rows.get(source_id)
        if identity is None:
            raise KeyError("Face identity not found.")
        return identity
    with _FACE_IDENTITY_LOCK:
        rows = _face_identity_rows(client)
        source = dict(rows.get(source_id) or {})
        target = dict(rows.get(target_id) or {})
        if not source or not target:
            raise KeyError("Face identity not found.")
        source_person_id = _text(source.get("person_id"))
        target_person_id = _text(target.get("person_id"))
        if not target_person_id and source_person_id:
            target["person_id"] = source_person_id
            target["person_name"] = _text(source.get("person_name") or source.get("name"))
            target["name"] = _text(source.get("person_name") or source.get("name"))
            target_person_id = source_person_id
        source_centroid = source.get("centroid") if isinstance(source.get("centroid"), list) else []
        target_centroid = target.get("centroid") if isinstance(target.get("centroid"), list) else []
        target_references = _face_reference_embeddings(target)
        source_references = _face_reference_embeddings(source)
        source_count = _as_int(source.get("centroid_count"), 0, minimum=0)
        target_count = _as_int(target.get("centroid_count"), 0, minimum=0)
        if source_centroid and len(source_centroid) == len(target_centroid) and source_count > 0 and target_count > 0:
            total = source_count + target_count
            target["centroid"] = [
                ((float(left) * target_count) + (float(right) * source_count)) / total
                for left, right in zip(target_centroid, source_centroid)
            ]
            target["centroid_count"] = total
        elif source_centroid and not target_centroid:
            target["centroid"] = source_centroid
            target["centroid_count"] = max(1, source_count)
        target["observation_count"] = _as_int(target.get("observation_count"), 0, minimum=0) + _as_int(
            source.get("observation_count"), 0, minimum=0
        )
        target["event_count"] = _as_int(target.get("event_count"), 0, minimum=0) + _as_int(
            source.get("event_count"), 0, minimum=0
        )
        combined_observations = _face_observations(
            {
                "observations": [
                    *_face_observations(target),
                    *_face_observations(source),
                ]
            }
        )
        if combined_observations:
            target["observations"] = combined_observations
        preserved_anchors = list(target_references)
        if not _face_observations(source):
            # A legacy identity may have no retained crops, so its existing match
            # vectors are the only way to preserve that confirmed appearance.
            preserved_anchors.extend(source_references)
        if preserved_anchors:
            target["anchor_references"] = _curate_face_reference_embeddings(
                {
                    "anchor_references": preserved_anchors,
                    "best_quality": target.get("best_quality"),
                }
            )
        merged_from = [
            *[_text(value) for value in target.get("merged_identity_ids") or []],
            _text(source.get("id")),
            *[_text(value) for value in source.get("merged_identity_ids") or []],
        ]
        target["merged_identity_ids"] = list(dict.fromkeys(value for value in merged_from if value))
        if not _text(target.get("name")) and _text(source.get("name")):
            target["name"] = _text(source.get("name"))
        if _text(source.get("first_seen")) and (
            not _text(target.get("first_seen")) or _text(source.get("first_seen")) < _text(target.get("first_seen"))
        ):
            target["first_seen"] = _text(source.get("first_seen"))
        if _text(source.get("last_seen")) > _text(target.get("last_seen")):
            target["last_seen"] = _text(source.get("last_seen"))
            target["last_event_id"] = _text(source.get("last_event_id"))
        if _as_float(source.get("best_quality"), 0.0) > _as_float(target.get("best_quality"), 0.0):
            target["best_quality"] = _as_float(source.get("best_quality"), 0.0)
            target["face_b64"] = _text(source.get("face_b64"))
            target["face_content_type"] = _text(source.get("face_content_type") or "image/jpeg")
        target["reference_centroids"] = _curate_face_reference_embeddings(
            target,
            extra_references=source_references,
        )
        saved = _save_face_identity(client, target)
        (client or redis_client).hdel(_FACE_IDENTITIES_KEY, source_id)
    _rewrite_face_session_identity(client, source_id, target_id)
    if target_person_id:
        linked_name = _people_person_name(client, target_person_id) or _text(saved.get("name"))
        _people_attach_face_identity(
            client,
            person_id=target_person_id,
            identity_id=target_id,
            label=linked_name,
        )
    if source_person_id:
        _people_detach_face_identity(client, person_id=source_person_id, identity_id=source_id)
    return saved


def _rewrite_face_sessions_for_split(
    client: Any,
    *,
    source_id: str,
    target_id: str,
    selected_event_ids: set[str],
    remaining_event_ids: set[str],
) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    try:
        keys = list(redis_obj.scan_iter(match=f"{_FACE_SESSION_PREFIX}*"))
    except Exception:
        return
    for raw_key in keys:
        key = _text(raw_key)
        try:
            raw = redis_obj.get(key)
            session = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        except Exception:
            continue
        if not isinstance(session, dict):
            continue
        event_id = _text(session.get("event_id"))
        current = list(dict.fromkeys(_text(value) for value in session.get("identity_ids") or [] if _text(value)))
        if source_id not in current or event_id not in selected_event_ids:
            continue
        updated = list(current)
        if event_id not in remaining_event_ids:
            updated = [value for value in updated if value != source_id]
        if target_id and target_id not in updated:
            updated.append(target_id)
        session["identity_ids"] = updated
        _save_face_session(redis_obj, session)


def _unmerge_face_observations(
    client: Any,
    identity_id: str,
    observation_ids: List[str],
) -> Dict[str, Any]:
    selected_ids = set(_text(value) for value in observation_ids if _text(value))
    if not selected_ids:
        raise ValueError("Select at least one face image to unmerge.")
    with _FACE_IDENTITY_LOCK:
        identities = _face_identity_rows(client)
        source = dict(identities.get(identity_id) or {})
        if not source:
            raise KeyError("Face identity not found.")
        observations = _face_observations(source)
        selected = [row for row in observations if _text(row.get("id")) in selected_ids]
        remaining = [row for row in observations if _text(row.get("id")) not in selected_ids]
        if len(selected) != len(selected_ids):
            raise ValueError("One or more selected face images are no longer available.")
        if not remaining:
            raise ValueError("Keep at least one image with this person before unmerging.")
        if not all(isinstance(row.get("embedding"), list) and row.get("embedding") for row in selected):
            raise ValueError("These older images cannot be unmerged because their face vectors were not retained.")

        source = _rebuild_face_identity_from_observations(source, remaining, keep_name=True)
        new_id = f"face_{uuid.uuid4().hex[:16]}"
        split = _rebuild_face_identity_from_observations(
            {
                "id": new_id,
                "name": "",
                "created_at": _now_iso(),
                "best_quality": 0.0,
            },
            selected,
            keep_name=False,
        )
        source = _save_face_identity(client, source)
        split = _save_face_identity(client, split)

    selected_event_ids = {_text(row.get("event_id")) for row in selected if _text(row.get("event_id"))}
    remaining_event_ids = {_text(row.get("event_id")) for row in remaining if _text(row.get("event_id"))}
    _rewrite_face_sessions_for_split(
        client,
        source_id=identity_id,
        target_id=new_id,
        selected_event_ids=selected_event_ids,
        remaining_event_ids=remaining_event_ids,
    )
    return {"source": source, "split": split, "moved": len(selected)}


def _face_identity_linked_events(client: Any, identity_id: str) -> Dict[str, Dict[str, Any]]:
    redis_obj = client or redis_client
    token = _text(identity_id)
    if redis_obj is None or not token:
        return {}
    linked: Dict[str, Dict[str, Any]] = {}
    for source in _discover_event_sources(redis_obj):
        try:
            raw_events = redis_obj.lrange(_event_key(source), 0, -1) or []
        except Exception:
            continue
        for raw_event in raw_events:
            try:
                event = json.loads(raw_event) if isinstance(raw_event, (str, bytes, bytearray)) else raw_event
            except Exception:
                continue
            if not isinstance(event, dict):
                continue
            event_id = _text(event.get("id"))
            if not event_id or event_id in linked:
                continue
            data = event.get("data") if isinstance(event.get("data"), dict) else {}
            session = _load_face_session(redis_obj, data.get("face_session_id"))
            session_ids = {_text(value) for value in session.get("identity_ids") or [] if _text(value)}
            if token in session_ids:
                linked[event_id] = event
    return linked


def _move_face_images(
    client: Any,
    source_id: str,
    target_id: str,
    selection_ids: List[str],
) -> Dict[str, Any]:
    redis_obj = client or redis_client
    requested = set(_text(value) for value in selection_ids if _text(value))
    if not requested:
        raise ValueError("Select at least one image to move.")

    create_unknown = target_id == _FACE_NEW_UNKNOWN_TARGET
    if not target_id:
        raise ValueError("Choose the person these images belong to.")
    if not create_unknown and source_id == target_id:
        raise ValueError("Choose a different person for the selected images.")

    source_linked_events = _face_identity_linked_events(redis_obj, source_id)
    target_linked_events: Dict[str, Dict[str, Any]] = {}
    with _FACE_IDENTITY_LOCK:
        identities = _face_identity_rows(redis_obj)
        source = dict(identities.get(source_id) or {})
        if not source:
            raise KeyError("Face identity not found.")
        source_original_event_count = _as_int(source.get("event_count"), 0, minimum=0)

        if create_unknown:
            target_id = f"face_{uuid.uuid4().hex[:16]}"
            target = {
                "id": target_id,
                "name": "",
                "created_at": _now_iso(),
                "observation_count": 0,
                "event_count": 0,
                "best_quality": 0.0,
            }
        else:
            target = dict(identities.get(target_id) or {})
            if not target:
                raise KeyError("Destination person not found.")
            target_linked_events = _face_identity_linked_events(redis_obj, target_id)
        target_original_event_count = _as_int(target.get("event_count"), 0, minimum=0)

        source_observations = _face_observations(source)
        observations_by_id = {_text(row.get("id")): row for row in source_observations}
        requested_observation_ids = set(requested)
        missing_observations = requested_observation_ids - set(observations_by_id)
        if missing_observations:
            raise ValueError("One or more selected images are no longer available.")

        selected = [row for row in source_observations if _text(row.get("id")) in requested_observation_ids]
        remaining = [row for row in source_observations if _text(row.get("id")) not in requested_observation_ids]
        if selected and not all(isinstance(row.get("embedding"), list) and row.get("embedding") for row in selected):
            raise ValueError("One or more selected face captures no longer has a saved face vector.")

        target_observations = _face_observations(target)
        if selected:
            if not target_observations and not target.get("anchor_references"):
                existing_target_references = _face_reference_embeddings(target)
                if existing_target_references:
                    target["anchor_references"] = existing_target_references
            target_original_count = _as_int(target.get("observation_count"), len(target_observations), minimum=0)
            target = _rebuild_face_identity_from_observations(
                target,
                [*target_observations, *selected],
                keep_name=True,
            )
            target["observation_count"] = max(target_original_count, len(target_observations)) + len(selected)

            source_original_count = _as_int(source.get("observation_count"), len(source_observations), minimum=0)
            source = _rebuild_face_identity_from_observations(source, remaining, keep_name=True)
            source["observation_count"] = max(0, source_original_count - len(selected))
            if not remaining:
                source["observations"] = []
                for key in ("centroid", "centroid_count", "reference_centroids", "face_b64", "face_content_type", "best_quality"):
                    source.pop(key, None)
                if source.get("anchor_references"):
                    source["reference_centroids"] = _curate_face_reference_embeddings(source)

        selected_observation_event_ids = {
            _text(row.get("event_id")) for row in selected if _text(row.get("event_id"))
        }
        selected_event_ids = selected_observation_event_ids
        remaining_observation_event_ids = {
            _text(row.get("event_id")) for row in remaining if _text(row.get("event_id"))
        }
        moved_away_event_ids = selected_event_ids - remaining_observation_event_ids

        target_existing_event_ids = set(target_linked_events) | {
            _text(row.get("event_id")) for row in target_observations if _text(row.get("event_id"))
        }
        source["event_count"] = max(0, source_original_event_count - len(moved_away_event_ids))
        target["event_count"] = max(target_original_event_count, len(target_existing_event_ids)) + len(
            selected_event_ids - target_existing_event_ids
        )

        selected_times = [_text(row.get("seen_at")) for row in selected if _text(row.get("seen_at"))]
        if selected_times:
            first_selected = min(selected_times)
            last_selected = max(selected_times)
            if not _text(target.get("first_seen")) or first_selected < _text(target.get("first_seen")):
                target["first_seen"] = first_selected
            if last_selected > _text(target.get("last_seen")):
                target["last_seen"] = last_selected

        source_remaining_linked = set(source_linked_events) - moved_away_event_ids
        delete_source = not remaining and not source_remaining_linked
        target["updated_at"] = _now_iso()
        target = _save_face_identity(redis_obj, target)
        if delete_source:
            redis_obj.hdel(_FACE_IDENTITIES_KEY, source_id)
        else:
            source["updated_at"] = _now_iso()
            source = _save_face_identity(redis_obj, source)

    _rewrite_face_sessions_for_split(
        redis_obj,
        source_id=source_id,
        target_id=target_id,
        selected_event_ids=selected_event_ids,
        remaining_event_ids=remaining_observation_event_ids,
    )
    if delete_source:
        _people_detach_face_identity(
            redis_obj,
            person_id=_text(source.get("person_id")),
            identity_id=source_id,
        )
    return {
        "source": {} if delete_source else source,
        "target": target,
        "source_removed": delete_source,
        "moved": len(selected),
        "face_captures": len(selected),
    }


def _remove_face_images(
    client: Any,
    identity_id: str,
    observation_ids: List[str],
) -> Dict[str, Any]:
    redis_obj = client or redis_client
    requested = set(_text(value) for value in observation_ids if _text(value))
    if not requested:
        raise ValueError("Select at least one face image to remove.")

    linked_events = _face_identity_linked_events(redis_obj, identity_id)
    with _FACE_IDENTITY_LOCK:
        identities = _face_identity_rows(redis_obj)
        identity = dict(identities.get(identity_id) or {})
        if not identity:
            raise KeyError("Face identity not found.")

        observations = _face_observations(identity)
        observations_by_id = {_text(row.get("id")): row for row in observations}
        missing = requested - set(observations_by_id)
        if missing:
            raise ValueError("One or more selected face images are no longer available.")
        selected = [row for row in observations if _text(row.get("id")) in requested]
        remaining = [row for row in observations if _text(row.get("id")) not in requested]

        # A manually merged identity can retain confirmed anchor vectors. Drop
        # anchors that came from a removed capture so its embedding no longer
        # influences future matching.
        selected_embeddings = [
            embedding
            for embedding in (row.get("embedding") for row in selected)
            if isinstance(embedding, list) and embedding
        ]
        anchors = identity.get("anchor_references")
        if isinstance(anchors, list) and selected_embeddings:
            identity["anchor_references"] = [
                anchor
                for anchor in anchors
                if not isinstance(anchor, list)
                or not anchor
                or all(_face_cosine_distance(anchor, selected_embedding) >= 0.005 for selected_embedding in selected_embeddings)
            ]

        original_observation_count = _as_int(identity.get("observation_count"), len(observations), minimum=0)
        for key in (
            "centroid",
            "centroid_count",
            "reference_centroids",
            "face_b64",
            "face_content_type",
            "best_quality",
        ):
            identity.pop(key, None)
        identity = _rebuild_face_identity_from_observations(identity, remaining, keep_name=True)
        identity["observation_count"] = max(0, original_observation_count - len(selected))

        selected_event_ids = {_text(row.get("event_id")) for row in selected if _text(row.get("event_id"))}
        remaining_event_ids = {_text(row.get("event_id")) for row in remaining if _text(row.get("event_id"))}
        removed_event_ids = selected_event_ids - remaining_event_ids
        identity["event_count"] = len((set(linked_events) - removed_event_ids) | remaining_event_ids)
        if not remaining and not identity["event_count"]:
            for key in ("first_seen", "last_seen", "last_event_id", "last_distance"):
                identity.pop(key, None)
        identity = _save_face_identity(redis_obj, identity)

    _rewrite_face_sessions_for_split(
        redis_obj,
        source_id=identity_id,
        target_id="",
        selected_event_ids=selected_event_ids,
        remaining_event_ids=remaining_event_ids,
    )
    return {
        "identity": identity,
        "removed": len(selected),
        "face_captures": len(selected),
    }


def _remove_face_identity(client: Any, identity_id: str) -> bool:
    redis_obj = client or redis_client
    if redis_obj is None or not identity_id:
        return False
    identity = _face_identity_rows(redis_obj).get(identity_id) or {}
    try:
        removed = bool(redis_obj.hdel(_FACE_IDENTITIES_KEY, identity_id))
    except Exception:
        return False
    if removed:
        _people_detach_face_identity(
            redis_obj,
            person_id=_text(identity.get("person_id")),
            identity_id=identity_id,
        )
        _rewrite_face_session_identity(redis_obj, identity_id, "")
    return removed


def _face_identity_gallery(client: Any, identity: Dict[str, Any]) -> List[Dict[str, Any]]:
    name = _face_identity_display_name(client, identity) or "Unknown person"
    gallery: List[Dict[str, Any]] = []
    for row in _face_observations(identity):
        face_b64 = _text(row.get("face_b64"))
        observation_id = _text(row.get("id"))
        has_embedding = isinstance(row.get("embedding"), list) and bool(row.get("embedding"))
        if not face_b64 or not observation_id or not has_embedding:
            continue
        gallery.append(
            {
                "value": observation_id,
                "src": f"data:{_text(row.get('face_content_type') or 'image/jpeg')};base64,{face_b64}",
                "alt": f"{name} face capture",
                "caption": _event_time_display(row.get("seen_at")),
                "meta": "Face capture",
                "selectable": True,
                "seen_at": _text(row.get("seen_at")),
            }
        )
    gallery.sort(key=lambda row: (_text(row.get("seen_at")), _text(row.get("value"))), reverse=True)
    return gallery


def _face_identity_forms(client: Any) -> List[Dict[str, Any]]:
    runtime = _face_runtime_status(client)
    enabled = bool(runtime.get("enabled"))
    state = _text(runtime.get("state")).lower()
    if not enabled:
        return [
            {
                "id": "face_id_disabled",
                "group": "face_person",
                "title": "Face ID needs to be enabled",
                "subtitle": "Settings › Models › Face ID",
                "detail": "Enable and load Face ID before Awareness will take burst snapshots or recognize people.",
                "fields_popup": False,
                "fields_dropdown": False,
                "sections_in_dropdown": False,
                "fields": [],
            }
        ]
    if state in {"installing", "loading", "idle", "error", "unavailable"}:
        error = _text(runtime.get("error"))
        return [
            {
                "id": "face_id_runtime_status",
                "group": "face_person",
                "title": "Face ID is loading" if state in {"installing", "loading", "idle"} else "Face ID is unavailable",
                "subtitle": _text(runtime.get("model") or "Facenet512"),
                "detail": error or _text(runtime.get("message")) or "The model is loading locally. Face review will appear when it is ready.",
                "fields_popup": False,
                "fields_dropdown": False,
                "sections_in_dropdown": False,
                "fields": [],
            }
        ]

    identities = _face_identity_rows(client, cleanup=True)
    if not identities:
        return [
            {
                "id": "face_id_empty",
                "group": "face_person",
                "title": "No faces captured yet",
                "subtitle": "Face ID is ready",
                "detail": "Awareness will add faces here after the next monitored camera event.",
                "fields_popup": False,
                "fields_dropdown": False,
                "sections_in_dropdown": False,
                "fields": [],
            }
        ]

    sorted_rows = sorted(
        identities.values(),
        key=lambda row: (
            not bool(_face_identity_display_name(client, row)),
            _face_identity_display_name(client, row).casefold(),
            _text(row.get("last_seen")),
        ),
    )
    people_options = _people_person_options(client)
    available_person_ids = {_text(row.get("value")) for row in people_options if _text(row.get("value"))}
    forms: List[Dict[str, Any]] = []
    for identity in sorted_rows:
        identity_id = _text(identity.get("id"))
        person_id = _text(identity.get("person_id"))
        name = _face_identity_display_name(client, identity)
        identity_people_options = [dict(row) for row in people_options]
        if person_id and person_id not in available_person_ids:
            identity_people_options.append(
                {
                    "value": person_id,
                    "label": f"Missing People record · {person_id}",
                }
            )
        event_count = _as_int(identity.get("event_count"), 0, minimum=0)
        observation_count = _as_int(identity.get("observation_count"), 0, minimum=0)
        gallery = _face_identity_gallery(client, identity)
        selectable_count = sum(1 for row in gallery if row.get("selectable"))
        missing_capture_count = max(0, observation_count - len(gallery))
        detail_parts = [f"{len(gallery)} sortable image{'s' if len(gallery) != 1 else ''}"]
        if missing_capture_count:
            detail_parts.append(
                f"{missing_capture_count} earlier detection{'s' if missing_capture_count != 1 else ''} without saved face crops"
            )
        if person_id and person_id in available_person_ids:
            detail_parts.append("Linked to Tater People")
        destination_options = [{"value": "", "label": "Choose a person…"}]
        for candidate in sorted_rows:
            candidate_id = _text(candidate.get("id"))
            if not candidate_id or candidate_id == identity_id:
                continue
            candidate_name = _face_identity_display_name(client, candidate)
            destination_options.append(
                {
                    "value": candidate_id,
                    "label": candidate_name or f"Unknown face · {candidate_id[-6:]}",
                    "description": f"Last seen {_event_time_display(candidate.get('last_seen'))}",
                }
            )
        destination_options.append(
            {
                "value": _FACE_NEW_UNKNOWN_TARGET,
                "label": "New unknown person",
                "description": "Create a separate person from the selected face captures.",
            }
        )
        forms.append(
            {
                "id": identity_id,
                "group": "face_person",
                "card_variant": "face_person",
                "title": name or f"Unknown face · {identity_id[-6:]}",
                "subtitle": f"Seen in {event_count} event{'s' if event_count != 1 else ''} • {_event_time_display(identity.get('last_seen'))}",
                "detail": " • ".join(detail_parts),
                "hero_image_src": _text(gallery[0].get("src")) if gallery else "",
                "hero_image_alt": f"{name or 'Unknown person'} face",
                "selectable": False,
                "click_opens_fields": True,
                "fields_popup": False,
                "fields_dropdown": True,
                "fields_dropdown_label": "View person and images",
                "sections_in_dropdown": False,
                "save_action": "awareness_save_face_identity",
                "save_label": "Save Person",
                "run_action": "awareness_remove_face_identity",
                "run_label": "Remove Person",
                "run_confirm": (
                    "Remove this entire person and all of their saved face images? "
                    "They will no longer be attached to historical events."
                ),
                "actions": [
                    {
                        "action": "awareness_move_face_images",
                        "label": "Move Selected Images",
                        "tooltip": "Move the checked face captures to the person selected below.",
                        "working_text": "Moving selected face images...",
                        "success_text": "Selected face images moved.",
                    },
                    {
                        "action": "awareness_remove_face_images",
                        "label": "Remove Selected Images",
                        "tooltip": "Remove only the checked face captures while keeping this person.",
                        "confirm": "Remove only the selected face images? This person will be kept.",
                        "tone": "danger",
                        "working_text": "Removing selected face images...",
                        "success_text": "Selected face images removed.",
                    },
                ] if selectable_count else [],
                "fields": [
                    {
                        "key": "name",
                        "label": "Person Name",
                        "type": "text",
                        "value": name,
                        "placeholder": "Fred",
                        "description": (
                            "Used for people who are not linked below. A linked Tater Person supplies the canonical name."
                        ),
                    },
                    {
                        "key": "person_id",
                        "label": "Tater Person",
                        "type": "select",
                        "value": person_id,
                        "options": identity_people_options,
                        "description": (
                            "Link this face to a master Person from Settings › People. The stable Person ID is added "
                            "to Awareness events for search and future automations."
                        ),
                        "full_width": True,
                    },
                    {
                        "key": "observation_ids",
                        "label": "All Images",
                        "type": "image_checklist",
                        "value": [],
                        "options": gallery,
                        "description": (
                            "Only saved face crops with recognition vectors are shown. Check images to move them "
                            "to another person or remove blurry and incorrect captures."
                        ),
                        "full_width": True,
                    },
                    *(
                        [
                            {
                                "key": "target_identity_id",
                                "label": "Move Selected Images To",
                                "type": "select",
                                "value": "",
                                "options": destination_options,
                                "description": "Choose an existing person or create a separate unknown person.",
                                "full_width": True,
                            }
                        ]
                        if selectable_count
                        else []
                    ),
                ],
            }
        )
    return forms


def _awareness_manager_ui(client: Any) -> Dict[str, Any]:
    monitors = _load_monitors(client)
    registry = _monitor_registry(client)
    event_page = _event_page_for_ui(client)
    event_forms = list(event_page.get("items") or [])
    face_forms = _face_identity_forms(client)
    monitor_forms = [
        _monitor_form(monitor, registry, client)
        for monitor in sorted(
            monitors.values(),
            key=lambda row: (_text(row.get("kind")), _text(row.get("name")).casefold(), _text(row.get("id"))),
        )
    ]
    default_kind = "camera"
    integration_options, integration_dependency = _monitor_integration_options(
        registry,
        current_kind=default_kind,
    )
    if not integration_options:
        default_kind = "sensor"
        integration_options, integration_dependency = _monitor_integration_options(
            registry,
            current_kind=default_kind,
        )
    default_integration = _text(integration_options[0].get("value")) if integration_options else ""
    device_options, device_dependency = _monitor_device_options(
        registry,
        current_integration=default_integration,
    )
    default_device = _text(device_options[0].get("value")) if device_options else ""
    default_trigger_options, trigger_dependency = _monitor_trigger_dependency(
        registry,
        current_device=default_device,
    )
    default_trigger_events = [_text(row.get("value")) for row in default_trigger_options if _text(row.get("value"))]
    default_description_options, description_dependency = _monitor_description_mode_dependency(
        registry,
        current_device=default_device,
        current_mode="image",
    )
    default_description_mode = _text(
        (default_description_options[0] if default_description_options else {}).get("value")
    ) or "image"
    linked_camera_integration_options = _monitor_linked_camera_integration_options(registry)
    linked_camera_options, linked_camera_dependency = _monitor_linked_camera_device_options(
        registry,
    )
    linked_description_options, linked_description_dependency = _monitor_description_mode_dependency(
        registry,
        current_device="",
        current_mode="image",
        source_key="linked_camera",
        include_default_options=False,
    )
    notification_options = _notification_destination_options(client)
    event_filters = _event_type_filters(client)
    event_list_view = _event_list_view_enabled(client)
    return {
        "kind": "settings_manager",
        "appearance": "awareness",
        "title": "Awareness Monitoring",
        "empty_message": "No cameras or sensors are being monitored yet.",
        "stats_refresh_button": True,
        "stats_refresh_label": "Refresh devices",
        "stats_refresh_action": "awareness_refresh_devices",
        "stats_controls_action": "awareness_save_event_filters",
        "stats_controls_auto_save": True,
        "stats_controls": [
            {
                "key": "show_camera_events",
                "label": "Cameras",
                "type": "checkbox",
                "value": bool(event_filters.get("camera", True)),
            },
            {
                "key": "show_doorbell_events",
                "label": "Doorbells",
                "type": "checkbox",
                "value": bool(event_filters.get("doorbell", True)),
            },
            {
                "key": "show_sensor_events",
                "label": "Sensors",
                "type": "checkbox",
                "value": bool(event_filters.get("sensor", True)),
            },
            {
                "key": "show_event_list_view",
                "label": "List View",
                "type": "checkbox",
                "value": bool(event_list_view),
            },
        ],
        "item_fields_dropdown": True,
        "item_fields_dropdown_label": "Monitor Settings",
        "item_fields_popup": True,
        "item_fields_popup_label": "Edit Monitored Source",
        "item_sections_in_dropdown": True,
        "default_tab": "events",
        "manager_tabs": [
            {
                "key": "events",
                "label": "Event History",
                "source": "items",
                "item_group": "event_list" if event_list_view else "event",
                "selector": False,
                "page_size": 24,
                "server_pagination": {
                    "enabled": True,
                    "action": "awareness_set_event_page",
                    "page": _as_int(event_page.get("page"), 1, minimum=1),
                    "page_size": _as_int(event_page.get("page_size"), _EVENT_PAGE_SIZE_DEFAULT, minimum=1),
                    "page_count": _as_int(event_page.get("page_count"), 1, minimum=1),
                    "total": _as_int(event_page.get("total"), 0, minimum=0),
                },
                "empty_message": "No stored awareness events found.",
            },
            {
                "key": "faces",
                "label": "Face ID",
                "source": "items",
                "item_group": "face_person",
                "selector": False,
                "empty_message": "No faces captured yet.",
            },
            {
                "key": "monitors",
                "label": "Monitored Sources",
                "source": "items",
                "item_group": "monitors",
                "selector": False,
                "empty_message": "No cameras or sensors are being monitored.",
            },
            {"key": "add", "label": "Add Source", "source": "add_form"},
        ],
        "add_form": {
            "action": "awareness_add_monitor",
            "submit_label": "Start Monitoring",
            "fields": [
                {
                    "type": "heading",
                    "label": "1. Choose What Awareness Should Watch",
                    "description": (
                        "Pick one camera or sensor. Awareness can optionally send its completed events; "
                        "use Automation Core for custom workflows."
                    ),
                },
                {
                    "key": "kind",
                    "label": "Source Type",
                    "type": "select",
                    "presentation": "cards",
                    "options": [
                        {
                            "value": "camera",
                            "label": "Camera",
                            "description": "Capture notable activity, snapshots, and vision descriptions.",
                            "icon": "◎",
                        },
                        {
                            "value": "sensor",
                            "label": "Sensor",
                            "description": "Record door, motion, presence, climate, and other sensor changes.",
                            "icon": "◇",
                        },
                    ],
                    "value": default_kind,
                    "full_width": True,
                },
                {
                    "key": "integration",
                    "label": "Integration",
                    "type": "select",
                    "presentation": "cards",
                    "options": integration_options,
                    "dependent_options": integration_dependency,
                    "value": default_integration,
                    "description": "Choose where Awareness should look for cameras or sensors.",
                    "full_width": True,
                },
                {
                    "key": "device",
                    "label": "Which Device?",
                    "type": "select",
                    "presentation": "cards",
                    "options": device_options,
                    "dependent_options": device_dependency,
                    "value": default_device,
                    "description": "Only compatible devices from enabled integrations are shown.",
                    "full_width": True,
                },
                {
                    "key": "trigger_events",
                    "label": "Capture Events When",
                    "type": "multiselect",
                    "presentation": "cards",
                    "options": default_trigger_options,
                    "dependent_options": trigger_dependency,
                    "value": default_trigger_events,
                    "description": "Only events explicitly reported by the selected device's integration are shown.",
                    "full_width": True,
                },
                {
                    "key": "description_mode",
                    "label": "Describe Camera Events With",
                    "type": "select",
                    "presentation": "cards",
                    "options": default_description_options,
                    "dependent_options": description_dependency,
                    "value": default_description_mode,
                    "description": (
                        "Use one snapshot or a short clip. Video is shown only for cameras whose integration advertises clip support."
                    ),
                    "full_width": True,
                    "show_when": {"source_key": "kind", "equals": "camera"},
                },
                {
                    "type": "heading",
                    "label": "2. Optional Camera Context",
                    "description": (
                        "When a sensor fires, Awareness can capture a camera and add its media and "
                        "description to the same sensor event."
                    ),
                    "show_when": {"source_key": "kind", "equals": "sensor"},
                },
                {
                    "key": "linked_camera_integration",
                    "label": "Camera Integration",
                    "type": "select",
                    "presentation": "cards",
                    "options": linked_camera_integration_options,
                    "value": "",
                    "description": "This is optional. Choose No camera for a text-only sensor event.",
                    "full_width": True,
                    "show_when": {"source_key": "kind", "equals": "sensor"},
                },
                {
                    "key": "linked_camera",
                    "label": "Which Camera?",
                    "type": "select",
                    "presentation": "cards",
                    "options": linked_camera_options,
                    "dependent_options": linked_camera_dependency,
                    "value": "",
                    "description": "This camera captures whenever the selected sensor event fires.",
                    "full_width": True,
                    "show_when": {"source_key": "kind", "equals": "sensor"},
                },
                {
                    "key": "linked_camera_description_mode",
                    "label": "Describe Sensor Events With",
                    "type": "select",
                    "presentation": "cards",
                    "options": linked_description_options,
                    "dependent_options": linked_description_dependency,
                    "value": "",
                    "description": (
                        "Choose an image or a short clip after selecting a camera. Video is offered "
                        "only when that integration reports clip support."
                    ),
                    "full_width": True,
                    "show_when": {"source_key": "kind", "equals": "sensor"},
                },
                {
                    "type": "heading",
                    "label": "Name The Place",
                    "description": "This makes event history easy to browse and ask Tater about.",
                },
                {
                    "key": "area",
                    "label": "Area",
                    "type": "text",
                    "placeholder": "Back Yard, Front Door, Garage…",
                    "value": "",
                },
                {
                    "key": "name",
                    "label": "Display Name (optional)",
                    "type": "text",
                    "placeholder": "Uses the device name when left blank",
                    "value": "",
                },
                {
                    "key": "enabled",
                    "label": "Start Monitoring Now",
                    "type": "checkbox",
                    "value": True,
                },
                {
                    "key": "face_id_enabled",
                    "label": "Use Face ID On This Camera",
                    "type": "checkbox",
                    "value": True,
                    "description": (
                        "Run face burst snapshots and recognition for this camera. "
                        "The Face ID model must also be enabled in Settings › Models."
                    ),
                    "show_when": {"source_key": "kind", "equals": "camera"},
                },
                {
                    "type": "heading",
                    "label": "Optional Notifications",
                    "description": (
                        "Send this source's completed event with the same description and stored image or clip. "
                        "Face ID results are included when available."
                    ),
                },
                {
                    "key": "notifications_enabled",
                    "label": "Notify For This Source",
                    "type": "checkbox",
                    "value": False,
                },
                {
                    "key": "notification_destinations",
                    "label": "Send Notifications To",
                    "type": "multiselect",
                    "presentation": "cards",
                    "options": notification_options,
                    "value": [],
                    "description": "Choose one or more notification destinations already connected to Tater.",
                    "full_width": True,
                    "show_when": {"source_key": "notifications_enabled", "equals": True},
                },
            ],
        },
        "item_forms": [*event_forms, *face_forms, *monitor_forms],
    }


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    event_stats = _event_stats_for_ui(client)
    counts = event_stats.get("counts") if isinstance(event_stats.get("counts"), dict) else {}
    total_count = _as_int(counts.get("total"), 0, minimum=0)
    last_event = _text(event_stats.get("last_event")) or "n/a"
    monitors = _load_monitors(client)
    enabled_count = sum(1 for monitor in monitors.values() if _bool(monitor.get("enabled"), True))
    monitored_cameras = sum(1 for monitor in monitors.values() if _text(monitor.get("kind")) == "camera")
    monitored_sensors = sum(1 for monitor in monitors.values() if _text(monitor.get("kind")) == "sensor")
    face_runtime = _face_runtime_status(client)
    face_identities = _face_identity_rows(client, cleanup=True) if face_runtime.get("enabled") else {}
    known_people = sum(1 for identity in face_identities.values() if _face_identity_display_name(client, identity))
    face_state = _text(face_runtime.get("state") or "disabled").replace("_", " ").title()
    return {
        "summary": "Choose the cameras and sensors Awareness should observe, review recognized faces, and browse the history it stores.",
        "stats": [
            {"label": "Monitored Sources", "value": len(monitors)},
            {"label": "Active", "value": enabled_count},
            {"label": "Cameras", "value": monitored_cameras},
            {"label": "Sensors", "value": monitored_sensors},
            {"label": "Stored Events", "value": total_count},
            {"label": "Face ID", "value": face_state},
            {"label": "Known People", "value": known_people},
            {"label": "Last Event", "value": last_event},
        ],
        "items": [],
        "empty_message": "No cameras or sensors are being monitored yet.",
        "ui": _awareness_manager_ui(client),
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def _value(values: Dict[str, Any], payload: Dict[str, Any], key: str, default: Any = "") -> Any:
    if key in values:
        return _unwrap_form_value(values.get(key))
    return _unwrap_form_value(payload.get(key, default))



def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    if client is None:
        raise ValueError("Redis connection is unavailable.")
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)
    action_name = _text(action).lower()
    if action_name in {"awareness_refresh_devices", "awareness_refresh_entities"}:
        _monitor_registry(client, refresh=True)
        return {"ok": True, "message": "Available cameras and sensors refreshed."}
    if action_name == "awareness_save_event_filters":
        current_filters = _event_type_filters(client)
        current_list_view = _event_list_view_enabled(client)
        show_camera = _bool(
            _value(values, body, "show_camera_events", current_filters.get("camera", True)),
            current_filters.get("camera", True),
        )
        show_doorbell = _bool(
            _value(values, body, "show_doorbell_events", current_filters.get("doorbell", True)),
            current_filters.get("doorbell", True),
        )
        show_sensor = _bool(
            _value(values, body, "show_sensor_events", current_filters.get("sensor", True)),
            current_filters.get("sensor", True),
        )
        show_event_list_view = _bool(
            _value(values, body, "show_event_list_view", current_list_view),
            current_list_view,
        )
        _runtime_set(
            client,
            events_filter_camera=show_camera,
            events_filter_doorbell=show_doorbell,
            events_filter_sensor=show_sensor,
            events_list_view=show_event_list_view,
            events_page=1,
        )
        return {"ok": True, "message": "Event filters updated."}
    if action_name == "awareness_set_event_page":
        requested_page = _as_int(_value(values, body, "page", 1), 1, minimum=1)
        requested_page_size = _as_int(
            _value(values, body, "page_size", _EVENT_PAGE_SIZE_DEFAULT),
            _EVENT_PAGE_SIZE_DEFAULT,
            minimum=1,
            maximum=_EVENT_PAGE_SIZE_MAX,
        )
        _runtime_set(client, events_page=requested_page)
        return {
            "ok": True,
            "page": requested_page,
            "page_size": requested_page_size,
        }
    if action_name == "awareness_merge_face_identities":
        requested_ids = _monitor_string_list(
            _value(values, body, "identity_ids", body.get("ids") or [])
        )
        identities = _face_identity_rows(client)
        identity_ids = list(dict.fromkeys(value for value in requested_ids if value in identities))
        if len(identity_ids) < 2:
            raise ValueError("Select at least two people to merge.")
        named_ids = [value for value in identity_ids if _face_identity_display_name(client, identities.get(value) or {})]
        target_id = named_ids[0] if named_ids else identity_ids[0]
        merged_count = 0
        for source_id in identity_ids:
            if source_id == target_id:
                continue
            _merge_face_identities(client, source_id, target_id)
            merged_count += 1
        target = _face_identity_rows(client).get(target_id) or {}
        _refresh_stored_face_events(client)
        target_label = _face_identity_display_name(client, target) or "one unknown person"
        return {
            "ok": True,
            "id": target_id,
            "merged": merged_count,
            "message": f"Selected faces merged into {target_label}.",
        }
    if action_name == "awareness_unmerge_face_observations":
        identity_id = _text(body.get("id"))
        observation_ids = _monitor_string_list(_value(values, body, "observation_ids", []))
        result = _unmerge_face_observations(client, identity_id, observation_ids)
        _refresh_stored_face_events(client)
        return {
            "ok": True,
            "id": _text((result.get("split") or {}).get("id")),
            "moved": _as_int(result.get("moved"), 0, minimum=0),
            "message": "Selected images moved into a separate unknown person.",
        }
    if action_name == "awareness_move_face_images":
        identity_id = _text(body.get("id"))
        target_identity_id = _text(_value(values, body, "target_identity_id", ""))
        observation_ids = _monitor_string_list(_value(values, body, "observation_ids", []))
        result = _move_face_images(client, identity_id, target_identity_id, observation_ids)
        _refresh_stored_face_events(client)
        target = result.get("target") if isinstance(result.get("target"), dict) else {}
        target_label = _face_identity_display_name(client, target) or f"Unknown face · {_text(target.get('id'))[-6:]}"
        moved = _as_int(result.get("moved"), 0, minimum=0)
        return {
            "ok": True,
            "id": _text(target.get("id")),
            "moved": moved,
            "source_removed": bool(result.get("source_removed")),
            "message": f"Moved {moved} image{'s' if moved != 1 else ''} to {target_label}.",
        }
    if action_name == "awareness_remove_face_images":
        identity_id = _text(body.get("id"))
        observation_ids = _monitor_string_list(_value(values, body, "observation_ids", []))
        result = _remove_face_images(client, identity_id, observation_ids)
        _refresh_stored_face_events(client)
        removed = _as_int(result.get("removed"), 0, minimum=0)
        return {
            "ok": True,
            "id": identity_id,
            "removed": removed,
            "message": f"Removed {removed} selected image{'s' if removed != 1 else ''}. The person was kept.",
        }
    if action_name == "awareness_save_face_identity":
        identity_id = _text(body.get("id"))
        name = " ".join(_text(_value(values, body, "name", "")).split())
        person_link_supplied = "person_id" in values or "person_id" in body
        requested_person_id = _text(_value(values, body, "person_id", "")) if person_link_supplied else ""
        if len(name) > 80:
            raise ValueError("Person name must be 80 characters or fewer.")
        merge_into = _text(_value(values, body, "merge_into", ""))
        if merge_into and merge_into != identity_id:
            identity = _merge_face_identities(client, identity_id, merge_into)
            identity_id = _text(identity.get("id"))
        with _FACE_IDENTITY_LOCK:
            identities = _face_identity_rows(client)
            identity = dict(identities.get(identity_id) or {})
            if not identity:
                raise KeyError("Face identity not found.")
            previous_person_id = _text(identity.get("person_id"))
            if person_link_supplied and requested_person_id:
                linked_name = _people_person_name(client, requested_person_id)
                if not linked_name:
                    raise ValueError("Choose an existing Tater Person.")
                _people_attach_face_identity(
                    client,
                    person_id=requested_person_id,
                    identity_id=identity_id,
                    label=linked_name,
                )
                if previous_person_id and previous_person_id != requested_person_id:
                    _people_detach_face_identity(
                        client,
                        person_id=previous_person_id,
                        identity_id=identity_id,
                    )
                identity["person_id"] = requested_person_id
                identity["person_name"] = linked_name
                identity["name"] = linked_name
            elif person_link_supplied:
                if previous_person_id:
                    _people_detach_face_identity(
                        client,
                        person_id=previous_person_id,
                        identity_id=identity_id,
                    )
                identity.pop("person_id", None)
                identity.pop("person_name", None)
                identity["name"] = name
            elif not merge_into:
                identity["name"] = name
            identity["updated_at"] = _now_iso()
            identity = _save_face_identity(client, identity)
        _refresh_stored_face_events(client)
        saved_name = _face_identity_display_name(client, identity)
        return {
            "ok": True,
            "id": identity_id,
            "name": saved_name,
            "person_id": _text(identity.get("person_id")),
            "message": f"Face linked to {saved_name}." if _text(identity.get("person_id")) else (
                f"Face saved as {saved_name}." if saved_name else "Face sorting updated."
            ),
        }
    if action_name == "awareness_remove_face_identity":
        identity_id = _text(body.get("id"))
        if not _remove_face_identity(client, identity_id):
            raise KeyError("Face identity not found.")
        _refresh_stored_face_events(client)
        return {"ok": True, "id": identity_id, "message": "Face identity removed."}
    if action_name == "awareness_add_monitor":
        monitor = _build_monitor_from_values(values=values, payload=body, client=client)
        monitor = _save_monitor(client, monitor)
        return {"ok": True, "id": monitor["id"], "message": "Awareness is now monitoring this source."}
    if action_name == "awareness_save_monitor":
        monitor_id = _text(body.get("id"))
        existing = _get_monitor(client, monitor_id)
        if not existing:
            raise KeyError("Monitored source not found.")
        monitor = _build_monitor_from_values(
            values=values,
            payload=body,
            client=client,
            existing=existing,
        )
        monitor = _save_monitor(client, monitor)
        return {"ok": True, "id": monitor["id"], "message": "Monitored source updated."}
    if action_name == "awareness_remove_monitor":
        monitor_id = _text(body.get("id"))
        if not _remove_monitor(client, monitor_id):
            raise KeyError("Monitored source not found.")
        return {
            "ok": True,
            "id": monitor_id,
            "message": "Source removed. Its stored event history was kept.",
        }
    if action_name in {
        "awareness_add_rule",
        "awareness_save_rule",
        "awareness_remove_rule",
        "awareness_run_now",
    }:
        raise ValueError("Awareness rules have moved to Automation Core. Add a camera or sensor monitor instead.")
    raise ValueError(f"Unknown action: {action_name}")



def _monitor_matches_event(
    monitor: Dict[str, Any],
    *,
    provider: str,
    entity_id: str,
    new_state: Dict[str, Any],
    old_state: Dict[str, Any],
) -> bool:
    provider_token = _normalize_event_provider(provider)
    if provider_token != _normalize_event_provider(monitor.get("provider")):
        return False
    event_entity = _text(entity_id).casefold()
    if not event_entity:
        return False
    event_refs: set[str] = set()
    for value in monitor.get("event_refs") or []:
        _ref_provider, raw_ref = _split_provider_ref(value, provider_token)
        token = _text(raw_ref or value).casefold()
        if token:
            event_refs.add(token)
    device_refs = {
        _text(monitor.get("device_ref")).casefold(),
        _text(monitor.get("device_id")).casefold(),
    }
    device_refs.discard("")
    matched = event_entity in (event_refs or device_refs)
    if not matched and _text(monitor.get("kind")) == "camera":
        device_id = _text(monitor.get("device_id")).casefold()
        raw_device_ref = _text(monitor.get("device_ref")).casefold()
        if raw_device_ref.startswith("camera:"):
            raw_device_ref = raw_device_ref.split(":", 1)[1]
        aliases = {device_id, raw_device_ref}
        if provider_token == "unifi_protect":
            matched = any(alias and len(alias) >= 3 and alias in event_entity for alias in aliases)
        elif event_entity in device_refs:
            attrs = new_state.get("attributes") if isinstance(new_state, dict) else {}
            hint = " ".join(
                _text((attrs or {}).get(key)).lower()
                for key in ("event_type", "detection_type", "device_class", "resource_type")
            )
            matched = any(token in hint for token in ("motion", "person", "vehicle", "animal", "package", "doorbell"))
    if not matched:
        return False
    new_value = _text((new_state or {}).get("state")).lower()
    old_value = _text((old_state or {}).get("state")).lower()
    new_attrs = (new_state or {}).get("attributes") if isinstance(new_state, dict) else {}
    old_attrs = (old_state or {}).get("attributes") if isinstance(old_state, dict) else {}
    if new_value == old_value and new_attrs == old_attrs:
        return False
    if _text(monitor.get("kind")) == "camera" and new_value in _MONITOR_INACTIVE_STATES:
        return False
    trigger = _monitor_event_trigger(monitor, entity_id, new_state, old_state)
    selected_triggers = {
        _monitor_trigger_token(value)
        for value in monitor.get("trigger_events") or []
        if _monitor_trigger_token(value)
    }
    return bool(trigger and (not selected_triggers or trigger in selected_triggers))


def _enqueue_monitor_event(client: Any, monitor_id: Any, event: Dict[str, Any]) -> None:
    redis_obj = client or redis_client
    payload = {
        "monitor_id": _text(monitor_id),
        "event": event if isinstance(event, dict) else {},
        "queued_at": time.time(),
    }
    redis_obj.lpush(_EXEC_QUEUE_KEY, json.dumps(payload))
    _runtime_set(redis_obj, queue_depth=_queue_depth(redis_obj))


async def _awareness_worker_loop(stop_event: Optional[object], llm_client: Any) -> None:
    del llm_client
    while not (stop_event and stop_event.is_set()):
        job = _dequeue_execution(redis_client)
        if not job:
            await asyncio.sleep(0.25)
            continue
        _runtime_set(redis_client, queue_depth=_queue_depth(redis_client))
        monitor_id = _text(job.get("monitor_id"))
        event_payload = job.get("event") if isinstance(job.get("event"), dict) else {}
        monitor = _get_monitor(redis_client, monitor_id)
        if not monitor or not _bool(monitor.get("enabled"), True):
            continue
        try:
            result = await _execute_monitor(monitor, event_payload)
            current = _get_monitor(redis_client, monitor_id) or monitor
            now_ts = time.time()
            current["last_event_ts"] = _as_float(job.get("queued_at"), now_ts)
            current["last_status"] = "ok"
            current["last_summary"] = _compact(_text(result.get("summary")), limit=180)
            current["last_error"] = ""
            current["updated_at"] = now_ts
            _save_monitor(redis_client, current)
            _runtime_set(redis_client, last_run_ts=now_ts, last_error="")
            logger.info(
                "[awareness] monitor %s (%s) observed %s",
                monitor_id,
                _text(monitor.get("kind")),
                current["last_summary"] or "an event",
            )
        except Exception as exc:
            now_ts = time.time()
            logger.exception("[awareness] monitor execution failed for %s", monitor_id)
            current = _get_monitor(redis_client, monitor_id) or monitor
            current["last_event_ts"] = _as_float(job.get("queued_at"), now_ts)
            current["last_status"] = "error"
            current["last_error"] = _compact(str(exc), limit=300)
            current["updated_at"] = now_ts
            _save_monitor(redis_client, current)
            _runtime_set(redis_client, last_run_ts=now_ts, last_error=str(exc))


async def _handle_trigger_state_change(
    *,
    provider: str,
    entity_id: str,
    new_state: Dict[str, Any],
    old_state: Dict[str, Any],
) -> None:
    provider_token = _normalize_event_provider(provider)
    monitors = _load_monitors(redis_client)
    for monitor in monitors.values():
        if not _bool(monitor.get("enabled"), True):
            continue
        try:
            if not _monitor_matches_event(
                monitor,
                provider=provider_token,
                entity_id=entity_id,
                new_state=new_state,
                old_state=old_state,
            ):
                continue
        except Exception:
            logger.exception("[awareness] event match failed for monitor %s", monitor.get("id"))
            continue
        dedupe_key = (
            f"awareness:monitor:dedupe:{provider_token}:{_text(monitor.get('id'))}:"
            f"{entity_id}:{_text(new_state.get('state')).lower()}"
        )
        if redis_client.set(dedupe_key, "1", ex=2, nx=True) is None:
            continue
        _enqueue_monitor_event(
            redis_client,
            _text(monitor.get("id")),
            {"entity_id": entity_id, "new_state": new_state, "old_state": old_state},
        )


async def _handle_state_change_event(event_payload: Dict[str, Any]) -> None:
    if not isinstance(event_payload, dict):
        return
    entity_id = _text(event_payload.get("entity_id"))
    if not entity_id:
        return
    new_state = event_payload.get("new_state") if isinstance(event_payload.get("new_state"), dict) else {}
    old_state = event_payload.get("old_state") if isinstance(event_payload.get("old_state"), dict) else {}
    await _handle_trigger_state_change(
        provider="homeassistant",
        entity_id=entity_id,
        new_state=new_state,
        old_state=old_state,
    )



def _unifi_monitored_camera_ids() -> set[str]:
    out: set[str] = set()
    try:
        monitors = _load_monitors(redis_client)
    except Exception:
        return out
    for monitor in monitors.values():
        if not isinstance(monitor, dict):
            continue
        if _text(monitor.get("kind")).lower() != "camera":
            continue
        if not _bool(monitor.get("enabled"), True):
            continue
        if _normalize_event_provider(monitor.get("provider")) != "unifi_protect":
            continue
        camera_target = _monitor_camera_target(monitor)
        camera_id = _text(_unifi_camera_id_from_entity(camera_target)).lower()
        if not camera_id:
            continue
        out.add(camera_id)
    return out

def _unifi_ws_event_item(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    item = payload.get("item")
    if isinstance(item, dict):
        out = dict(item)
        # Preserve websocket lifecycle metadata so event close/idle updates do not look like fresh presses.
        action = _text(payload.get("action"))
        model_key = _text(payload.get("modelKey") or payload.get("model_key"))
        event_id = _text(payload.get("id")) if model_key.lower() in {"event", "events"} else ""
        if action:
            out.setdefault("__ws_action", action)
        if model_key:
            out.setdefault("__ws_model_key", model_key)
        if event_id:
            out.setdefault("__ws_event_id", event_id)
        return out
    model_key = _text(payload.get("modelKey") or payload.get("model_key")).lower()
    if model_key in {"event", "events"}:
        out = dict(payload)
        if _text(payload.get("action")):
            out.setdefault("__ws_action", _text(payload.get("action")))
        if _text(payload.get("id")):
            out.setdefault("__ws_event_id", _text(payload.get("id")))
        out.setdefault("__ws_model_key", model_key)
        return out
    return None


def _unifi_ws_action(item: Dict[str, Any]) -> str:
    return _text(item.get("__ws_action") or item.get("action")).lower()


def _unifi_ws_model_key(item: Dict[str, Any]) -> str:
    return _text(item.get("__ws_model_key") or item.get("modelKey") or item.get("model_key")).lower()


def _unifi_event_device_id(item: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        token = _text(item.get(key))
        if token:
            return token
    return ""


def _unifi_event_type_token(item: Dict[str, Any]) -> str:
    raw_parts = [
        _text(item.get("type")),
        _text(item.get("eventType")),
        _text(item.get("event_type")),
    ]
    token = " ".join(part for part in raw_parts if part).lower()
    return re.sub(r"[^a-z0-9]+", "", token)


def _unifi_sensor_ws_state(event_token: str) -> Optional[str]:
    token = _text(event_token).lower()
    if "sensoropen" in token:
        return "on"
    if "sensorclosed" in token:
        return "off"
    return None


def _unifi_sensor_event_ts(item: Dict[str, Any]) -> float:
    start = _as_float(item.get("start"), 0.0)
    if start > 0:
        return start
    return _as_float(item.get("end"), 0.0)


def _unifi_event_id(item: Dict[str, Any]) -> str:
    return _unifi_event_device_id(item, "id", "eventId", "event_id", "__ws_event_id")


def _unifi_camera_event_ts(item: Dict[str, Any]) -> float:
    for key in ("start", "timestamp", "time", "ts", "createdAt", "created_at"):
        value = _as_float(item.get(key), 0.0)
        if value > 0:
            return value
    return _as_float(item.get("end"), 0.0)


def _unifi_should_emit_camera_ws_edge(*, camera_id: str, event_kind: str, event_ts: float, event_id: str) -> bool:
    camera_token = _text(camera_id).lower()
    kind_token = _slug(event_kind)
    evt_id = _text(event_id)
    evt_ts = _as_float(event_ts, 0.0)
    if not camera_token or not kind_token:
        return False
    key = f"{camera_token}:{kind_token}"
    with _UNIFI_CAMERA_EVENT_LOCK:
        prev_ts, prev_id = _UNIFI_CAMERA_LAST_EVENT.get(key, (0.0, ""))
        if evt_id and prev_id and evt_id == prev_id:
            return False
        if evt_ts > 0 and prev_ts > 0 and evt_ts <= prev_ts:
            return False
        _UNIFI_CAMERA_LAST_EVENT[key] = (max(prev_ts, evt_ts), evt_id)
    return True


def _unifi_doorbell_token(event_token: str) -> bool:
    token = _text(event_token).lower()
    return "ring" in token or "doorbell" in token


def _unifi_truthy_marker(value: Any) -> bool:
    marker = _unifi_marker_token(value).lower()
    return bool(marker and marker not in {"0", "false", "off", "none", "null", "nan"})


def _unifi_doorbell_ws_skip_reason(item: Dict[str, Any], event_token: str) -> str:
    if not _unifi_doorbell_token(event_token):
        return "not_doorbell"

    action = _unifi_ws_action(item)
    model_key = _unifi_ws_model_key(item)
    if model_key in {"event", "events"} and action in {"update", "remove", "delete", "deleted"}:
        return f"{model_key}_{action}"

    for key in ("end", "endTime", "end_time", "endedAt", "ended_at", "stop", "stoppedAt", "completedAt"):
        if key in item and _unifi_truthy_marker(item.get(key)):
            return f"{key}_set"

    for key in ("state", "status", "eventState", "event_state", "lifecycle", "stage"):
        token = _slug(item.get(key))
        if token in {"idle", "end", "ended", "complete", "completed", "done", "closed", "inactive", "stop", "stopped", "finished", "false", "off"}:
            return f"{key}:{token}"

    for key in ("isRinging", "is_ringing", "isDoorbellRinging", "is_doorbell_ringing", "doorbellRinging", "doorbell_ringing"):
        if key in item and not _bool(item.get(key), False):
            return f"{key}:false"

    return ""


def _unifi_should_emit_sensor_ws_edge(*, sensor_id: str, state: str, event_ts: float, event_id: str) -> bool:
    token = _text(sensor_id).lower()
    next_state = _text(state).lower()
    evt_id = _text(event_id)
    evt_ts = _as_float(event_ts, 0.0)
    if not token or next_state not in {"on", "off"}:
        return False
    with _UNIFI_SENSOR_EVENT_LOCK:
        prev_state, prev_ts, prev_id = _UNIFI_SENSOR_LAST_EVENT.get(token, ("", 0.0, ""))
        if evt_id and prev_id and evt_id == prev_id:
            return False
        if evt_ts > 0 and prev_ts > 0 and evt_ts <= prev_ts:
            return False
        if not evt_ts and next_state == prev_state:
            return False
        _UNIFI_SENSOR_LAST_EVENT[token] = (next_state, max(prev_ts, evt_ts), evt_id)
    return True


def _unifi_event_smart_types(item: Dict[str, Any], event_token: str) -> set[str]:
    found: set[str] = set()
    if any(token in event_token for token in ("person", "vehicle", "animal", "package", "face", "licenseplate")):
        for raw_type in ("person", "vehicle", "animal", "package", "face", "license_plate"):
            compact = raw_type.replace("_", "")
            if raw_type in event_token or compact in event_token:
                found.add(raw_type)
    for candidate in ("person", "vehicle", "animal", "package", "face", "license_plate"):
        try:
            if _unifi_event_matches_smart_type(item, candidate):
                found.add(candidate)
        except Exception:
            continue
    return {token for token in (_unifi_normalize_smart_type(x) for x in found) if token}




def _unifi_name_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    camera_names: Dict[str, str] = {}
    sensor_names: Dict[str, str] = {}
    registry = _monitor_registry(redis_client)
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        if _normalize_event_provider(device.get("integration_id")) != "unifi_protect":
            continue
        device_id = _text(device.get("id") or device.get("ref")).lower()
        if not device_id:
            continue
        name = _text(device.get("name")) or device_id
        if _monitor_device_kind(device) == "camera":
            camera_names[device_id] = name
        elif _monitor_device_kind(device) == "sensor":
            sensor_names[device_id] = name

    return camera_names, sensor_names


async def _handle_unifi_ws_event(item: Dict[str, Any]) -> bool:
    event_token = _unifi_event_type_token(item)
    if not event_token:
        return False

    handled = False
    now_ts = time.time()
    name_hint = _text(item.get("name") or item.get("title") or item.get("displayName"))

    camera_id = _unifi_event_device_id(
        item,
        "camera",
        "cameraId",
        "camera_id",
    )
    sensor_id = _unifi_event_device_id(
        item,
        "sensor",
        "sensorId",
        "sensor_id",
    )
    device_id = _unifi_event_device_id(
        item,
        "device",
        "deviceId",
        "device_id",
    )

    doorbell_like_event = _unifi_doorbell_token(event_token)
    doorbell_skip_reason = _unifi_doorbell_ws_skip_reason(item, event_token)
    is_ring_event = doorbell_like_event and not doorbell_skip_reason
    is_sensor_event = ("sensor" in event_token)
    is_smart_event = ("smartdetect" in event_token)

    if not camera_id and (doorbell_like_event or is_smart_event or ("camera" in event_token and not is_sensor_event)):
        camera_id = device_id
    if not sensor_id and is_sensor_event:
        sensor_id = device_id

    camera_id = _text(camera_id).lower()
    sensor_id = _text(sensor_id).lower()

    camera_names, sensor_names = _unifi_name_maps()
    camera_name = camera_names.get(camera_id) if camera_id else ""
    sensor_name = sensor_names.get(sensor_id) if sensor_id else ""
    if not camera_name:
        camera_name = name_hint or camera_id
    if not sensor_name:
        sensor_name = name_hint or sensor_id

    if is_ring_event and camera_id:
        doorbell_entity = _unifi_camera_doorbell_trigger(camera_id)
        event_id = _unifi_event_id(item)
        event_ts = _unifi_camera_event_ts(item)
        if _unifi_should_emit_camera_ws_edge(
            camera_id=camera_id,
            event_kind="doorbell",
            event_ts=event_ts,
            event_id=event_id,
        ):
            attrs = {"friendly_name": camera_name}
            if event_id:
                attrs["event_id"] = event_id
            if event_ts > 0:
                attrs["event_ts"] = event_ts
                attrs["event_start"] = event_ts
            event_end = _as_float(item.get("end"), 0.0)
            if event_end > 0:
                attrs["event_end"] = event_end
            await _handle_trigger_state_change(
                provider="unifi_protect",
                entity_id=doorbell_entity,
                new_state={"state": "on", "attributes": attrs},
                old_state={"state": "off", "attributes": attrs},
            )
        else:
            logger.debug(
                "[awareness] suppressed UniFi doorbell ws edge entity=%s token=%s event_id=%s start=%s",
                doorbell_entity,
                event_token,
                event_id,
                event_ts,
            )
        handled = True
    elif doorbell_skip_reason and doorbell_like_event and camera_id:
        logger.debug(
            "[awareness] ignored UniFi doorbell non-press entity=%s token=%s action=%s reason=%s",
            _unifi_camera_doorbell_trigger(camera_id),
            event_token,
            _unifi_ws_action(item) or "n/a",
            doorbell_skip_reason,
        )
        handled = True

    if "cameramotion" in event_token and camera_id:
        motion_entity = _unifi_camera_motion_trigger(camera_id)
        attrs = {"friendly_name": camera_name}
        event_id = _unifi_event_id(item)
        event_ts = _unifi_camera_event_ts(item)
        event_end = _as_float(item.get("end"), 0.0)
        if event_id:
            attrs["event_id"] = event_id
        if event_ts > 0:
            attrs["event_ts"] = event_ts
            attrs["event_start"] = event_ts
        if event_end > 0:
            attrs["event_end"] = event_end
        await _handle_trigger_state_change(
            provider="unifi_protect",
            entity_id=motion_entity,
            new_state={"state": "on", "attributes": attrs},
            old_state={"state": "off", "attributes": attrs},
        )
        handled = True

    if ("camerasmartdetect" in event_token or (is_smart_event and not is_sensor_event)) and camera_id:
        smart_types = _unifi_event_smart_types(item, event_token)
        if not smart_types and camera_id in _unifi_monitored_camera_ids():
            smart_types = {"motion"}
        for smart_type in sorted(smart_types):
            smart_entity = _unifi_camera_smart_trigger(camera_id, smart_type)
            attrs = {
                "friendly_name": f"{camera_name} {_unifi_smart_type_label(smart_type)}",
                "camera_name": camera_name,
                "detection_type": smart_type,
            }
            event_id = _unifi_event_id(item)
            event_ts = _unifi_camera_event_ts(item)
            event_end = _as_float(item.get("end"), 0.0)
            if event_id:
                attrs["event_id"] = event_id
            if event_ts > 0:
                attrs["event_ts"] = event_ts
                attrs["event_start"] = event_ts
            if event_end > 0:
                attrs["event_end"] = event_end
            await _handle_trigger_state_change(
                provider="unifi_protect",
                entity_id=smart_entity,
                new_state={"state": "on", "attributes": attrs},
                old_state={"state": "off", "attributes": attrs},
            )
            handled = True

    sensor_edge_state = _unifi_sensor_ws_state(event_token)
    if sensor_edge_state and sensor_id:
        sensor_entity = _unifi_sensor_entity(sensor_id)
        sensor_event_ts = _unifi_sensor_event_ts(item)
        sensor_event_id = _text(item.get("id"))
        if _unifi_should_emit_sensor_ws_edge(
            sensor_id=sensor_id,
            state=sensor_edge_state,
            event_ts=sensor_event_ts,
            event_id=sensor_event_id,
        ):
            attrs = {"friendly_name": sensor_name}
            prior = "off" if sensor_edge_state == "on" else "on"
            await _handle_trigger_state_change(
                provider="unifi_protect",
                entity_id=sensor_entity,
                new_state={"state": sensor_edge_state, "attributes": attrs},
                old_state={"state": prior, "attributes": attrs},
            )
            handled = True
        else:
            logger.debug(
                "[awareness] suppressed UniFi sensor ws edge entity=%s token=%s event_id=%s start=%s",
                sensor_entity,
                event_token,
                sensor_event_id,
                sensor_event_ts,
            )

    if handled:
        _runtime_set(
            redis_client,
            unifi_connected=True,
            unifi_ws_connected=True,
            ws_connected=False,
            unifi_last_event_ts=now_ts,
            unifi_last_event_type=event_token,
            last_ws_event_ts=now_ts,
            last_error="",
        )
    else:
        logger.debug(
            "[awareness] unifi ws event not mapped type=%s keys=%s",
            event_token,
            ",".join(sorted([_text(k) for k in item.keys()])[:16]),
        )
    return handled


def _hue_resource_event_state(payload: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    resource_type = _text(payload.get("resource_type") or payload.get("type")).lower()
    resource = payload.get("resource") if isinstance(payload.get("resource"), dict) else {}
    state_text = _text(payload.get("state"))
    attrs = {
        "friendly_name": _text(resource.get("metadata", {}).get("name")) if isinstance(resource.get("metadata"), dict) else "",
        "resource_type": resource_type,
    }
    if resource_type == "motion":
        motion = resource.get("motion") if isinstance(resource.get("motion"), dict) else {}
        active = bool(motion.get("motion")) if "motion" in motion else state_text.lower() in {"motion", "on", "true", "1"}
        return ("on" if active else "off"), attrs, {"state": "off" if active else "on", "attributes": attrs}
    if resource_type == "contact":
        contact = resource.get("contact") if isinstance(resource.get("contact"), dict) else {}
        report = contact.get("contact_report") if isinstance(contact.get("contact_report"), dict) else {}
        report_state = _text(report.get("state") or state_text).lower()
        open_state = report_state in {"no_contact", "open", "opened", "on", "true", "1"}
        return ("on" if open_state else "off"), attrs, {"state": "off" if open_state else "on", "attributes": attrs}
    if resource_type == "light":
        on = resource.get("on") if isinstance(resource.get("on"), dict) else {}
        active = bool(on.get("on")) if "on" in on else state_text.lower() == "on"
        return ("on" if active else "off"), attrs, {"state": "off" if active else "on", "attributes": attrs}
    return state_text or "changed", attrs, {"state": "", "attributes": attrs}


async def _handle_hue_runtime_event(payload: Dict[str, Any]) -> None:
    resource_type = _text(payload.get("resource_type") or payload.get("type")).lower()
    resource_id = _text(payload.get("id"))
    if not resource_type or not resource_id:
        return
    state, attrs, old_state = _hue_resource_event_state(payload)
    await _handle_trigger_state_change(
        provider="hue",
        entity_id=f"{resource_type}:{resource_id}",
        new_state={"state": state, "attributes": attrs},
        old_state=old_state,
    )


async def _handle_unifi_network_runtime_event(kind: str, payload: Dict[str, Any]) -> None:
    category = _text(payload.get("category")).lower()
    if category not in {"client", "device"}:
        category = "client" if _text(kind).lower().startswith("client_") else "device"
    row_id = _text(payload.get("id"))
    if not row_id:
        return
    previous = payload.get("previous") if isinstance(payload.get("previous"), dict) else {}
    event_kind = _text(kind).lower()
    active = event_kind not in {"client_disconnected", "device_missing"}
    await _handle_trigger_state_change(
        provider="unifi_network",
        entity_id=f"{category}:{row_id}",
        new_state={"state": "on" if active else "off", "attributes": dict(payload)},
        old_state={"state": "off" if active else "on", "attributes": dict(previous)},
    )


async def _handle_integration_runtime_event(event: Dict[str, Any]) -> None:
    if not isinstance(event, dict):
        return
    provider = _normalize_event_provider(event.get("provider"))
    kind = _text(event.get("kind")).lower()
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    event_ts = _as_float(event.get("ts"), time.time())

    if provider == "homeassistant" and kind == "state_changed":
        await _handle_state_change_event(payload)
        _runtime_set(redis_client, last_ws_event_ts=event_ts, last_error="")
        return

    if provider == "unifi_protect" and kind in {"protect_event", "unifi_protect_event"}:
        item = payload
        if "item" in item or "modelKey" in item or "model_key" in item:
            item = _unifi_ws_event_item(item) or {}
        if item:
            await _handle_unifi_ws_event(item)
        return

    if provider == "hue" and kind in {"resource_update", "hue_resource_update"}:
        await _handle_hue_runtime_event(payload)
        _runtime_set(redis_client, last_ws_event_ts=event_ts, last_error="")
        return

    if provider == "unifi_network" and kind in {
        "client_connected",
        "client_update",
        "client_disconnected",
        "device_seen",
        "device_update",
        "device_missing",
    }:
        await _handle_unifi_network_runtime_event(kind, payload)
        _runtime_set(redis_client, last_ws_event_ts=event_ts, last_error="")
        return

    if provider != "all":
        entity_id = _text(
            payload.get("entity_id")
            or payload.get("ref")
            or payload.get("device_ref")
            or payload.get("resource_ref")
            or payload.get("id")
            or payload.get("device_id")
        )
        if entity_id:
            attrs = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else dict(payload)
            new_state = payload.get("new_state") if isinstance(payload.get("new_state"), dict) else {}
            old_state = payload.get("old_state") if isinstance(payload.get("old_state"), dict) else {}
            if not new_state:
                new_state = {
                    "state": _text(payload.get("state") or payload.get("status") or "on"),
                    "attributes": attrs,
                }
            if not old_state:
                old_state = {
                    "state": _text(payload.get("old_state_value") or payload.get("previous_state") or ""),
                    "attributes": payload.get("previous_attributes") if isinstance(payload.get("previous_attributes"), dict) else {},
                }
            await _handle_trigger_state_change(provider=provider, entity_id=entity_id, new_state=new_state, old_state=old_state)
            _runtime_set(redis_client, last_ws_event_ts=event_ts, last_error="")
        return


async def _awareness_integration_runtime_loop(stop_event: Optional[object]) -> None:
    try:
        stored_seq_raw = redis_client.get(_AWARENESS_RUNTIME_SEQ_KEY)
    except Exception:
        stored_seq_raw = None
    if stored_seq_raw is None:
        last_seq = _integration_runtime_current_seq(redis_client)
        try:
            redis_client.set(_AWARENESS_RUNTIME_SEQ_KEY, str(last_seq))
        except Exception:
            logger.debug("[awareness] failed to initialize integration runtime cursor", exc_info=True)
    else:
        last_seq = _as_int(stored_seq_raw, 0, minimum=0)

    logger.info("[awareness] integration runtime event consumer started after seq=%s", last_seq)
    next_status_sync = 0.0
    while not (stop_event and stop_event.is_set()):
        now_ts = time.time()
        if now_ts >= next_status_sync:
            _sync_integration_runtime_status(redis_client)
            next_status_sync = now_ts + 5.0

        events = _integration_runtime_events(redis_client, after_seq=last_seq, limit=100)
        if not events:
            await asyncio.sleep(0.5)
            continue

        for event in events:
            seq = _as_int(event.get("seq"), last_seq, minimum=0)
            if seq <= last_seq:
                continue
            try:
                await _handle_integration_runtime_event(event)
            except Exception as exc:
                _runtime_set(redis_client, last_error=str(exc))
                logger.warning("[awareness] integration runtime event failed seq=%s: %s", seq, exc)
            finally:
                last_seq = max(last_seq, seq)
                try:
                    redis_client.set(_AWARENESS_RUNTIME_SEQ_KEY, str(last_seq))
                except Exception:
                    logger.debug("[awareness] failed to persist integration runtime cursor", exc_info=True)
        await asyncio.sleep(0)
    _sync_integration_runtime_status(redis_client)


async def _awareness_retention_loop(stop_event: Optional[object]) -> None:
    while not (stop_event and stop_event.is_set()):
        sources = _discover_event_sources(redis_client)
        for source in sources:
            _trim_events_for_source(redis_client, source)
        await asyncio.sleep(300.0)


async def _awareness_main(stop_event: Optional[object], llm_client: Any) -> None:
    monitors = _load_monitors(redis_client)
    enabled_monitors = sum(1 for monitor in monitors.values() if _bool(monitor.get("enabled"), True))
    worker_count = max(1, int(_AWARENESS_WORKER_COUNT))
    _runtime_set(
        redis_client,
        started_at=time.time(),
        ws_connected=False,
        unifi_connected=False,
        unifi_ws_connected=False,
        queue_depth=_queue_depth(redis_client),
        worker_count=worker_count,
        events_page=1,
        last_error="",
    )
    logger.info(
        "[awareness] core started v%s (%s) (monitors=%d enabled=%d workers=%d)",
        __version__,
        __file__,
        len(monitors),
        enabled_monitors,
        worker_count,
    )
    tasks = [
        asyncio.create_task(_awareness_worker_loop(stop_event, llm_client))
        for _ in range(worker_count)
    ]
    tasks.extend(
        [
            asyncio.create_task(_awareness_integration_runtime_loop(stop_event)),
            asyncio.create_task(_awareness_retention_loop(stop_event)),
        ]
    )
    try:
        while not (stop_event and stop_event.is_set()):
            await asyncio.sleep(0.5)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        _runtime_set(redis_client, ws_connected=False, unifi_connected=False, unifi_ws_connected=False)
        logger.info("[awareness] core stopped")


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    normalized_platform = _text(platform).lower()
    del normalized_platform
    return [
        {
            "id": "events_query",
            "description": (
                "Search stored Awareness event history for past activity around the home, including doors, windows, "
                "garage, and camera-covered areas. Use it for questions such as what happened, when something happened, "
                "counts, timelines, or summaries over a stated period. Do not use it to determine what is visibly "
                "happening right now or for a live/current camera view; use camera_control for a fresh camera snapshot."
            ),
            "usage": '{"function":"events_query","arguments":{"query":"what happened in the front yard today?"}}',
        },
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
    del platform, scope
    func = _text(tool_id).lower()
    if func not in {"events_query", "event_query", "events_search", "awareness_events_query"}:
        return None

    payload = dict(args) if isinstance(args, dict) else {}
    payload_origin = payload.get("origin") if isinstance(payload.get("origin"), dict) else origin
    redis_obj = redis_client if redis_client is not None else globals().get("redis_client")
    if redis_obj is None:
        return {
            "tool": "events_query",
            "ok": False,
            "error": "events store unavailable",
            "summary_for_user": "Awareness event storage is unavailable right now.",
        }
    try:
        return await _events_query_kernel(
            args=payload,
            llm_client=llm_client,
            origin=payload_origin,
            redis_obj=redis_obj,
        )
    except Exception as exc:
        return {
            "tool": "events_query",
            "ok": False,
            "error": f"events_query failed: {exc}",
            "summary_for_user": "I couldn't search awareness events right now.",
        }


def run(stop_event=None):
    llm_client = _get_primary_llm_client_from_env()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_awareness_main(stop_event, llm_client))
    except asyncio.CancelledError:
        logger.info("[awareness] awareness core cancelled; stopping")
    except KeyboardInterrupt:
        logger.info("[awareness] awareness core interrupted; stopping")
    except Exception:
        logger.exception("[awareness] awareness core crashed")
        raise
    finally:
        try:
            loop.close()
        except Exception:
            pass
