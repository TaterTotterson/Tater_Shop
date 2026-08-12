"""Observe selected cameras and sensors and keep a queryable home-activity history."""

import asyncio
import ast
import base64
import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv

from helpers import extract_json, get_llm_client_from_env, redis_client
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

__version__ = "4.1.0"
CORE_DESCRIPTION = (
    "Choose which cameras and sensors Tater should observe, retain their bounded event history and snapshots, "
    "and answer questions about past activity. Use Automation Core for triggers, notifications, announcements, "
    "and device actions."
)
TAGS = ["awareness", "cameras", "sensors", "event-history", "vision"]

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
            "description": "Store camera/doorbell snapshot images in Redis for future event gallery UI.",
        },
        "event_snapshot_max_kb": {
            "label": "Snapshot Max Size (KB)",
            "type": "number",
            "default": 768,
            "description": "Maximum JPEG size to store per event snapshot.",
        },
        "camera_monitor_cooldown_seconds": {
            "label": "Camera Event Cooldown (sec)",
            "type": "number",
            "default": 30,
            "description": "Minimum time between snapshot and vision checks for each monitored camera.",
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
    "event_type",
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
        source_mapped = False
        source_type = _category_token(source.get("type"))
        source_ref = _text(source.get("ref")).lower().replace("-", "_")
        state_on = _text(source.get("state_on")).lower()
        state_off = _text(source.get("state_off")).lower()
        ref_hint = source_ref if source_type in {"", "event", "binary", "sensor", "value"} else ""
        corpus = f"{source_type} {ref_hint}"
        if "license_plate" in corpus or "licenseplate" in corpus:
            add("license_plate")
            source_mapped = True
        for event in ("person", "vehicle", "animal", "package", "face", "doorbell", "motion"):
            if event in corpus:
                add(event)
                source_mapped = True
        if any(token in corpus for token in ("contact", "entry", "door_window", "open_close", "window")):
            add("opens", "closes")
            source_mapped = True
        if any(token in corpus for token in ("connectivity", "online", "network")):
            add("connects", "disconnects")
            source_mapped = True
        if source_type in {"occupancy", "presence", "switch", "light", "input", "power", "leak", "tamper"}:
            add("turns_on", "turns_off")
            source_mapped = True
        if source_type in {"temperature", "humidity", "illuminance", "energy", "battery", "sensor", "value"}:
            add("changed")
            source_mapped = True
        if not source_mapped and (state_on or state_off):
            add("turns_on", "turns_off")

    capability_corpus = " ".join(
        _category_token(value)
        for value in [
            *(device.get("category_ids") or []),
            *(device.get("capabilities") or []),
            *(device.get("features") or []),
            device.get("type"),
        ]
        if _category_token(value)
    )
    if "license_plate" in capability_corpus or "licenseplate" in capability_corpus:
        add("license_plate")
    for event in ("person", "vehicle", "animal", "package", "face", "doorbell", "motion"):
        if event in capability_corpus:
            add(event)

    categories = _monitor_device_categories(device)
    if "doorbell" in categories:
        add("doorbell")
    if not found:
        if "camera" in categories or _monitor_device_kind(device) == "camera":
            add("motion")
        if "motion" in categories:
            add("motion")
        if categories.intersection({"entry_sensor", "garage_door"}):
            add("opens", "closes")
        if categories.intersection({"presence", "leak"}):
            add("turns_on", "turns_off")
        if categories.intersection({"temperature", "humidity", "illuminance", "energy", "battery", "sensor"}):
            add("changed")
    if not found:
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
    state = _text(device.get("status") or device.get("state"))
    return {
        "value": value,
        "label": _text(device.get("name")) or _text(device.get("id") or device.get("ref")),
        "description": " • ".join(item for item in (room, provider_name) if item),
        "meta": state,
        "icon": "◎" if _monitor_device_kind(device) == "camera" else "◇",
    }


def _monitor_device_options(
    registry: Dict[str, Any],
    *,
    current_kind: str = "camera",
    current_device: str = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    options_by_kind: Dict[str, List[Dict[str, Any]]] = {"camera": [], "sensor": []}
    seen: set[str] = set()
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        kind = _monitor_device_kind(device)
        option = _monitor_device_option(device)
        value = _text(option.get("value"))
        if kind not in options_by_kind or not value or value in seen:
            continue
        seen.add(value)
        options_by_kind[kind].append(option)
    for rows in options_by_kind.values():
        rows.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("value"))))
    kind = current_kind if current_kind in options_by_kind else "camera"
    selected = [dict(row) for row in options_by_kind[kind]]
    current = _text(current_device)
    if current and not any(_text(row.get("value")) == current for row in selected):
        selected.append({"value": current, "label": f"{current} (saved)", "icon": "◆"})
    return selected, {
        "source_key": "kind",
        "options_by_source": options_by_kind,
        "default_options": [*options_by_kind["camera"], *options_by_kind["sensor"]],
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


def _events_query_compact_event_for_llm(event: Dict[str, Any]) -> Dict[str, Any]:
    source = _text(event.get("source"))
    data_payload = event.get("data") if isinstance(event.get("data"), dict) else {}
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
    all_compact_events = [_events_query_compact_event_for_llm(item) for item in fetched_sorted]
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
        description = _text(event.get("message"))
        subtitle = " • ".join([part for part in subtitle_parts if part])

        fields: List[Dict[str, Any]] = []
        snapshot = _event_snapshot_preview(client, event)
        snapshot_id = _text(snapshot.get("snapshot_id"))
        if (not list_view) and snapshot.get("data_url"):
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
    if any(token in corpus for token in ("contact", "entry", "door_window", "open_close", "window")):
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

    snapshot_store: Dict[str, Any] = {}
    jpeg: bytes = b""
    content_type = "image/jpeg"
    error_text = ""
    try:
        jpeg, content_type = await _capture_camera_snapshot(provider, camera_target)
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
        summary = _compact(summary, limit=180) or "Nothing notable."
    except Exception as exc:
        error_text = str(exc)
        logger.warning("[awareness] monitored camera capture failed for %s: %s", camera_target, exc)
        summary = ""
        snapshot_store = {"stored": False, "reason": "capture_failed", "bytes": 0}

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

    event_payload: Dict[str, Any] = {
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
        },
    }
    if error_text:
        event_payload["data"]["capture_error"] = _compact(error_text, limit=180)
    _monitor_snapshot_fields(event_payload, snapshot_store)
    _append_event(redis_client, source=area, payload=event_payload)
    return {"ok": True, "summary": summary, "event_type": event_kind, "warning": error_text}


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
    summary = _compact(summary, limit=180)
    event_payload = {
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
    _append_event(redis_client, source=area, payload=event_payload)
    return {"ok": True, "summary": summary, "event_type": action_token}


async def _execute_monitor(monitor: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    if _text(monitor.get("kind")) == "camera":
        return await _execute_camera_monitor(monitor, event)
    return await _execute_sensor_monitor(monitor, event)



def _monitor_form(
    monitor: Dict[str, Any],
    registry: Dict[str, Any],
) -> Dict[str, Any]:
    kind = _text(monitor.get("kind") or "camera")
    selected_device = _provider_ref(monitor.get("provider"), monitor.get("device_id"))
    device_options, device_dependency = _monitor_device_options(
        registry,
        current_kind=kind,
        current_device=selected_device,
    )
    trigger_options, trigger_dependency = _monitor_trigger_dependency(
        registry,
        current_device=selected_device,
        current_events=monitor.get("trigger_events"),
    )
    trigger_labels = [
        _text(_monitor_trigger_option(value).get("label"))
        for value in monitor.get("trigger_events") or []
        if _text(value)
    ]
    enabled_label = "Monitoring" if _bool(monitor.get("enabled"), True) else "Paused"
    last_event = _fmt_ts(monitor.get("last_event_ts"))
    return {
        "id": monitor["id"],
        "group": "monitors",
        "title": _text(monitor.get("name")) or _text(monitor.get("area")) or "Monitored source",
        "subtitle": (
            f"{enabled_label} • {kind.title()} • {_provider_label(monitor.get('provider'))} • "
            f"{', '.join(trigger_labels) or 'No triggers'} • last event: {last_event}"
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
        ],
    }


def _awareness_manager_ui(client: Any) -> Dict[str, Any]:
    monitors = _load_monitors(client)
    registry = _monitor_registry(client)
    event_page = _event_page_for_ui(client)
    event_forms = list(event_page.get("items") or [])
    monitor_forms = [
        _monitor_form(monitor, registry)
        for monitor in sorted(
            monitors.values(),
            key=lambda row: (_text(row.get("kind")), _text(row.get("name")).casefold(), _text(row.get("id"))),
        )
    ]
    default_kind = "camera"
    camera_options, device_dependency = _monitor_device_options(registry, current_kind="camera")
    if not camera_options:
        default_kind = "sensor"
        camera_options, device_dependency = _monitor_device_options(registry, current_kind="sensor")
    default_device = _text(camera_options[0].get("value")) if camera_options else ""
    default_trigger_options, trigger_dependency = _monitor_trigger_dependency(
        registry,
        current_device=default_device,
    )
    default_trigger_events = [_text(row.get("value")) for row in default_trigger_options if _text(row.get("value"))]
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
                    "description": "Pick one camera or sensor. Automations and notifications stay in Automation Core.",
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
                    "key": "device",
                    "label": "Which Device?",
                    "type": "select",
                    "presentation": "cards",
                    "options": camera_options,
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
                    "description": "Choose one or more events reported by this device. A motion-only camera will show only Motion.",
                    "full_width": True,
                },
                {
                    "type": "heading",
                    "label": "2. Name The Place",
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
            ],
        },
        "item_forms": [*event_forms, *monitor_forms],
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
    return {
        "summary": "Choose the cameras and sensors Awareness should observe and browse the history it stores.",
        "stats": [
            {"label": "Monitored Sources", "value": len(monitors)},
            {"label": "Active", "value": enabled_count},
            {"label": "Cameras", "value": monitored_cameras},
            {"label": "Sensors", "value": monitored_sensors},
            {"label": "Stored Events", "value": total_count},
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
