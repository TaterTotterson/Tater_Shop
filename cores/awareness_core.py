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
from notify import dispatch_notification, notifier_destination_catalog
from speech_settings import get_speech_settings as get_shared_speech_settings
from speech_tts import speak_announcement_targets
from tateros import integration_store as integration_store_module
from vision_settings import get_vision_settings as get_shared_vision_settings
from announcement_targets import build_announcement_target_options

__version__ = "3.4.7"

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
        "entity_catalog_ttl_sec": {
            "label": "Entity Catalog Cache (sec)",
            "type": "number",
            "default": 30,
            "description": "How long to cache provider entity discovery for UI dropdowns.",
        },
    },
}

CORE_WEBUI_TAB = {
    "label": "Awareness",
    "order": 20,
    "requires_running": True,
}

_RULES_KEY = "awareness:rules"
_EXEC_QUEUE_KEY = "awareness:exec_queue"
_RUNTIME_KEY = "awareness:runtime"
_EVENTS_PREFIX = "tater:automations:events:"
_EVENT_SNAPSHOT_PREFIX = "awareness:event_snapshot:"
_DISPLAY_PROFILE_HASH_KEY = "tater:display:profiles:v1"
_FIRMWARE_PROFILE_HASH_KEY = "tater:esphome:firmware:profiles:v1"
_S3BOX_FIRMWARE_PROFILE_KEY = "template:s3box_display"
_AWARENESS_WORKER_COUNT = 10
_AWARENESS_STARTED_TS = time.time()

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
_AREA_PRESETS = [
    "camera",
    "doorbell",
    "events",
    "front yard",
    "back yard",
    "front door",
    "back door",
    "driveway",
    "garage",
    "living room",
    "kitchen",
    "hallway",
    "office",
    "bedroom",
    "patio",
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
_UNIFI_SMART_TYPE_FALLBACK = ("person", "vehicle", "animal", "package")

_ENTITY_CACHE_LOCK = threading.Lock()
_ENTITY_CACHE: Dict[str, Dict[str, Any]] = {
    "homeassistant": {"ts": 0.0, "data": {}},
    "unifi_protect": {"ts": 0.0, "data": {}},
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
_EVENTS_QUERY_MAX_CANDIDATE_EVENTS_FOR_LLM = 320
_EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER = 180
_INTEGRATION_RUNTIME_EVENTS_KEY = "tater:integration_runtime:events"
_INTEGRATION_RUNTIME_EVENT_SEQ_KEY = "tater:integration_runtime:event_seq"
_INTEGRATION_RUNTIME_STATUS_KEY = "tater:integration_runtime:status"
_INTEGRATION_RUNTIME_STATES_KEY = "tater:integration_runtime:states"
_AWARENESS_RUNTIME_SEQ_KEY = "awareness:integration_runtime:last_seq"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _redis_field_text(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8").strip()
        except Exception:
            return ""
    return _text(value)


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


def _camera_startup_defer_seconds(client: Any) -> int:
    raw_default = _text(os.getenv("TATER_AWARENESS_CAMERA_STARTUP_DEFER_SECONDS") or "60")
    default = _as_int(raw_default, 60, minimum=0, maximum=1800)
    return _setting_int(client, "camera_startup_defer_seconds", default, minimum=0, maximum=1800)


def _camera_startup_defer_remaining(client: Any, rule: Dict[str, Any], reason: str) -> float:
    if _text(reason).lower() == "manual":
        return 0.0
    kind = _text((rule or {}).get("kind")).lower()
    if kind not in {"camera", "doorbell"}:
        return 0.0
    defer_seconds = _camera_startup_defer_seconds(client)
    if defer_seconds <= 0:
        return 0.0
    return max(0.0, (_AWARENESS_STARTED_TS + float(defer_seconds)) - time.time())


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


def _provider_from_rule_fields(rule: Dict[str, Any]) -> str:
    for item in _normalize_trigger_entities((rule or {}).get("trigger_entities")):
        provider = _provider_from_entity_ref(item)
        if provider:
            return provider
    for key in ("sensor_entity", "trigger_entity", "camera_entity"):
        provider = _provider_from_entity_ref((rule or {}).get(key))
        if provider:
            return provider
    return ""


def _provider_from_entity_ref(value: Any) -> str:
    provider, raw_value = _split_provider_ref(value, "")
    if provider != "all" and raw_value:
        return provider
    return ""


def _concrete_rule_provider(rule: Dict[str, Any], *, fallback: str = "") -> str:
    provider = _normalize_event_provider((rule or {}).get("provider"))
    if provider != "all":
        return provider
    inferred = _provider_from_rule_fields(rule)
    if inferred:
        return inferred
    fallback_provider = _normalize_event_provider(fallback)
    return fallback_provider if fallback_provider != "all" else ""


def _rule_provider(rule: Dict[str, Any], *, fallback: str = "") -> str:
    return _concrete_rule_provider(rule, fallback=fallback)


def _snapshot_target_for_camera(camera: Any, rule_provider: Any) -> Tuple[str, str]:
    provider, target = _split_provider_ref(camera, "")
    if provider != "all":
        return provider, target
    camera_target = _text(target or camera)
    inferred = _provider_from_entity_ref(camera_target)
    if inferred:
        return inferred, camera_target
    provider = _normalize_event_provider(rule_provider)
    return "" if provider == "all" else provider, camera_target


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
    try:
        raw_rows = redis_obj.lrange(_INTEGRATION_RUNTIME_EVENTS_KEY, 0, 999) or []
    except Exception:
        logger.debug("[awareness] failed to read integration runtime events", exc_info=True)
        return []
    events: List[Dict[str, Any]] = []
    for raw in raw_rows:
        event = _redis_json_object(raw)
        if not event:
            continue
        seq = _as_int(event.get("seq"), 0, minimum=0)
        if seq <= after_seq:
            continue
        event["seq"] = seq
        events.append(event)
    events.sort(key=lambda item: _as_int(item.get("seq"), 0, minimum=0))
    return events[:max_rows]


def _integration_runtime_state(client: Any, provider: Any, state_id: Any) -> Dict[str, Any]:
    redis_obj = client or redis_client
    provider_token = _normalize_event_provider(provider)
    entity_id = _text(state_id)
    if redis_obj is None or provider_token == "all" or not entity_id:
        return {}
    try:
        record = _redis_json_object(redis_obj.hget(_INTEGRATION_RUNTIME_STATES_KEY, f"{provider_token}:{entity_id}"))
    except Exception:
        logger.debug("[awareness] failed to read integration runtime state for %s:%s", provider_token, entity_id, exc_info=True)
        return {}
    return record or {}


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

    keep: List[str] = []
    for row in raw:
        try:
            payload = json.loads(row)
            ts = _parse_iso(payload.get("ha_time"))
            if ts and ts >= cutoff:
                keep.append(row)
        except Exception:
            continue

    try:
        pipe = redis_obj.pipeline()
        pipe.delete(key)
        if keep:
            pipe.rpush(key, *keep)
        pipe.execute()
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


def _normalize_players(value: Any) -> List[str]:
    raw = _unwrap_form_value(value)
    if isinstance(raw, str):
        token = raw.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                raw = _unwrap_form_value(json.loads(token))
            except Exception:
                try:
                    raw = _unwrap_form_value(ast.literal_eval(token))
                except Exception:
                    raw = token
        elif token.startswith("{") and token.endswith("}"):
            try:
                raw = _unwrap_form_value(json.loads(token))
            except Exception:
                try:
                    raw = _unwrap_form_value(ast.literal_eval(token))
                except Exception:
                    raw = token
    parts: List[str] = []
    if isinstance(raw, (list, tuple, set)):
        for item in raw:
            token = _text(_unwrap_form_value(item))
            if token:
                parts.append(token)
    elif isinstance(raw, dict):
        token = _text(_unwrap_form_value(raw))
        if token and token != _text(raw):
            parts.append(token)
        else:
            for item in raw.values():
                candidate = _text(_unwrap_form_value(item))
                if candidate:
                    parts.append(candidate)
    else:
        text = _text(raw)
        parts = [chunk.strip() for chunk in text.replace(",", "\n").split("\n") if chunk.strip()]
    seen: set[str] = set()
    out: List[str] = []
    for item in parts:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_trigger_entities(value: Any) -> List[str]:
    raw_value = value
    if isinstance(value, str):
        token = value.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                decoded = json.loads(token)
                raw_value = decoded
            except Exception:
                raw_value = value
    out: List[str] = []
    seen: set[str] = set()
    for item in _normalize_players(raw_value):
        token = _text(item)
        if not token:
            continue
        lowered = token.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(token)
    return out


def _normalize_rule(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None

    kind = _text(raw.get("kind")).lower()
    if kind not in {"camera", "doorbell", "entry_sensor"}:
        return None

    rule_id = _text(raw.get("id")) or str(uuid.uuid4())
    now_ts = time.time()

    base: Dict[str, Any] = {
        "id": rule_id,
        "kind": kind,
        "provider": _concrete_rule_provider(raw),
        "name": _text(raw.get("name")) or f"{kind.title()} rule",
        "enabled": _bool(raw.get("enabled"), True),
        "created_at": _as_float(raw.get("created_at"), now_ts),
        "updated_at": _as_float(raw.get("updated_at"), now_ts),
        "last_run_ts": _as_float(raw.get("last_run_ts"), 0.0),
        "last_status": _text(raw.get("last_status")),
        "last_summary": _text(raw.get("last_summary")),
        "last_error": _text(raw.get("last_error")),
    }
    if not _text(base.get("provider")):
        return None

    if kind == "camera":
        trigger_entities = _normalize_trigger_entities(raw.get("trigger_entities"))
        if not trigger_entities:
            trigger_entities = _normalize_trigger_entities(raw.get("trigger_entity"))
        legacy_device_services = _normalize_device_services(raw.get("device_services") or raw.get("device_service"))
        notification_targets = _normalize_notification_targets(
            raw.get("notification_targets") or raw.get("notification_destinations"),
            device_services=legacy_device_services,
        )
        base.update(
            {
                "camera_entity": _text(raw.get("camera_entity")),
                "area": _text(raw.get("area")) or "camera",
                "trigger_entities": trigger_entities,
                "trigger_entity": trigger_entities[0] if trigger_entities else "",
                "trigger_to_state": "on",
                "trigger_attribute": "",
                "trigger_attribute_value": "",
                "query": "",
                "cooldown_seconds": _as_int(raw.get("cooldown_seconds"), 30, minimum=0, maximum=86400),
                "notification_cooldown_seconds": _as_int(
                    raw.get("notification_cooldown_seconds"), 0, minimum=0, maximum=86400
                ),
                "ignore_vehicles": _bool(raw.get("ignore_vehicles"), False),
                "title": _text(raw.get("title") or "Camera Event"),
                "priority": "high"
                if _text(raw.get("priority") or "high").lower() in {"critical", "high"}
                else "normal",
                "device_services": _device_services_from_notification_targets(notification_targets) or legacy_device_services,
                "notification_targets": notification_targets,
                "display_notifications": _bool(raw.get("display_notifications"), False),
                "display_targets": _normalize_display_targets(raw.get("display_targets") or raw.get("display_target")),
            }
        )
        return base

    if kind == "doorbell":
        camera_entity = _text(raw.get("camera_entity"))
        trigger_entities = _normalize_trigger_entities(raw.get("trigger_entities"))
        if not trigger_entities:
            trigger_entities = _normalize_trigger_entities(raw.get("trigger_entity"))
        legacy_device_services = _normalize_device_services(raw.get("device_services") or raw.get("device_service"))
        notification_targets = _normalize_notification_targets(
            raw.get("notification_targets") or raw.get("notification_destinations"),
            device_services=legacy_device_services,
        )
        provider_token = _concrete_rule_provider({**raw, "provider": base.get("provider")})
        if provider_token == "unifi_protect" and camera_entity:
            camera_id = _text(_unifi_camera_id_from_entity(camera_entity)).lower()
            if camera_id:
                expected_trigger = _unifi_camera_doorbell_trigger(camera_id)
                expected_lower = expected_trigger.lower()
                current_lower = [_text(item).lower() for item in trigger_entities]
                if not trigger_entities:
                    trigger_entities = [expected_trigger]
                elif expected_lower not in current_lower:
                    # Legacy UniFi doorbell rules could end up bound to motion/smart triggers.
                    # If the current triggers are camera-scoped UniFi triggers, normalize to the doorbell press trigger.
                    camera_prefix = f"binary_sensor.unifi_{camera_id}_"
                    if current_lower and all(token.startswith(camera_prefix) for token in current_lower):
                        trigger_entities = [expected_trigger]
                    else:
                        trigger_entities = [expected_trigger] + [
                            item for item in trigger_entities if _text(item).lower() != expected_lower
                        ]
        base.update(
            {
                "camera_entity": camera_entity,
                "area": _text(raw.get("area")) or "front door",
                "trigger_entities": trigger_entities,
                "trigger_entity": trigger_entities[0] if trigger_entities else "",
                "trigger_to_state": "on",
                "trigger_attribute": "",
                "trigger_attribute_value": "",
                "players": _normalize_players(raw.get("players")),
                "title": _text(raw.get("title") or "Doorbell"),
                "priority": "high"
                if _text(raw.get("priority") or "normal").lower() in {"critical", "high"}
                else "normal",
                "notifications": _bool(raw.get("notifications"), True),
                "device_services": _device_services_from_notification_targets(notification_targets) or legacy_device_services,
                "notification_targets": notification_targets,
                "display_notifications": _bool(raw.get("display_notifications"), False),
                "display_targets": _normalize_display_targets(raw.get("display_targets") or raw.get("display_target")),
            }
        )
        return base

    if kind == "entry_sensor":
        trigger_entities = _normalize_trigger_entities(raw.get("trigger_entities"))
        sensor_entity = _text(raw.get("sensor_entity"))
        if sensor_entity and sensor_entity not in trigger_entities:
            trigger_entities = [sensor_entity]
        if not trigger_entities:
            trigger_entities = _normalize_trigger_entities(raw.get("trigger_entity"))
        sensor_type = _text(raw.get("sensor_type") or "door").lower()
        if sensor_type not in {"door", "window", "garage"}:
            sensor_type = "door"
        area_value = _text(raw.get("area"))
        legacy_device_services = _normalize_device_services(raw.get("device_services") or raw.get("device_service"))
        notification_targets = _normalize_notification_targets(
            raw.get("notification_targets") or raw.get("notification_destinations"),
            device_services=legacy_device_services,
        )
        base.update(
            {
                "sensor_entity": trigger_entities[0] if trigger_entities else sensor_entity,
                "camera_entity": _text(raw.get("camera_entity")),
                "sensor_type": sensor_type,
                "area": area_value or sensor_type,
                "trigger_entities": trigger_entities,
                "trigger_entity": trigger_entities[0] if trigger_entities else "",
                "trigger_to_state": _text(raw.get("trigger_to_state")),
                "players": _normalize_players(raw.get("players")),
                "title": _text(raw.get("title") or "Entry Sensor"),
                "priority": "high"
                if _text(raw.get("priority") or "normal").lower() in {"critical", "high"}
                else "normal",
                "notifications": _bool(raw.get("notifications"), False),
                "device_services": _device_services_from_notification_targets(notification_targets) or legacy_device_services,
                "notification_targets": notification_targets,
                "display_notifications": _bool(raw.get("display_notifications"), False),
                "display_targets": _normalize_display_targets(raw.get("display_targets") or raw.get("display_target")),
            }
        )
        return base


_RULE_FINGERPRINT_IGNORE_KEYS = {
    "id",
    "created_at",
    "updated_at",
    "last_run_ts",
    "last_status",
    "last_summary",
    "last_error",
}


def _rule_canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _rule_canonical_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        items = [_rule_canonical_value(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True, default=str))
    if isinstance(value, tuple):
        items = [_rule_canonical_value(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True, default=str))
    if isinstance(value, str):
        return value.strip().lower()
    return value


def _rule_fingerprint(rule: Dict[str, Any]) -> str:
    stable = {
        key: value
        for key, value in (rule or {}).items()
        if key not in _RULE_FINGERPRINT_IGNORE_KEYS
    }
    return json.dumps(_rule_canonical_value(stable), sort_keys=True, separators=(",", ":"))


def _rule_migration_score(rule: Dict[str, Any]) -> float:
    return max(
        _as_float((rule or {}).get("updated_at"), 0.0),
        _as_float((rule or {}).get("last_run_ts"), 0.0),
        _as_float((rule or {}).get("created_at"), 0.0),
    )


def _migrate_loaded_rules(
    redis_obj: Any,
    candidates: List[Tuple[str, Dict[str, Any], bool, float]],
    invalid_fields: Optional[set[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    chosen_by_fingerprint: Dict[str, Tuple[str, Dict[str, Any], bool, float]] = {}
    stale_fields: set[str] = set(invalid_fields or set())

    for field_name, normalized, needs_write, source_score in candidates:
        fingerprint = _rule_fingerprint(normalized)
        existing = chosen_by_fingerprint.get(fingerprint)
        if existing is None:
            chosen_by_fingerprint[fingerprint] = (field_name, normalized, needs_write, source_score)
            continue
        existing_field, existing_rule, existing_needs_write, existing_score = existing
        if source_score > existing_score:
            if existing_field:
                stale_fields.add(existing_field)
            chosen_by_fingerprint[fingerprint] = (field_name, normalized, needs_write, source_score)
        else:
            if field_name:
                stale_fields.add(field_name)
            if needs_write and not existing_needs_write:
                chosen_by_fingerprint[fingerprint] = (existing_field, existing_rule, True, existing_score)

    out: Dict[str, Dict[str, Any]] = {}
    fields_to_write: List[Tuple[str, Dict[str, Any]]] = []
    scores_by_id: Dict[str, float] = {}
    for field_name, normalized, needs_write, source_score in chosen_by_fingerprint.values():
        rule_id = _text(normalized.get("id"))
        if not rule_id:
            continue
        existing = out.get(rule_id)
        if existing is not None and scores_by_id.get(rule_id, 0.0) > source_score:
            if field_name:
                stale_fields.add(field_name)
            continue
        out[rule_id] = normalized
        scores_by_id[rule_id] = source_score
        if needs_write or (field_name and field_name != rule_id):
            fields_to_write.append((field_name, normalized))
        if field_name and field_name != rule_id:
            stale_fields.add(field_name)

    if redis_obj is not None:
        wrote = 0
        removed = 0
        for _field_name, normalized in fields_to_write:
            rule_id = _text(normalized.get("id"))
            if not rule_id:
                continue
            try:
                redis_obj.hset(_RULES_KEY, rule_id, json.dumps(normalized))
                wrote += 1
            except Exception:
                logger.debug("[awareness] failed to write migrated rule %s", rule_id, exc_info=True)
        for field_name in sorted(field for field in stale_fields if field and field not in out):
            try:
                removed += int(redis_obj.hdel(_RULES_KEY, field_name) or 0)
            except Exception:
                logger.debug("[awareness] failed to remove stale rule field %s", field_name, exc_info=True)
        if wrote or removed:
            logger.info("[awareness] normalized stored rules (written=%d removed_stale=%d)", wrote, removed)

    return out


def _load_rules(client: Any) -> Dict[str, Dict[str, Any]]:
    redis_obj = client or redis_client
    raw = redis_obj.hgetall(_RULES_KEY) if redis_obj else {}
    if not isinstance(raw, dict):
        return {}
    candidates: List[Tuple[str, Dict[str, Any], bool, float]] = []
    invalid_fields: set[str] = set()
    for field_name_raw, row in raw.items():
        field_name = _redis_field_text(field_name_raw)
        try:
            payload = json.loads(row)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        source_score = _rule_migration_score(payload)
        if not _text(payload.get("id")) and field_name:
            payload["id"] = field_name
        normalized = _normalize_rule(payload)
        if not normalized:
            if field_name:
                invalid_fields.add(field_name)
            continue
        rule_id = _text(normalized.get("id"))
        needs_write = field_name != rule_id
        if not needs_write:
            try:
                needs_write = json.dumps(payload, sort_keys=True) != json.dumps(normalized, sort_keys=True)
            except Exception:
                needs_write = True
        candidates.append((field_name, normalized, needs_write, source_score))
    return _migrate_loaded_rules(redis_obj, candidates, invalid_fields=invalid_fields)


def _get_rule(client: Any, rule_id: str) -> Optional[Dict[str, Any]]:
    rid = _text(rule_id)
    if not rid:
        return None
    redis_obj = client or redis_client
    raw = redis_obj.hget(_RULES_KEY, rid)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return _normalize_rule(payload)


def _save_rule(client: Any, rule: Dict[str, Any]) -> None:
    redis_obj = client or redis_client
    normalized = _normalize_rule(rule)
    if not normalized:
        raise ValueError("Invalid awareness rule payload.")
    redis_obj.hset(_RULES_KEY, normalized["id"], json.dumps(normalized))


def _remove_rule(client: Any, rule_id: str) -> bool:
    redis_obj = client or redis_client
    rid = _text(rule_id)
    if not rid:
        return False
    removed = redis_obj.hdel(_RULES_KEY, rid)
    return bool(removed)


def _queue_depth(client: Any) -> int:
    redis_obj = client or redis_client
    try:
        return int(redis_obj.llen(_EXEC_QUEUE_KEY) or 0)
    except Exception:
        return 0


def _enqueue_execution(client: Any, *, rule_id: str, reason: str, event: Optional[Dict[str, Any]] = None) -> None:
    redis_obj = client or redis_client
    payload = {
        "rule_id": _text(rule_id),
        "reason": _text(reason) or "manual",
        "event": event or {},
        "queued_at": time.time(),
    }
    redis_obj.lpush(_EXEC_QUEUE_KEY, json.dumps(payload))
    _runtime_set(redis_obj, queue_depth=_queue_depth(redis_obj))


def _enqueue_execution_after_delay(
    client: Any,
    *,
    rule_id: str,
    reason: str,
    event: Optional[Dict[str, Any]] = None,
    delay_seconds: float = 0.0,
) -> bool:
    redis_obj = client or redis_client
    delay = max(0.0, float(delay_seconds or 0.0))
    if delay <= 0.0:
        _enqueue_execution(redis_obj, rule_id=rule_id, reason=reason, event=event)
        return True

    rid = _text(rule_id)
    if not rid:
        return False
    dedupe_key = f"awareness:startup_defer:{rid}"
    try:
        if redis_obj.set(dedupe_key, "1", ex=max(30, int(delay) + 60), nx=True) is None:
            return False
    except Exception:
        logger.debug("[awareness] failed to reserve startup defer key %s", dedupe_key, exc_info=True)

    def _later() -> None:
        try:
            _enqueue_execution(redis_obj, rule_id=rid, reason=reason, event=event)
        except Exception:
            logger.debug("[awareness] failed to enqueue deferred execution for %s", rid, exc_info=True)

    timer = threading.Timer(delay, _later)
    timer.daemon = True
    timer.start()
    return True


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


def _ha_config_optional() -> Dict[str, str]:
    try:
        return _ha_config()
    except Exception:
        return {"base": "", "token": ""}


def _unifi_camera_id_from_entity(camera_entity: str) -> str:
    object_id = _entity_object_id(camera_entity)
    if object_id.startswith("unifi_"):
        return object_id[len("unifi_") :]
    return object_id


def _unifi_sensor_id_from_entity(sensor_entity: str) -> str:
    object_id = _entity_object_id(sensor_entity)
    if object_id.startswith("unifi_sensor_"):
        return object_id[len("unifi_sensor_") :]
    if object_id.startswith("unifi_"):
        return object_id[len("unifi_") :]
    return object_id


def _unifi_camera_entity(camera_id: str) -> str:
    return f"camera.unifi_{_text(camera_id).lower()}"


def _unifi_camera_motion_trigger(camera_id: str) -> str:
    return f"binary_sensor.unifi_{_text(camera_id).lower()}_motion"


def _unifi_camera_doorbell_trigger(camera_id: str) -> str:
    return f"binary_sensor.unifi_{_text(camera_id).lower()}_doorbell"


def _unifi_name_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _text(value).lower())


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


def _unifi_smart_type_values(raw: Any) -> List[str]:
    if isinstance(raw, (list, tuple, set)):
        return [_text(item) for item in raw if _text(item)]
    if isinstance(raw, str):
        return _normalize_players(raw)
    return []


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


def _unifi_smart_marker_from_container(raw: Any, smart_type: str, *, depth: int = 0) -> str:
    if depth > 6 or raw is None:
        return ""
    token = _unifi_normalize_smart_type(smart_type)
    if not token:
        return ""
    if isinstance(raw, dict):
        for key, value in raw.items():
            if _unifi_matches_smart_type_text(key, token):
                marker = _unifi_marker_token(value)
                if marker:
                    return f"{_text(key)}:{marker}"
        if _unifi_event_matches_smart_type(raw, token):
            marker = _unifi_marker_token(raw)
            if marker:
                return marker
        for value in raw.values():
            if isinstance(value, (dict, list)):
                marker = _unifi_smart_marker_from_container(value, token, depth=depth + 1)
                if marker:
                    return marker
        return ""
    if isinstance(raw, list):
        for item in raw[:12]:
            marker = _unifi_smart_marker_from_container(item, token, depth=depth + 1)
            if marker:
                return marker
    return ""


def _unifi_camera_recent_smart_types(camera_row: Dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for key in (
        "lastSmartDetectType",
        "lastSmartDetectTypes",
        "smartDetectType",
        "smartDetectTypes",
        "lastDetectionType",
        "lastDetectionTypes",
        "lastObjectType",
        "lastObjectTypes",
    ):
        value = camera_row.get(key)
        if isinstance(value, dict):
            for raw_type in value.keys():
                token = _unifi_normalize_smart_type(raw_type)
                if token:
                    out.add(token)
            continue
        for raw_type in _unifi_smart_type_values(value):
            token = _unifi_normalize_smart_type(raw_type)
            if token:
                out.add(token)
    return out


def _unifi_camera_any_smart_marker(camera_row: Dict[str, Any]) -> str:
    for key in (
        "lastSmartDetectEventId",
        "lastSmartDetectEventIds",
        "lastSmartDetectAt",
        "lastSmartDetect",
        "lastSmartDetectTs",
        "lastDetectionAt",
        "lastDetectionTs",
    ):
        marker = _unifi_marker_token(camera_row.get(key))
        if marker:
            return f"{key}:{marker}"
    for key, value in camera_row.items():
        key_text = _text(key)
        key_lower = key_text.lower()
        if "smart" not in key_lower and "detect" not in key_lower:
            continue
        if key_lower.endswith("types"):
            continue
        marker = _unifi_marker_token(value)
        if marker:
            return f"{key_text}:{marker}"
    return ""


def _unifi_nested_keyword_marker(
    raw: Any,
    *,
    include_any: Tuple[str, ...],
    must_have_any: Tuple[str, ...],
    exclude_any: Tuple[str, ...] = (),
    depth: int = 0,
) -> str:
    if raw is None or depth > 8:
        return ""
    if isinstance(raw, dict):
        for key, value in raw.items():
            key_text = _text(key)
            key_lower = key_text.lower()
            include_ok = any(token in key_lower for token in include_any) if include_any else True
            required_ok = any(token in key_lower for token in must_have_any) if must_have_any else True
            excluded = any(token in key_lower for token in exclude_any)
            if include_ok and required_ok and not excluded:
                marker = _unifi_marker_token(value)
                if marker:
                    return f"{key_text}:{marker}"
            nested = _unifi_nested_keyword_marker(
                value,
                include_any=include_any,
                must_have_any=must_have_any,
                exclude_any=exclude_any,
                depth=depth + 1,
            )
            if nested:
                return f"{key_text}.{nested}"
        return ""
    if isinstance(raw, list):
        for index, item in enumerate(raw[:16]):
            nested = _unifi_nested_keyword_marker(
                item,
                include_any=include_any,
                must_have_any=must_have_any,
                exclude_any=exclude_any,
                depth=depth + 1,
            )
            if nested:
                return f"[{index}].{nested}"
    return ""


def _unifi_camera_smart_detect_types(camera_row: Dict[str, Any]) -> List[str]:
    found: set[str] = set()

    def _add(raw_type: Any) -> None:
        token = _unifi_normalize_smart_type(raw_type)
        if token:
            found.add(token)

    feature_flags = camera_row.get("featureFlags")
    if isinstance(feature_flags, dict):
        for raw_type in _unifi_smart_type_values(feature_flags.get("smartDetectTypes")):
            _add(raw_type)

    for key in ("smartDetectTypes", "smart_detection_types", "smartTypes"):
        value = camera_row.get(key)
        if isinstance(value, dict):
            for raw_type in value.keys():
                _add(raw_type)
            continue
        for raw_type in _unifi_smart_type_values(value):
            _add(raw_type)

    for key in ("lastSmartDetects", "lastSmartDetectEvents", "lastSmartDetectEventIds", "smartDetects", "smartDetectEvents"):
        value = camera_row.get(key)
        if isinstance(value, dict):
            for raw_type in value.keys():
                _add(raw_type)

    has_smart_hint = bool(found)
    if not has_smart_hint and isinstance(feature_flags, dict):
        has_smart_hint = bool(feature_flags.get("hasSmartDetect"))
    if not has_smart_hint:
        has_smart_hint = any("smartdetect" in _text(key).lower() for key in camera_row.keys())
    if has_smart_hint and not found:
        found.update(_UNIFI_SMART_TYPE_FALLBACK)
    return sorted(found)


def _unifi_camera_smart_trigger(camera_id: str, smart_type: str) -> str:
    token = _unifi_normalize_smart_type(smart_type) or "smart_detect"
    return f"binary_sensor.unifi_{_text(camera_id).lower()}_smart_{token}"


def _unifi_camera_smart_marker(camera_row: Dict[str, Any], smart_type: str) -> str:
    token = _unifi_normalize_smart_type(smart_type)
    if not token:
        return ""
    for key in ("lastSmartDetects", "lastSmartDetectEvents", "lastSmartDetectEventIds", "smartDetects", "smartDetectEvents"):
        marker = _unifi_smart_marker_from_container(camera_row.get(key), token)
        if marker:
            return f"{key}:{marker}"
    for key, value in camera_row.items():
        key_text = _text(key)
        key_lower = key_text.lower()
        if "smart" not in key_lower and "detect" not in key_lower:
            continue
        if key_lower.endswith("types"):
            continue
        # HA's UniFi integration exposes fields like last_person_detect_event,
        # last_vehicle_detect_event, etc. Use the key itself as a type hint.
        if _unifi_matches_smart_type_text(key_text, token):
            direct_marker = _unifi_marker_token(value)
            if direct_marker:
                return f"{key_text}:{direct_marker}"
        marker = _unifi_smart_marker_from_container(value, token)
        if marker:
            return f"{key_text}:{marker}"
    recent_types = _unifi_camera_recent_smart_types(camera_row)
    if token in recent_types:
        generic_marker = _unifi_camera_any_smart_marker(camera_row)
        if generic_marker:
            return f"recent:{token}:{generic_marker}"
    nested_marker = _unifi_smart_marker_from_container(camera_row, token)
    if nested_marker:
        return f"nested:{nested_marker}"
    return ""


def _unifi_camera_name_index() -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        catalog = _entity_catalog(provider="unifi_protect")
    except Exception:
        return out
    cameras = catalog.get("cameras") if isinstance(catalog.get("cameras"), list) else []
    for camera_entity, label in cameras:
        camera_id = _text(_unifi_camera_id_from_entity(camera_entity)).lower()
        if not camera_id:
            continue
        label_text = _text(label)
        camera_name = label_text.rsplit("(", 1)[0].strip() if "(" in label_text else label_text
        candidates = [
            camera_name,
            _entity_object_id(camera_entity).replace("unifi_", "").replace("_", " "),
            camera_id,
        ]
        for candidate in candidates:
            key = _unifi_name_key(candidate)
            if key and key not in out:
                out[key] = camera_id
    return out


def _unifi_sensor_entity(sensor_id: str) -> str:
    return f"binary_sensor.unifi_sensor_{_text(sensor_id).lower()}"


def _unifi_payload_rows(payload: Any, key: str = "") -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    keys = [token for token in (key, "data", "items", "results", "devices") if token]
    for item_key in keys:
        value = payload.get(item_key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            return [row for row in value.values() if isinstance(row, dict)]
    if key:
        singular = key[:-1] if key.endswith("s") else key
        value = payload.get(singular)
        if isinstance(value, dict):
            return [value]
    return [payload] if _text(payload.get("id")) else []


def _unifi_list_from_integration(resource: str, paths: List[str]) -> List[Dict[str, Any]]:
    module = _integration_module("unifi_protect")
    if module is None:
        return []
    public_func = getattr(module, f"list_unifi_{resource}", None)
    if callable(public_func):
        return _unifi_payload_rows(public_func(), resource)
    helper = getattr(module, "_list_unifi_rows", None)
    if callable(helper):
        return _unifi_payload_rows(helper(resource, paths), resource)
    return []


def _unifi_list_resource(resource: str, paths: List[str]) -> List[Dict[str, Any]]:
    try:
        rows = _unifi_list_from_integration(resource, paths)
        if rows:
            return rows
    except Exception:
        logger.debug("[awareness] UniFi %s discovery via integration module failed", resource, exc_info=True)
    for path in paths:
        try:
            rows = _unifi_payload_rows(_unifi_request("GET", path), resource)
            if rows:
                return rows
        except Exception:
            logger.debug("[awareness] UniFi %s discovery failed path=%s", resource, path, exc_info=True)
    return []


def _unifi_list_cameras() -> List[Dict[str, Any]]:
    return _unifi_list_resource(
        "cameras",
        ["/proxy/protect/integration/v1/cameras", "/proxy/protect/api/cameras", "/proxy/protect/api/bootstrap"],
    )


def _unifi_list_sensors() -> List[Dict[str, Any]]:
    return _unifi_list_resource(
        "sensors",
        ["/proxy/protect/integration/v1/sensors", "/proxy/protect/api/sensors", "/proxy/protect/api/bootstrap"],
    )


def _ha_headers(token: str, *, json_content: bool = True) -> Dict[str, str]:
    headers = {"Authorization": f"Bearer {token}"}
    if json_content:
        headers["Content-Type"] = "application/json"
    return headers


def _state_matches_custom(rule: Dict[str, Any], new_state: Dict[str, Any], old_state: Dict[str, Any]) -> bool:
    expected_state = _text(rule.get("trigger_to_state")).lower()
    actual_state = _text((new_state or {}).get("state")).lower()
    if expected_state and actual_state != expected_state:
        return False
    if not expected_state and _text((old_state or {}).get("state")).lower() == actual_state:
        return False
    attr_key = _text(rule.get("trigger_attribute"))
    attr_expected = _text(rule.get("trigger_attribute_value")).lower()
    if not attr_key:
        return True
    attrs = (new_state or {}).get("attributes") or {}
    if not isinstance(attrs, dict):
        attrs = {}
    attr_value = attrs.get(attr_key)
    if isinstance(attr_value, list):
        tokens = [_text(item).lower() for item in attr_value]
        if not attr_expected:
            return bool(tokens)
        return any(attr_expected == token or attr_expected in token for token in tokens)
    attr_text = _text(attr_value).lower()
    if not attr_expected:
        return bool(attr_text)
    return attr_expected == attr_text or attr_expected in attr_text


def _rule_matches_event(
    rule: Dict[str, Any],
    *,
    provider: str,
    entity_id: str,
    new_state: Dict[str, Any],
    old_state: Dict[str, Any],
) -> bool:
    provider_token = _normalize_event_provider(provider)
    rule_provider = _rule_provider(rule)
    trigger_entities = _normalize_trigger_entities(rule.get("trigger_entities"))
    if not trigger_entities:
        legacy = _text(rule.get("trigger_entity"))
        if legacy:
            trigger_entities = [legacy]
    event_entity = _text(entity_id).lower()
    if trigger_entities:
        matched_entity = False
        for trigger_entity in trigger_entities:
            trigger_provider, trigger_raw = _split_provider_ref(trigger_entity, rule_provider)
            if trigger_provider not in {"all", provider_token}:
                continue
            if _text(trigger_raw).lower() == event_entity:
                matched_entity = True
                break
        if not matched_entity:
            return False
    elif rule_provider not in {"all", provider_token}:
        return False
    return _state_matches_custom(rule, new_state, old_state)


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


def _camera_cooldown_key(camera_entity: str) -> str:
    return f"tater:camera_event:cooldown:v2:{camera_entity}"


def _camera_notify_cooldown_key(camera_entity: str) -> str:
    return f"tater:camera_event:notify_cooldown:v2:{camera_entity}"


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


def _normalize_device_service(raw: Any) -> str:
    text = _text(raw)
    if not text:
        return ""
    if text.lower().startswith("notify."):
        return text.split(".", 1)[1].strip()
    if "." in text:
        left, right = text.split(".", 1)
        if left.strip().lower() == "notify":
            return right.strip()
    return text


def _normalize_device_services(raw: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in _normalize_players(raw):
        token = _normalize_device_service(item)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _notification_clean_targets_dict(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Any] = {}
    for key, value in raw.items():
        token = _text(key)
        if not token:
            continue
        if isinstance(value, bool):
            out[token] = bool(value)
            continue
        text = _text(value)
        if text:
            out[token] = text
    return out


def _notification_target_entry(platform: Any, targets: Any = None) -> Dict[str, Any]:
    platform_name = _text(platform).lower()
    if not platform_name:
        return {}
    return {
        "platform": platform_name,
        "targets": _notification_clean_targets_dict(targets),
    }


def _notification_encode_destination(platform: Any, targets: Any = None) -> str:
    entry = _notification_target_entry(platform, targets)
    if not entry:
        return ""
    try:
        return json.dumps(entry, separators=(",", ":"), sort_keys=True)
    except Exception:
        return ""


def _notification_decode_destination(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        entry = _notification_target_entry(raw.get("platform"), raw.get("targets"))
        if entry:
            return entry
        service = _normalize_device_service(raw.get("device_service"))
        return _notification_target_entry("homeassistant", {"device_service": service}) if service else {}
    value = _text(raw)
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        service = _normalize_device_service(value)
        return _notification_target_entry("homeassistant", {"device_service": service}) if service else {}
    if not isinstance(parsed, dict):
        return {}
    return _notification_target_entry(parsed.get("platform"), parsed.get("targets"))


def _normalize_notification_targets(raw: Any, *, device_services: Any = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    raw_items: List[Any]
    if isinstance(raw, (list, tuple, set)):
        raw_items = list(raw)
    elif isinstance(raw, dict):
        raw_items = [raw]
    elif raw in (None, ""):
        raw_items = []
    else:
        raw_text = _text(raw)
        raw_items = [raw_text] if raw_text.startswith("{") and raw_text.endswith("}") else _normalize_players(raw)

    for item in raw_items:
        entry = _notification_decode_destination(item)
        value = _notification_encode_destination(entry.get("platform"), entry.get("targets"))
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(entry)

    if not out:
        for service in _normalize_device_services(device_services):
            entry = _notification_target_entry("homeassistant", {"device_service": service})
            value = _notification_encode_destination(entry.get("platform"), entry.get("targets"))
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(entry)
    return out


def _notification_target_values(raw: Any, *, device_services: Any = None) -> List[str]:
    out: List[str] = []
    for entry in _normalize_notification_targets(raw, device_services=device_services):
        value = _notification_encode_destination(entry.get("platform"), entry.get("targets"))
        if value:
            out.append(value)
    return out


def _device_services_from_notification_targets(raw: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for entry in _normalize_notification_targets(raw):
        if _text(entry.get("platform")).lower() != "homeassistant":
            continue
        service = _normalize_device_service((entry.get("targets") or {}).get("device_service"))
        if not service or service in seen:
            continue
        seen.add(service)
        out.append(service)
    return out


def _normalize_display_target(raw: Any) -> str:
    token = _text(raw)
    if not token:
        return ""
    clean = re.sub(r"[^A-Za-z0-9:._,@-]+", "_", token).strip("._,")
    return clean[:120] or ""


def _normalize_display_targets(raw: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in _normalize_players(raw):
        token = _normalize_display_target(item)
        if not token or token in seen:
            continue
        if token.lower() == "all":
            return ["all"]
        seen.add(token)
        out.append(token)
    return out


def _display_target_pairs(current_values: Any = None) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = [("all", "All Tater displays")]
    seen: set[str] = {"all"}

    def add_target(value: Any, label: Any = "") -> None:
        token = _normalize_display_target(value)
        if not token or token.lower() in seen:
            return
        seen.add(token.lower())
        pairs.append((token, _text(label) or token))

    try:
        profile_rows = redis_client.hgetall(_DISPLAY_PROFILE_HASH_KEY) or {}
    except Exception:
        profile_rows = {}
    if isinstance(profile_rows, dict):
        for raw_key, raw_value in profile_rows.items():
            key = _text(raw_key)
            label = key
            try:
                profile = json.loads(_text(raw_value))
                if isinstance(profile, dict):
                    label = _text(profile.get("name") or profile.get("friendly_name") or profile.get("display_name") or key)
                    add_target(profile.get("display_target") or key, label)
                    continue
            except Exception:
                pass
            add_target(key, label)

    try:
        raw_profile = redis_client.hget(_FIRMWARE_PROFILE_HASH_KEY, _S3BOX_FIRMWARE_PROFILE_KEY)
        firmware_profile = json.loads(_text(raw_profile)) if raw_profile not in (None, "") else {}
    except Exception:
        firmware_profile = {}
    if isinstance(firmware_profile, dict):
        add_target(
            firmware_profile.get("display_target"),
            firmware_profile.get("friendly_name") or firmware_profile.get("display_name") or firmware_profile.get("display_target"),
        )

    for current in _normalize_display_targets(current_values):
        add_target(current, f"{current} (current)")
    return pairs


def _display_target_options(current_values: Any = None) -> List[Dict[str, str]]:
    return _multiselect_choices_from_pairs(_display_target_pairs(current_values), current_values=current_values)


def _display_snapshot_url(snapshot_store: Any) -> str:
    row = snapshot_store if isinstance(snapshot_store, dict) else {}
    snapshot_id = _text(row.get("snapshot_id"))
    if not snapshot_id:
        return ""
    return f"/tater-ha/v1/display/snapshots/{snapshot_id}"


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


async def _notify_homeassistant(
    *,
    title: str,
    message: str,
    priority: str,
    device_services: Any,
    origin: Dict[str, Any],
) -> Dict[str, Any]:
    return await _notify_destinations(
        title=title,
        message=message,
        priority=priority,
        notification_targets=[],
        device_services=device_services,
        origin=origin,
    )


async def _notify_destinations(
    *,
    title: str,
    message: str,
    priority: str,
    notification_targets: Any,
    device_services: Any = None,
    origin: Dict[str, Any],
) -> Dict[str, Any]:
    targets_list = _normalize_notification_targets(notification_targets, device_services=device_services)
    if not targets_list:
        return {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}
    meta = {"priority": "high" if _text(priority).lower() in {"high", "critical"} else "normal"}
    sent_count = 0
    errors: List[str] = []

    async def _dispatch_once(platform: str, targets: Dict[str, Any]) -> None:
        nonlocal sent_count
        try:
            result = await dispatch_notification(
                platform=platform,
                title=title,
                content=message,
                targets=targets,
                origin=origin,
                meta=meta,
            )
        except Exception as exc:
            errors.append(str(exc))
            return
        result_text = _text(result)
        if result_text.lower().startswith("queued notification"):
            sent_count += 1
            return
        errors.append(result_text or f"{platform} notifier returned empty result")

    # Keep persistent notifications off in Awareness routing; this path only
    # targets explicit Home Assistant notify services selected in the rule.
    for entry in targets_list:
        platform = _text(entry.get("platform")).lower()
        if not platform:
            continue
        targets = dict(entry.get("targets") or {})
        if platform == "homeassistant":
            targets.setdefault("persistent", False)
        await _dispatch_once(platform, targets)

    if sent_count > 0:
        result: Dict[str, Any] = {"ok": True, "sent_count": sent_count}
        if errors:
            result["warnings"] = errors
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(errors) or "notification dispatch failed"}


async def _notify_display(
    *,
    kind: str,
    title: str,
    message: str,
    priority: str,
    display_targets: Any,
    snapshot_store: Any,
    origin: Dict[str, Any],
    ttl_seconds: int = 90,
) -> Dict[str, Any]:
    targets = _normalize_display_targets(display_targets)
    if not targets:
        targets = ["all"]
    snapshot_url = _display_snapshot_url(snapshot_store)
    normalized_kind = _text(kind) or "notification"
    meta = {
        "display_kind": normalized_kind,
        "priority": "high" if _text(priority).lower() in {"high", "critical"} else "normal",
        "description": message,
        "summary": message,
        "snapshot_url": snapshot_url,
        "image_format": "jpeg" if snapshot_url else "",
        "ttl_seconds": _as_int(ttl_seconds, 90, minimum=6, maximum=3600),
    }
    try:
        result = await dispatch_notification(
            platform="display",
            title=title,
            content=message,
            targets={"target": targets[0] if len(targets) == 1 else "all", "targets": targets},
            origin=origin,
            meta=meta,
        )
    except Exception as exc:
        return {
            "ok": False,
            "sent_count": 0,
            "error": str(exc),
            "snapshot_url": snapshot_url,
            "targets": targets,
        }
    result_text = _text(result)
    ok = result_text.lower().startswith("queued notification")
    return {
        "ok": ok,
        "sent_count": 1 if ok else 0,
        "result": result_text,
        "snapshot_url": snapshot_url,
        "targets": targets,
    }


def _event_window(timeframe: str) -> Tuple[datetime, datetime, str]:
    now = datetime.now()
    token = _text(timeframe).lower()
    if token == "yesterday":
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1) - timedelta(seconds=1)
        return start, end, "yesterday"
    if token in {"last_24h", "last24h"}:
        return now - timedelta(hours=24), now, "in the last 24 hours"
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return start, end, "today"

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


def _events_query_compact_event_for_llm(event: Dict[str, Any]) -> Dict[str, Any]:
    source = _text(event.get("source"))
    data_payload = event.get("data") if isinstance(event.get("data"), dict) else {}
    area = _events_query_source_to_area(source) or _text(data_payload.get("area"))
    return {
        "event_id": _events_query_event_id(event),
        "source": source,
        "area": area,
        "ha_time": _text(event.get("ha_time")),
        "title": _text(event.get("title")),
        "message": _text(event.get("message")),
        "type": _text(event.get("type")),
        "entity_id": _text(event.get("entity_id")),
        "level": _text(event.get("level")),
        "data": data_payload,
    }


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


def _events_query_event_ids(candidate_events: List[Dict[str, Any]]) -> List[str]:
    return [
        str(item.get("event_id") or "").strip()
        for item in candidate_events
        if isinstance(item, dict) and str(item.get("event_id") or "").strip()
    ]


async def _events_query_llm_json_object(
    *,
    llm_client: Any,
    system_prompt: str,
    user_payload: Dict[str, Any],
    max_tokens: int = 700,
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
            max_tokens=max(80, int(max_tokens or 700)),
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
        max_tokens=800,
        temperature=0.0,
    )


def _events_query_normalize_interpretation(
    *,
    interpretation: Dict[str, Any],
    sources_catalog: List[str],
    now_local: datetime,
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
    if bool(interpretation.get("broad_summary")):
        return _events_query_event_ids(candidate_events), ""

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
        max_tokens=1600,
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
) -> Tuple[Optional[str], str]:
    if llm_client is None:
        return None, "LLM client is unavailable."
    system_prompt = (
        "You answer a homeowner's event-history question using only provided events.\n"
        "Rules:\n"
        "- Base the answer only on relevant_events.\n"
        "- If evidence is missing, say so clearly and do not guess.\n"
        "- Be concise and conversational.\n"
        "- Mention area/time naturally when useful.\n"
        "- For count questions, provide the count from evidence.\n"
        "- For presence questions, answer yes/no with evidence confidence from data.\n"
        "- Do not mention internal tools or prompts.\n"
    )
    payload = {
        "user_query": user_query,
        "interpreted_request": {
            "query_type": interpretation.get("query_type"),
            "response_mode": interpretation.get("response_mode"),
            "time_label": interpretation.get("time_label"),
            "semantic_focus": interpretation.get("semantic_focus"),
            "sources": interpretation.get("selected_sources"),
        },
        "candidate_event_count": int(candidate_count),
        "relevant_event_count": int(len(relevant_events)),
        "relevant_events": relevant_events,
    }
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0.15,
            max_tokens=420,
            timeout_ms=45_000,
        )
    except Exception as exc:
        return None, f"Final answer generation failed: {exc}"

    text = _text((response.get("message") or {}).get("content"))
    if not text:
        return None, "Final answer generation returned empty output."
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
    compact_events = [_events_query_compact_event_for_llm(item) for item in fetched_sorted]
    if len(compact_events) > _EVENTS_QUERY_MAX_CANDIDATE_EVENTS_FOR_LLM:
        compact_events = compact_events[-_EVENTS_QUERY_MAX_CANDIDATE_EVENTS_FOR_LLM:]
    logger.info(
        "[awareness] events_query fetched_events=%s candidate_events=%s",
        len(fetched_sorted),
        len(compact_events),
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

    if not relevant_ids and compact_events and bool(interpreted.get("broad_summary")):
        relevant_ids = [
            str(item.get("event_id") or "").strip()
            for item in compact_events
            if str(item.get("event_id") or "").strip()
        ]
        logger.info(
            "[awareness] events_query relevance returned empty for broad summary; using all candidate events (%s).",
            len(relevant_ids),
        )

    event_by_id = {str(item.get("event_id") or ""): item for item in compact_events}
    relevant_events = [event_by_id[event_id] for event_id in relevant_ids if event_id in event_by_id]
    if len(relevant_events) > _EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER:
        relevant_events = relevant_events[-_EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER:]
    logger.info(
        "[awareness] events_query relevant_event_ids=%s relevant_events=%s",
        len(relevant_ids),
        len(relevant_events),
    )

    final_text, final_err = await _events_query_compose_final_answer(
        llm_client=llm_client,
        user_query=query,
        interpretation=interpreted,
        relevant_events=relevant_events,
        candidate_count=len(compact_events),
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
        "candidate_event_count": int(len(compact_events)),
        "relevant_event_count": int(len(relevant_events)),
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


def _event_filter_form(
    *,
    filters: Dict[str, bool],
    list_view: bool,
    totals: Dict[str, int],
    visible_totals: Dict[str, int],
) -> Dict[str, Any]:
    labels = [("camera", "Cameras"), ("doorbell", "Doorbells"), ("sensor", "Sensors")]
    selected_labels = [label for key, label in labels if filters.get(key)]
    if selected_labels:
        subtitle = f"Showing: {', '.join(selected_labels)}"
    else:
        subtitle = "No event types selected. Enable at least one type to view matching events."
    subtitle += f" • View: {'List' if list_view else 'Current'}"
    subtitle += (
        f" • Visible {visible_totals.get('camera', 0)}/{totals.get('camera', 0)} cameras, "
        f"{visible_totals.get('doorbell', 0)}/{totals.get('doorbell', 0)} doorbells, "
        f"{visible_totals.get('sensor', 0)}/{totals.get('sensor', 0)} sensors"
    )
    return {
        "id": "awareness_event_filters",
        "group": "event",
        "title": "Event Filters",
        "subtitle": subtitle,
        "save_action": "awareness_save_event_filters",
        "save_label": "Apply Filters",
        "fields_popup": False,
        "fields_dropdown": True,
        "sections_in_dropdown": False,
        "fields": [
            {
                "key": "show_camera_events",
                "label": "Show Cameras",
                "type": "checkbox",
                "value": bool(filters.get("camera", True)),
            },
            {
                "key": "show_doorbell_events",
                "label": "Show Doorbells",
                "type": "checkbox",
                "value": bool(filters.get("doorbell", True)),
            },
            {
                "key": "show_sensor_events",
                "label": "Show Sensors",
                "type": "checkbox",
                "value": bool(filters.get("sensor", True)),
            },
            {
                "key": "show_event_list_view",
                "label": "List View (compact)",
                "type": "checkbox",
                "value": bool(list_view),
            },
        ],
    }


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
        if description and list_view:
            subtitle_parts.append(f"Summary: {_compact(description, limit=120)}")
        subtitle = " • ".join([part for part in subtitle_parts if part])

        fields: List[Dict[str, Any]] = []
        snapshot = _event_snapshot_preview(client, event)
        snapshot_id = _text(snapshot.get("snapshot_id"))
        if list_view and snapshot.get("data_url"):
            fields.append(
                {
                    "key": f"snapshot_thumb_{idx}",
                    "label": "Thumbnail",
                    "type": "image",
                    "src": _text(snapshot.get("data_url")),
                    "alt": f"{title} thumbnail",
                    "hide_label": True,
                    "display": "thumbnail",
                    "max_width": 160,
                    "max_height": 90,
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
        elif snapshot_id:
            fields.append(
                {
                    "key": f"snapshot_status_{idx}",
                    "label": "Snapshot",
                    "type": "text" if not list_view else "textarea",
                    "value": (
                        f"Stored snapshot unavailable ({snapshot_id})"
                        if not list_view
                        else f"Snapshot stored: {snapshot_id}"
                    ),
                    "read_only": True,
                    "hide_label": bool(list_view),
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
                "group": "event",
                "title": title,
                "subtitle": subtitle,
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


def _event_items_for_ui(client: Any) -> List[Dict[str, Any]]:
    return list(_event_page_for_ui(client).get("items") or [])


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

def _shared_announcement_tts_settings() -> Dict[str, Any]:
    shared = get_shared_speech_settings()
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


async def _execute_camera_rule(rule: Dict[str, Any], llm_client: Any, reason: str, event: Dict[str, Any]) -> Dict[str, Any]:
    del llm_client
    camera = _text(rule.get("camera_entity"))
    area = _text(rule.get("area")) or "camera"
    provider = _rule_provider(rule)
    snapshot_provider, camera_target = _snapshot_target_for_camera(camera, provider)
    provider = snapshot_provider
    if not camera:
        raise ValueError("Camera rule is missing camera_entity.")

    cooldown_seconds = _as_int(rule.get("cooldown_seconds"), 30, minimum=0, maximum=86400)
    cooldown_key = _camera_cooldown_key(camera)
    if not _acquire_cooldown(cooldown_key, cooldown_seconds):
        return {
            "ok": True,
            "summary": "Camera cooldown active.",
            "camera": camera,
            "area": area,
            "provider": provider,
            "skipped": "cooldown",
        }

    vision = get_shared_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    query = _text(rule.get("query"))
    ignore_vehicles = _bool(rule.get("ignore_vehicles"), False)

    try:
        jpeg, snapshot_content_type = await _capture_camera_snapshot(snapshot_provider, camera_target)
        summary = await _vision_describe(
            image_bytes=jpeg,
            api_base=_text(vision.get("api_base")),
            model=_text(vision.get("model")),
            api_key=_text(vision.get("api_key")),
            query=query,
            ignore_vehicles=ignore_vehicles,
            mode="camera",
            vision_mode=_text(vision.get("mode")),
            vision_provider=_text(vision.get("provider")),
        )
        summary = _compact(summary, limit=180) or "Nothing notable."
    except Exception:
        _clear_cooldown(cooldown_key)
        logger.exception("[awareness] camera snapshot/vision failed for %s", camera)
        raise

    if _is_nothing_notable_summary(summary):
        _clear_cooldown(cooldown_key)
        return {
            "ok": True,
            "summary": summary,
            "camera": camera,
            "area": area,
            "provider": provider,
            "skipped": "nothing_notable",
        }

    snapshot_store = _store_event_snapshot(redis_client, jpeg, content_type=snapshot_content_type)
    trigger_entity = _text(event.get("entity_id"))
    event_payload = {
        "source": _slug(area),
        "title": _text(rule.get("title") or "Camera Event"),
        "type": "camera_event",
        "message": summary,
        "entity_id": camera,
        "ha_time": _now_iso(),
        "level": "info",
        "data": {
            "area": area,
            "reason": reason,
            "trigger_entity": trigger_entity,
            "provider": provider,
            "query": query,
            "ignore_vehicles": ignore_vehicles,
        },
    }
    if snapshot_store.get("stored"):
        event_payload["snapshot_id"] = _text(snapshot_store.get("snapshot_id"))
        event_payload["data"]["snapshot_id"] = _text(snapshot_store.get("snapshot_id"))
        event_payload["data"]["snapshot_content_type"] = _text(snapshot_store.get("content_type") or "image/jpeg")
        event_payload["data"]["snapshot_bytes"] = _as_int(snapshot_store.get("bytes"), 0, minimum=0)
    elif snapshot_store.get("reason"):
        event_payload["data"]["snapshot_status"] = _text(snapshot_store.get("reason"))
        event_payload["data"]["snapshot_bytes"] = _as_int(snapshot_store.get("bytes"), 0, minimum=0)
    _append_event(redis_client, source=area, payload=event_payload)

    notify_result: Dict[str, Any] = {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}
    display_result: Dict[str, Any] = {"ok": True, "sent_count": 0, "skipped": "display_notifications_disabled"}
    notification_targets = _normalize_notification_targets(
        rule.get("notification_targets") or rule.get("notification_destinations"),
        device_services=rule.get("device_services") or rule.get("device_service"),
    )
    display_enabled = _bool(rule.get("display_notifications"), False)
    display_targets = _normalize_display_targets(rule.get("display_targets") or rule.get("display_target"))
    notification_cooldown_seconds = _as_int(rule.get("notification_cooldown_seconds"), 0, minimum=0, maximum=86400)
    if notification_targets or display_enabled:
        notify_key = _camera_notify_cooldown_key(camera)
        if notification_cooldown_seconds > 0 and not _acquire_cooldown(notify_key, notification_cooldown_seconds):
            notify_result = {"ok": True, "sent_count": 0, "skipped": "notification_cooldown"}
            display_result = {"ok": True, "sent_count": 0, "skipped": "notification_cooldown"}
        else:
            origin = {
                "platform": "awareness_core",
                "scope": "camera_rule",
                "rule_id": rule.get("id"),
                "camera": camera,
                "area": area,
                "provider": provider,
                "trigger_entity": trigger_entity,
            }
            if notification_targets:
                notify_result = await _notify_destinations(
                    title=_text(rule.get("title") or "Camera Event"),
                    message=summary,
                    priority=_text(rule.get("priority") or "high"),
                    notification_targets=notification_targets,
                    origin=origin,
                )
            if display_enabled:
                display_result = await _notify_display(
                    kind="camera",
                    title=_text(rule.get("title") or "Camera Event"),
                    message=summary,
                    priority=_text(rule.get("priority") or "high"),
                    display_targets=display_targets,
                    snapshot_store=snapshot_store,
                    origin=origin,
                    ttl_seconds=90,
                )
            if notification_cooldown_seconds > 0:
                attempted_results = []
                if notification_targets:
                    attempted_results.append(notify_result)
                if display_enabled:
                    attempted_results.append(display_result)
                if attempted_results and not any(row.get("ok") for row in attempted_results):
                    _clear_cooldown(notify_key)

    return {
        "ok": True,
        "summary": summary,
        "camera": camera,
        "area": area,
        "provider": provider,
        "notification": notify_result,
        "display_notification": display_result,
    }


async def _execute_doorbell_rule(rule: Dict[str, Any], llm_client: Any, reason: str, event: Dict[str, Any]) -> Dict[str, Any]:
    del llm_client
    camera = _text(rule.get("camera_entity"))
    area = _text(rule.get("area")) or "front door"
    provider = _rule_provider(rule)
    snapshot_provider, camera_target = _snapshot_target_for_camera(camera, provider)
    provider = snapshot_provider
    if not camera:
        raise ValueError("Doorbell rule is missing camera_entity.")
    vision = get_shared_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    players = _normalize_players(rule.get("players"))
    ha_for_playback = _ha_config_optional()
    shared_tts = _shared_announcement_tts_settings()
    tts_backend = _text(shared_tts.get("backend") or "wyoming")
    tts_model = _text(shared_tts.get("model"))
    tts_voice = _text(shared_tts.get("voice"))
    jpeg: bytes = b""
    snapshot_content_type = "image/jpeg"
    try:
        jpeg, snapshot_content_type = await _capture_camera_snapshot(snapshot_provider, camera_target)
        spoken_line = await _vision_describe(
            image_bytes=jpeg,
            api_base=_text(vision.get("api_base")),
            model=_text(vision.get("model")),
            api_key=_text(vision.get("api_key")),
            query="doorbell alert",
            ignore_vehicles=False,
            mode="doorbell",
            vision_mode=_text(vision.get("mode")),
            vision_provider=_text(vision.get("provider")),
        )
        spoken_line = _compact(spoken_line, limit=180) or "Someone is at the door."
    except Exception:
        logger.exception("[awareness] doorbell snapshot/vision failed for %s", camera)
        spoken_line = "Someone is at the door."
    snapshot_store = _store_event_snapshot(redis_client, jpeg, content_type=snapshot_content_type)
    tts_result: Dict[str, Any] = {"ok": True, "sent_count": 0, "skipped": "no_players", "backend": tts_backend}
    if players:
        try:
            tts_result = await speak_announcement_targets(
                text=spoken_line,
                backend=tts_backend,
                ha_base=_text(ha_for_playback.get("base")),
                token=_text(ha_for_playback.get("token")),
                targets=players,
                model=tts_model,
                voice=tts_voice,
                wyoming_host=_text(shared_tts.get("wyoming_host")),
                wyoming_port=shared_tts.get("wyoming_port"),
                wyoming_voice=_text(shared_tts.get("wyoming_voice")),
                voice_core_backend=_text(shared_tts.get("voice_core_backend")),
                voice_core_model=_text(shared_tts.get("voice_core_model")),
                voice_core_voice=_text(shared_tts.get("voice_core_voice")),
                voice_core_wyoming_host=_text(shared_tts.get("voice_core_wyoming_host")),
                voice_core_wyoming_port=shared_tts.get("voice_core_wyoming_port"),
                voice_core_wyoming_voice=_text(shared_tts.get("voice_core_wyoming_voice")),
                default_backend=tts_backend,
            )
        except Exception as exc:
            logger.warning("[awareness] doorbell TTS failed for %s: %s", camera, exc)
            tts_result = {"ok": False, "sent_count": 0, "error": str(exc), "backend": tts_backend}
    event_payload = {
        "source": _slug(area),
        "title": "Doorbell",
        "type": "doorbell",
        "message": spoken_line,
        "entity_id": camera,
        "ha_time": _now_iso(),
        "level": "info",
        "data": {
            "area": area,
            "players": players,
            "tts_backend": tts_backend,
            "tts_model": tts_model,
            "tts_voice": tts_voice,
            "reason": reason,
            "trigger_entity": _text(event.get("entity_id")),
            "provider": provider,
        },
    }
    if snapshot_store.get("stored"):
        event_payload["snapshot_id"] = _text(snapshot_store.get("snapshot_id"))
        event_payload["data"]["snapshot_id"] = _text(snapshot_store.get("snapshot_id"))
        event_payload["data"]["snapshot_content_type"] = _text(snapshot_store.get("content_type") or "image/jpeg")
        event_payload["data"]["snapshot_bytes"] = _as_int(snapshot_store.get("bytes"), 0, minimum=0)
    elif snapshot_store.get("reason"):
        event_payload["data"]["snapshot_status"] = _text(snapshot_store.get("reason"))
        event_payload["data"]["snapshot_bytes"] = _as_int(snapshot_store.get("bytes"), 0, minimum=0)
    _append_event(redis_client, source=area, payload=event_payload)
    origin = {
        "platform": "awareness_core",
        "scope": "doorbell_rule",
        "rule_id": rule.get("id"),
        "camera": camera,
        "area": area,
        "provider": provider,
    }
    notify_result = {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}
    if _bool(rule.get("notifications"), True):
        notification_targets = _normalize_notification_targets(
            rule.get("notification_targets") or rule.get("notification_destinations"),
            device_services=rule.get("device_services") or rule.get("device_service"),
        )
        notify_result = await _notify_destinations(
            title=_text(rule.get("title") or "Doorbell"),
            message=spoken_line,
            priority=_text(rule.get("priority") or "normal"),
            notification_targets=notification_targets,
            origin=origin,
        )
    display_result: Dict[str, Any] = {"ok": True, "sent_count": 0, "skipped": "display_notifications_disabled"}
    if _bool(rule.get("display_notifications"), False):
        display_result = await _notify_display(
            kind="doorbell",
            title=_text(rule.get("title") or "Doorbell"),
            message=spoken_line,
            priority=_text(rule.get("priority") or "normal"),
            display_targets=rule.get("display_targets") or rule.get("display_target"),
            snapshot_store=snapshot_store,
            origin=origin,
            ttl_seconds=90,
        )
    return {
        "ok": True,
        "summary": spoken_line,
        "camera": camera,
        "area": area,
        "provider": provider,
        "players": players,
        "tts": tts_result,
        "notification": notify_result,
        "display_notification": display_result,
    }


async def _execute_entry_sensor_rule(
    rule: Dict[str, Any],
    llm_client: Any,
    reason: str,
    event: Dict[str, Any],
) -> Dict[str, Any]:
    del llm_client
    provider = _rule_provider(rule)
    sensor_type = _text(rule.get("sensor_type") or "door").lower()
    if sensor_type not in {"door", "window", "garage"}:
        sensor_type = "door"
    camera = _text(rule.get("camera_entity"))
    snapshot_provider, camera_target = _snapshot_target_for_camera(camera, provider)
    entity_id = _text(event.get("entity_id")) or _text(rule.get("sensor_entity"))
    new_state = event.get("new_state") if isinstance(event.get("new_state"), dict) else {}
    old_state = event.get("old_state") if isinstance(event.get("old_state"), dict) else {}
    action_label, action_token = _entry_state_action((new_state or {}).get("state"))
    sensor_name = _friendly_entity_name(entity_id, new_state or old_state)
    summary = _compact(f"{sensor_name} {action_label}.", limit=120)
    spoken_action = "open" if action_token == "open" else ("closed" if action_token == "closed" else action_label)
    spoken_line = _compact(f"{sensor_name} {spoken_action}.", limit=180)
    title = _compact(f"{sensor_name} {action_label}", limit=90)
    area = _text(rule.get("area")) or sensor_name
    ha_for_playback = _ha_config_optional()
    shared_tts = _shared_announcement_tts_settings()
    tts_backend = _text(shared_tts.get("backend") or "wyoming")
    tts_model = _text(shared_tts.get("model"))
    tts_voice = _text(shared_tts.get("voice"))
    snapshot_store: Dict[str, Any] = {}
    if camera:
        try:
            jpeg, snapshot_content_type = await _capture_camera_snapshot(snapshot_provider, camera_target)
            snapshot_store = _store_event_snapshot(redis_client, jpeg, content_type=snapshot_content_type)
        except Exception as exc:
            logger.warning("[awareness] entry sensor snapshot failed for %s (%s): %s", camera, provider, exc)
            snapshot_store = {"stored": False, "reason": "capture_failed", "bytes": 0}
    event_payload = {
        "source": _slug(area),
        "title": title,
        "type": f"{sensor_type}_sensor_{action_token}",
        "message": summary,
        "entity_id": entity_id,
        "ha_time": _now_iso(),
        "level": "info",
        "data": {
            "area": area,
            "sensor_type": sensor_type,
            "reason": reason,
            "trigger_entity": entity_id,
            "new_state": _text((new_state or {}).get("state")),
            "old_state": _text((old_state or {}).get("state")),
            "provider": provider,
            "tts_backend": tts_backend,
            "tts_model": tts_model,
            "tts_voice": tts_voice,
        },
    }
    if camera:
        event_payload["data"]["camera_entity"] = camera
    if snapshot_store.get("stored"):
        event_payload["snapshot_id"] = _text(snapshot_store.get("snapshot_id"))
        event_payload["data"]["snapshot_id"] = _text(snapshot_store.get("snapshot_id"))
        event_payload["data"]["snapshot_content_type"] = _text(snapshot_store.get("content_type") or "image/jpeg")
        event_payload["data"]["snapshot_bytes"] = _as_int(snapshot_store.get("bytes"), 0, minimum=0)
    elif snapshot_store.get("reason"):
        event_payload["data"]["snapshot_status"] = _text(snapshot_store.get("reason"))
        event_payload["data"]["snapshot_bytes"] = _as_int(snapshot_store.get("bytes"), 0, minimum=0)
    _append_event(redis_client, source=area, payload=event_payload)
    players = _normalize_players(rule.get("players"))
    tts_result: Dict[str, Any] = {
        "ok": True,
        "sent_count": 0,
        "skipped": "open_only" if action_token != "open" else "no_players",
        "backend": tts_backend,
    }
    if action_token == "open" and players:
        try:
            tts_result = await speak_announcement_targets(
                text=spoken_line,
                backend=tts_backend,
                ha_base=_text(ha_for_playback.get("base")),
                token=_text(ha_for_playback.get("token")),
                targets=players,
                model=tts_model,
                voice=tts_voice,
                wyoming_host=_text(shared_tts.get("wyoming_host")),
                wyoming_port=shared_tts.get("wyoming_port"),
                wyoming_voice=_text(shared_tts.get("wyoming_voice")),
                voice_core_backend=_text(shared_tts.get("voice_core_backend")),
                voice_core_model=_text(shared_tts.get("voice_core_model")),
                voice_core_voice=_text(shared_tts.get("voice_core_voice")),
                voice_core_wyoming_host=_text(shared_tts.get("voice_core_wyoming_host")),
                voice_core_wyoming_port=shared_tts.get("voice_core_wyoming_port"),
                voice_core_wyoming_voice=_text(shared_tts.get("voice_core_wyoming_voice")),
                default_backend=tts_backend,
            )
        except Exception as exc:
            logger.warning("[awareness] entry sensor TTS failed for %s: %s", entity_id, exc)
            tts_result = {"ok": False, "sent_count": 0, "error": str(exc), "backend": tts_backend}
    origin = {
        "platform": "awareness_core",
        "scope": "entry_sensor_rule",
        "rule_id": rule.get("id"),
        "entity_id": entity_id,
        "sensor_type": sensor_type,
        "area": area,
        "provider": provider,
    }
    notify_result: Dict[str, Any]
    display_result: Dict[str, Any]
    if action_token != "open":
        notify_result = {"ok": True, "sent_count": 0, "skipped": "open_only"}
        display_result = {"ok": True, "sent_count": 0, "skipped": "open_only"}
    else:
        if not _bool(rule.get("notifications"), False):
            notify_result = {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}
        else:
            notification_targets = _normalize_notification_targets(
                rule.get("notification_targets") or rule.get("notification_destinations"),
                device_services=rule.get("device_services") or rule.get("device_service"),
            )
            notify_result = await _notify_destinations(
                title=_text(rule.get("title") or "Entry Sensor"),
                message=summary,
                priority=_text(rule.get("priority") or "normal"),
                notification_targets=notification_targets,
                origin=origin,
            )
        if _bool(rule.get("display_notifications"), False):
            display_result = await _notify_display(
                kind="camera" if snapshot_store.get("stored") else "notification",
                title=_text(rule.get("title") or "Entry Sensor"),
                message=summary,
                priority=_text(rule.get("priority") or "normal"),
                display_targets=rule.get("display_targets") or rule.get("display_target"),
                snapshot_store=snapshot_store,
                origin=origin,
                ttl_seconds=90,
            )
        else:
            display_result = {"ok": True, "sent_count": 0, "skipped": "display_notifications_disabled"}
    return {
        "ok": True,
        "summary": summary,
        "sensor_type": sensor_type,
        "entity_id": entity_id,
        "area": area,
        "provider": provider,
        "players": players,
        "tts": tts_result,
        "notification": notify_result,
        "display_notification": display_result,
    }


async def _execute_rule(rule: Dict[str, Any], llm_client: Any, reason: str, event: Dict[str, Any]) -> Dict[str, Any]:
    kind = _text(rule.get("kind")).lower()
    if kind == "camera":
        return await _execute_camera_rule(rule, llm_client, reason, event)
    if kind == "doorbell":
        return await _execute_doorbell_rule(rule, llm_client, reason, event)
    if kind == "entry_sensor":
        return await _execute_entry_sensor_rule(rule, llm_client, reason, event)
    raise ValueError(f"Unsupported rule kind: {kind}")


def _empty_entity_catalog() -> Dict[str, List[Tuple[str, str]]]:
    return {
        "cameras": [],
        "triggers": [],
        "doorbell_triggers": [],
        "media_players": [],
        "tts": [],
        "input_text": [],
        "notify_services": [],
        "weather_sensors": [],
        "weather_temp": [],
        "weather_wind": [],
        "weather_rain": [],
        "entry_sensors": [],
        "entry_sensors_door": [],
        "entry_sensors_window": [],
        "entry_sensors_garage": [],
    }


def _cached_catalog(provider: str, *, force_refresh: bool = False) -> Optional[Dict[str, List[Tuple[str, str]]]]:
    cache_ttl = _setting_int(redis_client, "entity_catalog_ttl_sec", 30, minimum=5, maximum=600)
    now_ts = time.time()
    key = _normalize_event_provider(provider)
    with _ENTITY_CACHE_LOCK:
        bucket = _ENTITY_CACHE.get(key) or {"ts": 0.0, "data": {}}
        _ENTITY_CACHE[key] = bucket
        if force_refresh:
            return None
        if (_as_float(bucket.get("ts"), 0.0) + cache_ttl) <= now_ts:
            return None
        data = bucket.get("data")
        if isinstance(data, dict) and data:
            return data
    return None


def _set_cached_catalog(provider: str, catalog: Dict[str, List[Tuple[str, str]]]) -> None:
    key = _normalize_event_provider(provider)
    with _ENTITY_CACHE_LOCK:
        bucket = _ENTITY_CACHE.get(key) or {"ts": 0.0, "data": {}}
        bucket["ts"] = time.time()
        bucket["data"] = catalog
        _ENTITY_CACHE[key] = bucket


def _stale_cached_catalog(provider: str) -> Optional[Dict[str, List[Tuple[str, str]]]]:
    key = _normalize_event_provider(provider)
    with _ENTITY_CACHE_LOCK:
        bucket = _ENTITY_CACHE.get(key) or {}
        data = bucket.get("data")
        if isinstance(data, dict) and data:
            return data
    return None


def _finalize_catalog(catalog: Dict[str, List[Tuple[str, str]]]) -> Dict[str, List[Tuple[str, str]]]:
    for key in list(catalog.keys()):
        deduped: List[Tuple[str, str]] = []
        seen: set[str] = set()
        for value, label in catalog[key]:
            token = _text(value)
            if not token or token in seen:
                continue
            seen.add(token)
            deduped.append((token, _text(label) or token))
        catalog[key] = sorted(deduped, key=lambda row: row[1].lower())
    if not catalog["doorbell_triggers"]:
        catalog["doorbell_triggers"] = list(catalog["triggers"])
    if not catalog["weather_temp"]:
        catalog["weather_temp"] = list(catalog["weather_sensors"])
    if not catalog["weather_wind"]:
        catalog["weather_wind"] = list(catalog["weather_sensors"])
    if not catalog["weather_rain"]:
        catalog["weather_rain"] = list(catalog["weather_sensors"])
    return catalog


def _ha_entity_catalog(force_refresh: bool = False) -> Dict[str, List[Tuple[str, str]]]:
    cached = _cached_catalog("homeassistant", force_refresh=force_refresh)
    if cached is not None:
        return _catalog_with_announcement_targets(cached)
    catalog = _empty_entity_catalog()
    try:
        ha = _ha_config()
        resp = requests.get(
            f"{ha['base']}/api/states",
            headers=_ha_headers(ha["token"], json_content=False),
            timeout=10,
        )
        resp.raise_for_status()
        states = resp.json() or []
    except Exception:
        stale = _stale_cached_catalog("homeassistant")
        if stale is not None:
            return _catalog_with_announcement_targets(stale)
        catalog = _catalog_with_announcement_targets(catalog)
        _set_cached_catalog("homeassistant", catalog)
        return catalog

    def _label(entry: Dict[str, Any]) -> str:
        entity_id = _text(entry.get("entity_id"))
        attrs = entry.get("attributes") or {}
        if not isinstance(attrs, dict):
            attrs = {}
        name = _text(attrs.get("friendly_name"))
        return f"{name} ({entity_id})" if name else entity_id

    def _is_temp_sensor(entity_id_lower: str, device_class: str, unit: str) -> bool:
        if device_class == "temperature":
            return True
        if unit in {"°c", "°f", "c", "f"}:
            return True
        return any(token in entity_id_lower for token in ("temp", "temperature"))

    def _is_wind_sensor(entity_id_lower: str, device_class: str) -> bool:
        if device_class == "wind_speed":
            return True
        return "wind" in entity_id_lower

    def _is_rain_sensor(entity_id_lower: str, device_class: str) -> bool:
        if device_class in {"precipitation", "precipitation_intensity"}:
            return True
        return any(token in entity_id_lower for token in ("rain", "precip"))

    for row in states:
        if not isinstance(row, dict):
            continue
        entity_id = _text(row.get("entity_id"))
        if not entity_id:
            continue
        label = _label(row)
        lower = entity_id.lower()
        attrs = row.get("attributes") or {}
        if not isinstance(attrs, dict):
            attrs = {}
        device_class = _text(attrs.get("device_class")).lower()
        unit = _text(attrs.get("unit_of_measurement")).lower()
        if lower.startswith("camera."):
            catalog["cameras"].append((entity_id, label))
        if lower.startswith("tts."):
            catalog["tts"].append((entity_id, label))
        if lower.startswith("input_text."):
            catalog["input_text"].append((entity_id, label))
        if lower.startswith(("sensor.", "weather.")):
            catalog["weather_sensors"].append((entity_id, label))
            if _is_temp_sensor(lower, device_class, unit):
                catalog["weather_temp"].append((entity_id, label))
            if _is_wind_sensor(lower, device_class):
                catalog["weather_wind"].append((entity_id, label))
            if _is_rain_sensor(lower, device_class):
                catalog["weather_rain"].append((entity_id, label))
        if lower.startswith(("binary_sensor.", "sensor.", "cover.")):
            name_hint = f"{lower} {_text(attrs.get('friendly_name')).lower()}"
            is_garage = device_class == "garage_door" or "garage" in name_hint
            is_window = device_class == "window" or "window" in name_hint
            is_door = device_class in {"door", "opening"} or any(
                token in name_hint for token in ("door", "contact", "opening")
            )
            if is_door:
                catalog["entry_sensors"].append((entity_id, label))
                catalog["entry_sensors_door"].append((entity_id, label))
            if is_window:
                catalog["entry_sensors"].append((entity_id, label))
                catalog["entry_sensors_window"].append((entity_id, label))
            # Garage options intentionally prefer cover.* so users select actionable garage door entities.
            if is_garage and lower.startswith("cover."):
                catalog["entry_sensors"].append((entity_id, label))
                catalog["entry_sensors_garage"].append((entity_id, label))
        if lower.startswith(("binary_sensor.", "sensor.", "event.", "button.", "switch.", "input_boolean.")):
            catalog["triggers"].append((entity_id, label))
            if any(token in lower for token in ("doorbell", "ring", "ding", "button", "press")):
                catalog["doorbell_triggers"].append((entity_id, label))

    try:
        svc_resp = requests.get(
            f"{ha['base']}/api/services",
            headers=_ha_headers(ha["token"], json_content=False),
            timeout=10,
        )
        svc_resp.raise_for_status()
        domains = svc_resp.json() or []
        for domain_row in domains:
            if not isinstance(domain_row, dict):
                continue
            if _text(domain_row.get("domain")).lower() != "notify":
                continue
            services = domain_row.get("services")
            if not isinstance(services, dict):
                continue
            for service_name, service_meta in services.items():
                svc = _text(service_name)
                if not svc:
                    continue
                full_service = f"notify.{svc}"
                meta = service_meta if isinstance(service_meta, dict) else {}
                nice_name = _text(meta.get("name"))
                label = f"{nice_name} ({full_service})" if nice_name else full_service
                catalog["notify_services"].append((full_service, label))
    except Exception:
        logger.debug("[awareness] notify service discovery failed", exc_info=True)

    catalog = _catalog_with_announcement_targets(catalog)
    _set_cached_catalog("homeassistant", catalog)
    return catalog


def _unifi_camera_is_doorbell(camera_row: Dict[str, Any]) -> bool:
    name = _text(camera_row.get("name")).lower()
    model = _text(camera_row.get("modelKey") or camera_row.get("model_key") or camera_row.get("type")).lower()
    hint = f"{name} {model}"
    return "doorbell" in hint or "g4db" in hint or "g5db" in hint


def _unifi_sensor_type(sensor_row: Dict[str, Any]) -> str:
    name = _text(sensor_row.get("name")).lower()
    mount_type = _text(sensor_row.get("mountType") or sensor_row.get("mount_type") or sensor_row.get("mount")).lower()
    sensor_type = _text(
        sensor_row.get("sensorType")
        or sensor_row.get("sensor_type")
        or sensor_row.get("modelKey")
        or sensor_row.get("model_key")
        or sensor_row.get("type")
    ).lower()
    hint = f"{name} {mount_type} {sensor_type}"
    if "garage" in hint:
        return "garage"
    if "window" in hint:
        return "window"
    return "door"


def _unifi_entity_catalog(force_refresh: bool = False) -> Dict[str, List[Tuple[str, str]]]:
    cached = _cached_catalog("unifi_protect", force_refresh=force_refresh)
    if cached is not None:
        return cached
    catalog = _empty_entity_catalog()
    # Keep Home Assistant support entities available so notifications still work.
    ha_support = _ha_entity_catalog(force_refresh=force_refresh)
    if not any(ha_support.get(key) for key in ("tts", "notify_services", "input_text")):
        stale_ha_support = _stale_cached_catalog("homeassistant")
        if stale_ha_support is not None:
            ha_support = stale_ha_support
    for key in ("tts", "input_text", "notify_services", "weather_sensors", "weather_temp", "weather_wind", "weather_rain"):
        catalog[key] = list(ha_support.get(key) or [])
    try:
        cameras = _unifi_list_cameras()
    except Exception:
        cameras = []
    try:
        sensors = _unifi_list_sensors()
    except Exception:
        sensors = []

    for row in cameras:
        if not isinstance(row, dict):
            continue
        camera_id = _text(row.get("id")).lower()
        if not camera_id:
            continue
        camera_name = _text(row.get("name")) or camera_id
        camera_entity = _unifi_camera_entity(camera_id)
        catalog["cameras"].append((camera_entity, f"{camera_name} ({camera_entity})"))
        motion_trigger = _unifi_camera_motion_trigger(camera_id)
        catalog["triggers"].append((motion_trigger, f"{camera_name} motion ({motion_trigger})"))
        for smart_type in _unifi_camera_smart_detect_types(row):
            smart_label = _unifi_smart_type_label(smart_type)
            smart_trigger = _unifi_camera_smart_trigger(camera_id, smart_type)
            catalog["triggers"].append((smart_trigger, f"{camera_name} {smart_label} ({smart_trigger})"))
        if _unifi_camera_is_doorbell(row):
            doorbell_trigger = _unifi_camera_doorbell_trigger(camera_id)
            catalog["doorbell_triggers"].append(
                (doorbell_trigger, f"{camera_name} doorbell button press ({doorbell_trigger})")
            )

    for row in sensors:
        if not isinstance(row, dict):
            continue
        sensor_id = _text(row.get("id")).lower()
        if not sensor_id:
            continue
        sensor_name = _text(row.get("name")) or sensor_id
        sensor_entity = _unifi_sensor_entity(sensor_id)
        catalog["entry_sensors"].append((sensor_entity, f"{sensor_name} ({sensor_entity})"))
        sensor_type = _unifi_sensor_type(row)
        if sensor_type == "window":
            catalog["entry_sensors_window"].append((sensor_entity, f"{sensor_name} ({sensor_entity})"))
        elif sensor_type == "garage":
            catalog["entry_sensors_garage"].append((sensor_entity, f"{sensor_name} ({sensor_entity})"))
        else:
            catalog["entry_sensors_door"].append((sensor_entity, f"{sensor_name} ({sensor_entity})"))
        # Sensors can be used as direct trigger entities in entry sensor rules.
        catalog["triggers"].append((sensor_entity, f"{sensor_name} ({sensor_entity})"))

    catalog = _finalize_catalog(catalog)
    _set_cached_catalog("unifi_protect", catalog)
    return catalog


def _prefixed_event_catalog(catalog: Dict[str, List[Tuple[str, str]]], provider: str) -> Dict[str, List[Tuple[str, str]]]:
    out = _empty_entity_catalog()
    provider_name = _provider_label(provider)
    for key, pairs in (catalog or {}).items():
        if key not in out:
            continue
        if key in {"cameras", "triggers", "doorbell_triggers", "entry_sensors", "entry_sensors_door", "entry_sensors_window", "entry_sensors_garage"}:
            out[key] = [
                (_provider_ref(provider, value), f"{provider_name}: {_text(label) or _text(value)}")
                for value, label in pairs or []
            ]
        else:
            out[key] = list(pairs or [])
    return out


def _catalog_extend(target: Dict[str, List[Tuple[str, str]]], source: Dict[str, List[Tuple[str, str]]]) -> None:
    for key in target:
        target[key].extend(source.get(key) or [])


def _device_label(provider_name: str, device: Dict[str, Any], device_id: str) -> str:
    name = _text(device.get("name")) or device_id
    device_type = _text(device.get("type")).replace("_", " ")
    suffix = f"{provider_name}: {name}"
    if device_type:
        suffix = f"{suffix} {device_type}"
    return f"{suffix} ({device_id})"


def _integration_devices_catalog(provider_filter: Optional[str] = None) -> Dict[str, List[Tuple[str, str]]]:
    catalog = _empty_entity_catalog()
    from integration_registry import get_integration_catalog, get_integration_device_group
    filter_provider = _normalize_event_provider(provider_filter or "all")

    for integration in get_integration_catalog():
        if not isinstance(integration, dict):
            continue
        integration_id = _text(integration.get("id"))
        provider = _normalize_event_provider(integration_id)
        if provider == "all":
            continue
        if filter_provider != "all" and provider != filter_provider:
            continue
        payload = get_integration_device_group(integration_id)
        group = payload.get("group") if isinstance(payload, dict) and isinstance(payload.get("group"), dict) else {}
        provider_name = _text(group.get("name") or integration.get("name")) or _provider_label(provider)
        devices = group.get("devices") if isinstance(group.get("devices"), list) else []
        for device in devices:
            if not isinstance(device, dict):
                continue
            device_id = _text(device.get("id"))
            if not device_id:
                continue
            device_type = _text(device.get("type")).lower()
            details = device.get("details") if isinstance(device.get("details"), dict) else {}
            capabilities = {str(item).strip().lower() for item in (device.get("capabilities") or []) if str(item).strip()}
            entity = _text(device.get("ref"))
            if not entity:
                if provider == "hue" and device_type in {"motion", "contact", "light", "temperature"}:
                    entity = f"{device_type}:{device_id}"
                elif provider == "unifi_network":
                    entity = f"{'client' if device_type == 'client' else 'device'}:{device_id}"
                elif provider == "aladdin" and device_type in {"garage", "garage_door"}:
                    entity = f"garage_door:{device_id}"
                elif provider == "ecobee_homekit" and device_type == "thermostat":
                    entity = device_id
                else:
                    entity = f"{device_type or 'device'}:{device_id}"
            ref = _provider_ref(provider, entity)
            label = _device_label(provider_name, device, entity)

            if "camera" in capabilities or device_type == "camera":
                catalog["cameras"].append((ref, label))
            if "doorbell" in capabilities:
                catalog["doorbell_triggers"].append((ref, label))
            if "motion" in capabilities or device_type == "motion":
                catalog["triggers"].append((ref, label))
            if "entry_sensor" in capabilities or device_type in {"contact", "entry_sensor", "garage_door"}:
                catalog["entry_sensors"].append((ref, label))
                name_hint = f"{_text(device.get('name')).lower()} {_text(details.get('resource_type')).lower()}"
                if "window" in capabilities or "window" in name_hint:
                    catalog["entry_sensors_window"].append((ref, label))
                elif "garage" in capabilities or "garage" in name_hint:
                    catalog["entry_sensors_garage"].append((ref, label))
                else:
                    catalog["entry_sensors_door"].append((ref, label))
                catalog["triggers"].append((ref, label))
            if "temperature" in capabilities or device_type in {"temperature", "thermostat"}:
                catalog["weather_sensors"].append((ref, label))
                catalog["weather_temp"].append((ref, label))
            if "humidity" in capabilities:
                catalog["weather_sensors"].append((ref, label))

            for event_source in device.get("event_sources") or []:
                if not isinstance(event_source, dict):
                    continue
                source_ref = _text(event_source.get("ref"))
                if not source_ref:
                    continue
                source_type = _text(event_source.get("type")).lower()
                full_ref = _provider_ref(provider, source_ref)
                source_label = f"{provider_name}: {_text(device.get('name')) or device_id} {source_type or 'event'} ({source_ref})"
                if source_type == "doorbell":
                    catalog["doorbell_triggers"].append((full_ref, source_label))
                elif source_type in {"door", "window", "garage", "contact", "entry_sensor"}:
                    catalog["entry_sensors"].append((full_ref, source_label))
                    if source_type == "window":
                        catalog["entry_sensors_window"].append((full_ref, source_label))
                    elif source_type == "garage":
                        catalog["entry_sensors_garage"].append((full_ref, source_label))
                    else:
                        catalog["entry_sensors_door"].append((full_ref, source_label))
                    catalog["triggers"].append((full_ref, source_label))
                else:
                    catalog["triggers"].append((full_ref, source_label))

            if provider == "unifi_network" or device_type in {"light", "switch", "client", "network_device"}:
                catalog["triggers"].append((ref, label))
    return catalog


def _all_integrations_entity_catalog(force_refresh: bool = False) -> Dict[str, List[Tuple[str, str]]]:
    cached = _cached_catalog("all", force_refresh=force_refresh)
    if cached is not None:
        return cached
    catalog = _empty_entity_catalog()
    _catalog_extend(catalog, _prefixed_event_catalog(_ha_entity_catalog(force_refresh=force_refresh), "homeassistant"))
    _catalog_extend(catalog, _prefixed_event_catalog(_unifi_entity_catalog(force_refresh=force_refresh), "unifi_protect"))
    _catalog_extend(catalog, _integration_devices_catalog())
    catalog = _finalize_catalog(catalog)
    catalog = _catalog_with_announcement_targets(catalog)
    _set_cached_catalog("all", catalog)
    return catalog


def _entity_catalog(force_refresh: bool = False, *, provider: Optional[str] = None) -> Dict[str, List[Tuple[str, str]]]:
    active_provider = _normalize_event_provider(provider or "all")
    if active_provider == "all":
        return _all_integrations_entity_catalog(force_refresh=force_refresh)
    if active_provider == "unifi_protect":
        return _unifi_entity_catalog(force_refresh=force_refresh)
    if active_provider == "homeassistant":
        return _ha_entity_catalog(force_refresh=force_refresh)
    cached = _cached_catalog(active_provider, force_refresh=force_refresh)
    if cached is not None:
        return cached
    catalog = _integration_devices_catalog(provider_filter=active_provider)
    catalog = _finalize_catalog(catalog)
    _set_cached_catalog(active_provider, catalog)
    return catalog


def _choices_from_pairs(
    pairs: List[Tuple[str, str]],
    *,
    placeholder: str,
    current_value: str = "",
    current_label: str = "",
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = [{"value": "", "label": placeholder}]
    seen: set[str] = {""}
    for value, label in pairs:
        token = _text(value)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append({"value": token, "label": _text(label) or token})
    current = _text(current_value)
    if current and current not in seen:
        out.append({"value": current, "label": _text(current_label) or f"{current} (current)"})
    return out


def _multiselect_choices_from_pairs(
    pairs: List[Tuple[str, str]],
    *,
    current_values: Any = None,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for value, label in pairs:
        token = _text(value)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append({"value": token, "label": _text(label) or token})
    for current in _normalize_players(current_values):
        token = _text(current)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append({"value": token, "label": f"{token} (current)"})
    return out


def _notification_destination_catalog(redis_obj: Any) -> Dict[str, Any]:
    if redis_obj is None:
        return {"platforms": []}
    try:
        payload = notifier_destination_catalog(redis_client=redis_obj, limit=250)
    except Exception:
        logger.debug("[awareness] notifier destination catalog failed", exc_info=True)
        return {"platforms": []}
    if not isinstance(payload, dict):
        return {"platforms": []}
    if not isinstance(payload.get("platforms"), list):
        payload["platforms"] = []
    return payload


def _notification_catalog_platform_map(catalog: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    rows = catalog.get("platforms") if isinstance(catalog, dict) else []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        platform = _text(row.get("platform")).lower()
        if platform:
            out[platform] = row
    return out


def _notification_destination_label(platform: Any, targets: Any) -> str:
    platform_name = _text(platform).lower()
    payload = _notification_clean_targets_dict(targets)
    if platform_name == "discord":
        channel = _text(payload.get("channel") or payload.get("channel_id"))
        guild = _text(payload.get("guild") or payload.get("guild_name") or payload.get("guild_id"))
        if channel and guild:
            return f"{channel} • {guild}"
        return channel or guild or "Discord target"
    if platform_name == "irc":
        return _text(payload.get("channel")) or "IRC channel"
    if platform_name == "matrix":
        return _text(payload.get("room_alias") or payload.get("room_id") or payload.get("channel")) or "Matrix room"
    if platform_name == "telegram":
        channel = _text(payload.get("channel"))
        chat_id = _text(payload.get("chat_id"))
        if channel and chat_id and channel != chat_id:
            return f"{channel} • {chat_id}"
        return channel or chat_id or "Telegram chat"
    if platform_name == "homeassistant":
        return _normalize_device_service(payload.get("device_service")) or "Home Assistant defaults"
    if platform_name == "webui":
        return "WebUI chat"
    if platform_name == "macos":
        scope = _text(payload.get("scope"))
        device_id = _text(payload.get("device_id"))
        if scope and device_id:
            return f"{scope} • {device_id}"
        return scope or device_id or "macOS target"
    if platform_name == "little_spud":
        user = _text(payload.get("user"))
        device_name = _text(payload.get("device_name"))
        device_id = _text(payload.get("device_id"))
        node_id = _text(payload.get("node_id"))
        if user and device_name:
            return f"{user} on {device_name}"
        if user and device_id:
            return f"{user} on {device_id}"
        return device_name or device_id or node_id or _text(payload.get("scope")) or "Little Spud"
    return _text(payload.get("channel") or payload.get("room_id") or payload.get("chat_id") or payload.get("device_id")) or "Destination"


def _notification_destination_options(
    current_targets: Any = None,
    *,
    device_services: Any = None,
    catalog: Dict[str, Any],
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    for platform_name, platform_row in _notification_catalog_platform_map(catalog).items():
        platform_label = _text(platform_row.get("label")) or platform_name
        requires_target = bool(platform_row.get("requires_target"))
        if not requires_target:
            value = _notification_encode_destination(platform_name, {})
            if value and value not in seen:
                out.append({"value": value, "label": f"{platform_label}: defaults"})
                seen.add(value)
        destinations = platform_row.get("destinations")
        if not isinstance(destinations, list):
            continue
        for row in destinations:
            if not isinstance(row, dict):
                continue
            targets = _notification_clean_targets_dict(row.get("targets"))
            value = _notification_encode_destination(platform_name, targets)
            if not value or value in seen:
                continue
            label = _text(row.get("label")) or _notification_destination_label(platform_name, targets)
            out.append({"value": value, "label": f"{platform_label}: {label}"})
            seen.add(value)

    for entry in _normalize_notification_targets(current_targets, device_services=device_services):
        value = _notification_encode_destination(entry.get("platform"), entry.get("targets"))
        if not value or value in seen:
            continue
        platform = _text(entry.get("platform")).lower()
        platform_row = _notification_catalog_platform_map(catalog).get(platform, {})
        platform_label = _text(platform_row.get("label")) or platform or "Notifier"
        label = _notification_destination_label(platform, entry.get("targets"))
        out.append({"value": value, "label": f"{platform_label}: {label} (current)"})
        seen.add(value)
    return out


def _players_text(value: Any) -> str:
    return "\n".join(_normalize_players(value))


def _announcement_target_pairs(
    homeassistant_pairs: List[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    del homeassistant_pairs
    pairs: List[Tuple[str, str]] = []
    seen: set[str] = set()

    for option in _awareness_playback_target_options():
        if not isinstance(option, dict):
            continue
        token = _text(option.get("value"))
        if not token or token in seen:
            continue
        seen.add(token)
        pairs.append((token, _text(option.get("label")) or token))

    return pairs


def _catalog_with_announcement_targets(
    catalog: Dict[str, List[Tuple[str, str]]],
) -> Dict[str, List[Tuple[str, str]]]:
    next_catalog = _empty_entity_catalog()
    source = catalog if isinstance(catalog, dict) else {}
    for key in next_catalog:
        next_catalog[key] = list(source.get(key) or [])
    next_catalog["media_players"] = _announcement_target_pairs(next_catalog.get("media_players") or [])
    return _finalize_catalog(next_catalog)


def _awareness_playback_target_options(*, current_values: Any = None) -> List[Dict[str, str]]:
    ha = _ha_config_optional()
    return build_announcement_target_options(
        homeassistant_base_url=ha.get("base", ""),
        homeassistant_token=ha.get("token", ""),
        include_homeassistant=True,
        include_sonos=True,
        include_unifi_protect=True,
        current_values=current_values,
    )


def _announcement_tts_fields(
    rule: Dict[str, Any],
    catalog: Dict[str, List[Tuple[str, str]]],
    *,
    players_description: str,
) -> List[Dict[str, Any]]:
    del catalog
    playback_options = [
        dict(option)
        for option in _awareness_playback_target_options(current_values=rule.get("players"))
        if isinstance(option, dict)
    ]
    return [
        {
            "key": "players",
            "label": "Playback Targets",
            "type": "multiselect",
            "description": players_description,
            "options": playback_options,
            "value": _normalize_players(rule.get("players")),
        },
    ]



def _announcement_tts_add_fields(
    catalog: Dict[str, List[Tuple[str, str]]],
    *,
    show_when: Dict[str, Any],
    players_description: str,
) -> List[Dict[str, Any]]:
    fields = _announcement_tts_fields(
        {
            "players": [],
        },
        catalog,
        players_description=players_description,
    )
    wrapped: List[Dict[str, Any]] = []
    for field in fields:
        field_copy = dict(field)
        show_when_all: List[Dict[str, Any]] = [dict(show_when)]
        existing_show = field_copy.pop("show_when", None)
        if isinstance(existing_show, dict):
            show_when_all.append(existing_show)
        existing_show_all = field_copy.pop("show_when_all", None)
        if isinstance(existing_show_all, list):
            show_when_all.extend([cond for cond in existing_show_all if isinstance(cond, dict)])
        field_copy["show_when_all"] = show_when_all
        wrapped.append(field_copy)
    return wrapped


def _rule_subtitle(rule: Dict[str, Any]) -> str:
    kind = _text(rule.get("kind"))
    enabled = "Enabled" if _bool(rule.get("enabled"), True) else "Disabled"
    last_run = _fmt_ts(rule.get("last_run_ts"))
    provider = _provider_label(_rule_provider(rule))
    return f"{enabled} - {kind} - {provider} - last: {last_run}"


def _to_area_label(area_value: str) -> str:
    value = _text(area_value).replace("_", " ").strip()
    if not value:
        return ""
    return " ".join(part.capitalize() for part in value.split())


def _area_options(rules: Dict[str, Dict[str, Any]], *, current_value: str = "") -> List[Dict[str, str]]:
    candidates: set[str] = set()
    for preset in _AREA_PRESETS:
        token = _text(preset).lower()
        if token:
            candidates.add(token)
    for source in _discover_event_sources(redis_client):
        token = _text(source).replace("_", " ").lower()
        if token:
            candidates.add(" ".join(token.split()))
    for rule in (rules or {}).values():
        area = _text((rule or {}).get("area")).lower()
        if area:
            candidates.add(" ".join(area.split()))

    out: List[Dict[str, str]] = [{"value": "", "label": "(Select area)"}]
    seen: set[str] = {""}
    for area in sorted(candidates):
        if area in seen:
            continue
        seen.add(area)
        out.append({"value": area, "label": _to_area_label(area) or area})

    current = _text(current_value).lower()
    if current and current not in seen:
        out.append({"value": current, "label": _to_area_label(current) or f"{current} (current)"})
    return out


def _entity_object_id(entity_id: str) -> str:
    _provider, raw_entity = _split_provider_ref(entity_id, "")
    token = _text(raw_entity or entity_id).lower()
    if "." in token:
        return token.split(".", 1)[1]
    return token


def _camera_aliases(camera_entity: str) -> List[str]:
    object_id = _entity_object_id(camera_entity)
    aliases: set[str] = {object_id}
    for suffix in ("_high", "_low", "_snapshot", "_stream", "_still"):
        if object_id.endswith(suffix):
            aliases.add(object_id[: -len(suffix)])
    parts = [part for part in re.split(r"[_\-.]+", object_id) if part]
    if len(parts) >= 2:
        aliases.add("_".join(parts[:2]))
    if parts:
        aliases.add(parts[0])
    return sorted([item for item in aliases if len(item) >= 3], key=len, reverse=True)


def _trigger_matches_camera(camera_entity: str, trigger_entity: str) -> bool:
    camera_provider, raw_camera = _split_provider_ref(camera_entity, "all")
    trigger_provider, raw_trigger = _split_provider_ref(trigger_entity, camera_provider)
    if camera_provider != "all" and trigger_provider != "all" and camera_provider != trigger_provider:
        return False
    trigger_object = _entity_object_id(raw_trigger)
    for alias in _camera_aliases(raw_camera):
        if alias and alias in trigger_object:
            return True
    return False


def _trigger_dependency_for_camera(
    *,
    catalog: Dict[str, List[Tuple[str, str]]],
    current_camera: str,
    current_triggers: Any,
    doorbell: bool,
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    trigger_pairs = catalog.get("doorbell_triggers") if doorbell else catalog.get("triggers")
    trigger_pairs = trigger_pairs if isinstance(trigger_pairs, list) else []
    cameras = catalog.get("cameras") if isinstance(catalog.get("cameras"), list) else []
    if doorbell:
        # Ensure every camera exposes a derived doorbell trigger option even when
        # backend catalog heuristics fail to classify a camera as a doorbell.
        existing = {_text(pair[0]).lower() for pair in trigger_pairs}
        for camera_entity, camera_label in cameras:
            camera_provider, raw_camera_entity = _split_provider_ref(camera_entity, "all")
            if camera_provider != "unifi_protect":
                continue
            camera_id = _text(_unifi_camera_id_from_entity(raw_camera_entity)).lower()
            if not camera_id:
                continue
            derived = _provider_ref("unifi_protect", _unifi_camera_doorbell_trigger(camera_id))
            if derived.lower() in existing:
                continue
            label_name = _catalog_label_name(camera_label, camera_entity)
            trigger_pairs.append((derived, f"{label_name} doorbell button press ({derived})"))
            existing.add(derived.lower())
        doorbell_only = [
            pair for pair in trigger_pairs
            if _text(_split_provider_ref(pair[0], "")[1]).lower().endswith("_doorbell")
        ]
        if doorbell_only:
            trigger_pairs = doorbell_only
    current_trigger_values = _normalize_trigger_entities(current_triggers)

    default_options = _multiselect_choices_from_pairs(
        trigger_pairs,
        current_values=current_trigger_values if not _text(current_camera) else [],
    )

    options_by_source: Dict[str, List[Dict[str, str]]] = {}
    for camera_entity, _label in cameras:
        token = _text(camera_entity)
        if not token:
            continue
        narrowed_pairs = [pair for pair in trigger_pairs if _trigger_matches_camera(token, pair[0])]
        if not narrowed_pairs:
            continue
        options_by_source[token] = _multiselect_choices_from_pairs(
            narrowed_pairs,
            current_values=current_trigger_values if _text(current_camera) == token else [],
        )

    current_camera_token = _text(current_camera)
    if current_camera_token and current_camera_token not in options_by_source and current_trigger_values:
        options_by_source[current_camera_token] = _multiselect_choices_from_pairs(
            [],
            current_values=current_trigger_values,
        )

    return default_options, {
        "source_key": "camera_entity",
        "options_by_source": options_by_source,
        "default_options": default_options,
    }


def _entry_sensor_dependency_options(
    *,
    catalog: Dict[str, List[Tuple[str, str]]],
    current_type: str,
    current_entity: str,
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    sensor_type = _text(current_type).lower()
    if sensor_type not in {"door", "window", "garage"}:
        sensor_type = "door"
    all_pairs = catalog.get("entry_sensors") or catalog.get("triggers") or []
    door_pairs = catalog.get("entry_sensors_door") or all_pairs
    window_pairs = catalog.get("entry_sensors_window") or all_pairs
    garage_pairs = catalog.get("entry_sensors_garage") or []
    placeholder = "(Select door/window/garage sensor)"
    default_options = _choices_from_pairs(
        all_pairs,
        placeholder=placeholder,
        current_value=_text(current_entity),
    )
    options_by_source: Dict[str, List[Dict[str, str]]] = {
        "door": _choices_from_pairs(
            door_pairs,
            placeholder=placeholder,
            current_value=_text(current_entity) if sensor_type == "door" else "",
        ),
        "window": _choices_from_pairs(
            window_pairs,
            placeholder=placeholder,
            current_value=_text(current_entity) if sensor_type == "window" else "",
        ),
        "garage": _choices_from_pairs(
            garage_pairs,
            placeholder=placeholder,
            current_value=_text(current_entity) if sensor_type == "garage" else "",
        ),
    }
    return default_options, {
        "source_key": "sensor_type",
        "options_by_source": options_by_source,
        "default_options": default_options,
    }


def _camera_form(
    rule: Dict[str, Any],
    catalog: Dict[str, List[Tuple[str, str]]],
    notification_catalog: Dict[str, Any],
    rules: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    camera_options = _choices_from_pairs(
        catalog.get("cameras") or [],
        placeholder="(Select camera)",
        current_value=_text(rule.get("camera_entity")),
    )
    trigger_options, trigger_dependency = _trigger_dependency_for_camera(
        catalog=catalog,
        current_camera=_text(rule.get("camera_entity")),
        current_triggers=rule.get("trigger_entities") or rule.get("trigger_entity"),
        doorbell=False,
    )
    area_options = _area_options(
        rules,
        current_value=_text(rule.get("area")),
    )
    notify_destination_options = _notification_destination_options(
        rule.get("notification_targets") or rule.get("notification_destinations"),
        device_services=rule.get("device_services") or rule.get("device_service"),
        catalog=notification_catalog,
    )
    notify_destination_values = _notification_target_values(
        rule.get("notification_targets") or rule.get("notification_destinations"),
        device_services=rule.get("device_services") or rule.get("device_service"),
    )
    display_target_options = _display_target_options(rule.get("display_targets") or rule.get("display_target"))
    return {
        "id": rule["id"],
        "group": "camera",
        "title": _text(rule.get("name")) or "Camera rule",
        "subtitle": _rule_subtitle(rule),
        "save_action": "awareness_save_rule",
        "remove_action": "awareness_remove_rule",
        "run_action": "awareness_run_now",
        "run_label": "Run Now",
        "remove_confirm": "Remove this camera rule?",
        "fields": [
            {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": _bool(rule.get("enabled"), True)},
            {"key": "name", "label": "Rule Name", "type": "text", "value": _text(rule.get("name"))},
            {
                "key": "camera_entity",
                "label": "Camera",
                "type": "select",
                "options": camera_options,
                "value": _text(rule.get("camera_entity")),
            },
            {
                "key": "area",
                "label": "Area",
                "type": "select",
                "options": area_options,
                "value": _text(rule.get("area")),
            },
        ],
        "sections": [
            {
                "label": "Trigger",
                "fields": [
                    {
                        "key": "trigger_entities",
                        "label": "Trigger Entities",
                        "type": "multiselect",
                        "description": "Select one or more trigger entities. Any selected trigger will run this rule.",
                        "options": trigger_options,
                        "dependent_options": trigger_dependency,
                        "value": _normalize_trigger_entities(rule.get("trigger_entities") or rule.get("trigger_entity")),
                    },
                ],
            },
            {
                "label": "Detection",
                "fields": [
                    {
                        "key": "cooldown_seconds",
                        "label": "Cooldown (sec)",
                        "type": "number",
                        "value": _as_int(rule.get("cooldown_seconds"), 30, minimum=0, maximum=86400),
                    },
                    {
                        "key": "notification_cooldown_seconds",
                        "label": "Notification Cooldown (sec)",
                        "type": "number",
                        "value": _as_int(
                            rule.get("notification_cooldown_seconds"), 0, minimum=0, maximum=86400
                        ),
                    },
                    {
                        "key": "ignore_vehicles",
                        "label": "Ignore Vehicles",
                        "type": "checkbox",
                        "value": _bool(rule.get("ignore_vehicles"), False),
                    },
                ],
            },
            {
                "label": "Notifications",
                "fields": [
                    {"key": "title", "label": "Title", "type": "text", "value": _text(rule.get("title") or "Camera Event")},
                    {
                        "key": "priority",
                        "label": "Priority",
                        "type": "select",
                        "options": [{"value": "high", "label": "High"}, {"value": "normal", "label": "Normal"}],
                        "value": _text(rule.get("priority") or "high"),
                    },
                    {
                        "key": "notification_targets",
                        "label": "Notification Destinations",
                        "type": "multiselect",
                        "description": "Choose one or more notifier destinations from Tater's shared notification catalog.",
                        "options": notify_destination_options,
                        "value": notify_destination_values,
                    },
                    {
                        "key": "display_notifications",
                        "label": "Show On ESPHome Displays",
                        "type": "checkbox",
                        "value": _bool(rule.get("display_notifications"), False),
                    },
                    {
                        "key": "display_targets",
                        "label": "Display Targets",
                        "type": "multiselect",
                        "description": "Choose one or more S3Box display targets. Leave empty to use all displays when enabled.",
                        "options": display_target_options,
                        "value": _normalize_display_targets(rule.get("display_targets") or rule.get("display_target")),
                    },
                ],
            },
        ],
    }


def _doorbell_form(
    rule: Dict[str, Any],
    catalog: Dict[str, List[Tuple[str, str]]],
    notification_catalog: Dict[str, Any],
    rules: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    camera_options = _choices_from_pairs(
        catalog.get("cameras") or [],
        placeholder="(Select camera)",
        current_value=_text(rule.get("camera_entity")),
    )
    trigger_options, trigger_dependency = _trigger_dependency_for_camera(
        catalog=catalog,
        current_camera=_text(rule.get("camera_entity")),
        current_triggers=rule.get("trigger_entities") or rule.get("trigger_entity"),
        doorbell=True,
    )
    area_options = _area_options(
        rules,
        current_value=_text(rule.get("area")),
    )
    notify_destination_options = _notification_destination_options(
        rule.get("notification_targets") or rule.get("notification_destinations"),
        device_services=rule.get("device_services") or rule.get("device_service"),
        catalog=notification_catalog,
    )
    notify_destination_values = _notification_target_values(
        rule.get("notification_targets") or rule.get("notification_destinations"),
        device_services=rule.get("device_services") or rule.get("device_service"),
    )
    display_target_options = _display_target_options(rule.get("display_targets") or rule.get("display_target"))
    return {
        "id": rule["id"],
        "group": "doorbell",
        "title": _text(rule.get("name")) or "Doorbell rule",
        "subtitle": _rule_subtitle(rule),
        "save_action": "awareness_save_rule",
        "remove_action": "awareness_remove_rule",
        "run_action": "awareness_run_now",
        "run_label": "Run Now",
        "remove_confirm": "Remove this doorbell rule?",
        "fields": [
            {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": _bool(rule.get("enabled"), True)},
            {"key": "name", "label": "Rule Name", "type": "text", "value": _text(rule.get("name"))},
            {
                "key": "camera_entity",
                "label": "Camera",
                "type": "select",
                "options": camera_options,
                "value": _text(rule.get("camera_entity")),
            },
            {
                "key": "area",
                "label": "Area",
                "type": "select",
                "options": area_options,
                "value": _text(rule.get("area")),
            },
            {
                "key": "notifications",
                "label": "Send Notifications",
                "type": "checkbox",
                "value": _bool(rule.get("notifications"), True),
            },
        ],
        "sections": [
            {
                "label": "Trigger",
                "fields": [
                    {
                        "key": "trigger_entities",
                        "label": "Trigger Entities",
                        "type": "multiselect",
                        "description": "Select one or more trigger entities. Any selected trigger will run this rule.",
                        "options": trigger_options,
                        "dependent_options": trigger_dependency,
                        "value": _normalize_trigger_entities(rule.get("trigger_entities") or rule.get("trigger_entity")),
                    },
                ],
            },
            {
                "label": "Playback",
                "fields": _announcement_tts_fields(
                    rule,
                    catalog,
                    players_description="Speak the shared announcement voice on selected Voice Core satellites, Sonos speakers, Home Assistant media players, or UniFi Protect camera speakers.",
                ),
            },
            {
                "label": "Notifications",
                "fields": [
                    {"key": "title", "label": "Title", "type": "text", "value": _text(rule.get("title") or "Doorbell")},
                    {
                        "key": "priority",
                        "label": "Priority",
                        "type": "select",
                        "options": [{"value": "high", "label": "High"}, {"value": "normal", "label": "Normal"}],
                        "value": _text(rule.get("priority") or "normal"),
                    },
                    {
                        "key": "notification_targets",
                        "label": "Notification Destinations",
                        "type": "multiselect",
                        "description": "Choose one or more notifier destinations from Tater's shared notification catalog.",
                        "options": notify_destination_options,
                        "value": notify_destination_values,
                    },
                    {
                        "key": "display_notifications",
                        "label": "Show On ESPHome Displays",
                        "type": "checkbox",
                        "value": _bool(rule.get("display_notifications"), False),
                    },
                    {
                        "key": "display_targets",
                        "label": "Display Targets",
                        "type": "multiselect",
                        "description": "Choose one or more S3Box display targets. Leave empty to use all displays when enabled.",
                        "options": display_target_options,
                        "value": _normalize_display_targets(rule.get("display_targets") or rule.get("display_target")),
                    },
                ],
            },
        ],
    }



def _entry_sensor_form(
    rule: Dict[str, Any],
    catalog: Dict[str, List[Tuple[str, str]]],
    notification_catalog: Dict[str, Any],
    rules: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    camera_options = _choices_from_pairs(
        catalog.get("cameras") or [],
        placeholder="(Optional snapshot camera)",
        current_value=_text(rule.get("camera_entity")),
    )
    sensor_options, sensor_dependency = _entry_sensor_dependency_options(
        catalog=catalog,
        current_type=_text(rule.get("sensor_type") or "door"),
        current_entity=_text(rule.get("sensor_entity") or rule.get("trigger_entity")),
    )
    area_options = _area_options(
        rules,
        current_value=_text(rule.get("area")),
    )
    sensor_type = _text(rule.get("sensor_type") or "door").lower()
    if sensor_type not in {"door", "window", "garage"}:
        sensor_type = "door"
    notify_destination_options = _notification_destination_options(
        rule.get("notification_targets") or rule.get("notification_destinations"),
        device_services=rule.get("device_services") or rule.get("device_service"),
        catalog=notification_catalog,
    )
    notify_destination_values = _notification_target_values(
        rule.get("notification_targets") or rule.get("notification_destinations"),
        device_services=rule.get("device_services") or rule.get("device_service"),
    )
    display_target_options = _display_target_options(rule.get("display_targets") or rule.get("display_target"))
    return {
        "id": rule["id"],
        "group": "entry_sensor",
        "title": _text(rule.get("name")) or "Entry sensor rule",
        "subtitle": _rule_subtitle(rule),
        "save_action": "awareness_save_rule",
        "remove_action": "awareness_remove_rule",
        "run_action": "awareness_run_now",
        "run_label": "Run Now",
        "remove_confirm": "Remove this entry sensor rule?",
        "fields": [
            {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": _bool(rule.get("enabled"), True)},
            {"key": "name", "label": "Rule Name", "type": "text", "value": _text(rule.get("name"))},
            {
                "key": "sensor_type",
                "label": "Sensor Type",
                "type": "select",
                "options": [
                    {"value": "door", "label": "Door"},
                    {"value": "window", "label": "Window"},
                    {"value": "garage", "label": "Garage"},
                ],
                "value": sensor_type,
            },
            {
                "key": "sensor_entity",
                "label": "Sensor Entity",
                "type": "select",
                "options": sensor_options,
                "dependent_options": sensor_dependency,
                "value": _text(rule.get("sensor_entity") or rule.get("trigger_entity")),
            },
            {
                "key": "camera_entity",
                "label": "Snapshot Camera (optional)",
                "type": "select",
                "options": camera_options,
                "value": _text(rule.get("camera_entity")),
            },
            {
                "key": "area",
                "label": "Area",
                "type": "select",
                "options": area_options,
                "value": _text(rule.get("area")),
            },
            {
                "key": "notifications",
                "label": "Send Notifications (open only)",
                "type": "checkbox",
                "value": _bool(rule.get("notifications"), False),
            },
        ],
        "sections": [
            {
                "label": "Playback (open only)",
                "fields": _announcement_tts_fields(
                    rule,
                    catalog,
                    players_description="When the sensor opens, speak using the shared announcement voice on selected Voice Core satellites, Sonos speakers, Home Assistant media players, or UniFi Protect camera speakers.",
                ),
            },
            {
                "label": "Notifications",
                "fields": [
                    {
                        "key": "title",
                        "label": "Notification Title",
                        "type": "text",
                        "value": _text(rule.get("title") or "Entry Sensor"),
                    },
                    {
                        "key": "priority",
                        "label": "Priority",
                        "type": "select",
                        "options": [{"value": "high", "label": "High"}, {"value": "normal", "label": "Normal"}],
                        "value": _text(rule.get("priority") or "normal"),
                    },
                    {
                        "key": "notification_targets",
                        "label": "Notification Destinations",
                        "type": "multiselect",
                        "description": "Choose one or more notifier destinations from Tater's shared notification catalog.",
                        "options": notify_destination_options,
                        "value": notify_destination_values,
                    },
                    {
                        "key": "display_notifications",
                        "label": "Show On ESPHome Displays",
                        "type": "checkbox",
                        "value": _bool(rule.get("display_notifications"), False),
                    },
                    {
                        "key": "display_targets",
                        "label": "Display Targets",
                        "type": "multiselect",
                        "description": "Choose one or more S3Box display targets. Leave empty to use all displays when enabled.",
                        "options": display_target_options,
                        "value": _normalize_display_targets(rule.get("display_targets") or rule.get("display_target")),
                    },
                ],
            },
        ],
    }



def _awareness_manager_ui(client: Any) -> Dict[str, Any]:
    all_rules = _load_rules(client)
    rules: Dict[str, Dict[str, Any]] = dict(all_rules)
    catalog = _entity_catalog(provider="all")
    notification_catalog = _notification_destination_catalog(client)
    event_page = _event_page_for_ui(client)
    event_forms = list(event_page.get("items") or [])
    forms: List[Dict[str, Any]] = []
    for rule in sorted(
        rules.values(),
        key=lambda row: (_text(row.get("kind")), _text(row.get("name")).lower(), _text(row.get("id"))),
    ):
        kind = _text(rule.get("kind")).lower()
        if kind == "camera":
            forms.append(_camera_form(rule, catalog, notification_catalog, rules))
        elif kind == "doorbell":
            forms.append(_doorbell_form(rule, catalog, notification_catalog, rules))
        elif kind == "entry_sensor":
            forms.append(_entry_sensor_form(rule, catalog, notification_catalog, rules))
    all_forms = event_forms + forms
    camera_options = _choices_from_pairs(catalog.get("cameras") or [], placeholder="(Select camera)")
    area_options = _area_options(rules)
    trigger_options, trigger_dependency = _trigger_dependency_for_camera(
        catalog=catalog,
        current_camera="",
        current_triggers=[],
        doorbell=False,
    )
    doorbell_trigger_options, doorbell_trigger_dependency = _trigger_dependency_for_camera(
        catalog=catalog,
        current_camera="",
        current_triggers=[],
        doorbell=True,
    )
    entry_sensor_options, entry_sensor_dependency = _entry_sensor_dependency_options(
        catalog=catalog,
        current_type="door",
        current_entity="",
    )
    notify_destination_options = _notification_destination_options(catalog=notification_catalog)
    display_target_options = _display_target_options()
    event_filters = _event_type_filters(client)
    event_list_view = _event_list_view_enabled(client)
    show_camera = {"source_key": "kind", "equals": "camera"}
    show_doorbell = {"source_key": "kind", "equals": "doorbell"}
    show_entry = {"source_key": "kind", "equals": "entry_sensor"}
    show_doorbell_or_entry = {"source_key": "kind", "any_of": ["doorbell", "entry_sensor"]}
    show_camera_or_doorbell = {"source_key": "kind", "any_of": ["camera", "doorbell"]}
    show_camera_or_doorbell_or_entry = {"source_key": "kind", "any_of": ["camera", "doorbell", "entry_sensor"]}
    add_tts_fields = _announcement_tts_add_fields(
        catalog,
        show_when=show_doorbell_or_entry,
        players_description="Speak using the shared announcement voice on selected Voice Core satellites, Sonos speakers, Home Assistant media players, or UniFi Protect camera speakers.",
    )
    return {
        "kind": "settings_manager",
        "title": "Awareness Automations",
        "empty_message": "No awareness rules configured yet.",
        "stats_refresh_button": True,
        "stats_refresh_label": "Refresh",
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
        "item_fields_dropdown_label": "Rule Settings",
        "item_fields_popup": True,
        "item_fields_popup_label": "Rule Settings",
        "item_sections_in_dropdown": True,
        "default_tab": "events",
        "manager_tabs": [
            {
                "key": "events",
                "label": "Events",
                "source": "items",
                "item_group": "event",
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
                "key": "cameras",
                "label": "Cameras",
                "source": "items",
                "item_group": "camera",
                "selector": False,
                "empty_message": "No camera rules configured.",
            },
            {
                "key": "doorbells",
                "label": "Doorbells",
                "source": "items",
                "item_group": "doorbell",
                "selector": True,
                "selector_label": "Select Doorbell Rule",
                "empty_message": "No doorbell rules configured.",
            },
            {
                "key": "entries",
                "label": "Entry Sensors",
                "source": "items",
                "item_group": "entry_sensor",
                "selector": False,
                "empty_message": "No entry sensor rules configured.",
            },
            {"key": "create", "label": "Create Rule", "source": "add_form"},
        ],
        "add_form": {
            "action": "awareness_add_rule",
            "submit_label": "Add Rule",
            "fields": [
                {
                    "key": "kind",
                    "label": "Rule Type",
                    "type": "select",
                    "options": [
                        {"value": "camera", "label": "Camera"},
                        {"value": "doorbell", "label": "Doorbell"},
                        {"value": "entry_sensor", "label": "Door/Window/Garage Sensor"},
                    ],
                    "value": "camera",
                },
                {"key": "name", "label": "Name", "type": "text", "value": ""},
                {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": True},
                {
                    "key": "camera_entity",
                    "label": "Camera / Doorbell Camera",
                    "type": "select",
                    "options": camera_options,
                    "value": "",
                    "description": "Optional for entry sensors: capture a snapshot on each sensor event.",
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                {
                    "key": "area",
                    "label": "Area",
                    "type": "select",
                    "options": area_options,
                    "value": "",
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                {
                    "key": "sensor_type",
                    "label": "Sensor Type",
                    "type": "select",
                    "options": [
                        {"value": "door", "label": "Door"},
                        {"value": "window", "label": "Window"},
                        {"value": "garage", "label": "Garage"},
                    ],
                    "value": "door",
                    "show_when": show_entry,
                },
                {
                    "key": "sensor_entity",
                    "label": "Sensor Entity",
                    "type": "select",
                    "options": entry_sensor_options,
                    "dependent_options": entry_sensor_dependency,
                    "value": "",
                    "show_when": show_entry,
                },
                {
                    "key": "trigger_entities",
                    "label": "Trigger Entities",
                    "type": "multiselect",
                    "description": "Select one or more trigger entities. Any selected trigger will run this rule.",
                    "options": trigger_options,
                    "dependent_options": trigger_dependency,
                    "value": [],
                    "show_when": show_camera,
                },
                {
                    "key": "doorbell_trigger_entities",
                    "label": "Trigger Entities",
                    "type": "multiselect",
                    "description": "Select one or more trigger entities. Any selected trigger will run this rule.",
                    "options": doorbell_trigger_options,
                    "dependent_options": doorbell_trigger_dependency,
                    "value": [],
                    "show_when": show_doorbell,
                },
                {
                    "key": "cooldown_seconds",
                    "label": "Cooldown (sec)",
                    "type": "number",
                    "value": 30,
                    "show_when": show_camera,
                },
                {
                    "key": "notification_cooldown_seconds",
                    "label": "Notification Cooldown (sec)",
                    "type": "number",
                    "value": 0,
                    "show_when": show_camera,
                },
                {
                    "key": "ignore_vehicles",
                    "label": "Ignore Vehicles",
                    "type": "checkbox",
                    "value": False,
                    "show_when": show_camera,
                },
                {
                    "key": "notifications",
                    "label": "Send Notifications",
                    "type": "checkbox",
                    "value": False,
                    "show_when": show_doorbell_or_entry,
                },
                {
                    "key": "title",
                    "label": "Notification Title",
                    "type": "text",
                    "value": "Camera Event",
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                {
                    "key": "priority",
                    "label": "Priority",
                    "type": "select",
                    "options": [{"value": "high", "label": "High"}, {"value": "normal", "label": "Normal"}],
                    "value": "high",
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                {
                    "key": "notification_targets",
                    "label": "Notification Destinations",
                    "type": "multiselect",
                    "description": "Choose one or more notifier destinations from Tater's shared notification catalog.",
                    "options": notify_destination_options,
                    "value": [],
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                {
                    "key": "display_notifications",
                    "label": "Show On ESPHome Displays",
                    "type": "checkbox",
                    "value": False,
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                {
                    "key": "display_targets",
                    "label": "Display Targets",
                    "type": "multiselect",
                    "description": "Choose one or more S3Box display targets. Leave empty to use all displays when enabled.",
                    "options": display_target_options,
                    "value": [],
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                *add_tts_fields,
            ],
        },
        "item_forms": all_forms,
    }


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    event_stats = _event_stats_for_ui(client)
    counts = event_stats.get("counts") if isinstance(event_stats.get("counts"), dict) else {}
    total_count = _as_int(counts.get("total"), 0, minimum=0)
    camera_count = _as_int(counts.get("camera"), 0, minimum=0)
    doorbell_count = _as_int(counts.get("doorbell"), 0, minimum=0)
    sensor_count = _as_int(counts.get("sensor"), 0, minimum=0)
    other_count = _as_int(counts.get("other"), 0, minimum=0)
    source_count = _as_int(event_stats.get("source_count"), 0, minimum=0)
    last_event = _text(event_stats.get("last_event")) or "n/a"
    return {
        "summary": "Awareness event feed overview across camera, doorbell, and sensor activity.",
        "stats": [
            {"label": "Total Events", "value": total_count},
            {"label": "Camera Events", "value": camera_count},
            {"label": "Doorbell Events", "value": doorbell_count},
            {"label": "Sensor Events", "value": sensor_count},
            {"label": "Other Events", "value": other_count},
            {"label": "Event Sources", "value": source_count},
            {"label": "Last Event", "value": last_event},
        ],
        "items": [],
        "empty_message": "No awareness rules configured yet.",
        "ui": _awareness_manager_ui(client),
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def _value(values: Dict[str, Any], payload: Dict[str, Any], key: str, default: Any = "") -> Any:
    if key in values:
        return _unwrap_form_value(values.get(key))
    return _unwrap_form_value(payload.get(key, default))


def _build_rule_from_values(
    *,
    kind: str,
    values: Dict[str, Any],
    payload: Dict[str, Any],
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now_ts = time.time()
    previous = existing if isinstance(existing, dict) else {}
    explicit_provider = "provider" in values or "provider" in payload
    provider_value = _concrete_rule_provider(previous)
    base: Dict[str, Any] = {
        "id": _text(previous.get("id")) or str(uuid.uuid4()),
        "kind": kind,
        "provider": provider_value,
        "enabled": _bool(_value(values, payload, "enabled", previous.get("enabled", True)), True),
        "name": _text(_value(values, payload, "name", previous.get("name"))) or f"{kind.title()} rule",
        "created_at": _as_float(previous.get("created_at"), now_ts),
        "updated_at": now_ts,
        "last_run_ts": _as_float(previous.get("last_run_ts"), 0.0),
        "last_status": _text(previous.get("last_status")),
        "last_summary": _text(previous.get("last_summary")),
        "last_error": _text(previous.get("last_error")),
    }
    trigger_entities_default = _value(
        values,
        payload,
        "trigger_entities",
        previous.get("trigger_entities", previous.get("trigger_entity", "")),
    )
    if kind == "doorbell":
        trigger_entities_default = _value(values, payload, "doorbell_trigger_entities", trigger_entities_default)
    if kind == "entry_sensor":
        trigger_entities_default = _value(values, payload, "sensor_entity", trigger_entities_default)
    trigger_entities = _normalize_trigger_entities(trigger_entities_default)
    if not trigger_entities:
        legacy_trigger_default = _value(values, payload, "trigger_entity", previous.get("trigger_entity", ""))
        if kind == "doorbell":
            legacy_trigger_default = _value(values, payload, "doorbell_trigger_entity", legacy_trigger_default)
        if kind == "entry_sensor":
            legacy_trigger_default = _value(values, payload, "sensor_entity", legacy_trigger_default)
        trigger_entities = _normalize_trigger_entities(legacy_trigger_default)
    if kind == "doorbell":
        title_default = "Doorbell"
    elif kind == "entry_sensor":
        title_default = "Entry Sensor"
    else:
        title_default = "Camera Event"
    priority_default = "normal" if kind in {"doorbell", "entry_sensor"} else "high"
    trigger_to_state_default = "on" if kind == "camera" else ""
    notifications_default = False if kind == "entry_sensor" else True
    priority_value = _text(_value(values, payload, "priority", previous.get("priority", priority_default))).lower()
    notification_targets_present = "notification_targets" in values or "notification_targets" in payload
    notification_targets_value = _value(
        values,
        payload,
        "notification_targets",
        previous.get("notification_targets", previous.get("notification_destinations", "")),
    )
    device_services_value = _value(
        values,
        payload,
        "device_services",
        previous.get("device_services", previous.get("device_service", "")),
    )
    if notification_targets_present:
        notification_targets = _normalize_notification_targets(notification_targets_value)
    else:
        notification_targets = _normalize_notification_targets(
            notification_targets_value,
            device_services=device_services_value,
        )
    display_targets_value = _value(
        values,
        payload,
        "display_targets",
        previous.get("display_targets", previous.get("display_target", "")),
    )
    base.update(
        {
            "camera_entity": _text(_value(values, payload, "camera_entity", previous.get("camera_entity", ""))),
            "area": _text(_value(values, payload, "area", previous.get("area", ""))),
            "trigger_entities": trigger_entities,
            "trigger_entity": trigger_entities[0] if trigger_entities else "",
            "trigger_to_state": _text(
                _value(values, payload, "trigger_to_state", previous.get("trigger_to_state", trigger_to_state_default))
            ),
            "trigger_attribute": _text(
                _value(values, payload, "trigger_attribute", previous.get("trigger_attribute", ""))
            ),
            "trigger_attribute_value": _text(
                _value(
                    values,
                    payload,
                    "trigger_attribute_value",
                    previous.get("trigger_attribute_value", ""),
                )
            ),
            "query": _text(_value(values, payload, "query", previous.get("query", ""))),
            "cooldown_seconds": _as_int(
                _value(values, payload, "cooldown_seconds", previous.get("cooldown_seconds", 30)),
                30,
                minimum=0,
                maximum=86400,
            ),
            "notification_cooldown_seconds": _as_int(
                _value(
                    values,
                    payload,
                    "notification_cooldown_seconds",
                    previous.get("notification_cooldown_seconds", 0),
                ),
                0,
                minimum=0,
                maximum=86400,
            ),
            "ignore_vehicles": _bool(
                _value(values, payload, "ignore_vehicles", previous.get("ignore_vehicles", False)),
                False,
            ),
            "title": _text(_value(values, payload, "title", previous.get("title", title_default))),
            "priority": "high" if priority_value in {"critical", "high"} else "normal",
            "device_services": _device_services_from_notification_targets(notification_targets),
            "notification_targets": notification_targets,
            "display_notifications": _bool(
                _value(values, payload, "display_notifications", previous.get("display_notifications", False)),
                False,
            ),
            "display_targets": _normalize_display_targets(display_targets_value),
            "players": _normalize_players(_value(values, payload, "players", previous.get("players", []))),
            "notifications": _bool(
                _value(values, payload, "notifications", previous.get("notifications", notifications_default)),
                notifications_default,
            ),
            "sensor_type": _text(_value(values, payload, "sensor_type", previous.get("sensor_type", "door"))).lower(),
            "sensor_entity": _text(
                _value(
                    values,
                    payload,
                    "sensor_entity",
                    previous.get("sensor_entity", trigger_entities[0] if trigger_entities else ""),
                )
            ),
        }
    )
    inferred_provider = _provider_from_rule_fields(base)
    if explicit_provider:
        provider_value = _concrete_rule_provider(
            {**base, "provider": _value(values, payload, "provider", provider_value)}
        )
    elif inferred_provider:
        provider_value = inferred_provider
    base["provider"] = provider_value
    if not _text(base.get("provider")):
        raise ValueError("Select devices from a connected integration so Awareness can save the provider.")
    if kind == "doorbell":
        base["trigger_to_state"] = "on"
        base["trigger_attribute"] = ""
        base["trigger_attribute_value"] = ""
    if kind == "camera":
        if not base["camera_entity"]:
            raise ValueError("Camera entity is required for camera rules.")
        if not base["trigger_entities"]:
            raise ValueError("At least one trigger entity is required for camera rules.")
        if not base["area"]:
            base["area"] = "camera"
    elif kind == "doorbell":
        if not base["camera_entity"]:
            raise ValueError("Camera entity is required for doorbell rules.")
        if not base["trigger_entities"]:
            raise ValueError("At least one trigger entity is required for doorbell rules.")
        if not base["players"]:
            raise ValueError("At least one playback target is required for doorbell rules.")

        if not base["area"]:
            base["area"] = "front door"
    elif kind == "entry_sensor":
        if not base["trigger_entities"]:
            raise ValueError("Sensor entity is required for entry sensor rules.")
        base["sensor_entity"] = base["trigger_entities"][0]
        if base["sensor_type"] not in {"door", "window", "garage"}:
            base["sensor_type"] = "door"

        if not base["area"]:
            base["area"] = base["sensor_type"]
    return _normalize_rule(base) or base


def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    if client is None:
        raise ValueError("Redis connection is unavailable.")
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)
    action_name = _text(action).lower()
    if action_name == "awareness_refresh_entities":
        _entity_catalog(force_refresh=True, provider="all")
        return {"ok": True, "message": "Integration device catalog refreshed."}
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
    if action_name == "awareness_add_rule":
        kind = _text(_value(values, body, "kind")).lower()
        if kind not in {"camera", "doorbell", "entry_sensor"}:
            raise ValueError("Rule type must be camera, doorbell, or entry_sensor.")
        rule = _build_rule_from_values(kind=kind, values=values, payload=body, existing=None)
        _save_rule(client, rule)
        return {"ok": True, "id": rule["id"], "message": "Awareness rule added."}
    if action_name == "awareness_save_rule":
        rule_id = _text(body.get("id"))
        if not rule_id:
            raise ValueError("Rule id is required.")
        existing = _get_rule(client, rule_id)
        if not existing:
            raise KeyError("Awareness rule not found.")
        kind = _text(existing.get("kind")).lower()
        rule = _build_rule_from_values(kind=kind, values=values, payload=body, existing=existing)
        _save_rule(client, rule)
        return {"ok": True, "id": rule_id, "message": "Awareness rule saved."}
    if action_name == "awareness_remove_rule":
        rule_id = _text(body.get("id"))
        if not rule_id:
            raise ValueError("Rule id is required.")
        deleted = _remove_rule(client, rule_id)
        if not deleted:
            raise KeyError("Awareness rule not found.")
        return {"ok": True, "id": rule_id, "message": "Awareness rule removed."}
    if action_name == "awareness_run_now":
        rule_id = _text(body.get("id"))
        if not rule_id:
            raise ValueError("Rule id is required.")
        existing = _get_rule(client, rule_id)
        if not existing:
            raise KeyError("Awareness rule not found.")
        event: Dict[str, Any] = {}
        if _text(existing.get("kind")).lower() == "entry_sensor":
            entity_id = _text(existing.get("sensor_entity") or existing.get("trigger_entity"))
            friendly_name = _text(existing.get("name") or existing.get("area") or "Entry Sensor")
            event = {
                "entity_id": entity_id,
                "new_state": {"state": "open", "attributes": {"friendly_name": friendly_name}},
                "old_state": {"state": "closed", "attributes": {"friendly_name": friendly_name}},
            }
        _enqueue_execution(client, rule_id=rule_id, reason="manual", event=event)
        return {"ok": True, "id": rule_id, "message": "Awareness rule queued to run now."}
    raise ValueError(f"Unknown action: {action_name}")


async def _awareness_worker_loop(stop_event: Optional[object], llm_client: Any) -> None:
    while not (stop_event and stop_event.is_set()):
        job = _dequeue_execution(redis_client)
        if not job:
            await asyncio.sleep(0.25)
            continue
        _runtime_set(redis_client, queue_depth=_queue_depth(redis_client))
        rule_id = _text(job.get("rule_id"))
        reason = _text(job.get("reason") or "manual")
        event_payload = job.get("event") if isinstance(job.get("event"), dict) else {}
        rule = _get_rule(redis_client, rule_id)
        if not rule:
            logger.info("[awareness] skipping queued run for missing rule %s", rule_id)
            continue
        if not _bool(rule.get("enabled"), True) and reason != "manual":
            continue
        try:
            startup_defer = _camera_startup_defer_remaining(redis_client, rule, reason)
            if startup_defer > 0.0:
                requeued = _enqueue_execution_after_delay(
                    redis_client,
                    rule_id=rule_id,
                    reason=f"{reason or 'trigger'}_startup_deferred",
                    event=event_payload,
                    delay_seconds=startup_defer + 1.0,
                )
                result = {
                    "ok": True,
                    "summary": (
                        f"Camera vision warmup active; requeued in {int(startup_defer) + 1}s."
                        if requeued
                        else "Camera vision warmup active; already requeued."
                    ),
                    "skipped": "startup_warmup",
                }
            else:
                result = await _execute_rule(rule, llm_client, reason, event_payload)
            now_ts = time.time()
            rule_updates = result.get("rule_updates") if isinstance(result, dict) else {}
            if isinstance(rule_updates, dict):
                for key, value in rule_updates.items():
                    rule[key] = value
            rule["last_run_ts"] = now_ts
            rule["last_status"] = "ok"
            rule["last_summary"] = _text(result.get("summary"))
            rule["last_error"] = ""
            rule["updated_at"] = now_ts
            _save_rule(redis_client, rule)
            _runtime_set(redis_client, last_run_ts=now_ts, last_error="")
            summary = _compact(_text(result.get("summary")), limit=180)
            rule_kind = _text(rule.get("kind")) or "unknown"
            if summary:
                logger.info("[awareness] rule %s (%s) ran via %s: %s", rule_id, rule_kind, reason, summary)
            else:
                logger.info("[awareness] rule %s (%s) ran via %s", rule_id, rule_kind, reason)
        except Exception as exc:
            now_ts = time.time()
            logger.exception("[awareness] rule execution failed for %s", rule_id)
            rule["last_run_ts"] = now_ts
            rule["last_status"] = "error"
            rule["last_error"] = str(exc)
            rule["updated_at"] = now_ts
            _save_rule(redis_client, rule)
            _runtime_set(redis_client, last_run_ts=now_ts, last_error=str(exc))


async def _handle_trigger_state_change(
    *,
    provider: str,
    entity_id: str,
    new_state: Dict[str, Any],
    old_state: Dict[str, Any],
) -> None:
    provider_token = _normalize_event_provider(provider)
    rules = _load_rules(redis_client)
    for rule in rules.values():
        kind = _text(rule.get("kind"))
        if kind not in {"camera", "doorbell", "entry_sensor"}:
            continue
        if not _bool(rule.get("enabled"), True):
            continue
        try:
            if not _rule_matches_event(
                rule,
                provider=provider_token,
                entity_id=entity_id,
                new_state=new_state,
                old_state=old_state,
            ):
                continue
        except Exception:
            logger.exception("[awareness] trigger match failed for rule %s", rule.get("id"))
            continue
        dedupe_key = (
            f"awareness:dedupe:{provider_token}:{_text(rule.get('id'))}:{entity_id}:{_text(new_state.get('state')).lower()}"
        )
        if redis_client.set(dedupe_key, "1", ex=2, nx=True) is None:
            continue
        _enqueue_execution(
            redis_client,
            rule_id=_text(rule.get("id")),
            reason="trigger",
            event={"entity_id": entity_id, "new_state": new_state, "old_state": old_state},
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


def _unifi_camera_motion_marker(camera_row: Dict[str, Any]) -> str:
    for key in (
        "lastMotionEventId",
        "last_motion_event_id",
        "lastMotionEvent",
        "last_motion_event",
        "lastMotionAt",
        "last_motion_at",
        "lastMotionTs",
        "last_motion_ts",
        "lastMotion",
        "last_motion",
        "lastSmartDetect",
        "last_smart_detect",
        "lastSmartDetectAt",
        "last_smart_detect_at",
    ):
        marker = _unifi_marker_token(camera_row.get(key))
        if marker:
            return f"{key}:{marker}"
    for key, value in camera_row.items():
        key_text = _text(key)
        key_lower = key_text.lower()
        if "motion" not in key_lower:
            continue
        if any(token in key_lower for token in ("enable", "enabled", "setting", "sensitivity", "recording", "zone")):
            continue
        if not any(token in key_lower for token in ("event", "detect", "last", "time", "at", "ts", "id", "trigger")):
            continue
        marker = _unifi_marker_token(value)
        if marker:
            return f"{key_text}:{marker}"
    nested_marker = _unifi_nested_keyword_marker(
        camera_row,
        include_any=("motion",),
        must_have_any=("event", "detect", "last", "time", "at", "ts", "id", "trigger"),
        exclude_any=("enable", "enabled", "setting", "sensitivity", "recording", "zone"),
    )
    if nested_marker:
        return f"nested:{nested_marker}"
    for key in (
        "isMotionDetected",
        "is_motion_detected",
        "motionDetected",
        "motion_detected",
        "isPirMotionDetected",
        "is_pir_motion_detected",
    ):
        if key in camera_row and camera_row.get(key) is not None:
            return f"{key}:{1 if bool(camera_row.get(key)) else 0}"
    return ""


def _unifi_camera_doorbell_marker(camera_row: Dict[str, Any]) -> str:
    for key in (
        "lastRingEventId",
        "last_ring_event_id",
        "lastRingEventIds",
        "last_ring_event_ids",
        "lastRingEvent",
        "last_ring_event",
        "lastRingAt",
        "last_ring_at",
        "lastRingTs",
        "last_ring_ts",
        "lastRing",
        "last_ring",
        "lastDoorbellRing",
        "last_doorbell_ring",
        "lastDoorbellRingAt",
        "last_doorbell_ring_at",
        "lastDoorbellRingEventId",
        "last_doorbell_ring_event_id",
        "lastDoorbellPress",
        "last_doorbell_press",
        "lastDoorbellPressAt",
        "last_doorbell_press_at",
        "lastDoorbellPressEventId",
        "last_doorbell_press_event_id",
    ):
        marker = _unifi_marker_token(camera_row.get(key))
        if marker:
            return f"{key}:{marker}"
    for key, value in camera_row.items():
        key_text = _text(key)
        key_lower = key_text.lower()
        if not any(token in key_lower for token in ("ring", "press")):
            continue
        if any(token in key_lower for token in ("enable", "enabled", "setting", "sensitivity")):
            continue
        if not any(token in key_lower for token in ("event", "last", "time", "at", "ts", "id", "press")):
            continue
        if isinstance(value, bool) and not value:
            continue
        marker = _unifi_marker_token(value)
        if marker.lower() in {"0", "false", "off", "none", "null", "nan"}:
            continue
        if marker:
            return f"{key_text}:{marker}"
    nested_marker = _unifi_nested_keyword_marker(
        camera_row,
        include_any=("ring", "press"),
        must_have_any=("event", "last", "time", "at", "ts", "id", "press", "ring"),
        exclude_any=("enable", "enabled", "setting", "sensitivity"),
    )
    if nested_marker:
        return f"nested:{nested_marker}"
    for key in ("isRinging", "is_ringing", "isDoorbellRinging", "is_doorbell_ringing"):
        if key in camera_row and bool(camera_row.get(key)):
            return f"{key}:1"
    return ""


def _unifi_active_rule_smart_types_by_camera() -> Dict[str, set[str]]:
    out: Dict[str, set[str]] = {}
    try:
        rules = _load_rules(redis_client)
    except Exception:
        return out
    for rule in rules.values():
        if not isinstance(rule, dict):
            continue
        if _text(rule.get("kind")).lower() != "camera":
            continue
        if not _bool(rule.get("enabled"), True):
            continue
        if _rule_provider(rule) != "unifi_protect":
            continue
        camera_id = _text(_unifi_camera_id_from_entity(_text(rule.get("camera_entity")))).lower()
        if not camera_id:
            continue
        prefix = f"binary_sensor.unifi_{camera_id}_smart_"
        trigger_entities = _normalize_trigger_entities(rule.get("trigger_entities") or rule.get("trigger_entity"))
        for trigger in trigger_entities:
            token = _text(trigger).lower()
            if not token.startswith(prefix):
                continue
            smart_type = _unifi_normalize_smart_type(token[len(prefix) :])
            if not smart_type:
                continue
            out.setdefault(camera_id, set()).add(smart_type)
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


def _catalog_label_name(label: Any, fallback: str) -> str:
    text = _text(label)
    if not text:
        return fallback
    if "(" in text and text.endswith(")"):
        prefix = text.rsplit("(", 1)[0].strip()
        if prefix:
            return prefix
    return text


def _unifi_name_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    camera_names: Dict[str, str] = {}
    sensor_names: Dict[str, str] = {}
    try:
        catalog = _entity_catalog(provider="unifi_protect")
    except Exception:
        return camera_names, sensor_names

    for entity, label in catalog.get("cameras") or []:
        camera_id = _text(_unifi_camera_id_from_entity(entity)).lower()
        if not camera_id:
            continue
        camera_names[camera_id] = _catalog_label_name(label, camera_id)

    for entity, label in catalog.get("entry_sensors") or []:
        sensor_id = _text(_unifi_sensor_id_from_entity(entity)).lower()
        if not sensor_id:
            continue
        sensor_names[sensor_id] = _catalog_label_name(label, sensor_id)

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
        if not smart_types:
            smart_types = set(_unifi_active_rule_smart_types_by_camera().get(camera_id) or set())
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
    rules = _load_rules(redis_client)
    enabled_rules = sum(1 for rule in rules.values() if _bool(rule.get("enabled"), True))
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
        "[awareness] core started v%s (%s) (rules=%d enabled=%d workers=%d)",
        __version__,
        __file__,
        len(rules),
        enabled_rules,
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
            "description": "Look up event history for activity around your home, including doors, windows, garage, and inside and outside camera areas.",
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
