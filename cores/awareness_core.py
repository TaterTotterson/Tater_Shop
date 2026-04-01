import asyncio
import base64
import json
import logging
import re
import threading
import time
import uuid
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import aiohttp
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv

from helpers import get_llm_client_from_env, redis_client
from notify import dispatch_notification
from vision_settings import get_vision_settings as get_shared_vision_settings

__version__ = "1.1.14"

load_dotenv()

logger = logging.getLogger("awareness_core")
logger.setLevel(logging.INFO)

CORE_SETTINGS = {
    "category": "Awareness Core Settings",
    "required": {
        "ws_reconnect_seconds": {
            "label": "HA WS Reconnect (sec)",
            "type": "number",
            "default": 5,
            "description": "Seconds between Home Assistant websocket reconnect attempts.",
        },
        "event_provider": {
            "label": "Event Provider",
            "type": "select",
            "options": ["homeassistant", "unifi_protect"],
            "default": "homeassistant",
            "description": "Select which provider powers camera/doorbell/sensor event rules.",
        },
        "unifi_poll_seconds": {
            "label": "UniFi Poll Interval (sec)",
            "type": "number",
            "default": 3,
            "description": "Seconds between UniFi Protect polling checks while UniFi provider is active.",
        },
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
        "brief_scheduler_tick_sec": {
            "label": "Brief Scheduler Tick (sec)",
            "type": "number",
            "default": 5,
            "description": "How often the brief scheduler checks for due jobs.",
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

_TRUE_TOKENS = {"1", "true", "yes", "on", "enabled", "y"}
_FALSE_TOKENS = {"0", "false", "no", "off", "disabled", "n"}
_SUPPORTED_EVENT_PROVIDERS = {"homeassistant", "unifi_protect"}
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
_UNIFI_HA_CAMERA_SUFFIXES = (
    "_person_detected",
    "_vehicle_detected",
    "_animal_detected",
    "_package_detected",
    "_object_detected",
    "_motion",
    "_doorbell",
    "_ring",
)

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
_EVENT_FILTER_DEFAULTS = {
    "camera": True,
    "doorbell": True,
    "sensor": True,
}


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
    if token in {"unifi", "protect"}:
        token = "unifi_protect"
    if token not in _SUPPORTED_EVENT_PROVIDERS:
        return "homeassistant"
    return token


def _event_provider(client: Any = None) -> str:
    return _normalize_event_provider(_settings(client).get("event_provider") or "homeassistant")


def _rule_provider(rule: Dict[str, Any], *, fallback: str = "homeassistant") -> str:
    kind = _text((rule or {}).get("kind")).lower()
    if kind == "brief":
        return "homeassistant"
    return _normalize_event_provider((rule or {}).get("provider") or fallback)


def _provider_label(provider: str) -> str:
    token = _normalize_event_provider(provider)
    if token == "unifi_protect":
        return "UniFi Protect"
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


def _normalize_players(value: Any) -> List[str]:
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
    else:
        text = str(value or "")
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


def _normalize_quote_history(value: Any, *, limit: int = 5) -> List[str]:
    raw_value = value
    if isinstance(value, str):
        token = value.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                raw_value = json.loads(token)
            except Exception:
                raw_value = value
    items = _normalize_players(raw_value)
    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        text = _compact(_text(item), limit=220)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= max(1, int(limit)):
            break
    return out


def _quote_history_with_new(history: Any, quote: str, *, limit: int = 5) -> List[str]:
    clean_quote = _compact(_text(quote), limit=220)
    merged: List[str] = []
    if clean_quote:
        merged.append(clean_quote)
    for item in _normalize_quote_history(history, limit=max(1, int(limit)) * 2):
        if clean_quote and item.lower() == clean_quote.lower():
            continue
        merged.append(item)
    return _normalize_quote_history(merged, limit=limit)


def _normalize_rule(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None

    kind = _text(raw.get("kind")).lower()
    if kind not in {"camera", "doorbell", "entry_sensor", "brief"}:
        return None

    rule_id = _text(raw.get("id")) or str(uuid.uuid4())
    now_ts = time.time()

    base: Dict[str, Any] = {
        "id": rule_id,
        "kind": kind,
        "provider": "homeassistant" if kind == "brief" else _normalize_event_provider(raw.get("provider") or "homeassistant"),
        "name": _text(raw.get("name")) or f"{kind.title()} rule",
        "enabled": _bool(raw.get("enabled"), True),
        "created_at": _as_float(raw.get("created_at"), now_ts),
        "updated_at": _as_float(raw.get("updated_at"), now_ts),
        "last_run_ts": _as_float(raw.get("last_run_ts"), 0.0),
        "last_status": _text(raw.get("last_status")),
        "last_summary": _text(raw.get("last_summary")),
        "last_error": _text(raw.get("last_error")),
    }

    if kind == "camera":
        trigger_entities = _normalize_trigger_entities(raw.get("trigger_entities"))
        if not trigger_entities:
            trigger_entities = _normalize_trigger_entities(raw.get("trigger_entity"))
        base.update(
            {
                "camera_entity": _text(raw.get("camera_entity")),
                "area": _text(raw.get("area")) or "camera",
                "trigger_entities": trigger_entities,
                "trigger_entity": trigger_entities[0] if trigger_entities else "",
                "trigger_to_state": _text(raw.get("trigger_to_state") or "on"),
                "trigger_attribute": _text(raw.get("trigger_attribute")),
                "trigger_attribute_value": _text(raw.get("trigger_attribute_value")),
                "query": _text(raw.get("query") or "brief camera alert"),
                "cooldown_seconds": _as_int(raw.get("cooldown_seconds"), 30, minimum=0, maximum=86400),
                "notification_cooldown_seconds": _as_int(
                    raw.get("notification_cooldown_seconds"), 0, minimum=0, maximum=86400
                ),
                "ignore_vehicles": _bool(raw.get("ignore_vehicles"), False),
                "title": _text(raw.get("title") or "Camera Event"),
                "priority": "high"
                if _text(raw.get("priority") or "high").lower() in {"critical", "high"}
                else "normal",
                "api_notification": _bool(raw.get("api_notification"), False),
                "device_services": _normalize_device_services(raw.get("device_services") or raw.get("device_service")),
            }
        )
        return base

    if kind == "doorbell":
        trigger_entities = _normalize_trigger_entities(raw.get("trigger_entities"))
        if not trigger_entities:
            trigger_entities = _normalize_trigger_entities(raw.get("trigger_entity"))
        base.update(
            {
                "camera_entity": _text(raw.get("camera_entity")),
                "area": _text(raw.get("area")) or "front door",
                "trigger_entities": trigger_entities,
                "trigger_entity": trigger_entities[0] if trigger_entities else "",
                "trigger_to_state": _text(raw.get("trigger_to_state") or "on"),
                "trigger_attribute": _text(raw.get("trigger_attribute")),
                "trigger_attribute_value": _text(raw.get("trigger_attribute_value")),
                "tts_entity": _text(raw.get("tts_entity") or "tts.piper"),
                "players": _normalize_players(raw.get("players")),
                "title": _text(raw.get("title") or "Doorbell"),
                "priority": "high"
                if _text(raw.get("priority") or "normal").lower() in {"critical", "high"}
                else "normal",
                "notifications": _bool(raw.get("notifications"), True),
                "api_notification": _bool(raw.get("api_notification"), False),
                "device_services": _normalize_device_services(raw.get("device_services") or raw.get("device_service")),
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
        base.update(
            {
                "sensor_entity": trigger_entities[0] if trigger_entities else sensor_entity,
                "sensor_type": sensor_type,
                "area": area_value or sensor_type,
                "trigger_entities": trigger_entities,
                "trigger_entity": trigger_entities[0] if trigger_entities else "",
                "trigger_to_state": _text(raw.get("trigger_to_state")),
                "tts_entity": _text(raw.get("tts_entity") or "tts.piper"),
                "players": _normalize_players(raw.get("players")),
                "title": _text(raw.get("title") or "Entry Sensor"),
                "priority": "high"
                if _text(raw.get("priority") or "normal").lower() in {"critical", "high"}
                else "normal",
                "notifications": _bool(raw.get("notifications"), False),
                "api_notification": _bool(raw.get("api_notification"), False),
                "device_services": _normalize_device_services(raw.get("device_services") or raw.get("device_service")),
            }
        )
        return base

    interval = _as_int(raw.get("interval_minutes"), 60, minimum=1, maximum=10080)
    next_run_ts = _as_float(raw.get("next_run_ts"), 0.0)
    if next_run_ts <= 0:
        next_run_ts = now_ts + (interval * 60)

    base.update(
        {
            "brief_kind": _text(raw.get("brief_kind") or "events_query_brief").lower(),
            "interval_minutes": interval,
            "next_run_ts": next_run_ts,
            "input_text_entity": _text(raw.get("input_text_entity")),
            "timeframe": _text(raw.get("timeframe") or "today").lower(),
            "area": _text(raw.get("area")),
            "hours": _as_int(raw.get("hours"), 12, minimum=1, maximum=72),
            "weather_temp_entity": _text(raw.get("weather_temp_entity")),
            "weather_wind_entity": _text(raw.get("weather_wind_entity")),
            "weather_rain_entity": _text(raw.get("weather_rain_entity")),
            "query": _text(raw.get("query")),
            "include_date": _bool(raw.get("include_date"), False),
            "tone": _text(raw.get("tone") or "zen"),
            "prompt_hint": _text(raw.get("prompt_hint")),
            "max_chars": _as_int(raw.get("max_chars"), 100, minimum=40, maximum=240),
            "quote_history": _normalize_quote_history(raw.get("quote_history") or raw.get("last_quotes"), limit=5),
        }
    )
    return base


def _load_rules(client: Any) -> Dict[str, Dict[str, Any]]:
    redis_obj = client or redis_client
    raw = redis_obj.hgetall(_RULES_KEY) if redis_obj else {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for _, row in raw.items():
        try:
            payload = json.loads(row)
        except Exception:
            continue
        normalized = _normalize_rule(payload)
        if not normalized:
            continue
        out[normalized["id"]] = normalized
    return out


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
    settings = redis_client.hgetall("homeassistant_settings") or {}
    base = _text(settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
    token = _text(settings.get("HA_TOKEN"))
    if not token:
        raise ValueError(
            "Home Assistant token is not set. Open WebUI -> Settings -> Home Assistant Settings and add HA_TOKEN."
        )
    return {"base": base, "token": token}


def _unifi_protect_config() -> Dict[str, str]:
    base = _text(redis_client.get("tater:unifi_protect:base_url") or "https://10.4.20.127").rstrip("/")
    api_key = _text(redis_client.get("tater:unifi_protect:api_key"))
    if not api_key:
        raise ValueError(
            "UniFi Protect API key is not set. Open WebUI -> Settings -> Integrations -> UniFi Protect and add API key."
        )
    if not base:
        base = "https://10.4.20.127"
    return {"base": base, "api_key": api_key}


def _unifi_headers(api_key: str, *, json_content: bool = True) -> Dict[str, str]:
    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    if json_content:
        headers["Content-Type"] = "application/json"
    return headers


def _unifi_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    stream: bool = False,
) -> Any:
    conf = _unifi_protect_config()
    url_path = path if str(path).startswith("/") else f"/{path}"
    url = f"{conf['base']}{url_path}"
    req_headers = _unifi_headers(conf["api_key"], json_content=not stream)
    if headers:
        req_headers.update(headers)
    # UniFi Protect currently uses unverified TLS in this integration path.
    # Suppress urllib3's repetitive warning spam for this known request flow.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", InsecureRequestWarning)
        resp = requests.request(
            method,
            url,
            headers=req_headers,
            params=params,
            json=json_body,
            timeout=20,
            verify=False,
            stream=stream,
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"UniFi Protect HTTP {resp.status_code}: {resp.text[:200]}")
    if stream:
        return resp.content, resp.headers
    try:
        return resp.json()
    except Exception:
        return {}


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


def _unifi_camera_id_from_ha_entity(entity_id: str, state_obj: Dict[str, Any]) -> str:
    object_id = _entity_object_id(entity_id)
    attrs = state_obj.get("attributes") if isinstance(state_obj, dict) else {}
    if not isinstance(attrs, dict):
        attrs = {}

    friendly = _text(attrs.get("friendly_name"))
    cleaned = object_id
    for suffix in _UNIFI_HA_CAMERA_SUFFIXES:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]
            break
    candidates = [friendly, object_id.replace("_", " "), cleaned.replace("_", " ")]
    index = _unifi_camera_name_index()
    if not index:
        return ""
    for candidate in candidates:
        key = _unifi_name_key(candidate)
        if not key:
            continue
        if key in index:
            return index[key]
        for known_key, camera_id in index.items():
            if not known_key:
                continue
            if key in known_key or known_key in key:
                return camera_id
    return ""


def _unifi_detect_type_from_ha_entity(entity_id: str, state_obj: Dict[str, Any]) -> str:
    entity = _text(entity_id).lower()
    object_id = _entity_object_id(entity)
    attrs = state_obj.get("attributes") if isinstance(state_obj, dict) else {}
    if not isinstance(attrs, dict):
        attrs = {}
    device_class = _text(attrs.get("device_class")).lower()
    source = " ".join(
        [
            entity,
            _text(attrs.get("friendly_name")).lower(),
            device_class,
            _text(attrs.get("event_type")).lower(),
            _text(attrs.get("event_types")).lower(),
        ]
    )
    if "person" in source:
        return "person"
    if "animal" in source:
        return "animal"
    if "vehicle" in source:
        return "vehicle"
    if "package" in source:
        return "package"
    if "motion" in source:
        return "motion"
    if any(token in source for token in ("ring", "pressed")):
        return "doorbell"
    if "doorbell" in source:
        if any(token in source for token in ("detect", "smart", "motion", "person", "animal", "vehicle", "package")):
            return ""
        if object_id.endswith("_doorbell") or object_id == "doorbell":
            return "doorbell"
        if device_class == "doorbell":
            return "doorbell"
    return ""


def _unifi_translate_ha_entity_to_trigger(entity_id: str, state_obj: Dict[str, Any]) -> str:
    entity = _text(entity_id).lower()
    if not entity.startswith(("binary_sensor.", "sensor.")):
        return ""
    detect_type = _unifi_detect_type_from_ha_entity(entity, state_obj)
    if not detect_type:
        return ""
    camera_id = _unifi_camera_id_from_ha_entity(entity, state_obj)
    if not camera_id:
        return ""
    if detect_type == "doorbell":
        return _unifi_camera_doorbell_trigger(camera_id)
    if detect_type == "motion":
        return _unifi_camera_motion_trigger(camera_id)
    return _unifi_camera_smart_trigger(camera_id, detect_type)


def _unifi_sensor_entity(sensor_id: str) -> str:
    return f"binary_sensor.unifi_sensor_{_text(sensor_id).lower()}"


def _unifi_list_cameras() -> List[Dict[str, Any]]:
    rows = _unifi_request("GET", "/proxy/protect/integration/v1/cameras")
    return rows if isinstance(rows, list) else []

def _unifi_list_sensors() -> List[Dict[str, Any]]:
    rows = _unifi_request("GET", "/proxy/protect/integration/v1/sensors")
    return rows if isinstance(rows, list) else []


def _ha_headers(token: str, *, json_content: bool = True) -> Dict[str, str]:
    headers = {"Authorization": f"Bearer {token}"}
    if json_content:
        headers["Content-Type"] = "application/json"
    return headers


def _ha_ws_url(base: str) -> str:
    if base.startswith("https://"):
        return base.replace("https://", "wss://", 1) + "/api/websocket"
    return base.replace("http://", "ws://", 1) + "/api/websocket"


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


def _rule_matches_event(rule: Dict[str, Any], *, entity_id: str, new_state: Dict[str, Any], old_state: Dict[str, Any]) -> bool:
    trigger_entities = [token.lower() for token in _normalize_trigger_entities(rule.get("trigger_entities"))]
    if not trigger_entities:
        legacy = _text(rule.get("trigger_entity")).lower()
        if legacy:
            trigger_entities = [legacy]
    event_entity = _text(entity_id).lower()
    if trigger_entities and event_entity not in trigger_entities:
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
    return f"tater:camera_event:last_ts:{camera_entity}"


def _camera_notify_cooldown_key(camera_entity: str) -> str:
    return f"tater:camera_event:last_notify_ts:{camera_entity}"


def _within_cooldown(key: str, cooldown_seconds: int) -> bool:
    try:
        last = redis_client.get(key)
        if not last:
            return False
        return (int(time.time()) - int(last)) < max(0, int(cooldown_seconds))
    except Exception:
        return False


def _mark_cooldown(key: str) -> None:
    try:
        redis_client.set(key, str(int(time.time())))
    except Exception:
        logger.debug("[awareness] failed to mark cooldown key %s", key, exc_info=True)


def _compact(text: str, limit: int = 220) -> str:
    out = re.sub(r"\s+", " ", _text(text))
    if len(out) <= limit:
        return out
    cut = out[:limit]
    if " " in cut[40:]:
        cut = cut[: cut.rfind(" ")]
    return cut.rstrip(".,;: ") + "..."


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


def _vision_describe_sync(
    *,
    image_bytes: bytes,
    api_base: str,
    model: str,
    api_key: str,
    query: str,
    ignore_vehicles: bool,
    mode: str,
) -> str:
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:image/jpeg;base64,{b64}"
    if mode == "doorbell":
        prompt = (
            "Write one spoken doorbell sentence. Start with 'Someone is at the door'. "
            "If a person is visible, mention count/clothing/package. "
            "If no person is visible, still start that way and describe the scene."
        )
    else:
        prompt = (
            "Write one short sentence describing this camera snapshot. "
            "Mention visible people and packages. "
            "If nothing notable is happening output exactly: 'Nothing notable.' "
            f"User hint: {query or 'brief camera alert'}"
        )
        if ignore_vehicles:
            prompt += " Do not mention vehicles."
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a concise vision assistant."},
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


async def _vision_describe(
    *,
    image_bytes: bytes,
    api_base: str,
    model: str,
    api_key: str,
    query: str,
    ignore_vehicles: bool,
    mode: str,
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
    )


async def _notify_homeassistant(
    *,
    title: str,
    message: str,
    priority: str,
    api_notification: bool,
    device_services: Any,
    origin: Dict[str, Any],
) -> Dict[str, Any]:
    services = _normalize_device_services(device_services)
    if not (services or api_notification):
        return {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}
    meta = {"priority": "high" if _text(priority).lower() in {"high", "critical"} else "normal"}
    sent_count = 0
    errors: List[str] = []

    async def _dispatch_once(targets: Dict[str, Any]) -> None:
        nonlocal sent_count
        try:
            result = await dispatch_notification(
                platform="homeassistant",
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
        errors.append(result_text or "homeassistant notifier returned empty result")

    # Keep persistent notifications off in Awareness routing; this path is for
    # VoicePE/API notifications and explicit notify services selected in rule.
    if api_notification:
        await _dispatch_once({"persistent": False, "api_notification": True})

    for service in services:
        await _dispatch_once(
            {
                "persistent": False,
                "api_notification": False,
                "device_service": service,
            }
        )

    if sent_count > 0:
        result: Dict[str, Any] = {"ok": True, "sent_count": sent_count}
        if errors:
            result["warnings"] = errors
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(errors) or "homeassistant notifier failed"}


def _tts_speak_sync(*, ha_base: str, token: str, tts_entity: str, players: List[str], message: str) -> None:
    if not players:
        return
    payload_template = {"entity_id": tts_entity, "message": message, "cache": True}
    headers = _ha_headers(token)
    for player in players:
        payload = dict(payload_template)
        payload["media_player_entity_id"] = player
        primary = requests.post(f"{ha_base}/api/services/tts/speak", headers=headers, json=payload, timeout=15)
        if primary.status_code < 400:
            continue
        fallback = requests.post(f"{ha_base}/api/services/tts/piper_say", headers=headers, json=payload, timeout=15)
        if fallback.status_code >= 400:
            raise RuntimeError(f"TTS failed (speak:{primary.status_code}, piper_say:{fallback.status_code})")


async def _tts_speak(*, ha_base: str, token: str, tts_entity: str, players: List[str], message: str) -> None:
    await asyncio.to_thread(
        _tts_speak_sync,
        ha_base=ha_base,
        token=token,
        tts_entity=tts_entity,
        players=players,
        message=message,
    )


def _ha_set_input_text_sync(ha_base: str, token: str, entity_id: str, value: str) -> None:
    entity = _text(entity_id)
    if not entity:
        return
    text_value = str(value or "")
    max_len = _ha_get_input_text_max_len_sync(ha_base, token, entity)
    if max_len is not None and len(text_value) > max_len:
        text_value = text_value[:max_len]
        logger.info(
            "[awareness] truncated input_text payload for %s to %s chars",
            entity,
            max_len,
        )
    url = f"{ha_base}/api/services/input_text/set_value"
    payload = {"entity_id": entity, "value": text_value}
    resp = requests.post(url, headers=_ha_headers(token), json=payload, timeout=10)
    if resp.status_code >= 400:
        raise RuntimeError(f"input_text.set_value HTTP {resp.status_code}: {resp.text[:200]}")


async def _ha_set_input_text(ha_base: str, token: str, entity_id: str, value: str) -> None:
    await asyncio.to_thread(_ha_set_input_text_sync, ha_base, token, entity_id, value)


def _ha_get_input_text_max_len_sync(ha_base: str, token: str, entity_id: str) -> Optional[int]:
    entity = _text(entity_id)
    if not entity:
        return None
    try:
        resp = requests.get(
            f"{ha_base}/api/states/{quote(entity, safe='')}",
            headers=_ha_headers(token, json_content=False),
            timeout=10,
        )
        if resp.status_code >= 400:
            return None
        payload = resp.json() or {}
        attrs = payload.get("attributes") if isinstance(payload, dict) else {}
        if not isinstance(attrs, dict):
            return None
        for key in ("max", "max_length", "maxlen"):
            raw = attrs.get(key)
            try:
                value = int(raw)
            except Exception:
                continue
            if 1 <= value <= 10000:
                return value
    except Exception:
        return None
    return None


def _ha_fetch_history_sync(
    ha_base: str,
    token: str,
    entities: List[str],
    start: datetime,
    end: datetime,
) -> Dict[str, List[Dict[str, Any]]]:
    clean_entities = [entity for entity in entities if _text(entity)]
    if not clean_entities:
        return {}
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%S")
    end_iso = end.strftime("%Y-%m-%dT%H:%M:%S")
    url = f"{ha_base}/api/history/period/{start_iso}"
    params = {"filter_entity_id": ",".join(clean_entities), "end_time": end_iso}
    resp = requests.get(url, headers=_ha_headers(token, json_content=False), params=params, timeout=15)
    if resp.status_code >= 400:
        raise RuntimeError(f"history HTTP {resp.status_code}: {resp.text[:200]}")
    payload = resp.json() or []
    out: Dict[str, List[Dict[str, Any]]] = {}
    for series in payload:
        if not isinstance(series, list) or not series:
            continue
        entity_id = _text((series[0] or {}).get("entity_id"))
        if entity_id:
            out[entity_id] = series
    return out


async def _ha_fetch_history(
    ha_base: str,
    token: str,
    entities: List[str],
    start: datetime,
    end: datetime,
) -> Dict[str, List[Dict[str, Any]]]:
    return await asyncio.to_thread(_ha_fetch_history_sync, ha_base, token, entities, start, end)


def _extract_numeric(series: List[Dict[str, Any]]) -> List[float]:
    values: List[float] = []
    for item in series or []:
        state = _text((item or {}).get("state"))
        if not state or state in {"unknown", "unavailable"}:
            continue
        try:
            values.append(float(state))
        except Exception:
            continue
    return values


def _summary_stats(values: List[float]) -> Optional[Dict[str, float]]:
    if not values:
        return None
    return {"min": min(values), "max": max(values), "avg": sum(values) / len(values)}


def _is_nothing_notable_summary(value: Any) -> bool:
    token = _text(value).lower()
    if not token:
        return False
    token = re.sub(r"\s+", " ", token)
    token = token.rstrip(".!? ")
    return token == "nothing notable"


async def _execute_camera_rule(rule: Dict[str, Any], llm_client: Any, reason: str, event: Dict[str, Any]) -> Dict[str, Any]:
    del llm_client
    camera = _text(rule.get("camera_entity"))
    area = _text(rule.get("area")) or "camera"
    provider = _rule_provider(rule)
    if not camera:
        raise ValueError("Camera rule is missing camera_entity.")
    vision = get_shared_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    cooldown_seconds = _as_int(rule.get("cooldown_seconds"), 30, minimum=0, maximum=86400)
    notification_cooldown_seconds = _as_int(
        rule.get("notification_cooldown_seconds"), 0, minimum=0, maximum=86400
    )
    if _within_cooldown(_camera_cooldown_key(camera), cooldown_seconds):
        return {"ok": True, "summary": f"Skipped '{rule.get('name')}' due to cooldown.", "skipped": "cooldown"}
    used_fallback = False
    jpeg: bytes = b""
    try:
        if provider == "unifi_protect":
            jpeg = await _unifi_camera_snapshot(_unifi_camera_id_from_entity(camera))
        else:
            ha = _ha_config()
            jpeg = await _camera_snapshot(ha["base"], ha["token"], camera)
        summary = await _vision_describe(
            image_bytes=jpeg,
            api_base=_text(vision.get("api_base")),
            model=_text(vision.get("model")),
            api_key=_text(vision.get("api_key")),
            query=_text(rule.get("query") or "brief camera alert"),
            ignore_vehicles=_bool(rule.get("ignore_vehicles"), False),
            mode="camera",
        )
    except Exception:
        logger.exception("[awareness] camera snapshot/vision failed for %s", camera)
        summary = "Motion event detected, but camera analysis was unavailable."
        used_fallback = True
    summary = _compact(summary) or "Motion event detected."
    nothing_notable = _is_nothing_notable_summary(summary)
    if not nothing_notable:
        snapshot_store = _store_event_snapshot(redis_client, jpeg, content_type="image/jpeg")
        event_payload = {
            "source": _slug(area),
            "title": f"{area.title()} motion",
            "type": "camera_motion",
            "message": summary,
            "entity_id": camera,
            "ha_time": _now_iso(),
            "level": "info",
            "data": {
                "area": area,
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
    _mark_cooldown(_camera_cooldown_key(camera))
    device_services = _normalize_device_services(rule.get("device_services") or rule.get("device_service"))
    api_notification = _bool(rule.get("api_notification"), False)
    notify_requested = bool(device_services or api_notification)
    if nothing_notable:
        notify_result: Dict[str, Any] = {"ok": True, "sent_count": 0, "skipped": "nothing_notable"}
    elif (
        notify_requested
        and notification_cooldown_seconds > 0
        and _within_cooldown(_camera_notify_cooldown_key(camera), notification_cooldown_seconds)
    ):
        notify_result = {
            "ok": True,
            "sent_count": 0,
            "skipped": "notification_cooldown",
            "notification_cooldown_seconds": notification_cooldown_seconds,
        }
    else:
        notify_result = await _notify_homeassistant(
            title=_text(rule.get("title") or "Camera Event"),
            message=summary,
            priority=_text(rule.get("priority") or "high"),
            api_notification=api_notification,
            device_services=device_services,
            origin={
                "platform": "awareness_core",
                "scope": "camera_rule",
                "rule_id": rule.get("id"),
                "camera": camera,
                "area": area,
                "provider": provider,
            },
        )
        if notify_result.get("ok") and int(notify_result.get("sent_count") or 0) > 0:
            _mark_cooldown(_camera_notify_cooldown_key(camera))
    return {
        "ok": True,
        "summary": summary,
        "camera": camera,
        "area": area,
        "provider": provider,
        "vision_fallback_used": used_fallback,
        "notification": notify_result,
    }


async def _execute_doorbell_rule(rule: Dict[str, Any], llm_client: Any, reason: str, event: Dict[str, Any]) -> Dict[str, Any]:
    del llm_client
    camera = _text(rule.get("camera_entity"))
    area = _text(rule.get("area")) or "front door"
    provider = _rule_provider(rule)
    if not camera:
        raise ValueError("Doorbell rule is missing camera_entity.")
    vision = get_shared_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    players = _normalize_players(rule.get("players"))
    tts_entity = _text(rule.get("tts_entity") or "tts.piper")
    jpeg: bytes = b""
    try:
        if provider == "unifi_protect":
            jpeg = await _unifi_camera_snapshot(_unifi_camera_id_from_entity(camera))
        else:
            ha = _ha_config()
            jpeg = await _camera_snapshot(ha["base"], ha["token"], camera)
        spoken_line = await _vision_describe(
            image_bytes=jpeg,
            api_base=_text(vision.get("api_base")),
            model=_text(vision.get("model")),
            api_key=_text(vision.get("api_key")),
            query="doorbell alert",
            ignore_vehicles=False,
            mode="doorbell",
        )
        spoken_line = _compact(spoken_line, limit=180) or "Someone is at the door."
    except Exception:
        logger.exception("[awareness] doorbell snapshot/vision failed for %s", camera)
        spoken_line = "Someone is at the door."
    snapshot_store = _store_event_snapshot(redis_client, jpeg, content_type="image/jpeg")
    if players:
        ha = _ha_config()
        await _tts_speak(
            ha_base=ha["base"],
            token=ha["token"],
            tts_entity=tts_entity,
            players=players,
            message=spoken_line,
        )
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
            "tts_entity": tts_entity,
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
    notify_result = {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}
    if _bool(rule.get("notifications"), True):
        device_services = _normalize_device_services(rule.get("device_services") or rule.get("device_service"))
        api_notification = _bool(rule.get("api_notification"), False)
        notify_result = await _notify_homeassistant(
            title=_text(rule.get("title") or "Doorbell"),
            message=spoken_line,
            priority=_text(rule.get("priority") or "normal"),
            api_notification=api_notification,
            device_services=device_services,
            origin={
                "platform": "awareness_core",
                "scope": "doorbell_rule",
                "rule_id": rule.get("id"),
                "camera": camera,
                "area": area,
                "provider": provider,
            },
        )
    return {
        "ok": True,
        "summary": spoken_line,
        "camera": camera,
        "area": area,
        "provider": provider,
        "players": players,
        "notification": notify_result,
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
        },
    }
    _append_event(redis_client, source=area, payload=event_payload)
    players = _normalize_players(rule.get("players"))
    tts_entity = _text(rule.get("tts_entity") or "tts.piper")
    tts_result: Dict[str, Any] = {
        "ok": True,
        "sent_count": 0,
        "skipped": "open_only" if action_token != "open" else "no_players",
    }
    if action_token == "open" and players:
        try:
            ha = _ha_config()
            await _tts_speak(
                ha_base=ha["base"],
                token=ha["token"],
                tts_entity=tts_entity,
                players=players,
                message=spoken_line,
            )
            tts_result = {"ok": True, "sent_count": len(players)}
        except Exception as exc:
            logger.warning("[awareness] entry sensor TTS failed for %s: %s", entity_id, exc)
            tts_result = {"ok": False, "sent_count": 0, "error": str(exc)}
    notify_result: Dict[str, Any]
    if action_token != "open":
        notify_result = {"ok": True, "sent_count": 0, "skipped": "open_only"}
    elif not _bool(rule.get("notifications"), False):
        notify_result = {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}
    else:
        device_services = _normalize_device_services(rule.get("device_services") or rule.get("device_service"))
        api_notification = _bool(rule.get("api_notification"), False)
        notify_result = await _notify_homeassistant(
            title=_text(rule.get("title") or "Entry Sensor"),
            message=summary,
            priority=_text(rule.get("priority") or "normal"),
            api_notification=api_notification,
            device_services=device_services,
            origin={
                "platform": "awareness_core",
                "scope": "entry_sensor_rule",
                "rule_id": rule.get("id"),
                "entity_id": entity_id,
                "sensor_type": sensor_type,
                "area": area,
                "provider": provider,
            },
        )
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


def _event_items_for_ui(client: Any) -> List[Dict[str, Any]]:
    filters = _event_type_filters(client)
    list_view = _event_list_view_enabled(client)
    sources = _discover_event_sources(client)
    if not sources:
        return []
    events = _load_events_for_sources(
        client,
        sources=sources,
        start=datetime(1970, 1, 1),
        end=datetime.now() + timedelta(days=1),
        limit_per_source=0,
    )
    items: List[Dict[str, Any]] = []
    for idx, event in enumerate(events):
        event_bucket = _event_type_bucket(event)
        is_camera_event = event_bucket in {"camera", "doorbell"}
        if not _event_allowed_by_filter(filters, event_bucket):
            continue
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
        if list_view and is_camera_event and snapshot.get("data_url"):
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


async def _run_events_brief(rule: Dict[str, Any], llm_client: Any) -> Dict[str, Any]:
    del llm_client
    ha = _ha_config()
    max_chars = min(100, _as_int(rule.get("max_chars"), 100, minimum=40, maximum=240))
    start, end, label = _event_window(rule.get("timeframe"))
    area = _slug(rule.get("area")) if _text(rule.get("area")) else ""
    if area:
        sources = [area]
    else:
        sources = _discover_event_sources(redis_client)
    events = _load_events_for_sources(redis_client, sources, start, end, limit_per_source=250)
    if not events:
        summary = f"No activity detected {label}."
    else:
        top: List[str] = []
        for event in events[:3]:
            msg = _text(event.get("message") or event.get("title"))
            if msg:
                top.append(msg)
        summary = "; ".join(top) if top else f"Activity detected {label}."
    summary = _compact(summary, limit=max_chars)
    input_text_entity = _text(rule.get("input_text_entity"))
    if input_text_entity:
        try:
            await _ha_set_input_text(ha["base"], ha["token"], input_text_entity, summary)
        except Exception as exc:
            logger.warning("[awareness] events brief failed to write %s: %s", input_text_entity, exc)
    return {"ok": True, "summary": summary, "timeframe": label, "event_count": len(events)}


async def _run_weather_brief(rule: Dict[str, Any], llm_client: Any) -> Dict[str, Any]:
    del llm_client
    ha = _ha_config()
    max_chars = min(100, _as_int(rule.get("max_chars"), 100, minimum=40, maximum=240))
    core_settings = _settings(redis_client)
    # Legacy fallback keeps prior installs working after removing automation verbas.
    legacy_settings = (
        redis_client.hgetall("verba_settings:Weather Brief")
        or redis_client.hgetall("verba_settings: Weather Brief")
        or {}
    )
    temp_entity = (
        _text(rule.get("weather_temp_entity"))
        or _text(core_settings.get("weather_temp_entity"))
        or _text(legacy_settings.get("TEMP_ENTITY"))
    )
    wind_entity = (
        _text(rule.get("weather_wind_entity"))
        or _text(core_settings.get("weather_wind_entity"))
        or _text(legacy_settings.get("WIND_ENTITY"))
    )
    rain_entity = (
        _text(rule.get("weather_rain_entity"))
        or _text(core_settings.get("weather_rain_entity"))
        or _text(legacy_settings.get("RAIN_ENTITY"))
    )
    hours = _as_int(rule.get("hours"), 12, minimum=1, maximum=72)
    now = datetime.now()
    start = now - timedelta(hours=hours)
    entities = [entity for entity in [temp_entity, wind_entity, rain_entity] if entity]
    if not entities:
        raise ValueError("Weather Brief sensors are not configured in verba settings.")
    history = await _ha_fetch_history(ha["base"], ha["token"], entities, start, now)
    metrics: Dict[str, Any] = {"temperature": None, "wind": None, "rain": None}
    if temp_entity and temp_entity in history:
        summary = _summary_stats(_extract_numeric(history[temp_entity]))
        if summary:
            metrics["temperature"] = summary
    if wind_entity and wind_entity in history:
        summary = _summary_stats(_extract_numeric(history[wind_entity]))
        if summary:
            metrics["wind"] = summary
    if rain_entity and rain_entity in history:
        vals = _extract_numeric(history[rain_entity])
        if vals:
            total = sum(v for v in vals if v >= 0)
            metrics["rain"] = {"total": total, "max": max(vals), "rained": total > 0}
    if not any(metrics.values()):
        summary_text = f"No recent weather data available for the last {hours} hours."
    else:
        parts = [f"In the last {hours} hours"]
        if metrics["temperature"]:
            parts.append(
                f"temps ranged {round(metrics['temperature']['min'])}-{round(metrics['temperature']['max'])}"
            )
        if metrics["wind"]:
            parts.append(f"winds up to {round(metrics['wind']['max'])}")
        if metrics["rain"]:
            parts.append(f"rain ~{round(metrics['rain']['total'], 1)}" if metrics["rain"]["rained"] else "no rain")
        summary_text = ". ".join(parts) + "."
    summary_text = _compact(summary_text, limit=max_chars)
    input_text_entity = _text(rule.get("input_text_entity")) or _text(legacy_settings.get("INPUT_TEXT_ENTITY"))
    if input_text_entity:
        try:
            await _ha_set_input_text(ha["base"], ha["token"], input_text_entity, summary_text)
        except Exception as exc:
            logger.warning("[awareness] weather brief failed to write %s: %s", input_text_entity, exc)
    return {"ok": True, "summary": summary_text, "hours": hours}


def _time_greeting(dt: datetime) -> str:
    hour = dt.hour
    if 5 <= hour < 12:
        return "Good morning"
    if 12 <= hour < 17:
        return "Good afternoon"
    if 17 <= hour < 22:
        return "Good evening"
    return "Hello"


def _json_object_from_text(text: Any) -> Dict[str, Any]:
    raw = _text(text)
    if not raw:
        return {}
    candidate = raw
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        candidate = "\n".join(lines).strip()
    try:
        payload = json.loads(candidate)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        pass
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start >= 0 and end > start:
        try:
            payload = json.loads(candidate[start : end + 1])
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
    return {}


def _format_datetime_for_brief(now: datetime) -> Tuple[str, str]:
    date_text = now.strftime("%A, %B %d, %Y")
    time_text = now.strftime("%I:%M %p").lstrip("0")
    return date_text, time_text


async def _run_zen_greeting(rule: Dict[str, Any], llm_client: Any) -> Dict[str, Any]:
    if llm_client is None:
        raise ValueError("LLM client is unavailable for zen greeting generation.")
    ha = _ha_config()
    now = datetime.now()
    greeting = _time_greeting(now)
    max_chars = min(100, _as_int(rule.get("max_chars"), 100, minimum=40, maximum=240))
    include_date = _bool(rule.get("include_date"), False)
    tone = _text(rule.get("tone") or "zen")
    prompt_hint = _text(rule.get("prompt_hint"))
    system = (
        "You write short, calming dashboard messages. "
        "One sentence, plain text, no markdown, no emojis, no quotes. "
        f"TOTAL OUTPUT MUST BE {max_chars} CHARACTERS OR FEWER."
    )
    date_str = now.strftime("%A, %B %d") if include_date else ""
    user_payload = {
        "time": now.strftime("%H:%M"),
        "date": date_str,
        "opening_greeting": greeting,
        "tone": tone,
        "extra_hint": prompt_hint,
        "instruction": "Create a one-line zen message that starts with the greeting.",
    }
    output = ""
    try:
        resp = await llm_client.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            temperature=0.8,
            max_tokens=90,
            timeout_ms=30_000,
        )
        output = _text((resp.get("message") or {}).get("content"))
    except Exception as exc:
        logger.info("[awareness] zen greeting generation failed, using fallback: %s", exc)
    if not output:
        if include_date and date_str:
            output = f"{greeting} - {date_str}. Breathe and begin gently."
        else:
            output = f"{greeting}. Breathe and begin gently."
    output = _compact(output, limit=max_chars)
    input_text_entity = _text(rule.get("input_text_entity"))
    if input_text_entity:
        try:
            await _ha_set_input_text(ha["base"], ha["token"], input_text_entity, output)
        except Exception as exc:
            logger.warning("[awareness] zen greeting failed to write %s: %s", input_text_entity, exc)
    return {"ok": True, "summary": output}


async def _run_alan_watts_greeting(rule: Dict[str, Any], llm_client: Any) -> Dict[str, Any]:
    ha = _ha_config()
    now = datetime.now()
    greeting = _time_greeting(now)
    date_text, time_text = _format_datetime_for_brief(now)
    max_chars = min(100, _as_int(rule.get("max_chars"), 100, minimum=40, maximum=240))
    prompt_hint = _text(rule.get("prompt_hint"))
    recent_quotes = _normalize_quote_history(rule.get("quote_history"), limit=5)
    message = ""
    quote = ""
    quote_source = "Alan Watts"
    if llm_client is not None:
        system = (
            "You write warm greeting cards in a reflective, conversational voice. "
            "Return JSON only with keys message, quote, quote_source. "
            "message rules: plain text, natural flow, starts with the provided greeting, includes date and time. "
            "quote rules: provide one Alan Watts quote and avoid repeating any recent quotes provided. "
            f"TOTAL OUTPUT MUST BE {max_chars} CHARACTERS OR FEWER (message + quote line). "
            "No markdown, no emojis."
        )
        user_payload = {
            "greeting": greeting,
            "date": date_text,
            "time": time_text,
            "recent_quotes_do_not_repeat": recent_quotes,
            "max_total_chars": max_chars,
            "extra_hint": prompt_hint,
            "instruction": (
                "Write a smooth greeting for the user and include a single quote line."
            ),
        }
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                temperature=0.8,
                max_tokens=180,
                timeout_ms=30_000,
            )
            parsed = _json_object_from_text((resp.get("message") or {}).get("content"))
            message = _text(parsed.get("message"))
            quote = _text(parsed.get("quote"))
            quote_source = _text(parsed.get("quote_source") or "Alan Watts")
        except Exception as exc:
            logger.info("[awareness] alan watts greeting generation failed, using fallback: %s", exc)
    if not message:
        message = f"{greeting}. It's {date_text} at {time_text}, and this moment is enough to begin gently."
    elif not message.lower().startswith(greeting.lower()):
        message = f"{greeting}. {message}"
    if date_text.lower() not in message.lower():
        message = f"{message} Today is {date_text}."
    if time_text.lower() not in message.lower():
        message = f"{message} The time is {time_text}."
    quote_line = ""
    if quote:
        quote_line = f"\"{quote}\" - {quote_source}"
    output = f"{message}\n\n{quote_line}".strip() if quote_line else message
    if len(output) > max_chars and quote_line:
        allowed_message = max_chars - len(quote_line) - 2
        if allowed_message >= 40:
            message = _compact(message, limit=allowed_message)
            output = f"{message}\n\n{quote_line}".strip()
    if len(output) > max_chars:
        output = _compact(output, limit=max_chars)
    input_text_entity = _text(rule.get("input_text_entity"))
    if input_text_entity:
        try:
            await _ha_set_input_text(ha["base"], ha["token"], input_text_entity, output)
        except Exception as exc:
            logger.warning("[awareness] alan watts greeting failed to write %s: %s", input_text_entity, exc)
    history = _quote_history_with_new(recent_quotes, quote, limit=5) if quote else recent_quotes
    return {
        "ok": True,
        "summary": output,
        "quote": quote,
        "quote_source": quote_source,
        "rule_updates": {"quote_history": history},
    }


async def _execute_brief_rule(rule: Dict[str, Any], llm_client: Any) -> Dict[str, Any]:
    kind = _text(rule.get("brief_kind")).lower()
    if kind == "weather_brief":
        return await _run_weather_brief(rule, llm_client)
    if kind == "zen_greeting":
        return await _run_zen_greeting(rule, llm_client)
    if kind == "alan_watts_greeting":
        return await _run_alan_watts_greeting(rule, llm_client)
    return await _run_events_brief(rule, llm_client)


async def _execute_rule(rule: Dict[str, Any], llm_client: Any, reason: str, event: Dict[str, Any]) -> Dict[str, Any]:
    kind = _text(rule.get("kind")).lower()
    if kind == "camera":
        return await _execute_camera_rule(rule, llm_client, reason, event)
    if kind == "doorbell":
        return await _execute_doorbell_rule(rule, llm_client, reason, event)
    if kind == "entry_sensor":
        return await _execute_entry_sensor_rule(rule, llm_client, reason, event)
    if kind == "brief":
        return await _execute_brief_rule(rule, llm_client)
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
        return cached
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
        catalog = _finalize_catalog(catalog)
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
        if lower.startswith("media_player."):
            catalog["media_players"].append((entity_id, label))
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

    catalog = _finalize_catalog(catalog)
    _set_cached_catalog("homeassistant", catalog)
    return catalog


def _unifi_camera_is_doorbell(camera_row: Dict[str, Any]) -> bool:
    name = _text(camera_row.get("name")).lower()
    model = _text(camera_row.get("modelKey")).lower()
    hint = f"{name} {model}"
    return "doorbell" in hint or "g4db" in hint or "g5db" in hint


def _unifi_sensor_type(sensor_row: Dict[str, Any]) -> str:
    name = _text(sensor_row.get("name")).lower()
    mount_type = _text(sensor_row.get("mountType")).lower()
    hint = f"{name} {mount_type}"
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
    # Keep weather/tts/notify/input_text sourced from Home Assistant so briefs and notifications still work.
    ha_support = _ha_entity_catalog(force_refresh=force_refresh)
    for key in ("media_players", "tts", "input_text", "notify_services", "weather_sensors", "weather_temp", "weather_wind", "weather_rain"):
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


def _entity_catalog(force_refresh: bool = False, *, provider: Optional[str] = None) -> Dict[str, List[Tuple[str, str]]]:
    active_provider = _normalize_event_provider(provider or _event_provider(redis_client))
    if active_provider == "unifi_protect":
        return _unifi_entity_catalog(force_refresh=force_refresh)
    return _ha_entity_catalog(force_refresh=force_refresh)


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


def _players_text(value: Any) -> str:
    return "\n".join(_normalize_players(value))


def _rule_subtitle(rule: Dict[str, Any]) -> str:
    kind = _text(rule.get("kind"))
    enabled = "Enabled" if _bool(rule.get("enabled"), True) else "Disabled"
    last_run = _fmt_ts(rule.get("last_run_ts"))
    if kind == "brief":
        return f"{enabled} - {kind} - next: {_fmt_ts(rule.get('next_run_ts'))} - last: {last_run}"
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
    token = _text(entity_id).lower()
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
    trigger_object = _entity_object_id(trigger_entity)
    for alias in _camera_aliases(camera_entity):
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
    notify_service_options = _multiselect_choices_from_pairs(
        catalog.get("notify_services") or [],
        current_values=rule.get("device_services") or rule.get("device_service"),
    )
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
                    {
                        "key": "trigger_to_state",
                        "label": "To State (optional)",
                        "type": "text",
                        "value": _text(rule.get("trigger_to_state") or "on"),
                    },
                    {
                        "key": "trigger_attribute",
                        "label": "Attribute Key (optional)",
                        "type": "text",
                        "value": _text(rule.get("trigger_attribute")),
                    },
                    {
                        "key": "trigger_attribute_value",
                        "label": "Attribute Value (optional)",
                        "type": "text",
                        "value": _text(rule.get("trigger_attribute_value")),
                    },
                ],
            },
            {
                "label": "Detection",
                "fields": [
                    {"key": "query", "label": "Vision Hint", "type": "text", "value": _text(rule.get("query"))},
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
                        "key": "api_notification",
                        "label": "VoicePE Notifications",
                        "type": "checkbox",
                        "value": _bool(rule.get("api_notification"), False),
                    },
                    {
                        "key": "device_services",
                        "label": "Phone Notify Services",
                        "type": "multiselect",
                        "options": notify_service_options,
                        "value": _normalize_device_services(rule.get("device_services") or rule.get("device_service")),
                    },
                ],
            },
        ],
    }


def _doorbell_form(
    rule: Dict[str, Any],
    catalog: Dict[str, List[Tuple[str, str]]],
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
    tts_options = _choices_from_pairs(
        catalog.get("tts") or [],
        placeholder="(Select TTS entity)",
        current_value=_text(rule.get("tts_entity")),
    )
    notify_service_options = _multiselect_choices_from_pairs(
        catalog.get("notify_services") or [],
        current_values=rule.get("device_services") or rule.get("device_service"),
    )
    media_player_options = _multiselect_choices_from_pairs(
        catalog.get("media_players") or [],
        current_values=rule.get("players"),
    )
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
                    {
                        "key": "trigger_to_state",
                        "label": "To State (optional)",
                        "type": "text",
                        "value": _text(rule.get("trigger_to_state") or "on"),
                    },
                    {
                        "key": "trigger_attribute",
                        "label": "Attribute Key (optional)",
                        "type": "text",
                        "value": _text(rule.get("trigger_attribute")),
                    },
                    {
                        "key": "trigger_attribute_value",
                        "label": "Attribute Value (optional)",
                        "type": "text",
                        "value": _text(rule.get("trigger_attribute_value")),
                    },
                ],
            },
            {
                "label": "TTS",
                "fields": [
                    {
                        "key": "tts_entity",
                        "label": "TTS Entity",
                        "type": "select",
                        "options": tts_options,
                        "value": _text(rule.get("tts_entity")),
                    },
                    {
                        "key": "players",
                        "label": "Media Players",
                        "type": "multiselect",
                        "description": "Select one or more media_player entities for doorbell TTS.",
                        "options": media_player_options,
                        "value": _normalize_players(rule.get("players")),
                    },
                ],
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
                        "key": "notifications",
                        "label": "Send Notifications",
                        "type": "checkbox",
                        "value": _bool(rule.get("notifications"), True),
                    },
                    {
                        "key": "api_notification",
                        "label": "VoicePE Notifications",
                        "type": "checkbox",
                        "value": _bool(rule.get("api_notification"), False),
                    },
                    {
                        "key": "device_services",
                        "label": "Phone Notify Services",
                        "type": "multiselect",
                        "options": notify_service_options,
                        "value": _normalize_device_services(rule.get("device_services") or rule.get("device_service")),
                    },
                ],
            },
        ],
    }


def _entry_sensor_form(
    rule: Dict[str, Any],
    catalog: Dict[str, List[Tuple[str, str]]],
    rules: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
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
    notify_service_options = _multiselect_choices_from_pairs(
        catalog.get("notify_services") or [],
        current_values=rule.get("device_services") or rule.get("device_service"),
    )
    tts_options = _choices_from_pairs(
        catalog.get("tts") or [],
        placeholder="(Select TTS entity)",
        current_value=_text(rule.get("tts_entity")),
    )
    media_player_options = _multiselect_choices_from_pairs(
        catalog.get("media_players") or [],
        current_values=rule.get("players"),
    )
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
                "key": "area",
                "label": "Area",
                "type": "select",
                "options": area_options,
                "value": _text(rule.get("area")),
            },
        ],
        "sections": [
            {
                "label": "TTS (open only)",
                "fields": [
                    {
                        "key": "tts_entity",
                        "label": "TTS Entity",
                        "type": "select",
                        "options": tts_options,
                        "value": _text(rule.get("tts_entity") or "tts.piper"),
                    },
                    {
                        "key": "players",
                        "label": "Media Players",
                        "type": "multiselect",
                        "description": "When sensor opens, speak an alert on selected media_player entities.",
                        "options": media_player_options,
                        "value": _normalize_players(rule.get("players")),
                    },
                ],
            },
            {
                "label": "Notifications",
                "fields": [
                    {
                        "key": "notifications",
                        "label": "Send Notifications (open only)",
                        "type": "checkbox",
                        "value": _bool(rule.get("notifications"), False),
                    },
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
                        "key": "api_notification",
                        "label": "VoicePE Notifications",
                        "type": "checkbox",
                        "value": _bool(rule.get("api_notification"), False),
                    },
                    {
                        "key": "device_services",
                        "label": "Phone Notify Services",
                        "type": "multiselect",
                        "options": notify_service_options,
                        "value": _normalize_device_services(rule.get("device_services") or rule.get("device_service")),
                    },
                ],
            }
        ],
    }


def _brief_form(rule: Dict[str, Any], catalog: Dict[str, List[Tuple[str, str]]]) -> Dict[str, Any]:
    input_text_options = _choices_from_pairs(
        catalog.get("input_text") or [],
        placeholder="(None)",
        current_value=_text(rule.get("input_text_entity")),
    )
    weather_temp_options = _choices_from_pairs(
        catalog.get("weather_temp") or catalog.get("weather_sensors") or [],
        placeholder="(Select temperature sensor)",
        current_value=_text(rule.get("weather_temp_entity")),
    )
    weather_wind_options = _choices_from_pairs(
        catalog.get("weather_wind") or catalog.get("weather_sensors") or [],
        placeholder="(Select wind sensor)",
        current_value=_text(rule.get("weather_wind_entity")),
    )
    weather_rain_options = _choices_from_pairs(
        catalog.get("weather_rain") or catalog.get("weather_sensors") or [],
        placeholder="(Select rain sensor)",
        current_value=_text(rule.get("weather_rain_entity")),
    )
    show_events = {"source_key": "brief_kind", "equals": "events_query_brief"}
    show_weather = {"source_key": "brief_kind", "equals": "weather_brief"}
    show_zen = {"source_key": "brief_kind", "equals": "zen_greeting"}
    show_alan = {"source_key": "brief_kind", "equals": "alan_watts_greeting"}
    quote_history_preview = "\n".join(_normalize_quote_history(rule.get("quote_history"), limit=5))
    return {
        "id": rule["id"],
        "group": "brief",
        "title": _text(rule.get("name")) or "Brief job",
        "subtitle": _rule_subtitle(rule),
        "save_action": "awareness_save_rule",
        "remove_action": "awareness_remove_rule",
        "run_action": "awareness_run_now",
        "run_label": "Run Now",
        "remove_confirm": "Remove this brief job?",
        "fields": [
            {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": _bool(rule.get("enabled"), True)},
            {"key": "name", "label": "Job Name", "type": "text", "value": _text(rule.get("name"))},
            {
                "key": "brief_kind",
                "label": "Brief Type",
                "type": "select",
                "options": [
                    {"value": "events_query_brief", "label": "Events Brief"},
                    {"value": "weather_brief", "label": "Weather Brief"},
                    {"value": "zen_greeting", "label": "Zen Greeting"},
                    {"value": "alan_watts_greeting", "label": "Alan Watts Greeting"},
                ],
                "value": _text(rule.get("brief_kind") or "events_query_brief"),
            },
            {
                "key": "interval_minutes",
                "label": "Interval (minutes)",
                "type": "number",
                "value": _as_int(rule.get("interval_minutes"), 60, minimum=1, maximum=10080),
            },
            {
                "key": "input_text_entity",
                "label": "Output input_text (optional)",
                "type": "select",
                "options": input_text_options,
                "value": _text(rule.get("input_text_entity")),
            },
        ],
        "sections": [
            {
                "label": "Events Brief",
                "fields": [
                    {
                        "key": "timeframe",
                        "label": "Timeframe",
                        "type": "select",
                        "options": [
                            {"value": "today", "label": "Today"},
                            {"value": "yesterday", "label": "Yesterday"},
                            {"value": "last_24h", "label": "Last 24 Hours"},
                        ],
                        "value": _text(rule.get("timeframe") or "today"),
                        "show_when": show_events,
                    },
                    {
                        "key": "area",
                        "label": "Area Source (optional)",
                        "type": "text",
                        "value": _text(rule.get("area")),
                        "show_when": show_events,
                    },
                    {
                        "key": "query",
                        "label": "Query Hint (optional)",
                        "type": "text",
                        "value": _text(rule.get("query")),
                        "show_when": show_events,
                    },
                ],
            },
            {
                "label": "Weather Brief",
                "fields": [
                    {
                        "key": "hours",
                        "label": "Hours",
                        "type": "number",
                        "value": _as_int(rule.get("hours"), 12, minimum=1, maximum=72),
                        "show_when": show_weather,
                    },
                    {
                        "key": "weather_temp_entity",
                        "label": "Temperature Sensor",
                        "type": "select",
                        "options": weather_temp_options,
                        "value": _text(rule.get("weather_temp_entity")),
                        "show_when": show_weather,
                    },
                    {
                        "key": "weather_wind_entity",
                        "label": "Wind Sensor",
                        "type": "select",
                        "options": weather_wind_options,
                        "value": _text(rule.get("weather_wind_entity")),
                        "show_when": show_weather,
                    },
                    {
                        "key": "weather_rain_entity",
                        "label": "Rain Sensor",
                        "type": "select",
                        "options": weather_rain_options,
                        "value": _text(rule.get("weather_rain_entity")),
                        "show_when": show_weather,
                    }
                ],
            },
            {
                "label": "Zen Greeting",
                "fields": [
                    {
                        "key": "include_date",
                        "label": "Include Date",
                        "type": "checkbox",
                        "value": _bool(rule.get("include_date"), False),
                        "show_when": show_zen,
                    },
                    {
                        "key": "tone",
                        "label": "Tone",
                        "type": "text",
                        "value": _text(rule.get("tone") or "zen"),
                        "show_when": show_zen,
                    },
                    {
                        "key": "prompt_hint",
                        "label": "Prompt Hint",
                        "type": "text",
                        "value": _text(rule.get("prompt_hint")),
                        "show_when": show_zen,
                    },
                    {
                        "key": "max_chars",
                        "label": "Max Characters",
                        "type": "number",
                        "value": _as_int(rule.get("max_chars"), 100, minimum=40, maximum=240),
                        "show_when": show_zen,
                    },
                ],
            },
            {
                "label": "Alan Watts Greeting",
                "fields": [
                    {
                        "key": "prompt_hint",
                        "label": "Prompt Hint",
                        "type": "text",
                        "value": _text(rule.get("prompt_hint")),
                        "show_when": show_alan,
                    },
                    {
                        "key": "max_chars",
                        "label": "Max Characters",
                        "type": "number",
                        "value": _as_int(rule.get("max_chars"), 100, minimum=40, maximum=240),
                        "show_when": show_alan,
                    },
                    {
                        "key": "quote_history_preview",
                        "label": "Recent Quotes (last 5, auto)",
                        "type": "textarea",
                        "read_only": True,
                        "value": quote_history_preview,
                        "show_when": show_alan,
                    },
                ],
            },
        ],
    }


def _awareness_manager_ui(client: Any) -> Dict[str, Any]:
    all_rules = _load_rules(client)
    active_provider = _event_provider(client)
    rules: Dict[str, Dict[str, Any]] = {}
    for rule_id, rule in all_rules.items():
        kind = _text((rule or {}).get("kind")).lower()
        if kind == "brief":
            rules[rule_id] = rule
            continue
        if _rule_provider(rule) == active_provider:
            rules[rule_id] = rule
    catalog = _entity_catalog(provider=active_provider)
    event_forms = _event_items_for_ui(client)
    forms: List[Dict[str, Any]] = []
    for rule in sorted(
        rules.values(),
        key=lambda row: (_text(row.get("kind")), _text(row.get("name")).lower(), _text(row.get("id"))),
    ):
        kind = _text(rule.get("kind")).lower()
        if kind == "camera":
            forms.append(_camera_form(rule, catalog, rules))
        elif kind == "doorbell":
            forms.append(_doorbell_form(rule, catalog, rules))
        elif kind == "entry_sensor":
            forms.append(_entry_sensor_form(rule, catalog, rules))
        elif kind == "brief":
            forms.append(_brief_form(rule, catalog))
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
    tts_options = _choices_from_pairs(catalog.get("tts") or [], placeholder="(Select TTS entity)")
    input_text_options = _choices_from_pairs(catalog.get("input_text") or [], placeholder="(None)")
    weather_temp_options = _choices_from_pairs(
        catalog.get("weather_temp") or catalog.get("weather_sensors") or [],
        placeholder="(Select temperature sensor)",
    )
    weather_wind_options = _choices_from_pairs(
        catalog.get("weather_wind") or catalog.get("weather_sensors") or [],
        placeholder="(Select wind sensor)",
    )
    weather_rain_options = _choices_from_pairs(
        catalog.get("weather_rain") or catalog.get("weather_sensors") or [],
        placeholder="(Select rain sensor)",
    )
    entry_sensor_options, entry_sensor_dependency = _entry_sensor_dependency_options(
        catalog=catalog,
        current_type="door",
        current_entity="",
    )
    notify_service_options = _multiselect_choices_from_pairs(
        catalog.get("notify_services") or [],
    )
    media_player_options = _multiselect_choices_from_pairs(
        catalog.get("media_players") or [],
    )
    event_filters = _event_type_filters(client)
    event_list_view = _event_list_view_enabled(client)
    show_camera = {"source_key": "kind", "equals": "camera"}
    show_doorbell = {"source_key": "kind", "equals": "doorbell"}
    show_entry = {"source_key": "kind", "equals": "entry_sensor"}
    show_doorbell_or_entry = {"source_key": "kind", "any_of": ["doorbell", "entry_sensor"]}
    show_brief = {"source_key": "kind", "equals": "brief"}
    show_camera_or_doorbell = {"source_key": "kind", "any_of": ["camera", "doorbell"]}
    show_camera_or_doorbell_or_entry = {"source_key": "kind", "any_of": ["camera", "doorbell", "entry_sensor"]}
    show_brief_events = [
        {"source_key": "kind", "equals": "brief"},
        {"source_key": "brief_kind", "equals": "events_query_brief"},
    ]
    show_brief_weather = [
        {"source_key": "kind", "equals": "brief"},
        {"source_key": "brief_kind", "equals": "weather_brief"},
    ]
    show_brief_zen = [
        {"source_key": "kind", "equals": "brief"},
        {"source_key": "brief_kind", "equals": "zen_greeting"},
    ]
    show_brief_zen_or_alan = [
        {"source_key": "kind", "equals": "brief"},
        {"source_key": "brief_kind", "any_of": ["zen_greeting", "alan_watts_greeting"]},
    ]
    return {
        "kind": "settings_manager",
        "title": f"Awareness Rule Manager ({_provider_label(active_provider)} Events)",
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
            {
                "key": "briefs",
                "label": "Briefs",
                "source": "items",
                "item_group": "brief",
                "selector": False,
                "empty_message": "No brief jobs configured.",
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
                        {"value": "brief", "label": "Brief"},
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
                    "show_when": show_camera_or_doorbell,
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
                    "key": "trigger_to_state",
                    "label": "To State (optional)",
                    "type": "text",
                    "value": "on",
                    "show_when": show_camera_or_doorbell,
                },
                {
                    "key": "trigger_attribute",
                    "label": "Attribute Key (optional)",
                    "type": "text",
                    "value": "",
                    "show_when": show_camera_or_doorbell,
                },
                {
                    "key": "trigger_attribute_value",
                    "label": "Attribute Value (optional)",
                    "type": "text",
                    "value": "",
                    "show_when": show_camera_or_doorbell,
                },
                {
                    "key": "query",
                    "label": "Vision Hint",
                    "type": "text",
                    "value": "",
                    "show_when": show_camera,
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
                    "key": "notifications",
                    "label": "Send Notifications",
                    "type": "checkbox",
                    "value": False,
                    "show_when": show_doorbell_or_entry,
                },
                {
                    "key": "api_notification",
                    "label": "VoicePE Notifications",
                    "type": "checkbox",
                    "value": True,
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                {
                    "key": "device_services",
                    "label": "Phone Notify Services",
                    "type": "multiselect",
                    "options": notify_service_options,
                    "value": [],
                    "show_when": show_camera_or_doorbell_or_entry,
                },
                {
                    "key": "tts_entity",
                    "label": "TTS Entity",
                    "type": "select",
                    "options": tts_options,
                    "value": "",
                    "show_when": show_doorbell_or_entry,
                },
                {
                    "key": "players",
                    "label": "Media Players",
                    "type": "multiselect",
                    "description": "Select one or more media_player entities for doorbell/entry sensor TTS.",
                    "options": media_player_options,
                    "value": [],
                    "show_when": show_doorbell_or_entry,
                },
                {
                    "key": "brief_kind",
                    "label": "Brief Type",
                    "type": "select",
                    "options": [
                        {"value": "events_query_brief", "label": "Events Brief"},
                        {"value": "weather_brief", "label": "Weather Brief"},
                        {"value": "zen_greeting", "label": "Zen Greeting"},
                        {"value": "alan_watts_greeting", "label": "Alan Watts Greeting"},
                    ],
                    "value": "events_query_brief",
                    "show_when": show_brief,
                },
                {
                    "key": "interval_minutes",
                    "label": "Interval Minutes",
                    "type": "number",
                    "value": 60,
                    "show_when": show_brief,
                },
                {
                    "key": "input_text_entity",
                    "label": "Output Text Block (input_text)",
                    "type": "select",
                    "options": input_text_options,
                    "value": "",
                    "show_when": show_brief,
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe (events brief)",
                    "type": "select",
                    "options": [
                        {"value": "today", "label": "Today"},
                        {"value": "yesterday", "label": "Yesterday"},
                        {"value": "last_24h", "label": "Last 24 Hours"},
                    ],
                    "value": "today",
                    "show_when_all": show_brief_events,
                },
                {
                    "key": "hours",
                    "label": "Hours (weather brief)",
                    "type": "number",
                    "value": 12,
                    "show_when_all": show_brief_weather,
                },
                {
                    "key": "weather_temp_entity",
                    "label": "Temperature Sensor (weather brief)",
                    "type": "select",
                    "options": weather_temp_options,
                    "value": "",
                    "show_when_all": show_brief_weather,
                },
                {
                    "key": "weather_wind_entity",
                    "label": "Wind Sensor (weather brief)",
                    "type": "select",
                    "options": weather_wind_options,
                    "value": "",
                    "show_when_all": show_brief_weather,
                },
                {
                    "key": "weather_rain_entity",
                    "label": "Rain Sensor (weather brief)",
                    "type": "select",
                    "options": weather_rain_options,
                    "value": "",
                    "show_when_all": show_brief_weather,
                },
                {
                    "key": "include_date",
                    "label": "Include Date (zen)",
                    "type": "checkbox",
                    "value": False,
                    "show_when_all": show_brief_zen,
                },
                {
                    "key": "tone",
                    "label": "Tone (zen)",
                    "type": "text",
                    "value": "zen",
                    "show_when_all": show_brief_zen,
                },
                {
                    "key": "prompt_hint",
                    "label": "Prompt Hint",
                    "type": "text",
                    "value": "",
                    "show_when_all": show_brief_zen_or_alan,
                },
                {
                    "key": "max_chars",
                    "label": "Max Characters",
                    "type": "number",
                    "value": 100,
                    "show_when_all": show_brief_zen_or_alan,
                },
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
        return values.get(key)
    return payload.get(key, default)


def _build_rule_from_values(
    *,
    kind: str,
    values: Dict[str, Any],
    payload: Dict[str, Any],
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now_ts = time.time()
    previous = existing if isinstance(existing, dict) else {}
    active_provider = _event_provider(redis_client)
    provider_value = (
        "homeassistant"
        if kind == "brief"
        else _normalize_event_provider(
            _value(values, payload, "provider", previous.get("provider") or active_provider)
        )
    )
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
    trigger_to_state_default = "on" if kind in {"camera", "doorbell"} else ""
    notifications_default = False if kind == "entry_sensor" else True
    priority_value = _text(_value(values, payload, "priority", previous.get("priority", priority_default))).lower()
    device_services_value = _value(
        values,
        payload,
        "device_services",
        previous.get("device_services", previous.get("device_service", "")),
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
            "api_notification": _bool(
                _value(values, payload, "api_notification", previous.get("api_notification", False)),
                False,
            ),
            "device_services": _normalize_device_services(device_services_value),
            "tts_entity": _text(_value(values, payload, "tts_entity", previous.get("tts_entity", "tts.piper"))),
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
            "brief_kind": _text(
                _value(values, payload, "brief_kind", previous.get("brief_kind", "events_query_brief"))
            ).lower(),
            "interval_minutes": _as_int(
                _value(values, payload, "interval_minutes", previous.get("interval_minutes", 60)),
                60,
                minimum=1,
                maximum=10080,
            ),
            "next_run_ts": _as_float(previous.get("next_run_ts"), 0.0),
            "input_text_entity": _text(
                _value(values, payload, "input_text_entity", previous.get("input_text_entity", ""))
            ),
            "timeframe": _text(_value(values, payload, "timeframe", previous.get("timeframe", "today"))).lower(),
            "hours": _as_int(
                _value(values, payload, "hours", previous.get("hours", 12)),
                12,
                minimum=1,
                maximum=72,
            ),
            "weather_temp_entity": _text(
                _value(values, payload, "weather_temp_entity", previous.get("weather_temp_entity", ""))
            ),
            "weather_wind_entity": _text(
                _value(values, payload, "weather_wind_entity", previous.get("weather_wind_entity", ""))
            ),
            "weather_rain_entity": _text(
                _value(values, payload, "weather_rain_entity", previous.get("weather_rain_entity", ""))
            ),
            "include_date": _bool(
                _value(values, payload, "include_date", previous.get("include_date", False)),
                False,
            ),
            "tone": _text(_value(values, payload, "tone", previous.get("tone", "zen"))),
            "prompt_hint": _text(_value(values, payload, "prompt_hint", previous.get("prompt_hint", ""))),
            "max_chars": _as_int(
                _value(values, payload, "max_chars", previous.get("max_chars", 100)),
                100,
                minimum=40,
                maximum=240,
            ),
            "quote_history": _normalize_quote_history(previous.get("quote_history") or previous.get("last_quotes"), limit=5),
        }
    )
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
            raise ValueError("At least one media player is required for doorbell rules.")
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
    elif kind == "brief":
        brief_kind = base["brief_kind"]
        if brief_kind not in {"events_query_brief", "weather_brief", "zen_greeting", "alan_watts_greeting"}:
            raise ValueError(
                "Brief type must be events_query_brief, weather_brief, zen_greeting, or alan_watts_greeting."
            )
        if base["next_run_ts"] <= 0:
            base["next_run_ts"] = now_ts + (base["interval_minutes"] * 60)
    return _normalize_rule(base) or base


def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    if client is None:
        raise ValueError("Redis connection is unavailable.")
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)
    action_name = _text(action).lower()
    if action_name == "awareness_refresh_entities":
        provider = _event_provider(client)
        _entity_catalog(force_refresh=True, provider=provider)
        return {"ok": True, "message": f"Entity catalog refreshed for {_provider_label(provider)}."}
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
        )
        return {"ok": True, "message": "Event filters updated."}
    if action_name == "awareness_add_rule":
        kind = _text(_value(values, body, "kind")).lower()
        if kind not in {"camera", "doorbell", "entry_sensor", "brief"}:
            raise ValueError("Rule type must be camera, doorbell, entry_sensor, or brief.")
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
        _enqueue_execution(client, rule_id=rule_id, reason="manual", event={})
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
        if _text(rule.get("kind")) != "brief" and reason != "manual":
            if _rule_provider(rule) != _event_provider(redis_client):
                continue
        try:
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
            if _text(rule.get("kind")) == "brief":
                interval = _as_int(rule.get("interval_minutes"), 60, minimum=1, maximum=10080)
                rule["next_run_ts"] = now_ts + (interval * 60)
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


async def _awareness_brief_scheduler_loop(stop_event: Optional[object]) -> None:
    while not (stop_event and stop_event.is_set()):
        tick = _setting_int(redis_client, "brief_scheduler_tick_sec", 5, minimum=1, maximum=60)
        now_ts = time.time()
        rules = _load_rules(redis_client)
        for rule in rules.values():
            if _text(rule.get("kind")) != "brief":
                continue
            if not _bool(rule.get("enabled"), True):
                continue
            interval = _as_int(rule.get("interval_minutes"), 60, minimum=1, maximum=10080)
            next_run = _as_float(rule.get("next_run_ts"), 0.0)
            if next_run <= 0:
                rule["next_run_ts"] = now_ts + (interval * 60)
                rule["updated_at"] = now_ts
                _save_rule(redis_client, rule)
                continue
            if now_ts >= next_run:
                _enqueue_execution(redis_client, rule_id=_text(rule.get("id")), reason="schedule", event={})
                rule["next_run_ts"] = now_ts + (interval * 60)
                rule["updated_at"] = now_ts
                _save_rule(redis_client, rule)
        await asyncio.sleep(float(tick))


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
        if _rule_provider(rule) != provider_token:
            continue
        if not _bool(rule.get("enabled"), True):
            continue
        try:
            if not _rule_matches_event(rule, entity_id=entity_id, new_state=new_state, old_state=old_state):
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
    active_provider = _event_provider(redis_client)
    if active_provider == "homeassistant":
        await _handle_trigger_state_change(
            provider="homeassistant",
            entity_id=entity_id,
            new_state=new_state,
            old_state=old_state,
        )
        return
    if active_provider != "unifi_protect":
        return
    translated_entity = _unifi_translate_ha_entity_to_trigger(entity_id, new_state)
    if not translated_entity:
        translated_entity = _unifi_translate_ha_entity_to_trigger(entity_id, old_state)
    if not translated_entity:
        return
    unifi_new = dict(new_state)
    unifi_old = dict(old_state)
    new_attrs = unifi_new.get("attributes") if isinstance(unifi_new.get("attributes"), dict) else {}
    old_attrs = unifi_old.get("attributes") if isinstance(unifi_old.get("attributes"), dict) else {}
    new_attrs = dict(new_attrs)
    old_attrs = dict(old_attrs)
    new_attrs["source_entity_id"] = entity_id
    old_attrs["source_entity_id"] = entity_id
    unifi_new["attributes"] = new_attrs
    unifi_old["attributes"] = old_attrs
    source_domain = entity_id.split(".", 1)[0] if "." in entity_id else ""
    new_state_token = _text(unifi_new.get("state")).lower()
    old_state_token = _text(unifi_old.get("state")).lower()
    binary_tokens = {"on", "off", "open", "closed", "opening", "closing", "locked", "unlocked"}
    if source_domain == "event" or (new_state_token not in binary_tokens and old_state_token not in binary_tokens):
        # HA event entities often use timestamp/state strings instead of on/off.
        # Normalize to an edge event so trigger_to_state=on rules can match.
        unifi_new["state"] = "on"
        unifi_old["state"] = "off"
    await _handle_trigger_state_change(
        provider="unifi_protect",
        entity_id=translated_entity,
        new_state=unifi_new,
        old_state=unifi_old,
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


async def _awareness_unifi_poll_loop(stop_event: Optional[object]) -> None:
    motion_markers: Dict[str, str] = {}
    doorbell_markers: Dict[str, str] = {}
    smart_camera_markers: Dict[str, str] = {}
    sensor_states: Dict[str, str] = {}
    while not (stop_event and stop_event.is_set()):
        poll_seconds = _setting_int(redis_client, "unifi_poll_seconds", 3, minimum=1, maximum=60)
        if _event_provider(redis_client) != "unifi_protect":
            _runtime_set(redis_client, unifi_connected=False)
            await asyncio.sleep(float(poll_seconds))
            continue
        try:
            cameras = _unifi_list_cameras()
            sensors = _unifi_list_sensors()
            now_ts = time.time()
            _runtime_set(redis_client, unifi_connected=True, unifi_last_poll_ts=now_ts, ws_connected=False)
            required_smart_types_by_camera = _unifi_active_rule_smart_types_by_camera()

            active_motion_entities: set[str] = set()
            active_doorbell_entities: set[str] = set()
            active_smart_entities: set[str] = set()
            active_smart_cameras: set[str] = set()
            active_sensor_entities: set[str] = set()

            for row in cameras:
                if not isinstance(row, dict):
                    continue
                camera_id = _text(row.get("id")).lower()
                if not camera_id:
                    continue
                camera_name = _text(row.get("name")) or camera_id
                attrs = {"friendly_name": camera_name}

                motion_entity = _unifi_camera_motion_trigger(camera_id)
                motion_marker = _unifi_camera_motion_marker(row)
                active_motion_entities.add(motion_entity)
                prev_motion = motion_markers.get(motion_entity)
                if motion_marker and prev_motion is not None and motion_marker != prev_motion:
                    logger.info(
                        "[awareness] unifi motion marker changed for %s (%s)",
                        camera_name,
                        motion_entity,
                    )
                    await _handle_trigger_state_change(
                        provider="unifi_protect",
                        entity_id=motion_entity,
                        new_state={"state": "on", "attributes": attrs},
                        old_state={"state": "off", "attributes": attrs},
                    )
                    _runtime_set(redis_client, last_ws_event_ts=now_ts)
                if motion_marker:
                    motion_markers[motion_entity] = motion_marker
                else:
                    motion_markers.setdefault(motion_entity, "")

                required_smart_types = set(required_smart_types_by_camera.get(camera_id) or set())
                discovered_smart_types = set(_unifi_camera_smart_detect_types(row))
                all_smart_types = set(discovered_smart_types)
                all_smart_types.update(required_smart_types)
                for smart_type in sorted(all_smart_types):
                    active_smart_entities.add(_unifi_camera_smart_trigger(camera_id, smart_type))
                active_smart_cameras.add(camera_id)

                any_smart_marker = _unifi_camera_any_smart_marker(row)
                prev_any_smart = smart_camera_markers.get(camera_id)
                if any_smart_marker and prev_any_smart is not None and any_smart_marker != prev_any_smart:
                    trigger_types = set(required_smart_types)
                    if trigger_types:
                        recent_types = _unifi_camera_recent_smart_types(row)
                        overlap = trigger_types.intersection(recent_types)
                        if overlap:
                            trigger_types = overlap
                    if not trigger_types:
                        trigger_types = set(all_smart_types)
                    if not trigger_types:
                        trigger_types = set(_UNIFI_SMART_TYPE_FALLBACK)
                    for smart_type in sorted(trigger_types):
                        smart_entity = _unifi_camera_smart_trigger(camera_id, smart_type)
                        active_smart_entities.add(smart_entity)
                        smart_attrs = {
                            "friendly_name": f"{camera_name} {_unifi_smart_type_label(smart_type)}",
                            "camera_name": camera_name,
                            "detection_type": smart_type,
                        }
                        await _handle_trigger_state_change(
                            provider="unifi_protect",
                            entity_id=smart_entity,
                            new_state={"state": "on", "attributes": smart_attrs},
                            old_state={"state": "off", "attributes": smart_attrs},
                        )
                    _runtime_set(redis_client, last_ws_event_ts=now_ts)
                if any_smart_marker:
                    smart_camera_markers[camera_id] = any_smart_marker

                if _unifi_camera_is_doorbell(row):
                    doorbell_entity = _unifi_camera_doorbell_trigger(camera_id)
                    doorbell_marker = _unifi_camera_doorbell_marker(row)
                    active_doorbell_entities.add(doorbell_entity)
                    prev_doorbell = doorbell_markers.get(doorbell_entity)
                    if doorbell_marker and prev_doorbell is not None and doorbell_marker != prev_doorbell:
                        await _handle_trigger_state_change(
                            provider="unifi_protect",
                            entity_id=doorbell_entity,
                            new_state={"state": "on", "attributes": attrs},
                            old_state={"state": "off", "attributes": attrs},
                        )
                        _runtime_set(redis_client, last_ws_event_ts=now_ts)
                    if doorbell_marker:
                        doorbell_markers[doorbell_entity] = doorbell_marker

            for row in sensors:
                if not isinstance(row, dict):
                    continue
                sensor_id = _text(row.get("id")).lower()
                if not sensor_id:
                    continue
                sensor_name = _text(row.get("name")) or sensor_id
                sensor_entity = _unifi_sensor_entity(sensor_id)
                active_sensor_entities.add(sensor_entity)
                state = "on" if bool(row.get("isOpened")) else "off"
                previous_state = sensor_states.get(sensor_entity)
                attrs = {"friendly_name": sensor_name}
                if previous_state is not None and previous_state != state:
                    await _handle_trigger_state_change(
                        provider="unifi_protect",
                        entity_id=sensor_entity,
                        new_state={"state": state, "attributes": attrs},
                        old_state={"state": previous_state, "attributes": attrs},
                    )
                    _runtime_set(redis_client, last_ws_event_ts=now_ts)
                sensor_states[sensor_entity] = state

            motion_markers = {k: v for k, v in motion_markers.items() if k in active_motion_entities}
            doorbell_markers = {k: v for k, v in doorbell_markers.items() if k in active_doorbell_entities}
            smart_camera_markers = {k: v for k, v in smart_camera_markers.items() if k in active_smart_cameras}
            sensor_states = {k: v for k, v in sensor_states.items() if k in active_sensor_entities}
        except Exception as exc:
            _runtime_set(redis_client, unifi_connected=False, last_error=str(exc))
            logger.warning("[awareness] unifi poll loop error: %s", exc)
        await asyncio.sleep(float(poll_seconds))


async def _awareness_ws_loop(stop_event: Optional[object]) -> None:
    while not (stop_event and stop_event.is_set()):
        reconnect_seconds = _setting_int(redis_client, "ws_reconnect_seconds", 5, minimum=1, maximum=60)
        active_provider = _event_provider(redis_client)
        if active_provider not in {"homeassistant", "unifi_protect"}:
            _runtime_set(redis_client, ws_connected=False)
            await asyncio.sleep(float(reconnect_seconds))
            continue
        try:
            ha = _ha_config()
            ws_url = _ha_ws_url(ha["base"])
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url, heartbeat=30) as ws:
                    first = await ws.receive(timeout=10)
                    if first.type != aiohttp.WSMsgType.TEXT:
                        raise RuntimeError("Unexpected HA websocket hello payload")
                    hello = json.loads(first.data)
                    if hello.get("type") != "auth_required":
                        raise RuntimeError(f"Unexpected HA websocket hello: {hello}")
                    await ws.send_json({"type": "auth", "access_token": ha["token"]})
                    auth_resp = await ws.receive(timeout=10)
                    if auth_resp.type != aiohttp.WSMsgType.TEXT:
                        raise RuntimeError("Unexpected HA websocket auth payload")
                    auth_data = json.loads(auth_resp.data)
                    if auth_data.get("type") != "auth_ok":
                        raise RuntimeError(f"HA websocket auth failed: {auth_data}")
                    await ws.send_json({"id": 1, "type": "subscribe_events", "event_type": "state_changed"})
                    _runtime_set(redis_client, ws_connected=True, ws_url=ws_url)
                    logger.info("[awareness] Home Assistant websocket connected: %s", ws_url)
                    while not (stop_event and stop_event.is_set()):
                        try:
                            msg = await ws.receive(timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(msg.data)
                            if payload.get("type") == "event":
                                event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
                                if _text(event.get("event_type")) != "state_changed":
                                    continue
                                data = event.get("data") if isinstance(event.get("data"), dict) else {}
                                await _handle_state_change_event(data)
                                _runtime_set(redis_client, last_ws_event_ts=time.time())
                            elif (
                                payload.get("type") == "result"
                                and payload.get("id") == 1
                                and not payload.get("success")
                            ):
                                raise RuntimeError(f"HA subscribe_events failed: {payload}")
                        elif msg.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                            raise RuntimeError("Home Assistant websocket connection closed")
        except Exception as exc:
            _runtime_set(redis_client, ws_connected=False, last_error=str(exc))
            logger.warning("[awareness] websocket loop error: %s", exc)
            await asyncio.sleep(float(reconnect_seconds))
    _runtime_set(redis_client, ws_connected=False)


async def _awareness_retention_loop(stop_event: Optional[object]) -> None:
    while not (stop_event and stop_event.is_set()):
        sources = _discover_event_sources(redis_client)
        for source in sources:
            _trim_events_for_source(redis_client, source)
        await asyncio.sleep(300.0)


async def _awareness_main(stop_event: Optional[object], llm_client: Any) -> None:
    rules = _load_rules(redis_client)
    enabled_rules = sum(1 for rule in rules.values() if _bool(rule.get("enabled"), True))
    _runtime_set(
        redis_client,
        started_at=time.time(),
        ws_connected=False,
        unifi_connected=False,
        queue_depth=_queue_depth(redis_client),
        last_error="",
    )
    logger.info(
        "[awareness] core started v%s (%s) (rules=%d enabled=%d)",
        __version__,
        __file__,
        len(rules),
        enabled_rules,
    )
    tasks = [
        asyncio.create_task(_awareness_worker_loop(stop_event, llm_client)),
        asyncio.create_task(_awareness_brief_scheduler_loop(stop_event)),
        asyncio.create_task(_awareness_ws_loop(stop_event)),
        asyncio.create_task(_awareness_unifi_poll_loop(stop_event)),
        asyncio.create_task(_awareness_retention_loop(stop_event)),
    ]
    try:
        while not (stop_event and stop_event.is_set()):
            await asyncio.sleep(0.5)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        _runtime_set(redis_client, ws_connected=False)
        logger.info("[awareness] core stopped")


def run(stop_event=None):
    llm_client = get_llm_client_from_env()
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
