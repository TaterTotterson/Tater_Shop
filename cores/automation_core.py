"""General event-to-action automations for Tater integrations and voice targets."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from announcement_targets import build_announcement_target_options
import requests

from helpers import describe_image_with_local_llm, redis_client
from integration_registry import get_integration_device_registry, run_integration_device_action
from notify import dispatch_notification, notifier_destination_catalog
from speech_settings import get_speech_settings
from speech_tts import speak_announcement_targets
from vision_settings import get_vision_settings


__version__ = "1.0.0"
MIN_TATER_VERSION = "59"
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
_HISTORY_LIMIT = 200
_WORKER_COUNT = 4

_TRUE = {"1", "true", "yes", "on", "enabled", "y"}
_FALSE = {"0", "false", "no", "off", "disabled", "n"}
_ON_STATES = {"on", "open", "opened", "active", "detected", "connected", "online", "home", "present", "true", "1"}
_OFF_STATES = {
    "off",
    "closed",
    "close",
    "inactive",
    "clear",
    "disconnected",
    "offline",
    "away",
    "not_present",
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


def _device_categories(device: Dict[str, Any]) -> set[str]:
    return {_token(item) for item in (device.get("category_ids") or device.get("capabilities") or []) if _token(item)}


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


def _category_rows(registry: Dict[str, Any], *, actionable_only: bool = False) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for category in registry.get("categories") or []:
        if not isinstance(category, dict):
            continue
        devices = [item for item in category.get("devices") or [] if isinstance(item, dict)]
        if not devices:
            continue
        if actionable_only and not any(_device_actions(item) for item in devices):
            continue
        category_id = _token(category.get("id"))
        if not category_id:
            continue
        rows.append(
            {
                "id": category_id,
                "name": _text(category.get("name")) or category_id.replace("_", " ").title(),
                "devices": devices,
                "order": _int(category.get("order"), 1000),
            }
        )
    rows.sort(key=lambda item: (item["order"], item["name"].casefold()))
    return rows


def _category_options(registry: Dict[str, Any], *, actionable_only: bool = False) -> List[Dict[str, str]]:
    return [
        {"value": row["id"], "label": f"{row['name']} ({len(row['devices'])})"}
        for row in _category_rows(registry, actionable_only=actionable_only)
    ]


def _device_option(device: Dict[str, Any]) -> Dict[str, str]:
    provider = _text(device.get("integration_id"))
    device_id = _device_id(device)
    name = _text(device.get("name")) or device_id
    room = _text(device.get("room") or device.get("area"))
    integration = _text(device.get("integration_name")) or provider
    suffix = " • ".join(item for item in (room, integration) if item)
    return {
        "value": _encode_device(provider, device_id),
        "label": f"{name} — {suffix}" if suffix else name,
    }


def _device_dependency(
    registry: Dict[str, Any],
    *,
    current_category: str = "",
    current_values: Any = None,
    multiple: bool = False,
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    current = _list(current_values)
    options_by_source: Dict[str, List[Dict[str, str]]] = {}
    all_options: List[Dict[str, str]] = []
    seen: set[str] = set()
    for category in _category_rows(registry):
        rows = [_device_option(device) for device in category["devices"]]
        if not multiple:
            rows = [{"value": "", "label": "Any device in this category"}, *rows]
        options_by_source[category["id"]] = rows
        for row in rows:
            if not row["value"] or row["value"] in seen:
                continue
            seen.add(row["value"])
            all_options.append(row)
    selected_rows = options_by_source.get(_token(current_category), all_options)
    for value in current:
        if value and not any(row.get("value") == value for row in selected_rows):
            selected_rows.append({"value": value, "label": f"{value} (saved)"})
    return selected_rows, {
        "source_key": "trigger_category" if not multiple else "action_category",
        "options_by_source": options_by_source,
        "default_options": all_options,
    }


def _action_dependency(
    registry: Dict[str, Any],
    *,
    current_category: str = "",
    current_action: str = "",
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    options_by_source: Dict[str, List[Dict[str, str]]] = {}
    all_actions: set[str] = set()
    for category in _category_rows(registry, actionable_only=True):
        actions = sorted({action for device in category["devices"] for action in _device_actions(device)})
        all_actions.update(actions)
        options_by_source[category["id"]] = [
            {"value": action, "label": _ACTION_LABELS.get(action, action.replace("_", " ").title())}
            for action in actions
        ]
    default_options = [
        {"value": action, "label": _ACTION_LABELS.get(action, action.replace("_", " ").title())}
        for action in sorted(all_actions)
    ]
    selected = list(options_by_source.get(_token(current_category), default_options))
    if current_action and not any(row["value"] == current_action for row in selected):
        selected.append({"value": current_action, "label": f"{current_action.replace('_', ' ').title()} (saved)"})
    return selected, {
        "source_key": "action_category",
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
            dict(row)
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
    if not trigger_category or trigger_event not in {row["value"] for row in _EVENT_OPTIONS}:
        return None
    if action_type not in {"device", "tts", "notification", "camera_ai"}:
        return None
    now = time.time()
    rule = {
        "id": _text(raw.get("id")) or str(uuid.uuid4()),
        "name": _text(raw.get("name")) or "New automation",
        "enabled": _bool(raw.get("enabled"), True),
        "preset": _token(raw.get("preset") or "custom"),
        "trigger_category": trigger_category,
        "trigger_device": _text(raw.get("trigger_device")),
        "trigger_room": _token(raw.get("trigger_room")),
        "trigger_event": trigger_event,
        "trigger_attribute": _text(raw.get("trigger_attribute")),
        "trigger_value": _text(raw.get("trigger_value")),
        "cooldown_seconds": _int(raw.get("cooldown_seconds"), 30, minimum=0, maximum=86400),
        "action_type": action_type,
        "action_category": _token(raw.get("action_category")),
        "action_scope": _token(raw.get("action_scope") or "category"),
        "action_devices": _list(raw.get("action_devices")),
        "action_room": _token(raw.get("action_room")),
        "action_operation": _token(raw.get("action_operation")),
        "action_value": _text(raw.get("action_value")),
        "action_mode": _text(raw.get("action_mode")),
        "action_text": _text(raw.get("action_text")),
        "action_payload_json": _text(raw.get("action_payload_json")),
        "tts_text": _text(raw.get("tts_text")),
        "tts_targets": _list(raw.get("tts_targets")),
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
        rule["action_scope"] = "category"
    if action_type == "device":
        if not rule["action_category"] or not rule["action_operation"]:
            return None
        if rule["action_scope"] == "devices" and not rule["action_devices"]:
            return None
    elif action_type == "tts":
        if not rule["tts_text"] or not rule["tts_targets"]:
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
    for raw in client.lrange(_INTEGRATION_EVENTS_KEY, 0, 999) or []:
        event = _json_record(raw)
        if not event:
            continue
        seq = _int(event.get("seq"), 0, minimum=0)
        if seq <= after_seq:
            continue
        event["seq"] = seq
        rows.append(event)
    rows.sort(key=lambda item: _int(item.get("seq"), 0))
    return rows[: max(1, limit)]


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

    room = _token(rule.get("trigger_room"))
    if room and not any(_device_room(device) == room for device in devices):
        return False, {}

    state, old_state = _event_states(event)
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    kind = _text(event.get("kind")).lower()
    haystack = f"{kind} {state} {json.dumps(payload, default=str)[:20000]}".lower()
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
            word in haystack for word in ("doorbell", "ring", "pressed", "button_press")
        )
    elif trigger in {"motion", "person", "vehicle", "animal", "package", "face", "license_plate"}:
        needles = {trigger}
        if trigger == "license_plate":
            needles.update({"licenseplate", "plate"})
        matched = (
            not _event_is_terminal(event)
            and state not in _OFF_STATES
            and any(needle in haystack for needle in needles)
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
        "event_seq": _int(event.get("seq"), 0),
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
    message = _render_template(rule.get("tts_text"), context)
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
    )
    if isinstance(result, dict) and result.get("ok") is False:
        raise RuntimeError(_text(result.get("error")) or "TTS delivery failed.")
    count = _int((result or {}).get("sent_count") if isinstance(result, dict) else 0, 0)
    return {"ok": True, "summary": f'Spoke “{message[:120]}” to {count or len(_list(rule.get("tts_targets")))} target(s).'}


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
    if stored is None:
        last_seq = _int(redis_client.get(_INTEGRATION_EVENT_SEQ_KEY), 0)
        redis_client.set(_CURSOR_KEY, str(last_seq))
    else:
        last_seq = _int(stored, 0)
    while not (stop_event and stop_event.is_set()):
        events = _integration_events(redis_client, last_seq)
        if not events:
            await asyncio.sleep(0.25)
            continue
        for event in events:
            seq = _int(event.get("seq"), last_seq)
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
        "trigger_device",
        "trigger_room",
        "trigger_event",
        "trigger_attribute",
        "trigger_value",
        "cooldown_seconds",
        "action_type",
        "action_category",
        "action_scope",
        "action_devices",
        "action_room",
        "action_operation",
        "action_value",
        "action_mode",
        "action_text",
        "action_payload_json",
        "tts_text",
        "tts_targets",
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
        if not _text(rule.get("tts_text")):
            raise ValueError("Enter the words Tater should speak.")
        if not _list(rule.get("tts_targets")):
            raise ValueError("Choose at least one announcement target.")
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


def _editor_fields(rule: Dict[str, Any], registry: Dict[str, Any], client: Any) -> List[Dict[str, Any]]:
    trigger_category = _token(rule.get("trigger_category"))
    action_category = _token(rule.get("action_category"))
    trigger_device_options, trigger_device_dependency = _device_dependency(
        registry,
        current_category=trigger_category,
        current_values=rule.get("trigger_device"),
        multiple=False,
    )
    action_device_options, action_device_dependency = _device_dependency(
        registry,
        current_category=action_category,
        current_values=rule.get("action_devices"),
        multiple=True,
    )
    action_options, action_dependency = _action_dependency(
        registry,
        current_category=action_category,
        current_action=_token(rule.get("action_operation")),
    )
    show_device_action = {"source_key": "action_type", "equals": "device"}
    show_tts = {"source_key": "action_type", "equals": "tts"}
    show_notification = {"source_key": "action_type", "equals": "notification"}
    show_camera_ai = {"source_key": "action_type", "equals": "camera_ai"}
    show_selected_devices = [
        show_device_action,
        {"source_key": "action_scope", "equals": "devices"},
    ]
    show_trigger_value = {"source_key": "trigger_event", "any_of": ["equals", "contains", "above", "below"]}
    show_numeric_action = {
        "source_key": "action_operation",
        "any_of": ["set_brightness", "set_position", "set_temperature", "set_volume"],
    }
    show_action_text = {
        "source_key": "action_operation",
        "any_of": ["set_color", "play_media", "play_url", "announce"],
    }
    return [
        {"key": "preset", "label": "Preset", "type": "hidden", "value": "custom"},
        {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": _bool(rule.get("enabled"), True)},
        {
            "key": "name",
            "label": "Automation Name",
            "type": "text",
            "required": True,
            "placeholder": "Person detected in the front yard",
            "value": _text(rule.get("name")),
        },
        {
            "key": "trigger_category",
            "label": "When this category…",
            "type": "select",
            "options": _category_options(registry),
            "value": trigger_category,
            "description": "Categories come from the devices exposed by enabled Tater integrations.",
        },
        {
            "key": "trigger_device",
            "label": "Trigger Device",
            "type": "select",
            "options": trigger_device_options,
            "dependent_options": trigger_device_dependency,
            "value": _text(rule.get("trigger_device")),
            "description": "Leave this on any device to watch the entire category.",
        },
        {
            "key": "trigger_room",
            "label": "Trigger Room",
            "type": "select",
            "options": _room_options(registry),
            "value": _token(rule.get("trigger_room")),
        },
        {
            "key": "trigger_event",
            "label": "Does this…",
            "type": "select",
            "options": _EVENT_OPTIONS,
            "value": _token(rule.get("trigger_event") or "changed"),
        },
        {
            "key": "trigger_attribute",
            "label": "Value Path (optional)",
            "type": "text",
            "placeholder": "new_state.attributes.temperature",
            "value": _text(rule.get("trigger_attribute")),
            "description": "For equals/above/below, optionally compare one nested event value.",
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
            "key": "cooldown_seconds",
            "label": "Cooldown (seconds)",
            "type": "number",
            "value": _int(rule.get("cooldown_seconds"), 30),
            "description": "Prevents repeated device events from running this automation too often.",
        },
        {
            "key": "action_type",
            "label": "Then Tater should…",
            "type": "select",
            "options": [
                {"value": "device", "label": "Control integration devices"},
                {"value": "tts", "label": "Speak an announcement"},
                {"value": "notification", "label": "Send a notification"},
                {"value": "camera_ai", "label": "Describe a camera, then announce or notify"},
            ],
            "value": _token(rule.get("action_type") or "device"),
        },
        {
            "key": "action_category",
            "label": "Action Category",
            "type": "select",
            "options": _category_options(registry, actionable_only=True),
            "value": action_category,
            "show_when": show_device_action,
        },
        {
            "key": "action_scope",
            "label": "Control",
            "type": "select",
            "options": [
                {"value": "category", "label": "Every compatible device in the category"},
                {"value": "devices", "label": "Only selected devices"},
            ],
            "value": _token(rule.get("action_scope") or "category"),
            "show_when": show_device_action,
        },
        {
            "key": "action_devices",
            "label": "Action Devices",
            "type": "multiselect",
            "options": action_device_options,
            "dependent_options": action_device_dependency,
            "value": _list(rule.get("action_devices")),
            "show_when_all": show_selected_devices,
        },
        {
            "key": "action_room",
            "label": "Action Room",
            "type": "select",
            "options": _room_options(registry),
            "value": _token(rule.get("action_room")),
            "description": "Optional: limit the category or selected devices to one room.",
            "show_when": show_device_action,
        },
        {
            "key": "action_operation",
            "label": "Device Action",
            "type": "select",
            "options": action_options,
            "dependent_options": action_dependency,
            "value": _token(rule.get("action_operation")),
            "show_when": show_device_action,
        },
        {
            "key": "action_value",
            "label": "Action Value",
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
            "label": "Action Text / URL",
            "type": "text",
            "value": _text(rule.get("action_text")),
            "show_when_all": [show_device_action, show_action_text],
        },
        {
            "key": "action_payload_json",
            "label": "Advanced Action Data (JSON)",
            "type": "textarea",
            "placeholder": '{"key":"value"}',
            "value": _text(rule.get("action_payload_json")),
            "description": "Optional provider-specific data merged into the device action.",
            "show_when": show_device_action,
        },
        {
            "key": "tts_text",
            "label": "Words To Speak",
            "type": "textarea",
            "placeholder": "A person was detected in the front yard.",
            "value": _text(rule.get("tts_text")),
            "description": "You can use {device}, {room}, {event}, {state}, {value}, {category}, or {provider}.",
            "show_when": show_tts,
        },
        {
            "key": "tts_targets",
            "label": "Announcement Targets",
            "type": "multiselect",
            "options": _announcement_options(rule.get("tts_targets")),
            "value": _list(rule.get("tts_targets")),
            "description": "Choose Voice Core satellites, Sonos, Home Assistant players, or other supported speakers.",
            "show_when": show_tts,
        },
        {
            "key": "notification_title",
            "label": "Notification Title",
            "type": "text",
            "value": _text(rule.get("notification_title") or "Tater Automation"),
            "show_when": show_notification,
        },
        {
            "key": "notification_message",
            "label": "Notification Message",
            "type": "textarea",
            "value": _text(rule.get("notification_message")),
            "description": "Template variables are the same as TTS announcements.",
            "show_when": show_notification,
        },
        {
            "key": "notification_targets",
            "label": "Notification Destinations",
            "type": "multiselect",
            "options": _notification_options(client, rule.get("notification_targets")),
            "value": _list(rule.get("notification_targets")),
            "show_when": show_notification,
        },
        {
            "key": "notification_priority",
            "label": "Priority",
            "type": "select",
            "options": [{"value": "normal", "label": "Normal"}, {"value": "high", "label": "High"}],
            "value": _text(rule.get("notification_priority") or "normal"),
            "show_when": show_notification,
        },
        {
            "key": "camera_source",
            "label": "Camera Source",
            "type": "select",
            "options": [
                {"value": "trigger", "label": "The camera that triggered this automation"},
                {"value": "selected", "label": "Always use a selected camera"},
            ],
            "value": _token(rule.get("camera_source") or "trigger"),
            "show_when": show_camera_ai,
        },
        {
            "key": "camera_device",
            "label": "Camera",
            "type": "select",
            "options": _devices_for_category_options(
                registry,
                "camera",
                require_actions=("camera_snapshot", "snapshot"),
                placeholder="Select a camera",
            ),
            "value": _text(rule.get("camera_device")),
            "show_when_all": [
                show_camera_ai,
                {"source_key": "camera_source", "equals": "selected"},
            ],
        },
        {
            "key": "vision_prompt",
            "label": "What Vision Should Describe",
            "type": "textarea",
            "value": _text(
                rule.get("vision_prompt")
                or "Briefly describe the important activity in this image. Do not invent details."
            ),
            "show_when": show_camera_ai,
        },
        {
            "key": "vision_fallback",
            "label": "Fallback Message",
            "type": "text",
            "value": _text(rule.get("vision_fallback") or "Camera activity was detected."),
            "description": "Used when the camera or vision model is temporarily unavailable.",
            "show_when": show_camera_ai,
        },
        {
            "key": "camera_tts_text",
            "label": "Camera Announcement",
            "type": "textarea",
            "value": _text(rule.get("camera_tts_text") or "{vision}"),
            "description": "Use {vision} to include the camera description.",
            "show_when": show_camera_ai,
        },
        {
            "key": "camera_tts_targets",
            "label": "Camera Announcement Targets",
            "type": "multiselect",
            "options": _announcement_options(rule.get("camera_tts_targets")),
            "value": _list(rule.get("camera_tts_targets")),
            "description": "Optional. Leave empty if this camera automation only sends notifications.",
            "show_when": show_camera_ai,
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
        },
        {
            "key": "camera_notification_targets",
            "label": "Camera Notification Destinations",
            "type": "multiselect",
            "options": _notification_options(client, rule.get("camera_notification_targets")),
            "value": _list(rule.get("camera_notification_targets")),
            "description": "Optional. You can announce, notify, or do both.",
            "show_when": show_camera_ai,
        },
        {
            "key": "camera_notification_priority",
            "label": "Camera Notification Priority",
            "type": "select",
            "options": [{"value": "normal", "label": "Normal"}, {"value": "high", "label": "High"}],
            "value": _text(rule.get("camera_notification_priority") or "normal"),
            "show_when": show_camera_ai,
        },
    ]


def _rule_form(rule: Dict[str, Any], registry: Dict[str, Any], client: Any) -> Dict[str, Any]:
    status = _text(rule.get("last_status")) or "not run"
    subtitle = (
        f"{'Enabled' if _bool(rule.get('enabled'), True) else 'Disabled'} • "
        f"{_text(rule.get('trigger_category')).replace('_', ' ').title()} → "
        f"{_text(rule.get('action_type')).replace('_', ' ').title()} • "
        f"last {status}: {_now_label(rule.get('last_run_ts'))}"
    )
    return {
        "id": rule["id"],
        "group": "rules",
        "title": _text(rule.get("name")) or "Automation",
        "subtitle": subtitle,
        "save_action": "automation_save_rule",
        "run_action": "automation_run_now",
        "run_label": "Test Now",
        "remove_action": "automation_remove_rule",
        "remove_confirm": "Remove this automation?",
        "fields": _editor_fields(rule, registry, client),
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


def _starter_forms(registry: Dict[str, Any], client: Any) -> List[Dict[str, Any]]:
    camera_options = _devices_for_category_options(
        registry,
        "camera",
        require_actions=("camera_snapshot", "snapshot"),
        placeholder="Select a camera",
    )
    entry_options = _devices_for_category_options(
        registry,
        "entry_sensor",
        placeholder="Select a door, window, or garage sensor",
    )
    tts_options = _announcement_options()
    notification_options = _notification_options(client)
    forms = [
        {
            "id": "starter_person_announcement",
            "group": "starters",
            "title": "Person Detection Announcement",
            "subtitle": "Camera sees a person → Vision describes it → Tater speaks",
            "save_action": "automation_add_preset",
            "save_label": "Create Automation",
            "fields": [
                {
                    "key": "name",
                    "label": "Automation Name",
                    "type": "text",
                    "value": "Person detection announcement",
                },
                {"key": "camera_device", "label": "Camera", "type": "select", "options": camera_options, "value": ""},
                {
                    "key": "tts_targets",
                    "label": "Announcement Targets",
                    "type": "multiselect",
                    "options": tts_options,
                    "value": [],
                },
                {
                    "key": "tts_text",
                    "label": "Words To Speak",
                    "type": "textarea",
                    "value": "{vision}",
                    "description": "Use {vision} for Tater's short description of the camera snapshot.",
                },
                {"key": "cooldown_seconds", "label": "Cooldown (seconds)", "type": "number", "value": 60},
            ],
        },
        {
            "id": "starter_camera_alert",
            "group": "starters",
            "title": "Smart Camera Alert",
            "subtitle": "Camera activity → Vision describes it → Tater sends a notification",
            "save_action": "automation_add_preset",
            "save_label": "Create Automation",
            "fields": [
                {"key": "name", "label": "Automation Name", "type": "text", "value": "Smart camera alert"},
                {"key": "camera_device", "label": "Camera", "type": "select", "options": camera_options, "value": ""},
                {
                    "key": "trigger_event",
                    "label": "When Camera…",
                    "type": "select",
                    "options": [
                        {"value": "motion", "label": "Detects motion"},
                        {"value": "person", "label": "Detects a person"},
                        {"value": "vehicle", "label": "Detects a vehicle"},
                        {"value": "animal", "label": "Detects an animal"},
                        {"value": "package", "label": "Detects a package"},
                    ],
                    "value": "motion",
                },
                {
                    "key": "notification_targets",
                    "label": "Notification Destinations",
                    "type": "multiselect",
                    "options": notification_options,
                    "value": [],
                },
                {
                    "key": "notification_title",
                    "label": "Notification Title",
                    "type": "text",
                    "value": "Camera Activity",
                },
                {
                    "key": "notification_message",
                    "label": "Notification Message",
                    "type": "textarea",
                    "value": "{vision}",
                },
                {"key": "cooldown_seconds", "label": "Cooldown (seconds)", "type": "number", "value": 60},
            ],
        },
        {
            "id": "starter_doorbell_announcement",
            "group": "starters",
            "title": "Doorbell Announcement",
            "subtitle": "Doorbell pressed → Vision checks the camera → Tater speaks",
            "save_action": "automation_add_preset",
            "save_label": "Create Automation",
            "fields": [
                {"key": "name", "label": "Automation Name", "type": "text", "value": "Doorbell announcement"},
                {
                    "key": "camera_device",
                    "label": "Doorbell Camera",
                    "type": "select",
                    "options": camera_options,
                    "value": "",
                },
                {
                    "key": "tts_targets",
                    "label": "Announcement Targets",
                    "type": "multiselect",
                    "options": tts_options,
                    "value": [],
                },
                {
                    "key": "tts_text",
                    "label": "Words To Speak",
                    "type": "textarea",
                    "value": "{vision}",
                },
                {"key": "cooldown_seconds", "label": "Cooldown (seconds)", "type": "number", "value": 10},
            ],
        },
        {
            "id": "starter_entry_announcement",
            "group": "starters",
            "title": "Door, Window, or Garage Announcement",
            "subtitle": "Entry sensor opens → Tater speaks on selected satellites or speakers",
            "save_action": "automation_add_preset",
            "save_label": "Create Automation",
            "fields": [
                {"key": "name", "label": "Automation Name", "type": "text", "value": "Entry announcement"},
                {
                    "key": "entry_device",
                    "label": "Entry Sensor",
                    "type": "select",
                    "options": entry_options,
                    "value": "",
                },
                {
                    "key": "tts_targets",
                    "label": "Announcement Targets",
                    "type": "multiselect",
                    "options": tts_options,
                    "value": [],
                },
                {
                    "key": "tts_text",
                    "label": "Words To Speak",
                    "type": "textarea",
                    "value": "{device} opened.",
                },
                {"key": "cooldown_seconds", "label": "Cooldown (seconds)", "type": "number", "value": 5},
            ],
        },
    ]
    awareness_count = _int(client.hlen("awareness:rules"), 0) if hasattr(client, "hlen") else 0
    if awareness_count > 0:
        forms.append(
            {
                "id": "starter_import_awareness",
                "group": "starters",
                "title": "Bring In Awareness Automations",
                "subtitle": f"Import {awareness_count} existing Awareness rule(s) for review",
                "save_action": "automation_import_awareness",
                "save_label": "Import As Disabled",
                "fields": [
                    {
                        "key": "migration_note",
                        "label": "Safe Migration",
                        "type": "textarea",
                        "read_only": True,
                        "value": (
                            "Imported automations stay disabled so Awareness and Automation Core cannot both "
                            "run the same rule. Review and test each imported rule before enabling it."
                        ),
                    }
                ],
            }
        )
    return forms


def _rule_from_starter(starter_id: str, values: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    now = time.time()
    name = _text(_value(values, body, "name"))
    cooldown = _int(_value(values, body, "cooldown_seconds", 30), 30, minimum=0, maximum=86400)
    tts_targets = _list(_value(values, body, "tts_targets"))
    tts_text = _text(_value(values, body, "tts_text"))
    camera_device = _text(_value(values, body, "camera_device"))
    base: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "name": name,
        "enabled": True,
        "preset": "custom",
        "cooldown_seconds": cooldown,
        "created_at": now,
        "updated_at": now,
    }
    if starter_id == "starter_person_announcement":
        if not camera_device or not tts_targets:
            raise ValueError("Choose a camera and at least one announcement target.")
        base.update(
            {
                "name": name or "Person detection announcement",
                "trigger_category": "camera",
                "trigger_device": camera_device,
                "trigger_event": "person",
                "action_type": "camera_ai",
                "camera_source": "selected",
                "camera_device": camera_device,
                "vision_prompt": "Briefly describe the person and the important visible activity. Do not invent details.",
                "vision_fallback": "A person was detected.",
                "camera_tts_text": tts_text or "{vision}",
                "camera_tts_targets": tts_targets,
            }
        )
    elif starter_id == "starter_camera_alert":
        notification_targets = _list(_value(values, body, "notification_targets"))
        if not camera_device or not notification_targets:
            raise ValueError("Choose a camera and at least one notification destination.")
        base.update(
            {
                "name": name or "Smart camera alert",
                "trigger_category": "camera",
                "trigger_device": camera_device,
                "trigger_event": _token(_value(values, body, "trigger_event", "motion")),
                "action_type": "camera_ai",
                "camera_source": "selected",
                "camera_device": camera_device,
                "vision_prompt": "Briefly describe the important visible activity in this camera image. Do not invent details.",
                "vision_fallback": "Camera activity was detected.",
                "camera_notification_title": _text(
                    _value(values, body, "notification_title", "Camera Activity")
                ),
                "camera_notification_message": _text(
                    _value(values, body, "notification_message", "{vision}")
                ),
                "camera_notification_targets": notification_targets,
            }
        )
    elif starter_id == "starter_doorbell_announcement":
        if not camera_device or not tts_targets:
            raise ValueError("Choose a doorbell camera and at least one announcement target.")
        base.update(
            {
                "name": name or "Doorbell announcement",
                "trigger_category": "camera",
                "trigger_device": camera_device,
                "trigger_event": "doorbell",
                "action_type": "camera_ai",
                "camera_source": "selected",
                "camera_device": camera_device,
                "vision_prompt": "Briefly describe who or what is at the door. Do not invent details.",
                "vision_fallback": "Someone is at the door.",
                "camera_tts_text": tts_text or "{vision}",
                "camera_tts_targets": tts_targets,
            }
        )
    elif starter_id == "starter_entry_announcement":
        entry_device = _text(_value(values, body, "entry_device"))
        if not entry_device or not tts_targets:
            raise ValueError("Choose an entry sensor and at least one announcement target.")
        base.update(
            {
                "name": name or "Entry announcement",
                "trigger_category": "entry_sensor",
                "trigger_device": entry_device,
                "trigger_event": "opens",
                "action_type": "tts",
                "tts_text": tts_text or "{device} opened.",
                "tts_targets": tts_targets,
            }
        )
    else:
        raise KeyError("Unknown automation starter.")
    normalized = _normalize_rule(base)
    if not normalized:
        raise ValueError("The starter automation could not be created from those selections.")
    return normalized


def _awareness_target(provider: Any, value: Any) -> str:
    token = _text(value)
    if not token:
        return ""
    if "|" in token:
        return token
    return _encode_device(provider, token)


def _awareness_notification_targets(value: Any) -> List[str]:
    targets: List[str] = []
    raw = value if isinstance(value, list) else []
    for item in raw:
        if not isinstance(item, dict):
            continue
        encoded = _encode_notification_target(item.get("platform"), item.get("targets"))
        if encoded:
            targets.append(encoded)
    return targets


def _awareness_trigger_event(rule: Dict[str, Any]) -> str:
    trigger_text = " ".join(
        _list(rule.get("trigger_entities") or rule.get("trigger_entity"))
    ).lower()
    for event in ("person", "vehicle", "animal", "package", "face"):
        if event in trigger_text:
            return event
    if "license" in trigger_text or "plate" in trigger_text:
        return "license_plate"
    if _token(rule.get("kind")) == "doorbell":
        return "doorbell"
    if _token(rule.get("kind")) == "entry_sensor":
        return "opens"
    return "motion"


def _convert_awareness_rule(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    kind = _token(raw.get("kind"))
    source_id = _text(raw.get("id"))
    provider = _text(raw.get("provider"))
    now = time.time()
    base: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "name": f"{_text(raw.get('name')) or kind.replace('_', ' ').title()} (imported)",
        "enabled": False,
        "preset": "custom",
        "cooldown_seconds": _int(raw.get("cooldown_seconds"), 30),
        "created_at": now,
        "updated_at": now,
        "source_core": "awareness",
        "source_rule_id": source_id,
    }
    if kind in {"camera", "doorbell"}:
        camera = _awareness_target(provider, raw.get("camera_entity"))
        if not camera:
            return None
        tts_targets = _list(raw.get("players"))
        notification_targets = _awareness_notification_targets(
            raw.get("notification_targets") or raw.get("notification_destinations")
        )
        if not tts_targets and not notification_targets:
            return None
        fallback = "Someone is at the door." if kind == "doorbell" else "Camera activity was detected."
        prompt = (
            "Briefly describe who or what is at the door. Do not invent details."
            if kind == "doorbell"
            else "Briefly describe the important visible camera activity. Do not invent details."
        )
        base.update(
            {
                "trigger_category": "camera",
                "trigger_device": camera,
                "trigger_event": _awareness_trigger_event(raw),
                "action_type": "camera_ai",
                "camera_source": "selected",
                "camera_device": camera,
                "vision_prompt": prompt,
                "vision_fallback": fallback,
                "camera_tts_text": "{vision}",
                "camera_tts_targets": tts_targets,
                "camera_notification_title": _text(raw.get("title") or "Camera Activity"),
                "camera_notification_message": "{vision}",
                "camera_notification_targets": notification_targets,
                "camera_notification_priority": _text(raw.get("priority") or "normal"),
            }
        )
    elif kind == "entry_sensor":
        sensor = _awareness_target(
            provider,
            raw.get("sensor_entity") or raw.get("trigger_entity"),
        )
        if not sensor:
            return None
        tts_targets = _list(raw.get("players"))
        notification_targets = _awareness_notification_targets(
            raw.get("notification_targets") or raw.get("notification_destinations")
        )
        base.update(
            {
                "trigger_category": "entry_sensor",
                "trigger_device": sensor,
                "trigger_event": "opens",
            }
        )
        if tts_targets:
            base.update(
                {
                    "action_type": "tts",
                    "tts_text": "{device} opened.",
                    "tts_targets": tts_targets,
                }
            )
        elif notification_targets:
            base.update(
                {
                    "action_type": "notification",
                    "notification_title": _text(raw.get("title") or "Entry Sensor"),
                    "notification_message": "{device} opened.",
                    "notification_targets": notification_targets,
                    "notification_priority": _text(raw.get("priority") or "normal"),
                }
            )
        else:
            return None
    else:
        return None
    return _normalize_rule(base)


def _import_awareness_rules(client: Any) -> Dict[str, Any]:
    existing_sources = {
        _text(rule.get("source_rule_id"))
        for rule in _load_rules(client).values()
        if _token(rule.get("source_core")) == "awareness"
    }
    raw_rules = client.hgetall("awareness:rules") or {}
    imported = 0
    skipped = 0
    for field, value in raw_rules.items():
        payload = _json_record(value)
        if not payload:
            skipped += 1
            continue
        payload.setdefault("id", _text(field))
        source_id = _text(payload.get("id"))
        if source_id in existing_sources:
            skipped += 1
            continue
        converted = _convert_awareness_rule(payload)
        if not converted:
            skipped += 1
            continue
        _save_rule(client, converted)
        existing_sources.add(source_id)
        imported += 1
    return {"imported": imported, "skipped": skipped}


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
        (row["value"] for row in _category_options(registry)),
        "",
    )
    default_action_category = next(
        (row["value"] for row in _category_options(registry, actionable_only=True)),
        "",
    )
    default_action_options, _dependency = _action_dependency(
        registry,
        current_category=default_action_category,
    )
    blank = {
        "name": "",
        "enabled": True,
        "trigger_category": default_trigger_category,
        "trigger_event": "changed",
        "cooldown_seconds": 30,
        "action_type": "device",
        "action_category": default_action_category,
        "action_scope": "category",
        "action_operation": default_action_options[0]["value"] if default_action_options else "",
        "notification_title": "Tater Automation",
        "notification_priority": "normal",
    }
    forms = _starter_forms(registry, client)
    forms.extend(_history_form(row) for row in history)
    forms.extend(
        _rule_form(rule, registry, client)
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
            "default_tab": "rules" if rules else "starters",
            "manager_tabs": [
                {
                    "key": "starters",
                    "label": "Quick Start",
                    "source": "items",
                    "item_group": "starters",
                    "selector": False,
                    "empty_message": "No automation starters are available.",
                },
                {
                    "key": "rules",
                    "label": "Automations",
                    "source": "items",
                    "item_group": "rules",
                    "selector": False,
                    "empty_message": "No automations configured.",
                },
                {
                    "key": "history",
                    "label": "Run History",
                    "source": "items",
                    "item_group": "history",
                    "selector": False,
                    "empty_message": "No automation runs recorded yet.",
                },
                {"key": "create", "label": "Create Automation", "source": "add_form"},
            ],
            "add_form": {
                "action": "automation_add_rule",
                "submit_label": "Create Automation",
                "fields": _editor_fields(blank, registry, client),
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
    if action_name == "automation_add_preset":
        starter_id = _text(body.get("id"))
        rule = _rule_from_starter(starter_id, values, body)
        _save_rule(client, rule)
        return {"ok": True, "id": rule["id"], "message": "Starter automation created."}
    if action_name == "automation_import_awareness":
        result = _import_awareness_rules(client)
        return {
            "ok": True,
            **result,
            "message": (
                f"Imported {result['imported']} Awareness automation(s) as disabled. "
                f"Skipped {result['skipped']} unsupported or already imported rule(s)."
            ),
        }
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
