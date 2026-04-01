import asyncio
import json
import logging
import time
import uuid
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv

from helpers import redis_client
from notify import dispatch_notification, notifier_destination_catalog

__version__ = "1.0.0"

load_dotenv()

logger = logging.getLogger("connectivity_core")
logger.setLevel(logging.INFO)

CORE_SETTINGS = {
    "category": "Connectivity Core Settings",
    "required": {
        "poll_interval_sec": {
            "label": "Poll Interval (sec)",
            "type": "number",
            "default": 15,
            "description": "Seconds between UniFi Network snapshot polls.",
        },
        "events_retention": {
            "label": "Events Retention",
            "type": "select",
            "options": ["2d", "7d", "30d", "forever"],
            "default": "7d",
            "description": "How long connectivity events are kept in Redis.",
        },
        "max_events": {
            "label": "Max Stored Events",
            "type": "number",
            "default": 600,
            "description": "Hard cap on stored connectivity events.",
        },
        "alerts_enabled": {
            "label": "Send Alerts",
            "type": "checkbox",
            "default": False,
            "description": "When enabled, matching events are sent to enabled routes.",
        },
        "monitor_wan": {
            "label": "Monitor WAN Status",
            "type": "checkbox",
            "default": True,
            "description": "Alert when WAN/internet status changes.",
        },
        "monitor_devices": {
            "label": "Monitor Device Status",
            "type": "checkbox",
            "default": True,
            "description": "Track UniFi device online/offline changes.",
        },
        "monitor_clients": {
            "label": "Monitor Client Presence",
            "type": "checkbox",
            "default": False,
            "description": "Track client connect/disconnect changes.",
        },
        "offline_device_threshold": {
            "label": "Offline Device Threshold",
            "type": "number",
            "default": 1,
            "description": "Raise a threshold alert when offline devices reach this count.",
        },
        "alert_on_client_changes": {
            "label": "Alert On Client Changes",
            "type": "checkbox",
            "default": False,
            "description": "Send alerts for client connect/disconnect events.",
        },
        "alert_on_device_changes": {
            "label": "Alert On Device Changes",
            "type": "checkbox",
            "default": True,
            "description": "Send alerts for device online/offline transitions.",
        },
        "alert_on_wan_changes": {
            "label": "Alert On WAN Changes",
            "type": "checkbox",
            "default": True,
            "description": "Send alerts for WAN up/down transitions.",
        },
    },
}

CORE_WEBUI_TAB = {
    "label": "Connectivity",
    "order": 21,
    "requires_running": True,
}

_SETTINGS_KEY = "connectivity_core_settings"
_RUNTIME_KEY = "connectivity_core:runtime"
_ROUTES_KEY = "connectivity_core:routes"
_SNAPSHOT_KEY = "connectivity_core:last_snapshot"
_EVENTS_PREFIX = "tater:connectivity:events:"
_EVENT_SOURCE = "network"
_DEFAULT_PLATFORMS = ["discord", "irc", "matrix", "telegram", "homeassistant", "webui", "macos"]
_TRUE_TOKENS = {"1", "true", "yes", "on", "enabled", "y"}
_FALSE_TOKENS = {"0", "false", "no", "off", "disabled", "n"}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    token = _text(value).lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    return bool(default)


def _as_int(value: Any, default: int = 0, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    if minimum is not None and parsed < minimum:
        parsed = int(minimum)
    if maximum is not None and parsed > maximum:
        parsed = int(maximum)
    return parsed


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _compact(text: Any, limit: int = 220) -> str:
    clean = " ".join(_text(text).split())
    if len(clean) <= limit:
        return clean
    trimmed = clean[:limit]
    if " " in trimmed[30:]:
        trimmed = trimmed[: trimmed.rfind(" ")]
    return trimmed.rstrip(".,;: ") + "..."


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_iso(value: Any) -> Optional[datetime]:
    token = _text(value)
    if not token:
        return None
    try:
        return datetime.fromisoformat(token)
    except Exception:
        return None


def _event_key(source: str) -> str:
    token = _text(source).lower().replace(" ", "_") or _EVENT_SOURCE
    return f"{_EVENTS_PREFIX}{token}"


def _settings(client: Any = None) -> Dict[str, Any]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {}
    raw = redis_obj.hgetall(_SETTINGS_KEY) or {}
    clean: Dict[str, Any] = {}
    for key, value in raw.items():
        token = _text(value)
        low = token.lower()
        if low in _TRUE_TOKENS:
            clean[_text(key)] = True
        elif low in _FALSE_TOKENS:
            clean[_text(key)] = False
        else:
            clean[_text(key)] = token
    return clean


def _setting_int(client: Any, key: str, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    return _as_int(_settings(client).get(key, default), default, minimum=minimum, maximum=maximum)


def _runtime_get(client: Any = None) -> Dict[str, Any]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {}
    raw = redis_obj.hgetall(_RUNTIME_KEY) or {}
    out: Dict[str, Any] = {}
    for key, value in raw.items():
        token = _text(value)
        low = token.lower()
        if low in _TRUE_TOKENS:
            out[_text(key)] = True
            continue
        if low in _FALSE_TOKENS:
            out[_text(key)] = False
            continue
        try:
            if "." in token:
                out[_text(key)] = float(token)
            else:
                out[_text(key)] = int(token)
            continue
        except Exception:
            pass
        out[_text(key)] = token
    return out


def _runtime_set(client: Any, **updates: Any) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    payload = {str(key): str(value) for key, value in updates.items() if key}
    if not payload:
        return
    try:
        redis_obj.hset(_RUNTIME_KEY, mapping=payload)
    except Exception:
        logger.debug("[connectivity] failed to update runtime", exc_info=True)


def _events_retention_seconds(client: Any) -> Optional[int]:
    token = _text(_settings(client).get("events_retention") or "7d").lower()
    mapping = {
        "2d": 2 * 24 * 3600,
        "7d": 7 * 24 * 3600,
        "30d": 30 * 24 * 3600,
        "forever": None,
    }
    return mapping.get(token, 7 * 24 * 3600)


def _trim_events(client: Any, source: str) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    key = _event_key(source)
    max_events = _setting_int(redis_obj, "max_events", 600, minimum=100, maximum=5000)
    try:
        redis_obj.ltrim(key, 0, max_events - 1)
    except Exception:
        return

    retention = _events_retention_seconds(redis_obj)
    if retention is None:
        return
    cutoff = datetime.now() - timedelta(seconds=retention)
    try:
        raw_rows = redis_obj.lrange(key, 0, -1) or []
    except Exception:
        return
    keep: List[str] = []
    for row in raw_rows:
        try:
            payload = json.loads(row)
        except Exception:
            continue
        ts = _parse_iso(payload.get("ha_time"))
        if ts is None or ts < cutoff:
            continue
        keep.append(json.dumps(payload))
    try:
        pipe = redis_obj.pipeline()
        pipe.delete(key)
        if keep:
            pipe.rpush(key, *reversed(keep))
        pipe.execute()
    except Exception:
        logger.debug("[connectivity] failed trimming events", exc_info=True)


def _append_event(client: Any, source: str, payload: Dict[str, Any]) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    key = _event_key(source)
    body = dict(payload or {})
    body.setdefault("ha_time", _now_iso())
    body.setdefault("source", source)
    try:
        redis_obj.lpush(key, json.dumps(body))
    except Exception:
        logger.warning("[connectivity] failed to append event", exc_info=True)
        return
    _trim_events(redis_obj, source)


def _load_events(client: Any, *, source: str = _EVENT_SOURCE, limit: int = 200) -> List[Dict[str, Any]]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return []
    end_idx = max(1, _as_int(limit, 200, minimum=1, maximum=2000)) - 1
    try:
        rows = redis_obj.lrange(_event_key(source), 0, end_idx) or []
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row)
        except Exception:
            continue
        if isinstance(payload, dict):
            out.append(payload)
    return out


def _save_snapshot(client: Any, snapshot: Dict[str, Any]) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    try:
        redis_obj.set(_SNAPSHOT_KEY, json.dumps(snapshot))
    except Exception:
        logger.debug("[connectivity] failed to persist snapshot", exc_info=True)


def _load_snapshot(client: Any) -> Optional[Dict[str, Any]]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return None
    try:
        raw = redis_obj.get(_SNAPSHOT_KEY)
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _clean_targets_dict(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for key, value in raw.items():
        k = _text(key)
        v = _text(value)
        if k and v:
            out[k] = v
    return out


def _encode_destination_value(platform: str, targets: Dict[str, Any]) -> str:
    plat = _text(platform).lower()
    if not plat:
        return ""
    payload = {"platform": plat, "targets": _clean_targets_dict(targets)}
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)
    except Exception:
        return ""


def _decode_destination_value(raw_value: Any) -> Tuple[str, Dict[str, str]]:
    token = _text(raw_value)
    if not token:
        return "", {}
    try:
        parsed = json.loads(token)
    except Exception:
        return "", {}
    if not isinstance(parsed, dict):
        return "", {}
    return _text(parsed.get("platform")).lower(), _clean_targets_dict(parsed.get("targets"))


def _target_from_text(platform: str, text_value: Any) -> Dict[str, str]:
    token = _text(text_value)
    plat = _text(platform).lower()
    if not token:
        return {}
    if plat == "discord":
        if token.startswith("#"):
            return {"channel": token}
        if token.isdigit():
            return {"channel_id": token}
        return {"channel": f"#{token}"}
    if plat == "irc":
        return {"channel": token if token.startswith("#") else f"#{token}"}
    if plat == "matrix":
        return {"room_id": token}
    if plat == "telegram":
        return {"chat_id": token}
    if plat == "homeassistant":
        return {"device_service": token}
    if plat == "macos":
        return {"scope": token}
    return {}


def _load_destination_catalog(client: Any) -> Dict[str, Any]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {"platforms": []}
    try:
        payload = notifier_destination_catalog(redis_client=redis_obj, limit=250)
    except Exception:
        return {"platforms": []}
    if not isinstance(payload, dict):
        return {"platforms": []}
    if not isinstance(payload.get("platforms"), list):
        payload["platforms"] = []
    return payload


def _catalog_platform_rows(catalog: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    rows = catalog.get("platforms") if isinstance(catalog, dict) else []
    if not isinstance(rows, list):
        rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        platform = _text(row.get("platform")).lower()
        if platform:
            out[platform] = row
    for fallback in _DEFAULT_PLATFORMS:
        if fallback not in out:
            out[fallback] = {"platform": fallback, "label": fallback.title(), "destinations": []}
    return out


def _platform_choices(catalog: Dict[str, Any], *, current_value: str = "") -> List[Dict[str, str]]:
    rows = _catalog_platform_rows(catalog)
    out: List[Dict[str, str]] = []
    for platform in sorted(rows.keys()):
        row = rows.get(platform) or {}
        out.append({"value": platform, "label": _text(row.get("label")) or platform.title()})
    current = _text(current_value).lower()
    if current and current not in {item["value"] for item in out}:
        out.append({"value": current, "label": f"{current} (current)"})
    return out


def _destination_options_for_platform(
    platform: str,
    *,
    catalog: Dict[str, Any],
    current_targets: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    plat = _text(platform).lower()
    rows = _catalog_platform_rows(catalog)
    row = rows.get(plat) or {}
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    default_value = _encode_destination_value(plat, {})
    if default_value:
        out.append({"value": default_value, "label": "Portal defaults"})
        seen.add(default_value)

    destinations = row.get("destinations") if isinstance(row, dict) else []
    if isinstance(destinations, list):
        for item in destinations:
            if not isinstance(item, dict):
                continue
            targets = _clean_targets_dict(item.get("targets"))
            encoded = _encode_destination_value(plat, targets)
            if not encoded or encoded in seen:
                continue
            label = _text(item.get("label")) or encoded
            out.append({"value": encoded, "label": label})
            seen.add(encoded)

    current_clean = _clean_targets_dict(current_targets)
    if current_clean:
        current_encoded = _encode_destination_value(plat, current_clean)
        if current_encoded and current_encoded not in seen:
            out.append({"value": current_encoded, "label": "Current target"})
    return out


def _destination_dependency(catalog: Dict[str, Any], *, current_targets_by_platform: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    rows = _catalog_platform_rows(catalog)
    current = current_targets_by_platform if isinstance(current_targets_by_platform, dict) else {}
    options_by_source: Dict[str, List[Dict[str, str]]] = {}
    default_platform = "discord" if "discord" in rows else (next(iter(rows.keys()), ""))
    for platform in rows.keys():
        options_by_source[platform] = _destination_options_for_platform(
            platform,
            catalog=catalog,
            current_targets=current.get(platform),
        )
    default_options = options_by_source.get(default_platform, [{"value": "", "label": "Portal defaults"}])
    return {
        "source_key": "platform",
        "options_by_source": options_by_source,
        "default_options": default_options,
    }


def _normalize_route(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    platform = _text(raw.get("platform")).lower()
    if not platform:
        return None
    route_id = _text(raw.get("id")) or str(uuid.uuid4())
    targets = _clean_targets_dict(raw.get("targets"))
    name = _text(raw.get("name")) or f"{platform.title()} route"
    now_ts = time.time()
    return {
        "id": route_id,
        "name": name,
        "platform": platform,
        "targets": targets,
        "enabled": _bool(raw.get("enabled"), True),
        "created_at": _as_float(raw.get("created_at"), now_ts),
        "updated_at": _as_float(raw.get("updated_at"), now_ts),
        "last_sent_ts": _as_float(raw.get("last_sent_ts"), 0.0),
        "last_result": _text(raw.get("last_result")),
    }


def _load_routes(client: Any) -> Dict[str, Dict[str, Any]]:
    redis_obj = client or redis_client
    if redis_obj is None:
        return {}
    raw = redis_obj.hgetall(_ROUTES_KEY) or {}
    out: Dict[str, Dict[str, Any]] = {}
    for route_id, blob in raw.items():
        try:
            payload = json.loads(blob)
        except Exception:
            continue
        normalized = _normalize_route(payload)
        if not normalized:
            continue
        out[normalized["id"]] = normalized
    return out


def _save_route(client: Any, route: Dict[str, Any]) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        raise ValueError("Redis connection unavailable.")
    normalized = _normalize_route(route)
    if not normalized:
        raise ValueError("Invalid route payload.")
    normalized["updated_at"] = time.time()
    redis_obj.hset(_ROUTES_KEY, normalized["id"], json.dumps(normalized))


def _remove_route(client: Any, route_id: str) -> bool:
    redis_obj = client or redis_client
    if redis_obj is None:
        return False
    rid = _text(route_id)
    if not rid:
        return False
    try:
        removed = redis_obj.hdel(_ROUTES_KEY, rid)
    except Exception:
        return False
    return bool(removed)


def _unifi_network_config() -> Dict[str, str]:
    base = _text(redis_client.get("tater:unifi_network:base_url") or "https://10.4.20.1").rstrip("/")
    api_key = _text(redis_client.get("tater:unifi_network:api_key"))
    if not api_key:
        raise ValueError(
            "UniFi Network API key is not set. Open WebUI -> Settings -> Integrations -> UniFi Network and add API key."
        )
    if not base:
        base = "https://10.4.20.1"
    return {"base": base, "api_key": api_key}


def _unifi_network_configured() -> bool:
    return bool(_text(redis_client.get("tater:unifi_network:api_key")))


def _unifi_headers(api_key: str) -> Dict[str, str]:
    return {"X-API-KEY": api_key, "Accept": "application/json"}


def _unifi_request(method: str, path: str, *, params: Optional[Dict[str, Any]] = None) -> Any:
    conf = _unifi_network_config()
    clean_path = path if str(path).startswith("/") else f"/{path}"
    url = f"{conf['base']}/proxy/network/integration{clean_path}"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", InsecureRequestWarning)
        resp = requests.request(
            method=method.upper(),
            url=url,
            headers=_unifi_headers(conf["api_key"]),
            params=params or None,
            timeout=20,
            verify=False,
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"UniFi Network HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        return resp.json()
    except Exception:
        return {}


def _unifi_get_sites() -> List[Dict[str, Any]]:
    payload = _unifi_request("GET", "/v1/sites")
    rows = payload.get("data") if isinstance(payload, dict) else []
    return rows if isinstance(rows, list) else []


def _unifi_get_paged(path: str, *, limit: int = 200) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    page_limit = max(25, min(500, int(limit)))
    for _ in range(2000):
        payload = _unifi_request("GET", path, params={"offset": str(offset), "limit": str(page_limit)})
        if not isinstance(payload, dict):
            break
        data = payload.get("data")
        page = data if isinstance(data, list) else []
        out.extend([row for row in page if isinstance(row, dict)])
        try:
            total = int(payload.get("totalCount"))
        except Exception:
            total = None
        try:
            count = int(payload.get("count"))
        except Exception:
            count = len(page)
        if total is not None and len(out) >= total:
            break
        if count < page_limit:
            break
        offset += page_limit
    return out


def _device_status(row: Dict[str, Any]) -> str:
    state = _text(row.get("state") or row.get("status")).upper()
    if state in {"ONLINE", "CONNECTED", "ADOPTED"}:
        return "online"
    if state in {"OFFLINE", "DISCONNECTED", "ISOLATED"}:
        return "offline"
    is_online = row.get("isOnline")
    if is_online is True:
        return "online"
    if is_online is False:
        return "offline"
    online = row.get("online")
    if online is True:
        return "online"
    if online is False:
        return "offline"
    return "unknown"


def _client_link_type(row: Dict[str, Any]) -> str:
    token = _text(row.get("type") or row.get("connectionType") or row.get("linkType")).upper()
    if token == "WIRED":
        return "wired"
    if token == "WIRELESS":
        return "wireless"
    if row.get("isWireless") is True:
        return "wireless"
    if row.get("isWired") is True:
        return "wired"
    return "unknown"


def _client_id(row: Dict[str, Any]) -> str:
    return _text(row.get("id") or row.get("macAddress") or row.get("mac") or row.get("hostname") or row.get("name"))


def _client_name(row: Dict[str, Any]) -> str:
    return _text(row.get("name") or row.get("hostname") or row.get("displayName") or _client_id(row))


def _device_id(row: Dict[str, Any]) -> str:
    return _text(row.get("id") or row.get("macAddress") or row.get("mac") or row.get("name"))


def _device_name(row: Dict[str, Any]) -> str:
    return _text(row.get("name") or row.get("displayName") or _device_id(row))


def _as_bool_token(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    token = _text(value).lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    if token in {"up", "online", "connected", "ok", "healthy"}:
        return True
    if token in {"down", "offline", "disconnected", "unhealthy", "failed"}:
        return False
    return None


def _site_wan_up(site: Dict[str, Any]) -> Optional[bool]:
    keys = [
        "internetUp",
        "isInternetConnected",
        "wanUp",
        "isWanConnected",
        "uplinkConnected",
        "internetStatus",
        "wanStatus",
    ]
    for key in keys:
        if key in site:
            parsed = _as_bool_token(site.get(key))
            if parsed is not None:
                return parsed
    for container_key in ("wan", "uplink", "internet", "health"):
        nested = site.get(container_key)
        if not isinstance(nested, dict):
            continue
        for key in keys:
            if key in nested:
                parsed = _as_bool_token(nested.get(key))
                if parsed is not None:
                    return parsed
    return None


def _coalesce_int(row: Dict[str, Any], keys: List[str]) -> int:
    for key in keys:
        value = row.get(key)
        try:
            parsed = int(float(value))
            if parsed >= 0:
                return parsed
        except Exception:
            continue
    return 0


def _build_snapshot(site: Dict[str, Any], clients: List[Dict[str, Any]], devices: List[Dict[str, Any]]) -> Dict[str, Any]:
    client_rows = [row for row in clients if isinstance(row, dict)]
    device_rows = [row for row in devices if isinstance(row, dict)]

    clients_by_id: Dict[str, Dict[str, Any]] = {}
    wired = 0
    wireless = 0
    client_unknown = 0
    rx_total = 0
    tx_total = 0

    for row in client_rows:
        cid = _client_id(row)
        if not cid:
            continue
        link = _client_link_type(row)
        if link == "wired":
            wired += 1
        elif link == "wireless":
            wireless += 1
        else:
            client_unknown += 1
        clients_by_id[cid] = {
            "id": cid,
            "name": _client_name(row),
            "link": link,
            "ip": _text(row.get("ipAddress") or row.get("ip")),
            "mac": _text(row.get("macAddress") or row.get("mac")),
        }
        rx_total += _coalesce_int(row, ["rxBytes", "bytesReceived", "downloadBytes", "rx_bytes"])
        tx_total += _coalesce_int(row, ["txBytes", "bytesSent", "uploadBytes", "tx_bytes"])

    devices_by_id: Dict[str, Dict[str, Any]] = {}
    device_online = 0
    device_offline = 0
    device_unknown = 0

    for row in device_rows:
        did = _device_id(row)
        if not did:
            continue
        status = _device_status(row)
        if status == "online":
            device_online += 1
        elif status == "offline":
            device_offline += 1
        else:
            device_unknown += 1
        devices_by_id[did] = {
            "id": did,
            "name": _device_name(row),
            "status": status,
            "ip": _text(row.get("ipAddress") or row.get("ip")),
            "mac": _text(row.get("macAddress") or row.get("mac")),
            "model": _text(row.get("model") or row.get("type")),
        }

    site_name = _text(site.get("name") or site.get("internalReference") or site.get("id") or "Unknown Site")
    wan_up = _site_wan_up(site)

    return {
        "polled_at": _now_iso(),
        "site": {
            "id": _text(site.get("id")),
            "name": site_name,
            "wan_up": wan_up,
        },
        "counts": {
            "clients_total": len(clients_by_id),
            "clients_wired": wired,
            "clients_wireless": wireless,
            "clients_unknown_link": client_unknown,
            "devices_total": len(devices_by_id),
            "devices_online": device_online,
            "devices_offline": device_offline,
            "devices_unknown_status": device_unknown,
            "traffic_rx_bytes": rx_total,
            "traffic_tx_bytes": tx_total,
        },
        "clients": clients_by_id,
        "devices": devices_by_id,
    }


def _fetch_network_snapshot_sync() -> Dict[str, Any]:
    sites = _unifi_get_sites()
    if not sites:
        raise RuntimeError("No UniFi Network sites returned from /v1/sites")
    site = sites[0] if isinstance(sites[0], dict) else {}
    site_id = _text(site.get("id"))
    if not site_id:
        raise RuntimeError("UniFi Network site id missing from /v1/sites")
    clients = _unifi_get_paged(f"/v1/sites/{site_id}/clients")
    devices = _unifi_get_paged(f"/v1/sites/{site_id}/devices")
    return _build_snapshot(site, clients, devices)


def _event_level(type_token: str) -> str:
    token = _text(type_token).lower()
    if token in {"wan_down", "device_threshold", "api_down", "devices_offline"}:
        return "warning"
    return "info"


def _make_event(*, event_type: str, title: str, message: str, data: Optional[Dict[str, Any]] = None, alert: bool = False) -> Dict[str, Any]:
    return {
        "source": _EVENT_SOURCE,
        "title": _compact(title, limit=120),
        "type": _text(event_type).lower() or "status",
        "message": _compact(message, limit=280),
        "ha_time": _now_iso(),
        "level": _event_level(event_type),
        "alert": bool(alert),
        "data": data if isinstance(data, dict) else {},
    }


def _names_from_ids(rows: Dict[str, Dict[str, Any]], ids: List[str], *, limit: int = 5) -> List[str]:
    out: List[str] = []
    for token in ids[: max(1, int(limit))]:
        row = rows.get(token) if isinstance(rows, dict) else None
        if isinstance(row, dict):
            out.append(_text(row.get("name") or token))
        else:
            out.append(_text(token))
    return [name for name in out if name]


def _build_change_events(prev: Optional[Dict[str, Any]], curr: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(curr, dict):
        return []
    events: List[Dict[str, Any]] = []
    if not isinstance(prev, dict):
        return events

    monitor_wan = _bool(settings.get("monitor_wan"), True)
    monitor_devices = _bool(settings.get("monitor_devices"), True)
    monitor_clients = _bool(settings.get("monitor_clients"), False)

    alert_on_wan = _bool(settings.get("alert_on_wan_changes"), True)
    alert_on_devices = _bool(settings.get("alert_on_device_changes"), True)
    alert_on_clients = _bool(settings.get("alert_on_client_changes"), False)

    prev_site = prev.get("site") if isinstance(prev.get("site"), dict) else {}
    curr_site = curr.get("site") if isinstance(curr.get("site"), dict) else {}
    prev_wan = prev_site.get("wan_up")
    curr_wan = curr_site.get("wan_up")

    if monitor_wan and isinstance(prev_wan, bool) and isinstance(curr_wan, bool) and prev_wan != curr_wan:
        if curr_wan:
            events.append(
                _make_event(
                    event_type="wan_up",
                    title="WAN restored",
                    message=f"Internet/WAN appears back online for {curr_site.get('name') or 'site'}.",
                    data={"site": _text(curr_site.get("name"))},
                    alert=alert_on_wan,
                )
            )
        else:
            events.append(
                _make_event(
                    event_type="wan_down",
                    title="WAN down",
                    message=f"Internet/WAN appears offline for {curr_site.get('name') or 'site'}.",
                    data={"site": _text(curr_site.get("name"))},
                    alert=alert_on_wan,
                )
            )

    prev_devices = prev.get("devices") if isinstance(prev.get("devices"), dict) else {}
    curr_devices = curr.get("devices") if isinstance(curr.get("devices"), dict) else {}

    if monitor_devices:
        went_offline: List[str] = []
        came_online: List[str] = []
        for device_id, row in curr_devices.items():
            current_status = _text((row or {}).get("status"))
            previous_status = _text((prev_devices.get(device_id) or {}).get("status"))
            if current_status == "offline" and previous_status in {"online", "unknown", ""}:
                went_offline.append(device_id)
            elif current_status == "online" and previous_status == "offline":
                came_online.append(device_id)

        if went_offline:
            names = _names_from_ids(curr_devices, went_offline)
            more = "" if len(went_offline) <= len(names) else f" (+{len(went_offline) - len(names)} more)"
            events.append(
                _make_event(
                    event_type="devices_offline",
                    title="Devices offline",
                    message=f"{len(went_offline)} UniFi device(s) went offline: {', '.join(names)}{more}.",
                    data={"count": len(went_offline)},
                    alert=alert_on_devices,
                )
            )

        if came_online:
            names = _names_from_ids(curr_devices, came_online)
            more = "" if len(came_online) <= len(names) else f" (+{len(came_online) - len(names)} more)"
            events.append(
                _make_event(
                    event_type="devices_online",
                    title="Devices online",
                    message=f"{len(came_online)} UniFi device(s) came online: {', '.join(names)}{more}.",
                    data={"count": len(came_online)},
                    alert=alert_on_devices,
                )
            )

        threshold = _as_int(settings.get("offline_device_threshold"), 1, minimum=1, maximum=500)
        prev_off = _as_int((prev.get("counts") or {}).get("devices_offline"), 0, minimum=0)
        curr_off = _as_int((curr.get("counts") or {}).get("devices_offline"), 0, minimum=0)
        if prev_off < threshold <= curr_off:
            events.append(
                _make_event(
                    event_type="device_threshold",
                    title="Offline threshold reached",
                    message=f"Offline UniFi devices reached {curr_off} (threshold {threshold}).",
                    data={"threshold": threshold, "offline": curr_off},
                    alert=True,
                )
            )

    prev_clients = prev.get("clients") if isinstance(prev.get("clients"), dict) else {}
    curr_clients = curr.get("clients") if isinstance(curr.get("clients"), dict) else {}

    if monitor_clients:
        prev_ids = set(prev_clients.keys())
        curr_ids = set(curr_clients.keys())
        joined = sorted(list(curr_ids - prev_ids))
        left = sorted(list(prev_ids - curr_ids))

        if joined:
            names = _names_from_ids(curr_clients, joined)
            more = "" if len(joined) <= len(names) else f" (+{len(joined) - len(names)} more)"
            events.append(
                _make_event(
                    event_type="clients_joined",
                    title="Clients connected",
                    message=f"{len(joined)} client(s) appeared: {', '.join(names)}{more}.",
                    data={"count": len(joined)},
                    alert=alert_on_clients,
                )
            )
        if left:
            names = _names_from_ids(prev_clients, left)
            more = "" if len(left) <= len(names) else f" (+{len(left) - len(names)} more)"
            events.append(
                _make_event(
                    event_type="clients_left",
                    title="Clients disconnected",
                    message=f"{len(left)} client(s) disappeared: {', '.join(names)}{more}.",
                    data={"count": len(left)},
                    alert=alert_on_clients,
                )
            )

    return events


async def _dispatch_event_alerts(event: Dict[str, Any], *, client: Any = None) -> Dict[str, Any]:
    redis_obj = client or redis_client
    settings = _settings(redis_obj)
    if not _bool(settings.get("alerts_enabled"), False):
        return {"ok": True, "sent": 0, "skipped": "alerts_disabled"}

    routes = [route for route in _load_routes(redis_obj).values() if _bool(route.get("enabled"), True)]
    if not routes:
        return {"ok": True, "sent": 0, "skipped": "no_routes"}

    title = _text(event.get("title") or "Connectivity Alert")
    message = _text(event.get("message") or "Connectivity event detected.")
    sent = 0
    errors: List[str] = []

    for route in routes:
        platform = _text(route.get("platform")).lower()
        targets = route.get("targets") if isinstance(route.get("targets"), dict) else {}
        try:
            result = await dispatch_notification(
                platform=platform,
                title=title,
                content=message,
                targets=targets,
                origin={
                    "platform": "connectivity_core",
                    "scope": "network_alert",
                    "event_type": _text(event.get("type")),
                },
                meta={
                    "priority": "high" if _text(event.get("level")).lower() in {"warning", "error"} else "normal",
                    "tags": ["connectivity"],
                },
            )
            if isinstance(result, str) and result.startswith("Cannot queue"):
                errors.append(f"{platform}: {result}")
            else:
                sent += 1
                route["last_sent_ts"] = time.time()
                route["last_result"] = "ok"
                _save_route(redis_obj, route)
        except Exception as exc:
            errors.append(f"{platform}: {exc}")

    return {
        "ok": sent > 0 and not errors,
        "sent": sent,
        "errors": errors,
    }


def _event_time_display(value: Any) -> str:
    parsed = _parse_iso(value)
    if parsed is not None:
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    return _text(value) or "n/a"


def _stats_from_snapshot(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {
            "site_name": "n/a",
            "wan": "unknown",
            "counts": {
                "clients_total": 0,
                "clients_wired": 0,
                "clients_wireless": 0,
                "clients_unknown_link": 0,
                "devices_total": 0,
                "devices_online": 0,
                "devices_offline": 0,
                "devices_unknown_status": 0,
                "traffic_rx_bytes": 0,
                "traffic_tx_bytes": 0,
            },
            "polled_at": "n/a",
        }
    site = snapshot.get("site") if isinstance(snapshot.get("site"), dict) else {}
    counts = snapshot.get("counts") if isinstance(snapshot.get("counts"), dict) else {}
    wan_up = site.get("wan_up")
    if isinstance(wan_up, bool):
        wan_text = "up" if wan_up else "down"
    else:
        wan_text = "unknown"
    return {
        "site_name": _text(site.get("name")) or "n/a",
        "wan": wan_text,
        "counts": {
            "clients_total": _as_int(counts.get("clients_total"), 0, minimum=0),
            "clients_wired": _as_int(counts.get("clients_wired"), 0, minimum=0),
            "clients_wireless": _as_int(counts.get("clients_wireless"), 0, minimum=0),
            "clients_unknown_link": _as_int(counts.get("clients_unknown_link"), 0, minimum=0),
            "devices_total": _as_int(counts.get("devices_total"), 0, minimum=0),
            "devices_online": _as_int(counts.get("devices_online"), 0, minimum=0),
            "devices_offline": _as_int(counts.get("devices_offline"), 0, minimum=0),
            "devices_unknown_status": _as_int(counts.get("devices_unknown_status"), 0, minimum=0),
            "traffic_rx_bytes": _as_int(counts.get("traffic_rx_bytes"), 0, minimum=0),
            "traffic_tx_bytes": _as_int(counts.get("traffic_tx_bytes"), 0, minimum=0),
        },
        "polled_at": _text(snapshot.get("polled_at")) or "n/a",
    }


def _bytes_label(value: int) -> str:
    amount = float(_as_int(value, 0, minimum=0))
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while amount >= 1024 and idx < len(units) - 1:
        amount /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(amount)} {units[idx]}"
    return f"{amount:.1f} {units[idx]}"


def _build_route_from_values(*, values: Dict[str, Any], payload: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    previous = existing if isinstance(existing, dict) else {}
    route_id = _text(previous.get("id")) or str(uuid.uuid4())

    platform = _text(values.get("platform", payload.get("platform", previous.get("platform", "discord")))).lower()
    destination_raw = values.get("destination", payload.get("destination", ""))
    destination_platform, destination_targets = _decode_destination_value(destination_raw)
    if destination_platform:
        platform = destination_platform

    targets = dict(destination_targets)
    target_text = _text(values.get("target_text", payload.get("target_text", "")))
    if not targets and target_text:
        targets = _target_from_text(platform, target_text)

    name = _text(values.get("name", payload.get("name", previous.get("name", "")))) or f"{platform.title()} route"
    enabled = _bool(values.get("enabled", payload.get("enabled", previous.get("enabled", True))), True)

    now_ts = time.time()
    route = {
        "id": route_id,
        "name": name,
        "platform": platform,
        "targets": _clean_targets_dict(targets),
        "enabled": enabled,
        "created_at": _as_float(previous.get("created_at"), now_ts),
        "updated_at": now_ts,
        "last_sent_ts": _as_float(previous.get("last_sent_ts"), 0.0),
        "last_result": _text(previous.get("last_result")),
    }
    normalized = _normalize_route(route)
    if not normalized:
        raise ValueError("Invalid route settings.")
    return normalized


def _route_forms(client: Any) -> List[Dict[str, Any]]:
    catalog = _load_destination_catalog(client)
    dependency = _destination_dependency(catalog)
    platform_choices = _platform_choices(catalog)
    forms: List[Dict[str, Any]] = []

    for route in sorted(_load_routes(client).values(), key=lambda row: (_text(row.get("name")).lower(), _text(row.get("id")))):
        platform = _text(route.get("platform")).lower() or "discord"
        current_targets = route.get("targets") if isinstance(route.get("targets"), dict) else {}
        destination_options = _destination_options_for_platform(platform, catalog=catalog, current_targets=current_targets)
        destination_value = _encode_destination_value(platform, current_targets)
        route_subtitle = f"{'Enabled' if _bool(route.get('enabled'), True) else 'Disabled'} • {platform}"
        if current_targets:
            route_subtitle += f" • targets: {', '.join([f'{k}={v}' for k, v in current_targets.items()])}"
        forms.append(
            {
                "id": route["id"],
                "group": "route",
                "title": _text(route.get("name")) or "Alert Route",
                "subtitle": route_subtitle,
                "save_action": "connectivity_save_route",
                "remove_action": "connectivity_remove_route",
                "remove_confirm": "Remove this connectivity alert route?",
                "fields": [
                    {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": _bool(route.get("enabled"), True)},
                    {"key": "name", "label": "Route Name", "type": "text", "value": _text(route.get("name"))},
                    {"key": "platform", "label": "Platform", "type": "select", "options": platform_choices, "value": platform},
                    {
                        "key": "destination",
                        "label": "Destination",
                        "type": "select",
                        "options": destination_options,
                        "dependent_options": dependency,
                        "value": destination_value,
                    },
                    {
                        "key": "target_text",
                        "label": "Target Override (optional)",
                        "type": "text",
                        "value": "",
                        "description": "Optional fallback target if destination list is empty (e.g. #alerts, !room:id, chat_id).",
                    },
                ],
            }
        )
    return forms


def _event_forms(client: Any, *, limit: int = 120) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, event in enumerate(_load_events(client, limit=limit)):
        title = _text(event.get("title")) or "Connectivity Event"
        message = _text(event.get("message"))
        level = _text(event.get("level") or "info").lower()
        subtitle = f"{_event_time_display(event.get('ha_time'))} • {level}"
        out.append(
            {
                "id": f"event_{idx}",
                "group": "event",
                "title": title,
                "subtitle": subtitle,
                "fields": [
                    {
                        "key": f"message_{idx}",
                        "label": "",
                        "type": "textarea",
                        "value": message,
                        "read_only": True,
                        "hide_label": True,
                    }
                ],
            }
        )
    return out


def _connectivity_manager_ui(client: Any) -> Dict[str, Any]:
    catalog = _load_destination_catalog(client)
    platform_choices = _platform_choices(catalog)
    dependency = _destination_dependency(catalog)
    default_platform = platform_choices[0]["value"] if platform_choices else "discord"
    default_dest_options = _destination_options_for_platform(default_platform, catalog=catalog)

    settings = _settings(client)
    route_forms = _route_forms(client)
    event_forms = _event_forms(client)

    return {
        "kind": "settings_manager",
        "title": "Connectivity Manager",
        "empty_message": "No connectivity routes configured yet.",
        "stats_refresh_button": True,
        "stats_refresh_label": "Refresh",
        "stats_controls_action": "connectivity_save_controls",
        "stats_controls_auto_save": True,
        "stats_controls": [
            {"key": "alerts_enabled", "label": "Alerts", "type": "checkbox", "value": _bool(settings.get("alerts_enabled"), False)},
            {"key": "monitor_wan", "label": "WAN", "type": "checkbox", "value": _bool(settings.get("monitor_wan"), True)},
            {"key": "monitor_devices", "label": "Devices", "type": "checkbox", "value": _bool(settings.get("monitor_devices"), True)},
            {"key": "monitor_clients", "label": "Clients", "type": "checkbox", "value": _bool(settings.get("monitor_clients"), False)},
        ],
        "item_fields_dropdown": True,
        "item_fields_dropdown_label": "Route Settings",
        "item_fields_popup": True,
        "item_fields_popup_label": "Route Settings",
        "default_tab": "events",
        "manager_tabs": [
            {
                "key": "events",
                "label": "Events",
                "source": "items",
                "item_group": "event",
                "selector": False,
                "empty_message": "No connectivity events yet.",
            },
            {
                "key": "routes",
                "label": "Alert Routes",
                "source": "items",
                "item_group": "route",
                "selector": False,
                "empty_message": "No alert routes configured yet.",
            },
            {
                "key": "create",
                "label": "Add Route",
                "source": "add_form",
            },
        ],
        "add_form": {
            "action": "connectivity_add_route",
            "submit_label": "Add Route",
            "fields": [
                {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": True},
                {"key": "name", "label": "Route Name", "type": "text", "value": ""},
                {"key": "platform", "label": "Platform", "type": "select", "options": platform_choices, "value": default_platform},
                {
                    "key": "destination",
                    "label": "Destination",
                    "type": "select",
                    "options": default_dest_options,
                    "dependent_options": dependency,
                    "value": default_dest_options[0]["value"] if default_dest_options else "",
                },
                {
                    "key": "target_text",
                    "label": "Target Override (optional)",
                    "type": "text",
                    "value": "",
                    "description": "Optional fallback target if destination list is empty.",
                },
            ],
        },
        "item_forms": event_forms + route_forms,
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def _value(values: Dict[str, Any], payload: Dict[str, Any], key: str, default: Any = "") -> Any:
    if key in values:
        return values.get(key)
    return payload.get(key, default)


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    snapshot = _load_snapshot(client)
    runtime = _runtime_get(client)
    stats_payload = _stats_from_snapshot(snapshot)
    counts = stats_payload.get("counts") if isinstance(stats_payload.get("counts"), dict) else {}

    routes = _load_routes(client)
    enabled_routes = sum(1 for route in routes.values() if _bool(route.get("enabled"), True))
    events = _load_events(client, limit=200)

    return {
        "summary": "Connectivity Core monitors UniFi Network clients, devices, WAN status, and route-based alerts.",
        "stats": [
            {"label": "Site", "value": _text(stats_payload.get("site_name")) or "n/a"},
            {"label": "WAN", "value": _text(stats_payload.get("wan")) or "unknown"},
            {"label": "Clients Total", "value": _as_int(counts.get("clients_total"), 0, minimum=0)},
            {"label": "Clients Wired", "value": _as_int(counts.get("clients_wired"), 0, minimum=0)},
            {"label": "Clients Wireless", "value": _as_int(counts.get("clients_wireless"), 0, minimum=0)},
            {"label": "Devices Total", "value": _as_int(counts.get("devices_total"), 0, minimum=0)},
            {"label": "Devices Offline", "value": _as_int(counts.get("devices_offline"), 0, minimum=0)},
            {"label": "Traffic RX", "value": _bytes_label(_as_int(counts.get("traffic_rx_bytes"), 0, minimum=0))},
            {"label": "Traffic TX", "value": _bytes_label(_as_int(counts.get("traffic_tx_bytes"), 0, minimum=0))},
            {"label": "Routes Enabled", "value": enabled_routes},
            {"label": "Stored Events", "value": len(events)},
            {"label": "Last Poll", "value": _event_time_display(stats_payload.get("polled_at"))},
            {"label": "API Connected", "value": "yes" if _bool(runtime.get("api_connected"), False) else "no"},
        ],
        "items": [],
        "empty_message": "No connectivity data yet.",
        "ui": _connectivity_manager_ui(client),
    }


def _settings_update(client: Any, updates: Dict[str, Any]) -> None:
    redis_obj = client or redis_client
    if redis_obj is None:
        return
    payload = {str(key): str(value) for key, value in updates.items() if key}
    if not payload:
        return
    redis_obj.hset(_SETTINGS_KEY, mapping=payload)


def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    if client is None:
        raise ValueError("Redis connection is unavailable.")

    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)
    action_name = _text(action).lower()

    if action_name == "connectivity_save_controls":
        current = _settings(client)
        updates = {
            "alerts_enabled": "1" if _bool(_value(values, body, "alerts_enabled", current.get("alerts_enabled", False)), False) else "0",
            "monitor_wan": "1" if _bool(_value(values, body, "monitor_wan", current.get("monitor_wan", True)), True) else "0",
            "monitor_devices": "1" if _bool(_value(values, body, "monitor_devices", current.get("monitor_devices", True)), True) else "0",
            "monitor_clients": "1" if _bool(_value(values, body, "monitor_clients", current.get("monitor_clients", False)), False) else "0",
        }
        _settings_update(client, updates)
        return {"ok": True, "message": "Connectivity monitor controls updated."}

    if action_name == "connectivity_add_route":
        route = _build_route_from_values(values=values, payload=body, existing=None)
        _save_route(client, route)
        return {"ok": True, "id": route["id"], "message": "Connectivity alert route added."}

    if action_name == "connectivity_save_route":
        route_id = _text(body.get("id"))
        if not route_id:
            raise ValueError("Route id is required.")
        existing = _load_routes(client).get(route_id)
        if not existing:
            raise KeyError("Connectivity route not found.")
        route = _build_route_from_values(values=values, payload=body, existing=existing)
        _save_route(client, route)
        return {"ok": True, "id": route_id, "message": "Connectivity alert route saved."}

    if action_name == "connectivity_remove_route":
        route_id = _text(body.get("id"))
        if not route_id:
            raise ValueError("Route id is required.")
        removed = _remove_route(client, route_id)
        if not removed:
            raise KeyError("Connectivity route not found.")
        return {"ok": True, "id": route_id, "message": "Connectivity alert route removed."}

    raise ValueError(f"Unknown action: {action_name}")


async def _connectivity_poll_loop(stop_event: Optional[object]) -> None:
    previous_snapshot = _load_snapshot(redis_client)
    had_api_error = False

    while not (stop_event and stop_event.is_set()):
        settings = _settings(redis_client)
        poll_sec = _as_int(settings.get("poll_interval_sec"), 15, minimum=3, maximum=300)

        if not _unifi_network_configured():
            _runtime_set(redis_client, api_connected=False, last_error="", last_poll_ts=time.time())
            had_api_error = False
            await asyncio.sleep(float(poll_sec))
            continue

        try:
            current_snapshot = await asyncio.to_thread(_fetch_network_snapshot_sync)
            now_ts = time.time()
            _save_snapshot(redis_client, current_snapshot)
            _runtime_set(
                redis_client,
                api_connected=True,
                last_error="",
                last_poll_ts=now_ts,
                site_name=_text(((current_snapshot.get("site") or {}).get("name"))),
            )

            if had_api_error:
                recovered_event = _make_event(
                    event_type="api_up",
                    title="UniFi API restored",
                    message="Connectivity Core restored access to UniFi Network API.",
                    alert=True,
                )
                _append_event(redis_client, _EVENT_SOURCE, recovered_event)
                await _dispatch_event_alerts(recovered_event, client=redis_client)
                had_api_error = False

            events = _build_change_events(previous_snapshot, current_snapshot, settings)
            for event in events:
                _append_event(redis_client, _EVENT_SOURCE, event)
                if _bool(event.get("alert"), False):
                    await _dispatch_event_alerts(event, client=redis_client)

            previous_snapshot = current_snapshot

        except Exception as exc:
            now_ts = time.time()
            _runtime_set(redis_client, api_connected=False, last_error=str(exc), last_poll_ts=now_ts)
            if not had_api_error:
                down_event = _make_event(
                    event_type="api_down",
                    title="UniFi API unavailable",
                    message=f"Connectivity Core failed to poll UniFi Network API: {_compact(exc, 180)}",
                    alert=True,
                )
                _append_event(redis_client, _EVENT_SOURCE, down_event)
                await _dispatch_event_alerts(down_event, client=redis_client)
                had_api_error = True
            logger.warning("[connectivity] poll error: %s", exc)

        await asyncio.sleep(float(poll_sec))


async def _connectivity_main(stop_event: Optional[object]) -> None:
    routes = _load_routes(redis_client)
    _runtime_set(
        redis_client,
        started_at=time.time(),
        api_connected=False,
        last_error="",
        route_count=len(routes),
    )
    logger.info(
        "[connectivity] core started v%s (%s) (routes=%d)",
        __version__,
        __file__,
        len(routes),
    )

    task = asyncio.create_task(_connectivity_poll_loop(stop_event))
    try:
        while not (stop_event and stop_event.is_set()):
            await asyncio.sleep(0.5)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        _runtime_set(redis_client, api_connected=False)
        logger.info("[connectivity] core stopped")


def run(stop_event=None):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_connectivity_main(stop_event))
    except asyncio.CancelledError:
        logger.info("[connectivity] core cancelled; stopping")
    except KeyboardInterrupt:
        logger.info("[connectivity] core interrupted; stopping")
    except Exception:
        logger.exception("[connectivity] core crashed")
        raise
    finally:
        try:
            loop.close()
        except Exception:
            pass
