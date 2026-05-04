"""Environment Core receives local environment telemetry and keeps a normalized live state."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from html import escape as html_escape
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, quote

from helpers import extract_json, redis_client

__version__ = "1.4.11"
MIN_TATER_VERSION = "59"
CORE_DESCRIPTION = "Local environment telemetry receiver for weather stations and configured sensor integrations."
TAGS = ["environment", "weather", "ecowitt", "telemetry"]

logger = logging.getLogger("environment_core")
logger.setLevel(logging.INFO)

CORE_SETTINGS = {
    "category": "Environment Core Settings",
    # Let Hydra answer from cached Environment Core readings even if the polling loop is stopped.
    "hydra_tools_require_running": False,
    "required": {
        "ECOWITT_PASSKEY": {
            "label": "Ecowitt PASSKEY",
            "type": "password",
            "default": "",
            "description": "Optional. If set, inbound Ecowitt uploads must include this PASSKEY.",
        },
        "ENVIRONMENT_ECOWITT_OUTDOOR_AREA": {
            "label": "Ecowitt Outdoor Area",
            "type": "text",
            "default": "Outside",
            "description": "Area label applied to outdoor Ecowitt readings.",
        },
        "ENVIRONMENT_ECOWITT_INDOOR_AREA": {
            "label": "Ecowitt Indoor Area",
            "type": "text",
            "default": "Inside",
            "description": "Area label applied to indoor Ecowitt readings.",
        },
        "ENVIRONMENT_INTEGRATION_POLL_SECONDS": {
            "label": "Integration Poll Seconds",
            "type": "number",
            "default": "300",
            "description": "How often the Environment Core polls selected integration sensors.",
        },
        "ENVIRONMENT_ENABLE_WEATHERAPI": {
            "label": "Enable WeatherAPI Forecast",
            "type": "checkbox",
            "default": False,
            "description": "When enabled, Environment Core polls the WeatherAPI.com integration for current conditions and forecast data.",
        },
        "ENVIRONMENT_CURRENT_CONDITION_LIVE_SOURCE": {
            "label": "Current Card Readings Source",
            "type": "select",
            "default": "provider:ecowitt",
            "description": "Integration or selected sensor used for measured values on the Current Conditions card.",
        },
        "ENVIRONMENT_CURRENT_CONDITION_CONDITION_SOURCE": {
            "label": "Current Card Artwork Source",
            "type": "select",
            "default": "provider:weather_api",
            "description": "Integration or selected sensor used for sunny/rainy condition text and artwork.",
        },
        "ENVIRONMENT_FORECAST_PROVIDER": {
            "label": "Forecast Provider",
            "type": "select",
            "default": "weather_api",
            "description": "Forecast provider shown in the Forecast tab.",
        },
        "ENVIRONMENT_HISTORY_LIMIT": {
            "label": "History Samples",
            "type": "number",
            "default": "288",
            "description": "Maximum recent snapshots retained in Redis.",
        },
        "ENVIRONMENT_STALE_AFTER_MINUTES": {
            "label": "Stale After Minutes",
            "type": "number",
            "default": "30",
            "description": "UI marks readings stale after this many minutes without a new upload.",
        },
    },
}

CORE_WEBUI_TAB = {
    "label": "Environment",
    "order": 34,
    "requires_running": False,
}

SETTINGS_KEY = "environment_core_settings"
LATEST_KEY = "environment:latest"
LATEST_ECOWITT_KEY = "environment:latest:ecowitt"
LATEST_UNIFI_PROTECT_KEY = "environment:latest:unifi_protect"
LATEST_ECOBEE_HOMEKIT_KEY = "environment:latest:ecobee_homekit"
LATEST_HUE_KEY = "environment:latest:hue"
LATEST_HOMEASSISTANT_KEY = "environment:latest:homeassistant"
LATEST_WEATHER_API_KEY = "environment:latest:weather_api"
SOURCES_KEY = "environment:sources"
HISTORY_KEY = "environment:history"
HEARTBEAT_KEY = "environment:heartbeat"
SELECTED_SENSORS_KEY = "environment:selected_sensors"
CANDIDATE_SENSORS_KEY = "environment:candidate_sensors"

DEFAULT_HISTORY_LIMIT = 288
DEFAULT_STALE_AFTER_MINUTES = 30
DEFAULT_INTEGRATION_POLL_SECONDS = 300

LATEST_PROVIDER_KEYS = {
    "ecowitt": LATEST_ECOWITT_KEY,
    "unifi_protect": LATEST_UNIFI_PROTECT_KEY,
    "ecobee_homekit": LATEST_ECOBEE_HOMEKIT_KEY,
    "hue": LATEST_HUE_KEY,
    "homeassistant": LATEST_HOMEASSISTANT_KEY,
    "weather_api": LATEST_WEATHER_API_KEY,
}

PROVIDER_LABELS = {
    "ecowitt": "Ecowitt",
    "unifi_protect": "UniFi Protect",
    "ecobee_homekit": "Ecobee HomeKit",
    "hue": "Philips Hue",
    "homeassistant": "Home Assistant",
    "weather_api": "WeatherAPI.com",
    "environment": "Environment",
}

ECOWITT_FIELD_META: Dict[str, Tuple[str, str, str]] = {
    "tempf": ("Outdoor Temperature", "temperature", "F"),
    "tempinf": ("Indoor Temperature", "temperature", "F"),
    "dewptf": ("Dew Point", "temperature", "F"),
    "feelslikef": ("Feels Like", "temperature", "F"),
    "windchillf": ("Wind Chill", "temperature", "F"),
    "heatindexf": ("Heat Index", "temperature", "F"),
    "humidity": ("Outdoor Humidity", "humidity", "%"),
    "humidityin": ("Indoor Humidity", "humidity", "%"),
    "baromrelin": ("Relative Pressure", "pressure", "inHg"),
    "baromabsin": ("Absolute Pressure", "pressure", "inHg"),
    "winddir": ("Wind Direction", "wind", "deg"),
    "windspeedmph": ("Wind Speed", "wind", "mph"),
    "windgustmph": ("Wind Gust", "wind", "mph"),
    "maxdailygust": ("Max Daily Gust", "wind", "mph"),
    "rainratein": ("Rain Rate", "rain", "in/hr"),
    "eventrainin": ("Event Rain", "rain", "in"),
    "hourlyrainin": ("Hourly Rain", "rain", "in"),
    "dailyrainin": ("Daily Rain", "rain", "in"),
    "weeklyrainin": ("Weekly Rain", "rain", "in"),
    "monthlyrainin": ("Monthly Rain", "rain", "in"),
    "yearlyrainin": ("Yearly Rain", "rain", "in"),
    "totalrainin": ("Total Rain", "rain", "in"),
    "solarradiation": ("Solar Radiation", "solar", "W/m2"),
    "uv": ("UV Index", "solar", ""),
    "lightning": ("Lightning Distance", "lightning", "mi"),
    "lightning_num": ("Lightning Strikes", "lightning", ""),
    "lightning_time": ("Last Lightning", "lightning", ""),
    "co2": ("CO2", "air", "ppm"),
    "pm25_ch1": ("PM2.5 Channel 1", "air", "ug/m3"),
    "pm25_avg_24h_ch1": ("PM2.5 24h Channel 1", "air", "ug/m3"),
}

CATEGORY_LABELS = {
    "temperature": "Temperature",
    "humidity": "Humidity",
    "pressure": "Pressure",
    "wind": "Wind",
    "rain": "Rain",
    "solar": "Solar & UV",
    "air": "Air Quality",
    "condition": "Conditions",
    "forecast": "Forecast",
    "lightning": "Lightning",
    "soil": "Soil",
    "leak": "Leak Sensors",
    "battery": "Batteries",
    "system": "Station",
    "other": "Other",
}

HYDRA_WEATHER_CATEGORIES = {
    "air",
    "condition",
    "forecast",
    "humidity",
    "lightning",
    "pressure",
    "rain",
    "solar",
    "temperature",
    "wind",
}


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "ignore").strip()
    return str(value or "").strip()


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = _text(value).lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _as_int(value: Any, default: int, *, minimum: int = 0, maximum: int = 100000) -> int:
    try:
        parsed = int(float(_text(value)))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _as_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(_text(value))
    except Exception:
        return None


def _clean_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", _text(value).lower()).strip("_")


def _load_settings(client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    try:
        raw = store.hgetall(SETTINGS_KEY) or {}
    except Exception:
        raw = {}
    return {
        "ecowitt_passkey": _text(raw.get("ECOWITT_PASSKEY")),
        "ecowitt_outdoor_area": _text(raw.get("ENVIRONMENT_ECOWITT_OUTDOOR_AREA")) or "Outside",
        "ecowitt_indoor_area": _text(raw.get("ENVIRONMENT_ECOWITT_INDOOR_AREA")) or "Inside",
        "integration_poll_seconds": _as_int(
            raw.get("ENVIRONMENT_INTEGRATION_POLL_SECONDS"),
            DEFAULT_INTEGRATION_POLL_SECONDS,
            minimum=30,
            maximum=86400,
        ),
        "weather_api_enabled": _as_bool(raw.get("ENVIRONMENT_ENABLE_WEATHERAPI"), False),
        "current_live_source": _text(raw.get("ENVIRONMENT_CURRENT_CONDITION_LIVE_SOURCE")) or "provider:ecowitt",
        "current_condition_source": _text(raw.get("ENVIRONMENT_CURRENT_CONDITION_CONDITION_SOURCE")) or "provider:weather_api",
        "forecast_provider": _clean_key(raw.get("ENVIRONMENT_FORECAST_PROVIDER")) or "weather_api",
        "history_limit": _as_int(raw.get("ENVIRONMENT_HISTORY_LIMIT"), DEFAULT_HISTORY_LIMIT, minimum=1, maximum=10000),
        "stale_after_minutes": _as_int(
            raw.get("ENVIRONMENT_STALE_AFTER_MINUTES"),
            DEFAULT_STALE_AFTER_MINUTES,
            minimum=1,
            maximum=10080,
        ),
    }


def _settings_field_rows(
    settings: Dict[str, Any],
    *,
    provider_snapshots: Optional[Dict[str, Dict[str, Any]]] = None,
    selected_sensors: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    provider_snapshots = provider_snapshots if isinstance(provider_snapshots, dict) else {}
    selected_sensors = selected_sensors if isinstance(selected_sensors, list) else []
    return [
        {
            "key": "ENVIRONMENT_ECOWITT_OUTDOOR_AREA",
            "label": "Ecowitt Outdoor Area",
            "type": "text",
            "value": _text(settings.get("ecowitt_outdoor_area")) or "Outside",
        },
        {
            "key": "ENVIRONMENT_ECOWITT_INDOOR_AREA",
            "label": "Ecowitt Indoor Area",
            "type": "text",
            "value": _text(settings.get("ecowitt_indoor_area")) or "Inside",
        },
        {
            "key": "ENVIRONMENT_INTEGRATION_POLL_SECONDS",
            "label": "Poll Seconds",
            "type": "number",
            "value": int(settings.get("integration_poll_seconds") or DEFAULT_INTEGRATION_POLL_SECONDS),
            "min": 30,
            "max": 86400,
            "description": "How often selected integration sensors are polled by the running core.",
        },
        {
            "key": "ENVIRONMENT_ENABLE_WEATHERAPI",
            "label": "WeatherAPI Forecast",
            "type": "checkbox",
            "value": bool(settings.get("weather_api_enabled")),
            "description": "Poll WeatherAPI.com through Settings > Integrations and show forecast data in Environment Core.",
        },
        {
            "key": "ENVIRONMENT_CURRENT_CONDITION_LIVE_SOURCE",
            "label": "Current Card Readings Source",
            "type": "select",
            "value": _text(settings.get("current_live_source")) or "provider:ecowitt",
            "options": _environment_display_source_options(provider_snapshots, selected_sensors, include_condition_only=False),
            "description": "Choose the measured values for the card: temperature, humidity, wind, and feels-like.",
        },
        {
            "key": "ENVIRONMENT_CURRENT_CONDITION_CONDITION_SOURCE",
            "label": "Current Card Artwork Source",
            "type": "select",
            "value": _text(settings.get("current_condition_source")) or "provider:weather_api",
            "options": _environment_display_source_options(provider_snapshots, selected_sensors, include_condition_only=True),
            "description": "Choose the source for condition text and the sunny/rainy/cloudy card artwork.",
        },
        {
            "key": "ENVIRONMENT_FORECAST_PROVIDER",
            "label": "Forecast Provider",
            "type": "select",
            "value": _text(settings.get("forecast_provider")) or "weather_api",
            "options": _forecast_provider_options(provider_snapshots),
            "description": "WeatherAPI.com is the only forecast provider right now; this selector is ready for additional providers.",
        },
        {
            "key": "ENVIRONMENT_STALE_AFTER_MINUTES",
            "label": "Stale After Minutes",
            "type": "number",
            "value": int(settings.get("stale_after_minutes") or DEFAULT_STALE_AFTER_MINUTES),
            "min": 1,
            "max": 10080,
        },
        {
            "key": "ENVIRONMENT_HISTORY_LIMIT",
            "label": "History Samples",
            "type": "number",
            "value": int(settings.get("history_limit") or DEFAULT_HISTORY_LIMIT),
            "min": 1,
            "max": 10000,
        },
    ]


def _parse_dateutc(value: Any) -> Optional[float]:
    text = _text(value).replace("+", " ")
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            continue
    return None


def _format_ts(ts: Any) -> str:
    value = _as_float(ts)
    if not value:
        return "n/a"
    return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")


def _age_label(ts: Any) -> str:
    value = _as_float(ts)
    if not value:
        return "never"
    delta = max(0.0, time.time() - value)
    if delta < 90:
        return f"{int(delta)}s ago"
    minutes = delta / 60.0
    if minutes < 90:
        return f"{int(minutes)}m ago"
    hours = minutes / 60.0
    if hours < 48:
        return f"{round(hours, 1)}h ago"
    return f"{int(hours / 24)}d ago"


def _passkey_hash(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _source_id_from_payload(payload: Dict[str, Any]) -> str:
    pass_hash = _passkey_hash(payload.get("PASSKEY") or payload.get("passkey"))
    if pass_hash:
        return f"ecowitt:{pass_hash[:12]}"
    model = _clean_key(payload.get("model") or payload.get("stationtype"))
    return f"ecowitt:{model or 'unknown'}"


def _value_label(value: Any, unit: str = "") -> str:
    number = _as_float(value)
    if number is not None:
        rounded = round(number, 2)
        if float(rounded).is_integer():
            text = str(int(rounded))
        else:
            text = str(rounded).rstrip("0").rstrip(".")
    else:
        text = _text(value)
    return f"{text} {unit}".strip()


def _humanize_key(key: str) -> str:
    text = key.replace("_", " ")
    text = re.sub(r"([a-z]+)(\d+)$", r"\1 \2", text)
    return " ".join(part.upper() if part in {"uv", "pm25", "co2"} else part.capitalize() for part in text.split())


def _ecowitt_dynamic_meta(key: str) -> Tuple[str, str, str]:
    token = _clean_key(key)
    if token in ECOWITT_FIELD_META:
        return ECOWITT_FIELD_META[token]
    if re.fullmatch(r"temp\d+f", token):
        return (f"Temperature {token[4:-1]}", "temperature", "F")
    if re.fullmatch(r"humidity\d+", token):
        return (f"Humidity {token[8:]}", "humidity", "%")
    if re.fullmatch(r"soiltemp\d+f", token):
        return (f"Soil Temperature {token[8:-1]}", "soil", "F")
    if re.fullmatch(r"soilmoisture\d+", token):
        return (f"Soil Moisture {token[12:]}", "soil", "%")
    if token.startswith("pm25_"):
        return (_humanize_key(token), "air", "ug/m3")
    if token.startswith("leak"):
        return (_humanize_key(token), "leak", "")
    if token.endswith("batt") or token.endswith("battery") or "batt" in token:
        return (_humanize_key(token), "battery", "")
    if token in {"stationtype", "model", "freq", "dateutc"}:
        return (_humanize_key(token), "system", "")
    return (_humanize_key(token), "other", "")


def _is_low_battery(key: str, value: Any) -> bool:
    number = _as_float(value)
    if number is None:
        return _text(value).lower() in {"low", "1", "true", "bad"}
    token = _clean_key(key)
    if token.endswith("batt") and number in {0.0, 1.0}:
        return number == 1.0
    return number <= 1.2 if number > 0 else False


def _battery_status_label(key: str, value: Any) -> str:
    token = _clean_key(key)
    number = _as_float(value)
    if token.endswith("batt") and number in {0.0, 1.0}:
        return "Low" if number == 1.0 else "OK"
    return _value_label(value)


def _normalize_ecowitt_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}
    for key, value in (payload or {}).items():
        raw_key = _text(key)
        if not raw_key:
            continue
        clean[raw_key] = _text(value)

    now_ts = time.time()
    sample_ts = _parse_dateutc(clean.get("dateutc")) or now_ts
    passkey = clean.get("PASSKEY") or clean.get("passkey")
    pass_hash = _passkey_hash(passkey)
    source_id = _source_id_from_payload(clean)

    readings: List[Dict[str, Any]] = []
    raw_safe: Dict[str, Any] = {}
    for key, value in clean.items():
        key_l = _clean_key(key)
        if key_l == "passkey":
            raw_safe["PASSKEY"] = f"sha256:{pass_hash[:12]}" if pass_hash else ""
            continue
        label, category, unit = _ecowitt_dynamic_meta(key_l)
        display_value = _battery_status_label(key_l, value) if category == "battery" else _value_label(value, unit)
        row = {
            "key": key_l,
            "label": label,
            "category": category,
            "unit": unit,
            "value": _as_float(value) if _as_float(value) is not None else _text(value),
            "display": display_value,
        }
        if category == "battery":
            row["tone"] = "danger" if _is_low_battery(key_l, value) else "good"
        readings.append(row)
        raw_safe[key] = value

    readings.sort(key=lambda row: (str(row.get("category") or ""), str(row.get("label") or "")))
    return {
        "provider": "ecowitt",
        "source_id": source_id,
        "stationtype": _text(clean.get("stationtype")),
        "model": _text(clean.get("model")),
        "frequency": _text(clean.get("freq")),
        "received_at": now_ts,
        "sample_time": sample_ts,
        "sample_time_text": _format_ts(sample_ts),
        "passkey_hash": pass_hash,
        "readings": readings,
        "raw": raw_safe,
    }


def _ecowitt_area_for_key(key: Any, settings: Dict[str, Any]) -> str:
    token = _clean_key(key)
    indoor_area = _text(settings.get("ecowitt_indoor_area")) or "Inside"
    outdoor_area = _text(settings.get("ecowitt_outdoor_area")) or "Outside"
    if token in {"tempinf", "humidityin", "baromabsin"} or token.endswith("in"):
        return indoor_area
    if token in {"stationtype", "model", "freq", "dateutc"} or "batt" in token:
        return ""
    return outdoor_area


def _apply_ecowitt_areas(snapshot: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {}
    for row in snapshot.get("readings") or []:
        if not isinstance(row, dict):
            continue
        area = _ecowitt_area_for_key(row.get("key"), settings)
        if area:
            row["area"] = area
    return snapshot


def _provider_label(provider: Any) -> str:
    token = _clean_key(provider)
    return PROVIDER_LABELS.get(token, _humanize_key(token) if token else "Environment")


def _reading_row(
    *,
    key: Any,
    label: str,
    category: str,
    unit: str,
    value: Any,
    provider: str,
    source_id: str,
    source_name: str,
    area: str = "",
    display: Optional[str] = None,
    tone: str = "",
) -> Dict[str, Any]:
    number = _as_float(value)
    row: Dict[str, Any] = {
        "key": _clean_key(key),
        "label": label,
        "category": _clean_key(category) or "other",
        "unit": unit,
        "value": number if number is not None else _text(value),
        "display": display or _value_label(number if number is not None else value, unit),
        "provider": _clean_key(provider),
        "provider_label": _provider_label(provider),
        "source_id": source_id,
        "source_name": source_name,
    }
    if _text(area):
        row["area"] = _text(area)
    if tone:
        row["tone"] = tone
    return row


def _snapshot_from_readings(
    *,
    provider: str,
    source_id: str,
    readings: List[Dict[str, Any]],
    raw: Optional[Dict[str, Any]] = None,
    model: str = "",
    stationtype: str = "",
    frequency: str = "",
) -> Dict[str, Any]:
    now_ts = time.time()
    clean_readings = [row for row in readings or [] if isinstance(row, dict) and _text(row.get("key"))]
    clean_readings.sort(
        key=lambda row: (
            str(row.get("category") or ""),
            str(row.get("source_name") or ""),
            str(row.get("label") or ""),
        )
    )
    return {
        "provider": _clean_key(provider),
        "source_id": source_id,
        "stationtype": stationtype,
        "model": model,
        "frequency": frequency,
        "received_at": now_ts,
        "sample_time": now_ts,
        "sample_time_text": _format_ts(now_ts),
        "readings": clean_readings,
        "raw": raw if isinstance(raw, dict) else {},
    }


def _flatten_scalars(value: Any, prefix: str = "", depth: int = 0) -> Iterable[Tuple[str, str, Any]]:
    if depth > 5:
        return
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = _text(key)
            if not key_text:
                continue
            path = f"{prefix}.{key_text}" if prefix else key_text
            if isinstance(item, (dict, list, tuple)):
                yield from _flatten_scalars(item, path, depth + 1)
            else:
                yield path, key_text, item
        return
    if isinstance(value, (list, tuple)) and len(value) <= 8:
        for index, item in enumerate(value):
            path = f"{prefix}.{index}" if prefix else str(index)
            if isinstance(item, (dict, list, tuple)):
                yield from _flatten_scalars(item, path, depth + 1)
            else:
                yield path, str(index), item


def _path_token(path: Any) -> str:
    return _clean_key(re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", _text(path).replace(".", "_")))


def _first_numeric_field(
    flat: Iterable[Tuple[str, str, Any]],
    include_tokens: Iterable[str],
    *,
    exclude_tokens: Iterable[str] = (),
) -> Tuple[Optional[float], str]:
    include = {_clean_key(token) for token in include_tokens if _text(token)}
    exclude = {_clean_key(token) for token in exclude_tokens if _text(token)}
    for path, leaf, value in flat:
        path_clean = _path_token(path)
        leaf_clean = _path_token(leaf)
        if any(token and token in path_clean for token in exclude):
            continue
        if leaf_clean not in include and not any(token and token in path_clean for token in include):
            continue
        number = _as_float(value)
        if number is not None:
            return number, path
    return None, ""


def _first_text_field(
    flat: Iterable[Tuple[str, str, Any]],
    include_tokens: Iterable[str],
    *,
    exclude_tokens: Iterable[str] = (),
) -> Tuple[str, str]:
    include = {_clean_key(token) for token in include_tokens if _text(token)}
    exclude = {_clean_key(token) for token in exclude_tokens if _text(token)}
    for path, leaf, value in flat:
        path_clean = _path_token(path)
        leaf_clean = _path_token(leaf)
        if any(token and token in path_clean for token in exclude):
            continue
        if leaf_clean not in include and not any(token and token in path_clean for token in include):
            continue
        text = _text(value)
        if text:
            return text, path
    return "", ""


def _unit_hint(flat: Iterable[Tuple[str, str, Any]], measurement: str, default: str) -> str:
    wanted = _clean_key(measurement)
    for path, leaf, value in flat:
        path_clean = _path_token(path)
        leaf_clean = _path_token(leaf)
        if "unit" not in leaf_clean and not path_clean.endswith("_unit"):
            continue
        if wanted and wanted not in path_clean:
            continue
        text = _text(value).lower()
        if text in {"f", "fahrenheit"}:
            return "F"
        if text in {"c", "celsius", "centigrade"}:
            return "C"
        if text in {"%", "percent", "percentage"}:
            return "%"
    return default


def _source_name(row: Dict[str, Any], fallback: str) -> str:
    for key in ("name", "displayName", "display_name", "friendlyName", "friendly_name", "marketName", "market_name"):
        value = _text(row.get(key))
        if value:
            return value
    return fallback


def _load_json_value(key: str, default: Any, client: Any = None) -> Any:
    store = client or redis_client
    try:
        raw = store.get(key)
    except Exception:
        raw = None
    if not raw:
        return default
    try:
        return json.loads(_text(raw))
    except Exception:
        return default


def _save_json_value(key: str, value: Any, client: Any = None) -> None:
    (client or redis_client).set(key, json.dumps(value, sort_keys=True))


def _sensor_selection_key(provider: Any, sensor_id: Any, measurement: Any = "") -> str:
    provider_key = _clean_key(provider)
    sensor_text = _text(sensor_id)
    measurement_text = _clean_key(measurement)
    return f"{provider_key}:{sensor_text}:{measurement_text}" if measurement_text else f"{provider_key}:{sensor_text}"


def _normalize_selection(row: Dict[str, Any]) -> Dict[str, Any]:
    provider = _clean_key(row.get("provider"))
    sensor_id = _text(row.get("sensor_id") or row.get("id"))
    measurement = _clean_key(row.get("measurement"))
    key = _text(row.get("key")) or _sensor_selection_key(provider, sensor_id, measurement)
    return {
        "key": key,
        "provider": provider,
        "sensor_id": sensor_id,
        "measurement": measurement,
        "label": _text(row.get("label")) or _text(row.get("name")) or sensor_id or key,
        "area": _text(row.get("area")),
        "category": _clean_key(row.get("category")),
        "unit": _text(row.get("unit")),
        "value_path": _text(row.get("value_path")),
        "enabled": _as_bool(row.get("enabled"), True),
    }


def _load_selected_sensors(client: Any = None) -> List[Dict[str, Any]]:
    raw = _load_json_value(SELECTED_SENSORS_KEY, [], client)
    rows = raw if isinstance(raw, list) else []
    selected: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in rows:
        if not isinstance(item, dict):
            continue
        row = _normalize_selection(item)
        key = _text(row.get("key"))
        provider = _text(row.get("provider"))
        if not key or not provider or key in seen:
            continue
        selected.append(row)
        seen.add(key)
    selected.sort(key=lambda item: (_provider_label(item.get("provider")).casefold(), _text(item.get("area")).casefold(), _text(item.get("label")).casefold()))
    return selected


def _save_selected_sensors(rows: List[Dict[str, Any]], client: Any = None) -> None:
    normalized = [_normalize_selection(row) for row in rows or [] if isinstance(row, dict)]
    _save_json_value(SELECTED_SENSORS_KEY, normalized, client)


def _selected_by_provider(provider: str, client: Any = None) -> Dict[str, Dict[str, Any]]:
    wanted = _clean_key(provider)
    return {
        _text(row.get("key")): row
        for row in _load_selected_sensors(client)
        if _clean_key(row.get("provider")) == wanted and _as_bool(row.get("enabled"), True)
    }


def _load_candidate_cache(client: Any = None) -> Dict[str, Any]:
    raw = _load_json_value(CANDIDATE_SENSORS_KEY, {}, client)
    if not isinstance(raw, dict):
        return {"updated_at": 0, "items": []}
    items = raw.get("items") if isinstance(raw.get("items"), list) else []
    return {
        "updated_at": _as_float(raw.get("updated_at")) or 0,
        "items": [dict(item) for item in items if isinstance(item, dict)],
    }


def _save_candidate_cache(items: List[Dict[str, Any]], client: Any = None) -> Dict[str, Any]:
    payload = {
        "updated_at": time.time(),
        "items": [dict(item) for item in items or [] if isinstance(item, dict) and _text(item.get("key"))],
    }
    _save_json_value(CANDIDATE_SENSORS_KEY, payload, client)
    return payload


def _candidate_row(
    *,
    provider: str,
    sensor_id: Any,
    label: str,
    category: str,
    unit: str = "",
    measurement: str = "",
    value_path: str = "",
    area: str = "",
    current_display: str = "",
    capabilities: Optional[List[str]] = None,
) -> Dict[str, Any]:
    measurement_key = _clean_key(measurement or category)
    return {
        "key": _sensor_selection_key(provider, sensor_id, measurement_key),
        "provider": _clean_key(provider),
        "provider_label": _provider_label(provider),
        "sensor_id": _text(sensor_id),
        "label": label,
        "area": _text(area),
        "category": _clean_key(category),
        "unit": unit,
        "measurement": measurement_key,
        "value_path": value_path,
        "current": current_display,
        "capabilities": capabilities or [CATEGORY_LABELS.get(_clean_key(category), _clean_key(category).title())],
    }


def _candidate_options(candidates: List[Dict[str, Any]], selected: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    selected_keys = {_text(row.get("key")) for row in selected or []}
    options: List[Dict[str, str]] = []
    for row in candidates or []:
        key = _text(row.get("key"))
        if not key or key in selected_keys:
            continue
        pieces = [_provider_label(row.get("provider")), _text(row.get("label"))]
        category = CATEGORY_LABELS.get(_clean_key(row.get("category")), _clean_key(row.get("category")).title())
        if category:
            pieces.append(category)
        current = _text(row.get("current"))
        if current:
            pieces.append(current)
        options.append({"value": key, "label": " - ".join(part for part in pieces if part)})
    options.sort(key=lambda item: _text(item.get("label")).casefold())
    return options


def _candidate_table_rows(candidates: List[Dict[str, Any]], selected: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    selected_keys = {_text(row.get("key")) for row in selected or []}
    rows: List[Dict[str, str]] = []
    for row in candidates or []:
        key = _text(row.get("key"))
        rows.append(
            {
                "source": _provider_label(row.get("provider")),
                "sensor": _text(row.get("label")) or _text(row.get("sensor_id")),
                "measurement": CATEGORY_LABELS.get(_clean_key(row.get("category")), _clean_key(row.get("category")).title()),
                "current": _text(row.get("current")) or "-",
                "area": _text(row.get("area")) or "-",
                "selected": "Yes" if key in selected_keys else "No",
            }
        )
    return rows


def _apply_selection_to_reading(row: Dict[str, Any], selection: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    label = _text(selection.get("label"))
    area = _text(selection.get("area"))
    category = _clean_key(out.get("category"))
    if label:
        suffix = {
            "temperature": "Temperature",
            "humidity": "Humidity",
            "battery": "Battery",
            "pressure": "Pressure",
            "solar": "Light",
        }.get(category, CATEGORY_LABELS.get(category, _humanize_key(category)))
        out["source_name"] = label
        out["label"] = f"{label} {suffix}".strip()
    if area:
        out["area"] = area
    return out


def _normalize_unifi_protect_sensors(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    readings: List[Dict[str, Any]] = []
    for index, row in enumerate(rows or []):
        if not isinstance(row, dict):
            continue
        sensor_id = _text(row.get("id") or row.get("_id") or row.get("mac") or row.get("uuid") or f"sensor_{index + 1}")
        source_name = _source_name(row, sensor_id or f"Sensor {index + 1}")
        source_id = f"unifi_protect:{_clean_key(sensor_id or source_name)}"
        key_base = f"unifi_protect_{_clean_key(sensor_id or source_name)}"
        flat = list(_flatten_scalars(row))

        temp_value, temp_path = _first_numeric_field(
            flat,
            ("temperature", "temperature_c", "temperaturec", "temperature_f", "temperaturef", "temp"),
            exclude_tokens=("target", "desired", "threshold", "limit", "minimum", "maximum", "min", "max"),
        )
        if temp_value is not None:
            temp_path_clean = _path_token(temp_path)
            temp_unit = "F" if temp_path_clean.endswith("_f") or "fahrenheit" in temp_path_clean else _unit_hint(flat, "temperature", "C")
            readings.append(
                _reading_row(
                    key=f"{key_base}_temperature",
                    label=f"{source_name} Temperature",
                    category="temperature",
                    unit=temp_unit,
                    value=temp_value,
                    provider="unifi_protect",
                    source_id=source_id,
                    source_name=source_name,
                )
            )

        humidity_value, _humidity_path = _first_numeric_field(
            flat,
            ("humidity", "relative_humidity", "humidity_percent", "humiditypercentage"),
            exclude_tokens=("target", "desired", "threshold", "limit"),
        )
        if humidity_value is not None:
            readings.append(
                _reading_row(
                    key=f"{key_base}_humidity",
                    label=f"{source_name} Humidity",
                    category="humidity",
                    unit="%",
                    value=humidity_value,
                    provider="unifi_protect",
                    source_id=source_id,
                    source_name=source_name,
                )
            )

        battery_value, battery_path = _first_numeric_field(
            flat,
            ("battery_percentage", "batterypercentage", "battery_percent", "batterylevel", "battery_level", "battery"),
        )
        if battery_value is not None:
            unit = "%" if "percent" in _path_token(battery_path) or battery_value > 1.2 else ""
            readings.append(
                _reading_row(
                    key=f"{key_base}_battery",
                    label=f"{source_name} Battery",
                    category="battery",
                    unit=unit,
                    value=battery_value,
                    provider="unifi_protect",
                    source_id=source_id,
                    source_name=source_name,
                    tone="danger" if (unit == "%" and battery_value <= 20) or (unit != "%" and battery_value <= 1.2) else "good",
                )
            )
            continue

        battery_text, _battery_text_path = _first_text_field(flat, ("battery_status", "batterystatus", "battery"))
        if battery_text:
            low = battery_text.lower() in {"low", "critical", "bad", "replace", "replace_soon"}
            readings.append(
                _reading_row(
                    key=f"{key_base}_battery",
                    label=f"{source_name} Battery",
                    category="battery",
                    unit="",
                    value=battery_text,
                    display=battery_text,
                    provider="unifi_protect",
                    source_id=source_id,
                    source_name=source_name,
                    tone="danger" if low else "good",
                )
            )
    return readings


def _normalize_ecobee_homekit_thermostats(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    readings: List[Dict[str, Any]] = []
    for index, row in enumerate(rows or []):
        if not isinstance(row, dict):
            continue
        thermostat_id = _text(row.get("id") or f"thermostat_{index + 1}")
        source_name = _text(row.get("name")) or thermostat_id
        source_id = f"ecobee_homekit:{_clean_key(thermostat_id)}"
        key_base = f"ecobee_homekit_{_clean_key(thermostat_id)}"
        unit = _text(row.get("temperature_unit")).upper() or "F"
        if unit not in {"C", "F"}:
            unit = "F"
        temp_value = row.get("current_temperature_f") if unit == "F" else row.get("current_temperature_c")
        if _as_float(temp_value) is None:
            temp_value = row.get("current_temperature_c") if unit == "F" else row.get("current_temperature_f")
            unit = "C" if unit == "F" else "F"
        if _as_float(temp_value) is not None:
            readings.append(
                _reading_row(
                    key=f"{key_base}_current_temperature",
                    label=f"{source_name} Temperature",
                    category="temperature",
                    unit=unit,
                    value=temp_value,
                    provider="ecobee_homekit",
                    source_id=source_id,
                    source_name=source_name,
                )
            )

        humidity = row.get("current_humidity")
        if _as_float(humidity) is not None:
            readings.append(
                _reading_row(
                    key=f"{key_base}_current_humidity",
                    label=f"{source_name} Humidity",
                    category="humidity",
                    unit="%",
                    value=humidity,
                    provider="ecobee_homekit",
                    source_id=source_id,
                    source_name=source_name,
                )
            )
    return readings


def _store_snapshot(snapshot: Dict[str, Any], client: Any = None, provider_key: Optional[str] = None) -> None:
    store = client or redis_client
    settings = _load_settings(store)
    blob = json.dumps(snapshot, sort_keys=True)
    provider = _clean_key(provider_key or snapshot.get("provider") or "environment")
    store.set(LATEST_KEY, blob)
    latest_key = LATEST_PROVIDER_KEYS.get(provider)
    if latest_key:
        store.set(latest_key, blob)
    store.hset(SOURCES_KEY, snapshot.get("source_id") or f"{provider}:unknown", blob)
    store.lpush(HISTORY_KEY, blob)
    store.ltrim(HISTORY_KEY, 0, max(0, int(settings.get("history_limit") or DEFAULT_HISTORY_LIMIT) - 1))


def _unifi_protect_configured(client: Any = None) -> Tuple[bool, str]:
    try:
        from integrations.unifi_protect import unifi_protect_configured
    except Exception as exc:
        return False, f"UniFi Protect integration unavailable: {exc}"
    try:
        configured = bool(unifi_protect_configured(client))
    except Exception as exc:
        return False, str(exc)
    return configured, "Configured" if configured else "Set up UniFi Protect in Settings > Integrations."


def _ecobee_homekit_configured() -> Tuple[bool, str]:
    try:
        from integrations.homekit import integration_status
    except Exception as exc:
        return False, f"Ecobee HomeKit integration unavailable: {exc}"
    try:
        status = integration_status()
    except Exception as exc:
        return False, str(exc)
    configured = bool(status.get("configured"))
    return configured, _text(status.get("message")) or ("Configured" if configured else "Pair Ecobee HomeKit in Settings > Integrations.")


def _hue_configured(client: Any = None) -> Tuple[bool, str]:
    try:
        from integrations.hue import read_hue_settings
    except Exception as exc:
        return False, f"Philips Hue integration unavailable: {exc}"
    try:
        settings = read_hue_settings(client)
    except Exception as exc:
        return False, str(exc)
    configured = bool(_text(settings.get("HUE_APP_KEY")))
    return configured, "Configured" if configured else "Link Philips Hue in Settings > Integrations."


def _homeassistant_configured(client: Any = None) -> Tuple[bool, str]:
    try:
        from integrations.homeassistant import load_homeassistant_config
    except Exception as exc:
        return False, f"Home Assistant integration unavailable: {exc}"
    try:
        config = load_homeassistant_config(required=False, client=client)
    except Exception as exc:
        return False, str(exc)
    configured = bool(_text(config.get("base")) and _text(config.get("token")))
    return configured, "Configured" if configured else "Set up Home Assistant in Settings > Integrations."


def _weather_api_configured(client: Any = None) -> Tuple[bool, str]:
    try:
        from integrations.weather_api import weatherapi_configured
    except Exception as exc:
        return False, f"WeatherAPI.com integration unavailable: {exc}"
    try:
        configured = bool(weatherapi_configured(client))
    except Exception as exc:
        return False, str(exc)
    return configured, "Configured" if configured else "Set up WeatherAPI.com in Settings > Integrations."


def _hue_api_root(bridge_root: Any) -> str:
    from urllib.parse import urlparse, urlunparse

    text = _text(bridge_root)
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"http://{text}")
    host = parsed.hostname or parsed.netloc or parsed.path
    try:
        port = int(parsed.port or 0)
    except Exception:
        port = 0
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    netloc = host
    if port and port not in {80, 443}:
        netloc = f"{host}:{port}"
    return urlunparse(("https", netloc, "", "", "", "")).rstrip("/")


def _hue_get_resource(resource: str, client: Any = None) -> List[Dict[str, Any]]:
    try:
        import requests
        import warnings
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        from integrations.hue import read_hue_settings
    except Exception as exc:
        raise RuntimeError(f"Philips Hue integration unavailable: {exc}") from exc
    settings = read_hue_settings(client)
    bridge = _hue_api_root(settings.get("HUE_BRIDGE_HOST"))
    app_key = _text(settings.get("HUE_APP_KEY"))
    timeout = _as_int(settings.get("HUE_TIMEOUT_SECONDS"), 10, minimum=2, maximum=60)
    if not bridge or not app_key:
        raise ValueError("Philips Hue is not linked.")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", InsecureRequestWarning)
        response = requests.get(
            f"{bridge}/clip/v2/resource/{_clean_key(resource)}",
            headers={"hue-application-key": app_key, "Accept": "application/json"},
            timeout=timeout,
            verify=False,
        )
    if response.status_code == 404:
        return []
    if response.status_code >= 400:
        raise RuntimeError(f"Philips Hue HTTP {response.status_code}: {response.text[:200]}")
    try:
        parsed = response.json()
    except Exception as exc:
        raise RuntimeError(f"Philips Hue returned invalid JSON: {exc}") from exc
    data = parsed.get("data") if isinstance(parsed, dict) else []
    return data if isinstance(data, list) else []


def _hue_device_names(client: Any = None) -> Dict[str, str]:
    names: Dict[str, str] = {}
    for row in _hue_get_resource("device", client):
        if not isinstance(row, dict):
            continue
        rid = _text(row.get("id"))
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        name = _text(metadata.get("name")) or rid
        if rid:
            names[rid] = name
    return names


def _hue_temperature_value(row: Dict[str, Any]) -> Optional[float]:
    temp = row.get("temperature") if isinstance(row.get("temperature"), dict) else {}
    value = _as_float(temp.get("temperature"))
    if value is None:
        value = _as_float(row.get("temperature"))
    if value is None:
        return None
    return round(value / 100.0, 2) if abs(value) > 150 else round(value, 2)


def _hue_humidity_value(row: Dict[str, Any]) -> Optional[float]:
    humidity = row.get("humidity") if isinstance(row.get("humidity"), dict) else {}
    value = _as_float(humidity.get("humidity") or humidity.get("relative_humidity") or row.get("humidity"))
    if value is None:
        return None
    return round(value / 100.0, 2) if abs(value) > 100 else round(value, 2)


def _hue_sensor_label(row: Dict[str, Any], device_names: Dict[str, str]) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    name = _text(metadata.get("name"))
    owner = row.get("owner") if isinstance(row.get("owner"), dict) else {}
    owner_name = device_names.get(_text(owner.get("rid")), "")
    return name or owner_name or _text(row.get("id")) or "Hue Sensor"


def _discover_hue_candidates(client: Any = None) -> List[Dict[str, Any]]:
    configured, _message = _hue_configured(client)
    if not configured:
        return []
    device_names = _hue_device_names(client)
    rows: List[Dict[str, Any]] = []
    for item in _hue_get_resource("temperature", client):
        if not isinstance(item, dict):
            continue
        value = _hue_temperature_value(item)
        if value is None:
            continue
        rid = _text(item.get("id"))
        label = _hue_sensor_label(item, device_names)
        rows.append(
            _candidate_row(
                provider="hue",
                sensor_id=rid,
                label=label,
                category="temperature",
                unit="C",
                measurement="temperature",
                value_path="temperature.temperature",
                current_display=_value_label(value, "C"),
            )
        )
    for item in _hue_get_resource("relative_humidity", client):
        if not isinstance(item, dict):
            continue
        value = _hue_humidity_value(item)
        if value is None:
            continue
        rid = _text(item.get("id"))
        label = _hue_sensor_label(item, device_names)
        rows.append(
            _candidate_row(
                provider="hue",
                sensor_id=rid,
                label=label,
                category="humidity",
                unit="%",
                measurement="humidity",
                value_path="humidity.humidity",
                current_display=_value_label(value, "%"),
            )
        )
    return rows


def _homeassistant_states(client: Any = None) -> List[Dict[str, Any]]:
    try:
        import requests
        from integrations.homeassistant import load_homeassistant_config
    except Exception as exc:
        raise RuntimeError(f"Home Assistant integration unavailable: {exc}") from exc
    config = load_homeassistant_config(required=True, client=client)
    base = _text(config.get("base")).rstrip("/")
    token = _text(config.get("token"))
    response = requests.get(
        f"{base}/api/states",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Home Assistant HTTP {response.status_code}: {response.text[:200]}")
    try:
        parsed = response.json()
    except Exception as exc:
        raise RuntimeError(f"Home Assistant returned invalid JSON: {exc}") from exc
    return parsed if isinstance(parsed, list) else []


def _ha_state_category(entity_id: str, attrs: Dict[str, Any], value_path: str = "state") -> Tuple[str, str]:
    device_class = _clean_key(attrs.get("device_class"))
    unit = _text(attrs.get("unit_of_measurement"))
    path = _clean_key(value_path)
    haystack = f"{device_class} {unit} {_clean_key(entity_id)} {path}"
    if device_class in {"temperature"} or "temperature" in haystack or unit in {"\u00b0F", "\u00b0C", "F", "C"}:
        return "temperature", unit or "F"
    if device_class in {"humidity"} or "humidity" in haystack or unit == "%":
        return "humidity", unit or "%"
    if device_class in {"pressure", "atmospheric_pressure"} or "pressure" in haystack:
        return "pressure", unit
    if device_class in {"battery"} or "battery" in haystack:
        return "battery", unit or "%"
    if device_class in {"illuminance"} or "illuminance" in haystack or unit.lower() in {"lx", "lux"}:
        return "solar", unit or "lx"
    return "", unit


def _ha_state_name(entity_id: str, attrs: Dict[str, Any], value_path: str = "") -> str:
    friendly = _text(attrs.get("friendly_name"))
    if friendly:
        if value_path and value_path != "state":
            suffix = _humanize_key(value_path)
            return f"{friendly} {suffix}"
        return friendly
    return entity_id


def _discover_homeassistant_candidates(client: Any = None) -> List[Dict[str, Any]]:
    configured, _message = _homeassistant_configured(client)
    if not configured:
        return []
    rows: List[Dict[str, Any]] = []
    for item in _homeassistant_states(client):
        if not isinstance(item, dict):
            continue
        entity_id = _text(item.get("entity_id"))
        if not entity_id:
            continue
        domain = entity_id.split(".", 1)[0]
        attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
        if domain == "sensor":
            value = _as_float(item.get("state"))
            category, unit = _ha_state_category(entity_id, attrs, "state")
            if value is None or not category:
                continue
            rows.append(
                _candidate_row(
                    provider="homeassistant",
                    sensor_id=entity_id,
                    label=_ha_state_name(entity_id, attrs),
                    category=category,
                    unit=unit,
                    measurement=category,
                    value_path="state",
                    current_display=_value_label(value, unit),
                )
            )
            continue
        if domain == "climate":
            for attr_key in ("current_temperature", "current_humidity"):
                value = _as_float(attrs.get(attr_key))
                if value is None:
                    continue
                category, unit = _ha_state_category(entity_id, attrs, attr_key)
                if attr_key == "current_humidity":
                    category, unit = "humidity", "%"
                if not category:
                    continue
                rows.append(
                    _candidate_row(
                        provider="homeassistant",
                        sensor_id=entity_id,
                        label=_ha_state_name(entity_id, attrs, attr_key),
                        category=category,
                        unit=unit,
                        measurement=attr_key,
                        value_path=f"attributes.{attr_key}",
                        current_display=_value_label(value, unit),
                    )
                )
    rows.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("key"))))
    return rows


def _discover_unifi_candidates(client: Any = None) -> List[Dict[str, Any]]:
    configured, _message = _unifi_protect_configured(client)
    if not configured:
        return []
    try:
        from integrations.unifi_protect import list_unifi_sensors
    except Exception:
        return []
    sensors = list_unifi_sensors()
    readings = _normalize_unifi_protect_sensors(sensors if isinstance(sensors, list) else [])
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in readings:
        source_id = _text(row.get("source_id"))
        if not source_id:
            continue
        entry = grouped.setdefault(
            source_id,
            {
                "provider": "unifi_protect",
                "sensor_id": source_id.split(":", 1)[1] if ":" in source_id else source_id,
                "label": _text(row.get("source_name")) or source_id,
                "capabilities": [],
                "current": [],
            },
        )
        category = CATEGORY_LABELS.get(_clean_key(row.get("category")), _clean_key(row.get("category")).title())
        if category and category not in entry["capabilities"]:
            entry["capabilities"].append(category)
        if _text(row.get("display")):
            entry["current"].append(_text(row.get("display")))
    return [
        _candidate_row(
            provider="unifi_protect",
            sensor_id=item["sensor_id"],
            label=item["label"],
            category="other",
            measurement="sensor",
            current_display=", ".join(item.get("current") or [])[:90],
            capabilities=item.get("capabilities") or ["Sensor"],
        )
        for item in grouped.values()
    ]


def _discover_ecobee_candidates(client: Any = None) -> List[Dict[str, Any]]:
    configured, _message = _ecobee_homekit_configured()
    if not configured:
        return []
    try:
        from integrations.homekit import list_homekit_thermostats
    except Exception:
        return []
    thermostats = list_homekit_thermostats()
    rows: List[Dict[str, Any]] = []
    for row in thermostats if isinstance(thermostats, list) else []:
        if not isinstance(row, dict):
            continue
        thermostat_id = _text(row.get("id"))
        label = _text(row.get("name")) or thermostat_id
        unit = _text(row.get("temperature_unit")).upper() or "F"
        value = row.get("current_temperature_f") if unit == "F" else row.get("current_temperature_c")
        current_parts = []
        if _as_float(value) is not None:
            current_parts.append(_value_label(value, unit))
        if _as_float(row.get("current_humidity")) is not None:
            current_parts.append(_value_label(row.get("current_humidity"), "%"))
        rows.append(
            _candidate_row(
                provider="ecobee_homekit",
                sensor_id=thermostat_id,
                label=label,
                category="temperature",
                unit=unit,
                measurement="thermostat",
                current_display=", ".join(current_parts),
                capabilities=["Temperature", "Humidity"],
            )
        )
    return rows


def _discover_environment_sensor_candidates(client: Any = None) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    errors: List[str] = []
    for provider, discover in (
        ("unifi_protect", _discover_unifi_candidates),
        ("ecobee_homekit", _discover_ecobee_candidates),
        ("hue", _discover_hue_candidates),
        ("homeassistant", _discover_homeassistant_candidates),
    ):
        try:
            results.extend(discover(client))
        except Exception as exc:
            logger.warning("[Environment] %s sensor discovery failed: %s", provider, exc)
            errors.append(f"{_provider_label(provider)}: {exc}")
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in results:
        key = _text(row.get("key"))
        if key:
            deduped[key] = row
    payload = _save_candidate_cache(list(deduped.values()), client)
    payload["errors"] = errors
    payload["message"] = f"Discovered {len(payload.get('items') or [])} environment sensor candidate{'s' if len(payload.get('items') or []) != 1 else ''}."
    return payload


def _selection_for_source(provider: str, source_id: Any, selected: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    provider_key = _clean_key(provider)
    source_text = _text(source_id)
    source_suffix = source_text.split(":", 1)[1] if ":" in source_text else source_text
    source_clean = _clean_key(source_suffix)
    for row in selected.values():
        if _clean_key(row.get("provider")) != provider_key:
            continue
        sensor_clean = _clean_key(row.get("sensor_id"))
        key_clean = _clean_key(row.get("key"))
        if source_clean and source_clean == sensor_clean:
            return row
        if key_clean and source_clean and source_clean in key_clean:
            return row
    return None


def _poll_unifi_protect(client: Any = None) -> Dict[str, Any]:
    configured, message = _unifi_protect_configured(client)
    if not configured:
        return {"ok": False, "provider": "unifi_protect", "message": message}
    try:
        from integrations.unifi_protect import list_unifi_sensors
    except Exception as exc:
        return {"ok": False, "provider": "unifi_protect", "message": f"UniFi Protect integration unavailable: {exc}"}
    try:
        rows = list_unifi_sensors()
    except Exception as exc:
        logger.warning("[Environment] UniFi Protect sensor poll failed: %s", exc)
        return {"ok": False, "provider": "unifi_protect", "message": str(exc)}
    sensors = rows if isinstance(rows, list) else []
    selected = _selected_by_provider("unifi_protect", client)
    if not selected:
        return {"ok": True, "provider": "unifi_protect", "reading_count": 0, "source_count": len(sensors), "message": "No UniFi Protect sensors are selected."}
    readings = [
        _apply_selection_to_reading(row, selection)
        for row in _normalize_unifi_protect_sensors(sensors)
        for selection in [_selection_for_source("unifi_protect", row.get("source_id"), selected)]
        if selection
    ]
    raw = {
        "sensor_count": len(sensors),
        "sensor_names": [_source_name(row, _text(row.get("id")) or "sensor") for row in sensors if isinstance(row, dict)][:50],
    }
    if not readings:
        return {
            "ok": True,
            "provider": "unifi_protect",
            "reading_count": 0,
            "source_count": len(sensors),
            "message": f"UniFi Protect returned {len(sensors)} sensor{'s' if len(sensors) != 1 else ''}, but no temperature or humidity readings were found.",
        }
    snapshot = _snapshot_from_readings(
        provider="unifi_protect",
        source_id="unifi_protect:sensors",
        readings=readings,
        raw=raw,
        model="UniFi Protect Sensors",
    )
    _store_snapshot(snapshot, client, provider_key="unifi_protect")
    logger.info("[Environment] UniFi Protect poll stored %d readings from %d sensors.", len(readings), len(sensors))
    return {
        "ok": True,
        "provider": "unifi_protect",
        "reading_count": len(readings),
        "source_count": len(sensors),
        "message": f"Stored {len(readings)} UniFi Protect environment reading{'s' if len(readings) != 1 else ''}.",
    }


def _poll_ecobee_homekit(client: Any = None) -> Dict[str, Any]:
    configured, message = _ecobee_homekit_configured()
    if not configured:
        return {"ok": False, "provider": "ecobee_homekit", "message": message}
    try:
        from integrations.homekit import list_homekit_thermostats
    except Exception as exc:
        return {"ok": False, "provider": "ecobee_homekit", "message": f"Ecobee HomeKit integration unavailable: {exc}"}
    try:
        rows = list_homekit_thermostats()
    except Exception as exc:
        logger.warning("[Environment] Ecobee HomeKit poll failed: %s", exc)
        return {"ok": False, "provider": "ecobee_homekit", "message": str(exc)}
    thermostats = rows if isinstance(rows, list) else []
    selected = _selected_by_provider("ecobee_homekit", client)
    if not selected:
        return {"ok": True, "provider": "ecobee_homekit", "reading_count": 0, "source_count": len(thermostats), "message": "No Ecobee HomeKit thermostats are selected."}
    readings = [
        _apply_selection_to_reading(row, selection)
        for row in _normalize_ecobee_homekit_thermostats(thermostats)
        for selection in [_selection_for_source("ecobee_homekit", row.get("source_id"), selected)]
        if selection
    ]
    raw = {
        "thermostat_count": len(thermostats),
        "thermostat_names": [_text(row.get("name")) or _text(row.get("id")) for row in thermostats if isinstance(row, dict)][:50],
    }
    if not readings:
        return {
            "ok": True,
            "provider": "ecobee_homekit",
            "reading_count": 0,
            "source_count": len(thermostats),
            "message": f"Ecobee HomeKit returned {len(thermostats)} thermostat{'s' if len(thermostats) != 1 else ''}, but no environment readings were found.",
        }
    snapshot = _snapshot_from_readings(
        provider="ecobee_homekit",
        source_id="ecobee_homekit:thermostats",
        readings=readings,
        raw=raw,
        model="Ecobee HomeKit Thermostats",
    )
    _store_snapshot(snapshot, client, provider_key="ecobee_homekit")
    logger.info("[Environment] Ecobee HomeKit poll stored %d readings from %d thermostats.", len(readings), len(thermostats))
    return {
        "ok": True,
        "provider": "ecobee_homekit",
        "reading_count": len(readings),
        "source_count": len(thermostats),
        "message": f"Stored {len(readings)} Ecobee HomeKit environment reading{'s' if len(readings) != 1 else ''}.",
    }


def _poll_hue(client: Any = None) -> Dict[str, Any]:
    configured, message = _hue_configured(client)
    if not configured:
        return {"ok": False, "provider": "hue", "message": message}
    selected = _selected_by_provider("hue", client)
    if not selected:
        return {"ok": True, "provider": "hue", "reading_count": 0, "source_count": 0, "message": "No Philips Hue sensors are selected."}
    readings: List[Dict[str, Any]] = []
    raw: Dict[str, Any] = {"selected_count": len(selected)}
    device_names = _hue_device_names(client)
    by_id: Dict[str, Dict[str, Any]] = {}
    for resource in ("temperature", "relative_humidity"):
        for row in _hue_get_resource(resource, client):
            if isinstance(row, dict) and _text(row.get("id")):
                by_id[_text(row.get("id"))] = row
    for selection in selected.values():
        sensor_id = _text(selection.get("sensor_id"))
        row = by_id.get(sensor_id)
        if not row:
            continue
        category = _clean_key(selection.get("category")) or _clean_key(selection.get("measurement"))
        if category == "humidity":
            value = _hue_humidity_value(row)
            unit = "%"
        else:
            category = "temperature"
            value = _hue_temperature_value(row)
            unit = "C"
        if value is None:
            continue
        label = _text(selection.get("label")) or _hue_sensor_label(row, device_names)
        reading = _reading_row(
            key=f"hue_{_clean_key(sensor_id)}_{category}",
            label=f"{label} {CATEGORY_LABELS.get(category, category.title()).rstrip('s')}",
            category=category,
            unit=unit,
            value=value,
            provider="hue",
            source_id=f"hue:{sensor_id}",
            source_name=label,
            area=_text(selection.get("area")),
        )
        readings.append(reading)
    raw["reading_count"] = len(readings)
    if not readings:
        return {"ok": True, "provider": "hue", "reading_count": 0, "source_count": len(selected), "message": "Selected Philips Hue sensors returned no environment readings."}
    snapshot = _snapshot_from_readings(
        provider="hue",
        source_id="hue:sensors",
        readings=readings,
        raw=raw,
        model="Philips Hue Sensors",
    )
    _store_snapshot(snapshot, client, provider_key="hue")
    logger.info("[Environment] Philips Hue poll stored %d readings.", len(readings))
    return {
        "ok": True,
        "provider": "hue",
        "reading_count": len(readings),
        "source_count": len(selected),
        "message": f"Stored {len(readings)} Philips Hue environment reading{'s' if len(readings) != 1 else ''}.",
    }


def _poll_homeassistant(client: Any = None) -> Dict[str, Any]:
    configured, message = _homeassistant_configured(client)
    if not configured:
        return {"ok": False, "provider": "homeassistant", "message": message}
    selected = _selected_by_provider("homeassistant", client)
    if not selected:
        return {"ok": True, "provider": "homeassistant", "reading_count": 0, "source_count": 0, "message": "No Home Assistant sensors are selected."}
    states = _homeassistant_states(client)
    state_by_entity = {_text(row.get("entity_id")): row for row in states if isinstance(row, dict)}
    readings: List[Dict[str, Any]] = []
    for selection in selected.values():
        entity_id = _text(selection.get("sensor_id"))
        state_row = state_by_entity.get(entity_id)
        if not state_row:
            continue
        attrs = state_row.get("attributes") if isinstance(state_row.get("attributes"), dict) else {}
        value_path = _text(selection.get("value_path")) or "state"
        if value_path.startswith("attributes."):
            attr_key = value_path.split(".", 1)[1]
            value = attrs.get(attr_key)
        else:
            value = state_row.get("state")
        number = _as_float(value)
        if number is None:
            continue
        category = _clean_key(selection.get("category"))
        unit = _text(selection.get("unit"))
        if not category:
            category, unit_hint = _ha_state_category(entity_id, attrs, value_path)
            unit = unit or unit_hint
        if not category:
            continue
        label = _text(selection.get("label")) or _ha_state_name(entity_id, attrs, value_path.replace("attributes.", ""))
        readings.append(
            _reading_row(
                key=f"homeassistant_{_clean_key(entity_id)}_{_clean_key(value_path)}",
                label=f"{label} {CATEGORY_LABELS.get(category, category.title()).rstrip('s')}",
                category=category,
                unit=unit,
                value=number,
                provider="homeassistant",
                source_id=f"homeassistant:{entity_id}",
                source_name=label,
                area=_text(selection.get("area")),
            )
        )
    if not readings:
        return {"ok": True, "provider": "homeassistant", "reading_count": 0, "source_count": len(selected), "message": "Selected Home Assistant sensors returned no environment readings."}
    snapshot = _snapshot_from_readings(
        provider="homeassistant",
        source_id="homeassistant:sensors",
        readings=readings,
        raw={"selected_count": len(selected), "state_count": len(states)},
        model="Home Assistant Sensors",
    )
    _store_snapshot(snapshot, client, provider_key="homeassistant")
    logger.info("[Environment] Home Assistant poll stored %d readings.", len(readings))
    return {
        "ok": True,
        "provider": "homeassistant",
        "reading_count": len(readings),
        "source_count": len(selected),
        "message": f"Stored {len(readings)} Home Assistant environment reading{'s' if len(readings) != 1 else ''}.",
    }


def _weatherapi_value(payload: Dict[str, Any], key_us: str, key_metric: str, units: str) -> Tuple[Any, str]:
    if units == "metric":
        return payload.get(key_metric), key_metric
    return payload.get(key_us), key_us


def _normalize_weatherapi_forecast(data: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    current = data.get("current") if isinstance(data.get("current"), dict) else {}
    location = data.get("location") if isinstance(data.get("location"), dict) else {}
    units = _text(settings.get("DEFAULT_UNITS")).lower()
    if units not in {"us", "metric"}:
        units = "us"
    source_name = ", ".join(
        part
        for part in (
            _text(location.get("name")),
            _text(location.get("region")),
            _text(location.get("country")),
        )
        if part
    ) or _text(settings.get("DEFAULT_LOCATION")) or "WeatherAPI.com"
    source_id = f"weather_api:{_clean_key(source_name)}"
    readings: List[Dict[str, Any]] = []

    condition = current.get("condition") if isinstance(current.get("condition"), dict) else {}
    condition_text = _text(condition.get("text"))
    if condition_text:
        readings.append(
            _reading_row(
                key="weather_api_condition",
                label="Current Condition",
                category="condition",
                unit="",
                value=condition_text,
                display=condition_text,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )

    temp_value, temp_key = _weatherapi_value(current, "temp_f", "temp_c", units)
    temp_unit = "C" if temp_key.endswith("_c") else "F"
    if _as_float(temp_value) is not None:
        readings.append(
            _reading_row(
                key="tempc" if temp_unit == "C" else "tempf",
                label="WeatherAPI Temperature",
                category="temperature",
                unit=temp_unit,
                value=temp_value,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )

    feels_value, feels_key = _weatherapi_value(current, "feelslike_f", "feelslike_c", units)
    feels_unit = "C" if feels_key.endswith("_c") else "F"
    if _as_float(feels_value) is not None:
        readings.append(
            _reading_row(
                key="weather_api_feelslike",
                label="Feels Like",
                category="temperature",
                unit=feels_unit,
                value=feels_value,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )

    for raw_key, label, category, unit in (
        ("humidity", "Humidity", "humidity", "%"),
        ("cloud", "Cloud Cover", "condition", "%"),
        ("uv", "UV Index", "solar", ""),
    ):
        value = current.get(raw_key)
        if value is not None:
            readings.append(
                _reading_row(
                    key=raw_key if raw_key in {"humidity", "uv"} else f"weather_api_{raw_key}",
                    label=label,
                    category=category,
                    unit=unit,
                    value=value,
                    provider="weather_api",
                    source_id=source_id,
                    source_name=source_name,
                    area="Forecast",
                )
            )

    wind_value, wind_key = _weatherapi_value(current, "wind_mph", "wind_kph", units)
    if _as_float(wind_value) is not None:
        readings.append(
            _reading_row(
                key="windspeedmph" if wind_key.endswith("_mph") else "weather_api_wind_kph",
                label="Wind Speed",
                category="wind",
                unit="mph" if wind_key.endswith("_mph") else "kph",
                value=wind_value,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )
    gust_value, gust_key = _weatherapi_value(current, "gust_mph", "gust_kph", units)
    if _as_float(gust_value) is not None:
        readings.append(
            _reading_row(
                key="windgustmph" if gust_key.endswith("_mph") else "weather_api_gust_kph",
                label="Wind Gust",
                category="wind",
                unit="mph" if gust_key.endswith("_mph") else "kph",
                value=gust_value,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )
    wind_dir = _text(current.get("wind_dir"))
    if wind_dir:
        readings.append(
            _reading_row(
                key="weather_api_wind_dir",
                label="Wind Direction",
                category="wind",
                unit="",
                value=wind_dir,
                display=wind_dir,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )

    pressure_value, pressure_key = _weatherapi_value(current, "pressure_in", "pressure_mb", units)
    if _as_float(pressure_value) is not None:
        readings.append(
            _reading_row(
                key="baromrelin" if pressure_key.endswith("_in") else "weather_api_pressure_mb",
                label="Pressure",
                category="pressure",
                unit="inHg" if pressure_key.endswith("_in") else "mb",
                value=pressure_value,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )
    precip_value, precip_key = _weatherapi_value(current, "precip_in", "precip_mm", units)
    if _as_float(precip_value) is not None:
        readings.append(
            _reading_row(
                key="rainratein" if precip_key.endswith("_in") else "weather_api_precip_mm",
                label="Precipitation",
                category="rain",
                unit="in" if precip_key.endswith("_in") else "mm",
                value=precip_value,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )
    visibility_value, visibility_key = _weatherapi_value(current, "vis_miles", "vis_km", units)
    if _as_float(visibility_value) is not None:
        readings.append(
            _reading_row(
                key="weather_api_visibility",
                label="Visibility",
                category="condition",
                unit="mi" if visibility_key.endswith("_miles") else "km",
                value=visibility_value,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )

    air_quality = current.get("air_quality") if isinstance(current.get("air_quality"), dict) else {}
    epa = air_quality.get("us-epa-index")
    if epa is not None:
        readings.append(
            _reading_row(
                key="weather_api_aqi_us_epa",
                label="US EPA AQI",
                category="air",
                unit="",
                value=epa,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )
    pm25 = air_quality.get("pm2_5")
    if pm25 is not None:
        readings.append(
            _reading_row(
                key="weather_api_pm25",
                label="PM2.5",
                category="air",
                unit="ug/m3",
                value=pm25,
                provider="weather_api",
                source_id=source_id,
                source_name=source_name,
                area="Forecast",
            )
        )
    return readings


def _poll_weather_api(client: Any = None) -> Dict[str, Any]:
    settings = _load_settings(client)
    if not _as_bool(settings.get("weather_api_enabled"), False):
        return {"ok": True, "provider": "weather_api", "reading_count": 0, "message": "WeatherAPI.com forecast is disabled."}
    configured, message = _weather_api_configured(client)
    if not configured:
        return {"ok": False, "provider": "weather_api", "message": message}
    try:
        from integrations.weather_api import fetch_weatherapi_forecast, read_weatherapi_settings
    except Exception as exc:
        return {"ok": False, "provider": "weather_api", "message": f"WeatherAPI.com integration unavailable: {exc}"}
    try:
        weather_settings = read_weatherapi_settings(client)
        data, error = fetch_weatherapi_forecast(
            location=weather_settings.get("DEFAULT_LOCATION"),
            days=weather_settings.get("DEFAULT_DAYS"),
            include_aqi=weather_settings.get("INCLUDE_AQI"),
            include_pollen=weather_settings.get("INCLUDE_POLLEN"),
            include_alerts=weather_settings.get("INCLUDE_ALERTS"),
            timeout_seconds=weather_settings.get("TIMEOUT_SECONDS"),
            client=client,
        )
    except Exception as exc:
        logger.warning("[Environment] WeatherAPI.com poll failed: %s", exc)
        return {"ok": False, "provider": "weather_api", "message": str(exc)}
    if error:
        logger.warning("[Environment] WeatherAPI.com poll failed: %s", error)
        return {"ok": False, "provider": "weather_api", "message": error}
    if not isinstance(data, dict) or not data:
        return {"ok": False, "provider": "weather_api", "message": "WeatherAPI.com returned no forecast data."}
    readings = _normalize_weatherapi_forecast(data, weather_settings)
    location = data.get("location") if isinstance(data.get("location"), dict) else {}
    source_name = ", ".join(
        part
        for part in (
            _text(location.get("name")),
            _text(location.get("region")),
            _text(location.get("country")),
        )
        if part
    ) or _text(weather_settings.get("DEFAULT_LOCATION")) or "WeatherAPI.com"
    snapshot = _snapshot_from_readings(
        provider="weather_api",
        source_id=f"weather_api:{_clean_key(source_name)}",
        readings=readings,
        raw=data,
        model=source_name,
        stationtype="WeatherAPI.com Forecast",
    )
    current = data.get("current") if isinstance(data.get("current"), dict) else {}
    last_updated_epoch = _as_float(current.get("last_updated_epoch"))
    if last_updated_epoch:
        snapshot["sample_time"] = last_updated_epoch
        snapshot["sample_time_text"] = _format_ts(last_updated_epoch)
    _store_snapshot(snapshot, client, provider_key="weather_api")
    logger.info("[Environment] WeatherAPI.com poll stored %d readings.", len(readings))
    return {
        "ok": True,
        "provider": "weather_api",
        "reading_count": len(readings),
        "source_count": 1,
        "message": f"Stored WeatherAPI.com forecast for {source_name}.",
    }


def _poll_enabled_integrations(client: Any = None) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    settings = _load_settings(client)
    selected_providers = {
        _clean_key(row.get("provider"))
        for row in _load_selected_sensors(client)
        if _as_bool(row.get("enabled"), True)
    }
    pollers = {
        "unifi_protect": _poll_unifi_protect,
        "ecobee_homekit": _poll_ecobee_homekit,
        "hue": _poll_hue,
        "homeassistant": _poll_homeassistant,
    }
    for provider, poller in pollers.items():
        if provider in selected_providers:
            results.append(poller(client))
    if _as_bool(settings.get("weather_api_enabled"), False):
        results.append(_poll_weather_api(client))
    if not results:
        return {"ok": True, "message": "No integration sensors or forecast providers are enabled.", "results": []}
    stored = sum(int(result.get("reading_count") or 0) for result in results if result.get("ok"))
    failures = [result for result in results if not result.get("ok")]
    pieces = [_text(result.get("message")) for result in results if _text(result.get("message"))]
    return {
        "ok": not failures,
        "reading_count": stored,
        "results": results,
        "message": " ".join(pieces) or f"Integration poll complete with {stored} reading{'s' if stored != 1 else ''}.",
    }


def _load_json_key(key: str, client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    try:
        raw = store.get(key)
    except Exception:
        raw = None
    if not raw:
        return {}
    try:
        parsed = json.loads(_text(raw))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _load_history(limit: int = 10, client: Any = None) -> List[Dict[str, Any]]:
    store = client or redis_client
    try:
        rows = store.lrange(HISTORY_KEY, 0, max(0, int(limit) - 1)) or []
    except Exception:
        rows = []
    out: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            parsed = json.loads(_text(raw))
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            out.append(parsed)
    return out


def _snapshot_provider_summary(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    provider = _clean_key(snapshot.get("provider") or "ecowitt")
    return {
        "id": provider,
        "label": _provider_label(provider),
        "source_id": _text(snapshot.get("source_id")),
        "received_at": snapshot.get("received_at"),
        "reading_count": len(snapshot.get("readings") or []),
    }


def _load_provider_snapshots(client: Any = None) -> Dict[str, Dict[str, Any]]:
    snapshots: Dict[str, Dict[str, Any]] = {}
    for provider, key in LATEST_PROVIDER_KEYS.items():
        snapshot = _load_json_key(key, client)
        if snapshot:
            snapshots[provider] = snapshot
    latest = _load_json_key(LATEST_KEY, client)
    if latest:
        provider = _clean_key(latest.get("provider") or "ecowitt")
        snapshots.setdefault(provider, latest)
    return snapshots


def _combined_snapshot(provider_snapshots: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ordered: List[Dict[str, Any]] = []
    for provider in LATEST_PROVIDER_KEYS:
        snapshot = provider_snapshots.get(provider)
        if snapshot:
            ordered.append(snapshot)
    for provider, snapshot in (provider_snapshots or {}).items():
        if provider not in LATEST_PROVIDER_KEYS and snapshot:
            ordered.append(snapshot)
    if not ordered:
        return {}

    if len(ordered) == 1:
        single = dict(ordered[0])
        provider = _clean_key(single.get("provider") or "ecowitt")
        source_id = single.get("source_id") or provider
        enriched_readings: List[Dict[str, Any]] = []
        for row in single.get("readings") or []:
            if not isinstance(row, dict):
                continue
            next_row = dict(row)
            next_row.setdefault("provider", provider)
            next_row.setdefault("provider_label", _provider_label(provider))
            next_row.setdefault("source_id", source_id)
            next_row.setdefault("source_name", single.get("model") or single.get("stationtype") or _provider_label(provider))
            enriched_readings.append(next_row)
        single["readings"] = enriched_readings
        single["providers"] = [_snapshot_provider_summary(single)]
        return single

    readings: List[Dict[str, Any]] = []
    raw: Dict[str, Any] = {}
    providers: List[Dict[str, Any]] = []
    received_values: List[float] = []
    sample_values: List[float] = []
    for snapshot in ordered:
        provider = _clean_key(snapshot.get("provider") or "ecowitt")
        providers.append(_snapshot_provider_summary(snapshot))
        raw[provider] = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
        received = _as_float(snapshot.get("received_at"))
        sample = _as_float(snapshot.get("sample_time"))
        if received is not None:
            received_values.append(received)
        if sample is not None:
            sample_values.append(sample)
        for row in snapshot.get("readings") or []:
            if not isinstance(row, dict):
                continue
            next_row = dict(row)
            next_row.setdefault("provider", provider)
            next_row.setdefault("provider_label", _provider_label(provider))
            next_row.setdefault("source_id", snapshot.get("source_id") or provider)
            next_row.setdefault("source_name", _provider_label(provider))
            readings.append(next_row)

    readings.sort(
        key=lambda row: (
            str(row.get("category") or ""),
            str(row.get("provider_label") or ""),
            str(row.get("source_name") or ""),
            str(row.get("label") or ""),
        )
    )
    received_at = max(received_values) if received_values else time.time()
    sample_time = max(sample_values) if sample_values else received_at
    provider_label = " + ".join(item.get("label") or item.get("id") or "Source" for item in providers)
    return {
        "provider": "environment",
        "source_id": "environment:combined",
        "stationtype": "",
        "model": provider_label,
        "frequency": "",
        "received_at": received_at,
        "sample_time": sample_time,
        "sample_time_text": _format_ts(sample_time),
        "providers": providers,
        "readings": readings,
        "raw": raw,
    }


def _readings_by_category(snapshot: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in snapshot.get("readings") or []:
        if not isinstance(row, dict):
            continue
        category = _text(row.get("category")) or "other"
        grouped.setdefault(category, []).append(row)
    return grouped


def _reading(snapshot: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    wanted = _clean_key(key)
    for row in snapshot.get("readings") or []:
        if isinstance(row, dict) and _clean_key(row.get("key")) == wanted:
            return row
    return None


def _reading_display(snapshot: Dict[str, Any], key: str, default: str = "-") -> str:
    row = _reading(snapshot, key)
    if not row:
        return default
    return _text(row.get("display")) or default


def _category_display(snapshot: Dict[str, Any], category: str, default: str = "-") -> str:
    wanted = _clean_key(category)
    for row in snapshot.get("readings") or []:
        if not isinstance(row, dict) or _clean_key(row.get("category")) != wanted:
            continue
        display = _text(row.get("display"))
        if display:
            return display
    return default


def _status_word(*, configured: bool) -> str:
    return "Ready" if configured else "Needs setup"


def _provider_status_rows(
    _settings: Dict[str, Any],
    provider_snapshots: Dict[str, Dict[str, Any]],
    client: Any = None,
) -> List[Dict[str, str]]:
    unifi_configured, unifi_message = _unifi_protect_configured(client)
    ecobee_configured, ecobee_message = _ecobee_homekit_configured()
    hue_configured, hue_message = _hue_configured(client)
    ha_configured, ha_message = _homeassistant_configured(client)
    weather_configured, weather_message = _weather_api_configured(client)
    weather_enabled = _as_bool(_settings.get("weather_api_enabled"), False)
    selected = _load_selected_sensors(client)
    selected_counts: Dict[str, int] = {}
    for row in selected:
        provider = _clean_key(row.get("provider"))
        if not _as_bool(row.get("enabled"), True):
            continue
        selected_counts[provider] = selected_counts.get(provider, 0) + 1
    specs = [
        {
            "provider": "ecowitt",
            "configured": True,
            "setup": "Webhook receiver",
        },
        {
            "provider": "unifi_protect",
            "configured": unifi_configured,
            "setup": "Configured" if unifi_configured else unifi_message,
        },
        {
            "provider": "ecobee_homekit",
            "configured": ecobee_configured,
            "setup": "Configured" if ecobee_configured else ecobee_message,
        },
        {
            "provider": "hue",
            "configured": hue_configured,
            "setup": "Configured" if hue_configured else hue_message,
        },
        {
            "provider": "homeassistant",
            "configured": ha_configured,
            "setup": "Configured" if ha_configured else ha_message,
        },
        {
            "provider": "weather_api",
            "configured": weather_configured,
            "setup": "Enabled" if weather_enabled and weather_configured else "Disabled" if not weather_enabled else weather_message,
            "selected": "Enabled" if weather_enabled else "Disabled",
        },
    ]
    rows: List[Dict[str, str]] = []
    for spec in specs:
        provider = _clean_key(spec.get("provider"))
        snapshot = provider_snapshots.get(provider) or {}
        reading_count = len(snapshot.get("readings") or []) if snapshot else 0
        rows.append(
            {
                "source": _provider_label(provider),
                "state": _status_word(configured=bool(spec.get("configured"))),
                "setup": _text(spec.get("setup")) or "-",
                "last_sample": _age_label(snapshot.get("received_at")) if snapshot else "never",
                "readings": str(reading_count) if snapshot else "-",
                "selected": _text(spec.get("selected"))
                or (str(selected_counts.get(provider, 0)) if provider != "ecowitt" else "Webhook"),
            }
        )
    return rows


def _sensor_rows(rows: Iterable[Dict[str, Any]], *, include_meta: bool = True) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        label = _text(row.get("label"))
        value = _text(row.get("display"))
        if not label or not value:
            continue
        meta_parts = [_text(row.get("area")), _text(row.get("provider_label"))]
        meta = " - ".join(part for part in meta_parts if part)
        out.append(
            {
                "label": label,
                "value": value,
                "meta": meta
                or (CATEGORY_LABELS.get(_text(row.get("category")), _text(row.get("category"))) if include_meta else ""),
            }
        )
    return out


def _history_summary(history: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for item in history[:8]:
        rows.append(
            {
                "label": _age_label(item.get("received_at")),
                "value": _reading_display(item, "tempf", _reading_display(item, "tempinf", _category_display(item, "temperature"))),
                "meta": item.get("model") or item.get("stationtype") or _provider_label(item.get("provider") or "ecowitt"),
            }
        )
    return rows


def _reading_rows_by_keys(snapshot: Dict[str, Any], keys: Iterable[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for key in keys:
        row = _reading(snapshot, key)
        row_key = _clean_key(row.get("key")) if isinstance(row, dict) else ""
        if row and row_key and row_key not in seen:
            rows.append(row)
            seen.add(row_key)
    return rows


def _reading_rows_by_categories(
    snapshot: Dict[str, Any],
    categories: Iterable[str],
    *,
    limit: int = 18,
    exclude_keys: Iterable[str] = (),
    exclude_providers: Iterable[str] = (),
) -> List[Dict[str, Any]]:
    wanted = [_clean_key(category) for category in categories if _clean_key(category)]
    exclude_key_set = {_clean_key(key) for key in exclude_keys}
    exclude_provider_set = {_clean_key(provider) for provider in exclude_providers}
    rows: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, str]] = set()
    for category in wanted:
        for row in snapshot.get("readings") or []:
            if not isinstance(row, dict) or _clean_key(row.get("category")) != category:
                continue
            key = _clean_key(row.get("key"))
            provider = _clean_key(row.get("provider"))
            if key in exclude_key_set or provider in exclude_provider_set:
                continue
            identity = (provider, _clean_key(row.get("source_id")), key)
            if identity in seen:
                continue
            rows.append(row)
            seen.add(identity)
            if len(rows) >= max(1, int(limit)):
                return rows
    return rows


def _area_overview_rows(snapshot: Dict[str, Any]) -> List[Dict[str, str]]:
    areas: Dict[str, Dict[str, Any]] = {}
    for row in snapshot.get("readings") or []:
        if not isinstance(row, dict):
            continue
        category = _clean_key(row.get("category"))
        if category not in {"temperature", "humidity", "air", "solar", "condition"}:
            continue
        area = _text(row.get("area")) or _text(row.get("source_name")) or _provider_label(row.get("provider"))
        if not area:
            continue
        entry = areas.setdefault(area, {"area": area, "sources": set()})
        provider_label = _text(row.get("provider_label")) or _provider_label(row.get("provider"))
        if provider_label:
            entry["sources"].add(provider_label)
        display = _text(row.get("display"))
        if not display:
            continue
        key = _clean_key(row.get("key"))
        label = _text(row.get("label"))
        if category == "temperature" and "temperature" not in entry:
            entry["temperature"] = display
        elif category == "humidity" and "humidity" not in entry:
            entry["humidity"] = display
        elif category == "air" and "air" not in entry:
            entry["air"] = f"{label}: {display}" if label else display
        elif category == "solar" and key == "uv" and "light" not in entry:
            entry["light"] = f"UV {display}"
        elif category == "condition" and "condition" not in entry:
            entry["condition"] = display

    def area_sort(row: Dict[str, Any]) -> Tuple[int, str]:
        token = _clean_key(row.get("area"))
        priority = 2
        if token in {"outside", "outdoor", "forecast"}:
            priority = 0
        elif token in {"inside", "indoor"}:
            priority = 1
        return priority, _text(row.get("area")).casefold()

    rows: List[Dict[str, str]] = []
    for entry in sorted(areas.values(), key=area_sort):
        rows.append(
            {
                "area": _text(entry.get("area")),
                "temperature": _text(entry.get("temperature")) or "-",
                "humidity": _text(entry.get("humidity")) or "-",
                "condition": _text(entry.get("condition")) or _text(entry.get("light")) or _text(entry.get("air")) or "-",
                "source": ", ".join(sorted(entry.get("sources") or [])) or "-",
            }
        )
    return rows[:24]


def _source_health_rows(provider_status_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for row in provider_status_rows or []:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "source": _text(row.get("source")),
                "state": _text(row.get("state")),
                "selected": _text(row.get("selected")),
                "last_sample": _text(row.get("last_sample")),
                "readings": _text(row.get("readings")),
            }
        )
    return rows


def _source_reading_points(provider_status_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    for row in provider_status_rows or []:
        if not isinstance(row, dict):
            continue
        value = _as_int(row.get("readings"), 0, minimum=0, maximum=100000)
        if value <= 0:
            continue
        points.append({"label": _text(row.get("source")) or "Source", "value": value})
    return points


def _snapshot_has_category(snapshot: Dict[str, Any], category: str) -> bool:
    wanted = _clean_key(category)
    for row in snapshot.get("readings") or []:
        if isinstance(row, dict) and _clean_key(row.get("category")) == wanted:
            return True
    return False


def _provider_source_option(provider: str, provider_snapshots: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    provider_key = _clean_key(provider)
    snapshot = provider_snapshots.get(provider_key) or {}
    suffix = "ready" if snapshot else "waiting"
    return {"value": f"provider:{provider_key}", "label": f"{_provider_label(provider_key)} ({suffix})"}


def _environment_display_source_options(
    provider_snapshots: Dict[str, Dict[str, Any]],
    selected_sensors: List[Dict[str, Any]],
    *,
    include_condition_only: bool,
) -> List[Dict[str, Any]]:
    provider_options: List[Dict[str, str]] = []
    provider_order = ("ecowitt", "weather_api", "unifi_protect", "ecobee_homekit", "hue", "homeassistant")
    for provider in provider_order:
        snapshot = provider_snapshots.get(provider) or {}
        if include_condition_only and provider != "weather_api" and not _snapshot_has_category(snapshot, "condition"):
            continue
        provider_options.append(_provider_source_option(provider, provider_snapshots))

    sensor_options: List[Dict[str, str]] = []
    for row in selected_sensors or []:
        if not isinstance(row, dict) or not _as_bool(row.get("enabled"), True):
            continue
        category = _clean_key(row.get("category"))
        if include_condition_only and category != "condition":
            continue
        key = _text(row.get("key"))
        if not key:
            continue
        label = _text(row.get("label")) or key
        area = _text(row.get("area"))
        provider = _provider_label(row.get("provider"))
        pieces = [label]
        if area:
            pieces.append(area)
        if provider:
            pieces.append(provider)
        sensor_options.append({"value": f"sensor:{key}", "label": " - ".join(pieces)})
    sensor_options.sort(key=lambda item: _text(item.get("label")).casefold())

    groups: List[Dict[str, Any]] = [{"label": "Integrations", "options": provider_options}]
    if sensor_options:
        groups.append({"label": "Selected Sensors", "options": sensor_options})
    return groups


def _forecast_provider_options(provider_snapshots: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    weather_snapshot = provider_snapshots.get("weather_api") or {}
    suffix = "ready" if weather_snapshot else "waiting"
    return [{"value": "weather_api", "label": f"WeatherAPI.com ({suffix})"}]


def _selected_sensor_display_snapshot(
    selection_key: str,
    combined_snapshot: Dict[str, Any],
    selected_sensors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    selection = next((row for row in selected_sensors or [] if _text(row.get("key")) == selection_key), None)
    if not isinstance(selection, dict):
        return {}
    provider = _clean_key(selection.get("provider"))
    sensor_clean = _clean_key(selection.get("sensor_id"))
    selection_clean = _clean_key(selection.get("key"))
    rows: List[Dict[str, Any]] = []
    for row in combined_snapshot.get("readings") or []:
        if not isinstance(row, dict):
            continue
        if provider and _clean_key(row.get("provider")) != provider:
            continue
        source_suffix = _text(row.get("source_id")).split(":", 1)[1] if ":" in _text(row.get("source_id")) else _text(row.get("source_id"))
        source_clean = _clean_key(source_suffix)
        row_key = _clean_key(row.get("key"))
        if sensor_clean and (source_clean == sensor_clean or sensor_clean in source_clean or sensor_clean in row_key):
            rows.append(row)
            continue
        if selection_clean and (row_key == selection_clean or selection_clean in row_key):
            rows.append(row)
    if not rows:
        return {}
    label = _text(selection.get("label")) or selection_key
    return {
        "provider": provider or "environment",
        "source_id": f"display:{selection_key}",
        "model": label,
        "received_at": combined_snapshot.get("received_at"),
        "sample_time": combined_snapshot.get("sample_time"),
        "readings": rows,
    }


def _display_snapshot_for_source(
    source: Any,
    combined_snapshot: Dict[str, Any],
    provider_snapshots: Dict[str, Dict[str, Any]],
    selected_sensors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    source_text = _text(source)
    if source_text.startswith("sensor:"):
        return _selected_sensor_display_snapshot(source_text.split(":", 1)[1], combined_snapshot, selected_sensors)
    if source_text.startswith("provider:"):
        provider = _clean_key(source_text.split(":", 1)[1])
        if provider in {"environment", "all"}:
            return combined_snapshot if isinstance(combined_snapshot, dict) else {}
        return provider_snapshots.get(provider) or {}
    provider = _clean_key(source_text)
    if provider:
        return provider_snapshots.get(provider) or {}
    return {}


def _overview_card(
    *,
    card_id: str,
    title: str,
    subtitle: str,
    detail: str,
    sensor_title: str,
    sensor_rows: List[Dict[str, Any]],
    badges: Optional[List[Dict[str, str]]] = None,
    summary_rows: Optional[List[Dict[str, str]]] = None,
    sections: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "id": card_id,
        "group": "overview",
        "title": title,
        "subtitle": subtitle,
        "detail": detail,
        "hero_badges": badges or [],
        "summary_rows": summary_rows or [],
        "sensor_title": sensor_title,
        "sensor_rows": _sensor_rows(sensor_rows, include_meta=True),
        "sections": sections or [],
    }


def _weatherapi_units(client: Any = None) -> str:
    try:
        from integrations.weather_api import read_weatherapi_settings

        settings = read_weatherapi_settings(client)
    except Exception:
        settings = {}
    units = _text(settings.get("DEFAULT_UNITS")).lower()
    return units if units in {"us", "metric"} else "us"


def _weather_condition_theme(condition: Any) -> Dict[str, str]:
    text = _text(condition).lower()
    if any(token in text for token in ("thunder", "storm", "lightning")):
        return {"kind": "storm", "top": "#293047", "bottom": "#5d4f86", "accent": "#ffd166", "ink": "#f8fbff"}
    if any(token in text for token in ("rain", "drizzle", "shower", "sleet")):
        return {"kind": "rain", "top": "#2d6f9f", "bottom": "#12384f", "accent": "#8fe3ff", "ink": "#f8fbff"}
    if any(token in text for token in ("snow", "ice", "blizzard", "flurr")):
        return {"kind": "snow", "top": "#e8f7ff", "bottom": "#7fb7d9", "accent": "#ffffff", "ink": "#153046"}
    if any(token in text for token in ("fog", "mist", "haze", "smoke")):
        return {"kind": "fog", "top": "#cfd8dc", "bottom": "#7f8f95", "accent": "#f4fbff", "ink": "#20343a"}
    if any(token in text for token in ("cloud", "overcast")):
        return {"kind": "cloud", "top": "#6fa8dc", "bottom": "#456a86", "accent": "#dfeaf2", "ink": "#f8fbff"}
    if any(token in text for token in ("wind", "breez")):
        return {"kind": "wind", "top": "#60c3b4", "bottom": "#2e7973", "accent": "#e6fff9", "ink": "#f8fbff"}
    if any(token in text for token in ("sun", "clear")):
        return {"kind": "sun", "top": "#f7b733", "bottom": "#f26b38", "accent": "#fff2a8", "ink": "#24160d"}
    return {"kind": "partly", "top": "#74b9ff", "bottom": "#5177c2", "accent": "#fff0a8", "ink": "#f8fbff"}


def _weather_condition_icon(kind: str, *, x: int, y: int, scale: float = 1.0, ink: str = "#ffffff", accent: str = "#fff2a8") -> str:
    def n(value: float) -> str:
        return f"{value:.2f}".rstrip("0").rstrip(".")

    def pt(dx: float, dy: float) -> Tuple[str, str]:
        return n(x + dx * scale), n(y + dy * scale)

    if kind == "sun":
        cx, cy = pt(80, 78)
        rays = []
        for x1, y1, x2, y2 in ((80, 8, 80, 28), (80, 128, 80, 150), (10, 78, 34, 78), (126, 78, 150, 78), (30, 28, 46, 44), (114, 112, 132, 130), (30, 130, 48, 112), (114, 44, 132, 28)):
            sx1, sy1 = pt(x1, y1)
            sx2, sy2 = pt(x2, y2)
            rays.append(f'<line x1="{sx1}" y1="{sy1}" x2="{sx2}" y2="{sy2}" stroke="{html_escape(accent)}" stroke-width="{n(8 * scale)}" stroke-linecap="round" opacity="0.86"/>')
        return "".join(rays) + f'<circle cx="{cx}" cy="{cy}" r="{n(42 * scale)}" fill="{html_escape(accent)}" opacity="0.95"/>'

    cloud = []
    for cx0, cy0, r0 in ((70, 86, 34), (108, 78, 44), (150, 92, 32)):
        cx1, cy1 = pt(cx0, cy0)
        cloud.append(f'<circle cx="{cx1}" cy="{cy1}" r="{n(r0 * scale)}" fill="{html_escape(ink)}" opacity="0.9"/>')
    rx, ry = pt(48, 88)
    cloud.append(f'<rect x="{rx}" y="{ry}" width="{n(136 * scale)}" height="{n(48 * scale)}" rx="{n(24 * scale)}" fill="{html_escape(ink)}" opacity="0.9"/>')
    cloud_svg = "".join(cloud)

    if kind == "partly":
        sun = _weather_condition_icon("sun", x=x - int(48 * scale), y=y - int(38 * scale), scale=scale * 0.62, ink=ink, accent=accent)
        return f'<g opacity="0.95">{sun}</g>{cloud_svg}'
    if kind == "rain":
        drops = []
        for dx in (62, 104, 146):
            x1, y1 = pt(dx, 148)
            x2, y2 = pt(dx - 16, 188)
            drops.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{html_escape(accent)}" stroke-width="{n(8 * scale)}" stroke-linecap="round"/>')
        return cloud_svg + "".join(drops)
    if kind == "storm":
        p1 = pt(106, 132)
        p2 = pt(82, 188)
        p3 = pt(116, 178)
        p4 = pt(94, 230)
        bolt = f'<polygon points="{p1[0]},{p1[1]} {p2[0]},{p2[1]} {p3[0]},{p3[1]} {p4[0]},{p4[1]}" fill="{html_escape(accent)}"/>'
        return cloud_svg + bolt
    if kind == "snow":
        flakes = []
        for dx, dy in ((72, 160), (112, 188), (150, 160)):
            cx1, cy1 = pt(dx, dy)
            r = n(7 * scale)
            flakes.append(f'<circle cx="{cx1}" cy="{cy1}" r="{r}" fill="{html_escape(accent)}"/>')
        return cloud_svg + "".join(flakes)
    if kind == "fog":
        lines = []
        for dy in (70, 104, 138, 172):
            x1, y1 = pt(30, dy)
            x2, y2 = pt(178, dy)
            lines.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{html_escape(accent)}" stroke-width="{n(10 * scale)}" stroke-linecap="round" opacity="0.72"/>')
        return "".join(lines)
    if kind == "wind":
        lines = []
        for dy, width in ((74, 150), (112, 118), (150, 160)):
            x1, y1 = pt(26, dy)
            x2, y2 = pt(width, dy)
            lines.append(f'<path d="M {x1} {y1} C {n(x + 70 * scale)} {y1}, {n(x + 94 * scale)} {n(y + (dy - 24) * scale)}, {x2} {y2}" fill="none" stroke="{html_escape(accent)}" stroke-width="{n(10 * scale)}" stroke-linecap="round"/>')
        return "".join(lines)
    return cloud_svg


def _weather_day_label(value: Any) -> str:
    text = _text(value)
    if not text:
        return "Day"
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
        return parsed.strftime("%a")
    except Exception:
        return text[:10]


def _weatherapi_forecast_days(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
    forecast = raw.get("forecast") if isinstance(raw.get("forecast"), dict) else {}
    days: List[Dict[str, Any]] = []
    for item in forecast.get("forecastday") or []:
        if isinstance(item, dict):
            days.append(item)
    return days


def _weather_current_card_data_uri(condition_snapshot: Dict[str, Any], *, live_snapshot: Optional[Dict[str, Any]] = None, units: str) -> str:
    live = live_snapshot if isinstance(live_snapshot, dict) and live_snapshot else condition_snapshot
    condition = _reading_display(condition_snapshot, "weather_api_condition", "Current Conditions")
    theme = _weather_condition_theme(condition)
    temp = _reading_display(live, "tempf", _reading_display(live, "tempc", _category_display(live, "temperature")))
    feels = _reading_display(live, "feelslikef", _reading_display(condition_snapshot, "weather_api_feelslike"))
    humidity = _reading_display(live, "humidity", _category_display(live, "humidity"))
    wind = _reading_display(live, "windspeedmph", _category_display(live, "wind", _reading_display(condition_snapshot, "weather_api_wind_kph")))
    accent = theme["accent"]
    ink = theme["ink"]
    icon = _weather_condition_icon(theme["kind"], x=620, y=40, scale=0.92, ink="#ffffff", accent=accent)
    svg = f"""
<svg xmlns="http://www.w3.org/2000/svg" width="960" height="360" viewBox="0 0 960 360">
  <defs>
    <linearGradient id="bg" x1="0" x2="1" y1="0" y2="1">
      <stop offset="0" stop-color="{html_escape(theme['top'])}"/>
      <stop offset="1" stop-color="{html_escape(theme['bottom'])}"/>
    </linearGradient>
    <linearGradient id="shine" x1="0" x2="1" y1="0" y2="0">
      <stop offset="0" stop-color="#ffffff" stop-opacity="0.22"/>
      <stop offset="1" stop-color="#ffffff" stop-opacity="0"/>
    </linearGradient>
  </defs>
  <rect width="960" height="360" rx="34" fill="url(#bg)"/>
  <circle cx="850" cy="18" r="180" fill="url(#shine)"/>
  <text x="54" y="76" fill="{html_escape(ink)}" font-family="Inter, Arial, sans-serif" font-size="26" font-weight="700" opacity="0.82">Current Conditions</text>
  <text x="54" y="182" fill="{html_escape(ink)}" font-family="Inter, Arial, sans-serif" font-size="96" font-weight="800">{html_escape(temp)}</text>
  <text x="58" y="238" fill="{html_escape(ink)}" font-family="Inter, Arial, sans-serif" font-size="36" font-weight="750">{html_escape(condition)}</text>
  <text x="60" y="296" fill="{html_escape(ink)}" font-family="Inter, Arial, sans-serif" font-size="24" opacity="0.86">Feels {html_escape(feels)}   Humidity {html_escape(humidity)}   Wind {html_escape(wind)}</text>
  <g>{icon}</g>
</svg>
""".strip()
    return "data:image/svg+xml;charset=utf-8," + quote(svg)


def _weather_forecast_cards_data_uri(snapshot: Dict[str, Any], *, units: str) -> str:
    days = _weatherapi_forecast_days(snapshot)[:5]
    width = 1080
    height = 360
    gap = 18
    card_w = int((width - 72 - gap * max(0, len(days) - 1)) / max(1, len(days)))
    cards: List[str] = []
    for index, item in enumerate(days):
        day = item.get("day") if isinstance(item.get("day"), dict) else {}
        condition = day.get("condition") if isinstance(day.get("condition"), dict) else {}
        condition_text = _text(condition.get("text")) or "Forecast"
        theme = _weather_condition_theme(condition_text)
        x = 36 + index * (card_w + gap)
        y = 34
        high = _weatherapi_temp(day, "maxtemp_f", "maxtemp_c", units)
        low = _weatherapi_temp(day, "mintemp_f", "mintemp_c", units)
        rain = f"{_text(day.get('daily_chance_of_rain')) or '0'}%"
        wind = _weatherapi_speed(day, "maxwind_mph", "maxwind_kph", units)
        icon = _weather_condition_icon(theme["kind"], x=x + int(card_w * 0.18), y=y + 50, scale=0.5, ink="#ffffff", accent=theme["accent"])
        cards.append(
            f"""
  <g>
    <rect x="{x}" y="{y}" width="{card_w}" height="292" rx="28" fill="{html_escape(theme['top'])}"/>
    <rect x="{x}" y="{y}" width="{card_w}" height="292" rx="28" fill="{html_escape(theme['bottom'])}" opacity="0.58"/>
    <text x="{x + 26}" y="{y + 44}" fill="{html_escape(theme['ink'])}" font-family="Inter, Arial, sans-serif" font-size="26" font-weight="800">{html_escape(_weather_day_label(item.get('date')))}</text>
    <text x="{x + card_w - 24}" y="{y + 44}" text-anchor="end" fill="{html_escape(theme['ink'])}" font-family="Inter, Arial, sans-serif" font-size="18" opacity="0.82">{html_escape(_text(item.get('date'))[5:] or '')}</text>
    <g opacity="0.95">{icon}</g>
    <text x="{x + 26}" y="{y + 172}" fill="{html_escape(theme['ink'])}" font-family="Inter, Arial, sans-serif" font-size="30" font-weight="800">{html_escape(high)}</text>
    <text x="{x + 26}" y="{y + 204}" fill="{html_escape(theme['ink'])}" font-family="Inter, Arial, sans-serif" font-size="20" opacity="0.88">Low {html_escape(low)}</text>
    <text x="{x + 26}" y="{y + 238}" fill="{html_escape(theme['ink'])}" font-family="Inter, Arial, sans-serif" font-size="18" font-weight="700">{html_escape(condition_text[:22])}</text>
    <text x="{x + 26}" y="{y + 270}" fill="{html_escape(theme['ink'])}" font-family="Inter, Arial, sans-serif" font-size="16" opacity="0.84">Rain {html_escape(rain)}   Wind {html_escape(wind)}</text>
  </g>
""".strip()
        )

    if not cards:
        cards.append(
            """
  <text x="540" y="180" text-anchor="middle" fill="#edf3f0" font-family="Inter, Arial, sans-serif" font-size="28" font-weight="700">Waiting for forecast data</text>
""".strip()
        )

    svg = f"""
<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <rect width="{width}" height="{height}" rx="28" fill="#101820"/>
  <text x="38" y="24" fill="#edf3f0" font-family="Inter, Arial, sans-serif" font-size="22" font-weight="800">Daily Forecast</text>
  {''.join(cards)}
</svg>
""".strip()
    return "data:image/svg+xml;charset=utf-8," + quote(svg)


def _weatherapi_temp(day: Dict[str, Any], key_us: str, key_metric: str, units: str) -> str:
    value = day.get(key_metric if units == "metric" else key_us)
    unit = "C" if units == "metric" else "F"
    return _value_label(value, unit) if value is not None else "-"


def _weatherapi_speed(day: Dict[str, Any], key_us: str, key_metric: str, units: str) -> str:
    value = day.get(key_metric if units == "metric" else key_us)
    unit = "kph" if units == "metric" else "mph"
    return _value_label(value, unit) if value is not None else "-"


def _weatherapi_precip(day: Dict[str, Any], key_us: str, key_metric: str, units: str) -> str:
    value = day.get(key_metric if units == "metric" else key_us)
    unit = "mm" if units == "metric" else "in"
    return _value_label(value, unit) if value is not None else "-"


def _weatherapi_daily_rows(snapshot: Dict[str, Any], *, units: str) -> List[Dict[str, str]]:
    raw = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
    forecast = raw.get("forecast") if isinstance(raw.get("forecast"), dict) else {}
    out: List[Dict[str, str]] = []
    for item in forecast.get("forecastday") or []:
        if not isinstance(item, dict):
            continue
        day = item.get("day") if isinstance(item.get("day"), dict) else {}
        astro = item.get("astro") if isinstance(item.get("astro"), dict) else {}
        condition = day.get("condition") if isinstance(day.get("condition"), dict) else {}
        out.append(
            {
                "date": _text(item.get("date")) or "-",
                "condition": _text(condition.get("text")) or "-",
                "high": _weatherapi_temp(day, "maxtemp_f", "maxtemp_c", units),
                "low": _weatherapi_temp(day, "mintemp_f", "mintemp_c", units),
                "rain": f"{_text(day.get('daily_chance_of_rain')) or '0'}%",
                "snow": f"{_text(day.get('daily_chance_of_snow')) or '0'}%",
                "precip": _weatherapi_precip(day, "totalprecip_in", "totalprecip_mm", units),
                "wind": _weatherapi_speed(day, "maxwind_mph", "maxwind_kph", units),
                "uv": _text(day.get("uv")) or "-",
                "sunrise": _text(astro.get("sunrise")) or "-",
                "sunset": _text(astro.get("sunset")) or "-",
            }
        )
    return out


def _weatherapi_hourly_rows(snapshot: Dict[str, Any], *, units: str, limit: int = 24) -> List[Dict[str, str]]:
    raw = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
    current = raw.get("current") if isinstance(raw.get("current"), dict) else {}
    forecast = raw.get("forecast") if isinstance(raw.get("forecast"), dict) else {}
    now_epoch = _as_float(current.get("last_updated_epoch")) or (time.time() - 3600)
    out: List[Dict[str, str]] = []
    for forecast_day in forecast.get("forecastday") or []:
        if not isinstance(forecast_day, dict):
            continue
        for hour in forecast_day.get("hour") or []:
            if not isinstance(hour, dict):
                continue
            epoch = _as_float(hour.get("time_epoch"))
            if epoch is not None and epoch < now_epoch:
                continue
            condition = hour.get("condition") if isinstance(hour.get("condition"), dict) else {}
            temp_value = hour.get("temp_c" if units == "metric" else "temp_f")
            wind_value = hour.get("wind_kph" if units == "metric" else "wind_mph")
            out.append(
                {
                    "time": datetime.fromtimestamp(epoch).strftime("%a %I %p").replace(" 0", " ") if epoch else _text(hour.get("time")) or "-",
                    "temp": _value_label(temp_value, "C" if units == "metric" else "F") if temp_value is not None else "-",
                    "condition": _text(condition.get("text")) or "-",
                    "rain": f"{_text(hour.get('chance_of_rain')) or '0'}%",
                    "snow": f"{_text(hour.get('chance_of_snow')) or '0'}%",
                    "wind": _value_label(wind_value, "kph" if units == "metric" else "mph") if wind_value is not None else "-",
                    "humidity": f"{_text(hour.get('humidity')) or '-'}%",
                    "uv": _text(hour.get("uv")) or "-",
                }
            )
            if len(out) >= max(1, int(limit)):
                return out
    return out


def _weatherapi_alert_rows(snapshot: Dict[str, Any]) -> List[Dict[str, str]]:
    raw = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
    alerts = raw.get("alerts") if isinstance(raw.get("alerts"), dict) else {}
    out: List[Dict[str, str]] = []
    for item in alerts.get("alert") or []:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "event": _text(item.get("event") or item.get("headline")) or "Alert",
                "severity": _text(item.get("severity")) or "-",
                "areas": _text(item.get("areas")) or "-",
                "effective": _text(item.get("effective")) or "-",
                "expires": _text(item.get("expires")) or "-",
            }
        )
    return out


def _weatherapi_air_rows(snapshot: Dict[str, Any]) -> List[Dict[str, str]]:
    raw = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
    current = raw.get("current") if isinstance(raw.get("current"), dict) else {}
    air = current.get("air_quality") if isinstance(current.get("air_quality"), dict) else {}
    pollen = current.get("pollen") if isinstance(current.get("pollen"), dict) else {}
    rows: List[Dict[str, str]] = []
    for key, value in air.items():
        if value in (None, ""):
            continue
        rows.append({"metric": _humanize_key(_clean_key(key)), "value": _value_label(value), "source": "Air Quality"})
    for key, value in pollen.items():
        if value in (None, ""):
            continue
        rows.append({"metric": _humanize_key(_clean_key(key)), "value": _value_label(value), "source": "Pollen"})
    return rows


def _chart_time_label(snapshot: Dict[str, Any]) -> str:
    ts = _as_float(snapshot.get("sample_time")) or _as_float(snapshot.get("received_at"))
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%H:%M")


def _trend_points(history: List[Dict[str, Any]], key: str, *, samples: int = 72) -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    for item in reversed(history[: max(1, int(samples))]):
        row = _reading(item, key)
        if not row:
            continue
        value = _as_float(row.get("value"))
        if value is None:
            continue
        unit = _text(row.get("unit"))
        points.append(
            {
                "label": _chart_time_label(item),
                "value": round(value, 3),
                "display": _value_label(value, unit),
            }
        )
    return points


def _chart_color(token: str) -> str:
    colors = {
        "temperature": "#ef8a4c",
        "humidity": "#5bd6c6",
        "wind": "#79a7ff",
        "rain": "#5aa9e6",
        "lightning": "#f4d35e",
        "solar": "#f6ae2d",
    }
    return colors.get(token, "#5bd6c6")


def _line_chart_data_uri(title: str, points: List[Dict[str, Any]], *, unit: str = "", color: str = "#5bd6c6") -> str:
    width = 800
    height = 280
    left = 58
    right = 28
    top = 36
    bottom = 58
    plot_w = width - left - right
    plot_h = height - top - bottom
    values = [_as_float(point.get("value")) for point in points]
    values = [value for value in values if value is not None]

    def label_value(value: float) -> str:
        return _value_label(value, unit)

    if not values:
        svg = f"""
<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <rect width="{width}" height="{height}" rx="16" fill="#101820"/>
  <text x="{left}" y="52" fill="#edf3f0" font-family="Inter, Arial, sans-serif" font-size="26" font-weight="700">{html_escape(title)}</text>
  <text x="{left}" y="142" fill="#8b9c9f" font-family="Inter, Arial, sans-serif" font-size="20">Waiting for trend data</text>
</svg>
""".strip()
        return "data:image/svg+xml;charset=utf-8," + quote(svg)

    min_v = min(values)
    max_v = max(values)
    if min_v == max_v:
        min_v -= 1
        max_v += 1
    span = max(max_v - min_v, 0.000001)

    def x_for(index: int) -> float:
        if len(points) <= 1:
            return left + plot_w / 2
        return left + (index / (len(points) - 1)) * plot_w

    def y_for(value: float) -> float:
        return top + plot_h - ((value - min_v) / span) * plot_h

    coords: List[Tuple[float, float]] = []
    for index, point in enumerate(points):
        value = _as_float(point.get("value"))
        if value is None:
            continue
        coords.append((x_for(index), y_for(value)))

    if not coords:
        return _line_chart_data_uri(title, [], unit=unit, color=color)

    path = " ".join(f"{'M' if index == 0 else 'L'} {x:.2f} {y:.2f}" for index, (x, y) in enumerate(coords))
    first_x, _first_y = coords[0]
    last_x, last_y = coords[-1]
    area_path = f"{path} L {last_x:.2f} {top + plot_h:.2f} L {first_x:.2f} {top + plot_h:.2f} Z"
    first_label = _text(points[0].get("label"))
    last_label = _text(points[-1].get("label"))
    latest_value = _as_float(points[-1].get("value")) or 0.0
    latest_display = _text(points[-1].get("display")) or label_value(latest_value)
    high_label = label_value(max(values))
    low_label = label_value(min(values))
    mid_y = top + plot_h / 2
    grid_lines = "\n".join(
        f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="#26323d" stroke-width="1"/>'
        for y in (top, mid_y, top + plot_h)
    )
    dots = "\n".join(
        f'<circle cx="{x:.2f}" cy="{y:.2f}" r="2.2" fill="{html_escape(color)}" opacity="0.78"/>'
        for x, y in coords[-12:]
    )

    svg = f"""
<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <defs>
    <linearGradient id="area" x1="0" x2="0" y1="0" y2="1">
      <stop offset="0" stop-color="{html_escape(color)}" stop-opacity="0.30"/>
      <stop offset="1" stop-color="{html_escape(color)}" stop-opacity="0.02"/>
    </linearGradient>
  </defs>
  <rect width="{width}" height="{height}" rx="16" fill="#101820"/>
  <text x="{left}" y="30" fill="#edf3f0" font-family="Inter, Arial, sans-serif" font-size="22" font-weight="700">{html_escape(title)}</text>
  <text x="{width - right}" y="30" text-anchor="end" fill="#edf3f0" font-family="Inter, Arial, sans-serif" font-size="20" font-weight="700">{html_escape(latest_display)}</text>
  {grid_lines}
  <path d="{html_escape(area_path)}" fill="url(#area)"/>
  <path d="{html_escape(path)}" fill="none" stroke="{html_escape(color)}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
  {dots}
  <circle cx="{last_x:.2f}" cy="{last_y:.2f}" r="5.2" fill="#edf3f0" stroke="{html_escape(color)}" stroke-width="3"/>
  <text x="{left}" y="{height - 28}" fill="#8b9c9f" font-family="Inter, Arial, sans-serif" font-size="16">{html_escape(first_label)}</text>
  <text x="{width - right}" y="{height - 28}" text-anchor="end" fill="#8b9c9f" font-family="Inter, Arial, sans-serif" font-size="16">{html_escape(last_label)}</text>
  <text x="{left}" y="{height - 8}" fill="#8b9c9f" font-family="Inter, Arial, sans-serif" font-size="15">Low {html_escape(low_label)}</text>
  <text x="{width - right}" y="{height - 8}" text-anchor="end" fill="#8b9c9f" font-family="Inter, Arial, sans-serif" font-size="15">High {html_escape(high_label)}</text>
</svg>
""".strip()
    return "data:image/svg+xml;charset=utf-8," + quote(svg)


def _trend_image_field(
    history: List[Dict[str, Any]],
    key: str,
    label: str,
    description: str,
    *,
    color_token: str,
    samples: int = 72,
) -> Dict[str, Any]:
    unit = ""
    for item in history:
        row = _reading(item, key)
        if row:
            unit = _text(row.get("unit"))
            break
    points = _trend_points(history, key, samples=samples)
    return {
        "key": f"chart_{_clean_key(key)}",
        "label": label,
        "type": "image",
        "src": _line_chart_data_uri(label, points, unit=unit, color=_chart_color(color_token)),
        "alt": label,
        "caption": f"{len(points)} recent sample{'s' if len(points) != 1 else ''}",
        "description": description,
        "read_only": True,
    }


def _trend_card(*, card_id: str, title: str, detail: str, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "id": card_id,
        "group": "trend",
        "title": title,
        "subtitle": "Recent history",
        "detail": detail,
        "sections": [
            {
                "label": "Graphs",
                "inline": True,
                "fields": fields,
            }
        ],
    }


def _environment_manager_ui(
    snapshot: Dict[str, Any],
    history: List[Dict[str, Any]],
    client: Any = None,
    provider_snapshots: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    settings = _load_settings(client)
    provider_snapshots = provider_snapshots if isinstance(provider_snapshots, dict) else _load_provider_snapshots(client)
    provider_status_rows = _provider_status_rows(settings, provider_snapshots, client)
    selected_sensors = _load_selected_sensors(client)
    candidate_cache = _load_candidate_cache(client)
    candidate_items = candidate_cache.get("items") if isinstance(candidate_cache.get("items"), list) else []
    candidate_options = _candidate_options(candidate_items, selected_sensors)
    candidate_updated_at = candidate_cache.get("updated_at")
    provider_summaries = snapshot.get("providers") if isinstance(snapshot.get("providers"), list) else []
    weather_snapshot = provider_snapshots.get("weather_api") or {}
    weather_enabled = _as_bool(settings.get("weather_api_enabled"), False)
    forecast_provider = _clean_key(settings.get("forecast_provider")) or "weather_api"
    forecast_snapshot = provider_snapshots.get(forecast_provider) or {}
    forecast_enabled = weather_enabled if forecast_provider == "weather_api" else False
    weather_units = _weatherapi_units(client)
    source_count = len(provider_summaries) if provider_summaries else (1 if snapshot else 0)
    grouped = _readings_by_category(snapshot)
    battery_rows = grouped.get("battery") or []
    low_battery_count = sum(1 for row in battery_rows if _text(row.get("tone")) == "danger")
    battery_summary = (
        "No readings"
        if not battery_rows
        else f"{low_battery_count} low"
        if low_battery_count
        else f"{len(battery_rows)} OK"
    )
    has_snapshot = bool(snapshot)
    received_at = snapshot.get("received_at") if has_snapshot else 0
    stale_after_s = int(settings.get("stale_after_minutes") or DEFAULT_STALE_AFTER_MINUTES) * 60
    is_stale = bool(received_at and (time.time() - float(received_at)) > stale_after_s)
    webhook_path = "/api/cores/environment_core/webhook/ecowitt"
    current_condition_rows = _reading_rows_by_keys(
        snapshot,
        (
            "weather_api_condition",
            "tempf",
            "weather_api_feelslike",
            "humidity",
            "tempinf",
            "humidityin",
            "windspeedmph",
            "windgustmph",
            "dailyrainin",
            "uv",
            "weather_api_aqi_us_epa",
        ),
    )
    weather_rows = _reading_rows_by_keys(
        snapshot,
        (
            "weather_api_condition",
            "tempf",
            "weather_api_feelslike",
            "humidity",
            "baromrelin",
            "weather_api_cloud",
            "weather_api_visibility",
        ),
    )
    area_rows = _area_overview_rows(snapshot)
    room_rows = []
    for row in _reading_rows_by_categories(snapshot, ("temperature", "humidity"), limit=32, exclude_providers=("weather_api",)):
        key = _clean_key(row.get("key"))
        area = _clean_key(row.get("area"))
        if key in {"tempf", "humidity"} and area in {"outside", "outdoor"}:
            continue
        room_rows.append(row)
    movement_rows = _reading_rows_by_categories(snapshot, ("wind", "rain", "lightning"), limit=18)
    air_light_rows = _reading_rows_by_categories(snapshot, ("solar", "air"), limit=18)
    source_health_rows = _source_health_rows(provider_status_rows)
    source_reading_points = _source_reading_points(provider_status_rows)
    current_condition_snapshot = _display_snapshot_for_source(
        settings.get("current_condition_source"),
        snapshot,
        provider_snapshots,
        selected_sensors,
    )
    live_station_snapshot = _display_snapshot_for_source(
        settings.get("current_live_source"),
        snapshot,
        provider_snapshots,
        selected_sensors,
    )
    current_condition_fields = (
        [
            {
                "key": "environment_current_condition_card",
                "label": "Current Conditions",
                "type": "image",
                "src": _weather_current_card_data_uri(current_condition_snapshot, live_snapshot=live_station_snapshot, units=weather_units),
                "alt": "Current environment conditions",
                "hide_label": True,
                "read_only": True,
            }
        ]
        if current_condition_snapshot or live_station_snapshot
        else []
    )
    current_condition_sections = (
        [
            {
                "label": "Current Conditions",
                "inline": True,
                "fields": current_condition_fields,
            }
        ]
        if current_condition_fields
        else []
    )
    overview_summary_rows = [
        {"label": "Outdoor", "value": _reading_display(snapshot, "tempf", _category_display(snapshot, "temperature"))},
        {"label": "Feels Like", "value": _reading_display(snapshot, "weather_api_feelslike", _reading_display(snapshot, "feelslikef"))},
        {"label": "Indoor", "value": _reading_display(snapshot, "tempinf")},
        {"label": "Humidity", "value": _reading_display(snapshot, "humidity", _category_display(snapshot, "humidity"))},
        {"label": "Wind", "value": _reading_display(snapshot, "windspeedmph")},
        {"label": "Rain Today", "value": _reading_display(snapshot, "dailyrainin")},
        {"label": "Forecast", "value": _reading_display(forecast_snapshot, "weather_api_condition")},
        {"label": "Batteries", "value": battery_summary},
    ]
    overview_badges = [
        {"label": "Live" if has_snapshot and not is_stale else "Stale" if has_snapshot else "Waiting", "tone": "good" if has_snapshot and not is_stale else "warning"},
        {"label": f"{source_count} source{'s' if source_count != 1 else ''}" if source_count else "No sources", "tone": "muted"},
    ]
    if battery_rows:
        overview_badges.append({"label": "Low Battery" if low_battery_count else "Batteries OK", "tone": "danger" if low_battery_count else "good"})
    current_condition_card = (
        {
            "id": "overview:current_conditions",
            "group": "overview",
            "title": "Current Conditions",
            "subtitle": _reading_display(current_condition_snapshot, "weather_api_condition", "Live weather"),
            "sections": current_condition_sections,
        }
        if current_condition_sections
        else None
    )
    forecast_visual_card = {
        "id": f"forecast:{forecast_provider}:cards",
        "group": "forecast",
        "title": "Daily Forecast",
        "subtitle": _provider_label(forecast_provider),
        "sections": [
            {
                "label": "Daily Cards",
                "inline": True,
                "fields": [
                    {
                        "key": "weatherapi_daily_cards",
                        "label": "Daily Forecast Cards",
                        "type": "image",
                        "src": _weather_forecast_cards_data_uri(forecast_snapshot, units=weather_units),
                        "alt": "Daily forecast cards",
                        "hide_label": True,
                        "read_only": True,
                    }
                ],
            }
        ],
    }

    overview_cards: List[Dict[str, Any]] = []
    if area_rows:
        overview_cards.append(
            _overview_card(
                card_id="overview:areas",
                title="Areas",
                subtitle=f"{len(area_rows)} area{'s' if len(area_rows) != 1 else ''}",
                detail="Current temperature, humidity, and condition readings grouped by area.",
                sensor_title="",
                sensor_rows=[],
                sections=[
                    {
                        "label": "Area Readings",
                        "inline": True,
                        "fields": [
                            {
                                "key": "environment_area_overview",
                                "label": "Area Overview",
                                "type": "table",
                                "columns": [
                                    {"key": "area", "label": "Area"},
                                    {"key": "temperature", "label": "Temp"},
                                    {"key": "humidity", "label": "Humidity"},
                                    {"key": "condition", "label": "Condition"},
                                    {"key": "source", "label": "Source"},
                                ],
                                "rows": area_rows,
                                "read_only": True,
                            }
                        ],
                    }
                ],
            )
        )
    if weather_rows:
        overview_cards.append(
            _overview_card(
                card_id="overview:weather",
                title="Weather Now",
                subtitle="Outdoor station and forecast",
                detail="The outside-facing readings Tater can use for weather-aware automations.",
                sensor_title="Outside",
                sensor_rows=weather_rows,
                badges=[{"label": _reading_display(weather_snapshot, "weather_api_condition", "Current"), "tone": "muted"}],
                summary_rows=[
                    {"label": "Temp", "value": _reading_display(snapshot, "tempf", _category_display(snapshot, "temperature"))},
                    {"label": "Humidity", "value": _reading_display(snapshot, "humidity")},
                    {"label": "Pressure", "value": _reading_display(snapshot, "baromrelin")},
                ],
            )
        )
    if room_rows:
        overview_cards.append(
            _overview_card(
                card_id="overview:rooms",
                title="Rooms & Sensors",
                subtitle=f"{len(room_rows)} reading{'s' if len(room_rows) != 1 else ''}",
                detail="Indoor, room, and selected integration sensors in one scan-friendly card.",
                sensor_title="Selected Sensors",
                sensor_rows=room_rows,
                summary_rows=[
                    {"label": "Indoor", "value": _reading_display(snapshot, "tempinf", _category_display(snapshot, "temperature"))},
                    {"label": "Indoor Humidity", "value": _reading_display(snapshot, "humidityin")},
                ],
            )
        )
    if movement_rows:
        overview_cards.append(
            _overview_card(
                card_id="overview:wind_rain",
                title="Wind, Rain & Lightning",
                subtitle="Weather movement",
                detail="Fast-changing outdoor readings that are useful for alerts and context.",
                sensor_title="Movement",
                sensor_rows=movement_rows,
                summary_rows=[
                    {"label": "Wind", "value": _reading_display(snapshot, "windspeedmph")},
                    {"label": "Gust", "value": _reading_display(snapshot, "windgustmph")},
                    {"label": "Rain Today", "value": _reading_display(snapshot, "dailyrainin")},
                    {"label": "Lightning", "value": _reading_display(snapshot, "lightning_num", _reading_display(snapshot, "lightning"))},
                ],
            )
        )
    if air_light_rows:
        overview_cards.append(
            _overview_card(
                card_id="overview:air_light",
                title="Air, UV & Light",
                subtitle="Exposure and air quality",
                detail="Solar, UV, and air quality readings from the station and enabled services.",
                sensor_title="Air and Light",
                sensor_rows=air_light_rows,
                summary_rows=[
                    {"label": "UV", "value": _reading_display(snapshot, "uv")},
                    {"label": "Solar", "value": _reading_display(snapshot, "solarradiation")},
                    {"label": "AQI", "value": _reading_display(snapshot, "weather_api_aqi_us_epa")},
                ],
            )
        )
    if battery_rows:
        overview_cards.append(
            _overview_card(
                card_id="overview:batteries",
                title="Sensor Batteries",
                subtitle=f"{len(battery_rows)} battery reading{'s' if len(battery_rows) != 1 else ''}",
                detail="Battery fields are decoded where known; raw values are shown otherwise.",
                sensor_title="Batteries",
                sensor_rows=battery_rows,
                badges=[
                    {
                        "label": "Low Battery" if low_battery_count else "OK",
                        "tone": "danger" if low_battery_count else "good",
                    }
                ],
            )
        )
    overview_cards.append(
        {
            "id": "overview:sources",
            "group": "overview",
            "title": "Source Health",
            "subtitle": f"{source_count} active source{'s' if source_count != 1 else ''}" if source_count else "Waiting for sources",
            "detail": "A quick check of which integrations are feeding Environment Core.",
            "summary_rows": [
                {"label": "Active", "value": str(source_count)},
                {"label": "Selected", "value": str(sum(1 for row in selected_sensors if _as_bool(row.get("enabled"), True)))},
                {"label": "Last Sample", "value": _age_label(received_at) if has_snapshot else "never"},
            ],
            "sections": [
                {
                    "label": "Readings by Source",
                    "inline": True,
                    "fields": [
                        {
                            "key": "environment_source_reading_counts",
                            "label": "Readings by Source",
                            "type": "bar_chart",
                            "points": source_reading_points,
                            "read_only": True,
                        },
                        {
                            "key": "environment_source_health",
                            "label": "Source Health",
                            "type": "table",
                            "columns": [
                                {"key": "source", "label": "Source"},
                                {"key": "state", "label": "State"},
                                {"key": "selected", "label": "Selected"},
                                {"key": "last_sample", "label": "Last Sample"},
                                {"key": "readings", "label": "Readings"},
                            ],
                            "rows": source_health_rows,
                            "read_only": True,
                        },
                    ],
                }
            ],
        }
    )

    item_forms: List[Dict[str, Any]] = ([current_condition_card] if current_condition_card else []) + [
        {
            "id": "overview",
            "group": "overview",
            "title": "Live Environment",
            "subtitle": f"{source_count} active source{'s' if source_count != 1 else ''}" if source_count else "Waiting for telemetry",
            "detail": (
                f"Last sample {_age_label(received_at)}."
                if has_snapshot
                else f"Point Ecowitt custom server uploads at {webhook_path}."
            ),
            "hero_badges": overview_badges,
            "summary_rows": overview_summary_rows,
            "sensor_title": "At a Glance",
            "sensor_rows": _sensor_rows(current_condition_rows, include_meta=False),
        },
        forecast_visual_card,
        {
            "id": f"forecast:{forecast_provider}",
            "group": "forecast",
            "title": f"{_provider_label(forecast_provider)} Forecast",
            "subtitle": (
                f"Last forecast {_age_label(forecast_snapshot.get('received_at'))}"
                if forecast_snapshot
                else "Enabled, waiting for forecast" if forecast_enabled else "Disabled"
            ),
            "detail": (
                _reading_display(forecast_snapshot, "weather_api_condition")
                if forecast_snapshot
                else f"Enable {_provider_label(forecast_provider)} in Environment Core settings and configure it in Settings > Integrations."
            ),
            "hero_badges": [
                {"label": "Enabled" if forecast_enabled else "Disabled", "tone": "good" if forecast_enabled else "muted"},
                {
                    "label": _text(forecast_snapshot.get("model")) or _provider_label(forecast_provider),
                    "tone": "muted",
                },
            ],
            "summary_rows": [
                {"label": "Current", "value": _reading_display(forecast_snapshot, "tempf", _reading_display(forecast_snapshot, "tempc"))},
                {"label": "Feels Like", "value": _reading_display(forecast_snapshot, "weather_api_feelslike")},
                {"label": "Humidity", "value": _reading_display(forecast_snapshot, "humidity")},
                {"label": "Wind", "value": _reading_display(forecast_snapshot, "windspeedmph", _reading_display(forecast_snapshot, "weather_api_wind_kph"))},
                {"label": "UV", "value": _reading_display(forecast_snapshot, "uv")},
                {"label": "AQI", "value": _reading_display(forecast_snapshot, "weather_api_aqi_us_epa")},
            ],
            "sections": [
                {
                    "label": "Daily Forecast",
                    "inline": True,
                    "fields": [
                        {
                            "key": "weatherapi_daily_forecast",
                            "label": "Daily Forecast",
                            "type": "table",
                            "columns": [
                                {"key": "date", "label": "Date"},
                                {"key": "condition", "label": "Condition"},
                                {"key": "high", "label": "High"},
                                {"key": "low", "label": "Low"},
                                {"key": "rain", "label": "Rain"},
                                {"key": "snow", "label": "Snow"},
                                {"key": "precip", "label": "Precip"},
                                {"key": "wind", "label": "Wind"},
                                {"key": "uv", "label": "UV"},
                                {"key": "sunrise", "label": "Sunrise"},
                                {"key": "sunset", "label": "Sunset"},
                            ],
                            "rows": _weatherapi_daily_rows(forecast_snapshot, units=weather_units),
                            "read_only": True,
                        }
                    ],
                },
                {
                    "label": "Next Hours",
                    "inline": True,
                    "fields": [
                        {
                            "key": "weatherapi_hourly_forecast",
                            "label": "Hourly Forecast",
                            "type": "table",
                            "columns": [
                                {"key": "time", "label": "Time"},
                                {"key": "temp", "label": "Temp"},
                                {"key": "condition", "label": "Condition"},
                                {"key": "rain", "label": "Rain"},
                                {"key": "snow", "label": "Snow"},
                                {"key": "wind", "label": "Wind"},
                                {"key": "humidity", "label": "Humidity"},
                                {"key": "uv", "label": "UV"},
                            ],
                            "rows": _weatherapi_hourly_rows(forecast_snapshot, units=weather_units),
                            "read_only": True,
                        }
                    ],
                },
                {
                    "label": "Air, Pollen, Alerts",
                    "inline": True,
                    "fields": [
                        {
                            "key": "weatherapi_air_quality",
                            "label": "Air and Pollen",
                            "type": "table",
                            "columns": [
                                {"key": "source", "label": "Source"},
                                {"key": "metric", "label": "Metric"},
                                {"key": "value", "label": "Value"},
                            ],
                            "rows": _weatherapi_air_rows(forecast_snapshot),
                            "read_only": True,
                        },
                        {
                            "key": "weatherapi_alerts",
                            "label": "Alerts",
                            "type": "table",
                            "columns": [
                                {"key": "event", "label": "Event"},
                                {"key": "severity", "label": "Severity"},
                                {"key": "areas", "label": "Areas"},
                                {"key": "effective", "label": "Effective"},
                                {"key": "expires", "label": "Expires"},
                            ],
                            "rows": _weatherapi_alert_rows(forecast_snapshot),
                            "read_only": True,
                        },
                    ],
                },
            ],
            "run_action": "environment_poll_integrations",
            "run_label": "Poll Forecast",
        },
        {
            "id": "settings:sources",
            "group": "settings",
            "title": "Environment Sources",
            "subtitle": "Core settings",
            "detail": "Configured integrations contribute readings after their sensors are added to Environment Core.",
            "sections": [
                {
                    "label": "Status",
                    "inline": True,
                    "fields": [
                        {
                            "key": "environment_source_status",
                            "label": "Sources",
                            "type": "table",
                            "columns": [
                                {"key": "source", "label": "Source"},
                                {"key": "state", "label": "State"},
                                {"key": "setup", "label": "Setup"},
                                {"key": "selected", "label": "Selected"},
                                {"key": "last_sample", "label": "Last Sample"},
                                {"key": "readings", "label": "Readings"},
                            ],
                            "rows": provider_status_rows,
                            "read_only": True,
                        }
                    ],
                },
                {
                    "label": "Settings",
                    "inline": True,
                    "fields": _settings_field_rows(
                        settings,
                        provider_snapshots=provider_snapshots,
                        selected_sensors=selected_sensors,
                    ),
                },
            ],
            "save_action": "environment_save_settings",
            "save_label": "Save Sources",
            "run_action": "environment_poll_integrations",
            "run_label": "Poll Now",
        },
        {
            "id": "settings:discovery",
            "group": "settings",
            "title": "Sensor Discovery",
            "subtitle": f"{len(candidate_items)} candidate{'s' if len(candidate_items) != 1 else ''}",
            "detail": (
                f"Last discovery {_age_label(candidate_updated_at)}."
                if candidate_updated_at
                else "Run discovery after configuring integrations, then add the sensors you want in the Sources tab."
            ),
            "sections": [
                {
                    "label": "Candidates",
                    "inline": True,
                    "fields": [
                        {
                            "key": "environment_candidates",
                            "label": "Discovered Sensors",
                            "type": "table",
                            "columns": [
                                {"key": "source", "label": "Source"},
                                {"key": "sensor", "label": "Sensor"},
                                {"key": "measurement", "label": "Measurement"},
                                {"key": "current", "label": "Current"},
                                {"key": "area", "label": "Area"},
                                {"key": "selected", "label": "Selected"},
                            ],
                            "rows": _candidate_table_rows(candidate_items, selected_sensors),
                            "read_only": True,
                        }
                    ],
                }
            ],
            "run_action": "environment_discover_sensors",
            "run_label": "Discover Sensors",
        },
        {
            "id": "setup",
            "group": "source",
            "title": "Ecowitt Upload Target",
            "subtitle": "Custom Server receiver",
            "detail": "Configure WS View Plus or the Ecowitt web UI to use Ecowitt protocol and this Tater path.",
            "sections": [
                {
                    "label": "Receiver",
                    "inline": True,
                    "fields": [
                        {"key": "path", "label": "Path", "type": "text", "value": webhook_path, "read_only": True},
                        {"key": "method", "label": "Method", "type": "text", "value": "POST", "read_only": True},
                        {"key": "protocol", "label": "Protocol", "type": "text", "value": "Ecowitt", "read_only": True},
                    ],
                }
            ],
            "run_action": "environment_clear_history",
            "run_label": "Clear History",
            "run_confirm": "Clear Environment Core weather history?",
        },
    ]
    overview_insert_index = 2 if current_condition_card else 1
    item_forms[overview_insert_index:overview_insert_index] = overview_cards

    for selection in selected_sensors:
        key = _text(selection.get("key"))
        provider = _clean_key(selection.get("provider"))
        label = _text(selection.get("label")) or key
        area = _text(selection.get("area")) or "Unassigned"
        enabled = _as_bool(selection.get("enabled"), True)
        item_forms.append(
            {
                "id": f"source:{key}",
                "group": "source",
                "title": label,
                "subtitle": _provider_label(provider),
                "detail": f"{area} - {CATEGORY_LABELS.get(_clean_key(selection.get('category')), _humanize_key(selection.get('category')) or 'Sensor')}",
                "hero_badges": [
                    {"label": "Enabled" if enabled else "Disabled", "tone": "good" if enabled else "muted"},
                    {"label": area, "tone": "muted"},
                ],
                "summary_rows": [
                    {"label": "Provider", "value": _provider_label(provider)},
                    {"label": "Sensor", "value": _text(selection.get("sensor_id")) or key},
                    {"label": "Area", "value": area},
                    {"label": "Type", "value": CATEGORY_LABELS.get(_clean_key(selection.get("category")), _humanize_key(selection.get("category")) or "Sensor")},
                ],
                "fields": [
                    {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": enabled},
                    {"key": "label", "label": "Display Name", "type": "text", "value": label},
                    {"key": "area", "label": "Area", "type": "text", "value": _text(selection.get("area")), "placeholder": "Office, Living Room, Outside"},
                ],
                "save_action": "environment_save_sensor_source",
                "save_label": "Save Source",
                "remove_action": "environment_remove_sensor_source",
                "remove_label": "Remove Source",
                "remove_confirm": f"Remove {label} from Environment Core?",
            }
        )

    item_forms.extend(
        [
            _trend_card(
                card_id="trend:temperature",
                title="Temperature Graphs",
                detail="Outdoor and indoor temperature over recent Ecowitt uploads.",
                fields=[
                    _trend_image_field(history, "tempf", "Outdoor Temperature", "Recent outdoor temperature trend.", color_token="temperature"),
                    _trend_image_field(history, "tempinf", "Indoor Temperature", "Recent indoor temperature trend.", color_token="temperature"),
                ],
            ),
            _trend_card(
                card_id="trend:wind",
                title="Wind Graphs",
                detail="Sustained wind, gusts, and daily maximum gust over recent uploads.",
                fields=[
                    _trend_image_field(history, "windspeedmph", "Wind Speed", "Recent sustained wind speed.", color_token="wind"),
                    _trend_image_field(history, "windgustmph", "Wind Gust", "Recent wind gust readings.", color_token="wind"),
                    _trend_image_field(history, "maxdailygust", "Max Daily Gust", "Daily maximum gust reported by Ecowitt.", color_token="wind"),
                ],
            ),
            _trend_card(
                card_id="trend:rain",
                title="Rain Graphs",
                detail="Rain rate and daily accumulation over recent uploads.",
                fields=[
                    _trend_image_field(history, "rainratein", "Rain Rate", "Recent rain rate trend.", color_token="rain"),
                    _trend_image_field(history, "dailyrainin", "Rain Today", "Daily rain accumulation trend.", color_token="rain"),
                ],
            ),
            _trend_card(
                card_id="trend:lightning",
                title="Lightning Graphs",
                detail="Lightning strike count and distance when supported by the station.",
                fields=[
                    _trend_image_field(history, "lightning_num", "Lightning Strikes", "Recent reported lightning strike count.", color_token="lightning"),
                    _trend_image_field(history, "lightning", "Lightning Distance", "Recent distance to lightning activity.", color_token="lightning"),
                ],
            ),
        ]
    )

    return {
        "kind": "settings_manager",
        "title": "Environment Core",
        "stats_refresh_button": True,
        "stats_refresh_label": "Refresh",
        "empty_message": "No environment telemetry has been received yet.",
        "manager_tabs": [
            {"key": "overview", "label": "Overview", "source": "items", "item_group": "overview"},
            {"key": "forecast", "label": "Forecast", "source": "items", "item_group": "forecast"},
            {"key": "trends", "label": "Graphs", "source": "items", "item_group": "trend"},
            {"key": "add_source", "label": "Add Sensor", "source": "add_form"},
            {"key": "sources", "label": "Sources", "source": "items", "item_group": "source", "selector": True},
            {"key": "settings", "label": "Settings", "source": "items", "item_group": "settings"},
        ],
        "default_tab": "overview",
        "add_form": {
            "action": "environment_add_sensor_source",
            "submit_label": "Add Sensor",
            "fields": [
                {
                    "key": "sensor_key",
                    "label": "Sensor",
                    "type": "select",
                    "options": candidate_options or [{"value": "", "label": "Run Discover Sensors first"}],
                    "description": "Choose one discovered integration sensor to add to Environment Core.",
                },
                {"key": "area", "label": "Area", "type": "text", "placeholder": "Office, Living Room, Outside, Back Porch"},
                {"key": "label", "label": "Display Name", "type": "text", "placeholder": "Optional name override"},
                {"key": "enabled", "label": "Enabled", "type": "checkbox", "value": True},
            ],
        },
        "item_forms": item_forms,
    }


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    provider_snapshots = _load_provider_snapshots(client)
    snapshot = _combined_snapshot(provider_snapshots)
    weather_snapshot = provider_snapshots.get("weather_api") or {}
    history = _load_history(limit=96, client=client)
    settings = _load_settings(client)
    received_at = snapshot.get("received_at") if snapshot else 0
    stale_after_s = int(settings.get("stale_after_minutes") or DEFAULT_STALE_AFTER_MINUTES) * 60
    stale = bool(received_at and (time.time() - float(received_at)) > stale_after_s)
    provider_labels = [
        _provider_label(provider)
        for provider in LATEST_PROVIDER_KEYS
        if provider_snapshots.get(provider)
    ]

    return {
        "summary": "Local environment telemetry from Ecowitt and enabled integration sensors.",
        "stats": [
            {"label": "Sources", "value": ", ".join(provider_labels) if provider_labels else "Waiting"},
            {"label": "Last Sample", "value": _age_label(received_at)},
            {"label": "Outdoor Temp", "value": _reading_display(snapshot, "tempf", _category_display(snapshot, "temperature"))},
            {"label": "Humidity", "value": _reading_display(snapshot, "humidity", _category_display(snapshot, "humidity"))},
            {"label": "Wind", "value": _reading_display(snapshot, "windspeedmph")},
            {"label": "Forecast", "value": _reading_display(weather_snapshot, "weather_api_condition")},
            {"label": "Rain Today", "value": _reading_display(snapshot, "dailyrainin")},
            {"label": "Status", "value": "Stale" if stale else "Live" if snapshot else "Waiting"},
        ],
        "items": [],
        "empty_message": "No environment telemetry has reached Tater yet.",
        "ui": _environment_manager_ui(snapshot, history, client, provider_snapshots),
    }


def _payload_from_webhook(payload: Dict[str, Any], body: str = "") -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, list):
                values[key] = value[-1] if value else ""
            else:
                values[key] = value
    if not values and body:
        values.update({key: value for key, value in parse_qsl(body, keep_blank_values=True)})
    return values


def _action_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    if not isinstance(payload, dict):
        return values
    for key, value in payload.items():
        if key not in {"values", "fields"}:
            values[key] = value
    for nested_key in ("fields", "values"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            values.update(nested)
    return values


def _save_environment_settings(payload: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    values = _action_values(payload)
    text_keys = (
        "ENVIRONMENT_ECOWITT_OUTDOOR_AREA",
        "ENVIRONMENT_ECOWITT_INDOOR_AREA",
        "ENVIRONMENT_CURRENT_CONDITION_LIVE_SOURCE",
        "ENVIRONMENT_CURRENT_CONDITION_CONDITION_SOURCE",
        "ENVIRONMENT_FORECAST_PROVIDER",
    )
    int_keys = {
        "ENVIRONMENT_INTEGRATION_POLL_SECONDS": (DEFAULT_INTEGRATION_POLL_SECONDS, 30, 86400),
        "ENVIRONMENT_STALE_AFTER_MINUTES": (DEFAULT_STALE_AFTER_MINUTES, 1, 10080),
        "ENVIRONMENT_HISTORY_LIMIT": (DEFAULT_HISTORY_LIMIT, 1, 10000),
    }
    bool_keys = ("ENVIRONMENT_ENABLE_WEATHERAPI",)
    mapping: Dict[str, str] = {}
    for key in text_keys:
        if key in values:
            mapping[key] = _text(values.get(key))
    for key, (default, minimum, maximum) in int_keys.items():
        if key in values:
            mapping[key] = str(_as_int(values.get(key), default, minimum=minimum, maximum=maximum))
    for key in bool_keys:
        if key in values:
            mapping[key] = "true" if _as_bool(values.get(key), False) else "false"
    if not mapping:
        return {"ok": True, "message": "No Environment Core settings changed.", "changed": []}
    (client or redis_client).hset(SETTINGS_KEY, mapping=mapping)
    return {
        "ok": True,
        "message": "Environment source settings saved.",
        "changed": sorted(mapping.keys()),
        "settings": _load_settings(client),
    }


def _candidate_by_key(sensor_key: Any, client: Any = None) -> Dict[str, Any]:
    wanted = _text(sensor_key)
    if not wanted:
        return {}
    cache = _load_candidate_cache(client)
    for row in cache.get("items") or []:
        if isinstance(row, dict) and _text(row.get("key")) == wanted:
            return dict(row)
    return {}


def _item_sensor_key(payload: Dict[str, Any]) -> str:
    values = _action_values(payload)
    raw = _text(values.get("key") or values.get("sensor_key") or (payload or {}).get("id"))
    if raw.startswith("source:"):
        return raw.split(":", 1)[1]
    return raw


def _add_sensor_source(payload: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    values = _action_values(payload)
    sensor_key = _text(values.get("sensor_key"))
    candidate = _candidate_by_key(sensor_key, client)
    if not candidate:
        raise ValueError("Run Discover Sensors first, then choose a discovered sensor.")
    selected = _load_selected_sensors(client)
    if any(_text(row.get("key")) == sensor_key for row in selected):
        raise ValueError("That sensor is already selected.")
    row = _normalize_selection(
        {
            **candidate,
            "label": _text(values.get("label")) or _text(candidate.get("label")),
            "area": _text(values.get("area")) or _text(candidate.get("area")),
            "enabled": _as_bool(values.get("enabled"), True),
        }
    )
    selected.append(row)
    _save_selected_sensors(selected, client)
    return {"ok": True, "message": f"Added {row.get('label')} to Environment Core.", "source": row}


def _save_sensor_source(payload: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    values = _action_values(payload)
    sensor_key = _item_sensor_key(payload)
    if not sensor_key:
        raise ValueError("Sensor source key is required.")
    selected = _load_selected_sensors(client)
    changed = False
    for row in selected:
        if _text(row.get("key")) != sensor_key:
            continue
        if "label" in values:
            row["label"] = _text(values.get("label")) or row.get("label")
        if "area" in values:
            row["area"] = _text(values.get("area"))
        if "enabled" in values:
            row["enabled"] = _as_bool(values.get("enabled"), True)
        changed = True
        break
    if not changed:
        raise ValueError("Selected sensor source was not found.")
    _save_selected_sensors(selected, client)
    return {"ok": True, "message": "Environment sensor source saved."}


def _remove_sensor_source(payload: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    sensor_key = _item_sensor_key(payload)
    if not sensor_key:
        raise ValueError("Sensor source key is required.")
    selected = _load_selected_sensors(client)
    next_rows = [row for row in selected if _text(row.get("key")) != sensor_key]
    if len(next_rows) == len(selected):
        raise ValueError("Selected sensor source was not found.")
    _save_selected_sensors(next_rows, client)
    return {"ok": True, "message": "Environment sensor source removed."}


def handle_core_webhook(
    *,
    webhook: str,
    payload: Dict[str, Any],
    query: Optional[Dict[str, Any]] = None,
    body: str = "",
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    hook = _clean_key(webhook)
    if hook != "ecowitt":
        raise KeyError(f"Unsupported Environment Core webhook: {webhook}")

    settings = _load_settings(client)
    incoming = _payload_from_webhook(payload, body=body)
    if query:
        for key, value in query.items():
            incoming.setdefault(key, value)
    if not incoming:
        raise ValueError("Ecowitt upload did not include any readings.")

    configured_passkey = _text(settings.get("ecowitt_passkey"))
    incoming_passkey = _text(incoming.get("PASSKEY") or incoming.get("passkey"))
    if configured_passkey and incoming_passkey != configured_passkey:
        raise ValueError("Ecowitt PASSKEY did not match Environment Core settings.")

    snapshot = _apply_ecowitt_areas(_normalize_ecowitt_payload(incoming), settings)
    _store_snapshot(snapshot, client)
    logger.info("[Environment] Ecowitt upload stored from %s with %d readings.", snapshot.get("source_id"), len(snapshot.get("readings") or []))
    return {
        "ok": True,
        "message": "OK",
        "provider": "ecowitt",
        "reading_count": len(snapshot.get("readings") or []),
        "source_id": snapshot.get("source_id"),
    }


def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    action_name = _clean_key(action)
    if action_name == "environment_clear_history":
        client.delete(HISTORY_KEY)
        return {"ok": True, "message": "Environment history cleared."}
    if action_name == "environment_save_settings":
        return _save_environment_settings(payload, client)
    if action_name == "environment_discover_sensors":
        result = _discover_environment_sensor_candidates(client)
        return {
            "ok": not bool(result.get("errors")),
            "message": _text(result.get("message")) or "Sensor discovery complete.",
            "candidate_count": len(result.get("items") or []),
            "errors": result.get("errors") or [],
        }
    if action_name == "environment_add_sensor_source":
        return _add_sensor_source(payload, client)
    if action_name == "environment_save_sensor_source":
        return _save_sensor_source(payload, client)
    if action_name == "environment_remove_sensor_source":
        return _remove_sensor_source(payload, client)
    if action_name == "environment_poll_integrations":
        values = _action_values(payload)
        if values:
            _save_environment_settings(payload, client)
        return _poll_enabled_integrations(client)
    raise KeyError(f"Unsupported Environment Core action: {action}")


def _hydra_limit(value: Any, default: int = 16) -> int:
    return _as_int(value, default, minimum=1, maximum=80)


def _hydra_reading_matches(row: Dict[str, Any], args: Dict[str, Any]) -> bool:
    area = _clean_key(args.get("area") or args.get("room") or args.get("location"))
    category = _clean_key(args.get("category") or args.get("type") or args.get("measurement"))
    provider = _clean_key(args.get("provider") or args.get("source") or args.get("integration"))
    text = _clean_key(args.get("sensor") or args.get("query"))
    if area and area not in _clean_key(row.get("area")) and area not in _clean_key(row.get("source_name")):
        return False
    if category == "weather":
        if _clean_key(row.get("category")) not in HYDRA_WEATHER_CATEGORIES:
            return False
    elif category and category not in {_clean_key(row.get("category")), _clean_key(row.get("label")), _clean_key(row.get("key"))}:
        haystack = f"{_clean_key(row.get('category'))} {_clean_key(row.get('label'))} {_clean_key(row.get('key'))}"
        if category not in haystack:
            return False
    if provider and provider not in {_clean_key(row.get("provider")), _clean_key(row.get("provider_label"))}:
        return False
    if text:
        haystack = " ".join(
            _clean_key(part)
            for part in (
                row.get("label"),
                row.get("key"),
                row.get("area"),
                row.get("source_name"),
                row.get("provider_label"),
                row.get("category"),
            )
        )
        if text not in haystack:
            return False
    return True


def _hydra_reading_row(row: Dict[str, Any]) -> Dict[str, str]:
    return {
        "label": _text(row.get("label")),
        "value": _text(row.get("display")) or _value_label(row.get("value"), _text(row.get("unit"))),
        "area": _text(row.get("area")),
        "category": CATEGORY_LABELS.get(_clean_key(row.get("category")), _humanize_key(_clean_key(row.get("category")))),
        "provider": _text(row.get("provider_label")) or _provider_label(row.get("provider")),
        "source": _text(row.get("source_name")) or _text(row.get("source_id")),
    }


def _hydra_relevant_readings(snapshot: Dict[str, Any], args: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = [row for row in snapshot.get("readings") or [] if isinstance(row, dict) and _hydra_reading_matches(row, args)]
    preferred_order = {
        "condition": 0,
        "temperature": 1,
        "humidity": 2,
        "wind": 3,
        "rain": 4,
        "pressure": 5,
        "solar": 6,
        "air": 7,
        "lightning": 8,
        "battery": 20,
    }
    rows.sort(
        key=lambda row: (
            preferred_order.get(_clean_key(row.get("category")), 12),
            _text(row.get("area")).casefold(),
            _text(row.get("provider_label")).casefold(),
            _text(row.get("label")).casefold(),
        )
    )
    return rows[: _hydra_limit(args.get("limit"), 16)]


def _hydra_summary_from_readings(readings: List[Dict[str, str]], *, prefix: str) -> str:
    if not readings:
        return f"{prefix} I do not have matching Environment Core readings yet."
    pieces: List[str] = []
    for row in readings[:10]:
        label = _text(row.get("label"))
        value = _text(row.get("value"))
        area = _text(row.get("area"))
        if not label or not value:
            continue
        area_part = f"{area} " if area and area.lower() not in label.lower() else ""
        pieces.append(f"{area_part}{label}: {value}")
    if not pieces:
        return f"{prefix} I found matching readings, but none had display values yet."
    return f"{prefix} " + "; ".join(pieces) + "."


def _hydra_request_text(args: Dict[str, Any], origin: Optional[Dict[str, Any]] = None) -> str:
    payload = args if isinstance(args, dict) else {}
    for key in ("request", "question", "user_query", "prompt", "text", "content", "message", "query"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    arg_origin = payload.get("origin")
    if isinstance(arg_origin, dict):
        for key in ("request_text", "raw_message", "query", "question", "text", "content", "message", "body"):
            value = arg_origin.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    if isinstance(origin, dict):
        for key in ("request_text", "raw_message", "user_text", "query", "question", "text", "content", "message", "body"):
            value = origin.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _hydra_filter_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    readings = [row for row in snapshot.get("readings") or [] if isinstance(row, dict)]
    areas = sorted({_text(row.get("area")) for row in readings if _text(row.get("area"))}, key=str.casefold)
    categories = sorted({_clean_key(row.get("category")) for row in readings if _clean_key(row.get("category"))})
    providers = sorted(
        {
            _clean_key(row.get("provider")) or _clean_key(row.get("provider_label"))
            for row in readings
            if _clean_key(row.get("provider")) or _clean_key(row.get("provider_label"))
        }
    )
    sensor_labels = sorted({_text(row.get("label")) for row in readings if _text(row.get("label"))}, key=str.casefold)
    return {
        "areas": areas[:40],
        "categories": categories[:40],
        "providers": providers[:20],
        "sensor_labels": sensor_labels[:80],
    }


def _hydra_normalized_filter_args(raw: Dict[str, Any], original: Dict[str, Any]) -> Dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    out: Dict[str, Any] = {}
    for key in ("area", "room", "location", "category", "provider", "source", "integration", "sensor", "query", "date", "day"):
        value = source.get(key)
        if value is None:
            continue
        text = _text(value)
        if text:
            out[key] = text
    limit = source.get("limit", original.get("limit") if isinstance(original, dict) else None)
    if limit not in (None, ""):
        out["limit"] = _hydra_limit(limit)
    if "limit" not in out and isinstance(original, dict) and original.get("limit") not in (None, ""):
        out["limit"] = _hydra_limit(original.get("limit"))
    hours = source.get("hours", original.get("hours") if isinstance(original, dict) else None)
    if hours not in (None, ""):
        out["hours"] = _hydra_limit(hours, default=12)
    return out


async def _hydra_ai_normalize_environment_args(
    args: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    llm_client: Any = None,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    request_text = _hydra_request_text(args, origin)
    if llm_client is None or not request_text:
        return dict(args or {})

    context = _hydra_filter_context(payload)
    user_payload = {
        "request": request_text,
        "current_tool_arguments": dict(args or {}),
        "available_environment_context": context,
    }
    system_prompt = (
        "You normalize a user's natural-language request into Environment Core filters. "
        "Return only one JSON object. Do not answer the user. "
        "Allowed keys: area, category, provider, sensor, query, date, day, hours, limit. Use an empty string to omit a filter. "
        "Allowed categories include weather, condition, temperature, humidity, wind, rain, pressure, solar, air, lightning, battery, system, and other. "
        "Use category weather for broad current weather or conditions requests. "
        "Use category forecast for forecasts, tomorrow, tonight, this week, rain chance later, or future weather requests. "
        "For forecast requests, include date when the user gives or implies a specific YYYY-MM-DD date; otherwise leave date empty. "
        "Use provider only when the user clearly names a specific integration/provider. "
        "Use query only for a real sensor label/name the user asked for; do not put generic words like weather, current, conditions, outside, or local in query. "
        "Prefer the available areas, categories, providers, and sensor labels from the payload."
    )
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            temperature=0.0,
            max_tokens=280,
            timeout_ms=25_000,
        )
    except Exception as exc:
        logger.warning("[Environment] AI filter normalization failed: %s", exc)
        return dict(args or {})

    raw = _text((response.get("message") or {}).get("content"))
    parsed_text = extract_json(raw) or raw
    try:
        parsed = json.loads(parsed_text)
    except Exception as exc:
        logger.warning("[Environment] AI filter normalization returned invalid JSON: %s", exc)
        return dict(args or {})
    return _hydra_normalized_filter_args(parsed if isinstance(parsed, dict) else {}, dict(args or {}))


def _hydra_environment_payload(client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    provider_snapshots = _load_provider_snapshots(store)
    snapshot = _combined_snapshot(provider_snapshots)
    settings = _load_settings(store)
    selected = _load_selected_sensors(store)
    provider_status = _provider_status_rows(settings, provider_snapshots, store)
    received_at = snapshot.get("received_at") if snapshot else 0
    stale_after_s = int(settings.get("stale_after_minutes") or DEFAULT_STALE_AFTER_MINUTES) * 60
    stale = bool(received_at and (time.time() - float(received_at)) > stale_after_s)
    return {
        "provider_snapshots": provider_snapshots,
        "snapshot": snapshot,
        "settings": settings,
        "selected_sensors": selected,
        "provider_status": provider_status,
        "received_at": received_at,
        "stale": stale,
    }


def _hydra_forecast_requested(args: Dict[str, Any]) -> bool:
    return _clean_key(args.get("category") or args.get("type") or args.get("measurement")) == "forecast"


def _hydra_forecast_summary(
    *,
    provider_label: str,
    last_sample: str,
    daily_rows: List[Dict[str, str]],
    hourly_rows: List[Dict[str, str]],
    date_filter: str = "",
    stale: bool = False,
) -> str:
    if not daily_rows and not hourly_rows:
        return f"{provider_label} forecast data is not available in Environment Core yet."
    prefix = f"{provider_label} forecast was last updated {last_sample}" + (" and may be stale." if stale else ".")
    if date_filter:
        prefix += f" Matching date: {date_filter}."
    pieces: List[str] = []
    for row in daily_rows[:5]:
        date_text = _text(row.get("date")) or "Forecast"
        condition = _text(row.get("condition")) or "-"
        high = _text(row.get("high")) or "-"
        low = _text(row.get("low")) or "-"
        rain = _text(row.get("rain")) or "-"
        wind = _text(row.get("wind")) or "-"
        pieces.append(f"{date_text}: {condition}, high {high}, low {low}, rain {rain}, wind {wind}")
    if not pieces:
        for row in hourly_rows[:8]:
            time_text = _text(row.get("time")) or "Hour"
            condition = _text(row.get("condition")) or "-"
            temp = _text(row.get("temp")) or "-"
            rain = _text(row.get("rain")) or "-"
            wind = _text(row.get("wind")) or "-"
            pieces.append(f"{time_text}: {condition}, {temp}, rain {rain}, wind {wind}")
    return prefix + " " + "; ".join(pieces) + "."


def _hydra_forecast_payload(args: Dict[str, Any], client: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else _load_settings(client)
    provider = _clean_key(settings.get("forecast_provider")) or "weather_api"
    provider_label = _provider_label(provider)
    provider_snapshots = payload.get("provider_snapshots") if isinstance(payload.get("provider_snapshots"), dict) else {}
    forecast_snapshot = provider_snapshots.get(provider) if isinstance(provider_snapshots.get(provider), dict) else {}
    if provider != "weather_api":
        return {
            "tool": "environment_conditions",
            "ok": False,
            "forecast_provider": provider,
            "forecast": {"provider": provider_label, "daily": [], "hourly": []},
            "sources": payload.get("provider_status") or [],
            "summary_for_user": f"{provider_label} does not expose forecast data to Environment Core yet.",
        }
    if not forecast_snapshot:
        enabled = _as_bool(settings.get("weather_api_enabled"), False)
        detail = "Enabled, waiting for forecast data." if enabled else "Enable WeatherAPI Forecast in Environment Core settings."
        return {
            "tool": "environment_conditions",
            "ok": False,
            "forecast_provider": provider,
            "forecast": {"provider": provider_label, "daily": [], "hourly": []},
            "sources": payload.get("provider_status") or [],
            "summary_for_user": f"{provider_label} forecast data is not available yet. {detail}",
        }

    units = _weatherapi_units(client)
    daily_rows = _weatherapi_daily_rows(forecast_snapshot, units=units)
    date_filter = _text(args.get("date"))
    if date_filter:
        daily_rows = [row for row in daily_rows if _text(row.get("date")) == date_filter]
    daily_limit = _hydra_limit(args.get("limit"), default=7)
    hourly_limit = _hydra_limit(args.get("hours"), default=12)
    daily_rows = daily_rows[:daily_limit]
    hourly_rows = _weatherapi_hourly_rows(forecast_snapshot, units=units, limit=hourly_limit)
    last = _age_label(forecast_snapshot.get("received_at"))
    stale_after_s = int(settings.get("stale_after_minutes") or DEFAULT_STALE_AFTER_MINUTES) * 60
    received_at = _as_float(forecast_snapshot.get("received_at"))
    stale = bool(received_at and (time.time() - float(received_at)) > stale_after_s)
    return {
        "tool": "environment_conditions",
        "ok": True,
        "stale": stale,
        "last_sample": last,
        "forecast_provider": provider,
        "filters": {key: args.get(key) for key in ("category", "date", "day", "provider", "source", "integration", "query") if args.get(key)},
        "forecast": {
            "provider": provider_label,
            "location": _text(forecast_snapshot.get("model")) or provider_label,
            "units": units,
            "daily": daily_rows,
            "hourly": hourly_rows,
        },
        "sources": payload.get("provider_status") or [],
        "summary_for_user": _hydra_forecast_summary(
            provider_label=provider_label,
            last_sample=last,
            daily_rows=daily_rows,
            hourly_rows=hourly_rows,
            date_filter=date_filter,
            stale=stale,
        ),
    }


def _environment_conditions_kernel(args: Dict[str, Any], client: Any = None, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else _hydra_environment_payload(client)
    if _hydra_forecast_requested(args):
        return _hydra_forecast_payload(args, client or redis_client, payload)
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    if not snapshot:
        return {
            "tool": "environment_conditions",
            "ok": False,
            "error": "No Environment Core readings are available.",
            "summary_for_user": "I do not have Environment Core readings yet.",
        }
    readings = [_hydra_reading_row(row) for row in _hydra_relevant_readings(snapshot, args)]
    last = _age_label(payload.get("received_at"))
    stale = bool(payload.get("stale"))
    prefix = f"Environment readings were last updated {last}" + (" and may be stale." if stale else ".")
    return {
        "tool": "environment_conditions",
        "ok": True,
        "stale": stale,
        "last_sample": last,
        "filters": {key: args.get(key) for key in ("area", "room", "location", "category", "provider", "source", "integration", "sensor", "query") if args.get(key)},
        "readings": readings,
        "sources": payload.get("provider_status") or [],
        "summary_for_user": _hydra_summary_from_readings(readings, prefix=prefix),
    }


def _environment_sensors_kernel(args: Dict[str, Any], client: Any = None, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else _hydra_environment_payload(client)
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    selected = payload.get("selected_sensors") if isinstance(payload.get("selected_sensors"), list) else []
    readings = [_hydra_reading_row(row) for row in _hydra_relevant_readings(snapshot, args)] if snapshot else []
    selected_rows = []
    for row in selected:
        if not isinstance(row, dict):
            continue
        selected_rows.append(
            {
                "label": _text(row.get("label")) or _text(row.get("key")),
                "area": _text(row.get("area")),
                "category": CATEGORY_LABELS.get(_clean_key(row.get("category")), _humanize_key(_clean_key(row.get("category")))),
                "provider": _provider_label(row.get("provider")),
                "enabled": _as_bool(row.get("enabled"), True),
            }
        )
    source_count = len([row for row in payload.get("provider_status") or [] if _text(row.get("last_sample")) != "never"])
    summary = (
        f"Environment Core has {len(selected_rows)} selected sensor source{'s' if len(selected_rows) != 1 else ''} "
        f"and {source_count} source{'s' if source_count != 1 else ''} with recent readings."
    )
    if readings:
        summary += " " + "; ".join(f"{row.get('label')}: {row.get('value')}" for row in readings[:8] if row.get("label") and row.get("value")) + "."
    return {
        "tool": "environment_sensors",
        "ok": True,
        "selected_sensors": selected_rows,
        "latest_readings": readings,
        "sources": payload.get("provider_status") or [],
        "summary_for_user": summary,
    }


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    del platform
    return [
        {
            "id": "environment_conditions",
            "description": "Read live local weather, environment sensor readings, and WeatherAPI forecast data from Environment Core. Use this for current weather, temperature, humidity, wind, rain, rain chance, tomorrow/tonight/weekly forecasts, and selected sensor readings.",
            "usage": '{"function":"environment_conditions","arguments":{"request":"What is tomorrow\'s forecast?"}}',
        },
        {
            "id": "environment_sensors",
            "description": "List Environment Core sensor sources, providers, areas, selected integration sensors, and latest environment readings.",
            "usage": '{"function":"environment_sensors","arguments":{"request":"Show office humidity sensors"}}',
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
    func = _clean_key(tool_id)
    payload = dict(args) if isinstance(args, dict) else {}
    client = redis_client if redis_client is not None else globals().get("redis_client")
    if client is None:
        return {
            "tool": func or "environment",
            "ok": False,
            "error": "Environment Core store is unavailable.",
            "summary_for_user": "Environment Core storage is unavailable right now.",
        }
    try:
        if func in {"environment_conditions", "environment_weather", "environment_current_conditions"}:
            environment_payload = _hydra_environment_payload(client)
            normalized_args = await _hydra_ai_normalize_environment_args(
                payload,
                environment_payload,
                llm_client=llm_client,
                origin=origin,
            )
            return _environment_conditions_kernel(normalized_args, client, payload=environment_payload)
        if func in {"environment_sensors", "environment_sensor_status", "environment_sources"}:
            environment_payload = _hydra_environment_payload(client)
            normalized_args = await _hydra_ai_normalize_environment_args(
                payload,
                environment_payload,
                llm_client=llm_client,
                origin=origin,
            )
            return _environment_sensors_kernel(normalized_args, client, payload=environment_payload)
    except Exception as exc:
        return {
            "tool": func or "environment",
            "ok": False,
            "error": f"{func or 'environment'} failed: {exc}",
            "summary_for_user": "I could not read Environment Core data right now.",
        }
    return None


def run(stop_event: Optional[object] = None) -> None:
    logger.info("[Environment] Core started.")
    last_integration_poll = 0.0
    while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
        try:
            redis_client.set(HEARTBEAT_KEY, str(time.time()))
        except Exception:
            logger.debug("[Environment] heartbeat update failed", exc_info=True)
        try:
            settings = _load_settings(redis_client)
            poll_seconds = int(settings.get("integration_poll_seconds") or DEFAULT_INTEGRATION_POLL_SECONDS)
            integrations_enabled = any(
                _clean_key(row.get("provider")) in {"unifi_protect", "ecobee_homekit", "hue", "homeassistant"}
                and _as_bool(row.get("enabled"), True)
                for row in _load_selected_sensors(redis_client)
            ) or _as_bool(settings.get("weather_api_enabled"), False)
            now_ts = time.time()
            if integrations_enabled and now_ts - last_integration_poll >= poll_seconds:
                last_integration_poll = now_ts
                result = _poll_enabled_integrations(redis_client)
                if result.get("reading_count"):
                    logger.info("[Environment] Integration poll stored %s readings.", result.get("reading_count"))
                elif _text(result.get("message")):
                    logger.info("[Environment] Integration poll: %s", result.get("message"))
        except Exception:
            logger.exception("[Environment] integration poll failed")
        time.sleep(5.0)
    logger.info("[Environment] Core stopped.")
