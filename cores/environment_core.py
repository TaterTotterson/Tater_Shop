"""Environment Core receives local environment telemetry and keeps a normalized live state."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl

from helpers import redis_client

__version__ = "1.0.0"
MIN_TATER_VERSION = "59"
CORE_DESCRIPTION = "Local environment telemetry receiver for weather stations and future sensor feeds."
TAGS = ["environment", "weather", "ecowitt", "telemetry"]

logger = logging.getLogger("environment_core")
logger.setLevel(logging.INFO)

CORE_SETTINGS = {
    "category": "Environment Core Settings",
    "required": {
        "ECOWITT_ENABLED": {
            "label": "Enable Ecowitt Receiver",
            "type": "checkbox",
            "default": "true",
            "description": "Accept Ecowitt custom-server uploads on the Environment Core webhook.",
        },
        "ECOWITT_PASSKEY": {
            "label": "Ecowitt PASSKEY",
            "type": "password",
            "default": "",
            "description": "Optional. If set, inbound Ecowitt uploads must include this PASSKEY.",
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
SOURCES_KEY = "environment:sources"
HISTORY_KEY = "environment:history"
HEARTBEAT_KEY = "environment:heartbeat"

DEFAULT_HISTORY_LIMIT = 288
DEFAULT_STALE_AFTER_MINUTES = 30

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
    "lightning": "Lightning",
    "soil": "Soil",
    "leak": "Leak Sensors",
    "battery": "Batteries",
    "system": "Station",
    "other": "Other",
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
        "ecowitt_enabled": _as_bool(raw.get("ECOWITT_ENABLED"), True),
        "ecowitt_passkey": _text(raw.get("ECOWITT_PASSKEY")),
        "history_limit": _as_int(raw.get("ENVIRONMENT_HISTORY_LIMIT"), DEFAULT_HISTORY_LIMIT, minimum=1, maximum=10000),
        "stale_after_minutes": _as_int(
            raw.get("ENVIRONMENT_STALE_AFTER_MINUTES"),
            DEFAULT_STALE_AFTER_MINUTES,
            minimum=1,
            maximum=10080,
        ),
    }


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


def _store_snapshot(snapshot: Dict[str, Any], client: Any = None) -> None:
    store = client or redis_client
    settings = _load_settings(store)
    blob = json.dumps(snapshot, sort_keys=True)
    store.set(LATEST_KEY, blob)
    store.set(LATEST_ECOWITT_KEY, blob)
    store.hset(SOURCES_KEY, snapshot.get("source_id") or "ecowitt:unknown", blob)
    store.lpush(HISTORY_KEY, blob)
    store.ltrim(HISTORY_KEY, 0, max(0, int(settings.get("history_limit") or DEFAULT_HISTORY_LIMIT) - 1))


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


def _sensor_rows(rows: Iterable[Dict[str, Any]], *, include_meta: bool = True) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        label = _text(row.get("label"))
        value = _text(row.get("display"))
        if not label or not value:
            continue
        out.append(
            {
                "label": label,
                "value": value,
                "meta": CATEGORY_LABELS.get(_text(row.get("category")), _text(row.get("category"))) if include_meta else "",
            }
        )
    return out


def _history_summary(history: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for item in history[:8]:
        rows.append(
            {
                "label": _age_label(item.get("received_at")),
                "value": _reading_display(item, "tempf", _reading_display(item, "tempinf", "-")),
                "meta": item.get("model") or item.get("stationtype") or "Ecowitt",
            }
        )
    return rows


def _environment_manager_ui(snapshot: Dict[str, Any], history: List[Dict[str, Any]], client: Any = None) -> Dict[str, Any]:
    settings = _load_settings(client)
    grouped = _readings_by_category(snapshot)
    has_snapshot = bool(snapshot)
    received_at = snapshot.get("received_at") if has_snapshot else 0
    stale_after_s = int(settings.get("stale_after_minutes") or DEFAULT_STALE_AFTER_MINUTES) * 60
    is_stale = bool(received_at and (time.time() - float(received_at)) > stale_after_s)
    webhook_path = "/api/cores/environment_core/webhook/ecowitt"

    item_forms: List[Dict[str, Any]] = [
        {
            "id": "overview",
            "group": "overview",
            "title": "Live Environment",
            "subtitle": "Ecowitt receiver",
            "detail": (
                f"Last upload {_age_label(received_at)}."
                if has_snapshot
                else f"Point Ecowitt custom server uploads at {webhook_path}."
            ),
            "hero_badges": [
                {"label": "Live" if has_snapshot and not is_stale else "Stale" if has_snapshot else "Waiting", "tone": "good" if has_snapshot and not is_stale else "warning"},
                {"label": snapshot.get("model") or snapshot.get("stationtype") or "Ecowitt", "tone": "muted"},
            ],
            "summary_rows": [
                {"label": "Outdoor", "value": _reading_display(snapshot, "tempf")},
                {"label": "Humidity", "value": _reading_display(snapshot, "humidity")},
                {"label": "Wind", "value": _reading_display(snapshot, "windspeedmph")},
                {"label": "Rain Today", "value": _reading_display(snapshot, "dailyrainin")},
            ],
            "sensor_title": "Current Conditions",
            "sensor_rows": _sensor_rows(
                [
                    row
                    for key in (
                        "tempf",
                        "humidity",
                        "tempinf",
                        "humidityin",
                        "baromrelin",
                        "windspeedmph",
                        "windgustmph",
                        "rainratein",
                        "dailyrainin",
                        "solarradiation",
                        "uv",
                    )
                    for row in [_reading(snapshot, key)]
                    if row
                ],
                include_meta=False,
            ),
        },
        {
            "id": "setup",
            "group": "overview",
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

    for category in ("temperature", "humidity", "pressure", "wind", "rain", "solar", "air", "lightning", "soil", "leak", "other"):
        rows = grouped.get(category) or []
        if not rows:
            continue
        item_forms.append(
            {
                "id": f"category:{category}",
                "group": "sensor",
                "title": CATEGORY_LABELS.get(category, category.title()),
                "subtitle": f"{len(rows)} reading{'s' if len(rows) != 1 else ''}",
                "detail": "Normalized from the latest Ecowitt upload.",
                "sensor_title": CATEGORY_LABELS.get(category, category.title()),
                "sensor_rows": _sensor_rows(rows, include_meta=False),
            }
        )

    battery_rows = grouped.get("battery") or []
    item_forms.append(
        {
            "id": "batteries",
            "group": "battery",
            "title": "Sensor Batteries",
            "subtitle": f"{len(battery_rows)} battery reading{'s' if len(battery_rows) != 1 else ''}",
            "detail": "Ecowitt battery fields are decoded where known; raw values are shown otherwise.",
            "hero_badges": [
                {
                    "label": "Low Battery" if any(_text(row.get("tone")) == "danger" for row in battery_rows) else "OK",
                    "tone": "danger" if any(_text(row.get("tone")) == "danger" for row in battery_rows) else "good",
                }
            ],
            "sensor_title": "Batteries",
            "sensor_rows": _sensor_rows(battery_rows, include_meta=False),
        }
    )

    raw_rows = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
    raw_text = json.dumps(raw_rows, indent=2, sort_keys=True) if raw_rows else "{}"
    item_forms.append(
        {
            "id": "raw:latest",
            "group": "raw",
            "title": "Latest Raw Payload",
            "subtitle": snapshot.get("source_id") or "No source yet",
            "detail": "PASSKEY is stored as a short hash, not displayed raw.",
            "sections": [
                {
                    "label": "Payload",
                    "inline": True,
                    "fields": [
                        {"key": "raw_payload", "label": "Payload", "type": "textarea", "value": raw_text, "read_only": True},
                    ],
                }
            ],
        }
    )
    item_forms.append(
        {
            "id": "raw:history",
            "group": "raw",
            "title": "Recent History",
            "subtitle": f"{len(history)} retained sample{'s' if len(history) != 1 else ''} shown",
            "detail": "Most recent samples are kept in Redis for quick trend checks.",
            "sensor_title": "Recent Samples",
            "sensor_rows": _history_summary(history),
        }
    )

    return {
        "kind": "settings_manager",
        "title": "Environment Core",
        "stats_refresh_button": True,
        "stats_refresh_label": "Refresh",
        "empty_message": "No environment telemetry has been received yet.",
        "manager_tabs": [
            {"key": "overview", "label": "Overview", "source": "items", "item_group": "overview"},
            {"key": "readings", "label": "Measurements", "source": "items", "item_group": "sensor", "selector": True},
            {"key": "batteries", "label": "Batteries", "source": "items", "item_group": "battery"},
            {"key": "raw", "label": "Raw", "source": "items", "item_group": "raw", "selector": True},
        ],
        "default_tab": "overview",
        "item_forms": item_forms,
    }


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    snapshot = _load_json_key(LATEST_KEY, client)
    history = _load_history(limit=24, client=client)
    settings = _load_settings(client)
    received_at = snapshot.get("received_at") if snapshot else 0
    stale_after_s = int(settings.get("stale_after_minutes") or DEFAULT_STALE_AFTER_MINUTES) * 60
    stale = bool(received_at and (time.time() - float(received_at)) > stale_after_s)

    return {
        "summary": "Local environment telemetry from Ecowitt and future environment providers.",
        "stats": [
            {"label": "Provider", "value": "Ecowitt" if snapshot else "Waiting"},
            {"label": "Last Upload", "value": _age_label(received_at)},
            {"label": "Outdoor Temp", "value": _reading_display(snapshot, "tempf")},
            {"label": "Humidity", "value": _reading_display(snapshot, "humidity")},
            {"label": "Wind", "value": _reading_display(snapshot, "windspeedmph")},
            {"label": "Rain Today", "value": _reading_display(snapshot, "dailyrainin")},
            {"label": "Status", "value": "Stale" if stale else "Live" if snapshot else "Waiting"},
        ],
        "items": [],
        "empty_message": "No Ecowitt uploads have reached Tater yet.",
        "ui": _environment_manager_ui(snapshot, history, client),
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
    if not bool(settings.get("ecowitt_enabled", True)):
        raise ValueError("Ecowitt receiver is disabled.")

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

    snapshot = _normalize_ecowitt_payload(incoming)
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
    raise KeyError(f"Unsupported Environment Core action: {action}")


def run(stop_event: Optional[object] = None) -> None:
    logger.info("[Environment] Core started.")
    while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
        try:
            redis_client.set(HEARTBEAT_KEY, str(time.time()))
        except Exception:
            logger.debug("[Environment] heartbeat update failed", exc_info=True)
        time.sleep(5.0)
    logger.info("[Environment] Core stopped.")
