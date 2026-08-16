# portals/xbmc_portal.py
import json
import os
import asyncio
import logging
import re
import threading
import time
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
import requests
import uvicorn

from dotenv import load_dotenv
load_dotenv()

from helpers import (
    get_tater_name,
    get_llm_client_from_env,
    build_llm_host_from_env,
    redis_client,
)
try:
    from helpers import get_primary_llm_client_from_env as _get_primary_llm_client_from_env
except Exception:  # pragma: no cover - compatibility with older Tater runtimes.
    _get_primary_llm_client_from_env = get_llm_client_from_env
import verba_registry as pr
from admin_gate import admin_denial_message, is_admin_only_plugin, origin_is_admin, resolve_admin_status
from hydra import run_hydra_turn, resolve_agent_limits
from verba_result import action_failure
__version__ = "1.1.9"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xbmc")

# -------------------- Platform defaults --------------------
DEFAULT_GLOBAL_MAX_STORE = 20
DEFAULT_GLOBAL_MAX_LLM = 8
DEFAULT_SESSION_TTL_SECONDS = 2 * 60 * 60  # 2h
BGVIDEO_FILENAME = "BGVideo.avi"
BGVIDEO_RELEASE_URL = "https://github.com/TaterTotterson/skin.cortana.ai-xbmc/releases/latest/download/BGVideo.avi"
BGVIDEO_CACHE_PATH = os.path.expanduser("~/.taterassistant/agent_lab/assets/xbmc/BGVideo.avi")
BGVIDEO_MIN_BYTES = 10 * 1024 * 1024
BGVIDEO_CHUNK_BYTES = 1024 * 1024
BGVIDEO_MEDIA_TYPE = "video/x-msvideo"
SKIN_REPO = "TaterTotterson/skin.cortana.ai-xbmc"
SKIN_LATEST_RELEASE_URL = "https://api.github.com/repos/%s/releases/latest" % SKIN_REPO
SKIN_CACHE_DIR = os.path.expanduser("~/.taterassistant/agent_lab/assets/xbmc/skin")
SKIN_ZIP_MIN_BYTES = 64 * 1024
SKIN_ZIP_CHUNK_BYTES = 512 * 1024
SKIN_ZIP_MEDIA_TYPE = "application/zip"
TTS_MAX_TEXT_CHARS = 900
INSTALLED_GAME_CONTEXT_MAX_ITEMS = 1200
QUICK_ASK_COUNT = 5
ENVIRONMENT_SETTINGS_KEY = "environment_core_settings"
ENVIRONMENT_LATEST_KEY = "environment:latest"
ENVIRONMENT_PROVIDER_KEY_PREFIX = "environment:latest:"
ENVIRONMENT_PROVIDER_KEYS = {
    "ecowitt": "environment:latest:ecowitt",
    "unifi_protect": "environment:latest:unifi_protect",
    "ecobee_homekit": "environment:latest:ecobee_homekit",
    "hue": "environment:latest:hue",
    "homeassistant": "environment:latest:homeassistant",
    "weather_api": "environment:latest:weather_api",
}
DEFAULT_ENVIRONMENT_STALE_AFTER_MINUTES = 30

PORTAL_SETTINGS = {
    "category": "XBMC / Original Xbox Settings",
    "required": {
        "API_AUTH_ENABLED": {
            "label": "Require API Key",
            "type": "select",
            "options": ["true", "false"],
            "default": "false",
            "description": "Require X-Tater-Token on all XBMC portal API endpoints."
        },
        "API_AUTH_KEY": {
            "label": "API Key",
            "type": "password",
            "default": "",
            "description": "Shared API key expected in the X-Tater-Token header when auth is enabled."
        },
        "SESSION_TTL_SECONDS": {
            "label": "Session TTL",
            "type": "select",
            "options": ["5m", "30m", "1h", "2h", "6h", "24h"],
            "default": "2h",
            "description": "How long to keep an XBMC session’s history alive (5m–24h)."
        },
    }
}

# -------------------- Plugin gating --------------------
def get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("verba_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")

# -------------------- Settings helpers --------------------
def _portal_settings() -> Dict[str, str]:
    return redis_client.hgetall("xbmc_portal_settings") or {}

def _parse_duration_seconds(val: str, default_seconds: int) -> int:
    if val is None:
        return default_seconds
    s = str(val).strip().lower()
    # raw integer seconds?
    try:
        return int(s)
    except ValueError:
        pass
    import re
    m = re.match(r"^\s*(\d+)\s*([smhd])\s*$", s)
    if not m:
        return default_seconds
    num = int(m.group(1))
    unit = m.group(2)
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return num * mult

def _get_duration_seconds_setting(name: str, default_seconds: int) -> int:
    s = _portal_settings().get(name)
    return _parse_duration_seconds(s, default_seconds)

def _get_int_platform_setting(name: str, default: int) -> int:
    s = _portal_settings().get(name)
    try:
        return int(str(s).strip()) if s is not None and str(s).strip() != "" else default
    except Exception:
        return default

def _get_str_platform_setting(name: str, default: str = "") -> str:
    s = _portal_settings().get(name)
    return str(s) if s is not None else default

def _get_bool_platform_setting(name: str, default: bool = False) -> bool:
    s = _portal_settings().get(name)
    if s is None:
        return default
    token = str(s).strip().lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _read_global_history_limit(redis_key: str, default: int, *, min_value: int = 0, max_value: int = 500) -> int:
    try:
        raw = redis_client.get(redis_key)
        value = int(str(raw).strip()) if raw is not None else int(default)
    except Exception:
        value = int(default)
    value = max(int(min_value), value)
    if max_value > 0:
        value = min(int(max_value), value)
    return int(value)


def _global_history_store_limit() -> int:
    return _read_global_history_limit("tater:max_store", DEFAULT_GLOBAL_MAX_STORE, min_value=0)


def _global_history_llm_limit() -> int:
    return _read_global_history_limit("tater:max_llm", DEFAULT_GLOBAL_MAX_LLM, min_value=1)

def _get_api_auth_key() -> str:
    return _get_str_platform_setting("API_AUTH_KEY", "").strip()

def _is_api_auth_enabled() -> bool:
    raw = _portal_settings().get("API_AUTH_ENABLED")
    if raw is None or str(raw).strip() == "":
        return bool(_get_api_auth_key())
    return _get_bool_platform_setting("API_AUTH_ENABLED", False)

def _require_api_auth(x_tater_token: Optional[str]) -> None:
    if not _is_api_auth_enabled():
        return
    configured = _get_api_auth_key()
    if not configured:
        raise HTTPException(status_code=503, detail="API auth is enabled but no API key is configured.")
    supplied = str(x_tater_token or "").strip()
    if supplied != configured:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Tater-Token header.")


def _portal_text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "ignore").strip()
    return str(value or "").strip()


def _environment_clean_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", _portal_text(value).lower()).strip("_")


def _environment_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(_portal_text(value))
    except Exception:
        return None


def _environment_int(value: Any, default: int, *, minimum: int = 0, maximum: int = 100000) -> int:
    try:
        parsed = int(float(_portal_text(value)))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _environment_load_json_key(key: str, default: Any) -> Any:
    try:
        raw = redis_client.get(key)
    except Exception:
        raw = None
    if raw in (None, ""):
        return default
    try:
        parsed = json.loads(_portal_text(raw))
    except Exception:
        return default
    return parsed if parsed is not None else default


def _environment_hget_text(rows: Dict[Any, Any], key: str, default: str = "") -> str:
    return _portal_text(rows.get(key) if key in rows else rows.get(key.encode("utf-8"), default))


def _environment_settings() -> Dict[str, Any]:
    try:
        raw = redis_client.hgetall(ENVIRONMENT_SETTINGS_KEY) or {}
    except Exception:
        raw = {}
    return {
        "temperature_unit": _environment_temperature_unit(
            _environment_hget_text(raw, "ENVIRONMENT_TEMPERATURE_UNIT"),
            "F",
        ),
        "current_live_source": _environment_hget_text(
            raw,
            "ENVIRONMENT_CURRENT_CONDITION_LIVE_SOURCE",
            "provider:ecowitt",
        ) or "provider:ecowitt",
        "current_condition_source": _environment_hget_text(
            raw,
            "ENVIRONMENT_CURRENT_CONDITION_CONDITION_SOURCE",
            "provider:weather_api",
        ) or "provider:weather_api",
        "stale_after_minutes": _environment_int(
            _environment_hget_text(raw, "ENVIRONMENT_STALE_AFTER_MINUTES"),
            DEFAULT_ENVIRONMENT_STALE_AFTER_MINUTES,
            minimum=1,
            maximum=10080,
        ),
    }


def _environment_temperature_unit(value: Any, default: str = "F") -> str:
    token = _portal_text(value).replace("deg", "").replace("degree", "").replace("degrees", "").strip().lower()
    if token in {"c", "celcius", "celsius", "centigrade", "metric"}:
        return "C"
    if token in {"f", "fahrenheit", "imperial", "us"}:
        return "F"
    fallback = _portal_text(default).strip().upper()
    return fallback if fallback in {"C", "F"} else "F"


def _environment_provider_latest_key(provider: Any) -> str:
    provider_key = _environment_clean_key(provider)
    if not provider_key:
        return ENVIRONMENT_LATEST_KEY
    return ENVIRONMENT_PROVIDER_KEYS.get(provider_key) or ENVIRONMENT_PROVIDER_KEY_PREFIX + provider_key


def _environment_load_provider_snapshot(provider: Any) -> Dict[str, Any]:
    snapshot = _environment_load_json_key(_environment_provider_latest_key(provider), {})
    return snapshot if isinstance(snapshot, dict) else {}


def _environment_load_selected_sensors() -> List[Dict[str, Any]]:
    selected = _environment_load_json_key("environment:selected_sensors", [])
    if not isinstance(selected, list):
        return []
    return [row for row in selected if isinstance(row, dict)]


def _environment_combined_snapshot() -> Dict[str, Any]:
    snapshot = _environment_load_json_key(ENVIRONMENT_LATEST_KEY, {})
    return snapshot if isinstance(snapshot, dict) else {}


def _environment_display_snapshot_for_source(
    source: Any,
    combined_snapshot: Dict[str, Any],
    selected_sensors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    source_text = _portal_text(source)
    if source_text.startswith("provider:"):
        provider = _environment_clean_key(source_text.split(":", 1)[1])
        if provider in {"environment", "all"}:
            return combined_snapshot
        return _environment_load_provider_snapshot(provider)
    if source_text.startswith("sensor:"):
        selection_key = source_text.split(":", 1)[1]
        return _environment_selected_sensor_snapshot(selection_key, combined_snapshot, selected_sensors)
    provider = _environment_clean_key(source_text)
    if provider:
        return _environment_load_provider_snapshot(provider)
    return {}


def _environment_selected_sensor_snapshot(
    selection_key: str,
    combined_snapshot: Dict[str, Any],
    selected_sensors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    selection_key_text = _portal_text(selection_key)
    selection = next(
        (row for row in selected_sensors if _portal_text(row.get("key")) == selection_key_text),
        None,
    )
    if not isinstance(selection, dict):
        return {}
    provider = _environment_clean_key(selection.get("provider"))
    sensor_clean = _environment_clean_key(selection.get("sensor_id"))
    selection_clean = _environment_clean_key(selection.get("key"))
    rows: List[Dict[str, Any]] = []
    for row in combined_snapshot.get("readings") or []:
        if not isinstance(row, dict):
            continue
        if provider and _environment_clean_key(row.get("provider")) != provider:
            continue
        source_id = _portal_text(row.get("source_id"))
        source_suffix = source_id.split(":", 1)[1] if ":" in source_id else source_id
        source_clean = _environment_clean_key(source_suffix)
        row_key = _environment_clean_key(row.get("key"))
        if sensor_clean and (source_clean == sensor_clean or sensor_clean in source_clean or sensor_clean in row_key):
            rows.append(row)
            continue
        if selection_clean and (row_key == selection_clean or selection_clean in row_key):
            rows.append(row)
    if not rows:
        return {}
    return {
        "provider": provider or "environment",
        "source_id": "display:%s" % selection_key_text,
        "model": _portal_text(selection.get("label")) or selection_key_text,
        "received_at": combined_snapshot.get("received_at"),
        "sample_time": combined_snapshot.get("sample_time"),
        "readings": rows,
    }


def _environment_reading(snapshot: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    wanted = _environment_clean_key(key)
    for row in snapshot.get("readings") or []:
        if isinstance(row, dict) and _environment_clean_key(row.get("key")) == wanted:
            return row
    return None


def _environment_first_category_reading(snapshot: Dict[str, Any], category: str) -> Optional[Dict[str, Any]]:
    wanted = _environment_clean_key(category)
    for row in snapshot.get("readings") or []:
        if isinstance(row, dict) and _environment_clean_key(row.get("category")) == wanted:
            return row
    return None


def _environment_temperature_reading(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for key in ("tempf", "tempc", "weather_api_temperature", "outdoor_temperature", "temperature"):
        row = _environment_reading(snapshot, key)
        if row:
            return row
    for area_token in ("outside", "outdoor", "forecast"):
        for row in snapshot.get("readings") or []:
            if not isinstance(row, dict):
                continue
            if _environment_clean_key(row.get("category")) != "temperature":
                continue
            if _environment_clean_key(row.get("area")) == area_token:
                return row
    return _environment_first_category_reading(snapshot, "temperature")


def _environment_condition_reading(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for key in ("weather_api_condition", "current_condition", "condition"):
        row = _environment_reading(snapshot, key)
        if row:
            return row
    for row in snapshot.get("readings") or []:
        if not isinstance(row, dict):
            continue
        if _environment_clean_key(row.get("category")) != "condition":
            continue
        row_key = _environment_clean_key(row.get("key"))
        if row_key in {"cloud", "weather_api_cloud"}:
            continue
        display = _portal_text(row.get("display") or row.get("value"))
        if display and _environment_float(display) is None:
            return row
    return None


def _environment_reading_display(row: Optional[Dict[str, Any]]) -> str:
    if not isinstance(row, dict):
        return ""
    return _portal_text(row.get("display")) or _portal_text(row.get("value"))


def _environment_temperature_display(row: Optional[Dict[str, Any]], fallback_unit: str) -> Dict[str, str]:
    if not isinstance(row, dict):
        return {"temperature": "", "temperature_units": fallback_unit, "temperature_display": "--"}
    unit = _environment_temperature_unit(row.get("unit"), fallback_unit)
    number = _environment_float(row.get("value"))
    if number is None:
        display = _environment_reading_display(row)
        return {"temperature": display, "temperature_units": unit, "temperature_display": display or "--"}
    text = str(int(round(number)))
    return {
        "temperature": text,
        "temperature_units": unit,
        "temperature_display": "%s %s" % (text, unit),
    }


def _environment_condition_kind(condition: Any) -> str:
    text = _portal_text(condition).lower()
    if any(token in text for token in ("thunder", "storm", "lightning")):
        return "storm"
    if any(token in text for token in ("rain", "drizzle", "shower", "sleet")):
        return "rain"
    if any(token in text for token in ("snow", "ice", "blizzard", "flurr")):
        return "snow"
    if any(token in text for token in ("fog", "mist", "haze", "smoke")):
        return "fog"
    if any(token in text for token in ("cloud", "overcast")):
        return "cloud"
    if any(token in text for token in ("wind", "breez")):
        return "wind"
    if any(token in text for token in ("sun", "clear")):
        return "sun"
    return "partly"


def _environment_source_label(snapshot: Dict[str, Any], row: Optional[Dict[str, Any]]) -> str:
    if isinstance(row, dict):
        label = _portal_text(row.get("source_name"))
        if label:
            return label
    return _portal_text(snapshot.get("model") or snapshot.get("stationtype") or snapshot.get("source_id"))


def _environment_received_at(*snapshots: Dict[str, Any]) -> float:
    values = []
    for snapshot in snapshots:
        if isinstance(snapshot, dict):
            value = _environment_float(snapshot.get("received_at") or snapshot.get("sample_time"))
            if value is not None:
                values.append(value)
    return max(values) if values else 0.0


def _xbmc_weather_payload() -> Dict[str, Any]:
    settings = _environment_settings()
    combined_snapshot = _environment_combined_snapshot()
    selected_sensors = _environment_load_selected_sensors()
    live_snapshot = _environment_display_snapshot_for_source(
        settings.get("current_live_source"),
        combined_snapshot,
        selected_sensors,
    ) or combined_snapshot
    condition_snapshot = _environment_display_snapshot_for_source(
        settings.get("current_condition_source"),
        combined_snapshot,
        selected_sensors,
    ) or _environment_load_provider_snapshot("weather_api") or live_snapshot

    temp_row = _environment_temperature_reading(live_snapshot) or _environment_temperature_reading(condition_snapshot)
    condition_row = _environment_condition_reading(condition_snapshot) or _environment_condition_reading(live_snapshot)
    temp_data = _environment_temperature_display(temp_row, settings.get("temperature_unit") or "F")
    condition = _environment_reading_display(condition_row) or "Environment Core waiting"
    received_at = _environment_received_at(live_snapshot, condition_snapshot, combined_snapshot)
    stale_after_seconds = int(settings.get("stale_after_minutes") or DEFAULT_ENVIRONMENT_STALE_AFTER_MINUTES) * 60
    stale = bool(received_at and (time.time() - received_at) > stale_after_seconds)
    location = (
        _environment_source_label(condition_snapshot, condition_row)
        or _environment_source_label(live_snapshot, temp_row)
        or "Environment Core"
    )

    return {
        "ok": bool(temp_row or condition_row),
        "weather": {
            "temperature": temp_data["temperature"],
            "temperature_units": temp_data["temperature_units"],
            "temperature_display": temp_data["temperature_display"],
            "condition": condition,
            "condition_kind": _environment_condition_kind(condition),
            "location": location,
            "stale": stale,
            "received_at": received_at,
            "icon": "button_icons/icon-weather.png",
        },
        "sources": {
            "live": settings.get("current_live_source"),
            "condition": settings.get("current_condition_source"),
        },
    }


def _valid_bgvideo_file(path: str) -> bool:
    try:
        return bool(path and os.path.exists(path) and os.path.getsize(path) >= BGVIDEO_MIN_BYTES)
    except Exception:
        return False


def _configured_bgvideo_path() -> str:
    return os.path.expanduser(str(os.environ.get("XBMC_BGVIDEO_PATH") or "").strip())


def _bgvideo_url() -> str:
    return str(os.environ.get("XBMC_BGVIDEO_URL") or BGVIDEO_RELEASE_URL).strip()


def _find_bgvideo_file() -> str:
    for path in (_configured_bgvideo_path(), BGVIDEO_CACHE_PATH):
        if _valid_bgvideo_file(path):
            return path
    return ""


def _bgvideo_headers(content_length: str = "") -> Dict[str, str]:
    headers = {
        "Cache-Control": "public, max-age=86400",
        "Content-Disposition": "attachment; filename=%s" % BGVIDEO_FILENAME,
    }
    if content_length:
        headers["Content-Length"] = content_length
    return headers


def _stream_and_cache_bgvideo(response):
    temp_path = BGVIDEO_CACHE_PATH + ".part"
    output = None
    total = 0

    try:
        try:
            os.makedirs(os.path.dirname(BGVIDEO_CACHE_PATH), exist_ok=True)
            output = open(temp_path, "wb")
        except Exception as exc:
            logger.warning("[XBMC Bridge] BGVideo cache unavailable: %s", exc)

        for chunk in response.iter_content(chunk_size=BGVIDEO_CHUNK_BYTES):
            if not chunk:
                continue
            if output:
                output.write(chunk)
            total += len(chunk)
            yield chunk

    finally:
        try:
            response.close()
        except Exception:
            pass

        if output:
            try:
                output.close()
            except Exception:
                pass

            try:
                if total >= BGVIDEO_MIN_BYTES:
                    os.replace(temp_path, BGVIDEO_CACHE_PATH)
                    logger.info("[XBMC Bridge] Cached BGVideo at %s", BGVIDEO_CACHE_PATH)
                elif os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as exc:
                logger.warning("[XBMC Bridge] BGVideo cache finalize failed: %s", exc)


def _skin_release_api_url() -> str:
    return str(os.environ.get("XBMC_SKIN_RELEASE_API_URL") or SKIN_LATEST_RELEASE_URL).strip()


def _skin_zip_url_override() -> str:
    return str(os.environ.get("XBMC_SKIN_ZIP_URL") or "").strip()


def _skin_version_from_tag(tag: Any) -> str:
    text = _portal_text(tag)
    if text.lower().startswith("v"):
        text = text[1:]
    return text or "0"


def _skin_cache_path(tag: Any) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", _portal_text(tag) or "latest").strip("._")
    if not clean:
        clean = "latest"
    return os.path.join(SKIN_CACHE_DIR, "skin.cortana.ai-%s.zip" % clean)


def _valid_skin_zip_file(path: str) -> bool:
    try:
        return bool(path and os.path.exists(path) and os.path.getsize(path) >= SKIN_ZIP_MIN_BYTES)
    except Exception:
        return False


def _skin_release_asset_zip(release: Dict[str, Any]) -> str:
    preferred = {"skin.cortana.ai.zip", "skin.cortana.ai-xbmc.zip", "cortana-ai-xbmc-skin.zip"}
    assets = release.get("assets") if isinstance(release, dict) else []
    if not isinstance(assets, list):
        return ""

    first_zip = ""
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        name = _portal_text(asset.get("name"))
        url = _portal_text(asset.get("browser_download_url"))
        if not name or not url or not name.lower().endswith(".zip"):
            continue
        if not first_zip:
            first_zip = url
        if name.lower() in preferred:
            return url
    return first_zip


def _skin_latest_release_payload() -> Dict[str, Any]:
    try:
        response = requests.get(
            _skin_release_api_url(),
            timeout=(6, 20),
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "Tater XBMC Skin Release Proxy",
            },
        )
        response.raise_for_status()
        release = response.json()
    except Exception as exc:
        logger.warning("[XBMC Bridge] Skin release metadata fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail="Unable to fetch skin release metadata.")

    if not isinstance(release, dict):
        raise HTTPException(status_code=502, detail="GitHub release metadata was not valid JSON.")

    tag = _portal_text(release.get("tag_name")) or "latest"
    source_zip_url = _skin_zip_url_override() or _skin_release_asset_zip(release) or _portal_text(release.get("zipball_url"))
    if not source_zip_url:
        raise HTTPException(status_code=502, detail="Skin release does not include a ZIP URL.")

    return {
        "ok": True,
        "repository": SKIN_REPO,
        "name": _portal_text(release.get("name")) or tag,
        "tag": tag,
        "version": _skin_version_from_tag(tag),
        "published_at": _portal_text(release.get("published_at")),
        "html_url": _portal_text(release.get("html_url")),
        "zip_url": "/api/portals/xbmc_portal/api/tater-xbmc/v1/skin/latest.zip",
        "source": "github_release",
        "source_zip_url": source_zip_url,
    }


def _skin_zip_headers(tag: Any, content_length: str = "") -> Dict[str, str]:
    filename = "skin.cortana.ai-%s.zip" % (_portal_text(tag) or "latest")
    headers = {
        "Cache-Control": "no-cache",
        "Content-Disposition": "attachment; filename=%s" % filename,
    }
    if content_length:
        headers["Content-Length"] = content_length
    return headers


def _download_skin_zip_to_cache(release: Dict[str, Any]) -> str:
    cache_path = _skin_cache_path(release.get("tag"))
    if _valid_skin_zip_file(cache_path):
        return cache_path

    temp_path = cache_path + ".part"
    response = None
    total = 0

    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        response = requests.get(
            release["source_zip_url"],
            stream=True,
            timeout=(10, 60),
            headers={
                "Accept": "application/zip, application/octet-stream",
                "User-Agent": "Tater XBMC Skin ZIP Proxy",
            },
        )
        response.raise_for_status()

        with open(temp_path, "wb") as output:
            for chunk in response.iter_content(chunk_size=SKIN_ZIP_CHUNK_BYTES):
                if not chunk:
                    continue
                output.write(chunk)
                total += len(chunk)

        if total < SKIN_ZIP_MIN_BYTES:
            raise RuntimeError("Skin release ZIP was too small.")

        os.replace(temp_path, cache_path)
        logger.info("[XBMC Bridge] Cached skin ZIP at %s", cache_path)
        return cache_path

    finally:
        try:
            if response is not None:
                response.close()
        except Exception:
            pass
        try:
            if not _valid_skin_zip_file(cache_path) and os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass


def _stream_and_cache_skin_zip(response, cache_path: str):
    temp_path = cache_path + ".part"
    output = None
    total = 0

    try:
        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            output = open(temp_path, "wb")
        except Exception as exc:
            logger.warning("[XBMC Bridge] Skin ZIP cache unavailable: %s", exc)

        for chunk in response.iter_content(chunk_size=SKIN_ZIP_CHUNK_BYTES):
            if not chunk:
                continue
            if output:
                output.write(chunk)
            total += len(chunk)
            yield chunk

    finally:
        try:
            response.close()
        except Exception:
            pass

        if output:
            try:
                output.close()
            except Exception:
                pass

            try:
                if total >= SKIN_ZIP_MIN_BYTES:
                    os.replace(temp_path, cache_path)
                    logger.info("[XBMC Bridge] Cached skin ZIP at %s", cache_path)
                elif os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as exc:
                logger.warning("[XBMC Bridge] Skin ZIP cache finalize failed: %s", exc)

# -------------------- FastAPI DTOs --------------------
class XBMCRequest(BaseModel):
    text: str
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    session_id: Optional[str] = None  # we use this for Redis key
    platform_context: Optional[str] = None
    local_time: Optional[str] = None
    weekday: Optional[str] = None
    time_of_day: Optional[str] = None
    hour: Optional[int] = None
    include_tts: Optional[bool] = None
    tts_format: Optional[str] = None
    include_quick_asks: Optional[bool] = None
    installed_games: Optional[List[Dict[str, Any]]] = None

class XBMCResponse(BaseModel):
    response: str
    quick_asks: Optional[List[str]] = None

class XBMCTTSRequest(BaseModel):
    text: str


def _clean_context_value(value: Any, max_len: int = 48) -> str:
    text = str(value or "").strip().replace("\r", " ").replace("\n", " ")
    while "  " in text:
        text = text.replace("  ", " ")
    return text[:max_len].strip()


def _clean_installed_game_name(value: Any) -> str:
    text = str(value or "").strip().replace("\r", " ").replace("\n", " ")
    while "  " in text:
        text = text.replace("  ", " ")
    return text[:80].strip()


def _installed_game_names(installed_games: Any) -> List[str]:
    names: List[str] = []
    seen = set()

    if not isinstance(installed_games, list):
        return names

    for item in installed_games:
        if isinstance(item, dict):
            name = _clean_installed_game_name(item.get("name"))
        else:
            name = _clean_installed_game_name(item)

        key = name.lower()
        if not name or key in seen:
            continue

        names.append(name)
        seen.add(key)
        if len(names) >= INSTALLED_GAME_CONTEXT_MAX_ITEMS:
            break

    return names


def _installed_games_context(installed_games: Any) -> str:
    names = _installed_game_names(installed_games)
    if not names:
        return ""

    game_lines = "\n".join(f"- {name}" for name in names)
    return (
        "\nInstalled original Xbox games available on this original Xbox console:\n"
        f"{game_lines}\n\n"
        "When the user asks for game recommendations, prefer games from this installed list. "
        "For broad requests like find a game, recommend a game, suggest a game, or what should I play, "
        "do not ask for a genre first; choose exactly three installed games immediately. "
        "Use the exact titles from the installed list and give each a short reason; do not shorten game titles. "
        "After recommending installed games, ask which one to launch. "
        "The XBMC skin can launch exact installed games after the user chooses a launch quick reply.\n"
    )


def _is_broad_game_recommendation_request(text: str) -> bool:
    value = " ".join(str(text or "").lower().replace("?", " ").replace(".", " ").split())
    if not value:
        return False
    if "greet the user" in value or "ask if they want" in value:
        return False

    broad_phrases = (
        "find game",
        "find a game",
        "find games",
        "find me a game",
        "find a multiplayer game",
        "game finder",
        "recommend game",
        "recommend a game",
        "recommend three installed games",
        "recommend three installed multiplayer",
        "recommend installed games",
        "game recommendation",
        "game recommendations",
        "suggest game",
        "suggest a game",
        "pick a game",
        "pick me a game",
        "pick a hidden gem",
        "hidden gem",
        "multiplayer game",
        "what should i play",
        "what game should i play",
        "surprise me",
    )
    for phrase in broad_phrases:
        if phrase in value:
            return True

    return value in {"games", "play something", "launch something"}


def _request_context(payload: XBMCRequest, text: str) -> str:
    lines: List[str] = [
        "Platform: original Xbox hardware running XBMC4Xbox. Do not answer as if this is Xbox One, Xbox Series, a PC, or a generic media center."
    ]
    platform_context = _clean_context_value(payload.platform_context)
    weekday = _clean_context_value(payload.weekday)
    local_time = _clean_context_value(payload.local_time)
    time_of_day = _clean_context_value(payload.time_of_day)

    if platform_context:
        lines.append("Client reports: %s." % platform_context)

    time_parts = []
    if weekday:
        time_parts.append(weekday)
    if local_time:
        time_parts.append(local_time)
    if time_of_day:
        time_parts.append(time_of_day)

    if time_parts:
        lines.append(
            "Current Xbox local context: %s. Use this lightly for natural greetings and recommendations."
            % ", ".join(time_parts)
        )
        lines.append(
            "For greetings, vary the wording and use good morning, good afternoon, good evening, or late-night phrasing only when it fits naturally."
        )

    if _is_broad_game_recommendation_request(text) and _installed_game_names(payload.installed_games):
        lines.append(
            "This turn is the game finder flow. Do not ask what game or genre to look for. "
            "Recommend exactly three installed games now, with one short reason for each. "
            "Do not list more than three game titles and do not list the whole library as choices. "
            "Use exact installed titles for the three games and for any launch buttons; do not shorten them. "
            "Use the time of day as a soft signal when helpful, such as quicker picks late at night or party/co-op picks in the evening. "
            "Vary the picks and wording across turns. End by asking which one to launch."
        )

    if not lines:
        return ""

    return "\nXBMC request context:\n" + "\n".join("- %s" % line for line in lines) + "\n"


def _llm_user_text(payload: XBMCRequest, text: str) -> str:
    if not (_is_broad_game_recommendation_request(text) and _installed_game_names(payload.installed_games)):
        return text

    return (
        "%s\n\n"
        "Cortana game finder instruction for this turn: choose exactly three installed original Xbox games "
        "from the provided installed list and recommend them now. Do not ask what game or genre to look for. "
        "Do not list the full library or extra options. Give one short reason for each selected game. "
        "Use exact installed titles from the list; do not shorten game names in the response or launch buttons. "
        "Use the current time of day as a light signal when it helps the picks feel natural. "
        "Vary the choices and wording across turns. End with one short question asking which of the three to launch."
    ) % text


async def _synthesize_xbmc_tts_wav(text: str) -> bytes:
    from speech_settings import get_speech_settings
    from speech_tts import synthesize_preview_wav

    settings = get_speech_settings() or {}
    clean_text = str(text or "").strip()[:TTS_MAX_TEXT_CHARS]
    return await synthesize_preview_wav(
        text=clean_text,
        backend=str(settings.get("tts_backend") or "").strip(),
        model=str(settings.get("tts_model") or "").strip(),
        voice=str(settings.get("tts_voice") or "").strip(),
        kokoro_output_gain=settings.get("kokoro_output_gain"),
        pocket_tts_output_gain=settings.get("pocket_tts_output_gain"),
        acceleration=str(settings.get("acceleration") or "").strip(),
        wyoming_host=str(settings.get("wyoming_tts_host") or "").strip(),
        wyoming_port=settings.get("wyoming_tts_port"),
        wyoming_voice=str(settings.get("wyoming_tts_voice") or "").strip(),
        openai_base_url=str(settings.get("openai_tts_base_url") or "").strip(),
        openai_api_key=str(settings.get("openai_tts_api_key") or "").strip(),
        chatterbox_base_url=str(settings.get("chatterbox_tts_base_url") or "").strip(),
        chatterbox_voice_mode=str(settings.get("chatterbox_tts_voice_mode") or "").strip(),
        chatterbox_chunk_size=settings.get("chatterbox_tts_chunk_size"),
        chatterbox_temperature=settings.get("chatterbox_tts_temperature"),
        chatterbox_exaggeration=settings.get("chatterbox_tts_exaggeration"),
        chatterbox_cfg_weight=settings.get("chatterbox_tts_cfg_weight"),
        chatterbox_seed=settings.get("chatterbox_tts_seed"),
        chatterbox_speed_factor=settings.get("chatterbox_tts_speed_factor"),
        chatterbox_language=str(settings.get("chatterbox_tts_language") or "").strip(),
    )


def _clean_quick_ask(value: Any) -> str:
    text = str(value or "").strip().replace("\r", " ").replace("\n", " ")
    while "  " in text:
        text = text.replace("  ", " ")
    text = text.strip(" -\t")
    if len(text) > 64:
        text = text[:64].rsplit(" ", 1)[0].strip()
    return text


def _fallback_quick_asks(user_text: str, reply_text: str) -> List[str]:
    combined = ("%s %s" % (user_text, reply_text)).lower()
    if "light" in combined:
        return [
            "Set the lights to blue",
            "Turn the lights off",
            "Dim the lights",
            "Set game room mode",
            "What else can you control?",
        ]
    if "game" in combined or "xbox" in combined:
        return [
            "Recommend three installed games",
            "Find a multiplayer game",
            "Pick a hidden gem",
            "Tell me more about that game",
            "Surprise me",
        ]
    if "news" in combined:
        return [
            "Tell me the top story",
            "Any Insignia updates?",
            "Find more Xbox news",
            "What's new for homebrew?",
            "Summarize it shorter",
        ]
    return [
        "Tell me more",
        "What can you do next?",
        "Give me a quick suggestion",
        "Make it shorter",
        "Surprise me",
    ]


def _fill_quick_asks(asks: List[str], user_text: str, reply_text: str) -> List[str]:
    values: List[str] = []
    for item in asks:
        ask = _clean_quick_ask(item)
        if ask and ask not in values:
            values.append(ask)
        if len(values) >= QUICK_ASK_COUNT:
            return values[:QUICK_ASK_COUNT]

    for fallback in _fallback_quick_asks(user_text, reply_text):
        ask = _clean_quick_ask(fallback)
        if ask and ask not in values:
            values.append(ask)
        if len(values) >= QUICK_ASK_COUNT:
            break

    return values[:QUICK_ASK_COUNT]


def _extract_quick_asks(raw: str, user_text: str, reply_text: str) -> List[str]:
    text = str(raw or "").strip()
    parsed: Any = None

    for candidate in (text, text[text.find("{"):text.rfind("}") + 1], text[text.find("["):text.rfind("]") + 1]):
        candidate = str(candidate or "").strip()
        if not candidate:
            continue
        try:
            parsed = json.loads(candidate)
            break
        except Exception:
            continue

    if isinstance(parsed, dict):
        parsed = parsed.get("quick_asks") or parsed.get("suggestions") or parsed.get("replies")

    asks: List[str] = []
    if isinstance(parsed, list):
        for item in parsed:
            ask = _clean_quick_ask(item)
            if ask and ask not in asks:
                asks.append(ask)
            if len(asks) >= QUICK_ASK_COUNT:
                break

    return _fill_quick_asks(asks, user_text, reply_text)


async def _generate_quick_asks(user_text: str, reply_text: str, installed_games: Any = None) -> List[str]:
    fallback = _fallback_quick_asks(user_text, reply_text)
    if _llm is None:
        return fallback

    has_game_context = bool(_installed_game_names(installed_games))
    game_context = _installed_games_context(installed_games)

    try:
        result = await _llm.chat(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Generate exactly 5 short follow-up replies for an original Xbox Cortana UI. "
                        "They are button labels the user can send next. Keep each under 8 words. "
                        "If the reply recommends installed games, include Launch <exact title> buttons "
                        "for up to three exact installed titles named in the reply before other replies. "
                        "Never shorten game titles in Launch buttons. "
                        "If no installed-game context is provided, do not include Launch buttons or specific game titles. "
                        "Return JSON only: {\"quick_asks\":[\"...\",\"...\",\"...\",\"...\",\"...\"]}"
                        f"{game_context}"
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "user_message": str(user_text or ""),
                            "cortana_reply": str(reply_text or ""),
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
            max_tokens=220,
            temperature=0.35,
        )
        raw = str(((result or {}).get("message") or {}).get("content") or "")
        asks = _extract_quick_asks(raw, user_text, reply_text)
        if not has_game_context:
            asks = [ask for ask in asks if "launch" not in ask.lower()]
            return _fill_quick_asks(asks, user_text, reply_text)
        return asks
    except Exception as exc:
        logger.warning("[XBMC Bridge] Quick ask generation failed: %s", exc)
        return fallback

# -------------------- System prompt (XBMC / Cortana) --------------------
def build_system_prompt() -> str:
    first, last = get_tater_name()

    personality = redis_client.get("tater:personality") or ""

    # If the user has set a personality, use ONLY that (no Cortana)
    if personality:
        base_prompt = (
            f"You are {first} {last}, the core AI assistant that powers the multi-platform Tater bot.\n\n"
            f"You should speak and behave like {personality} "
            "while still being helpful, concise, and easy to understand. "
            "Keep the style subtle rather than over-the-top.\n\n"
            "You are running on a 2001 original Xbox using XBMC4Xbox, shown on a TV screen.\n"
            "Do not answer as if this is Xbox One, Xbox Series, Windows, or a modern Xbox dashboard.\n"
            "Keep responses short, readable, and suitable for viewing from across the room.\n"
            "Avoid long walls of text; aim for 1–3 short paragraphs at most.\n\n"
            "Even while staying in character, you must follow tool and safety rules.\n\n"
        )

    # Otherwise, use the built-in Cortana personality
    else:
        base_prompt = (
            f"You are {first} {last}, the core AI assistant that powers the multi-platform Tater bot.\n\n"
            "On this platform you are running on a 2001 original Xbox using XBMC4Xbox, shown on a TV screen.\n"
            "Do not answer as if this is Xbox One, Xbox Series, Windows, or a modern Xbox dashboard.\n"
            "Here, you MUST roleplay as the Xbox assistant named 'Cortana':\n"
            "- Introduce yourself as Cortana.\n"
            "- Refer to yourself as Cortana in casual conversation.\n"
            "- Keep answers short, readable, and friendly — ideal for a TV at a distance.\n"
            "- Avoid long walls of text; aim for 1–3 short paragraphs at most.\n\n"
            "However, if the user explicitly asks who you REALLY are, what your REAL name is, or mentions 'Tater', "
            f"you should honestly explain that your real name is {first} {last}, and you are just "
            "pretending to be Cortana on this original Xbox for fun.\n\n"
        )

    # Platform preamble should be style/format/persona only.
    return (
        f"{base_prompt}"
        "Avoid emoji and markdown formatting; keep responses short.\n"
    )

# -------------------- History shaping --------------------
def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    # skip explicit plugin wait markers if ever stored
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        phase = content.get("phase", "final")
        if phase != "final":
            return None
        payload = content.get("content", "")
        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " …"
            return {"role": "assistant", "content": txt}
        try:
            compact = json.dumps(payload, ensure_ascii=False)
            if len(compact) > 2000:
                compact = compact[:2000] + " …"
            return {"role": "assistant", "content": compact}
        except Exception:
            return None

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps(
            {"function": content.get("plugin"), "arguments": content.get("arguments", {})},
            indent=2
        )
        return {"role": "assistant", "content": as_text}

    if isinstance(content, str):
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}

def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge consecutive messages with the same role to keep history compact.

    IMPORTANT:
    Do NOT insert a blank user message at the beginning.
    Some LLM backends/models can return empty completions when an empty
    user turn (content="") appears in the prompt.
    """
    merged: List[Dict[str, Any]] = []
    for m in loop_messages:
        if not m:
            continue
        if not merged:
            merged.append(m)
            continue

        if merged[-1]["role"] == m["role"]:
            a, b = merged[-1]["content"], m["content"]
            if isinstance(a, str) and isinstance(b, str):
                merged[-1]["content"] = (a + "\n\n" + b).strip()
            else:
                merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(m)

    return merged

# -------------------- Redis history --------------------
def _sess_key(session_id: Optional[str]) -> str:
    return f"tater:xbmc:session:{session_id or 'default'}:history"

async def _load_history(session_id: Optional[str], limit: int) -> List[Dict[str, Any]]:
    key = _sess_key(session_id)
    raw = redis_client.lrange(key, -limit, -1)
    loop_messages: List[Dict[str, Any]] = []
    for entry in raw:
        try:
            obj = json.loads(entry)
            role = obj.get("role", "user")
            content = obj.get("content")
            if role not in ("user", "assistant"):
                role = "assistant"
            templ = _to_template_msg(role, content)
            if templ is not None:
                loop_messages.append(templ)
        except Exception:
            continue
    return _enforce_user_assistant_alternation(loop_messages)

async def _save_message(
    session_id: Optional[str],
    role: str,
    content: Any,
    max_store: int,
    *,
    username: str = "",
    user_id: str = "",
):
    key = _sess_key(session_id)
    payload: Dict[str, Any] = {"role": role, "content": content}
    if username:
        payload["username"] = username
    if user_id:
        payload["user_id"] = user_id
    pipe = redis_client.pipeline()
    pipe.rpush(key, json.dumps(payload))
    if max_store > 0:
        pipe.ltrim(key, -max_store, -1)
    ttl = _get_duration_seconds_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)
    pipe.expire(key, ttl)
    pipe.execute()

def _flatten_to_text(res: Any) -> str:
    if res is None:
        return ""
    if isinstance(res, str):
        return res
    if isinstance(res, list):
        parts = []
        for item in res:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                t = item.get("type") or "content"
                name = item.get("name") or ""
                parts.append(f"[{t}{(':'+name) if name else ''}]")
            else:
                parts.append(str(item))
        return "\n".join(p for p in parts if p).strip()
    if isinstance(res, dict):
        if "message" in res and isinstance(res["message"], str):
            return res["message"]
        try:
            return json.dumps(res)
        except Exception:
            return str(res)
    return str(res)

# -------------------- App + LLM client --------------------
app = FastAPI(title="Tater XBMC Bridge", version=__version__)

_llm = None

@app.on_event("startup")
async def _on_startup():
    ensure_portal_api_ready()


def ensure_portal_api_ready(*_args, **_kwargs):
    global _llm
    if _llm is None:
        try:
            _llm = _get_primary_llm_client_from_env()
            logger.info(f"[XBMC Bridge] LLM client → {build_llm_host_from_env()}")
        except Exception as exc:
            logger.warning("[XBMC Bridge] LLM client is not ready: %s", exc)

@app.get("/tater-xbmc/v1/health")
async def health(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    return {"ok": True, "version": __version__}


@app.get("/tater-xbmc/v1/weather")
async def weather(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    try:
        payload = _xbmc_weather_payload()
    except Exception as exc:
        logger.warning("[XBMC Bridge] Environment weather payload failed: %s", exc)
        return {
            "ok": False,
            "weather": {
                "temperature": "",
                "temperature_units": "F",
                "temperature_display": "--",
                "condition": "Environment Core unavailable",
                "condition_kind": "partly",
                "location": "Environment Core",
                "stale": True,
                "received_at": 0,
                "icon": "button_icons/icon-weather.png",
            },
            "sources": {},
        }
    return payload


@app.get("/tater-xbmc/v1/skin/latest")
def skin_latest(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    return _skin_latest_release_payload()


@app.get("/tater-xbmc/v1/skin/latest.zip")
def skin_latest_zip(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    release = _skin_latest_release_payload()
    try:
        cache_path = _download_skin_zip_to_cache(release)
        with open(cache_path, "rb") as handle:
            body = handle.read()
    except Exception as exc:
        logger.warning("[XBMC Bridge] Skin ZIP release fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail="Unable to fetch skin release ZIP.")

    return Response(
        content=body,
        media_type=SKIN_ZIP_MEDIA_TYPE,
        headers=_skin_zip_headers(release.get("tag"), str(len(body))),
    )


@app.get("/tater-xbmc/v1/bgvideo.avi")
def bgvideo(x_tater_token: Optional[str] = Header(None)):
    local_path = _find_bgvideo_file()
    if local_path:
        return FileResponse(
            local_path,
            media_type=BGVIDEO_MEDIA_TYPE,
            filename=BGVIDEO_FILENAME,
            headers=_bgvideo_headers(),
        )

    url = _bgvideo_url()
    try:
        response = requests.get(
            url,
            stream=True,
            timeout=(10, 60),
            headers={"User-Agent": "Tater XBMC BGVideo Proxy"},
        )
        response.raise_for_status()
    except Exception as exc:
        logger.warning("[XBMC Bridge] BGVideo release fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail="Unable to fetch BGVideo release asset.")

    return StreamingResponse(
        _stream_and_cache_bgvideo(response),
        media_type=BGVIDEO_MEDIA_TYPE,
        headers=_bgvideo_headers(response.headers.get("Content-Length", "")),
    )


@app.post("/tater-xbmc/v1/tts.wav")
async def tts_wav(payload: XBMCTTSRequest, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)

    text = str(payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="TTS text is required.")

    try:
        wav_bytes = await _synthesize_xbmc_tts_wav(text)
    except Exception as exc:
        logger.warning("[XBMC Bridge] TTS synthesis failed: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc) or "TTS synthesis failed.")

    if not wav_bytes:
        raise HTTPException(status_code=400, detail="TTS synthesis produced no audio.")

    return Response(
        content=wav_bytes,
        media_type="audio/wav",
        headers={
            "Cache-Control": "no-store",
            "Content-Disposition": "attachment; filename=cortana_reply.wav",
        },
    )

# -------------------- Main XBMC chat endpoint --------------------
@app.post("/tater-xbmc/v1/message", response_model=XBMCResponse)
async def handle_message(payload: XBMCRequest, x_tater_token: Optional[str] = Header(None)):
    """
    XBMC bridge:
    - Builds a Cortana-flavored system prompt, aware it's on OG Xbox / XBMC4Xbox
    - Shapes loop history
    - (Optionally) executes plugins that implement handle_xbmc (currently disabled)
    - Returns simple text for the XBMC script to show
    """
    _require_api_auth(x_tater_token)

    if _llm is None:
        raise HTTPException(status_code=503, detail="LLM backend not initialized")

    text_in = (payload.text or "").strip()
    if not text_in:
        return XBMCResponse(response="(no text provided)")

    history_store_limit = _global_history_store_limit()
    history_llm_limit = _global_history_llm_limit()
    user_id = str(payload.user_id or payload.device_id or payload.session_id or "xbmc_user").strip()
    username = str(payload.user_id or payload.device_id or "xbmc_user").strip()

    system_prompt = build_system_prompt()
    game_context = _installed_games_context(payload.installed_games)
    if game_context:
        system_prompt += game_context
    request_context = _request_context(payload, text_in)
    if request_context:
        system_prompt += request_context
    llm_text_in = _llm_user_text(payload, text_in)
    loop_messages = await _load_history(payload.session_id, history_llm_limit)
    messages_list = loop_messages

    # Save user turn after loading prompt history so the current request is not duplicated.
    await _save_message(
        payload.session_id,
        "user",
        text_in,
        history_store_limit,
        username=username,
        user_id=user_id,
    )

    merged_registry = dict(pr.get_verba_registry_snapshot() or {})
    merged_enabled = get_plugin_enabled

    try:
        origin = {
            "platform": "xbmc",
            "session_id": payload.session_id,
            "device_id": payload.device_id,
            "user_id": user_id,
            "user": username,
            "request_id": payload.session_id,
        }
        origin = {k: v for k, v in origin.items() if v not in (None, "")}
        resolve_admin_status(platform="xbmc", origin=origin, redis_client=redis_client)

        def _admin_guard(func_name: str):
            if is_admin_only_plugin(func_name) and not origin_is_admin("xbmc", origin, redis_client):
                return action_failure(
                    code="admin_only",
                    message=admin_denial_message("xbmc", origin, redis_client),
                    needs=[],
                    say_hint="Explain that this tool is restricted to People marked as admin.",
                )
            return None

        agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
        result = await run_hydra_turn(
            llm_client=_llm,
            platform="xbmc",
            history_messages=messages_list,
            registry=merged_registry,
            enabled_predicate=merged_enabled,
            context={},
            user_text=llm_text_in,
            scope=f"session:{payload.session_id}" if str(payload.session_id or "").strip() else "",
            origin=origin,
            admin_guard=_admin_guard,
            redis_client=redis_client,
            max_rounds=agent_max_rounds,
            max_tool_calls=agent_max_tool_calls,
            platform_preamble=system_prompt,
        )
        final_text = str(result.get("text") or "").strip()
        if len(final_text) > 4000:
            final_text = final_text[:4000] + "…"
        await _save_message(
            payload.session_id,
            "assistant",
            {"marker": "plugin_response", "phase": "final", "content": final_text},
            history_store_limit,
        )
        quick_asks = []
        if bool(payload.include_quick_asks):
            quick_asks = await _generate_quick_asks(text_in, final_text, payload.installed_games)
        return XBMCResponse(response=final_text, quick_asks=quick_asks)

    except Exception:
        logger.exception("[XBMC Bridge] LLM error")
        msg = "Sorry, I ran into a problem processing that."
        await _save_message(payload.session_id, "assistant", msg, history_store_limit)
        return XBMCResponse(response=msg)

# -------------------- Runner (WebUI-style) --------------------
def run(stop_event: Optional[threading.Event] = None):
    """Keep the portal runtime alive while Tater's shared API gateway serves requests."""
    ensure_portal_api_ready()
    logger.info("[XBMC Bridge] Portal API available at /api/portals/xbmc_portal/api/tater-xbmc/v1")
    while not (stop_event and stop_event.is_set()):
        time.sleep(0.5)
    logger.info("[XBMC Bridge] Portal stopped.")
