# cores/voice_core.py
import json
import os
import asyncio
import logging
import threading
import time
import uuid
import base64
import contextlib
import inspect
import importlib
import socket
import math
import audioop
import io
import wave
from typing import Optional, Dict, Any, List, Tuple, Callable, cast

from fastapi import FastAPI, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field
import uvicorn
import requests
import re
import aiohttp

from helpers import (
    get_llm_client_from_env,
    redis_client,
)
import verba_registry as pr
from hydra import run_hydra_turn, resolve_agent_limits

try:
    from wyoming.client import AsyncTcpClient
    from wyoming.asr import Transcribe, Transcript
    from wyoming.tts import Synthesize
    from wyoming.audio import AudioStart as WyomingAudioStart, AudioChunk as WyomingAudioChunk, AudioStop as WyomingAudioStop
    from wyoming.error import Error as WyomingError
    try:
        from wyoming.tts import SynthesizeVoice  # optional in older wyoming packages
    except Exception:
        SynthesizeVoice = None
    try:
        from wyoming.info import Describe, Info  # optional for voice catalog discovery
    except Exception:
        Describe = None
        Info = None
    WYOMING_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - import guard for deployments without wyoming package
    AsyncTcpClient = None
    Transcribe = None
    Transcript = None
    Synthesize = None
    SynthesizeVoice = None
    Describe = None
    Info = None
    WyomingAudioStart = None
    WyomingAudioChunk = None
    WyomingAudioStop = None
    WyomingError = None
    WYOMING_IMPORT_ERROR = str(exc)

from dotenv import load_dotenv
__version__ = "2.0.43"

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("voice_core")

# -------------------- Platform defaults (overridable in WebUI) --------------------
BIND_HOST = "0.0.0.0"
TIMEOUT_SECONDS = 60  # LLM request timeout in seconds

# Shared chat-history defaults from main Hydra/WebUI settings
DEFAULT_GLOBAL_MAX_STORE = 20
DEFAULT_GLOBAL_MAX_LLM = 8
DEFAULT_SESSION_TTL_SECONDS = 2 * 60 * 60  # 2h

# Continued chat (auto follow-up) defaults
DEFAULT_CONTINUED_CHAT_ENABLED = False

# Follow-up defaults
DEFAULT_FOLLOWUP_IDLE_TIMEOUT_S = 12.0
DEFAULT_SATELLITE_MAP_CACHE_TTL_S = 3600  # 1h

# Cache keys
REDIS_SATELLITE_MAP_KEY = "tater:ha:assist_satellite_map:v2"  # json map: device_id -> entity_id
REDIS_VOICE_SATELLITE_REGISTRY_KEY = "tater:voice:satellites:registry:v1"
REDIS_VOICE_SATELLITE_BLOCKED_KEY = "tater:voice:satellites:blocked:v1"
REDIS_WYOMING_TTS_VOICES_KEY = "tater:voice:wyoming:tts_voices:v1"
REDIS_WYOMING_TTS_VOICES_META_KEY = "tater:voice:wyoming:tts_voices:meta:v1"
VOICE_CORE_SETTINGS_HASH_KEY = "voice_core_settings"
VOICE_CORE_LEGACY_SETTINGS_HASH_KEYS = ("homeassistant_portal_settings",)

VOICE_BACKEND_MODE_HA = "homeassistant_bridge"
VOICE_BACKEND_MODE_NATIVE = "native_voice_pipeline"
VOICE_BACKEND_MODE_OPTIONS = [VOICE_BACKEND_MODE_HA, VOICE_BACKEND_MODE_NATIVE]
VOICE_MULTISELECT_FIELDS: set[str] = set()

DEFAULT_VOICE_DISCOVERY_SCAN_SECONDS = 45
DEFAULT_VOICE_DISCOVERY_MDNS_TIMEOUT_S = 3.0
DEFAULT_WYOMING_STT_HOST = "127.0.0.1"
DEFAULT_WYOMING_STT_PORT = 10300
DEFAULT_WYOMING_TTS_HOST = "127.0.0.1"
DEFAULT_WYOMING_TTS_PORT = 10200
DEFAULT_WYOMING_TTS_VOICE = ""
DEFAULT_VOICE_SAMPLE_RATE_HZ = 16000
DEFAULT_VOICE_SAMPLE_WIDTH = 2
DEFAULT_VOICE_CHANNELS = 1
DEFAULT_VOICE_SESSION_TTL_SECONDS = 300
DEFAULT_NATIVE_MAX_AUDIO_BYTES = 4 * 1024 * 1024
DEFAULT_NATIVE_WYOMING_TIMEOUT_SECONDS = 45
DEFAULT_COMPAT_EVENT_BACKLOG = 200
DEFAULT_COMPAT_TCP_HOST = "0.0.0.0"
DEFAULT_COMPAT_TCP_PORT = 9766
DEFAULT_VOICE_CORE_BIND_PORT = 8797
DEFAULT_ESPHOME_API_PORT = 6053
DEFAULT_ESPHOME_CONNECT_TIMEOUT_S = 12.0
DEFAULT_ESPHOME_RETRY_SECONDS = 15
DEFAULT_ESPHOME_TTS_CHUNK_BYTES = 3200
DEFAULT_ESPHOME_AUDIO_IDLE_TIMEOUT_S = 1.6
DEFAULT_ESPHOME_SESSION_MAX_LISTEN_SECONDS = 15.0
DEFAULT_ESPHOME_NO_VOICE_TIMEOUT_S = 8.0
DEFAULT_ESPHOME_SERVER_VAD_ENABLED = True
DEFAULT_ESPHOME_SERVER_VAD_THRESHOLD_DBFS = -50.0
DEFAULT_ESPHOME_SERVER_VAD_SILENCE_SECONDS = 0.45
DEFAULT_ESPHOME_SERVER_VAD_MIN_SPEECH_CHUNKS = 10
DEFAULT_ESPHOME_SERVER_VAD_MIN_SPEECH_SECONDS = 0.35
DEFAULT_ESPHOME_SERVER_VAD_DROP_DB = 14.0
DEFAULT_ESPHOME_SERVER_VAD_TRIGGER_MARGIN_DB = 2.0
DEFAULT_ESPHOME_SERVER_VAD_RELEASE_MARGIN_DB = 1.5
DEFAULT_ESPHOME_SERVER_VAD_STRONG_MARGIN_DB = 14.0
DEFAULT_ESPHOME_SERVER_VAD_STRONG_STREAK_CHUNKS = 6
DEFAULT_ESPHOME_SERVER_VAD_WARMUP_SECONDS = 0.55
DEFAULT_ESPHOME_SERVER_VAD_MAX_RELEASE_ABOVE_FLOOR_DB = 5.0
DEFAULT_ESPHOME_ANNOUNCEMENT_TIMEOUT_S = 20.0
DEFAULT_ESPHOME_TTS_URL_TTL_S = 180
DEFAULT_ESPHOME_TTS_TRIM_ENABLED = True
DEFAULT_ESPHOME_TTS_TRIM_THRESHOLD_DBFS = -52.0
DEFAULT_ESPHOME_TTS_TRIM_LEAD_MS = 40
DEFAULT_ESPHOME_TTS_TRIM_TAIL_MS = 620
DEFAULT_ESPHOME_UDP_TTS_SAMPLES_PER_CHUNK = 512

VOICE_STATE_IDLE = "idle"
VOICE_STATE_LISTENING = "listening"
VOICE_STATE_THINKING = "thinking"
VOICE_STATE_SPEAKING = "speaking"
VOICE_STATE_ERROR = "error"


class NoTranscriptError(RuntimeError):
    """Raised when STT completes without usable transcript text."""

CORE_SETTINGS = {
    "category": "Voice Core Settings",
    "required": {
        "bind_port": {
            "label": "Voice Core API Port",
            "type": "number",
            "default": DEFAULT_VOICE_CORE_BIND_PORT,
            "description": "TCP port for the Voice Core API service.",
        },
        "VOICE_DISCOVERY_MDNS_TIMEOUT_S": {
            "label": "mDNS Discovery Window (sec)",
            "type": "number",
            "default": DEFAULT_VOICE_DISCOVERY_MDNS_TIMEOUT_S,
            "description": "How long each manual discovery run listens for ESPHome mDNS service announcements.",
        },
        "VOICE_WYOMING_STT_HOST": {
            "label": "Wyoming STT Host",
            "type": "string",
            "default": DEFAULT_WYOMING_STT_HOST,
            "description": "Host running the Wyoming STT service (for example Whisper).",
        },
        "VOICE_WYOMING_STT_PORT": {
            "label": "Wyoming STT Port",
            "type": "number",
            "default": DEFAULT_WYOMING_STT_PORT,
            "description": "TCP port for Wyoming STT.",
        },
        "VOICE_WYOMING_TTS_HOST": {
            "label": "Wyoming TTS Host",
            "type": "string",
            "default": DEFAULT_WYOMING_TTS_HOST,
            "description": "Host running the Wyoming TTS service (for example Piper).",
        },
        "VOICE_WYOMING_TTS_PORT": {
            "label": "Wyoming TTS Port",
            "type": "number",
            "default": DEFAULT_WYOMING_TTS_PORT,
            "description": "TCP port for Wyoming TTS.",
        },
        "VOICE_WYOMING_TTS_VOICE": {
            "label": "Wyoming TTS Voice",
            "type": "select",
            "options": [],
            "default": DEFAULT_WYOMING_TTS_VOICE,
            "description": "Voice to request from Wyoming TTS. Use Refresh TTS Voices to load available voices from Piper.",
        },
        "VOICE_NATIVE_DEBUG": {
            "label": "Native Voice Debug Logs",
            "type": "select",
            "options": ["true", "false"],
            "default": "false",
            "description": "Enable extra logging for native voice pipeline compatibility behavior.",
        },
        "VOICE_ESPHOME_API_PORT": {
            "label": "ESPHome API Port",
            "type": "number",
            "default": DEFAULT_ESPHOME_API_PORT,
            "description": "Default API port for ESPHome device connections.",
        },
        "VOICE_ESPHOME_PASSWORD": {
            "label": "ESPHome API Password",
            "type": "password",
            "default": "",
            "description": "ESPHome native API password for devices that require it.",
        },
        "VOICE_ESPHOME_NOISE_PSK": {
            "label": "ESPHome Noise PSK",
            "type": "password",
            "default": "",
            "description": "Optional Noise PSK for encrypted ESPHome native sessions.",
        },
        "VOICE_ESPHOME_CONNECT_TIMEOUT_S": {
            "label": "ESPHome Connect Timeout (sec)",
            "type": "number",
            "default": DEFAULT_ESPHOME_CONNECT_TIMEOUT_S,
            "description": "Timeout when opening ESPHome native API connections.",
        },
        "VOICE_ESPHOME_RETRY_SECONDS": {
            "label": "ESPHome Retry Interval (sec)",
            "type": "number",
            "default": DEFAULT_ESPHOME_RETRY_SECONDS,
            "description": "Retry delay between ESPHome connection attempts.",
        },
    }
}

CORE_WEBUI_TAB = {
    "label": "Voice",
    "order": 20,
    "requires_running": False,
}

# --- Duration parsing (supports "5m", "2h", "24h", or raw seconds like "7200") ---
def _parse_duration_seconds(val: str, default_seconds: int) -> int:
    if val is None:
        return default_seconds
    s = str(val).strip().lower()
    # raw integer seconds?
    try:
        return int(s)
    except ValueError:
        pass
    m = re.match(r"^\s*(\d+)\s*([smhd])\s*$", s)
    if not m:
        return default_seconds
    num = int(m.group(1))
    unit = m.group(2)
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return num * mult

def _voice_core_settings() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for legacy_key in VOICE_CORE_LEGACY_SETTINGS_HASH_KEYS:
        try:
            payload = redis_client.hgetall(legacy_key) or {}
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            out.update({str(k): str(v) for k, v in payload.items()})
    try:
        payload = redis_client.hgetall(VOICE_CORE_SETTINGS_HASH_KEY) or {}
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        out.update({str(k): str(v) for k, v in payload.items()})
    return out


def _portal_settings() -> Dict[str, str]:
    # Backward-compatible alias while this module transitions from portal to core.
    return _voice_core_settings()

def _get_duration_seconds_setting(name: str, default_seconds: int) -> int:
    s = _portal_settings().get(name)
    return _parse_duration_seconds(s, default_seconds)

def _get_int_platform_setting(name: str, default: int) -> int:
    s = _portal_settings().get(name)
    try:
        return int(str(s).strip()) if s is not None and str(s).strip() != "" else default
    except Exception:
        return default

def _get_float_platform_setting(name: str, default: float) -> float:
    s = _portal_settings().get(name)
    try:
        return float(str(s).strip()) if s is not None and str(s).strip() != "" else default
    except Exception:
        return default

def _get_bool_platform_setting(name: str, default: bool) -> bool:
    s = _portal_settings().get(name)
    if s is None:
        return default
    v = str(s).strip().lower()
    if v in ("1", "true", "yes", "y", "on", "enabled"):
        return True
    if v in ("0", "false", "no", "n", "off", "disabled"):
        return False
    return default


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _normalize_csv_or_lines(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        parts = [str(item or "").strip() for item in raw]
    else:
        text = str(raw or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            parts = [str(item or "").strip() for item in parsed]
        else:
            parts = [part.strip() for part in re.split(r"[\n,]+", text)]
    out: List[str] = []
    seen = set()
    for part in parts:
        token = str(part or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def _parse_json_string_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        return _normalize_csv_or_lines(list(raw))
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None
    if isinstance(parsed, list):
        return _normalize_csv_or_lines(parsed)
    return _normalize_csv_or_lines(text)


def _coerce_webui_multiselect_json(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    values = _parse_json_string_list(raw)
    return json.dumps(values, ensure_ascii=False)


def _json_dumps_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _wyoming_tts_voice_selection(raw: Any) -> Dict[str, str]:
    token = _text(raw)
    if not token:
        return {}

    parsed: Any = None
    with contextlib.suppress(Exception):
        parsed = json.loads(token)
    if isinstance(parsed, dict):
        out: Dict[str, str] = {}
        for key in ("name", "language", "speaker"):
            value = _text(parsed.get(key))
            if value:
                out[key] = value
        return out

    if "::" in token:
        name, speaker = token.split("::", 1)
        name = _text(name)
        speaker = _text(speaker)
        out = {}
        if name:
            out["name"] = name
        if speaker:
            out["speaker"] = speaker
        return out

    return {"name": token}


def _wyoming_tts_voice_selection_value(selection: Dict[str, Any]) -> str:
    normalized: Dict[str, str] = {}
    for key in ("name", "language", "speaker"):
        value = _text((selection or {}).get(key))
        if value:
            normalized[key] = value
    if not normalized:
        return ""
    return _json_dumps_compact(normalized)


def _wyoming_tts_voice_selection_label(selection: Dict[str, Any]) -> str:
    voice_name = _text((selection or {}).get("name"))
    language = _text((selection or {}).get("language"))
    speaker = _text((selection or {}).get("speaker"))
    if voice_name and speaker:
        return f"{voice_name} ({speaker})"
    if voice_name:
        return voice_name
    if language and speaker:
        return f"{language} ({speaker})"
    if language:
        return language
    return "Default"


def _load_wyoming_tts_voice_catalog() -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    global _wyoming_tts_voice_catalog_mem, _wyoming_tts_voice_catalog_meta_mem
    try:
        raw_rows = redis_client.get(REDIS_WYOMING_TTS_VOICES_KEY)
        raw_meta = redis_client.hgetall(REDIS_WYOMING_TTS_VOICES_META_KEY) or {}
    except Exception:
        return list(_wyoming_tts_voice_catalog_mem), dict(_wyoming_tts_voice_catalog_meta_mem)

    rows: List[Dict[str, str]] = []
    try:
        parsed = json.loads(raw_rows) if raw_rows else []
    except Exception:
        parsed = []
    if isinstance(parsed, list):
        for row in parsed:
            if not isinstance(row, dict):
                continue
            value = _text(row.get("value"))
            label = _text(row.get("label"))
            if not value:
                continue
            rows.append({"value": value, "label": label or value})

    meta: Dict[str, Any] = dict(_wyoming_tts_voice_catalog_meta_mem)
    if isinstance(raw_meta, dict):
        meta["host"] = _text(raw_meta.get("host"))
        with contextlib.suppress(Exception):
            meta["port"] = int(raw_meta.get("port") or 0)
        with contextlib.suppress(Exception):
            meta["count"] = int(raw_meta.get("count") or len(rows))
        with contextlib.suppress(Exception):
            meta["last_refresh_ts"] = float(raw_meta.get("last_refresh_ts") or 0.0)
        meta["last_error"] = _text(raw_meta.get("last_error"))

    _wyoming_tts_voice_catalog_mem = list(rows)
    _wyoming_tts_voice_catalog_meta_mem = dict(meta)
    return rows, meta


def _save_wyoming_tts_voice_catalog(rows: List[Dict[str, str]], *, host: str, port: int, error: str = "") -> None:
    global _wyoming_tts_voice_catalog_mem, _wyoming_tts_voice_catalog_meta_mem
    clean_rows: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = _text(row.get("value"))
        label = _text(row.get("label")) or value
        if not value:
            continue
        clean_rows.append({"value": value, "label": label})

    now = _native_now()
    meta = {
        "host": _text(host),
        "port": int(port or 0),
        "count": len(clean_rows),
        "last_refresh_ts": now,
        "last_error": _text(error),
    }
    _wyoming_tts_voice_catalog_mem = list(clean_rows)
    _wyoming_tts_voice_catalog_meta_mem = dict(meta)

    with contextlib.suppress(Exception):
        redis_client.set(REDIS_WYOMING_TTS_VOICES_KEY, _json_dumps_compact(clean_rows))
        redis_client.hset(
            REDIS_WYOMING_TTS_VOICES_META_KEY,
            mapping={
                "host": meta["host"],
                "port": str(meta["port"]),
                "count": str(meta["count"]),
                "last_refresh_ts": str(meta["last_refresh_ts"]),
                "last_error": meta["last_error"],
            },
        )


def _wyoming_tts_voice_option_rows(*, current_value: Any) -> List[Dict[str, str]]:
    rows, _meta = _load_wyoming_tts_voice_catalog()
    options: List[Dict[str, str]] = [{"value": "", "label": "Default (service default voice)"}]
    seen = {""}
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = _text(row.get("value"))
        label = _text(row.get("label")) or value
        if not value or value in seen:
            continue
        seen.add(value)
        options.append({"value": value, "label": label})

    current_token = _text(current_value)
    if current_token and current_token not in seen:
        selection = _wyoming_tts_voice_selection(current_token)
        label = _wyoming_tts_voice_selection_label(selection) if selection else current_token
        options.append({"value": current_token, "label": f"{label} (saved)"})

    return options

def _voice_backend_mode() -> str:
    return VOICE_BACKEND_MODE_NATIVE


def _voice_pipeline_config_snapshot() -> Dict[str, Any]:
    settings = _portal_settings()
    stt_host = _text(settings.get("VOICE_WYOMING_STT_HOST")) or DEFAULT_WYOMING_STT_HOST
    tts_host = _text(settings.get("VOICE_WYOMING_TTS_HOST")) or DEFAULT_WYOMING_TTS_HOST
    stt_port = _get_int_platform_setting("VOICE_WYOMING_STT_PORT", DEFAULT_WYOMING_STT_PORT)
    tts_port = _get_int_platform_setting("VOICE_WYOMING_TTS_PORT", DEFAULT_WYOMING_TTS_PORT)
    tts_voice = _text(settings.get("VOICE_WYOMING_TTS_VOICE")) or DEFAULT_WYOMING_TTS_VOICE
    return {
        "backend_mode": _voice_backend_mode(),
        "discovery_enabled": False,
        "discovery_scan_seconds": 0,
        "discovery_exclude_ha_assist": False,
        "satellite_targets": _parse_json_string_list(settings.get("VOICE_SATELLITE_TARGETS")),
        "manual_targets": _normalize_csv_or_lines(settings.get("VOICE_MANUAL_TARGETS")),
        "sensor_entity_ids": _parse_json_string_list(settings.get("VOICE_SENSOR_ENTITY_IDS")),
        "eou_mode": _text(settings.get("VOICE_EOU_MODE")) or "device",
        "sample_rate_hz": _get_int_platform_setting("VOICE_STREAM_SAMPLE_RATE_HZ", DEFAULT_VOICE_SAMPLE_RATE_HZ),
        "sample_width_bytes": DEFAULT_VOICE_SAMPLE_WIDTH,
        "channels": DEFAULT_VOICE_CHANNELS,
        "native_session_ttl_s": _get_int_platform_setting("VOICE_NATIVE_SESSION_TTL_S", DEFAULT_VOICE_SESSION_TTL_SECONDS),
        "native_max_audio_bytes": _get_int_platform_setting(
            "VOICE_NATIVE_MAX_AUDIO_BYTES",
            DEFAULT_NATIVE_MAX_AUDIO_BYTES,
        ),
        "wyoming_timeout_s": _get_float_platform_setting(
            "VOICE_NATIVE_WYOMING_TIMEOUT_S",
            DEFAULT_NATIVE_WYOMING_TIMEOUT_SECONDS,
        ),
        "compat_bridge_enabled": _get_bool_platform_setting("VOICE_COMPAT_ENABLED", True),
        "compat_require_adopted": _get_bool_platform_setting("VOICE_COMPAT_REQUIRE_ADOPTED", False),
        "compat_event_backlog": _get_int_platform_setting("VOICE_COMPAT_EVENT_BACKLOG", DEFAULT_COMPAT_EVENT_BACKLOG),
        "compat_tcp_enabled": _get_bool_platform_setting("VOICE_COMPAT_TCP_ENABLED", False),
        "compat_tcp_host": _text(settings.get("VOICE_COMPAT_TCP_HOST")) or DEFAULT_COMPAT_TCP_HOST,
        "compat_tcp_port": _get_int_platform_setting("VOICE_COMPAT_TCP_PORT", DEFAULT_COMPAT_TCP_PORT),
        "compat_tcp_require_token": _get_bool_platform_setting("VOICE_COMPAT_TCP_REQUIRE_TOKEN", True),
        "compat_tcp_token_set": bool(_text(settings.get("VOICE_COMPAT_TCP_TOKEN"))),
        "esphome_native_enabled": True,
        "esphome_api_port": _get_int_platform_setting("VOICE_ESPHOME_API_PORT", DEFAULT_ESPHOME_API_PORT),
        "esphome_connect_timeout_s": _get_float_platform_setting(
            "VOICE_ESPHOME_CONNECT_TIMEOUT_S",
            DEFAULT_ESPHOME_CONNECT_TIMEOUT_S,
        ),
        "esphome_retry_seconds": _get_int_platform_setting("VOICE_ESPHOME_RETRY_SECONDS", DEFAULT_ESPHOME_RETRY_SECONDS),
        "esphome_auto_target_manual": _get_bool_platform_setting("VOICE_ESPHOME_AUTO_TARGET_MANUAL", True),
        "esphome_password_set": bool(_text(settings.get("VOICE_ESPHOME_PASSWORD"))),
        "esphome_noise_psk_set": bool(_text(settings.get("VOICE_ESPHOME_NOISE_PSK"))),
        "wyoming_stt": {
            "host": stt_host,
            "port": stt_port,
            "uri": f"tcp://{stt_host}:{stt_port}",
        },
        "wyoming_tts": {
            "host": tts_host,
            "port": tts_port,
            "uri": f"tcp://{tts_host}:{tts_port}",
            "voice": tts_voice,
        },
        "debug": _get_bool_platform_setting("VOICE_NATIVE_DEBUG", False),
    }


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
    return str(_portal_settings().get("API_AUTH_KEY") or "").strip()

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

# -------------------- FastAPI DTOs --------------------


class VoiceConfigOut(BaseModel):
    config: Dict[str, Any]


class VoiceSatellitesOut(BaseModel):
    satellites: List[Dict[str, Any]]


class VoiceSatelliteAdoptIn(BaseModel):
    selector: Optional[str] = None
    satellite_id: Optional[str] = None
    host: Optional[str] = None
    entity_id: Optional[str] = None
    name: Optional[str] = None
    area_name: Optional[str] = None
    source: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class VoiceSatelliteRemoveIn(BaseModel):
    selector: str


class VoiceNativeSessionStartIn(BaseModel):
    satellite_selector: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    user_id: Optional[str] = None
    language: Optional[str] = None
    wake_word: Optional[str] = None
    initial_text: Optional[str] = None
    sample_rate_hz: Optional[int] = None
    sample_width_bytes: Optional[int] = None
    channels: Optional[int] = None
    context: Dict[str, Any] = Field(default_factory=dict)


class VoiceNativeSessionAudioIn(BaseModel):
    audio_base64: str
    timestamp_ms: Optional[int] = None
    final_chunk: bool = False


class VoiceNativeSessionFinalizeIn(BaseModel):
    text_override: Optional[str] = None
    language: Optional[str] = None


class VoiceNativeSessionOut(BaseModel):
    session: Dict[str, Any]


class VoiceNativeSessionProcessOut(BaseModel):
    session: Dict[str, Any]
    transcript: str
    response_text: str
    tts_audio_base64: str
    tts_audio_format: Dict[str, Any]


class VoiceNativeStatusOut(BaseModel):
    status: Dict[str, Any]


class VoiceCompatConnectIn(BaseModel):
    selector: str
    transport: Optional[str] = None
    protocol: Optional[str] = None
    name: Optional[str] = None
    host: Optional[str] = None
    area_name: Optional[str] = None
    satellite_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class VoiceCompatDisconnectIn(BaseModel):
    selector: str
    reason: Optional[str] = None


class VoiceCompatWakeIn(BaseModel):
    selector: str
    wake_word: Optional[str] = None
    language: Optional[str] = None
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)


class VoiceCompatAudioStartIn(BaseModel):
    selector: str
    session_id: Optional[str] = None
    language: Optional[str] = None
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    sample_rate_hz: Optional[int] = None
    sample_width_bytes: Optional[int] = None
    channels: Optional[int] = None
    context: Dict[str, Any] = Field(default_factory=dict)


class VoiceCompatAudioChunkIn(BaseModel):
    selector: str
    session_id: str
    audio_base64: str
    timestamp_ms: Optional[int] = None
    final_chunk: bool = False


class VoiceCompatAudioStopIn(BaseModel):
    selector: str
    session_id: str
    text_override: Optional[str] = None
    language: Optional[str] = None


class VoiceCompatControlIn(BaseModel):
    selector: str
    command: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class VoiceCompatEventsOut(BaseModel):
    selector: str
    cursor: int
    events: List[Dict[str, Any]]


class VoiceESPHomeConnectIn(BaseModel):
    selector: str
    host: Optional[str] = None
    port: Optional[int] = None


class VoiceESPHomeDisconnectIn(BaseModel):
    selector: str


class VoiceESPHomeStatusOut(BaseModel):
    status: Dict[str, Any]

# -------------------- Plugin gating --------------------
def get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("verba_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")


def _voice_core_platform_tokens(raw: Any) -> List[str]:
    items: List[str] = []
    if isinstance(raw, str):
        items = [part.strip() for part in re.split(r"[,\s]+", raw) if part.strip()]
    elif isinstance(raw, (list, tuple, set)):
        items = [str(item).strip() for item in raw if str(item).strip()]
    out: List[str] = []
    seen = set()
    for item in items:
        token = _lower(item)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _voice_core_adapted_platforms(plugin: Any) -> List[str]:
    raw_platforms = _voice_core_platform_tokens(getattr(plugin, "platforms", []) or [])
    if not raw_platforms:
        return []
    normalized = set(raw_platforms)
    out = list(raw_platforms)

    # Voice Core uses the Home Assistant Hydra platform semantics today.
    # We accept explicit voice_core plugins by mapping them into homeassistant execution.
    if "voice_core" in normalized and "homeassistant" not in normalized:
        out.append("homeassistant")
        normalized.add("homeassistant")

    # Also advertise voice_core when a plugin already supports HA/both, so the
    # capability is visible for future platform migration.
    if (
        "voice_core" not in normalized
        and normalized.intersection({"homeassistant", "both", "all", "any", "*"})
    ):
        out.append("voice_core")
    return out


class _VoiceCoreVerbaAdapter:
    def __init__(self, plugin: Any):
        self._plugin = plugin
        self.platforms = _voice_core_adapted_platforms(plugin)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._plugin, name)

    async def _invoke_best_handler(self, *args: Any, **kwargs: Any) -> Any:
        handlers = (
            getattr(self._plugin, "handle_voice_core", None),
            getattr(self._plugin, "handle_homeassistant", None),
        )
        for handler in handlers:
            if not callable(handler):
                continue
            result = handler(*args, **kwargs)
            if inspect.isawaitable(result):
                return await result
            return result
        plugin_name = _text(getattr(self._plugin, "name", "")) or self._plugin.__class__.__name__
        raise RuntimeError(
            f"Plugin `{plugin_name}` does not implement handle_voice_core or handle_homeassistant."
        )

    async def handle_homeassistant(self, *args: Any, **kwargs: Any) -> Any:
        return await self._invoke_best_handler(*args, **kwargs)

    async def handle_voice_core(self, *args: Any, **kwargs: Any) -> Any:
        return await self._invoke_best_handler(*args, **kwargs)


def _voice_core_registry_snapshot() -> Dict[str, Any]:
    base = dict(pr.get_verba_registry_snapshot() or {})
    adapted: Dict[str, Any] = {}
    for plugin_id, plugin in base.items():
        key = _text(plugin_id)
        if not key or plugin is None:
            continue
        adapted[key] = _VoiceCoreVerbaAdapter(plugin)
    return adapted

# -------------------- Stable conversation key (CRITICAL for continued chat) --------------------
def _conv_key_from_fields(
    *,
    device_id: Optional[str],
    area_id: Optional[str],
    session_id: Optional[str],
    ctx: Optional[Dict[str, Any]] = None,
) -> str:
    context = ctx if isinstance(ctx, dict) else {}
    did = _text(device_id) or _text(context.get("device_id"))
    if did:
        return f"device:{did}"

    aid = _text(area_id) or _text(context.get("area_id"))
    if aid:
        return f"area:{aid}"

    sid = _text(session_id)
    if sid:
        return f"session:{sid}"

    return "default"


# -------------------- System prompt (Discord/IRC style, HA scoped) --------------------
def build_system_prompt(ctx: Optional[Dict[str, Any]] = None) -> str:
    # ---- Voice / room context ----
    room_clause = ""
    if ctx:
        area_name = (ctx.get("area_name") or "").strip()
        device_name = (ctx.get("device_name") or "").strip()
        if area_name or device_name:
            room_clause = (
                "VOICE CONTEXT:\n"
                f"- Device: {device_name or '(unknown)'}\n"
                f"- Area/Room: {area_name or '(unknown)'}\n\n"
                "DEFAULT ROOM RULE:\n"
                "If the user asks to control lights/switches/fans/etc and does NOT specify a room, "
                "assume they mean the Area/Room shown above.\n\n"
            )

    # Platform preamble should be style/format only.
    return (
        "You are a Home Assistant-savvy AI assistant.\n"
        f"{room_clause}"
        "Use plain text only; no emojis and no markdown formatting.\n"
        "Keep replies concise and easy to understand.\n"
    )

# -------------------- History shaping (Discord-style alternation) --------------------
def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    # Skip waiting lines from tools
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # Do NOT inject HA context markers into the LLM prompt
    if isinstance(content, dict) and content.get("marker") == "ha_context":
        return None

    # Include final plugin responses in context
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

    # Represent plugin calls as plain text (so history still makes sense)
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps(
            {"function": content.get("plugin"), "arguments": content.get("arguments", {})},
            indent=2,
        )
        return {"role": "assistant", "content": as_text}

    # Text + fallback
    if isinstance(content, str):
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}

def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep the history compact and readable by merging consecutive messages
    with the same role.

    IMPORTANT:
    We intentionally do NOT insert a blank user message at the beginning.
    Some LLM backends/models can respond with empty completions when they
    see an empty user turn (content="").
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

# -------------------- Redis history + context helpers --------------------
def _sess_key(conv_key: str) -> str:
    return f"tater:ha:session:{conv_key}:history"

def _ctx_key(conv_key: str) -> str:
    return f"tater:ha:session:{conv_key}:ctx"

def _load_ctx(conv_key: str) -> Dict[str, Any]:
    raw = redis_client.get(_ctx_key(conv_key))
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def _save_ctx(conv_key: str, ctx: Dict[str, Any]) -> None:
    if not ctx:
        return
    ttl = _get_duration_seconds_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)
    try:
        redis_client.setex(_ctx_key(conv_key), ttl, json.dumps(ctx))
    except Exception:
        pass

async def _load_history(conv_key: str, limit: int) -> List[Dict[str, Any]]:
    key = _sess_key(conv_key)
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

async def _save_message(conv_key: str, role: str, content: Any, max_store: int):
    key = _sess_key(conv_key)
    pipe = redis_client.pipeline()
    pipe.rpush(key, json.dumps({"role": role, "content": content}))
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


async def _run_homeassistant_text_turn(
    *,
    text_in: str,
    user_id: Optional[str] = None,
    device_id: Optional[str] = None,
    area_id: Optional[str] = None,
    session_id: Optional[str] = None,
    incoming_context: Optional[Dict[str, Any]] = None,
    allow_followup: bool = False,
) -> Tuple[str, str, Dict[str, Any]]:
    user_text = _text(text_in)
    if not user_text:
        return "(no text provided)", "default", {}

    if _llm is None:
        raise RuntimeError("LLM backend not initialized")

    history_store_limit = _global_history_store_limit()
    history_llm_limit = _global_history_llm_limit()

    raw_ctx = incoming_context if isinstance(incoming_context, dict) else {}
    ctx_in = {str(k): v for k, v in raw_ctx.items() if _text(k)}

    conv_key = _conv_key_from_fields(
        device_id=device_id,
        area_id=area_id,
        session_id=session_id,
        ctx=ctx_in,
    )
    ctx: Dict[str, Any] = _load_ctx(conv_key)
    if ctx_in:
        ctx.update(ctx_in)

    if _text(device_id) and not _text(ctx.get("device_id")):
        ctx["device_id"] = _text(device_id)
    if _text(area_id) and not _text(ctx.get("area_id")):
        ctx["area_id"] = _text(area_id)

    if ctx:
        _save_ctx(conv_key, ctx)

    await _save_message(conv_key, "user", user_text, history_store_limit)

    system_prompt = build_system_prompt(ctx if ctx else None)
    messages_list = await _load_history(conv_key, history_llm_limit)

    if not messages_list or messages_list[-1].get("role") != "user":
        messages_list.append({"role": "user", "content": user_text})

    merged_registry = _voice_core_registry_snapshot()
    merged_enabled = get_plugin_enabled

    origin = {
        "platform": "homeassistant",
        "entrypoint": "voice_core",
        "user": user_id,
        "user_id": user_id,
        "session_id": session_id,
        "device_id": device_id or ctx.get("device_id"),
        "area_id": area_id or ctx.get("area_id"),
        "request_id": session_id or conv_key,
    }
    origin = {k: v for k, v in origin.items() if v not in (None, "")}
    agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)

    result = await run_hydra_turn(
        llm_client=_llm,
        platform="homeassistant",
        history_messages=messages_list,
        registry=merged_registry,
        enabled_predicate=merged_enabled,
        context=ctx,
        user_text=user_text,
        scope=conv_key if conv_key != "default" else "",
        origin=origin,
        redis_client=redis_client,
        max_rounds=agent_max_rounds,
        max_tool_calls=agent_max_tool_calls,
        platform_preamble=system_prompt,
    )

    final_text = _text(result.get("text"))
    if len(final_text) > 4000:
        final_text = final_text[:4000] + "…"

    await _save_message(
        conv_key,
        "assistant",
        {"marker": "plugin_response", "phase": "final", "content": final_text},
        history_store_limit,
    )

    if allow_followup and _get_bool_platform_setting("CONTINUED_CHAT_ENABLED", DEFAULT_CONTINUED_CHAT_ENABLED):
        asyncio.create_task(_maybe_reopen_listening(conv_key, ctx, final_text))

    return final_text, conv_key, ctx


# -------------------- Minimal HA client (platform local) --------------------
class _HA:
    def __init__(self):
        # Prefer shared Home Assistant settings first
        shared = redis_client.hgetall("homeassistant_settings") or {}
        base = (shared.get("HA_BASE_URL") or "").strip()
        token = (shared.get("HA_TOKEN") or "").strip()

        # Backward-compatible fallback (legacy storage)
        if not base or not token:
            legacy = (
                redis_client.hgetall("verba_settings: Home Assistant")
                or redis_client.hgetall("verba_settings:Home Assistant")
                or {}
            )
            base = base or (legacy.get("HA_BASE_URL") or "").strip()
            token = token or (legacy.get("HA_TOKEN") or "").strip()

        self.base = (base or "http://homeassistant.local:8123").rstrip("/")
        if not token:
            raise ValueError("HA_TOKEN missing in Home Assistant settings.")
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _req(self, method: str, path: str, json_body=None, timeout=10):
        r = requests.request(method, f"{self.base}{path}", headers=self.headers, json=json_body, timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        try:
            return r.json()
        except Exception:
            return r.text

    def get_state(self, entity_id: str):
        return self._req("GET", f"/api/states/{entity_id}")

    def call_service(self, domain: str, service: str, data: dict, return_response: bool = False):
        qs = "?return_response=true" if return_response else ""
        return self._req("POST", f"/api/services/{domain}/{service}{qs}", json_body=data)

    def ws_url(self) -> str:
        # http://host:8123 -> ws://host:8123
        if self.base.startswith("https://"):
            return self.base.replace("https://", "wss://", 1) + "/api/websocket"
        return self.base.replace("http://", "ws://", 1) + "/api/websocket"

# -------------------- Voice PE ring helpers --------------------
def _voice_pe_entities() -> List[str]:
    s = _portal_settings()
    ids = [
        (s.get("VOICE_PE_ENTITY_1") or "").strip(),
        (s.get("VOICE_PE_ENTITY_2") or "").strip(),
        (s.get("VOICE_PE_ENTITY_3") or "").strip(),
        (s.get("VOICE_PE_ENTITY_4") or "").strip(),
        (s.get("VOICE_PE_ENTITY_5") or "").strip(),
    ]
    return [e for e in ids if e]

def _ring_on():
    ents = _voice_pe_entities()
    if not ents:
        return
    ha = _HA()
    for eid in ents:
        try:
            ha.call_service("light", "turn_on", {"entity_id": eid})
        except Exception as e:
            logger.warning(f"[notify] failed to turn on ring {eid}: {e}")

def _ring_off():
    ents = _voice_pe_entities()
    if not ents:
        return
    ha = _HA()
    for eid in ents:
        try:
            ha.call_service("light", "turn_off", {"entity_id": eid})
        except Exception as e:
            logger.warning(f"[notify] failed to turn off ring {eid}: {e}")

# -------------------- Assist satellite resolver (device_id→entity) --------------------
_satellite_refresh_lock = asyncio.Lock()
_satellite_map_mem: Dict[str, str] = {}
_satellite_map_mem_ts: float = 0.0
_voice_satellite_registry_mem: List[Dict[str, Any]] = []
_voice_satellite_registry_redis_warned = False

async def _ha_ws_call(session: aiohttp.ClientSession, ws: aiohttp.ClientWebSocketResponse, msg: dict, expect_id: int, timeout: float = 20.0) -> Any:
    await ws.send_json(msg)
    end = time.time() + timeout
    while time.time() < end:
        m = await ws.receive(timeout=timeout)
        if m.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(m.data)
            if data.get("type") == "result" and data.get("id") == expect_id:
                if not data.get("success"):
                    raise RuntimeError(f"HA WS call failed: {data}")
                return data.get("result")
        elif m.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
            break
    raise TimeoutError("Timed out waiting for HA WS result")

async def _fetch_satellite_map_from_ha() -> Dict[str, str]:
    """
    Build a map of:
      device_id -> assist_satellite.entity_id
    using HA entity registry.
    """
    ha = _HA()
    ws_url = ha.ws_url()

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url, heartbeat=30) as ws:
            # auth_required
            first = await ws.receive(timeout=10)
            if first.type != aiohttp.WSMsgType.TEXT:
                raise RuntimeError("Unexpected HA WS message during auth_required")
            hello = json.loads(first.data)
            if hello.get("type") != "auth_required":
                raise RuntimeError(f"Unexpected HA WS hello: {hello}")

            await ws.send_json({"type": "auth", "access_token": ha.token})
            auth_resp = await ws.receive(timeout=10)
            if auth_resp.type != aiohttp.WSMsgType.TEXT:
                raise RuntimeError("Unexpected HA WS message during auth")
            auth_data = json.loads(auth_resp.data)
            if auth_data.get("type") != "auth_ok":
                raise RuntimeError(f"HA WS auth failed: {auth_data}")

            entity_reg = await _ha_ws_call(
                session, ws,
                {"id": 1, "type": "config/entity_registry/list"},
                expect_id=1,
                timeout=30,
            )

    # device_id -> assist_satellite entity_id
    device_sat: Dict[str, str] = {}
    if isinstance(entity_reg, list):
        for e in entity_reg:
            try:
                ent = (e.get("entity_id") or "").strip()
                if not ent.startswith("assist_satellite."):
                    continue
                if e.get("disabled_by") not in (None, ""):
                    continue
                did = e.get("device_id")
                if not did:
                    continue
                did_text = str(did).strip()
                if not did_text:
                    continue
                if did_text not in device_sat:
                    device_sat[did_text] = ent
            except Exception:
                continue

    return device_sat

def _satellite_cache_ttl_s() -> int:
    return _get_int_platform_setting("SATELLITE_MAP_CACHE_TTL_S", DEFAULT_SATELLITE_MAP_CACHE_TTL_S)

def _load_satellite_map_from_redis() -> Tuple[Dict[str, str], bool]:
    """
    Returns (map, ok). ok=False means missing/invalid.
    """
    try:
        raw = redis_client.get(REDIS_SATELLITE_MAP_KEY)
        if not raw:
            return {}, False
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return {}, False
        clean = {str(k): str(v) for k, v in obj.items() if k and v}
        return clean, True
    except Exception:
        return {}, False

def _save_satellite_map_to_redis(m: Dict[str, str]) -> None:
    try:
        ttl = _satellite_cache_ttl_s()
        redis_client.setex(REDIS_SATELLITE_MAP_KEY, ttl, json.dumps(m))
    except Exception:
        pass


def _ha_token_available() -> bool:
    try:
        shared = redis_client.hgetall("homeassistant_settings") or {}
    except Exception:
        shared = {}
    token = _text(shared.get("HA_TOKEN"))
    if token:
        return True
    try:
        legacy = (
            redis_client.hgetall("verba_settings: Home Assistant")
            or redis_client.hgetall("verba_settings:Home Assistant")
            or {}
        )
    except Exception:
        legacy = {}
    return bool(_text(legacy.get("HA_TOKEN")))


def _name_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _lower(value))


def _name_tokens_match(a: str, b: str) -> bool:
    left = _name_token(a)
    right = _name_token(b)
    if not left or not right:
        return False
    if left == right:
        return True
    if len(left) >= 8 and right.startswith(left):
        return True
    if len(right) >= 8 and left.startswith(right):
        return True
    return False


def _entity_core_name(entity_id: Any) -> str:
    token = _lower(entity_id)
    if token.startswith("assist_satellite."):
        token = token.split(".", 1)[1]
    if token.endswith("_assist_satellite"):
        token = token[: -len("_assist_satellite")]
    return token


def _ha_assist_name_candidates(entity_id: Any) -> List[str]:
    entity = _lower(entity_id)
    core = _entity_core_name(entity_id)
    out: List[str] = []
    for raw in (entity, core, entity.replace("_", "-"), core.replace("_", "-")):
        token = _text(raw)
        if not token:
            continue
        out.append(token)
    return out


async def _ha_assist_entity_ids_for_discovery(*, force_refresh: bool) -> List[str]:
    if not _ha_token_available():
        return []

    if force_refresh:
        try:
            fresh = await _fetch_satellite_map_from_ha()
            if fresh:
                _save_satellite_map_to_redis(fresh)
                _native_debug(f"discovery ha-assist refresh entries={len(fresh)}")
                return sorted({_lower(v) for v in fresh.values() if _text(v)})
        except Exception as exc:
            _native_debug(f"discovery ha-assist refresh failed: {exc}")

    cached, ok = _load_satellite_map_from_redis()
    if ok:
        return sorted({_lower(v) for v in cached.values() if _text(v)})
    return []


def _mdns_row_matches_ha_assist(row: Dict[str, Any], ha_entity_ids: List[str]) -> Tuple[bool, str]:
    if not isinstance(row, dict) or not ha_entity_ids:
        return False, ""
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    node_name = _text(row.get("name"))
    service_name = _text(metadata.get("mdns_service")).split(".", 1)[0]
    source_tokens = [node_name, service_name]
    for source in source_tokens:
        if not source:
            continue
        for entity_id in ha_entity_ids:
            for candidate in _ha_assist_name_candidates(entity_id):
                if _name_tokens_match(source, candidate):
                    return True, f"{source}~{candidate}"
    return False, ""


def _selector_from_satellite_row(row: Dict[str, Any]) -> str:
    satellite_id = _text(row.get("satellite_id") or row.get("device_id"))
    if satellite_id:
        return f"device:{satellite_id}"
    entity_id = _lower(row.get("entity_id"))
    if entity_id:
        return f"entity:{entity_id}"
    host = _lower(row.get("host"))
    if host:
        return f"host:{host}"
    return ""


def _normalize_voice_satellite_row(raw: Any) -> Dict[str, Any]:
    row = raw if isinstance(raw, dict) else {}
    normalized: Dict[str, Any] = {
        "selector": "",
        "satellite_id": _text(row.get("satellite_id") or row.get("device_id")),
        "entity_id": _lower(row.get("entity_id")),
        "host": _lower(row.get("host")),
        "name": _text(row.get("name")),
        "area_name": _text(row.get("area_name")),
        "source": _text(row.get("source")) or "manual",
        "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
        "last_seen_ts": 0.0,
    }
    try:
        normalized["last_seen_ts"] = float(row.get("last_seen_ts") or 0.0)
    except Exception:
        normalized["last_seen_ts"] = 0.0
    selector = _text(row.get("selector"))
    if not selector:
        selector = _selector_from_satellite_row(normalized)
    normalized["selector"] = selector
    return normalized


def _load_voice_satellite_registry() -> List[Dict[str, Any]]:
    global _voice_satellite_registry_mem, _voice_satellite_registry_redis_warned
    try:
        raw = redis_client.get(REDIS_VOICE_SATELLITE_REGISTRY_KEY)
        if not raw:
            return [dict(row) for row in _voice_satellite_registry_mem]
        payload = json.loads(raw)
        if not isinstance(payload, list):
            return [dict(row) for row in _voice_satellite_registry_mem]
    except Exception as exc:
        if not _voice_satellite_registry_redis_warned:
            _voice_satellite_registry_redis_warned = True
            logger.warning("[voice_core] satellite registry redis unavailable; using in-memory fallback: %s", exc)
        return [dict(row) for row in _voice_satellite_registry_mem]

    rows: List[Dict[str, Any]] = []
    seen = set()
    for entry in payload:
        row = _normalize_voice_satellite_row(entry)
        selector = _text(row.get("selector"))
        if not selector or selector in seen:
            continue
        seen.add(selector)
        rows.append(row)
    _voice_satellite_registry_mem = [dict(row) for row in rows]
    return rows


def _save_voice_satellite_registry(rows: List[Dict[str, Any]]) -> None:
    global _voice_satellite_registry_mem, _voice_satellite_registry_redis_warned
    clean: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        normalized = _normalize_voice_satellite_row(row)
        selector = _text(normalized.get("selector"))
        if not selector or selector in seen:
            continue
        seen.add(selector)
        clean.append(normalized)
    _voice_satellite_registry_mem = [dict(row) for row in clean]
    try:
        redis_client.set(REDIS_VOICE_SATELLITE_REGISTRY_KEY, json.dumps(clean, ensure_ascii=False))
    except Exception as exc:
        if not _voice_satellite_registry_redis_warned:
            _voice_satellite_registry_redis_warned = True
            logger.warning("[voice_core] satellite registry redis unavailable; using in-memory fallback: %s", exc)


def _upsert_voice_satellite(row: Dict[str, Any]) -> Dict[str, Any]:
    incoming = _normalize_voice_satellite_row(row)
    selector = _text(incoming.get("selector"))
    if not selector:
        raise ValueError("satellite selector is required")

    current = _load_voice_satellite_registry()
    merged: List[Dict[str, Any]] = []
    replaced = False
    for existing in current:
        if _text(existing.get("selector")) != selector:
            merged.append(existing)
            continue
        updated = dict(existing)
        for key, value in incoming.items():
            if key == "metadata":
                base_meta = existing.get("metadata") if isinstance(existing.get("metadata"), dict) else {}
                next_meta = value if isinstance(value, dict) else {}
                updated["metadata"] = {**base_meta, **next_meta}
                continue
            if key == "source":
                incoming_source = _lower(value)
                existing_source = _lower(existing.get("source"))
                existing_meta = existing.get("metadata") if isinstance(existing.get("metadata"), dict) else {}
                if existing_source == "esphome_native" and incoming_source in {"mdns_esphome", "manual", ""}:
                    continue
                if incoming_source == "mdns_esphome" and bool(existing_meta.get("esphome_selected")):
                    continue
            if value in ("", None) and key != "last_seen_ts":
                continue
            updated[key] = value
        updated["last_seen_ts"] = float(time.time())
        merged.append(_normalize_voice_satellite_row(updated))
        replaced = True
    if not replaced:
        incoming["last_seen_ts"] = float(time.time())
        merged.append(incoming)
    _save_voice_satellite_registry(merged)
    for row_out in merged:
        if _text(row_out.get("selector")) == selector:
            return row_out
    return incoming


def _remove_voice_satellite(selector: str) -> bool:
    token = _text(selector)
    if not token:
        return False
    current = _load_voice_satellite_registry()
    kept = [row for row in current if _text(row.get("selector")) != token]
    if len(kept) == len(current):
        return False
    _save_voice_satellite_registry(kept)
    return True


def _set_satellite_esphome_selected(selector: str, selected: bool) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        return {}
    row = _voice_core_find_satellite(token)
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    next_row = dict(row) if isinstance(row, dict) else {"selector": token}
    next_meta = dict(metadata)
    next_meta["esphome_selected"] = bool(selected)
    next_row["metadata"] = next_meta
    return _upsert_voice_satellite(next_row)


def _clear_legacy_esphome_selected_flags() -> int:
    rows = _load_voice_satellite_registry()
    if not rows:
        return 0

    changed = 0
    next_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            next_rows.append(row)
            continue
        source = _lower(row.get("source"))
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if source in {"esphome_native"}:
            next_rows.append(row)
            continue
        if not bool(metadata.get("esphome_selected")):
            next_rows.append(row)
            continue
        updated = dict(row)
        next_meta = dict(metadata)
        next_meta.pop("esphome_selected", None)
        updated["metadata"] = next_meta
        next_rows.append(updated)
        changed += 1

    if changed:
        _save_voice_satellite_registry(next_rows)
    return changed


def _voice_satellite_option_rows(*, current_values: List[str]) -> List[Dict[str, str]]:
    options_by_value: Dict[str, Dict[str, str]] = {}
    for row in _load_voice_satellite_registry():
        selector = _text(row.get("selector"))
        if not selector:
            continue
        name = _text(row.get("name"))
        entity_id = _text(row.get("entity_id"))
        host = _text(row.get("host"))
        source = _text(row.get("source"))
        label = " • ".join([part for part in [name or selector, entity_id, host, source] if part]) or selector
        options_by_value[selector] = {"value": selector, "label": label}

    for value in current_values:
        token = _text(value)
        if not token or token in options_by_value:
            continue
        options_by_value[token] = {"value": token, "label": f"{token} (saved)"}

    return sorted(options_by_value.values(), key=lambda row: _lower(row.get("label")))


def _ha_sensor_entity_option_rows() -> List[Dict[str, str]]:
    try:
        states = _HA()._req("GET", "/api/states", timeout=10)
    except Exception:
        return []

    if not isinstance(states, list):
        return []

    options: List[Dict[str, str]] = []
    seen = set()
    for row in states:
        if not isinstance(row, dict):
            continue
        entity_id = _lower(row.get("entity_id"))
        if not entity_id:
            continue
        if not (
            entity_id.startswith("sensor.")
            or entity_id.startswith("binary_sensor.")
            or entity_id.startswith("assist_satellite.")
        ):
            continue
        if entity_id in seen:
            continue
        seen.add(entity_id)

        attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
        friendly_name = _text(attrs.get("friendly_name"))
        area_name = _text(attrs.get("area_name"))
        state = _text(row.get("state"))
        label = friendly_name or entity_id
        if area_name:
            label = f"{label} ({area_name})"
        if state:
            label = f"{label} • state: {state}"
        options.append({"value": entity_id, "label": label})
        if len(options) >= 600:
            break
    return sorted(options, key=lambda item: _lower(item.get("label")))


def _voice_sensor_option_rows(*, current_values: List[str]) -> List[Dict[str, str]]:
    options_by_value = {row.get("value"): row for row in _ha_sensor_entity_option_rows() if row.get("value")}
    for value in current_values:
        token = _lower(value)
        if not token or token in options_by_value:
            continue
        options_by_value[token] = {"value": token, "label": f"{token} (saved)"}
    return sorted(options_by_value.values(), key=lambda item: _lower(item.get("label")))


def webui_settings_fields(
    *,
    fields: Any,
    current_settings: Any = None,
    **_kwargs,
) -> List[Dict[str, Any]]:
    base_fields = list(fields or [])
    current = current_settings if isinstance(current_settings, dict) else {}
    keys_present = {_text(item.get("key")) for item in base_fields if isinstance(item, dict)}
    selected_satellites = _parse_json_string_list(current.get("VOICE_SATELLITE_TARGETS"))
    selected_sensors = _parse_json_string_list(current.get("VOICE_SENSOR_ENTITY_IDS"))
    satellite_options: List[Dict[str, str]] = []
    sensor_options: List[Dict[str, str]] = []
    if "VOICE_SATELLITE_TARGETS" in keys_present:
        satellite_options = _voice_satellite_option_rows(current_values=selected_satellites)
    if "VOICE_SENSOR_ENTITY_IDS" in keys_present:
        sensor_options = _voice_sensor_option_rows(current_values=selected_sensors)

    out: List[Dict[str, Any]] = []
    for item in base_fields:
        if not isinstance(item, dict):
            out.append(item)
            continue
        key = _text(item.get("key"))
        if key == "VOICE_SATELLITE_TARGETS":
            updated = dict(item)
            updated["type"] = "multiselect"
            updated["options"] = satellite_options
            updated["value"] = selected_satellites
            updated["default"] = []
            updated["description"] = (
                f"{_text(item.get('description'))} "
                "Selectors come from discovered and manually added satellite entries."
            ).strip()
            out.append(updated)
            continue
        if key == "VOICE_SENSOR_ENTITY_IDS":
            updated = dict(item)
            updated["type"] = "multiselect"
            updated["options"] = sensor_options
            updated["value"] = selected_sensors
            updated["default"] = []
            updated["description"] = (
                f"{_text(item.get('description'))} "
                "Options are loaded from Home Assistant sensor/binary_sensor entities."
            ).strip()
            out.append(updated)
            continue
        out.append(item)
    return out


def webui_prepare_settings_values(*, values: Any, **_kwargs) -> Dict[str, Any]:
    out = dict(values or {})
    for key in VOICE_MULTISELECT_FIELDS:
        if key not in out:
            continue
        coerced = _coerce_webui_multiselect_json(out.get(key))
        if coerced is not None:
            out[key] = coerced
    if "VOICE_MANUAL_TARGETS" in out:
        out["VOICE_MANUAL_TARGETS"] = "\n".join(_normalize_csv_or_lines(out.get("VOICE_MANUAL_TARGETS")))
    return out


def _run_async_blocking(coro: Any) -> Any:
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            try:
                loop.close()
            except Exception:
                pass


def _voice_core_setting_fields(current: Dict[str, Any]) -> List[Dict[str, Any]]:
    required = CORE_SETTINGS.get("required") if isinstance(CORE_SETTINGS, dict) else {}
    required_map = required if isinstance(required, dict) else {}
    exposed_keys = {
        "VOICE_WYOMING_STT_HOST",
        "VOICE_WYOMING_STT_PORT",
        "VOICE_WYOMING_TTS_HOST",
        "VOICE_WYOMING_TTS_PORT",
        "VOICE_WYOMING_TTS_VOICE",
        "VOICE_NATIVE_WYOMING_TIMEOUT_S",
        "VOICE_NATIVE_SESSION_TTL_S",
        "VOICE_NATIVE_MAX_AUDIO_BYTES",
        "VOICE_NATIVE_DEBUG",
        "VOICE_ESPHOME_API_PORT",
        "VOICE_ESPHOME_PASSWORD",
        "VOICE_ESPHOME_NOISE_PSK",
        "VOICE_ESPHOME_CONNECT_TIMEOUT_S",
    }
    fields: List[Dict[str, Any]] = []
    for setting_key, setting_meta in required_map.items():
        if not isinstance(setting_meta, dict):
            continue
        if setting_key not in exposed_keys:
            continue
        default_value = setting_meta.get("default", "")
        raw_value = current.get(setting_key, default_value)
        fields.append(
            {
                "key": setting_key,
                "label": setting_meta.get("label", setting_key),
                "type": setting_meta.get("type", "text"),
                "description": setting_meta.get("description", ""),
                "options": setting_meta.get("options", []),
                "value": raw_value,
                "default": default_value,
            }
        )
    fields = webui_settings_fields(fields=fields, current_settings=current)
    for item in fields:
        if not isinstance(item, dict):
            continue
        if _text(item.get("key")) != "VOICE_WYOMING_TTS_VOICE":
            continue
        item["type"] = "select"
        current_value = _text(item.get("value"))
        item["options"] = _wyoming_tts_voice_option_rows(current_value=current_value)
        if current_value == "":
            item["value"] = ""
        item["description"] = (
            f"{_text(item.get('description'))} "
            "Pick Default to use Piper's default voice."
        ).strip()
    return fields


def _voice_core_format_timestamp(ts: Any) -> str:
    try:
        raw = float(ts)
    except Exception:
        return "-"
    if raw <= 0:
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(raw))
    except Exception:
        return "-"


def _voice_core_find_satellite(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        return {}
    for row in _load_voice_satellite_registry():
        if _text(row.get("selector")) == token:
            return row if isinstance(row, dict) else {}
    return {}


def _voice_core_settings_sections(field_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: List[Tuple[str, List[str]]] = [
        (
            "Wyoming",
            [
                "VOICE_WYOMING_STT_HOST",
                "VOICE_WYOMING_STT_PORT",
                "VOICE_WYOMING_TTS_HOST",
                "VOICE_WYOMING_TTS_PORT",
                "VOICE_WYOMING_TTS_VOICE",
                "VOICE_NATIVE_WYOMING_TIMEOUT_S",
                "VOICE_NATIVE_SESSION_TTL_S",
                "VOICE_NATIVE_MAX_AUDIO_BYTES",
                "VOICE_NATIVE_DEBUG",
            ],
        ),
        (
            "ESPHome",
            [
                "VOICE_ESPHOME_API_PORT",
                "VOICE_ESPHOME_PASSWORD",
                "VOICE_ESPHOME_NOISE_PSK",
                "VOICE_ESPHOME_CONNECT_TIMEOUT_S",
            ],
        ),
    ]
    sections: List[Dict[str, Any]] = []
    for label, keys in groups:
        fields = [dict(field_map[key]) for key in keys if key in field_map]
        if not fields:
            continue
        sections.append(
            {
                "label": label,
                "inline": True,
                "fields": fields,
            }
        )
    return sections


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    del redis_client

    current = _voice_core_settings()
    field_rows = _voice_core_setting_fields(current)
    field_map = {str(row.get("key") or "").strip(): row for row in field_rows if isinstance(row, dict)}

    runtime_status = _native_runtime_status()
    esphome_status = _esphome_native_status()
    clients = esphome_status.get("clients") if isinstance(esphome_status.get("clients"), dict) else {}
    connected_clients = sum(
        1
        for row in clients.values()
        if isinstance(row, dict) and bool(row.get("connected"))
    )
    satellites = [row for row in _load_voice_satellite_registry() if _text(row.get("selector"))]
    satellites = sorted(satellites, key=lambda row: _lower(row.get("name") or row.get("selector")))
    discovery = runtime_status.get("discovery") if isinstance(runtime_status.get("discovery"), dict) else {}
    last_counts = discovery.get("last_counts") if isinstance(discovery.get("last_counts"), dict) else {}
    _voice_rows, voice_meta = _load_wyoming_tts_voice_catalog()
    selected_tts_voice = _wyoming_tts_voice_selection(current.get("VOICE_WYOMING_TTS_VOICE"))
    selected_tts_voice_label = _wyoming_tts_voice_selection_label(selected_tts_voice) if selected_tts_voice else "Default"
    tts_voice_count = int(voice_meta.get("count") or 0)
    tts_voice_error = _text(voice_meta.get("last_error"))

    settings_item = {
        "id": "voice_pipeline_settings",
        "group": "settings",
        "title": "Voice Core Settings",
        "subtitle": (
            f"Backend: {_text(runtime_status.get('backend_mode')) or VOICE_BACKEND_MODE_NATIVE} • "
            f"STT: {_text(current.get('VOICE_WYOMING_STT_HOST'))}:{_text(current.get('VOICE_WYOMING_STT_PORT')) or DEFAULT_WYOMING_STT_PORT} • "
            f"TTS: {_text(current.get('VOICE_WYOMING_TTS_HOST'))}:{_text(current.get('VOICE_WYOMING_TTS_PORT')) or DEFAULT_WYOMING_TTS_PORT}"
        ),
        "save_action": "voice_save_settings",
        "save_label": "Save Settings",
        "sections": _voice_core_settings_sections(field_map),
    }

    item_forms: List[Dict[str, Any]] = [
        settings_item,
        {
            "id": "wyoming_tts_voice_refresh",
            "group": "settings",
            "title": "Refresh Wyoming TTS Voices",
            "subtitle": (
                f"Selected: {selected_tts_voice_label} • "
                f"cached voices: {tts_voice_count} • "
                f"last refresh: {_voice_core_format_timestamp(voice_meta.get('last_refresh_ts'))}"
            ),
            "run_action": "voice_refresh_tts_voices",
            "run_label": "Refresh TTS Voices",
            "fields": [
                {
                    "key": "tts_endpoint",
                    "label": "TTS endpoint",
                    "type": "text",
                    "value": (
                        f"{_text(current.get('VOICE_WYOMING_TTS_HOST')) or DEFAULT_WYOMING_TTS_HOST}:"
                        f"{_text(current.get('VOICE_WYOMING_TTS_PORT')) or DEFAULT_WYOMING_TTS_PORT}"
                    ),
                    "read_only": True,
                },
                {
                    "key": "last_error",
                    "label": "Last refresh error",
                    "type": "text",
                    "value": tts_voice_error or "-",
                    "read_only": True,
                },
            ],
        },
        {
            "id": "satellite_discovery_refresh",
            "group": "satellites",
            "title": "Discover / Refresh Satellites",
            "subtitle": (
                f"Last run: {_voice_core_format_timestamp(discovery.get('last_run_ts'))} • "
                f"mdns={int(last_counts.get('mdns_esphome') or 0)}"
            ),
            "run_action": "voice_refresh_satellites",
            "run_label": "Discover / Refresh",
            "fields": [
                {
                    "key": "discovery_mode",
                    "label": "Discovery mode",
                    "type": "text",
                    "value": "manual-only (scan on demand)",
                    "read_only": True,
                },
                {
                    "key": "mdns_window",
                    "label": "mDNS window (sec)",
                    "type": "text",
                    "value": str(_native_discovery_mdns_timeout_s()),
                    "read_only": True,
                },
            ],
        },
    ]

    for row in satellites:
        selector = _text(row.get("selector"))
        if not selector:
            continue
        host = _text(row.get("host"))
        if not host and selector.startswith("host:"):
            host = _text(selector.split(":", 1)[1])
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        sensor_rows = metadata.get("sensor_entity_ids") if isinstance(metadata.get("sensor_entity_ids"), list) else []
        sensor_ids = [str(item).strip() for item in sensor_rows if str(item).strip()]
        entity_count = int(metadata.get("esphome_entity_count") or 0)
        client = clients.get(selector) if isinstance(clients.get(selector), dict) else {}
        connected = bool(client.get("connected"))
        selected = bool(metadata.get("esphome_selected"))
        run_action = ""
        run_label = ""
        if selected:
            run_action = "voice_satellite_disconnect"
            run_label = "Disconnect"
        elif host:
            run_action = "voice_satellite_connect"
            run_label = "Connect"
        connection_label = "connected" if connected else "selected (reconnecting)" if selected else "disconnected"

        item_forms.append(
            {
                "id": selector,
                "group": "satellites",
                "title": _text(row.get("name")) or selector,
                "subtitle": (
                    f"{selector} • source: {_text(row.get('source')) or '-'} • "
                    f"area: {_text(row.get('area_name')) or '-'} • "
                    f"last seen: {_voice_core_format_timestamp(row.get('last_seen_ts'))}"
                ),
                "remove_action": "voice_satellite_remove",
                "remove_confirm": f"Remove satellite {selector}?",
                "remove_label": "Remove",
                "run_action": run_action,
                "run_label": run_label,
                "fields": [
                    {"key": "selector", "label": "Selector", "type": "text", "value": selector, "read_only": True},
                    {"key": "host", "label": "Host", "type": "text", "value": host or "-", "read_only": True},
                    {"key": "entity_id", "label": "Entity ID", "type": "text", "value": _text(row.get("entity_id")) or "-", "read_only": True},
                    {"key": "satellite_id", "label": "Satellite ID", "type": "text", "value": _text(row.get("satellite_id")) or "-", "read_only": True},
                    {"key": "connection", "label": "ESPHome Connection", "type": "text", "value": connection_label, "read_only": True},
                    {"key": "selected", "label": "Selected", "type": "text", "value": "yes" if selected else "no", "read_only": True},
                    {"key": "entity_count", "label": "ESPHome Entities", "type": "text", "value": str(entity_count) if entity_count > 0 else "-", "read_only": True},
                    {"key": "sensors", "label": "Sensors", "type": "textarea", "value": "\n".join(sensor_ids) if sensor_ids else "-", "read_only": True},
                ],
            }
        )

    stats = [
        {"label": "Backend", "value": _text(runtime_status.get("backend_mode")) or VOICE_BACKEND_MODE_NATIVE},
        {"label": "Satellites", "value": len(satellites)},
        {"label": "Active Sessions", "value": int(runtime_status.get("sessions_active") or 0)},
        {"label": "ESPHome Clients", "value": f"{connected_clients}/{len(clients)}"},
    ]

    add_form_source_options = [
        {"value": "manual", "label": "manual"},
        {"value": "esphome", "label": "esphome"},
        {"value": "mdns_esphome", "label": "mdns_esphome"},
    ]

    return {
        "summary": (
            "Home Assistant-compatible voice pipeline backend for Satellite devices. "
            "Use this tab to configure Wyoming, manage satellites, and manage ESPHome connectivity."
        ),
        "stats": stats,
        "ui": {
            "kind": "settings_manager",
            "title": "Voice Core Manager",
            "empty_message": "No voice data available yet.",
            "default_tab": "satellites",
            "item_fields_dropdown": True,
            "item_fields_dropdown_label": "Details",
            "item_sections_in_dropdown": True,
            "manager_tabs": [
                {
                    "key": "satellites",
                    "label": "Satellites",
                    "source": "items",
                    "item_group": "satellites",
                    "page_size": 8,
                    "empty_message": "No satellites discovered yet.",
                },
                {
                    "key": "settings",
                    "label": "Settings",
                    "source": "items",
                    "item_group": "settings",
                    "empty_message": "No settings fields available.",
                },
                {
                    "key": "add",
                    "label": "Add",
                    "source": "add_form",
                },
            ],
            "add_form": {
                "action": "voice_adopt_satellite",
                "submit_label": "Add Satellite",
                "fields": [
                    {
                        "key": "selector",
                        "label": "Selector (optional)",
                        "type": "text",
                        "value": "",
                        "placeholder": "device:abc123 or host:10.0.0.50",
                    },
                    {
                        "key": "host",
                        "label": "Host / IP (optional)",
                        "type": "text",
                        "value": "",
                        "placeholder": "10.0.0.50",
                    },
                    {
                        "key": "name",
                        "label": "Name (optional)",
                        "type": "text",
                        "value": "",
                    },
                    {
                        "key": "area_name",
                        "label": "Area (optional)",
                        "type": "text",
                        "value": "",
                    },
                    {
                        "key": "entity_id",
                        "label": "Entity ID (optional)",
                        "type": "text",
                        "value": "",
                        "placeholder": "assist_satellite.kitchen",
                    },
                    {
                        "key": "source",
                        "label": "Source",
                        "type": "select",
                        "options": add_form_source_options,
                        "value": "manual",
                    },
                ],
            },
            "item_forms": item_forms,
        },
    }


def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client if redis_client is not None else globals().get("redis_client")
    if client is None:
        raise ValueError("Redis connection is unavailable.")

    body = payload if isinstance(payload, dict) else {}
    values = body.get("values") if isinstance(body.get("values"), dict) else {}
    action_name = _lower(action)
    request_token = _get_api_auth_key() or None

    def _value(key: str, default: Any = "") -> Any:
        if key in values:
            return values.get(key)
        return body.get(key, default)

    def _selector_from_body() -> str:
        return _text(body.get("id")) or _text(_value("selector"))

    def _coerce_port(raw: Any) -> Optional[int]:
        token = _text(raw)
        if not token:
            return None
        try:
            parsed = int(token)
        except Exception:
            raise ValueError("Port must be an integer.")
        if parsed < 1 or parsed > 65535:
            raise ValueError("Port must be between 1 and 65535.")
        return parsed

    if action_name == "voice_save_settings":
        incoming = dict(values or {})
        if not incoming:
            incoming = {
                str(key): value
                for key, value in body.items()
                if str(key) not in {"id", "values"}
            }
        prepared = webui_prepare_settings_values(values=incoming)
        mapping = {
            k: json.dumps(v) if isinstance(v, (dict, list, bool)) else str(v)
            for k, v in prepared.items()
        }
        if mapping:
            client.hset(VOICE_CORE_SETTINGS_HASH_KEY, mapping=mapping)
        return {"ok": True, "message": "Voice settings saved."}

    if action_name == "voice_adopt_satellite":
        adopt_payload = {
            "selector": _text(_value("selector")),
            "satellite_id": _text(_value("satellite_id")),
            "host": _text(_value("host")),
            "entity_id": _text(_value("entity_id")),
            "name": _text(_value("name")),
            "area_name": _text(_value("area_name")),
            "source": _text(_value("source")) or "manual",
            "metadata": _value("metadata", {}) if isinstance(_value("metadata", {}), dict) else {},
        }
        model = VoiceSatelliteAdoptIn(**adopt_payload)
        _run_async_blocking(voice_satellite_adopt(model, x_tater_token=request_token))
        return {"ok": True, "message": "Satellite added."}

    if action_name == "voice_refresh_satellites":
        result = _run_async_blocking(voice_satellite_refresh(x_tater_token=request_token))
        counts = result.get("counts") if isinstance(result, dict) else {}
        counts_map = counts if isinstance(counts, dict) else {}
        refreshed = int(sum(int(v) for v in counts_map.values())) if counts_map else int(result.get("refreshed") or 0)
        return {"ok": True, "message": f"Satellite discovery refreshed ({refreshed} entries)."}

    if action_name == "voice_refresh_tts_voices":
        try:
            result = _run_async_blocking(_native_wyoming_refresh_tts_voices())
            count = int(result.get("count") or 0) if isinstance(result, dict) else 0
            host = _text((result or {}).get("host"))
            port = int((result or {}).get("port") or 0)
            endpoint = f"{host}:{port}" if host else "configured endpoint"
            return {"ok": True, "message": f"Loaded {count} Wyoming TTS voices from {endpoint}."}
        except Exception as exc:
            raise ValueError(f"Failed to refresh Wyoming TTS voices: {exc}") from exc

    if action_name == "voice_esphome_reconcile":
        _run_async_blocking(voice_esphome_reconcile(x_tater_token=request_token))
        return {"ok": True, "message": "ESPHome reconcile complete."}

    if action_name == "voice_satellite_remove":
        selector = _selector_from_body()
        if not selector:
            raise ValueError("Satellite selector is required.")
        model = VoiceSatelliteRemoveIn(selector=selector)
        _run_async_blocking(voice_satellite_remove(model, x_tater_token=request_token))
        return {"ok": True, "message": f"Removed {selector}."}

    if action_name == "voice_satellite_connect":
        selector = _selector_from_body()
        if not selector:
            raise ValueError("Satellite selector is required.")
        host = _lower(_value("host"))
        if not host:
            sat = _voice_core_find_satellite(selector)
            host = _lower(sat.get("host"))
        if not host and selector.startswith("host:"):
            host = _lower(selector.split(":", 1)[1])
        model = VoiceESPHomeConnectIn(
            selector=selector,
            host=host or None,
            port=_coerce_port(_value("port")),
        )
        _run_async_blocking(voice_esphome_connect(model, x_tater_token=request_token))
        return {"ok": True, "message": f"Connected {selector}."}

    if action_name == "voice_satellite_disconnect":
        selector = _selector_from_body()
        if not selector:
            raise ValueError("Satellite selector is required.")
        model = VoiceESPHomeDisconnectIn(selector=selector)
        _run_async_blocking(voice_esphome_disconnect(model, x_tater_token=request_token))
        return {"ok": True, "message": f"Disconnected {selector}."}

    raise ValueError(f"Unsupported Voice tab action: {action_name or '(empty)'}")


async def _get_satellite_map(force_refresh: bool = False) -> Dict[str, str]:
    """
    Uses:
      memory -> redis -> HA WS
    """
    global _satellite_map_mem, _satellite_map_mem_ts

    ttl = float(_satellite_cache_ttl_s())

    if not force_refresh and _satellite_map_mem and (time.time() - _satellite_map_mem_ts) < ttl:
        return _satellite_map_mem

    async with _satellite_refresh_lock:
        if not force_refresh and _satellite_map_mem and (time.time() - _satellite_map_mem_ts) < ttl:
            return _satellite_map_mem

        if not force_refresh:
            cached, ok = _load_satellite_map_from_redis()
            if ok and cached:
                _satellite_map_mem = cached
                _satellite_map_mem_ts = time.time()
                return cached

        try:
            fresh = await _fetch_satellite_map_from_ha()
            if fresh:
                _satellite_map_mem = fresh
                _satellite_map_mem_ts = time.time()
                _save_satellite_map_to_redis(fresh)
                logger.info(f"[followup] refreshed assist satellite map ({len(fresh)} areas)")
                return fresh
        except Exception as e:
            logger.warning(f"[followup] failed refreshing satellite map: {e}")

        cached, ok = _load_satellite_map_from_redis()
        if ok:
            _satellite_map_mem = cached
            _satellite_map_mem_ts = time.time()
            return cached

        return {}

async def _resolve_assist_satellite_entity(ctx: Dict[str, Any]) -> Optional[str]:
    if not ctx:
        return None
    device_id = (ctx.get("device_id") or "").strip()
    if not device_id:
        return None

    m = await _get_satellite_map(force_refresh=False)
    ent = (m.get(device_id) or "").strip()
    if ent:
        return ent

    m2 = await _get_satellite_map(force_refresh=True)
    ent2 = (m2.get(device_id) or "").strip()
    return ent2 or None

async def _wait_for_satellite_idle(entity_id: str, timeout_s: float) -> bool:
    idle_like = {"idle", "off", "ready", "standby"}
    busy_like = {"listening", "processing", "responding", "speaking", "replying", "playing", "on"}

    ha = _HA()
    end = time.time() + max(1.0, timeout_s)

    while time.time() < end:
        try:
            st = ha.get_state(entity_id)
            state = str(st.get("state") or "").strip().lower()

            if state in ("unknown", "unavailable", ""):
                await asyncio.sleep(0.35)
                continue

            if state in idle_like:
                return True

            if state in busy_like:
                await asyncio.sleep(0.35)
                continue

            if state not in busy_like:
                return True

        except Exception:
            await asyncio.sleep(0.35)

    return False

async def _generate_followup_question(assistant_text: str) -> str:
    """
    Generate a VERY short spoken follow-up cue for start_conversation.start_message.

    IMPORTANT:
    - This MUST NOT be a question (no '?'), to avoid creating weird loops.
    - This text is NOT saved into Redis history and is NOT passed to _should_follow_up().
    """
    # Keep this deterministic/local to avoid async HTTP client shutdown races in background tasks.
    # This path runs after final replies and does not need an extra LLM call.
    cues = (
        "I'm listening.",
        "Go ahead.",
        "Tell me.",
        "Say it.",
    )
    tail = (assistant_text or "").strip().lower()[-240:]
    if not tail:
        return "I'm listening."
    idx = sum(ord(ch) for ch in tail) % len(cues)
    return cues[idx]

def _start_satellite_followup(entity_id: str, start_message: str) -> None:
    msg = (start_message or "").strip()
    msg = msg.replace("?", "").strip()
    if not msg:
        msg = "I'm listening."

    ha = _HA()
    ha.call_service(
        "assist_satellite",
        "start_conversation",
        {
            "entity_id": entity_id,
            "start_message": msg,
            "preannounce": False,
        },
    )

def _should_follow_up(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    tail = t[-200:]
    return "?" in tail and tail.rstrip().endswith("?")

async def _maybe_reopen_listening(conv_key: str, ctx: Dict[str, Any], assistant_text: str):
    """
    If assistant ended in a question, and we have device context,
    reopen listening on the same Assist satellite device.

    Flow:
    - Trigger ONLY based on assistant_text (the real assistant reply)
    - Wait for satellite idle
    - Call assist_satellite.start_conversation with a short AI-generated *cue*
      that is NOT a question (no '?') so it can't cause weird loops.

    NOTE:
    conv_key is only used for logging/clarity (history continuity is handled by _conv_key()).
    """
    if _voice_backend_mode() != VOICE_BACKEND_MODE_HA:
        return

    if not _should_follow_up(assistant_text):
        return

    sat = await _resolve_assist_satellite_entity(ctx)
    if not sat:
        logger.info("[followup] skip (no assist satellite found for device_id)")
        return

    idle_timeout = _get_float_platform_setting("FOLLOWUP_IDLE_TIMEOUT_S", DEFAULT_FOLLOWUP_IDLE_TIMEOUT_S)

    ok = await _wait_for_satellite_idle(sat, idle_timeout)
    if not ok:
        logger.info(f"[followup] skip (satellite not idle within {idle_timeout}s): {sat}")
        return

    try:
        cue = await _generate_followup_question(assistant_text)

        # Hard safety: never allow a question mark in start_message
        cue = (cue or "").strip().replace("?", "").strip()
        if not cue:
            cue = "I'm listening."

        # IMPORTANT:
        # - We DO NOT pass `cue` into _should_follow_up()
        # - We DO NOT save `cue` into history
        await asyncio.to_thread(_start_satellite_followup, sat, cue)

        logger.info(f"[followup] start_conversation on {sat} (conv_key={conv_key}, cue={cue!r})")
    except Exception as e:
        logger.warning(f"[followup] failed to start_conversation on {sat}: {e}")


# -------------------- Native voice runtime (session/state/discovery) --------------------
_native_voice_sessions: Dict[str, Dict[str, Any]] = {}
_native_voice_sessions_lock = asyncio.Lock()
_native_voice_discovery_task: Optional[asyncio.Task] = None
_voice_core_runtime_port = int(DEFAULT_VOICE_CORE_BIND_PORT)
_native_voice_discovery_state: Dict[str, Any] = {
    "runs": 0,
    "last_run_ts": 0.0,
    "last_success_ts": 0.0,
    "last_error": "",
    "last_counts": {},
}
_native_compat_bridge_lock = asyncio.Lock()
_native_compat_bridge: Dict[str, Any] = {
    "seq": 0,
    "selectors": {},
}
_compat_tcp_server: Optional[asyncio.AbstractServer] = None
_compat_tcp_clients: set[asyncio.Task] = set()
_esphome_native_lock = asyncio.Lock()
_esphome_native_task: Optional[asyncio.Task] = None
_esphome_native_clients: Dict[str, Dict[str, Any]] = {}
_esphome_native_stats: Dict[str, Any] = {
    "runs": 0,
    "last_run_ts": 0.0,
    "last_success_ts": 0.0,
    "last_error": "",
}
_esphome_voice_runtime: Dict[str, Dict[str, Any]] = {}
_esphome_tts_url_store: Dict[str, Dict[str, Any]] = {}
_esphome_tts_url_store_lock = threading.Lock()
_wyoming_tts_voice_catalog_mem: List[Dict[str, str]] = []
_wyoming_tts_voice_catalog_meta_mem: Dict[str, Any] = {
    "host": "",
    "port": 0,
    "count": 0,
    "last_refresh_ts": 0.0,
    "last_error": "",
}


def _compat_bridge_enabled() -> bool:
    return True


def _compat_require_adopted() -> bool:
    return False


def _compat_event_backlog() -> int:
    value = _get_int_platform_setting("VOICE_COMPAT_EVENT_BACKLOG", DEFAULT_COMPAT_EVENT_BACKLOG)
    return max(20, min(2000, int(value)))


def _adopted_selector_set() -> set[str]:
    return set()


def _selector_normalize(value: Any) -> str:
    return _text(value)


def _selector_allowed(selector: str) -> bool:
    token = _selector_normalize(selector)
    if not token:
        return False
    return True


async def _compat_require_selector(selector: str) -> str:
    token = _selector_normalize(selector)
    if not token:
        raise HTTPException(status_code=400, detail="selector is required.")
    if not _compat_bridge_enabled():
        raise HTTPException(status_code=503, detail="Compatibility bridge is disabled in Voice settings.")
    if not _selector_allowed(token):
        raise HTTPException(status_code=403, detail="Selector is not allowed by current Voice compatibility settings.")
    return token


def _compat_bridge_selector_state(selector: str) -> Dict[str, Any]:
    selectors = _native_compat_bridge.get("selectors")
    if not isinstance(selectors, dict):
        selectors = {}
        _native_compat_bridge["selectors"] = selectors
    row = selectors.get(selector)
    if isinstance(row, dict):
        return row
    row = {
        "selector": selector,
        "connected": False,
        "transport": "",
        "protocol": "",
        "name": "",
        "host": "",
        "area_name": "",
        "satellite_id": "",
        "metadata": {},
        "last_seen_ts": 0.0,
        "last_connect_ts": 0.0,
        "last_disconnect_ts": 0.0,
        "last_error": "",
        "last_session_id": "",
        "event_cursor": 0,
        "events": [],
    }
    selectors[selector] = row
    return row


async def _compat_emit_event(selector: str, event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    async with _native_compat_bridge_lock:
        seq = int(_native_compat_bridge.get("seq") or 0) + 1
        _native_compat_bridge["seq"] = seq
        row = _compat_bridge_selector_state(selector)
        item = {
            "seq": seq,
            "ts": _native_now(),
            "selector": selector,
            "type": _text(event_type) or "event",
            "payload": payload if isinstance(payload, dict) else {},
        }
        events = row.get("events")
        if not isinstance(events, list):
            events = []
            row["events"] = events
        events.append(item)
        backlog = _compat_event_backlog()
        if len(events) > backlog:
            del events[:-backlog]
        row["event_cursor"] = seq
        row["last_seen_ts"] = item["ts"]
        return item


async def _compat_set_connected(selector: str, *, connected: bool, info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = info if isinstance(info, dict) else {}
    async with _native_compat_bridge_lock:
        row = _compat_bridge_selector_state(selector)
        now = _native_now()
        row["connected"] = bool(connected)
        row["last_seen_ts"] = now
        if connected:
            row["last_connect_ts"] = now
            row["last_error"] = ""
        else:
            row["last_disconnect_ts"] = now
        for key in ("transport", "protocol", "name", "host", "area_name", "satellite_id"):
            value = _text(payload.get(key))
            if value:
                row[key] = value
        meta = payload.get("metadata")
        if isinstance(meta, dict) and meta:
            base_meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            row["metadata"] = {**base_meta, **meta}

    await _compat_emit_event(
        selector,
        "connection",
        {
            "connected": bool(connected),
            "reason": _text(payload.get("reason")),
        },
    )
    return await _compat_get_selector_status(selector)


async def _compat_set_last_session(selector: str, session_id: str) -> None:
    async with _native_compat_bridge_lock:
        row = _compat_bridge_selector_state(selector)
        row["last_session_id"] = _text(session_id)
        row["last_seen_ts"] = _native_now()


async def _compat_set_error(selector: str, error_text: str) -> None:
    msg = _text(error_text)
    async with _native_compat_bridge_lock:
        row = _compat_bridge_selector_state(selector)
        row["last_error"] = msg
        row["last_seen_ts"] = _native_now()
    await _compat_emit_event(selector, "error", {"text": msg})


async def _compat_get_selector_status(selector: str) -> Dict[str, Any]:
    async with _native_compat_bridge_lock:
        row = dict(_compat_bridge_selector_state(selector))
        row["events"] = []
        return row


async def _compat_pull_events(selector: str, *, since: int, limit: int) -> Dict[str, Any]:
    max_limit = max(1, min(500, int(limit)))
    since_seq = max(0, int(since))
    async with _native_compat_bridge_lock:
        row = _compat_bridge_selector_state(selector)
        events = row.get("events")
        if not isinstance(events, list):
            events = []
        out = [item for item in events if int(item.get("seq") or 0) > since_seq]
        if len(out) > max_limit:
            out = out[-max_limit:]
        cursor = int(row.get("event_cursor") or _native_compat_bridge.get("seq") or 0)
        return {"selector": selector, "cursor": cursor, "events": out}


async def _compat_emit_pipeline_state(selector: str, *, session_id: str, state: str, extra: Optional[Dict[str, Any]] = None) -> None:
    payload = {"session_id": _text(session_id), "state": _text(state)}
    if isinstance(extra, dict) and extra:
        payload.update(extra)
    await _compat_emit_event(selector, "pipeline_state", payload)


def _esphome_native_enabled() -> bool:
    return True


def _esphome_api_port() -> int:
    value = _get_int_platform_setting("VOICE_ESPHOME_API_PORT", DEFAULT_ESPHOME_API_PORT)
    if value <= 0:
        return DEFAULT_ESPHOME_API_PORT
    return int(value)


def _esphome_connect_timeout_s() -> float:
    value = _get_float_platform_setting("VOICE_ESPHOME_CONNECT_TIMEOUT_S", DEFAULT_ESPHOME_CONNECT_TIMEOUT_S)
    return max(3.0, float(value))


def _esphome_retry_seconds() -> int:
    value = _get_int_platform_setting("VOICE_ESPHOME_RETRY_SECONDS", DEFAULT_ESPHOME_RETRY_SECONDS)
    return max(3, int(value))


def _esphome_no_voice_timeout_s() -> float:
    value = _get_float_platform_setting("VOICE_ESPHOME_NO_VOICE_TIMEOUT_S", DEFAULT_ESPHOME_NO_VOICE_TIMEOUT_S)
    return max(3.0, min(20.0, float(value)))


def _esphome_server_vad_enabled() -> bool:
    return _get_bool_platform_setting("VOICE_ESPHOME_SERVER_VAD_ENABLED", DEFAULT_ESPHOME_SERVER_VAD_ENABLED)


def _esphome_server_vad_threshold_dbfs() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_THRESHOLD_DBFS",
        DEFAULT_ESPHOME_SERVER_VAD_THRESHOLD_DBFS,
    )
    return min(-5.0, max(-80.0, float(value)))


def _esphome_server_vad_silence_s() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_SILENCE_SECONDS",
        DEFAULT_ESPHOME_SERVER_VAD_SILENCE_SECONDS,
    )
    return min(5.0, max(0.25, float(value)))


def _esphome_server_vad_drop_db() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_DROP_DB",
        DEFAULT_ESPHOME_SERVER_VAD_DROP_DB,
    )
    return min(40.0, max(4.0, float(value)))


def _esphome_server_vad_trigger_margin_db() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_TRIGGER_MARGIN_DB",
        DEFAULT_ESPHOME_SERVER_VAD_TRIGGER_MARGIN_DB,
    )
    return min(30.0, max(2.0, float(value)))


def _esphome_server_vad_release_margin_db() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_RELEASE_MARGIN_DB",
        DEFAULT_ESPHOME_SERVER_VAD_RELEASE_MARGIN_DB,
    )
    return min(20.0, max(1.0, float(value)))


def _esphome_server_vad_strong_margin_db() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_STRONG_MARGIN_DB",
        DEFAULT_ESPHOME_SERVER_VAD_STRONG_MARGIN_DB,
    )
    return min(20.0, max(1.0, float(value)))


def _esphome_server_vad_strong_streak_chunks() -> int:
    value = _get_int_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_STRONG_STREAK_CHUNKS",
        DEFAULT_ESPHOME_SERVER_VAD_STRONG_STREAK_CHUNKS,
    )
    return min(30, max(2, int(value)))


def _esphome_server_vad_max_release_above_floor_db() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_MAX_RELEASE_ABOVE_FLOOR_DB",
        DEFAULT_ESPHOME_SERVER_VAD_MAX_RELEASE_ABOVE_FLOOR_DB,
    )
    return min(20.0, max(2.0, float(value)))


def _esphome_server_vad_min_speech_chunks() -> int:
    value = _get_int_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_MIN_SPEECH_CHUNKS",
        DEFAULT_ESPHOME_SERVER_VAD_MIN_SPEECH_CHUNKS,
    )
    return min(60, max(1, int(value)))


def _esphome_server_vad_min_speech_seconds() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_SERVER_VAD_MIN_SPEECH_SECONDS",
        DEFAULT_ESPHOME_SERVER_VAD_MIN_SPEECH_SECONDS,
    )
    return min(3.0, max(0.05, float(value)))


def _esphome_auto_target_manual() -> bool:
    return _get_bool_platform_setting("VOICE_ESPHOME_AUTO_TARGET_MANUAL", True)


def _esphome_password() -> str:
    return _text(_portal_settings().get("VOICE_ESPHOME_PASSWORD"))


def _esphome_noise_psk() -> str:
    return _text(_portal_settings().get("VOICE_ESPHOME_NOISE_PSK"))


def _esphome_import() -> Tuple[Optional[Any], str]:
    try:
        module = importlib.import_module("aioesphomeapi")
        return module, ""
    except Exception as exc:
        return None, str(exc)


def _esphome_module_attr(module: Any, name: str) -> Any:
    if module is not None:
        value = getattr(module, name, None)
        if value is not None:
            return value
        sub_model = getattr(module, "model", None)
        if sub_model is not None:
            value = getattr(sub_model, name, None)
            if value is not None:
                return value
    with contextlib.suppress(Exception):
        model_module = importlib.import_module("aioesphomeapi.model")
        value = getattr(model_module, name, None)
        if value is not None:
            return value
    return None


def _esphome_event_type_value(module: Any, *candidates: str) -> Any:
    enum_cls = _esphome_module_attr(module, "VoiceAssistantEventType")
    if enum_cls is None:
        return None
    for candidate in candidates:
        token = _text(candidate)
        if not token:
            continue
        direct = getattr(enum_cls, token, None)
        if direct is not None:
            return direct
    wanted = {_lower(item) for item in candidates if _text(item)}
    for attr_name in dir(enum_cls):
        if _lower(attr_name) in wanted:
            return getattr(enum_cls, attr_name, None)
    return None


def _esphome_payload_strings(data: Optional[Dict[str, Any]]) -> Dict[str, str]:
    payload = data if isinstance(data, dict) else {}
    out: Dict[str, str] = {}
    for key, value in payload.items():
        token = _text(key)
        if not token:
            continue
        if value is None:
            continue
        if isinstance(value, bool):
            out[token] = "1" if value else "0"
            continue
        out[token] = str(value)
    return out


def _esphome_audio_settings_format(audio_settings: Any) -> Dict[str, int]:
    base = {
        "rate": int(DEFAULT_VOICE_SAMPLE_RATE_HZ),
        "width": int(DEFAULT_VOICE_SAMPLE_WIDTH),
        "channels": int(DEFAULT_VOICE_CHANNELS),
    }
    source = audio_settings if audio_settings is not None else {}

    def _read_int(candidates: List[str], default: int) -> int:
        for candidate in candidates:
            value = None
            if isinstance(source, dict):
                value = source.get(candidate)
            else:
                value = getattr(source, candidate, None)
            try:
                parsed = int(value)
            except Exception:
                continue
            if parsed > 0:
                return parsed
        return int(default)

    return {
        "rate": _read_int(["rate", "sample_rate", "sample_rate_hz"], base["rate"]),
        "width": _read_int(["width", "sample_width", "sample_width_bytes"], base["width"]),
        "channels": _read_int(["channels", "num_channels"], base["channels"]),
    }


def _esphome_pcm_dbfs(audio_bytes: bytes, *, sample_width: int) -> Optional[float]:
    data = bytes(audio_bytes or b"")
    width = int(sample_width or DEFAULT_VOICE_SAMPLE_WIDTH)
    if not data or width < 1 or width > 4:
        return None

    frame_size = max(1, width)
    usable = len(data) - (len(data) % frame_size)
    if usable <= 0:
        return None
    if usable != len(data):
        data = data[:usable]

    with contextlib.suppress(Exception):
        rms = float(audioop.rms(data, width))
        if rms <= 0.0:
            return -120.0
        full_scale = float((1 << ((8 * width) - 1)) - 1)
        if full_scale <= 0.0:
            return None
        normalized = min(1.0, max(rms / full_scale, 1e-9))
        return 20.0 * math.log10(normalized)
    return None


def _esphome_chunk_seconds(audio_bytes: bytes, audio_format: Dict[str, Any]) -> float:
    data = bytes(audio_bytes or b"")
    if not data:
        return 0.0
    rate = int(audio_format.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or DEFAULT_VOICE_CHANNELS)
    frame_bytes = max(1, width * channels)
    samples = len(data) / float(frame_bytes)
    return max(0.0, samples / float(max(1, rate)))


def _esphome_voice_feature_snapshot(info: Any, client: Any, module: Any) -> Dict[str, Any]:
    flags = 0
    api_audio_bit = 0
    speaker_bit = 0
    feature_enum = _esphome_module_attr(module, "VoiceAssistantFeature")
    if feature_enum is not None:
        with contextlib.suppress(Exception):
            api_audio_bit = int(getattr(feature_enum, "API_AUDIO"))
        with contextlib.suppress(Exception):
            speaker_bit = int(getattr(feature_enum, "SPEAKER"))

    compat_fn = getattr(info, "voice_assistant_feature_flags_compat", None)
    if callable(compat_fn):
        with contextlib.suppress(Exception):
            api_version = getattr(client, "api_version", None)
            if api_version is not None:
                flags = int(compat_fn(api_version) or 0)
            else:
                flags = int(compat_fn() or 0)
    if not flags:
        for attr in ("voice_assistant_feature_flags", "voice_assistant_feature_flags_compat"):
            value = getattr(info, attr, None)
            if callable(value):
                with contextlib.suppress(Exception):
                    value = value()
            with contextlib.suppress(Exception):
                parsed = int(value or 0)
                if parsed:
                    flags = parsed
                    break

    api_audio_known = bool(api_audio_bit and flags)
    speaker_known = bool(speaker_bit and flags)
    return {
        "flags": int(flags),
        "api_audio_bit": int(api_audio_bit),
        "speaker_bit": int(speaker_bit),
        "api_audio_known": api_audio_known,
        "speaker_known": speaker_known,
        "api_audio_supported": True if not api_audio_known else bool(api_audio_bit and (int(flags) & int(api_audio_bit))),
        "speaker_supported": True if not speaker_known else bool(speaker_bit and (int(flags) & int(speaker_bit))),
    }


def _esphome_entity_type_token(info: Any) -> str:
    cls = _text(getattr(getattr(info, "__class__", None), "__name__", ""))
    if not cls:
        return ""
    token = _lower(cls)
    if token.endswith("info"):
        token = token[:-4]
    token = token.replace("_", "")
    mapping = {
        "binarysensor": "binary_sensor",
        "textsensor": "text_sensor",
        "numbersensor": "sensor",
        "select": "select",
        "switch": "switch",
        "light": "light",
        "sensor": "sensor",
        "button": "button",
        "cover": "cover",
        "lock": "lock",
        "fan": "fan",
        "climate": "climate",
        "mediaplayer": "media_player",
        "camera": "camera",
    }
    if token in mapping:
        return mapping[token]
    if "sensor" in token:
        return "sensor"
    return token


def _esphome_entity_info_row(info: Any) -> Optional[Dict[str, Any]]:
    entity_type = _esphome_entity_type_token(info)
    key_val = getattr(info, "key", None)
    try:
        entity_key = int(key_val) if key_val is not None else 0
    except Exception:
        entity_key = 0
    object_id = _text(getattr(info, "object_id", None))
    name = _text(getattr(info, "name", None))
    device_class = _text(getattr(info, "device_class", None))
    unit = _text(getattr(info, "unit_of_measurement", None))
    if not object_id and not name and not entity_type and not entity_key:
        return None
    slug = _lower(object_id or name).replace(" ", "_")
    entity_id = f"{entity_type}.{slug}" if entity_type and slug else ""
    return {
        "key": int(entity_key),
        "type": entity_type or "entity",
        "object_id": object_id,
        "name": name,
        "device_class": device_class,
        "unit": unit,
        "entity_id": entity_id,
    }


async def _esphome_fetch_entities_snapshot(client: Any, *, timeout: float) -> List[Dict[str, Any]]:
    method = getattr(client, "list_entities_services", None)
    if not callable(method):
        return []
    try:
        result = method()
    except Exception:
        return []
    if inspect.isawaitable(result):
        with contextlib.suppress(Exception):
            result = await asyncio.wait_for(result, timeout=max(1.0, float(timeout)))

    entries: List[Any] = []
    if isinstance(result, tuple):
        for item in result:
            if isinstance(item, list):
                entries.extend(item)
    elif isinstance(result, list):
        entries = list(result)

    rows: List[Dict[str, Any]] = []
    for item in entries:
        row = _esphome_entity_info_row(item)
        if not isinstance(row, dict):
            continue
        rows.append(row)
    rows.sort(key=lambda row: (_text(row.get("type")), _text(row.get("name")), _text(row.get("object_id"))))
    return rows


async def _esphome_client_call(client: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = getattr(client, method_name, None)
    if not callable(method):
        raise RuntimeError(f"ESPHome client missing method: {method_name}")
    result = method(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _esphome_send_event(
    client: Any,
    module: Any,
    event_candidates: Tuple[str, ...],
    data: Optional[Dict[str, Any]] = None,
) -> bool:
    event_type = _esphome_event_type_value(module, *event_candidates)
    if event_type is None:
        _native_debug(f"esphome event unavailable candidates={event_candidates}")
        return False
    payload = _esphome_payload_strings(data)
    try:
        await _esphome_client_call(
            client,
            "send_voice_assistant_event",
            event_type,
            payload if payload else None,
        )
        return True
    except Exception as exc:
        _native_debug(
            f"esphome event send failed candidates={event_candidates} error={exc}"
        )
        return False


class _ESPHomeVoiceUDPServer(asyncio.DatagramProtocol):
    """UDP bridge for ESPHome devices that do not support API_AUDIO."""

    def __init__(self, selector: str, on_audio: Callable[[bytes], None]) -> None:
        super().__init__()
        self.selector = _text(selector)
        self._on_audio = on_audio
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.remote_addr: Optional[Tuple[str, int]] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.DatagramTransport, transport)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        packet = bytes(data or b"")
        if not packet:
            return
        if self.remote_addr is None:
            self.remote_addr = addr
            _native_debug(
                f"esphome udp peer discovered selector={self.selector} peer={addr[0]}:{addr[1]}"
            )
        elif addr != self.remote_addr:
            return
        with contextlib.suppress(Exception):
            self._on_audio(packet)

    def error_received(self, exc: Exception) -> None:
        logger.warning(
            "[native-voice] esphome udp error selector=%s error=%s",
            self.selector,
            exc,
        )

    def close(self) -> None:
        if self.transport is not None:
            with contextlib.suppress(Exception):
                self.transport.close()
        self.transport = None
        self.remote_addr = None

    def send_audio_bytes(self, data: bytes) -> bool:
        packet = bytes(data or b"")
        if not packet:
            return False
        if self.transport is None or self.remote_addr is None:
            return False
        with contextlib.suppress(Exception):
            self.transport.sendto(packet, self.remote_addr)
            return True
        return False


def _esphome_close_udp_locked(runtime: Dict[str, Any]) -> None:
    protocol = runtime.get("udp_protocol")
    transport = runtime.get("udp_transport")
    runtime["udp_protocol"] = None
    runtime["udp_transport"] = None
    runtime["udp_port"] = 0
    if isinstance(protocol, _ESPHomeVoiceUDPServer):
        protocol.close()
        return
    if transport is not None:
        with contextlib.suppress(Exception):
            transport.close()


async def _esphome_start_udp_server(
    selector: str,
    runtime: Dict[str, Any],
    on_audio: Callable[[bytes], None],
) -> int:
    _esphome_close_udp_locked(runtime)
    loop = asyncio.get_running_loop()

    def _factory() -> _ESPHomeVoiceUDPServer:
        return _ESPHomeVoiceUDPServer(selector, on_audio)

    transport, protocol = await loop.create_datagram_endpoint(
        _factory,
        local_addr=("0.0.0.0", 0),
    )
    udp_protocol = cast(_ESPHomeVoiceUDPServer, protocol)
    udp_transport = cast(asyncio.DatagramTransport, transport)
    sock = udp_transport.get_extra_info("sockname")
    udp_port = int(sock[1]) if isinstance(sock, tuple) and len(sock) >= 2 else 0
    if udp_port <= 0:
        with contextlib.suppress(Exception):
            udp_transport.close()
        raise RuntimeError("failed to allocate UDP audio port")
    runtime["udp_protocol"] = udp_protocol
    runtime["udp_transport"] = udp_transport
    runtime["udp_port"] = udp_port
    _native_debug(f"esphome udp server ready selector={_text(selector)} port={udp_port}")
    return udp_port


async def _esphome_stream_tts_audio(client: Any, tts_audio: bytes, *, chunk_size: int = DEFAULT_ESPHOME_TTS_CHUNK_BYTES) -> int:
    data = bytes(tts_audio or b"")
    if not data:
        return 0
    size = max(512, int(chunk_size))
    offset = 0
    chunks = 0
    while offset < len(data):
        chunk = data[offset: offset + size]
        offset += len(chunk)
        await _esphome_client_call(client, "send_voice_assistant_audio", chunk)
        chunks += 1
    return chunks


async def _esphome_stream_tts_audio_udp(
    protocol: Optional[_ESPHomeVoiceUDPServer],
    tts_audio: bytes,
    *,
    audio_format: Optional[Dict[str, Any]],
) -> int:
    data = bytes(tts_audio or b"")
    if not data:
        return 0
    if not isinstance(protocol, _ESPHomeVoiceUDPServer):
        return 0
    if protocol.remote_addr is None:
        return 0

    fmt = audio_format if isinstance(audio_format, dict) else {}
    rate = int(fmt.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(fmt.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(fmt.get("channels") or DEFAULT_VOICE_CHANNELS)
    if rate <= 0:
        rate = int(DEFAULT_VOICE_SAMPLE_RATE_HZ)
    if width <= 0:
        width = int(DEFAULT_VOICE_SAMPLE_WIDTH)
    if channels <= 0:
        channels = int(DEFAULT_VOICE_CHANNELS)

    frame_bytes = max(1, width * channels)
    chunk_bytes = max(frame_bytes, frame_bytes * int(DEFAULT_ESPHOME_UDP_TTS_SAMPLES_PER_CHUNK))
    offset = 0
    chunks = 0
    while offset < len(data):
        chunk = data[offset: offset + chunk_bytes]
        offset += len(chunk)
        if not chunk:
            continue
        if not protocol.send_audio_bytes(chunk):
            break
        chunks += 1
        samples = len(chunk) / float(frame_bytes)
        duration_s = samples / float(max(1, rate))
        await asyncio.sleep(max(0.001, duration_s * 0.9))
    return chunks


def _esphome_url_run_end_timeout_s() -> float:
    value = _get_float_platform_setting(
        "VOICE_ESPHOME_ANNOUNCEMENT_TIMEOUT_S",
        DEFAULT_ESPHOME_ANNOUNCEMENT_TIMEOUT_S,
    )
    return max(2.0, min(35.0, float(value)))


def _esphome_cancel_announcement_wait(runtime: Dict[str, Any]) -> None:
    task = runtime.get("announcement_task")
    if isinstance(task, asyncio.Task):
        if task is asyncio.current_task():
            runtime["announcement_task"] = None
            return
        task.cancel()
    runtime["announcement_task"] = None


async def _esphome_finalize_after_announcement(
    selector: str,
    client: Any,
    module: Any,
    *,
    reason: str,
) -> bool:
    token = _text(selector)
    if not token:
        return False
    runtime = _esphome_voice_runtime_state(token)
    lock = runtime.get("lock")
    if lock is None or not hasattr(lock, "acquire"):
        runtime["lock"] = asyncio.Lock()
        lock = runtime["lock"]
    async with lock:
        waiting = bool(runtime.get("awaiting_announcement"))
        session_id = _text(runtime.get("awaiting_announcement_session_id"))
        if not waiting:
            return False
        runtime["awaiting_announcement"] = False
        runtime["awaiting_announcement_session_id"] = ""
        _esphome_cancel_announcement_wait(runtime)
    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_END", "RUN_END"), None)
    _native_debug(
        f"esphome announcement finalize selector={token} session_id={session_id} reason={reason}"
    )
    return True


def _esphome_schedule_announcement_timeout(
    selector: str,
    client: Any,
    module: Any,
    timeout_s: float,
) -> None:
    token = _text(selector)
    if not token:
        return
    runtime = _esphome_voice_runtime_state(token)

    async def _timer() -> None:
        try:
            await asyncio.sleep(max(0.2, float(timeout_s)))
            completed = await _esphome_finalize_after_announcement(
                token,
                client,
                module,
                reason="announcement_timeout",
            )
            if completed:
                logger.info(
                    "[native-voice] announcement timeout finalize selector=%s timeout_s=%.2f",
                    token,
                    float(timeout_s),
                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            _native_debug(f"esphome announcement timeout task failed selector={token} error={exc}")

    task = asyncio.create_task(_timer())
    runtime["announcement_task"] = task


def _esphome_tts_url_ttl_s() -> float:
    value = _get_float_platform_setting("VOICE_ESPHOME_TTS_URL_TTL_S", DEFAULT_ESPHOME_TTS_URL_TTL_S)
    return max(30.0, min(900.0, float(value)))


def _voice_core_service_host_for_peer(peer_host: str) -> str:
    env_host = _text(os.getenv("VOICE_CORE_PUBLIC_HOST"))
    if env_host:
        return env_host

    if _text(BIND_HOST) and BIND_HOST not in {"0.0.0.0", "::"}:
        return _text(BIND_HOST)

    targets: List[str] = []
    peer = _lower(peer_host)
    if peer and not peer.startswith("127."):
        targets.append(peer)
    targets.append("8.8.8.8")

    for target in targets:
        with contextlib.suppress(Exception):
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                probe.connect((target, 80))
                host = _text(probe.getsockname()[0])
                if host and not host.startswith("127."):
                    return host

    with contextlib.suppress(Exception):
        host = _text(socket.gethostbyname(socket.gethostname()))
        if host and not host.startswith("127."):
            return host
    return "127.0.0.1"


def _esphome_selector_host(selector: str) -> str:
    token = _text(selector)
    if token.startswith("host:"):
        return _lower(token.split(":", 1)[1])
    registry = _load_voice_satellite_registry()
    for row in registry:
        if _text(row.get("selector")) != token:
            continue
        host = _lower(row.get("host"))
        if host:
            return host
    return ""


def _esphome_trim_tts_pcm(audio_bytes: bytes, audio_format: Optional[Dict[str, Any]]) -> bytes:
    data = bytes(audio_bytes or b"")
    if not data:
        return b""
    if not _get_bool_platform_setting("VOICE_ESPHOME_TTS_TRIM_ENABLED", DEFAULT_ESPHOME_TTS_TRIM_ENABLED):
        return data

    fmt = audio_format if isinstance(audio_format, dict) else {}
    rate = int(fmt.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(fmt.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(fmt.get("channels") or DEFAULT_VOICE_CHANNELS)
    if width < 1 or width > 4:
        return data
    if channels < 1:
        return data

    frame_bytes = width * channels
    if frame_bytes <= 0:
        return data
    usable = len(data) - (len(data) % frame_bytes)
    if usable <= 0:
        return data
    if usable != len(data):
        data = data[:usable]

    window_ms = 20
    window_frames = max(1, int((rate * window_ms) / 1000))
    window_bytes = max(frame_bytes, window_frames * frame_bytes)
    total_windows = max(1, len(data) // window_bytes)
    if total_windows <= 1:
        return data

    threshold_dbfs = _get_float_platform_setting(
        "VOICE_ESPHOME_TTS_TRIM_THRESHOLD_DBFS",
        DEFAULT_ESPHOME_TTS_TRIM_THRESHOLD_DBFS,
    )
    full_scale = float((1 << ((8 * width) - 1)) - 1)
    if full_scale <= 0.0:
        return data
    normalized = min(1.0, max(10.0 ** (float(threshold_dbfs) / 20.0), 1e-6))
    threshold_rms = max(1.0, full_scale * normalized)

    lead_ms = max(0.0, _get_float_platform_setting("VOICE_ESPHOME_TTS_TRIM_LEAD_MS", DEFAULT_ESPHOME_TTS_TRIM_LEAD_MS))
    tail_ms = max(0.0, _get_float_platform_setting("VOICE_ESPHOME_TTS_TRIM_TAIL_MS", DEFAULT_ESPHOME_TTS_TRIM_TAIL_MS))
    keep_lead_windows = int((lead_ms + (window_ms - 1)) // window_ms)
    keep_tail_windows = int((tail_ms + (window_ms - 1)) // window_ms)

    active_first: Optional[int] = None
    active_last: Optional[int] = None
    for idx in range(total_windows):
        start = idx * window_bytes
        end = min(len(data), start + window_bytes)
        chunk = data[start:end]
        if not chunk:
            continue
        with contextlib.suppress(Exception):
            rms = float(audioop.rms(chunk, width))
            if rms >= threshold_rms:
                if active_first is None:
                    active_first = idx
                active_last = idx
                continue
    if active_first is None or active_last is None:
        return data

    start_window = max(0, int(active_first) - keep_lead_windows)
    end_window = min(total_windows, int(active_last) + 1 + keep_tail_windows)
    start_offset = start_window * window_bytes
    end_offset = min(len(data), end_window * window_bytes)
    if start_offset <= 0 and end_offset >= len(data):
        return data
    trimmed = data[start_offset:end_offset]
    return trimmed if trimmed else data


def _esphome_pcm_to_wav(audio_bytes: bytes, audio_format: Optional[Dict[str, Any]]) -> Tuple[bytes, Dict[str, int]]:
    pcm = _esphome_trim_tts_pcm(audio_bytes, audio_format)
    if not pcm:
        return b"", {
            "rate": int(DEFAULT_VOICE_SAMPLE_RATE_HZ),
            "width": int(DEFAULT_VOICE_SAMPLE_WIDTH),
            "channels": int(DEFAULT_VOICE_CHANNELS),
        }

    fmt = audio_format if isinstance(audio_format, dict) else {}
    rate = int(fmt.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(fmt.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(fmt.get("channels") or DEFAULT_VOICE_CHANNELS)

    if rate < 8000 or rate > 192000:
        rate = int(DEFAULT_VOICE_SAMPLE_RATE_HZ)
    if width not in {1, 2, 3, 4}:
        width = int(DEFAULT_VOICE_SAMPLE_WIDTH)
    if channels < 1 or channels > 8:
        channels = int(DEFAULT_VOICE_CHANNELS)

    block_align = max(1, width * channels)
    usable = len(pcm) - (len(pcm) % block_align)
    if usable <= 0:
        return b"", {
            "rate": rate,
            "width": width,
            "channels": channels,
        }
    if usable != len(pcm):
        pcm = pcm[:usable]

    with io.BytesIO() as out:
        with wave.open(out, "wb") as wav_file:
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(width)
            wav_file.setframerate(rate)
            wav_file.writeframes(pcm)
        return out.getvalue(), {
            "rate": rate,
            "width": width,
            "channels": channels,
        }


def _esphome_tts_url_prune_locked(now_ts: Optional[float] = None) -> int:
    now = float(now_ts if isinstance(now_ts, (int, float)) else _native_now())
    removed = 0
    for stream_id, row in list(_esphome_tts_url_store.items()):
        if not isinstance(row, dict):
            _esphome_tts_url_store.pop(stream_id, None)
            removed += 1
            continue
        expires_ts = float(row.get("expires_ts") or 0.0)
        if expires_ts > 0.0 and now >= expires_ts:
            _esphome_tts_url_store.pop(stream_id, None)
            removed += 1
    return removed


def _esphome_store_tts_url(selector: str, session_id: str, audio_bytes: bytes, audio_format: Optional[Dict[str, Any]]) -> str:
    wav_bytes, normalized_format = _esphome_pcm_to_wav(audio_bytes, audio_format)
    if not wav_bytes:
        return ""

    stream_id = uuid.uuid4().hex
    expires_ts = _native_now() + _esphome_tts_url_ttl_s()
    with _esphome_tts_url_store_lock:
        _esphome_tts_url_prune_locked()
        _esphome_tts_url_store[stream_id] = {
            "id": stream_id,
            "selector": _text(selector),
            "session_id": _text(session_id),
            "created_ts": _native_now(),
            "expires_ts": expires_ts,
            "audio_format": normalized_format,
            "wav_bytes": wav_bytes,
        }

    host = _voice_core_service_host_for_peer(_esphome_selector_host(selector))
    port = int(_voice_core_runtime_port or DEFAULT_VOICE_CORE_BIND_PORT)
    url = f"http://{host}:{port}/tater-ha/v1/voice/esphome/tts/{stream_id}.wav"
    _native_debug(
        f"esphome tts url prepared selector={_text(selector)} session_id={_text(session_id)} bytes={len(wav_bytes)} url={url}"
    )
    return url


def _esphome_fetch_tts_url(stream_id: str) -> Optional[Dict[str, Any]]:
    token = _text(stream_id)
    if not token:
        return None
    with _esphome_tts_url_store_lock:
        _esphome_tts_url_prune_locked()
        row = _esphome_tts_url_store.get(token)
        if not isinstance(row, dict):
            return None
        return dict(row)


async def _esphome_selector_speaker_supported(selector: str) -> bool:
    token = _text(selector)
    if not token:
        return True
    async with _esphome_native_lock:
        row = _esphome_native_clients.get(token)
    if isinstance(row, dict):
        value = row.get("voice_speaker_supported")
        if isinstance(value, bool):
            return value
    return True


def _esphome_voice_runtime_state(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    row = _esphome_voice_runtime.get(token)
    if not isinstance(row, dict):
        row = {
            "session_id": "",
            "conversation_id": "",
            "audio_format": {
                "rate": int(DEFAULT_VOICE_SAMPLE_RATE_HZ),
                "width": int(DEFAULT_VOICE_SAMPLE_WIDTH),
                "channels": int(DEFAULT_VOICE_CHANNELS),
            },
            "session_start_ts": 0.0,
            "last_audio_ts": 0.0,
            "audio_chunks": 0,
            "audio_bytes": 0,
            "vad_voice_seen": False,
            "vad_soft_speech_chunks": 0,
            "vad_speech_chunks": 0,
            "vad_speech_seconds": 0.0,
            "vad_last_strong_speech_ts": 0.0,
            "vad_strong_streak": 0,
            "vad_silence_start_ts": 0.0,
            "vad_last_dbfs": None,
            "vad_noise_floor_dbfs": None,
            "vad_peak_dbfs": None,
            "vad_dynamic_trigger_dbfs": None,
            "vad_dynamic_release_dbfs": None,
            "vad_start_sent": False,
            "api_audio_supported": True,
            "udp_transport": None,
            "udp_protocol": None,
            "udp_port": 0,
            "awaiting_announcement": False,
            "awaiting_announcement_session_id": "",
            "announcement_task": None,
            "watchdog_task": None,
            "lock": asyncio.Lock(),
        }
        _esphome_voice_runtime[token] = row
    lock = row.get("lock")
    if lock is None or not hasattr(lock, "acquire"):
        row["lock"] = asyncio.Lock()
    return row


def _esphome_cancel_watchdog(runtime: Dict[str, Any]) -> None:
    task = runtime.get("watchdog_task")
    if isinstance(task, asyncio.Task):
        if task is asyncio.current_task():
            runtime["watchdog_task"] = None
            return
        task.cancel()
    runtime["watchdog_task"] = None


async def _esphome_session_watchdog(selector: str, client: Any, module: Any, session_id: str) -> None:
    token = _text(selector)
    sid = _text(session_id)
    if not token or not sid:
        return

    idle_timeout = max(0.8, float(DEFAULT_ESPHOME_AUDIO_IDLE_TIMEOUT_S))
    max_listen = max(idle_timeout + 1.0, float(DEFAULT_ESPHOME_SESSION_MAX_LISTEN_SECONDS))
    no_voice_timeout = _esphome_no_voice_timeout_s()

    while True:
        await asyncio.sleep(0.35)
        runtime = _esphome_voice_runtime.get(token)
        if not isinstance(runtime, dict):
            return
        current_session = _text(runtime.get("session_id"))
        if not current_session or current_session != sid:
            return

        now = _native_monotonic()
        chunks = int(runtime.get("audio_chunks") or 0)
        last_audio_ts = float(runtime.get("last_audio_ts") or 0.0)
        start_ts = float(runtime.get("session_start_ts") or 0.0)
        voice_seen = bool(runtime.get("vad_voice_seen"))

        if chunks > 0 and last_audio_ts > 0.0 and (now - last_audio_ts) >= idle_timeout:
            logger.info(
                "[native-voice] watchdog finalize selector=%s session_id=%s reason=idle_timeout chunks=%s bytes=%s",
                token,
                sid,
                chunks,
                int(runtime.get("audio_bytes") or 0),
            )
            with contextlib.suppress(Exception):
                await _esphome_finalize_voice_session(
                    token,
                    client,
                    module,
                    abort=False,
                    reason="idle_timeout",
                )
            return

        if chunks > 0 and (not voice_seen) and start_ts > 0.0 and (now - start_ts) >= no_voice_timeout:
            logger.info(
                "[native-voice] watchdog finalize selector=%s session_id=%s reason=no_voice_timeout chunks=%s bytes=%s",
                token,
                sid,
                chunks,
                int(runtime.get("audio_bytes") or 0),
            )
            with contextlib.suppress(Exception):
                await _esphome_finalize_voice_session(
                    token,
                    client,
                    module,
                    abort=False,
                    reason="no_voice_timeout",
                )
            return

        if start_ts > 0.0 and (now - start_ts) >= max_listen:
            logger.info(
                "[native-voice] watchdog finalize selector=%s session_id=%s reason=max_listen_timeout chunks=%s bytes=%s",
                token,
                sid,
                chunks,
                int(runtime.get("audio_bytes") or 0),
            )
            with contextlib.suppress(Exception):
                await _esphome_finalize_voice_session(
                    token,
                    client,
                    module,
                    abort=False,
                    reason="max_listen_timeout",
                )
            return


async def _native_mark_session_aborted(session_id: str, reason: str) -> None:
    token = _text(session_id)
    if not token:
        return
    async with _native_voice_sessions_lock:
        session = _native_voice_sessions.get(token)
        if not isinstance(session, dict):
            return
        session["processing"] = False
        session["error"] = _text(reason) or "aborted"
        session["expires_ts"] = _native_now() + _native_session_ttl_s()
        _native_session_set_state(session, VOICE_STATE_IDLE)


async def _esphome_finalize_voice_session(
    selector: str,
    client: Any,
    module: Any,
    *,
    abort: bool,
    reason: str = "",
) -> None:
    token = _text(selector)
    if not token:
        return
    runtime = _esphome_voice_runtime_state(token)
    lock = runtime.get("lock")
    if lock is None or not hasattr(lock, "acquire"):
        runtime["lock"] = asyncio.Lock()
        lock = runtime["lock"]

    async with lock:
        _esphome_cancel_watchdog(runtime)
        _esphome_cancel_announcement_wait(runtime)
        session_id = _text(runtime.get("session_id"))
        conversation_id = _text(runtime.get("conversation_id"))
        api_audio_supported = bool(runtime.get("api_audio_supported", True))
        udp_protocol = runtime.get("udp_protocol")
        runtime["session_id"] = ""
        runtime["conversation_id"] = ""
        runtime["session_start_ts"] = 0.0
        runtime["last_audio_ts"] = 0.0
        runtime["vad_voice_seen"] = False
        runtime["vad_soft_speech_chunks"] = 0
        runtime["vad_speech_chunks"] = 0
        runtime["vad_speech_seconds"] = 0.0
        runtime["vad_last_strong_speech_ts"] = 0.0
        runtime["vad_strong_streak"] = 0
        runtime["vad_silence_start_ts"] = 0.0
        runtime["vad_last_dbfs"] = None
        runtime["vad_noise_floor_dbfs"] = None
        runtime["vad_peak_dbfs"] = None
        runtime["vad_dynamic_trigger_dbfs"] = None
        runtime["vad_dynamic_release_dbfs"] = None
        runtime["vad_start_sent"] = False
        runtime["awaiting_announcement"] = False
        runtime["awaiting_announcement_session_id"] = ""
        if bool(abort):
            _esphome_close_udp_locked(runtime)

    if not session_id:
        return

    if abort:
        await _native_mark_session_aborted(session_id, reason or "device_stopped")
        await _compat_emit_event(
            token,
            "esphome_session_aborted",
            {"session_id": session_id, "reason": _text(reason) or "device_stopped"},
        )
        logger.info(
            "[native-voice] session aborted selector=%s session_id=%s reason=%s",
            token,
            session_id,
            _text(reason) or "device_stopped",
        )
        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_END", "RUN_END"), None)
        return

    close_udp_after_finalize = not api_audio_supported
    try:
        # Home Assistant emits STT_VAD_END before microphone stop/awaiting-response transitions.
        with contextlib.suppress(Exception):
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_STT_VAD_END", "STT_VAD_END"),
                None,
            )
        await _native_append_audio_chunk(
            session_id,
            VoiceNativeSessionAudioIn(audio_base64="", final_chunk=True),
        )
        result = await _native_process_session(session_id)
        transcript = _text(result.get("transcript"))
        response_text = _text(result.get("response_text"))
        tts_b64 = _text(result.get("tts_audio_base64"))
        tts_audio = base64.b64decode(tts_b64) if tts_b64 else b""
        tts_format = result.get("tts_audio_format") if isinstance(result.get("tts_audio_format"), dict) else {}
        speaker_supported = await _esphome_selector_speaker_supported(token)
        tts_mode = "stream_api" if (speaker_supported and api_audio_supported) else (
            "stream_udp" if speaker_supported else "url"
        )
        tts_url = "voice-assistant://stream"
        if not speaker_supported:
            prepared_url = _esphome_store_tts_url(token, session_id, tts_audio, tts_format)
            if prepared_url:
                tts_url = prepared_url
            else:
                # Fallback when URL streaming could not be prepared; continue with API audio stream.
                speaker_supported = True
                tts_mode = "stream_api_fallback" if api_audio_supported else "stream_udp_fallback"
        wait_for_announcement = (not speaker_supported) and tts_url.startswith(("http://", "https://"))
        run_end_timeout_s = _esphome_url_run_end_timeout_s() if wait_for_announcement else 0.0

        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_STT_END", "STT_END"),
            {"text": transcript},
        )
        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_INTENT_START", "INTENT_START"),
            None,
        )
        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_INTENT_END", "INTENT_END"),
            {
                "conversation_id": conversation_id or session_id,
                "continue_conversation": "0",
            },
        )
        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_TTS_START", "TTS_START"),
            {"text": response_text},
        )
        tts_chunks = 0
        if speaker_supported:
            # Matches ESPHome/Assist event flow for streamed local speaker output.
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_INTENT_PROGRESS", "INTENT_PROGRESS"),
                {"tts_start_streaming": "1"},
            )
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_TTS_STREAM_START", "TTS_STREAM_START"),
                None,
            )
            if api_audio_supported:
                tts_chunks = await _esphome_stream_tts_audio(client, tts_audio)
            else:
                tts_chunks = await _esphome_stream_tts_audio_udp(
                    udp_protocol if isinstance(udp_protocol, _ESPHomeVoiceUDPServer) else None,
                    tts_audio,
                    audio_format=tts_format,
                )
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_TTS_STREAM_END", "TTS_STREAM_END"),
                None,
            )
        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_TTS_END", "TTS_END"),
            {"url": tts_url},
        )
        if wait_for_announcement:
            async with lock:
                runtime["awaiting_announcement"] = True
                runtime["awaiting_announcement_session_id"] = session_id
                _esphome_cancel_announcement_wait(runtime)
                _esphome_schedule_announcement_timeout(
                    token,
                    client,
                    module,
                    timeout_s=run_end_timeout_s,
                )
            _native_debug(
                f"esphome awaiting announcement_finished selector={token} session_id={session_id} timeout_s={run_end_timeout_s:.2f}"
            )
        else:
            await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_END", "RUN_END"), None)
        await _compat_emit_event(
            token,
            "esphome_session_result",
            {
                "session_id": session_id,
                "conversation_id": conversation_id,
                "transcript": transcript,
                "response_text": response_text,
                "tts_audio_bytes": len(tts_audio),
                "tts_audio_chunks": int(tts_chunks),
                "tts_url": tts_url,
                "tts_mode": tts_mode,
                "run_end_mode": "announcement" if wait_for_announcement else "immediate",
            },
        )
        logger.info(
            "[native-voice] session result selector=%s session_id=%s transcript_len=%s response_len=%s tts_bytes=%s tts_chunks=%s tts_mode=%s run_end_mode=%s tts_url=%s",
            token,
            session_id,
            len(transcript),
            len(response_text),
            len(tts_audio),
            int(tts_chunks),
            tts_mode,
            "announcement" if wait_for_announcement else "immediate",
            tts_url,
        )
    except NoTranscriptError as exc:
        msg = _text(exc) or "No transcript produced from audio."
        logger.info(
            "[native-voice] no transcript selector=%s session_id=%s reason=%s",
            token,
            session_id,
            msg,
        )
        await _compat_set_error(token, msg)
        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_ERROR", "ERROR"),
            {"code": "no_speech_detected", "message": msg},
        )
        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_END", "RUN_END"), None)
    except Exception as exc:
        msg = str(exc)
        logger.warning(
            "[native-voice] session finalize failed selector=%s session_id=%s error=%s",
            token,
            session_id,
            msg,
        )
        await _compat_set_error(token, msg)
        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_ERROR", "ERROR"),
            {"code": "tater_pipeline_error", "message": msg},
        )
        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_END", "RUN_END"), None)
        raise
    finally:
        if close_udp_after_finalize:
            async with lock:
                _esphome_close_udp_locked(runtime)


async def _esphome_subscribe_voice_assistant(
    selector: str,
    client: Any,
    module: Any,
    *,
    api_audio_supported: bool = True,
) -> Callable[[], None]:
    subscribe = getattr(client, "subscribe_voice_assistant", None)
    if not callable(subscribe):
        raise RuntimeError("ESPHome client does not support subscribe_voice_assistant()")

    token = _text(selector)
    if not token:
        raise RuntimeError("selector is required")

    runtime = _esphome_voice_runtime_state(token)

    def _runtime_lock() -> asyncio.Lock:
        lock_obj = runtime.get("lock")
        if lock_obj is None or not hasattr(lock_obj, "acquire"):
            runtime["lock"] = asyncio.Lock()
            lock_obj = runtime["lock"]
        return cast(asyncio.Lock, lock_obj)

    async def _ingest_audio_chunk(data: bytes) -> None:
        lock = _runtime_lock()
        async with lock:
            session_id = _text(runtime.get("session_id"))
        if not session_id:
            return
        audio_bytes = bytes(data or b"")
        if not audio_bytes:
            return
        try:
            await _native_append_audio_chunk(
                session_id,
                VoiceNativeSessionAudioIn(
                    audio_base64=base64.b64encode(audio_bytes).decode("ascii"),
                    final_chunk=False,
                ),
            )
            should_finalize = False
            finalize_details: Dict[str, Any] = {}
            dbfs: Optional[float] = None
            trigger_threshold: Optional[float] = None
            release_threshold: Optional[float] = None
            strong_threshold: Optional[float] = None
            emit_vad_start = False
            async with lock:
                now = _native_monotonic()
                runtime["audio_chunks"] = int(runtime.get("audio_chunks") or 0) + 1
                runtime["audio_bytes"] = int(runtime.get("audio_bytes") or 0) + len(audio_bytes)
                runtime["last_audio_ts"] = now
                chunks = int(runtime.get("audio_chunks") or 0)
                total = int(runtime.get("audio_bytes") or 0)
                audio_format = runtime.get("audio_format") if isinstance(runtime.get("audio_format"), dict) else {}
                chunk_seconds = _esphome_chunk_seconds(audio_bytes, audio_format)
                sample_width = int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
                dbfs = _esphome_pcm_dbfs(audio_bytes, sample_width=sample_width)
                if dbfs is not None:
                    runtime["vad_last_dbfs"] = round(float(dbfs), 2)

                if _esphome_server_vad_enabled() and dbfs is not None:
                    abs_floor = _esphome_server_vad_threshold_dbfs()
                    drop_db = _esphome_server_vad_drop_db()
                    trigger_margin = _esphome_server_vad_trigger_margin_db()
                    release_margin = _esphome_server_vad_release_margin_db()
                    strong_margin = _esphome_server_vad_strong_margin_db()
                    strong_streak_chunks = _esphome_server_vad_strong_streak_chunks()
                    min_speech_chunks = _esphome_server_vad_min_speech_chunks()
                    min_speech_seconds = _esphome_server_vad_min_speech_seconds()
                    silence_target_s = _esphome_server_vad_silence_s()
                    start_ts = float(runtime.get("session_start_ts") or 0.0)

                    floor_prev = runtime.get("vad_noise_floor_dbfs")
                    floor = float(floor_prev) if isinstance(floor_prev, (int, float)) else float(abs_floor)
                    if (start_ts > 0.0) and ((now - start_ts) < float(DEFAULT_ESPHOME_SERVER_VAD_WARMUP_SECONDS)):
                        if dbfs < floor:
                            floor = (floor * 0.85) + (float(dbfs) * 0.15)
                        runtime["vad_noise_floor_dbfs"] = round(float(floor), 2)
                    else:
                        if not bool(runtime.get("vad_voice_seen")) and dbfs <= (floor + trigger_margin):
                            floor = (floor * 0.92) + (float(dbfs) * 0.08)
                        runtime["vad_noise_floor_dbfs"] = round(float(floor), 2)

                        peak_prev = runtime.get("vad_peak_dbfs")
                        peak = float(peak_prev) if isinstance(peak_prev, (int, float)) else float(dbfs)
                        if dbfs > peak:
                            peak = float(dbfs)
                        runtime["vad_peak_dbfs"] = round(float(peak), 2)

                        trigger_threshold = max(float(abs_floor), float(floor) + float(trigger_margin))
                        release_raw = max(float(floor) + float(release_margin), float(peak) - float(drop_db))
                        # Keep release below trigger (hysteresis) and within a sensible band above noise floor.
                        release_upper = float(trigger_threshold) - 0.5
                        release_floor_cap = float(floor) + float(_esphome_server_vad_max_release_above_floor_db())
                        release_lower = float(floor) + 0.5
                        release_threshold = min(float(release_raw), float(release_upper), float(release_floor_cap))
                        release_threshold = max(float(release_threshold), float(release_lower))
                        strong_threshold = max(float(trigger_threshold) + float(strong_margin), float(trigger_threshold) + 1.0)
                        runtime["vad_dynamic_trigger_dbfs"] = round(float(trigger_threshold), 2)
                        runtime["vad_dynamic_release_dbfs"] = round(float(release_threshold), 2)

                        if not bool(runtime.get("vad_voice_seen")):
                            start_detected = False
                            if dbfs >= trigger_threshold:
                                start_detected = True
                                runtime["vad_soft_speech_chunks"] = 0
                            else:
                                soft_trigger_threshold = max(float(abs_floor), float(trigger_threshold) - 1.0)
                                if dbfs >= soft_trigger_threshold:
                                    soft_chunks = int(runtime.get("vad_soft_speech_chunks") or 0) + 1
                                    runtime["vad_soft_speech_chunks"] = soft_chunks
                                    if soft_chunks >= max(2, min_speech_chunks - 1):
                                        start_detected = True
                                else:
                                    runtime["vad_soft_speech_chunks"] = 0

                            if start_detected:
                                runtime["vad_voice_seen"] = True
                                runtime["vad_silence_start_ts"] = now
                                runtime["vad_last_strong_speech_ts"] = 0.0
                                runtime["vad_strong_streak"] = 0
                                if strong_threshold is not None and dbfs >= float(strong_threshold):
                                    streak = int(runtime.get("vad_strong_streak") or 0) + 1
                                    runtime["vad_strong_streak"] = streak
                                    if streak >= strong_streak_chunks:
                                        runtime["vad_speech_chunks"] = int(runtime.get("vad_speech_chunks") or 0) + 1
                                        runtime["vad_speech_seconds"] = float(runtime.get("vad_speech_seconds") or 0.0) + float(chunk_seconds)
                                        runtime["vad_last_strong_speech_ts"] = now
                                        runtime["vad_silence_start_ts"] = 0.0
                                if not bool(runtime.get("vad_start_sent")):
                                    runtime["vad_start_sent"] = True
                                    emit_vad_start = True
                        else:
                            if strong_threshold is not None and dbfs >= float(strong_threshold):
                                streak = int(runtime.get("vad_strong_streak") or 0) + 1
                                runtime["vad_strong_streak"] = streak
                                if streak >= strong_streak_chunks:
                                    runtime["vad_speech_chunks"] = int(runtime.get("vad_speech_chunks") or 0) + 1
                                    runtime["vad_speech_seconds"] = float(runtime.get("vad_speech_seconds") or 0.0) + float(chunk_seconds)
                                    runtime["vad_last_strong_speech_ts"] = now
                                    runtime["vad_silence_start_ts"] = 0.0
                            else:
                                runtime["vad_strong_streak"] = 0
                                silence_start_ts = float(runtime.get("vad_silence_start_ts") or 0.0)
                                if silence_start_ts <= 0.0:
                                    silence_start_ts = now
                                    runtime["vad_silence_start_ts"] = silence_start_ts
                                last_strong_ts = float(runtime.get("vad_last_strong_speech_ts") or 0.0)
                                silence_anchor = last_strong_ts if last_strong_ts > 0.0 else silence_start_ts
                                silence_elapsed = max(0.0, now - silence_anchor)
                                speech_chunks = int(runtime.get("vad_speech_chunks") or 0)
                                speech_seconds = float(runtime.get("vad_speech_seconds") or 0.0)
                                if (
                                    silence_elapsed >= silence_target_s
                                    and (speech_chunks >= min_speech_chunks or speech_seconds >= min_speech_seconds)
                                ):
                                    should_finalize = True
                                    finalize_details = {
                                        "speech_chunks": speech_chunks,
                                        "speech_seconds": speech_seconds,
                                        "silence_elapsed": silence_elapsed,
                                        "dbfs": float(dbfs),
                                        "trigger_threshold": float(trigger_threshold),
                                        "release_threshold": float(release_threshold),
                                        "strong_threshold": float(strong_threshold) if strong_threshold is not None else 0.0,
                                        "noise_floor_dbfs": float(floor),
                                        "peak_dbfs": float(peak),
                                    }
            if chunks in {1, 5, 10} or (chunks > 0 and (chunks % 50 == 0)):
                dbfs_text = "-" if dbfs is None else f"{dbfs:.1f}"
                trigger_text = "-" if trigger_threshold is None else f"{trigger_threshold:.1f}"
                release_text = "-" if release_threshold is None else f"{release_threshold:.1f}"
                strong_text = "-" if strong_threshold is None else f"{strong_threshold:.1f}"
                floor_text = "-"
                peak_text = "-"
                silence_text = "-"
                speech_text = "0.00"
                strong_streak = 0
                voice_seen = False
                async with lock:
                    floor_val = runtime.get("vad_noise_floor_dbfs")
                    peak_val = runtime.get("vad_peak_dbfs")
                    silence_val = runtime.get("vad_silence_start_ts")
                    speech_val = runtime.get("vad_speech_seconds")
                    strong_streak = int(runtime.get("vad_strong_streak") or 0)
                    voice_seen = bool(runtime.get("vad_voice_seen"))
                if isinstance(floor_val, (int, float)):
                    floor_text = f"{float(floor_val):.1f}"
                if isinstance(peak_val, (int, float)):
                    peak_text = f"{float(peak_val):.1f}"
                if isinstance(silence_val, (int, float)) and float(silence_val) > 0.0:
                    silence_text = f"{max(0.0, _native_monotonic() - float(silence_val)):.2f}"
                if isinstance(speech_val, (int, float)):
                    speech_text = f"{max(0.0, float(speech_val)):.2f}"
                _native_debug(
                    f"esphome audio selector={token} session_id={session_id} chunks={chunks} bytes={total} "
                    f"dbfs={dbfs_text} trigger={trigger_text} release={release_text} strong={strong_text} floor={floor_text} "
                    f"peak={peak_text} silence_s={silence_text} speech_s={speech_text} "
                    f"streak={strong_streak} voice_seen={str(voice_seen).lower()}"
                )
            if emit_vad_start:
                sent = await _esphome_send_event(
                    client,
                    module,
                    ("VOICE_ASSISTANT_STT_VAD_START", "STT_VAD_START"),
                    None,
                )
                _native_debug(
                    f"esphome vad_start selector={token} session_id={session_id} sent={bool(sent)}"
                )
            if should_finalize:
                logger.info(
                    "[native-voice] server_vad finalize selector=%s session_id=%s silence_s=%.2f speech_chunks=%s speech_s=%.2f dbfs=%.1f trigger=%.1f release=%.1f strong=%.1f floor=%.1f peak=%.1f",
                    token,
                    session_id,
                    float(finalize_details.get("silence_elapsed") or 0.0),
                    int(finalize_details.get("speech_chunks") or 0),
                    float(finalize_details.get("speech_seconds") or 0.0),
                    float(finalize_details.get("dbfs") or 0.0),
                    float(finalize_details.get("trigger_threshold") or 0.0),
                    float(finalize_details.get("release_threshold") or 0.0),
                    float(finalize_details.get("strong_threshold") or 0.0),
                    float(finalize_details.get("noise_floor_dbfs") or 0.0),
                    float(finalize_details.get("peak_dbfs") or 0.0),
                )
                with contextlib.suppress(Exception):
                    await _esphome_finalize_voice_session(
                        token,
                        client,
                        module,
                        abort=False,
                        reason="server_vad",
                    )
        except HTTPException as exc:
            if int(getattr(exc, "status_code", 0) or 0) in {404, 409}:
                _native_debug(
                    f"esphome audio ignored selector={token} session_id={session_id} status={int(getattr(exc, 'status_code', 0) or 0)}"
                )
                return
            logger.warning(
                "[native-voice] audio ingest failed selector=%s session_id=%s error=%s",
                token,
                session_id,
                exc,
            )
            await _compat_set_error(token, str(exc))
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_ERROR", "ERROR"),
                {"code": "tater_audio_ingest", "message": str(exc)},
            )
        except Exception as exc:
            logger.warning(
                "[native-voice] audio ingest failed selector=%s session_id=%s error=%s",
                token,
                session_id,
                exc,
            )
            await _compat_set_error(token, str(exc))
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_ERROR", "ERROR"),
                {"code": "tater_audio_ingest", "message": str(exc)},
            )

    def _schedule_udp_audio_ingest(data: bytes) -> None:
        packet = bytes(data or b"")
        if not packet:
            return
        asyncio.create_task(_ingest_audio_chunk(packet))

    async def _handle_start(
        conversation_id: str,
        _flags: int,
        audio_settings: Any,
        wake_word_phrase: Optional[str],
    ) -> Optional[int]:
        if WYOMING_IMPORT_ERROR:
            msg = (
                "Wyoming dependency is unavailable in Voice Core runtime. "
                f"Import error: {WYOMING_IMPORT_ERROR}"
            )
            logger.warning("[native-voice] %s selector=%s", msg, token)
            await _compat_set_error(token, msg)
            await _compat_emit_event(
                token,
                "esphome_pipeline_unavailable",
                {"reason": "wyoming_unavailable"},
            )
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_ERROR", "ERROR"),
                {"code": "wyoming_unavailable", "message": msg},
            )
            await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_END", "RUN_END"), None)
            return None

        if _text(runtime.get("session_id")):
            with contextlib.suppress(Exception):
                await _esphome_finalize_voice_session(
                    token,
                    client,
                    module,
                    abort=True,
                    reason="new_session_started",
                )

        audio_format = _esphome_audio_settings_format(audio_settings)
        session = await _native_create_session(
            VoiceNativeSessionStartIn(
                satellite_selector=token,
                wake_word=_text(wake_word_phrase),
                sample_rate_hz=int(audio_format.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ),
                sample_width_bytes=int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH),
                channels=int(audio_format.get("channels") or DEFAULT_VOICE_CHANNELS),
                context={
                    "esphome_conversation_id": _text(conversation_id),
                    "source": "esphome_native",
                },
            )
        )
        session_id = _text(session.get("id"))
        udp_port = 0
        if not api_audio_supported:
            try:
                udp_port = await _esphome_start_udp_server(
                    token,
                    runtime,
                    _schedule_udp_audio_ingest,
                )
            except Exception as exc:
                msg = f"Failed to start UDP audio bridge: {exc}"
                logger.warning("[native-voice] %s selector=%s", msg, token)
                await _compat_set_error(token, msg)
                await _native_mark_session_aborted(session_id, "udp_bridge_start_failed")
                await _esphome_send_event(
                    client,
                    module,
                    ("VOICE_ASSISTANT_ERROR", "ERROR"),
                    {"code": "udp_bridge_start_failed", "message": msg},
                )
                await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_END", "RUN_END"), None)
                return None

        lock = _runtime_lock()
        async with lock:
            _esphome_cancel_watchdog(runtime)
            _esphome_cancel_announcement_wait(runtime)
            if api_audio_supported:
                _esphome_close_udp_locked(runtime)
            runtime["session_id"] = session_id
            runtime["conversation_id"] = _text(conversation_id)
            runtime["audio_format"] = audio_format
            runtime["audio_chunks"] = 0
            runtime["audio_bytes"] = 0
            runtime["session_start_ts"] = _native_monotonic()
            runtime["last_audio_ts"] = 0.0
            runtime["vad_voice_seen"] = False
            runtime["vad_soft_speech_chunks"] = 0
            runtime["vad_speech_chunks"] = 0
            runtime["vad_speech_seconds"] = 0.0
            runtime["vad_last_strong_speech_ts"] = 0.0
            runtime["vad_strong_streak"] = 0
            runtime["vad_silence_start_ts"] = 0.0
            runtime["vad_last_dbfs"] = None
            runtime["vad_noise_floor_dbfs"] = None
            runtime["vad_peak_dbfs"] = None
            runtime["vad_dynamic_trigger_dbfs"] = None
            runtime["vad_dynamic_release_dbfs"] = None
            runtime["vad_start_sent"] = False
            runtime["api_audio_supported"] = bool(api_audio_supported)
            if not api_audio_supported:
                runtime["udp_port"] = int(udp_port)
            runtime["awaiting_announcement"] = False
            runtime["awaiting_announcement_session_id"] = ""
            runtime["watchdog_task"] = asyncio.create_task(
                _esphome_session_watchdog(token, client, module, session_id)
            )

        await _compat_emit_event(
            token,
            "esphome_session_started",
            {
                "session_id": session_id,
                "conversation_id": _text(conversation_id),
                "wake_word": _text(wake_word_phrase),
                "audio_format": audio_format,
            },
        )
        logger.info(
            "[native-voice] session start selector=%s conversation_id=%s session_id=%s wake_word=%s rate=%s width=%s ch=%s",
            token,
            _text(conversation_id),
            session_id,
            _text(wake_word_phrase),
            int(audio_format.get("rate") or 0),
            int(audio_format.get("width") or 0),
            int(audio_format.get("channels") or 0),
        )
        _native_debug(
            f"esphome session start flags selector={token} flags={int(_flags or 0)} transport={'api_audio' if api_audio_supported else f'udp:{udp_port}'}"
        )
        _native_debug(
            "esphome watchdog config "
            f"selector={token} idle_timeout_s={max(0.8, float(DEFAULT_ESPHOME_AUDIO_IDLE_TIMEOUT_S)):.2f} "
            f"max_listen_s={max(max(0.8, float(DEFAULT_ESPHOME_AUDIO_IDLE_TIMEOUT_S)) + 1.0, float(DEFAULT_ESPHOME_SESSION_MAX_LISTEN_SECONDS)):.2f} "
            f"no_voice_timeout_s={_esphome_no_voice_timeout_s():.2f}"
        )
        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_START", "RUN_START"), None)
        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_STT_START", "STT_START"), None)
        return int(udp_port) if not api_audio_supported else 0

    async def _handle_audio(data: bytes) -> None:
        await _ingest_audio_chunk(bytes(data or b""))

    async def _handle_stop(abort: bool) -> None:
        lock = runtime.get("lock")
        if lock is None or not hasattr(lock, "acquire"):
            runtime["lock"] = asyncio.Lock()
            lock = runtime["lock"]
        async with lock:
            session_id = _text(runtime.get("session_id"))
            chunks = int(runtime.get("audio_chunks") or 0)
            total = int(runtime.get("audio_bytes") or 0)
            _esphome_cancel_watchdog(runtime)
            _esphome_cancel_announcement_wait(runtime)
            runtime["awaiting_announcement"] = False
            runtime["awaiting_announcement_session_id"] = ""
        logger.info(
            "[native-voice] session stop selector=%s session_id=%s abort=%s chunks=%s bytes=%s",
            token,
            session_id,
            bool(abort),
            chunks,
            total,
        )
        with contextlib.suppress(Exception):
            await _esphome_finalize_voice_session(
                token,
                client,
                module,
                abort=bool(abort),
                reason="device_stop" if abort else "",
            )

    async def _handle_announcement_finished(*_args: Any, **_kwargs: Any) -> None:
        completed = await _esphome_finalize_after_announcement(
            token,
            client,
            module,
            reason="announcement_finished",
        )
        if completed:
            logger.info("[native-voice] announcement finished selector=%s", token)

    subscribe_kwargs: Dict[str, Any] = {
        "handle_start": _handle_start,
        "handle_stop": _handle_stop,
    }
    with contextlib.suppress(Exception):
        sig = inspect.signature(subscribe)
        if "handle_audio" in sig.parameters:
            subscribe_kwargs["handle_audio"] = _handle_audio if api_audio_supported else None
        if "handle_announcement_finished" in sig.parameters:
            subscribe_kwargs["handle_announcement_finished"] = _handle_announcement_finished
    if "handle_audio" not in subscribe_kwargs:
        subscribe_kwargs["handle_audio"] = _handle_audio if api_audio_supported else None

    try:
        unsub = subscribe(**subscribe_kwargs)
    except TypeError:
        # Backward-compatible fallback for older/newer aioesphomeapi signatures.
        fallback_kwargs = dict(subscribe_kwargs)
        fallback_kwargs.pop("handle_announcement_finished", None)
        unsub = subscribe(**fallback_kwargs)

    if inspect.isawaitable(unsub):
        unsub = await unsub
    if not callable(unsub):
        raise RuntimeError("ESPHome subscribe_voice_assistant did not return an unsubscribe callback")
    return unsub


def _esphome_target_map() -> Dict[str, str]:
    registry = _load_voice_satellite_registry()
    by_selector: Dict[str, str] = {}
    for row in registry:
        selector = _text(row.get("selector"))
        host = _lower(row.get("host"))
        if not host and selector.startswith("host:"):
            host = _lower(selector.split(":", 1)[1])
        if not selector or not host:
            continue
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if bool(metadata.get("esphome_selected")):
            by_selector[selector] = host

    return by_selector


def _esphome_client_connected(client: Any, fallback: bool = False) -> bool:
    if client is None:
        return False

    marker = getattr(client, "is_connected", None)
    if callable(marker):
        try:
            value = marker()
            if inspect.isawaitable(value):
                return fallback
            return bool(value)
        except Exception:
            return fallback
    if isinstance(marker, bool):
        return marker

    marker2 = getattr(client, "connected", None)
    if isinstance(marker2, bool):
        return marker2
    return fallback


async def _esphome_call_client_method(client: Any, method_name: str, *, timeout: float) -> Tuple[bool, str]:
    method = getattr(client, method_name, None)
    if not callable(method):
        return False, "unavailable"
    try:
        result = method()
    except TypeError:
        return False, "signature_mismatch"
    except Exception as exc:
        return False, f"error:{exc}"
    try:
        if inspect.isawaitable(result):
            await asyncio.wait_for(result, timeout=timeout)
    except Exception as exc:
        return False, f"error:{exc}"
    return True, "ok"


async def _esphome_verify_connection(client: Any, *, timeout: float) -> Tuple[bool, str]:
    if client is None:
        return False, "missing_client"

    marker_before = _esphome_client_connected(client, fallback=False)
    ping_ok, ping_reason = await _esphome_call_client_method(client, "ping", timeout=timeout)
    info_ok, info_reason = await _esphome_call_client_method(client, "device_info", timeout=timeout)
    marker_after = _esphome_client_connected(client, fallback=False)

    if marker_before or marker_after or ping_ok or info_ok:
        details = (
            f"marker_before={marker_before} marker_after={marker_after} "
            f"ping={ping_reason} device_info={info_reason}"
        )
        return True, details
    return False, f"marker_before={marker_before} ping={ping_reason} device_info={info_reason}"


async def _esphome_disconnect_selector(selector: str, *, reason: str) -> None:
    token = _text(selector)
    if not token:
        return

    async with _esphome_native_lock:
        row = _esphome_native_clients.get(token)
        client = row.get("client") if isinstance(row, dict) else None
        unsubscribe = row.get("unsubscribe") if isinstance(row, dict) else None
        was_connected = bool(row.get("connected", False)) if isinstance(row, dict) else False
        if isinstance(row, dict):
            row["connected"] = False
            row["client"] = None
            row["unsubscribe"] = None
            row["last_disconnect_ts"] = _native_now()
            row["last_error"] = _text(reason)

    runtime = _esphome_voice_runtime.get(token) if isinstance(_esphome_voice_runtime, dict) else None
    if isinstance(runtime, dict) and _text(runtime.get("session_id")):
        _esphome_cancel_watchdog(runtime)
        module, _ = _esphome_import()
        if module is not None and client is not None:
            with contextlib.suppress(Exception):
                await _esphome_finalize_voice_session(
                    token,
                    client,
                    module,
                    abort=True,
                    reason=_text(reason) or "disconnect",
                )
        else:
            with contextlib.suppress(Exception):
                await _native_mark_session_aborted(_text(runtime.get("session_id")), _text(reason) or "disconnect")
    if isinstance(runtime, dict):
        _esphome_close_udp_locked(runtime)
    _esphome_voice_runtime.pop(token, None)

    if callable(unsubscribe):
        with contextlib.suppress(Exception):
            unsubscribe()

    disconnect_fn = getattr(client, "disconnect", None)
    if callable(disconnect_fn):
        try:
            result = disconnect_fn()
            if inspect.isawaitable(result):
                await asyncio.wait_for(result, timeout=_esphome_connect_timeout_s())
        except Exception:
            pass

    if not was_connected and client is None:
        return

    await _compat_set_connected(token, connected=False, info={"reason": _text(reason) or "esphome_disconnect"})
    await _compat_emit_event(token, "esphome_disconnected", {"reason": _text(reason)})


async def _esphome_client_stopped(selector: str, *, expected_disconnect: bool) -> None:
    token = _text(selector)
    if not token:
        return
    was_connected = False
    runtime = _esphome_voice_runtime.get(token) if isinstance(_esphome_voice_runtime, dict) else None
    if isinstance(runtime, dict) and _text(runtime.get("session_id")):
        _esphome_cancel_watchdog(runtime)
        with contextlib.suppress(Exception):
            await _native_mark_session_aborted(
                _text(runtime.get("session_id")),
                "expected_disconnect" if expected_disconnect else "connection_lost",
            )
    if isinstance(runtime, dict):
        _esphome_close_udp_locked(runtime)
    _esphome_voice_runtime.pop(token, None)

    async with _esphome_native_lock:
        row = _esphome_native_clients.get(token)
        if isinstance(row, dict):
            was_connected = bool(row.get("connected", False)) or row.get("client") is not None
            row["connected"] = False
            row["client"] = None
            row["unsubscribe"] = None
            row["last_disconnect_ts"] = _native_now()
            if not expected_disconnect:
                row["last_error"] = "connection_lost"

    if expected_disconnect and not was_connected:
        return

    await _compat_set_connected(
        token,
        connected=False,
        info={"reason": "expected_disconnect" if expected_disconnect else "connection_lost"},
    )
    await _compat_emit_event(
        token,
        "esphome_disconnected",
        {"reason": "expected_disconnect" if expected_disconnect else "connection_lost"},
    )


async def _esphome_build_client(module: Any, *, host: str, port: int) -> Any:
    APIClient = getattr(module, "APIClient", None)
    if APIClient is None:
        raise RuntimeError("aioesphomeapi.APIClient is unavailable.")

    password = _esphome_password()
    noise_psk = _esphome_noise_psk()
    if noise_psk:
        try:
            return APIClient(host, port, password, noise_psk=noise_psk)
        except TypeError:
            pass
        try:
            return APIClient(address=host, port=port, password=password, noise_psk=noise_psk)
        except TypeError:
            pass
        try:
            return APIClient(host=host, port=port, password=password, noise_psk=noise_psk)
        except TypeError:
            pass

    try:
        return APIClient(host, port, password)
    except TypeError:
        pass
    try:
        return APIClient(address=host, port=port, password=password)
    except TypeError:
        pass
    return APIClient(host=host, port=port, password=password)


async def _esphome_connect_selector(selector: str, *, host: str, port: Optional[int] = None, source: str = "loop") -> Dict[str, Any]:
    token = _text(selector)
    host_token = _lower(host)
    if not token or not host_token:
        raise RuntimeError("selector and host are required for ESPHome connect")

    if not _selector_allowed(token):
        raise RuntimeError("selector is not allowed by Voice compatibility settings")

    module, import_error = _esphome_import()
    if module is None:
        msg = f"aioesphomeapi unavailable: {import_error or 'unknown error'}"
        await _compat_set_error(token, msg)
        async with _esphome_native_lock:
            row = _esphome_native_clients.get(token) or {}
            row.update(
                {
                    "selector": token,
                    "host": host_token,
                    "port": int(port or _esphome_api_port()),
                    "connected": False,
                    "last_attempt_ts": _native_now(),
                    "last_error": msg,
                    "source": source,
                }
            )
            _esphome_native_clients[token] = row
        raise RuntimeError(msg)

    timeout = _esphome_connect_timeout_s()
    connect_port = int(port or _esphome_api_port())
    now = _native_now()
    _native_debug(
        f"esphome connect attempt selector={token} host={host_token} port={connect_port} source={source}"
    )

    async with _esphome_native_lock:
        row = _esphome_native_clients.get(token) or {}
        row.update(
            {
                "selector": token,
                "host": host_token,
                "port": connect_port,
                "connected": False,
                "last_attempt_ts": now,
                "source": source,
            }
        )
        _esphome_native_clients[token] = row

    try:
        client = await _esphome_build_client(module, host=host_token, port=connect_port)
        connect_fn = getattr(client, "connect", None)
        if not callable(connect_fn):
            raise RuntimeError("aioesphomeapi client has no connect() method")

        kwargs: Dict[str, Any] = {}
        with contextlib.suppress(Exception):
            sig = inspect.signature(connect_fn)
            if "login" in sig.parameters:
                kwargs["login"] = True
            if "on_stop" in sig.parameters:
                async def _on_stop(expected_disconnect: bool) -> None:
                    await _esphome_client_stopped(token, expected_disconnect=bool(expected_disconnect))
                kwargs["on_stop"] = _on_stop

        result = connect_fn(**kwargs) if kwargs else connect_fn()
        if inspect.isawaitable(result):
            await asyncio.wait_for(result, timeout=timeout)
        await asyncio.sleep(0.2)
        verified, verify_reason = await _esphome_verify_connection(client, timeout=max(1.0, timeout))
        _native_debug(
            f"esphome connect verification selector={token} verified={verified} details={verify_reason}"
        )
        if not verified:
            raise RuntimeError(
                "ESPHome API connection could not be verified. "
                f"Details: {verify_reason}"
            )
        device_name = token
        voice_features = {
            "flags": 0,
            "api_audio_bit": 0,
            "speaker_bit": 0,
            "api_audio_known": False,
            "speaker_known": False,
            "api_audio_supported": True,
            "speaker_supported": True,
        }
        entity_rows: List[Dict[str, Any]] = []
        sensor_ids: List[str] = []
        with contextlib.suppress(Exception):
            info = await _esphome_client_call(client, "device_info")
            name_fields = (
                getattr(info, "friendly_name", None),
                getattr(info, "name", None),
            )
            for candidate in name_fields:
                label = _text(candidate)
                if label:
                    device_name = label
                    break
            voice_features = _esphome_voice_feature_snapshot(info, client, module)
        with contextlib.suppress(Exception):
            entity_rows = await _esphome_fetch_entities_snapshot(
                client,
                timeout=max(2.0, timeout),
            )
        if entity_rows:
            seen_sensor = set()
            for row in entity_rows:
                entity_type = _text(row.get("type"))
                if entity_type not in {"sensor", "binary_sensor", "text_sensor"}:
                    continue
                entity_id = _text(row.get("entity_id")) or _text(row.get("object_id"))
                if not entity_id or entity_id in seen_sensor:
                    continue
                seen_sensor.add(entity_id)
                sensor_ids.append(entity_id)

        unsubscribe = await _esphome_subscribe_voice_assistant(
            token,
            client,
            module,
            api_audio_supported=bool(voice_features.get("api_audio_supported")),
        )
        logger.info(
            "[native-voice] esphome voice features selector=%s flags=%s api_audio_known=%s api_audio_supported=%s speaker_supported=%s",
            token,
            int(voice_features.get("flags") or 0),
            bool(voice_features.get("api_audio_known")),
            bool(voice_features.get("api_audio_supported")),
            bool(voice_features.get("speaker_supported")),
        )

        _upsert_voice_satellite(
            {
                "selector": token,
                "host": host_token,
                "name": device_name,
                "source": "esphome_native",
                "metadata": {
                    "esphome_port": connect_port,
                    "voice_feature_flags": int(voice_features.get("flags") or 0),
                    "voice_api_audio_known": bool(voice_features.get("api_audio_known")),
                    "voice_api_audio_supported": bool(voice_features.get("api_audio_supported")),
                    "voice_speaker_supported": bool(voice_features.get("speaker_supported")),
                    "esphome_entity_count": len(entity_rows),
                    "sensor_entity_ids": sensor_ids,
                },
            }
        )
        await _compat_set_connected(
            token,
            connected=True,
            info={
                "transport": "esphome_native",
                "protocol": "esphome_api",
                "host": host_token,
                "metadata": {
                    "port": connect_port,
                    "verify": verify_reason,
                    "voice_feature_flags": int(voice_features.get("flags") or 0),
                    "voice_api_audio_known": bool(voice_features.get("api_audio_known")),
                    "voice_api_audio_supported": bool(voice_features.get("api_audio_supported")),
                    "voice_speaker_supported": bool(voice_features.get("speaker_supported")),
                    "esphome_entity_count": len(entity_rows),
                    "sensor_entity_ids": sensor_ids,
                },
            },
        )
        await _compat_emit_event(token, "esphome_connected", {"host": host_token, "port": connect_port})

        async with _esphome_native_lock:
            row = _esphome_native_clients.get(token) or {}
            row.update(
                {
                    "selector": token,
                    "host": host_token,
                    "port": connect_port,
                    "client": client,
                    "unsubscribe": unsubscribe,
                    "connected": True,
                    "voice_feature_flags": int(voice_features.get("flags") or 0),
                    "voice_api_audio_known": bool(voice_features.get("api_audio_known")),
                    "voice_api_audio_supported": bool(voice_features.get("api_audio_supported")),
                    "voice_speaker_supported": bool(voice_features.get("speaker_supported")),
                    "esphome_entity_count": len(entity_rows),
                    "sensor_entity_ids": sensor_ids,
                    "entities": entity_rows,
                    "last_success_ts": _native_now(),
                    "last_error": "",
                    "source": source,
                }
            )
            _esphome_native_clients[token] = row
            return dict(row)

    except Exception as exc:
        unsubscribe_cb = locals().get("unsubscribe")
        if callable(unsubscribe_cb):
            with contextlib.suppress(Exception):
                unsubscribe_cb()
        client_obj = locals().get("client")
        disconnect_fn = getattr(client_obj, "disconnect", None)
        if callable(disconnect_fn):
            with contextlib.suppress(Exception):
                result = disconnect_fn()
                if inspect.isawaitable(result):
                    await asyncio.wait_for(result, timeout=max(1.0, timeout))
        msg = str(exc)
        lower_msg = _lower(msg)
        display_msg = msg
        _native_debug(f"esphome connect failed selector={token} host={host_token} error={msg}")
        if "requires encryption" in lower_msg:
            display_msg = (
                "ESPHome API requires encryption (noise_psk). "
                "Set VOICE_ESPHOME_NOISE_PSK to the device API encryption key."
            )
        await _compat_set_error(token, display_msg)
        async with _esphome_native_lock:
            row = _esphome_native_clients.get(token) or {}
            row.update(
                {
                    "selector": token,
                    "host": host_token,
                    "port": connect_port,
                    "connected": False,
                    "last_error": display_msg,
                    "source": source,
                }
            )
            _esphome_native_clients[token] = row
        raise RuntimeError(display_msg) from exc


async def _esphome_disconnect_all(reason: str, *, clear: bool = False) -> None:
    async with _esphome_native_lock:
        selectors = list(_esphome_native_clients.keys())
    for selector in selectors:
        await _esphome_disconnect_selector(selector, reason=reason)
    if clear:
        async with _esphome_native_lock:
            _esphome_native_clients.clear()


async def _esphome_reconcile_once(*, force: bool = False) -> Dict[str, Any]:
    now = _native_now()
    _esphome_native_stats["runs"] = int(_esphome_native_stats.get("runs") or 0) + 1
    _esphome_native_stats["last_run_ts"] = now

    if not _esphome_native_enabled():
        await _esphome_disconnect_all("esphome_native_disabled", clear=True)
        return _esphome_native_status()

    targets = _esphome_target_map()
    retry_seconds = _esphome_retry_seconds()

    async with _esphome_native_lock:
        snapshot = {k: dict(v) for k, v in _esphome_native_clients.items()}

    for selector, row in snapshot.items():
        if selector not in targets:
            await _esphome_disconnect_selector(selector, reason="not_targeted")
            continue
        client = row.get("client")
        connected_row = bool(row.get("connected", False))
        if connected_row and not _esphome_client_connected(client, fallback=connected_row):
            await _esphome_disconnect_selector(selector, reason="connection_lost")

    for selector, host in targets.items():
        row = snapshot.get(selector) or {}
        if bool(row.get("connected", False)) and _esphome_client_connected(row.get("client"), fallback=True):
            continue
        last_attempt = float(row.get("last_attempt_ts") or 0.0)
        if (not force) and ((now - last_attempt) < retry_seconds):
            _native_debug(
                f"esphome reconcile backoff selector={selector} wait_left={max(0, int(retry_seconds - (now - last_attempt)))}"
            )
            continue
        try:
            await _esphome_connect_selector(selector, host=host, source="reconcile")
            _esphome_native_stats["last_success_ts"] = _native_now()
            _esphome_native_stats["last_error"] = ""
        except Exception as exc:
            _esphome_native_stats["last_error"] = str(exc)
            _native_debug(f"esphome reconcile connect_failed selector={selector} host={host} error={exc}")

    return _esphome_native_status()


async def _esphome_native_loop() -> None:
    while True:
        try:
            await _esphome_reconcile_once(force=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _esphome_native_stats["last_error"] = str(exc)
            _esphome_native_stats["last_run_ts"] = _native_now()
            logger.warning("[native-voice] esphome loop error: %s", exc)
        await asyncio.sleep(float(max(2, _esphome_retry_seconds())))


def _esphome_native_status() -> Dict[str, Any]:
    module, import_error = _esphome_import()
    targets = _esphome_target_map()
    clients: Dict[str, Any] = {}
    for selector, row in _esphome_native_clients.items():
        if not isinstance(row, dict):
            continue
        runtime = _esphome_voice_runtime.get(selector) if isinstance(_esphome_voice_runtime, dict) else {}
        clients[selector] = {
            "selector": _text(row.get("selector") or selector),
            "host": _text(row.get("host")),
            "port": int(row.get("port") or _esphome_api_port()),
            "connected": bool(row.get("connected", False)),
            "selected": selector in targets,
            "voice_subscribed": bool(row.get("unsubscribe")),
            "active_session_id": _text(runtime.get("session_id")) if isinstance(runtime, dict) else "",
            "voice_feature_flags": int(row.get("voice_feature_flags") or 0),
            "voice_api_audio_known": bool(row.get("voice_api_audio_known")),
            "voice_api_audio_supported": bool(row.get("voice_api_audio_supported")),
            "last_attempt_ts": float(row.get("last_attempt_ts") or 0.0),
            "last_success_ts": float(row.get("last_success_ts") or 0.0),
            "last_disconnect_ts": float(row.get("last_disconnect_ts") or 0.0),
            "last_error": _text(row.get("last_error")),
            "source": _text(row.get("source")),
        }
    return {
        "enabled": _esphome_native_enabled(),
        "available": module is not None,
        "import_error": _text(import_error),
        "task_running": bool(_esphome_native_task and not _esphome_native_task.done()),
        "api_port": _esphome_api_port(),
        "connect_timeout_s": _esphome_connect_timeout_s(),
        "retry_seconds": _esphome_retry_seconds(),
        "auto_target_manual": _esphome_auto_target_manual(),
        "password_set": bool(_esphome_password()),
        "noise_psk_set": bool(_esphome_noise_psk()),
        "targets": targets,
        "clients": clients,
        "stats": dict(_esphome_native_stats),
    }


def _compat_tcp_enabled() -> bool:
    return _get_bool_platform_setting("VOICE_COMPAT_TCP_ENABLED", False)


def _compat_tcp_host() -> str:
    return _text(_portal_settings().get("VOICE_COMPAT_TCP_HOST")) or DEFAULT_COMPAT_TCP_HOST


def _compat_tcp_port() -> int:
    value = _get_int_platform_setting("VOICE_COMPAT_TCP_PORT", DEFAULT_COMPAT_TCP_PORT)
    if value <= 0:
        return DEFAULT_COMPAT_TCP_PORT
    return int(value)


def _compat_tcp_require_token() -> bool:
    return _get_bool_platform_setting("VOICE_COMPAT_TCP_REQUIRE_TOKEN", True)


def _compat_tcp_expected_token() -> str:
    configured = _text(_portal_settings().get("VOICE_COMPAT_TCP_TOKEN"))
    if configured:
        return configured
    return _get_api_auth_key()


async def _compat_tcp_send(writer: asyncio.StreamWriter, payload: Dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n"
    writer.write(body)
    await writer.drain()


async def _compat_tcp_reply(
    writer: asyncio.StreamWriter,
    *,
    action: str,
    request_id: str,
    ok: bool,
    data: Optional[Dict[str, Any]] = None,
    error: str = "",
) -> None:
    payload = {
        "ok": bool(ok),
        "action": _text(action),
        "request_id": _text(request_id),
        "ts": _native_now(),
    }
    if data and isinstance(data, dict):
        payload.update(data)
    if not ok:
        payload["error"] = _text(error) or "request failed"
    await _compat_tcp_send(writer, payload)


async def _compat_tcp_handle_action(action: str, body: Dict[str, Any], client_state: Dict[str, Any]) -> Dict[str, Any]:
    action_name = _lower(action)
    selectors_touched = client_state.get("selectors_touched")
    if not isinstance(selectors_touched, set):
        selectors_touched = set()
        client_state["selectors_touched"] = selectors_touched

    if action_name == "hello":
        return {
            "capabilities": [
                "voice.compat.connect",
                "voice.compat.disconnect",
                "voice.compat.status",
                "voice.compat.events",
                "voice.compat.wake",
                "voice.compat.audio.start",
                "voice.compat.audio.chunk",
                "voice.compat.audio.stop",
                "voice.compat.control",
            ],
            "transport": "compat_jsonl_tcp",
            "auth_required": _compat_tcp_require_token(),
            "runtime_status": _native_runtime_status(),
        }

    if action_name == "auth":
        supplied = _text(body.get("token"))
        expected = _compat_tcp_expected_token()
        if _compat_tcp_require_token():
            if not expected:
                raise HTTPException(status_code=503, detail="Compat TCP token is required but not configured.")
            if supplied != expected:
                raise HTTPException(status_code=401, detail="Invalid compat adapter token.")
        client_state["authed"] = True
        return {"authed": True}

    if _compat_tcp_require_token() and not bool(client_state.get("authed", False)):
        raise HTTPException(status_code=401, detail="Call auth first.")

    if action_name == "connect":
        selector = await _compat_require_selector(body.get("selector"))
        selectors_touched.add(selector)
        now = _native_now()
        _upsert_voice_satellite(
            {
                "selector": selector,
                "satellite_id": _text(body.get("satellite_id")),
                "host": _lower(body.get("host")),
                "name": _text(body.get("name")) or selector,
                "area_name": _text(body.get("area_name")),
                "source": _text(body.get("transport")) or "compat_tcp",
                "metadata": body.get("metadata") if isinstance(body.get("metadata"), dict) else {},
                "last_seen_ts": now,
            }
        )
        status = await _compat_set_connected(
            selector,
            connected=True,
            info={
                "transport": _text(body.get("transport")) or "compat_tcp",
                "protocol": _text(body.get("protocol")),
                "name": _text(body.get("name")),
                "host": _text(body.get("host")),
                "area_name": _text(body.get("area_name")),
                "satellite_id": _text(body.get("satellite_id")),
                "metadata": body.get("metadata") if isinstance(body.get("metadata"), dict) else {},
            },
        )
        return {"selector": selector, "status": status}

    if action_name == "disconnect":
        selector = await _compat_require_selector(body.get("selector"))
        status = await _compat_set_connected(
            selector,
            connected=False,
            info={"reason": _text(body.get("reason")) or "adapter_disconnect"},
        )
        return {"selector": selector, "status": status}

    if action_name == "status":
        selector = await _compat_require_selector(body.get("selector"))
        status = await _compat_get_selector_status(selector)
        return {"selector": selector, "status": status, "runtime_status": _native_runtime_status()}

    if action_name == "events":
        selector = await _compat_require_selector(body.get("selector"))
        since = int(body.get("since") or 0)
        limit = int(body.get("limit") or 100)
        result = await _compat_pull_events(selector, since=since, limit=limit)
        return result

    if action_name == "wake":
        selector = await _compat_require_selector(body.get("selector"))
        selectors_touched.add(selector)
        await _compat_emit_event(
            selector,
            "wake",
            {
                "wake_word": _text(body.get("wake_word")),
                "language": _text(body.get("language")),
            },
        )
        session = await _native_create_session(
            VoiceNativeSessionStartIn(
                satellite_selector=selector,
                device_id=_text(body.get("device_id")),
                area_id=_text(body.get("area_id")),
                user_id=_text(body.get("user_id")),
                language=_text(body.get("language")),
                wake_word=_text(body.get("wake_word")),
                context=body.get("context") if isinstance(body.get("context"), dict) else {},
            )
        )
        return {"selector": selector, "session": session}

    if action_name == "audio_start":
        selector = await _compat_require_selector(body.get("selector"))
        selectors_touched.add(selector)
        session_id = _text(body.get("session_id"))
        if session_id:
            session = await _native_get_session_or_404(session_id)
            session_data = _native_public_session(session)
        else:
            session_data = await _native_create_session(
                VoiceNativeSessionStartIn(
                    satellite_selector=selector,
                    device_id=_text(body.get("device_id")),
                    area_id=_text(body.get("area_id")),
                    user_id=_text(body.get("user_id")),
                    language=_text(body.get("language")),
                    sample_rate_hz=int(body.get("sample_rate_hz")) if body.get("sample_rate_hz") else None,
                    sample_width_bytes=int(body.get("sample_width_bytes")) if body.get("sample_width_bytes") else None,
                    channels=int(body.get("channels")) if body.get("channels") else None,
                    context=body.get("context") if isinstance(body.get("context"), dict) else {},
                )
            )
            session_id = _text(session_data.get("id"))
        await _compat_set_last_session(selector, session_id)
        await _compat_emit_event(
            selector,
            "streaming_started",
            {
                "session_id": session_id,
                "audio_format": session_data.get("audio_format") if isinstance(session_data.get("audio_format"), dict) else {},
            },
        )
        return {"selector": selector, "session": session_data}

    if action_name == "audio_chunk":
        selector = await _compat_require_selector(body.get("selector"))
        selectors_touched.add(selector)
        session_id = _text(body.get("session_id"))
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required for audio_chunk.")
        session = await _native_append_audio_chunk(
            session_id,
            VoiceNativeSessionAudioIn(
                audio_base64=_text(body.get("audio_base64")),
                timestamp_ms=int(body.get("timestamp_ms")) if body.get("timestamp_ms") is not None else None,
                final_chunk=bool(body.get("final_chunk", False)),
            ),
        )
        await _compat_set_last_session(selector, session_id)
        return {"selector": selector, "session": session}

    if action_name == "audio_stop":
        selector = await _compat_require_selector(body.get("selector"))
        selectors_touched.add(selector)
        session_id = _text(body.get("session_id"))
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required for audio_stop.")
        try:
            result = await _native_process_session(
                session_id,
                text_override=_text(body.get("text_override")) or None,
                language_override=_text(body.get("language")) or None,
            )
        except Exception as exc:
            await _compat_set_error(selector, str(exc))
            raise
        await _compat_set_last_session(selector, session_id)
        return {"selector": selector, **result}

    if action_name == "control":
        selector = await _compat_require_selector(body.get("selector"))
        command = _lower(body.get("command"))
        if command not in {"run", "pause", "reset"}:
            raise HTTPException(status_code=400, detail="Unsupported command. Expected run, pause, or reset.")
        event = await _compat_emit_event(
            selector,
            "control",
            {
                "command": command,
                "payload": body.get("payload") if isinstance(body.get("payload"), dict) else {},
            },
        )
        return {"selector": selector, "event": event}

    raise HTTPException(status_code=400, detail=f"Unsupported adapter action: {action_name}")


async def _compat_tcp_client_loop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    peer_text = str(peer)
    client_state: Dict[str, Any] = {
        "authed": not _compat_tcp_require_token(),
        "selectors_touched": set(),
    }
    task = asyncio.current_task()
    if task is not None:
        _compat_tcp_clients.add(task)
    _native_debug(f"compat tcp client connected: {peer_text}")
    try:
        await _compat_tcp_reply(
            writer,
            action="hello",
            request_id="",
            ok=True,
            data={
                "transport": "compat_jsonl_tcp",
                "auth_required": _compat_tcp_require_token(),
                "runtime_status": _native_runtime_status(),
            },
        )
        while not reader.at_eof():
            line = await reader.readline()
            if not line:
                break
            text_line = line.decode("utf-8", errors="ignore").strip()
            if not text_line:
                continue
            request_id = ""
            action = ""
            try:
                body = json.loads(text_line)
                if not isinstance(body, dict):
                    raise ValueError("request must be a JSON object")
                request_id = _text(body.get("request_id"))
                action = _text(body.get("action"))
                if not action:
                    raise ValueError("action is required")
                result = await _compat_tcp_handle_action(action, body, client_state)
                await _compat_tcp_reply(writer, action=action, request_id=request_id, ok=True, data=result)
            except HTTPException as exc:
                msg = _text(exc.detail) or f"HTTP {exc.status_code}"
                await _compat_tcp_reply(writer, action=action, request_id=request_id, ok=False, error=msg)
            except Exception as exc:
                await _compat_tcp_reply(writer, action=action, request_id=request_id, ok=False, error=str(exc))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("[native-voice] compat tcp client error (%s): %s", peer_text, exc)
    finally:
        touched = client_state.get("selectors_touched")
        if isinstance(touched, set):
            for selector in list(touched):
                try:
                    await _compat_set_connected(selector, connected=False, info={"reason": "adapter_client_closed"})
                except Exception:
                    pass
        if task is not None:
            _compat_tcp_clients.discard(task)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        _native_debug(f"compat tcp client disconnected: {peer_text}")


async def _compat_tcp_start_server() -> None:
    global _compat_tcp_server
    if not _compat_tcp_enabled():
        return
    if _compat_tcp_server is not None:
        return
    host = _compat_tcp_host()
    port = _compat_tcp_port()
    try:
        _compat_tcp_server = await asyncio.start_server(_compat_tcp_client_loop, host=host, port=port)
        logger.info("[native-voice] compat tcp adapter listening on %s:%s", host, port)
    except Exception as exc:
        _compat_tcp_server = None
        logger.warning("[native-voice] failed to start compat tcp adapter on %s:%s: %s", host, port, exc)


async def _compat_tcp_stop_server() -> None:
    global _compat_tcp_server
    server = _compat_tcp_server
    _compat_tcp_server = None
    if server is not None:
        try:
            server.close()
            await server.wait_closed()
        except Exception:
            pass
    tasks = list(_compat_tcp_clients)
    for task in tasks:
        task.cancel()
    if tasks:
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
    _compat_tcp_clients.clear()


def _native_voice_debug_enabled() -> bool:
    return _get_bool_platform_setting("VOICE_NATIVE_DEBUG", False)


def _native_debug(message: str) -> None:
    if _native_voice_debug_enabled():
        logger.info("[native-voice] %s", message)


def _native_session_ttl_s() -> int:
    return _get_int_platform_setting("VOICE_NATIVE_SESSION_TTL_S", DEFAULT_VOICE_SESSION_TTL_SECONDS)


def _native_max_audio_bytes() -> int:
    value = _get_int_platform_setting("VOICE_NATIVE_MAX_AUDIO_BYTES", DEFAULT_NATIVE_MAX_AUDIO_BYTES)
    return max(32_000, int(value))


def _native_wyoming_timeout_s() -> float:
    value = _get_float_platform_setting("VOICE_NATIVE_WYOMING_TIMEOUT_S", DEFAULT_NATIVE_WYOMING_TIMEOUT_SECONDS)
    return max(5.0, float(value))


def _native_default_audio_format() -> Dict[str, int]:
    return {
        "rate": _get_int_platform_setting("VOICE_STREAM_SAMPLE_RATE_HZ", DEFAULT_VOICE_SAMPLE_RATE_HZ),
        "width": DEFAULT_VOICE_SAMPLE_WIDTH,
        "channels": DEFAULT_VOICE_CHANNELS,
    }


def _native_now() -> float:
    return float(time.time())


def _native_monotonic() -> float:
    return float(time.monotonic())


def _native_session_set_state(session: Dict[str, Any], state: str) -> None:
    session["state"] = state
    session["updated_ts"] = _native_now()
    history = session.get("state_history")
    if not isinstance(history, list):
        history = []
        session["state_history"] = history
    history.append({"state": state, "ts": session["updated_ts"]})
    if len(history) > 32:
        del history[:-32]


def _native_public_session(session: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "id": _text(session.get("id")),
        "state": _text(session.get("state")) or VOICE_STATE_IDLE,
        "created_ts": float(session.get("created_ts") or 0.0),
        "updated_ts": float(session.get("updated_ts") or 0.0),
        "expires_ts": float(session.get("expires_ts") or 0.0),
        "processing": bool(session.get("processing", False)),
        "satellite_selector": _text(session.get("satellite_selector")),
        "device_id": _text(session.get("device_id")),
        "area_id": _text(session.get("area_id")),
        "user_id": _text(session.get("user_id")),
        "language": _text(session.get("language")),
        "wake_word": _text(session.get("wake_word")),
        "backend_mode": _text(session.get("backend_mode")) or _voice_backend_mode(),
        "audio_bytes": int(session.get("audio_received_bytes") or 0),
        "audio_format": session.get("audio_format") if isinstance(session.get("audio_format"), dict) else {},
        "context": session.get("context") if isinstance(session.get("context"), dict) else {},
        "transcript": _text(session.get("transcript")),
        "response_text": _text(session.get("response_text")),
        "tts_audio_bytes": int(session.get("tts_audio_bytes_len") or 0),
        "tts_audio_format": session.get("tts_audio_format") if isinstance(session.get("tts_audio_format"), dict) else {},
        "error": _text(session.get("error")),
        "state_history": session.get("state_history") if isinstance(session.get("state_history"), list) else [],
    }
    return out


async def _native_gc_sessions_locked(now_ts: Optional[float] = None) -> None:
    now = float(now_ts if now_ts is not None else _native_now())
    stale_ids: List[str] = []
    for session_id, session in _native_voice_sessions.items():
        expires_ts = float(session.get("expires_ts") or 0.0)
        if expires_ts > 0 and expires_ts <= now:
            stale_ids.append(session_id)
    for session_id in stale_ids:
        _native_voice_sessions.pop(session_id, None)


def _native_b64decode_audio(audio_base64: str) -> bytes:
    raw = _text(audio_base64)
    if not raw:
        return b""
    try:
        return base64.b64decode(raw, validate=True)
    except Exception:
        # Allow permissive decoding for clients that omit padding.
        pad = "=" * ((4 - (len(raw) % 4)) % 4)
        return base64.b64decode(raw + pad)


async def _native_create_session(payload: VoiceNativeSessionStartIn) -> Dict[str, Any]:
    now = _native_now()
    ttl = _native_session_ttl_s()
    audio_format = _native_default_audio_format()
    if payload.sample_rate_hz and payload.sample_rate_hz > 0:
        audio_format["rate"] = int(payload.sample_rate_hz)
    if payload.sample_width_bytes and payload.sample_width_bytes > 0:
        audio_format["width"] = int(payload.sample_width_bytes)
    if payload.channels and payload.channels > 0:
        audio_format["channels"] = int(payload.channels)

    session_id = uuid.uuid4().hex
    session: Dict[str, Any] = {
        "id": session_id,
        "created_ts": now,
        "updated_ts": now,
        "expires_ts": now + ttl,
        "state": VOICE_STATE_LISTENING,
        "state_history": [{"state": VOICE_STATE_LISTENING, "ts": now}],
        "backend_mode": _voice_backend_mode(),
        "satellite_selector": _text(payload.satellite_selector),
        "device_id": _text(payload.device_id),
        "area_id": _text(payload.area_id),
        "user_id": _text(payload.user_id),
        "language": _text(payload.language),
        "wake_word": _text(payload.wake_word),
        "audio_format": audio_format,
        "audio_bytes": b"",
        "audio_received_bytes": 0,
        "context": payload.context if isinstance(payload.context, dict) else {},
        "transcript": "",
        "response_text": "",
        "tts_audio_bytes": b"",
        "tts_audio_bytes_len": 0,
        "tts_audio_format": {},
        "processing": False,
        "error": "",
        "final_chunk_seen": False,
    }
    initial_text = _text(payload.initial_text)
    if initial_text:
        session["transcript"] = initial_text
        session["audio_received_bytes"] = 0
        session["final_chunk_seen"] = True

    async with _native_voice_sessions_lock:
        await _native_gc_sessions_locked(now)
        _native_voice_sessions[session_id] = session
    selector = _text(session.get("satellite_selector"))
    if selector:
        await _compat_set_last_session(selector, session_id)
        await _compat_emit_pipeline_state(
            selector,
            session_id=session_id,
            state=VOICE_STATE_LISTENING,
            extra={"wake_word": _text(session.get("wake_word"))},
        )
    return _native_public_session(session)


async def _native_get_session_or_404(session_id: str) -> Dict[str, Any]:
    token = _text(session_id)
    if not token:
        raise HTTPException(status_code=400, detail="session id is required.")
    async with _native_voice_sessions_lock:
        await _native_gc_sessions_locked()
        session = _native_voice_sessions.get(token)
        if not isinstance(session, dict):
            raise HTTPException(status_code=404, detail="Native voice session not found.")
        ttl = _native_session_ttl_s()
        session["expires_ts"] = _native_now() + ttl
        return session


async def _native_append_audio_chunk(session_id: str, chunk: VoiceNativeSessionAudioIn) -> Dict[str, Any]:
    audio_bytes = _native_b64decode_audio(chunk.audio_base64)
    if not audio_bytes and not chunk.final_chunk:
        raise HTTPException(status_code=400, detail="audio chunk is empty.")

    await _native_get_session_or_404(session_id)
    async with _native_voice_sessions_lock:
        existing = _native_voice_sessions.get(_text(session_id))
        if not isinstance(existing, dict):
            raise HTTPException(status_code=404, detail="Native voice session not found.")
        if bool(existing.get("processing", False)):
            raise HTTPException(status_code=409, detail="Session is already being processed.")

        buffered = existing.get("audio_bytes")
        if not isinstance(buffered, (bytes, bytearray)):
            buffered = b""
        new_size = len(buffered) + len(audio_bytes)
        if new_size > _native_max_audio_bytes():
            raise HTTPException(status_code=413, detail="Session audio buffer exceeds configured limit.")

        if audio_bytes:
            existing["audio_bytes"] = bytes(buffered) + audio_bytes
            existing["audio_received_bytes"] = int(existing.get("audio_received_bytes") or 0) + len(audio_bytes)
        if chunk.final_chunk:
            existing["final_chunk_seen"] = True
        _native_session_set_state(existing, VOICE_STATE_LISTENING)
        existing["error"] = ""
        existing["expires_ts"] = _native_now() + _native_session_ttl_s()
        public = _native_public_session(existing)

    selector = _text(public.get("satellite_selector"))
    if selector:
        await _compat_emit_event(
            selector,
            "streaming",
            {
                "session_id": _text(session_id),
                "audio_bytes": int(public.get("audio_bytes") or 0),
                "final_chunk": bool(chunk.final_chunk),
            },
        )
        if chunk.final_chunk:
            await _compat_emit_event(selector, "streaming_stopped", {"session_id": _text(session_id)})
    return public


def _native_wyoming_tts_endpoint() -> Tuple[str, int]:
    cfg = _voice_pipeline_config_snapshot()
    tts = cfg.get("wyoming_tts") if isinstance(cfg.get("wyoming_tts"), dict) else {}
    host = _text(tts.get("host")) or DEFAULT_WYOMING_TTS_HOST
    port = int(tts.get("port") or DEFAULT_WYOMING_TTS_PORT)
    return host, port


async def _native_wyoming_refresh_tts_voices() -> Dict[str, Any]:
    if AsyncTcpClient is None or Describe is None or Info is None:
        raise RuntimeError(f"Wyoming client dependency is unavailable: {WYOMING_IMPORT_ERROR or 'unknown import error'}")

    host, port = _native_wyoming_tts_endpoint()
    timeout = _native_wyoming_timeout_s()
    _native_debug(f"TTS describe {host}:{port}")

    voices: List[Dict[str, str]] = []
    seen_values: set[str] = set()
    info_obj: Any = None
    try:
        async with AsyncTcpClient(host, port) as client:
            await asyncio.wait_for(client.write_event(Describe().event()), timeout=timeout)
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                left = max(0.1, deadline - time.monotonic())
                event = await asyncio.wait_for(client.read_event(), timeout=left)
                if event is None:
                    break
                if WyomingError is not None and WyomingError.is_type(event.type):
                    err = WyomingError.from_event(event)
                    raise RuntimeError(f"Wyoming TTS describe error: {err.text} ({err.code or 'unknown'})")
                if Info.is_type(event.type):
                    info_obj = Info.from_event(event)
                    break
    except Exception as exc:
        existing_rows, _meta = _load_wyoming_tts_voice_catalog()
        _save_wyoming_tts_voice_catalog(existing_rows, host=host, port=port, error=str(exc))
        raise

    if info_obj is None:
        msg = "Wyoming TTS did not return info after describe."
        existing_rows, _meta = _load_wyoming_tts_voice_catalog()
        _save_wyoming_tts_voice_catalog(existing_rows, host=host, port=port, error=msg)
        raise RuntimeError(msg)

    tts_programs = getattr(info_obj, "tts", None)
    if not isinstance(tts_programs, list):
        tts_programs = []
    for program in tts_programs:
        program_name = _text(getattr(program, "name", None))
        voice_rows = getattr(program, "voices", None)
        if not isinstance(voice_rows, list):
            continue
        for voice in voice_rows:
            voice_name = _text(getattr(voice, "name", None))
            languages_obj = getattr(voice, "languages", None)
            languages: List[str] = []
            if isinstance(languages_obj, list):
                languages = [_text(item) for item in languages_obj if _text(item)]
            language_summary = ", ".join(languages[:2])
            speakers = getattr(voice, "speakers", None)
            speaker_rows = speakers if isinstance(speakers, list) else []
            if speaker_rows:
                for speaker_row in speaker_rows:
                    speaker_name = _text(getattr(speaker_row, "name", None))
                    selection: Dict[str, str] = {}
                    if voice_name:
                        selection["name"] = voice_name
                    elif languages:
                        selection["language"] = languages[0]
                    if speaker_name:
                        selection["speaker"] = speaker_name
                    value = _wyoming_tts_voice_selection_value(selection)
                    if not value or value in seen_values:
                        continue
                    seen_values.add(value)
                    label = _wyoming_tts_voice_selection_label(selection)
                    if language_summary:
                        label = f"{label} • {language_summary}"
                    if program_name:
                        label = f"{label} • {program_name}"
                    voices.append({"value": value, "label": label})
                continue

            selection = {}
            if voice_name:
                selection["name"] = voice_name
            elif languages:
                selection["language"] = languages[0]
            value = _wyoming_tts_voice_selection_value(selection)
            if not value or value in seen_values:
                continue
            seen_values.add(value)
            label = _wyoming_tts_voice_selection_label(selection)
            if language_summary:
                label = f"{label} • {language_summary}"
            if program_name:
                label = f"{label} • {program_name}"
            voices.append({"value": value, "label": label})

    voices = sorted(voices, key=lambda row: _lower(row.get("label")))
    _save_wyoming_tts_voice_catalog(voices, host=host, port=port, error="")
    return {"host": host, "port": port, "voices": voices, "count": len(voices)}


async def _native_wyoming_transcribe(
    *,
    audio_bytes: bytes,
    rate: int,
    width: int,
    channels: int,
    language: Optional[str],
) -> str:
    if (
        AsyncTcpClient is None
        or Transcribe is None
        or Transcript is None
        or WyomingAudioStart is None
        or WyomingAudioChunk is None
        or WyomingAudioStop is None
    ):
        raise RuntimeError(f"Wyoming client dependency is unavailable: {WYOMING_IMPORT_ERROR or 'unknown import error'}")
    if not audio_bytes:
        return ""
    cfg = _voice_pipeline_config_snapshot()
    stt = cfg.get("wyoming_stt") if isinstance(cfg.get("wyoming_stt"), dict) else {}
    host = _text(stt.get("host")) or DEFAULT_WYOMING_STT_HOST
    port = int(stt.get("port") or DEFAULT_WYOMING_STT_PORT)
    timeout = _native_wyoming_timeout_s()

    _native_debug(f"STT connect {host}:{port} bytes={len(audio_bytes)} rate={rate} width={width} ch={channels}")

    async with AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(client.write_event(Transcribe(language=_text(language) or None).event()), timeout=timeout)
        await asyncio.wait_for(
            client.write_event(WyomingAudioStart(rate=rate, width=width, channels=channels).event()),
            timeout=timeout,
        )

        max_chunk = 32_000
        offset = 0
        while offset < len(audio_bytes):
            chunk = audio_bytes[offset: offset + max_chunk]
            offset += len(chunk)
            await asyncio.wait_for(
                client.write_event(
                    WyomingAudioChunk(
                        rate=rate,
                        width=width,
                        channels=channels,
                        audio=chunk,
                    ).event()
                ),
                timeout=timeout,
            )
        await asyncio.wait_for(client.write_event(WyomingAudioStop().event()), timeout=timeout)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            left = max(0.1, deadline - time.monotonic())
            event = await asyncio.wait_for(client.read_event(), timeout=left)
            if event is None:
                break
            if Transcript.is_type(event.type):
                transcript = _text(Transcript.from_event(event).text)
                _native_debug(f"STT transcript={transcript!r}")
                return transcript
            if WyomingError.is_type(event.type):
                err = WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming STT error: {err.text} ({err.code or 'unknown'})")

    raise RuntimeError("Wyoming STT did not return a transcript.")


async def _native_wyoming_synthesize(text: str) -> Tuple[bytes, Dict[str, Any]]:
    if (
        AsyncTcpClient is None
        or Synthesize is None
        or WyomingAudioStart is None
        or WyomingAudioChunk is None
        or WyomingAudioStop is None
    ):
        raise RuntimeError(f"Wyoming client dependency is unavailable: {WYOMING_IMPORT_ERROR or 'unknown import error'}")
    prompt = _text(text)
    if not prompt:
        return b"", {}

    cfg = _voice_pipeline_config_snapshot()
    tts = cfg.get("wyoming_tts") if isinstance(cfg.get("wyoming_tts"), dict) else {}
    host = _text(tts.get("host")) or DEFAULT_WYOMING_TTS_HOST
    port = int(tts.get("port") or DEFAULT_WYOMING_TTS_PORT)
    selected_voice = _wyoming_tts_voice_selection(tts.get("voice"))
    selected_voice_label = _wyoming_tts_voice_selection_label(selected_voice) if selected_voice else "default"
    timeout = _native_wyoming_timeout_s()

    _native_debug(f"TTS connect {host}:{port} text_len={len(prompt)} voice={selected_voice_label}")

    audio_out = bytearray()
    audio_format: Dict[str, Any] = {}
    saw_audio_start = False
    saw_audio_stop = False

    synth_event = None
    if selected_voice and SynthesizeVoice is not None:
        voice_obj = SynthesizeVoice(
            name=_text(selected_voice.get("name")) or None,
            language=_text(selected_voice.get("language")) or None,
            speaker=_text(selected_voice.get("speaker")) or None,
        )
        synth_event = Synthesize(text=prompt, voice=voice_obj).event()
    elif selected_voice:
        # Backward compatibility if SynthesizeVoice is unavailable in runtime package.
        with contextlib.suppress(Exception):
            synth_event = Synthesize(text=prompt, voice=selected_voice).event()
    if synth_event is None:
        synth_event = Synthesize(text=prompt).event()

    async with AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(client.write_event(synth_event), timeout=timeout)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            left = max(0.1, deadline - time.monotonic())
            event = await asyncio.wait_for(client.read_event(), timeout=left)
            if event is None:
                break
            if WyomingAudioStart.is_type(event.type):
                start = WyomingAudioStart.from_event(event)
                saw_audio_start = True
                audio_format = {"rate": start.rate, "width": start.width, "channels": start.channels}
                continue
            if WyomingAudioChunk.is_type(event.type):
                chunk = WyomingAudioChunk.from_event(event)
                audio_out.extend(chunk.audio or b"")
                continue
            if WyomingAudioStop.is_type(event.type):
                saw_audio_stop = True
                break
            if WyomingError.is_type(event.type):
                err = WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming TTS error: {err.text} ({err.code or 'unknown'})")

    if not saw_audio_start:
        raise RuntimeError("Wyoming TTS did not emit audio-start.")
    if not saw_audio_stop:
        _native_debug("TTS stream ended without explicit audio-stop")
    return bytes(audio_out), audio_format


def _native_discovery_mdns_timeout_s() -> float:
    value = _get_float_platform_setting("VOICE_DISCOVERY_MDNS_TIMEOUT_S", DEFAULT_VOICE_DISCOVERY_MDNS_TIMEOUT_S)
    return max(0.5, min(20.0, float(value)))


def _native_discover_satellites_mdns_sync(scan_seconds: float) -> List[Dict[str, Any]]:
    try:
        from zeroconf import ServiceBrowser, ServiceStateChange, Zeroconf  # type: ignore
    except Exception:
        return []

    service_types = ("_esphomelib._tcp.local.", "_esphome._tcp.local.")
    timeout_ms = max(200, int(float(scan_seconds) * 1000))
    discovered: Dict[str, Dict[str, Any]] = {}
    lock = threading.Lock()

    def _decode(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            with contextlib.suppress(Exception):
                return value.decode("utf-8", "ignore").strip()
            return ""
        return str(value).strip()

    def _collect_addresses(info: Any) -> List[str]:
        out: List[str] = []
        seen = set()
        parsed = None
        with contextlib.suppress(Exception):
            parsed = info.parsed_addresses()
        if isinstance(parsed, list):
            for addr in parsed:
                token = _lower(addr)
                if not token or token in seen:
                    continue
                seen.add(token)
                out.append(token)
        if out:
            return out

        raw_addresses = getattr(info, "addresses", None)
        if isinstance(raw_addresses, (list, tuple)):
            import socket

            for raw in raw_addresses:
                token = ""
                try:
                    if isinstance(raw, (bytes, bytearray)) and len(raw) == 4:
                        token = _lower(socket.inet_ntoa(raw))
                    elif isinstance(raw, (bytes, bytearray)) and len(raw) == 16:
                        token = _lower(socket.inet_ntop(socket.AF_INET6, raw))
                except Exception:
                    token = ""
                if not token or token in seen:
                    continue
                seen.add(token)
                out.append(token)
        return out

    def _first_connectable_host(addresses: List[str], server_host: str) -> str:
        for token in addresses:
            if token.startswith("127.") or token == "::1":
                continue
            return token
        return server_host

    def _on_service_state(*args: Any, **kwargs: Any) -> None:
        zc = kwargs.get("zeroconf")
        service_type = kwargs.get("service_type")
        name = kwargs.get("name")
        state_change = kwargs.get("state_change")
        if zc is None and len(args) >= 1:
            zc = args[0]
        if service_type is None and len(args) >= 2:
            service_type = args[1]
        if name is None and len(args) >= 3:
            name = args[2]
        if state_change is None and len(args) >= 4:
            state_change = args[3]
        if zc is None or not service_type or not name:
            return
        if state_change not in (ServiceStateChange.Added, ServiceStateChange.Updated):
            return
        info = None
        with contextlib.suppress(Exception):
            info = zc.get_service_info(service_type, name, timeout=timeout_ms)
        if info is None:
            return

        server_host = _lower(_decode(getattr(info, "server", "")).rstrip("."))
        addresses = _collect_addresses(info)
        host = _first_connectable_host(addresses, server_host)
        if not host:
            return

        props_raw = getattr(info, "properties", None)
        props_src = props_raw if isinstance(props_raw, dict) else {}
        props: Dict[str, str] = {}
        for raw_k, raw_v in props_src.items():
            key = _decode(raw_k)
            if not key:
                continue
            props[key] = _decode(raw_v)

        service_name = _decode(name).split(".", 1)[0]
        node_name = _decode(props.get("name")) or _decode(props.get("node_name")) or service_name or host
        selector = f"host:{host}"
        row = {
            "selector": selector,
            "host": host,
            "name": node_name,
            "source": "mdns_esphome",
            "metadata": {
                "mdns_service": _decode(name),
                "mdns_type": _decode(service_type),
                "mdns_server": server_host,
                "mdns_addresses": addresses,
                "mdns_properties": props,
            },
        }

        with lock:
            discovered[selector] = row
        _native_debug(f"discovery mdns candidate selector={selector} name={node_name} host={host}")

    zeroconf = Zeroconf()
    browsers = []
    try:
        for service_type in service_types:
            with contextlib.suppress(Exception):
                browsers.append(ServiceBrowser(zeroconf, service_type, handlers=[_on_service_state]))
        time.sleep(float(max(0.5, scan_seconds)))
    finally:
        for browser in browsers:
            with contextlib.suppress(Exception):
                browser.cancel()
        with contextlib.suppress(Exception):
            zeroconf.close()

    return list(discovered.values())


async def _native_discover_satellites_mdns() -> List[Dict[str, Any]]:
    timeout_s = _native_discovery_mdns_timeout_s()
    try:
        rows = await asyncio.to_thread(_native_discover_satellites_mdns_sync, timeout_s)
        return rows if isinstance(rows, list) else []
    except Exception as exc:
        logger.debug("[native-voice] mDNS discovery failed: %s", exc)
        return []


async def _native_discover_satellites_once(*, force_ha_refresh: bool = False) -> Dict[str, Any]:
    now = _native_now()
    counts = {"mdns_esphome": 0}

    for row in await _native_discover_satellites_mdns():
        selector = _text((row or {}).get("selector"))
        if not selector:
            continue
        _upsert_voice_satellite(row if isinstance(row, dict) else {})
        counts["mdns_esphome"] += 1

    _native_voice_discovery_state["runs"] = int(_native_voice_discovery_state.get("runs") or 0) + 1
    _native_voice_discovery_state["last_run_ts"] = now
    _native_voice_discovery_state["last_success_ts"] = now
    _native_voice_discovery_state["last_error"] = ""
    _native_voice_discovery_state["last_counts"] = counts
    _native_debug(
        "discovery summary "
        f"mdns={counts.get('mdns_esphome', 0)}"
    )
    return counts


async def _native_discovery_loop() -> None:
    while True:
        try:
            if _voice_backend_mode() == VOICE_BACKEND_MODE_NATIVE and _get_bool_platform_setting("VOICE_DISCOVERY_ENABLED", True):
                await _native_discover_satellites_once(force_ha_refresh=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _native_voice_discovery_state["last_error"] = str(exc)
            _native_voice_discovery_state["last_run_ts"] = _native_now()
            logger.warning("[native-voice] discovery loop error: %s", exc)

        wait_seconds = max(5, _get_int_platform_setting("VOICE_DISCOVERY_SCAN_SECONDS", DEFAULT_VOICE_DISCOVERY_SCAN_SECONDS))
        await asyncio.sleep(float(wait_seconds))


async def _native_process_session(
    session_id: str,
    *,
    text_override: Optional[str] = None,
    language_override: Optional[str] = None,
) -> Dict[str, Any]:
    token = _text(session_id)
    if not token:
        raise HTTPException(status_code=400, detail="session id is required.")

    selector = ""
    transcript = ""
    response_text = ""
    public_session: Dict[str, Any] = {}
    async with _native_voice_sessions_lock:
        await _native_gc_sessions_locked()
        session = _native_voice_sessions.get(token)
        if not isinstance(session, dict):
            raise HTTPException(status_code=404, detail="Native voice session not found.")
        if bool(session.get("processing", False)):
            raise HTTPException(status_code=409, detail="Session is already being processed.")
        session["processing"] = True
        session["error"] = ""
        _native_session_set_state(session, VOICE_STATE_THINKING)
        selector = _text(session.get("satellite_selector"))
        audio_blob = session.get("audio_bytes") if isinstance(session.get("audio_bytes"), (bytes, bytearray)) else b""
        audio_bytes = bytes(audio_blob)
        audio_format = session.get("audio_format") if isinstance(session.get("audio_format"), dict) else _native_default_audio_format()
        language = _text(language_override) or _text(session.get("language"))
        context = dict(session.get("context") or {})
        device_id = _text(session.get("device_id"))
        area_id = _text(session.get("area_id"))
        user_id = _text(session.get("user_id"))
        conv_session_id = token
        transcript = _text(text_override) or _text(session.get("transcript"))

    if selector:
        await _compat_emit_pipeline_state(selector, session_id=token, state=VOICE_STATE_THINKING)

    try:
        if not transcript:
            transcript = await _native_wyoming_transcribe(
                audio_bytes=audio_bytes,
                rate=int(audio_format.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ),
                width=int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH),
                channels=int(audio_format.get("channels") or DEFAULT_VOICE_CHANNELS),
                language=language,
            )
        transcript = _text(transcript)
        if not transcript:
            raise NoTranscriptError("No transcript produced from audio.")
        _native_debug(
            f"hydra turn start selector={selector} session_id={token} transcript_len={len(transcript)}"
        )

        response_text, _, merged_ctx = await _run_homeassistant_text_turn(
            text_in=transcript,
            user_id=user_id or None,
            device_id=device_id or None,
            area_id=area_id or None,
            session_id=conv_session_id,
            incoming_context=context,
            allow_followup=False,
        )
        _native_debug(
            f"hydra turn result selector={selector} session_id={token} response_len={len(_text(response_text))}"
        )

        async with _native_voice_sessions_lock:
            session = _native_voice_sessions.get(token)
            if isinstance(session, dict):
                _native_session_set_state(session, VOICE_STATE_SPEAKING)
                session["transcript"] = transcript
                session["response_text"] = response_text
                session["context"] = merged_ctx if isinstance(merged_ctx, dict) else context
                selector = _text(session.get("satellite_selector")) or selector

        if selector:
            await _compat_emit_pipeline_state(selector, session_id=token, state=VOICE_STATE_SPEAKING)

        tts_bytes, tts_format = await _native_wyoming_synthesize(response_text)
        tts_b64 = base64.b64encode(tts_bytes).decode("ascii") if tts_bytes else ""

        async with _native_voice_sessions_lock:
            session = _native_voice_sessions.get(token)
            if not isinstance(session, dict):
                raise HTTPException(status_code=404, detail="Native voice session expired during processing.")
            session["tts_audio_bytes"] = tts_bytes
            session["tts_audio_bytes_len"] = len(tts_bytes)
            session["tts_audio_format"] = tts_format
            session["processing"] = False
            session["error"] = ""
            session["expires_ts"] = _native_now() + _native_session_ttl_s()
            _native_session_set_state(session, VOICE_STATE_IDLE)
            selector = _text(session.get("satellite_selector")) or selector
            public_session = _native_public_session(session)

        if selector:
            await _compat_emit_pipeline_state(selector, session_id=token, state=VOICE_STATE_IDLE)
            await _compat_emit_event(
                selector,
                "session_result",
                {
                    "session_id": token,
                    "transcript": transcript,
                    "response_text": response_text,
                    "tts_audio_bytes": len(tts_bytes),
                    "tts_audio_format": tts_format,
                },
            )

        return {
            "session": public_session,
            "transcript": transcript,
            "response_text": response_text,
            "tts_audio_base64": tts_b64,
            "tts_audio_format": tts_format,
        }
    except Exception as exc:
        err = _text(exc) or exc.__class__.__name__
        async with _native_voice_sessions_lock:
            session = _native_voice_sessions.get(token)
            if isinstance(session, dict):
                session["processing"] = False
                session["error"] = err
                session["expires_ts"] = _native_now() + _native_session_ttl_s()
                _native_session_set_state(session, VOICE_STATE_IDLE)
                selector = _text(session.get("satellite_selector")) or selector
        if selector:
            await _compat_emit_pipeline_state(
                selector,
                session_id=token,
                state=VOICE_STATE_IDLE,
                extra={"error": err},
            )
        raise


def _native_runtime_status() -> Dict[str, Any]:
    mdns_available = False
    try:
        import zeroconf.asyncio  # type: ignore  # noqa: F401
        mdns_available = True
    except Exception:
        mdns_available = False
    esphome_status = _esphome_native_status()

    discovery = dict(_native_voice_discovery_state)
    selectors = _native_compat_bridge.get("selectors")
    selectors_map = selectors if isinstance(selectors, dict) else {}
    compat_connected = 0
    for row in selectors_map.values():
        if isinstance(row, dict) and bool(row.get("connected", False)):
            compat_connected += 1
    sessions_count = 0
    active_count = 0
    now = _native_now()
    for session in _native_voice_sessions.values():
        if not isinstance(session, dict):
            continue
        expires_ts = float(session.get("expires_ts") or 0.0)
        if expires_ts > 0 and expires_ts <= now:
            continue
        sessions_count += 1
        if _text(session.get("state")) not in {"", VOICE_STATE_IDLE}:
            active_count += 1
    return {
        "backend_mode": _voice_backend_mode(),
        "discovery_enabled": False,
        "discovery_task_running": False,
        "discovery": discovery,
        "sessions_total": sessions_count,
        "sessions_active": active_count,
        "session_ttl_s": _native_session_ttl_s(),
        "max_audio_bytes": _native_max_audio_bytes(),
        "wyoming_timeout_s": _native_wyoming_timeout_s(),
        "wyoming_available": WYOMING_IMPORT_ERROR is None,
        "wyoming_error": WYOMING_IMPORT_ERROR or "",
        "mdns_discovery_available": mdns_available,
        "compat_bridge_enabled": _compat_bridge_enabled(),
        "compat_require_adopted": _compat_require_adopted(),
        "compat_selectors_total": len(selectors_map),
        "compat_selectors_connected": compat_connected,
        "compat_event_cursor": int(_native_compat_bridge.get("seq") or 0),
        "compat_tcp_enabled": _compat_tcp_enabled(),
        "compat_tcp_host": _compat_tcp_host(),
        "compat_tcp_port": _compat_tcp_port(),
        "compat_tcp_server_running": bool(_compat_tcp_server is not None and _compat_tcp_server.is_serving()),
        "compat_tcp_clients": len(_compat_tcp_clients),
        "compat_tcp_require_token": _compat_tcp_require_token(),
        "compat_tcp_token_set": bool(_compat_tcp_expected_token()),
        "esphome_native": esphome_status,
        "esphome_native_available": bool(esphome_status.get("available")),
    }

# -------------------- App + LLM client --------------------
app = FastAPI(title="Tater Voice Core", version="2.0")  # stable conv_key for continued chat

_llm = None

@app.on_event("startup")
async def _on_startup():
    global _llm, _native_voice_discovery_task, _esphome_native_task
    _llm = get_llm_client_from_env()
    logger.info(
        "[voice_core] startup version=%s backend=%s discovery_mode=%s esphome_native=%s",
        __version__,
        _voice_backend_mode(),
        "manual_only",
        _esphome_native_enabled(),
    )
    try:
        redis_client.delete(REDIS_VOICE_SATELLITE_BLOCKED_KEY)
        _native_debug("startup cleared legacy blocked satellite cache")
    except Exception:
        pass
    cleared = 0
    with contextlib.suppress(Exception):
        cleared = _clear_legacy_esphome_selected_flags()
    if cleared:
        logger.info("[voice_core] cleared stale esphome_selected flags for %s satellites", cleared)
    # Discovery is manual-only: run when user clicks Discover / Refresh.
    _native_voice_discovery_task = None
    if _esphome_native_task is None or _esphome_native_task.done():
        _esphome_native_task = asyncio.create_task(_esphome_native_loop())
    await _compat_tcp_start_server()
    logger.info("[voice_core] discovery mode=manual_only (no startup scan, no background scan)")


@app.on_event("shutdown")
async def _on_shutdown():
    global _native_voice_discovery_task, _esphome_native_task
    await _compat_tcp_stop_server()
    task = _native_voice_discovery_task
    _native_voice_discovery_task = None
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    esphome_task = _esphome_native_task
    _esphome_native_task = None
    if esphome_task is not None:
        esphome_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await esphome_task
    await _esphome_disconnect_all("portal_shutdown", clear=True)

@app.get("/tater-ha/v1/health")
async def health(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    return {"ok": True, "version": "2.0"}

# -------------------- Native voice config + satellite registry API --------------------
@app.get("/tater-ha/v1/voice/config", response_model=VoiceConfigOut)
async def voice_config(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    return {"config": _voice_pipeline_config_snapshot()}


@app.get("/tater-ha/v1/voice/satellites", response_model=VoiceSatellitesOut)
async def voice_satellites(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    rows = list(_load_voice_satellite_registry())
    rows = sorted(rows, key=lambda row: _lower(row.get("name") or row.get("selector")))
    return {"satellites": rows}


@app.post("/tater-ha/v1/voice/satellites/adopt")
async def voice_satellite_adopt(payload: VoiceSatelliteAdoptIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = _text(payload.selector)
    if not selector:
        if _text(payload.satellite_id):
            selector = f"device:{_text(payload.satellite_id)}"
        elif _text(payload.entity_id):
            selector = f"entity:{_lower(payload.entity_id)}"
        elif _text(payload.host):
            selector = f"host:{_lower(payload.host)}"
    if not selector:
        raise HTTPException(status_code=400, detail="Provide selector, satellite_id, entity_id, or host.")

    host = _lower(payload.host)
    if not host and selector.startswith("host:"):
        host = _lower(selector.split(":", 1)[1])

    saved = _upsert_voice_satellite(
        {
            "selector": selector,
            "satellite_id": _text(payload.satellite_id),
            "entity_id": _lower(payload.entity_id),
            "host": host,
            "name": _text(payload.name),
            "area_name": _text(payload.area_name),
            "source": _text(payload.source) or "manual",
            "metadata": payload.metadata or {},
        }
    )
    return {"ok": True, "satellite": saved}


@app.post("/tater-ha/v1/voice/satellites/remove")
async def voice_satellite_remove(payload: VoiceSatelliteRemoveIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = _text(payload.selector)
    if not selector:
        raise HTTPException(status_code=400, detail="selector is required.")
    removed = _remove_voice_satellite(selector)
    return {"ok": True, "selector": selector, "removed": bool(removed)}


@app.post("/tater-ha/v1/voice/satellites/refresh")
async def voice_satellite_refresh(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    counts = await _native_discover_satellites_once(force_ha_refresh=True)
    with contextlib.suppress(Exception):
        await _esphome_reconcile_once(force=True)
    logger.info("[voice_core] manual satellite refresh counts=%s", counts)
    return {
        "ok": True,
        "refreshed": int(sum(int(v) for v in counts.values())),
        "counts": counts,
        "satellites": _load_voice_satellite_registry(),
        "status": _native_runtime_status(),
    }


@app.get("/tater-ha/v1/voice/wyoming/tts/voices")
async def voice_wyoming_tts_voices(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    voices, meta = _load_wyoming_tts_voice_catalog()
    return {"ok": True, "voices": voices, "meta": meta}


@app.post("/tater-ha/v1/voice/wyoming/tts/voices/refresh")
async def voice_wyoming_tts_voices_refresh(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    result = await _native_wyoming_refresh_tts_voices()
    return {"ok": True, **result}


@app.get("/tater-ha/v1/voice/esphome/tts/{stream_id}.wav")
async def voice_esphome_tts_wav(stream_id: str):
    row = _esphome_fetch_tts_url(stream_id)
    if not isinstance(row, dict):
        raise HTTPException(status_code=404, detail="TTS stream not found or expired.")
    wav_bytes = row.get("wav_bytes")
    data = bytes(wav_bytes) if isinstance(wav_bytes, (bytes, bytearray)) else b""
    if not data:
        raise HTTPException(status_code=404, detail="TTS stream audio is unavailable.")
    _native_debug(
        f"esphome tts url fetch stream_id={_text(stream_id)} session_id={_text(row.get('session_id'))} "
        f"selector={_text(row.get('selector'))} bytes={len(data)}"
    )
    return Response(
        content=data,
        media_type="audio/wav",
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/tater-ha/v1/voice/native/status", response_model=VoiceNativeStatusOut)
async def voice_native_status(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    return {"status": _native_runtime_status()}


@app.post("/tater-ha/v1/voice/native/discovery/run", response_model=VoiceNativeStatusOut)
async def voice_native_discovery_run(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    await _native_discover_satellites_once(force_ha_refresh=True)
    return {"status": _native_runtime_status()}


@app.post("/tater-ha/v1/voice/native/sessions", response_model=VoiceNativeSessionOut)
async def voice_native_session_start(payload: VoiceNativeSessionStartIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    session = await _native_create_session(payload)
    return {"session": session}


@app.get("/tater-ha/v1/voice/native/sessions/{session_id}", response_model=VoiceNativeSessionOut)
async def voice_native_session_get(session_id: str, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    session = await _native_get_session_or_404(session_id)
    return {"session": _native_public_session(session)}


@app.post("/tater-ha/v1/voice/native/sessions/{session_id}/audio", response_model=VoiceNativeSessionOut)
async def voice_native_session_audio(
    session_id: str,
    payload: VoiceNativeSessionAudioIn,
    x_tater_token: Optional[str] = Header(None),
):
    _require_api_auth(x_tater_token)
    session = await _native_append_audio_chunk(session_id, payload)
    return {"session": session}


@app.post("/tater-ha/v1/voice/native/sessions/{session_id}/finalize", response_model=VoiceNativeSessionProcessOut)
async def voice_native_session_finalize(
    session_id: str,
    payload: VoiceNativeSessionFinalizeIn,
    x_tater_token: Optional[str] = Header(None),
):
    _require_api_auth(x_tater_token)
    try:
        result = await _native_process_session(
            session_id,
            text_override=payload.text_override,
            language_override=payload.language,
        )
    except HTTPException:
        raise
    except Exception as exc:
        selector = ""
        async with _native_voice_sessions_lock:
            session = _native_voice_sessions.get(_text(session_id))
            if isinstance(session, dict):
                session["processing"] = False
                session["error"] = str(exc)
                _native_session_set_state(session, VOICE_STATE_ERROR)
                selector = _text(session.get("satellite_selector"))
        if selector:
            await _compat_set_error(selector, str(exc))
            await _compat_emit_pipeline_state(selector, session_id=_text(session_id), state=VOICE_STATE_ERROR)
        raise HTTPException(status_code=500, detail=str(exc))
    return result


# -------------------- Compatibility bridge API (transport adapter surface) --------------------
@app.post("/tater-ha/v1/voice/compat/connect")
async def voice_compat_connect(payload: VoiceCompatConnectIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    now = _native_now()
    _upsert_voice_satellite(
        {
            "selector": selector,
            "satellite_id": _text(payload.satellite_id),
            "host": _lower(payload.host),
            "name": _text(payload.name) or selector,
            "area_name": _text(payload.area_name),
            "source": _text(payload.transport) or "compat_bridge",
            "metadata": payload.metadata or {},
            "last_seen_ts": now,
        }
    )
    status = await _compat_set_connected(
        selector,
        connected=True,
        info={
            "transport": _text(payload.transport),
            "protocol": _text(payload.protocol),
            "name": _text(payload.name),
            "host": _text(payload.host),
            "area_name": _text(payload.area_name),
            "satellite_id": _text(payload.satellite_id),
            "metadata": payload.metadata or {},
        },
    )
    return {
        "ok": True,
        "selector": selector,
        "status": status,
        "voice_config": _voice_pipeline_config_snapshot(),
        "runtime_status": _native_runtime_status(),
    }


@app.post("/tater-ha/v1/voice/compat/disconnect")
async def voice_compat_disconnect(payload: VoiceCompatDisconnectIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    status = await _compat_set_connected(
        selector,
        connected=False,
        info={"reason": _text(payload.reason)},
    )
    return {"ok": True, "selector": selector, "status": status}


@app.get("/tater-ha/v1/voice/compat/status")
async def voice_compat_status(
    selector: str = Query(..., description="Satellite selector (for example device:abc123)"),
    x_tater_token: Optional[str] = Header(None),
):
    _require_api_auth(x_tater_token)
    token = await _compat_require_selector(selector)
    status = await _compat_get_selector_status(token)
    return {"ok": True, "selector": token, "status": status, "runtime_status": _native_runtime_status()}


@app.get("/tater-ha/v1/voice/compat/events", response_model=VoiceCompatEventsOut)
async def voice_compat_events(
    selector: str = Query(..., description="Satellite selector"),
    since: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    x_tater_token: Optional[str] = Header(None),
):
    _require_api_auth(x_tater_token)
    token = await _compat_require_selector(selector)
    result = await _compat_pull_events(token, since=since, limit=limit)
    return result


@app.get("/tater-ha/v1/voice/compat/adapter/info")
async def voice_compat_adapter_info(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    status = _native_runtime_status()
    return {
        "ok": True,
        "transport": "compat_jsonl_tcp",
        "runtime_status": status,
        "tcp_server_running": bool(_compat_tcp_server is not None and _compat_tcp_server.is_serving()),
        "tcp_host": _compat_tcp_host(),
        "tcp_port": _compat_tcp_port(),
        "tcp_require_token": _compat_tcp_require_token(),
        "esphome_native_available": bool(status.get("esphome_native_available")),
    }


@app.get("/tater-ha/v1/voice/esphome/status", response_model=VoiceESPHomeStatusOut)
async def voice_esphome_status(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    return {"status": _esphome_native_status()}


@app.post("/tater-ha/v1/voice/esphome/reconcile", response_model=VoiceESPHomeStatusOut)
async def voice_esphome_reconcile(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    status = await _esphome_reconcile_once(force=True)
    return {"status": status}


@app.post("/tater-ha/v1/voice/esphome/connect", response_model=VoiceESPHomeStatusOut)
async def voice_esphome_connect(payload: VoiceESPHomeConnectIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    host = _lower(payload.host)
    if not host:
        host = _esphome_target_map().get(selector, "")
    if not host and selector.startswith("host:"):
        host = _lower(selector.split(":", 1)[1])
    if not host:
        raise HTTPException(status_code=400, detail="No host resolved for selector. Provide host or add the satellite first.")
    logger.info(
        "[voice_core] manual esphome connect selector=%s host=%s port=%s",
        selector,
        host,
        int(payload.port) if payload.port else _esphome_api_port(),
    )
    await _esphome_connect_selector(
        selector,
        host=host,
        port=int(payload.port) if payload.port else None,
        source="manual_endpoint",
    )
    _set_satellite_esphome_selected(selector, True)
    return {"status": _esphome_native_status()}


@app.post("/tater-ha/v1/voice/esphome/disconnect", response_model=VoiceESPHomeStatusOut)
async def voice_esphome_disconnect(payload: VoiceESPHomeDisconnectIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    _set_satellite_esphome_selected(selector, False)
    await _esphome_disconnect_selector(selector, reason="manual_endpoint")
    return {"status": _esphome_native_status()}


@app.post("/tater-ha/v1/voice/compat/wake")
async def voice_compat_wake(payload: VoiceCompatWakeIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    await _compat_emit_event(
        selector,
        "wake",
        {
            "wake_word": _text(payload.wake_word),
            "language": _text(payload.language),
        },
    )
    session_data = await _native_create_session(
        VoiceNativeSessionStartIn(
            satellite_selector=selector,
            device_id=_text(payload.device_id),
            area_id=_text(payload.area_id),
            user_id=_text(payload.user_id),
            language=_text(payload.language),
            wake_word=_text(payload.wake_word),
            context=payload.context or {},
        )
    )
    return {"ok": True, "selector": selector, "session": session_data}


@app.post("/tater-ha/v1/voice/compat/audio/start")
async def voice_compat_audio_start(payload: VoiceCompatAudioStartIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    session_id = _text(payload.session_id)
    if session_id:
        session = await _native_get_session_or_404(session_id)
        session_data = _native_public_session(session)
    else:
        session_data = await _native_create_session(
            VoiceNativeSessionStartIn(
                satellite_selector=selector,
                device_id=_text(payload.device_id),
                area_id=_text(payload.area_id),
                user_id=_text(payload.user_id),
                language=_text(payload.language),
                sample_rate_hz=payload.sample_rate_hz,
                sample_width_bytes=payload.sample_width_bytes,
                channels=payload.channels,
                context=payload.context or {},
            )
        )
        session_id = _text(session_data.get("id"))
    await _compat_set_last_session(selector, session_id)
    await _compat_emit_event(
        selector,
        "streaming_started",
        {
            "session_id": session_id,
            "audio_format": session_data.get("audio_format") if isinstance(session_data.get("audio_format"), dict) else {},
        },
    )
    return {"ok": True, "selector": selector, "session": session_data}


@app.post("/tater-ha/v1/voice/compat/audio/chunk")
async def voice_compat_audio_chunk(payload: VoiceCompatAudioChunkIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    session = await _native_append_audio_chunk(
        _text(payload.session_id),
        VoiceNativeSessionAudioIn(
            audio_base64=payload.audio_base64,
            timestamp_ms=payload.timestamp_ms,
            final_chunk=payload.final_chunk,
        ),
    )
    await _compat_set_last_session(selector, _text(payload.session_id))
    return {"ok": True, "selector": selector, "session": session}


@app.post("/tater-ha/v1/voice/compat/audio/stop")
async def voice_compat_audio_stop(payload: VoiceCompatAudioStopIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    try:
        result = await _native_process_session(
            _text(payload.session_id),
            text_override=payload.text_override,
            language_override=payload.language,
        )
    except Exception as exc:
        await _compat_set_error(selector, str(exc))
        raise
    await _compat_set_last_session(selector, _text(payload.session_id))
    return {"ok": True, "selector": selector, **result}


@app.post("/tater-ha/v1/voice/compat/control")
async def voice_compat_control(payload: VoiceCompatControlIn, x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    selector = await _compat_require_selector(payload.selector)
    command = _lower(payload.command)
    if command not in {"run", "pause", "reset"}:
        raise HTTPException(status_code=400, detail="Unsupported command. Expected run, pause, or reset.")
    event = await _compat_emit_event(
        selector,
        "control",
        {
            "command": command,
            "payload": payload.payload if isinstance(payload.payload, dict) else {},
        },
    )
    return {"ok": True, "selector": selector, "event": event}

def run(stop_event: Optional[threading.Event] = None):
    """Match your other platforms’ run signature and graceful stop behavior."""
    global _voice_core_runtime_port
    settings = _voice_core_settings()
    raw_port = settings.get("bind_port")
    try:
        port = int(raw_port) if raw_port is not None else DEFAULT_VOICE_CORE_BIND_PORT
    except (TypeError, ValueError):
        logger.warning(
            f"[Voice Core] Invalid bind_port value '{raw_port}', defaulting to {DEFAULT_VOICE_CORE_BIND_PORT}"
        )
        port = DEFAULT_VOICE_CORE_BIND_PORT

    def _port_available(host: str, candidate: int) -> bool:
        with contextlib.suppress(Exception):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((host, int(candidate)))
                return True
        return False

    requested_port = int(port)
    if not _port_available(BIND_HOST, requested_port):
        fallback_port = None
        for candidate in range(DEFAULT_VOICE_CORE_BIND_PORT, DEFAULT_VOICE_CORE_BIND_PORT + 40):
            if candidate == requested_port:
                continue
            if _port_available(BIND_HOST, candidate):
                fallback_port = candidate
                break
        if fallback_port is None:
            logger.error(
                "[Voice Core] Requested port %s is unavailable and no fallback port was found.",
                requested_port,
            )
            return
        logger.warning(
            "[Voice Core] Port %s is already in use; falling back to %s.",
            requested_port,
            fallback_port,
        )
        port = int(fallback_port)
    _voice_core_runtime_port = int(port)

    config = uvicorn.Config(app, host=BIND_HOST, port=port, log_level="info", access_log=False)
    server = uvicorn.Server(config)

    def _serve():
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()

        async def _start():
            try:
                await server.serve()
            except SystemExit as exc:
                code = getattr(exc, "code", 1)
                if code not in (None, 0):
                    logger.error(
                        f"[Voice Core] Server failed to start on {BIND_HOST}:{port} (likely already in use)."
                    )
            except Exception:
                logger.exception(f"[Voice Core] Server failed on {BIND_HOST}:{port}")

        task = loop.create_task(_start())

        def _watch():
            if not stop_event:
                return
            while not stop_event.is_set():
                time.sleep(0.5)
            try:
                server.should_exit = True
            except Exception:
                pass

        if stop_event:
            threading.Thread(target=_watch, daemon=True).start()

        try:
            loop.run_until_complete(task)
        finally:
            if not loop.is_closed():
                loop.stop()
                loop.close()

    logger.info(f"[Voice Core] Listening on http://{BIND_HOST}:{port}")
    _serve()
