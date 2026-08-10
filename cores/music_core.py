"""Tater Tube music library, voice playback, queue, and built-in player for Tater."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import io
import json
import logging
import math
import random
import struct
import threading
import time
import uuid
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import requests

from helpers import extract_json, get_llm_client_from_env, redis_client

try:
    from helpers import get_primary_llm_client_from_env as _get_primary_llm_client_from_env
except Exception:  # pragma: no cover - compatibility with older Tater runtimes.
    _get_primary_llm_client_from_env = get_llm_client_from_env


__version__ = "3.1.2"
MIN_TATER_VERSION = "99.5"
CORE_DESCRIPTION = (
    "Connect Tater Tube Server to Tater; browse music, build AI-named recommendations from listening history, and keep "
    "voice-controlled queues playing with a smart continuous-radio refill across "
    "clock-synchronized satellites, native Sonos groups, stereo pairs, and media players."
)
TAGS = [
    "music",
    "player",
    "tater-tube",
    "satellite",
    "stereo",
    "multi-room",
    "queue",
    "recommendations",
    "album-art",
]

logger = logging.getLogger("music_core")
logger.setLevel(logging.INFO)

CORE_SETTINGS = {
    "category": "Music Core Settings",
    "hydra_tools_require_running": True,
    "required": {
        "catalog_sync_interval_seconds": {
            "label": "Catalog Sync Interval (sec)",
            "type": "number",
            "default": 900,
            "description": "How often Music Core refreshes artists, albums, genres, and tracks.",
        },
        "default_targets": {
            "label": "Default Players",
            "type": "text",
            "default": "",
            "description": "Fallback playback destinations when a room or speaking satellite is unavailable.",
        },
        "default_volume_percent": {
            "label": "Default Volume (%)",
            "type": "number",
            "default": 75,
            "description": "Starting volume for Tater satellite media sessions.",
        },
        "mixed_sync_default_adjustment_ms": {
            "label": "Default Mixed Sync Adjustment (ms)",
            "type": "number",
            "default": 0,
            "description": (
                "Fine-tunes mixed Sonos and Tater satellite groups. Positive values delay satellites; "
                "negative values start them earlier."
            ),
        },
        "default_shuffle": {
            "label": "Shuffle Broad Requests",
            "type": "checkbox",
            "default": True,
            "description": "Shuffle genre, artist, and general music requests by default.",
        },
        "maximum_queue_tracks": {
            "label": "Maximum Queue Tracks",
            "type": "number",
            "default": 200,
            "description": "Maximum number of matched tracks placed in one queue.",
        },
        "recommendations_enabled": {
            "label": "Tater Recommendations",
            "type": "checkbox",
            "default": True,
            "description": "Use listening history and Tater's primary AI model to prepare named music mixes.",
        },
        "recommendation_interval_hours": {
            "label": "Recommendation Refresh (hours)",
            "type": "number",
            "default": 12,
            "description": "How often Music Core refreshes Tater Recommendations in the background.",
        },
        "recommendation_playlist_count": {
            "label": "Recommendation Playlists",
            "type": "number",
            "default": 3,
            "description": "Number of AI-named recommendation playlists to prepare.",
        },
        "recommendation_items_per_playlist": {
            "label": "Albums & Songs Per Playlist",
            "type": "number",
            "default": 6,
            "description": "Maximum recommended albums and songs in each playlist.",
        },
        "prompt_context_enabled": {
            "label": "Music Prompt Context",
            "type": "checkbox",
            "default": True,
            "description": "Share the selected Person's compact music profile with Tater when that Person is speaking.",
        },
        "prompt_profile_interval_hours": {
            "label": "Music Profile Refresh (hours)",
            "type": "number",
            "default": 12,
            "description": "How often Music Core refreshes the selected Person's prompt-ready listening profile.",
        },
    },
    "tags": TAGS,
}

CORE_WEBUI_TAB = {
    "label": "Music",
    "order": 36,
    "requires_running": True,
}

SETTINGS_KEY = "music_core_settings"
RUNTIME_KEY = "music_core_runtime"
CATALOG_KEY = "music_core_catalog_v1"
PLAYER_KEY = "music_core_player_v1"
HISTORY_KEY = "music_core_listening_history_v1"
RECOMMENDATIONS_KEY = "music_core_recommendations_v1"
PROMPT_PROFILE_KEY = "music_core_prompt_profile_v1"
TATER_TUBE_ACTIVITY_KEY = "tater_tube_activity_feed_v1"
MAX_TATER_TUBE_ACTIVITY_EVENTS = 200
REQUEST_TIMEOUT_SECONDS = 30
ARTWORK_CONNECT_TIMEOUT_SECONDS = 2.0
ARTWORK_READ_TIMEOUT_SECONDS = 5.0
ARTWORK_INFLIGHT_WAIT_TIMEOUT_SECONDS = 6.0
ARTWORK_FAILURE_CACHE_SECONDS = 15.0
ARTWORK_MAX_CONCURRENT_FETCHES = 4
DEFAULT_SYNC_INTERVAL_SECONDS = 900
MAX_CATALOG_TRACKS = 20000
MAX_SEARCH_RESULTS = 100
MAX_HISTORY_EVENTS = 300
MAX_RECOMMENDATION_CANDIDATES = 200
MAX_PROMPT_CONTEXT_CHARS = 1400
CATALOG_ARTWORK_SCHEMA = 4
CATALOG_MEMORY_CACHE_TTL_SECONDS = 15.0
CONTINUATION_TRIGGER_REMAINING_TRACKS = 2
CONTINUATION_BATCH_TRACKS = 12
MAX_CONTINUATION_CANDIDATES = 200
PROVIDER_LABELS = {"tater_tube": "Tater Tube Server"}
CATALOG_PROVIDER_IDS = {"tater_tube"}
GENERIC_SEARCH_WORDS = {
    "a",
    "an",
    "and",
    "for",
    "from",
    "me",
    "music",
    "of",
    "on",
    "play",
    "please",
    "some",
    "song",
    "songs",
    "the",
}

# Broad browse/search families. Specific source tags remain visible, while a
# track such as "Roots Reggae" is also discoverable through "Reggae".
GENRE_FAMILIES = (
    ("Alternative", ("alternative", "indie")),
    ("Blues", ("blues",)),
    ("Children's", ("children s music", "kids music", "nursery rhyme")),
    ("Classical", ("classical", "baroque", "opera", "orchestral", "chamber music")),
    ("Country", ("country", "bluegrass", "americana", "honky tonk")),
    ("Dance", ("dance", "disco")),
    (
        "Electronic",
        (
            "electronic",
            "electronica",
            "edm",
            "house",
            "techno",
            "trance",
            "ambient",
            "dubstep",
            "drum and bass",
            "dnb",
        ),
    ),
    ("Folk", ("folk", "singer songwriter")),
    ("Gospel", ("gospel", "worship", "christian music")),
    ("Hip-Hop/Rap", ("hip hop", "hiphop", "rap", "trap", "boom bap")),
    ("Holiday", ("holiday", "christmas music")),
    ("Jazz", ("jazz", "bebop", "swing")),
    ("Latin", ("latin", "salsa", "reggaeton", "bachata", "merengue", "bossa nova")),
    ("Metal", ("metal",)),
    ("New Age", ("new age",)),
    ("Pop", ("pop", "kpop")),
    ("Punk", ("punk",)),
    ("R&B/Soul", ("r b", "rnb", "rhythm and blues", "soul", "funk", "motown")),
    ("Reggae", ("reggae", "dub", "dancehall", "ska", "rocksteady")),
    ("Rock", ("rock", "grunge")),
    ("Soundtrack", ("soundtrack", "film score", "video game music")),
    ("Spoken Word", ("spoken word", "audiobook")),
    ("World", ("world music", "afrobeat", "highlife")),
)
GENRE_CANONICAL_NAMES = {
    "alternative": "Alternative",
    "blues": "Blues",
    "children s": "Children's",
    "children s music": "Children's",
    "kids music": "Children's",
    "classical": "Classical",
    "country": "Country",
    "dance": "Dance",
    "electronic": "Electronic",
    "folk": "Folk",
    "gospel": "Gospel",
    "hip hop": "Hip-Hop/Rap",
    "hiphop": "Hip-Hop/Rap",
    "hip hop rap": "Hip-Hop/Rap",
    "holiday": "Holiday",
    "jazz": "Jazz",
    "latin": "Latin",
    "metal": "Metal",
    "new age": "New Age",
    "pop": "Pop",
    "punk": "Punk",
    "r b": "R&B/Soul",
    "r b soul": "R&B/Soul",
    "rnb": "R&B/Soul",
    "rhythm and blues": "R&B/Soul",
    "reggae": "Reggae",
    "rock": "Rock",
    "soundtrack": "Soundtrack",
    "spoken word": "Spoken Word",
    "world": "World",
    "world music": "World",
}

_state_lock = threading.RLock()
_artwork_cache_lock = threading.RLock()
_artwork_cache: Dict[str, Dict[str, Any]] = {}
_artwork_inflight: Dict[str, threading.Event] = {}
_artwork_failure_until: Dict[str, float] = {}
_artwork_fetch_slots = threading.BoundedSemaphore(ARTWORK_MAX_CONCURRENT_FETCHES)
_catalog_memory_cache_lock = threading.RLock()
_catalog_memory_cache: Dict[str, Any] = {
    "store": None,
    "loaded_at": 0.0,
    "payload": {},
}
_catalog_sync_lock = threading.Lock()
_catalog_sync_started_at = 0.0
_recommendation_lock = threading.Lock()
_recommendation_started_at = 0.0
_recommendation_thread: Optional[threading.Thread] = None
_profile_lock = threading.Lock()
_profile_started_at = 0.0
_profile_thread: Optional[threading.Thread] = None
_continuation_lock = threading.Lock()
_continuation_started_at = 0.0
_continuation_thread: Optional[threading.Thread] = None
_client_continuation_lock = threading.Lock()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace").strip()
    return str(value).strip()


def _assistant_first_name(client: Any = None) -> str:
    store = client or globals().get("redis_client")
    try:
        value = _text(store.get("tater:first_name")) if store is not None else ""
    except Exception:
        value = ""
    first_name = value.split()[0] if value else ""
    return first_name[:48] or "Tater"


def _assistant_possessive(client: Any = None) -> str:
    name = _assistant_first_name(client)
    return f"{name}'" if name.casefold().endswith("s") else f"{name}'s"


def _recommendations_label(client: Any = None) -> str:
    return f"{_assistant_possessive(client)} Recommendations"


def _list(value: Any) -> List[str]:
    raw: List[Any]
    if isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        token = _text(value)
        parsed: Any = None
        if token.startswith("[") and token.endswith("]"):
            try:
                parsed = json.loads(token)
            except Exception:
                parsed = None
        if isinstance(parsed, list):
            raw = parsed
        else:
            raw = token.replace("\n", ",").split(",") if token else []
    result: List[str] = []
    seen = set()
    for item in raw:
        token = _text(item)
        key = token.casefold()
        if not token or key in seen:
            continue
        seen.add(key)
        result.append(token)
    return result


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


_PEOPLE_API_MODULE: Any = None
_PEOPLE_API_UNAVAILABLE = False


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
            spec = importlib.util.spec_from_file_location("tater_people_api", candidate)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                _PEOPLE_API_MODULE = module
                return _PEOPLE_API_MODULE
    except Exception:
        pass
    _PEOPLE_API_UNAVAILABLE = True
    return None


def _people_person_rows(client: Any = None) -> List[Dict[str, Any]]:
    module = _people_api_module()
    load_store = getattr(module, "load_store", None) if module is not None else None
    if not callable(load_store):
        return []
    try:
        store = load_store(client or globals().get("redis_client"))
    except Exception:
        return []
    people = list(store.get("people") or []) if isinstance(store, dict) else []
    rows = [dict(row) for row in people if isinstance(row, dict)]
    rows.sort(key=lambda row: (_text(row.get("display_name")).casefold(), _text(row.get("id"))))
    return rows


def _people_person_name(person_id: Any, client: Any = None) -> str:
    wanted = _text(person_id)
    if not wanted:
        return ""
    for person in _people_person_rows(client):
        if _text(person.get("id")) == wanted:
            return _text(person.get("display_name")) or wanted
    return ""


def _people_person_options(client: Any = None) -> List[Dict[str, str]]:
    options = [{"value": "", "label": "Choose a person"}]
    for person in _people_person_rows(client):
        person_id = _text(person.get("id"))
        display_name = _text(person.get("display_name"))
        if person_id and display_name:
            options.append({"value": person_id, "label": display_name})
    return options


def _context_person_id(*sources: Any) -> str:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for candidate in (source, source.get("people_resolution")):
            if not isinstance(candidate, dict):
                continue
            person_id = _text(candidate.get("master_user_id") or candidate.get("person_id"))
            if person_id:
                return person_id
    return ""


def _provider_id(value: Any, default: str = "tater_tube") -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"tater_tube", "tatertube", "tater_tube_server"}:
        return "tater_tube"
    return "tater_tube" if _text(default) == "tater_tube" else _text(default)


def _decode_hash(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    return {_text(key): _text(value) for key, value in raw.items() if _text(key)}


def _settings(client: Any = None) -> Dict[str, str]:
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    try:
        settings = _decode_hash(store.hgetall(SETTINGS_KEY) or {})
        settings.pop("provider", None)
        return settings
    except Exception:
        return {}


def _target_group_signature(targets: Any) -> str:
    values = sorted({_text(value) for value in _list(targets) if _text(value)})
    if not values:
        return ""
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()[:20]


def _mixed_sync_calibrations(cfg: Dict[str, Any]) -> Dict[str, int]:
    raw = cfg.get("mixed_sync_calibrations")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        return {}
    return {
        _text(key): _as_int(value, 0, -750, 3000)
        for key, value in raw.items()
        if _text(key)
    }


def _player_transport_mode(value: Any) -> str:
    mode = _text(value).casefold()
    return mode if mode in {"auto", "native", "airplay"} else "auto"


def _player_calibrations(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return per-destination volume and timeline calibration settings."""
    raw = cfg.get("player_calibrations")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        return {}
    calibrations: Dict[str, Dict[str, Any]] = {}
    for raw_target, raw_values in raw.items():
        target = _text(raw_target)
        if not target or not isinstance(raw_values, dict):
            continue
        calibration: Dict[str, Any] = {
            "volume_percent": _as_int(raw_values.get("volume_percent"), 75, 0, 100),
            "sync_offset_ms": _as_int(raw_values.get("sync_offset_ms"), 0, -1000, 1000),
        }
        if target.casefold().startswith(("sonos:", "integration:sonos:")):
            calibration["transport_mode"] = _player_transport_mode(
                raw_values.get("transport_mode")
            )
        calibrations[target] = calibration
    return calibrations


def _target_calibration(
    target: Any,
    cfg: Dict[str, Any],
    *,
    default_volume: int = 75,
) -> Dict[str, Any]:
    target_id = _text(target)
    saved = _player_calibrations(cfg).get(target_id, {})
    calibration: Dict[str, Any] = {
        "volume_percent": _as_int(saved.get("volume_percent"), default_volume, 0, 100),
        "sync_offset_ms": _as_int(saved.get("sync_offset_ms"), 0, -1000, 1000),
    }
    if target_id.casefold().startswith(("sonos:", "integration:sonos:")):
        calibration["transport_mode"] = _player_transport_mode(saved.get("transport_mode"))
    return calibration


def _normalize_player_settings(
    raw: Any,
    *,
    targets: Any = None,
    cfg: Optional[Dict[str, Any]] = None,
    default_volume: int = 75,
) -> Dict[str, Dict[str, Any]]:
    source = raw
    if isinstance(source, str):
        try:
            source = json.loads(source)
        except Exception:
            source = {}
    source = source if isinstance(source, dict) else {}
    target_ids = _list(targets) if targets is not None else _list(list(source.keys()))
    settings: Dict[str, Dict[str, Any]] = {}
    current_cfg = cfg if isinstance(cfg, dict) else {}
    for target in target_ids:
        fallback = _target_calibration(target, current_cfg, default_volume=default_volume)
        values = source.get(target) if isinstance(source.get(target), dict) else {}
        setting: Dict[str, Any] = {
            "volume_percent": _as_int(
                values.get("volume_percent"),
                fallback["volume_percent"],
                0,
                100,
            ),
            "sync_offset_ms": _as_int(
                values.get("sync_offset_ms"),
                fallback["sync_offset_ms"],
                -1000,
                1000,
            ),
        }
        if target.casefold().startswith(("sonos:", "integration:sonos:")):
            setting["transport_mode"] = _player_transport_mode(
                values.get("transport_mode", fallback.get("transport_mode"))
            )
        settings[target] = setting
    return settings


def _save_player_calibrations(client: Any, raw: Any) -> Dict[str, Dict[str, Any]]:
    cfg = _settings(client)
    calibrations = _player_calibrations(cfg)
    updates = _normalize_player_settings(raw, cfg=cfg)
    calibrations.update(updates)
    _save_hash(
        client,
        SETTINGS_KEY,
        {"player_calibrations": json.dumps(calibrations, sort_keys=True)},
    )
    return calibrations


def _selected_player_settings(
    targets: Any,
    cfg: Dict[str, Any],
    *,
    default_volume: int = 75,
) -> Dict[str, Dict[str, Any]]:
    return {
        target: _target_calibration(target, cfg, default_volume=default_volume)
        for target in _list(targets)
    }


def _is_native_target(value: Any) -> bool:
    target = _text(value).casefold()
    return target.startswith(("voice_core:native:", "voice_core:stereo:", "native:", "stereo:"))


def _uses_audio_sync_transcode(targets: Any) -> bool:
    """Use one normalized PCM source for every Music Core playback target."""
    return bool(_list(targets))


def _mixed_sync_from_player_settings(
    targets: Any,
    settings: Dict[str, Dict[str, Any]],
    fallback: int,
) -> int:
    native_offsets = [
        _as_int(settings.get(target, {}).get("sync_offset_ms"), 0, -1000, 1000)
        for target in _list(targets)
        if _is_native_target(target)
    ]
    external_offsets = [
        _as_int(settings.get(target, {}).get("sync_offset_ms"), 0, -1000, 1000)
        for target in _list(targets)
        if not _is_native_target(target)
    ]
    if not native_offsets or not external_offsets:
        return _as_int(fallback, 0, -750, 3000)
    native_average = round(sum(native_offsets) / len(native_offsets))
    external_average = round(sum(external_offsets) / len(external_offsets))
    return _as_int(
        _as_int(fallback, 0, -750, 3000) + native_average - external_average,
        fallback,
        -750,
        3000,
    )


def _sync_test_wav(*, duration_seconds: float = 6.0) -> bytes:
    sample_rate = 16000
    frame_count = int(sample_rate * max(2.0, min(10.0, duration_seconds)))
    click_frames = int(sample_rate * 0.025)
    interval_frames = int(sample_rate * 0.5)
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        frames = bytearray()
        for frame_index in range(frame_count):
            click_index = frame_index % interval_frames
            if click_index < click_frames:
                envelope = math.exp(-8.0 * click_index / max(1, click_frames))
                sample = int(26000 * envelope * math.sin(2.0 * math.pi * 1400.0 * click_index / sample_rate))
            else:
                sample = 0
            frames.extend(struct.pack("<h", sample))
        wav_file.writeframes(bytes(frames))
    return output.getvalue()


def _mixed_sync_adjustment(targets: Any, cfg: Dict[str, Any]) -> int:
    default = _as_int(cfg.get("mixed_sync_default_adjustment_ms"), 0, -750, 3000)
    signature = _target_group_signature(targets)
    return _mixed_sync_calibrations(cfg).get(signature, default) if signature else default


def _save_mixed_sync_adjustment(client: Any, targets: Any, value: Any) -> int:
    cfg = _settings(client)
    adjustment = _as_int(value, _mixed_sync_adjustment(targets, cfg), -750, 3000)
    signature = _target_group_signature(targets)
    if not signature:
        return adjustment
    calibrations = _mixed_sync_calibrations(cfg)
    calibrations[signature] = adjustment
    _save_hash(client, SETTINGS_KEY, {"mixed_sync_calibrations": json.dumps(calibrations, sort_keys=True)})
    return adjustment


def _runtime(client: Any = None) -> Dict[str, str]:
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    try:
        return _decode_hash(store.hgetall(RUNTIME_KEY) or {})
    except Exception:
        return {}


def _save_hash(client: Any, key: str, values: Dict[str, Any]) -> None:
    if client is None:
        return
    cleaned = {str(key): str(value) for key, value in values.items() if value is not None}
    if cleaned:
        client.hset(key, mapping=cleaned)


def _load_json(client: Any, key: str, default: Any) -> Any:
    if client is None:
        return default
    try:
        raw = client.get(key)
        if raw in (None, ""):
            return default
        return json.loads(_text(raw))
    except Exception:
        return default


def _save_json(client: Any, key: str, value: Any) -> None:
    if client is not None:
        client.set(key, json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _normalize_server_url(value: Any) -> str:
    raw = _text(value).rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Server URL must begin with http:// or https://")
    if raw.endswith("/api"):
        raw = raw[:-4]
    return raw.rstrip("/")


def _api_url(server_url: str, path: str) -> str:
    return f"{server_url.rstrip('/')}/api/{path.lstrip('/')}"


def _sanitize_tater_artwork_reference(value: Any) -> tuple[str, str]:
    """Keep only Tater's artwork route and remove the paired-player secret."""
    raw = _text(value)
    if not raw:
        return "", ""
    parsed = urlparse(raw)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return "", ""
    if not parsed.scheme and parsed.netloc:
        return "", ""
    if parsed.path != "/api/tater/music/artwork":
        return "", ""
    query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if key.casefold() != "player_token"
    ]
    version = next((item for key, item in query if key.casefold() == "v"), "")
    clean_query = urlencode(query)
    reference = parsed.path
    if clean_query:
        reference = f"{reference}?{clean_query}"
    return reference, version


def _normalize_cached_tater_artwork(track: Dict[str, Any]) -> None:
    reference, version = _sanitize_tater_artwork_reference(track.get("artwork_path"))
    track["provider"] = "tater_tube"
    track["artwork_path"] = reference
    track["artwork_item_id"] = ""
    if reference:
        track["has_artwork"] = True
        if version and not _text(track.get("artwork_version")):
            track["artwork_version"] = version
    else:
        track["has_artwork"] = False
        track["artwork_version"] = ""


def _unwrap_response(response: Any) -> Any:
    try:
        body = response.json()
    except Exception:
        body = {}
    if not bool(getattr(response, "ok", False)):
        error = body.get("error") if isinstance(body, dict) else {}
        message = error.get("message") if isinstance(error, dict) else ""
        detail = _text(message) or f"Music provider returned HTTP {getattr(response, 'status_code', 0)}."
        if int(getattr(response, "status_code", 0) or 0) == 401:
            raise PermissionError(detail)
        raise RuntimeError(detail)
    if isinstance(body, dict) and body.get("success") is False:
        error = body.get("error")
        message = error.get("message") if isinstance(error, dict) else error
        raise RuntimeError(_text(message) or "The music provider rejected the request.")
    if isinstance(body, dict) and "data" in body:
        return body.get("data")
    return body


@dataclass
class TaterTubeMusicProvider:
    server_url: str
    token: str
    provider_id = "tater_tube"

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "TaterTubeMusicProvider":
        return cls(
            server_url=_normalize_server_url(
                settings.get("tater_tube_server_url") or settings.get("server_url")
            ),
            token=_text(settings.get("tater_tube_token") or settings.get("token")),
        )

    @property
    def connected(self) -> bool:
        return bool(self.server_url and self.token)

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
        authenticated: bool = True,
    ) -> Any:
        if not self.server_url:
            raise ValueError("Tater Tube Server URL is not configured.")
        headers = {"Accept": "application/json"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if authenticated and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        response = requests.request(
            method.upper(),
            _api_url(self.server_url, path),
            headers=headers,
            json=payload,
            timeout=max(5, int(timeout)),
        )
        return _unwrap_response(response)

    @classmethod
    def pair(cls, server_url: str, pin: str, name: str) -> Dict[str, Any]:
        provider = cls(server_url=_normalize_server_url(server_url), token="")
        return provider.request(
            "POST",
            "tater/players/pair",
            payload={"pin": pin, "name": name},
            authenticated=False,
        )

    def stream_url(self, track: Dict[str, Any], *, audio_sync: bool = False) -> str:
        category_id = _text(track.get("category_id"))
        if category_id.startswith("local:"):
            category_id = category_id[len("local:") :]
        path = _text(track.get("path"))
        if not category_id or not path:
            return _text(track.get("stream_url"))
        values = {
            "category_id": category_id,
            "source": _as_int(track.get("source_index"), 0, 0, 10000),
            "path": path,
            "player_token": self.token,
        }
        if audio_sync:
            values.update({"transcode": "1", "profile": "audio_sync"})
        query = urlencode(values)
        return f"{self.server_url}/api/tater/local/stream?{query}"

    def artwork_url(self, track: Dict[str, Any]) -> str:
        reference, _version = _sanitize_tater_artwork_reference(track.get("artwork_path"))
        if not reference:
            return ""
        parsed = urlparse(reference)
        query = [
            (key, item)
            for key, item in parse_qsl(parsed.query, keep_blank_values=True)
            if key.casefold() != "player_token"
        ]
        if self.token:
            query.append(("player_token", self.token))
        server = urlparse(self.server_url)
        return urlunparse(
            (
                server.scheme,
                server.netloc,
                parsed.path,
                "",
                urlencode(query),
                "",
            )
        )

    def catalog(self) -> Dict[str, Any]:
        try:
            data = self.request(
                "GET",
                f"tater/music/catalog?limit={MAX_CATALOG_TRACKS}",
                timeout=180,
            )
            if isinstance(data, dict) and isinstance(data.get("tracks"), list):
                return data
        except RuntimeError as exc:
            if "HTTP 404" not in _text(exc):
                raise
        return self._legacy_catalog()

    def _legacy_catalog(self) -> Dict[str, Any]:
        libraries_payload = self.request("GET", "tater/music/libraries", timeout=60)
        libraries = libraries_payload.get("libraries") if isinstance(libraries_payload, dict) else []
        tracks: List[Dict[str, Any]] = []
        library_names: Dict[str, str] = {}
        for library in libraries if isinstance(libraries, list) else []:
            if not isinstance(library, dict):
                continue
            library_id = _text(library.get("ratingKey") or library.get("key"))
            if not library_id:
                continue
            library_names[library_id] = _text(library.get("title"))
            albums_payload = self.request(
                "GET",
                f"tater/music/albums?category_id={quote(library_id, safe='')}",
                timeout=180,
            )
            albums = albums_payload.get("albums") if isinstance(albums_payload, dict) else []
            for album in albums if isinstance(albums, list) else []:
                if not isinstance(album, dict):
                    continue
                album_id = _text(album.get("ratingKey") or album.get("key"))
                if not album_id:
                    continue
                track_payload = self.request(
                    "GET",
                    f"tater/music/tracks?album_id={quote(album_id, safe='')}",
                    timeout=180,
                )
                rows = track_payload.get("tracks") if isinstance(track_payload, dict) else []
                for row in rows if isinstance(rows, list) else []:
                    if not isinstance(row, dict):
                        continue
                    item = dict(row)
                    item.setdefault("album", album.get("title"))
                    item.setdefault("artist", album.get("artist"))
                    item.setdefault("albumArtist", album.get("albumArtist"))
                    item.setdefault("genres", album.get("genres"))
                    if _text(album.get("poster")) and _as_bool(album.get("hasArtwork"), False):
                        item["poster"] = album.get("poster")
                        item["hasArtwork"] = True
                    tracks.append(item)
        return {
            "catalog_id": "",
            "tracks": tracks,
            "total": len(tracks),
            "libraries": library_names,
            "artists": [],
            "albums": [],
            "genres": [],
            "legacy": True,
        }

def _provider(client: Any = None, provider_id: Any = "") -> Any:
    del provider_id
    return TaterTubeMusicProvider.from_settings(_settings(client))


def _paired(
    settings: Optional[Dict[str, Any]] = None,
    provider_id: Any = "",
) -> bool:
    cfg = settings if isinstance(settings, dict) else _settings()
    del provider_id
    try:
        return bool(TaterTubeMusicProvider.from_settings(cfg).connected)
    except Exception:
        return False


def _genre_key(value: Any) -> str:
    return " ".join(
        "".join(char.lower() if char.isalnum() else " " for char in _text(value)).split()
    )


def _genre_marker_matches(key: str, marker: str) -> bool:
    return key == marker or f" {marker} " in f" {key} "


def _genres(value: Any, fallback: Any = "") -> List[str]:
    raw: List[Any]
    if isinstance(value, list) and any(_text(item) for item in value):
        raw = value
    else:
        text = _text(value or fallback)
        for separator in (";", "|"):
            text = text.replace(separator, ",")
        raw = text.split(",") if text else []
    result: List[str] = []
    seen = set()
    for item in raw:
        genre = _text(item)
        source_key = _genre_key(genre)
        if not source_key:
            continue
        expanded = [GENRE_CANONICAL_NAMES.get(source_key, genre)]
        expanded.extend(
            name
            for name, markers in GENRE_FAMILIES
            if any(_genre_marker_matches(source_key, marker) for marker in markers)
        )
        for candidate in expanded:
            key = candidate.casefold()
            if not candidate or key in seen:
                continue
            seen.add(key)
            result.append(candidate)
    return result


def _normalize_track(row: Dict[str, Any]) -> Dict[str, Any]:
    track_id = _text(row.get("ratingKey") or row.get("rating_key") or row.get("key") or row.get("id"))
    title = _text(row.get("title")) or "Untitled"
    artist = _text(row.get("artist"))
    album_artist = _text(row.get("albumArtist") or row.get("album_artist"))
    album = _text(row.get("album"))
    genres = _genres(row.get("genres"), row.get("genre"))
    duration = _as_float(row.get("durationSeconds") or row.get("duration_seconds") or row.get("duration"))
    if duration > 100000:
        duration /= 1000.0
    source_index = _as_int(row.get("sourceIndex") or row.get("source_index"), 0, 0, 10000)
    path = _text(row.get("path") or row.get("partKey") or row.get("part_key"))
    artwork_path, artwork_query_version = _sanitize_tater_artwork_reference(
        row.get("poster") or row.get("artwork_url") or row.get("artwork_path")
    )
    artwork_version = _text(row.get("artwork_version")) or artwork_query_version
    if not track_id:
        identity = "\x00".join(
            [
                _text(row.get("categoryId") or row.get("category_id")),
                str(source_index),
                path,
                artist,
                album,
                title,
            ]
        )
        track_id = "track:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return {
        "id": track_id,
        "title": title,
        "artist": artist,
        "album_artist": album_artist,
        "album": album,
        "genres": genres,
        "genre": ", ".join(genres),
        "year": _text(row.get("date") or row.get("year")),
        "track_number": _as_int(row.get("index") or row.get("track_number"), 0, 0, 10000),
        "disc_number": _as_int(
            row.get("disc") or row.get("disc_number"),
            0,
            0,
            1000,
        ),
        "duration_seconds": max(0.0, duration),
        "duration_display": _text(row.get("durationDisplay") or row.get("duration_display")),
        "category_id": _text(row.get("categoryId") or row.get("category_id")),
        "source_index": source_index,
        "path": path,
        "stream_path": _text(row.get("stream_path")),
        "provider_track_id": _text(
            row.get("provider_track_id")
            or row.get("ratingKey")
            or row.get("rating_key")
            or row.get("key")
        ),
        "container": _text(row.get("container") or Path(path).suffix.lstrip(".")).lower(),
        "media_type": _text(row.get("media_type") or row.get("content_type")).lower(),
        "size_bytes": _as_int(row.get("sizeBytes") or row.get("size_bytes"), 0, 0, 10**15),
        "modified_unix": _as_int(row.get("modifiedUnix") or row.get("modified_unix"), 0, 0, 10**12),
        "artwork_path": artwork_path,
        "artwork_item_id": "",
        "artwork_version": artwork_version,
        "has_artwork": bool(artwork_path),
        "provider": "tater_tube",
    }


def _facet_values(tracks: Iterable[Dict[str, Any]], key: str) -> List[str]:
    values: Dict[str, str] = {}
    for track in tracks:
        raw_values = track.get(key)
        if isinstance(raw_values, list):
            candidates = raw_values
        else:
            candidates = [raw_values]
        for raw in candidates:
            value = _text(raw)
            if value:
                values[value.casefold()] = value
    return sorted(values.values(), key=str.casefold)


def _catalog(client: Any = None, provider_id: Any = "") -> Dict[str, Any]:
    del provider_id
    store = client or globals().get("redis_client")
    now = time.monotonic()
    with _catalog_memory_cache_lock:
        cached = _catalog_memory_cache.get("payload")
        if (
            _catalog_memory_cache.get("store") is store
            and isinstance(cached, dict)
            and now - float(_catalog_memory_cache.get("loaded_at") or 0.0)
            < CATALOG_MEMORY_CACHE_TTL_SECONDS
        ):
            return cached

        payload = _load_json(store, CATALOG_KEY, {})
        if not isinstance(payload, dict):
            payload = {}
        if _text(payload.get("provider") or "tater_tube").lower() != "tater_tube":
            payload = {}
        for track in payload.get("tracks") or []:
            if isinstance(track, dict):
                _normalize_cached_tater_artwork(track)
                genres = _genres(track.get("genres"), track.get("genre"))
                track["genres"] = genres
                track["genre"] = ", ".join(genres)
        if isinstance(payload.get("tracks"), list):
            payload["genres"] = _facet_values(payload["tracks"], "genres")
        _catalog_memory_cache.update(
            {"store": store, "loaded_at": now, "payload": payload}
        )
        return payload


def _catalog_needs_artwork_refresh(client: Any = None, provider_id: Any = "") -> bool:
    store = client or globals().get("redis_client")
    # The core loop checks this every second. Reuse the in-process catalog so a
    # multi-megabyte library is not decoded from Redis on every heartbeat.
    payload = _catalog(store, provider_id)
    if not isinstance(payload, dict) or not payload:
        return True
    return (
        _text(payload.get("provider") or "tater_tube").lower() != "tater_tube"
        or _as_int(payload.get("artwork_schema"), 0, 0, 100) < CATALOG_ARTWORK_SCHEMA
    )


def _sync_catalog_impl(client: Any = None, provider_id: Any = "") -> Dict[str, Any]:
    del provider_id
    store = client or globals().get("redis_client")
    selected = "tater_tube"
    provider = _provider(store)
    if not provider.connected:
        raise ValueError(
            f"Connect {PROVIDER_LABELS.get(selected, selected)} before syncing its music library."
        )
    raw = provider.catalog()
    tracks = [
        _normalize_track(row)
        for row in (raw.get("tracks") if isinstance(raw, dict) else []) or []
        if isinstance(row, dict)
    ]
    artists = _facet_values(
        [
            {
                "artist": _text(track.get("album_artist")) or _text(track.get("artist")),
            }
            for track in tracks
        ],
        "artist",
    )
    albums = _facet_values(tracks, "album")
    genres = _facet_values(tracks, "genres")
    payload = {
        "provider": selected,
        "artwork_schema": CATALOG_ARTWORK_SCHEMA,
        "catalog_id": _text(raw.get("catalog_id")) if isinstance(raw, dict) else "",
        "tracks": tracks,
        "artists": artists,
        "albums": albums,
        "genres": genres,
        "libraries": raw.get("libraries") if isinstance(raw, dict) and isinstance(raw.get("libraries"), dict) else {},
        "synced_at": time.time(),
        "legacy_provider_api": bool(raw.get("legacy")) if isinstance(raw, dict) else False,
    }
    _save_json(store, CATALOG_KEY, payload)
    with _catalog_memory_cache_lock:
        _catalog_memory_cache.update(
            {
                "store": store,
                "loaded_at": time.monotonic(),
                "payload": payload,
            }
        )
    _save_hash(
        store,
        RUNTIME_KEY,
        {
            "status": "connected",
            "provider": selected,
            "last_sync_at": payload["synced_at"],
            "last_error": "",
            "track_count": len(tracks),
            "artist_count": len(artists),
            "album_count": len(albums),
            "genre_count": len(genres),
        },
    )
    return payload


def _sync_catalog(client: Any = None, provider_id: Any = "") -> Dict[str, Any]:
    global _catalog_sync_started_at
    if not _catalog_sync_lock.acquire(blocking=False):
        raise RuntimeError("Music library sync is already running.")
    store = client or globals().get("redis_client")
    _catalog_sync_started_at = time.time()
    try:
        payload = _sync_catalog_impl(store, provider_id)
        finished_at = time.time()
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_sync_finished_at": finished_at,
                "last_sync_duration_ms": max(0.0, (finished_at - _catalog_sync_started_at) * 1000.0),
                "last_sync_error": "",
                "last_sync_error_at": "",
                "sync_run_count": _as_int(runtime.get("sync_run_count"), 0, 0, 1_000_000_000) + 1,
            },
        )
        return payload
    except Exception as exc:
        finished_at = time.time()
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_sync_finished_at": finished_at,
                "last_sync_duration_ms": max(0.0, (finished_at - _catalog_sync_started_at) * 1000.0),
                "last_sync_error": _text(exc)[:500],
                "last_sync_error_at": finished_at,
            },
        )
        raise
    finally:
        _catalog_sync_started_at = 0.0
        _catalog_sync_lock.release()


def _clean_query_tokens(value: Any) -> List[str]:
    cleaned = "".join(char.lower() if char.isalnum() else " " for char in _text(value))
    tokens = [token for token in cleaned.split() if token and token not in GENERIC_SEARCH_WORDS]
    return tokens


def _track_haystack(track: Dict[str, Any]) -> str:
    return " ".join(
        [
            _text(track.get("title")),
            _text(track.get("artist")),
            _text(track.get("album_artist")),
            _text(track.get("album")),
            _text(track.get("genre")),
            _text(track.get("year")),
        ]
    ).casefold()


def _matches_filter(track: Dict[str, Any], key: str, value: Any) -> bool:
    wanted = _text(value).casefold()
    if not wanted:
        return True
    if key == "genre":
        wanted_values = {_text(item).casefold() for item in _genres([value])}
        values = {
            _text(item).casefold()
            for item in _genres(track.get("genres"), track.get("genre"))
        }
        return bool(wanted_values & values) or any(
            wanted in item for item in values
        )
    if key == "artist":
        value = f"{_text(track.get('artist'))} {_text(track.get('album_artist'))}".casefold()
        return wanted in value
    return wanted in _text(track.get(key)).casefold()


def _score_track(track: Dict[str, Any], filters: Dict[str, Any]) -> int:
    score = 0
    for key, weight in (("title", 140), ("artist", 100), ("album", 80), ("genre", 60)):
        wanted = _text(filters.get(key)).casefold()
        if not wanted:
            continue
        if key == "genre":
            values = [
                _text(item).casefold()
                for item in _genres(track.get("genres"), track.get("genre"))
            ]
        elif key == "artist":
            values = [_text(track.get("artist")).casefold(), _text(track.get("album_artist")).casefold()]
        else:
            values = [_text(track.get(key)).casefold()]
        if wanted in values:
            score += weight
        elif any(wanted in item for item in values):
            score += max(1, weight // 2)
    query_tokens = _clean_query_tokens(filters.get("query"))
    haystack = _track_haystack(track)
    score += sum(12 for token in query_tokens if token in haystack)
    return score


def _search_tracks(
    *,
    query: Any = "",
    title: Any = "",
    artist: Any = "",
    album: Any = "",
    genre: Any = "",
    limit: int = MAX_SEARCH_RESULTS,
    client: Any = None,
    provider_id: Any = "",
) -> List[Dict[str, Any]]:
    payload = _catalog(client, provider_id)
    tracks = payload.get("tracks") if isinstance(payload.get("tracks"), list) else []
    filters = {
        "query": _text(query),
        "title": _text(title),
        "artist": _text(artist),
        "album": _text(album),
        "genre": _text(genre),
    }
    query_tokens = _clean_query_tokens(query)
    rows: List[Dict[str, Any]] = []
    for track in tracks:
        if not isinstance(track, dict):
            continue
        if any(
            not _matches_filter(track, key, filters[key])
            for key in ("title", "artist", "album", "genre")
            if filters[key]
        ):
            continue
        haystack = _track_haystack(track)
        if query_tokens and any(token not in haystack for token in query_tokens):
            continue
        rows.append({**track, "_score": _score_track(track, filters)})
    rows.sort(
        key=lambda row: (
            -_as_int(row.get("_score"), 0, 0, 100000),
            _text(row.get("album_artist") or row.get("artist")).casefold(),
            _text(row.get("album")).casefold(),
            _as_int(row.get("track_number"), 0, 0, 10000),
            _text(row.get("title")).casefold(),
        )
    )
    result = []
    for row in rows[: max(1, min(1000, int(limit)))]:
        cleaned = dict(row)
        cleaned.pop("_score", None)
        result.append(cleaned)
    return result


def _public_track(track: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        key: track.get(key)
        for key in (
            "id",
            "title",
            "artist",
            "album_artist",
            "album",
            "genres",
            "genre",
            "year",
            "track_number",
            "duration_seconds",
            "duration_display",
            "provider",
        )
    }
    result["artwork_url"] = _artwork_display_url(track) if track else ""
    return result


def _player(client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, PLAYER_KEY, {})
    if not isinstance(payload, dict):
        payload = {}
    stale_provider = _text(payload.get("provider")).casefold() not in {"", "tater_tube"}
    payload.setdefault("status", "idle")
    payload["provider"] = "tater_tube"
    payload.setdefault("queue", [])
    if stale_provider:
        payload.update(
            {
                "status": "stopped",
                "queue": [],
                "queue_original": [],
                "current": {},
                "index": -1,
                "started_at": 0.0,
                "continuation_pending": False,
            }
        )
    for collection_key in ("queue", "queue_original"):
        for track in payload.get(collection_key) or []:
            if isinstance(track, dict):
                _normalize_cached_tater_artwork(track)
    current = payload.get("current") if isinstance(payload.get("current"), dict) else {}
    if current:
        _normalize_cached_tater_artwork(current)
    payload.setdefault("index", -1)
    targets = _normalize_stereo_targets(payload.get("targets") or payload.get("target"))
    payload["targets"] = targets
    payload["target"] = targets[0] if targets else ""
    payload["mixed_sync_adjustment_ms"] = _mixed_sync_adjustment(targets, _settings(store))
    payload.setdefault("shuffle", False)
    payload.setdefault("repeat", "off")
    payload.setdefault(
        "continuous_radio",
        _provider_id(payload.get("provider")) in CATALOG_PROVIDER_IDS,
    )
    payload.setdefault("continuation_pending", False)
    payload.setdefault("radio_name", "Tater Continuous Radio")
    return payload


def _save_player(player: Dict[str, Any], client: Any = None) -> None:
    store = client or globals().get("redis_client")
    targets = _normalize_stereo_targets(player.get("targets") or player.get("target"))
    player["targets"] = targets
    player["target"] = targets[0] if targets else ""
    player["updated_at"] = time.time()
    _save_json(store, PLAYER_KEY, player)


def _listening_history(client: Any = None) -> List[Dict[str, Any]]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, HISTORY_KEY, [])
    if not isinstance(payload, list):
        return []
    return [dict(row) for row in payload if isinstance(row, dict)]


def get_tater_tube_activity_events(
    *, redis_client=None, limit: int = 100, **_kwargs
) -> List[Dict[str, Any]]:
    """Expose privacy-safe listening activity to Tater Tube Core."""
    store = redis_client or globals().get("redis_client")
    payload = _load_json(store, TATER_TUBE_ACTIVITY_KEY, [])
    rows = [dict(row) for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
    return rows[-_as_int(limit, 100, 1, MAX_TATER_TUBE_ACTIVITY_EVENTS):]


def _publish_tater_tube_activity(track: Dict[str, Any], *, client: Any = None) -> None:
    store = client or globals().get("redis_client")
    title = _text(track.get("title"))
    if store is None or not title:
        return
    now = time.time()
    rows = get_tater_tube_activity_events(
        redis_client=store, limit=MAX_TATER_TUBE_ACTIVITY_EVENTS
    )
    track_id = _text(track.get("id"))
    if rows:
        latest = rows[-1]
        if (
            _text(latest.get("media_id")) == track_id
            and now - _as_float(latest.get("occurred_at")) < 30.0
        ):
            return
    rows.append(
        {
            "source": "music_core",
            "media_id": track_id or title,
            "media_type": "music",
            "title": title,
            "state": "started",
            "occurred_at": now,
            "metadata": {
                "action": "played",
                "artist": _text(track.get("artist") or track.get("album_artist")),
                "album": _text(track.get("album")),
                "genres": [_text(value) for value in track.get("genres") or [] if _text(value)],
                "provider": _provider_id(track.get("provider")),
            },
        }
    )
    _save_json(store, TATER_TUBE_ACTIVITY_KEY, rows[-MAX_TATER_TUBE_ACTIVITY_EVENTS:])


def _record_listening_history(
    track: Dict[str, Any],
    targets: Any = None,
    *,
    person_id: Any = "",
    client: Any = None,
) -> None:
    """Record successful starts without retaining credentials or stream URLs."""
    store = client or globals().get("redis_client")
    track_id = _text(track.get("id"))
    title = _text(track.get("title"))
    if store is None or not (track_id or title):
        return
    selected_person_id = _text(person_id) or _text(_settings(store).get("prompt_person_id"))
    now = time.time()
    history = _listening_history(store)
    if history:
        latest = history[-1]
        if (
            _text(latest.get("track_id")) == track_id
            and _text(latest.get("person_id")) == selected_person_id
            and now - _as_float(latest.get("played_at")) < 30.0
        ):
            return
    history.append(
        {
            "track_id": track_id,
            "title": title or "Untitled",
            "artist": _text(track.get("artist") or track.get("album_artist")),
            "album_artist": _text(track.get("album_artist") or track.get("artist")),
            "album": _text(track.get("album")),
            "genres": [_text(value) for value in track.get("genres") or [] if _text(value)],
            "provider": _provider_id(track.get("provider")),
            "targets": _list(targets),
            "person_id": selected_person_id,
            "person_name": _people_person_name(selected_person_id, store) if selected_person_id else "",
            "played_at": now,
        }
    )
    _save_json(store, HISTORY_KEY, history[-MAX_HISTORY_EVENTS:])
    _publish_tater_tube_activity(track, client=store)


def _recommendations(client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, RECOMMENDATIONS_KEY, {})
    return payload if isinstance(payload, dict) else {}


def _music_prompt_profile(client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, PROMPT_PROFILE_KEY, {})
    return payload if isinstance(payload, dict) else {}


def _profile_history(
    client: Any,
    *,
    person_id: Any,
    provider_id: Any,
) -> List[Dict[str, Any]]:
    selected_person = _text(person_id)
    selected_provider = _provider_id(provider_id)
    return [
        row
        for row in _listening_history(client)
        if _provider_id(row.get("provider")) == selected_provider
        and (not _text(row.get("person_id")) or _text(row.get("person_id")) == selected_person)
    ]


def _recommendation_candidates(
    catalog: Dict[str, Any],
    history: List[Dict[str, Any]],
    *,
    limit: int = MAX_RECOMMENDATION_CANDIDATES,
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    if not tracks:
        return [], {}

    artist_counts: Dict[str, int] = {}
    album_counts: Dict[str, int] = {}
    genre_counts: Dict[str, int] = {}
    recent_track_ids = set()
    recent_albums = set()
    for event in history[-120:]:
        artist = _text(event.get("album_artist") or event.get("artist")).casefold()
        album = _text(event.get("album")).casefold()
        if artist:
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
        if album:
            album_counts[album] = album_counts.get(album, 0) + 1
        for genre in event.get("genres") or []:
            token = _text(genre).casefold()
            if token:
                genre_counts[token] = genre_counts.get(token, 0) + 1
    for event in history[-40:]:
        track_id = _text(event.get("track_id"))
        album = _text(event.get("album")).casefold()
        if track_id:
            recent_track_ids.add(track_id)
        if album:
            recent_albums.add(album)

    def affinity(track: Dict[str, Any]) -> int:
        artist = _text(track.get("album_artist") or track.get("artist")).casefold()
        album = _text(track.get("album")).casefold()
        genres = [_text(value).casefold() for value in track.get("genres") or []]
        return (
            artist_counts.get(artist, 0) * 5
            + album_counts.get(album, 0) * 3
            + sum(genre_counts.get(genre, 0) * 2 for genre in genres if genre)
        )

    ordered_tracks = sorted(
        tracks,
        key=lambda track: (
            _text(track.get("id")) in recent_track_ids,
            -affinity(track),
            _text(track.get("artist") or track.get("album_artist")).casefold(),
            _text(track.get("album")).casefold(),
            _as_int(track.get("disc_number"), 0, 0, 1000),
            _as_int(track.get("track_number"), 0, 0, 10000),
            _text(track.get("title")).casefold(),
        ),
    )

    albums: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for track in tracks:
        artist = _text(track.get("album_artist") or track.get("artist"))
        album = _text(track.get("album"))
        if not album:
            continue
        albums.setdefault((artist.casefold(), album.casefold()), []).append(track)

    ranked_albums = sorted(
        albums.items(),
        key=lambda entry: (
            entry[0][1] in recent_albums,
            -max(affinity(track) for track in entry[1]),
            entry[0],
        ),
    )
    candidates: List[Dict[str, Any]] = []
    candidate_map: Dict[str, Dict[str, Any]] = {}
    album_limit = min(80, max(1, limit // 2))
    for (_artist_key, _album_key), album_tracks in ranked_albums[:album_limit]:
        album_tracks = sorted(
            album_tracks,
            key=lambda track: (
                _as_int(track.get("disc_number"), 0, 0, 1000),
                _as_int(track.get("track_number"), 0, 0, 10000),
                _text(track.get("title")).casefold(),
            ),
        )
        hero = next(
            (track for track in album_tracks if _as_bool(track.get("has_artwork"), False)),
            album_tracks[0],
        )
        artist = _text(hero.get("album_artist") or hero.get("artist"))
        album = _text(hero.get("album")) or "Untitled album"
        candidate_id = "album:" + hashlib.sha256(
            f"{_provider_id(hero.get('provider'))}\x00{artist.casefold()}\x00{album.casefold()}".encode("utf-8")
        ).hexdigest()[:18]
        candidate = {
            "id": candidate_id,
            "type": "album",
            "title": album,
            "artist": artist,
            "album": album,
            "genres": sorted(
                {
                    _text(genre)
                    for track in album_tracks
                    for genre in track.get("genres") or []
                    if _text(genre)
                }
            )[:8],
            "track_count": len(album_tracks),
            "track_ids": [_text(track.get("id")) for track in album_tracks if _text(track.get("id"))],
            "image_track_id": _text(hero.get("id")),
        }
        candidates.append({key: value for key, value in candidate.items() if key not in {"track_ids", "image_track_id"}})
        candidate_map[candidate_id] = candidate

    song_limit = max(0, limit - len(candidates))
    for track in ordered_tracks[:song_limit]:
        track_id = _text(track.get("id"))
        if not track_id:
            continue
        candidate_id = f"song:{track_id}"
        artist = _text(track.get("artist") or track.get("album_artist"))
        candidate = {
            "id": candidate_id,
            "type": "song",
            "title": _text(track.get("title")) or "Untitled",
            "artist": artist,
            "album": _text(track.get("album")),
            "genres": [_text(value) for value in track.get("genres") or [] if _text(value)][:8],
            "track_count": 1,
            "track_ids": [track_id],
            "image_track_id": track_id,
        }
        candidates.append({key: value for key, value in candidate.items() if key not in {"track_ids", "image_track_id"}})
        candidate_map[candidate_id] = candidate
    return candidates[:limit], candidate_map


def _music_llm_json(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    system_prompt: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    if llm_client is None:
        raise RuntimeError("No primary LLM is configured for Tater.")
    response = loop.run_until_complete(
        llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            max_tokens=None,
            temperature=0.4,
        )
    )
    raw = _text(((response or {}).get("message") or {}).get("content"))
    blob = extract_json(raw) or raw
    try:
        parsed = json.loads(blob)
    except Exception as exc:
        raise RuntimeError("The music recommendation model did not return valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("The music recommendation model returned an unsupported response.")
    return parsed


def _profile_ranked_values(
    history: List[Dict[str, Any]],
    *,
    field: str,
    limit: int,
) -> List[tuple[str, int]]:
    counts: Dict[str, tuple[str, int]] = {}
    for event in history:
        raw_values = event.get(field)
        values = raw_values if isinstance(raw_values, list) else [raw_values]
        for raw_value in values:
            value = _text(raw_value)
            key = value.casefold()
            if not value or not key:
                continue
            original, count = counts.get(key, (value, 0))
            counts[key] = (original, count + 1)
    ranked = sorted(counts.values(), key=lambda row: (-row[1], row[0].casefold()))
    return ranked[: max(1, limit)]


def _generate_music_prompt_profile_impl(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    client: Any = None,
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    person_id = _text(cfg.get("prompt_person_id"))
    if not person_id:
        raise ValueError("Choose a Person in Music Core Settings before building music prompt context.")
    person_name = _people_person_name(person_id, store)
    if not person_name:
        raise ValueError("The selected Music Core Person no longer exists.")
    provider_id = _provider_id(cfg.get("provider"))
    history = _profile_history(store, person_id=person_id, provider_id=provider_id)
    if not history:
        raise ValueError(f"Play some music for {person_name} before building a music profile.")

    artists = _profile_ranked_values(history, field="album_artist", limit=20)
    if not artists:
        artists = _profile_ranked_values(history, field="artist", limit=20)
    genres = _profile_ranked_values(history, field="genres", limit=20)
    recent_tracks: List[Dict[str, Any]] = []
    seen_tracks = set()
    for event in reversed(history):
        title = _text(event.get("title"))
        artist = _text(event.get("artist") or event.get("album_artist"))
        identity = (_text(event.get("track_id")) or title.casefold(), artist.casefold())
        if not title or identity in seen_tracks:
            continue
        seen_tracks.add(identity)
        recent_tracks.append(
            {
                "title": title,
                "artist": artist,
                "album": _text(event.get("album")),
                "played_at": _as_float(event.get("played_at")),
            }
        )
        if len(recent_tracks) >= 6:
            break

    result = _music_llm_json(
        loop,
        llm_client,
        (
            "Build a compact factual music taste profile for one person from listening counts and recent tracks. "
            "Do not infer demographic, emotional, medical, political, or other sensitive traits. Use only artist "
            "and genre names supplied in the input. Return JSON only in this exact shape: "
            '{"taste_summary":"one short sentence","favorite_artists":["name"],'
            '"favorite_genres":["name"]}.'
        ),
        {
            "person_name": person_name,
            "artist_counts": [{"name": name, "plays": count} for name, count in artists],
            "genre_counts": [{"name": name, "plays": count} for name, count in genres],
            "recent_tracks": recent_tracks,
        },
    )
    allowed_artists = {name.casefold(): name for name, _count in artists}
    allowed_genres = {name.casefold(): name for name, _count in genres}
    favorite_artists = [
        allowed_artists[value.casefold()]
        for value in _list(result.get("favorite_artists"))
        if value.casefold() in allowed_artists
    ][:8]
    favorite_genres = [
        allowed_genres[value.casefold()]
        for value in _list(result.get("favorite_genres"))
        if value.casefold() in allowed_genres
    ][:8]
    if not favorite_artists:
        favorite_artists = [name for name, _count in artists[:6]]
    if not favorite_genres:
        favorite_genres = [name for name, _count in genres[:6]]

    profile = {
        "person_id": person_id,
        "person_name": person_name,
        "provider": provider_id,
        "generated_at": time.time(),
        "history_event_count": len(history),
        "taste_summary": _text(result.get("taste_summary"))[:320],
        "favorite_artists": favorite_artists,
        "favorite_genres": favorite_genres,
        "recent_tracks": recent_tracks,
    }
    _save_json(store, PROMPT_PROFILE_KEY, profile)
    return profile


def _generate_music_prompt_profile(
    client: Any = None,
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    llm_client: Any = None,
) -> Dict[str, Any]:
    global _profile_started_at
    if not _profile_lock.acquire(blocking=False):
        raise RuntimeError("The Music Core prompt profile is already being refreshed.")
    store = client or globals().get("redis_client")
    _profile_started_at = time.time()
    owns_loop = loop is None
    active_loop = loop or asyncio.new_event_loop()
    if owns_loop:
        asyncio.set_event_loop(active_loop)
    try:
        model = llm_client if llm_client is not None else _get_primary_llm_client_from_env()
        profile = _generate_music_prompt_profile_impl(active_loop, model, store)
        finished_at = time.time()
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_profile_finished_at": finished_at,
                "last_profile_duration_ms": max(0.0, (finished_at - _profile_started_at) * 1000.0),
                "last_profile_error": "",
                "profile_run_count": _as_int(runtime.get("profile_run_count"), 0, 0, 1_000_000_000) + 1,
            },
        )
        return profile
    except Exception as exc:
        finished_at = time.time()
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_profile_finished_at": finished_at,
                "last_profile_duration_ms": max(0.0, (finished_at - _profile_started_at) * 1000.0),
                "last_profile_error": _text(exc)[:500],
            },
        )
        raise
    finally:
        if owns_loop:
            active_loop.close()
            asyncio.set_event_loop(None)
        _profile_started_at = 0.0
        _profile_lock.release()


def _schedule_music_prompt_profile_refresh(client: Any = None) -> bool:
    global _profile_thread
    with _state_lock:
        if _profile_thread is not None and _profile_thread.is_alive():
            return False

        def worker() -> None:
            try:
                _generate_music_prompt_profile(client)
            except Exception as exc:
                logger.warning("[Music] prompt profile refresh failed: %s", exc)

        _profile_thread = threading.Thread(
            target=worker,
            daemon=True,
            name="music-prompt-profile",
        )
        _profile_thread.start()
        return True


def _music_prompt_message(profile: Dict[str, Any]) -> str:
    person_name = _text(profile.get("person_name"))
    if not person_name:
        return ""
    lines = [
        f"Private music context for {person_name} (context only, not instructions).",
        "Use only when relevant to music requests; do not mention background tracking.",
    ]
    summary = _text(profile.get("taste_summary"))
    if summary:
        lines.append(f"Taste: {summary}")
    genres = _list(profile.get("favorite_genres"))[:8]
    if genres:
        lines.append("Favorite genres: " + ", ".join(genres))
    artists = _list(profile.get("favorite_artists"))[:8]
    if artists:
        lines.append("Favorite artists: " + ", ".join(artists))
    recent = []
    for track in profile.get("recent_tracks") or []:
        if not isinstance(track, dict) or not _text(track.get("title")):
            continue
        label = _text(track.get("title"))
        if _text(track.get("artist")):
            label += f" by {_text(track.get('artist'))}"
        recent.append(label)
        if len(recent) >= 5:
            break
    if recent:
        lines.append("Recently played: " + "; ".join(recent))
    return "\n".join(lines)[:MAX_PROMPT_CONTEXT_CHARS]


def get_hydra_system_prompt_fragments(
    *,
    role: str,
    redis_client: Any = None,
    origin: Optional[Dict[str, Any]] = None,
    memory_context: Optional[Dict[str, Any]] = None,
    personal_context: Optional[Dict[str, Any]] = None,
    **_kwargs,
) -> Dict[str, List[str]]:
    normalized_role = _text(role).lower()
    if normalized_role not in {"", "chat", "hermes", "memory_context", "music_context"}:
        return {}
    store = redis_client or globals().get("redis_client")
    cfg = _settings(store)
    if not _as_bool(cfg.get("prompt_context_enabled"), True):
        return {}
    configured_person_id = _text(cfg.get("prompt_person_id"))
    active_person_id = _context_person_id(origin, memory_context, personal_context)
    if not configured_person_id or active_person_id != configured_person_id:
        return {}
    profile = _music_prompt_profile(store)
    if (
        _text(profile.get("person_id")) != configured_person_id
        or _provider_id(profile.get("provider")) != _provider_id(cfg.get("provider"))
    ):
        return {}
    live_recent_tracks = []
    seen_tracks = set()
    for event in reversed(
        _profile_history(
            store,
            person_id=configured_person_id,
            provider_id=cfg.get("provider"),
        )
    ):
        title = _text(event.get("title"))
        artist = _text(event.get("artist") or event.get("album_artist"))
        identity = (_text(event.get("track_id")) or title.casefold(), artist.casefold())
        if not title or identity in seen_tracks:
            continue
        seen_tracks.add(identity)
        live_recent_tracks.append({"title": title, "artist": artist})
        if len(live_recent_tracks) >= 5:
            break
    if live_recent_tracks:
        profile = {**profile, "recent_tracks": live_recent_tracks}
    message = _music_prompt_message(profile)
    if not message:
        return {}
    return {
        "chat": [message],
        "hermes": [message],
        "memory_context": [message],
        "music_context": [message],
    }


def _generate_recommendations_impl(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    client: Any = None,
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    assistant_name = _assistant_first_name(store)
    recommendations_label = _recommendations_label(store)
    cfg = _settings(store)
    provider_id = _provider_id(cfg.get("provider"))
    if provider_id not in CATALOG_PROVIDER_IDS:
        raise ValueError(f"{recommendations_label} require a catalog-based music provider.")
    if not _paired(cfg, provider_id):
        raise ValueError(f"Connect {PROVIDER_LABELS[provider_id]} before making recommendations.")
    history = [
        row
        for row in _listening_history(store)
        if _provider_id(row.get("provider")) == provider_id
    ]
    if not history:
        raise ValueError(f"Play at least one song before asking {assistant_name} for recommendations.")
    catalog = _catalog(store, provider_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(store, provider_id)
    candidates, candidate_map = _recommendation_candidates(catalog, history)
    if not candidates:
        raise ValueError("The active music library has no recommendation candidates.")

    playlist_count = _as_int(cfg.get("recommendation_playlist_count"), 3, 1, 6)
    item_count = _as_int(cfg.get("recommendation_items_per_playlist"), 6, 3, 12)
    artist_counts: Dict[str, int] = {}
    genre_counts: Dict[str, int] = {}
    for event in history[-120:]:
        artist = _text(event.get("album_artist") or event.get("artist"))
        if artist:
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
        for genre in event.get("genres") or []:
            token = _text(genre)
            if token:
                genre_counts[token] = genre_counts.get(token, 0) + 1
    recent = [
        {
            "title": _text(event.get("title")),
            "artist": _text(event.get("artist") or event.get("album_artist")),
            "album": _text(event.get("album")),
            "genres": list(event.get("genres") or [])[:6],
        }
        for event in reversed(history[-40:])
    ]
    result = _music_llm_json(
        loop,
        llm_client,
        (
            f"You are {assistant_name}, a warm, imaginative personal music curator. Build named playlists from the "
            "listener's recent history and the supplied catalog candidates. Choose only exact candidate IDs "
            "from the catalog. Mix familiar taste with useful discovery, avoid filling every playlist with "
            "the same artist or album, and let playlists mix albums and individual songs when that makes sense. "
            "Give every playlist a short memorable name and a one-sentence description. Give every selection "
            "a short reason. Return JSON only in this exact shape: "
            '{"summary":"one friendly sentence","playlists":[{"name":"playlist name",'
            '"description":"one sentence","items":[{"candidate_id":"exact id","reason":"short reason"}]}]}. '
            f"Return up to {playlist_count} playlists with up to {item_count} unique selections in each."
        ),
        {
            "listening_patterns": {
                "top_artists": sorted(artist_counts.items(), key=lambda row: (-row[1], row[0]))[:20],
                "top_genres": sorted(genre_counts.items(), key=lambda row: (-row[1], row[0]))[:20],
            },
            "recent_listening": recent,
            "catalog_candidates": candidates,
        },
    )

    playlists: List[Dict[str, Any]] = []
    for position, raw_playlist in enumerate(result.get("playlists") or []):
        if not isinstance(raw_playlist, dict) or len(playlists) >= playlist_count:
            continue
        selections: List[Dict[str, Any]] = []
        flattened_track_ids: List[str] = []
        seen_candidates = set()
        seen_tracks = set()
        for raw_item in raw_playlist.get("items") or []:
            if isinstance(raw_item, str):
                raw_item = {"candidate_id": raw_item}
            if not isinstance(raw_item, dict) or len(selections) >= item_count:
                continue
            candidate_id = _text(raw_item.get("candidate_id"))
            candidate = candidate_map.get(candidate_id)
            if not candidate or candidate_id in seen_candidates:
                continue
            seen_candidates.add(candidate_id)
            track_ids = [
                track_id
                for track_id in candidate.get("track_ids") or []
                if track_id and track_id not in seen_tracks
            ]
            if not track_ids:
                continue
            seen_tracks.update(track_ids)
            flattened_track_ids.extend(track_ids)
            selections.append(
                {
                    "candidate_id": candidate_id,
                    "type": candidate["type"],
                    "title": candidate["title"],
                    "artist": candidate["artist"],
                    "album": candidate["album"],
                    "track_count": candidate["track_count"],
                    "track_ids": track_ids,
                    "image_track_id": candidate.get("image_track_id"),
                    "reason": _text(raw_item.get("reason"))[:240]
                    or f"{assistant_name} thinks this fits the mood of this mix.",
                }
            )
        if not selections:
            continue
        playlists.append(
            {
                "id": uuid.uuid4().hex[:12],
                "name": _text(raw_playlist.get("name"))[:80] or f"{assistant_name} Mix {position + 1}",
                "description": _text(raw_playlist.get("description"))[:300]
                or "A fresh mix shaped by what has been playing lately.",
                "items": selections,
                "track_ids": flattened_track_ids,
            }
        )
    if not playlists:
        raise RuntimeError("The music recommendation model did not select any valid catalog items.")

    now = time.time()
    published = {
        "provider": provider_id,
        "generated_at": now,
        "summary": _text(result.get("summary"))[:500]
        or f"{assistant_name} made a few fresh mixes from what has been playing lately.",
        "history_event_count": len(history),
        "playlists": playlists,
    }
    _save_json(store, RECOMMENDATIONS_KEY, published)
    _save_hash(
        store,
        RUNTIME_KEY,
        {
            "last_recommendation_at": now,
            "last_recommendation_attempt_at": now,
            "last_recommendation_error": "",
            "last_recommendation_error_at": "",
        },
    )
    return published


def _generate_recommendations(
    client: Any = None,
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    llm_client: Any = None,
) -> Dict[str, Any]:
    global _recommendation_started_at
    if not _recommendation_lock.acquire(blocking=False):
        raise RuntimeError("Tater music recommendations are already being refreshed.")
    store = client or globals().get("redis_client")
    _recommendation_started_at = time.time()
    owns_loop = loop is None
    active_loop = loop or asyncio.new_event_loop()
    if owns_loop:
        asyncio.set_event_loop(active_loop)
    try:
        model = llm_client if llm_client is not None else _get_primary_llm_client_from_env()
        result = _generate_recommendations_impl(active_loop, model, store)
        finished_at = time.time()
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_recommendation_finished_at": finished_at,
                "last_recommendation_duration_ms": max(
                    0.0,
                    (finished_at - _recommendation_started_at) * 1000.0,
                ),
                "recommendation_run_count": _as_int(
                    runtime.get("recommendation_run_count"),
                    0,
                    0,
                    1_000_000_000,
                )
                + 1,
            },
        )
        return result
    except Exception as exc:
        now = time.time()
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_recommendation_attempt_at": now,
                "last_recommendation_finished_at": now,
                "last_recommendation_duration_ms": max(
                    0.0,
                    (now - _recommendation_started_at) * 1000.0,
                ),
                "last_recommendation_error": _text(exc)[:500],
                "last_recommendation_error_at": now,
            },
        )
        raise
    finally:
        if owns_loop:
            active_loop.close()
            asyncio.set_event_loop(None)
        _recommendation_started_at = 0.0
        _recommendation_lock.release()


def _schedule_recommendation_refresh(client: Any = None) -> bool:
    """Start one detached refresh so model latency never pauses queue advancement."""
    global _recommendation_thread
    with _state_lock:
        if _recommendation_thread is not None and _recommendation_thread.is_alive():
            return False

        def worker() -> None:
            try:
                _generate_recommendations(client)
            except Exception as exc:
                logger.warning("[Music] recommendation refresh failed: %s", exc)

        _recommendation_thread = threading.Thread(
            target=worker,
            name="music-recommendations",
            daemon=True,
        )
        _recommendation_thread.start()
        return True


def _radio_session_token(player: Dict[str, Any]) -> str:
    token = _text(player.get("queue_session_id"))
    if token:
        return token
    created_at = _text(player.get("created_at"))
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    identity = "\x00".join(
        [
            created_at,
            _provider_id(player.get("provider")),
            *[_text(track.get("id")) for track in queue[:4] if isinstance(track, dict)],
        ]
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20] if identity else ""


def _continuation_candidate_tracks(
    player: Dict[str, Any],
    client: Any = None,
    *,
    limit: int = MAX_CONTINUATION_CANDIDATES,
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """Rank real catalog tracks with the active song and queue as the strongest signal."""
    store = client or globals().get("redis_client")
    provider_id = _provider_id(player.get("provider"), _provider_id(_settings(store).get("provider")))
    catalog = _catalog(store, provider_id)
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    if not tracks:
        return [], {}, []

    queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
    index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
    current = (
        dict(player.get("current"))
        if isinstance(player.get("current"), dict)
        else (queue[index] if queue else {})
    )
    nearby = queue[max(0, index - 2) : min(len(queue), index + 4)]
    current_artist = _text(current.get("album_artist") or current.get("artist")).casefold()
    current_album = _text(current.get("album")).casefold()
    current_genres = {
        _text(value).casefold() for value in current.get("genres") or [] if _text(value)
    }
    nearby_artists = {
        _text(track.get("album_artist") or track.get("artist")).casefold()
        for track in nearby
        if _text(track.get("album_artist") or track.get("artist"))
    }
    nearby_genres = {
        _text(genre).casefold()
        for track in nearby
        for genre in track.get("genres") or []
        if _text(genre)
    }

    history = [
        row
        for row in _listening_history(store)[-120:]
        if _provider_id(row.get("provider")) == provider_id
    ]
    history_artist_counts: Dict[str, int] = {}
    history_genre_counts: Dict[str, int] = {}
    recent_history_ids = set()
    for event in history:
        artist = _text(event.get("album_artist") or event.get("artist")).casefold()
        if artist:
            history_artist_counts[artist] = history_artist_counts.get(artist, 0) + 1
        for genre in event.get("genres") or []:
            token = _text(genre).casefold()
            if token:
                history_genre_counts[token] = history_genre_counts.get(token, 0) + 1
    for event in history[-50:]:
        if _text(event.get("track_id")):
            recent_history_ids.add(_text(event.get("track_id")))

    queue_ids = {_text(track.get("id")) for track in queue if _text(track.get("id"))}

    def similarity(track: Dict[str, Any]) -> int:
        artist = _text(track.get("album_artist") or track.get("artist")).casefold()
        album = _text(track.get("album")).casefold()
        genres = {_text(value).casefold() for value in track.get("genres") or [] if _text(value)}
        score = 0
        if current_artist and artist == current_artist:
            score += 120
        if current_album and album == current_album:
            score += 45
        score += len(current_genres & genres) * 90
        if artist in nearby_artists:
            score += 55
        score += len(nearby_genres & genres) * 35
        score += min(25, history_artist_counts.get(artist, 0) * 3)
        score += min(30, sum(history_genre_counts.get(genre, 0) for genre in genres))
        return score

    session_token = _radio_session_token(player)

    def ordering(track: Dict[str, Any]) -> tuple[Any, ...]:
        track_id = _text(track.get("id"))
        dispersion = hashlib.sha256(f"{session_token}\x00{track_id}".encode("utf-8")).hexdigest()
        return (
            track_id in recent_history_ids,
            -similarity(track),
            dispersion,
        )

    unique_pool = [track for track in tracks if _text(track.get("id")) not in queue_ids]
    if not unique_pool:
        unique_pool = list(tracks)
    ordered = sorted(unique_pool, key=ordering)
    relevant = [track for track in ordered if similarity(track) > 0]
    discovery = [track for track in ordered if similarity(track) <= 0]
    selected = [*relevant[:160], *discovery[: max(0, limit - min(160, len(relevant)))]]
    selected = selected[:limit]
    candidate_map = {_text(track.get("id")): track for track in selected if _text(track.get("id"))}
    candidates = [
        {
            "id": track_id,
            "title": _text(track.get("title")) or "Untitled",
            "artist": _text(track.get("artist") or track.get("album_artist")),
            "album": _text(track.get("album")),
            "genres": [_text(value) for value in track.get("genres") or [] if _text(value)][:8],
            "year": _text(track.get("year")),
        }
        for track_id, track in candidate_map.items()
    ]
    return candidates, candidate_map, selected


def _append_continuation_tracks(
    session_token: str,
    tracks: List[Dict[str, Any]],
    *,
    station_name: str = "",
    source: str = "ai",
    allow_repeats: bool = False,
    client: Any = None,
) -> int:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        if (
            _text(player.get("status")).lower() != "playing"
            or _provider_id(player.get("provider")) not in CATALOG_PROVIDER_IDS
            or _radio_session_token(player) != session_token
        ):
            return 0
        queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
        if not queue:
            return 0
        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        existing_ids = {_text(track.get("id")) for track in queue if _text(track.get("id"))}
        incoming: List[Dict[str, Any]] = []
        seen_incoming = set()
        for track in tracks:
            if not isinstance(track, dict):
                continue
            track_id = _text(track.get("id"))
            if not track_id or track_id in seen_incoming:
                continue
            if not allow_repeats and track_id in existing_ids:
                continue
            seen_incoming.add(track_id)
            incoming.append(dict(track))
        if not incoming:
            return 0

        maximum = max(
            2,
            _as_int(_settings(store).get("maximum_queue_tracks"), 200, 1, 1000),
        )
        required_space = max(0, len(queue) + len(incoming) - maximum)
        trim_count = min(index, required_space)
        if trim_count:
            queue = queue[trim_count:]
            index -= trim_count
        available = max(0, maximum - len(queue))
        incoming = incoming[:available]
        if not incoming:
            return 0
        queue.extend(incoming)
        player.update(
            {
                "queue": queue,
                "queue_original": [dict(track) for track in queue],
                "index": index,
                "current": queue[index],
                "continuous_radio": True,
                "continuation_pending": False,
                "radio_name": _text(station_name)[:80]
                or _text(player.get("radio_name"))
                or "Tater Continuous Radio",
                "radio_source": source,
                "radio_last_refill_at": time.time(),
                "radio_last_refill_count": len(incoming),
            }
        )
        _save_player(player, store)
        return len(incoming)


def _fallback_continuation_tracks(
    player: Dict[str, Any],
    client: Any = None,
    *,
    count: int = CONTINUATION_BATCH_TRACKS,
) -> List[Dict[str, Any]]:
    store = client or globals().get("redis_client")
    _candidates, _candidate_map, ordered = _continuation_candidate_tracks(
        player,
        store,
        limit=MAX_CONTINUATION_CANDIDATES,
    )
    if ordered:
        return [dict(track) for track in ordered[: max(1, count)]]
    provider_id = _provider_id(player.get("provider"))
    catalog = _catalog(store, provider_id)
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    return tracks[: max(1, count)]


def _select_continuation_tracks(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    player: Dict[str, Any],
    client: Any = None,
) -> tuple[List[Dict[str, Any]], str]:
    store = client or globals().get("redis_client")
    candidates, candidate_map, ordered = _continuation_candidate_tracks(player, store)
    if not candidates:
        raise ValueError("The active music library has no tracks for continuous radio.")
    queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
    index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
    current = (
        dict(player.get("current"))
        if isinstance(player.get("current"), dict)
        else (queue[index] if queue else {})
    )
    compact = lambda track: {
        "title": _text(track.get("title")),
        "artist": _text(track.get("artist") or track.get("album_artist")),
        "album": _text(track.get("album")),
        "genres": [_text(value) for value in track.get("genres") or [] if _text(value)][:8],
        "year": _text(track.get("year")),
    }
    history = [
        {
            "title": _text(event.get("title")),
            "artist": _text(event.get("artist") or event.get("album_artist")),
            "album": _text(event.get("album")),
            "genres": list(event.get("genres") or [])[:6],
        }
        for event in reversed(_listening_history(store)[-30:])
    ]
    result = _music_llm_json(
        loop,
        llm_client,
        (
            "You are Tater's continuous-radio music programmer. The currently playing song is the strongest "
            "signal. Choose a smooth sequence of similar songs from only the supplied catalog IDs, using artist, "
            "genre, album context, era, and the nearby queue to preserve the current musical direction. Listening "
            "history is secondary context. Avoid abrupt genre changes and unnecessary repeats, but allow adjacent "
            "artists and discoveries that genuinely fit. Give the station a short creative name. Return JSON only "
            "in this exact shape: "
            '{"station_name":"short name","items":[{"track_id":"exact catalog id"}]}. '
            f"Return up to {CONTINUATION_BATCH_TRACKS} unique tracks in playback order."
        ),
        {
            "currently_playing": compact(current),
            "recent_queue": [compact(track) for track in queue[max(0, index - 2) : index]],
            "up_next": [compact(track) for track in queue[index + 1 : index + 4]],
            "recent_listening": history,
            "catalog_candidates": candidates,
        },
    )
    selections: List[Dict[str, Any]] = []
    seen = set()
    for row in result.get("items") or []:
        if isinstance(row, str):
            row = {"track_id": row}
        if not isinstance(row, dict) or len(selections) >= CONTINUATION_BATCH_TRACKS:
            continue
        track_id = _text(row.get("track_id") or row.get("candidate_id"))
        track = candidate_map.get(track_id)
        if not track or track_id in seen:
            continue
        seen.add(track_id)
        selections.append(dict(track))
    for track in ordered:
        track_id = _text(track.get("id"))
        if len(selections) >= CONTINUATION_BATCH_TRACKS:
            break
        if track_id and track_id not in seen:
            seen.add(track_id)
            selections.append(dict(track))
    if not selections:
        raise RuntimeError("The continuous-radio model did not select any playable tracks.")
    return (
        selections,
        _text(result.get("station_name")) or "Tater Continuous Radio",
    )


def _generate_continuation_impl(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    player: Dict[str, Any],
    session_token: str,
    client: Any = None,
) -> int:
    selections, station_name = _select_continuation_tracks(
        loop,
        llm_client,
        player,
        client,
    )
    return _append_continuation_tracks(
        session_token,
        selections,
        station_name=station_name,
        source="ai",
        client=client,
    )


def _generate_continuation(
    player: Dict[str, Any],
    session_token: str,
    client: Any = None,
    *,
    llm_client: Any = None,
) -> int:
    global _continuation_started_at
    if not _continuation_lock.acquire(blocking=False):
        return 0
    _continuation_started_at = time.time()
    store = client or globals().get("redis_client")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        model = llm_client if llm_client is not None else _get_primary_llm_client_from_env()
        added = _generate_continuation_impl(loop, model, player, session_token, store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_continuation_at": time.time(),
                "last_continuation_error": "",
                "last_continuation_error_at": "",
            },
        )
        return added
    except Exception as exc:
        fallback = _fallback_continuation_tracks(
            player,
            store,
            count=CONTINUATION_BATCH_TRACKS,
        )
        added = _append_continuation_tracks(
            session_token,
            fallback,
            station_name="Tater Continuous Radio",
            source="smart_fallback",
            allow_repeats=True,
            client=store,
        )
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_continuation_at": time.time() if added else "",
                "last_continuation_error": _text(exc)[:500],
                "last_continuation_error_at": time.time(),
            },
        )
        logger.warning("[Music] continuous-radio AI refill failed; added %s fallback tracks: %s", added, exc)
        return added
    finally:
        with _state_lock:
            latest = _player(store)
            if (
                _radio_session_token(latest) == session_token
                and latest.get("continuation_pending")
            ):
                latest["continuation_pending"] = False
                _save_player(latest, store)
        finished_at = time.time()
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_continuation_finished_at": finished_at,
                "last_continuation_duration_ms": max(
                    0.0,
                    (finished_at - _continuation_started_at) * 1000.0,
                ),
                "continuation_run_count": _as_int(
                    runtime.get("continuation_run_count"),
                    0,
                    0,
                    1_000_000_000,
                )
                + 1,
            },
        )
        loop.close()
        asyncio.set_event_loop(None)
        _continuation_started_at = 0.0
        _continuation_lock.release()


def _schedule_continuation_refresh(
    player: Optional[Dict[str, Any]] = None,
    client: Any = None,
) -> bool:
    global _continuation_thread
    store = client or globals().get("redis_client")
    with _state_lock:
        current_player = dict(player) if isinstance(player, dict) else _player(store)
        queue = [dict(row) for row in current_player.get("queue") or [] if isinstance(row, dict)]
        if (
            _text(current_player.get("status")).lower() != "playing"
            or _provider_id(current_player.get("provider")) not in CATALOG_PROVIDER_IDS
            or _text(current_player.get("repeat")).lower() == "one"
            or not queue
        ):
            return False
        index = _as_int(current_player.get("index"), 0, 0, max(0, len(queue) - 1))
        if len(queue) - index - 1 > CONTINUATION_TRIGGER_REMAINING_TRACKS:
            return False
        maximum = max(
            2,
            _as_int(_settings(store).get("maximum_queue_tracks"), 200, 1, 1000),
        )
        if len(queue) >= maximum and index == 0:
            return False
        if _continuation_thread is not None and _continuation_thread.is_alive():
            return False
        session_token = _radio_session_token(current_player) or uuid.uuid4().hex
        current_player["queue_session_id"] = session_token
        current_player["continuous_radio"] = True
        current_player["continuation_pending"] = True
        _save_player(current_player, store)
        snapshot = json.loads(json.dumps(current_player))

        def worker() -> None:
            _generate_continuation(snapshot, session_token, store)

        _continuation_thread = threading.Thread(
            target=worker,
            name="music-continuous-radio",
            daemon=True,
        )
        _continuation_thread.start()
        return True


def _append_end_of_queue_fallback(player: Dict[str, Any], client: Any = None) -> int:
    store = client or globals().get("redis_client")
    session_token = _radio_session_token(player)
    if not session_token:
        return 0
    tracks = _fallback_continuation_tracks(player, store, count=4)
    return _append_continuation_tracks(
        session_token,
        tracks,
        station_name=_text(player.get("radio_name")) or "Tater Continuous Radio",
        source="end_of_queue_fallback",
        allow_repeats=True,
        client=store,
    )


def _origin_value(origin: Dict[str, Any], *keys: str) -> str:
    nested = origin.get("origin") if isinstance(origin.get("origin"), dict) else {}
    for key in keys:
        value = _text(origin.get(key) if origin.get(key) not in (None, "") else nested.get(key))
        if value:
            return value
    return ""


def _stereo_member_target_map() -> Dict[str, str]:
    """Map each configured stereo member to its pair playback target."""
    try:
        from tater_voice import stereo_pairs

        pairs = stereo_pairs.list_pairs()
    except Exception:
        return {}

    routes: Dict[str, str] = {}
    for pair in pairs if isinstance(pairs, list) else []:
        if not isinstance(pair, dict):
            continue
        pair_selector = _text(pair.get("selector"))
        if not pair_selector:
            pair_id = _text(pair.get("id"))
            pair_selector = f"stereo:{pair_id}" if pair_id else ""
        if not pair_selector:
            continue
        pair_target = (
            pair_selector
            if pair_selector.lower().startswith("voice_core:")
            else f"voice_core:{pair_selector}"
        )
        for key in ("left_selector", "right_selector"):
            member_selector = _text(pair.get(key))
            if not member_selector:
                continue
            member_target = (
                member_selector
                if member_selector.lower().startswith("voice_core:")
                else f"voice_core:{member_selector}"
            )
            routes[member_selector.casefold()] = pair_target
            routes[member_target.casefold()] = pair_target
    return routes


def _normalize_stereo_targets(value: Any) -> List[str]:
    """Replace paired satellite selections with one deduplicated stereo target."""
    routes = _stereo_member_target_map()
    return _list(
        [
            routes.get(target.casefold(), target)
            for target in _list(value)
            if not target.casefold().startswith("integration:roon:")
        ]
    )


def _compact_target_option(row: Dict[str, Any]) -> Dict[str, Any]:
    """Use shorter satellite labels in Music Core's space-limited pickers."""
    option = dict(row)
    label = _text(option.get("label"))
    prefix = "Tater Satellite:"
    if label.casefold().startswith(prefix.casefold()):
        option["label"] = f"Tater Sat:{label[len(prefix):]}"
    return option


def _target_options(
    current_values: Any = None,
    provider_id: Any = "",
    *,
    include_stereo_members: bool = False,
) -> List[Dict[str, str]]:
    try:
        from announcement_targets import build_announcement_target_options

        rows = build_announcement_target_options(
            homeassistant_base_url="",
            homeassistant_token="",
            include_homeassistant=True,
            include_sonos=True,
            include_airplay=True,
            include_voice_core=True,
            include_integrations=True,
            current_values=current_values,
        )
        options = [
            _compact_target_option(row)
            for row in rows
            if (
                isinstance(row, dict)
                and _text(row.get("value"))
                and not _text(row.get("value")).casefold().startswith("integration:roon:")
            )
        ]
        if not include_stereo_members:
            paired_members = _stereo_member_target_map()
            options = [
                row
                for row in options
                if _text(row.get("value")).casefold() not in paired_members
            ]
        return sorted(options, key=lambda row: _text(row.get("label")).casefold())
    except Exception as exc:
        logger.debug("[Music] target discovery unavailable: %s", exc)
        return []


def _target_alias_map(options: List[Dict[str, Any]]) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    for row in options:
        if not isinstance(row, dict):
            continue
        target = _text(row.get("value"))
        if not target:
            continue
        aliases[target.casefold()] = target
        for alias in _list(row.get("target_aliases")):
            aliases[alias.casefold()] = target
    return aliases


def _canonical_option_targets(targets: Any, options: List[Dict[str, Any]]) -> List[str]:
    aliases = _target_alias_map(options)
    return _list([aliases.get(target.casefold(), target) for target in _list(targets)])


def _target_from_query(value: Any, options: Optional[List[Dict[str, str]]] = None) -> str:
    token = _text(value)
    if not token:
        return ""
    lower = token.casefold()
    candidates = options if isinstance(options, list) else _target_options()
    if lower.startswith(("voice_core:", "ha:", "sonos:", "airplay:", "integration:")):
        return _target_alias_map(candidates).get(lower, token)
    exact = [
        _text(row.get("value"))
        for row in candidates
        if lower in {_text(row.get("value")).casefold(), _text(row.get("label")).casefold()}
    ]
    if exact:
        return exact[0]
    partial = [
        _text(row.get("value"))
        for row in candidates
        if lower in _text(row.get("label")).casefold()
    ]
    return partial[0] if len(partial) == 1 else ""


def _room_target_from_query(value: Any, options: List[Dict[str, str]]) -> str:
    """Resolve an automatic room destination, preferring Sonos over room satellites."""
    room_name = _text(value).casefold()
    if not room_name:
        return ""
    if room_name.startswith(("voice_core:", "ha:", "sonos:", "airplay:", "integration:")):
        return _text(value)
    matches = [
        row
        for row in options
        if isinstance(row, dict)
        and _text(row.get("value"))
        and room_name in f"{_text(row.get('label'))} {_text(row.get('value'))}".casefold()
    ]
    if not matches:
        return ""

    def rank(row: Dict[str, Any]) -> tuple[int, str]:
        target = _text(row.get("value")).casefold()
        label = _text(row.get("label")).casefold()
        if target.startswith(("sonos:", "integration:sonos:")) or "sonos" in label:
            priority = 0
        elif target.startswith("integration:"):
            priority = 1
        elif target.startswith("voice_core:"):
            priority = 2
        else:
            priority = 3
        return priority, label

    return _text(sorted(matches, key=rank)[0].get("value"))


def _preferred_room_target(room_names: Iterable[str], client: Any = None) -> str:
    names = [_text(value) for value in room_names if _text(value)]
    if not names:
        return ""
    try:
        from integration_registry import get_integration_room_preferred_media_player

        result = get_integration_room_preferred_media_player(names, client or redis_client)
        return _text(result.get("target")) if isinstance(result, dict) else ""
    except Exception:
        return ""


def _resolve_targets(
    requested: Any = "",
    *,
    room: Any = "",
    origin: Optional[Dict[str, Any]] = None,
    client: Any = None,
    provider_id: Any = "",
) -> List[str]:
    store = client or globals().get("redis_client")
    requested_values = _list(requested)
    context = origin if isinstance(origin, dict) else {}
    explicit_room_names = _list(room)
    origin_room_names = (
        []
        if explicit_room_names or requested_values
        else _list(_origin_value(context, "room_name", "area_name", "room_id", "area_id"))
    )
    room_names = explicit_room_names or origin_room_names
    preferred_by_room = {
        room_name: _preferred_room_target([room_name], store)
        for room_name in room_names
    }
    options = _target_options(
        current_values=[*requested_values, *preferred_by_room.values()],
        provider_id=provider_id,
        include_stereo_members=True,
    )
    if explicit_room_names:
        resolved_rooms = [
            preferred_by_room.get(room_name) or _room_target_from_query(room_name, options)
            for room_name in explicit_room_names
        ]
        if any(not target for target in resolved_rooms):
            return []
        return _normalize_stereo_targets(resolved_rooms)

    if requested_values:
        explicit = []
        for value in requested_values:
            direct_value = _text(value)
            if direct_value.casefold().startswith(("voice_core:", "ha:", "sonos:", "airplay:", "integration:")):
                target = _target_alias_map(options).get(direct_value.casefold(), direct_value)
            else:
                target = (
                    _preferred_room_target([direct_value], store)
                    or _target_from_query(direct_value, options)
                    or _room_target_from_query(direct_value, options)
                )
            explicit.append(target)
        if any(not target for target in explicit):
            return []
        return _normalize_stereo_targets(explicit)

    if room_names:
        resolved_rooms = [
            preferred_by_room.get(room_name) or _room_target_from_query(room_name, options)
            for room_name in room_names
        ]
        if any(not target for target in resolved_rooms):
            return []
        return _normalize_stereo_targets(resolved_rooms)

    selector = _origin_value(
        context,
        "satellite_selector",
        "voice_core_selector",
        "device_selector",
    )
    if selector:
        return _normalize_stereo_targets(
            [selector if selector.startswith("voice_core:") else f"voice_core:{selector}"]
        )
    cfg = _settings(store)
    defaults = _list(cfg.get("default_targets") or cfg.get("default_target"))
    resolved_defaults = [_target_from_query(value, options) for value in defaults]
    return _normalize_stereo_targets([target for target in resolved_defaults if target])


def _resolve_target(
    requested: Any = "",
    *,
    room: Any = "",
    origin: Optional[Dict[str, Any]] = None,
    client: Any = None,
    provider_id: Any = "",
) -> str:
    targets = _resolve_targets(
        requested,
        room=room,
        origin=origin,
        client=client,
        provider_id=provider_id,
    )
    return targets[0] if targets else ""


def _target_summary(targets: Any) -> str:
    values = _list(targets)
    if not values:
        return "no players"
    if len(values) == 1:
        return values[0]
    return f"{len(values)} destinations"


def _track_label(track: Dict[str, Any]) -> str:
    title = _text(track.get("title")) or "Untitled"
    artist = _text(track.get("artist") or track.get("album_artist"))
    return f"{title} by {artist}" if artist else title


def _track_media_type(track: Dict[str, Any]) -> str:
    declared = _text(track.get("media_type")).split(";", 1)[0].strip().lower()
    if declared.startswith("audio/"):
        return declared
    extension = Path(_text(track.get("path"))).suffix.lower()
    if not extension and _text(track.get("container")):
        extension = "." + _text(track.get("container")).lower().lstrip(".")
    return {
        ".aac": "audio/aac",
        ".aiff": "audio/aiff",
        ".alac": "audio/x-alac",
        ".flac": "audio/flac",
        ".m4a": "audio/mp4",
        ".m4b": "audio/mp4",
        ".mp3": "audio/mpeg",
        ".ogg": "audio/ogg",
        ".opus": "audio/ogg",
        ".wav": "audio/wav",
        ".wma": "audio/x-ms-wma",
    }.get(extension, "application/octet-stream")


def _play_track(
    track: Dict[str, Any],
    targets: Any,
    *,
    volume_percent: int,
    start_position_seconds: float = 0.0,
    mixed_sync_adjustment_ms: int = 0,
    player_settings: Optional[Dict[str, Dict[str, Any]]] = None,
    airplay_group_id: str = "",
    client: Any = None,
) -> Dict[str, Any]:
    provider = _provider(client, track.get("provider"))
    target_ids = _list(targets)
    selected_player_settings = (
        player_settings if isinstance(player_settings, dict) else {}
    )
    audio_sync_transcode = _uses_audio_sync_transcode(target_ids)
    source_url = provider.stream_url(track, audio_sync=audio_sync_transcode)
    if not source_url:
        raise RuntimeError(f"No stream is available for {_track_label(track)}.")
    from media_playback import play_media_url_targets

    duration = max(0.0, _as_float(track.get("duration_seconds")))
    source_path = Path(_text(track.get("path")) or "music-track")
    playback_media_type = "audio/wav" if audio_sync_transcode else _track_media_type(track)
    playback_filename = (
        f"{source_path.stem}.sync.wav"
        if audio_sync_transcode
        else source_path.name
    )
    result = play_media_url_targets(
        target_ids,
        source_url,
        media_type=playback_media_type,
        media_content_type="music",
        filename=playback_filename,
        text=f"Playing {_track_label(track)}.",
        title=_text(track.get("title")) or Path(_text(track.get("path")) or "music-track").stem,
        artist=_text(track.get("artist") or track.get("album_artist")),
        album=_text(track.get("album")),
        duration_seconds=duration,
        volume_percent=volume_percent,
        start_position_seconds=max(0.0, _as_float(start_position_seconds)),
        mixed_sync_adjustment_ms=_as_int(mixed_sync_adjustment_ms, 0, -750, 3000),
        target_volume_percent={
            target: _as_int(values.get("volume_percent"), volume_percent, 0, 100)
            for target, values in dict(player_settings or {}).items()
            if _text(target) and isinstance(values, dict)
        },
        target_sync_offset_ms={
            target: _as_int(values.get("sync_offset_ms"), 0, -1000, 1000)
            for target, values in dict(player_settings or {}).items()
            if _text(target) and isinstance(values, dict)
        },
        target_transport_mode={
            target: _player_transport_mode(values.get("transport_mode"))
            for target, values in dict(player_settings or {}).items()
            if _text(target)
            and isinstance(values, dict)
            and target.casefold().startswith(("sonos:", "integration:sonos:"))
        },
        airplay_group_id=_text(airplay_group_id),
        timeout_s=max(180.0, duration + 120.0),
        respect_reply_playback=False,
    )
    if not isinstance(result, dict) or result.get("ok") is False:
        raise RuntimeError(_text((result or {}).get("error")) or "Music playback failed.")
    result["audio_sync_transcode_used"] = audio_sync_transcode
    if audio_sync_transcode:
        result["audio_sync_transcode_profile"] = "audio_sync"
    return result


def _stop_target(targets: Any) -> List[str]:
    warnings: List[str] = []
    try:
        from announcement_targets import split_announcement_targets

        grouped = split_announcement_targets(_list(targets))
    except Exception as exc:
        return [_text(exc)]

    selectors = list(grouped.get("voice_core_selectors") or [])
    if selectors:
        try:
            from tater_voice import native_satellite, stereo_pairs

            for selector in selectors:
                members = [selector]
                pair = stereo_pairs.get_pair(selector) if stereo_pairs.is_stereo_selector(selector) else {}
                if isinstance(pair, dict) and pair:
                    members = [
                        _text(pair.get("left_selector")),
                        _text(pair.get("right_selector")),
                    ]
                for member in members:
                    if not member:
                        continue
                    try:
                        native_satellite.run_on_runtime_loop(
                            native_satellite.send_command(
                                member,
                                "media.session.stop",
                                {"reason": "music_core_stop"},
                            ),
                            timeout=8.0,
                        )
                    except Exception as exc:
                        warnings.append(f"{member}: {exc}")
        except Exception as exc:
            warnings.append(_text(exc))

    airplay_players = list(grouped.get("airplay_players") or [])
    try:
        from announcement_targets import resolve_sonos_airplay_target

        for speaker in grouped.get("sonos_speakers") or []:
            bridge_target = _text(resolve_sonos_airplay_target(f"sonos:{speaker}"))
            bridge_id = bridge_target.removeprefix("airplay:")
            if bridge_id and bridge_id not in airplay_players:
                airplay_players.append(bridge_id)
    except Exception:
        pass
    if airplay_players:
        try:
            from airplay_bridge import stop_airplay_targets

            result = stop_airplay_targets(airplay_players)
            warnings.extend(
                _text(value)
                for value in list(result.get("warnings") or [])
                if _text(value)
            )
        except Exception as exc:
            warnings.append(f"AirPlay Bridge: {exc}")

    integration_targets = list(grouped.get("integration_devices") or [])
    integration_targets.extend(
        {"integration_id": "homeassistant", "device_id": entity_id}
        for entity_id in grouped.get("homeassistant_media_players") or []
    )
    integration_targets.extend(
        {"integration_id": "sonos", "device_id": speaker}
        for speaker in grouped.get("sonos_speakers") or []
    )
    if integration_targets:
        try:
            from integration_registry import run_integration_device_action

            for row in integration_targets:
                try:
                    run_integration_device_action(
                        _text(row.get("integration_id")),
                        "stop",
                        _text(row.get("device_id")),
                        {},
                    )
                except Exception as exc:
                    warnings.append(
                        f"{_text(row.get('integration_id'))}:{_text(row.get('device_id'))}: {exc}"
                    )
        except Exception as exc:
            warnings.append(_text(exc))
    return warnings


def _player_position_seconds(player: Dict[str, Any], *, now: Optional[float] = None) -> float:
    position = max(0.0, _as_float(player.get("position_offset_seconds")))
    started_at = _as_float(player.get("started_at"))
    if _text(player.get("status")).lower() == "playing" and started_at > 0:
        position += max(0.0, (time.time() if now is None else float(now)) - started_at)
    return position


def _native_session_members(player: Dict[str, Any]) -> List[Dict[str, Any]]:
    playback_result = (
        player.get("playback_result")
        if isinstance(player.get("playback_result"), dict)
        else {}
    )
    members: List[Dict[str, Any]] = []
    for session in list(playback_result.get("voice_core_sessions") or []):
        if not isinstance(session, dict) or not _text(session.get("session_id")):
            continue
        selectors = _list(session.get("selectors") or session.get("target"))
        for selector in selectors:
            if selector:
                members.append(
                    {
                        "selector": selector,
                        "session_id": _text(session.get("session_id")),
                        "target": _text(session.get("target")),
                    }
                )
    return members


def _require_native_seek_support(targets: Any) -> None:
    try:
        from announcement_targets import split_announcement_targets
        from tater_voice import native_satellite, stereo_pairs

        grouped = split_announcement_targets(_list(targets))
        selectors = list(grouped.get("voice_core_selectors") or [])
        members: List[str] = []
        for selector in selectors:
            pair = stereo_pairs.get_pair(selector) if stereo_pairs.is_stereo_selector(selector) else {}
            if isinstance(pair, dict) and pair:
                members.extend(
                    member
                    for member in (
                        _text(pair.get("left_selector")),
                        _text(pair.get("right_selector")),
                    )
                    if member
                )
            elif selector:
                members.append(selector)
        unsupported = []
        for member in members:
            supported = native_satellite.run_on_runtime_loop(
                native_satellite.client_has_capability(
                    member,
                    "media_session_start_position",
                ),
                timeout=4.0,
            )
            if not supported:
                unsupported.append(member)
        if unsupported:
            raise ValueError(
                "Seeking needs the latest satellite firmware on "
                + ", ".join(unsupported)
                + "."
            )
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Could not confirm satellite seek support: {exc}") from exc


def _set_target_volume(player: Dict[str, Any], volume_percent: int) -> Dict[str, Any]:
    targets = _list(player.get("targets") or player.get("target"))
    if not targets:
        return {"sent_count": 0, "warnings": ["No playback destinations are selected."]}
    try:
        from announcement_targets import split_announcement_targets

        grouped = split_announcement_targets(targets)
    except Exception as exc:
        return {"sent_count": 0, "warnings": [_text(exc)]}

    sent_count = 0
    warnings: List[str] = []
    airplay_players = list(grouped.get("airplay_players") or [])
    if airplay_players:
        try:
            from airplay_bridge import set_airplay_target_volumes

            result = set_airplay_target_volumes(
                {f"airplay:{player}": volume_percent for player in airplay_players}
            )
            sent_count += _as_int(result.get("sent_count"), 0, 0, len(airplay_players))
            warnings.extend(
                _text(value)
                for value in list(result.get("warnings") or [])
                if _text(value)
            )
        except Exception as exc:
            warnings.append(f"AirPlay Bridge: {exc}")
    bridged_sonos_speakers = set()
    try:
        from airplay_bridge import set_airplay_target_volumes
        from announcement_targets import resolve_sonos_airplay_target

        for speaker in grouped.get("sonos_speakers") or []:
            bridge_target = _text(resolve_sonos_airplay_target(f"sonos:{speaker}"))
            if not bridge_target:
                continue
            result = set_airplay_target_volumes({bridge_target: volume_percent})
            if _as_int(result.get("sent_count"), 0, 0, 1) > 0:
                sent_count += 1
                bridged_sonos_speakers.add(_text(speaker))
    except Exception:
        pass
    native_members = _native_session_members(player)
    if grouped.get("voice_core_selectors") and not native_members:
        warnings.append("The current satellite playback session is unavailable; start the track again.")
    if native_members:
        try:
            from tater_voice import native_satellite, stereo_pairs

            pair_scales: Dict[str, int] = {}
            for target in grouped.get("voice_core_selectors") or []:
                pair = stereo_pairs.get_pair(target) if stereo_pairs.is_stereo_selector(target) else {}
                if not isinstance(pair, dict) or not pair:
                    continue
                pair_scales[_text(pair.get("left_selector"))] = _as_int(
                    pair.get("left_volume_percent"), 100, 0, 100
                )
                pair_scales[_text(pair.get("right_selector"))] = _as_int(
                    pair.get("right_volume_percent"), 100, 0, 100
                )
            for member in native_members:
                selector = _text(member.get("selector"))
                try:
                    supported = native_satellite.run_on_runtime_loop(
                        native_satellite.client_has_capability(selector, "media_session_volume"),
                        timeout=4.0,
                    )
                    if not supported:
                        raise RuntimeError("update satellite firmware to enable live music volume")
                    member_volume = round(volume_percent * pair_scales.get(selector, 100) / 100)
                    native_satellite.run_on_runtime_loop(
                        native_satellite.send_request(
                            selector,
                            "media.session.volume",
                            {
                                "session_id": _text(member.get("session_id")),
                                "volume_percent": max(0, min(100, member_volume)),
                            },
                            timeout_s=4.0,
                        ),
                        timeout=6.0,
                    )
                    sent_count += 1
                except Exception as exc:
                    warnings.append(f"{selector}: {exc}")
        except Exception as exc:
            warnings.append(_text(exc))

    integration_targets = list(grouped.get("integration_devices") or [])
    integration_targets.extend(
        {"integration_id": "homeassistant", "device_id": entity_id}
        for entity_id in grouped.get("homeassistant_media_players") or []
    )
    integration_targets.extend(
        {"integration_id": "sonos", "device_id": speaker}
        for speaker in grouped.get("sonos_speakers") or []
        if _text(speaker) not in bridged_sonos_speakers
    )
    if integration_targets:
        try:
            from integration_registry import run_integration_device_action

            for row in integration_targets:
                integration_id = _text(row.get("integration_id"))
                device_id = _text(row.get("device_id"))
                try:
                    result = run_integration_device_action(
                        integration_id,
                        "set_volume",
                        device_id,
                        {
                            "volume_percent": volume_percent,
                            "volume": volume_percent / 100.0,
                        },
                    )
                    if isinstance(result, dict) and result.get("ok") is False:
                        raise RuntimeError(_text(result.get("error")) or "volume change failed")
                    sent_count += 1
                except Exception as exc:
                    warnings.append(f"{integration_id}:{device_id}: {exc}")
        except Exception as exc:
            warnings.append(_text(exc))
    return {"sent_count": sent_count, "warnings": warnings}


def _start_player_index(
    index: int,
    *,
    start_position_seconds: float = 0.0,
    record_history: bool = True,
    client: Any = None,
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if not queue:
            raise ValueError("The music queue is empty.")
        if index < 0 or index >= len(queue):
            raise ValueError("The requested queue position is unavailable.")
        targets = _list(player.get("targets") or player.get("target"))
        if not targets:
            raise ValueError("Choose at least one satellite or media player before playing music.")
        track = queue[index]
        cfg = _settings(store)
        saved_volume = player.get("volume_percent")
        if saved_volume in (None, ""):
            saved_volume = cfg.get("default_volume_percent")
        volume = _as_int(
            saved_volume,
            75,
            0,
            100,
        )
        playback_result = (
            player.get("playback_result")
            if isinstance(player.get("playback_result"), dict)
            else {}
        )
        reusable_airplay_group_id = (
            _text(playback_result.get("airplay_bridge_group_id"))
            if _text(player.get("status")).lower() == "playing"
            else ""
        )
        if _text(player.get("status")).lower() == "playing" and not reusable_airplay_group_id:
            _stop_target(targets)
        duration = max(0.0, _as_float(track.get("duration_seconds")))
        start_position = max(0.0, _as_float(start_position_seconds))
        if duration > 0:
            start_position = min(duration, start_position)
        result = _play_track(
            track,
            targets,
            volume_percent=volume,
            start_position_seconds=start_position,
            mixed_sync_adjustment_ms=_mixed_sync_from_player_settings(
                targets,
                _selected_player_settings(targets, cfg, default_volume=volume),
                _mixed_sync_adjustment(targets, cfg),
            ),
            player_settings=_selected_player_settings(
                targets,
                cfg,
                default_volume=volume,
            ),
            airplay_group_id=reusable_airplay_group_id,
            client=store,
        )
        playback_result = {
            key: result.get(key)
            for key in (
                "target_count",
                "sent_count",
                "homeassistant_target_count",
                "voice_core_sent_count",
                "sonos_sent_count",
                "sonos_airplay_target_count",
                "sonos_airplay_routes",
                "airplay_bridge_target_count",
                "airplay_bridge_prepared_count",
                "airplay_bridge_primed_count",
                "airplay_bridge_sent_count",
                "airplay_bridge_group_id",
                "airplay_bridge_start_unix_ms",
                "airplay_native_start_lead_ms",
                "airplay_minimum_start_unix_ms",
                "airplay_bridge_reused",
                "airplay_bridge_reuse_fallback",
                "airplay_prepare_retried",
                "resume_fallback_used",
                "integration_sent_count",
                "media_session_sent_count",
                "media_session_fallback_count",
                "mixed_sync_adjustment_ms",
                "mixed_native_start_lead_ms",
                "sonos_proxy_used",
                "audio_sync_transcode_used",
                "audio_sync_transcode_profile",
            )
            if result.get(key) is not None
        }
        if isinstance(result.get("sonos_group"), dict):
            playback_result["sonos_group"] = dict(result["sonos_group"])
        voice_core_sessions = [
            dict(row)
            for row in list(result.get("voice_core_sessions") or [])
            if isinstance(row, dict) and _text(row.get("session_id"))
        ]
        if voice_core_sessions:
            playback_result["voice_core_sessions"] = voice_core_sessions
        player.update(
            {
                "status": "playing",
                "index": index,
                "current": track,
                "started_at": time.time(),
                "position_offset_seconds": start_position,
                "duration_seconds": duration,
                "volume_percent": volume,
                "mixed_sync_adjustment_ms": _mixed_sync_from_player_settings(
                    targets,
                    _selected_player_settings(targets, cfg, default_volume=volume),
                    _mixed_sync_adjustment(targets, cfg),
                ),
                "last_error": "",
                "playback_result": playback_result,
                "warnings": [
                    _text(value)
                    for value in list(result.get("warnings") or [])
                    if _text(value)
                ],
            }
        )
        _save_player(player, store)
        if record_history:
            _record_listening_history(
                track,
                targets,
                person_id=player.get("person_id"),
                client=store,
            )
        return player


def _route_player_targets(
    targets: Any,
    *,
    restart_playing: bool = True,
    force_restart: bool = False,
    client: Any = None,
) -> Dict[str, Any]:
    """Move the one global player session without replacing its queue or state."""
    store = client or globals().get("redis_client")
    next_targets = _normalize_stereo_targets(targets)
    if not next_targets:
        raise ValueError("Choose one or more valid music destinations.")
    with _state_lock:
        player = _player(store)
        old_targets = _list(player.get("targets") or player.get("target"))
        targets_changed = old_targets != next_targets
        was_playing = _text(player.get("status")).lower() == "playing"
        restart_required = was_playing and (targets_changed or force_restart)
        if not targets_changed and not restart_required:
            return player

        position = _player_position_seconds(player) if was_playing else max(
            0.0,
            _as_float(player.get("position_offset_seconds")),
        )
        warnings: List[str] = []
        if restart_required and old_targets:
            warnings = _stop_target(old_targets)
        player["targets"] = next_targets
        if restart_required:
            player.update(
                {
                    "status": "stopped",
                    "started_at": 0.0,
                    "position_offset_seconds": position,
                }
            )
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store)

        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if restart_required and restart_playing and queue:
            return _start_player_index(
                _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1)),
                start_position_seconds=position,
                record_history=False,
                client=store,
            )
        return player


def _seek_player(position_seconds: float, *, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    player = _player(store)
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    if not queue:
        raise ValueError("The music queue is empty.")
    duration = max(0.0, _as_float(player.get("duration_seconds")))
    if duration <= 0:
        raise ValueError("This track does not report a duration, so it cannot be seeked.")
    position = max(0.0, min(max(0.0, duration - 1.0), _as_float(position_seconds)))
    if position > 0:
        _require_native_seek_support(player.get("targets") or player.get("target"))
    return _start_player_index(
        _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1)),
        start_position_seconds=position,
        record_history=False,
        client=store,
    )


def _create_and_start_queue(
    tracks: List[Dict[str, Any]],
    *,
    targets: List[str],
    shuffle: bool,
    volume_percent: int,
    person_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    if not tracks:
        raise ValueError("No matching music was found.")
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    selected_person_id = _text(person_id) or _text(cfg.get("prompt_person_id"))
    maximum = _as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000)
    original_queue = [dict(track) for track in tracks[:maximum]]
    queue = [dict(track) for track in original_queue]
    if shuffle and len(queue) > 1:
        random.SystemRandom().shuffle(queue)
    with _state_lock:
        previous = _player(store)
        old_targets = _list(previous.get("targets") or previous.get("target"))
        if previous.get("status") == "playing" and old_targets:
            _stop_target(old_targets)
        player = {
            "status": "queued",
            "provider": _provider_id(queue[0].get("provider"), _provider_id(cfg.get("provider"))),
            "queue": queue,
            "queue_original": original_queue,
            "index": 0,
            "current": queue[0],
            "targets": _list(targets),
            "person_id": selected_person_id,
            "shuffle": bool(shuffle),
            "repeat": _text(previous.get("repeat") or "off"),
            "volume_percent": volume_percent,
            "mixed_sync_adjustment_ms": _mixed_sync_adjustment(targets, cfg),
            "created_at": time.time(),
            "queue_session_id": uuid.uuid4().hex,
            "continuous_radio": True,
            "continuation_pending": False,
            "radio_name": "Tater Continuous Radio",
            "started_at": 0.0,
            "position_offset_seconds": 0.0,
            "duration_seconds": 0.0,
            "last_error": "",
        }
        _save_player(player, store)
    return _start_player_index(0, client=store)


def _advance_player(direction: int, *, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if not queue:
            raise ValueError("The music queue is empty.")
        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        if direction < 0 and time.time() - _as_float(player.get("started_at")) > 8:
            next_index = index
        else:
            next_index = index + (1 if direction >= 0 else -1)
        repeat = _text(player.get("repeat") or "off").lower()
        if next_index >= len(queue):
            if repeat == "all":
                next_index = 0
            else:
                if _provider_id(player.get("provider")) in CATALOG_PROVIDER_IDS:
                    _append_end_of_queue_fallback(player, store)
                    player = _player(store)
                    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
                    index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
                    next_index = index + 1
                if next_index >= len(queue):
                    _stop_target(player.get("targets") or player.get("target"))
                    player.update(
                        {
                            "status": "finished",
                            "index": len(queue) - 1,
                            "started_at": 0.0,
                            "position_offset_seconds": max(
                                0.0, _as_float(player.get("duration_seconds"))
                            ),
                        }
                    )
                    _save_player(player, store)
                    return player
        if next_index < 0:
            next_index = len(queue) - 1 if repeat == "all" else 0
    return _start_player_index(next_index, client=store)


def _set_player_shuffle(enabled: bool, *, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        queue = [dict(track) for track in list(player.get("queue") or []) if isinstance(track, dict)]
        if not queue:
            player["shuffle"] = bool(enabled)
            _save_player(player, store)
            return player

        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        original = [
            dict(track)
            for track in list(player.get("queue_original") or queue)
            if isinstance(track, dict)
        ]
        player["queue_original"] = original
        if enabled:
            remaining = queue[index + 1 :]
            random.SystemRandom().shuffle(remaining)
        else:
            used = queue[: index + 1]

            def track_token(track: Dict[str, Any]) -> str:
                return _text(track.get("id") or track.get("url") or track.get("stream_url")) or json.dumps(
                    track,
                    sort_keys=True,
                    default=str,
                )

            used_counts: Dict[str, int] = {}
            for track in used:
                token = track_token(track)
                used_counts[token] = used_counts.get(token, 0) + 1
            remaining = []
            for track in original:
                token = track_token(track)
                if used_counts.get(token, 0) > 0:
                    used_counts[token] -= 1
                    continue
                remaining.append(dict(track))
        player["queue"] = [*queue[: index + 1], *remaining]
        player["shuffle"] = bool(enabled)
        _save_player(player, store)
        return player


def _pause_player(*, client: Any = None) -> Dict[str, Any]:
    """Stop active transports while preserving the current track position."""
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        if _text(player.get("status")).lower() != "playing":
            return player
        position = _player_position_seconds(player)
        duration = max(0.0, _as_float(player.get("duration_seconds")))
        if duration > 0:
            position = min(duration, position)
        targets = _list(player.get("targets") or player.get("target"))
        warnings = _stop_target(targets) if targets else []
        player.update(
            {
                "status": "paused",
                "started_at": 0.0,
                "position_offset_seconds": position,
            }
        )
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store)
        return player


def _resume_player(*, client: Any = None) -> Dict[str, Any]:
    """Resume a paused queue from its persisted position, or start it normally."""
    store = client or globals().get("redis_client")
    player = _player(store)
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    if not queue:
        raise ValueError("The music queue is empty.")
    status = _text(player.get("status")).lower()
    if status == "playing":
        return player
    return _start_player_index(
        _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1)),
        start_position_seconds=(
            _player_position_seconds(player)
            if status == "paused"
            else 0.0
        ),
        record_history=status != "paused",
        client=store,
    )


def _stop_player(*, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        targets = _list(player.get("targets") or player.get("target"))
        warnings = _stop_target(targets) if targets else []
        player.update(
            {
                "status": "stopped",
                "started_at": 0.0,
                "position_offset_seconds": 0.0,
            }
        )
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store)
        return player


def _advance_finished_player(client: Any = None) -> None:
    store = client or globals().get("redis_client")
    player = _reconcile_native_playback(_player(store), store)
    if _text(player.get("status")).lower() != "playing":
        return
    duration = _as_float(player.get("duration_seconds"))
    if duration <= 0 or _player_position_seconds(player) < duration + 0.15:
        return
    try:
        if _text(player.get("repeat")).lower() == "one":
            _start_player_index(_as_int(player.get("index"), 0, 0, 100000), client=store)
        else:
            _advance_player(1, client=store)
    except Exception as exc:
        player = _player(store)
        player.update({"status": "error", "last_error": _text(exc)[:500]})
        _save_player(player, store)


def _reconcile_native_playback(player: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    if _text(player.get("status")).lower() != "playing":
        return player
    playback_result = (
        player.get("playback_result")
        if isinstance(player.get("playback_result"), dict)
        else {}
    )
    sessions = [
        row
        for row in list(playback_result.get("voice_core_sessions") or [])
        if isinstance(row, dict) and _text(row.get("session_id"))
    ]
    if not sessions:
        return player
    try:
        from tater_voice import native_satellite

        snapshot = native_satellite.status_snapshot_sync()
    except Exception:
        return player
    clients = snapshot.get("clients") if isinstance(snapshot, dict) else {}
    if not isinstance(clients, dict):
        return player

    failed: List[str] = []
    for session in sessions:
        session_id = _text(session.get("session_id"))
        selectors = _list(session.get("selectors") or session.get("target"))
        states = []
        for selector in selectors:
            client_row = clients.get(selector)
            media_session = (
                client_row.get("media_session")
                if isinstance(client_row, dict) and isinstance(client_row.get("media_session"), dict)
                else {}
            )
            if _text(media_session.get("session_id")) == session_id:
                states.append(media_session)
        if not states or any(bool(state.get("active")) for state in states):
            continue
        finished_states = [state for state in states if _as_float(state.get("finished_ts")) > 0]
        if finished_states and any(state.get("ok") is False for state in finished_states):
            failed.append(_text(session.get("target")) or ", ".join(selectors))

    if not failed:
        return player
    warning = "Playback failed on " + ", ".join(failed) + "."
    warnings = [_text(value) for value in list(player.get("warnings") or []) if _text(value)]
    if warning not in warnings:
        warnings.append(warning)
    player["warnings"] = warnings
    sent_count = _as_int(playback_result.get("sent_count"), len(sessions), 0, 10000)
    voice_core_sent_count = _as_int(
        playback_result.get("voice_core_sent_count"),
        len(sessions),
        0,
        10000,
    )
    all_dispatched_targets_are_tracked_native_sessions = (
        len(sessions) >= voice_core_sent_count and sent_count <= voice_core_sent_count
    )
    if len(failed) == len(sessions) and all_dispatched_targets_are_tracked_native_sessions:
        player["status"] = "error"
        player["last_error"] = warning
        player["started_at"] = 0.0
    _save_player(player, client)
    return player


def _validate_catalog_provider_targets(targets: Any) -> None:
    roon_targets = [
        target
        for target in _list(targets)
        if target.lower().startswith("integration:roon:")
    ]
    if roon_targets:
        raise ValueError(
            "Roon zones cannot receive Music Core streams. Choose satellites, stereo pairs, "
            "or another supported media player."
        )


def _play_request(args: Dict[str, Any], origin: Optional[Dict[str, Any]], client: Any) -> Dict[str, Any]:
    cfg = _settings(client)
    selected_provider = _provider_id(args.get("provider"), _provider_id(cfg.get("provider")))
    catalog = _catalog(client, selected_provider)
    if not isinstance(catalog.get("tracks"), list) or not catalog.get("tracks"):
        catalog = _sync_catalog(client, selected_provider)
    query = _text(args.get("query") or args.get("music"))
    title = _text(args.get("title") or args.get("track") or args.get("song"))
    artist = _text(args.get("artist"))
    album = _text(args.get("album"))
    genre = _text(args.get("genre"))
    matches = _search_tracks(
        query=query,
        title=title,
        artist=artist,
        album=album,
        genre=genre,
        limit=_as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000),
        client=client,
        provider_id=selected_provider,
    )
    requested_targets = (
        args.get("targets")
        or args.get("target")
        or args.get("destinations")
        or args.get("destination")
        or args.get("players")
        or args.get("player")
    )
    targets = _resolve_targets(
        requested_targets,
        room=args.get("rooms") or args.get("room"),
        origin=origin,
        client=client,
        provider_id=selected_provider,
    )
    if not targets:
        raise ValueError("Choose one or more satellites, stereo pairs, or media players for this music.")
    _validate_catalog_provider_targets(targets)
    broad_request = bool(genre or artist or (query and not title and not album))
    shuffle = _as_bool(
        args.get("shuffle"),
        _as_bool(cfg.get("default_shuffle"), True) if broad_request else False,
    )
    requested_volume = args.get("volume_percent")
    if requested_volume in (None, ""):
        requested_volume = args.get("volume")
    if requested_volume in (None, ""):
        requested_volume = cfg.get("default_volume_percent")
    volume = _as_int(requested_volume, 75, 0, 100)
    player = _create_and_start_queue(
        matches,
        targets=targets,
        shuffle=shuffle,
        volume_percent=volume,
        person_id=_context_person_id(origin) or _text(cfg.get("prompt_person_id")),
        client=client,
    )
    return {
        "ok": True,
        "provider": selected_provider,
        "target": targets[0],
        "targets": targets,
        "target_count": len(targets),
        "queue_count": len(player.get("queue") or []),
        "shuffle": bool(player.get("shuffle")),
        "warnings": list(player.get("warnings") or []),
        "now_playing": _public_track(player.get("current") or {}),
        "summary_for_user": (
            f"Playing {_track_label(player.get('current') or {})} on {_target_summary(targets)}. "
            f"The queue has {len(player.get('queue') or [])} track"
            f"{'' if len(player.get('queue') or []) == 1 else 's'}, and continuous radio will keep it playing."
        ),
    }


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    return [
        {
            "id": "music_play",
            "description": (
                "Use when the user asks to play music by song, artist, album, genre, or description. "
                "Put user-named rooms in rooms, specific user-named speakers in targets, and leave both "
                "empty when playback should follow the speaking room."
            ),
            "usage": (
                '{"function":"music_play","arguments":{"query":"reggae music","genre":"reggae",'
                '"artist":"","album":"","title":"","targets":[],'
                '"rooms":["Family Room"],"shuffle":true,"volume_percent":75}}'
            ),
        },
        {
            "id": "music_search",
            "description": "Search Music Core without starting playback.",
            "usage": (
                '{"function":"music_search","arguments":{"query":"","genre":"","artist":"","album":"","title":"",'
                '"limit":10}}'
            ),
        },
        {
            "id": "music_control",
            "description": (
                "Control the Music Core queue: next, previous, stop, replay, shuffle, repeat, "
                "or set one or more playback destinations."
            ),
            "usage": (
                '{"function":"music_control","arguments":'
                '{"action":"next|previous|stop|replay|shuffle|repeat|set_targets",'
                '"targets":["Kitchen","Living Room"],"enabled":true,"mode":"off|all|one"}}'
            ),
        },
        {
            "id": "music_now_playing",
            "description": "Read the current Music Core track, queue, target, and playback state.",
            "usage": '{"function":"music_now_playing","arguments":{}}',
        },
        {
            "id": "music_browse",
            "description": "Browse artists, albums, genres, or tracks from Tater Tube Server.",
            "usage": (
                '{"function":"music_browse","arguments":{"category":"artists|albums|genres|tracks",'
                '"limit":50}}'
            ),
        },
    ]


async def run_hydra_kernel_tool(
    *,
    tool_id: str,
    args: Optional[Dict[str, Any]] = None,
    origin: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
    **_kwargs,
) -> Optional[Dict[str, Any]]:
    store = redis_client or globals().get("redis_client")
    values = args if isinstance(args, dict) else {}
    if tool_id == "music_play":
        try:
            return await asyncio.to_thread(_play_request, values, origin, store)
        except Exception as exc:
            return {
                "ok": False,
                "error": {"code": "music_play_failed", "message": _text(exc)},
                "say_hint": "Explain the music playback problem and ask for any missing song or destination detail.",
            }
    if tool_id == "music_search":
        try:
            cfg = _settings(store)
            selected_provider = _provider_id(
                values.get("provider"),
                _provider_id(cfg.get("provider")),
            )
            if not (_catalog(store, selected_provider).get("tracks") or []):
                await asyncio.to_thread(_sync_catalog, store, selected_provider)
            matches = _search_tracks(
                query=values.get("query"),
                title=values.get("title") or values.get("track") or values.get("song"),
                artist=values.get("artist"),
                album=values.get("album"),
                genre=values.get("genre"),
                limit=_as_int(values.get("limit"), 10, 1, 50),
                client=store,
                provider_id=selected_provider,
            )
            public = [_public_track(track) for track in matches]
            return {
                "ok": True,
                "provider": selected_provider,
                "count": len(public),
                "tracks": public,
                "summary_for_user": f"Found {len(public)} matching track{'' if len(public) == 1 else 's'}.",
            }
        except Exception as exc:
            return {"ok": False, "error": {"code": "music_search_failed", "message": _text(exc)}}
    if tool_id == "music_control":
        action = _text(values.get("action")).lower()
        try:
            if action == "next":
                player = await asyncio.to_thread(_advance_player, 1, client=store)
            elif action == "previous":
                player = await asyncio.to_thread(_advance_player, -1, client=store)
            elif action == "stop":
                player = await asyncio.to_thread(_stop_player, client=store)
            elif action == "replay":
                current = _player(store)
                player = await asyncio.to_thread(
                    _start_player_index,
                    _as_int(current.get("index"), 0, 0, 100000),
                    client=store,
                )
            elif action in {"play", "resume"}:
                player = await asyncio.to_thread(_resume_player, client=store)
            elif action == "pause":
                player = await asyncio.to_thread(_pause_player, client=store)
            elif action == "shuffle":
                player = _player(store)
                queue = player.get("queue") if isinstance(player.get("queue"), list) else []
                current = player.get("current") if isinstance(player.get("current"), dict) else {}
                remaining = [row for row in queue if _text(row.get("id")) != _text(current.get("id"))]
                if _as_bool(values.get("enabled"), True):
                    random.SystemRandom().shuffle(remaining)
                player["queue"] = ([current] if current else []) + remaining
                player["index"] = 0 if current else -1
                player["shuffle"] = _as_bool(values.get("enabled"), True)
                _save_player(player, store)
            elif action == "repeat":
                mode = _text(values.get("mode") or "off").lower()
                if mode not in {"off", "all", "one"}:
                    raise ValueError("Repeat mode must be off, all, or one.")
                player = _player(store)
                player["repeat"] = mode
                _save_player(player, store)
            elif action in {"set_target", "set_targets"}:
                player = _player(store)
                player_provider = _provider_id(player.get("provider"))
                targets = _resolve_targets(
                    values.get("targets") or values.get("target"),
                    room=values.get("rooms") or values.get("room"),
                    origin=origin,
                    client=store,
                    provider_id=player_provider,
                )
                if not targets:
                    raise ValueError("Choose one or more valid music destinations.")
                _validate_catalog_provider_targets(targets)
                player = await asyncio.to_thread(
                    _route_player_targets,
                    targets,
                    client=store,
                )
            else:
                raise ValueError(
                    "Music control action must be next, previous, stop, replay, shuffle, repeat, or set_targets."
                )
            targets = _list(player.get("targets") or player.get("target"))
            return {
                "ok": True,
                "status": _text(player.get("status")),
                "target": targets[0] if targets else "",
                "targets": targets,
                "target_count": len(targets),
                "now_playing": _public_track(player.get("current") or {}),
                "queue_count": len(player.get("queue") or []),
                "continuous_radio": bool(player.get("continuous_radio")),
                "radio_name": _text(player.get("radio_name")),
                "summary_for_user": (
                    f"Music is {_text(player.get('status')) or 'idle'} on {_target_summary(targets)}."
                ),
            }
        except Exception as exc:
            return {"ok": False, "error": {"code": "music_control_failed", "message": _text(exc)}}
    if tool_id == "music_now_playing":
        player = _player(store)
        targets = _list(player.get("targets") or player.get("target"))
        return {
            "ok": True,
            "status": _text(player.get("status")),
            "target": targets[0] if targets else "",
            "targets": targets,
            "target_count": len(targets),
            "now_playing": _public_track(player.get("current") or {}),
            "queue_count": len(player.get("queue") or []),
            "queue_index": _as_int(player.get("index"), -1, -1, 100000),
            "shuffle": bool(player.get("shuffle")),
            "repeat": _text(player.get("repeat") or "off"),
            "continuous_radio": bool(player.get("continuous_radio")),
            "radio_name": _text(player.get("radio_name")),
            "summary_for_user": (
                f"{_track_label(player.get('current') or {})} is {_text(player.get('status'))} "
                f"on {_target_summary(targets)}."
                if player.get("current")
                else "Music Core is idle."
            ),
        }
    if tool_id == "music_browse":
        selected_provider = _provider_id(
            values.get("provider"),
            _provider_id(_settings(store).get("provider")),
        )
        catalog = _catalog(store, selected_provider)
        if not (catalog.get("tracks") or []):
            catalog = await asyncio.to_thread(_sync_catalog, store, selected_provider)
        category = _text(values.get("category") or "genres").lower()
        limit = _as_int(values.get("limit"), 50, 1, 200)
        if category == "tracks":
            items: Any = [
                _public_track(row)
                for row in (catalog.get("tracks") or [])[:limit]
            ]
        elif category in {"artists", "albums", "genres"}:
            items = list(catalog.get(category) or [])[:limit]
        else:
            return {
                "ok": False,
                "error": {
                    "code": "music_browse_category",
                    "message": "Choose artists, albums, genres, or tracks.",
                },
            }
        return {
            "ok": True,
            "provider": selected_provider,
            "category": category,
            "items": items,
            "count": len(items),
            "summary_for_user": f"Music Core has {len(items)} {category} in this result.",
        }
    return None


def _format_time(timestamp: Any) -> str:
    value = _as_float(timestamp)
    if value <= 0:
        return "never"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def _art_data_uri(track: Dict[str, Any]) -> str:
    label = _text(track.get("album") or track.get("artist") or track.get("title") or "Music")
    digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
    color_a = f"#{digest[:6]}"
    color_b = f"#{digest[6:12]}"
    safe_label = (
        label.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )[:30]
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="360" height="360" viewBox="0 0 360 360">
<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="{color_a}"/><stop offset="1" stop-color="{color_b}"/></linearGradient></defs>
<rect width="360" height="360" rx="28" fill="#15111f"/><circle cx="180" cy="165" r="118" fill="url(#g)" opacity=".92"/>
<circle cx="180" cy="165" r="48" fill="#15111f"/><circle cx="180" cy="165" r="13" fill="#ffbd59"/>
<path d="M254 77v142c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V96l-88 19v124c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V95z" fill="#fff" opacity=".9"/>
<rect x="24" y="304" width="312" height="34" rx="17" fill="#15111f" opacity=".86"/><text x="180" y="327" fill="#fff" font-family="system-ui,sans-serif" font-size="17" text-anchor="middle">{safe_label}</text>
</svg>"""
    return "data:image/svg+xml;charset=utf-8," + quote(svg, safe="")


def _artwork_proxy_url(track: Dict[str, Any]) -> str:
    track_id = _text(track.get("id"))
    if not track_id or not _as_bool(track.get("has_artwork"), False):
        return ""
    query = {
        "track_id": track_id,
        "provider": _provider_id(track.get("provider")),
    }
    version = _text(track.get("artwork_version")) or _text(
        _as_int(track.get("modified_unix"), 0, 0, 10**12)
    )
    if version and version != "0":
        query["v"] = version[:128]
    return f"/api/cores/music_core/webhook/artwork?{urlencode(query)}"


def _artwork_display_url(track: Dict[str, Any]) -> str:
    return _artwork_proxy_url(track) or _art_data_uri(track)


def _facet_art_track(catalog: Dict[str, Any], singular: str, value: Any) -> Dict[str, Any]:
    wanted = _text(value).casefold()
    fallback: Dict[str, Any] = {}
    for track in catalog.get("tracks") or []:
        if not isinstance(track, dict):
            continue
        if singular == "album":
            matches = _text(track.get("album")).casefold() == wanted
        elif singular == "artist":
            matches = wanted in {
                _text(track.get("artist")).casefold(),
                _text(track.get("album_artist")).casefold(),
            }
        else:
            matches = wanted in {
                _text(genre).casefold() for genre in track.get("genres") or []
            }
        if not matches:
            continue
        fallback = fallback or track
        if _as_bool(track.get("has_artwork"), False):
            return track
    return fallback


def _player_item(
    player: Dict[str, Any],
    target_options: List[Dict[str, str]],
    active_provider: str,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    current = player.get("current") if isinstance(player.get("current"), dict) else {}
    status = _text(player.get("status") or "idle").upper()
    targets = _list(player.get("targets") or player.get("target"))
    target_summary = _target_summary(targets)
    player_warnings = [_text(value) for value in list(player.get("warnings") or []) if _text(value)]
    player_provider = _provider_id(player.get("provider"), active_provider)
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    queue_count = len(queue)
    current_index = _as_int(player.get("index"), -1, -1, max(0, queue_count - 1))
    duration_seconds = max(0.0, _as_float(player.get("duration_seconds")))
    position_seconds = _player_position_seconds(player)
    if duration_seconds > 0:
        position_seconds = min(duration_seconds, position_seconds)
    track_list = [
        {
            "id": f"queue:{index}",
            "position": index + 1,
            "title": _text(track.get("title")) or "Untitled",
            "artist": _text(track.get("artist") or track.get("album_artist")),
            "album": _text(track.get("album")),
            "duration": _text(track.get("duration_display")),
            "active": index == current_index,
            "image_src": _artwork_proxy_url(track),
            "image_alt": f"{_track_label(track)} artwork",
        }
        for index, track in enumerate(queue[:200])
        if isinstance(track, dict)
    ]
    targets_field = {
        "key": "targets",
        "label": "Play On",
        "type": "multiselect",
        "value": targets,
        "size": max(4, min(8, len(target_options))),
        "options": target_options or [{"value": "", "label": "No players discovered"}],
        "description": "Choose any mix of satellites, stereo pairs, and supported media players.",
    }
    default_volume = _as_int(player.get("volume_percent"), 75, 0, 100)
    player_rows = []
    for option in target_options:
        if not isinstance(option, dict):
            continue
        target = _text(option.get("value"))
        if not target:
            continue
        calibration = _target_calibration(target, cfg, default_volume=default_volume)
        transport_options = [
            dict(value)
            for value in list(option.get("transport_options") or [])
            if isinstance(value, dict) and _text(value.get("value"))
        ]
        player_rows.append(
            {
                "target": target,
                "label": _text(option.get("label")) or target,
                "meta": _text(
                    option.get("description")
                    or option.get("meta")
                    or option.get("room")
                    or option.get("area")
                ),
                "selected": target in targets,
                "kind": _client_target_kind(target),
                "sync_quality": (
                    "precise"
                    if _is_native_target(target)
                    else "automatic"
                    if transport_options
                    else "bridge"
                    if target.casefold().startswith("airplay:")
                    else "best_effort"
                ),
                "transport_options": transport_options,
                "airplay_bridge_target": _text(option.get("airplay_bridge_target")),
                **calibration,
            }
        )
    if status == "PLAYING":
        transport_toggle = {
            "action": "music_ui_pause",
            "label": "⏸",
            "aria_label": "Pause music",
            "tooltip": "Pause music",
            "working_text": "Pausing music...",
            "success_text": "Music paused.",
        }
    else:
        resume = status == "PAUSED" and bool(current)
        transport_toggle = {
            "action": "music_ui_play",
            "label": "▶",
            "aria_label": "Resume music" if resume else "Play music",
            "tooltip": "Resume music" if resume else "Play music",
            "working_text": "Resuming music..." if resume else "Finding and starting music...",
            "success_text": "Music resumed." if resume else "Music started.",
        }
    return {
        "id": "player:main",
        "group": "player",
        "card_variant": "player_bar",
        "title": _track_label(current) if current else "Music Player",
        "subtitle": f"{status} · {_text(current.get('album')) or 'No album selected'}",
        "detail": (
            _text(player.get("last_error"))
            if status == "ERROR" and _text(player.get("last_error"))
            else "Playback warning: " + player_warnings[0]
            if player_warnings
            else (
                f"Playing on {target_summary}. Queue position "
                f"{current_index + 1} of {queue_count}. Continuous radio keeps adding similar tracks."
            )
            if current
            else "Search your connected music library and choose where it should play."
        ),
        "hero_image_src": _artwork_display_url(current),
        "hero_image_alt": f"{_track_label(current) if current else 'Music'} artwork",
        "playback": {
            "status": status.lower(),
            "position_seconds": position_seconds,
            "duration_seconds": duration_seconds,
            "position_updated_at": time.time(),
            "seekable": bool(current and duration_seconds > 0),
            "seek_action": "music_ui_seek",
            "seek_relative_action": "music_ui_seek_relative",
            "seek_step_seconds": 15,
        },
        "hero_badges": [
            {
                "label": status,
                "tone": "good" if status == "PLAYING" else ("warn" if status == "ERROR" else "muted"),
            },
            {
                "label": (
                    "RADIO MIXING"
                    if player.get("continuation_pending")
                    else _text(player.get("radio_name") or "Continuous Radio").upper()
                ),
                "tone": "good",
            },
            {"label": PROVIDER_LABELS[player_provider].upper(), "tone": "muted"},
            {"label": f"{queue_count} TRACKS", "tone": "muted"},
            {"label": "SHUFFLE" if player.get("shuffle") else "IN ORDER", "tone": "muted"},
            {"label": f"REPEAT {_text(player.get('repeat') or 'off').upper()}", "tone": "muted"},
            {"label": f"{len(targets)} PLAYER{'' if len(targets) == 1 else 'S'}", "tone": "muted"},
            *([{"label": "PARTIAL PLAYBACK", "tone": "warn"}] if player_warnings else []),
        ],
        "summary_rows": [
            {"label": "Artist", "value": _text(current.get("artist") or current.get("album_artist")) or "—"},
            {"label": "Album", "value": _text(current.get("album")) or "—"},
            {"label": "Genre", "value": _text(current.get("genre")) or "—"},
            {"label": "Destinations", "value": target_summary if targets else "Choose below"},
        ],
        "fields_popup": False,
        "fields_dropdown": False,
        "popup_fields": [dict(targets_field)],
        "player_rows": player_rows,
        "test_sync_action": "music_ui_test_sync",
        "settings_title": "Choose Speakers & Players",
        "settings_label": "🔊",
        "settings_aria_label": "Choose speakers and players",
        "settings_tooltip": "Choose speakers and players",
        "show_save_button": False,
        "fields": [
            {
                "key": "volume_percent",
                "label": "Volume",
                "type": "range",
                "value": _as_int(player.get("volume_percent"), 75, 0, 100),
                "min": 0,
                "max": 100,
                "step": 1,
                "suffix": "%",
                "action": "music_ui_set_volume",
            },
        ],
        "track_list": track_list,
        "track_list_label": "Current Track List",
        "track_list_action": "music_ui_queue_play",
        "track_list_shuffle": bool(player.get("shuffle")),
        "track_list_shuffle_action": "music_ui_set_shuffle",
        "save_action": "music_ui_save_player",
        "save_label": "Set Player",
        "actions": [
            {
                "action": "music_ui_previous",
                "label": "⏮",
                "aria_label": "Previous track",
                "tooltip": "Previous track",
                "working_text": "Loading previous track...",
                "success_text": "Previous track started.",
            },
            transport_toggle,
            {
                "action": "music_ui_stop",
                "label": "■",
                "aria_label": "Stop music",
                "tooltip": "Stop music",
                "tone": "danger",
                "working_text": "Stopping music...",
                "success_text": "Music stopped.",
            },
            {
                "action": "music_ui_next",
                "label": "⏭",
                "aria_label": "Next track",
                "tooltip": "Next track",
                "working_text": "Loading next track...",
                "success_text": "Next track started.",
            },
        ],
    }


def _search_item() -> Dict[str, Any]:
    return {
        "id": "search:music",
        "group": "search",
        "card_variant": "music_search",
        "title": "Search Your Library",
        "subtitle": "Find a genre, artist, album, or song and start a fresh track list.",
        "fields_dropdown": False,
        "fields": [
            {
                "key": "query",
                "label": "Find Music",
                "type": "text",
                "value": "",
                "placeholder": "Reggae, Bob Marley, Exodus, or a song title",
            },
        ],
        "run_action": "music_ui_play",
        "run_label": "Play Search",
    }


def _facet_items(catalog: Dict[str, Any], category: str, label: str) -> List[Dict[str, Any]]:
    items = []
    singular = category[:-1] if category.endswith("s") else category
    for value in list(catalog.get(category) or [])[:500]:
        artwork_track = _facet_art_track(catalog, singular, value)
        items.append(
            {
                "id": f"{singular}:{value}",
                "group": category,
                "card_variant": "library_tile",
                "title": _text(value),
                "subtitle": f"Browse and play this {singular}.",
                "hero_image_src": (
                    _artwork_display_url(artwork_track)
                    if artwork_track
                    else _art_data_uri({singular: value, "title": value})
                ),
                "hero_badges": [{"label": label.upper(), "tone": "muted"}],
                "run_action": "music_ui_facet_play",
                "run_label": f"Play {label}",
            }
        )
    return items


def _client_target_kind(value: Any) -> str:
    token = _text(value).lower()
    if token.startswith("voice_core:"):
        return "satellite"
    if token.startswith("airplay:"):
        return "airplay_bridge"
    if token.startswith(("ha:", "sonos:", "integration:")):
        return "media_player"
    return "player"


def _personalized_client_tracks(
    catalog: Dict[str, Any],
    *,
    provider_id: str,
    limit: int,
    client: Any = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Build a fast For You feed from AI picks and recent listening affinity."""
    store = client or globals().get("redis_client")
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    clean_limit = max(1, min(200, int(limit)))
    history = [
        row
        for row in _listening_history(store)
        if _provider_id(row.get("provider")) == provider_id
    ][-MAX_HISTORY_EVENTS:]
    if not history:
        return tracks[:clean_limit], {
            "kind": "library",
            "title": "Library",
            "summary": "Play some music and Tater will personalize this list for you.",
            "history_event_count": 0,
            "ai_seed_count": 0,
        }

    artist_affinity: Dict[str, float] = {}
    album_affinity: Dict[str, float] = {}
    genre_affinity: Dict[str, float] = {}
    track_plays: Dict[str, int] = {}
    history_count = len(history)
    for position, event in enumerate(history):
        # Recent plays carry more weight, while older plays still preserve long-term taste.
        recency = 1.0 + (4.0 * (position + 1) / history_count)
        artist = _text(event.get("album_artist") or event.get("artist")).casefold()
        album = _text(event.get("album")).casefold()
        track_id = _text(event.get("track_id"))
        if artist:
            artist_affinity[artist] = artist_affinity.get(artist, 0.0) + (12.0 * recency)
        if album:
            album_affinity[album] = album_affinity.get(album, 0.0) + (8.0 * recency)
        for raw_genre in event.get("genres") or []:
            genre = _text(raw_genre).casefold()
            if genre:
                genre_affinity[genre] = genre_affinity.get(genre, 0.0) + (5.0 * recency)
        if track_id:
            track_plays[track_id] = track_plays.get(track_id, 0) + 1

    catalog_track_ids = {
        _text(track.get("id"))
        for track in tracks
        if _text(track.get("id"))
    }
    published = _recommendations(store)
    ai_seed_ids: List[str] = []
    seen_ai_ids = set()
    if _provider_id(published.get("provider"), "") == provider_id:
        for playlist in published.get("playlists") or []:
            if not isinstance(playlist, dict):
                continue
            for raw_track_id in playlist.get("track_ids") or []:
                track_id = _text(raw_track_id)
                if track_id in catalog_track_ids and track_id not in seen_ai_ids:
                    seen_ai_ids.add(track_id)
                    ai_seed_ids.append(track_id)
    ai_rank = {track_id: position for position, track_id in enumerate(ai_seed_ids)}

    recent_window = min(32, max(8, history_count // 3))
    recently_played_ids = {
        _text(event.get("track_id"))
        for event in history[-recent_window:]
        if _text(event.get("track_id"))
    }
    mix_token = _text(published.get("generated_at")) or _text(history[-1].get("played_at"))

    def rank(track: Dict[str, Any]) -> tuple[Any, ...]:
        track_id = _text(track.get("id"))
        artist = _text(track.get("album_artist") or track.get("artist")).casefold()
        album = _text(track.get("album")).casefold()
        genres = {
            _text(value).casefold()
            for value in track.get("genres") or []
            if _text(value)
        }
        score = (
            artist_affinity.get(artist, 0.0)
            + album_affinity.get(album, 0.0)
            + sum(genre_affinity.get(genre, 0.0) for genre in genres)
            + min(60.0, track_plays.get(track_id, 0) * 6.0)
        )
        if track_id in ai_rank:
            score += max(1000.0, 10000.0 - (ai_rank[track_id] * 40.0))
        if track_id in recently_played_ids:
            score -= 12000.0
        dispersion = hashlib.sha256(
            f"{provider_id}\x00{mix_token}\x00{track_id}".encode("utf-8")
        ).hexdigest()
        return (
            -score,
            dispersion,
            _text(track.get("title")).casefold(),
        )

    ranked = sorted(tracks, key=rank)

    # Reorder only within small relevance bands so the feed stays varied without
    # allowing low-affinity catalog tracks to jump ahead of strong matches.
    selected: List[Dict[str, Any]] = []
    artist_counts: Dict[str, int] = {}
    album_counts: Dict[tuple[str, str], int] = {}
    band_size = 24
    for offset in range(0, len(ranked), band_size):
        band = list(enumerate(ranked[offset : offset + band_size]))
        while band and len(selected) < clean_limit:
            position, track = min(
                band,
                key=lambda item: (
                    artist_counts.get(
                        _text(item[1].get("album_artist") or item[1].get("artist")).casefold(),
                        0,
                    ),
                    album_counts.get(
                        (
                            _text(item[1].get("album_artist") or item[1].get("artist")).casefold(),
                            _text(item[1].get("album")).casefold(),
                        ),
                        0,
                    ),
                    item[0],
                ),
            )
            band.remove((position, track))
            selected.append(track)
            artist = _text(track.get("album_artist") or track.get("artist")).casefold()
            album_key = (artist, _text(track.get("album")).casefold())
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
            album_counts[album_key] = album_counts.get(album_key, 0) + 1
        if len(selected) >= clean_limit:
            break

    ai_seed_count = len(ai_seed_ids)
    summary = (
        "Tater blended your AI picks with the artists, albums, and genres you play."
        if ai_seed_count
        else "Tater shaped these songs from the artists, albums, and genres you play."
    )
    return selected, {
        "kind": "personalized",
        "title": "For You",
        "summary": summary,
        "history_event_count": history_count,
        "ai_seed_count": ai_seed_count,
    }


def get_client_music_state(
    *,
    query: Any = "",
    limit: int = 60,
    refresh: bool = False,
    client: Any = None,
) -> Dict[str, Any]:
    """Return the credential-free Music Core state used by trusted Tater clients."""
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    active_provider = _provider_id(cfg.get("provider"))
    connected = _paired(cfg, active_provider)
    catalog: Dict[str, Any] = {}
    if active_provider in CATALOG_PROVIDER_IDS:
        if refresh and connected:
            catalog = _sync_catalog(store, active_provider)
        else:
            catalog = _catalog(store, active_provider)
    player = _reconcile_native_playback(_player(store), store)
    clean_limit = _as_int(limit, 60, 1, 200)
    clean_query = _text(query)
    if clean_query and active_provider in CATALOG_PROVIDER_IDS:
        tracks = _search_tracks(
            query=clean_query,
            limit=clean_limit,
            client=store,
            provider_id=active_provider,
        )
        track_feed = {
            "kind": "search",
            "title": "Results",
            "summary": "",
            "history_event_count": 0,
            "ai_seed_count": 0,
        }
    else:
        tracks, track_feed = _personalized_client_tracks(
            catalog,
            provider_id=active_provider,
            limit=clean_limit,
            client=store,
        )
    target_options = _target_options(
        current_values=player.get("targets"),
        provider_id=active_provider,
    )
    saved_player_targets = _list(player.get("targets") or player.get("target"))
    player_targets = _canonical_option_targets(saved_player_targets, target_options)
    if player_targets != saved_player_targets:
        player["targets"] = player_targets
        _save_player(player, store)
    targets: List[Dict[str, Any]] = []
    for row in target_options:
        if not isinstance(row, dict):
            continue
        target_id = _text(row.get("value"))
        if not target_id:
            continue
        transport_options = [
            {
                "value": _text(option.get("value")),
                "label": _text(option.get("label")) or _text(option.get("value")),
            }
            for option in list(row.get("transport_options") or [])
            if isinstance(option, dict) and _text(option.get("value"))
        ]
        calibration = _target_calibration(
            target_id,
            cfg,
            default_volume=_as_int(player.get("volume_percent"), 75, 0, 100),
        )
        targets.append(
            {
                "id": target_id,
                "label": _text(row.get("label")) or target_id,
                "kind": _client_target_kind(target_id),
                "description": _text(row.get("description") or row.get("meta")),
                "airplay_bridge_target": _text(row.get("airplay_bridge_target")),
                "transport_options": transport_options,
                "transport_mode": (
                    _player_transport_mode(calibration.get("transport_mode"))
                    if transport_options
                    else ""
                ),
            }
        )
    providers = [
        {
            "id": provider_id,
            "label": label,
            "connected": _paired(cfg, provider_id),
            "active": provider_id == active_provider,
            "local_playback": provider_id in CATALOG_PROVIDER_IDS,
        }
        for provider_id, label in PROVIDER_LABELS.items()
    ]
    queue = [
        _public_track(track)
        for track in list(player.get("queue") or [])[:200]
        if isinstance(track, dict)
    ]
    duration_seconds = max(
        0.0,
        _as_float(
            player.get("duration_seconds")
            or (player.get("current") or {}).get("duration_seconds")
        ),
    )
    position_seconds = _player_position_seconds(player)
    if duration_seconds > 0:
        position_seconds = min(duration_seconds, position_seconds)

    published = _recommendations(store)
    recommendations: List[Dict[str, Any]] = []
    if _provider_id(published.get("provider"), "") == active_provider:
        catalog_tracks = {
            _text(track.get("id")): track
            for track in catalog.get("tracks") or []
            if isinstance(track, dict) and _text(track.get("id"))
        }
        for playlist in published.get("playlists") or []:
            if not isinstance(playlist, dict) or not _text(playlist.get("id")):
                continue
            playlist_tracks = [
                _public_track(catalog_tracks[track_id])
                for track_id in (_text(value) for value in playlist.get("track_ids") or [])
                if track_id in catalog_tracks
            ]
            if not playlist_tracks:
                continue
            recommendations.append(
                {
                    "id": _text(playlist.get("id")),
                    "name": _text(playlist.get("name")) or "Tater Mix",
                    "description": _text(playlist.get("description")),
                    "tracks": playlist_tracks,
                    "track_count": len(playlist_tracks),
                    "artwork_url": _text(playlist_tracks[0].get("artwork_url")),
                }
            )
    return {
        "ok": True,
        "available": True,
        "version": __version__,
        "provider": {
            "id": active_provider,
            "label": PROVIDER_LABELS[active_provider],
            "connected": connected,
            "local_playback": active_provider in CATALOG_PROVIDER_IDS,
        },
        "providers": providers,
        "tracks": [_public_track(track) for track in tracks],
        "track_feed": track_feed,
        "track_count": len(catalog.get("tracks") or []),
        "artists": list(catalog.get("artists") or [])[:200],
        "albums": list(catalog.get("albums") or [])[:200],
        "genres": list(catalog.get("genres") or [])[:200],
        "recommendations": recommendations,
        "recommendation_summary": _text(published.get("summary")),
        "recommendation_generated_at": _as_float(published.get("generated_at")),
        "targets": targets,
        "player": {
            "status": _text(player.get("status") or "idle"),
            "provider": _provider_id(player.get("provider"), active_provider),
            "current": _public_track(player.get("current") or {}),
            "targets": player_targets,
            "target": player_targets[0] if player_targets else "",
            "queue_count": len(player.get("queue") or []),
            "queue_index": _as_int(player.get("index"), -1, -1, 100000),
            "queue": queue,
            "shuffle": bool(player.get("shuffle")),
            "repeat": _text(player.get("repeat") or "off"),
            "continuous_radio": bool(player.get("continuous_radio")),
            "radio_name": _text(player.get("radio_name")),
            "continuation_pending": bool(player.get("continuation_pending")),
            "position_seconds": position_seconds,
            "duration_seconds": duration_seconds,
            "seekable": bool(player.get("current") and duration_seconds > 0),
            "volume_percent": _as_int(
                player.get("volume_percent"),
                _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                0,
                100,
            ),
        },
        "synced_at": _as_float(catalog.get("synced_at")),
    }


def _client_track(track_id: Any, provider_id: str, client: Any) -> Dict[str, Any]:
    wanted = _text(track_id)
    if not wanted:
        raise ValueError("Choose a track first.")
    catalog = _catalog(client, provider_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(client, provider_id)
    for track in catalog.get("tracks") or []:
        if isinstance(track, dict) and _text(track.get("id")) == wanted:
            return dict(track)
    raise ValueError("That track is no longer in the active music library.")


def _client_tracks(track_ids: Any, provider_id: str, client: Any) -> List[Dict[str, Any]]:
    requested_ids = _list(track_ids)[:200]
    if not requested_ids:
        raise ValueError("Choose at least one track first.")
    catalog = _catalog(client, provider_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(client, provider_id)
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    tracks: List[Dict[str, Any]] = []
    seen = set()
    for track_id in requested_ids:
        if track_id in seen:
            continue
        track = track_by_id.get(track_id)
        if track is None:
            raise ValueError("One or more tracks are no longer in the active music library.")
        seen.add(track_id)
        tracks.append(dict(track))
    return tracks


def _client_local_continuation(
    values: Dict[str, Any],
    provider_id: str,
    client: Any,
) -> Dict[str, Any]:
    """Choose the next continuous-radio batch without starting a Tater player."""
    catalog = _catalog(client, provider_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(client, provider_id)
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    requested_ids = _list(values.get("track_ids"))[:200]
    current_track_id = _text(values.get("track_id"))
    queue = [
        dict(track_by_id[track_id])
        for track_id in requested_ids
        if track_id in track_by_id
    ]
    if not queue and current_track_id in track_by_id:
        queue = [dict(track_by_id[current_track_id])]
    if not queue:
        raise ValueError("Little Spud's current music queue is no longer in the active library.")
    index = _as_int(
        values.get("queue_index"),
        next(
            (
                position
                for position, track in enumerate(queue)
                if _text(track.get("id")) == current_track_id
            ),
            0,
        ),
        0,
        max(0, len(queue) - 1),
    )
    current = queue[index]
    _record_listening_history(
        current,
        ["little_spud:local"],
        client=client,
    )
    player = {
        "status": "playing",
        "provider": provider_id,
        "queue": queue,
        "queue_original": queue,
        "index": index,
        "current": current,
        "targets": ["little_spud:local"],
        "queue_session_id": _text(values.get("queue_session_id")) or uuid.uuid4().hex,
        "continuous_radio": True,
    }
    selections: List[Dict[str, Any]] = []
    station_name = "Little Spud Continuous Radio"
    source = "smart_fallback"
    acquired = _client_continuation_lock.acquire(blocking=False)
    loop: Optional[asyncio.AbstractEventLoop] = None
    try:
        if acquired:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            model = _get_primary_llm_client_from_env()
            selections, station_name = _select_continuation_tracks(
                loop,
                model,
                player,
                client,
            )
            source = "ai"
    except Exception as exc:
        logger.warning("[Music] Little Spud AI continuation failed; using smart fallback: %s", exc)
    finally:
        if loop is not None:
            loop.close()
            asyncio.set_event_loop(None)
        if acquired:
            _client_continuation_lock.release()
    if not selections:
        selections = _fallback_continuation_tracks(
            player,
            client,
            count=CONTINUATION_BATCH_TRACKS,
        )
    if not selections:
        raise ValueError("The active music library has no tracks for continuous radio.")
    return {
        "ok": True,
        "tracks": [_public_track(track) for track in selections[:CONTINUATION_BATCH_TRACKS]],
        "station_name": station_name,
        "source": source,
        "continuous_radio": True,
    }


def run_client_music_action(
    action: Any,
    payload: Optional[Dict[str, Any]] = None,
    *,
    client: Any = None,
) -> Dict[str, Any]:
    """Run a bounded Music Core action for native Tater clients."""
    store = client or globals().get("redis_client")
    values = payload if isinstance(payload, dict) else {}
    command = _text(action).lower()
    cfg = _settings(store)
    selected_provider = _provider_id(
        values.get("provider"),
        _provider_id(cfg.get("provider")),
    )
    if command in {"refresh", "sync"}:
        _sync_catalog(store, selected_provider)
        return get_client_music_state(client=store)
    if command == "local_play_started":
        track = _client_track(values.get("track_id"), selected_provider, store)
        _record_listening_history(
            track,
            ["little_spud:local"],
            client=store,
        )
        return {"ok": True}
    if command in {"continue_local", "local_continuation"}:
        return _client_local_continuation(values, selected_provider, store)
    if command in {"play_recommendation", "recommendation"}:
        player = _play_recommendation(
            values.get("recommendation_id"),
            store,
            requested_targets=values.get("targets") or values.get("target"),
            volume_percent=values.get("volume_percent"),
        )
        return {
            "ok": True,
            "summary_for_user": (
                f"Playing a Tater recommendation on "
                f"{_target_summary(player.get('targets'))}."
            ),
            "state": get_client_music_state(client=store),
        }
    if command in {"play_queue", "replace_queue", "play_album"}:
        tracks = _client_tracks(values.get("track_ids"), selected_provider, store)
        targets = _resolve_targets(
            values.get("targets") or values.get("target"),
            client=store,
            provider_id=selected_provider,
        )
        if not targets:
            raise ValueError("Choose a satellite or media player.")
        _validate_catalog_provider_targets(targets)
        player = _create_and_start_queue(
            tracks,
            targets=targets,
            shuffle=False,
            volume_percent=_as_int(
                values.get("volume_percent"),
                _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                0,
                100,
            ),
            client=store,
        )
        return {
            "ok": True,
            "summary_for_user": (
                f"Playing {len(tracks)} tracks on {_target_summary(targets)}."
            ),
            "now_playing": _public_track(player.get("current") or {}),
            "state": get_client_music_state(client=store),
        }
    if command == "play":
        track_id = _text(values.get("track_id"))
        if track_id:
            track = _client_track(track_id, selected_provider, store)
            targets = _resolve_targets(
                values.get("targets") or values.get("target"),
                client=store,
                provider_id=selected_provider,
            )
            if not targets:
                raise ValueError("Choose a satellite or media player.")
            _validate_catalog_provider_targets(targets)
            player = _create_and_start_queue(
                [track],
                targets=targets,
                shuffle=False,
                volume_percent=_as_int(
                    values.get("volume_percent"),
                    _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                    0,
                    100,
                ),
                client=store,
            )
            result = {
                "ok": True,
                "summary_for_user": (
                    f"Playing {_track_label(track)} on {_target_summary(targets)}."
                ),
                "now_playing": _public_track(player.get("current") or {}),
            }
        else:
            result = _play_request(values, {}, store)
        return {
            **result,
            "state": get_client_music_state(client=store),
        }

    player = _reconcile_native_playback(_player(store), store)
    requested_targets = values.get("targets") or values.get("target")
    routed_targets: List[str] = []
    if _list(requested_targets):
        routed_targets = _resolve_targets(
            requested_targets,
            client=store,
            provider_id=_provider_id(player.get("provider"), selected_provider),
        )
        if not routed_targets:
            raise ValueError("Choose one or more valid music destinations.")
        _validate_catalog_provider_targets(routed_targets)
    if command in {"set_target", "set_targets"}:
        updated = _route_player_targets(routed_targets, client=store)
        return {
            "ok": True,
            "player": {
                "status": _text(updated.get("status")),
                "current": _public_track(updated.get("current") or {}),
            },
            "state": get_client_music_state(client=store),
        }
    if command == "set_volume":
        if routed_targets:
            player = _route_player_targets(routed_targets, client=store)
        volume = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        live_result = {"sent_count": 0, "warnings": []}
        if _text(player.get("status")).lower() == "playing":
            live_result = _set_target_volume(player, volume)
            if _as_int(live_result.get("sent_count"), 0, 0, 10000) <= 0:
                warning = "; ".join(
                    _text(value)
                    for value in list(live_result.get("warnings") or [])
                    if _text(value)
                )
                raise ValueError(warning or "The active players could not change volume.")
        player["volume_percent"] = volume
        player["warnings"] = [
            _text(value)
            for value in list(live_result.get("warnings") or [])
            if _text(value)
        ]
        _save_player(player, store)
        return {
            "ok": True,
            "state": get_client_music_state(client=store),
        }
    if command == "seek":
        if routed_targets:
            player = _route_player_targets(
                routed_targets,
                restart_playing=False,
                client=store,
            )
        updated = _seek_player(
            _as_float(values.get("position_seconds")),
            client=store,
        )
        return {
            "ok": True,
            "player": {
                "status": _text(updated.get("status")),
                "current": _public_track(updated.get("current") or {}),
            },
            "state": get_client_music_state(client=store),
        }
    if command in {"next", "previous", "stop", "replay", "play", "resume", "pause"}:
        route_before_action = command in {"next", "previous", "replay", "play", "resume"}
        if routed_targets and route_before_action:
            player = _route_player_targets(
                routed_targets,
                restart_playing=command in {"play", "resume"},
                client=store,
            )
        if command == "next":
            updated = _advance_player(1, client=store)
        elif command == "previous":
            updated = _advance_player(-1, client=store)
        elif command == "stop":
            updated = _stop_player(client=store)
        elif command == "replay":
            updated = _start_player_index(
                _as_int(player.get("index"), 0, 0, 100000),
                client=store,
            )
        elif command in {"play", "resume"}:
            updated = _resume_player(client=store)
        else:
            updated = _pause_player(client=store)
        if routed_targets and command in {"stop", "pause"}:
            updated = _route_player_targets(routed_targets, client=store)
        return {
            "ok": True,
            "player": {
                "status": _text(updated.get("status")),
                "current": _public_track(updated.get("current") or {}),
            },
            "state": get_client_music_state(client=store),
        }
    raise ValueError(
        "Music action must be play, play_queue, play_recommendation, next, previous, stop, "
        "pause, resume, replay, seek, set_volume, continue_local, or refresh."
    )


def get_client_music_stream_source(
    track_id: Any,
    *,
    provider_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    """Resolve one Tater Tube stream for Tater's authenticated client proxy."""
    store = client or globals().get("redis_client")
    selected_provider = _provider_id(
        provider_id,
        _provider_id(_settings(store).get("provider")),
    )
    track = _client_track(track_id, selected_provider, store)
    source_url = _provider(store, selected_provider).stream_url(track)
    if not source_url:
        raise ValueError("That track does not have a playable stream.")
    return {
        "source_url": source_url,
        "media_type": _track_media_type(track),
        "filename": Path(_text(track.get("path")) or "music-track").name,
        "track": _public_track(track),
        "provider": selected_provider,
    }


def _provider_connection_detail(
    cfg: Dict[str, Any],
    provider_id: str,
) -> str:
    del provider_id
    return _text(
        cfg.get("tater_tube_server_url") or cfg.get("server_url")
    ) or "Pair with a Player PIN from Tater Tube Server."


def _provider_fields(cfg: Dict[str, Any], provider_id: str) -> List[Dict[str, Any]]:
    del provider_id
    return [
        {
            "key": "server_url",
            "label": "Tater Tube Server URL",
            "type": "text",
            "required": True,
            "value": _text(
                cfg.get("tater_tube_server_url") or cfg.get("server_url")
            ),
            "placeholder": "http://tater-tube-server:8080",
        },
        {
            "key": "name",
            "label": "Music Player Name",
            "type": "text",
            "value": _text(
                cfg.get("tater_tube_player_name") or cfg.get("player_name")
            )
            or "Tater Music Core",
        },
        {
            "key": "pin",
            "label": "6-digit Player Pairing PIN",
            "type": "password",
            "value": "",
            "description": (
                "Required for first connection. Leave blank to keep the existing pairing."
            ),
        },
    ]


def _provider_cards(
    cfg: Dict[str, Any],
    catalog: Dict[str, Any],
    active_provider: str,
) -> List[Dict[str, Any]]:
    del active_provider
    provider_id = "tater_tube"
    label = PROVIDER_LABELS[provider_id]
    connected = _paired(cfg)
    actions: List[Dict[str, Any]] = [
        {
            "action": "music_provider_connect",
            "label": "Connect / Test",
            "working_text": f"Connecting to {label}...",
            "success_text": f"{label} connected.",
        }
    ]
    if connected:
        actions.extend(
            [
                {
                    "action": "music_provider_activate",
                    "label": "Rescan Library",
                    "working_text": f"Loading the {label} library...",
                    "success_text": f"{label} library loaded.",
                },
                {
                    "action": "music_provider_disconnect",
                    "label": "Disconnect",
                    "tone": "danger",
                    "confirm": f"Disconnect Music Core from {label}?",
                },
            ]
        )
    return [
        {
            "id": "provider:tater_tube",
            "group": "providers",
            "title": label,
            "subtitle": "Connected music source" if connected else "Not connected",
            "detail": _provider_connection_detail(cfg, provider_id),
            "hero_badges": [
                {
                    "label": "CONNECTED" if connected else "SETUP NEEDED",
                    "tone": "good" if connected else "warn",
                },
                {"label": f"{len(catalog.get('tracks') or [])} TRACKS", "tone": "muted"},
            ],
            "fields": _provider_fields(cfg, provider_id),
            "fields_popup": False,
            "fields_dropdown": True,
            "actions": actions,
        }
    ]


def _recommendation_ui_items(
    cfg: Dict[str, Any],
    catalog: Dict[str, Any],
    runtime: Dict[str, Any],
    active_provider: str,
    client: Any = None,
) -> List[Dict[str, Any]]:
    assistant_name = _assistant_first_name(client)
    recommendations_label = _recommendations_label(client)
    default_mix_title = f"{assistant_name} Mix"
    history = [
        row
        for row in _listening_history(client)
        if _provider_id(row.get("provider")) == active_provider
    ]
    enabled = _as_bool(cfg.get("recommendations_enabled"), True)
    published = _recommendations(client)
    if _provider_id(published.get("provider"), "") != active_provider:
        published = {}
    generated_at = _as_float(published.get("generated_at"))
    last_error = _text(runtime.get("last_recommendation_error"))
    if not enabled:
        detail = f"Turn on {recommendations_label} in Settings to create AI-named music mixes."
    elif not history:
        detail = f"Start playing music and {assistant_name} will learn enough to prepare your first mixes."
    elif last_error and not published:
        detail = last_error
    elif generated_at:
        detail = (
            f"Built from {len(history)} listening event{'' if len(history) == 1 else 's'} · "
            f"updated {_format_time(generated_at)}"
        )
    else:
        detail = f"{assistant_name} has listening history and is ready to prepare your first mixes."

    items: List[Dict[str, Any]] = [
        {
            "id": "recommendations:overview",
            "group": "recommendations",
            "card_variant": "recommendations_intro",
            "title": recommendations_label,
            "assistant_name": assistant_name,
            "subtitle": _text(published.get("summary"))
            or "Named playlists made from what you actually listen to.",
            "detail": detail,
            "generated_at": generated_at,
            "history_event_count": len(history),
            "recommendations_enabled": enabled,
            "refresh_available": bool(enabled and history),
            "refresh_running": bool(
                _recommendation_lock.locked()
                or (_recommendation_thread is not None and _recommendation_thread.is_alive())
            ),
            "run_action": "music_recommendations_refresh",
            "run_label": "Refresh Recommendations",
        }
    ]
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    for playlist in published.get("playlists") or []:
        if not isinstance(playlist, dict) or not _text(playlist.get("id")):
            continue
        recommendation_items = []
        album_count = 0
        song_count = 0
        for selection in playlist.get("items") or []:
            if not isinstance(selection, dict):
                continue
            selection_type = _text(selection.get("type")) or "song"
            if selection_type == "album":
                album_count += 1
            else:
                song_count += 1
            art_track = track_by_id.get(_text(selection.get("image_track_id"))) or {}
            recommendation_items.append(
                {
                    "id": _text(selection.get("candidate_id")),
                    "type": selection_type,
                    "title": _text(selection.get("title")) or "Untitled",
                    "artist": _text(selection.get("artist")),
                    "album": _text(selection.get("album")),
                    "reason": _text(selection.get("reason")),
                    "track_count": _as_int(selection.get("track_count"), 1, 1, 10000),
                    "image_src": _artwork_display_url(art_track) if art_track else "",
                    "image_alt": f"{_text(selection.get('title')) or 'Music'} artwork",
                }
            )
        if not recommendation_items:
            continue
        hero_src = _text(recommendation_items[0].get("image_src"))
        items.append(
            {
                "id": f"recommendation:{_text(playlist.get('id'))}",
                "group": "recommendations",
                "card_variant": "recommendation_playlist",
                "title": _text(playlist.get("name")) or default_mix_title,
                "subtitle": _text(playlist.get("description")),
                "hero_image_src": hero_src,
                "hero_image_alt": f"{_text(playlist.get('name')) or default_mix_title} artwork",
                "hero_badges": [
                    {"label": "AI PLAYLIST", "tone": "good"},
                    {"label": f"{album_count} ALBUM{'' if album_count == 1 else 'S'}", "tone": "muted"},
                    {"label": f"{song_count} SONG{'' if song_count == 1 else 'S'}", "tone": "muted"},
                    {"label": f"{len(playlist.get('track_ids') or [])} TRACKS", "tone": "muted"},
                ],
                "recommendation_items": recommendation_items,
                "run_action": "music_recommendation_play",
                "run_label": "Play Playlist",
            }
        )
    return items


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    assistant_name = _assistant_first_name(store)
    assistant_possessive = _assistant_possessive(store)
    recommendations_label = _recommendations_label(store)
    cfg = _settings(store)
    prompt_person_id = _text(cfg.get("prompt_person_id"))
    people_options = _people_person_options(store)
    if prompt_person_id and not any(
        _text(option.get("value")) == prompt_person_id for option in people_options
    ):
        people_options.append({"value": prompt_person_id, "label": f"Saved Person: {prompt_person_id}"})
    active_provider = _provider_id(cfg.get("provider"))
    runtime = _runtime(store)
    catalog = _catalog(store, active_provider)
    player = _reconcile_native_playback(_player(store), store)
    connected = _paired(cfg, active_provider)
    saved_player_targets = _normalize_stereo_targets(
        player.get("targets") or player.get("target")
    )
    saved_default_targets = _normalize_stereo_targets(
        cfg.get("default_targets") or cfg.get("default_target")
    )
    saved_targets = _list([*saved_player_targets, *saved_default_targets])
    target_options = _target_options(
        current_values=saved_targets,
        provider_id=active_provider,
    )
    saved_player_targets = _canonical_option_targets(saved_player_targets, target_options)
    saved_default_targets = _canonical_option_targets(saved_default_targets, target_options)
    saved_targets = _list([*saved_player_targets, *saved_default_targets])
    player = dict(player)
    player["targets"] = saved_player_targets
    known_targets = set(_target_alias_map(target_options))
    for saved in saved_targets:
        if saved and saved.casefold() not in known_targets:
            target_options.append({"value": saved, "label": f"Saved player: {saved}"})
            known_targets.add(saved.casefold())

    item_forms = [_player_item(player, target_options, active_provider, cfg), _search_item()]
    item_forms.extend(
        _recommendation_ui_items(cfg, catalog, runtime, active_provider, store)
    )
    item_forms.extend(_facet_items(catalog, "genres", "Genre"))
    item_forms.extend(_facet_items(catalog, "artists", "Artist"))
    item_forms.extend(_facet_items(catalog, "albums", "Album"))
    item_forms.extend(_provider_cards(cfg, catalog, active_provider))
    item_forms.extend(
        [
            {
                "id": "settings:music",
                "group": "settings",
                "title": "Music Core Settings",
                "subtitle": "Defaults for catalog refreshes, queues, and playback.",
                "fields": [
                    {
                        "key": "catalog_sync_interval_seconds",
                        "label": "Catalog Sync Interval (sec)",
                        "type": "number",
                        "compact": True,
                        "value": _as_int(cfg.get("catalog_sync_interval_seconds"), 900, 60, 86400),
                    },
                    {
                        "key": "default_targets",
                        "label": "Default Players",
                        "type": "multiselect",
                        "value": saved_default_targets,
                        "size": max(4, min(8, len(target_options))),
                        "options": target_options,
                        "description": (
                            "Used only when the request does not name rooms or players "
                            "and did not originate from a satellite."
                        ),
                    },
                    {
                        "key": "default_volume_percent",
                        "label": "Default Volume (%)",
                        "type": "number",
                        "value": _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                    },
                    {
                        "key": "mixed_sync_default_adjustment_ms",
                        "label": "Default Mixed Sync Adjustment (ms)",
                        "type": "number",
                        "value": _as_int(cfg.get("mixed_sync_default_adjustment_ms"), 0, -750, 3000),
                        "min": -750,
                        "max": 3000,
                        "step": 25,
                        "description": (
                            "Starting adjustment for new Sonos + Tater Sat groups. Each group can be calibrated "
                            "from the player popup."
                        ),
                    },
                    {
                        "key": "default_shuffle",
                        "label": "Shuffle Broad Requests",
                        "type": "checkbox",
                        "value": _as_bool(cfg.get("default_shuffle"), True),
                    },
                    {
                        "key": "maximum_queue_tracks",
                        "label": "Maximum Queue Tracks",
                        "type": "number",
                        "value": _as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000),
                    },
                    {
                        "key": "recommendations_enabled",
                        "label": recommendations_label,
                        "type": "checkbox",
                        "value": _as_bool(cfg.get("recommendations_enabled"), True),
                        "description": (
                            f"Uses listening metadata and {assistant_possessive} primary AI model to make named playlists."
                        ),
                    },
                    {
                        "key": "recommendation_interval_hours",
                        "label": "Recommendation Refresh (hours)",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_interval_hours"), 12, 1, 168),
                    },
                    {
                        "key": "recommendation_playlist_count",
                        "label": "Recommendation Playlists",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_playlist_count"), 3, 1, 6),
                    },
                    {
                        "key": "recommendation_items_per_playlist",
                        "label": "Albums & Songs Per Playlist",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_items_per_playlist"), 6, 3, 12),
                    },
                    {
                        "key": "prompt_context_enabled",
                        "label": "Music Prompt Context",
                        "type": "checkbox",
                        "value": _as_bool(cfg.get("prompt_context_enabled"), True),
                        "description": (
                            "Gives Tater a small music profile only when the selected Person is speaking."
                        ),
                    },
                    {
                        "key": "prompt_person_id",
                        "label": "Music Profile Person",
                        "type": "select",
                        "value": prompt_person_id,
                        "options": people_options,
                        "description": (
                            "Links listening history, favorite genres and artists, recent tracks, and prompt context "
                            "to one Person from Tater's People settings."
                        ),
                    },
                    {
                        "key": "prompt_profile_interval_hours",
                        "label": "Music Profile Refresh (hours)",
                        "type": "number",
                        "value": _as_int(cfg.get("prompt_profile_interval_hours"), 12, 1, 168),
                    },
                ],
                "save_action": "music_save_settings",
                "save_label": "Save Music Settings",
            },
        ]
    )
    return {
        "summary": "Tater Tube music with voice control, personalized recommendations, and multi-room playback.",
        "stats": [
            {
                "label": "Music Source",
                "value": (
                    PROVIDER_LABELS[active_provider]
                    if connected
                    else f"{PROVIDER_LABELS[active_provider]} · Setup Needed"
                ),
            },
            {"label": "Tracks", "value": len(catalog.get("tracks") or [])},
            {"label": "Artists", "value": len(catalog.get("artists") or [])},
            {"label": "Albums", "value": len(catalog.get("albums") or [])},
            {"label": "Genres", "value": len(catalog.get("genres") or [])},
            {
                "label": "Last Scan",
                "value": _format_time(
                    catalog.get("synced_at") or runtime.get("last_sync_at")
                ),
            },
        ],
        "items": [],
        "empty_message": "Connect Tater Tube Server to load your music library.",
        "ui": {
            "kind": "settings_manager",
            "title": "Music Core",
            "appearance": "music_library",
            "live_updates": True,
            "poll_interval_ms": 3000,
            "persistent_item_groups": ["player"],
            "default_tab": "library",
            "manager_tabs": [
                {
                    "key": "library",
                    "label": "Browse Library",
                    "source": "grouped_items",
                    "groups": [
                        {
                            "key": "search",
                            "label": "Search",
                            "item_group": "search",
                            "selector": False,
                            "empty_message": "Search is unavailable.",
                        },
                        {
                            "key": "genres",
                            "label": "Genres",
                            "item_group": "genres",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No genres are available in the active library.",
                        },
                        {
                            "key": "artists",
                            "label": "Artists",
                            "item_group": "artists",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No artists are available in the active library.",
                        },
                        {
                            "key": "albums",
                            "label": "Albums",
                            "item_group": "albums",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No albums are available in the active library.",
                        },
                    ],
                },
                {
                    "key": "recommendations",
                    "label": recommendations_label,
                    "source": "items",
                    "item_group": "recommendations",
                    "empty_message": f"Play some music to help {assistant_name} build recommendations.",
                },
                {"key": "providers", "label": "Tater Tube", "source": "items", "item_group": "providers"},
                {"key": "settings", "label": "Settings", "source": "items", "item_group": "settings"},
            ],
            "item_fields_dropdown": True,
            "item_fields_popup": True,
            "item_forms": item_forms,
        },
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values") if isinstance(payload, dict) else {}
    return values if isinstance(values, dict) else {}


def _provider_from_card(payload: Dict[str, Any], fallback: Any = "") -> str:
    item_id = _text(payload.get("id"))
    if item_id and item_id != "provider:tater_tube":
        raise ValueError("Music Core only supports Tater Tube Server.")
    candidate = _text(payload.get("provider") or fallback).lower().replace("-", "_")
    if candidate and candidate not in {"tater_tube", "tatertube", "tater_tube_server"}:
        raise ValueError("Music Core only supports Tater Tube Server.")
    return "tater_tube"


def _connect_provider(
    provider_id: str,
    values: Dict[str, Any],
    client: Any,
) -> Dict[str, Any]:
    if provider_id != "tater_tube":
        raise ValueError("Music Core only supports Tater Tube Server.")
    cfg = _settings(client)
    updates: Dict[str, Any] = {}
    server_url = _normalize_server_url(
        values.get("server_url")
        or cfg.get("tater_tube_server_url")
        or cfg.get("server_url")
    )
    name = _text(
        values.get("name")
        or cfg.get("tater_tube_player_name")
        or cfg.get("player_name")
    ) or "Tater Music Core"
    pin = "".join(char for char in _text(values.get("pin")) if char.isdigit())
    token = _text(cfg.get("tater_tube_token") or cfg.get("token"))
    player_id = _text(cfg.get("tater_tube_player_id") or cfg.get("player_id"))
    if pin:
        if len(pin) != 6:
            raise ValueError("Enter the 6-digit Player PIN created by Tater Tube Server.")
        paired = TaterTubeMusicProvider.pair(server_url, pin, name)
        if not isinstance(paired, dict) or not _text(paired.get("token")):
            raise RuntimeError("Tater Tube Server did not return a music player token.")
        token = _text(paired.get("token"))
        player_id = _text(paired.get("player_id"))
        name = _text(paired.get("player_name")) or name
    if not token:
        raise ValueError("Enter a 6-digit Player PIN to pair Tater Tube Server.")
    updates.update(
        {
            "tater_tube_server_url": server_url,
            "tater_tube_player_name": name,
            "tater_tube_player_id": player_id,
            "tater_tube_token": token,
            "server_url": server_url,
            "player_name": name,
            "player_id": player_id,
            "token": token,
        }
    )

    _save_hash(client, SETTINGS_KEY, updates)
    catalog = _sync_catalog(client, provider_id)
    return {
        "ok": True,
        "message": (
            f"{PROVIDER_LABELS[provider_id]} connected and loaded "
            f"{len(catalog.get('tracks') or [])} tracks."
        ),
    }


def _disconnect_provider(provider_id: str, client: Any) -> Dict[str, Any]:
    if provider_id != "tater_tube":
        raise ValueError("Music Core only supports Tater Tube Server.")
    fields = (
        "tater_tube_server_url",
        "tater_tube_player_name",
        "tater_tube_player_id",
        "tater_tube_token",
        "server_url",
        "player_name",
        "player_id",
        "token",
    )
    player = _player(client)
    if _provider_id(player.get("provider")) == provider_id:
        _stop_player(client=client)
    if client is not None:
        client.hdel(SETTINGS_KEY, *fields)
        cached = _load_json(client, CATALOG_KEY, {})
        if _provider_id(cached.get("provider")) == provider_id:
            client.delete(CATALOG_KEY)
            with _catalog_memory_cache_lock:
                _catalog_memory_cache.update(
                    {"store": client, "loaded_at": time.monotonic(), "payload": {}}
                )
    _save_hash(client, RUNTIME_KEY, {"status": "disconnected", "last_error": ""})
    return {"ok": True, "message": f"{PROVIDER_LABELS[provider_id]} disconnected locally."}


def _play_recommendation(
    item_id: Any,
    client: Any = None,
    *,
    requested_targets: Any = None,
    volume_percent: Any = None,
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    recommendations_label = _recommendations_label(store)
    recommendation_id = _text(item_id)
    if recommendation_id.startswith("recommendation:"):
        recommendation_id = recommendation_id.split(":", 1)[1]
    published = _recommendations(store)
    cfg = _settings(store)
    provider_id = _provider_id(cfg.get("provider"))
    if _provider_id(published.get("provider"), "") != provider_id:
        raise ValueError(f"Refresh {recommendations_label} for the active music provider first.")
    playlist = next(
        (
            row
            for row in published.get("playlists") or []
            if isinstance(row, dict) and _text(row.get("id")) == recommendation_id
        ),
        None,
    )
    if not isinstance(playlist, dict):
        raise ValueError("That Tater recommendation is no longer available.")
    catalog = _catalog(store, provider_id)
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    tracks = [
        dict(track_by_id[track_id])
        for track_id in playlist.get("track_ids") or []
        if _text(track_id) in track_by_id
    ]
    if not tracks:
        raise ValueError("Those recommended tracks are no longer in the active library. Refresh recommendations.")

    current = _player(store)
    selected_targets = _list(requested_targets) or _list(
        current.get("targets") or current.get("target")
    ) or _list(cfg.get("default_targets") or cfg.get("default_target"))
    targets = _resolve_targets(
        selected_targets,
        client=store,
        provider_id=provider_id,
    )
    if not targets:
        raise ValueError("Choose one or more players in the Music Player before starting this playlist.")
    _validate_catalog_provider_targets(targets)
    volume = _as_int(
        current.get("volume_percent") if volume_percent is None else volume_percent,
        _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
        0,
        100,
    )
    return _create_and_start_queue(
        tracks,
        targets=targets,
        shuffle=False,
        volume_percent=volume,
        client=store,
    )


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    assistant_name = _assistant_first_name(store)
    recommendations_label = _recommendations_label(store)
    action_name = _text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)

    if action_name in {"music_pair_tater_tube", "music_provider_connect"}:
        provider_id = (
            "tater_tube"
            if action_name == "music_pair_tater_tube"
            else _provider_from_card(body)
        )
        return _connect_provider(provider_id, values, store)

    if action_name == "music_provider_activate":
        provider_id = _provider_from_card(body)
        if not _paired(_settings(store), provider_id):
            raise ValueError(f"Connect {PROVIDER_LABELS[provider_id]} first.")
        catalog = _sync_catalog(store, provider_id)
        return {
            "ok": True,
            "message": (
                f"{PROVIDER_LABELS[provider_id]} loaded "
                f"{len(catalog.get('tracks') or [])} tracks."
            ),
        }

    if action_name in {"music_disconnect", "music_provider_disconnect"}:
        provider_id = (
            "tater_tube"
            if action_name == "music_disconnect"
            else _provider_from_card(body)
        )
        return _disconnect_provider(provider_id, store)

    if action_name == "music_sync_now":
        selected_provider = _provider_id(_settings(store).get("provider"))
        catalog = _sync_catalog(store, selected_provider)
        return {"ok": True, "message": f"Music library updated with {len(catalog.get('tracks') or [])} tracks."}

    if action_name == "music_save_settings":
        current_settings = _settings(store)
        allowed = {
            "catalog_sync_interval_seconds",
            "default_targets",
            "default_volume_percent",
            "mixed_sync_default_adjustment_ms",
            "default_shuffle",
            "maximum_queue_tracks",
            "recommendations_enabled",
            "recommendation_interval_hours",
            "recommendation_playlist_count",
            "recommendation_items_per_playlist",
            "prompt_context_enabled",
            "prompt_person_id",
            "prompt_profile_interval_hours",
        }
        updates = {key: values.get(key) for key in allowed if key in values}
        if "default_targets" in updates:
            updates["default_targets"] = json.dumps(
                _normalize_stereo_targets(updates["default_targets"])
            )
        if "prompt_person_id" in updates:
            updates["prompt_person_id"] = _text(updates.get("prompt_person_id"))
            if updates["prompt_person_id"] and not _people_person_name(
                updates["prompt_person_id"], store
            ):
                raise ValueError("Choose an existing Person for Music Prompt Context.")
        person_changed = (
            "prompt_person_id" in updates
            and _text(updates.get("prompt_person_id")) != _text(current_settings.get("prompt_person_id"))
        )
        _save_hash(store, SETTINGS_KEY, updates)
        if person_changed:
            store.delete(PROMPT_PROFILE_KEY)
            store.hdel(
                RUNTIME_KEY,
                "last_profile_finished_at",
                "last_profile_duration_ms",
                "last_profile_error",
            )
        next_settings = {**current_settings, **updates}
        selected_person_id = _text(next_settings.get("prompt_person_id"))
        if (
            person_changed
            and _as_bool(next_settings.get("prompt_context_enabled"), True)
            and selected_person_id
            and _profile_history(
                store,
                person_id=selected_person_id,
                provider_id=next_settings.get("provider"),
            )
        ):
            _schedule_music_prompt_profile_refresh(store)
        return {"ok": True, "message": "Music Core settings saved."}

    if action_name == "music_recommendations_refresh":
        started = _schedule_recommendation_refresh(store)
        return {
            "ok": True,
            "message": (
                f"{assistant_name} is preparing fresh recommendation playlists in the background."
                if started
                else f"{assistant_name} is already refreshing music recommendations."
            ),
        }

    if action_name == "music_recommendation_play":
        player = _play_recommendation(body.get("id"), store)
        return {
            "ok": True,
            "message": f"Playing {_track_label(player.get('current') or {})} from {recommendations_label}.",
        }

    if action_name == "music_ui_play":
        existing = _player(store)
        selected_provider = _provider_id(
            values.get("provider"),
            _provider_id(_settings(store).get("provider")),
        )
        existing_provider = _provider_id(
            existing.get("provider"),
            _provider_id(_settings(store).get("provider")),
        )
        queue = existing.get("queue") if isinstance(existing.get("queue"), list) else []
        if (
            not _text(values.get("query"))
            and queue
            and selected_provider == existing_provider
        ):
            resume_existing = _text(existing.get("status")).lower() == "paused"
            old_targets = _list(existing.get("targets") or existing.get("target"))
            requested_targets = values.get("targets")
            if not _list(requested_targets):
                requested_targets = old_targets
            targets = _resolve_targets(
                requested_targets,
                client=store,
                provider_id=selected_provider,
            )
            if not targets:
                raise ValueError("Choose one or more valid satellites, stereo pairs, or media players.")
            _validate_catalog_provider_targets(targets)
            existing["shuffle"] = _as_bool(values.get("shuffle"), bool(existing.get("shuffle")))
            existing["volume_percent"] = _as_int(
                values.get("volume_percent"),
                _as_int(existing.get("volume_percent"), 75, 0, 100),
                0,
                100,
            )
            _save_player(existing, store)
            _route_player_targets(targets, client=store)
            player = _resume_player(client=store)
            return {
                "ok": True,
                "message": (
                    f"Resumed {_track_label(player.get('current') or {})}."
                    if resume_existing
                    else f"Playing {_track_label(player.get('current') or {})}."
                ),
            }
        requested_targets = values.get("targets")
        if not _list(requested_targets):
            requested_targets = _list(existing.get("targets") or existing.get("target"))
        result = _play_request(
            {
                "provider": selected_provider,
                "query": values.get("query"),
                "targets": requested_targets,
                "shuffle": (
                    values.get("shuffle")
                    if values.get("shuffle") is not None
                    else existing.get("shuffle")
                ),
                "volume_percent": (
                    values.get("volume_percent")
                    if values.get("volume_percent") is not None
                    else existing.get("volume_percent")
                ),
            },
            {},
            store,
        )
        return {"ok": True, "message": _text(result.get("summary_for_user"))}

    if action_name == "music_ui_save_player":
        player = _player(store)
        current_settings = _settings(store)
        selected_provider = "tater_tube"
        old_targets = _list(player.get("targets") or player.get("target"))
        old_player_settings = _selected_player_settings(
            old_targets,
            current_settings,
            default_volume=_as_int(player.get("volume_percent"), 75, 0, 100),
        )
        targets = _resolve_targets(
            values.get("targets") or values.get("target"),
            client=store,
            provider_id=selected_provider,
        )
        if not targets:
            raise ValueError("Choose one or more valid satellites, stereo pairs, or media players.")
        _validate_catalog_provider_targets(targets)
        requested_volume = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        submitted_player_settings = _normalize_player_settings(
            values.get("player_settings"),
            targets=targets,
            cfg=current_settings,
            default_volume=requested_volume,
        )
        if "player_settings" in values:
            _save_player_calibrations(store, submitted_player_settings)
        next_settings = _settings(store)
        if "mixed_sync_adjustment_ms" in values:
            base_mixed_sync_adjustment = _save_mixed_sync_adjustment(
                store,
                targets,
                values.get("mixed_sync_adjustment_ms"),
            )
        else:
            base_mixed_sync_adjustment = _mixed_sync_adjustment(targets, next_settings)
        mixed_sync_adjustment = _mixed_sync_from_player_settings(
            targets,
            submitted_player_settings,
            base_mixed_sync_adjustment,
        )
        was_playing = _text(player.get("status")).lower() == "playing"
        old_mixed_sync_adjustment = _mixed_sync_from_player_settings(
            old_targets,
            old_player_settings,
            _mixed_sync_adjustment(old_targets, current_settings),
        )
        mixed_sync_changed = old_mixed_sync_adjustment != mixed_sync_adjustment
        player_settings_changed = old_player_settings != {
            target: submitted_player_settings[target]
            for target in targets
            if target in submitted_player_settings
        }
        player["provider"] = selected_provider
        player["mixed_sync_adjustment_ms"] = mixed_sync_adjustment
        player["shuffle"] = _as_bool(values.get("shuffle"), bool(player.get("shuffle")))
        player["volume_percent"] = requested_volume
        _save_player(player, store)
        targets_changed = old_targets != targets
        player = _route_player_targets(
            targets,
            force_restart=mixed_sync_changed or player_settings_changed,
            client=store,
        )
        if was_playing and (targets_changed or mixed_sync_changed or player_settings_changed):
            return {
                "ok": True,
                "message": (
                    f"Updated player calibration for {_target_summary(targets)}."
                    if (mixed_sync_changed or player_settings_changed) and not targets_changed
                    else f"Moved music to {_target_summary(targets)}."
                ),
            }
        return {"ok": True, "message": f"Music player set to {_target_summary(targets)}."}

    if action_name == "music_ui_test_sync":
        player = _player(store)
        cfg = _settings(store)
        selected_provider = _provider_id(
            values.get("provider"),
            _provider_id(cfg.get("provider")),
        )
        targets = _resolve_targets(
            values.get("targets") or values.get("target"),
            client=store,
            provider_id=selected_provider,
        )
        if not targets:
            raise ValueError("Choose one or more players before testing sync.")
        _validate_catalog_provider_targets(targets)
        volume = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        player_settings = _normalize_player_settings(
            values.get("player_settings"),
            targets=targets,
            cfg=cfg,
            default_volume=volume,
        )
        active_targets = _list(player.get("targets") or player.get("target"))
        if _text(player.get("status")).lower() == "playing" and active_targets:
            _stop_target(active_targets)
            player["status"] = "stopped"
            player["started_at"] = 0.0
            _save_player(player, store)

        from media_playback import play_media_url_targets

        result = play_media_url_targets(
            targets,
            "",
            audio_bytes=_sync_test_wav(),
            media_type="audio/wav",
            media_content_type="music",
            filename="tater-sync-test.wav",
            text="Tater player sync test",
            volume_percent=volume,
            mixed_sync_adjustment_ms=_mixed_sync_from_player_settings(
                targets,
                player_settings,
                _mixed_sync_adjustment(targets, cfg),
            ),
            target_volume_percent={
                target: setting["volume_percent"]
                for target, setting in player_settings.items()
            },
            target_sync_offset_ms={
                target: setting["sync_offset_ms"]
                for target, setting in player_settings.items()
            },
            target_transport_mode={
                target: _player_transport_mode(setting.get("transport_mode"))
                for target, setting in player_settings.items()
                if target.casefold().startswith(("sonos:", "integration:sonos:"))
            },
            timeout_s=30.0,
            respect_reply_playback=False,
        )
        if not isinstance(result, dict) or result.get("ok") is False:
            raise ValueError(_text((result or {}).get("error")) or "The sync test could not start.")
        return {
            "ok": True,
            "message": "Playing sync clicks. Adjust any player that sounds early or late, then save.",
        }

    if action_name == "music_ui_set_volume":
        player = _player(store)
        volume = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        live_result = {"sent_count": 0, "warnings": []}
        if _text(player.get("status")).lower() == "playing":
            live_result = _set_target_volume(player, volume)
            if _as_int(live_result.get("sent_count"), 0, 0, 10000) <= 0:
                warning = "; ".join(
                    _text(value)
                    for value in list(live_result.get("warnings") or [])
                    if _text(value)
                )
                raise ValueError(warning or "The active players could not change volume.")
        player["volume_percent"] = volume
        calibrated_targets = _list(player.get("targets") or player.get("target"))
        if calibrated_targets:
            cfg = _settings(store)
            _save_player_calibrations(
                store,
                {
                    target: {
                        **_target_calibration(target, cfg, default_volume=volume),
                        "volume_percent": volume,
                    }
                    for target in calibrated_targets
                },
            )
        warnings = [
            _text(value)
            for value in list(live_result.get("warnings") or [])
            if _text(value)
        ]
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store)
        return {
            "ok": True,
            "message": (
                f"Music volume set to {volume}%. " + " ".join(warnings)
                if warnings
                else f"Music volume set to {volume}%."
            ),
        }

    if action_name == "music_ui_seek":
        player = _seek_player(
            _as_float(values.get("position_seconds")),
            client=store,
        )
        position = _player_position_seconds(player)
        return {"ok": True, "message": f"Moved to {round(position)} seconds."}

    if action_name == "music_ui_seek_relative":
        current = _player(store)
        delta = _as_float(values.get("delta_seconds"))
        player = _seek_player(
            _player_position_seconds(current) + delta,
            client=store,
        )
        position = _player_position_seconds(player)
        return {"ok": True, "message": f"Moved to {round(position)} seconds."}

    if action_name == "music_ui_set_shuffle":
        player = _set_player_shuffle(
            _as_bool(values.get("shuffle"), False),
            client=store,
        )
        return {
            "ok": True,
            "message": "Shuffle is on." if player.get("shuffle") else "Shuffle is off.",
        }

    if action_name == "music_ui_stop":
        _stop_player(client=store)
        return {"ok": True, "message": "Music stopped."}

    if action_name == "music_ui_pause":
        player = _pause_player(client=store)
        return {
            "ok": True,
            "message": f"Paused {_track_label(player.get('current') or {})}.",
        }

    if action_name == "music_ui_next":
        player = _advance_player(1, client=store)
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_previous":
        player = _advance_player(-1, client=store)
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_queue_play":
        item_id = _text(body.get("id"))
        try:
            index = int(item_id.split(":", 1)[1])
        except Exception as exc:
            raise ValueError("Queue position is invalid.") from exc
        player = _start_player_index(index, client=store)
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_facet_play":
        item_id = _text(body.get("id"))
        facet, separator, value = item_id.partition(":")
        if not separator or not value or facet not in {"genre", "artist", "album"}:
            raise ValueError("Music category is invalid.")
        current_player = _player(store)
        current_settings = _settings(store)
        player_result = _play_request(
            {
                "provider": _provider_id(current_settings.get("provider")),
                facet: value,
                "targets": (
                    _list(current_player.get("targets") or current_player.get("target"))
                    or _list(
                        current_settings.get("default_targets")
                        or current_settings.get("default_target")
                    )
                ),
                "shuffle": facet != "album",
                "volume_percent": current_player.get("volume_percent"),
            },
            {},
            store,
        )
        return {"ok": True, "message": _text(player_result.get("summary_for_user"))}

    raise ValueError(f"Unknown Music Core action: {action_name}")


def _fetch_track_artwork(track: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    provider_id = _provider_id(track.get("provider"))
    provider = _provider(client, provider_id)
    artwork_url_fn = getattr(provider, "artwork_url", None)
    source_url = artwork_url_fn(track) if callable(artwork_url_fn) else ""
    source_url = _text(source_url)
    if not source_url:
        raise KeyError("This provider does not have artwork for the track.")

    cache_key = hashlib.sha256(source_url.encode("utf-8")).hexdigest()
    with _artwork_cache_lock:
        cached = _artwork_cache.get(cache_key)
        if isinstance(cached, dict) and cached.get("body"):
            return dict(cached)
        if _artwork_failure_until.get(cache_key, 0.0) > time.monotonic():
            raise RuntimeError("Provider artwork is temporarily unavailable.")
        inflight = _artwork_inflight.get(cache_key)
        fetch_owner = inflight is None
        if fetch_owner:
            inflight = threading.Event()
            _artwork_inflight[cache_key] = inflight

    if not fetch_owner:
        if not inflight.wait(ARTWORK_INFLIGHT_WAIT_TIMEOUT_SECONDS):
            raise TimeoutError("Timed out waiting for provider artwork.")
        with _artwork_cache_lock:
            cached = _artwork_cache.get(cache_key)
            if isinstance(cached, dict) and cached.get("body"):
                return dict(cached)
        raise RuntimeError("Provider artwork is temporarily unavailable.")

    slot_acquired = False
    try:
        slot_acquired = _artwork_fetch_slots.acquire(
            timeout=ARTWORK_INFLIGHT_WAIT_TIMEOUT_SECONDS,
        )
        if not slot_acquired:
            raise TimeoutError("Provider artwork queue is busy.")
        response = requests.get(
            source_url,
            headers={"Accept": "image/jpeg,image/png,image/webp,image/*"},
            timeout=(ARTWORK_CONNECT_TIMEOUT_SECONDS, ARTWORK_READ_TIMEOUT_SECONDS),
        )
        response.raise_for_status()
        body = bytes(response.content or b"")
        content_type = _text(response.headers.get("Content-Type")).split(";", 1)[0].lower()
        if not content_type.startswith("image/"):
            raise ValueError("The music provider did not return an image.")
        if not body or len(body) > 12 * 1024 * 1024:
            raise ValueError("The provider artwork is empty or too large.")

        cached = {"body": body, "content_type": content_type}
        with _artwork_cache_lock:
            if len(_artwork_cache) >= 256:
                _artwork_cache.clear()
            _artwork_cache[cache_key] = cached
            _artwork_failure_until.pop(cache_key, None)
        return dict(cached)
    except Exception as exc:
        with _artwork_cache_lock:
            _artwork_failure_until[cache_key] = (
                time.monotonic() + ARTWORK_FAILURE_CACHE_SECONDS
            )
        logger.warning(
            "[Music] artwork fetch failed track=%s error=%s",
            _text(track.get("id")) or "-",
            _text(exc) or type(exc).__name__,
        )
        raise
    finally:
        if slot_acquired:
            _artwork_fetch_slots.release()
        with _artwork_cache_lock:
            completed = _artwork_inflight.pop(cache_key, None)
        if completed is not None:
            completed.set()


def _fallback_track_artwork(track: Dict[str, Any]) -> Dict[str, Any]:
    label = _text(track.get("album") or track.get("artist") or track.get("title") or "Music")
    digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
    color_a = f"#{digest[:6]}"
    color_b = f"#{digest[6:12]}"
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="360" height="360" viewBox="0 0 360 360">
<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="{color_a}"/><stop offset="1" stop-color="{color_b}"/></linearGradient></defs>
<rect width="360" height="360" rx="28" fill="#15111f"/><circle cx="180" cy="165" r="118" fill="url(#g)" opacity=".92"/>
<circle cx="180" cy="165" r="48" fill="#15111f"/><circle cx="180" cy="165" r="13" fill="#ffbd59"/>
<path d="M254 77v142c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V96l-88 19v124c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V95z" fill="#fff" opacity=".9"/>
</svg>"""
    return {"body": svg.encode("utf-8"), "content_type": "image/svg+xml"}


def handle_core_webhook(
    *,
    webhook: str,
    query: Optional[Dict[str, Any]] = None,
    redis_client=None,
    **_kwargs,
) -> Any:
    if _text(webhook).lower() != "artwork":
        raise KeyError(f"Unsupported Music Core webhook: {webhook}")
    params = query if isinstance(query, dict) else {}
    provider_id = _provider_id(
        params.get("provider"),
        _provider_id(_settings(redis_client).get("provider")),
    )
    track = _client_track(params.get("track_id"), provider_id, redis_client)
    fallback = False
    try:
        artwork = _fetch_track_artwork(track, redis_client)
    except Exception:
        artwork = _fallback_track_artwork(track)
        fallback = True
    from starlette.responses import Response

    headers = {
        "Cache-Control": "private, max-age=30" if fallback else "private, max-age=86400"
    }
    if fallback:
        headers["X-Tater-Artwork-Fallback"] = "1"
    return Response(
        content=artwork["body"],
        media_type=artwork["content_type"],
        headers=headers,
    )


def get_core_system_tasks(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    assistant_name = _assistant_first_name(store)
    recommendations_label = _recommendations_label(store)
    cfg = _settings(store)
    runtime = _runtime(store)
    provider_id = _provider_id(cfg.get("provider"))
    connected = _paired(cfg, provider_id)
    catalog_interval = _as_int(
        cfg.get("catalog_sync_interval_seconds"),
        DEFAULT_SYNC_INTERVAL_SECONDS,
        60,
        86400,
    )
    recommendation_interval = (
        _as_int(cfg.get("recommendation_interval_hours"), 12, 1, 168) * 3600
    )
    profile_interval = (
        _as_int(cfg.get("prompt_profile_interval_hours"), 12, 1, 168) * 3600
    )
    recommendations_enabled = _as_bool(cfg.get("recommendations_enabled"), True)
    prompt_context_enabled = _as_bool(cfg.get("prompt_context_enabled"), True)
    prompt_person_id = _text(cfg.get("prompt_person_id"))
    prompt_person_name = _people_person_name(prompt_person_id, store) if prompt_person_id else ""
    has_history = any(
        _provider_id(row.get("provider")) == provider_id
        for row in _listening_history(store)
    )
    last_sync = max(
        _as_float(runtime.get("last_sync_finished_at")),
        _as_float(runtime.get("last_sync_at")),
    )
    last_recommendation = max(
        _as_float(runtime.get("last_recommendation_finished_at")),
        _as_float(runtime.get("last_recommendation_at")),
        _as_float(runtime.get("last_recommendation_attempt_at")),
    )
    last_continuation = max(
        _as_float(runtime.get("last_continuation_finished_at")),
        _as_float(runtime.get("last_continuation_at")),
    )
    last_profile = _as_float(runtime.get("last_profile_finished_at"))
    recommendation_running = bool(
        _recommendation_lock.locked()
        or (_recommendation_thread is not None and _recommendation_thread.is_alive())
    )
    continuation_running = bool(
        _continuation_lock.locked()
        or (_continuation_thread is not None and _continuation_thread.is_alive())
    )
    profile_running = bool(
        _profile_lock.locked()
        or (_profile_thread is not None and _profile_thread.is_alive())
    )
    has_profile_history = bool(
        prompt_person_id
        and _profile_history(
            store,
            person_id=prompt_person_id,
            provider_id=provider_id,
        )
    )
    profile_available = bool(
        connected
        and prompt_context_enabled
        and prompt_person_id
        and prompt_person_name
        and has_profile_history
    )
    return {
        "label": "Music Core",
        "order": 35,
        "tasks": [
            {
                "id": "catalog_sync",
                "label": "Music Library Sync",
                "description": "Refreshes artists, albums, genres, tracks, and provider artwork.",
                "interval_seconds": catalog_interval,
                "running": _catalog_sync_lock.locked(),
                "started_at": _catalog_sync_started_at,
                "finished_at": last_sync,
                "duration_ms": _as_float(runtime.get("last_sync_duration_ms")),
                "next_run_at": last_sync + catalog_interval if last_sync else 0.0,
                "last_error": _text(runtime.get("last_sync_error")),
                "run_count": _as_int(runtime.get("sync_run_count"), 0, 0, 1_000_000_000),
                "available": connected,
                "unavailable_reason": f"Connect {PROVIDER_LABELS.get(provider_id, provider_id)} before syncing music.",
                "status": "idle" if connected else "waiting",
                "requires_running": True,
                "order": 10,
            },
            {
                "id": "recommendation_refresh",
                "label": recommendations_label,
                "description": "Builds fresh AI-named playlists from listening history.",
                "interval_seconds": recommendation_interval,
                "enabled": recommendations_enabled,
                "running": recommendation_running,
                "started_at": _recommendation_started_at,
                "finished_at": last_recommendation,
                "duration_ms": _as_float(runtime.get("last_recommendation_duration_ms")),
                "next_run_at": (
                    last_recommendation + recommendation_interval
                    if last_recommendation and recommendations_enabled
                    else 0.0
                ),
                "last_error": _text(runtime.get("last_recommendation_error")),
                "run_count": _as_int(
                    runtime.get("recommendation_run_count"),
                    0,
                    0,
                    1_000_000_000,
                ),
                "available": connected and has_history,
                "unavailable_reason": (
                    f"Connect {PROVIDER_LABELS.get(provider_id, provider_id)} before refreshing recommendations."
                    if not connected
                    else f"Play some music first so {assistant_name} has listening history to use."
                ),
                "status": "idle" if connected and has_history else "waiting",
                "requires_running": True,
                "order": 20,
            },
            {
                "id": "music_profile_refresh",
                "label": "Music Prompt Profile",
                "description": "Builds compact favorite genre, favorite artist, and recent-track context for the selected Person.",
                "interval_seconds": profile_interval,
                "enabled": prompt_context_enabled,
                "running": profile_running,
                "started_at": _profile_started_at,
                "finished_at": last_profile,
                "duration_ms": _as_float(runtime.get("last_profile_duration_ms")),
                "next_run_at": (
                    last_profile + profile_interval
                    if last_profile and prompt_context_enabled
                    else 0.0
                ),
                "last_error": _text(runtime.get("last_profile_error")),
                "run_count": _as_int(
                    runtime.get("profile_run_count"),
                    0,
                    0,
                    1_000_000_000,
                ),
                "available": profile_available,
                "unavailable_reason": (
                    f"Connect {PROVIDER_LABELS.get(provider_id, provider_id)} before building a music profile."
                    if not connected
                    else "Turn on Music Prompt Context in Music Core Settings."
                    if not prompt_context_enabled
                    else "Choose an existing Person in Music Core Settings."
                    if not prompt_person_id or not prompt_person_name
                    else f"Play some music for {prompt_person_name} first."
                ),
                "status": "idle" if profile_available else "waiting",
                "requires_running": True,
                "order": 30,
            },
            {
                "id": "continuous_radio_refill",
                "label": "Continuous-Radio Refill",
                "description": "Automatically extends an active queue when playback nears its final tracks.",
                "interval_seconds": 0,
                "enabled": True,
                "manual": False,
                "schedule_label": "Event driven",
                "next_run_label": "Near queue end",
                "running": continuation_running,
                "started_at": _continuation_started_at,
                "finished_at": last_continuation,
                "duration_ms": _as_float(runtime.get("last_continuation_duration_ms")),
                "last_error": _text(runtime.get("last_continuation_error")),
                "run_count": _as_int(
                    runtime.get("continuation_run_count"),
                    0,
                    0,
                    1_000_000_000,
                ),
                "available": connected,
                "unavailable_reason": f"Connect {PROVIDER_LABELS.get(provider_id, provider_id)} before starting continuous radio.",
                "status": "idle" if connected else "waiting",
                "requires_running": True,
                "order": 40,
            },
        ],
    }


def run_core_system_task(*, task_id: str, redis_client=None, **_kwargs) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    task = _text(task_id).lower()
    provider_id = _provider_id(_settings(store).get("provider"))
    if task == "catalog_sync":
        catalog = _sync_catalog(store, provider_id)
        return {"ok": True, "track_count": len(catalog.get("tracks") or [])}
    if task == "recommendation_refresh":
        recommendations = _generate_recommendations(store)
        return {
            "ok": True,
            "playlist_count": len(recommendations.get("playlists") or []),
        }
    if task == "music_profile_refresh":
        profile = _generate_music_prompt_profile(store)
        return {
            "ok": True,
            "person_id": _text(profile.get("person_id")),
            "history_event_count": _as_int(profile.get("history_event_count"), 0, 0, 1_000_000_000),
        }
    raise KeyError(f"Unknown Music Core task: {task_id}")


def run(stop_event: Optional[object] = None) -> None:
    logger.info("[Music] Core starting.")
    try:
        while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
            cfg = _settings()
            active_provider = _provider_id(cfg.get("provider"))
            if not _paired(cfg, active_provider):
                _save_hash(redis_client, RUNTIME_KEY, {"status": "waiting_for_pairing"})
                time.sleep(1.0)
                continue
            runtime = _runtime()
            now = time.time()
            interval = _as_int(
                cfg.get("catalog_sync_interval_seconds"),
                DEFAULT_SYNC_INTERVAL_SECONDS,
                60,
                86400,
            )
            try:
                if (
                    active_provider in CATALOG_PROVIDER_IDS
                    and (
                        _catalog_needs_artwork_refresh(provider_id=active_provider)
                        or now - _as_float(runtime.get("last_sync_at")) >= interval
                    )
                ):
                    _sync_catalog(provider_id=active_provider)
                    runtime = _runtime()
                _advance_finished_player()
                _schedule_continuation_refresh()
                recommendation_interval = (
                    _as_int(cfg.get("recommendation_interval_hours"), 12, 1, 168) * 3600
                )
                published = _recommendations()
                last_recommendation_cycle = max(
                    _as_float(runtime.get("last_recommendation_at")),
                    _as_float(runtime.get("last_recommendation_attempt_at")),
                )
                if _provider_id(published.get("provider"), "") != active_provider:
                    last_recommendation_cycle = 0.0
                has_history = any(
                    _provider_id(row.get("provider")) == active_provider
                    for row in _listening_history()
                )
                if (
                    _as_bool(cfg.get("recommendations_enabled"), True)
                    and active_provider in CATALOG_PROVIDER_IDS
                    and has_history
                    and now - last_recommendation_cycle >= recommendation_interval
                ):
                    _schedule_recommendation_refresh()
                profile_interval = (
                    _as_int(cfg.get("prompt_profile_interval_hours"), 12, 1, 168) * 3600
                )
                prompt_person_id = _text(cfg.get("prompt_person_id"))
                prompt_profile = _music_prompt_profile()
                last_profile_cycle = max(
                    _as_float(runtime.get("last_profile_finished_at")),
                    _as_float(prompt_profile.get("generated_at")),
                )
                if (
                    _text(prompt_profile.get("person_id")) != prompt_person_id
                    or _provider_id(prompt_profile.get("provider")) != active_provider
                ):
                    last_profile_cycle = 0.0
                if (
                    _as_bool(cfg.get("prompt_context_enabled"), True)
                    and prompt_person_id
                    and _people_person_name(prompt_person_id)
                    and _profile_history(
                        None,
                        person_id=prompt_person_id,
                        provider_id=active_provider,
                    )
                    and now - last_profile_cycle >= profile_interval
                ):
                    _schedule_music_prompt_profile_refresh()
            except PermissionError as exc:
                logger.warning("[Music] provider authorization was revoked: %s", exc)
                redis_client.hdel(SETTINGS_KEY, "tater_tube_token", "token")
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {
                        "status": "authorization_revoked",
                        "last_error": _text(exc)[:500],
                        "last_error_at": now,
                    },
                )
            except Exception as exc:
                logger.warning("[Music] background cycle failed: %s", exc)
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {
                        "status": "error",
                        "last_error": _text(exc)[:500],
                        "last_error_at": now,
                    },
                )
            time.sleep(1.0)
    finally:
        logger.info("[Music] Core stopped.")
