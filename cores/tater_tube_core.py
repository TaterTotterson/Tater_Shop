"""Connect Tater to Tater Tube Server for viewing context and recommendations."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

from helpers import extract_json, get_llm_client_from_env, redis_client

try:
    from helpers import get_primary_llm_client_from_env as _get_primary_llm_client_from_env
except Exception:  # pragma: no cover - compatibility with older Tater runtimes.
    _get_primary_llm_client_from_env = get_llm_client_from_env


__version__ = "1.2.0"
MIN_TATER_VERSION = "59"
CORE_DESCRIPTION = (
    "Connect Tater to Tater Tube Server, inject recent viewing context into prompts, "
    "publish AI-selected movie and series recommendations, and voice Tater's Picks "
    "with the user's TTS settings."
)
TAGS = ["tater-tube", "media", "recommendations", "context", "tts"]

logger = logging.getLogger("tater_tube_core")
logger.setLevel(logging.INFO)

CORE_SETTINGS = {
    "category": "Tater Tube Core Settings",
    "hydra_tools_require_running": False,
    "required": {
        "profile_id": {
            "label": "Viewing Profile",
            "type": "text",
            "default": "household",
            "description": "Server profile used for viewing context and recommendations.",
        },
        "poll_interval_seconds": {
            "label": "Context Sync Interval (sec)",
            "type": "number",
            "default": 300,
            "description": "How often Core refreshes viewing context from Tater Tube Server.",
        },
        "recommendation_interval_hours": {
            "label": "Recommendation Interval (hours)",
            "type": "number",
            "default": 6,
            "description": "How often Tater prepares a fresh recommendation batch.",
        },
        "recommendation_count": {
            "label": "Recommendation Count",
            "type": "number",
            "default": 8,
            "description": "Number of Tater's Picks to publish per batch.",
        },
        "recommendation_expiry_hours": {
            "label": "Picks Stay Fresh (hours)",
            "type": "number",
            "default": 24,
            "description": "How long a recommendation batch remains visible on players.",
        },
        "candidate_limit": {
            "label": "Catalog Candidate Limit",
            "type": "number",
            "default": 200,
            "description": "Maximum launchable catalog items sent to the recommendation model.",
        },
        "prompt_context_enabled": {
            "label": "Prompt Context Enabled",
            "type": "checkbox",
            "default": True,
            "description": "Let Tater use compact recent viewing context when it is relevant.",
        },
        "prompt_context_max_chars": {
            "label": "Prompt Context Max Characters",
            "type": "number",
            "default": 2400,
            "description": "Maximum viewing-context text added to Hydra prompts.",
        },
        "tts_enabled": {
            "label": "Tater's Picks Welcome",
            "type": "checkbox",
            "default": True,
            "description": "Play one personalized spoken briefing when Tater's Picks opens.",
        },
    },
    "tags": TAGS,
}

CORE_WEBUI_TAB = {
    "label": "Tater Tube",
    "order": 37,
    "requires_running": False,
}

SETTINGS_KEY = "tater_tube_core_settings"
RUNTIME_KEY = "tater_tube_core_runtime"
CONTEXT_KEY = "tater_tube_core_context"
RECOMMENDATIONS_KEY = "tater_tube_core_recommendations"
DEFAULT_PROFILE_ID = "household"
REQUEST_TIMEOUT_SECONDS = 25
TTS_MAX_TEXT_CHARS = 800


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace").strip()
    return str(value).strip()


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        result = int(float(value))
    except Exception:
        result = int(default)
    return max(minimum, min(maximum, result))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _decode_hash(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    return {_text(key): _text(value) for key, value in raw.items() if _text(key)}


def _settings(client: Any = None) -> Dict[str, str]:
    redis_obj = client or globals().get("redis_client")
    if redis_obj is None:
        return {}
    try:
        return _decode_hash(redis_obj.hgetall(SETTINGS_KEY) or {})
    except Exception:
        return {}


def _runtime(client: Any = None) -> Dict[str, str]:
    redis_obj = client or globals().get("redis_client")
    if redis_obj is None:
        return {}
    try:
        return _decode_hash(redis_obj.hgetall(RUNTIME_KEY) or {})
    except Exception:
        return {}


def _save_hash(client: Any, key: str, values: Dict[str, Any]) -> None:
    if client is None:
        return
    cleaned = {str(k): str(v) for k, v in values.items() if v is not None}
    if cleaned:
        client.hset(key, mapping=cleaned)


def _load_json(client: Any, key: str, default: Any) -> Any:
    if client is None:
        return default
    try:
        raw = client.get(key)
        if raw in (None, ""):
            return default
        parsed = json.loads(_text(raw))
        return parsed
    except Exception:
        return default


def _save_json(client: Any, key: str, value: Any) -> None:
    if client is None:
        return
    client.set(key, json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _normalize_server_url(value: Any) -> str:
    raw = _text(value).rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Server URL must begin with http:// or https://")
    if raw.endswith("/api"):
        return raw[:-4]
    return raw


def _api_url(server_url: str, path: str) -> str:
    return f"{server_url.rstrip('/')}/api/{path.lstrip('/')}"


def _api_request(
    method: str,
    path: str,
    *,
    settings: Optional[Dict[str, str]] = None,
    server_url: str = "",
    token: str = "",
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> Any:
    cfg = settings if isinstance(settings, dict) else {}
    base = _normalize_server_url(server_url or cfg.get("server_url"))
    auth_token = _text(token or cfg.get("token"))
    if not base:
        raise ValueError("Tater Tube Server URL is not configured.")
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    response = requests.request(
        method.upper(),
        _api_url(base, path),
        headers=headers,
        json=payload,
        timeout=max(5, int(timeout)),
    )
    try:
        body = response.json()
    except Exception:
        body = {}
    if not response.ok:
        error = body.get("error") if isinstance(body, dict) else {}
        message = error.get("message") if isinstance(error, dict) else ""
        error_message = _text(message) or f"Tater Tube Server returned HTTP {response.status_code}."
        if response.status_code == 401:
            raise PermissionError(error_message)
        raise RuntimeError(error_message)
    if isinstance(body, dict) and body.get("success") is False:
        error = body.get("error")
        message = error.get("message") if isinstance(error, dict) else error
        raise RuntimeError(_text(message) or "Tater Tube Server rejected the request.")
    if isinstance(body, dict) and "data" in body:
        return body.get("data")
    return body


async def _synthesize_tts_wav(text: str) -> bytes:
    from speech_settings import get_speech_settings
    from speech_tts import synthesize_preview_wav

    settings = get_speech_settings() or {}
    return await synthesize_preview_wav(
        text=_text(text)[:TTS_MAX_TEXT_CHARS],
        backend=_text(settings.get("tts_backend")),
        model=_text(settings.get("tts_model")),
        voice=_text(settings.get("tts_voice")),
        kokoro_output_gain=settings.get("kokoro_output_gain"),
        pocket_tts_output_gain=settings.get("pocket_tts_output_gain"),
        acceleration=_text(settings.get("acceleration")),
        wyoming_host=_text(settings.get("wyoming_tts_host")),
        wyoming_port=settings.get("wyoming_tts_port"),
        wyoming_voice=_text(settings.get("wyoming_tts_voice")),
        openai_base_url=_text(settings.get("openai_tts_base_url")),
        openai_api_key=_text(settings.get("openai_tts_api_key")),
        chatterbox_base_url=_text(settings.get("chatterbox_tts_base_url")),
        chatterbox_voice_mode=_text(settings.get("chatterbox_tts_voice_mode")),
        chatterbox_chunk_size=settings.get("chatterbox_tts_chunk_size"),
        chatterbox_temperature=settings.get("chatterbox_tts_temperature"),
        chatterbox_exaggeration=settings.get("chatterbox_tts_exaggeration"),
        chatterbox_cfg_weight=settings.get("chatterbox_tts_cfg_weight"),
        chatterbox_seed=settings.get("chatterbox_tts_seed"),
        chatterbox_speed_factor=settings.get("chatterbox_tts_speed_factor"),
        chatterbox_language=_text(settings.get("chatterbox_tts_language")),
    )


def _process_tts_request(loop: asyncio.AbstractEventLoop, client: Any = None) -> int:
    redis_obj = client or globals().get("redis_client")
    cfg = _settings(redis_obj)
    if not _as_bool(cfg.get("tts_enabled"), True):
        return 0
    claimed = _api_request(
        "GET",
        "tater/core/tts/requests?limit=1",
        settings=cfg,
        timeout=10,
    )
    rows = claimed.get("requests") if isinstance(claimed, dict) else []
    if not isinstance(rows, list) or not rows:
        return 0

    processed = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        request_id = _text(row.get("id"))
        text = _text(row.get("text"))
        if not request_id:
            continue
        payload: Dict[str, Any]
        try:
            if not text:
                raise ValueError("TTS request text is empty.")
            wav_bytes = loop.run_until_complete(_synthesize_tts_wav(text))
            if not wav_bytes:
                raise RuntimeError("Tater TTS produced no audio.")
            payload = {
                "audio_base64": base64.b64encode(bytes(wav_bytes)).decode("ascii"),
                "content_type": "audio/wav",
            }
        except Exception as exc:
            logger.warning("[Tater Tube] TTS request %s failed: %s", request_id, exc)
            payload = {"error": _text(exc)[:500] or "Tater TTS failed."}

        _api_request(
            "POST",
            f"tater/core/tts/requests/{request_id}/complete",
            settings=cfg,
            payload=payload,
            timeout=60,
        )
        processed += 1
        _save_hash(
            redis_obj,
            RUNTIME_KEY,
            {
                "last_tts_at": time.time(),
                "last_tts_request_id": request_id,
                "last_tts_error": _text(payload.get("error")),
            },
        )
    return processed


def _profile_id(settings: Dict[str, str]) -> str:
    value = _text(settings.get("profile_id")).lower()
    return value or DEFAULT_PROFILE_ID


def _paired(settings: Dict[str, str]) -> bool:
    return bool(_text(settings.get("server_url")) and _text(settings.get("token")))


def _format_time(timestamp: float) -> str:
    if timestamp <= 0:
        return "never"
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def _sync_context(client: Any = None) -> Dict[str, Any]:
    redis_obj = client or globals().get("redis_client")
    cfg = _settings(redis_obj)
    if not _paired(cfg):
        raise ValueError("Pair Tater Tube Core before syncing viewing context.")
    profile = _profile_id(cfg)
    data = _api_request(
        "GET",
        f"tater/core/context?profile_id={profile}&limit=50",
        settings=cfg,
    )
    context = data if isinstance(data, dict) else {}
    now = time.time()
    context["synced_at"] = now
    _save_json(redis_obj, CONTEXT_KEY, context)
    _save_hash(
        redis_obj,
        RUNTIME_KEY,
        {
            "status": "connected",
            "last_sync_at": now,
            "last_error": "",
            "event_count": len(context.get("events") or []),
        },
    )
    return context


def _llm_json(loop: asyncio.AbstractEventLoop, llm_client: Any, system_prompt: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if llm_client is None:
        raise RuntimeError("No primary LLM is configured for Tater.")
    response = loop.run_until_complete(
        llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            max_tokens=1800,
            temperature=0.35,
        )
    )
    raw = _text(((response or {}).get("message") or {}).get("content"))
    blob = extract_json(raw) or raw
    try:
        parsed = json.loads(blob)
    except Exception as exc:
        raise RuntimeError("The recommendation model did not return valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("The recommendation model returned an unsupported response.")
    return parsed


def _generate_recommendations(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    client: Any = None,
) -> Dict[str, Any]:
    redis_obj = client or globals().get("redis_client")
    cfg = _settings(redis_obj)
    if not _paired(cfg):
        raise ValueError("Pair Tater Tube Core before generating recommendations.")
    context = _load_json(redis_obj, CONTEXT_KEY, {})
    if not isinstance(context, dict) or not isinstance(context.get("events"), list):
        context = _sync_context(redis_obj)
    events = context.get("events") if isinstance(context.get("events"), list) else []
    if not events:
        raise ValueError("Tater needs at least one viewing event before making picks.")

    candidate_limit = _as_int(cfg.get("candidate_limit"), 200, 20, 500)
    candidate_data = _api_request(
        "GET",
        f"tater/core/candidates?limit={candidate_limit}&profile_id={_profile_id(cfg)}",
        settings=cfg,
    )
    candidates = candidate_data.get("candidates") if isinstance(candidate_data, dict) else []
    candidates = [row for row in candidates if isinstance(row, dict) and _text(row.get("id"))]
    if not candidates:
        raise ValueError("Tater Tube Server has no launchable recommendation candidates.")

    count = _as_int(cfg.get("recommendation_count"), 8, 1, 12)
    compact_events = []
    source_counts: Dict[str, int] = {}
    media_type_counts: Dict[str, int] = {}
    for event in events[:40]:
        if not isinstance(event, dict):
            continue
        source = _text(event.get("source")) or "unknown"
        media_type = _text(event.get("media_type")) or "video"
        source_counts[source] = source_counts.get(source, 0) + 1
        media_type_counts[media_type] = media_type_counts.get(media_type, 0) + 1
        compact_events.append(
            {
                "title": _text(event.get("title")),
                "series_title": _text(event.get("series_title")),
                "media_type": media_type,
                "source": source,
                "state": _text(event.get("state")),
                "progress": (
                    round(
                        100
                        * _as_float(event.get("position_ms"))
                        / max(1.0, _as_float(event.get("duration_ms"), 1.0))
                    )
                    if _as_float(event.get("duration_ms")) > 0
                    else None
                ),
            }
        )
    compact_candidates = []
    for candidate in candidates:
        compact_candidates.append(
            {
                "id": _text(candidate.get("id")),
                "title": _text(candidate.get("title")),
                "media_type": _text(candidate.get("media_type")),
                "source": _text(candidate.get("source")),
                "year": _text(candidate.get("year")),
                "description": _text(candidate.get("description"))[:300],
            }
        )

    result = _llm_json(
        loop,
        llm_client,
        (
            "You are Tater, a warm personal media curator inside a retro VCR media player. "
            "Viewing history can span local movies and series, live over-the-air channels, "
            "public-access video, music, and other player modules. Use all of it as taste context. "
            "Choose only from the supplied catalog candidates. Base choices on viewing history without "
            "overstating what the household likes. Prefer a useful mix of sources and media types when "
            "the candidates support it, avoid recently completed titles, and keep each reason to one "
            "friendly sentence. The summary is spoken once when the recommendations page opens. Write it "
            "as a natural two-sentence welcome that mentions a viewing pattern and briefly describes the "
            "mix you selected without reading every title. Do not include a time-of-day greeting because "
            "the player adds the correct greeting. Return JSON only in this exact shape: "
            '{"summary":"two short spoken sentences","items":[{"candidate_id":"exact id","reason":"one sentence"}]}. '
            f"Return up to {count} unique items."
        ),
        {
            "profile_id": _profile_id(cfg),
            "viewing_patterns": {
                "events_by_source": source_counts,
                "events_by_media_type": media_type_counts,
            },
            "recent_viewing": compact_events,
            "catalog_candidates": compact_candidates,
        },
    )
    candidate_by_id = {_text(candidate.get("id")): candidate for candidate in candidates}
    allowed = set(candidate_by_id)
    selections: List[Dict[str, str]] = []
    seen = set()
    for row in result.get("items") or []:
        if not isinstance(row, dict):
            continue
        candidate_id = _text(row.get("candidate_id"))
        if candidate_id not in allowed or candidate_id in seen or len(selections) >= count:
            continue
        seen.add(candidate_id)
        candidate = candidate_by_id[candidate_id]
        selections.append(
            {
                "candidate_id": candidate_id,
                "title": _text(candidate.get("title")),
                "media_type": _text(candidate.get("media_type")),
                "reason": _text(row.get("reason"))[:240] or "Tater thinks this belongs on your screen.",
            }
        )
    if not selections:
        raise RuntimeError("The recommendation model did not select any valid catalog items.")

    briefing = _text(result.get("summary"))[:500]
    if not briefing:
        briefing = (
            "I've looked across what has been playing lately and put together a fresh mix. "
            "There should be a little something here for whatever kind of screen time you want next."
        )
    expires = _as_int(cfg.get("recommendation_expiry_hours"), 24, 1, 168)
    published = _api_request(
        "POST",
        "tater/core/recommendations",
        settings=cfg,
        payload={
            "profile_id": _profile_id(cfg),
            "summary": briefing,
            "expires_in_hours": expires,
            "items": selections,
        },
    )
    now = time.time()
    cache = {
        "generated_at": now,
        "summary": briefing,
        "items": selections,
        "server_response": published if isinstance(published, dict) else {},
    }
    _save_json(redis_obj, RECOMMENDATIONS_KEY, cache)
    _save_hash(
        redis_obj,
        RUNTIME_KEY,
        {
            "status": "connected",
            "last_recommendation_at": now,
            "last_error": "",
            "recommendation_count": len(selections),
        },
    )
    return cache


def _prompt_context(client: Any = None) -> str:
    redis_obj = client or globals().get("redis_client")
    cfg = _settings(redis_obj)
    if not _as_bool(cfg.get("prompt_context_enabled"), True):
        return ""
    context = _load_json(redis_obj, CONTEXT_KEY, {})
    events = context.get("events") if isinstance(context, dict) else []
    if not isinstance(events, list) or not events:
        return ""
    lines = [
        "Private Tater Tube viewing context for the household. Use only when relevant; "
        "do not claim a preference from one watch and do not mention background tracking."
    ]
    seen = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        title = _text(event.get("title"))
        if not title:
            continue
        media_id = _text(event.get("media_id")) or title.lower()
        state = _text(event.get("state"))
        identity = (media_id, state)
        if identity in seen:
            continue
        seen.add(identity)
        series = _text(event.get("series_title"))
        media_type = _text(event.get("media_type")) or "video"
        source = _text(event.get("source")) or "unknown source"
        progress = ""
        duration = _as_float(event.get("duration_ms"))
        if duration > 0:
            percent = round(100 * _as_float(event.get("position_ms")) / duration)
            progress = f", {max(0, min(100, percent))}%"
        label = f"{title}"
        if series and series.lower() != title.lower():
            label += f" ({series})"
        lines.append(f"- {label} — {media_type}, {state}{progress}, via {source}")
        if len(lines) >= 13:
            break
    maximum = _as_int(cfg.get("prompt_context_max_chars"), 2400, 512, 12000)
    return "\n".join(lines)[:maximum]


def get_hydra_system_prompt_fragments(
    *,
    role: str,
    redis_client: Any = None,
    **_kwargs,
) -> Dict[str, List[str]]:
    message = _prompt_context(redis_client)
    if not message:
        return {}
    normalized_role = _text(role).lower()
    if normalized_role in {"", "chat", "hermes", "memory_context", "tater_tube_context"}:
        return {
            "chat": [message],
            "hermes": [message],
            "memory_context": [message],
            "tater_tube_context": [message],
        }
    return {}


def _history_forms(context: Dict[str, Any]) -> List[Dict[str, Any]]:
    forms = []
    events = context.get("events") if isinstance(context.get("events"), list) else []
    for index, event in enumerate(events[:30]):
        if not isinstance(event, dict):
            continue
        title = _text(event.get("title")) or "Untitled"
        forms.append(
            {
                "id": f"history:{index}",
                "group": "history",
                "title": title,
                "subtitle": " · ".join(
                    part
                    for part in [
                        _text(event.get("media_type")),
                        _text(event.get("state")),
                        _text(event.get("source")),
                    ]
                    if part
                ),
                "detail": _text(event.get("occurred_at")),
            }
        )
    return forms


def _recommendation_forms(recommendations: Dict[str, Any]) -> List[Dict[str, Any]]:
    forms = []
    for index, item in enumerate(recommendations.get("items") or []):
        if not isinstance(item, dict):
            continue
        forms.append(
            {
                "id": f"pick:{index}",
                "group": "picks",
                "title": _text(item.get("title")) or "Tater Pick",
                "subtitle": " · ".join(
                    part for part in [_text(item.get("media_type")), _text(item.get("reason"))] if part
                ),
            }
        )
    return forms


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    cfg = _settings(client)
    runtime = _runtime(client)
    context = _load_json(client, CONTEXT_KEY, {})
    recommendations = _load_json(client, RECOMMENDATIONS_KEY, {})
    events = context.get("events") if isinstance(context, dict) and isinstance(context.get("events"), list) else []
    picks = (
        recommendations.get("items")
        if isinstance(recommendations, dict) and isinstance(recommendations.get("items"), list)
        else []
    )
    connected = _paired(cfg)
    item_forms = [
        {
            "id": "overview:connection",
            "group": "overview",
            "title": "Tater Tube Server",
            "subtitle": "Connected" if connected else "Not paired",
            "detail": _text(cfg.get("server_url")) or "Use the Connect tab with a server-generated PIN.",
            "hero_badges": [
                {"label": "CONNECTED" if connected else "PAIRING NEEDED", "tone": "good" if connected else "warn"},
                {"label": f"{len(events)} watch events", "tone": "muted"},
                {"label": f"{len(picks)} current picks", "tone": "muted"},
                {"label": "PICKS VOICE ON" if _as_bool(cfg.get("tts_enabled"), True) else "PICKS VOICE OFF", "tone": "good" if _as_bool(cfg.get("tts_enabled"), True) else "muted"},
            ],
            "run_action": "tater_tube_sync_now",
            "run_label": "Sync Now",
        },
        {
            "id": "overview:recommend",
            "group": "overview",
            "title": "Tater's Picks",
            "subtitle": f"{len(picks)} recommendations cached",
            "detail": _text(recommendations.get("summary")) or "Generate recommendations from recent viewing and the server catalog.",
            "run_action": "tater_tube_recommend_now",
            "run_label": "Make Fresh Picks",
        },
        {
            "id": "settings:core",
            "group": "settings",
            "title": "Sync & Recommendation Settings",
            "subtitle": f"Profile: {_profile_id(cfg)}",
            "fields": [
                {"key": "profile_id", "label": "Viewing Profile", "type": "text", "value": _profile_id(cfg)},
                {
                    "key": "poll_interval_seconds",
                    "label": "Context Sync Interval (sec)",
                    "type": "number",
                    "value": _as_int(cfg.get("poll_interval_seconds"), 300, 30, 86400),
                },
                {
                    "key": "recommendation_interval_hours",
                    "label": "Recommendation Interval (hours)",
                    "type": "number",
                    "value": _as_int(cfg.get("recommendation_interval_hours"), 6, 1, 168),
                },
                {
                    "key": "recommendation_count",
                    "label": "Recommendation Count",
                    "type": "number",
                    "value": _as_int(cfg.get("recommendation_count"), 8, 1, 12),
                },
                {
                    "key": "recommendation_expiry_hours",
                    "label": "Picks Stay Fresh (hours)",
                    "type": "number",
                    "value": _as_int(cfg.get("recommendation_expiry_hours"), 24, 1, 168),
                },
                {
                    "key": "candidate_limit",
                    "label": "Catalog Candidate Limit",
                    "type": "number",
                    "value": _as_int(cfg.get("candidate_limit"), 200, 20, 500),
                },
                {
                    "key": "prompt_context_enabled",
                    "label": "Prompt Context Enabled",
                    "type": "checkbox",
                    "value": _as_bool(cfg.get("prompt_context_enabled"), True),
                },
                {
                    "key": "prompt_context_max_chars",
                    "label": "Prompt Context Max Characters",
                    "type": "number",
                    "value": _as_int(cfg.get("prompt_context_max_chars"), 2400, 512, 12000),
                },
                {
                    "key": "tts_enabled",
                    "label": "Tater's Picks Welcome",
                    "type": "checkbox",
                    "value": _as_bool(cfg.get("tts_enabled"), True),
                },
            ],
            "save_action": "tater_tube_save_settings",
            "save_label": "Save Settings",
        },
        {
            "id": "settings:disconnect",
            "group": "settings",
            "title": "Disconnect Core",
            "subtitle": "Remove this Core's local server credential.",
            "detail": "The server connection can also be revoked from Configuration → Tater.",
            "run_action": "tater_tube_disconnect",
            "run_label": "Disconnect",
            "run_confirm": "Disconnect Tater Tube Core from the server?",
        },
    ]
    item_forms.extend(_history_forms(context if isinstance(context, dict) else {}))
    item_forms.extend(_recommendation_forms(recommendations if isinstance(recommendations, dict) else {}))
    return {
        "summary": "Viewing context and AI recommendations shared with Tater Tube Server.",
        "stats": [
            {"label": "Status", "value": "Connected" if connected else "Not Paired"},
            {"label": "Watch Events", "value": len(events)},
            {"label": "Current Picks", "value": len(picks)},
            {"label": "Last Sync", "value": _format_time(_as_float(runtime.get("last_sync_at")))},
            {"label": "Last Picks", "value": _format_time(_as_float(runtime.get("last_recommendation_at")))},
        ],
        "items": [],
        "empty_message": "Pair Tater Tube Core to begin.",
        "ui": {
            "kind": "settings_manager",
            "title": "Tater Tube Core",
            "default_tab": "overview",
            "manager_tabs": [
                {"key": "overview", "label": "Overview", "source": "items", "item_group": "overview"},
                {"key": "history", "label": "Viewing Context", "source": "items", "item_group": "history"},
                {"key": "picks", "label": "Tater's Picks", "source": "items", "item_group": "picks"},
                {"key": "settings", "label": "Settings", "source": "items", "item_group": "settings"},
                {"key": "connect", "label": "Connect", "source": "add_form"},
            ],
            "item_fields_dropdown": True,
            "item_fields_dropdown_label": "Details",
            "item_fields_popup": True,
            "item_fields_popup_label": "Details",
            "item_forms": item_forms,
            "add_form": {
                "action": "tater_tube_pair",
                "submit_label": "Pair Tater Core",
                "fields": [
                    {
                        "key": "server_url",
                        "label": "Tater Tube Server URL",
                        "type": "text",
                        "required": True,
                        "value": _text(cfg.get("server_url")),
                        "placeholder": "http://tater-tube-server:8080",
                    },
                    {
                        "key": "name",
                        "label": "Core Name",
                        "type": "text",
                        "value": _text(cfg.get("core_name")) or "Tater",
                    },
                    {
                        "key": "pin",
                        "label": "6-digit Pairing PIN",
                        "type": "text",
                        "required": True,
                        "value": "",
                    },
                ],
            },
        },
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    action_name = _text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)

    if action_name == "tater_tube_pair":
        server_url = _normalize_server_url(values.get("server_url") or body.get("server_url"))
        name = _text(values.get("name") or body.get("name")) or "Tater"
        pin = "".join(ch for ch in _text(values.get("pin") or body.get("pin")) if ch.isdigit())
        if len(pin) != 6:
            raise ValueError("Enter the 6-digit PIN created by Tater Tube Server.")
        paired = _api_request(
            "POST",
            "tater/core/pair",
            server_url=server_url,
            payload={"pin": pin, "name": name},
        )
        if not isinstance(paired, dict) or not _text(paired.get("token")):
            raise RuntimeError("The server did not return a Tater Core token.")
        _save_hash(
            client,
            SETTINGS_KEY,
            {
                "server_url": server_url,
                "core_name": _text(paired.get("core_name")) or name,
                "core_id": _text(paired.get("core_id")),
                "token": _text(paired.get("token")),
            },
        )
        _save_hash(client, RUNTIME_KEY, {"status": "connected", "last_error": ""})
        try:
            _sync_context(client)
        except Exception as exc:
            logger.warning("[Tater Tube] Initial context sync failed: %s", exc)
        return {"ok": True, "message": f"Paired {_text(paired.get('core_name')) or name} with Tater Tube Server."}

    if action_name == "tater_tube_disconnect":
        if client is not None:
            client.hdel(SETTINGS_KEY, "token", "core_id")
            client.delete(CONTEXT_KEY, RECOMMENDATIONS_KEY)
            _save_hash(client, RUNTIME_KEY, {"status": "disconnected", "last_error": ""})
        return {"ok": True, "message": "Tater Tube Core disconnected locally."}

    if action_name == "tater_tube_save_settings":
        allowed = {
            "profile_id",
            "poll_interval_seconds",
            "recommendation_interval_hours",
            "recommendation_count",
            "recommendation_expiry_hours",
            "candidate_limit",
            "prompt_context_enabled",
            "prompt_context_max_chars",
            "tts_enabled",
        }
        updates = {key: values.get(key) for key in allowed if key in values}
        _save_hash(client, SETTINGS_KEY, updates)
        return {"ok": True, "message": "Tater Tube Core settings saved."}

    if action_name == "tater_tube_sync_now":
        context = _sync_context(client)
        return {"ok": True, "message": f"Synced {len(context.get('events') or [])} viewing events."}

    if action_name == "tater_tube_recommend_now":
        loop = asyncio.new_event_loop()
        try:
            recommendations = _generate_recommendations(loop, _get_primary_llm_client_from_env(), client)
        finally:
            loop.close()
        return {"ok": True, "message": f"Published {len(recommendations.get('items') or [])} fresh picks."}

    raise ValueError(f"Unknown action: {action_name}")


def run(stop_event: Optional[object] = None) -> None:
    logger.info("[Tater Tube] Core starting.")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    llm_client = None
    try:
        llm_client = _get_primary_llm_client_from_env()
    except Exception as exc:
        logger.warning("[Tater Tube] LLM is not ready yet: %s", exc)

    try:
        while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
            cfg = _settings()
            if not _paired(cfg):
                _save_hash(redis_client, RUNTIME_KEY, {"status": "waiting_for_pairing"})
                time.sleep(5)
                continue

            runtime = _runtime()
            now = time.time()
            poll_interval = _as_int(cfg.get("poll_interval_seconds"), 300, 30, 86400)
            recommendation_interval = (
                _as_int(cfg.get("recommendation_interval_hours"), 6, 1, 168) * 3600
            )
            try:
                try:
                    _process_tts_request(loop)
                except PermissionError:
                    raise
                except Exception as exc:
                    logger.warning("[Tater Tube] TTS cycle failed: %s", exc)
                    _save_hash(
                        redis_client,
                        RUNTIME_KEY,
                        {"last_tts_error": _text(exc)[:500], "last_tts_error_at": now},
                    )
                if now - _as_float(runtime.get("last_sync_at")) >= poll_interval:
                    _sync_context()
                    runtime = _runtime()
                if now - _as_float(runtime.get("last_recommendation_at")) >= recommendation_interval:
                    if llm_client is None:
                        llm_client = _get_primary_llm_client_from_env()
                    _generate_recommendations(loop, llm_client)
            except PermissionError as exc:
                logger.warning("[Tater Tube] Server authorization was revoked: %s", exc)
                redis_client.hdel(SETTINGS_KEY, "token", "core_id")
                redis_client.delete(CONTEXT_KEY, RECOMMENDATIONS_KEY)
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {"status": "authorization_revoked", "last_error": _text(exc)[:500], "last_error_at": now},
                )
            except Exception as exc:
                logger.warning("[Tater Tube] Background cycle failed: %s", exc)
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {"status": "error", "last_error": _text(exc)[:500], "last_error_at": now},
                )
            time.sleep(0.5)
    finally:
        try:
            loop.close()
        except Exception:
            pass
        logger.info("[Tater Tube] Core stopped.")
