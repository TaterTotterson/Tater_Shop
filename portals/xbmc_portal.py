# portals/xbmc_portal.py
import json
import os
import asyncio
import logging
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
__version__ = "1.1.6"


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
TTS_MAX_TEXT_CHARS = 900
INSTALLED_GAME_CONTEXT_MAX_ITEMS = 1200

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

# -------------------- FastAPI DTOs --------------------
class XBMCRequest(BaseModel):
    text: str
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    session_id: Optional[str] = None  # we use this for Redis key
    include_tts: Optional[bool] = None
    tts_format: Optional[str] = None
    include_quick_asks: Optional[bool] = None
    installed_games: Optional[List[Dict[str, Any]]] = None

class XBMCResponse(BaseModel):
    response: str
    quick_asks: Optional[List[str]] = None

class XBMCTTSRequest(BaseModel):
    text: str


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
        "\nInstalled original Xbox games available on this console:\n"
        f"{game_lines}\n\n"
        "When the user asks for game recommendations, prefer games from this installed list. "
        "If you recommend an installed game, use the exact title from the list and ask whether to launch it. "
        "The XBMC skin can launch exact installed games after the user chooses the launch quick reply.\n"
    )


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
        return ["Set the lights to blue", "Turn the lights off", "What else can you control?"]
    if "game" in combined or "xbox" in combined:
        return ["Tell me more about that game", "Recommend another game", "Find a multiplayer game"]
    if "news" in combined:
        return ["Tell me the top story", "Any Insignia updates?", "Find more Xbox news"]
    return ["Tell me more", "What can you do next?", "Give me a quick suggestion"]


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
            if len(asks) >= 3:
                break

    while len(asks) < 3:
        for fallback in _fallback_quick_asks(user_text, reply_text):
            ask = _clean_quick_ask(fallback)
            if ask and ask not in asks:
                asks.append(ask)
            if len(asks) >= 3:
                break
        break

    return asks[:3]


async def _generate_quick_asks(user_text: str, reply_text: str, installed_games: Any = None) -> List[str]:
    fallback = _fallback_quick_asks(user_text, reply_text)
    if _llm is None:
        return fallback

    game_context = _installed_games_context(installed_games)

    try:
        result = await _llm.chat(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Generate exactly 3 short follow-up replies for an original Xbox Cortana UI. "
                        "They are button labels the user can send next. Keep each under 8 words. "
                        "If the reply recommends an installed game, one button may be Launch <exact title>. "
                        "Return JSON only: {\"quick_asks\":[\"...\",\"...\",\"...\"]}"
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
            max_tokens=160,
            temperature=0.35,
        )
        raw = str(((result or {}).get("message") or {}).get("content") or "")
        return _extract_quick_asks(raw, user_text, reply_text)
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
            "You are running on an original Xbox using XBMC4Xbox, shown on a TV screen.\n"
            "Keep responses short, readable, and suitable for viewing from across the room.\n"
            "Avoid long walls of text; aim for 1–3 short paragraphs at most.\n\n"
            "Even while staying in character, you must follow tool and safety rules.\n\n"
        )

    # Otherwise, use the built-in Cortana personality
    else:
        base_prompt = (
            f"You are {first} {last}, the core AI assistant that powers the multi-platform Tater bot.\n\n"
            "On this platform you are running on an original Xbox using XBMC4Xbox, shown on a TV screen.\n"
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
app = FastAPI(title="Tater XBMC Bridge", version="1.0")

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
    return {"ok": True, "version": "1.0"}


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
            user_text=text_in,
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
