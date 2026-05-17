# portals/xbmc_portal.py
import json
import os
import asyncio
import logging
import threading
import time
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import uvicorn

from dotenv import load_dotenv
load_dotenv()

from helpers import (
    get_tater_name,
    get_llm_client_from_env,
    build_llm_host_from_env,
    redis_client,
)
import verba_registry as pr
from admin_gate import admin_denial_message, is_admin_only_plugin, origin_is_admin, resolve_admin_status
from hydra import run_hydra_turn, resolve_agent_limits
from verba_result import action_failure
__version__ = "1.1.3"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xbmc")

# -------------------- Platform defaults (overridable in WebUI) --------------------
BIND_HOST = "0.0.0.0"
TIMEOUT_SECONDS = 60

DEFAULT_GLOBAL_MAX_STORE = 20
DEFAULT_GLOBAL_MAX_LLM = 8
DEFAULT_SESSION_TTL_SECONDS = 2 * 60 * 60  # 2h

PORTAL_SETTINGS = {
    "category": "XBMC / Original Xbox Settings",
    "required": {
        "bind_port": {
            "label": "Legacy Bind Port",
            "type": "number",
            "default": 8790,
            "description": "Deprecated. XBMC now uses Tater's main API path: /api/portals/xbmc_portal/api/tater-xbmc/v1"
        },
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

# -------------------- FastAPI DTOs --------------------
class XBMCRequest(BaseModel):
    text: str
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    session_id: Optional[str] = None  # we use this for Redis key

class XBMCResponse(BaseModel):
    response: str

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
            _llm = get_llm_client_from_env()
            logger.info(f"[XBMC Bridge] LLM client → {build_llm_host_from_env()}")
        except Exception as exc:
            logger.warning("[XBMC Bridge] LLM client is not ready: %s", exc)

@app.get("/tater-xbmc/v1/health")
async def health(x_tater_token: Optional[str] = Header(None)):
    _require_api_auth(x_tater_token)
    return {"ok": True, "version": "1.0"}

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
        return XBMCResponse(response=final_text)

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
