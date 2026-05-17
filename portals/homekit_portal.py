# portals/homekit_portal.py
import os
import json
import asyncio
import logging
import threading
import time
from typing import Optional, Dict, Any, List
import uvicorn
from fastapi import FastAPI, HTTPException, Header
from dotenv import load_dotenv
import verba_registry as pr
from admin_gate import admin_denial_message, is_admin_only_plugin, origin_is_admin, resolve_admin_status
from helpers import (
    get_llm_client_from_env,
    build_llm_host_from_env,
    redis_client,
)
from hydra import run_hydra_turn, resolve_agent_limits
from verba_result import action_failure
__version__ = "1.1.3"


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("homekit")

# -------------------- Platform defaults --------------------
BIND_HOST = "0.0.0.0"
DEFAULT_PORT = 8789
TIMEOUT_SECONDS = 60
DEFAULT_GLOBAL_MAX_STORE = 20
DEFAULT_GLOBAL_MAX_LLM = 8
DEFAULT_SESSION_TTL_SECONDS = 60 * 60  # 1h

# -------------------- Platform settings --------------------
PORTAL_SETTINGS = {
    "category": "HomeKit / Siri",
    "required": {
        "bind_port": {
            "label": "Legacy Bind Port",
            "type": "number",
            "default": DEFAULT_PORT,
            "description": "Deprecated. Siri / Shortcuts now uses Tater's main API path: /api/portals/homekit_portal/api/tater-homekit/v1"
        },
        "SESSION_TTL_SECONDS": {
            "label": "Session TTL (seconds)",
            "type": "number",
            "default": DEFAULT_SESSION_TTL_SECONDS,
            "description": "How long to keep a Siri session alive."
        },
        "API_AUTH_ENABLED": {
            "label": "Require API Key",
            "type": "select",
            "options": ["true", "false"],
            "default": "false",
            "description": "Require X-Tater-Token on HomeKit API requests."
        },
        "API_AUTH_KEY": {
            "label": "API Key",
            "type": "password",
            "default": "",
            "description": "Shared API key expected in the X-Tater-Token header when auth is enabled."
        },
        "AUTH_TOKEN": {
            "label": "Legacy Auth Token (optional)",
            "type": "password",
            "default": "",
            "description": "Backward-compatible fallback token if API Key is empty."
        },
    }
}

def _portal_settings() -> Dict[str, str]:
    return redis_client.hgetall("homekit_portal_settings") or {}

def _get_int_setting(name: str, default: int) -> int:
    s = _portal_settings().get(name)
    if s is None or str(s).strip() == "":
        return default
    try:
        return int(str(s).strip())
    except Exception:
        return default

def _get_str_setting(name: str, default: str = "") -> str:
    s = _portal_settings().get(name)
    return s if s is not None else default

def _get_bool_setting(name: str, default: bool = False) -> bool:
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
    primary = _get_str_setting("API_AUTH_KEY", "").strip()
    if primary:
        return primary
    return _get_str_setting("AUTH_TOKEN", "").strip()

def _is_api_auth_enabled() -> bool:
    raw = _portal_settings().get("API_AUTH_ENABLED")
    if raw is None or str(raw).strip() == "":
        return bool(_get_api_auth_key())
    return _get_bool_setting("API_AUTH_ENABLED", False)

def _require_api_auth(x_tater_token: Optional[str]) -> None:
    if not _is_api_auth_enabled():
        return
    configured = _get_api_auth_key()
    if not configured:
        raise HTTPException(status_code=503, detail="API auth is enabled but no API key is configured.")
    supplied = str(x_tater_token or "").strip()
    if supplied != configured:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Tater-Token header.")

# -------------------- Plugin gating --------------------
def _get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("verba_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")

# -------------------- History helpers --------------------
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
            return json.dumps(res, ensure_ascii=False)
        except Exception:
            return str(res)
    return str(res)

def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    # skip tool “waiting” markers if you ever add them
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # include FINAL plugin responses as plain assistant text
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        if content.get("phase", "final") != "final":
            return None
        payload = content.get("content", "")
        txt = _flatten_to_text(payload).strip()
        if len(txt) > 4000:
            txt = txt[:4000] + " …"
        return {"role": "assistant", "content": txt}

    # stringify plugin_call so the model “sees” prior actions
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps({
            "function": content.get("plugin"),
            "arguments": content.get("arguments", {})
        }, ensure_ascii=False)
        return {"role": "assistant", "content": as_text}

    # default cases
    if isinstance(content, str):
        return {"role": role, "content": content}

    # anything else, compact to string
    return {"role": role, "content": _flatten_to_text(content)}

def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge consecutive same-role turns to keep history compact.

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
            merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(m)

    return merged

def _sess_key(session_id: Optional[str]) -> str:
    return f"tater:homekit:session:{session_id or 'default'}:history"

async def _load_history(session_id: Optional[str], limit: int) -> List[Dict[str, Any]]:
    key = _sess_key(session_id)
    raw = redis_client.lrange(key, -limit, -1)
    loop_messages: List[Dict[str, Any]] = []
    for entry in raw:
        try:
            obj = json.loads(entry)
            role = obj.get("role", "user")
            content = obj.get("content")
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
    ttl: int,
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
    pipe.expire(key, ttl)
    pipe.execute()

# -------------------- System prompt (HomeKit voice preamble) --------------------
def build_system_prompt() -> str:
    # Platform preamble should be style/format only. Keep voice-focused for Siri playback.
    return (
        "You are a HomeKit/Siri-savvy AI assistant.\n"
        "Responses are spoken aloud by Siri; keep replies short, plain text, and natural.\n"
        "Do not use emojis or markdown.\n"
    )

# -------------------- FastAPI app --------------------
app = FastAPI(title="Tater HomeKit / Siri Bridge", version="1.0")

_llm = None

@app.on_event("startup")
async def _on_startup():
    ensure_portal_api_ready()


def ensure_portal_api_ready(*_args, **_kwargs):
    global _llm
    if _llm is None:
        try:
            _llm = get_llm_client_from_env()
            logger.info(f"[HomeKit] LLM client → {build_llm_host_from_env()}")
        except Exception as exc:
            logger.warning("[HomeKit] LLM client is not ready: %s", exc)

@app.post("/tater-homekit/v1/message")
async def handle_message(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)):
    """
    Expected JSON from Shortcut:
    {
      "text": "ask tater to turn on the office light",
      "session_id": "iphone-masta"
    }
    """
    if _llm is None:
        raise HTTPException(503, "LLM not ready")

    _require_api_auth(x_tater_token)

    text_in = (payload.get("text") or "").strip()
    if not text_in:
        return {"reply": "(no text provided)"}

    session_id = payload.get("session_id") or "default"
    history_store_limit = _global_history_store_limit()
    history_llm_limit = _global_history_llm_limit()
    session_ttl = _get_int_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)
    user_id = str(payload.get("user_id") or payload.get("device_id") or session_id or "homekit_user").strip()
    username = str(
        payload.get("username")
        or payload.get("user")
        or payload.get("name")
        or payload.get("device_name")
        or user_id
        or "homekit_user"
    ).strip()

    system_prompt = build_system_prompt()
    loop_messages = await _load_history(session_id, history_llm_limit)
    messages_list = loop_messages

    await _save_message(
        session_id,
        "user",
        text_in,
        history_store_limit,
        session_ttl,
        username=username,
        user_id=user_id,
    )

    merged_registry = dict(pr.get_verba_registry_snapshot() or {})
    merged_enabled = _get_plugin_enabled

    try:
        origin = {
            "platform": "homekit",
            "session_id": session_id,
            "device_id": payload.get("device_id"),
            "user_id": user_id,
            "user": username,
            "request_id": session_id,
        }
        origin = {k: v for k, v in origin.items() if v not in (None, "")}
        resolve_admin_status(platform="homekit", origin=origin, redis_client=redis_client)

        def _admin_guard(func_name: str):
            if is_admin_only_plugin(func_name) and not origin_is_admin("homekit", origin, redis_client):
                return action_failure(
                    code="admin_only",
                    message=admin_denial_message("homekit", origin, redis_client),
                    needs=[],
                    say_hint="Explain that this tool is restricted to People marked as admin.",
                )
            return None

        agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
        result = await run_hydra_turn(
            llm_client=_llm,
            platform="homekit",
            history_messages=messages_list,
            registry=merged_registry,
            enabled_predicate=merged_enabled,
            context={},
            user_text=text_in,
            scope=f"session:{session_id}" if str(session_id or "").strip() else "",
            origin=origin,
            admin_guard=_admin_guard,
            redis_client=redis_client,
            max_rounds=agent_max_rounds,
            max_tool_calls=agent_max_tool_calls,
            platform_preamble=system_prompt,
        )
        final_text = str(result.get("text") or "").strip()
        if len(final_text) > 2000:
            final_text = final_text[:2000] + "…"
        await _save_message(
            session_id,
            "assistant",
            {"marker": "plugin_response", "phase": "final", "content": final_text},
            history_store_limit,
            session_ttl,
        )
        return {"reply": final_text}

    except Exception as e:
        logger.exception("[HomeKit] LLM error")
        await _save_message(session_id, "assistant", f"LLM error: {e}", history_store_limit, session_ttl)
        return {"reply": "Sorry, I had a problem talking to Tater."}

def run(stop_event: Optional[threading.Event] = None):
    ensure_portal_api_ready()
    logger.info("[HomeKit] Portal API available at /api/portals/homekit_portal/api/tater-homekit/v1")
    while not (stop_event and stop_event.is_set()):
        time.sleep(0.5)
    logger.info("[HomeKit] Portal stopped.")
