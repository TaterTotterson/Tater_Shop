"""Meshtastic integration portal for Tater."""

import asyncio
import json
import logging
import re
import threading
import time
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

import verba_registry as pr
from helpers import get_llm_client_from_env, redis_client
from hydra import resolve_agent_limits, run_hydra_turn
from notify.queue import is_expired

__version__ = "0.1.1"
PORTAL_DESCRIPTION = "Meshtastic integration portal for Tater."
MIN_TATER_VERSION = "59"
TAGS = ["radio", "mesh", "offgrid"]


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("meshtastic.tater")

DEFAULT_BRIDGE_URL = "http://127.0.0.1:8433"
DEFAULT_POLL_INTERVAL_SECONDS = 3.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 15.0
DEFAULT_MAX_OUTBOUND_LENGTH = 160
DEFAULT_MAX_CHUNKS = 3
DEFAULT_SESSION_TTL_SECONDS = 6 * 60 * 60
DEFAULT_GLOBAL_MAX_STORE = 20
DEFAULT_GLOBAL_MAX_LLM = 8
NOTIFY_QUEUE_KEY = "notifyq:meshtastic"
NOTIFY_POLL_INTERVAL = 0.5

_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_MD_FENCE_RE = re.compile(r"```(?:[A-Za-z0-9_+\-]+)?\n?([\s\S]*?)```")
_MD_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")
_MD_DECORATION_RE = re.compile(r"[*_~>#]+")
_WHITESPACE_RE = re.compile(r"\s+")

PORTAL_SETTINGS = {
    "category": "Meshtastic Settings",
    "tags": TAGS,
    "required": {
        "bridge_url": {
            "label": "Bridge URL",
            "type": "string",
            "default": DEFAULT_BRIDGE_URL,
            "description": "Base URL for the local Tater Meshtastic Bridge service.",
        },
        "api_token": {
            "label": "API Token",
            "type": "password",
            "default": "",
            "description": "Optional bearer token expected by the bridge.",
        },
        "poll_interval_sec": {
            "label": "Poll Interval (sec)",
            "type": "number",
            "default": DEFAULT_POLL_INTERVAL_SECONDS,
            "description": "How often to poll the bridge for new mesh messages.",
        },
        "request_timeout_sec": {
            "label": "HTTP Timeout (sec)",
            "type": "number",
            "default": DEFAULT_REQUEST_TIMEOUT_SECONDS,
            "description": "Timeout for bridge API requests.",
        },
        "response_policy": {
            "label": "Response Policy",
            "type": "select",
            "options": ["mention_only", "direct_only", "all_messages"],
            "default": "mention_only",
            "description": "When the portal should answer channel traffic.",
        },
        "trigger_keywords": {
            "label": "Trigger Keywords",
            "type": "string",
            "default": "tater",
            "description": "Comma-separated names that count as a mention on shared channels.",
        },
        "session_mode": {
            "label": "Session Mode",
            "type": "select",
            "options": ["node", "node_channel"],
            "default": "node_channel",
            "description": "Choose whether memory is tracked per node or per node+channel.",
        },
        "allowed_channels": {
            "label": "Allowed Channels",
            "type": "string",
            "default": "",
            "description": "Choose one or more mesh channels above 0 that Tater is allowed to answer on. Channel 0 is never allowed for assistant replies.",
        },
        "allow_direct_messages": {
            "label": "Allow Direct Messages",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
            "description": "If true, direct mesh messages can trigger replies.",
        },
        "allow_broadcasts": {
            "label": "Allow Broadcast Replies",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
            "description": "If true, channel traffic can receive channel replies.",
        },
        "max_outbound_length": {
            "label": "Max Outbound Length",
            "type": "number",
            "default": DEFAULT_MAX_OUTBOUND_LENGTH,
            "description": "Soft character limit for each outbound radio message chunk.",
        },
        "enable_chunking": {
            "label": "Enable Chunking",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
            "description": "Split long replies into numbered multi-part messages.",
        },
        "max_chunks": {
            "label": "Max Chunks",
            "type": "number",
            "default": DEFAULT_MAX_CHUNKS,
            "description": "Maximum number of numbered parts to send for one reply.",
        },
        "session_ttl_seconds": {
            "label": "Session TTL (sec)",
            "type": "number",
            "default": DEFAULT_SESSION_TTL_SECONDS,
            "description": "How long to keep mesh chat history alive in Redis.",
        },
        "resume_mode": {
            "label": "Resume Mode",
            "type": "select",
            "options": ["from_now", "from_last_seen"],
            "default": "from_now",
            "description": "Choose whether the portal ignores existing bridge backlog on startup.",
        },
    },
}


def _portal_settings() -> Dict[str, str]:
    return redis_client.hgetall("meshtastic_portal_settings") or {}


def _get_str_setting(name: str, default: str = "") -> str:
    value = _portal_settings().get(name)
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _get_float_setting(name: str, default: float) -> float:
    value = _portal_settings().get(name)
    try:
        return float(str(value).strip()) if value is not None and str(value).strip() else float(default)
    except Exception:
        return float(default)


def _get_int_setting(name: str, default: int) -> int:
    value = _portal_settings().get(name)
    try:
        return int(float(str(value).strip())) if value is not None and str(value).strip() else int(default)
    except Exception:
        return int(default)


def _get_bool_setting(name: str, default: bool = False) -> bool:
    value = _portal_settings().get(name)
    if value is None:
        return default
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _coerce_channel_token(value: Any) -> str:
    try:
        return str(int(float(str(value).strip())))
    except Exception:
        return ""


def _coerce_reply_channel_token(value: Any) -> str:
    token = _coerce_channel_token(value)
    if not token:
        return ""
    try:
        return token if int(token) > 0 else ""
    except Exception:
        return ""


def _coerce_allowed_channel_values(raw: Any) -> List[str]:
    if raw is None:
        values: List[Any] = []
    elif isinstance(raw, (list, tuple, set)):
        values = list(raw)
    else:
        values = str(raw).split(",")

    out: List[str] = []
    seen = set()
    for value in values:
        token = _coerce_reply_channel_token(value)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _bridge_client_from_settings(raw_settings: Optional[Dict[str, Any]] = None) -> "BridgeClient":
    current = raw_settings if isinstance(raw_settings, dict) else {}
    bridge_url = str(current.get("bridge_url") or _get_str_setting("bridge_url", DEFAULT_BRIDGE_URL)).strip() or DEFAULT_BRIDGE_URL
    api_token = str(current.get("api_token") or _get_str_setting("api_token", "")).strip()
    timeout = _get_float_setting("request_timeout_sec", DEFAULT_REQUEST_TIMEOUT_SECONDS)
    if current.get("request_timeout_sec") not in (None, ""):
        try:
            timeout = float(str(current.get("request_timeout_sec")).strip())
        except Exception:
            timeout = _get_float_setting("request_timeout_sec", DEFAULT_REQUEST_TIMEOUT_SECONDS)
    return BridgeClient(
        base_url=bridge_url,
        api_token=api_token,
        timeout=max(2.0, float(timeout)),
    )


def _channel_option_rows(*, channels_payload: Any, selected_values: Optional[List[str]] = None) -> List[Dict[str, str]]:
    selected = [token for token in (selected_values or []) if token]
    options_by_value: Dict[str, Dict[str, str]] = {}

    channels = []
    if isinstance(channels_payload, dict):
        channels = list(channels_payload.get("channels") or [])
    elif isinstance(channels_payload, list):
        channels = list(channels_payload)

    for row in channels:
        if not isinstance(row, dict):
            continue
        value = _coerce_reply_channel_token(row.get("index"))
        if not value:
            continue
        name = str(row.get("name") or f"Channel {value}").strip()
        role = str(row.get("role") or "").strip()
        label = f"{value} - {name}"
        if role:
            label = f"{label} ({role})"
        options_by_value[value] = {"value": value, "label": label}

    for value in selected:
        if value in options_by_value:
            continue
        options_by_value[value] = {"value": value, "label": f"{value} (saved)"}

    return sorted(options_by_value.values(), key=lambda row: (_safe_channel_sort_key(row.get("value")), str(row.get("label") or "").lower()))


def _safe_channel_sort_key(value: Any) -> int:
    token = _coerce_channel_token(value)
    return int(token) if token else 999999


def webui_settings_fields(
    *,
    fields: Any,
    current_settings: Any = None,
    **_kwargs,
) -> List[Dict[str, Any]]:
    base_fields = list(fields or [])
    current = current_settings if isinstance(current_settings, dict) else {}

    selected_allowed = _coerce_allowed_channel_values(current.get("allowed_channels"))
    legacy_default = _coerce_reply_channel_token(current.get("default_reply_channel"))
    if not selected_allowed and legacy_default:
        selected_allowed = [legacy_default]

    fetch_error = ""
    channel_options: List[Dict[str, str]] = []
    try:
        bridge_client = _bridge_client_from_settings(current)
        channel_options = _channel_option_rows(
            channels_payload=bridge_client.get_channels(),
            selected_values=selected_allowed,
        )
    except Exception as exc:
        fetch_error = str(exc).strip()
        channel_options = _channel_option_rows(channels_payload=[], selected_values=selected_allowed)

    out: List[Dict[str, Any]] = []
    for item in base_fields:
        if not isinstance(item, dict):
            out.append(item)
            continue

        key = str(item.get("key") or "").strip()
        updated = dict(item)
        if key == "allowed_channels":
            updated["type"] = "multiselect"
            updated["options"] = channel_options
            updated["value"] = selected_allowed
            updated["default"] = []
            base_desc = str(updated.get("description") or "").strip()
            extra = "Pick one or more reply channels above 0. If nothing is selected, Tater will not answer on mesh channels."
            if fetch_error:
                extra = f"{extra} Bridge lookup failed: {fetch_error}"
            updated["description"] = f"{base_desc} {extra}".strip()
        out.append(updated)

    return out


def webui_prepare_settings_values(*, values: Any, **_kwargs) -> Dict[str, Any]:
    out = dict(values or {})
    if "allowed_channels" in out:
        out["allowed_channels"] = ",".join(_coerce_allowed_channel_values(out.get("allowed_channels")))
    out["default_reply_channel"] = ""
    return out


def _read_global_history_limit(
    redis_key: str,
    default: int,
    *,
    min_value: int = 0,
    max_value: int = 500,
) -> int:
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


def _history_key(session_key: str) -> str:
    return f"tater:meshtastic:{session_key}:history"


def _save_history(session_key: str, role: str, content: Any) -> None:
    max_items = _global_history_store_limit()
    ttl_seconds = max(60, _get_int_setting("session_ttl_seconds", DEFAULT_SESSION_TTL_SECONDS))
    key = _history_key(session_key)
    item = {"role": str(role or "user").strip() or "user", "content": content}
    payload = json.dumps(item, ensure_ascii=False)
    pipe = redis_client.pipeline()
    pipe.rpush(key, payload)
    if max_items > 0:
        pipe.ltrim(key, -max_items, -1)
    pipe.expire(key, ttl_seconds)
    pipe.execute()


def _flatten_to_text(res: Any) -> str:
    if res is None:
        return ""
    if isinstance(res, str):
        return res
    if isinstance(res, list):
        parts: List[str] = []
        for item in res:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                token = item.get("type") or "content"
                name = item.get("name") or ""
                parts.append(f"[{token}{(':' + name) if name else ''}]")
            else:
                parts.append(str(item))
        return "\n".join(part for part in parts if part).strip()
    if isinstance(res, dict):
        if "message" in res and isinstance(res["message"], str):
            return res["message"]
        try:
            return json.dumps(res, ensure_ascii=False)
        except Exception:
            return str(res)
    return str(res)


def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        if content.get("phase", "final") != "final":
            return None
        payload = _flatten_to_text(content.get("content", "")).strip()
        if len(payload) > 4000:
            payload = payload[:4000] + " ..."
        return {"role": "assistant", "content": payload}

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        compact = json.dumps(
            {
                "function": content.get("plugin"),
                "arguments": content.get("arguments", {}),
            },
            ensure_ascii=False,
        )
        return {"role": "assistant", "content": compact}

    return {"role": role, "content": _flatten_to_text(content)}


def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for item in loop_messages:
        if not item:
            continue
        role = str(item.get("role") or "").strip()
        content = _flatten_to_text(item.get("content", "")).strip()
        if not role or not content:
            continue
        msg = {"role": role, "content": content}
        if not merged:
            merged.append(msg)
            continue
        if merged[-1]["role"] == msg["role"]:
            merged[-1]["content"] = (merged[-1]["content"] + "\n\n" + msg["content"]).strip()
        else:
            merged.append(msg)
    return merged


def _load_history(session_key: str, limit: int) -> List[Dict[str, Any]]:
    key = _history_key(session_key)
    try:
        raw_items = redis_client.lrange(key, -max(1, int(limit)), -1)
    except Exception:
        raw_items = []

    loop_messages: List[Dict[str, Any]] = []
    for raw in raw_items:
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        templ = _to_template_msg(str(payload.get("role") or "user"), payload.get("content"))
        if templ:
            loop_messages.append(templ)
    return _enforce_user_assistant_alternation(loop_messages)


def _get_plugin_enabled(name: str) -> bool:
    enabled = redis_client.hget("verba_enabled", name)
    if not (enabled and str(enabled).lower() == "true"):
        return False
    # Keep radio chat from exposing IRC-specific moderation tools.
    return not str(name or "").startswith("irc_admin_")


def _clean_mesh_text(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""

    text = _MD_FENCE_RE.sub(lambda m: m.group(1).strip(), text)
    text = _MD_INLINE_CODE_RE.sub(lambda m: m.group(1).strip(), text)
    text = _MD_LINK_RE.sub(lambda m: f"{m.group(1)} ({m.group(2)})", text)
    text = text.replace("\r", "\n")
    text = text.replace("•", "- ")
    text = text.replace("–", "-")
    text = text.replace("—", "-")
    text = _MD_DECORATION_RE.sub("", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _ascii_mesh_text(raw: Any) -> str:
    cleaned = _clean_mesh_text(raw)
    normalized = unicodedata.normalize("NFKD", cleaned)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.replace("\t", " ")
    ascii_text = ascii_text.replace("\x00", "")
    ascii_text = re.sub(r"[ ]{2,}", " ", ascii_text)
    ascii_text = re.sub(r"\n{3,}", "\n\n", ascii_text)
    return ascii_text.strip()


def _compact_for_mesh(raw: Any) -> str:
    text = _ascii_mesh_text(raw)
    if not text:
        return ""
    lines = [line.strip(" -") for line in text.splitlines()]
    text = " ".join(line for line in lines if line)
    text = _WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _chunk_mesh_text(text: str, max_len: int, max_chunks: int) -> List[str]:
    compact = _compact_for_mesh(text)
    if not compact:
        return []
    if len(compact) <= max_len:
        return [compact]

    remaining = compact
    chunks: List[str] = []
    while remaining and len(chunks) < max_chunks:
        prefix = f"({len(chunks) + 1}/{max_chunks}) "
        available = max(16, max_len - len(prefix))
        if len(remaining) <= available:
            chunks.append(prefix + remaining)
            remaining = ""
            break
        cut = remaining.rfind(" ", 0, available)
        if cut <= 0:
            cut = available
        piece = remaining[:cut].rstrip(" ,;:-")
        remaining = remaining[cut:].lstrip()
        if piece:
            chunks.append(prefix + piece)

    if remaining:
        if not chunks:
            return [compact[: max_len - 4].rstrip() + " ..."]
        last_prefix = f"({len(chunks)}/{max_chunks}) "
        tail = chunks[-1][len(last_prefix):].rstrip()
        tail = tail[: max(8, max_len - len(last_prefix) - 4)].rstrip()
        chunks[-1] = last_prefix + tail + " ..."
    return chunks


def _shape_outbound_reply(text: str) -> List[str]:
    max_len = max(40, _get_int_setting("max_outbound_length", DEFAULT_MAX_OUTBOUND_LENGTH))
    allow_chunking = _get_bool_setting("enable_chunking", True)
    max_chunks = max(1, _get_int_setting("max_chunks", DEFAULT_MAX_CHUNKS))

    compact = _compact_for_mesh(text)
    if not compact:
        return ["I don't have a useful reply yet."]
    if len(compact) <= max_len:
        return [compact]
    if not allow_chunking:
        return [compact[: max_len - 4].rstrip() + " ..."]
    return _chunk_mesh_text(compact, max_len=max_len, max_chunks=max_chunks)


def _mesh_keywords() -> List[str]:
    raw = _get_str_setting("trigger_keywords", "tater")
    keywords = [token.strip().lower() for token in raw.split(",") if token.strip()]
    return keywords or ["tater"]


def _message_mentions_tater(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    for keyword in _mesh_keywords():
        if keyword and re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", lowered):
            return True
    return False


def _sanitize_request_text(text: str) -> str:
    output = str(text or "").strip()
    for keyword in _mesh_keywords():
        pattern = rf"^\s*[@!]*{re.escape(keyword)}[\s,:-]+"
        updated = re.sub(pattern, "", output, count=1, flags=re.IGNORECASE)
        if updated != output:
            output = updated.strip()
            break
    return output or str(text or "").strip()


def _allowed_channels() -> set[int]:
    raw = _get_str_setting("allowed_channels", "")
    allowed: set[int] = set()
    if raw:
        for token in raw.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                allowed.add(int(token))
            except Exception:
                logger.warning("[Meshtastic] Ignoring invalid allowed channel token: %s", token)
        return allowed

    legacy_default = _get_int_setting("default_reply_channel", 0)
    if int(legacy_default) > 0:
        return {int(legacy_default)}
    return set()


def _channel_allowed(channel: int) -> bool:
    if int(channel) <= 0:
        return False
    allowed = _allowed_channels()
    return int(channel) in allowed


def _normalize_channel_value(raw: Any) -> Optional[int]:
    if raw in (None, ""):
        return None
    try:
        return int(float(str(raw).strip()))
    except Exception:
        return None


def _message_channel(message: Dict[str, Any]) -> int:
    normalized = _normalize_channel_value((message or {}).get("channel"))
    if normalized is not None:
        return int(normalized)
    return _primary_allowed_channel()


def _coalesce_channel_value(*raw_values: Any) -> int:
    for raw in raw_values:
        normalized = _normalize_channel_value(raw)
        if normalized is not None and int(normalized) > 0:
            return int(normalized)
    return _primary_allowed_channel()


def _primary_allowed_channel() -> int:
    allowed = sorted(_allowed_channels())
    return int(allowed[0]) if allowed else 0


def _is_direct_message(message: Dict[str, Any]) -> bool:
    delivery = str(message.get("delivery") or "").strip().lower()
    if delivery == "direct":
        return True
    target_node = str(((message.get("to") or {}).get("node_id")) or "").strip().lower()
    return target_node in {"^local", "local", "^me"}


def _should_respond_to_message(message: Dict[str, Any]) -> bool:
    channel = _message_channel(message)
    if not _channel_allowed(channel):
        return False

    direct = _is_direct_message(message)
    if direct and not _get_bool_setting("allow_direct_messages", True):
        return False
    if not direct and not _get_bool_setting("allow_broadcasts", True):
        return False

    policy = _get_str_setting("response_policy", "mention_only").lower()
    text = str(message.get("text") or "").strip()
    if direct:
        return True
    if policy == "all_messages":
        return True
    if policy == "direct_only":
        return False
    return _message_mentions_tater(text)


def _session_key_for_message(message: Dict[str, Any]) -> str:
    sender = message.get("from") or {}
    node_id = str(sender.get("node_id") or sender.get("short_name") or sender.get("long_name") or "unknown").strip()
    node_id = node_id or "unknown"
    session_mode = _get_str_setting("session_mode", "node_channel").lower()
    if session_mode == "node":
        return node_id
    channel = _message_channel(message)
    return f"{node_id}:ch{channel}"


def _mesh_origin_for_message(message: Dict[str, Any]) -> Dict[str, Any]:
    sender = message.get("from") or {}
    channel = _message_channel(message)
    request_id = str(message.get("event_id") or message.get("message_id") or time.time())
    origin = {
        "platform": "meshtastic",
        "channel": channel,
        "chat_type": "direct" if _is_direct_message(message) else "channel",
        "node_id": str(sender.get("node_id") or "").strip(),
        "user": str(sender.get("long_name") or sender.get("short_name") or sender.get("node_id") or "mesh_user").strip(),
        "request_id": request_id,
    }
    return {k: v for k, v in origin.items() if v not in (None, "")}


def build_system_prompt() -> str:
    max_chars = max(40, _get_int_setting("max_outbound_length", DEFAULT_MAX_OUTBOUND_LENGTH))
    return (
        "You are replying over encrypted Meshtastic radio.\n"
        f"Keep the final answer under about {max_chars} ASCII characters whenever possible.\n"
        "Use plain ASCII text only. No markdown, no emoji, no tables, no long lists, and no code fences.\n"
        "Prefer one direct answer. If tools return too much information, summarize it for radio.\n"
        "Do not use IRC admin or moderation actions.\n"
    )


class BridgeClient:
    def __init__(self, *, base_url: str, api_token: str, timeout: float) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.api_token = api_token.strip()
        self.timeout = float(timeout)
        self.session = requests.Session()

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def _url(self, path: str) -> str:
        return urljoin(self.base_url, path.lstrip("/"))

    def get_status(self) -> Dict[str, Any]:
        response = self.session.get(self._url("/status"), headers=self._headers(), timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_messages(self, *, since_id: int, limit: int = 50) -> Dict[str, Any]:
        response = self.session.get(
            self._url("/messages"),
            headers=self._headers(),
            params={"since_id": int(max(0, since_id)), "limit": int(max(1, min(limit, 200)))},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get_channels(self) -> Dict[str, Any]:
        response = self.session.get(
            self._url("/channels"),
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def send_message(self, *, text: str, channel: int, destination: str) -> Dict[str, Any]:
        response = self.session.post(
            self._url("/send"),
            headers={**self._headers(), "Content-Type": "application/json"},
            json={
                "text": str(text or "").strip(),
                "channel": int(channel),
                "destination": str(destination or "broadcast").strip() or "broadcast",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()


class MeshtasticPortalRuntime:
    def __init__(self) -> None:
        self.bridge = BridgeClient(
            base_url=_get_str_setting("bridge_url", DEFAULT_BRIDGE_URL),
            api_token=_get_str_setting("api_token", ""),
            timeout=max(2.0, _get_float_setting("request_timeout_sec", DEFAULT_REQUEST_TIMEOUT_SECONDS)),
        )
        self.llm_client = get_llm_client_from_env()
        self.last_event_id = 0

    async def _prime_cursor(self) -> None:
        if _get_str_setting("resume_mode", "from_now").lower() != "from_now":
            return
        try:
            status = await asyncio.to_thread(self.bridge.get_status)
        except Exception as exc:
            logger.warning("[Meshtastic] Unable to prime cursor from bridge status: %s", exc)
            return
        try:
            self.last_event_id = int(status.get("latest_event_id") or 0)
        except Exception:
            self.last_event_id = 0

    async def _send_chunks(self, *, text: str, channel: int, destination: str) -> None:
        if int(channel) <= 0:
            logger.info("[Meshtastic] Skipping outbound reply on blocked channel %s", channel)
            return
        if not _channel_allowed(channel):
            logger.info("[Meshtastic] Skipping outbound reply on unallowed channel %s", channel)
            return
        for chunk in _shape_outbound_reply(text):
            await asyncio.to_thread(
                self.bridge.send_message,
                text=chunk,
                channel=channel,
                destination=destination,
            )

    def _reply_route_for_message(self, message: Dict[str, Any]) -> Optional[Tuple[int, str]]:
        channel = _message_channel(message)
        if not _channel_allowed(channel):
            return None
        sender = str(((message.get("from") or {}).get("node_id")) or "").strip()

        if _is_direct_message(message) and _get_bool_setting("allow_direct_messages", True) and sender:
            return channel, sender

        if _get_bool_setting("allow_broadcasts", True):
            return channel, "broadcast"

        if sender and _get_bool_setting("allow_direct_messages", True):
            return channel, sender
        return None

    async def _handle_inbound_message(self, message: Dict[str, Any]) -> None:
        text = str(message.get("text") or "").strip()
        if not text:
            return
        if not _should_respond_to_message(message):
            return

        session_key = _session_key_for_message(message)
        effective_request = _sanitize_request_text(text)
        _save_history(session_key, "user", effective_request)

        history_limit = _global_history_llm_limit()
        history = _load_history(session_key, history_limit)
        registry = dict(pr.get_verba_registry_snapshot() or {})
        origin = _mesh_origin_for_message(message)
        scope_value = f"pm:{session_key}" if _is_direct_message(message) else f"chan:{session_key}"
        agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)

        try:
            result = await run_hydra_turn(
                llm_client=self.llm_client,
                platform="meshtastic",
                history_messages=history,
                registry=registry,
                enabled_predicate=_get_plugin_enabled,
                context={},
                user_text=effective_request,
                scope=scope_value,
                origin=origin,
                redis_client=redis_client,
                max_rounds=agent_max_rounds,
                max_tool_calls=agent_max_tool_calls,
                platform_preamble=build_system_prompt(),
            )
        except Exception:
            logger.exception("[Meshtastic] Hydra turn failed")
            apology = "Sorry, I hit a problem while working on that."
            route = self._reply_route_for_message(message)
            if route:
                channel, destination = route
                try:
                    await self._send_chunks(text=apology, channel=channel, destination=destination)
                except Exception:
                    logger.exception("[Meshtastic] Failed to send fallback apology")
            _save_history(
                session_key,
                "assistant",
                {"marker": "plugin_response", "phase": "final", "content": apology},
            )
            return

        final_text = str(result.get("text") or "").strip()
        if not final_text:
            final_text = "I don't have anything useful to send back yet."

        route = self._reply_route_for_message(message)
        if route:
            channel, destination = route
            await self._send_chunks(text=final_text, channel=channel, destination=destination)
        _save_history(
            session_key,
            "assistant",
            {"marker": "plugin_response", "phase": "final", "content": final_text},
        )

    async def _poll_messages_worker(self, stop_event: Optional[threading.Event]) -> None:
        backoff = 1.0
        while not (stop_event and stop_event.is_set()):
            try:
                payload = await asyncio.to_thread(
                    self.bridge.get_messages,
                    since_id=self.last_event_id,
                    limit=50,
                )
                messages = payload.get("messages") or []
                for message in messages:
                    if not isinstance(message, dict):
                        continue
                    event_id = int(message.get("event_id") or 0)
                    if event_id > self.last_event_id:
                        self.last_event_id = event_id
                    if str(message.get("direction") or "inbound").lower() != "inbound":
                        continue
                    await self._handle_inbound_message(message)

                latest_id = int(payload.get("latest_event_id") or self.last_event_id or 0)
                self.last_event_id = max(self.last_event_id, latest_id)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[Meshtastic] Bridge poll failed: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2.0)
                continue

            interval = max(0.25, _get_float_setting("poll_interval_sec", DEFAULT_POLL_INTERVAL_SECONDS))
            await asyncio.sleep(interval)

    async def _notify_queue_worker(self, stop_event: Optional[threading.Event]) -> None:
        while not (stop_event and stop_event.is_set()):
            try:
                item_json = await asyncio.to_thread(redis_client.lpop, NOTIFY_QUEUE_KEY)
            except Exception:
                item_json = None

            if not item_json:
                await asyncio.sleep(NOTIFY_POLL_INTERVAL)
                continue

            try:
                item = json.loads(item_json)
            except Exception:
                logger.warning("[Meshtastic notifyq] invalid JSON item; skipping")
                continue

            if is_expired(item):
                continue

            targets = item.get("targets") or {}
            destination = (
                str(targets.get("node_id") or targets.get("destination") or targets.get("to") or "broadcast").strip()
                or "broadcast"
            )
            channel = _coalesce_channel_value(
                targets.get("channel"),
                targets.get("channel_index"),
            )

            title = str(item.get("title") or "").strip()
            message = str(item.get("message") or "").strip()
            payload = f"{title}: {message}" if title and message else (title or message)
            if not payload:
                continue

            try:
                await self._send_chunks(text=payload, channel=channel, destination=destination)
            except Exception:
                logger.exception("[Meshtastic notifyq] failed to send queued message")

    async def run(self, stop_event: Optional[threading.Event]) -> None:
        await self._prime_cursor()
        notify_task = asyncio.create_task(self._notify_queue_worker(stop_event))
        poll_task = asyncio.create_task(self._poll_messages_worker(stop_event))
        logger.info("[Meshtastic] Portal started")

        try:
            if stop_event:
                while not stop_event.is_set():
                    await asyncio.sleep(0.5)
            else:
                await asyncio.Event().wait()
        finally:
            for task in (notify_task, poll_task):
                if task and not task.done():
                    task.cancel()
            await asyncio.gather(notify_task, poll_task, return_exceptions=True)
            logger.info("[Meshtastic] Portal stopped")


def run(stop_event: Optional[threading.Event] = None) -> None:
    runtime = MeshtasticPortalRuntime()
    asyncio.run(runtime.run(stop_event))
