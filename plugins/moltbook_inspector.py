# plugins/moltbook_inspector.py
import json
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from plugin_base import ToolPlugin
from helpers import redis_client, get_tater_name

logger = logging.getLogger("moltbook_inspector")
logger.setLevel(logging.INFO)

# -------------------- Redis keys (must match moltbook_platform.py) --------------------
MOLT_SETTINGS_KEY = "moltbook_platform_settings"
MOLT_STATS_KEY = "tater:moltbook:stats"
MOLT_EVENTS_KEY = "tater:moltbook:events"

DM_CONV_INDEX_KEY = "tater:moltbook:dm:conversations"
DM_META_KEY_FMT = "tater:moltbook:dm:{cid}:meta"
DM_MSGS_KEY_FMT = "tater:moltbook:dm:{cid}:messages"


# -------------------- Small helpers --------------------
def _hgetall_str(key: str) -> Dict[str, str]:
    d = redis_client.hgetall(key) or {}
    out: Dict[str, str] = {}
    for k, v in d.items():
        out[str(k)] = "" if v is None else str(v)
    return out


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(str(v).strip())
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(str(v).strip())
    except Exception:
        return default


def _safe_json_loads(s: Any) -> Any:
    if not isinstance(s, str):
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _compact(text: str, n: int) -> str:
    t = (text or "").strip()
    if len(t) <= n:
        return t
    return t[: max(0, n - 1)].rstrip() + "…"


def _strip_md_for_tts(text: str) -> str:
    if not text:
        return ""
    out = str(text)
    out = re.sub(r"[`*_]{1,3}", "", out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _looks_like_tool_json(text: str) -> bool:
    t = (text or "").strip()
    if not (t.startswith("{") and t.endswith("}")):
        return False
    return '"function"' in t or '"arguments"' in t


def _dm_meta_key(cid: str) -> str:
    return DM_META_KEY_FMT.format(cid=cid)


def _dm_msgs_key(cid: str) -> str:
    return DM_MSGS_KEY_FMT.format(cid=cid)


def _read_events(limit: int) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit or 20), 200))
    raw = redis_client.lrange(MOLT_EVENTS_KEY, -limit, -1) or []
    out: List[Dict[str, Any]] = []
    for r in raw:
        try:
            obj = json.loads(r)
            if isinstance(obj, dict):
                out.append(obj)
        except Exception:
            continue
    return out


def _format_event_line(e: Dict[str, Any]) -> str:
    et = (e.get("type") or "event").strip()
    ts = str(e.get("ts") or "")
    url = e.get("url") or e.get("post_url") or ""
    cid = e.get("conversation_id") or ""
    pid = e.get("post_id") or e.get("id") or ""

    bits = [f"- [{et}]"]
    if ts:
        bits.append(f"ts={ts}")
    if pid:
        bits.append(f"id={pid}")
    if cid:
        bits.append(f"cid={cid}")
    if url:
        bits.append(str(url))
    return " ".join(bits)


def _get_registration_info() -> Dict[str, str]:
    s = _hgetall_str(MOLT_SETTINGS_KEY)
    return {
        "agent_id": s.get("agent_id", ""),
        "agent_name": s.get("agent_name", ""),
        "api_key_set": "true" if bool(s.get("api_key", "").strip()) else "false",
        "claim_url": s.get("claim_url", ""),
        "verification_code": s.get("verification_code", ""),
        "profile_url": s.get("profile_url", ""),
        "created_at": s.get("created_at", ""),
        "status": s.get("status", ""),
        "tweet_template": s.get("tweet_template", ""),
        "claim_message_template": s.get("claim_message_template", ""),
    }


def _get_platform_settings_subset() -> Dict[str, str]:
    s = _hgetall_str(MOLT_SETTINGS_KEY)
    # Keep this readable (these are commonly useful for debugging behavior)
    keys = [
        "mode",
        "dry_run",
        "feed_source",
        "feed_sort",
        "feed_limit",
        "check_interval_seconds",
        "dm_check_interval_seconds",
        "max_actions_per_cycle",
        "allow_comments",
        "allow_votes",
        "allow_self_posts",
        "self_post_chance_pct",
        "intro_post_on_claim",
        "reply_max_chars",
        "events_max",
        "dm_messages_max_per_conv",
    ]
    out: Dict[str, str] = {}
    for k in keys:
        if k in s and str(s.get(k) or "").strip() != "":
            out[k] = str(s.get(k) or "").strip()
    # redact secrets
    if "api_key" in s:
        out["api_key"] = "set" if str(s.get("api_key") or "").strip() else "missing"
    return out


def _get_stats() -> Dict[str, str]:
    return _hgetall_str(MOLT_STATS_KEY)


def _count_dm_conversations() -> int:
    try:
        return int(redis_client.scard(DM_CONV_INDEX_KEY) or 0)
    except Exception:
        return 0


def _list_dm_conversations(limit: int) -> List[Tuple[str, Dict[str, str]]]:
    """
    Returns list of (cid, meta_map) sorted by updated_ts desc.
    """
    limit = max(1, min(int(limit or 10), 50))
    try:
        cids = list(redis_client.smembers(DM_CONV_INDEX_KEY) or [])
    except Exception:
        cids = []

    metas: List[Tuple[str, Dict[str, str]]] = []
    for cid in cids:
        meta = _hgetall_str(_dm_meta_key(str(cid)))
        metas.append((str(cid), meta))

    metas.sort(key=lambda x: _safe_int(x[1].get("updated_ts"), 0), reverse=True)
    return metas[:limit]


def _load_dm_messages(cid: str, limit: int = 40) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit or 40), 200))
    raw = redis_client.lrange(_dm_msgs_key(cid), -limit, -1) or []
    msgs: List[Dict[str, Any]] = []
    for r in raw:
        try:
            obj = json.loads(r)
            if isinstance(obj, dict):
                msgs.append(obj)
        except Exception:
            continue
    return msgs


def _get_last_event_of_type(events: List[Dict[str, Any]], etype: str) -> Optional[Dict[str, Any]]:
    etype = (etype or "").strip().lower()
    for e in reversed(events or []):
        if str(e.get("type") or "").strip().lower() == etype:
            return e
    return None


def _summarize_mode(settings: Dict[str, str]) -> str:
    mode = (settings.get("mode") or "unknown").strip()
    dry_run = (settings.get("dry_run") or "").strip().lower() in ("1", "true", "yes", "on")
    if not mode or mode == "unknown":
        return "Mode: unknown"
    if dry_run:
        return f"Mode: {mode} (dry_run ON — no writes)"
    return f"Mode: {mode} (live writes enabled)"


def _format_settings_lines(settings: Dict[str, str]) -> str:
    if not settings:
        return "- (no platform settings stored yet)"
    lines = []
    for k in sorted(settings.keys()):
        lines.append(f"- {k}: {settings[k]}")
    return "\n".join(lines)


# -------------------- LLM helpers --------------------
async def _llm_chat(llm_client, messages: List[Dict[str, str]], timeout: int = 60) -> str:
    """
    Uses the common llm_client.chat(messages, timeout=...) shape used elsewhere in Tater.
    Falls back gracefully if the client differs.
    """
    if not llm_client:
        return ""
    try:
        resp = await llm_client.chat(messages, timeout=timeout)
    except TypeError:
        # Some wrappers accept kwargs differently
        resp = await llm_client.chat(messages=messages, timeout=timeout)
    except Exception:
        try:
            resp = await llm_client.chat(messages=messages)
        except Exception:
            return ""

    out = (resp.get("message", {}) or {}).get("content", "") or ""
    return (out or "").strip()


def _llm_system_identity() -> str:
    first, last = get_tater_name()
    return (
        f"You are {first} {last}, a friendly assistant helping your human manage Moltbook.\n"
        "Hard rules:\n"
        "- Do NOT output JSON tool calls.\n"
        "- Do NOT output raw code.\n"
        "- Keep responses practical and human-friendly.\n"
    )


async def _llm_summarize_dm(llm_client, cid: str, meta: Dict[str, str], msgs: List[Dict[str, Any]]) -> str:
    """
    Summarize what the DM is about in 2-4 sentences.
    """
    if not llm_client:
        return "DM summary unavailable (LLM client missing)."

    lines = []
    for m in msgs[-20:]:
        frm = (m.get("from") or "unknown")
        txt = (m.get("text") or "")
        if txt:
            lines.append(f"{frm}: {txt}")

    participants = meta.get("participants") or meta.get("users") or meta.get("members") or ""
    if participants and isinstance(participants, str) and participants.strip().startswith("["):
        parsed = _safe_json_loads(participants)
        if parsed is not None:
            participants = json.dumps(parsed, ensure_ascii=False)

    prompt = (
        "Summarize this Moltbook DM conversation in 2-4 sentences.\n"
        "- Mention the main topic(s).\n"
        "- Mention any open questions or next steps.\n"
        "- No JSON.\n\n"
        f"Conversation ID: {cid}\n"
        f"Participants: {participants}\n"
        "Recent messages:\n"
        + "\n".join(lines)
    )

    out = await _llm_chat(
        llm_client,
        [
            {"role": "system", "content": _llm_system_identity()},
            {"role": "user", "content": prompt},
        ],
        timeout=60,
    )
    if _looks_like_tool_json(out):
        return "DM summary unavailable (model returned tool JSON)."
    return _compact(out, 900)


async def _llm_draft_dm_reply(llm_client, cid: str, meta: Dict[str, str], msgs: List[Dict[str, Any]], max_chars: int) -> str:
    """
    Draft a reply to the latest user message in the DM conversation.
    """
    if not llm_client:
        return "Draft unavailable (LLM client missing)."

    max_chars = max(120, min(int(max_chars or 600), 1200))

    lines = []
    for m in msgs[-16:]:
        frm = (m.get("from") or "unknown")
        txt = (m.get("text") or "")
        if txt:
            lines.append(f"{frm}: {txt}")

    participants = meta.get("participants") or meta.get("users") or meta.get("members") or ""
    if participants and isinstance(participants, str) and participants.strip().startswith("["):
        parsed = _safe_json_loads(participants)
        if parsed is not None:
            participants = json.dumps(parsed, ensure_ascii=False)

    prompt = (
        "Write a friendly DM reply to keep the conversation flowing.\n"
        "- Be warm and natural.\n"
        "- Answer what you can based on the context.\n"
        "- Ask ONE good follow-up question if it helps.\n"
        "- Keep it short.\n"
        "- No JSON.\n"
        f"- Hard limit: {max_chars} characters.\n\n"
        f"Conversation ID: {cid}\n"
        f"Participants: {participants}\n"
        "Recent messages:\n"
        + "\n".join(lines)
    )

    out = await _llm_chat(
        llm_client,
        [
            {"role": "system", "content": _llm_system_identity()},
            {"role": "user", "content": prompt},
        ],
        timeout=60,
    )
    if _looks_like_tool_json(out):
        return "Draft unavailable (model returned tool JSON)."
    return _compact(out, max_chars)


async def _llm_brief_status(llm_client, reg: Dict[str, str], stats: Dict[str, str], settings: Dict[str, str], events: List[Dict[str, Any]]) -> str:
    """
    Produce a friendly 3-6 line “what’s going on” summary for the human.
    """
    if not llm_client:
        return ""

    last_post_url = (stats.get("last_post_url") or "").strip()
    last_post_evt = _get_last_event_of_type(events, "post_created")
    last_comment_evt = _get_last_event_of_type(events, "comment")
    last_dm_evt = _get_last_event_of_type(events, "dm_received")

    prompt = (
        "Write a short, friendly status update for the human about Moltbook activity.\n"
        "- 3 to 6 bullet points.\n"
        "- Highlight: mode/dry_run, last activity, and if there are new DMs.\n"
        "- No JSON.\n\n"
        f"Agent: {reg.get('agent_name') or '(unknown)'}\n"
        f"Profile: {reg.get('profile_url') or '(none)'}\n"
        f"{_summarize_mode(settings)}\n"
        f"Posts: {stats.get('posts_created', '0')}  Comments: {stats.get('comments_created', '0')}  Votes: {stats.get('votes_cast', '0')}\n"
        f"DMs received: {stats.get('dms_received', '0')}  DMs sent: {stats.get('dms_sent', '0')}\n"
        f"Last post url (if any): {last_post_url or '(none)'}\n"
        f"Last post event: {json.dumps(last_post_evt, ensure_ascii=False) if last_post_evt else '(none)'}\n"
        f"Last comment event: {json.dumps(last_comment_evt, ensure_ascii=False) if last_comment_evt else '(none)'}\n"
        f"Last dm_received event: {json.dumps(last_dm_evt, ensure_ascii=False) if last_dm_evt else '(none)'}\n"
    )

    out = await _llm_chat(
        llm_client,
        [
            {"role": "system", "content": _llm_system_identity()},
            {"role": "user", "content": prompt},
        ],
        timeout=45,
    )
    if _looks_like_tool_json(out):
        return ""
    return _compact(out.strip(), 900)


# -------------------- Plugin --------------------
class MoltbookInspectorPlugin(ToolPlugin):
    name = "moltbook_inspector"
    plugin_name = "Moltbook Inspector"
    version = "1.0.1"  # bumped
    min_tater_version = "50"

    usage = (
        "{\n"
        '  "function": "moltbook_inspector",\n'
        '  "arguments": {\n'
        '    "action": "status|status_ai|registration|settings|stats|events|dm_list|dm_summary|dm_reply_draft",\n'
        '    "limit": 20,\n'
        '    "conversation_id": "optional",\n'
        '    "reply_max_chars": 600\n'
        "  }\n"
        "}\n"
    )

    description = (
        "Inspect Moltbook platform state stored in Redis and (optionally) generate friendly AI summaries/draft replies. "
        "Use this to see recent events, registration info, stats, and DM conversation summaries."
    )
    plugin_dec = "Reads Moltbook platform data stored in Redis and summarizes it for the user."
    pretty_name = "Checking Moltbook"
    settings_category = "Moltbook"

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking Moltbook now. "
        "Only output that message."
    )

    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "xbmc"]

    async def _run_action(self, args: Dict[str, Any], llm_client=None) -> str:
        args = args or {}
        action = (args.get("action") or "status").strip().lower()
        limit = _safe_int(args.get("limit"), 20)
        cid = (args.get("conversation_id") or "").strip()
        reply_max_chars = _safe_int(args.get("reply_max_chars"), 600)

        reg = _get_registration_info()
        stats = _get_stats()
        settings = _get_platform_settings_subset()
        events = _read_events(limit=max(20, min(limit, 200)))  # use slightly more for AI summaries

        if action == "status":
            dm_count = _count_dm_conversations()
            claimed = (stats.get("claimed") or "").lower() in ("1", "true", "yes", "on")
            agent_status = stats.get("agent_status") or reg.get("status") or "unknown"

            lines = [
                "🦞 Moltbook status",
                f"- Agent: {reg.get('agent_name') or '(unknown)'}",
                f"- Status: {agent_status} ({'claimed' if claimed else 'not claimed'})",
                f"- Profile: {reg.get('profile_url') or '(none)'}",
                f"- {_summarize_mode(settings)}",
                f"- Posts: {stats.get('posts_created', '0')}",
                f"- Comments: {stats.get('comments_created', '0')}",
                f"- Votes: {stats.get('votes_cast', '0')}",
                f"- DMs received: {stats.get('dms_received', '0')}",
                f"- DMs sent: {stats.get('dms_sent', '0')}",
                f"- DM conversations: {dm_count}",
                f"- Tool calls blocked: {stats.get('tool_call_blocked', '0')}",
            ]
            last_post_url = (stats.get("last_post_url", "") or "").strip()
            if last_post_url:
                lines.append(f"- Last post: {last_post_url}")

            return "\n".join(lines).strip()

        if action == "status_ai":
            # Friendly “what’s going on” summary for better conversation flow
            async def inner():
                out = await _llm_brief_status(llm_client, reg, stats, settings, events)
                if out:
                    return f"🦞 Moltbook check-in\n{out}".strip()
                # fallback
                return await self._run_action({"action": "status", "limit": limit}, llm_client)

            return await self._hybrid(inner)

        if action == "registration":
            lines = [
                "🦞 Moltbook registration",
                f"- Agent name: {reg.get('agent_name') or '(unknown)'}",
                f"- Agent id: {reg.get('agent_id') or '(unknown)'}",
                f"- Created at: {reg.get('created_at') or '(unknown)'}",
                f"- API key stored: {reg.get('api_key_set')}",
            ]

            claim_url = (reg.get("claim_url", "") or "").strip()
            vcode = (reg.get("verification_code", "") or "").strip()
            if claim_url:
                lines.append(f"- Claim URL: {claim_url}")
            if vcode:
                lines.append(f"- Verification code: {vcode}")

            tweet = (reg.get("tweet_template", "") or "").strip()
            if tweet:
                lines.append("\nTweet template:\n" + tweet)

            msg_tmpl = (reg.get("claim_message_template", "") or "").strip()
            if msg_tmpl:
                lines.append("\nClaim message template:\n" + msg_tmpl)

            return "\n".join(lines).strip()

        if action == "settings":
            lines = [
                "🦞 Moltbook platform settings (from Redis)",
                _format_settings_lines(settings),
            ]
            return "\n".join(lines).strip()

        if action == "stats":
            dm_count = _count_dm_conversations()
            fields = [
                ("agent_status", stats.get("agent_status", "")),
                ("claimed", stats.get("claimed", "")),
                ("posts_created", stats.get("posts_created", "0")),
                ("comments_created", stats.get("comments_created", "0")),
                ("votes_cast", stats.get("votes_cast", "0")),
                ("dms_received", stats.get("dms_received", "0")),
                ("dms_sent", stats.get("dms_sent", "0")),
                ("tool_call_blocked", stats.get("tool_call_blocked", "0")),
                ("last_post_url", stats.get("last_post_url", "")),
                ("last_activity_ts", stats.get("last_activity_ts", "")),
            ]
            lines = ["🦞 Moltbook stats"] + [f"- {k}: {v}" for k, v in fields if str(v).strip() != ""]
            lines.append(f"- dm_conversations: {dm_count}")
            lines.append(f"- stats_fields_total: {len(stats)}")
            return "\n".join(lines).strip()

        if action == "events":
            ev = _read_events(limit=limit)
            if not ev:
                return "No Moltbook events found yet."
            lines = [f"🦞 Recent Moltbook events (last {len(ev)}):"]
            lines.extend(_format_event_line(e) for e in ev)
            return "\n".join(lines).strip()

        if action == "dm_list":
            items = _list_dm_conversations(limit=limit)
            if not items:
                return "No Moltbook DM conversations stored yet."
            lines = [f"🦞 DM conversations (top {len(items)} by recent activity):"]
            for cid2, meta in items:
                updated = meta.get("updated_ts", "")
                last_seen = meta.get("last_seen_ts", "")
                new_last_poll = meta.get("new_messages_last_poll", "")
                lines.append(f"- cid={cid2} updated_ts={updated} last_seen_ts={last_seen} new_last_poll={new_last_poll}")
            lines.append("\nTip: Use action=dm_summary (or dm_reply_draft) with conversation_id.")
            return "\n".join(lines).strip()

        if action == "dm_summary":
            # If cid omitted, summarize the most recent conversation
            if not cid:
                items = _list_dm_conversations(limit=1)
                if not items:
                    return "No Moltbook DM conversations stored yet."
                cid, meta = items[0]
            else:
                meta = _hgetall_str(_dm_meta_key(cid))
                if not meta:
                    return f"No DM metadata found for conversation_id={cid}"

            msgs = _load_dm_messages(cid, limit=60)
            if not msgs:
                return f"No stored messages found for conversation_id={cid}"

            async def inner():
                summary = await _llm_summarize_dm(llm_client, cid, meta, msgs)
                return (
                    f"🦞 DM summary\n"
                    f"- conversation_id: {cid}\n"
                    f"- messages_loaded: {len(msgs)}\n\n"
                    f"{summary}"
                ).strip()

            return await self._hybrid(inner)

        if action == "dm_reply_draft":
            # Draft a reply to the latest DM message to improve conversation flow
            if not cid:
                items = _list_dm_conversations(limit=1)
                if not items:
                    return "No Moltbook DM conversations stored yet."
                cid, meta = items[0]
            else:
                meta = _hgetall_str(_dm_meta_key(cid))
                if not meta:
                    return f"No DM metadata found for conversation_id={cid}"

            msgs = _load_dm_messages(cid, limit=80)
            if not msgs:
                return f"No stored messages found for conversation_id={cid}"

            async def inner():
                draft = await _llm_draft_dm_reply(llm_client, cid, meta, msgs, reply_max_chars)
                return (
                    f"🦞 DM reply draft\n"
                    f"- conversation_id: {cid}\n"
                    f"- messages_loaded: {len(msgs)}\n\n"
                    f"{draft}"
                ).strip()

            return await self._hybrid(inner)

        return "Unknown action. Use: status | status_ai | registration | settings | stats | events | dm_list | dm_summary | dm_reply_draft"

    async def _hybrid(self, coro_fn):
        """
        Run an async inner() in a way that works both inside and outside an event loop.
        """
        try:
            asyncio.get_running_loop()
            return await coro_fn()
        except RuntimeError:
            return asyncio.run(coro_fn())

    # -------- Platform handlers --------
    async def handle_discord(self, message, args, llm_client):
        return await self._run_action(args, llm_client)

    async def handle_webui(self, args, llm_client):
        return await self._run_action(args, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        out = await self._run_action(args, llm_client)
        out = _strip_md_for_tts(out)
        out = _compact(out, 900)
        return f"{user}: {out}"

    async def handle_homeassistant(self, args, llm_client):
        out = await self._run_action(args, llm_client)
        return (out or "").strip()

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self._run_action(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        out = await self._run_action(args, llm_client)
        out = _strip_md_for_tts(out)
        return _compact(out, 600)

    async def handle_xbmc(self, args, llm_client):
        out = await self._run_action(args, llm_client)
        return (out or "").strip()


plugin = MoltbookInspectorPlugin()
