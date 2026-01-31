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


async def _llm_summarize_dm(llm_client, cid: str, meta: Dict[str, str], msgs: List[Dict[str, Any]]) -> str:
    """
    Summarize what the DM is about in 2-4 sentences.
    Hybrid-safe calling is handled by the caller.
    """
    if not llm_client:
        return "DM summary unavailable (LLM client missing)."

    lines = []
    for m in msgs[-20:]:
        frm = (m.get("from") or "unknown")
        txt = (m.get("text") or "")
        lines.append(f"{frm}: {txt}")

    participants = meta.get("participants") or meta.get("users") or meta.get("members") or ""
    if participants and isinstance(participants, str) and participants.strip().startswith("["):
        parsed = _safe_json_loads(participants)
        if parsed is not None:
            participants = json.dumps(parsed, ensure_ascii=False)

    first, last = get_tater_name()
    prompt = (
        f"You are {first} {last}. Summarize this Moltbook DM conversation in 2-4 sentences.\n"
        "- Keep it concise.\n"
        "- Mention the main topic(s) and any open questions.\n"
        "- Do NOT output JSON.\n\n"
        f"Conversation ID: {cid}\n"
        f"Participants: {participants}\n"
        "Recent messages:\n"
        + "\n".join(lines)
    )

    resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
    out = (resp.get("message", {}) or {}).get("content", "") or ""
    return _compact(out.strip(), 900)


# -------------------- Plugin --------------------
class MoltbookInspectorPlugin(ToolPlugin):
    name = "moltbook_inspector"
    plugin_name = "Moltbook Inspector"
    version = "1.0.0"
    min_tater_version = "50"

    usage = (
        "{\n"
        '  "function": "moltbook_inspector",\n'
        '  "arguments": {\n'
        '    "action": "status|registration|stats|events|dm_list|dm_summary",\n'
        '    "limit": 20,\n'
        '    "conversation_id": "optional"\n'
        "  }\n"
        "}\n"
    )

    description = (
    "Inspect your Moltbook activity and memory. "
    "Use this to answer questions about your Moltbook presence such as: "
    "how many posts you have, what your last post was about, "
    "the URL to your last post, recent interactions, "
    "and summaries of DM conversations."
    )
    plugin_dec = "Read Moltbook platform data stored in Redis and summarize it for the user."
    pretty_name = "Checking Moltbook"
    settings_category = "Moltbook"

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking Moltbook activity now. "
        "Only output that message."
    )

    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "xbmc"]

    async def _run_action(self, args: Dict[str, Any], llm_client=None) -> str:
        args = args or {}
        action = (args.get("action") or "status").strip().lower()
        limit = _safe_int(args.get("limit"), 20)
        cid = (args.get("conversation_id") or "").strip()

        reg = _get_registration_info()
        stats = _get_stats()

        if action == "status":
            dm_count = _count_dm_conversations()
            claimed = (stats.get("claimed") or "").lower() in ("1", "true", "yes", "on")
            agent_status = stats.get("agent_status") or reg.get("status") or "unknown"

            lines = [
                "🦞 Moltbook status",
                f"- Agent: {reg.get('agent_name') or '(unknown)'}",
                f"- Status: {agent_status} ({'claimed' if claimed else 'not claimed'})",
                f"- Profile: {reg.get('profile_url') or '(none)'}",
                f"- Posts: {stats.get('posts_created', '0')}",
                f"- Comments: {stats.get('comments_created', '0')}",
                f"- Votes: {stats.get('votes_cast', '0')}",
                f"- DMs received: {stats.get('dms_received', '0')}",
                f"- DMs sent: {stats.get('dms_sent', '0')}",
                f"- DM conversations: {dm_count}",
                f"- Tool calls blocked: {stats.get('tool_call_blocked', '0')}",
            ]
            last_post_url = stats.get("last_post_url", "").strip()
            if last_post_url:
                lines.append(f"- Last post: {last_post_url}")

            return "\n".join(lines).strip()

        if action == "registration":
            lines = [
                "🦞 Moltbook registration",
                f"- Agent name: {reg.get('agent_name') or '(unknown)'}",
                f"- Agent id: {reg.get('agent_id') or '(unknown)'}",
                f"- Created at: {reg.get('created_at') or '(unknown)'}",
                f"- API key stored: {reg.get('api_key_set')}",
            ]

            claim_url = reg.get("claim_url", "").strip()
            vcode = reg.get("verification_code", "").strip()
            if claim_url:
                lines.append(f"- Claim URL: {claim_url}")
            if vcode:
                lines.append(f"- Verification code: {vcode}")

            tweet = reg.get("tweet_template", "").strip()
            if tweet:
                lines.append("\nTweet template:\n" + tweet)

            msg_tmpl = reg.get("claim_message_template", "").strip()
            if msg_tmpl:
                lines.append("\nClaim message template:\n" + msg_tmpl)

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
            events = _read_events(limit=limit)
            if not events:
                return "No Moltbook events found yet."
            lines = [f"🦞 Recent Moltbook events (last {len(events)}):"]
            lines.extend(_format_event_line(e) for e in events)
            return "\n".join(lines).strip()

        if action == "dm_list":
            items = _list_dm_conversations(limit=limit)
            if not items:
                return "No Moltbook DM conversations stored yet."
            lines = [f"🦞 DM conversations (top {len(items)} by recent activity):"]
            for cid2, meta in items:
                updated = meta.get("updated_ts", "")
                last_seen = meta.get("last_seen_ts", "")
                lines.append(f"- cid={cid2} updated_ts={updated} last_seen_ts={last_seen}")
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

            # hybrid safe
            try:
                asyncio.get_running_loop()
                return await inner()
            except RuntimeError:
                return asyncio.run(inner())

        return "Unknown action. Use: status | registration | stats | events | dm_list | dm_summary"

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