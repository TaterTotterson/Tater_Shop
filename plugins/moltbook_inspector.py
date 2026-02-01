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

# Internal defaults (not exposed in usage)
_DEFAULT_EVENTS_LIMIT = 25
_DEFAULT_DM_LIST_LIMIT = 12
_DEFAULT_DM_MESSAGES_LOAD = 90
_DEFAULT_REPLY_MAX_CHARS = 650


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
    et = str(e.get("type") or "event").strip()
    ts = str(e.get("ts") or "").strip()
    url = str(e.get("url") or e.get("post_url") or "").strip()
    cid = str(e.get("conversation_id") or "").strip()
    pid = str(e.get("post_id") or e.get("id") or "").strip()

    bits = [f"[{et}]"]
    if ts:
        bits.append(f"ts={ts}")
    if pid:
        bits.append(f"id={pid}")
    if cid:
        bits.append(f"cid={cid}")
    if url:
        bits.append(url)
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
    keys = [
        "mode",
        "dry_run",
        "feed_source",
        "feed_sort",
        "feed_limit",
        "check_interval_seconds",
        "dm_check_interval_seconds",
        "thread_check_interval_seconds",
        "max_actions_per_cycle",
        "max_dm_replies_per_cycle",
        "allow_comments",
        "allow_votes",
        "allow_self_posts",
        "self_post_chance_pct",
        "intro_post_on_claim",
        "reply_max_chars",
        "events_max",
        "dm_messages_max_per_conv",
        "post_cooldown_seconds",
        "comment_cooldown_seconds",
        "dm_reply_cooldown_seconds",
        "dm_reply_min_age_seconds",
        "max_thread_replies_per_cycle",
        "thread_reply_cooldown_seconds",
    ]
    out: Dict[str, str] = {}
    for k in keys:
        if k in s and str(s.get(k) or "").strip() != "":
            out[k] = str(s.get(k) or "").strip()

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


def _most_recent_dm_conversation() -> Optional[Tuple[str, Dict[str, str]]]:
    items = _list_dm_conversations(limit=1)
    return items[0] if items else None


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
        return "unknown"
    if dry_run:
        return f"{mode} (dry_run on)"
    return f"{mode} (live writes)"


def _extract_last_post_from_events(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    evt = _get_last_event_of_type(events, "post_created")
    if not evt:
        return None

    title = str(evt.get("title") or "").strip()
    submolt = str(evt.get("submolt") or "").strip()
    url = str(evt.get("url") or evt.get("post_url") or "").strip()
    post_id = str(evt.get("post_id") or evt.get("id") or "").strip()
    summary = str(evt.get("summary") or "").strip()

    if not (title or summary or url or post_id):
        return None

    return {
        "title": title,
        "submolt": submolt,
        "url": url,
        "post_id": post_id,
        "summary": summary,
        "ts": evt.get("ts"),
        "raw": evt,
    }


# -------------------- LLM helpers --------------------
async def _llm_chat(llm_client, messages: List[Dict[str, str]], timeout: int = 60) -> str:
    if not llm_client:
        return ""
    try:
        resp = await llm_client.chat(messages, timeout=timeout)
    except TypeError:
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
        "- Keep responses natural and conversation-friendly.\n"
        "- Do not invent facts. Use only the data provided.\n"
    )


def _action_style_instructions(action: str) -> str:
    """
    Tight guidance per action so output feels like chat instead of a report.
    """
    a = (action or "").strip().lower()
    if a == "status":
        return (
            "Write a short check-in (2–5 lines). "
            "Mention mode, whether DMs look active, and what the agent most recently did. "
            "If there’s a last post link, include it naturally."
        )
    if a == "last_post":
        return (
            "Answer like a person in chat. "
            "In 2–4 sentences: what the last post was about (topic + vibe), and include the link. "
            "Avoid labels like 'Title:' or 'Submolt:' unless it helps readability."
        )
    if a == "events":
        return (
            "Give a quick summary of what kinds of events happened recently (1–3 sentences), "
            "then show up to 10 of the most recent events as short bullet lines."
        )
    if a == "dm_list":
        return (
            "In 1–2 sentences, say how many DM threads exist and whether any look new. "
            "Then list up to 8 threads with a short hint each (no IDs unless necessary)."
        )
    if a == "dm_summary":
        return (
            "Summarize the most recent DM conversation in 2–4 sentences. "
            "Mention topic, tone, and any next step."
        )
    if a == "dm_reply_draft":
        return (
            "Write a friendly reply draft (under 650 chars). "
            "Be warm and helpful. Ask one good follow-up question if useful."
        )
    if a == "registration":
        return (
            "Explain registration state in plain English. "
            "If the agent isn't claimed or is missing config, say what's missing and what to do next."
        )
    if a == "settings":
        return (
            "Summarize settings in chat form (3–8 bullets max). "
            "Call out mode/dry_run and a few important knobs. Do not dump every key."
        )
    if a == "stats":
        return (
            "Summarize the stats in a friendly way (3–7 bullets). "
            "Mention posts/comments/votes and DM counts, plus last activity if available."
        )
    return "Respond naturally and briefly. Include links when available. No JSON."


async def _llm_render_action(llm_client, action: str, payload: Dict[str, Any], timeout: int = 50) -> str:
    """
    Convert payload into a conversational response.
    """
    if not llm_client:
        return ""

    prompt = (
        f"Action: {action}\n"
        f"Instructions: {_action_style_instructions(action)}\n\n"
        "Data (JSON):\n"
        f"{json.dumps(payload, ensure_ascii=False)}\n"
    )

    out = await _llm_chat(
        llm_client,
        [
            {"role": "system", "content": _llm_system_identity()},
            {"role": "user", "content": prompt},
        ],
        timeout=timeout,
    )

    if _looks_like_tool_json(out):
        return ""

    return _compact(out.strip(), 1200)


# -------------------- Plugin --------------------
class MoltbookInspectorPlugin(ToolPlugin):
    name = "moltbook_inspector"
    plugin_name = "Moltbook Inspector"
    version = "1.0.4"
    min_tater_version = "50"

    # Minimal usage: only action
    usage = (
        "{\n"
        '  "function": "moltbook_inspector",\n'
        '  "arguments": {\n'
        '    "action": "status|last_post|registration|settings|stats|events|dm_list|dm_summary|dm_reply_draft"\n'
        "  }\n"
        "}\n"
    )

    description = (
    "Use this to check Moltbook (a social feed for AI assistants). It can give you a quick overview of recent activity, "
    "tell you what Tater posted most recently (with a link when available), and help summarize or draft replies to the newest DM thread. "
    "Prefer concise, human-friendly answers over raw logs."
    )

    plugin_dec = "Reads Moltbook platform state from Redis and summarizes it naturally for the user."
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

        reg = _get_registration_info()
        stats = _get_stats()
        settings = _get_platform_settings_subset()
        events = _read_events(limit=_DEFAULT_EVENTS_LIMIT)

        # Build payloads per action
        if action == "status":
            last_post = _extract_last_post_from_events(events)
            last_dm = _get_last_event_of_type(events, "dm_received")
            payload = {
                "agent_name": reg.get("agent_name") or "",
                "profile_url": reg.get("profile_url") or "",
                "mode": _summarize_mode(settings),
                "counts": {
                    "posts_created": stats.get("posts_created", "0"),
                    "comments_created": stats.get("comments_created", "0"),
                    "votes_cast": stats.get("votes_cast", "0"),
                    "dms_received": stats.get("dms_received", "0"),
                    "dms_sent": stats.get("dms_sent", "0"),
                    "dm_conversations": _count_dm_conversations(),
                },
                "last_activity_ts": stats.get("last_activity_ts", ""),
                "last_post": last_post or {},
                "last_dm_event": last_dm or {},
                "dry_run": settings.get("dry_run", ""),
            }

            async def inner():
                rendered = await _llm_render_action(llm_client, action, payload, timeout=45)
                if rendered:
                    return rendered
                # fallback (still readable)
                lines = [
                    f"Agent: {payload['agent_name'] or '(unknown)'}",
                    f"Mode: {payload['mode']}",
                    f"Posts: {payload['counts']['posts_created']}  Comments: {payload['counts']['comments_created']}  Votes: {payload['counts']['votes_cast']}",
                    f"DMs: {payload['counts']['dms_received']} received / {payload['counts']['dms_sent']} sent  (threads: {payload['counts']['dm_conversations']})",
                ]
                if payload.get("profile_url"):
                    lines.append(f"Profile: {payload['profile_url']}")
                lp = payload.get("last_post") or {}
                if lp.get("title") or lp.get("url"):
                    lines.append(f"Last post: {lp.get('title') or '(title unknown)'}")
                    if lp.get("url"):
                        lines.append(lp["url"])
                return "\n".join(lines).strip()

            return await self._hybrid(inner)

        if action == "last_post":
            last_post = _extract_last_post_from_events(events)
            stats_url = (stats.get("last_post_url") or "").strip()

            payload = {
                "agent_name": reg.get("agent_name") or "",
                "last_post": last_post or {},
                "fallback_last_post_url": stats_url,
            }

            async def inner():
                if not last_post and not stats_url:
                    rendered = await _llm_render_action(
                        llm_client,
                        action,
                        {"agent_name": payload["agent_name"], "last_post": {}, "fallback_last_post_url": ""},
                        timeout=35,
                    )
                    return rendered or "I don’t see any recorded posts yet."

                # ensure URL exists if we only have stats_url
                if last_post and not last_post.get("url") and stats_url:
                    last_post["url"] = stats_url
                    payload["last_post"] = last_post

                rendered = await _llm_render_action(llm_client, action, payload, timeout=35)
                if rendered:
                    return rendered

                # fallback
                lp = payload.get("last_post") or {}
                url = (lp.get("url") or payload.get("fallback_last_post_url") or "").strip()
                title = (lp.get("title") or "").strip()
                about = (lp.get("summary") or "").strip()
                parts = []
                if title:
                    parts.append(f"Last post: {title}")
                if about:
                    parts.append(about)
                if url:
                    parts.append(url)
                return "\n".join(parts).strip() if parts else (url or "I found a last post link, but no details.")

            return await self._hybrid(inner)

        if action == "registration":
            payload = {"registration": reg}
            async def inner():
                rendered = await _llm_render_action(llm_client, action, payload, timeout=40)
                if rendered:
                    return rendered
                # fallback
                lines = [
                    f"Agent: {reg.get('agent_name') or '(unknown)'}",
                    f"Agent id: {reg.get('agent_id') or '(unknown)'}",
                    f"Created: {reg.get('created_at') or '(unknown)'}",
                    f"API key stored: {reg.get('api_key_set')}",
                ]
                if reg.get("claim_url"):
                    lines.append(f"Claim URL: {reg['claim_url']}")
                if reg.get("verification_code"):
                    lines.append(f"Verification code: {reg['verification_code']}")
                return "\n".join(lines).strip()
            return await self._hybrid(inner)

        if action == "settings":
            payload = {"settings": settings}
            async def inner():
                rendered = await _llm_render_action(llm_client, action, payload, timeout=40)
                if rendered:
                    return rendered
                # fallback (trim)
                keep = ["mode", "dry_run", "feed_source", "feed_sort", "feed_limit", "check_interval_seconds", "dm_check_interval_seconds", "max_actions_per_cycle", "reply_max_chars", "api_key"]
                lines = []
                for k in keep:
                    if k in settings and str(settings.get(k) or "").strip():
                        lines.append(f"- {k}: {settings.get(k)}")
                return "Settings:\n" + ("\n".join(lines) if lines else "- (none found)")
            return await self._hybrid(inner)

        if action == "stats":
            payload = {
                "stats": stats,
                "dm_conversations": _count_dm_conversations(),
                "mode": _summarize_mode(settings),
                "profile_url": reg.get("profile_url") or "",
                "agent_name": reg.get("agent_name") or "",
            }
            async def inner():
                rendered = await _llm_render_action(llm_client, action, payload, timeout=40)
                if rendered:
                    return rendered
                # fallback
                lines = [
                    f"Posts: {stats.get('posts_created','0')}",
                    f"Comments: {stats.get('comments_created','0')}",
                    f"Votes: {stats.get('votes_cast','0')}",
                    f"DMs: {stats.get('dms_received','0')} received / {stats.get('dms_sent','0')} sent",
                ]
                if stats.get("last_activity_ts"):
                    lines.append(f"Last activity: {stats.get('last_activity_ts')}")
                if stats.get("last_post_url"):
                    lines.append(f"Last post link: {stats.get('last_post_url')}")
                return "\n".join(lines).strip()
            return await self._hybrid(inner)

        if action == "events":
            ev = _read_events(limit=_DEFAULT_EVENTS_LIMIT)
            payload = {"events": ev[-_DEFAULT_EVENTS_LIMIT:]}
            async def inner():
                rendered = await _llm_render_action(llm_client, action, payload, timeout=45)
                if rendered:
                    return rendered
                # fallback: list last 10
                if not ev:
                    return "No Moltbook events found yet."
                last10 = ev[-10:]
                return "Recent events:\n" + "\n".join(f"- {_format_event_line(e)}" for e in last10)
            return await self._hybrid(inner)

        if action == "dm_list":
            items = _list_dm_conversations(limit=_DEFAULT_DM_LIST_LIMIT)
            payload = {"dm_threads": [{"cid": cid, "meta": meta} for cid, meta in items]}
            async def inner():
                rendered = await _llm_render_action(llm_client, action, payload, timeout=45)
                if rendered:
                    return rendered
                if not items:
                    return "No Moltbook DM conversations stored yet."
                lines = []
                for cid, meta in items[:8]:
                    updated = meta.get("updated_ts", "")
                    new_last = meta.get("new_messages_last_poll", "")
                    lines.append(f"- updated={updated} new={new_last} cid={cid}")
                return "DM threads:\n" + "\n".join(lines)
            return await self._hybrid(inner)

        if action == "dm_summary":
            recent = _most_recent_dm_conversation()
            if not recent:
                return "No Moltbook DM conversations stored yet."
            cid, meta = recent
            msgs = _load_dm_messages(cid, limit=min(140, _DEFAULT_DM_MESSAGES_LOAD))
            payload = {"conversation": {"cid": cid, "meta": meta, "recent_messages": msgs[-25:]}}
            async def inner():
                rendered = await _llm_render_action(llm_client, action, payload, timeout=55)
                if rendered:
                    return rendered
                return "I found a DM thread, but couldn’t summarize it (LLM unavailable)."
            return await self._hybrid(inner)

        if action == "dm_reply_draft":
            recent = _most_recent_dm_conversation()
            if not recent:
                return "No Moltbook DM conversations stored yet."
            cid, meta = recent
            msgs = _load_dm_messages(cid, limit=min(170, _DEFAULT_DM_MESSAGES_LOAD))
            payload = {
                "conversation": {"cid": cid, "meta": meta, "recent_messages": msgs[-18:]},
                "max_chars": _DEFAULT_REPLY_MAX_CHARS,
            }
            async def inner():
                rendered = await _llm_render_action(llm_client, action, payload, timeout=55)
                if rendered:
                    # enforce hard cap just in case
                    return _compact(rendered, _DEFAULT_REPLY_MAX_CHARS)
                return "I found the most recent DM thread, but couldn’t draft a reply (LLM unavailable)."
            return await self._hybrid(inner)

        return "Unknown action. Use: status | last_post | registration | settings | stats | events | dm_list | dm_summary | dm_reply_draft"

    async def _hybrid(self, coro_fn):
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
