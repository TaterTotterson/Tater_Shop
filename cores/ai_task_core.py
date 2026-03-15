import asyncio
import calendar
import json
import logging
import os
import re
import time
import uuid
from bisect import bisect_left
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import redis

import plugin_registry as pr
from plugin_base import ToolPlugin
from plugin_kernel import plugin_supports_platform
from helpers import (
    get_llm_client_from_env,
)
from cerberus import run_cerberus_turn, resolve_agent_limits
from notify import dispatch_notification

from dotenv import load_dotenv
__version__ = "1.0.3"

load_dotenv()

logger = logging.getLogger("ai_task_core")
logger.setLevel(logging.INFO)

CORE_SETTINGS = {
    "category": "AI Task Scheduler Core Settings",
    "required": {},
}

CORE_WEBUI_TAB = {
    "label": "AI Tasks",
    "order": 10,
    "requires_running": True,
}

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)

REMINDER_KEY_PREFIX = "reminders:"
REMINDER_DUE_ZSET = "reminders:due"
SCHEDULER_EXCLUDED_TOOLS = {"send_message", "reminder", "ai_tasks"}
MEDIA_TYPES = {"image", "audio", "video", "file"}


class _StubObject:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class _StubChannel:
    def __init__(self, channel_id: str, name: str | None = None):
        self.id = int(channel_id) if str(channel_id).isdigit() else 0
        self.name = name or "scheduled"

    async def send(self, *args, **kwargs):
        return None


class _StubAuthor:
    def __init__(self, name: str):
        self.name = name or "scheduler"
        self.display_name = self.name
        self.mention = f"@{self.name}"


class _StubGuild:
    def __init__(self, guild_id: str | None):
        self.id = int(guild_id) if str(guild_id or "").isdigit() else 0


class _StubMessage:
    def __init__(self, content: str, channel: _StubChannel, author: _StubAuthor, guild: _StubGuild | None = None):
        self.content = content
        self.channel = channel
        self.author = author
        self.guild = guild
        self.attachments = []
        self.id = int(time.time() * 1000)


class _StubIRCBot:
    def privmsg(self, target, message):
        return None


class _StubMatrixRoom:
    def __init__(self, room_id: str):
        self.room_id = room_id or "scheduled"


class _StubMatrixClient:
    pass


def _has_platform_handler(plugin: ToolPlugin, platform: str) -> bool:
    handler_name = f"handle_{platform}"
    method = getattr(plugin.__class__, handler_name, None)
    if not callable(method):
        return False
    base = getattr(ToolPlugin, handler_name, None)
    if base is None:
        return True
    return method is not base


def _build_platform_context(
    platform: str,
    *,
    origin: Dict[str, Any],
    targets: Dict[str, Any],
    task_prompt: str,
    reminder_id: str,
) -> Dict[str, Any]:
    platform = (platform or "").strip().lower()
    user = (origin or {}).get("user") or "scheduler"

    if platform == "discord":
        channel_id = str(targets.get("channel_id") or origin.get("channel_id") or "0")
        channel_name = str(targets.get("channel") or origin.get("channel") or "scheduled").lstrip("#")
        guild_id = str(targets.get("guild_id") or origin.get("guild_id") or "0")
        channel = _StubChannel(channel_id, channel_name)
        author = _StubAuthor(user)
        guild = _StubGuild(guild_id) if guild_id else None
        message = _StubMessage(task_prompt, channel, author, guild)
        return {"message": message}

    if platform == "irc":
        return {
            "bot": _StubIRCBot(),
            "channel": str(targets.get("channel") or origin.get("channel") or "#scheduled"),
            "user": user,
            "raw_message": task_prompt,
            "raw": task_prompt,
        }

    if platform == "matrix":
        room_id = str(targets.get("room_id") or origin.get("room_id") or "scheduled")
        return {
            "client": _StubMatrixClient(),
            "room": _StubMatrixRoom(room_id),
            "sender": user,
            "body": task_prompt,
        }

    if platform == "telegram":
        chat_id = str(targets.get("chat_id") or origin.get("chat_id") or "0")
        update = {
            "message": {
                "message_id": reminder_id,
                "text": task_prompt,
                "chat": {"id": chat_id, "type": "private", "title": targets.get("channel")},
                "from": {"id": user, "username": user},
            }
        }
        return {"update": update}

    if platform == "homeassistant":
        return {"context": origin or {}}

    if platform == "macos":
        return {
            "context": origin or {},
            "request_text": task_prompt,
            "raw_message": task_prompt,
            "raw": task_prompt,
        }

    return {}


def get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")


def _load_reminder(reminder_id: str) -> Optional[Dict[str, Any]]:
    raw = redis_client.get(f"{REMINDER_KEY_PREFIX}{reminder_id}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _save_reminder(reminder_id: str, reminder: Dict[str, Any]) -> None:
    redis_client.set(f"{REMINDER_KEY_PREFIX}{reminder_id}", json.dumps(reminder))


def _delete_reminder(reminder_id: str) -> None:
    redis_client.delete(f"{REMINDER_KEY_PREFIX}{reminder_id}")


def _peek_next_due() -> Optional[Tuple[str, float]]:
    items = redis_client.zrange(REMINDER_DUE_ZSET, 0, 0, withscores=True)
    if not items:
        return None
    reminder_id, score = items[0]
    try:
        return str(reminder_id), float(score)
    except Exception:
        return None


def _pop_due(reminder_id: str) -> None:
    redis_client.zrem(REMINDER_DUE_ZSET, reminder_id)


def _format_due_sleep(now: float, due_ts: float) -> float:
    if due_ts <= now:
        return 0.0
    return min(1.0, max(0.1, due_ts - now))


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _is_enabled(value: Any, default: bool = True) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)

def _normalize_weekdays(raw: Any) -> List[int]:
    if not isinstance(raw, list):
        return []
    out: List[int] = []
    for item in raw:
        day = _as_int(item, -1)
        if 0 <= day <= 6:
            out.append(day)
    if not out:
        return []
    return sorted(set(out))


def _normalize_monthdays(raw: Any) -> List[int]:
    if not isinstance(raw, list):
        return []
    out: List[int] = []
    for item in raw:
        day = _as_int(item, -1)
        if 1 <= day <= 31:
            out.append(day)
    if not out:
        return []
    return sorted(set(out))


def _normalize_clock_values(raw: Any, *, minimum: int, maximum: int) -> List[int]:
    if not isinstance(raw, list):
        return []
    out: List[int] = []
    for item in raw:
        value = _as_int(item, -1)
        if minimum <= value <= maximum:
            out.append(value)
    if not out:
        return []
    return sorted(set(out))


def _next_local_time_occurrence(
    *,
    now_ts: float,
    hour: int,
    minute: int,
    second: int,
    weekdays: Optional[List[int]] = None,
) -> float:
    now_local = datetime.fromtimestamp(float(now_ts))
    h = min(23, max(0, int(hour)))
    m = min(59, max(0, int(minute)))
    s = min(59, max(0, int(second)))

    candidate = now_local.replace(hour=h, minute=m, second=s, microsecond=0)
    if candidate.timestamp() <= now_ts:
        candidate = candidate + timedelta(days=1)

    day_filter = _normalize_weekdays(weekdays or [])
    if not day_filter:
        return candidate.timestamp()

    while candidate.weekday() not in day_filter:
        candidate = candidate + timedelta(days=1)
    return candidate.timestamp()


def _next_monthly_local_time_occurrence(
    *,
    now_ts: float,
    hour: int,
    minute: int,
    second: int,
    monthdays: Optional[List[int]] = None,
) -> float:
    now_local = datetime.fromtimestamp(float(now_ts))
    h = min(23, max(0, int(hour)))
    m = min(59, max(0, int(minute)))
    s = min(59, max(0, int(second)))

    day_filter = _normalize_monthdays(monthdays or [])
    if not day_filter:
        return 0.0

    for month_offset in range(0, 61):
        month_index = (now_local.month - 1) + month_offset
        year = now_local.year + (month_index // 12)
        month = (month_index % 12) + 1
        last_day = calendar.monthrange(year, month)[1]
        for day in day_filter:
            if day > last_day:
                continue
            candidate = now_local.replace(
                year=year,
                month=month,
                day=day,
                hour=h,
                minute=m,
                second=s,
                microsecond=0,
            )
            if candidate.timestamp() > now_ts:
                return candidate.timestamp()
    return 0.0


def _next_cron_occurrence(
    *,
    now_ts: float,
    hours: Any,
    minutes: Any,
    seconds: Any,
    weekdays: Any = None,
) -> float:
    valid_hours = _normalize_clock_values(hours, minimum=0, maximum=23)
    valid_minutes = _normalize_clock_values(minutes, minimum=0, maximum=59)
    valid_seconds = _normalize_clock_values(seconds, minimum=0, maximum=59)
    valid_weekdays = _normalize_weekdays(weekdays or [])
    if not valid_hours or not valid_minutes or not valid_seconds:
        return 0.0

    base = datetime.fromtimestamp(float(now_ts)).replace(microsecond=0) + timedelta(seconds=1)
    day_start = base.replace(hour=0, minute=0, second=0, microsecond=0)

    for day_offset in range(0, 370):
        day = day_start + timedelta(days=day_offset)
        if valid_weekdays and day.weekday() not in valid_weekdays:
            continue

        h_start = base.hour if day_offset == 0 else 0
        h_idx = bisect_left(valid_hours, h_start)
        while h_idx < len(valid_hours):
            hour = valid_hours[h_idx]
            m_start = base.minute if (day_offset == 0 and hour == base.hour) else 0
            m_idx = bisect_left(valid_minutes, m_start)
            while m_idx < len(valid_minutes):
                minute = valid_minutes[m_idx]
                s_start = base.second if (day_offset == 0 and hour == base.hour and minute == base.minute) else 0
                s_idx = bisect_left(valid_seconds, s_start)
                if s_idx < len(valid_seconds):
                    second = valid_seconds[s_idx]
                    candidate = day.replace(hour=hour, minute=minute, second=second, microsecond=0)
                    if candidate.timestamp() > now_ts:
                        return candidate.timestamp()
                m_idx += 1
            h_idx += 1
    return 0.0


def _next_run_for_schedule(schedule: Dict[str, Any], now_ts: float) -> float:
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    recurrence_kind = str(recurrence.get("kind") or "").strip().lower()
    if recurrence_kind == "cron_simple":
        return _next_cron_occurrence(
            now_ts=now_ts,
            hours=recurrence.get("hours"),
            minutes=recurrence.get("minutes"),
            seconds=recurrence.get("seconds"),
            weekdays=recurrence.get("weekdays"),
        )
    if recurrence_kind in {"daily_local_time", "weekly_local_time"}:
        hour = _as_int(recurrence.get("hour"), 0)
        minute = _as_int(recurrence.get("minute"), 0)
        second = _as_int(recurrence.get("second"), 0)
        weekdays = recurrence.get("weekdays") if recurrence_kind == "weekly_local_time" else None
        return _next_local_time_occurrence(
            now_ts=now_ts,
            hour=hour,
            minute=minute,
            second=second,
            weekdays=weekdays if isinstance(weekdays, list) else None,
        )
    if recurrence_kind == "monthly_local_time":
        hour = _as_int(recurrence.get("hour"), 0)
        minute = _as_int(recurrence.get("minute"), 0)
        second = _as_int(recurrence.get("second"), 0)
        monthdays = recurrence.get("monthdays")
        return _next_monthly_local_time_occurrence(
            now_ts=now_ts,
            hour=hour,
            minute=minute,
            second=second,
            monthdays=monthdays if isinstance(monthdays, list) else None,
        )
    return 0.0


def _is_media_dict(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    return str(item.get("type") or "").strip().lower() in MEDIA_TYPES


def _extract_structured_text(payload: Dict[str, Any]) -> str:
    for key in ("message", "content", "text", "summary"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _flatten_result_payload(res: Any) -> Tuple[str, List[Dict[str, Any]]]:
    if res is None:
        return "", []

    if isinstance(res, str):
        return res.strip(), []

    if isinstance(res, list):
        text_parts: List[str] = []
        attachments: List[Dict[str, Any]] = []
        for item in res:
            if isinstance(item, str):
                if item.strip():
                    text_parts.append(item.strip())
                continue

            if _is_media_dict(item):
                attachments.append(dict(item))
                continue

            if isinstance(item, dict):
                extracted = _extract_structured_text(item)
                if extracted:
                    text_parts.append(extracted)
                else:
                    try:
                        text_parts.append(json.dumps(item, ensure_ascii=False))
                    except Exception:
                        text_parts.append(str(item))
                continue

            as_text = str(item).strip()
            if as_text:
                text_parts.append(as_text)

        return "\n".join(text_parts).strip(), attachments

    if isinstance(res, dict):
        if _is_media_dict(res):
            caption = _extract_structured_text(res)
            return caption, [dict(res)]

        extracted = _extract_structured_text(res)
        if extracted:
            return extracted, []
        try:
            return json.dumps(res, ensure_ascii=False), []
        except Exception:
            return str(res), []

    return str(res).strip(), []


def _has_webui_handler(plugin: ToolPlugin) -> bool:
    method = getattr(plugin.__class__, "handle_webui", None)
    base = getattr(ToolPlugin, "handle_webui", None)
    return callable(method) and method is not base


def _has_automation_handler(plugin: ToolPlugin) -> bool:
    return callable(getattr(plugin, "handle_automation", None))


def _supports_scheduled_tools(plugin_name: str, plugin: ToolPlugin, platform: str) -> bool:
    if plugin_name in SCHEDULER_EXCLUDED_TOOLS:
        return False
    if getattr(plugin, "notifier", False):
        return False

    if not plugin_supports_platform(plugin, platform):
        return False
    return _has_platform_handler(plugin, platform)


def _normalize_runtime_task_prompt(task_prompt: str) -> str:
    text = str(task_prompt or "").strip()
    if not text:
        return ""
    try:
        from kernel_tools import _ai_tasks_clean_task_prompt
        normalized = str(_ai_tasks_clean_task_prompt(text) or "").strip()
        if normalized:
            return normalized
    except Exception:
        pass
    return text


def _sanitize_scheduled_output_text(text: str) -> str:
    out = str(text or "").strip()
    if not out:
        return ""

    patterns = [
        r"^\s*since\s+no\s+existing\s+[^:\n]{0,260}?\s+was\s+found,\s*here(?:'|’)s\s+(?:an?\s+)?(?:original\s+)?[^:\n]{0,120}:\s*",
        r"^\s*i\s+(?:couldn't|could not)\s+find\s+[^:\n]{0,260}?\s*,\s*here(?:'|’)s\s+[^:\n]{0,120}:\s*",
        r"^\s*here(?:'|’)s\s+(?:an?\s+)?original\s+(?:one|joke)\s*(?:for\s+you)?\s*:\s*",
    ]
    for pattern in patterns:
        out = re.sub(pattern, "", out, flags=re.IGNORECASE)
    return out.strip()


async def _render_scheduled_message(
    llm_client,
    reminder_id: str,
    task_prompt: str,
    origin: Dict[str, Any],
    platform: str,
    targets: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]]]:
    task_prompt = _normalize_runtime_task_prompt(task_prompt)
    if not task_prompt:
        return "", []

    system_prompt = (
        "You are running a scheduled task.\n"
        "Keep replies concise and task-focused.\n"
        "Do not use repo_browser.* tool syntax.\n"
        "Execute the task now; do not create, modify, or cancel schedules.\n"
        "Return only the requested deliverable content.\n"
        "Do not include process commentary or meta-prefaces such as "
        "\"Since no existing ... was found\".\n"
        "If the task asks for creative output (for example a joke), provide it directly.\n"
    )

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    user_prompt = (
        f"Scheduled task id: {reminder_id}\n"
        f"Current local time: {now_str}\n"
        f"Origin: {json.dumps(origin or {}, ensure_ascii=False)}\n\n"
        f"Task:\n{task_prompt}\n"
    )

    messages = [
        {"role": "user", "content": user_prompt},
    ]

    merged_registry = dict(pr.get_registry_snapshot() or {})
    merged_enabled = get_plugin_enabled

    def _enabled(name: str) -> bool:
        plugin = merged_registry.get(name)
        if not plugin:
            return False
        if not merged_enabled(name):
            return False
        return _supports_scheduled_tools(name, plugin, platform)

    origin_payload = dict(origin or {})
    origin_payload = {k: v for k, v in origin_payload.items() if v not in (None, "")}
    context = _build_platform_context(
        platform,
        origin=origin_payload,
        targets=targets or {},
        task_prompt=task_prompt,
        reminder_id=reminder_id,
    )
    blocked_scheduler_tools = {str(name).strip().lower() for name in SCHEDULER_EXCLUDED_TOOLS}

    def _scheduler_admin_guard(tool_name: str) -> Optional[Dict[str, Any]]:
        func = str(tool_name or "").strip().lower()
        if func in blocked_scheduler_tools:
            return {
                "tool": func,
                "ok": False,
                "error": {
                    "code": "scheduler_tool_blocked",
                    "message": "That scheduling tool is unavailable while running a scheduled task.",
                },
                "summary_for_user": "Running the scheduled task now.",
            }
        return None

    agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
    result = await run_cerberus_turn(
        llm_client=llm_client,
        platform=platform,
        history_messages=messages,
        registry=merged_registry,
        enabled_predicate=_enabled,
        context=context,
        user_text=task_prompt,
        scope=f"ai_task:{reminder_id}",
        origin=origin_payload,
        redis_client=redis_client,
        max_rounds=agent_max_rounds,
        max_tool_calls=agent_max_tool_calls,
        platform_preamble=system_prompt,
        admin_guard=_scheduler_admin_guard,
    )
    text = str(result.get("text") or "").strip()
    text = _sanitize_scheduled_output_text(text)
    attachments = result.get("artifacts") or []
    if not text and not attachments:
        text = "Scheduled task completed."
    return text, attachments


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client if redis_client is not None else globals().get("redis_client")
    if client is None:
        return {
            "summary": "Redis connection is unavailable.",
            "stats": [],
            "items": [],
            "empty_message": "Cannot load scheduled tasks without Redis.",
        }

    schedules = _ai_tasks_ui_load_schedules(client)
    total_count = len(schedules)
    enabled_count = sum(1 for row in schedules if _ai_tasks_ui_is_enabled(row.get("_enabled"), True))
    disabled_count = total_count - enabled_count

    sort_rows = sorted(
        schedules,
        key=lambda item: (
            0 if _ai_tasks_ui_is_enabled(item.get("_enabled"), True) else 1,
            float(item.get("_due_ts") or 0.0) if float(item.get("_due_ts") or 0.0) > 0 else float("inf"),
        ),
    )

    items: List[Dict[str, str]] = []
    for row in sort_rows[:25]:
        rid = str(row.get("_id") or "").strip()
        if not rid:
            continue
        schedule = row.get("schedule") if isinstance(row.get("schedule"), dict) else {}
        interval = _ai_tasks_ui_as_float(schedule.get("interval_sec"), 0.0)
        enabled = _ai_tasks_ui_is_enabled(row.get("enabled", row.get("_enabled")), True)
        platform = str(row.get("platform") or "").strip() or "unknown"
        title = str(row.get("title") or "").strip()
        task_prompt = str(row.get("task_prompt") or "").strip()
        message = str(row.get("message") or "").strip()
        command_text = task_prompt or message
        recurrence_text = _ai_tasks_ui_recurrence_label(schedule, interval)
        status_label = "Enabled" if enabled else "Disabled"

        due_ts = _ai_tasks_ui_as_float(row.get("_due_ts"), _ai_tasks_ui_as_float(schedule.get("next_run_ts"), 0.0))
        if due_ts > 0:
            due_local = datetime.fromtimestamp(due_ts).strftime("%Y-%m-%d %H:%M:%S")
            due_text = f"{due_local} ({_ai_tasks_ui_format_relative_due(due_ts)})"
        else:
            due_text = "n/a"

        preview = " ".join(command_text.split()).strip() or "(empty)"
        if len(preview) > 220:
            preview = preview[:217].rstrip() + "..."

        items.append(
            {
                "title": _ai_tasks_ui_derive_title(title, command_text),
                "subtitle": f"{platform} · {status_label} · {recurrence_text}",
                "detail": f"Next run: {due_text} · {preview}",
            }
        )

    return {
        "summary": "Scheduled AI tasks and upcoming run queue.",
        "stats": [
            {"label": "Total", "value": total_count},
            {"label": "Enabled", "value": enabled_count},
            {"label": "Disabled", "value": disabled_count},
        ],
        "items": items,
        "empty_message": "No scheduled AI tasks found.",
        "ui": _ai_tasks_ui_manager_payload(sort_rows),
    }


# ---- Embedded AI Tasks UI (migrated from webui/webui_ai_tasks.py) ----

_AI_TASKS_UI_DEFAULT_PORTALS = ["homeassistant", "discord", "irc", "matrix", "telegram", "macos"]



def _ai_tasks_ui_ordinal_day(value: Any) -> str:
    try:
        day = int(value)
    except Exception:
        return str(value or "").strip()
    if day <= 0:
        return str(day)
    if 10 <= (day % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def _ai_tasks_ui_is_enabled(raw: Any, default: bool = True) -> bool:
    if raw is None:
        return bool(default)
    if isinstance(raw, bool):
        return raw
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if value in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _ai_tasks_ui_as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _ai_tasks_ui_load_schedules(redis_client) -> List[Dict[str, Any]]:
    items_by_id: Dict[str, Dict[str, Any]] = {}
    due_by_id: Dict[str, float] = {}

    due_rows = redis_client.zrange("reminders:due", 0, -1, withscores=True) or []
    for reminder_id, due_ts in due_rows:
        rid = str(reminder_id)
        try:
            due_by_id[rid] = float(due_ts)
        except Exception:
            continue

    for key in redis_client.scan_iter(match="reminders:*", count=500):
        key_s = str(key)
        if key_s == "reminders:due":
            continue
        if not key_s.startswith("reminders:"):
            continue
        rid = key_s.split("reminders:", 1)[1].strip()
        if not rid:
            continue

        raw = redis_client.get(key_s)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue

        due_ts = due_by_id.get(rid)
        if due_ts is None:
            schedule = obj.get("schedule") if isinstance(obj.get("schedule"), dict) else {}
            due_ts = _ai_tasks_ui_as_float(schedule.get("next_run_ts"), 0.0)

        obj["_id"] = rid
        obj["_due_ts"] = float(due_ts or 0.0)
        obj["_enabled"] = _ai_tasks_ui_is_enabled(obj.get("enabled"), True)
        items_by_id[rid] = obj

    return list(items_by_id.values())


def _ai_tasks_ui_save_reminder(redis_client, reminder_id: str, reminder: Dict[str, Any]) -> None:
    redis_client.set(f"reminders:{reminder_id}", json.dumps(reminder))


def _ai_tasks_ui_delete_schedule(redis_client, reminder_id: str) -> None:
    rid = str(reminder_id or "").strip()
    if not rid:
        return
    redis_client.zrem("reminders:due", rid)
    redis_client.delete(f"reminders:{rid}")


def _ai_tasks_ui_set_due(redis_client, reminder_id: str, due_ts: float) -> None:
    rid = str(reminder_id or "").strip()
    if not rid:
        return
    if due_ts > 0:
        redis_client.zadd("reminders:due", {rid: float(due_ts)})
    else:
        redis_client.zrem("reminders:due", rid)


def _ai_tasks_ui_recompute_next_run(schedule: Dict[str, Any], now_ts: Optional[float] = None) -> float:
    if not isinstance(schedule, dict):
        return 0.0
    now = float(now_ts if now_ts is not None else time.time())
    try:
        next_run = float(_next_run_for_schedule(schedule, now) or 0.0)
        return next_run if next_run > 0 else 0.0
    except Exception:
        return 0.0


def _ai_tasks_ui_join_with_and(values: List[str]) -> str:
    cleaned = [str(v).strip() for v in values if str(v).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def _ai_tasks_ui_day_name_full(day_index: int) -> str:
    names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    if 0 <= day_index <= 6:
        return names[day_index]
    return str(day_index)


def _ai_tasks_ui_schedule_edit_text(schedule: Dict[str, Any]) -> str:
    if not isinstance(schedule, dict):
        return ""
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    kind = str(recurrence.get("kind") or "").strip().lower()
    cron_text = str(schedule.get("cron") or recurrence.get("cron") or "").strip()

    hour = int(recurrence.get("hour") or 0)
    minute = int(recurrence.get("minute") or 0)
    second = int(recurrence.get("second") or 0)
    if second:
        time_part = f"{hour:02d}:{minute:02d}:{second:02d}"
    else:
        time_part = f"{hour:02d}:{minute:02d}"

    weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []
    valid_days: List[int] = []
    for day in weekdays:
        try:
            day_i = int(day)
        except Exception:
            continue
        if 0 <= day_i <= 6 and day_i not in valid_days:
            valid_days.append(day_i)
    valid_days = sorted(valid_days)

    if kind in {"daily_local_time", "weekly_local_time"}:
        if valid_days == [0, 1, 2, 3, 4]:
            return f"weekdays at {time_part}"
        if valid_days == [5, 6]:
            return f"weekends at {time_part}"
        if valid_days:
            day_phrase = _ai_tasks_ui_join_with_and([_ai_tasks_ui_day_name_full(day) for day in valid_days])
            return f"on {day_phrase} each week at {time_part}"
        return f"everyday at {time_part}"

    if kind == "monthly_local_time":
        monthdays = recurrence.get("monthdays") if isinstance(recurrence.get("monthdays"), list) else []
        valid_monthdays: List[int] = []
        for day in monthdays:
            try:
                day_i = int(day)
            except Exception:
                continue
            if 1 <= day_i <= 31 and day_i not in valid_monthdays:
                valid_monthdays.append(day_i)
        valid_monthdays = sorted(valid_monthdays)
        if valid_monthdays:
            day_phrase = _ai_tasks_ui_join_with_and([_ai_tasks_ui_ordinal_day(day) for day in valid_monthdays])
            return f"on the {day_phrase} of every month at {time_part}"
        return f"every month at {time_part}"

    if kind == "cron_simple":
        hours = recurrence.get("hours") if isinstance(recurrence.get("hours"), list) else []
        minutes = recurrence.get("minutes") if isinstance(recurrence.get("minutes"), list) else []
        seconds = recurrence.get("seconds") if isinstance(recurrence.get("seconds"), list) else []
        weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []

        valid_days: List[int] = []
        for day in weekdays:
            try:
                day_i = int(day)
            except Exception:
                continue
            if 0 <= day_i <= 6 and day_i not in valid_days:
                valid_days.append(day_i)
        valid_days = sorted(valid_days)

        if len(hours) == 1 and len(minutes) == 1 and len(seconds) == 1:
            one_time = f"{int(hours[0]):02d}:{int(minutes[0]):02d}"
            if valid_days == [0, 1, 2, 3, 4]:
                return f"weekdays at {one_time}"
            if valid_days == [5, 6]:
                return f"weekends at {one_time}"
            if valid_days:
                day_phrase = _ai_tasks_ui_join_with_and([_ai_tasks_ui_day_name_full(day) for day in valid_days])
                return f"on {day_phrase} each week at {one_time}"
            return f"everyday at {one_time}"

        if len(hours) == 24 and len(minutes) == 60 and len(seconds) == 60 and not valid_days:
            return "every second"
        if len(hours) == 24 and len(minutes) == 60 and seconds == [0] and not valid_days:
            return "every minute"
        if len(hours) == 24 and minutes == [0] and seconds == [0] and not valid_days:
            return "every hour"

    if cron_text:
        return cron_text
    return ""


def _ai_tasks_ui_parse_schedule_input(raw_value: str) -> Tuple[Optional[Dict[str, Any]], str]:
    text = str(raw_value or "").strip()
    if not text:
        return None, "Schedule is required."

    try:
        from plugins.ai_tasks import AITasksPlugin

        parser = AITasksPlugin()
        now_ts = float(time.time())

        parsed, err = parser._parse_cron_schedule(text, now_ts=now_ts)
        if not isinstance(parsed, dict):
            parsed, err = parser._parse_human_schedule(text, now_ts=now_ts)
        if not isinstance(parsed, dict):
            return None, str(err or "Could not parse schedule.")

        next_run = _ai_tasks_ui_as_float(parsed.get("next_run_ts"), 0.0)
        recurrence = parsed.get("recurrence") if isinstance(parsed.get("recurrence"), dict) else {}
        interval = _ai_tasks_ui_as_float(parsed.get("interval_sec"), 0.0)
        if next_run <= 0 or not recurrence:
            return None, "Could not compute next run from schedule."

        recurrence_payload = dict(recurrence)
        cron_text = str(parsed.get("cron") or recurrence_payload.get("cron") or "").strip()
        if cron_text:
            recurrence_payload["cron"] = cron_text

        schedule_payload: Dict[str, Any] = {
            "next_run_ts": float(next_run),
            "interval_sec": float(interval),
            "anchor_ts": float(next_run),
            "recurrence": recurrence_payload,
        }
        if cron_text:
            schedule_payload["cron"] = cron_text

        return schedule_payload, ""
    except Exception as exc:
        return None, f"Schedule parser error: {exc}"


def _ai_tasks_ui_format_relative_due(due_ts: float) -> str:
    if due_ts <= 0:
        return "n/a"
    now = time.time()
    delta = int(due_ts - now)
    if delta <= 0:
        return "now"
    minutes = delta // 60
    if minutes < 1:
        return "in <1 min"
    if minutes < 60:
        return f"in {minutes} min"
    hours = minutes // 60
    if hours < 24:
        rem = minutes % 60
        return f"in {hours}h {rem}m"
    days = hours // 24
    rem_h = hours % 24
    return f"in {days}d {rem_h}h"


def _ai_tasks_ui_recurrence_label(schedule: Dict[str, Any], interval: float) -> str:
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    kind = str(recurrence.get("kind") or "").strip().lower()
    hour = int(recurrence.get("hour") or 0)
    minute = int(recurrence.get("minute") or 0)
    second = int(recurrence.get("second") or 0)
    weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []
    time_part = f"{hour:02d}:{minute:02d}" + (f":{second:02d}" if second else "")

    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    valid_days = []
    for day in weekdays:
        try:
            day_i = int(day)
        except Exception:
            continue
        if 0 <= day_i <= 6:
            valid_days.append(day_names[day_i])
    valid_days = sorted(set(valid_days), key=day_names.index)

    if kind == "daily_local_time":
        if valid_days:
            return f"Weekly ({', '.join(valid_days)}) at {time_part}"
        return f"Daily at {time_part}"
    if kind == "weekly_local_time":
        if valid_days:
            return f"Weekly ({', '.join(valid_days)}) at {time_part}"
        return f"Weekly at {time_part}"
    if kind == "monthly_local_time":
        monthdays = recurrence.get("monthdays") if isinstance(recurrence.get("monthdays"), list) else []
        valid_monthdays = [_ai_tasks_ui_ordinal_day(day) for day in monthdays if str(day).strip()]
        if valid_monthdays:
            return f"Monthly ({', '.join(valid_monthdays)}) at {time_part}"
        return f"Monthly at {time_part}"
    if kind == "cron_simple":
        hours = recurrence.get("hours") if isinstance(recurrence.get("hours"), list) else []
        minutes = recurrence.get("minutes") if isinstance(recurrence.get("minutes"), list) else []
        seconds = recurrence.get("seconds") if isinstance(recurrence.get("seconds"), list) else []
        if len(hours) == 24 and len(minutes) == 60 and len(seconds) == 60 and not valid_days:
            return "Every second"
        if len(hours) == 24 and len(minutes) == 60 and seconds == [0] and not valid_days:
            return "Every minute"
        if len(hours) == 24 and minutes == [0] and seconds == [0] and not valid_days:
            return "Every hour"
        if len(hours) == 1 and len(minutes) == 1 and len(seconds) == 1:
            one_time = f"{int(hours[0]):02d}:{int(minutes[0]):02d}" + (
                f":{int(seconds[0]):02d}" if int(seconds[0]) else ""
            )
            if valid_days:
                return f"Weekly ({', '.join(valid_days)}) at {one_time}"
            return f"Daily at {one_time}"
        cron_text = str(recurrence.get("cron") or "").strip()
        if cron_text:
            return f"Cron ({cron_text})"
        return "Cron schedule"
    if interval > 0:
        return f"Legacy interval ({int(interval)}s)"
    return "One-shot"


def _ai_tasks_ui_derive_title(raw_title: str, command_text: str) -> str:
    title = str(raw_title or "").strip()
    seed = " ".join(str(command_text or "").split()).strip()
    if title:
        # Older titles may be auto-truncated (ending in "..."). Prefer the full prompt when available.
        if title.endswith("...") and seed and len(seed) > len(title):
            return seed
        return title
    if not seed:
        return "Scheduled task"
    return seed


def _ai_tasks_ui_clean_text(value: Any) -> str:
    return str(value or "").strip()


def _ai_tasks_ui_platform_options(schedules: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, str]]:
    names: List[str] = []
    for token in _AI_TASKS_UI_DEFAULT_PORTALS + ["webui"]:
        name = _ai_tasks_ui_clean_text(token).lower()
        if name and name not in names:
            names.append(name)
    for row in schedules or []:
        name = _ai_tasks_ui_clean_text(row.get("platform")).lower()
        if name and name not in names:
            names.append(name)
    return [{"value": name, "label": name} for name in names]


def _ai_tasks_ui_target_to_text(platform: str, targets: Dict[str, Any]) -> str:
    platform_name = _ai_tasks_ui_clean_text(platform).lower()
    payload = targets if isinstance(targets, dict) else {}
    candidates: List[Any]
    if platform_name == "discord":
        candidates = [payload.get("channel_id"), payload.get("channel")]
    elif platform_name == "irc":
        candidates = [payload.get("channel")]
    elif platform_name == "matrix":
        candidates = [payload.get("room_id"), payload.get("channel")]
    elif platform_name == "telegram":
        candidates = [payload.get("chat_id")]
    elif platform_name == "homeassistant":
        candidates = [payload.get("device_service")]
    else:
        candidates = [payload.get("channel"), payload.get("room_id"), payload.get("chat_id")]

    for candidate in candidates:
        text = _ai_tasks_ui_clean_text(candidate)
        if text:
            return text
    return ""


def _ai_tasks_ui_target_from_text(platform: str, target_text: str) -> Dict[str, Any]:
    platform_name = _ai_tasks_ui_clean_text(platform).lower()
    target = _ai_tasks_ui_clean_text(target_text)
    if not target:
        return {}

    if platform_name == "discord":
        if target.isdigit():
            return {"channel_id": target}
        return {"channel": target}
    if platform_name == "irc":
        return {"channel": target if target.startswith("#") else f"#{target}"}
    if platform_name == "matrix":
        return {"room_id": target}
    if platform_name == "telegram":
        return {"chat_id": target}
    if platform_name == "homeassistant":
        return {"device_service": target}
    return {"channel": target}


def _ai_tasks_ui_load_reminder(redis_client, reminder_id: str) -> Optional[Dict[str, Any]]:
    rid = _ai_tasks_ui_clean_text(reminder_id)
    if not rid:
        return None
    raw = redis_client.get(f"reminders:{rid}")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    parsed["_id"] = rid
    return parsed


def _ai_tasks_ui_manager_payload(schedules: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = list(schedules or [])
    platform_options = _ai_tasks_ui_platform_options(rows)
    forms: List[Dict[str, Any]] = []

    for row in rows[:120]:
        reminder_id = _ai_tasks_ui_clean_text(row.get("_id"))
        if not reminder_id:
            continue
        schedule = row.get("schedule") if isinstance(row.get("schedule"), dict) else {}
        interval = _ai_tasks_ui_as_float(schedule.get("interval_sec"), 0.0)
        enabled = _ai_tasks_ui_is_enabled(row.get("enabled", row.get("_enabled")), True)
        platform = _ai_tasks_ui_clean_text(row.get("platform")).lower() or "homeassistant"
        title = _ai_tasks_ui_clean_text(row.get("title"))
        task_prompt = _ai_tasks_ui_clean_text(row.get("task_prompt") or row.get("message"))
        recurrence_text = _ai_tasks_ui_recurrence_label(schedule, interval)
        schedule_text = _ai_tasks_ui_schedule_edit_text(schedule)
        target_text = _ai_tasks_ui_target_to_text(platform, row.get("targets") if isinstance(row.get("targets"), dict) else {})

        due_ts = _ai_tasks_ui_as_float(row.get("_due_ts"), _ai_tasks_ui_as_float(schedule.get("next_run_ts"), 0.0))
        if due_ts > 0:
            due_local = datetime.fromtimestamp(due_ts).strftime("%Y-%m-%d %H:%M:%S")
            due_text = f"{due_local} ({_ai_tasks_ui_format_relative_due(due_ts)})"
        else:
            due_text = "n/a"

        forms.append(
            {
                "id": reminder_id,
                "title": _ai_tasks_ui_derive_title(title, task_prompt),
                "subtitle": f"{platform} · {'Enabled' if enabled else 'Disabled'} · {recurrence_text} · next: {due_text}",
                "save_action": "ai_tasks_save_schedule",
                "remove_action": "ai_tasks_remove_schedule",
                "remove_confirm": f"Remove scheduled task {reminder_id}?",
                "fields": [
                    {
                        "key": "enabled",
                        "label": "Enabled",
                        "type": "checkbox",
                        "value": bool(enabled),
                    },
                    {
                        "key": "title",
                        "label": "Title",
                        "type": "text",
                        "value": title,
                    },
                    {
                        "key": "task_prompt",
                        "label": "Task Prompt",
                        "type": "textarea",
                        "value": task_prompt,
                    },
                    {
                        "key": "schedule_text",
                        "label": "Schedule",
                        "type": "text",
                        "description": "Examples: weekdays at 8am, every hour, on the 10th of every month, or 0 0 6 * * *",
                        "value": schedule_text,
                    },
                    {
                        "key": "platform",
                        "label": "Portal",
                        "type": "select",
                        "options": platform_options,
                        "value": platform,
                    },
                    {
                        "key": "target",
                        "label": "Target (optional)",
                        "type": "text",
                        "description": "Discord channel ID, IRC channel, Matrix room ID, Telegram chat ID, or Home Assistant service.",
                        "value": target_text,
                    },
                ],
            }
        )

    return {
        "kind": "settings_manager",
        "title": "AI Task Scheduler",
        "empty_message": "No scheduled AI tasks found.",
        "use_tabs": True,
        "create_tab_label": "Create Task",
        "items_tab_label": "Current Tasks",
        "item_fields_dropdown": True,
        "item_fields_dropdown_label": "Task Settings",
        "add_form": {
            "action": "ai_tasks_add_schedule",
            "submit_label": "Add Task",
            "fields": [
                {
                    "key": "title",
                    "label": "Title (optional)",
                    "type": "text",
                    "value": "",
                },
                {
                    "key": "task_prompt",
                    "label": "Task Prompt",
                    "type": "textarea",
                    "description": "What should run each time the task triggers.",
                    "value": "",
                },
                {
                    "key": "schedule_text",
                    "label": "Schedule",
                    "type": "text",
                    "description": "Examples: weekdays at 8am, every hour, on the 10th of every month, or 0 0 6 * * *",
                    "value": "",
                },
                {
                    "key": "platform",
                    "label": "Portal",
                    "type": "select",
                    "options": platform_options,
                    "value": "homeassistant",
                },
                {
                    "key": "target",
                    "label": "Target (optional)",
                    "type": "text",
                    "value": "",
                },
                {
                    "key": "enabled",
                    "label": "Enabled",
                    "type": "checkbox",
                    "value": True,
                },
            ],
        },
        "item_forms": forms,
    }


def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client if redis_client is not None else globals().get("redis_client")
    if client is None:
        raise ValueError("Redis connection is unavailable.")

    body = payload if isinstance(payload, dict) else {}
    values = body.get("values") if isinstance(body.get("values"), dict) else {}
    action_name = _ai_tasks_ui_clean_text(action).lower()

    def _value(key: str, default: Any = "") -> Any:
        if key in values:
            return values.get(key)
        return body.get(key, default)

    def _parse_schedule_text(schedule_text: str) -> Dict[str, Any]:
        parsed, error_text = _ai_tasks_ui_parse_schedule_input(schedule_text)
        if not isinstance(parsed, dict):
            raise ValueError(error_text or "Could not parse schedule.")
        recurrence = parsed.get("recurrence") if isinstance(parsed.get("recurrence"), dict) else {}
        next_run = _ai_tasks_ui_as_float(parsed.get("next_run_ts"), 0.0)
        interval = _ai_tasks_ui_as_float(parsed.get("interval_sec"), 0.0)
        if next_run <= 0 or not recurrence:
            raise ValueError("Could not compute next run from schedule.")
        schedule_payload: Dict[str, Any] = {
            "next_run_ts": float(next_run),
            "interval_sec": float(interval),
            "anchor_ts": float(next_run),
            "recurrence": dict(recurrence),
        }
        cron_text = _ai_tasks_ui_clean_text(parsed.get("cron") or recurrence.get("cron"))
        if cron_text:
            schedule_payload["cron"] = cron_text
        return schedule_payload

    if action_name == "ai_tasks_add_schedule":
        task_prompt = _ai_tasks_ui_clean_text(_value("task_prompt"))
        if not task_prompt:
            raise ValueError("Task prompt is required.")

        schedule_text = _ai_tasks_ui_clean_text(_value("schedule_text"))
        if not schedule_text:
            raise ValueError("Schedule is required.")
        schedule_payload = _parse_schedule_text(schedule_text)

        platform = _ai_tasks_ui_clean_text(_value("platform")).lower() or "homeassistant"
        allowed_platforms = {str(item.get("value") or "").strip().lower() for item in _ai_tasks_ui_platform_options([])}
        if platform not in allowed_platforms:
            raise ValueError(f"Unsupported portal: {platform}")

        target_text = _ai_tasks_ui_clean_text(_value("target"))
        enabled = _ai_tasks_ui_is_enabled(_value("enabled", True), True)
        title = _ai_tasks_ui_clean_text(_value("title"))
        if not title:
            title = _ai_tasks_ui_derive_title("", task_prompt)

        reminder_id = str(uuid.uuid4())
        reminder = {
            "id": reminder_id,
            "created_at": float(time.time()),
            "platform": platform,
            "title": title,
            "task_prompt": task_prompt,
            "targets": _ai_tasks_ui_target_from_text(platform, target_text),
            "origin": {},
            "meta": {},
            "schedule": schedule_payload,
            "enabled": bool(enabled),
        }
        _ai_tasks_ui_save_reminder(client, reminder_id, reminder)
        due_ts = _ai_tasks_ui_as_float(schedule_payload.get("next_run_ts"), 0.0)
        _ai_tasks_ui_set_due(client, reminder_id, due_ts if enabled else 0.0)
        return {"ok": True, "message": f"Scheduled task added ({title}).", "id": reminder_id}

    if action_name == "ai_tasks_save_schedule":
        reminder_id = _ai_tasks_ui_clean_text(_value("id"))
        if not reminder_id:
            raise ValueError("Task id is required.")
        current = _ai_tasks_ui_load_reminder(client, reminder_id)
        if not isinstance(current, dict):
            raise KeyError("Scheduled task not found.")

        task_prompt = _ai_tasks_ui_clean_text(_value("task_prompt"))
        if not task_prompt:
            task_prompt = _ai_tasks_ui_clean_text(current.get("task_prompt") or current.get("message"))
        if not task_prompt:
            raise ValueError("Task prompt is required.")

        platform = _ai_tasks_ui_clean_text(_value("platform")).lower() or _ai_tasks_ui_clean_text(current.get("platform")).lower()
        allowed_platforms = {str(item.get("value") or "").strip().lower() for item in _ai_tasks_ui_platform_options([])}
        if platform not in allowed_platforms:
            raise ValueError(f"Unsupported portal: {platform}")

        schedule_text = _ai_tasks_ui_clean_text(_value("schedule_text"))
        current_schedule = current.get("schedule") if isinstance(current.get("schedule"), dict) else {}
        current_schedule_text = _ai_tasks_ui_schedule_edit_text(current_schedule)
        if schedule_text:
            try:
                schedule_payload = _parse_schedule_text(schedule_text)
            except ValueError:
                if schedule_text == current_schedule_text:
                    schedule_payload = dict(current_schedule)
                else:
                    raise
        else:
            schedule_payload = dict(current_schedule)
            next_run = _ai_tasks_ui_recompute_next_run(schedule_payload, now_ts=time.time())
            if next_run > 0:
                schedule_payload["next_run_ts"] = float(next_run)
                schedule_payload.setdefault("anchor_ts", float(next_run))

        title = _ai_tasks_ui_clean_text(_value("title"))
        if not title:
            title = _ai_tasks_ui_derive_title(_ai_tasks_ui_clean_text(current.get("title")), task_prompt)

        target_changed = ("target" in values) or ("target" in body)
        if target_changed:
            targets = _ai_tasks_ui_target_from_text(platform, _ai_tasks_ui_clean_text(_value("target")))
        else:
            targets = current.get("targets") if isinstance(current.get("targets"), dict) else {}
            if not targets and platform != _ai_tasks_ui_clean_text(current.get("platform")).lower():
                targets = _ai_tasks_ui_target_from_text(platform, "")

        enabled = _ai_tasks_ui_is_enabled(_value("enabled", current.get("enabled")), True)
        current.update(
            {
                "platform": platform,
                "title": title,
                "task_prompt": task_prompt,
                "targets": targets,
                "schedule": schedule_payload,
                "enabled": bool(enabled),
            }
        )
        _ai_tasks_ui_save_reminder(client, reminder_id, current)
        due_ts = _ai_tasks_ui_as_float(schedule_payload.get("next_run_ts"), 0.0)
        _ai_tasks_ui_set_due(client, reminder_id, due_ts if enabled else 0.0)
        return {"ok": True, "message": f"Saved task {title}.", "id": reminder_id}

    if action_name == "ai_tasks_remove_schedule":
        reminder_id = _ai_tasks_ui_clean_text(_value("id"))
        if not reminder_id:
            raise ValueError("Task id is required.")
        existing = _ai_tasks_ui_load_reminder(client, reminder_id)
        if not isinstance(existing, dict):
            raise KeyError("Scheduled task not found.")
        _ai_tasks_ui_delete_schedule(client, reminder_id)
        title = _ai_tasks_ui_clean_text(existing.get("title")) or reminder_id
        return {"ok": True, "message": f"Removed task {title}.", "id": reminder_id}

    raise ValueError(f"Unknown action: {action_name}")


def run(stop_event: Optional[object] = None):
    logger.info("[AI Tasks] Scheduler service started.")
    llm_client = get_llm_client_from_env()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        while True:
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break

            now = time.time()
            next_due = _peek_next_due()
            if not next_due:
                time.sleep(0.5)
                continue

            reminder_id, due_ts = next_due
            sleep_for = _format_due_sleep(now, due_ts)
            if sleep_for > 0:
                time.sleep(sleep_for)
                continue

            _pop_due(reminder_id)
            reminder = _load_reminder(reminder_id)
            if not reminder:
                continue

            if not _is_enabled(reminder.get("enabled"), True):
                continue

            dest = str(reminder.get("platform") or "").strip().lower()
            title = reminder.get("title")
            message = reminder.get("message")
            task_prompt = reminder.get("task_prompt")
            targets = reminder.get("targets") or {}
            origin = reminder.get("origin") or {}
            meta = reminder.get("meta") or {}
            schedule = reminder.get("schedule") or {}

            if not dest or (not message and not task_prompt):
                logger.warning(f"[AI Tasks] Invalid reminder {reminder_id}; dropping.")
                _delete_reminder(reminder_id)
                continue

            try:
                outbound_message = (message or "").strip()
                rendered_text, rendered_attachments = loop.run_until_complete(
                    _render_scheduled_message(
                        llm_client=llm_client,
                        reminder_id=reminder_id,
                        task_prompt=(task_prompt or message or ""),
                        origin=origin,
                        platform=dest,
                        targets=targets,
                    )
                )
                if rendered_text:
                    outbound_message = rendered_text
                else:
                    outbound_message = (message or "").strip()

                if not outbound_message:
                    if rendered_attachments:
                        outbound_message = "Scheduled task completed with attachment."
                    else:
                        outbound_message = "Scheduled task completed."

                result = loop.run_until_complete(
                    dispatch_notification(
                        platform=dest,
                        title=title,
                        content=outbound_message,
                        targets=targets,
                        origin=origin,
                        meta=meta,
                        attachments=rendered_attachments,
                    )
                )
                if isinstance(result, str) and result.startswith("Cannot queue"):
                    logger.warning(f"[AI Tasks] {result} (reminder {reminder_id})")
            except Exception as e:
                logger.error(f"[AI Tasks] Failed to enqueue reminder {reminder_id}: {e}")

            next_run = _next_run_for_schedule(schedule, time.time())
            if next_run > 0:
                schedule["next_run_ts"] = float(next_run)
                reminder["schedule"] = schedule
                _save_reminder(reminder_id, reminder)
                redis_client.zadd(REMINDER_DUE_ZSET, {reminder_id: float(next_run)})
            else:
                _delete_reminder(reminder_id)

    finally:
        try:
            loop.close()
        except Exception:
            pass
        logger.info("[AI Tasks] Scheduler service stopped.")
