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

import verba_registry as pr
from verba_base import ToolVerba
from verba_kernel import verba_supports_platform
from helpers import (
    get_llm_client_from_env,
    redis_client,
)
from hydra import run_hydra_turn, resolve_agent_limits
from notify import dispatch_notification
from notify.queue import (
    ALLOWED_PLATFORMS,
    load_default_targets,
    normalize_origin,
    normalize_platform,
    resolve_targets,
)

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

REMINDER_KEY_PREFIX = "reminders:"
REMINDER_DUE_ZSET = "reminders:due"
SCHEDULER_EXCLUDED_TOOLS = {"send_message", "reminder", "ai_tasks"}
MEDIA_TYPES = {"image", "audio", "video", "file"}
_WEEKDAY_TOKEN_MAP = {
    "0": 0,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 0,
    "sun": 6,
    "sunday": 6,
    "sundays": 6,
    "mon": 0,
    "monday": 0,
    "mondays": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "tuesdays": 1,
    "wed": 2,
    "weds": 2,
    "wednesday": 2,
    "wednesdays": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "thursday": 3,
    "thursdays": 3,
    "fri": 4,
    "friday": 4,
    "fridays": 4,
    "sat": 5,
    "saturday": 5,
    "saturdays": 5,
}


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


def _has_platform_handler(plugin: ToolVerba, platform: str) -> bool:
    handler_name = f"handle_{platform}"
    method = getattr(plugin.__class__, handler_name, None)
    if not callable(method):
        return False
    base = getattr(ToolVerba, handler_name, None)
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
    enabled = redis_client.hget("verba_enabled", plugin_name)
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


def _has_webui_handler(plugin: ToolVerba) -> bool:
    method = getattr(plugin.__class__, "handle_webui", None)
    base = getattr(ToolVerba, "handle_webui", None)
    return callable(method) and method is not base


def _has_automation_handler(plugin: ToolVerba) -> bool:
    return callable(getattr(plugin, "handle_automation", None))


def _supports_scheduled_tools(plugin_name: str, plugin: ToolVerba, platform: str) -> bool:
    if plugin_name in SCHEDULER_EXCLUDED_TOOLS:
        return False
    if getattr(plugin, "notifier", False):
        return False

    if not verba_supports_platform(plugin, platform):
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

    merged_registry = dict(pr.get_verba_registry_snapshot() or {})
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
    result = await run_hydra_turn(
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


def _ai_tasks_parse_weekday_field(value: Any) -> Tuple[Optional[List[int]], str]:
    raw = str(value or "").strip().lower()
    if raw in {"*", "?"}:
        return [], ""
    if any(ch in raw for ch in ("/", "-")):
        return None, "Cron day-of-week supports only single values or comma lists (no ranges/steps)."

    out: List[int] = []
    for token in raw.split(","):
        key = token.strip().lower()
        if not key:
            continue
        mapped = _WEEKDAY_TOKEN_MAP.get(key)
        if mapped is None:
            return None, f"Invalid cron day-of-week token: {token}"
        if mapped not in out:
            out.append(mapped)
    return sorted(out), ""


def _ai_tasks_expand_numeric_field(
    value: Any,
    *,
    minimum: int,
    maximum: int,
    field_name: str,
) -> Tuple[Optional[List[int]], str]:
    text = str(value or "").strip().lower()
    if not text or text in {"*", "?"}:
        return list(range(minimum, maximum + 1)), ""

    out: List[int] = []
    for token in text.split(","):
        part = token.strip().lower()
        if not part:
            continue
        if part in {"*", "?"}:
            out.extend(range(minimum, maximum + 1))
            continue
        if part.startswith("*/"):
            step_txt = part[2:].strip()
            if not re.fullmatch(r"\d{1,3}", step_txt):
                return None, f"Invalid cron {field_name} step: {part}"
            step = int(step_txt)
            if step <= 0:
                return None, f"Cron {field_name} step must be > 0."
            out.extend(range(minimum, maximum + 1, step))
            continue
        if not re.fullmatch(r"\d{1,3}", part):
            return None, f"Invalid cron {field_name}: {part}"
        number = int(part)
        if number < minimum or number > maximum:
            return None, f"Cron {field_name} must be in {minimum}-{maximum}."
        out.append(number)

    unique = sorted(set(out))
    if not unique:
        return None, f"Cron {field_name} produced no values."
    return unique, ""


def _ai_tasks_schedule_interval_estimate(
    *,
    hours: List[int],
    minutes: List[int],
    seconds: List[int],
    weekdays: Optional[List[int]],
) -> float:
    day_filter = sorted(set(int(v) for v in (weekdays or []) if 0 <= int(v) <= 6))
    if day_filter:
        return 0.0
    if len(hours) == 24 and len(minutes) == 60 and len(seconds) == 60:
        return 1.0
    if len(hours) == 24 and len(minutes) == 60 and seconds == [0]:
        return 60.0
    if len(hours) == 24 and minutes == [0] and seconds == [0]:
        return 3600.0
    if len(hours) == 1 and len(minutes) == 1 and len(seconds) == 1:
        return 24 * 60 * 60.0
    return 0.0


def _ai_tasks_build_cron_simple_schedule(
    *,
    now_ts: float,
    hours: List[int],
    minutes: List[int],
    seconds: List[int],
    weekdays: Optional[List[int]] = None,
    cron: str = "",
) -> Optional[Dict[str, Any]]:
    next_run = _next_cron_occurrence(
        now_ts=now_ts,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        weekdays=weekdays or [],
    )
    if next_run <= 0:
        return None

    clean_hours = sorted(set(int(v) for v in (hours or []) if 0 <= int(v) <= 23))
    clean_minutes = sorted(set(int(v) for v in (minutes or []) if 0 <= int(v) <= 59))
    clean_seconds = sorted(set(int(v) for v in (seconds or []) if 0 <= int(v) <= 59))
    clean_weekdays = sorted(set(int(v) for v in (weekdays or []) if 0 <= int(v) <= 6))
    interval_estimate = _ai_tasks_schedule_interval_estimate(
        hours=clean_hours,
        minutes=clean_minutes,
        seconds=clean_seconds,
        weekdays=clean_weekdays,
    )

    recurrence: Dict[str, Any] = {
        "kind": "cron_simple",
        "hours": clean_hours,
        "minutes": clean_minutes,
        "seconds": clean_seconds,
    }
    if clean_weekdays:
        recurrence["weekdays"] = clean_weekdays
    if cron:
        recurrence["cron"] = str(cron).strip()

    out: Dict[str, Any] = {
        "next_run_ts": float(next_run),
        "interval_sec": float(interval_estimate),
        "anchor_ts": float(next_run),
        "recurrence": recurrence,
    }
    if cron:
        out["cron"] = str(cron).strip()
    return out


def _ai_tasks_parse_time_of_day(text: Any) -> Optional[Tuple[int, int, int]]:
    raw = str(text or "").strip()
    if not raw:
        return None

    m12 = re.search(r"\b(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?\s*(am|pm)\b", raw, flags=re.IGNORECASE)
    if m12:
        hour = int(m12.group(1))
        minute = int(m12.group(2) or 0)
        second = int(m12.group(3) or 0)
        mer = str(m12.group(4) or "").lower()
        if hour < 1 or hour > 12 or minute > 59 or second > 59:
            return None
        if mer == "am":
            hour = 0 if hour == 12 else hour
        else:
            hour = 12 if hour == 12 else hour + 12
        return hour, minute, second

    m24 = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?\b", raw)
    if m24:
        return int(m24.group(1)), int(m24.group(2)), int(m24.group(3) or 0)

    m_compact = re.search(r"\bat\s+(\d{3,4})(?:\b|$)", raw, flags=re.IGNORECASE)
    if m_compact:
        digits = str(m_compact.group(1) or "")
        if len(digits) == 3:
            hour = int(digits[0])
            minute = int(digits[1:])
        else:
            hour = int(digits[:2])
            minute = int(digits[2:])
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute, 0

    m_at = re.search(r"\bat\s+([01]?\d|2[0-3])(?::([0-5]\d))?(?::([0-5]\d))?(?:\b|$)", raw, flags=re.IGNORECASE)
    if m_at:
        return int(m_at.group(1)), int(m_at.group(2) or 0), int(m_at.group(3) or 0)

    return None


def _ai_tasks_extract_weekdays_from_text(value: Any) -> List[int]:
    text = str(value or "").strip().lower()
    if not text:
        return []
    if "weekdays" in text or "weekday" in text:
        return [0, 1, 2, 3, 4]
    if "weekends" in text or "weekend" in text:
        return [5, 6]
    out: List[int] = []
    for token, mapped in _WEEKDAY_TOKEN_MAP.items():
        if token.isdigit():
            continue
        if re.search(rf"\b{re.escape(token)}\b", text):
            if mapped not in out:
                out.append(mapped)
    return sorted(out)


def _ai_tasks_extract_monthdays_from_text(value: Any) -> List[int]:
    text = str(value or "").strip().lower()
    if not text:
        return []
    if not re.search(r"\b(every month|each month|monthly)\b", text):
        return []

    out: List[int] = []
    for token in re.findall(r"\b([12]?\d|3[01])(?:st|nd|rd|th)\b", text):
        day = int(token)
        if 1 <= day <= 31 and day not in out:
            out.append(day)

    if not out:
        for token in re.findall(r"\bon\s+the\s+([12]?\d|3[01])\b", text):
            day = int(token)
            if 1 <= day <= 31 and day not in out:
                out.append(day)
    return sorted(out)


def _ai_tasks_default_local_time_parts(now_ts: float) -> Tuple[int, int, int]:
    now_local = datetime.fromtimestamp(float(now_ts)).astimezone()
    return int(now_local.hour), int(now_local.minute), 0


def _ai_tasks_parse_cron_schedule(raw_value: Any, *, now_ts: float) -> Tuple[Optional[Dict[str, Any]], str]:
    text = str(raw_value or "").strip()
    if not text:
        return None, ""

    parts = text.split()
    if len(parts) not in {5, 6}:
        return None, ""

    if len(parts) == 5:
        sec_raw = "0"
        minute_raw, hour_raw, day_raw, month_raw, dow_raw = parts
    else:
        sec_raw, minute_raw, hour_raw, day_raw, month_raw, dow_raw = parts

    if day_raw not in {"*", "?"} or month_raw not in {"*", "?"}:
        return None, "Cron day-of-month and month must be '*' or '?' for ai_tasks."

    weekdays, weekday_err = _ai_tasks_parse_weekday_field(dow_raw)
    if weekday_err:
        return None, weekday_err
    if weekdays is None:
        return None, "Invalid cron day-of-week field."

    seconds, sec_err = _ai_tasks_expand_numeric_field(sec_raw, minimum=0, maximum=59, field_name="second")
    if sec_err:
        return None, sec_err
    minutes, min_err = _ai_tasks_expand_numeric_field(minute_raw, minimum=0, maximum=59, field_name="minute")
    if min_err:
        return None, min_err
    hours, hour_err = _ai_tasks_expand_numeric_field(hour_raw, minimum=0, maximum=23, field_name="hour")
    if hour_err:
        return None, hour_err

    cron_text = " ".join(parts)
    schedule = _ai_tasks_build_cron_simple_schedule(
        now_ts=now_ts,
        hours=hours or [],
        minutes=minutes or [],
        seconds=seconds or [],
        weekdays=weekdays or [],
        cron=cron_text,
    )
    if not isinstance(schedule, dict):
        return None, "Could not compute next run for cron schedule."
    return schedule, ""


def _ai_tasks_parse_human_schedule(raw_value: Any, *, now_ts: float) -> Tuple[Optional[Dict[str, Any]], str]:
    text = " ".join(str(raw_value or "").strip().lower().split())
    if not text:
        return None, ""

    if re.search(r"\b(every|each)\s+second\b|\bsecondly\b", text):
        schedule = _ai_tasks_build_cron_simple_schedule(
            now_ts=now_ts,
            hours=list(range(24)),
            minutes=list(range(60)),
            seconds=list(range(60)),
            cron="* * * * * *",
        )
        if schedule:
            return schedule, ""
        return None, "Could not compute next run for every-second schedule."

    m_every_seconds = re.search(r"\bevery\s+(\d+)\s+seconds?\b", text)
    if m_every_seconds:
        step = int(m_every_seconds.group(1))
        if step <= 0:
            return None, "Every-seconds value must be > 0."
        if step == 1:
            seconds = list(range(60))
            schedule = _ai_tasks_build_cron_simple_schedule(
                now_ts=now_ts,
                hours=list(range(24)),
                minutes=list(range(60)),
                seconds=seconds,
            )
        elif step <= 59:
            schedule = _ai_tasks_build_cron_simple_schedule(
                now_ts=now_ts,
                hours=list(range(24)),
                minutes=list(range(60)),
                seconds=list(range(0, 60, step)),
            )
        elif step % 60 == 0:
            minute_step = step // 60
            if minute_step > 59:
                return None, "Every-seconds value is too large for cron-like conversion."
            schedule = _ai_tasks_build_cron_simple_schedule(
                now_ts=now_ts,
                hours=list(range(24)),
                minutes=list(range(0, 60, minute_step)),
                seconds=[0],
            )
        else:
            return None, "Every-seconds value must be 1-59 or a multiple of 60."
        if schedule:
            return schedule, ""
        return None, "Could not compute next run for every-seconds schedule."

    if re.search(r"\b(every|each)\s+minute\b|\bminutely\b", text):
        schedule = _ai_tasks_build_cron_simple_schedule(
            now_ts=now_ts,
            hours=list(range(24)),
            minutes=list(range(60)),
            seconds=[0],
            cron="0 * * * * *",
        )
        if schedule:
            return schedule, ""
        return None, "Could not compute next run for every-minute schedule."

    m_every_minutes = re.search(r"\bevery\s+(\d+)\s+minutes?\b", text)
    if m_every_minutes:
        step = int(m_every_minutes.group(1))
        if step <= 0 or step > 59:
            return None, "Every-minutes value must be between 1 and 59."
        schedule = _ai_tasks_build_cron_simple_schedule(
            now_ts=now_ts,
            hours=list(range(24)),
            minutes=list(range(0, 60, step)),
            seconds=[0],
        )
        if schedule:
            return schedule, ""
        return None, "Could not compute next run for every-minutes schedule."

    if re.search(r"\b(every|each)\s+hour\b|\bhourly\b", text):
        schedule = _ai_tasks_build_cron_simple_schedule(
            now_ts=now_ts,
            hours=list(range(24)),
            minutes=[0],
            seconds=[0],
            cron="0 0 * * * *",
        )
        if schedule:
            return schedule, ""
        return None, "Could not compute next run for every-hour schedule."

    m_every_hours = re.search(r"\bevery\s+(\d+)\s+hours?\b", text)
    if m_every_hours:
        step = int(m_every_hours.group(1))
        if step <= 0 or step > 23:
            return None, "Every-hours value must be between 1 and 23."
        schedule = _ai_tasks_build_cron_simple_schedule(
            now_ts=now_ts,
            hours=list(range(0, 24, step)),
            minutes=[0],
            seconds=[0],
        )
        if schedule:
            return schedule, ""
        return None, "Could not compute next run for every-hours schedule."

    daily = bool(re.search(r"\b(every day|everyday|daily|each day)\b", text))
    weekly = bool(re.search(r"\b(every week|each week|weekly)\b", text))
    monthly = bool(re.search(r"\b(every month|each month|monthly)\b", text))
    weekdays = _ai_tasks_extract_weekdays_from_text(text)
    monthdays = _ai_tasks_extract_monthdays_from_text(text)
    if daily or weekly or monthly or weekdays or monthdays:
        time_parts = _ai_tasks_parse_time_of_day(text) or _ai_tasks_default_local_time_parts(now_ts)
        hour, minute, second = time_parts

        if monthly or monthdays:
            chosen_monthdays = monthdays or [int(datetime.fromtimestamp(float(now_ts)).astimezone().day)]
            next_run = _next_monthly_local_time_occurrence(
                now_ts=now_ts,
                hour=hour,
                minute=minute,
                second=second,
                monthdays=chosen_monthdays,
            )
            if next_run <= 0:
                return None, "Could not compute next run for monthly schedule."
            return {
                "next_run_ts": float(next_run),
                "interval_sec": 0.0,
                "anchor_ts": float(next_run),
                "recurrence": {
                    "kind": "monthly_local_time",
                    "hour": int(hour),
                    "minute": int(minute),
                    "second": int(second),
                    "monthdays": sorted(set(int(d) for d in chosen_monthdays if 1 <= int(d) <= 31)),
                },
            }, ""

        if weekdays or weekly:
            chosen_weekdays = weekdays or [datetime.fromtimestamp(float(now_ts)).astimezone().weekday()]
            next_run = _next_local_time_occurrence(
                now_ts=now_ts,
                hour=hour,
                minute=minute,
                second=second,
                weekdays=chosen_weekdays,
            )
            if next_run <= 0:
                return None, "Could not compute next run for weekly schedule."
            day_tokens = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
            dow_tokens = [day_tokens[int(d)] for d in sorted(set(chosen_weekdays)) if 0 <= int(d) <= 6]
            cron_text = f"{int(second)} {int(minute)} {int(hour)} * * {','.join(dow_tokens)}" if dow_tokens else ""
            recurrence: Dict[str, Any] = {
                "kind": "weekly_local_time",
                "hour": int(hour),
                "minute": int(minute),
                "second": int(second),
                "weekdays": sorted(set(int(d) for d in chosen_weekdays if 0 <= int(d) <= 6)),
            }
            if cron_text:
                recurrence["cron"] = cron_text
            schedule: Dict[str, Any] = {
                "next_run_ts": float(next_run),
                "interval_sec": 0.0,
                "anchor_ts": float(next_run),
                "recurrence": recurrence,
            }
            if cron_text:
                schedule["cron"] = cron_text
            return schedule, ""

        next_run = _next_local_time_occurrence(
            now_ts=now_ts,
            hour=hour,
            minute=minute,
            second=second,
            weekdays=None,
        )
        if next_run <= 0:
            return None, "Could not compute next run for daily schedule."
        cron_text = f"{int(second)} {int(minute)} {int(hour)} * * *"
        return {
            "next_run_ts": float(next_run),
            "interval_sec": 24 * 60 * 60.0,
            "anchor_ts": float(next_run),
            "recurrence": {
                "kind": "daily_local_time",
                "hour": int(hour),
                "minute": int(minute),
                "second": int(second),
                "cron": cron_text,
            },
            "cron": cron_text,
        }, ""

    return None, ""


def _ai_tasks_parse_schedule_text(raw_value: Any, *, now_ts: float) -> Tuple[Optional[Dict[str, Any]], str]:
    parsed, err = _ai_tasks_parse_cron_schedule(raw_value, now_ts=now_ts)
    if isinstance(parsed, dict):
        return parsed, ""
    cron_error = str(err or "").strip()

    parsed, err = _ai_tasks_parse_human_schedule(raw_value, now_ts=now_ts)
    if isinstance(parsed, dict):
        return parsed, ""
    human_error = str(err or "").strip()

    if human_error:
        return None, human_error
    if cron_error:
        return None, cron_error
    return None, ""


def _ai_tasks_ui_parse_schedule_input(raw_value: str) -> Tuple[Optional[Dict[str, Any]], str]:
    text = str(raw_value or "").strip()
    if not text:
        return None, "Schedule is required."

    now_ts = float(time.time())
    parsed, err = _ai_tasks_parse_schedule_text(text, now_ts=now_ts)
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


def _ai_tasks_kernel_extract_request_text(args: Dict[str, Any], origin: Optional[Dict[str, Any]]) -> str:
    payload = args if isinstance(args, dict) else {}
    for key in ("request", "request_text", "text", "message", "raw_message"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    origin_payload = origin if isinstance(origin, dict) else {}
    for key in ("request_text", "raw_message", "text", "message"):
        value = origin_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _ai_tasks_kernel_clean_task_prompt(task_text: Any) -> str:
    raw = str(task_text or "").strip()
    if not raw:
        return ""

    cleaned = _normalize_runtime_task_prompt(raw) or raw
    cleaned = re.sub(r"^\s*(?:hey\s+)?tater[\s,:-]+", "", cleaned, flags=re.IGNORECASE).strip()

    schedule_prefixes = (
        r"^\s*(?:every|each)\s+(?:second|minute|hour)\b\s*(?:,|:|-)?\s*",
        r"^\s*every\s+\d+\s*(?:seconds?|minutes?|hours?|days?|weeks?)\b\s*(?:,|:|-)?\s*",
        r"^\s*(?:every\s+day|everyday|daily|each\s+day|weekdays?|weekends?)\b(?:\s+at\s+\d{1,2}(?::\d{2})?(?::\d{2})?\s*(?:am|pm)?)?\s*(?:,|:|-)?\s*",
        r"^\s*(?:every\s+week|each\s+week|weekly)\b(?:\s+on\s+[a-z,\s]+)?(?:\s+at\s+\d{1,2}(?::\d{2})?(?::\d{2})?\s*(?:am|pm)?)?\s*(?:,|:|-)?\s*",
        r"^\s*on\s+(?:the\s+)?(?:[12]?\d|3[01])(?:st|nd|rd|th)(?:\s*(?:,|and)\s*(?:the\s+)?(?:[12]?\d|3[01])(?:st|nd|rd|th))*\s+of\s+(?:every|each)\s+month\b(?:\s+at\s+\d{1,2}(?::\d{2})?(?::\d{2})?\s*(?:am|pm)?)?\s*(?:,|:|-)?\s*",
        r"^\s*(?:every\s+month|each\s+month|monthly)\b(?:\s+on\s+(?:the\s+)?(?:[12]?\d|3[01])(?:st|nd|rd|th)(?:\s*(?:,|and)\s*(?:the\s+)?(?:[12]?\d|3[01])(?:st|nd|rd|th))*)?(?:\s+at\s+\d{1,2}(?::\d{2})?(?::\d{2})?\s*(?:am|pm)?)?\s*(?:,|:|-)?\s*",
        r"^\s*(?:in|after)\s+\d+\s*(?:seconds?|minutes?|hours?|days?|weeks?)\b\s*(?:,|:|-)?\s*",
        r"^\s*(?:at|@)\s*\d{1,2}(?::\d{2})?(?::\d{2})?\s*(?:am|pm)?\b\s*(?:,|:|-)?\s*",
    )
    for pattern in schedule_prefixes:
        candidate = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip(" ,.-")
        if candidate:
            cleaned = candidate

    cleaned = re.sub(r"^(?:to\s+)", "", cleaned, flags=re.IGNORECASE).strip(" ,.-")
    cleaned = " ".join(cleaned.split())
    if not cleaned:
        return raw
    if cleaned and cleaned[0].islower():
        cleaned = cleaned[0].upper() + cleaned[1:]
    if cleaned and not re.search(r"[.!?]$", cleaned):
        cleaned += "."
    return cleaned


def _ai_tasks_kernel_extract_target_hint(raw_text: Any) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    explicit = re.search(
        r"\b(?:in|to|on)\s+(?:the\s+)?(?:(?:channel|room|chat)\s+)?(#[A-Za-z0-9][A-Za-z0-9._:-]*|![^\s]+|@[A-Za-z0-9_]+)\b",
        text,
        flags=re.IGNORECASE,
    )
    if explicit:
        return str(explicit.group(1) or "").strip()
    matrix_ref = re.search(r"([!#][A-Za-z0-9._:-]+:[A-Za-z0-9._:-]+)", text)
    if matrix_ref:
        return str(matrix_ref.group(1) or "").strip()
    return ""


def _ai_tasks_kernel_coerce_targets(args: Dict[str, Any], request_text: str) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    targets = payload.get("targets")
    out: Dict[str, Any] = dict(targets) if isinstance(targets, dict) else {}

    for key in ("channel", "channel_id", "room", "room_id", "device_service", "chat_id"):
        value = payload.get(key)
        if value not in (None, ""):
            out[key] = value

    for alias_key in ("target", "destination", "to"):
        value = payload.get(alias_key)
        if value in (None, ""):
            continue
        text = str(value).strip()
        if not text:
            continue
        if text.lower() in {"current", "here", "this", "this chat", "this channel", "same chat", "same channel"}:
            continue
        if "channel" not in out and "room_id" not in out and "chat_id" not in out and "channel_id" not in out:
            out["channel"] = text

    if not out:
        hint = _ai_tasks_kernel_extract_target_hint(request_text)
        if hint:
            out["channel"] = hint

    return out


def _ai_tasks_kernel_normalize_targets(dest: str, targets: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(targets or {})
    channel_ref = (
        out.get("channel")
        or out.get("channel_id")
        or out.get("room")
        or out.get("room_id")
        or out.get("chat_id")
        or out.get("device_service")
    )
    if channel_ref in (None, ""):
        return out

    ref = str(channel_ref).strip()
    if not ref:
        return {}
    if dest == "discord":
        if ref.isdigit():
            return {"channel_id": ref}
        return {"channel": ref}
    if dest == "matrix":
        if not ref.startswith(("!", "#")) and ":" in ref:
            ref = f"#{ref}"
        return {"room_id": ref}
    if dest == "homeassistant":
        return {"device_service": ref}
    if dest == "telegram":
        return {"chat_id": ref}
    if dest == "irc":
        return {"channel": ref if ref.startswith("#") else f"#{ref}"}
    return {"channel": ref}


def _ai_tasks_kernel_load_platform_fallback_targets(dest: str, redis_obj: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if redis_obj is None:
        return out
    try:
        if dest == "discord":
            settings = redis_obj.hgetall("discord_portal_settings") or {}
            channel_id = str(settings.get("response_channel_id") or "").strip()
            if channel_id:
                out["channel_id"] = channel_id
            return out
        if dest == "irc":
            settings = (
                redis_obj.hgetall("irc_portal_settings")
                or redis_obj.hgetall("platform_settings:IRC Settings")
                or redis_obj.hgetall("platform_settings:IRC")
                or {}
            )
            channel = str(settings.get("irc_channel") or "").strip()
            if channel:
                out["channel"] = channel if channel.startswith("#") else f"#{channel}"
            return out
        if dest == "telegram":
            settings = redis_obj.hgetall("telegram_portal_settings") or {}
            chat_id = str(settings.get("response_chat_id") or "").strip()
            if chat_id:
                out["chat_id"] = chat_id
            return out
    except Exception:
        return {}
    return out


async def _ai_tasks_kernel_schedule(
    *,
    args: Optional[Dict[str, Any]],
    platform: str,
    origin: Optional[Dict[str, Any]],
    redis_obj: Any,
) -> Dict[str, Any]:
    payload = dict(args or {})
    origin_payload = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}
    if isinstance(origin, dict):
        for key, value in origin.items():
            if key not in origin_payload:
                origin_payload[key] = value

    request_text = _ai_tasks_kernel_extract_request_text(payload, origin_payload)

    raw_task_prompt = str(
        payload.get("task_prompt")
        or payload.get("prompt")
        or payload.get("task")
        or request_text
        or ""
    ).strip()
    task_prompt = _ai_tasks_kernel_clean_task_prompt(raw_task_prompt)
    if not task_prompt:
        return {"tool": "ai_tasks", "ok": False, "error": "Cannot queue: missing task prompt"}

    dest = normalize_platform(payload.get("platform"))
    if not dest:
        dest = normalize_platform(origin_payload.get("platform"))
    if not dest:
        dest = normalize_platform(platform)
    if dest not in ALLOWED_PLATFORMS:
        return {"tool": "ai_tasks", "ok": False, "error": "Cannot queue: missing destination platform"}

    now_ts = float(time.time())
    schedule_candidates: List[Any] = []
    seen_candidates: set[str] = set()
    for candidate in (
        payload.get("cron"),
        payload.get("when_ts"),
        payload.get("when"),
        payload.get("schedule"),
        payload.get("request"),
        request_text,
    ):
        if candidate in (None, ""):
            continue
        token = " ".join(str(candidate).strip().split())
        if not token or token in seen_candidates:
            continue
        seen_candidates.add(token)
        schedule_candidates.append(candidate)

    schedule_result: Optional[Dict[str, Any]] = None
    parse_error = ""
    for candidate in schedule_candidates:
        parsed, err = _ai_tasks_parse_schedule_text(candidate, now_ts=now_ts)
        if isinstance(parsed, dict):
            schedule_result = parsed
            break
        if err:
            parse_error = str(err or "").strip()

    if not isinstance(schedule_result, dict):
        guidance = (
            "Use `when`/`cron` in local time, for example `everyday at 6am`, "
            "`weekdays at 8am`, `on monday and wednesday each week`, "
            "`on the 10th of every month`, `every hour`, `every second`, "
            "or `0 0 6 * * *`."
        )
        if parse_error:
            return {"tool": "ai_tasks", "ok": False, "error": f"Cannot schedule: {parse_error} {guidance}"}
        return {"tool": "ai_tasks", "ok": False, "error": f"Cannot schedule: missing schedule details. {guidance}"}

    next_run = _ai_tasks_ui_as_float(schedule_result.get("next_run_ts"), 0.0)
    recurrence = schedule_result.get("recurrence") if isinstance(schedule_result.get("recurrence"), dict) else {}
    interval = _ai_tasks_ui_as_float(schedule_result.get("interval_sec"), 0.0)
    if next_run <= 0 or not recurrence:
        return {"tool": "ai_tasks", "ok": False, "error": "Cannot schedule: invalid recurrence."}

    title = str(payload.get("title") or "").strip() or _ai_tasks_ui_derive_title("", task_prompt)
    cron_text = str(schedule_result.get("cron") or recurrence.get("cron") or "").strip()

    raw_targets = _ai_tasks_kernel_coerce_targets(payload, request_text)
    normalized_targets = _ai_tasks_kernel_normalize_targets(dest, raw_targets)
    defaults = load_default_targets(dest, redis_obj)
    fallback_defaults = _ai_tasks_kernel_load_platform_fallback_targets(dest, redis_obj)
    if isinstance(fallback_defaults, dict) and fallback_defaults:
        for key, value in fallback_defaults.items():
            if key not in defaults and value not in (None, ""):
                defaults[key] = value
    resolved_targets, err = resolve_targets(dest, normalized_targets, origin_payload, defaults)
    if err:
        err_text = str(err).strip()
        if "missing target channel/room" in err_text.lower():
            err_text = (
                f"{err_text} for {dest}. "
                "Include a destination (targets.channel / targets.room_id / targets.chat_id), "
                "or configure that platform's default target."
            )
        return {"tool": "ai_tasks", "ok": False, "error": err_text}

    reminder_id = str(uuid.uuid4())
    schedule_payload: Dict[str, Any] = {
        "next_run_ts": float(next_run),
        "interval_sec": float(interval),
        "anchor_ts": float(next_run),
        "recurrence": dict(recurrence),
    }
    if cron_text:
        schedule_payload["cron"] = cron_text
        if isinstance(schedule_payload.get("recurrence"), dict):
            schedule_payload["recurrence"]["cron"] = cron_text

    reminder = {
        "id": reminder_id,
        "created_at": float(now_ts),
        "platform": dest,
        "title": title,
        "task_prompt": task_prompt,
        "targets": resolved_targets or {},
        "origin": normalize_origin(origin_payload),
        "meta": {
            "priority": payload.get("priority"),
            "tags": payload.get("tags"),
            "ttl_sec": payload.get("ttl_sec"),
        },
        "schedule": schedule_payload,
        "enabled": True,
    }
    redis_obj.set(f"{REMINDER_KEY_PREFIX}{reminder_id}", json.dumps(reminder))
    redis_obj.zadd(REMINDER_DUE_ZSET, {reminder_id: float(next_run)})

    next_run_human = datetime.fromtimestamp(float(next_run)).strftime("%Y-%m-%d %H:%M:%S")
    cadence = _ai_tasks_ui_recurrence_label(schedule_payload, float(interval or 0.0))
    summary = f"Recurring AI task scheduled {cadence.lower()} (next at {next_run_human})."
    return {
        "tool": "ai_tasks",
        "ok": True,
        "summary_for_user": summary,
        "result": summary,
        "reminder_id": reminder_id,
        "next_run_ts": float(next_run),
        "platform": dest,
        "title": title,
        "targets": resolved_targets or {},
        "schedule": schedule_payload,
    }


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
                "run_action": "ai_tasks_run_now",
                "run_label": "Run Now",
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
        "default_tab": "items",
        "manager_tabs": [
            {
                "key": "items",
                "label": "Current Tasks",
                "source": "items",
                "empty_message": "No scheduled AI tasks found.",
            },
            {
                "key": "create",
                "label": "Create Task",
                "source": "add_form",
            },
        ],
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

    if action_name == "ai_tasks_run_now":
        reminder_id = _ai_tasks_ui_clean_text(_value("id"))
        if not reminder_id:
            raise ValueError("Task id is required.")
        current = _ai_tasks_ui_load_reminder(client, reminder_id)
        if not isinstance(current, dict):
            raise KeyError("Scheduled task not found.")

        title = _ai_tasks_ui_clean_text(current.get("title")) or reminder_id
        enabled = _ai_tasks_ui_is_enabled(current.get("enabled"), True)
        if not enabled:
            return {
                "ok": True,
                "message": f"Task {title} is disabled. Enable it to run now.",
                "id": reminder_id,
            }

        schedule_payload = current.get("schedule") if isinstance(current.get("schedule"), dict) else {}
        if not schedule_payload:
            raise ValueError("This task has no valid schedule.")

        run_now_ts = float(time.time())
        schedule_payload["next_run_ts"] = run_now_ts
        schedule_payload.setdefault("anchor_ts", run_now_ts)
        current["schedule"] = schedule_payload
        _ai_tasks_ui_save_reminder(client, reminder_id, current)
        _ai_tasks_ui_set_due(client, reminder_id, run_now_ts)
        return {"ok": True, "message": f"Queued task {title} to run now.", "id": reminder_id}

    raise ValueError(f"Unknown action: {action_name}")


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    normalized_platform = str(platform or "").strip().lower()
    del normalized_platform
    return [
        {
            "id": "ai_tasks",
            "description": "schedule recurring AI tasks using natural phrases or cron and route output to a portal target",
            "usage": (
                '{"function":"ai_tasks","arguments":{"task_prompt":"<what to do each run>",'
                '"when":"weekdays at 8am","platform":"discord","targets":{"channel":"#general"}}}'
            ),
        },
    ]


async def run_hydra_kernel_tool(
    *,
    tool_id: str,
    args: Optional[Dict[str, Any]] = None,
    platform: str,
    scope: str = "",
    origin: Optional[Dict[str, Any]] = None,
    llm_client: Any = None,
    redis_client: Any = None,
    **_kwargs,
) -> Optional[Dict[str, Any]]:
    del scope, llm_client
    func = str(tool_id or "").strip().lower()
    if func != "ai_tasks":
        return None
    redis_obj = redis_client if redis_client is not None else globals().get("redis_client")
    if redis_obj is None:
        return {"tool": "ai_tasks", "ok": False, "error": "Scheduler store is unavailable."}
    return await _ai_tasks_kernel_schedule(
        args=args,
        platform=platform,
        origin=origin,
        redis_obj=redis_obj,
    )


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
