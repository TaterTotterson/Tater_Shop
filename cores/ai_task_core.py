import asyncio
import calendar
import json
import logging
import os
import threading
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
from notify import dispatch_notification, notifier_destination_catalog
from notify.queue import (
    ALLOWED_PLATFORMS,
    load_default_targets,
    normalize_origin,
    normalize_platform,
    resolve_targets,
)

from dotenv import load_dotenv
__version__ = "1.0.35"

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
SCHEDULER_EXCLUDED_TOOLS = {"reminder", "ai_tasks", "send_message", "attach_file"}
MEDIA_TYPES = {"image", "audio", "video", "file"}
class _StubObject:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class _StubTypingContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _StubChannel:
    def __init__(self, channel_id: str, name: str | None = None):
        self.id = int(channel_id) if str(channel_id).isdigit() else 0
        self.name = name or "scheduled"

    async def send(self, *args, **kwargs):
        return None

    async def trigger_typing(self):
        return None

    def typing(self):
        return _StubTypingContext()


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

    if platform == "webui":
        return {
            "context": origin or {},
            "request_text": task_prompt,
            "raw_message": task_prompt,
            "raw": task_prompt,
            "text": task_prompt,
            "sender": user,
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
    if recurrence_kind in {"one_time", "one_time_local_datetime", "oneshot", "one_shot"}:
        # One-time reminders are removed after execution.
        return 0.0
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


def _build_scheduler_reminder_prompt(destination_phrase: str, reminder_body: str) -> str:
    del destination_phrase
    body = str(reminder_body or "").strip()
    if body.lower().startswith("to "):
        body = body[3:].strip()
    if not body:
        body = "follow up with the user"
    return (
        "Compose one concise reminder message. "
        f"Remind the user to {body}. "
        "Return only the final message text to send."
    )


def _normalize_runtime_task_prompt(task_prompt: str) -> str:
    text = str(task_prompt or "").strip()
    if not text:
        return ""
    try:
        from kernel_tools import _ai_tasks_clean_task_prompt
        normalized = str(_ai_tasks_clean_task_prompt(text) or "").strip()
        if normalized:
            text = normalized
    except Exception:
        text = text

    # Backward-compat for legacy reminder prompts created before message-only mode.
    lower = text.lower()
    prefix = "send a message to "
    marker = " reminding the user to "
    if lower.startswith(prefix):
        pivot = lower.find(marker)
        if pivot > len(prefix):
            destination = text[len(prefix):pivot].strip(" ,.-")
            reminder_body = text[pivot + len(marker):].strip()
            if destination and reminder_body:
                return _build_scheduler_reminder_prompt(destination, reminder_body)
    return text


def _sanitize_scheduled_output_text(text: str) -> str:
    return str(text or "").strip()


async def _ai_tasks_llm_clean_output_text(text: str, llm_client: Any) -> str:
    base = str(text or "").strip()
    if not base or llm_client is None:
        return base
    system_prompt = (
        "Clean a scheduled-task response.\n"
        "Remove meta-prefaces/process commentary (for example explanations like "
        "'since no existing ... was found' or 'here's an original one').\n"
        "Keep only the final user-facing deliverable text.\n"
        "Return strict JSON only: {\"clean_text\":\"...\"}\n"
    )
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": base},
            ],
            temperature=0.0,
            max_tokens=220,
            timeout_ms=15_000,
        )
        content = str((response.get("message") or {}).get("content") or "").strip()
        parsed = _ai_tasks_kernel_parse_json_object(content)
        cleaned = str(parsed.get("clean_text") or "").strip()
        if cleaned:
            return cleaned
    except Exception:
        return base
    return base


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
        "Do not call send_message or attach_file; the scheduler handles delivery.\n"
        "If a task names multiple Home Assistant entities (for example multiple lights), "
        "run separate tool calls per entity instead of one bundled call.\n"
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
    text = await _ai_tasks_llm_clean_output_text(text, llm_client)
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
        "ui": _ai_tasks_ui_manager_payload(sort_rows, redis_obj=client),
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

    if kind in {"one_time", "one_time_local_datetime", "oneshot", "one_shot"}:
        run_at_ts = _ai_tasks_ui_as_float(
            recurrence.get("run_at_ts"),
            _ai_tasks_ui_as_float(schedule.get("next_run_ts"), 0.0),
        )
        if run_at_ts > 0:
            return datetime.fromtimestamp(run_at_ts).strftime("%Y-%m-%d %H:%M:%S")
        return ""

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


def _ai_tasks_build_one_time_schedule(*, run_at_ts: float) -> Optional[Dict[str, Any]]:
    due_ts = _ai_tasks_ui_as_float(run_at_ts, 0.0)
    if due_ts <= 0:
        return None
    run_local = datetime.fromtimestamp(float(due_ts)).astimezone()
    return {
        "next_run_ts": float(due_ts),
        "interval_sec": 0.0,
        "anchor_ts": float(due_ts),
        "recurrence": {
            "kind": "one_time_local_datetime",
            "run_at_ts": float(due_ts),
            "year": int(run_local.year),
            "month": int(run_local.month),
            "day": int(run_local.day),
            "hour": int(run_local.hour),
            "minute": int(run_local.minute),
            "second": int(run_local.second),
        },
    }


def _ai_tasks_legacy_llm_schedule_parse(
    raw_value: Any,
    *,
    now_ts: float,
    request_text: str = "",
) -> Tuple[Optional[Dict[str, Any]], str]:
    llm_client = get_llm_client_from_env()
    if llm_client is None:
        return None, "LLM schedule parser is unavailable."

    payload = dict(raw_value) if isinstance(raw_value, dict) else {"when": raw_value}
    text = str(request_text or payload.get("request") or payload.get("when") or raw_value or "").strip()
    if not text:
        return None, "missing schedule details."
    try:
        parsed, err = _ai_tasks_run_coro_blocking(
            _ai_tasks_kernel_parse_schedule_with_llm(
                args=payload,
                request_text=text,
                now_ts=now_ts,
                llm_client=llm_client,
            )
        )
        if isinstance(parsed, dict):
            return parsed, ""
        return None, str(err or "LLM could not infer a valid schedule.")
    except Exception:
        return None, "LLM schedule parser failed."


def _ai_tasks_parse_one_time_timestamp(raw_value: Any, *, now_ts: float) -> Tuple[Optional[float], str]:
    schedule, err = _ai_tasks_legacy_llm_schedule_parse(raw_value, now_ts=now_ts)
    if not isinstance(schedule, dict):
        return None, str(err or "")
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    kind = str(recurrence.get("kind") or "").strip().lower()
    if kind not in {"one_time", "one_time_local_datetime", "oneshot", "one_shot"}:
        return None, "LLM did not produce a one-time schedule."
    run_at_ts = _ai_tasks_ui_as_float(
        recurrence.get("run_at_ts"),
        _ai_tasks_ui_as_float(schedule.get("next_run_ts"), 0.0),
    )
    if run_at_ts <= now_ts:
        return None, "One-time schedule must be in the future."
    return float(run_at_ts), ""


def _ai_tasks_parse_one_time_schedule(raw_value: Any, *, now_ts: float) -> Tuple[Optional[Dict[str, Any]], str]:
    schedule, err = _ai_tasks_legacy_llm_schedule_parse(raw_value, now_ts=now_ts)
    if not isinstance(schedule, dict):
        return None, str(err or "")
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    kind = str(recurrence.get("kind") or "").strip().lower()
    if kind not in {"one_time", "one_time_local_datetime", "oneshot", "one_shot"}:
        return None, "LLM did not produce a one-time schedule."
    return schedule, ""


def _ai_tasks_parse_time_of_day(text: Any) -> Optional[Tuple[int, int, int]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    now_ts = float(time.time())
    schedule, _err = _ai_tasks_legacy_llm_schedule_parse(raw, now_ts=now_ts)
    if not isinstance(schedule, dict):
        return None
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    hour = recurrence.get("hour")
    minute = recurrence.get("minute")
    second = recurrence.get("second")
    if hour is not None and minute is not None:
        h = _as_int(hour, -1)
        m = _as_int(minute, -1)
        s = _as_int(second, 0)
        if 0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59:
            return h, m, s
    run_at_ts = _ai_tasks_ui_as_float(
        recurrence.get("run_at_ts"),
        _ai_tasks_ui_as_float(schedule.get("next_run_ts"), 0.0),
    )
    if run_at_ts <= 0:
        return None
    run_local = datetime.fromtimestamp(float(run_at_ts)).astimezone()
    return int(run_local.hour), int(run_local.minute), int(run_local.second)


def _ai_tasks_extract_weekdays_from_text(value: Any) -> List[int]:
    text = str(value or "").strip()
    if not text:
        return []
    schedule, _err = _ai_tasks_legacy_llm_schedule_parse(text, now_ts=float(time.time()))
    if not isinstance(schedule, dict):
        return []
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []
    out: List[int] = []
    for value_item in weekdays:
        day_i = _as_int(value_item, -1)
        if 0 <= day_i <= 6 and day_i not in out:
            out.append(day_i)
    return sorted(out)


def _ai_tasks_extract_monthdays_from_text(value: Any) -> List[int]:
    text = str(value or "").strip()
    if not text:
        return []
    schedule, _err = _ai_tasks_legacy_llm_schedule_parse(text, now_ts=float(time.time()))
    if not isinstance(schedule, dict):
        return []
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    monthdays = recurrence.get("monthdays") if isinstance(recurrence.get("monthdays"), list) else []
    out: List[int] = []
    for value_item in monthdays:
        day_i = _as_int(value_item, -1)
        if 1 <= day_i <= 31 and day_i not in out:
            out.append(day_i)
    return sorted(out)


def _ai_tasks_default_local_time_parts(now_ts: float) -> Tuple[int, int, int]:
    now_local = datetime.fromtimestamp(float(now_ts)).astimezone()
    return int(now_local.hour), int(now_local.minute), 0


def _ai_tasks_parse_cron_schedule(raw_value: Any, *, now_ts: float) -> Tuple[Optional[Dict[str, Any]], str]:
    schedule, err = _ai_tasks_legacy_llm_schedule_parse(raw_value, now_ts=now_ts)
    if not isinstance(schedule, dict):
        return None, str(err or "")
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    kind = str(recurrence.get("kind") or "").strip().lower()
    if kind != "cron_simple":
        return None, "LLM did not produce a cron schedule."
    return schedule, ""


def _ai_tasks_parse_human_schedule(raw_value: Any, *, now_ts: float) -> Tuple[Optional[Dict[str, Any]], str]:
    del raw_value, now_ts
    return None, "Human schedule parser is disabled; use LLM schedule parsing."


def _ai_tasks_run_coro_blocking(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result_box: Dict[str, Any] = {}
    error_box: Dict[str, Any] = {}

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            result_box["value"] = loop.run_until_complete(coro)
        except Exception as exc:
            error_box["error"] = exc
        finally:
            try:
                loop.close()
            except Exception:
                pass

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if "error" in error_box:
        raise error_box["error"]
    return result_box.get("value")


def _ai_tasks_parse_schedule_text(raw_value: Any, *, now_ts: float) -> Tuple[Optional[Dict[str, Any]], str]:
    return _ai_tasks_legacy_llm_schedule_parse(raw_value, now_ts=now_ts)


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

    if kind in {"one_time", "one_time_local_datetime", "oneshot", "one_shot"}:
        run_at_ts = _ai_tasks_ui_as_float(
            recurrence.get("run_at_ts"),
            _ai_tasks_ui_as_float(schedule.get("next_run_ts"), 0.0),
        )
        if run_at_ts > 0:
            return f"One time ({datetime.fromtimestamp(run_at_ts).strftime('%Y-%m-%d %H:%M:%S')})"
        return "One time"

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


async def _ai_tasks_llm_auto_title(
    command_text: str,
    *,
    llm_client: Any,
    max_words: int = 4,
    max_chars: int = 44,
) -> str:
    seed = " ".join(str(command_text or "").split()).strip()
    if not seed or llm_client is None:
        return ""

    system_prompt = (
        "Generate a short neutral title for a scheduled AI task.\n"
        "Return strict JSON only: {\"title\":\"...\"}\n"
        "Rules:\n"
        "- Keep it concise and descriptive.\n"
        "- Focus on the action intent.\n"
        "- Avoid schedule words and time phrasing.\n"
        "- Avoid punctuation except hyphen if needed.\n"
    )
    user_prompt = (
        f"Task text: {seed}\n"
        f"Max words: {int(max_words)}\n"
        f"Max characters: {int(max_chars)}\n"
        "Return only JSON."
    )
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=80,
            timeout_ms=15_000,
        )
        content = str((response.get("message") or {}).get("content") or "").strip()
        parsed = _ai_tasks_kernel_parse_json_object(content)
        title = " ".join(str(parsed.get("title") or "").split()).strip()
        if not title:
            return ""
        words = [word for word in title.split(" ") if word]
        if len(words) > max_words:
            title = " ".join(words[:max_words]).strip()
        if len(title) > max_chars:
            title = title[:max_chars].rstrip()
        return title
    except Exception:
        return ""


def _ai_tasks_ui_auto_title(command_text: str, *, max_words: int = 4, max_chars: int = 44) -> str:
    seed = " ".join(str(command_text or "").split()).strip()
    if not seed:
        return "Scheduled task"
    llm_client = get_llm_client_from_env()
    if llm_client is None:
        return "Scheduled task"
    try:
        title = _ai_tasks_run_coro_blocking(
            _ai_tasks_llm_auto_title(
                seed,
                llm_client=llm_client,
                max_words=max_words,
                max_chars=max_chars,
            )
        )
        title_text = str(title or "").strip()
        if title_text:
            return title_text
    except Exception:
        pass
    return "Scheduled task"


def _ai_tasks_ui_derive_title(raw_title: str, command_text: str) -> str:
    title = str(raw_title or "").strip()
    seed = " ".join(str(command_text or "").split()).strip()
    if title:
        # Older titles may be auto-truncated (ending in "..."). Prefer the full prompt when available.
        if title.endswith("...") and seed and len(seed) > len(title):
            return _ai_tasks_ui_auto_title(seed)
        return title
    if not seed:
        return "Scheduled task"
    return _ai_tasks_ui_auto_title(seed)


def _ai_tasks_ui_clean_text(value: Any) -> str:
    return str(value or "").strip()


def _ai_tasks_ui_load_destination_catalog(redis_obj: Any) -> Dict[str, Any]:
    if redis_obj is None:
        return {"platforms": []}
    try:
        payload = notifier_destination_catalog(redis_client=redis_obj, limit=250)
    except Exception:
        return {"platforms": []}
    if not isinstance(payload, dict):
        return {"platforms": []}
    platforms = payload.get("platforms")
    if not isinstance(platforms, list):
        payload["platforms"] = []
    return payload


def _ai_tasks_ui_catalog_platform_map(catalog: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    rows = catalog.get("platforms") if isinstance(catalog, dict) else []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        platform = _ai_tasks_ui_clean_text(row.get("platform")).lower()
        if not platform:
            continue
        out[platform] = row
    return out


def _ai_tasks_ui_clean_targets_dict(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for key, value in raw.items():
        token = _ai_tasks_ui_clean_text(key)
        text = _ai_tasks_ui_clean_text(value)
        if token and text:
            out[token] = text
    return out


def _ai_tasks_ui_destination_label(platform: str, targets: Dict[str, Any]) -> str:
    platform_name = _ai_tasks_ui_clean_text(platform).lower()
    payload = _ai_tasks_ui_clean_targets_dict(targets)
    if platform_name == "discord":
        channel = _ai_tasks_ui_clean_text(payload.get("channel") or payload.get("channel_id"))
        guild = _ai_tasks_ui_clean_text(payload.get("guild_id"))
        if channel and guild:
            return f"{channel} • guild {guild}"
        return channel or (f"guild {guild}" if guild else "Discord target")
    if platform_name == "irc":
        return _ai_tasks_ui_clean_text(payload.get("channel")) or "IRC channel"
    if platform_name == "matrix":
        return (
            _ai_tasks_ui_clean_text(payload.get("room_alias") or payload.get("room_id") or payload.get("channel"))
            or "Matrix room"
        )
    if platform_name == "telegram":
        channel = _ai_tasks_ui_clean_text(payload.get("channel"))
        chat_id = _ai_tasks_ui_clean_text(payload.get("chat_id"))
        if channel and chat_id and channel != chat_id:
            return f"{channel} • {chat_id}"
        return channel or chat_id or "Telegram chat"
    if platform_name == "homeassistant":
        return _ai_tasks_ui_clean_text(payload.get("device_service")) or "Home Assistant defaults"
    if platform_name == "webui":
        return "WebUI chat"
    if platform_name == "macos":
        scope = _ai_tasks_ui_clean_text(payload.get("scope"))
        device_id = _ai_tasks_ui_clean_text(payload.get("device_id"))
        if scope and device_id:
            return f"{scope} • {device_id}"
        return scope or device_id or "macOS target"
    return _ai_tasks_ui_target_to_text(platform_name, payload) or "Destination"


def _ai_tasks_ui_destination_phrase(
    platform: str,
    targets: Dict[str, Any],
    *,
    redis_obj: Any = None,
    catalog: Optional[Dict[str, Any]] = None,
) -> str:
    def _split_label_parts(raw_label: str) -> List[str]:
        parts: List[str] = []
        for token in str(raw_label or "").split("•"):
            text = _ai_tasks_ui_clean_text(token)
            if text:
                parts.append(text)
        return parts

    def _discord_room_token(value: str) -> str:
        token = _ai_tasks_ui_clean_text(value)
        if not token:
            return ""
        if token.isdigit() or token.startswith("#"):
            return token
        return f"#{token}"

    def _discord_destination_phrase(
        platform_label_value: str,
        *,
        clean_target_values: Dict[str, str],
        matched_target_values: Dict[str, str],
        matched_label_value: str,
    ) -> str:
        room_value = _discord_room_token(
            clean_target_values.get("channel")
            or matched_target_values.get("channel")
        )
        if not room_value:
            room_value = _ai_tasks_ui_clean_text(
                clean_target_values.get("channel_id")
                or matched_target_values.get("channel_id")
            )
        guild_value = _ai_tasks_ui_clean_text(
            clean_target_values.get("guild_name")
            or clean_target_values.get("guild")
            or matched_target_values.get("guild_name")
            or matched_target_values.get("guild")
        )
        guild_id_value = _ai_tasks_ui_clean_text(
            clean_target_values.get("guild_id")
            or matched_target_values.get("guild_id")
        )

        if matched_label_value:
            label_parts = _split_label_parts(matched_label_value)
            if label_parts:
                if not room_value:
                    room_value = _discord_room_token(label_parts[0])
                if len(label_parts) > 1 and not guild_value:
                    guild_value = label_parts[1]
        if not guild_value:
            guild_value = guild_id_value

        if room_value and guild_value:
            return f"{platform_label_value}: room: {room_value} • guild: {guild_value}"
        if room_value:
            return f"{platform_label_value}: room: {room_value}"
        if guild_value:
            return f"{platform_label_value}: guild: {guild_value}"
        return ""

    platform_name = _ai_tasks_ui_clean_text(platform).lower()
    clean_targets = _ai_tasks_ui_clean_targets_dict(targets)
    catalog_payload = catalog if isinstance(catalog, dict) else _ai_tasks_ui_load_destination_catalog(redis_obj)
    platform_row = _ai_tasks_ui_catalog_platform_map(catalog_payload).get(platform_name, {})
    platform_label = _ai_tasks_ui_clean_text(platform_row.get("label")) or platform_name or "destination"

    matched_label = ""
    matched_targets: Dict[str, str] = {}
    destinations = platform_row.get("destinations") if isinstance(platform_row, dict) else None
    if isinstance(destinations, list):
        encoded_current = _ai_tasks_ui_encode_destination_value(platform_name, clean_targets)
        for row in destinations:
            if not isinstance(row, dict):
                continue
            row_targets = _ai_tasks_ui_clean_targets_dict(row.get("targets"))
            row_value = _ai_tasks_ui_encode_destination_value(platform_name, row_targets)
            if encoded_current and row_value == encoded_current:
                matched_targets = row_targets
                matched_label = _ai_tasks_ui_clean_text(row.get("label")) or _ai_tasks_ui_destination_label(platform_name, row_targets)
                break

    if platform_name == "discord":
        explicit_discord_phrase = _discord_destination_phrase(
            platform_label,
            clean_target_values=clean_targets,
            matched_target_values=matched_targets,
            matched_label_value=matched_label,
        )
        if explicit_discord_phrase:
            return explicit_discord_phrase

    if matched_label:
        return f"{platform_label}: {matched_label}"

    destination_label = _ai_tasks_ui_destination_label(platform_name, clean_targets)
    if destination_label:
        return f"{platform_label}: {destination_label}"
    return platform_label or "destination"


def _ai_tasks_ui_encode_destination_value(platform: str, targets: Dict[str, Any]) -> str:
    platform_name = _ai_tasks_ui_clean_text(platform).lower()
    if not platform_name:
        return ""
    payload = {
        "platform": platform_name,
        "targets": _ai_tasks_ui_clean_targets_dict(targets),
    }
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)
    except Exception:
        return ""


def _ai_tasks_ui_decode_destination_value(raw_value: Any) -> Tuple[str, Dict[str, str]]:
    value = _ai_tasks_ui_clean_text(raw_value)
    if not value:
        return "", {}
    try:
        parsed = json.loads(value)
    except Exception:
        return "", {}
    if not isinstance(parsed, dict):
        return "", {}
    platform = _ai_tasks_ui_clean_text(parsed.get("platform")).lower()
    targets = _ai_tasks_ui_clean_targets_dict(parsed.get("targets"))
    return platform, targets


def _ai_tasks_ui_platform_options(
    schedules: Optional[List[Dict[str, Any]]] = None,
    *,
    redis_obj: Any = None,
    catalog: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    names: List[str] = []
    labels: Dict[str, str] = {}
    for token in _AI_TASKS_UI_DEFAULT_PORTALS + ["webui"]:
        name = _ai_tasks_ui_clean_text(token).lower()
        if not name:
            continue
        if name not in names:
            names.append(name)
        labels.setdefault(name, name)

    catalog_payload = catalog if isinstance(catalog, dict) else _ai_tasks_ui_load_destination_catalog(redis_obj)
    for platform_name, row in _ai_tasks_ui_catalog_platform_map(catalog_payload).items():
        if platform_name not in names:
            names.append(platform_name)
        label = _ai_tasks_ui_clean_text(row.get("label")) or platform_name
        labels[platform_name] = label

    for row in schedules or []:
        name = _ai_tasks_ui_clean_text(row.get("platform")).lower()
        if not name:
            continue
        if name not in names:
            names.append(name)
        labels.setdefault(name, name)

    return [{"value": name, "label": labels.get(name, name)} for name in names]


def _ai_tasks_ui_destination_options_for_platform(
    platform: str,
    *,
    catalog: Dict[str, Any],
    current_targets: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    platform_name = _ai_tasks_ui_clean_text(platform).lower()
    platform_row = _ai_tasks_ui_catalog_platform_map(catalog).get(platform_name, {})
    requires_target = bool(platform_row.get("requires_target")) if isinstance(platform_row, dict) else False
    if not isinstance(platform_row, dict):
        platform_row = {}

    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    if requires_target:
        out.append({"value": "", "label": "(Select destination)"})
        seen.add("")
    else:
        default_value = _ai_tasks_ui_encode_destination_value(platform_name, {})
        out.append({"value": default_value, "label": "Portal defaults"})
        seen.add(default_value)

    destinations = platform_row.get("destinations")
    if isinstance(destinations, list):
        for row in destinations:
            if not isinstance(row, dict):
                continue
            targets = _ai_tasks_ui_clean_targets_dict(row.get("targets"))
            value = _ai_tasks_ui_encode_destination_value(platform_name, targets)
            if not value or value in seen:
                continue
            label = _ai_tasks_ui_clean_text(row.get("label")) or _ai_tasks_ui_destination_label(platform_name, targets)
            out.append({"value": value, "label": label})
            seen.add(value)

    current_clean = _ai_tasks_ui_clean_targets_dict(current_targets)
    if current_clean:
        current_value = _ai_tasks_ui_encode_destination_value(platform_name, current_clean)
        if current_value and current_value not in seen:
            out.append(
                {
                    "value": current_value,
                    "label": f"{_ai_tasks_ui_destination_label(platform_name, current_clean)} (current)",
                }
            )

    return out


def _ai_tasks_ui_destination_options_all_platforms(*, catalog: Dict[str, Any]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = [{"value": "", "label": "(Select destination)"}]
    seen: set[str] = {""}
    for platform_name, platform_row in _ai_tasks_ui_catalog_platform_map(catalog).items():
        platform_label = _ai_tasks_ui_clean_text(platform_row.get("label")) or platform_name
        requires_target = bool(platform_row.get("requires_target"))
        if not requires_target:
            value = _ai_tasks_ui_encode_destination_value(platform_name, {})
            if value and value not in seen:
                out.append({"value": value, "label": f"{platform_label}: defaults"})
                seen.add(value)
        destinations = platform_row.get("destinations")
        if not isinstance(destinations, list):
            continue
        for row in destinations:
            if not isinstance(row, dict):
                continue
            targets = _ai_tasks_ui_clean_targets_dict(row.get("targets"))
            value = _ai_tasks_ui_encode_destination_value(platform_name, targets)
            if not value or value in seen:
                continue
            label = _ai_tasks_ui_clean_text(row.get("label")) or _ai_tasks_ui_destination_label(platform_name, targets)
            out.append({"value": value, "label": f"{platform_label}: {label}"})
            seen.add(value)
    return out


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


def _ai_tasks_kernel_parse_json_object(raw_text: Any) -> Dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _ai_tasks_kernel_collect_schedule_candidates(args: Dict[str, Any], request_text: str) -> List[Any]:
    payload = args if isinstance(args, dict) else {}
    out: List[Any] = []
    seen: set[str] = set()
    for candidate in (
        payload.get("cron"),
        payload.get("when_ts"),
        payload.get("in_seconds"),
        payload.get("when"),
        payload.get("at"),
        payload.get("run_at"),
        payload.get("schedule"),
        payload.get("request"),
        request_text,
    ):
        if candidate in (None, ""):
            continue
        token = " ".join(str(candidate).strip().split())
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(candidate)
    return out


def _ai_tasks_kernel_int_list(raw: Any, *, minimum: int, maximum: int) -> List[int]:
    if not isinstance(raw, list):
        return []
    out: List[int] = []
    for value in raw:
        try:
            item = int(value)
        except Exception:
            continue
        if minimum <= item <= maximum and item not in out:
            out.append(item)
    return sorted(out)


def _ai_tasks_kernel_token_set(raw_text: Any) -> set[str]:
    text = str(raw_text or "").strip().lower()
    if not text:
        return set()
    tokens: List[str] = []
    current: List[str] = []
    for ch in text:
        if ch.isalnum():
            current.append(ch)
            continue
        if current:
            tokens.append("".join(current))
            current = []
    if current:
        tokens.append("".join(current))
    return set(tokens)


def _ai_tasks_kernel_normalize_weekdays_from_request(weekdays: List[int], request_text: str) -> List[int]:
    clean = sorted({int(day) for day in weekdays if 0 <= int(day) <= 6})
    if not clean:
        return []
    tokens = _ai_tasks_kernel_token_set(request_text)
    if {"weekday", "weekdays"} & tokens and clean == [1, 2, 3, 4, 5]:
        return [0, 1, 2, 3, 4]
    if {"weekend", "weekends"} & tokens and clean == [0, 6]:
        return [5, 6]
    return clean


def _ai_tasks_kernel_clock_parts(raw: Any) -> Tuple[Optional[Tuple[int, int, int]], str]:
    payload = raw if isinstance(raw, dict) else {}
    try:
        hour = int(payload.get("hour"))
        minute = int(payload.get("minute"))
    except Exception:
        return None, "LLM schedule parser missing hour/minute."
    second = _as_int(payload.get("second"), 0)
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        return None, "LLM schedule parser produced invalid clock values."
    return (hour, minute, second), ""


def _ai_tasks_kernel_schedule_from_llm(
    parsed: Dict[str, Any],
    *,
    now_ts: float,
    request_text: str = "",
) -> Tuple[Optional[Dict[str, Any]], str]:
    if not isinstance(parsed, dict):
        return None, "LLM schedule parser returned invalid JSON."

    if parsed.get("error") not in (None, ""):
        return None, str(parsed.get("error")).strip() or "LLM could not infer a schedule."

    kind = str(parsed.get("kind") or parsed.get("schedule_kind") or "").strip().lower()
    if not kind:
        return None, "LLM schedule parser did not return a schedule kind."

    if kind in {"one_time", "one_time_local_datetime", "oneshot", "one_shot"}:
        one_payload = parsed.get("one_time") if isinstance(parsed.get("one_time"), dict) else parsed
        run_at_ts = _ai_tasks_ui_as_float(
            one_payload.get("run_at_ts"),
            _ai_tasks_ui_as_float(one_payload.get("when_ts"), 0.0),
        )
        if run_at_ts <= now_ts:
            return None, "One-time schedule must be in the future."
        schedule = _ai_tasks_build_one_time_schedule(run_at_ts=float(run_at_ts))
        if not isinstance(schedule, dict):
            return None, "Could not compute one-time schedule."
        return schedule, ""

    if kind in {"daily", "daily_local_time"}:
        daily_payload = parsed.get("daily") if isinstance(parsed.get("daily"), dict) else parsed
        parts, err = _ai_tasks_kernel_clock_parts(daily_payload)
        if not parts:
            return None, err
        hour, minute, second = parts
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

    if kind in {"weekly", "weekly_local_time"}:
        weekly_payload = parsed.get("weekly") if isinstance(parsed.get("weekly"), dict) else parsed
        parts, err = _ai_tasks_kernel_clock_parts(weekly_payload)
        if not parts:
            return None, err
        hour, minute, second = parts
        weekdays = _ai_tasks_kernel_int_list(weekly_payload.get("weekdays"), minimum=0, maximum=6)
        if not weekdays:
            return None, "LLM schedule parser missing weekdays."
        weekdays = _ai_tasks_kernel_normalize_weekdays_from_request(weekdays, request_text)
        next_run = _next_local_time_occurrence(
            now_ts=now_ts,
            hour=hour,
            minute=minute,
            second=second,
            weekdays=weekdays,
        )
        if next_run <= 0:
            return None, "Could not compute next run for weekly schedule."
        day_tokens = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        dow_tokens = [day_tokens[int(d)] for d in weekdays if 0 <= int(d) <= 6]
        cron_text = f"{int(second)} {int(minute)} {int(hour)} * * {','.join(dow_tokens)}" if dow_tokens else ""
        recurrence: Dict[str, Any] = {
            "kind": "weekly_local_time",
            "hour": int(hour),
            "minute": int(minute),
            "second": int(second),
            "weekdays": weekdays,
        }
        schedule: Dict[str, Any] = {
            "next_run_ts": float(next_run),
            "interval_sec": 0.0,
            "anchor_ts": float(next_run),
            "recurrence": recurrence,
        }
        if cron_text:
            recurrence["cron"] = cron_text
            schedule["cron"] = cron_text
        return schedule, ""

    if kind in {"monthly", "monthly_local_time"}:
        monthly_payload = parsed.get("monthly") if isinstance(parsed.get("monthly"), dict) else parsed
        parts, err = _ai_tasks_kernel_clock_parts(monthly_payload)
        if not parts:
            return None, err
        hour, minute, second = parts
        monthdays = _ai_tasks_kernel_int_list(monthly_payload.get("monthdays"), minimum=1, maximum=31)
        if not monthdays:
            return None, "LLM schedule parser missing monthdays."
        next_run = _next_monthly_local_time_occurrence(
            now_ts=now_ts,
            hour=hour,
            minute=minute,
            second=second,
            monthdays=monthdays,
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
                "monthdays": monthdays,
            },
        }, ""

    if kind in {"cron", "cron_simple"}:
        cron_payload = parsed.get("cron") if isinstance(parsed.get("cron"), dict) else parsed
        hours = _ai_tasks_kernel_int_list(cron_payload.get("hours"), minimum=0, maximum=23)
        minutes = _ai_tasks_kernel_int_list(cron_payload.get("minutes"), minimum=0, maximum=59)
        seconds = _ai_tasks_kernel_int_list(cron_payload.get("seconds"), minimum=0, maximum=59)
        weekdays = _ai_tasks_kernel_int_list(cron_payload.get("weekdays"), minimum=0, maximum=6)
        weekdays = _ai_tasks_kernel_normalize_weekdays_from_request(weekdays, request_text)
        if not hours or not minutes or not seconds:
            return None, "LLM schedule parser missing cron clock fields."
        cron_text = str(cron_payload.get("cron") or parsed.get("cron_text") or "").strip()
        schedule = _ai_tasks_build_cron_simple_schedule(
            now_ts=now_ts,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            weekdays=weekdays,
            cron=cron_text,
        )
        if not isinstance(schedule, dict):
            return None, "Could not compute next run for cron schedule."
        return schedule, ""

    return None, f"Unsupported schedule kind: {kind}"


async def _ai_tasks_kernel_parse_schedule_with_llm(
    *,
    args: Dict[str, Any],
    request_text: str,
    now_ts: float,
    llm_client: Any,
) -> Tuple[Optional[Dict[str, Any]], str]:
    if llm_client is None:
        return None, "LLM schedule parser is unavailable."

    candidates = _ai_tasks_kernel_collect_schedule_candidates(args, request_text)
    if not candidates:
        return None, "missing schedule details."

    payload = args if isinstance(args, dict) else {}
    schedule_input = {
        "request_text": str(request_text or "").strip(),
        "args": {
            "cron": payload.get("cron"),
            "when_ts": payload.get("when_ts"),
            "in_seconds": payload.get("in_seconds"),
            "when": payload.get("when"),
            "at": payload.get("at"),
            "run_at": payload.get("run_at"),
            "schedule": payload.get("schedule"),
        },
        "candidates": candidates,
    }
    try:
        schedule_input_text = json.dumps(schedule_input, ensure_ascii=False)
    except Exception:
        schedule_input_text = str(schedule_input)
    now_local = datetime.fromtimestamp(float(now_ts)).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    system_prompt = (
        "You parse scheduling requests for an AI task scheduler.\n"
        "Return strict JSON only.\n"
        "Schema:\n"
        "{\n"
        '  "kind": "one_time_local_datetime|daily_local_time|weekly_local_time|monthly_local_time|cron_simple",\n'
        '  "one_time": {"run_at_ts": <unix_seconds_float>},\n'
        '  "daily": {"hour":0-23,"minute":0-59,"second":0-59},\n'
        '  "weekly": {"hour":0-23,"minute":0-59,"second":0-59,"weekdays":[0-6]},\n'
        '  "monthly": {"hour":0-23,"minute":0-59,"second":0-59,"monthdays":[1-31]},\n'
        '  "cron": {"hours":[0-23],"minutes":[0-59],"seconds":[0-59],"weekdays":[0-6],"cron":"optional"}\n'
        "}\n"
        "When ambiguous or impossible, return: {\"error\":\"...\"}.\n"
        "Treat this as a stateless single-turn parse.\n"
        "Do not use or infer any timing from prior chat history.\n"
        "The current-turn request_text in the input JSON is authoritative.\n"
        "If input fields conflict, prioritize request_text for schedule intent.\n"
        "Use local time.\n"
        "For relative phrases such as 'in 3 min', 'in 2 hours', or 'after 30 seconds', "
        "anchor strictly to the provided Current unix timestamp.\n"
        "Never anchor relative offsets to any previously discussed time.\n"
        "Weekday index mapping must be Python weekday indexing: "
        "Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6.\n"
        "For 'weekdays', use [0,1,2,3,4]. For 'weekends', use [5,6].\n"
        "Interpret compact time like 'at 630' as 06:30 local at the next valid future occurrence.\n"
        "For one-time reminders, always return kind one_time_local_datetime.\n"
    )
    user_prompt = (
        f"Current unix timestamp: {float(now_ts):.3f}\n"
        f"Current local time: {now_local}\n"
        f"Input JSON: {schedule_input_text}\n"
        "Return only the JSON object."
    )
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=320,
            timeout_ms=20_000,
        )
        content = str((response.get("message") or {}).get("content") or "").strip()
        parsed = _ai_tasks_kernel_parse_json_object(content)
        if not parsed:
            return None, "LLM schedule parser returned invalid JSON."
        schedule, err = _ai_tasks_kernel_schedule_from_llm(parsed, now_ts=now_ts, request_text=request_text)
        if isinstance(schedule, dict):
            return schedule, ""
        return None, str(err or "LLM could not infer a schedule.")
    except Exception:
        return None, "LLM schedule parser failed."


async def _ai_tasks_kernel_refine_task_prompt(task_prompt: str, request_text: str, llm_client: Any) -> Dict[str, Any]:
    base = str(task_prompt or "").strip()
    if not base or llm_client is None:
        return {}

    system_prompt = (
        "Rewrite scheduled task content into execution-only text.\n"
        "Remove schedule/time/calendar phrases (for example 'every weekday at 6:00 AM').\n"
        "Remove scheduling verbs and wording (create/schedule/cancel/update/at/every).\n"
        "Remove assistant-addressing lead-ins (for example 'hey tater').\n"
        "If one sentence targets multiple Home Assistant lights/areas, split into separate action sentences.\n"
        "Keep only what should happen when the task runs.\n"
        "If the request is a reminder intent (for example 'remind me to ...'), set reminder_intent=true.\n"
        "For reminders, set reminder_text to the thing the user should be reminded about, "
        "with no schedule wording and no leading 'remind me'.\n"
        "Return strict JSON only: "
        "{\"task_prompt\":\"...\",\"reminder_intent\":true|false,\"reminder_text\":\"...\"}\n"
    )
    user_prompt = (
        f"Original request: {request_text}\n"
        f"Candidate task prompt: {base}\n"
        "Return execution-only task text."
    )
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=180,
            timeout_ms=20_000,
        )
        content = str((response.get("message") or {}).get("content") or "").strip()
        parsed = _ai_tasks_kernel_parse_json_object(content)
        refined = str(parsed.get("task_prompt") or "").strip()
        if refined:
            reminder_text = str(parsed.get("reminder_text") or "").strip()
            reminder_flag = parsed.get("reminder_intent")
            if isinstance(reminder_flag, bool):
                reminder_intent = reminder_flag
            else:
                reminder_intent = str(reminder_flag or "").strip().lower() in {"1", "true", "yes", "y"}
            return {
                "task_prompt": refined,
                "reminder_intent": reminder_intent,
                "reminder_text": reminder_text,
            }
    except Exception:
        return {}
    return {}


async def _ai_tasks_kernel_extract_target_hint(raw_text: Any, llm_client: Any) -> str:
    text = str(raw_text or "").strip()
    if not text or llm_client is None:
        return ""
    system_prompt = (
        "Extract the single explicit destination hint from a user request.\n"
        "Return strict JSON only: {\"target_hint\":\"...\"}\n"
        "Use empty string when no explicit destination/channel/room/chat/service is present.\n"
        "Preserve the exact token if present (examples: #general, !room:id, @bot, device_service name).\n"
    )
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text},
            ],
            temperature=0.0,
            max_tokens=100,
            timeout_ms=15_000,
        )
        content = str((response.get("message") or {}).get("content") or "").strip()
        parsed = _ai_tasks_kernel_parse_json_object(content)
        hint = str(parsed.get("target_hint") or "").strip()
        if hint:
            return hint
    except Exception:
        return ""
    return ""


def _ai_tasks_kernel_collect_explicit_targets(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    targets = payload.get("targets")
    out: Dict[str, Any] = dict(targets) if isinstance(targets, dict) else {}

    def _generic_platform_token(value: Any) -> bool:
        text = " ".join(str(value or "").strip().split()).lower()
        if not text:
            return True
        if text.startswith(("#", "!", "@")):
            return False
        if text.isdigit():
            return False
        platform_tokens = {str(name).strip().lower() for name in ALLOWED_PLATFORMS}
        platform_tokens.update({"home assistant", "web ui", "web"})
        return text in platform_tokens

    for key in ("channel", "channel_id", "room", "room_id", "device_service", "chat_id"):
        value = payload.get(key)
        if value not in (None, ""):
            if key in {"channel", "room", "device_service"} and _generic_platform_token(value):
                continue
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
        if _generic_platform_token(text):
            continue
        if "channel" not in out and "room_id" not in out and "chat_id" not in out and "channel_id" not in out:
            out["channel"] = text

    return out


async def _ai_tasks_kernel_coerce_targets(args: Dict[str, Any], request_text: str, llm_client: Any) -> Dict[str, Any]:
    del request_text, llm_client
    # Routing must be explicit. Do not infer destination hints from free-form text.
    return _ai_tasks_kernel_collect_explicit_targets(args)


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
        normalized: Dict[str, Any] = {}
        if ref.isdigit():
            normalized["channel_id"] = ref
            channel_name = str(out.get("channel") or "").strip()
            if channel_name:
                normalized["channel"] = channel_name
        else:
            normalized["channel"] = ref
        guild_id = str(out.get("guild_id") or "").strip()
        if guild_id:
            normalized["guild_id"] = guild_id
        guild_name = str(out.get("guild_name") or out.get("guild") or "").strip()
        if guild_name:
            normalized["guild_name"] = guild_name
        return normalized
    if dest == "matrix":
        room_ref = ref
        if not room_ref.startswith(("!", "#")) and ":" in room_ref:
            room_ref = f"#{room_ref}"
        normalized = {"room_id": room_ref}
        room_alias = str(out.get("room_alias") or "").strip()
        if room_alias:
            normalized["room_alias"] = room_alias
        return normalized
    if dest == "homeassistant":
        return {"device_service": ref}
    if dest == "telegram":
        normalized = {"chat_id": ref}
        channel_name = str(out.get("channel") or "").strip()
        if channel_name:
            normalized["channel"] = channel_name
        return normalized
    if dest == "irc":
        return {"channel": ref if ref.startswith("#") else f"#{ref}"}
    return {"channel": ref}


def _ai_tasks_kernel_origin_targets_for_dest(dest: str, origin: Dict[str, Any]) -> Dict[str, Any]:
    origin_payload = origin if isinstance(origin, dict) else {}
    if dest == "discord":
        out: Dict[str, Any] = {}
        channel_id = str(origin_payload.get("channel_id") or "").strip()
        channel_name = str(origin_payload.get("channel") or "").strip()
        guild_id = str(origin_payload.get("guild_id") or "").strip()
        guild_name = str(origin_payload.get("guild_name") or origin_payload.get("guild") or "").strip()
        if channel_id:
            out["channel_id"] = channel_id
        elif channel_name:
            out["channel"] = channel_name
        if channel_name:
            out["channel"] = channel_name
        if guild_id:
            out["guild_id"] = guild_id
        if guild_name:
            out["guild_name"] = guild_name
        return out
    if dest == "matrix":
        room_id = str(origin_payload.get("room_id") or origin_payload.get("channel") or "").strip()
        if room_id:
            return {"room_id": room_id}
        return {}
    if dest == "telegram":
        out = {}
        chat_id = str(origin_payload.get("chat_id") or "").strip()
        channel_name = str(origin_payload.get("channel") or "").strip()
        if chat_id:
            out["chat_id"] = chat_id
        if channel_name:
            out["channel"] = channel_name
        return out
    if dest == "irc":
        channel = str(origin_payload.get("channel") or "").strip()
        if channel:
            return {"channel": channel if channel.startswith("#") else f"#{channel}"}
        return {}
    if dest == "homeassistant":
        device_service = str(origin_payload.get("device_service") or "").strip()
        if device_service:
            return {"device_service": device_service}
        return {}
    if dest == "webui":
        scope = str(origin_payload.get("scope") or "").strip()
        if scope:
            return {"scope": scope}
    return {}


def _ai_tasks_kernel_catalog_targets_match(
    dest: str,
    row_targets: Dict[str, Any],
    desired_targets: Dict[str, Any],
    origin: Dict[str, Any],
) -> bool:
    row = row_targets if isinstance(row_targets, dict) else {}
    desired = desired_targets if isinstance(desired_targets, dict) else {}
    origin_payload = origin if isinstance(origin, dict) else {}

    if dest == "discord":
        desired_id = str(desired.get("channel_id") or origin_payload.get("channel_id") or "").strip()
        row_id = str(row.get("channel_id") or "").strip()
        if desired_id and row_id:
            return desired_id == row_id
        desired_channel = str(desired.get("channel") or origin_payload.get("channel") or "").strip().lstrip("#").lower()
        row_channel = str(row.get("channel") or "").strip().lstrip("#").lower()
        if not (desired_channel and row_channel and desired_channel == row_channel):
            return False
        desired_guild = str(desired.get("guild_id") or origin_payload.get("guild_id") or "").strip()
        row_guild = str(row.get("guild_id") or "").strip()
        # Name-only matching is safe only when both sides carry a guild id.
        if not (desired_guild and row_guild):
            return False
        return desired_guild == row_guild

    if dest == "matrix":
        desired_room = str(desired.get("room_id") or origin_payload.get("room_id") or "").strip()
        row_room = str(row.get("room_id") or "").strip()
        return bool(desired_room and row_room and desired_room == row_room)

    if dest == "telegram":
        desired_chat = str(desired.get("chat_id") or origin_payload.get("chat_id") or "").strip()
        row_chat = str(row.get("chat_id") or "").strip()
        return bool(desired_chat and row_chat and desired_chat == row_chat)

    if dest == "irc":
        desired_channel = str(desired.get("channel") or origin_payload.get("channel") or "").strip().lower()
        row_channel = str(row.get("channel") or "").strip().lower()
        if desired_channel and not desired_channel.startswith("#"):
            desired_channel = f"#{desired_channel}"
        if row_channel and not row_channel.startswith("#"):
            row_channel = f"#{row_channel}"
        return bool(desired_channel and row_channel and desired_channel == row_channel)

    if dest == "homeassistant":
        desired_service = str(desired.get("device_service") or "").strip().lower()
        row_service = str(row.get("device_service") or "").strip().lower()
        return bool(desired_service and row_service and desired_service == row_service)

    return False


def _ai_tasks_kernel_canonicalize_targets_from_catalog(
    dest: str,
    targets: Dict[str, Any],
    origin: Dict[str, Any],
    redis_obj: Any,
) -> Dict[str, Any]:
    clean_targets = dict(targets or {})
    if not clean_targets:
        return clean_targets
    catalog = _ai_tasks_ui_load_destination_catalog(redis_obj)
    platform_row = _ai_tasks_ui_catalog_platform_map(catalog).get(dest, {})
    destinations = platform_row.get("destinations") if isinstance(platform_row, dict) else None
    if not isinstance(destinations, list):
        return clean_targets
    for row in destinations:
        if not isinstance(row, dict):
            continue
        row_targets = row.get("targets")
        if not isinstance(row_targets, dict):
            continue
        if _ai_tasks_kernel_catalog_targets_match(dest, row_targets, clean_targets, origin):
            # Keep the exact catalog target payload so UI labels/options align with room API entries.
            return dict(row_targets)
    return clean_targets


def _ai_tasks_kernel_destination_reference(dest: str, targets: Dict[str, Any], origin: Dict[str, Any]) -> str:
    payload = targets if isinstance(targets, dict) else {}
    origin_payload = origin if isinstance(origin, dict) else {}
    if dest == "webui":
        return ""
    if dest == "matrix":
        return str(
            payload.get("room_id")
            or payload.get("channel")
            or origin_payload.get("room_id")
            or origin_payload.get("channel")
            or ""
        ).strip()
    if dest == "discord":
        return str(
            payload.get("channel_id")
            or payload.get("channel")
            or origin_payload.get("channel_id")
            or origin_payload.get("channel")
            or ""
        ).strip()
    if dest == "telegram":
        return str(payload.get("chat_id") or origin_payload.get("chat_id") or "").strip()
    if dest == "homeassistant":
        return str(payload.get("device_service") or origin_payload.get("device_service") or "").strip()
    if dest == "irc":
        return str(payload.get("channel") or origin_payload.get("channel") or "").strip()
    return str(
        payload.get("channel")
        or payload.get("room_id")
        or payload.get("chat_id")
        or origin_payload.get("channel")
        or origin_payload.get("room_id")
        or origin_payload.get("chat_id")
        or ""
    ).strip()


def _ai_tasks_kernel_origin_from_scope(platform: str, scope: str) -> Dict[str, Any]:
    normalized_platform = normalize_platform(platform)
    text = str(scope or "").strip()
    if not text:
        return {}

    out: Dict[str, Any] = {}
    if normalized_platform in ALLOWED_PLATFORMS:
        out["platform"] = normalized_platform

    if normalized_platform == "discord":
        if text.startswith("channel:"):
            channel_id = text.split(":", 1)[1].strip()
            if channel_id:
                out["channel_id"] = channel_id
            return out
        if text.startswith("dm:"):
            dm_user = text.split(":", 1)[1].strip()
            if dm_user:
                out["chat_type"] = "dm"
                out["dm_user_id"] = dm_user
            return out
        return out

    if normalized_platform == "matrix":
        if text.startswith("room:"):
            room_id = text.split(":", 1)[1].strip()
            if room_id:
                out["room_id"] = room_id
        elif text.startswith(("!", "#")):
            out["room_id"] = text
        return out

    if normalized_platform == "telegram":
        if text.startswith("chat:"):
            chat_id = text.split(":", 1)[1].strip()
            if chat_id:
                out["chat_id"] = chat_id
        elif text:
            out["chat_id"] = text
        return out

    if normalized_platform == "irc":
        if text.startswith("chan:"):
            channel = text.split(":", 1)[1].strip()
            if channel:
                out["channel"] = channel
        elif text.startswith("#"):
            out["channel"] = text
        return out

    if normalized_platform == "webui":
        if text.startswith(("session:", "user:")):
            out["scope"] = text
        return out

    return out


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
    llm_client: Any = None,
) -> Dict[str, Any]:
    payload = dict(args or {})
    origin_payload = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}
    if isinstance(origin, dict):
        for key, value in origin.items():
            if value in (None, ""):
                continue
            if key not in origin_payload or origin_payload.get(key) in (None, ""):
                origin_payload[key] = value
            elif key == "platform":
                # Trust the runtime event platform over model-inferred origin payload.
                origin_payload[key] = value

    runtime_platform = normalize_platform(platform)
    if runtime_platform in ALLOWED_PLATFORMS:
        origin_payload["platform"] = runtime_platform

    request_text = _ai_tasks_kernel_extract_request_text(payload, origin_payload)

    raw_task_prompt = str(
        payload.get("task_prompt")
        or payload.get("prompt")
        or payload.get("task")
        or request_text
        or ""
    ).strip()
    refined_task = await _ai_tasks_kernel_refine_task_prompt(raw_task_prompt, request_text, llm_client)
    task_prompt = str(refined_task.get("task_prompt") or "").strip()
    reminder_intent = bool(refined_task.get("reminder_intent"))
    reminder_text = str(refined_task.get("reminder_text") or "").strip()
    if not task_prompt:
        return {"tool": "ai_tasks", "ok": False, "error": "Cannot queue: LLM task normalization failed."}

    explicit_arg_targets = _ai_tasks_kernel_collect_explicit_targets(payload)
    requested_dest = normalize_platform(payload.get("platform"))
    origin_dest = normalize_platform(origin_payload.get("platform"))
    dest = origin_dest or runtime_platform or requested_dest
    if requested_dest and requested_dest in ALLOWED_PLATFORMS and requested_dest != dest:
        requested_targets = _ai_tasks_kernel_normalize_targets(requested_dest, explicit_arg_targets)
        # Only allow cross-platform overrides when explicit target routing is provided.
        if requested_targets:
            dest = requested_dest
    used_webui_fallback = False
    if dest not in ALLOWED_PLATFORMS:
        dest = "webui"
        used_webui_fallback = True

    now_ts = float(time.time())
    schedule_result, parse_error = await _ai_tasks_kernel_parse_schedule_with_llm(
        args=payload,
        request_text=request_text,
        now_ts=now_ts,
        llm_client=llm_client,
    )

    if not isinstance(schedule_result, dict):
        detail = str(parse_error or "LLM could not infer a valid schedule.").strip()
        return {"tool": "ai_tasks", "ok": False, "error": f"Cannot schedule: {detail}"}

    next_run = _ai_tasks_ui_as_float(schedule_result.get("next_run_ts"), 0.0)
    recurrence = schedule_result.get("recurrence") if isinstance(schedule_result.get("recurrence"), dict) else {}
    interval = _ai_tasks_ui_as_float(schedule_result.get("interval_sec"), 0.0)
    if next_run <= 0 or not recurrence:
        return {"tool": "ai_tasks", "ok": False, "error": "Cannot schedule: invalid recurrence."}

    cron_text = str(schedule_result.get("cron") or recurrence.get("cron") or "").strip()

    raw_targets = await _ai_tasks_kernel_coerce_targets(payload, request_text, llm_client)
    if not raw_targets:
        # Default to the room/channel/chat where the request originated.
        raw_targets = _ai_tasks_kernel_origin_targets_for_dest(dest, origin_payload)

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
        if "missing target" in err_text.lower() and dest != "webui":
            fallback_dest = "webui"
            fallback_defaults = load_default_targets(fallback_dest, redis_obj)
            fallback_targets, fallback_err = resolve_targets(fallback_dest, {}, origin_payload, fallback_defaults)
            if not fallback_err:
                dest = fallback_dest
                resolved_targets = fallback_targets or {}
                err = None
                used_webui_fallback = True
    if err:
        err_text = str(err).strip()
        if "missing target channel/room" in err_text.lower():
            err_text = (
                f"{err_text} for {dest}. "
                "Include a destination (targets.channel / targets.room_id / targets.chat_id), "
                "or configure that platform's default target."
            )
        return {"tool": "ai_tasks", "ok": False, "error": err_text}

    resolved_targets = _ai_tasks_kernel_canonicalize_targets_from_catalog(
        dest,
        resolved_targets or {},
        origin_payload,
        redis_obj,
    )

    if reminder_intent:
        reminder_body = str(reminder_text or task_prompt).strip()
        task_prompt = _build_scheduler_reminder_prompt("", reminder_body)

    title_seed = reminder_text if (reminder_intent and reminder_text) else task_prompt
    title = str(payload.get("title") or "").strip()
    if not title:
        llm_title = await _ai_tasks_llm_auto_title(title_seed, llm_client=llm_client)
        title = str(llm_title or "").strip() or "Scheduled task"

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
    recurrence_kind = str(recurrence.get("kind") or "").strip().lower()
    if recurrence_kind in {"one_time", "one_time_local_datetime", "oneshot", "one_shot"}:
        summary = f"One-time AI task scheduled for {next_run_human}."
    else:
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
        "used_webui_fallback": bool(used_webui_fallback),
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


def _ai_tasks_ui_manager_payload(schedules: List[Dict[str, Any]], *, redis_obj: Any = None) -> Dict[str, Any]:
    rows = list(schedules or [])
    catalog = _ai_tasks_ui_load_destination_catalog(redis_obj)
    platform_map = _ai_tasks_ui_catalog_platform_map(catalog)
    add_destination_options = _ai_tasks_ui_destination_options_all_platforms(catalog=catalog)
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
        targets_payload = row.get("targets") if isinstance(row.get("targets"), dict) else {}
        destination_options = _ai_tasks_ui_destination_options_all_platforms(catalog=catalog)
        requires_target = bool(platform_map.get(platform, {}).get("requires_target"))
        if targets_payload:
            destination_value = _ai_tasks_ui_encode_destination_value(platform, targets_payload)
            known_values = {str(item.get("value") or "") for item in destination_options}
            if destination_value and destination_value not in known_values:
                destination_options.append(
                    {
                        "value": destination_value,
                        "label": f"{platform}: {_ai_tasks_ui_destination_label(platform, targets_payload)} (current)",
                    }
                )
        elif requires_target:
            destination_value = ""
        else:
            destination_value = _ai_tasks_ui_encode_destination_value(platform, {})

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
                        "key": "destination",
                        "label": "Room / Destination",
                        "type": "select",
                        "description": "Choose where this task should post output.",
                        "options": destination_options,
                        "value": destination_value,
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
        "item_fields_popup": True,
        "item_fields_popup_label": "Task Settings",
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
                    "key": "destination",
                    "label": "Room / Destination (optional)",
                    "type": "select",
                    "description": "Known rooms/channels discovered by the notifier destination catalog.",
                    "options": add_destination_options,
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

        catalog = _ai_tasks_ui_load_destination_catalog(client)
        platform = _ai_tasks_ui_clean_text(_value("platform")).lower() or "homeassistant"
        allowed_platforms = {
            str(item.get("value") or "").strip().lower()
            for item in _ai_tasks_ui_platform_options([], redis_obj=client, catalog=catalog)
        }
        destination_raw = _ai_tasks_ui_clean_text(_value("destination"))
        destination_platform, destination_targets = _ai_tasks_ui_decode_destination_value(destination_raw)
        if destination_platform and destination_platform in allowed_platforms:
            platform = destination_platform
        if platform not in allowed_platforms:
            raise ValueError(f"Unsupported portal: {platform}")

        targets: Dict[str, Any] = dict(destination_targets) if isinstance(destination_targets, dict) else {}
        if destination_raw and not destination_platform and not targets:
            targets = _ai_tasks_ui_target_from_text(platform, destination_raw)
        if not targets and not destination_platform:
            # Legacy/manual fallback for old payloads that still submit a text target field.
            target_text = _ai_tasks_ui_clean_text(_value("target"))
            if target_text:
                targets = _ai_tasks_ui_target_from_text(platform, target_text)

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
            "targets": targets,
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

        catalog = _ai_tasks_ui_load_destination_catalog(client)
        platform = _ai_tasks_ui_clean_text(_value("platform")).lower() or _ai_tasks_ui_clean_text(current.get("platform")).lower()
        allowed_platforms = {
            str(item.get("value") or "").strip().lower()
            for item in _ai_tasks_ui_platform_options([], redis_obj=client, catalog=catalog)
        }
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

        current_platform = _ai_tasks_ui_clean_text(current.get("platform")).lower()
        current_targets = current.get("targets") if isinstance(current.get("targets"), dict) else {}
        current_destination_value = _ai_tasks_ui_encode_destination_value(current_platform, current_targets)

        destination_raw = _ai_tasks_ui_clean_text(_value("destination"))
        destination_platform, destination_targets = _ai_tasks_ui_decode_destination_value(destination_raw)
        if destination_platform and destination_platform in allowed_platforms:
            stale_destination_for_platform_switch = (
                platform != current_platform
                and destination_raw
                and destination_raw == current_destination_value
            )
            if not stale_destination_for_platform_switch:
                platform = destination_platform

        if destination_platform and destination_platform == platform:
            targets = dict(destination_targets)
        elif destination_platform and destination_platform != platform:
            targets = {}
        elif destination_raw:
            targets = _ai_tasks_ui_target_from_text(platform, destination_raw)
        else:
            target_changed = ("target" in values) or ("target" in body)
            if target_changed:
                targets = _ai_tasks_ui_target_from_text(platform, _ai_tasks_ui_clean_text(_value("target")))
            elif platform != current_platform:
                targets = {}
            else:
                targets = current_targets

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
            "description": "schedule one-time or recurring AI tasks with natural phrases/cron and route output to a portal target",
            "usage": (
                '{"function":"ai_tasks","arguments":{"task_prompt":"<what to do>","when":"at 6:30am"}} '
                'or {"function":"ai_tasks","arguments":{"task_prompt":"<what to do each run>",'
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
    func = str(tool_id or "").strip().lower()
    if func != "ai_tasks":
        return None
    redis_obj = redis_client if redis_client is not None else globals().get("redis_client")
    if redis_obj is None:
        return {"tool": "ai_tasks", "ok": False, "error": "Scheduler store is unavailable."}
    origin_payload = dict(origin) if isinstance(origin, dict) else {}
    scope_origin = _ai_tasks_kernel_origin_from_scope(platform, scope)
    if isinstance(scope_origin, dict):
        for key, value in scope_origin.items():
            if value in (None, ""):
                continue
            if origin_payload.get(key) in (None, ""):
                origin_payload[key] = value
    return await _ai_tasks_kernel_schedule(
        args=args,
        platform=platform,
        origin=origin_payload,
        redis_obj=redis_obj,
        llm_client=llm_client,
    )


def run(stop_event: Optional[object] = None):
    queued = 0
    try:
        queued = int(redis_client.zcard(REMINDER_DUE_ZSET) or 0)
    except Exception:
        queued = 0
    logger.info("[AI Tasks] Scheduler service started (queued=%d).", queued)
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
                else:
                    logger.info("[AI Tasks] Reminder %s queued to %s.", reminder_id, dest)
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
