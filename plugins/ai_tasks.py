import json
import logging
import time
import uuid
import re
from bisect import bisect_left
from datetime import datetime, timedelta
from typing import Any, Dict

from plugin_base import ToolPlugin
from helpers import redis_client
from notify.queue import (
    ALLOWED_PLATFORMS,
    load_default_targets,
    normalize_origin,
    normalize_platform,
    resolve_targets,
)

logger = logging.getLogger("ai_tasks")
logger.setLevel(logging.INFO)

REMINDER_KEY_PREFIX = "reminders:"
REMINDER_DUE_ZSET = "reminders:due"
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
    "mon": 0,
    "monday": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "wed": 2,
    "wednesday": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "thursday": 3,
    "fri": 4,
    "friday": 4,
    "sat": 5,
    "saturday": 5,
}


class AITasksPlugin(ToolPlugin):
    name = "ai_tasks"
    required_args = ["task_prompt"]
    optional_args = [
        "task_prompt",
        "request",
        "title",
        "platform",
        "targets",
        "target",
        "destination",
        "to",
        "cron",
        "when_ts",
        "when",
        "in_seconds",
        "in_minutes",
        "in_hours",
        "priority",
        "tags",
        "ttl_sec",
        "origin",
        "channel_id",
        "channel",
        "guild_id",
        "room",
        "room_id",
        "device_service",
        "chat_id",
    ]
    version = "1.2.1"
    usage = '{"function":"ai_tasks","arguments":{"task_prompt":"Required instructions for what to do when the task runs (do not pre-generate final content now)","title":"Optional short title","platform":"discord|irc|matrix|homeassistant|ntfy|telegram (optional; defaults to origin)","targets":{"channel":"discord/irc channel","room_id":"matrix room id/alias","chat_id":"telegram chat id","device_service":"home assistant notify service"},"channel":"optional shortcut for discord/irc channel","room":"optional shortcut for matrix room (alias for room_id)","chat_id":"optional shortcut for telegram target","cron":"Optional cron schedule in local time (e.g. 0 0 6 * * *).","when":"Natural schedule phrase or cron (e.g. everyday at 6am, every hour, every second, 0 0 6 * * *).","priority":"normal|high","tags":["optional","strings"],"ttl_sec":0}}'
    description = (
        "Schedule recurring AI tasks using natural phrases or cron-like schedules in local time. "
        "At run time, AI can answer directly or call one tool before sending the final response."
    )
    pretty_name = "AI Tasks"
    when_to_use = "Schedule recurring tasks (everyday, hourly, secondly, weekly, or explicit cron)."
    common_needs = [
        "task to run",
        "when to run (natural phrase or cron)",
        "destination (optional; defaults to current channel/room)",
    ]
    missing_info_prompts = [
        "What should this task do each time it runs? Put that in `task_prompt`.",
        "When should this run? You can say `everyday at 6am`, `every hour`, `every second`, or provide cron (`0 10 6 * * *`). If destination isn't this same chat/room, include `targets` (or `channel`/`room`/`chat_id`).",
    ]

    platforms = ["discord", "irc", "matrix", "homeassistant", "telegram", "webui"]
    waiting_prompt_template = (
        "Write a short, friendly message telling {mention} you’re scheduling the task now. "
        "Only output that message."
    )

    _CURRENT_TARGET_ALIASES = {
        "current",
        "here",
        "this",
        "this chat",
        "this channel",
        "same chat",
        "same channel",
        "current chat",
        "current channel",
    }

    # ----------------------------
    # NEW: build a runtime prompt
    # ----------------------------
    @staticmethod
    def _build_runtime_prompt(user_message: str) -> str:
        raw = (user_message or "").strip()
        if not raw:
            return ""

        # Strip common "scheduling" lead-ins so the runtime prompt becomes "do the thing"
        text = raw

        # remove "add/create/set a task/reminder ..."
        text = re.sub(
            r"^(?:please\s+)?(?:add|create|set|schedule)\s+(?:a\s+)?(?:task|reminder)\s+(?:to\s+)?",
            "",
            text,
            flags=re.IGNORECASE,
        )

        # remove trailing scheduling phrases like "every morning at 6am", "daily at 06:00", etc.
        text = re.sub(
            r"\b(?:every|each)\s+(?:day|morning|night|weekday|weekend|week)\b.*$",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(r"\b(?:daily|weekly|monthly)\b.*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\b(?:at|@)\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)?\b.*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\bin\s+\d+\s*(?:seconds?|minutes?|hours?|days?)\b.*$", "", text, flags=re.IGNORECASE)

        text = text.strip(" .\n\t")

        # If we stripped too aggressively, fall back to original.
        if not text:
            text = raw

        # Guardrails: scheduled runs must NEVER try to schedule more tasks.
        # Also: encourage a single tool call, then produce final message.
        return (
            "You are running a previously scheduled task.\n"
            "IMPORTANT: Do NOT create/schedule/modify tasks or reminders. The schedule already exists.\n"
            "Do the requested work NOW. You may call at most ONE tool if needed, then write the final user-facing result.\n"
            f"User originally asked: {raw}\n"
            f"Task to perform now: {text}\n"
        )

    @staticmethod
    def _normalize_channel_targets(dest: str, targets: Dict[str, Any]) -> Dict[str, Any]:
        t = dict(targets or {})
        channel_ref = t.get("channel")
        room_ref = t.get("room")
        destination_ref = t.get("destination")
        to_ref = t.get("to")
        target_ref = t.get("target")

        if not channel_ref:
            if dest == "discord":
                channel_ref = t.get("channel_id") or room_ref or destination_ref or to_ref or target_ref
            elif dest == "irc":
                channel_ref = t.get("channel") or room_ref or destination_ref or to_ref or target_ref
            elif dest == "matrix":
                channel_ref = t.get("room_id") or room_ref or destination_ref or to_ref or target_ref
            elif dest == "homeassistant":
                channel_ref = t.get("device_service") or destination_ref or to_ref or target_ref
            elif dest == "telegram":
                channel_ref = t.get("chat_id") or room_ref or destination_ref or to_ref or target_ref

        if not channel_ref:
            return t

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

        return {"channel": ref}

    @classmethod
    def _is_current_target_alias(cls, value: Any) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return False
        text = " ".join(text.replace("_", " ").split())
        return text in cls._CURRENT_TARGET_ALIASES

    @staticmethod
    def _extract_target_hint(raw: Any) -> str:
        text = str(raw or "").strip()
        if not text:
            return ""
        for pattern in (r"![^\s]+", r"#[A-Za-z0-9][A-Za-z0-9._:-]*", r"@[A-Za-z0-9_]+"):
            m = re.search(pattern, text)
            if m:
                return m.group(0)
        text = re.sub(r"^(?:room|channel|chat)\s+", "", text, flags=re.IGNORECASE)
        text = re.sub(
            r"\s+(?:in|on)\s+(?:discord|irc|matrix|telegram|home\s*assistant|homeassistant)\b.*$",
            "",
            text,
            flags=re.IGNORECASE,
        )
        return text.strip(" .")

    @staticmethod
    def _coerce_targets(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, dict):
            return dict(payload)
        if isinstance(payload, str):
            hint = AITasksPlugin._extract_target_hint(payload)
            if hint:
                return {"channel": hint}
        return {}

    @staticmethod
    def _load_platform_fallback_targets(dest: str) -> Dict[str, Any]:
        platform = normalize_platform(dest)
        out: Dict[str, Any] = {}
        try:
            if platform == "discord":
                settings = redis_client.hgetall("discord_platform_settings") or {}
                channel_id = str(settings.get("response_channel_id") or "").strip()
                if channel_id:
                    out["channel_id"] = channel_id
                return out

            if platform == "irc":
                settings = (
                    redis_client.hgetall("irc_platform_settings")
                    or redis_client.hgetall("platform_settings:IRC Settings")
                    or redis_client.hgetall("platform_settings:IRC")
                    or {}
                )
                channel = str(settings.get("irc_channel") or "").strip()
                if channel:
                    out["channel"] = channel if channel.startswith("#") else f"#{channel}"
                return out

            if platform == "telegram":
                settings = redis_client.hgetall("telegram_platform_settings") or {}
                legacy = redis_client.hgetall("plugin_settings:Telegram Notifier") or {}
                chat_id = str(settings.get("response_chat_id") or legacy.get("telegram_chat_id") or "").strip()
                if chat_id:
                    out["chat_id"] = chat_id
                return out
        except Exception:
            return {}
        return {}

    @classmethod
    def _merge_target_aliases(cls, targets: Dict[str, Any], value: Any) -> Dict[str, Any]:
        out = dict(targets or {})
        if isinstance(value, dict):
            for key, val in value.items():
                if val in (None, ""):
                    continue
                if key not in out:
                    out[key] = val
            return out
        hint = cls._extract_target_hint(value)
        if hint and "channel" not in out and "room_id" not in out and "chat_id" not in out:
            out["channel"] = hint
        return out

    def _extract_args(self, args: Dict[str, Any]):
        args = args or {}
        title = args.get("title")
        platform = args.get("platform")
        origin = args.get("origin")

        request_text = str(args.get("request") or "").strip()
        if isinstance(origin, dict):
            origin_request_text = str(origin.get("request_text") or "").strip()
            if origin_request_text:
                request_text = origin_request_text or request_text

        task_prompt = args.get("task_prompt")
        # If task_prompt is missing, synthesize from request context.
        if task_prompt is None:
            task_prompt = self._build_runtime_prompt(request_text)

        targets = self._coerce_targets(args.get("targets"))
        for alias_key in ("target", "destination", "to"):
            if alias_key in args:
                targets = self._merge_target_aliases(targets, args.get(alias_key))
        for key in ("channel", "channel_id", "room", "room_id", "device_service", "chat_id"):
            value = args.get(key)
            if value and key not in targets:
                targets[key] = value

        # "current"/"here"/"this channel" should mean "use origin context", not a literal destination.
        for key in ("channel", "channel_id", "room", "room_id", "chat_id", "target", "destination", "to"):
            if key in targets and self._is_current_target_alias(targets.get(key)):
                targets.pop(key, None)

        cron_expr = args.get("cron")
        when_ts = args.get("when_ts")
        when_txt = args.get("when")

        meta = {
            "priority": args.get("priority"),
            "tags": args.get("tags"),
            "ttl_sec": args.get("ttl_sec"),
        }

        return (
            title,
            task_prompt,
            platform,
            targets,
            cron_expr,
            when_ts,
            when_txt,
            origin,
            meta,
        )

    @staticmethod
    def _next_local_time_occurrence(
        *,
        now_ts: float,
        hour: int,
        minute: int,
        second: int,
        weekdays: list[int] | None = None,
    ) -> float:
        now_local = datetime.fromtimestamp(float(now_ts)).astimezone()
        h = min(23, max(0, int(hour)))
        m = min(59, max(0, int(minute)))
        s = min(59, max(0, int(second)))

        candidate = now_local.replace(hour=h, minute=m, second=s, microsecond=0)
        if candidate.timestamp() <= now_ts:
            candidate = candidate + timedelta(days=1)

        day_filter = sorted(set(int(d) for d in (weekdays or []) if 0 <= int(d) <= 6))
        if not day_filter:
            return candidate.timestamp()

        while candidate.weekday() not in day_filter:
            candidate = candidate + timedelta(days=1)
        return candidate.timestamp()

    @staticmethod
    def _parse_weekday_field(value: str) -> tuple[list[int] | None, str]:
        raw = str(value or "").strip().lower()
        if raw in {"*", "?"}:
            return [], ""
        if any(ch in raw for ch in ("/", "-")):
            return None, "Cron day-of-week supports only single values or comma lists (no ranges/steps)."

        out: list[int] = []
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

    @staticmethod
    def _expand_numeric_field(
        value: Any,
        *,
        minimum: int,
        maximum: int,
        field_name: str,
    ) -> tuple[list[int] | None, str]:
        text = str(value or "").strip().lower()
        if not text or text in {"*", "?"}:
            return list(range(minimum, maximum + 1)), ""

        out: list[int] = []
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

    @classmethod
    def _next_cron_occurrence(
        cls,
        *,
        now_ts: float,
        hours: list[int],
        minutes: list[int],
        seconds: list[int],
        weekdays: list[int] | None = None,
    ) -> float:
        valid_hours = sorted(set(int(v) for v in (hours or []) if 0 <= int(v) <= 23))
        valid_minutes = sorted(set(int(v) for v in (minutes or []) if 0 <= int(v) <= 59))
        valid_seconds = sorted(set(int(v) for v in (seconds or []) if 0 <= int(v) <= 59))
        valid_weekdays = sorted(set(int(v) for v in (weekdays or []) if 0 <= int(v) <= 6))
        if not valid_hours or not valid_minutes or not valid_seconds:
            return 0.0

        base = datetime.fromtimestamp(float(now_ts)).astimezone().replace(microsecond=0) + timedelta(seconds=1)
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

    @staticmethod
    def _parse_time_of_day(text: Any) -> tuple[int, int, int] | None:
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
            hour = int(m24.group(1))
            minute = int(m24.group(2))
            second = int(m24.group(3) or 0)
            return hour, minute, second
        return None

    @staticmethod
    def _extract_weekdays_from_text(value: Any) -> list[int]:
        text = str(value or "").strip().lower()
        if not text:
            return []
        if "weekdays" in text:
            return [0, 1, 2, 3, 4]
        if "weekends" in text or "weekend" in text:
            return [5, 6]
        out: list[int] = []
        for token, mapped in _WEEKDAY_TOKEN_MAP.items():
            if token.isdigit():
                continue
            if re.search(rf"\b{re.escape(token)}\b", text):
                if mapped not in out:
                    out.append(mapped)
        return sorted(out)

    @classmethod
    def _build_cron_simple_schedule(
        cls,
        *,
        now_ts: float,
        hours: list[int],
        minutes: list[int],
        seconds: list[int],
        weekdays: list[int] | None = None,
        cron: str = "",
    ) -> Dict[str, Any] | None:
        next_run = cls._next_cron_occurrence(
            now_ts=now_ts,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            weekdays=weekdays or [],
        )
        if next_run <= 0:
            return None

        recurrence: Dict[str, Any] = {
            "kind": "cron_simple",
            "hours": sorted(set(int(v) for v in (hours or []) if 0 <= int(v) <= 23)),
            "minutes": sorted(set(int(v) for v in (minutes or []) if 0 <= int(v) <= 59)),
            "seconds": sorted(set(int(v) for v in (seconds or []) if 0 <= int(v) <= 59)),
        }
        weekday_list = sorted(set(int(v) for v in (weekdays or []) if 0 <= int(v) <= 6))
        if weekday_list:
            recurrence["weekdays"] = weekday_list

        interval_estimate = 0.0
        if not weekday_list and len(recurrence["hours"]) == 24 and len(recurrence["minutes"]) == 60 and len(recurrence["seconds"]) == 60:
            interval_estimate = 1.0
        elif not weekday_list and len(recurrence["hours"]) == 24 and len(recurrence["minutes"]) == 60 and recurrence["seconds"] == [0]:
            interval_estimate = 60.0
        elif not weekday_list and len(recurrence["hours"]) == 24 and recurrence["minutes"] == [0] and recurrence["seconds"] == [0]:
            interval_estimate = 3600.0
        elif not weekday_list and len(recurrence["hours"]) == 1 and len(recurrence["minutes"]) == 1 and len(recurrence["seconds"]) == 1:
            interval_estimate = 24 * 60 * 60.0

        out: Dict[str, Any] = {
            "next_run_ts": float(next_run),
            "interval_sec": float(interval_estimate),
            "recurrence": recurrence,
        }
        if cron:
            out["cron"] = str(cron).strip()
        return out

    def _parse_human_schedule(
        self,
        raw_value: Any,
        *,
        now_ts: float,
    ) -> tuple[Dict[str, Any] | None, str]:
        text = " ".join(str(raw_value or "").strip().lower().split())
        if not text:
            return None, ""

        if re.search(r"\b(every|each)\s+second\b|\bsecondly\b", text):
            schedule = self._build_cron_simple_schedule(
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
            elif step <= 59:
                seconds = list(range(0, 60, step))
            elif step % 60 == 0:
                minute_step = step // 60
                if minute_step > 59:
                    return None, "Every-seconds value is too large for cron-like conversion."
                schedule = self._build_cron_simple_schedule(
                    now_ts=now_ts,
                    hours=list(range(24)),
                    minutes=list(range(0, 60, minute_step)),
                    seconds=[0],
                )
                if schedule:
                    return schedule, ""
                return None, "Could not compute next run for every-seconds schedule."
            else:
                return None, "Every-seconds value must be 1-59 or a multiple of 60."
            schedule = self._build_cron_simple_schedule(
                now_ts=now_ts,
                hours=list(range(24)),
                minutes=list(range(60)),
                seconds=seconds,
            )
            if schedule:
                return schedule, ""
            return None, "Could not compute next run for every-seconds schedule."

        if re.search(r"\b(every|each)\s+minute\b|\bminutely\b", text):
            schedule = self._build_cron_simple_schedule(
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
            schedule = self._build_cron_simple_schedule(
                now_ts=now_ts,
                hours=list(range(24)),
                minutes=list(range(0, 60, step)),
                seconds=[0],
            )
            if schedule:
                return schedule, ""
            return None, "Could not compute next run for every-minutes schedule."

        if re.search(r"\b(every|each)\s+hour\b|\bhourly\b", text):
            schedule = self._build_cron_simple_schedule(
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
            schedule = self._build_cron_simple_schedule(
                now_ts=now_ts,
                hours=list(range(0, 24, step)),
                minutes=[0],
                seconds=[0],
            )
            if schedule:
                return schedule, ""
            return None, "Could not compute next run for every-hours schedule."

        daily = bool(re.search(r"\b(every day|everyday|daily|each day)\b", text))
        weekly = bool(re.search(r"\b(every week|weekly)\b", text))
        weekdays = self._extract_weekdays_from_text(text)
        if daily or weekly or weekdays:
            time_parts = self._parse_time_of_day(text)
            if not time_parts:
                return None, "Natural schedule is missing a time (for example: everyday at 6am)."
            hour, minute, second = time_parts
            cron_text = f"{int(second)} {int(minute)} {int(hour)} * * *"
            if weekdays or weekly:
                chosen_weekdays = weekdays or [datetime.fromtimestamp(float(now_ts)).astimezone().weekday()]
                day_tokens = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
                dow_tokens = [day_tokens[int(d)] for d in sorted(set(chosen_weekdays)) if 0 <= int(d) <= 6]
                if dow_tokens:
                    cron_text = f"{int(second)} {int(minute)} {int(hour)} * * {','.join(dow_tokens)}"
                schedule = self._build_cron_simple_schedule(
                    now_ts=now_ts,
                    hours=[hour],
                    minutes=[minute],
                    seconds=[second],
                    weekdays=chosen_weekdays,
                    cron=cron_text,
                )
                if schedule:
                    return schedule, ""
                return None, "Could not compute next run for weekly schedule."
            schedule = self._build_cron_simple_schedule(
                now_ts=now_ts,
                hours=[hour],
                minutes=[minute],
                seconds=[second],
                cron=cron_text,
            )
            if schedule:
                return schedule, ""
            return None, "Could not compute next run for daily schedule."

        return None, ""

    @staticmethod
    def _recurrence_label(recurrence: Dict[str, Any]) -> str:
        recurrence = recurrence if isinstance(recurrence, dict) else {}
        kind = str(recurrence.get("kind") or "").strip().lower()
        hour = int(recurrence.get("hour") or 0)
        minute = int(recurrence.get("minute") or 0)
        second = int(recurrence.get("second") or 0)
        time_part = f"{hour:02d}:{minute:02d}" + (f":{second:02d}" if second else "")

        if kind == "daily_local_time":
            return f"daily at {time_part}"
        if kind == "weekly_local_time":
            day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            days = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []
            day_labels: list[str] = []
            for day in days:
                try:
                    day_i = int(day)
                except Exception:
                    continue
                if 0 <= day_i <= 6:
                    name = day_names[day_i]
                    if name not in day_labels:
                        day_labels.append(name)
            if day_labels:
                return f"weekly ({', '.join(day_labels)}) at {time_part}"
            return f"weekly at {time_part}"
        if kind == "cron_simple":
            hours = recurrence.get("hours") if isinstance(recurrence.get("hours"), list) else []
            minutes = recurrence.get("minutes") if isinstance(recurrence.get("minutes"), list) else []
            seconds = recurrence.get("seconds") if isinstance(recurrence.get("seconds"), list) else []
            weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []
            if len(hours) == 24 and len(minutes) == 60 and len(seconds) == 60 and not weekdays:
                return "every second"
            if len(hours) == 24 and len(minutes) == 60 and seconds == [0] and not weekdays:
                return "every minute"
            if len(hours) == 24 and minutes == [0] and seconds == [0] and not weekdays:
                return "every hour"
            if len(hours) == 1 and len(minutes) == 1 and len(seconds) == 1:
                time_part = f"{int(hours[0]):02d}:{int(minutes[0]):02d}" + (
                    f":{int(seconds[0]):02d}" if int(seconds[0]) else ""
                )
                if weekdays:
                    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                    day_labels: list[str] = []
                    for day in weekdays:
                        try:
                            day_i = int(day)
                        except Exception:
                            continue
                        if 0 <= day_i <= 6:
                            name = day_names[day_i]
                            if name not in day_labels:
                                day_labels.append(name)
                    if day_labels:
                        return f"weekly ({', '.join(day_labels)}) at {time_part}"
                return f"daily at {time_part}"
            cron_text = str(recurrence.get("cron") or "").strip()
            if cron_text:
                return f"cron ({cron_text})"
            return "cron schedule"
        return "recurring"

    def _parse_cron_schedule(
        self,
        raw_value: Any,
        *,
        now_ts: float,
    ) -> tuple[Dict[str, Any] | None, str]:
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

        weekdays, weekday_err = self._parse_weekday_field(dow_raw)
        if weekday_err:
            return None, weekday_err
        if weekdays is None:
            return None, "Invalid cron day-of-week field."

        seconds, sec_err = self._expand_numeric_field(sec_raw, minimum=0, maximum=59, field_name="second")
        if sec_err:
            return None, sec_err
        minutes, min_err = self._expand_numeric_field(minute_raw, minimum=0, maximum=59, field_name="minute")
        if min_err:
            return None, min_err
        hours, hour_err = self._expand_numeric_field(hour_raw, minimum=0, maximum=23, field_name="hour")
        if hour_err:
            return None, hour_err

        cron_text = " ".join(parts)
        schedule = self._build_cron_simple_schedule(
            now_ts=now_ts,
            hours=hours or [],
            minutes=minutes or [],
            seconds=seconds or [],
            weekdays=weekdays or [],
            cron=cron_text,
        )
        if not isinstance(schedule, dict):
            return None, "Could not compute next run for cron schedule."
        recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
        if isinstance(recurrence, dict) and cron_text:
            recurrence["cron"] = cron_text
            schedule["recurrence"] = recurrence
        return schedule, ""

    async def _schedule(self, args: Dict[str, Any]):
        (
            title,
            task_prompt,
            platform,
            targets,
            cron_expr,
            when_ts,
            when_txt,
            origin,
            meta,
        ) = self._extract_args(args)

        task_prompt = (task_prompt or "").strip()

        if not task_prompt:
            return {"tool": "ai_tasks", "ok": False, "error": "Cannot queue: missing task prompt"}

        dest = normalize_platform(platform)
        if not dest:
            if isinstance(origin, dict):
                origin_platform = normalize_platform(origin.get("platform"))
                if origin_platform in ALLOWED_PLATFORMS:
                    dest = origin_platform

        if dest not in ALLOWED_PLATFORMS:
            return {"tool": "ai_tasks", "ok": False, "error": "Cannot queue: missing destination platform"}

        def _is_set(value: Any) -> bool:
            if value is None:
                return False
            if isinstance(value, str):
                text = value.strip().lower()
                return text not in {"", "0", "0.0", "false", "none", "null"}
            if isinstance(value, (int, float)):
                return float(value) != 0.0
            return True

        one_shot_time_fields = (
            "in_seconds",
            "in_minutes",
            "in_hours",
            "every_seconds",
            "every_minutes",
            "every_hours",
        )
        for key in one_shot_time_fields:
            if _is_set((args or {}).get(key)):
                return {
                    "tool": "ai_tasks",
                    "ok": False,
                    "error": (
                        f"`{key}` is no longer supported. "
                        "Use `when`/`cron` (for example: `everyday at 6am`, `every hour`, "
                        "`every second`, or `0 0 6 * * *`)."
                    ),
                }

        now = time.time()

        schedule_result: Dict[str, Any] | None = None
        cron_source = ""
        parse_error = ""

        schedule_candidates: list[Any] = []
        seen_candidates: set[str] = set()

        def _add_candidate(raw_value: Any) -> None:
            if raw_value in (None, ""):
                return
            text = " ".join(str(raw_value).strip().split())
            if not text or text in seen_candidates:
                return
            seen_candidates.add(text)
            schedule_candidates.append(raw_value)

        _add_candidate(cron_expr)
        _add_candidate(when_ts)
        _add_candidate(when_txt)
        _add_candidate((args or {}).get("request"))
        if isinstance(origin, dict):
            _add_candidate(origin.get("request_text"))

        for candidate in schedule_candidates:
            parsed, err = self._parse_cron_schedule(candidate, now_ts=now)
            if isinstance(parsed, dict):
                schedule_result = parsed
                cron_source = str(parsed.get("cron") or "").strip() or " ".join(str(candidate).strip().split())
                break
            if err:
                parse_error = err

        if not isinstance(schedule_result, dict):
            for candidate in schedule_candidates:
                parsed, err = self._parse_human_schedule(candidate, now_ts=now)
                if isinstance(parsed, dict):
                    schedule_result = parsed
                    cron_source = str(parsed.get("cron") or "").strip()
                    break
                if err:
                    parse_error = err

        if not isinstance(schedule_result, dict):
            guidance = (
                "Use `when`/`cron` in local time, for example `everyday at 6am`, "
                "`every hour`, `every second`, or `0 0 6 * * *`."
            )
            if parse_error:
                return {"tool": "ai_tasks", "ok": False, "error": f"Cannot schedule: {parse_error} {guidance}"}
            return {
                "tool": "ai_tasks",
                "ok": False,
                "error": f"Cannot schedule: missing schedule details. {guidance}",
            }

        next_run = float(schedule_result.get("next_run_ts") or 0.0)
        recurrence = schedule_result.get("recurrence") if isinstance(schedule_result.get("recurrence"), dict) else {}
        interval = float(schedule_result.get("interval_sec") or 0.0)
        if next_run <= 0 or not recurrence:
            return {"tool": "ai_tasks", "ok": False, "error": "Cannot schedule: invalid recurrence."}

        recurrence_cron = str(recurrence.get("cron") or "").strip()
        if recurrence_cron and not cron_source:
            cron_source = recurrence_cron

        if cron_source and isinstance(recurrence, dict):
            recurrence["cron"] = cron_source

        normalized_targets = self._normalize_channel_targets(dest, targets)
        defaults = load_default_targets(dest, redis_client)
        fallback_defaults = self._load_platform_fallback_targets(dest)
        if isinstance(fallback_defaults, dict) and fallback_defaults:
            for key, value in fallback_defaults.items():
                if key not in defaults and value not in (None, ""):
                    defaults[key] = value
        resolved_targets, err = resolve_targets(dest, normalized_targets, origin, defaults)
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
        if cron_source:
            schedule_payload["cron"] = cron_source

        reminder = {
            "id": reminder_id,
            "created_at": float(now),
            "platform": dest,
            "title": title,
            "task_prompt": task_prompt,  # what to do at runtime
            "targets": resolved_targets or {},
            "origin": normalize_origin(origin),
            "meta": meta or {},
            "schedule": schedule_payload,
        }

        redis_client.set(f"{REMINDER_KEY_PREFIX}{reminder_id}", json.dumps(reminder))
        redis_client.zadd(REMINDER_DUE_ZSET, {reminder_id: float(next_run)})

        human = datetime.fromtimestamp(next_run).strftime("%Y-%m-%d %H:%M:%S")
        cadence = self._recurrence_label(recurrence)
        result_text = f"Recurring AI task scheduled {cadence} (next at {human})."

        return {
            "tool": "ai_tasks",
            "ok": True,
            "result": result_text,
            "reminder_id": reminder_id,
            "next_run_ts": float(next_run),
            "platform": dest,
            "title": title,
            "targets": resolved_targets or {},
            "schedule": reminder.get("schedule") or {},
        }

    async def handle_discord(self, message, args, llm_client):
        return await self._schedule(args)

    async def handle_webui(self, args, llm_client):
        return await self._schedule(args)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._schedule(args)

    async def handle_homeassistant(self, args, llm_client):
        return await self._schedule(args)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self._schedule(args)

    async def handle_telegram(self, update, args, llm_client):
        return await self._schedule(args)


plugin = AITasksPlugin()
