import json
import logging
import time
import uuid
import re
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
    required_args = ["message"]
    optional_args = [
        "task_prompt",
        "title",
        "platform",
        "targets",
        "target",
        "destination",
        "to",
        "when_ts",
        "when",
        "in_seconds",
        "in_minutes",
        "in_hours",
        "every_seconds",
        "every_minutes",
        "every_hours",
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
    version = "1.0.5"
    usage = '{"function":"ai_tasks","arguments":{"message":"Required task prompt","task_prompt":"Optional explicit scheduled task prompt","title":"Optional short title","platform":"discord|irc|matrix|homeassistant|ntfy|telegram (optional; defaults to origin)","targets":{"channel":"discord/irc channel","room_id":"matrix room id/alias","chat_id":"telegram chat id","device_service":"home assistant notify service"},"channel":"optional shortcut for discord/irc channel","room":"optional shortcut for matrix room (alias for room_id)","chat_id":"optional shortcut for telegram target","when_ts":1730000000.0,"when":"2026-02-03 15:04:05 or 10am (local time)","in_seconds":3600,"every_seconds":0,"priority":"normal|high","tags":["optional","strings"],"ttl_sec":0}}'
    description = (
        "Schedule an AI task. Supports one-shot or recurring runs via every_seconds. "
        "At run time, AI can answer directly or call one tool before sending the final response."
    )
    pretty_name = "AI Tasks"
    when_to_use = "Schedule one-off or recurring tasks (daily/weekly/every N seconds) that run later."
    common_needs = [
        "task to run",
        "when to run (time or interval)",
        "destination (optional; defaults to current channel/room)",
    ]
    missing_info_prompts = [
        "When should this run? You can use `when` (like `6:10am`), `in_seconds`, `every_seconds`, or cron (`0 10 6 * *` / `0 10 6 * * *`). If destination isn't this same chat/room, include `targets` (or `channel`/`room`/`chat_id`).",
    ]

    platforms = ["discord", "irc", "matrix", "homeassistant", "telegram", "webui"]
    waiting_prompt_template = (
        "Write a short, friendly message telling {mention} you’re scheduling the task now. "
        "Only output that message."
    )

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
        message = args.get("message") or args.get("content")

        task_prompt = args.get("task_prompt")
        # NEW: if not provided, generate a runtime prompt from what the user said
        if task_prompt is None:
            task_prompt = self._build_runtime_prompt(message)

        platform = args.get("platform")

        targets = self._coerce_targets(args.get("targets"))
        for alias_key in ("target", "destination", "to"):
            if alias_key in args:
                targets = self._merge_target_aliases(targets, args.get(alias_key))
        for key in ("channel", "channel_id", "room", "room_id", "device_service", "chat_id"):
            if args.get(key) and key not in targets:
                targets[key] = args.get(key)

        when_ts = args.get("when_ts")
        when_txt = args.get("when")
        in_seconds = args.get("in_seconds")
        every_seconds = args.get("every_seconds")

        if in_seconds is None and args.get("in_minutes") is not None:
            in_seconds = float(args.get("in_minutes")) * 60
        if in_seconds is None and args.get("in_hours") is not None:
            in_seconds = float(args.get("in_hours")) * 3600

        if every_seconds is None and args.get("every_minutes") is not None:
            every_seconds = float(args.get("every_minutes")) * 60
        if every_seconds is None and args.get("every_hours") is not None:
            every_seconds = float(args.get("every_hours")) * 3600

        meta = {
            "priority": args.get("priority"),
            "tags": args.get("tags"),
            "ttl_sec": args.get("ttl_sec"),
        }
        origin = args.get("origin")
        return (
            title,
            message,
            task_prompt,
            platform,
            targets,
            when_ts,
            when_txt,
            in_seconds,
            every_seconds,
            origin,
            meta,
        )

    def _parse_when(self, when_ts, when_txt, in_seconds) -> float | None:
        now = time.time()

        if when_ts is not None:
            try:
                return float(when_ts)
            except Exception:
                pass

        if in_seconds is not None:
            try:
                return now + float(in_seconds)
            except Exception:
                pass

        if isinstance(when_txt, str) and when_txt.strip():
            text = when_txt.strip()
            text_lower = text.lower()
            if text.isdigit():
                try:
                    return float(text)
                except Exception:
                    return None

            try:
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                dt = datetime.fromisoformat(text)
            except Exception:
                dt = None

            if dt is None:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                    try:
                        dt = datetime.strptime(text, fmt)
                        break
                    except Exception:
                        dt = None

            if dt is None:
                m = re.match(r"^(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*$", text_lower)
                if m:
                    hour = int(m.group(1))
                    minute = int(m.group(2) or 0)
                    mer = m.group(3)
                    if hour < 1 or hour > 12 or minute > 59:
                        return None
                    if mer == "am":
                        hour = 0 if hour == 12 else hour
                    else:
                        hour = 12 if hour == 12 else hour + 12
                    now_dt = datetime.now().astimezone()
                    dt = now_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    if dt.timestamp() <= now_dt.timestamp():
                        dt = dt + timedelta(days=1)
                else:
                    m24 = re.match(r"^(?:at\s+)?(\d{1,2})(?::(\d{2}))\s*$", text_lower)
                    if m24:
                        hour = int(m24.group(1))
                        minute = int(m24.group(2))
                        if hour < 0 or hour > 23 or minute > 59:
                            return None
                        now_dt = datetime.now().astimezone()
                        dt = now_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
                        if dt.timestamp() <= now_dt.timestamp():
                            dt = dt + timedelta(days=1)

            if dt is None:
                return None

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)

            return dt.timestamp()

        return None

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
            # Compatibility shorthand: "0 10 6 * *" -> "0 10 6 * * *"
            # Some models drop the trailing day-of-week wildcard from 6-field cron.
            if (
                parts[3] in {"*", "?"}
                and parts[4] in {"*", "?"}
                and all(re.fullmatch(r"\d{1,2}", p) for p in parts[:3])
            ):
                sec_raw, minute_raw, hour_raw = parts[:3]
                day_raw = "*"
                month_raw = "*"
                dow_raw = "*"
            else:
                sec_raw = "0"
                minute_raw, hour_raw, day_raw, month_raw, dow_raw = parts
        else:
            sec_raw, minute_raw, hour_raw, day_raw, month_raw, dow_raw = parts

        for field_name, field_value in (
            ("second", sec_raw),
            ("minute", minute_raw),
            ("hour", hour_raw),
        ):
            if field_value in {"*", "?"}:
                return None, f"Cron {field_name} cannot be wildcard for ai_tasks."
            if any(ch in field_value for ch in ("/", "-", ",")):
                return None, f"Cron {field_name} supports only one numeric value for ai_tasks."
            if not re.fullmatch(r"\d{1,2}", field_value):
                return None, f"Invalid cron {field_name}: {field_value}"

        second = int(sec_raw)
        minute = int(minute_raw)
        hour = int(hour_raw)
        if second < 0 or second > 59 or minute < 0 or minute > 59 or hour < 0 or hour > 23:
            return None, "Cron time fields are out of range."

        if day_raw not in {"*", "?"} or month_raw not in {"*", "?"}:
            return None, "Cron day-of-month and month must be '*' or '?' for ai_tasks."

        weekdays, weekday_err = self._parse_weekday_field(dow_raw)
        if weekday_err:
            return None, weekday_err
        if weekdays is None:
            return None, "Invalid cron day-of-week field."

        next_run_ts = self._next_local_time_occurrence(
            now_ts=now_ts,
            hour=hour,
            minute=minute,
            second=second,
            weekdays=weekdays,
        )
        if weekdays:
            recurrence = {
                "kind": "weekly_local_time",
                "hour": int(hour),
                "minute": int(minute),
                "second": int(second),
                "weekdays": weekdays,
            }
            interval_sec = float(7 * 24 * 60 * 60)
        else:
            recurrence = {
                "kind": "daily_local_time",
                "hour": int(hour),
                "minute": int(minute),
                "second": int(second),
            }
            interval_sec = float(24 * 60 * 60)

        return {
            "next_run_ts": float(next_run_ts),
            "interval_sec": float(interval_sec),
            "recurrence": recurrence,
        }, ""

    async def _schedule(self, args: Dict[str, Any]):
        (
            title,
            message,
            task_prompt,
            platform,
            targets,
            when_ts,
            when_txt,
            in_seconds,
            every_seconds,
            origin,
            meta,
        ) = self._extract_args(args)

        message = (message or "").strip()
        task_prompt = (task_prompt or "").strip()

        if not task_prompt:
            task_prompt = self._build_runtime_prompt(message)

        if not task_prompt:
            return {"tool": "ai_tasks", "ok": False, "error": "Cannot queue: missing task prompt"}

        if not message:
            message = task_prompt

        dest = normalize_platform(platform)
        if not dest:
            if isinstance(origin, dict):
                origin_platform = normalize_platform(origin.get("platform"))
                if origin_platform in ALLOWED_PLATFORMS:
                    dest = origin_platform

        if dest not in ALLOWED_PLATFORMS:
            return {"tool": "ai_tasks", "ok": False, "error": "Cannot queue: missing destination platform"}

        now = time.time()

        try:
            interval = float(every_seconds) if every_seconds is not None else 0.0
        except Exception:
            interval = 0.0
        if interval < 0:
            interval = 0.0

        cron_schedule: Dict[str, Any] | None = None
        cron_error = ""
        if interval <= 0:
            for candidate in (when_ts, when_txt):
                parsed, err = self._parse_cron_schedule(candidate, now_ts=now)
                if isinstance(parsed, dict):
                    cron_schedule = parsed
                    break
                if err:
                    cron_error = err

        next_run = self._parse_when(when_ts, when_txt, in_seconds)
        if next_run is None and isinstance(cron_schedule, dict):
            next_run = float(cron_schedule.get("next_run_ts") or 0.0)
            interval = max(float(interval), float(cron_schedule.get("interval_sec") or 0.0))

        if next_run is None and interval > 0:
            next_run = now + max(1.0, interval)
        if next_run is None:
            if cron_error:
                return {"tool": "ai_tasks", "ok": False, "error": f"Cannot schedule: {cron_error}"}
            return {"tool": "ai_tasks", "ok": False, "error": "Cannot schedule: missing or invalid time"}

        if next_run < now:
            next_run = now

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
        reminder = {
            "id": reminder_id,
            "created_at": float(now),
            "platform": dest,
            "title": title,
            "message": message,  # what the user said
            "task_prompt": task_prompt,  # what to do at runtime
            "targets": resolved_targets or {},
            "origin": normalize_origin(origin),
            "meta": meta or {},
            "schedule": {
                "next_run_ts": float(next_run),
                "interval_sec": float(interval),
                "anchor_ts": float(next_run) if interval > 0 else 0.0,
            },
        }
        if isinstance(cron_schedule, dict) and isinstance(cron_schedule.get("recurrence"), dict):
            reminder["schedule"]["recurrence"] = dict(cron_schedule.get("recurrence") or {})

        redis_client.set(f"{REMINDER_KEY_PREFIX}{reminder_id}", json.dumps(reminder))
        redis_client.zadd(REMINDER_DUE_ZSET, {reminder_id: float(next_run)})

        human = datetime.fromtimestamp(next_run).strftime("%Y-%m-%d %H:%M:%S")
        if interval > 0:
            result_text = f"Recurring AI task scheduled every {int(interval)}s (next at {human})."
        else:
            result_text = f"AI task scheduled for {human}."

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
