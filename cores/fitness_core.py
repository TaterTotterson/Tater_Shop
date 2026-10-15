"""Privacy-first health history, wellness summaries, and Little Spud challenges."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from helpers import extract_json, get_llm_client_from_env, redis_client

try:
    from helpers import get_primary_llm_client_from_env as _get_primary_llm_client_from_env
except Exception:  # pragma: no cover - older compatible Tater builds.
    _get_primary_llm_client_from_env = get_llm_client_from_env

__version__ = "1.2.1"
MIN_TATER_VERSION = "200"
CORE_DESCRIPTION = (
    "Store health aggregates sent by Little Spud, summarize private wellness trends, and run opt-in challenges."
)
TAGS = ["fitness", "health", "wellness", "challenges", "little-spud"]

logger = logging.getLogger("fitness_core")

SETTINGS_KEY = "fitness_core_settings"
STATUS_KEY = "fitness_core:status"
PERSON_PREFIX = "fitness_core:person:"
DAILY_PREFIX = "fitness_core:daily:"
SOURCE_INDEX_PREFIX = "fitness_core:sources:"
SOURCE_DAILY_PREFIX = "fitness_core:source_daily:"
CHALLENGE_RECORDS_KEY = "fitness_core:challenge_records"
AI_SUMMARY_PREFIX = "fitness_core:ai_summary:"
CHALLENGE_NOTIFICATION_PREFIX = "fitness_core:challenge_notification:"
SPUD_LINK_NODES_KEY = "tater:spudlink:nodes:v1"

HEALTH_METRICS: Dict[str, Dict[str, Any]] = {
    "steps": {"title": "Steps", "unit": "steps", "group": "activity", "decimals": 0},
    "distance_m": {"title": "Distance", "unit": "m", "group": "activity", "decimals": 0},
    "active_energy_kcal": {"title": "Active energy", "unit": "kcal", "group": "activity", "decimals": 0},
    "exercise_minutes": {"title": "Exercise", "unit": "min", "group": "activity", "decimals": 0},
    "workouts": {"title": "Workouts", "unit": "workouts", "group": "activity", "decimals": 0},
    "sleep_minutes": {"title": "Sleep", "unit": "min", "group": "sleep", "decimals": 0},
    "sleep_core_minutes": {"title": "Core sleep", "unit": "min", "group": "sleep", "decimals": 0},
    "sleep_deep_minutes": {"title": "Deep sleep", "unit": "min", "group": "sleep", "decimals": 0},
    "sleep_rem_minutes": {"title": "REM sleep", "unit": "min", "group": "sleep", "decimals": 0},
    "sleep_awake_minutes": {"title": "Awake time", "unit": "min", "group": "sleep", "decimals": 0},
    "heart_rate_avg_bpm": {"title": "Heart rate", "unit": "bpm", "group": "heart", "decimals": 0},
    "heart_rate_min_bpm": {"title": "Minimum heart rate", "unit": "bpm", "group": "heart", "decimals": 0},
    "heart_rate_max_bpm": {"title": "Maximum heart rate", "unit": "bpm", "group": "heart", "decimals": 0},
    "resting_heart_rate_bpm": {"title": "Resting heart rate", "unit": "bpm", "group": "heart", "decimals": 0},
    "heart_rate_variability_ms": {"title": "Heart-rate variability", "unit": "ms", "group": "heart", "decimals": 0},
    "blood_pressure_systolic_mmhg": {"title": "Systolic pressure", "unit": "mmHg", "group": "vitals", "decimals": 0},
    "blood_pressure_diastolic_mmhg": {"title": "Diastolic pressure", "unit": "mmHg", "group": "vitals", "decimals": 0},
    "oxygen_saturation_percent": {"title": "Blood oxygen", "unit": "%", "group": "vitals", "decimals": 1},
    "respiratory_rate_per_min": {"title": "Respiratory rate", "unit": "/min", "group": "vitals", "decimals": 1},
    "weight_kg": {"title": "Weight", "unit": "kg", "group": "body", "decimals": 1},
    "body_fat_percent": {"title": "Body fat", "unit": "%", "group": "body", "decimals": 1},
}

INSIGHT_DEFINITIONS: List[Dict[str, Any]] = [
    {"id": "steps", "title": "Steps", "keys": ["steps"]},
    {"id": "activity", "title": "Activity", "keys": ["exercise_minutes", "active_energy_kcal", "workouts"]},
    {"id": "sleep", "title": "Sleep", "keys": ["sleep_minutes", "sleep_core_minutes", "sleep_deep_minutes", "sleep_rem_minutes"]},
    {"id": "heart_rate", "title": "Heart rate", "keys": ["heart_rate_avg_bpm", "heart_rate_min_bpm", "heart_rate_max_bpm"]},
    {"id": "resting_heart_rate", "title": "Resting heart rate", "keys": ["resting_heart_rate_bpm"]},
    {"id": "heart_rate_variability", "title": "Heart-rate variability", "keys": ["heart_rate_variability_ms"]},
    {"id": "blood_pressure", "title": "Blood pressure", "keys": ["blood_pressure_systolic_mmhg", "blood_pressure_diastolic_mmhg"]},
    {"id": "blood_oxygen", "title": "Blood oxygen", "keys": ["oxygen_saturation_percent"]},
    {"id": "respiratory_rate", "title": "Respiratory rate", "keys": ["respiratory_rate_per_min"]},
    {"id": "weight", "title": "Weight", "keys": ["weight_kg"]},
    {"id": "body_fat", "title": "Body fat", "keys": ["body_fat_percent"]},
]

CHALLENGE_TYPES: List[Dict[str, Any]] = [
    {"id": "steps", "title": "Steps", "unit": "steps", "decimals": 0, "score_kind": "highest_total"},
    {"id": "distance_m", "title": "Distance", "unit": "m", "decimals": 0, "score_kind": "highest_total"},
    {"id": "exercise_minutes", "title": "Active minutes", "unit": "min", "decimals": 0, "score_kind": "highest_total"},
    {"id": "workouts", "title": "Workouts", "unit": "workouts", "decimals": 0, "score_kind": "highest_total"},
    {"id": "active_energy_kcal", "title": "Active energy", "unit": "kcal", "decimals": 0, "score_kind": "highest_total"},
    {
        "id": "weight_checkins",
        "title": "Weight check-ins",
        "unit": "days",
        "decimals": 0,
        "score_kind": "checkin_consistency",
        "description": "Compare consistent private weigh-ins without sharing weight values.",
    },
]

CORE_SETTINGS = {
    "category": "Fitness Core Settings",
    "hydra_tools_require_running": False,
    "required": {
        "challenge_sync_interval_seconds": {
            "label": "Challenge refresh (seconds)",
            "type": "number",
            "default": 30,
            "description": "How often active challenge standings refresh.",
        },
    },
    "tags": TAGS,
}

CORE_WEBUI_TAB = {"label": "Fitness", "order": 38, "requires_running": False}


def _text(value: Any, limit: int = 240) -> str:
    return " ".join(str(value or "").strip().split())[:limit]


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _number_or_none(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool) or str(value).strip() == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _boolean(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _json_load(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    try:
        value = json.loads(raw.decode() if isinstance(raw, bytes) else str(raw))
        return value
    except Exception:
        return default


def _json_save(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _client(client: Any = None) -> Any:
    return client or redis_client


def _settings(client: Any = None) -> Dict[str, Any]:
    return dict(_client(client).hgetall(SETTINGS_KEY) or {})


def _person_key(person_id: str) -> str:
    return PERSON_PREFIX + _text(person_id, 160)


def _daily_key(person_id: str) -> str:
    return DAILY_PREFIX + _text(person_id, 160)


def _source_index_key(person_id: str) -> str:
    return SOURCE_INDEX_PREFIX + _text(person_id, 160)


def _source_id(node_id: str, platform: str, node_name: str = "") -> str:
    stable_node_id = _text(node_id, 160)
    if stable_node_id:
        return stable_node_id
    platform_name = _text(platform, 40) or "unknown"
    fallback_name = _text(node_name, 100)
    return f"{platform_name}:{fallback_name}" if fallback_name else platform_name


def _source_daily_key(person_id: str, source_id: str) -> str:
    digest = hashlib.sha256(_text(source_id, 160).encode("utf-8")).hexdigest()[:16]
    return f"{SOURCE_DAILY_PREFIX}{_text(person_id, 160)}:{digest}"


def _load_sources(client: Any, person_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw_source_id, raw in (client.hgetall(_source_index_key(person_id)) or {}).items():
        source_id = raw_source_id.decode() if isinstance(raw_source_id, bytes) else str(raw_source_id)
        row = _json_load(raw, {})
        if not isinstance(row, dict):
            row = {}
        row["id"] = _text(row.get("id") or source_id, 160)
        rows.append(row)
    rows.sort(key=lambda row: _number(row.get("last_sync_at")), reverse=True)
    return rows


def _source_day(client: Any, person_id: str, source_id: str, day_value: str) -> Dict[str, Any]:
    source_rows = client.hgetall(_source_daily_key(person_id, source_id)) or {}
    raw = source_rows.get(day_value)
    if raw is None:
        raw = source_rows.get(day_value.encode())
    row = _json_load(raw, {})
    if not isinstance(row, dict):
        return {}
    row["date"] = day_value
    return row


PREFERRED_SOURCE_METRICS = {
    "steps",
    "distance_m",
    "active_energy_kcal",
    "exercise_minutes",
    "workouts",
    "sleep_minutes",
    "sleep_core_minutes",
    "sleep_deep_minutes",
    "sleep_rem_minutes",
    "sleep_awake_minutes",
    "heart_rate_avg_bpm",
    "heart_rate_min_bpm",
    "heart_rate_max_bpm",
    "resting_heart_rate_bpm",
    "heart_rate_variability_ms",
}


def _merge_source_day(
    client: Any, person_id: str, day_value: str, preferred_source_id: str
) -> Dict[str, Any]:
    candidates: List[tuple[Dict[str, Any], Dict[str, Any]]] = []
    for source in _load_sources(client, person_id):
        row = _source_day(client, person_id, _text(source.get("id"), 160), day_value)
        if row:
            candidates.append((source, row))
    if not candidates:
        return {}

    newest = sorted(
        candidates,
        key=lambda item: _number(item[1].get("_synced_at") or item[0].get("last_sync_at")),
        reverse=True,
    )
    preferred = [item for item in candidates if _text(item[0].get("id"), 160) == preferred_source_id]
    preferred_first = preferred + [item for item in newest if item not in preferred]
    merged: Dict[str, Any] = {"date": day_value}
    blood_pressure_metrics = {
        "blood_pressure_systolic_mmhg",
        "blood_pressure_diastolic_mmhg",
    }
    for metric in HEALTH_METRICS:
        if metric in blood_pressure_metrics:
            continue
        ordered = preferred_first if metric in PREFERRED_SOURCE_METRICS else newest
        for _source, row in ordered:
            value = _number_or_none(row.get(metric))
            if value is not None:
                merged[metric] = value
                break
    for _source, row in newest:
        if not any(_number_or_none(row.get(metric)) is not None for metric in blood_pressure_metrics):
            continue
        for metric in blood_pressure_metrics:
            value = _number_or_none(row.get(metric))
            if value is not None:
                merged[metric] = value
        break
    return merged


def _materialize_merged_days(
    client: Any, person_id: str, day_values: List[str], preferred_source_id: str
) -> None:
    for day_value in dict.fromkeys(day_values):
        merged = _merge_source_day(client, person_id, day_value, preferred_source_id)
        if not merged:
            continue
        client.hset(
            _daily_key(person_id),
            day_value,
            _json_save({key: value for key, value in merged.items() if key != "date"}),
        )


def _load_person(client: Any, person_id: str, person_name: str = "") -> Dict[str, Any]:
    row = _json_load(client.get(_person_key(person_id)), {})
    if not isinstance(row, dict):
        row = {}
    row["id"] = _text(person_id, 160)
    if _text(person_name, 80):
        row["name"] = _text(person_name, 80)
    row.setdefault("name", "Tater person")
    return row


def _save_person(client: Any, person_id: str, row: Dict[str, Any]) -> None:
    client.set(_person_key(person_id), _json_save({key: value for key, value in row.items() if key != "id"}))


def _local_challenges(client: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for challenge_id, raw in (client.hgetall(CHALLENGE_RECORDS_KEY) or {}).items():
        row = _json_load(raw, {})
        if isinstance(row, dict):
            row["id"] = _text(challenge_id, 120)
            rows.append(row)
    return rows


def _save_local_challenge(client: Any, row: Dict[str, Any]) -> None:
    challenge_id = _text(row.get("id"), 120)
    client.hset(
        CHALLENGE_RECORDS_KEY,
        challenge_id,
        _json_save({key: value for key, value in row.items() if key != "id"}),
    )


def _challenge_definition(row: Dict[str, Any]) -> Dict[str, Any]:
    metric = _text(row.get("metric"), 40) or "steps"
    return next((item for item in CHALLENGE_TYPES if item["id"] == metric), CHALLENGE_TYPES[0])


def _challenge_duration_label(row: Dict[str, Any]) -> str:
    return {
        "3_hours": "3-hour",
        "day": "one-day",
        "week": "one-week",
    }.get(_text(row.get("duration"), 20), "one-day")


def _little_spud_node_ids_for_person(client: Any, person_id: str) -> List[str]:
    wanted = _text(person_id, 160)
    if not wanted:
        return []
    nodes: List[str] = []
    for raw_node_id, raw in (client.hgetall(SPUD_LINK_NODES_KEY) or {}).items():
        node = _json_load(raw, {})
        if not isinstance(node, dict) or _text(node.get("role"), 40).lower() != "little_spud":
            continue
        if _text(node.get("linked_person_id"), 160) != wanted:
            continue
        node_id = _text(node.get("id") or raw_node_id, 160)
        if node_id and node_id not in nodes:
            nodes.append(node_id)
    return nodes


def _claim_challenge_notification(client: Any, challenge_id: str, event: str, person_id: str) -> bool:
    key = CHALLENGE_NOTIFICATION_PREFIX + ":".join(
        (_text(challenge_id, 120), _text(event, 32), _text(person_id, 160))
    )
    try:
        return bool(client.set(key, "1", nx=True, ex=60 * 60 * 24 * 45))
    except TypeError:  # Lightweight compatible stores used by tests and older Tater builds.
        if client.get(key):
            return False
        client.set(key, "1")
        return True


def _dispatch_person_notification(
    client: Any,
    *,
    person_id: str,
    challenge_id: str,
    event: str,
    title: str,
    content: str,
    priority: str = "normal",
) -> None:
    node_ids = _little_spud_node_ids_for_person(client, person_id)
    if not node_ids:
        return
    try:
        from notify.core import dispatch_notification_sync
    except Exception as exc:
        logger.warning("[fitness_core] Little Spud notifications are unavailable: %s", exc)
        return
    if not _claim_challenge_notification(client, challenge_id, event, person_id):
        return
    for node_id in node_ids:
        try:
            result = dispatch_notification_sync(
                "little_spud",
                title,
                content,
                targets={"node_id": node_id},
                origin={
                    "platform": "fitness_core",
                    "source": "fitness_core",
                    "challenge_id": challenge_id,
                },
                meta={
                    "priority": priority,
                    "tags": ["fitness", "challenge", event],
                    "ttl_sec": 60 * 60 * 24 * 7,
                },
            )
            if not str(result or "").lower().startswith("queued notification"):
                logger.warning("[fitness_core] challenge notification was not queued: %s", result)
        except Exception as exc:
            logger.warning("[fitness_core] challenge notification failed for %s: %s", node_id, exc)


def _notify_challenge_event(client: Any, row: Dict[str, Any], event: str, actor_id: str = "") -> None:
    challenge_id = _text(row.get("id"), 120)
    challenger_id = _text(row.get("challenger_id"), 160)
    opponent_id = _text(row.get("opponent_id"), 160)
    challenger_name = _text(row.get("challenger_name"), 80) or "Someone"
    opponent_name = _text(row.get("opponent_name"), 80) or "Someone"
    metric_title = _text(_challenge_definition(row).get("title"), 80) or "Fitness"
    duration = _challenge_duration_label(row)
    if not challenge_id:
        return

    if event == "invited":
        _dispatch_person_notification(
            client,
            person_id=opponent_id,
            challenge_id=challenge_id,
            event=event,
            title="New fitness challenge",
            content=f"{challenger_name} challenged you to a {duration} {metric_title} challenge. Open Fitness to accept or decline.",
            priority="high",
        )
    elif event == "accepted":
        _dispatch_person_notification(
            client,
            person_id=challenger_id,
            challenge_id=challenge_id,
            event=event,
            title="Challenge accepted",
            content=f"{opponent_name} accepted your {metric_title} challenge. Your {duration} challenge has started.",
        )
    elif event == "declined":
        _dispatch_person_notification(
            client,
            person_id=challenger_id,
            challenge_id=challenge_id,
            event=event,
            title="Challenge declined",
            content=f"{opponent_name} declined your {metric_title} challenge.",
        )
    elif event == "cancelled":
        target_id = opponent_id if actor_id == challenger_id else challenger_id
        actor_name = challenger_name if actor_id == challenger_id else opponent_name
        _dispatch_person_notification(
            client,
            person_id=target_id,
            challenge_id=challenge_id,
            event=event,
            title="Challenge cancelled",
            content=f"{actor_name} cancelled your {metric_title} challenge.",
        )
    elif event == "completed":
        winner_id = _text(row.get("winner_id"), 160)
        for target_id, other_name in ((challenger_id, opponent_name), (opponent_id, challenger_name)):
            if not winner_id:
                outcome = f"Your {metric_title} challenge with {other_name} ended in a tie."
            elif winner_id == target_id:
                outcome = f"You won your {metric_title} challenge with {other_name}!"
            else:
                outcome = f"Your {metric_title} challenge with {other_name} is complete. {other_name} won."
            _dispatch_person_notification(
                client,
                person_id=target_id,
                challenge_id=challenge_id,
                event=event,
                title="Challenge complete",
                content=outcome,
                priority="high",
            )


def _finish_local_challenges(client: Any) -> None:
    now = time.time()
    for row in _local_challenges(client):
        if row.get("state") != "active" or _number(row.get("end_at")) > now:
            continue
        first = _number(row.get("challenger_score"))
        second = _number(row.get("opponent_score"))
        row["state"] = "completed"
        row["winner_id"] = row.get("challenger_id") if first > second else row.get("opponent_id") if second > first else ""
        row["updated_at"] = now
        _save_local_challenge(client, row)
        _notify_challenge_event(client, row, "completed")


def _local_challenge_view(client: Any, row: Dict[str, Any], person_id: str) -> Dict[str, Any]:
    viewer_is_challenger = _text(row.get("challenger_id"), 160) == person_id
    other_id = _text(row.get("opponent_id") if viewer_is_challenger else row.get("challenger_id"), 160)
    other_name_key = "opponent_name" if viewer_is_challenger else "challenger_name"
    other = _load_person(client, other_id, _text(row.get(other_name_key), 80))
    metric = _text(row.get("metric"), 40) or "steps"
    definition = next((item for item in CHALLENGE_TYPES if item["id"] == metric), CHALLENGE_TYPES[0])
    decimals = int(definition.get("decimals") or 0)
    my_score = round(_number(row.get("challenger_score" if viewer_is_challenger else "opponent_score")), decimals)
    their_score = round(_number(row.get("opponent_score" if viewer_is_challenger else "challenger_score")), decimals)
    winner_id = _text(row.get("winner_id"), 160)
    result = ""
    if row.get("state") == "completed":
        result = "tie" if not winner_id else "won" if winner_id == person_id else "lost"
    return {
        "id": _text(row.get("id"), 120),
        "state": _text(row.get("state"), 24),
        "duration": _text(row.get("duration"), 20),
        "opponent_id": other_id,
        "opponent_name": _text(other.get("name"), 80) or "Tater person",
        "metric": metric,
        "metric_title": definition["title"],
        "unit": definition["unit"],
        "score_kind": definition["score_kind"],
        "my_score": my_score,
        "their_score": their_score,
        "start_at": _number(row.get("start_at")),
        "end_at": _number(row.get("end_at")),
        "created_at": _number(row.get("created_at")),
        "incoming": row.get("state") == "pending" and not viewer_is_challenger,
        "result": result,
        "source": "this_tater",
    }


def _challenge_metric_value(metrics: Dict[str, Any], metric: str) -> Optional[float]:
    source_key = "weight_kg" if metric == "weight_checkins" else metric
    value = _number_or_none(metrics.get(source_key))
    return max(0.0, value) if value is not None else None


def _latest_metric_value(client: Any, person_id: str, metric: str) -> tuple[Optional[float], str]:
    days = _load_days(client, person_id)
    if not days:
        return None, ""
    today = days[0]
    return _challenge_metric_value(today, metric), _text(today.get("date"), 10)


def _update_local_progress(client: Any, person_id: str, metrics: Dict[str, Any], day: str) -> None:
    now = time.time()
    for row in _local_challenges(client):
        if row.get("state") != "active" or person_id not in {row.get("challenger_id"), row.get("opponent_id")}:
            continue
        prefix = "challenger" if row.get("challenger_id") == person_id else "opponent"
        metric = _text(row.get("metric"), 40) or "steps"
        current_value = _challenge_metric_value(metrics, metric)
        if current_value is None or not day:
            continue
        old_day = _text(row.get(f"{prefix}_day"), 10)
        old_value = _number(row.get(f"{prefix}_last_value"))
        old_score = _number(row.get(f"{prefix}_score"))
        if metric == "weight_checkins":
            increment = 0.0 if old_day == day else 1.0
        else:
            increment = max(0.0, current_value - old_value) if old_day == day else current_value
        row[f"{prefix}_score"] = old_score + increment
        row[f"{prefix}_last_value"] = 1.0 if metric == "weight_checkins" else current_value
        row[f"{prefix}_day"] = day
        row["updated_at"] = now
        _save_local_challenge(client, row)


def _load_days(client: Any, node_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for day_key, raw in (client.hgetall(_daily_key(node_id)) or {}).items():
        row = _json_load(raw, {})
        if isinstance(row, dict):
            row["date"] = _text(day_key, 10)
            rows.append(row)
    rows.sort(key=lambda row: row.get("date", ""), reverse=True)
    return rows[:120]


def _normalized_day(row: Dict[str, Any]) -> Dict[str, Any]:
    day_value = _text(row.get("date"), 10)
    try:
        date.fromisoformat(day_value)
    except ValueError as exc:
        raise ValueError("Each fitness day needs a valid YYYY-MM-DD date.") from exc
    normalized: Dict[str, Any] = {"date": day_value}
    for key, definition in HEALTH_METRICS.items():
        value = _number_or_none(row.get(key))
        if value is None:
            continue
        value = max(0.0, value)
        decimals = int(definition.get("decimals") or 0)
        normalized[key] = int(round(value)) if decimals <= 0 else round(value, min(3, decimals + 1))
    return normalized


def _format_metric_value(key: str, value: float) -> str:
    definition = HEALTH_METRICS.get(key) or {"unit": "", "decimals": 1}
    decimals = int(definition.get("decimals") or 0)
    if key == "distance_m":
        return f"{value / 1000:.1f} km"
    if key == "sleep_minutes":
        hours = int(value) // 60
        minutes = int(value) % 60
        return f"{hours}h {minutes}m" if hours else f"{minutes} min"
    rendered = f"{value:,.{decimals}f}" if decimals else f"{int(round(value)):,}"
    unit = _text(definition.get("unit"), 20)
    return f"{rendered} {unit}".strip()


def _latest_row_with_keys(days: List[Dict[str, Any]], keys: List[str]) -> Optional[Dict[str, Any]]:
    return next((row for row in days if any(_number_or_none(row.get(key)) is not None for key in keys)), None)


def _local_insights(days: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    insights: List[Dict[str, Any]] = []
    for definition in INSIGHT_DEFINITIONS:
        keys = list(definition["keys"])
        latest = _latest_row_with_keys(days, keys)
        if latest is None:
            continue
        present = [(key, _number_or_none(latest.get(key))) for key in keys]
        present = [(key, value) for key, value in present if value is not None]
        if not present:
            continue
        primary_key, primary_value = present[0]
        assert primary_value is not None
        if definition["id"] == "blood_pressure" and len(present) >= 2:
            value_text = f"{int(round(present[0][1]))}/{int(round(present[1][1]))} mmHg"
        elif definition["id"] == "heart_rate" and len(present) >= 3:
            value_text = (
                f"{int(round(present[0][1]))} bpm average · "
                f"{int(round(present[1][1]))}–{int(round(present[2][1]))} bpm range"
            )
        else:
            value_text = _format_metric_value(primary_key, primary_value)
        history = [
            _number_or_none(row.get(primary_key))
            for row in days[:7]
            if _number_or_none(row.get(primary_key)) is not None
        ]
        average = sum(history) / len(history) if history else primary_value
        detail = f"Latest recorded value: {value_text}."
        if len(history) >= 2 and definition["id"] not in {"blood_pressure", "heart_rate"}:
            detail += f" Your {len(history)}-day average is {_format_metric_value(primary_key, average)}."
        insights.append(
            {
                "id": definition["id"],
                "title": definition["title"],
                "headline": value_text,
                "detail": detail,
                "recorded_date": _text(latest.get("date"), 10),
                "keys": keys,
            }
        )
    return insights


def _summary(days: List[Dict[str, Any]], ai_summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    local_insights = _local_insights(days)
    if isinstance(ai_summary, dict) and _text(ai_summary.get("headline")) and _text(ai_summary.get("detail")):
        ai_rows = ai_summary.get("insights") if isinstance(ai_summary.get("insights"), list) else []
        ai_by_id = {
            _text(row.get("id"), 60): row
            for row in ai_rows
            if isinstance(row, dict) and _text(row.get("id"), 60)
        }
        merged: List[Dict[str, Any]] = []
        for local in local_insights:
            ai_row = ai_by_id.get(local["id"], {})
            merged.append(
                {
                    **local,
                    "headline": _text(ai_row.get("headline"), 100) or local["headline"],
                    "detail": _text(ai_row.get("detail"), 500) or local["detail"],
                }
            )
        return {
            "headline": _text(ai_summary.get("headline"), 100),
            "detail": _text(ai_summary.get("detail"), 700),
            "generated_at": _number(ai_summary.get("generated_at")),
            "kind": "ai_wellness",
            "insights": merged,
        }
    if not days or not local_insights:
        headline = "Ready when you are"
        detail = "Connect Apple Health or Health Connect in Little Spud to begin your private health history."
    else:
        headline = "Your health overview"
        detail = f"Tater has recent data for {len(local_insights)} health areas. Open a card for its latest value and trend."
    return {
        "headline": headline,
        "detail": detail,
        "generated_at": time.time(),
        "kind": "private_local",
        "insights": local_insights,
    }


def _days_signature(days: List[Dict[str, Any]]) -> str:
    serialized = _json_save(days[:14]).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()[:24]


def _ai_summary(client: Any, node_id: str, days: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not days:
        return None
    data_signature = _days_signature(days)
    cached = _json_load(client.get(AI_SUMMARY_PREFIX + node_id), {})
    if (
        isinstance(cached, dict)
        and int(_number(cached.get("schema"))) == 3
        and time.time() - _number(cached.get("generated_at")) < 24 * 3600
    ):
        return cached
    model = _get_primary_llm_client_from_env()
    if model is None:
        return None
    prompt = (
        "Write a warm, encouraging wellness recap from daily aggregate health totals. Give a positive, "
        "forward-looking overall wellness perspective based only on patterns actually present in the data, "
        "and briefly recognize constructive momentum where the evidence supports it. Do not declare that the "
        "person is healthy or unhealthy, diagnose, label a measurement normal or abnormal, assess medical risk, "
        "recommend treatment, or imply that missing data is zero. Be honest about limited or mixed data without "
        "sounding negative. Compare only trends actually present in the supplied days. "
        "Return JSON only: headline (max 8 words), detail (max 55 words), and insights. Each insight must use "
        "one supplied insight id and contain headline (max 8 words) and detail (max 38 words)."
    )
    available_insights = [row["id"] for row in _local_insights(days)]
    response = asyncio.run(
        model.chat(
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": json.dumps(
                        {"days": days[:14], "insight_ids": available_insights},
                        separators=(",", ":"),
                    ),
                },
            ],
            max_tokens=None,
            temperature=0.3,
        )
    )
    raw = _text(((response or {}).get("message") or {}).get("content"), 8000)
    parsed = json.loads(extract_json(raw) or raw)
    if not isinstance(parsed, dict) or not _text(parsed.get("headline")) or not _text(parsed.get("detail")):
        raise ValueError("The fitness summary model returned an unsupported response.")
    insight_ids = set(available_insights)
    insights = [
        {
            "id": _text(row.get("id"), 60),
            "headline": _text(row.get("headline"), 100),
            "detail": _text(row.get("detail"), 500),
        }
        for row in (parsed.get("insights") or [])
        if isinstance(row, dict)
        and _text(row.get("id"), 60) in insight_ids
        and _text(row.get("headline"), 100)
        and _text(row.get("detail"), 500)
    ]
    result = {
        "schema": 3,
        "headline": _text(parsed.get("headline"), 100),
        "detail": _text(parsed.get("detail"), 700),
        "insights": insights,
        "data_signature": data_signature,
        "generated_at": time.time(),
    }
    client.set(AI_SUMMARY_PREFIX + node_id, _json_save(result))
    return result


def _challenge_snapshot(client: Any, person_id: str) -> List[Dict[str, Any]]:
    _finish_local_challenges(client)
    challenges = [
        _local_challenge_view(client, row, person_id)
        for row in _local_challenges(client)
        if person_id in {row.get("challenger_id"), row.get("opponent_id")}
    ]
    challenges.sort(key=lambda row: _number(row.get("created_at")), reverse=True)
    return challenges


def get_client_fitness_state(
    *, person_id: str, person_name: str = "", people: Optional[List[Dict[str, Any]]] = None,
    node_id: str = "", node_name: str = "", refresh: bool = False, client: Any = None, **_kwargs: Any
) -> Dict[str, Any]:
    store = _client(client)
    person = _load_person(store, person_id, person_name)
    days = _load_days(store, person_id)
    sources = _load_sources(store, person_id)
    cached_ai = _json_load(store.get(AI_SUMMARY_PREFIX + person_id), {})
    if (
        not isinstance(cached_ai, dict)
        or int(_number(cached_ai.get("schema"))) != 3
        or time.time() - _number(cached_ai.get("generated_at")) >= 24 * 3600
    ):
        cached_ai = {}
    challenge_people = [
        {"id": _text(row.get("id"), 160), "name": _text(row.get("name"), 80)}
        for row in (people or [])
        if _text(row.get("id"), 160) and _text(row.get("id"), 160) != person_id
    ]
    return {
        "available": True,
        "configured": True,
        "person": {"id": person_id, "name": person.get("name", "")},
        "challenge_people": challenge_people,
        "today": days[0] if days else {},
        "recent_days": days[:30],
        "summary": _summary(days, cached_ai),
        "metric_catalog": [
            {"id": key, **definition}
            for key, definition in HEALTH_METRICS.items()
        ],
        "challenge_types": CHALLENGE_TYPES,
        "challenges": _challenge_snapshot(store, person_id),
        "last_sync_at": _number(person.get("last_sync_at")),
        "platform": person.get("platform", ""),
        "permissions": person.get("permissions", []),
        "preferred_source_id": _text(person.get("preferred_source_id"), 160),
        "sources": [
            {
                "id": _text(source.get("id"), 160),
                "name": _text(source.get("node_name"), 120),
                "platform": _text(source.get("platform"), 40),
                "last_sync_at": _number(source.get("last_sync_at")),
            }
            for source in sources
        ],
        "privacy": {
            "message": (
                "Little Spud sends daily aggregates only. Raw health samples stay on your phone, and challenge "
                "participants see scores rather than your private measurements."
            ),
        },
    }


def sync_client_fitness(
    *, person_id: str, person_name: str = "", people: Optional[List[Dict[str, Any]]] = None,
    node_id: str = "", node_name: str = "", payload: Optional[Dict[str, Any]] = None,
    client: Any = None, **_kwargs: Any
) -> Dict[str, Any]:
    store = _client(client)
    body = payload if isinstance(payload, dict) else {}
    days = body.get("days") if isinstance(body.get("days"), list) else []
    if len(days) > 120:
        raise ValueError("A fitness sync may include at most 120 days.")
    synced_at = time.time()
    platform = _text(body.get("platform"), 40)
    source_id = _source_id(node_id, platform, node_name)
    normalized_days: List[Dict[str, Any]] = []
    for raw in days:
        if not isinstance(raw, dict):
            continue
        row = _normalized_day(raw)
        normalized_days.append(row)
        stored_row = {key: value for key, value in row.items() if key != "date"}
        stored_row["_synced_at"] = synced_at
        store.hset(_source_daily_key(person_id, source_id), row["date"], _json_save(stored_row))
    person = _load_person(store, person_id, person_name)
    if not _text(person.get("preferred_source_id"), 160):
        person["preferred_source_id"] = source_id
    raw_permissions = body.get("permissions") if isinstance(body.get("permissions"), list) else []
    permissions = [_text(item, 60) for item in raw_permissions[:24]]
    source = {
        "id": source_id,
        "node_id": _text(node_id, 160),
        "node_name": _text(node_name, 120),
        "platform": platform,
        "time_zone": _text(body.get("time_zone"), 80),
        "permissions": permissions,
        "last_sync_at": synced_at,
    }
    store.hset(_source_index_key(person_id), source_id, _json_save(source))
    _materialize_merged_days(
        store,
        person_id,
        [row["date"] for row in normalized_days],
        _text(person.get("preferred_source_id"), 160),
    )
    person.update(
        {
            "name": _text(person_name, 80) or person.get("name") or "Tater person",
            "platform": platform,
            "time_zone": _text(body.get("time_zone"), 80),
            "permissions": permissions,
            "last_sync_at": synced_at,
            "last_node_id": _text(node_id, 160),
            "last_node_name": _text(node_name, 120),
        }
    )
    _save_person(store, person_id, person)
    merged_days = _load_days(store, person_id)
    current_day = merged_days[0] if merged_days else {}
    _update_local_progress(store, person_id, current_day, _text(current_day.get("date"), 10))
    return get_client_fitness_state(
        person_id=person_id,
        person_name=person_name,
        people=people,
        node_id=node_id,
        node_name=node_name,
        client=store,
    )


def run_client_fitness_challenge_action(
    *, person_id: str, person_name: str = "", people: Optional[List[Dict[str, Any]]] = None,
    node_id: str = "", node_name: str = "", action: str, payload: Optional[Dict[str, Any]] = None,
    client: Any = None, **_kwargs: Any
) -> Dict[str, Any]:
    store = _client(client)
    normalized = _text(action, 32).lower()
    if normalized not in {"create", "accept", "decline", "cancel"}:
        raise ValueError("Unsupported fitness challenge action.")
    body = payload if isinstance(payload, dict) else {}
    target_id = _text(body.get("person_id"), 160)
    challenge_id = _text(body.get("challenge_id"), 120)
    metric = _text(body.get("metric"), 40) or "steps"
    definition = next((item for item in CHALLENGE_TYPES if item["id"] == metric), None)
    supplied_value = _number_or_none(body.get("value"))
    people_by_id = {
        _text(row.get("id"), 160): _text(row.get("name"), 80)
        for row in (people or [])
        if _text(row.get("id"), 160)
    }
    local_row = next((row for row in _local_challenges(store) if row.get("id") == challenge_id), None)

    if normalized == "create":
        if not target_id or target_id not in people_by_id:
            raise ValueError("Choose a person from Tater People for this challenge.")
        if target_id == person_id:
            raise ValueError("Choose someone else for a challenge.")
        if definition is None:
            raise ValueError("Choose a supported fitness challenge type.")
        duration = _text(body.get("duration"), 20)
        duration = duration if duration in {"3_hours", "day", "week"} else "day"
        if metric == "weight_checkins" and duration == "3_hours":
            duration = "day"
        current_value, current_day = _latest_metric_value(store, person_id, metric)
        if supplied_value is not None:
            current_value = max(0.0, supplied_value)
        now = time.time()
        local_row = {
            "id": f"challenge_{uuid.uuid4().hex}",
            "challenger_id": person_id,
            "challenger_name": _text(person_name, 80) or "Tater person",
            "opponent_id": target_id,
            "opponent_name": people_by_id[target_id] or "Tater person",
            "metric": metric,
            "duration": duration,
            "state": "pending",
            "created_at": now,
            "start_at": 0,
            "end_at": 0,
            "challenger_score": 0.0,
            "opponent_score": 0.0,
            "challenger_last_value": 1.0 if metric == "weight_checkins" and current_value is not None else (current_value or 0.0),
            "opponent_last_value": 0.0,
            "challenger_day": current_day,
            "opponent_day": "",
            "winner_id": "",
            "updated_at": now,
        }
        _save_local_challenge(store, local_row)
        _notify_challenge_event(store, local_row, "invited", actor_id=person_id)
    elif local_row:
        if person_id not in {local_row.get("challenger_id"), local_row.get("opponent_id")}:
            raise ValueError("Fitness challenge not found.")
        now = time.time()
        if normalized == "accept":
            if local_row.get("state") != "pending" or local_row.get("opponent_id") != person_id:
                raise ValueError("That challenge can no longer be accepted.")
            durations = {"3_hours": 3 * 3600, "day": 24 * 3600, "week": 7 * 24 * 3600}
            challenge_metric = _text(local_row.get("metric"), 40) or "steps"
            challenger_value, challenger_day = _latest_metric_value(
                store, _text(local_row.get("challenger_id"), 160), challenge_metric
            )
            opponent_value, opponent_day = _latest_metric_value(store, person_id, challenge_metric)
            if supplied_value is not None:
                opponent_value = max(0.0, supplied_value)
            challenger_start_score = 1.0 if challenge_metric == "weight_checkins" and challenger_value is not None else 0.0
            opponent_start_score = 1.0 if challenge_metric == "weight_checkins" and opponent_value is not None else 0.0
            local_row.update(
                {
                    "state": "active",
                    "start_at": now,
                    "end_at": now + durations.get(local_row.get("duration"), durations["day"]),
                    "challenger_score": challenger_start_score,
                    "opponent_score": opponent_start_score,
                    "challenger_last_value": (
                        1.0 if challenge_metric == "weight_checkins" and challenger_value is not None else (challenger_value or 0.0)
                    ),
                    "opponent_last_value": (
                        1.0 if challenge_metric == "weight_checkins" and opponent_value is not None else (opponent_value or 0.0)
                    ),
                    "challenger_day": challenger_day,
                    "opponent_day": opponent_day,
                    "updated_at": now,
                }
            )
        elif normalized == "decline":
            if local_row.get("state") != "pending" or local_row.get("opponent_id") != person_id:
                raise ValueError("That challenge can no longer be declined.")
            local_row.update({"state": "declined", "updated_at": now})
        elif normalized == "cancel":
            if local_row.get("state") not in {"pending", "active"}:
                raise ValueError("That challenge is already finished.")
            local_row.update({"state": "cancelled", "updated_at": now})
        else:
            raise ValueError("Unsupported fitness challenge action.")
        _save_local_challenge(store, local_row)
        notification_event = {
            "accept": "accepted",
            "decline": "declined",
            "cancel": "cancelled",
        }[normalized]
        _notify_challenge_event(store, local_row, notification_event, actor_id=person_id)
    else:
        raise ValueError("Fitness challenge not found.")

    return get_client_fitness_state(
        person_id=person_id,
        person_name=person_name,
        people=people,
        node_id=node_id,
        node_name=node_name,
        client=store,
    )


def get_htmlui_tab_data(*, redis_client=None, **_kwargs: Any) -> Dict[str, Any]:
    store = _client(redis_client)
    keys = list(store.scan_iter(f"{PERSON_PREFIX}*"))
    people: List[Dict[str, Any]] = []
    total_steps = 0
    for raw_key in keys:
        key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
        person_id = key[len(PERSON_PREFIX):]
        person = _load_person(store, person_id)
        days = _load_days(store, person_id)
        today_steps = int(_number(days[0].get("steps"))) if days else 0
        total_steps += today_steps
        people.append(
            {
                "title": person.get("name") or "Tater person",
                "subtitle": "Linked through Tater People",
                "detail": f"{today_steps:,} steps today · Last sync {_text(person.get('last_sync_at')) or 'never'}",
            }
        )
    people.sort(key=lambda row: str(row["title"]).lower())
    status = dict(store.hgetall(STATUS_KEY) or {})
    return {
        "summary": "Private daily health trends, AI wellness summaries, and opt-in activity challenges.",
        "stats": [
            {"label": "People", "value": len(people)},
            {"label": "Steps today", "value": f"{total_steps:,}"},
            {"label": "Runtime", "value": _text(status.get("state")) or "stopped"},
        ],
        "items": people,
        "empty_message": "Open Fitness in Little Spud and connect Apple Health or Health Connect.",
    }


def run(stop_event=None) -> None:
    logger.info("[fitness_core] started v%s", __version__)
    last_ai_sweep = 0.0
    try:
        while not (stop_event and stop_event.is_set()):
            redis_client.hset(STATUS_KEY, mapping={"state": "ready", "last_seen": str(time.time())})
            try:
                _finish_local_challenges(redis_client)
            except Exception as exc:
                logger.info("[fitness_core] challenge completion sweep deferred: %s", exc)
            if time.time() - last_ai_sweep >= 3600:
                last_ai_sweep = time.time()
                for raw_key in list(redis_client.scan_iter(f"{PERSON_PREFIX}*"))[:20]:
                    key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                    person_id = key[len(PERSON_PREFIX):]
                    try:
                        _ai_summary(redis_client, person_id, _load_days(redis_client, person_id))
                    except Exception as exc:
                        logger.info("[fitness_core] AI wellness summary deferred for %s: %s", person_id, exc)
            try:
                interval = max(15.0, min(3600.0, _number(_settings().get("challenge_sync_interval_seconds"), 30)))
            except Exception:
                interval = 30.0
            if stop_event:
                if stop_event.wait(interval):
                    break
            else:
                time.sleep(interval)
    finally:
        logger.info("[fitness_core] stopped")
