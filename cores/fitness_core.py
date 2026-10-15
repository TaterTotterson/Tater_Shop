"""Privacy-first fitness history, summaries, and Little Spud step challenges."""

from __future__ import annotations

import asyncio
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

__version__ = "1.1.1"
MIN_TATER_VERSION = "200"
CORE_DESCRIPTION = (
    "Store health aggregates sent by Little Spud, show private fitness trends, and run opt-in step challenges."
)
TAGS = ["fitness", "health", "steps", "challenges", "little-spud"]

logger = logging.getLogger("fitness_core")

SETTINGS_KEY = "fitness_core_settings"
STATUS_KEY = "fitness_core:status"
PERSON_PREFIX = "fitness_core:person:"
DAILY_PREFIX = "fitness_core:daily:"
CHALLENGE_RECORDS_KEY = "fitness_core:challenge_records"
AI_SUMMARY_PREFIX = "fitness_core:ai_summary:"

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


def _finish_local_challenges(client: Any) -> None:
    now = time.time()
    for row in _local_challenges(client):
        if row.get("state") != "active" or _number(row.get("end_at")) > now:
            continue
        first = int(_number(row.get("challenger_score")))
        second = int(_number(row.get("opponent_score")))
        row["state"] = "completed"
        row["winner_id"] = row.get("challenger_id") if first > second else row.get("opponent_id") if second > first else ""
        row["updated_at"] = now
        _save_local_challenge(client, row)


def _local_challenge_view(client: Any, row: Dict[str, Any], person_id: str) -> Dict[str, Any]:
    viewer_is_challenger = _text(row.get("challenger_id"), 160) == person_id
    other_id = _text(row.get("opponent_id") if viewer_is_challenger else row.get("challenger_id"), 160)
    other_name_key = "opponent_name" if viewer_is_challenger else "challenger_name"
    other = _load_person(client, other_id, _text(row.get(other_name_key), 80))
    my_steps = int(_number(row.get("challenger_score" if viewer_is_challenger else "opponent_score")))
    their_steps = int(_number(row.get("opponent_score" if viewer_is_challenger else "challenger_score")))
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
        "my_steps": my_steps,
        "their_steps": their_steps,
        "start_at": _number(row.get("start_at")),
        "end_at": _number(row.get("end_at")),
        "created_at": _number(row.get("created_at")),
        "incoming": row.get("state") == "pending" and not viewer_is_challenger,
        "result": result,
        "source": "this_tater",
    }


def _update_local_progress(client: Any, person_id: str, steps: int, day: str) -> None:
    now = time.time()
    for row in _local_challenges(client):
        if row.get("state") != "active" or person_id not in {row.get("challenger_id"), row.get("opponent_id")}:
            continue
        prefix = "challenger" if row.get("challenger_id") == person_id else "opponent"
        old_day = _text(row.get(f"{prefix}_day"), 10)
        old_steps = int(_number(row.get(f"{prefix}_last_steps")))
        old_score = int(_number(row.get(f"{prefix}_score")))
        increment = max(0, steps - old_steps) if old_day == day else max(0, steps)
        row[f"{prefix}_score"] = old_score + increment
        row[f"{prefix}_last_steps"] = steps
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
    return {
        "date": day_value,
        "steps": max(0, int(_number(row.get("steps")))),
        "distance_m": round(max(0.0, _number(row.get("distance_m"))), 2),
        "active_energy_kcal": round(max(0.0, _number(row.get("active_energy_kcal"))), 2),
        "exercise_minutes": round(max(0.0, _number(row.get("exercise_minutes"))), 2),
        "workouts": max(0, int(_number(row.get("workouts")))),
        "sleep_minutes": round(max(0.0, _number(row.get("sleep_minutes"))), 2),
        "resting_heart_rate_bpm": round(max(0.0, _number(row.get("resting_heart_rate_bpm"))), 2),
        "weight_kg": round(max(0.0, _number(row.get("weight_kg"))), 3),
    }


def _summary(days: List[Dict[str, Any]], ai_summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if isinstance(ai_summary, dict) and _text(ai_summary.get("headline")) and _text(ai_summary.get("detail")):
        return {
            "headline": _text(ai_summary.get("headline"), 100),
            "detail": _text(ai_summary.get("detail"), 500),
            "generated_at": _number(ai_summary.get("generated_at")),
            "kind": "ai_wellness",
        }
    recent = days[:7]
    today = days[0] if days else {}
    steps = int(_number(today.get("steps")))
    average = int(sum(int(_number(row.get("steps"))) for row in recent) / max(1, len(recent)))
    active_days = sum(1 for row in recent if int(_number(row.get("steps"))) >= 5000)
    if not days:
        headline = "Ready when you are"
        detail = "Connect Apple Health or Health Connect in Little Spud to begin your private fitness history."
    elif steps >= 10000:
        headline = "Step goal reached"
        detail = f"You have {steps:,} steps today. Your {len(recent)}-day average is {average:,}."
    elif average and steps >= average:
        headline = "Ahead of your recent pace"
        detail = f"You have {steps:,} steps today, above your {len(recent)}-day average of {average:,}."
    else:
        headline = "Keep moving"
        detail = f"You have {steps:,} steps today. You were active on {active_days} of the last {len(recent)} days."
    return {"headline": headline, "detail": detail, "generated_at": time.time(), "kind": "private_local"}


def _ai_summary(client: Any, node_id: str, days: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not days:
        return None
    cached = _json_load(client.get(AI_SUMMARY_PREFIX + node_id), {})
    if isinstance(cached, dict) and time.time() - _number(cached.get("generated_at")) < 12 * 3600:
        return cached
    model = _get_primary_llm_client_from_env()
    if model is None:
        return None
    prompt = (
        "Write a friendly, strictly descriptive wellness recap from daily aggregate fitness totals. "
        "Do not diagnose, assess medical risk, recommend treatment, or imply that missing data is zero. "
        "Return JSON only with headline (max 8 words) and detail (max 55 words)."
    )
    response = asyncio.run(
        model.chat(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps({"days": days[:14]}, separators=(",", ":"))},
            ],
            max_tokens=None,
            temperature=0.2,
        )
    )
    raw = _text(((response or {}).get("message") or {}).get("content"), 2000)
    parsed = json.loads(extract_json(raw) or raw)
    if not isinstance(parsed, dict) or not _text(parsed.get("headline")) or not _text(parsed.get("detail")):
        raise ValueError("The fitness summary model returned an unsupported response.")
    result = {
        "headline": _text(parsed.get("headline"), 100),
        "detail": _text(parsed.get("detail"), 500),
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
    cached_ai = _json_load(store.get(AI_SUMMARY_PREFIX + person_id), {})
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
        "challenges": _challenge_snapshot(store, person_id),
        "last_sync_at": _number(person.get("last_sync_at")),
        "platform": person.get("platform", ""),
        "permissions": person.get("permissions", []),
        "privacy": {
            "message": "Little Spud sends daily totals only. Raw health samples stay on your phone.",
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
    for raw in days:
        if not isinstance(raw, dict):
            continue
        row = _normalized_day(raw)
        store.hset(_daily_key(person_id), row["date"], _json_save({key: value for key, value in row.items() if key != "date"}))
    person = _load_person(store, person_id, person_name)
    person.update(
        {
            "name": _text(person_name, 80) or person.get("name") or "Tater person",
            "platform": _text(body.get("platform"), 24),
            "time_zone": _text(body.get("time_zone"), 80),
            "permissions": [_text(item, 60) for item in (body.get("permissions") or [])[:24]],
            "last_sync_at": time.time(),
            "last_node_id": _text(node_id, 160),
            "last_node_name": _text(node_name, 120),
        }
    )
    _save_person(store, person_id, person)
    challenge_steps = max(0, int(_number(body.get("challenge_steps") or (days[0].get("steps") if days and isinstance(days[0], dict) else 0))))
    challenge_day = _text(days[0].get("date"), 10) if days and isinstance(days[0], dict) else ""
    _update_local_progress(store, person_id, challenge_steps, challenge_day)
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
    steps = max(0, int(_number(body.get("steps"))))
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
            raise ValueError("Choose someone else for a step challenge.")
        duration = _text(body.get("duration"), 20)
        duration = duration if duration in {"3_hours", "day", "week"} else "day"
        now = time.time()
        local_row = {
            "id": f"challenge_{uuid.uuid4().hex}",
            "challenger_id": person_id,
            "challenger_name": _text(person_name, 80) or "Tater person",
            "opponent_id": target_id,
            "opponent_name": people_by_id[target_id] or "Tater person",
            "duration": duration,
            "state": "pending",
            "created_at": now,
            "start_at": 0,
            "end_at": 0,
            "challenger_score": 0,
            "opponent_score": 0,
            "challenger_last_steps": steps,
            "opponent_last_steps": 0,
            "challenger_day": "",
            "opponent_day": "",
            "winner_id": "",
            "updated_at": now,
        }
        _save_local_challenge(store, local_row)
    elif local_row:
        if person_id not in {local_row.get("challenger_id"), local_row.get("opponent_id")}:
            raise ValueError("Fitness challenge not found.")
        now = time.time()
        if normalized == "accept":
            if local_row.get("state") != "pending" or local_row.get("opponent_id") != person_id:
                raise ValueError("That challenge can no longer be accepted.")
            durations = {"3_hours": 3 * 3600, "day": 24 * 3600, "week": 7 * 24 * 3600}
            challenger_days = _load_days(store, _text(local_row.get("challenger_id"), 160))
            challenger_steps = int(_number(challenger_days[0].get("steps"))) if challenger_days else 0
            challenger_day = _text(challenger_days[0].get("date"), 10) if challenger_days else ""
            opponent_days = _load_days(store, person_id)
            opponent_day = _text(opponent_days[0].get("date"), 10) if opponent_days else ""
            local_row.update(
                {
                    "state": "active",
                    "start_at": now,
                    "end_at": now + durations.get(local_row.get("duration"), durations["day"]),
                    "challenger_score": 0,
                    "opponent_score": 0,
                    "challenger_last_steps": challenger_steps,
                    "opponent_last_steps": steps,
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
        "summary": "Private daily health totals from Little Spud and opt-in step challenges.",
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
            if time.time() - last_ai_sweep >= 600:
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
