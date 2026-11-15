"""Manage compatible Reachy Mini apps through Tater's native satellite link."""

from __future__ import annotations

import base64
import binascii
import contextlib
import json
import logging
import random
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List


__version__ = "1.2.8"
MIN_TATER_VERSION = "99.5"
CORE_DESCRIPTION = (
    "Control Reachy Tater Satellite and Reachy Tater Embedded behavior directly "
    "from Tater, including tracking, motion, music reactions, vision, head leveling, "
    "Idle Life, and Face ID greetings backed by Tater People."
)
TAGS = [
    "reachy",
    "reachy-mini",
    "robot",
    "satellite",
    "idle-life",
    "motion",
    "face-id",
    "people",
]

CORE_SETTINGS = {
    "category": "Reachy Core Settings",
    "required": {},
}

CORE_WEBUI_TAB = {
    "label": "Reachy",
    "order": 24,
    "requires_running": False,
}

# Optional presentation metadata for Tater’s shared core workspace renderer.
CORE_WORKSPACE = {'schema': 1,
 'icon': 'robot',
 'description': 'Tracking, motion, vision, greetings, and Idle Life.',
 'views': {},
 'field_groups': [{'label': 'Behavior',
                   'terms': ['enabled',
                             'behavior',
                             'motion',
                             'strength',
                             'level',
                             'pitch',
                             'yaw',
                             'roll']},
                  {'label': 'Tracking & vision',
                   'terms': ['track', 'watch', 'vision', 'camera', 'follow', 'focus']},
                  {'label': 'People & greetings',
                   'terms': ['person', 'face', 'greeting', 'recognition']},
                  {'label': 'Idle & music',
                   'terms': ['idle', 'ambient', 'music', 'dance', 'reaction']}],
 'screens': {'*': {'layout': 'behavior',
                   'label': 'Reachy behavior',
                   'icon': 'robot',
                   'context_label': 'Device context'}},
 'preview_fields': ['enabled', 'motion_enabled', 'tracking_enabled'],
 'edit_label': 'Edit behavior'}
CORE_WEBUI_TAB["workspace"] = CORE_WORKSPACE

_SETTINGS_CAPABILITY = "reachy_settings"
_SETTINGS_PROTOCOL_VERSION = 1
_SUPPORTED_SECTIONS = {"motion", "watch", "idle_life", "face_id", "reachy"}
_FACE_POLL_SECONDS = 0.2
_FACE_SETTINGS_REFRESH_SECONDS = 20.0
_FACE_PRESENCE_STALE_SECONDS = 20.0
_FACE_SESSION_LOSS_SECONDS = 30.0
_FACE_SCAN_RETRY_SECONDS = 5.0
_MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024
_FACE_STATE_LOCK = threading.RLock()
_FACE_STATES: Dict[str, Dict[str, Any]] = {}
_GREETING_TEMPLATES = (
    "Oh, hello there, {name}!",
    "Hey {name}, nice to see you!",
    "Well hello, {name}!",
    "Hi {name}! Glad you stopped by.",
    "There you are, {name}!",
    "Hello {name}! It's good to see you.",
)

logger = logging.getLogger("reachy_core")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _native_snapshot() -> Dict[str, Any]:
    from tater_voice import native_satellite

    result = native_satellite.status_snapshot_sync()
    return result if isinstance(result, dict) else {}


def _native_request(
    selector: str,
    message_type: str,
    payload: Dict[str, Any] | None = None,
    *,
    timeout: float = 8.0,
) -> Dict[str, Any]:
    from tater_voice import native_satellite

    result = native_satellite.run_on_runtime_loop(
        native_satellite.send_request(
            selector,
            message_type,
            payload or {},
            timeout_s=timeout,
        ),
        timeout=timeout + 1.0,
    )
    return result if isinstance(result, dict) else {}


def _native_command(
    selector: str,
    message_type: str,
    payload: Dict[str, Any] | None = None,
    *,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    from tater_voice import native_satellite

    result = native_satellite.run_on_runtime_loop(
        native_satellite.send_command(selector, message_type, payload or {}),
        timeout=timeout,
    )
    return result if isinstance(result, dict) else {}


def _connected_reachys() -> List[Dict[str, Any]]:
    try:
        snapshot = _native_snapshot()
    except Exception:
        return []
    raw_clients = snapshot.get("clients")
    if not isinstance(raw_clients, dict):
        return []
    rows: List[Dict[str, Any]] = []
    for selector, raw in raw_clients.items():
        row = raw if isinstance(raw, dict) else {}
        board = _text(row.get("board")).lower().replace("-", "_")
        if not _as_bool(row.get("connected"), False) or board != "reachy_mini":
            continue
        capabilities = row.get("capabilities") if isinstance(row.get("capabilities"), dict) else {}
        rows.append(
            {
                **row,
                "selector": _text(row.get("selector") or selector),
                "capabilities": capabilities,
                "settings_supported": _as_bool(capabilities.get(_SETTINGS_CAPABILITY), False),
                "settings_version": int(_as_float(capabilities.get("reachy_settings_version"), 0)),
            }
        )
    return sorted(rows, key=lambda row: (_device_name(row).lower(), _text(row.get("selector"))))


def _device_name(row: Dict[str, Any]) -> str:
    return _text(row.get("device_name") or row.get("name") or row.get("selector")) or "Reachy Mini"


def _card_id(selector: str, section: str) -> str:
    return json.dumps(
        {"selector": selector, "section": section},
        separators=(",", ":"),
        sort_keys=True,
    )


def _section_group(selector: str, section: str) -> str:
    return f"{selector}::{section}"


def _parse_card_id(value: Any) -> tuple[str, str]:
    try:
        parsed = json.loads(_text(value))
    except Exception as exc:
        raise ValueError("Reachy card identifier is invalid. Refresh the page and try again.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Reachy card identifier is invalid. Refresh the page and try again.")
    selector = _text(parsed.get("selector"))
    section = _text(parsed.get("section"))
    if not selector or not section:
        raise ValueError("Reachy card identifier is incomplete. Refresh the page and try again.")
    return selector, section


def _field(
    key: str,
    label: str,
    field_type: str,
    value: Any,
    *,
    description: str = "",
    minimum: float | None = None,
    maximum: float | None = None,
    step: float | None = None,
    suffix: str = "",
    options: List[Dict[str, str]] | None = None,
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "key": key,
        "label": label,
        "type": field_type,
        "value": value,
    }
    if description:
        row["description"] = description
    if minimum is not None:
        row["min"] = minimum
    if maximum is not None:
        row["max"] = maximum
    if step is not None:
        row["step"] = step
    if suffix:
        row["suffix"] = suffix
    if options:
        row["options"] = options
    return row


def _device_card_base(
    row: Dict[str, Any],
    *,
    section: str,
    title: str,
    subtitle: str,
) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    return {
        "id": _card_id(selector, section),
        "group": _section_group(selector, section),
        "title": title,
        "subtitle": subtitle,
        "save_action": "reachy_save_settings",
        "save_label": "Save and Apply",
        "actions": [
            {
                "action": "reachy_reset_settings",
                "label": "Reset Section",
                "confirm": f"Restore the default {title.lower()} settings on {_device_name(row)}?",
            }
        ],
    }


def _vision_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="reachy",
        title="Vision and Head",
        subtitle="Camera permission and automatic head leveling owned by the Reachy app.",
    )
    card["fields"] = [
        _field(
            "allow_vision_snapshots",
            "Allow Vision Snapshots",
            "checkbox",
            _as_bool(values.get("allow_vision_snapshots")),
            description="Allow this paired Tater to request fresh still images for Reachy Vision and ambient comments.",
        ),
        _field(
            "auto_level_head",
            "Automatically Straighten Head",
            "checkbox",
            _as_bool(values.get("auto_level_head"), True),
            description="Measure and correct Reachy's resting head roll when the app starts.",
        ),
        _field(
            "head_level_gain",
            "Head Level Correction Strength",
            "number",
            _as_float(values.get("head_level_gain"), 0.65),
            minimum=0,
            maximum=1,
            step=0.05,
        ),
        _field(
            "head_level_max_correction_deg",
            "Maximum Head Level Correction",
            "number",
            _as_float(values.get("head_level_max_correction_deg"), 15),
            minimum=0,
            maximum=45,
            step=1,
            suffix="°",
        ),
    ]
    return card


def _motion_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="motion",
        title="Motion and Music",
        subtitle="Control expressive animations, talking motion, and music personality.",
    )
    card["fields"] = [
        _field("enabled", "Enable Reachy Motion", "checkbox", _as_bool(values.get("enabled"), True)),
        _field("intensity", "Overall Motion Intensity", "number", _as_float(values.get("intensity"), 0.65), minimum=0, maximum=1, step=0.05),
        _field("idle_enabled", "Idle Motion", "checkbox", _as_bool(values.get("idle_enabled"), True)),
        _field("speaking_enabled", "Talking Animations", "checkbox", _as_bool(values.get("speaking_enabled"), True)),
        _field("thinking_enabled", "Thinking and Tool Animations", "checkbox", _as_bool(values.get("thinking_enabled"), True)),
        _field("music_dance_enabled", "Dance to Music", "checkbox", _as_bool(values.get("music_dance_enabled"), True)),
        _field("music_dance_intensity", "Music Dance Intensity", "number", _as_float(values.get("music_dance_intensity"), 0.78), minimum=0, maximum=1, step=0.05),
        _field("music_reactions_enabled", "Spoken Song Reactions", "checkbox", _as_bool(values.get("music_reactions_enabled"), True)),
    ]
    return card


def _face_id_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="face_id",
        title="Face ID",
        subtitle=(
            "Use one locally approved snapshot to recognize a person, greet them naturally, "
            "and keep their linked People profile active for that tracking session."
        ),
    )
    card["fields"] = [
        _field(
            "enabled",
            "Enable Reachy Face ID",
            "checkbox",
            _as_bool(values.get("enabled")),
            description=(
                "Requires Idle Life, Reachy Vision snapshots, and Tater Face ID. "
                "A Face ID profile must be linked to a Person before Reachy uses its name. "
                "Reachy's local tracker must first hold a clear, centered face."
            ),
        ),
        _field(
            "store_photos",
            "Store Face ID Photos",
            "checkbox",
            _as_bool(values.get("store_photos"), False),
            description=(
                "Off keeps recognition read-only. When enabled, Reachy stores one cropped "
                "face observation per genuine tracking session. Known matches strengthen "
                "that Person's Face ID profile; unknown faces appear in People Face ID "
                "for later review and sorting."
            ),
        ),
        _field(
            "greetings_enabled",
            "Greet Recognized People",
            "checkbox",
            _as_bool(values.get("greetings_enabled"), True),
            description=(
                "Say one varied hello using the matched Person's display name when a new "
                "tracking session identifies them. Brief tracker dropouts keep the same "
                "session; Reachy must lose the face continuously for 30 seconds before "
                "a later match can greet again."
            ),
        ),
        _field(
            "conversation_identity_enabled",
            "Use Face ID in Conversations",
            "checkbox",
            _as_bool(values.get("conversation_identity_enabled"), True),
            description=(
                "Use the known person Reachy is currently tracking as the Person for Reachy conversations. "
                "This does not change identity handling on satellites without cameras."
            ),
        ),
        _field(
            "context_ttl_seconds",
            "Conversation Match Lifetime",
            "number",
            _as_float(values.get("context_ttl_seconds"), 300),
            minimum=30,
            maximum=3600,
            step=30,
            suffix="sec",
            description="Safety expiry if Reachy stops reporting visual tracking state.",
        ),
    ]
    return card


def _people_rows(redis_client: Any = None) -> List[Dict[str, Any]]:
    try:
        import people

        store = people.load_store(redis_client)
    except Exception:
        return []
    rows = [dict(item) for item in list(store.get("people") or []) if isinstance(item, dict)]
    rows.sort(key=lambda item: (_text(item.get("display_name")).lower(), _text(item.get("id"))))
    return rows


def _people_options(redis_client: Any = None) -> List[Dict[str, str]]:
    options = [{"value": "", "label": "Choose a person"}]
    for person in _people_rows(redis_client):
        person_id = _text(person.get("id"))
        display_name = _text(person.get("display_name"))
        if person_id and display_name:
            options.append({"value": person_id, "label": display_name})
    return options


def _face_enrollment_card(row: Dict[str, Any], redis_client: Any = None) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    people_options = _people_options(redis_client)
    has_people = len(people_options) > 1
    return {
        "id": _card_id(selector, "face_enrollment"),
        "group": _section_group(selector, "face_id"),
        "title": "Add a Face",
        "subtitle": (
            "Choose an existing Tater Person, then upload a clear photo or take one "
            "with this browser's camera."
        ),
        "save_action": "reachy_face_enroll",
        "save_label": "Add Face to Person",
        "fields": [
            _field(
                "person_id",
                "Person",
                "select",
                "",
                options=people_options,
                description=(
                    "The face will be added to this Person's Face ID profile."
                    if has_people
                    else "Create a Person in Tater's People settings first, then return here."
                ),
            ),
            {
                "key": "face_image",
                "label": "Face Photo",
                "type": "file",
                "value": "",
                "accept": "image/jpeg,image/png,image/webp",
                "file_encoding": "base64",
                "max_bytes": _MAX_SNAPSHOT_BYTES,
                "camera_capture": True,
                "camera_facing_mode": "user",
                "disabled": not has_people,
                "full_width": True,
                "description": (
                    "Use one clear, front-facing photo containing only this person. Live camera "
                    "capture is offered when the page is opened over HTTPS or on localhost."
                ),
            },
        ],
    }


def _tracking_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="watch",
        title="Person Tracking",
        subtitle="Keep Reachy's gaze on people while the head leads and the body follows only when needed.",
    )
    card["fields"] = [
        _field("enabled", "Track People", "checkbox", _as_bool(values.get("enabled"), True)),
        _field("track_during_voice", "Track During Voice Turns", "checkbox", _as_bool(values.get("track_during_voice"), True)),
        _field("poll_seconds", "Tracking Update Interval", "number", _as_float(values.get("poll_seconds"), 0.2), minimum=0.1, maximum=2, step=0.05, suffix="sec"),
        _field("tracking_weight", "Idle Tracking Strength", "number", _as_float(values.get("tracking_weight"), 1), minimum=0, maximum=1, step=0.05),
        _field("listening_tracking_weight", "Listening Tracking Strength", "number", _as_float(values.get("listening_tracking_weight"), 1), minimum=0, maximum=1, step=0.05),
        _field("voice_tracking_weight", "Speaking/Thinking Tracking Strength", "number", _as_float(values.get("voice_tracking_weight"), 1), minimum=0, maximum=1, step=0.05),
        _field("body_follow_start_deg", "Body Follow Start", "number", _as_float(values.get("body_follow_start_deg"), 32), minimum=5, maximum=90, step=1, suffix="°"),
        _field("body_follow_target_deg", "Body Follow Target", "number", _as_float(values.get("body_follow_target_deg"), 18), minimum=0, maximum=60, step=1, suffix="°"),
        _field("body_follow_gain", "Body Follow Gain", "number", _as_float(values.get("body_follow_gain"), 0.45), minimum=0.05, maximum=1, step=0.05),
        _field("body_follow_max_step_deg", "Maximum Body Step", "number", _as_float(values.get("body_follow_max_step_deg"), 5), minimum=0.25, maximum=20, step=0.25, suffix="°"),
        _field("body_yaw_limit_deg", "Body Turn Limit", "number", _as_float(values.get("body_yaw_limit_deg"), 120), minimum=30, maximum=180, step=5, suffix="°"),
        _field("body_recenter_delay_seconds", "Body Recenter Delay", "number", _as_float(values.get("body_recenter_delay_seconds"), 3), minimum=0, maximum=60, step=0.5, suffix="sec"),
        _field("body_recenter_step_deg", "Body Recenter Step", "number", _as_float(values.get("body_recenter_step_deg"), 3), minimum=0.25, maximum=20, step=0.25, suffix="°"),
    ]
    return card


def _idle_life_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="idle_life",
        title="Idle Life",
        subtitle="Give Reachy natural room exploration, spontaneous observations, and optional naps.",
    )
    card["actions"].insert(
        0,
        {
            "action": "reachy_test_ambient_comment",
            "label": "Test Ambient Comment",
        },
    )
    card["fields"] = [
        _field("enabled", "Enable Idle Life", "checkbox", _as_bool(values.get("enabled"))),
        _field("look_around_enabled", "Explore the Room", "checkbox", _as_bool(values.get("look_around_enabled"))),
        _field("ambient_comments_enabled", "Spontaneous Camera Comments", "checkbox", _as_bool(values.get("ambient_comments_enabled"))),
        _field("sleep_enabled", "Occasionally Go to Sleep", "checkbox", _as_bool(values.get("sleep_enabled"))),
        _field("activation_delay_seconds", "Initial Activity Delay", "number", _as_float(values.get("activation_delay_seconds"), 20), minimum=5, maximum=600, step=5, suffix="sec"),
        _field("look_around_min_seconds", "Room Look Minimum Delay", "number", _as_float(values.get("look_around_min_seconds"), 60), minimum=15, maximum=3600, step=5, suffix="sec"),
        _field("look_around_max_seconds", "Room Look Maximum Delay", "number", _as_float(values.get("look_around_max_seconds"), 150), minimum=15, maximum=7200, step=5, suffix="sec"),
        _field("ambient_comment_min_seconds", "Comment Minimum Delay", "number", _as_float(values.get("ambient_comment_min_seconds"), 1200), minimum=300, maximum=86400, step=60, suffix="sec"),
        _field("ambient_comment_max_seconds", "Comment Maximum Delay", "number", _as_float(values.get("ambient_comment_max_seconds"), 2700), minimum=300, maximum=172800, step=60, suffix="sec"),
        _field("sleep_after_seconds", "Sleep After No Activity", "number", _as_float(values.get("sleep_after_seconds"), 900), minimum=60, maximum=86400, step=60, suffix="sec"),
        _field("sleep_min_seconds", "Minimum Sleep Time", "number", _as_float(values.get("sleep_min_seconds"), 90), minimum=30, maximum=3600, step=30, suffix="sec"),
        _field("sleep_max_seconds", "Maximum Sleep Time", "number", _as_float(values.get("sleep_max_seconds"), 300), minimum=30, maximum=7200, step=30, suffix="sec"),
        _field("wake_on_sound", "Wake from Nearby Sound", "checkbox", _as_bool(values.get("wake_on_sound"), True)),
        _field("sound_wake_threshold", "Sound Wake Threshold", "number", _as_float(values.get("sound_wake_threshold"), 0.035), minimum=0.005, maximum=0.5, step=0.005),
        _field("quiet_hours_enabled", "Ambient Comment Quiet Hours", "checkbox", _as_bool(values.get("quiet_hours_enabled"))),
        _field("quiet_hours_start", "Quiet Hours Start", "time", _text(values.get("quiet_hours_start")) or "22:00"),
        _field("quiet_hours_end", "Quiet Hours End", "time", _text(values.get("quiet_hours_end")) or "07:00"),
    ]
    return card


def _status_card(row: Dict[str, Any], response: Dict[str, Any]) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    app = response.get("app") if isinstance(response.get("app"), dict) else {}
    status = response.get("status") if isinstance(response.get("status"), dict) else {}
    app_kind = _text(app.get("kind")) or "satellite"
    app_version = _text(app.get("version")) or _text(row.get("firmware_version")) or "unknown"
    return {
        "id": _card_id(selector, "all"),
        "group": _section_group(selector, "overview"),
        "title": _device_name(row),
        "subtitle": f"Reachy Tater {app_kind.title()} · app {app_version}",
        "detail": _text(status.get("connection_error")) or "Connected through Tater's authenticated native satellite link.",
        "hero_badges": [
            {"label": "CONNECTED", "tone": "good"},
            {"label": app_kind.upper(), "tone": "muted"},
            {"label": f"SETTINGS V{int(response.get('protocol_version') or 1)}", "tone": "muted"},
        ],
        "summary_rows": [
            {"label": "Room", "value": _text(row.get("room")) or "Unassigned"},
            {"label": "Selector", "value": selector},
            {"label": "Bridge", "value": _text(status.get("connection_state")) or "connected"},
        ],
        "actions": [
            {
                "action": "reachy_restart_bridge",
                "label": "Restart Reachy Bridge",
                "confirm": f"Restart the satellite bridge on {_device_name(row)}?",
            },
            {
                "action": "reachy_reset_settings",
                "label": "Reset All Reachy Settings",
                "confirm": f"Restore all managed Reachy settings on {_device_name(row)} to defaults?",
                "tone": "danger",
            },
        ],
    }


def _unsupported_card(row: Dict[str, Any]) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    return {
        "id": _card_id(selector, "unsupported"),
        "group": _section_group(selector, "overview"),
        "title": _device_name(row),
        "subtitle": "Reachy app update required",
        "detail": (
            "This Reachy is connected, but its app does not advertise remote settings. "
            "Update Reachy Tater Satellite or Reachy Tater Embedded, then restart the app."
        ),
        "hero_badges": [
            {"label": "CONNECTED", "tone": "good"},
            {"label": "UPDATE NEEDED", "tone": "warn"},
        ],
    }


def _error_card(row: Dict[str, Any], error: str) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    return {
        "id": _card_id(selector, "error"),
        "group": _section_group(selector, "overview"),
        "title": _device_name(row),
        "subtitle": "Reachy settings are temporarily unavailable",
        "detail": error or "The Reachy app did not answer the settings request.",
        "hero_badges": [{"label": "RETRY", "tone": "warn"}],
    }


def _read_settings(row: Dict[str, Any]) -> Dict[str, Any]:
    response = _native_request(_text(row.get("selector")), "reachy.settings.get", {}, timeout=5.0)
    if not _as_bool(response.get("ok"), False):
        raise RuntimeError(_text(response.get("error")) or "Reachy rejected the settings request.")
    version = int(_as_float(response.get("protocol_version"), 0))
    if version < _SETTINGS_PROTOCOL_VERSION:
        raise RuntimeError("The connected Reachy settings protocol is too old. Update the Reachy app.")
    return response


def _device_manager_tab(
    row: Dict[str, Any],
    index: int,
    *,
    sections: List[str],
) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    labels = {
        "overview": "Overview",
        "reachy": "Vision & Head",
        "watch": "Tracking",
        "motion": "Motion & Music",
        "idle_life": "Idle Life",
        "face_id": "Face ID",
    }
    return {
        "key": f"reachy-{index + 1}",
        "label": _device_name(row),
        "source": "grouped_items",
        "groups": [
            {
                "key": section,
                "label": labels[section],
                "item_group": _section_group(selector, section),
                "selector": False,
                "empty_message": f"No {labels[section].lower()} settings are available.",
            }
            for section in sections
        ],
    }


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    reachys = _connected_reachys()
    compatible = [row for row in reachys if row.get("settings_supported")]
    forms: List[Dict[str, Any]] = []
    tabs: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []

    for index, row in enumerate(reachys):
        if not row.get("settings_supported"):
            forms.append(_unsupported_card(row))
            tabs.append(
                _device_manager_tab(row, index, sections=["overview"])
            )
            continue
        try:
            response = _read_settings(row)
            settings = response.get("settings") if isinstance(response.get("settings"), dict) else {}
            forms.extend(
                [
                    _status_card(row, response),
                    _vision_card(row, settings.get("reachy") if isinstance(settings.get("reachy"), dict) else {}),
                    _tracking_card(row, settings.get("watch") if isinstance(settings.get("watch"), dict) else {}),
                    _motion_card(row, settings.get("motion") if isinstance(settings.get("motion"), dict) else {}),
                    _idle_life_card(row, settings.get("idle_life") if isinstance(settings.get("idle_life"), dict) else {}),
                    _face_id_card(row, settings.get("face_id") if isinstance(settings.get("face_id"), dict) else {}),
                    _face_enrollment_card(row, redis_client),
                ]
            )
            tabs.append(
                _device_manager_tab(
                    row,
                    index,
                    sections=["overview", "reachy", "watch", "motion", "idle_life", "face_id"],
                )
            )
        except Exception as exc:
            forms.append(_error_card(row, _text(exc)))
            tabs.append(
                _device_manager_tab(row, index, sections=["overview"])
            )

    for row in reachys:
        items.append(
            {
                "title": _device_name(row),
                "subtitle": _text(row.get("room")) or "Unassigned room",
                "detail": (
                    "Remote settings ready"
                    if row.get("settings_supported")
                    else "Reachy app update required"
                ),
            }
        )

    ui: Dict[str, Any] = {
        "kind": "settings_manager",
        "title": "Reachy Mini Control",
        "empty_message": (
            "No connected Reachy Mini was found. Start Reachy Tater Satellite or "
            "Reachy Tater Embedded and confirm it is connected to this Tater."
        ),
        "item_forms": forms,
    }
    if tabs:
        ui["manager_tabs"] = tabs
        ui["default_tab"] = tabs[0]["key"]

    return {
        "summary": "Manage settings stored on connected Reachy Mini apps.",
        "stats": [
            {"label": "Reachys", "value": len(reachys)},
            {"label": "Ready", "value": len(compatible)},
            {"label": "Update Needed", "value": len(reachys) - len(compatible)},
        ],
        "items": items,
        "empty_message": ui["empty_message"],
        "ui": ui,
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def _checked_result(result: Dict[str, Any]) -> Dict[str, Any]:
    if not _as_bool(result.get("ok"), False):
        raise ValueError(_text(result.get("error")) or "Reachy rejected the request.")
    return result


def _decode_face_upload(value: Any) -> tuple[bytes, str, str]:
    upload = value if isinstance(value, dict) else {}
    filename = _text(upload.get("filename"))[:160] or "face-photo.jpg"
    content_type = _text(upload.get("content_type")).lower().split(";", 1)[0]
    allowed_types = {"image/jpeg", "image/jpg", "image/png", "image/webp"}
    if content_type and content_type not in allowed_types:
        raise ValueError("Choose a JPEG, PNG, or WebP face photo.")
    encoded = _text(upload.get("data_b64"))
    if not encoded:
        raise ValueError("Choose a face photo or take one with the camera first.")
    if len(encoded) > ((_MAX_SNAPSHOT_BYTES + 2) // 3) * 4 + 8:
        raise ValueError("The face photo must be 8 MB or smaller.")
    try:
        image_bytes = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("The selected face photo could not be read.") from exc
    if not image_bytes:
        raise ValueError("The selected face photo is empty.")
    if len(image_bytes) > _MAX_SNAPSHOT_BYTES:
        raise ValueError("The face photo must be 8 MB or smaller.")
    return image_bytes, filename, content_type or "image/jpeg"


def _enroll_face_image(
    selector: str,
    values: Dict[str, Any],
    redis_client: Any = None,
) -> Dict[str, Any]:
    person_id = _text(values.get("person_id"))
    person = next(
        (row for row in _people_rows(redis_client) if _text(row.get("id")) == person_id),
        None,
    )
    if not person:
        raise ValueError("Choose an existing Tater Person.")
    person_name = _text(person.get("display_name")) or "this person"
    image_bytes, filename, content_type = _decode_face_upload(values.get("face_image"))

    try:
        import face_identity
    except Exception as exc:
        raise ValueError("Tater Face ID is unavailable.") from exc

    inspected = face_identity.recognize_image(
        image_bytes,
        source={
            "kind": "reachy_core_enrollment",
            "selector": selector,
            "filename": filename,
            "content_type": content_type,
        },
        record=False,
        redis_client=redis_client,
    )
    if not isinstance(inspected, dict):
        raise ValueError("Tater Face ID did not return a usable result.")
    faces_detected = int(_as_float(inspected.get("faces_detected"), 0))
    if faces_detected < 1:
        warning = _text(inspected.get("warning"))
        raise ValueError(warning or "No clear face was found. Try a closer, front-facing photo.")
    if faces_detected > 1:
        raise ValueError("More than one face was found. Use a photo containing only the selected person.")

    existing_ids = [_text(item) for item in list(inspected.get("identity_ids") or []) if _text(item)]
    if len(existing_ids) > 1:
        raise ValueError("The photo matched more than one Face ID profile. Use a clearer photo.")
    if existing_ids:
        identities = face_identity.identity_rows(redis_client)
        existing = identities.get(existing_ids[0]) if isinstance(identities, dict) else None
        existing_person_id = _text(existing.get("person_id")) if isinstance(existing, dict) else ""
        if existing_person_id and existing_person_id != person_id:
            existing_name = _text(face_identity.person_name(existing_person_id, redis_client)) or "another Person"
            raise ValueError(
                f"This face is already linked to {existing_name}. Use that Person or review the face in People settings."
            )

    recorded = face_identity.recognize_image(
        image_bytes,
        event_id=f"reachy_enrollment_{uuid.uuid4().hex[:16]}",
        seen_at=datetime.now().astimezone().isoformat(),
        source={
            "kind": "reachy_core_enrollment",
            "selector": selector,
            "filename": filename,
            "content_type": content_type,
        },
        record=True,
        redis_client=redis_client,
    )
    identity_ids = [_text(item) for item in list(recorded.get("identity_ids") or []) if _text(item)]
    if len(identity_ids) != 1:
        raise ValueError("Face ID could not save one clear face from this photo. Try another photo.")
    identity = face_identity.save_profile(
        identity_ids[0],
        person_id=person_id,
        person_link_supplied=True,
        redis_client=redis_client,
    )
    return {
        "ok": True,
        "identity_id": _text(identity.get("id")) or identity_ids[0],
        "person_id": person_id,
        "person_name": person_name,
        "message": f"Face added to {person_name}. Reachy can recognize them on the next tracking session.",
    }


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    action_name = _text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    selector, section = _parse_card_id(body.get("id"))

    if action_name == "reachy_face_enroll":
        if section != "face_enrollment":
            raise ValueError("This Reachy card cannot add a face.")
        return _enroll_face_image(selector, _payload_values(body), redis_client)

    if action_name == "reachy_save_settings":
        if section not in _SUPPORTED_SECTIONS:
            raise ValueError("This Reachy settings card cannot be saved.")
        values = _payload_values(body)
        if not values:
            raise ValueError("No Reachy settings were provided.")
        result = _checked_result(
            _native_request(
                selector,
                "reachy.settings.update",
                {"sections": {section: values}},
            )
        )
        return {
            **result,
            "message": _text(result.get("message")) or "Reachy settings saved.",
        }

    if action_name == "reachy_reset_settings":
        reset_sections: Any = "all" if section == "all" else section
        if reset_sections != "all" and reset_sections not in _SUPPORTED_SECTIONS:
            raise ValueError("This Reachy settings card cannot be reset.")
        result = _checked_result(
            _native_request(
                selector,
                "reachy.settings.reset",
                {"sections": reset_sections},
            )
        )
        return {
            **result,
            "message": _text(result.get("message")) or "Reachy defaults restored.",
        }

    if action_name == "reachy_test_ambient_comment":
        result = _checked_result(
            _native_request(
                selector,
                "reachy.settings.action",
                {"action": "test_ambient_comment"},
            )
        )
        return {
            **result,
            "message": _text(result.get("message")) or "Ambient comment queued.",
        }

    if action_name == "reachy_restart_bridge":
        result = _checked_result(
            _native_request(
                selector,
                "reachy.settings.action",
                {"action": "restart_bridge"},
            )
        )
        return {
            **result,
            "message": _text(result.get("message")) or "Reachy bridge restarting.",
        }

    raise KeyError(f"Unknown Reachy Core action: {action_name or 'missing'}")


def _face_state(selector: str) -> Dict[str, Any]:
    with _FACE_STATE_LOCK:
        state = _FACE_STATES.get(selector)
        if state is None:
            state = {
                "settings": {},
                "next_settings_at": 0.0,
                "next_scan_at": 0.0,
                "scan_active": False,
                "face_scan_complete": False,
                "person_id": "",
                "person_name": "",
                "identity_ids": [],
                "expires_at": 0.0,
                "greeting_pending": False,
                "greeting_active": False,
                "pending_greeting_text": "",
                "pending_greeting_template": "",
                "next_greeting_at": 0.0,
                "last_greeting_template": "",
                "alias_person_id": "",
                "injected_session_id": "",
                "tracking_visible": False,
                "face_currently_visible": False,
                "face_missing_since": 0.0,
            }
            _FACE_STATES[selector] = state
        return state


def _clear_face_person(
    state: Dict[str, Any],
    *,
    reset_tracking_session: bool = True,
) -> None:
    state["person_id"] = ""
    state["person_name"] = ""
    state["identity_ids"] = []
    state["expires_at"] = 0.0
    state["injected_session_id"] = ""
    state["greeting_pending"] = False
    state["greeting_active"] = False
    state["pending_greeting_text"] = ""
    state["pending_greeting_template"] = ""
    state["next_greeting_at"] = 0.0
    if reset_tracking_session:
        state["face_scan_complete"] = False
        state["face_currently_visible"] = False
        state["face_missing_since"] = 0.0


def _face_visible(row: Dict[str, Any], *, now: float) -> bool:
    status = row.get("last_status") if isinstance(row.get("last_status"), dict) else {}
    reachy = status.get("reachy") if isinstance(status.get("reachy"), dict) else {}
    if "face_visible" not in reachy:
        return False
    # last_seen_ts is stamped by Tater, so this remains reliable even when the
    # robot and server clocks are not synchronized.
    last_seen_at = _as_float(row.get("last_seen_ts"), 0.0)
    if last_seen_at > 0.0 and now - last_seen_at > _FACE_PRESENCE_STALE_SECONDS:
        return False
    return _as_bool(reachy.get("face_visible"), False)


def _face_id_ready(row: Dict[str, Any], *, now: float) -> bool:
    if not _face_visible(row, now=now):
        return False
    status = row.get("last_status") if isinstance(row.get("last_status"), dict) else {}
    reachy = status.get("reachy") if isinstance(status.get("reachy"), dict) else {}
    return _as_bool(reachy.get("face_id_ready"), False)


def _client_busy(row: Dict[str, Any]) -> bool:
    voice = row.get("voice") if isinstance(row.get("voice"), dict) else {}
    media = row.get("media_session") if isinstance(row.get("media_session"), dict) else {}
    overlay = row.get("audio_overlay") if isinstance(row.get("audio_overlay"), dict) else {}
    return any(
        (
            _as_bool(voice.get("active"), False),
            _as_bool(media.get("active"), False),
            _as_bool(overlay.get("active"), False),
        )
    )


def _refresh_face_settings(row: Dict[str, Any], state: Dict[str, Any], *, now: float) -> Dict[str, Any]:
    if now < _as_float(state.get("next_settings_at"), 0.0):
        return state.get("settings") if isinstance(state.get("settings"), dict) else {}
    try:
        response = _read_settings(row)
        settings = response.get("settings") if isinstance(response.get("settings"), dict) else {}
        state["settings"] = settings
        state["next_settings_at"] = now + _FACE_SETTINGS_REFRESH_SECONDS
    except Exception as exc:
        state["next_settings_at"] = now + 5.0
        logger.debug("[Reachy Core] Could not refresh Face ID settings for %s: %s", row.get("selector"), exc)
    return state.get("settings") if isinstance(state.get("settings"), dict) else {}


def _decode_snapshot(result: Dict[str, Any]) -> bytes:
    if not _as_bool(result.get("ok"), False):
        return b""
    encoded = _text(result.get("image_base64"))
    if not encoded:
        return b""
    try:
        image = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        return b""
    if not image or len(image) > _MAX_SNAPSHOT_BYTES:
        return b""
    return image


def _capture_face_image(selector: str) -> bytes:
    if not _face_id_ready(_current_client(selector), now=time.time()):
        return b""
    try:
        result = _native_request(
            selector,
            "camera.snapshot",
            {"reason": "reachy_core_face_id"},
            timeout=8.0,
        )
    except Exception as exc:
        logger.debug("[Reachy Core] Camera snapshot failed for %s: %s", selector, exc)
        return b""
    if not _face_id_ready(_current_client(selector), now=time.time()):
        return b""
    return _decode_snapshot(result)


def _quiet_hours_active(idle_settings: Dict[str, Any]) -> bool:
    if not _as_bool(idle_settings.get("quiet_hours_enabled"), False):
        return False

    def minutes(value: Any) -> int | None:
        token = _text(value)
        try:
            hour_text, minute_text = token.split(":", 1)
            hour = int(hour_text)
            minute = int(minute_text)
        except (TypeError, ValueError):
            return None
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None
        return (hour * 60) + minute

    start = minutes(idle_settings.get("quiet_hours_start"))
    end = minutes(idle_settings.get("quiet_hours_end"))
    if start is None or end is None or start == end:
        return False
    local_now = datetime.now().astimezone()
    current = (local_now.hour * 60) + local_now.minute
    if start < end:
        return start <= current < end
    return current >= start or current < end


def _current_client(selector: str) -> Dict[str, Any]:
    with contextlib.suppress(Exception):
        clients = _native_snapshot().get("clients")
        if isinstance(clients, dict):
            row = clients.get(selector)
            if isinstance(row, dict):
                return row
    return {}


def _face_person_active(selector: str, person_id: str) -> bool:
    with _FACE_STATE_LOCK:
        state = _FACE_STATES.get(selector)
        return bool(
            state
            and _as_bool(state.get("tracking_visible"), False)
            and _as_bool(state.get("face_currently_visible"), False)
            and _text(state.get("person_id")) == person_id
        )


def _queue_face_greeting(state: Dict[str, Any], person_name: str) -> None:
    previous = _text(state.get("last_greeting_template"))
    choices = [template for template in _GREETING_TEMPLATES if template != previous]
    template = random.choice(choices or list(_GREETING_TEMPLATES))
    state["greeting_pending"] = True
    state["greeting_active"] = False
    state["pending_greeting_text"] = template.format(name=person_name)
    state["pending_greeting_template"] = template
    state["next_greeting_at"] = 0.0


def _speak_face_greeting(selector: str, person_id: str, text_value: str) -> bool:
    row = _current_client(selector)
    if not row or _client_busy(row) or not _face_person_active(selector, person_id):
        return False
    from tater_voice import native_satellite, voice_pipeline
    from tater_voice.voice_pipeline import backends

    text_value = voice_pipeline._sanitize_spoken_response_text(text_value)[:220].strip()
    if not text_value:
        return False

    async def synthesize() -> tuple[bytes, Dict[str, Any], str, str]:
        return await backends._native_synthesize_text(
            text_value,
            values=voice_pipeline._shared_speech_voice_settings(),
        )

    audio_bytes, audio_format, backend, _note = native_satellite.run_on_runtime_loop(
        synthesize(),
        timeout=180.0,
    )
    if (
        not audio_bytes
        or _client_busy(_current_client(selector))
        or not _face_person_active(selector, person_id)
    ):
        return False
    session_id = f"reachy-face-greeting-{uuid.uuid4().hex}"
    audio_url = voice_pipeline._store_tts_url(selector, session_id, audio_bytes, audio_format)
    if not audio_url:
        return False
    result = _native_command(
        selector,
        "play.url",
        {"url": audio_url, "text": text_value, "tts_kind": "ambient"},
    )
    if _as_bool(result.get("ok"), False):
        logger.info(
            "[Reachy Core] Queued Face ID greeting selector=%s backend=%s person=%s",
            selector,
            backend or "default",
            person_id,
        )
        return True
    return False


def _greet_face_worker(
    selector: str,
    person_id: str,
    text_value: str,
    template: str,
) -> None:
    spoken = False
    try:
        spoken = _speak_face_greeting(selector, person_id, text_value)
    except Exception as exc:
        logger.debug("[Reachy Core] Face ID greeting failed for %s: %s", selector, exc)
    finally:
        with _FACE_STATE_LOCK:
            state = _FACE_STATES.get(selector)
            if (
                not state
                or _text(state.get("person_id")) != person_id
                or _text(state.get("pending_greeting_template")) != template
            ):
                return
            state["greeting_active"] = False
            if spoken:
                state["greeting_pending"] = False
                state["pending_greeting_text"] = ""
                state["pending_greeting_template"] = ""
                state["next_greeting_at"] = 0.0
                state["last_greeting_template"] = template
            else:
                state["next_greeting_at"] = time.time() + 5.0


def _face_result_summary(
    result: Dict[str, Any],
) -> tuple[List[str], Dict[str, Dict[str, Any]], int]:
    identity_ids: List[str] = []
    for identity_id in result.get("identity_ids") or []:
        token = _text(identity_id)
        if token and token not in identity_ids:
            identity_ids.append(token)
    people_found: Dict[str, Dict[str, Any]] = {}
    person_ids = list(result.get("person_ids") or [])
    names = list(result.get("people") or [])
    for index, person_id in enumerate(person_ids):
        token = _text(person_id)
        name = _text(names[index] if index < len(names) else "")
        if token and name:
            people_found[token] = {"person_id": token, "person_name": name}
    return (
        identity_ids,
        people_found,
        max(0, int(_as_float(result.get("faces_detected"), 0.0))),
    )


def _raise_for_face_result(result: Dict[str, Any]) -> None:
    status = _text(result.get("status")).lower()
    if status in {"disabled", "not_ready", "error"}:
        raise RuntimeError(
            _text(result.get("warning")) or f"Face ID returned {status or 'an error'}"
        )


def _recognize_face_worker(selector: str, device_name: str) -> None:
    with _FACE_STATE_LOCK:
        state = _face_state(selector)
        settings = dict(state.get("settings") or {})
    face_settings = settings.get("face_id") if isinstance(settings.get("face_id"), dict) else {}
    idle_settings = settings.get("idle_life") if isinstance(settings.get("idle_life"), dict) else {}
    people_found: Dict[str, Dict[str, Any]] = {}
    identity_ids: List[str] = []
    faces_detected = 0
    analysis_completed = False
    error = ""
    storage_error = ""
    try:
        image = _capture_face_image(selector)
        if not image:
            raise RuntimeError("Reachy did not return a camera frame")
        import face_identity

        event_id = f"reachy_core_{uuid.uuid4().hex}"
        source = {
            "owner": "reachy_core",
            "selector": selector,
            "device_name": device_name,
        }
        result = face_identity.recognize_image(
            image,
            event_id=event_id,
            source=source,
            record=False,
        )
        _raise_for_face_result(result)
        identity_ids, people_found, faces_detected = _face_result_summary(result)

        # Inspect read-only first so an opt-in recording request never stores a
        # multi-person scene. Recording repeats the analysis only once for the
        # single good snapshot selected for this tracking session.
        if _as_bool(face_settings.get("store_photos"), False) and faces_detected == 1:
            try:
                recorded = face_identity.recognize_image(
                    image,
                    event_id=event_id,
                    source={**source, "kind": "reachy_face_id_observation"},
                    record=True,
                )
                _raise_for_face_result(recorded)
                stored_ids, stored_people, stored_faces = _face_result_summary(recorded)
                if stored_faces != 1:
                    raise RuntimeError("Face ID photo storage did not return exactly one face")
                identity_ids = stored_ids or identity_ids
                people_found = stored_people or people_found
            except Exception as exc:
                storage_error = _text(exc)

        analysis_completed = True
    except Exception as exc:
        error = _text(exc)

    now = time.time()
    with _FACE_STATE_LOCK:
        state = _face_state(selector)
        state["scan_active"] = False
        state["next_scan_at"] = now + _FACE_SCAN_RETRY_SECONDS
        settings = state.get("settings") if isinstance(state.get("settings"), dict) else {}
        face_settings = settings.get("face_id") if isinstance(settings.get("face_id"), dict) else {}
        idle_settings = settings.get("idle_life") if isinstance(settings.get("idle_life"), dict) else {}
        row = _current_client(selector)
        if (
            not row
            or not _as_bool(face_settings.get("enabled"), False)
            or not _as_bool(state.get("tracking_visible"), False)
        ):
            _clear_face_person(state)
        elif analysis_completed and len(people_found) == 1 and faces_detected <= 1:
            state["face_scan_complete"] = True
            person = next(iter(people_found.values()))
            person_id = _text(person.get("person_id"))
            person_name = _text(person.get("person_name"))
            changed = person_id != _text(state.get("person_id"))
            state["person_id"] = person_id
            state["person_name"] = person_name
            state["identity_ids"] = list(identity_ids)
            state["expires_at"] = now + max(30.0, _as_float(face_settings.get("context_ttl_seconds"), 300.0))
            if changed:
                state["injected_session_id"] = ""
                if (
                    _as_bool(face_settings.get("greetings_enabled"), True)
                    and not _quiet_hours_active(idle_settings)
                ):
                    _queue_face_greeting(state, person_name)
        elif analysis_completed:
            _clear_face_person(state, reset_tracking_session=False)
            state["face_scan_complete"] = True

    if error:
        logger.debug("[Reachy Core] Face ID scan failed for %s: %s", selector, error)
    if storage_error:
        logger.warning(
            "[Reachy Core] Face ID matched but its optional photo could not be stored selector=%s error=%s",
            selector,
            storage_error,
        )


def _ensure_voice_alias(selector: str, state: Dict[str, Any]) -> str:
    person_id = _text(state.get("person_id"))
    person_name = _text(state.get("person_name"))
    if not person_id or not person_name:
        return ""
    external_id = f"reachy-face:{selector}:{person_id}"
    if _text(state.get("alias_person_id")) == person_id:
        return external_id
    import people

    people.attach_alias(
        person_id=person_id,
        platform="voice_core",
        external_id=external_id,
        label=person_name,
        kind="face_id",
    )
    state["alias_person_id"] = person_id
    return external_id


def _inject_face_identity(selector: str, session_id: str, state: Dict[str, Any]) -> None:
    if not session_id or _text(state.get("injected_session_id")) == session_id:
        return
    try:
        speaker_id = _ensure_voice_alias(selector, state)
    except Exception as exc:
        logger.debug("[Reachy Core] Could not link Face ID to People for %s: %s", selector, exc)
        return
    speaker_name = _text(state.get("person_name"))
    if not speaker_id or not speaker_name:
        return

    async def apply_identity() -> bool:
        from tater_voice import voice_pipeline

        runtime = voice_pipeline._selector_runtime(selector)
        lock = runtime.get("lock")
        if not hasattr(lock, "__aenter__"):
            return False
        async with lock:
            session = runtime.get("session")
            if session is None or _text(getattr(session, "session_id", "")) != session_id:
                return False
            session.speaker_id = speaker_id
            session.speaker_name = speaker_name
            session.speaker_score = 1.0
            session.speaker_match_reason = "reachy_face"
            if not isinstance(session.context, dict):
                session.context = {}
            session.context["speaker_id"] = speaker_id
            session.context["speaker_name"] = speaker_name
            session.context["speaker_score"] = 1.0
            return True

    from tater_voice import native_satellite

    try:
        applied = native_satellite.run_on_runtime_loop(apply_identity(), timeout=3.0)
    except Exception as exc:
        logger.debug("[Reachy Core] Could not apply Face ID voice context for %s: %s", selector, exc)
        return
    if applied:
        state["injected_session_id"] = session_id
        logger.info("[Reachy Core] Applied Face ID voice context selector=%s person=%s", selector, speaker_name)


def _face_id_tick() -> None:
    now = time.time()
    reachys = _connected_reachys()
    connected = {_text(row.get("selector")) for row in reachys}
    with _FACE_STATE_LOCK:
        for selector in list(_FACE_STATES):
            if selector not in connected:
                _FACE_STATES.pop(selector, None)

    for row in reachys:
        selector = _text(row.get("selector"))
        if not selector:
            continue
        with _FACE_STATE_LOCK:
            state = _face_state(selector)
            settings = _refresh_face_settings(row, state, now=now)
            face_settings = settings.get("face_id") if isinstance(settings.get("face_id"), dict) else {}
            idle_settings = settings.get("idle_life") if isinstance(settings.get("idle_life"), dict) else {}
            reachy_settings = settings.get("reachy") if isinstance(settings.get("reachy"), dict) else {}
            enabled = all(
                (
                    _as_bool(face_settings.get("enabled"), False),
                    _as_bool(idle_settings.get("enabled"), False),
                    _as_bool(reachy_settings.get("allow_vision_snapshots"), False),
                    _as_bool(row.get("capabilities", {}).get("camera_snapshot"), False),
                )
            )
            visible = enabled and _face_visible(row, now=now)
            state["face_currently_visible"] = visible
            if not enabled:
                _clear_face_person(state)
                state["tracking_visible"] = False
                state["next_scan_at"] = 0.0
                continue
            if not visible:
                if _as_bool(state.get("tracking_visible"), False):
                    missing_since = _as_float(state.get("face_missing_since"), 0.0)
                    if missing_since <= 0.0:
                        state["face_missing_since"] = now
                    elif now - missing_since >= _FACE_SESSION_LOSS_SECONDS:
                        logger.info(
                            "[Reachy Core] Ended Face ID tracking session after %.1fs without a face selector=%s",
                            now - missing_since,
                            selector,
                        )
                        _clear_face_person(state)
                        state["tracking_visible"] = False
                        state["next_scan_at"] = 0.0
                else:
                    _clear_face_person(state)
                    state["tracking_visible"] = False
                    state["next_scan_at"] = 0.0
                continue
            state["face_missing_since"] = 0.0
            if not _as_bool(state.get("tracking_visible"), False):
                state["tracking_visible"] = True
                state["next_scan_at"] = 0.0
                state["face_scan_complete"] = False
            if _text(state.get("person_id")):
                # Keep one successful match for the full continuous tracking
                # session. The satellite's fresh face-visible signal owns the
                # boundary; losing it clears the match and reacquisition scans
                # immediately. Refresh the safety expiry while status is fresh.
                state["expires_at"] = now + max(
                    30.0,
                    _as_float(face_settings.get("context_ttl_seconds"), 300.0),
                )

            voice = row.get("voice") if isinstance(row.get("voice"), dict) else {}
            if (
                _as_bool(face_settings.get("conversation_identity_enabled"), True)
                and _as_bool(voice.get("active"), False)
                and _text(state.get("person_id"))
            ):
                _inject_face_identity(selector, _text(voice.get("session_id")), state)

            greetings_enabled = _as_bool(face_settings.get("greetings_enabled"), True)
            if not greetings_enabled:
                state["greeting_pending"] = False
                state["pending_greeting_text"] = ""
                state["pending_greeting_template"] = ""
            greeting_text = _text(state.get("pending_greeting_text"))
            greeting_template = _text(state.get("pending_greeting_template"))
            if (
                greetings_enabled
                and _text(state.get("person_id"))
                and _as_bool(state.get("greeting_pending"), False)
                and not _as_bool(state.get("greeting_active"), False)
                and greeting_text
                and greeting_template
                and now >= _as_float(state.get("next_greeting_at"), 0.0)
                and not _quiet_hours_active(idle_settings)
                and not _client_busy(row)
            ):
                state["greeting_active"] = True
                greeting_worker = threading.Thread(
                    target=_greet_face_worker,
                    args=(
                        selector,
                        _text(state.get("person_id")),
                        greeting_text,
                        greeting_template,
                    ),
                    name=f"reachy-face-greeting-{selector}",
                    daemon=True,
                )
                greeting_worker.start()

            if (
                _face_id_ready(row, now=now)
                and not _as_bool(state.get("face_scan_complete"), False)
                and not _text(state.get("person_id"))
                and not _as_bool(state.get("scan_active"), False)
                and now >= _as_float(state.get("next_scan_at"), 0.0)
                and not _client_busy(row)
            ):
                state["scan_active"] = True
                worker = threading.Thread(
                    target=_recognize_face_worker,
                    args=(selector, _device_name(row)),
                    name=f"reachy-face-id-{selector}",
                    daemon=True,
                )
                worker.start()


def run(stop_event=None) -> None:
    """Manage Reachy settings plus low-latency Face ID presence and identity."""
    logger.info("[Reachy Core] Started; waiting for compatible Reachy satellites.")
    try:
        while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
            try:
                _face_id_tick()
            except Exception:
                logger.exception("[Reachy Core] Face ID background tick failed")
            wait = getattr(stop_event, "wait", None) if stop_event is not None else None
            if callable(wait):
                wait(_FACE_POLL_SECONDS)
            else:
                time.sleep(_FACE_POLL_SECONDS)
    finally:
        with _FACE_STATE_LOCK:
            _FACE_STATES.clear()
        logger.info("[Reachy Core] Stopped.")
