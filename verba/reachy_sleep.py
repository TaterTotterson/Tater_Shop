from __future__ import annotations

from typing import Any, Dict, List, Tuple

from verba_base import ToolVerba
from verba_result import action_failure, action_success


class ReachySleepPlugin(ToolVerba):
    name = "reachy_sleep"
    verba_name = "Reachy Sleep"
    pretty_name = "Reachy Sleep"
    version = "1.0.0"
    min_tater_version = "98.4"
    settings_category = "Reachy Sleep"
    description = (
        "Put a connected Reachy Mini into its sleep pose until that Reachy hears its configured wake word."
    )
    verba_dec = (
        "Use only when someone explicitly tells you to go to sleep, go to bed, or take a nap. Do not use merely "
        "because the user says they are going to sleep, asks a question about sleep, or asks Reachy to be quiet"
    )
    when_to_use = verba_dec
    how_to_use = (
        "Call without asking for confirmation when the command is clearly directed at Reachy. The Verba chooses a "
        "connected Reachy, preferring the requesting Reachy or one in the same room, and puts it to sleep until its "
        "local wake word is heard."
    )
    platforms = ["voice_core"]
    tags = ["reachy", "robot", "sleep", "wake-word", "motion"]
    routing_keywords = [
        "reachy go to sleep",
        "reachy go to bed",
        "put reachy to sleep",
        "time for bed reachy",
        "good night reachy",
        "take a nap reachy",
        "robot go to sleep",
    ]
    usage = '{"function":"reachy_sleep","arguments":{}}'
    example_calls = [
        '{"function":"reachy_sleep","arguments":{}}',
        '{"function":"reachy_sleep","arguments":{"target":"office-reachy"}}',
    ]
    common_needs = ["A connected Reachy running a sleep-capable Reachy Tater Satellite app."]
    missing_info_prompts = ["Make sure the Reachy Tater Satellite app is connected, then try again."]
    waiting_prompt_template = (
        "Write one very short, gentle acknowledgement that Reachy is getting ready to sleep. "
        "Do not say it is asleep until the tool succeeds."
    )
    required_settings = {
        "DEFAULT_REACHY_SELECTOR": {
            "label": "Default Reachy",
            "type": "text",
            "default": "",
            "description": (
                "Optional native satellite selector or Reachy device id to prefer when the asking satellite has no "
                "Reachy in the same room."
            ),
        }
    }

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip()

    @classmethod
    def _normalize_args(cls, args: Any) -> Dict[str, Any]:
        if not isinstance(args, dict):
            return {}
        payload = dict(args)
        nested = payload.get("arguments")
        if isinstance(nested, dict):
            merged = dict(nested)
            merged.update({key: value for key, value in payload.items() if key != "arguments"})
            return merged
        return payload

    @classmethod
    def _sleep_reachys(cls, status: Any) -> List[Dict[str, Any]]:
        clients = status.get("clients") if isinstance(status, dict) else {}
        if not isinstance(clients, dict):
            return []
        candidates: List[Dict[str, Any]] = []
        for selector, raw in clients.items():
            if not isinstance(raw, dict) or not bool(raw.get("connected")):
                continue
            capabilities = raw.get("capabilities") if isinstance(raw.get("capabilities"), dict) else {}
            board = cls._text(raw.get("board")).lower()
            if not bool(capabilities.get("sleep_until_wake")) or not board.startswith("reachy"):
                continue
            row = dict(raw)
            row["selector"] = cls._text(raw.get("selector") or selector)
            candidates.append(row)
        return sorted(
            candidates,
            key=lambda row: (
                -float(row.get("last_seen_ts") or 0.0),
                cls._text(row.get("selector")).casefold(),
            ),
        )

    @classmethod
    def _matches_selector(cls, candidate: Dict[str, Any], value: Any) -> bool:
        token = cls._text(value).casefold()
        if not token:
            return False
        return token in {
            cls._text(candidate.get("selector")).casefold(),
            cls._text(candidate.get("device_id")).casefold(),
        }

    @classmethod
    def _select_reachy(
        cls,
        candidates: List[Dict[str, Any]],
        origin: Dict[str, Any],
        default_selector: str = "",
        requested_target: str = "",
    ) -> Tuple[Dict[str, Any] | None, str]:
        if not candidates:
            return None, "none_available"

        if requested_target:
            for candidate in candidates:
                if cls._matches_selector(candidate, requested_target):
                    return candidate, "requested_target"

        origin_device = cls._text(origin.get("device_id"))
        for candidate in candidates:
            if cls._matches_selector(candidate, origin_device):
                return candidate, "requesting_reachy"

        origin_room = cls._text(
            origin.get("area_name") or origin.get("room_name") or origin.get("room")
        ).casefold()
        if origin_room:
            for candidate in candidates:
                candidate_room = cls._text(candidate.get("room")).casefold()
                if candidate_room and candidate_room == origin_room:
                    return candidate, "same_room"

        for candidate in candidates:
            if cls._matches_selector(candidate, default_selector):
                return candidate, "configured_default"

        return candidates[0], "most_recently_seen"

    @staticmethod
    def _default_reachy_selector() -> str:
        try:
            from helpers import redis_client

            settings = (
                redis_client.hgetall("verba_settings:Reachy Sleep")
                or redis_client.hgetall("verba_settings: Reachy Sleep")
                or {}
            )
            for key in ("DEFAULT_REACHY_SELECTOR", b"DEFAULT_REACHY_SELECTOR"):
                value = settings.get(key)
                if isinstance(value, (bytes, bytearray)):
                    value = value.decode("utf-8", "ignore")
                if str(value or "").strip():
                    return str(value).strip()
        except Exception:
            pass
        return ""

    async def handle_voice_core(self, args=None, llm_client=None, context=None):
        del llm_client, context
        payload = self._normalize_args(args or {})
        origin = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}

        if self._text(origin.get("platform")).lower() not in {"voice_core", "homeassistant"}:
            return action_failure(
                code="voice_satellite_required",
                message="Reachy Sleep only accepts requests from the voice satellite pipeline.",
                say_hint="Explain that this command must be requested through a voice satellite.",
            )

        try:
            from tater_voice import native_satellite

            status = await native_satellite.status()
        except Exception as exc:
            return action_failure(
                code="reachy_discovery_failed",
                message=f"Could not find connected Reachys: {exc}",
                say_hint="Explain that Tater could not check for a connected Reachy.",
            )

        candidates = self._sleep_reachys(status)
        requested_target = self._text(payload.get("target") or payload.get("reachy"))
        reachy, selection_reason = self._select_reachy(
            candidates,
            origin,
            self._default_reachy_selector(),
            requested_target,
        )
        if reachy is None:
            return action_failure(
                code="no_sleep_capable_reachy",
                message="No connected Reachy supports sleep-until-wake commands.",
                needs=["Connect a Reachy running the latest Reachy Tater Satellite app."],
                say_hint="Explain that no compatible Reachy is connected right now.",
            )

        selector = self._text(reachy.get("selector"))
        reachy_name = self._text(reachy.get("device_name") or reachy.get("name") or selector)
        try:
            result = await native_satellite.send_request(
                selector,
                "reachy.sleep",
                {"reason": "explicit_verba_request"},
                timeout_s=8.0,
            )
        except Exception as exc:
            return action_failure(
                code="reachy_sleep_failed",
                message=f"{reachy_name or 'Reachy'} could not go to sleep: {exc}",
                say_hint="Explain that Reachy could not enter its sleep pose and suggest trying again.",
            )

        if not isinstance(result, dict) or not bool(result.get("ok")):
            error = self._text(result.get("error")) if isinstance(result, dict) else ""
            return action_failure(
                code="reachy_sleep_failed",
                message=error or f"{reachy_name or 'Reachy'} did not confirm its sleep state.",
                say_hint="Explain that Reachy could not enter its sleep pose and suggest trying again.",
            )

        wake_word = self._text(result.get("wake_word"))
        wake_instruction = f'Say "{wake_word}" to wake it.' if wake_word else "Use its wake word to wake it."
        summary = f"{reachy_name or 'Reachy'} is going to sleep. {wake_instruction}"
        return action_success(
            facts={
                "reachy_selector": selector,
                "reachy_name": reachy_name,
                "selection_reason": selection_reason,
                "wake_word": wake_word,
                "already_sleeping": bool(result.get("already_sleeping")),
            },
            data={
                "reachy_selector": selector,
                "reachy_name": reachy_name,
                "wake_word": wake_word,
                "sleep_until_wake": True,
            },
            summary_for_user=summary,
            say_hint=(
                "Briefly confirm that Reachy is going to sleep and mention its returned wake word. "
                "Do not add another movement or camera action."
            ),
        )


verba = ReachySleepPlugin()
