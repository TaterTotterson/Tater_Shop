from __future__ import annotations

import asyncio
import base64
import binascii
from typing import Any, Dict, List, Tuple

from verba_base import ToolVerba
from verba_result import action_failure, action_success


MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024


class ReachyVisionPlugin(ToolVerba):
    name = "reachy_vision"
    verba_name = "Reachy Vision"
    pretty_name = "Reachy Vision"
    version = "1.0.0"
    min_tater_version = "98.4"
    settings_category = "Reachy Vision"
    description = (
        "Ask a connected Reachy Mini to take one fresh camera snapshot, then use Tater's configured vision model "
        "to answer appearance, outfit, object, and what-do-you-see questions."
    )
    verba_dec = (
        "Use when someone asks Tater to look at them or something in front of Reachy, including 'how do I look?', "
        "'do you like what I'm wearing?', 'what am I wearing?', 'look at this', or 'what do you see?'. "
        "Do not use for named home-security cameras; use Camera Control for those."
    )
    when_to_use = verba_dec
    how_to_use = (
        "Pass the user's complete visual question unchanged in query. The Verba chooses a connected Reachy, "
        "preferring the requesting Reachy or one in the same room, captures one still image, and analyzes it."
    )
    platforms = ["voice_core"]
    tags = ["reachy", "camera", "snapshot", "vision", "appearance", "outfit"]
    routing_keywords = [
        "how do i look",
        "do i look good",
        "what am i wearing",
        "do you like what i'm wearing",
        "do you like my outfit",
        "look at me",
        "look at this",
        "what do you see",
        "can you see me",
        "how does this look",
        "what is in front of you",
    ]
    usage = '{"function":"reachy_vision","arguments":{"query":"How do I look?"}}'
    example_calls = [
        '{"function":"reachy_vision","arguments":{"query":"How do I look?"}}',
        '{"function":"reachy_vision","arguments":{"query":"Do you like what I am wearing?"}}',
        '{"function":"reachy_vision","arguments":{"query":"Look at this and tell me what it is."}}',
        '{"function":"reachy_vision","arguments":{"query":"What do you see in front of you?"}}',
    ]
    common_needs = ["A connected Reachy with vision snapshots enabled."]
    missing_info_prompts = ["Please stand where the selected Reachy can see you and ask again."]
    waiting_prompt_template = (
        "Write a short, friendly message saying Reachy is taking a quick look now. "
        "Do not claim to have seen anything yet. Only output that message."
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
            return {"query": cls._text(args)} if cls._text(args) else {}
        payload = dict(args)
        nested = payload.get("arguments")
        if isinstance(nested, dict):
            merged = dict(nested)
            merged.update({key: value for key, value in payload.items() if key != "arguments"})
            return merged
        return payload

    @classmethod
    def _camera_reachys(cls, status: Any) -> List[Dict[str, Any]]:
        clients = status.get("clients") if isinstance(status, dict) else {}
        if not isinstance(clients, dict):
            return []
        candidates: List[Dict[str, Any]] = []
        for selector, raw in clients.items():
            if not isinstance(raw, dict) or not bool(raw.get("connected")):
                continue
            capabilities = raw.get("capabilities") if isinstance(raw.get("capabilities"), dict) else {}
            board = cls._text(raw.get("board")).lower()
            if not bool(capabilities.get("camera_snapshot")) or not board.startswith("reachy"):
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
    ) -> Tuple[Dict[str, Any] | None, str]:
        if not candidates:
            return None, "none_available"

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
    def _decode_snapshot(result: Any) -> bytes | None:
        if not isinstance(result, dict) or not bool(result.get("ok")):
            return None
        encoded = str(result.get("image_base64") or "").strip()
        if not encoded:
            return None
        try:
            image = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            return None
        if not image or len(image) > MAX_SNAPSHOT_BYTES:
            return None
        return image

    @staticmethod
    def _vision_prompt(query: str, reachy_name: str) -> str:
        return (
            f"This is one current still image from {reachy_name or 'Reachy Mini'}. "
            f"Answer the user's visual request directly: {query}\n"
            "For appearance or outfit questions, give a warm, honest, and useful opinion based only on what is "
            "visible. For object questions, describe or explain the visible object. Do not identify people or infer "
            "age, ethnicity, health, disability, religion, sexuality, or other sensitive traits. If the subject is "
            "not visible or the framing is poor, say that clearly. Keep a voice response concise and natural."
        )

    @staticmethod
    def _default_reachy_selector() -> str:
        try:
            from helpers import redis_client

            settings = (
                redis_client.hgetall("verba_settings:Reachy Vision")
                or redis_client.hgetall("verba_settings: Reachy Vision")
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
        query = self._text(
            payload.get("query")
            or payload.get("text")
            or origin.get("request_text")
        )
        if not query:
            return action_failure(
                code="missing_query",
                message="Please provide the visual question for Reachy.",
                needs=["Ask what Reachy should look at or comment on."],
                say_hint="Ask what the user wants Reachy to look at.",
            )

        if self._text(origin.get("platform")).lower() not in {"voice_core", "homeassistant"}:
            return action_failure(
                code="voice_satellite_required",
                message="Reachy Vision only accepts requests from the voice satellite pipeline.",
                say_hint="Explain that this feature must be requested through a voice satellite.",
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

        reachy, selection_reason = self._select_reachy(
            self._camera_reachys(status),
            origin,
            self._default_reachy_selector(),
        )
        if reachy is None:
            return action_failure(
                code="no_reachy_camera",
                message="No connected Reachy has vision snapshots enabled.",
                needs=["Connect a Reachy and enable Allow vision snapshots in its app settings."],
                say_hint="Explain that no Reachy camera is available right now.",
            )

        selector = self._text(reachy.get("selector"))
        reachy_name = self._text(reachy.get("device_name") or reachy.get("name") or selector)
        try:
            snapshot_result = await native_satellite.send_request(
                selector,
                "camera.snapshot",
                {"reason": "explicit_verba_request"},
                timeout_s=8.0,
            )
        except Exception as exc:
            return action_failure(
                code="reachy_snapshot_failed",
                message=f"{reachy_name or 'Reachy'} could not take a snapshot: {exc}",
                say_hint="Explain that Reachy could not take the picture and suggest trying again.",
            )

        image = self._decode_snapshot(snapshot_result)
        if image is None:
            error = self._text(snapshot_result.get("error")) if isinstance(snapshot_result, dict) else ""
            return action_failure(
                code="invalid_reachy_snapshot",
                message=error or f"{reachy_name or 'Reachy'} returned no usable camera image.",
                say_hint="Explain that Reachy's camera did not return a usable image.",
            )

        try:
            from kernel_tools import image_describe

            vision_result = await asyncio.to_thread(
                image_describe,
                prompt=self._vision_prompt(query, reachy_name),
                image_ref={
                    "type": "image",
                    "name": "reachy-snapshot.jpg",
                    "mimetype": "image/jpeg",
                    "bytes": image,
                },
            )
        except Exception as exc:
            return action_failure(
                code="reachy_vision_failed",
                message=f"Tater could not analyze Reachy's snapshot: {exc}",
                say_hint="Explain that the picture was captured but vision analysis failed.",
            )

        if not isinstance(vision_result, dict) or not bool(vision_result.get("ok")):
            error = vision_result.get("error") if isinstance(vision_result, dict) else {}
            error_message = self._text(error.get("message")) if isinstance(error, dict) else ""
            return action_failure(
                code="reachy_vision_failed",
                message=error_message or "Tater's vision model could not analyze Reachy's snapshot.",
                say_hint="Explain that the picture was captured but vision analysis failed.",
            )

        vision_data = vision_result.get("data") if isinstance(vision_result.get("data"), dict) else {}
        description = self._text(
            vision_data.get("description")
            or vision_data.get("text")
            or vision_result.get("summary_for_user")
        )
        if not description:
            return action_failure(
                code="empty_reachy_description",
                message="Tater's vision model returned no description.",
                say_hint="Explain that the image could not be described and suggest trying again.",
            )

        return action_success(
            facts={
                "reachy_selector": selector,
                "reachy_name": reachy_name,
                "selection_reason": selection_reason,
                "snapshot_bytes": len(image),
            },
            data={
                "description": description,
                "reachy_selector": selector,
                "reachy_name": reachy_name,
                "model": self._text(vision_data.get("model")),
            },
            summary_for_user=description,
            say_hint=(
                "Answer the user's visual question directly using only the returned description. "
                "Do not add details that the vision model did not report."
            ),
        )


verba = ReachyVisionPlugin()
