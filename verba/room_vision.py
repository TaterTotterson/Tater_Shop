from __future__ import annotations

import asyncio
import base64
import binascii
from typing import Any, Dict, List, Tuple

from verba_base import ToolVerba
from verba_result import action_failure, action_success


MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024


class RoomVisionPlugin(ToolVerba):
    name = "room_vision"
    verba_name = "Room Vision"
    pretty_name = "Room Vision"
    version = "1.0.1"
    min_tater_version = "98.4"
    settings_category = "Room Vision"
    description = (
        "Use an Echo Show or another camera-capable Tater satellite in the asking satellite's room to answer "
        "current visual questions about objects, appearance, outfits, and clothing for the weather."
    )
    verba_dec = (
        "Ask Tater what it sees using a camera-equipped satellite in your room. Identify objects, get outfit "
        "feedback, and check whether your clothing suits the current weather."
    )
    when_to_use = (
        "Use when someone asks a voice satellite a current visual question such as 'what is this?', "
        "'what do you see?', 'how do my clothes look?', 'does this outfit match?', or 'am I dressed warmly "
        "enough outside?'. Select only the asking camera satellite or a camera satellite assigned to the same "
        "room. Do not use for a named security camera or for a Reachy Mini; those have dedicated verbas."
    )
    how_to_use = (
        "Pass the user's complete visual question unchanged in query. Room Vision captures one fresh, ephemeral "
        "still image from the asking Show or a camera-capable satellite in the same room and asks Tater's configured "
        "vision model to answer it."
    )
    platforms = ["voice_core"]
    tags = ["room", "camera", "snapshot", "vision", "appearance", "outfit", "echo-show"]
    routing_keywords = [
        "what is this",
        "what am i holding",
        "look at this",
        "what do you see",
        "can you see me",
        "how do i look",
        "how do my clothes look",
        "how does my outfit look",
        "does this outfit match",
        "am i dressed warm enough",
        "am i dressed warmly enough",
        "what color is this",
    ]
    usage = '{"function":"room_vision","arguments":{"query":"What is this?"}}'
    example_calls = [
        '{"function":"room_vision","arguments":{"query":"What is this?"}}',
        '{"function":"room_vision","arguments":{"query":"How do my clothes look?"}}',
        '{"function":"room_vision","arguments":{"query":"Am I dressed warmly enough outside?"}}',
        '{"function":"room_vision","arguments":{"query":"Does this outfit match?"}}',
    ]
    common_needs = ["A connected camera-capable Tater satellite in the same room."]
    missing_info_prompts = ["Please stand where the room's Echo Show can see you and ask again."]
    waiting_prompt_template = (
        "Write a short, friendly message saying Tater is taking a quick look with the room camera. "
        "Do not claim to have seen anything yet. Only output that message."
    )
    required_settings: Dict[str, Dict[str, Any]] = {}

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
    def _camera_satellites(cls, status: Any) -> List[Dict[str, Any]]:
        clients = status.get("clients") if isinstance(status, dict) else {}
        if not isinstance(clients, dict):
            return []
        candidates: List[Dict[str, Any]] = []
        for selector, raw in clients.items():
            if not isinstance(raw, dict) or not bool(raw.get("connected")):
                continue
            capabilities = raw.get("capabilities") if isinstance(raw.get("capabilities"), dict) else {}
            board = cls._text(raw.get("board")).lower()
            if not bool(capabilities.get("camera_snapshot")) or board.startswith("reachy"):
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
    def _matches_origin(cls, candidate: Dict[str, Any], origin_device: Any) -> bool:
        token = cls._text(origin_device).casefold()
        if not token:
            return False
        values = {
            cls._text(candidate.get("selector")).casefold(),
            cls._text(candidate.get("device_id")).casefold(),
        }
        if token.startswith("native:"):
            values.add("native:" + cls._text(candidate.get("device_id")).casefold())
        return token in values

    @classmethod
    def _select_camera(
        cls,
        candidates: List[Dict[str, Any]],
        origin: Dict[str, Any],
    ) -> Tuple[Dict[str, Any] | None, str]:
        origin_device = origin.get("device_id") or origin.get("selector")
        for candidate in candidates:
            if cls._matches_origin(candidate, origin_device):
                return candidate, "requesting_camera_satellite"

        origin_room = cls._text(
            origin.get("area_name") or origin.get("room_name") or origin.get("room")
        ).casefold()
        if origin_room:
            for candidate in candidates:
                candidate_room = cls._text(
                    candidate.get("room") or candidate.get("area_name") or candidate.get("room_name")
                ).casefold()
                if candidate_room and candidate_room == origin_room:
                    return candidate, "same_room"
        return None, "no_same_room_camera"

    @staticmethod
    def _decode_snapshot(result: Any) -> Tuple[bytes | None, str]:
        if not isinstance(result, dict) or not bool(result.get("ok")):
            return None, ""
        encoded = str(result.get("image_base64") or "").strip()
        if not encoded:
            return None, ""
        try:
            image = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            return None, ""
        if not image or len(image) > MAX_SNAPSHOT_BYTES:
            return None, ""
        content_type = str(result.get("content_type") or "image/jpeg").strip().lower()
        if not content_type.startswith("image/"):
            content_type = "image/jpeg"
        return image, content_type

    @classmethod
    def _weather_context(cls) -> str:
        try:
            from tater_voice import display_feed

            summary = display_feed.build_weather_summary(selector="")
        except Exception:
            return ""
        if not isinstance(summary, dict) or not bool(summary.get("available")):
            return ""
        details = []
        for label, key in (
            ("temperature", "temperature_text"),
            ("feels like", "feels_like_text"),
            ("condition", "condition"),
            ("wind", "wind_text"),
        ):
            value = cls._text(summary.get(key))
            if value:
                details.append(f"{label}: {value}")
        return ", ".join(details)

    @staticmethod
    def _vision_prompt(query: str, camera_name: str, room: str, weather_context: str) -> str:
        weather_note = (
            f" Current outdoor conditions reported by Tater are: {weather_context}. Use them only when relevant, "
            "especially when judging whether visible clothing is suitable for outside."
            if weather_context
            else " If the question depends on outdoor conditions, explain that you can judge the visible layers but "
            "do not have current outdoor weather data."
        )
        return (
            f"This is one current still image from {camera_name or 'a Tater room camera'}"
            f"{f' in {room}' if room else ''}. Answer the user's exact visual request directly: {query}\n"
            "For appearance or outfit questions, give a warm, honest, practical opinion based only on what is "
            "visible. For objects, identify or explain only what the image supports. Do not identify people or infer "
            "age, ethnicity, health, disability, religion, sexuality, or other sensitive traits. If the subject is "
            "not visible or the framing is poor, say so clearly. Keep the spoken response concise and natural."
            + weather_note
        )

    async def handle_voice_core(self, args=None, llm_client=None, context=None):
        del llm_client, context
        payload = self._normalize_args(args or {})
        origin = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}
        query = self._text(payload.get("query") or payload.get("text") or origin.get("request_text"))
        if not query:
            return action_failure(
                code="missing_query",
                message="Please provide the visual question for Room Vision.",
                needs=["Ask what Tater should look at or comment on."],
                say_hint="Ask what the user wants the room camera to look at.",
            )
        origin_platform = self._text(origin.get("platform")).lower()
        origin_entrypoint = self._text(origin.get("entrypoint")).lower()
        if origin_platform != "voice_core" and not (
            origin_platform == "homeassistant" and origin_entrypoint == "voice_core"
        ):
            return action_failure(
                code="voice_satellite_required",
                message="Room Vision only accepts requests from the voice satellite pipeline.",
                say_hint="Explain that this feature must be requested through a voice satellite.",
            )

        try:
            from tater_voice import native_satellite

            status = await native_satellite.status()
        except Exception as exc:
            return action_failure(
                code="room_camera_discovery_failed",
                message=f"Could not check room cameras: {exc}",
                say_hint="Explain that Tater could not check for a camera in this room.",
            )

        camera, selection_reason = self._select_camera(self._camera_satellites(status), origin)
        if camera is None:
            return action_failure(
                code="no_room_camera",
                message="No connected camera-capable Tater satellite is assigned to the asking satellite's room.",
                needs=["Connect an Echo Show or camera-capable Tater satellite and assign it to this room."],
                say_hint="Explain that no Tater camera is available in this room right now.",
            )

        selector = self._text(camera.get("selector"))
        camera_name = self._text(camera.get("device_name") or camera.get("name") or selector)
        room = self._text(camera.get("room") or origin.get("area_name") or origin.get("room"))
        try:
            snapshot_result = await native_satellite.send_request(
                selector,
                "camera.snapshot",
                {"reason": "explicit_room_vision_request"},
                timeout_s=11.0,
            )
        except Exception as exc:
            return action_failure(
                code="room_camera_snapshot_failed",
                message=f"{camera_name or 'The room camera'} could not take a snapshot: {exc}",
                say_hint="Explain that the room camera could not take a picture and suggest trying again.",
            )

        image, content_type = self._decode_snapshot(snapshot_result)
        if image is None:
            error = self._text(snapshot_result.get("error")) if isinstance(snapshot_result, dict) else ""
            return action_failure(
                code="invalid_room_snapshot",
                message=error or f"{camera_name or 'The room camera'} returned no usable image.",
                say_hint="Explain that the room camera did not return a usable picture.",
            )

        weather_context = await asyncio.to_thread(self._weather_context)
        try:
            from kernel_tools import image_describe

            vision_result = await asyncio.to_thread(
                image_describe,
                prompt=self._vision_prompt(query, camera_name, room, weather_context),
                image_ref={
                    "type": "image",
                    "name": "room-vision-snapshot.jpg",
                    "mimetype": content_type,
                    "bytes": image,
                },
            )
        except Exception as exc:
            return action_failure(
                code="room_vision_failed",
                message=f"Tater captured the room image but could not analyze it: {exc}",
                say_hint="Explain that the picture was captured but vision analysis failed.",
            )

        if not isinstance(vision_result, dict) or not bool(vision_result.get("ok")):
            error = vision_result.get("error") if isinstance(vision_result, dict) else {}
            error_message = self._text(error.get("message")) if isinstance(error, dict) else ""
            return action_failure(
                code="room_vision_failed",
                message=error_message or "Tater's vision model could not analyze the room snapshot.",
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
                code="empty_room_description",
                message="Tater's vision model returned no answer for the room snapshot.",
                say_hint="Explain that the image could not be described and suggest trying again.",
            )

        return action_success(
            facts={
                "camera_selector": selector,
                "camera_name": camera_name,
                "camera_room": room,
                "selection_reason": selection_reason,
                "snapshot_bytes": len(image),
                "weather_context_available": bool(weather_context),
            },
            data={
                "description": description,
                "camera_selector": selector,
                "camera_name": camera_name,
                "camera_room": room,
                "model": self._text(vision_data.get("model")),
            },
            summary_for_user=description,
            say_hint=(
                "Answer the user's visual question directly using only the returned description. "
                "Do not add visual details that the vision model did not report."
            ),
        )


verba = RoomVisionPlugin()
