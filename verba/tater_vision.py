from __future__ import annotations

import asyncio
import base64
import binascii
import re
from typing import Any, Dict, List, Tuple

from verba_base import ToolVerba
from verba_result import action_failure, action_success


MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_CAMERAS_PER_REQUEST = 6


class TaterVisionPlugin(ToolVerba):
    name = "tater_vision"
    verba_name = "Tater Vision"
    pretty_name = "Tater Vision"
    version = "1.1.3"
    min_tater_version = "98.4"
    settings_category = "Tater Vision"
    description = (
        "Tater Vision is Tater's visual-question tool. Use it whenever the answer requires looking at a person, "
        "object, room, or named camera-covered area right now—including 'what is this?', 'how do I look?', "
        "'is the dog in the game room?', 'are there any dogs in the backyard?', and conversational follow-ups such "
        "as 'what did you see when you looked?'. If the user names a room or area, it automatically uses the cameras "
        "covering that location. If no location is named, use trusted room context supplied by a satellite or other "
        "room-aware platform; when no room context exists, ask which room, area, or camera to use."
    )
    verba_dec = (
        "Give Tater eyes in camera-equipped rooms and areas. Ask what you're holding, how an outfit looks, whether "
        "a pet is nearby, or what's happening around your home. Tater automatically chooses a camera from the "
        "location you name or, on a room-aware device, the room where you ask."
    )
    when_to_use = (
        "Use for every question whose answer requires current visual evidence from a Tater satellite camera or an "
        "integrated camera, whether or not the user says camera, snapshot, or vision. This includes named locations, "
        "questions about the asking room, appearance and object questions, animals or activity in an area, and "
        "follow-ups referring to a previous look. Do not use for weather-only or other nonvisual questions."
    )
    how_to_use = (
        "Pass the user's complete visual question unchanged in query. Tater Vision resolves a named location from "
        "known camera assignments; otherwise it uses trusted room context when the platform provides it. On a "
        "platform without room context, include a room, area, or camera name. It captures fresh, ephemeral stills "
        "from the relevant cameras and asks Tater's configured vision model to answer the question."
    )
    platforms = [
        "voice_core",
        "homeassistant",
        "webui",
        "little_spud",
        "macos",
        "xbmc",
        "homekit",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    tags = ["tater", "room", "area", "camera", "snapshot", "vision", "appearance", "outfit", "echo-show"]
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
        "what did you see",
        "when you looked",
        "is the dog in",
        "are there any dogs",
        "is anyone in",
        "what is happening in",
        "what's happening in",
        "backyard",
        "back yard",
        "front yard",
        "porch",
        "driveway",
    ]
    usage = '{"function":"tater_vision","arguments":{"query":"What is this?"}}'
    example_calls = [
        '{"function":"tater_vision","arguments":{"query":"What is this?"}}',
        '{"function":"tater_vision","arguments":{"query":"How do my clothes look?"}}',
        '{"function":"tater_vision","arguments":{"query":"Am I dressed warmly enough outside?"}}',
        '{"function":"tater_vision","arguments":{"query":"Does this outfit match?"}}',
        '{"function":"tater_vision","arguments":{"query":"Are there any dogs in the backyard?"}}',
        '{"function":"tater_vision","arguments":{"query":"Is the dog in the game room?"}}',
    ]
    common_needs = [
        "A connected camera assigned to the relevant room or area, plus a named location when the platform does not "
        "provide trusted room context."
    ]
    missing_info_prompts = ["Which room, area, or camera should Tater look through?"]
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
            if not bool(capabilities.get("camera_snapshot")):
                continue
            row = dict(raw)
            row["selector"] = cls._text(raw.get("selector") or selector)
            row["source"] = "native_satellite"
            row["name"] = cls._text(raw.get("device_name") or raw.get("name") or row["selector"])
            row["room"] = cls._camera_room(raw)
            candidates.append(row)
        return sorted(
            candidates,
            key=lambda row: (
                -float(row.get("last_seen_ts") or 0.0),
                cls._text(row.get("selector")).casefold(),
            ),
        )

    @classmethod
    def _integration_cameras(cls, rows: Any) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        for raw in rows if isinstance(rows, list) else []:
            if not isinstance(raw, dict):
                continue
            integration_id = cls._text(raw.get("integration_id"))
            device_id = cls._text(raw.get("id") or raw.get("ref") or raw.get("device_id"))
            if not integration_id or not device_id:
                continue
            row = dict(raw)
            row["source"] = "integration"
            row["integration_id"] = integration_id
            row["device_id"] = device_id
            row["name"] = cls._text(raw.get("name") or raw.get("device_name") or device_id)
            row["room"] = cls._camera_room(raw)
            candidates.append(row)
        return sorted(
            candidates,
            key=lambda row: (
                cls._text(row.get("room")).casefold(),
                cls._text(row.get("name")).casefold(),
            ),
        )

    @classmethod
    def _camera_room(cls, row: Dict[str, Any]) -> str:
        return cls._text(
            row.get("room")
            or row.get("area_name")
            or row.get("room_name")
            or row.get("area")
            or row.get("location")
        )

    @classmethod
    def _normalized_words(cls, value: Any) -> str:
        return " ".join(re.findall(r"[a-z0-9]+", cls._text(value).casefold()))

    @classmethod
    def _compact_words(cls, value: Any) -> str:
        return "".join(re.findall(r"[a-z0-9]+", cls._text(value).casefold()))

    @classmethod
    def _query_mentions(cls, query: str, label: Any) -> bool:
        normalized_label = cls._normalized_words(label)
        if len(normalized_label) < 3 or normalized_label in {
            "camera", "show", "echo show", "room", "outside", "inside", "current room", "this room"
        }:
            return False
        normalized_query = cls._normalized_words(query)
        if re.search(rf"(?:^| ){re.escape(normalized_label)}(?: |$)", normalized_query):
            return True
        compact_label = cls._compact_words(normalized_label)
        compact_query = cls._compact_words(normalized_query)
        return len(compact_label) >= 5 and compact_label in compact_query

    @classmethod
    def _camera_aliases(cls, camera: Dict[str, Any]) -> List[str]:
        values: List[str] = []
        for key in ("name", "device_name", "friendly_name"):
            value = cls._text(camera.get(key))
            if value:
                values.append(value)
        aliases = camera.get("aliases")
        if isinstance(aliases, str):
            values.extend(part.strip() for part in aliases.split(",") if part.strip())
        elif isinstance(aliases, (list, tuple, set)):
            values.extend(cls._text(value) for value in aliases if cls._text(value))
        return values

    @classmethod
    def _origin_room(cls, origin: Dict[str, Any], status: Any) -> str:
        direct = cls._text(origin.get("area_name") or origin.get("room_name") or origin.get("room"))
        if direct:
            return direct
        clients = status.get("clients") if isinstance(status, dict) else {}
        if not isinstance(clients, dict):
            return ""
        origin_device = origin.get("device_id") or origin.get("selector")
        for selector, raw in clients.items():
            if not isinstance(raw, dict):
                continue
            candidate = dict(raw)
            candidate["selector"] = cls._text(raw.get("selector") or selector)
            if cls._matches_origin(candidate, origin_device):
                return cls._camera_room(raw)
        return ""

    @classmethod
    def _known_rooms(cls, status: Any, candidates: List[Dict[str, Any]]) -> List[str]:
        rooms = {cls._camera_room(candidate) for candidate in candidates if cls._camera_room(candidate)}
        clients = status.get("clients") if isinstance(status, dict) else {}
        if isinstance(clients, dict):
            rooms.update(
                cls._camera_room(raw)
                for raw in clients.values()
                if isinstance(raw, dict) and cls._camera_room(raw)
            )
        return sorted(rooms, key=lambda value: (-len(cls._compact_words(value)), value.casefold()))

    @classmethod
    def _registry_rooms(cls, registry: Any) -> List[str]:
        if not isinstance(registry, dict):
            return []
        rooms = {
            cls._camera_room(row)
            for row in (registry.get("devices") or [])
            if isinstance(row, dict) and cls._camera_room(row)
        }
        return sorted(rooms, key=lambda value: (-len(cls._compact_words(value)), value.casefold()))

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
    def _select_cameras(
        cls,
        candidates: List[Dict[str, Any]],
        origin: Dict[str, Any],
        query: str,
        origin_room: str,
        known_rooms: List[str] | None = None,
    ) -> Tuple[List[Dict[str, Any]], str, str]:
        mentioned_rooms = [
            room for room in (known_rooms or []) if cls._query_mentions(query, room)
        ]
        if mentioned_rooms:
            longest = max(len(cls._compact_words(room)) for room in mentioned_rooms)
            best_rooms = {
                cls._normalized_words(room)
                for room in mentioned_rooms
                if len(cls._compact_words(room)) == longest
            }
            selected = [
                candidate
                for candidate in candidates
                if cls._normalized_words(cls._camera_room(candidate)) in best_rooms
            ]
            return selected[:MAX_CAMERAS_PER_REQUEST], "named_area", mentioned_rooms[0]

        # An explicitly named camera wins over its room so "front door camera" does not
        # accidentally capture every camera assigned to the same area.
        named_cameras = [
            candidate
            for candidate in candidates
            if any(cls._query_mentions(query, alias) for alias in cls._camera_aliases(candidate))
        ]
        if named_cameras:
            best_length = max(
                max((len(cls._compact_words(alias)) for alias in cls._camera_aliases(row)), default=0)
                for row in named_cameras
            )
            selected = [
                row
                for row in named_cameras
                if max((len(cls._compact_words(alias)) for alias in cls._camera_aliases(row)), default=0)
                == best_length
            ]
            return selected[:MAX_CAMERAS_PER_REQUEST], "named_camera", cls._camera_room(selected[0])

        normalized_origin_room = cls._normalized_words(origin_room)
        if not normalized_origin_room:
            return [], "origin_room_unknown", ""
        selected = [
            candidate
            for candidate in candidates
            if cls._normalized_words(cls._camera_room(candidate)) == normalized_origin_room
        ]
        origin_device = origin.get("device_id") or origin.get("selector")
        selected.sort(
            key=lambda candidate: (
                0 if cls._matches_origin(candidate, origin_device) else 1,
                0 if candidate.get("source") == "native_satellite" else 1,
                cls._text(candidate.get("name")).casefold(),
            )
        )
        return selected[:MAX_CAMERAS_PER_REQUEST], "asking_room", origin_room

    @classmethod
    def _decode_image_bytes(cls, value: Any) -> bytes | None:
        if isinstance(value, (bytes, bytearray, memoryview)):
            image = bytes(value)
            return image if 0 < len(image) <= MAX_SNAPSHOT_BYTES else None
        if not isinstance(value, str):
            return None
        encoded = value.strip()
        if encoded.startswith("data:") and "," in encoded:
            encoded = encoded.split(",", 1)[1]
        if not encoded:
            return None
        try:
            image = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            return None
        return image if 0 < len(image) <= MAX_SNAPSHOT_BYTES else None

    @classmethod
    def _snapshot_image_ref(
        cls,
        result: Any,
        *,
        camera_name: str,
        index: int,
    ) -> Tuple[Dict[str, Any] | None, str]:
        if isinstance(result, dict) and result.get("ok") is False:
            error = result.get("error")
            if isinstance(error, dict):
                error = error.get("message") or error.get("detail")
            return None, cls._text(error) or "The camera rejected the snapshot request."

        payload = result
        if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
            payload = payload["result"]
        content_type = "image/jpeg"
        if isinstance(payload, dict):
            candidate_type = cls._text(
                payload.get("content_type")
                or payload.get("mimetype")
                or payload.get("mime")
                or payload.get("media_content_type")
            ).lower()
            if candidate_type.startswith("image/"):
                content_type = candidate_type.split(";", 1)[0]
            existing_ref = {
                key: cls._text(payload.get(key))
                for key in ("path", "blob_key", "url", "file_id")
                if cls._text(payload.get(key))
            }
            if existing_ref:
                return {
                    "type": "image",
                    "name": f"tater-vision-{index + 1}.jpg",
                    "mimetype": content_type,
                    "device_name": camera_name,
                    **existing_ref,
                }, ""
            for key in ("image_base64", "bytes", "data", "content", "image_bytes", "image_data"):
                image = cls._decode_image_bytes(payload.get(key))
                if image:
                    return {
                        "type": "image",
                        "name": f"tater-vision-{index + 1}.jpg",
                        "mimetype": content_type,
                        "device_name": camera_name,
                        "bytes": image,
                    }, ""
        elif isinstance(payload, (tuple, list)) and payload:
            image = cls._decode_image_bytes(payload[0])
            if len(payload) > 1 and cls._text(payload[1]).lower().startswith("image/"):
                content_type = cls._text(payload[1]).lower().split(";", 1)[0]
            if image:
                return {
                    "type": "image",
                    "name": f"tater-vision-{index + 1}.jpg",
                    "mimetype": content_type,
                    "device_name": camera_name,
                    "bytes": image,
                }, ""
        else:
            image = cls._decode_image_bytes(payload)
            if image:
                return {
                    "type": "image",
                    "name": f"tater-vision-{index + 1}.jpg",
                    "mimetype": content_type,
                    "device_name": camera_name,
                    "bytes": image,
                }, ""
        return None, "The camera returned no usable image."

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
    def _vision_prompt(
        query: str,
        camera_name: str,
        room: str,
        weather_context: str,
        camera_count: int,
    ) -> str:
        weather_note = (
            f" Current outdoor conditions reported by Tater are: {weather_context}. Use them only when relevant, "
            "especially when judging whether visible clothing is suitable for outside."
            if weather_context
            else " If the question depends on outdoor conditions, explain that you can judge the visible layers but "
            "do not have current outdoor weather data."
        )
        return (
            f"This is a current still image from {camera_name or 'a Tater camera'}"
            f"{f' in {room}' if room else ''}. Answer the user's exact visual request directly: {query}\n"
            + (
                "This is one of several camera views Tater is checking. Report the evidence visible in this view "
                "so the final answer can combine every view. "
                if camera_count > 1
                else ""
            )
            + "For appearance or outfit questions, give a warm, honest, practical opinion based only on what is "
            "visible. For objects, identify or explain only what the image supports. Do not identify people or infer "
            "age, ethnicity, health, disability, religion, sexuality, or other sensitive traits. If the subject is "
            "not visible or the framing is poor, say so clearly. Keep the spoken response concise and natural."
            + weather_note
        )

    async def _handle(self, args=None, llm_client=None, context=None, *, platform: str = ""):
        del llm_client
        payload = self._normalize_args(args or {})
        call_context = context if isinstance(context, dict) else {}
        origin = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}
        if not origin and isinstance(call_context.get("origin"), dict):
            origin = dict(call_context.get("origin") or {})
            payload["origin"] = origin
        query = self._text(
            payload.get("query")
            or payload.get("text")
            or payload.get("request")
            or origin.get("request_text")
            or call_context.get("request_text")
            or call_context.get("body")
            or call_context.get("raw_message")
        )
        if not query:
            return action_failure(
                code="missing_query",
                message="Please provide the visual question for Tater Vision.",
                needs=["Ask what Tater should look at or comment on."],
                say_hint="Ask what the user wants Tater to look at.",
            )

        try:
            from tater_voice import native_satellite

            status = await native_satellite.status()
        except Exception:
            status = {"clients": {}}

        native_cameras = self._camera_satellites(status)
        try:
            from integration_registry import get_integration_devices_by_capability

            integration_rows = await asyncio.to_thread(get_integration_devices_by_capability, "camera")
        except Exception:
            integration_rows = []
        integration_cameras = self._integration_cameras(integration_rows)
        candidates = native_cameras + integration_cameras
        known_rooms = self._known_rooms(status, candidates)
        try:
            from integration_registry import get_integration_device_registry

            integration_registry = await asyncio.to_thread(get_integration_device_registry)
            known_rooms = sorted(
                set(known_rooms).union(self._registry_rooms(integration_registry)),
                key=lambda value: (-len(self._compact_words(value)), value.casefold()),
            )
        except Exception:
            pass
        origin_room = self._origin_room(origin, status)
        cameras, selection_reason, target_room = self._select_cameras(
            candidates,
            origin,
            query,
            origin_room,
            known_rooms,
        )
        if not cameras and selection_reason == "origin_room_unknown":
            voice_origin = self._text(platform).lower() == "voice_core" or (
                self._text(origin.get("platform")).lower() == "homeassistant"
                and self._text(origin.get("entrypoint")).lower() == "voice_core"
            )
            return action_failure(
                code="origin_room_unknown" if voice_origin else "camera_location_required",
                message=(
                    "Tater could not determine which room the asking satellite is assigned to."
                    if voice_origin
                    else "Tater Vision needs a room, area, or camera name for this request."
                ),
                needs=(
                    ["Assign the asking satellite to a room in Tater."]
                    if voice_origin
                    else ["Name the room, area, or camera Tater should use."]
                ),
                say_hint=(
                    "Explain that the asking satellite needs a room assignment before Tater can choose a nearby camera."
                    if voice_origin
                    else "Ask which room, area, or camera Tater should look through."
                ),
            )
        if not cameras:
            return action_failure(
                code="no_room_camera",
                message=f"No connected camera is assigned to {target_room or 'the current room'}.",
                needs=["Assign a camera-capable Tater satellite or integrated camera to this room or area."],
                say_hint=(
                    f"Explain that Tater cannot see {target_room} because no camera is assigned there."
                    if selection_reason == "named_area" and target_room
                    else "Explain that Tater cannot look here because no camera is available in the room you are in."
                ),
            )

        weather_context = await asyncio.to_thread(self._weather_context)
        descriptions: List[Dict[str, Any]] = []
        failures: List[Dict[str, str]] = []
        snapshot_bytes = 0
        try:
            from kernel_tools import image_describe
        except Exception as exc:
            return action_failure(
                code="tater_vision_unavailable",
                message=f"Tater's vision tool is unavailable: {exc}",
                say_hint="Explain that Tater could not start vision analysis.",
            )

        for index, camera in enumerate(cameras):
            camera_name = self._text(camera.get("name") or camera.get("device_name") or "Camera")
            room = self._camera_room(camera) or target_room
            try:
                if camera.get("source") == "native_satellite":
                    snapshot_result = await native_satellite.send_request(
                        self._text(camera.get("selector")),
                        "camera.snapshot",
                        {"reason": "explicit_tater_vision_request"},
                        timeout_s=11.0,
                    )
                else:
                    from integration_registry import run_integration_device_action

                    snapshot_result = await asyncio.to_thread(
                        run_integration_device_action,
                        self._text(camera.get("integration_id")),
                        "camera_snapshot",
                        self._text(camera.get("device_id")),
                        {},
                    )
                image_ref, snapshot_error = self._snapshot_image_ref(
                    snapshot_result,
                    camera_name=camera_name,
                    index=index,
                )
                if not image_ref:
                    failures.append({"camera": camera_name, "error": snapshot_error})
                    continue
                raw = image_ref.get("bytes")
                if isinstance(raw, (bytes, bytearray, memoryview)):
                    snapshot_bytes += len(raw)
                vision_result = await asyncio.to_thread(
                    image_describe,
                    prompt=self._vision_prompt(query, camera_name, room, weather_context, len(cameras)),
                    image_ref=image_ref,
                    name=image_ref.get("name"),
                    mimetype=image_ref.get("mimetype"),
                )
            except Exception as exc:
                failures.append({"camera": camera_name, "error": str(exc)})
                continue

            if not isinstance(vision_result, dict) or not bool(vision_result.get("ok")):
                error = vision_result.get("error") if isinstance(vision_result, dict) else {}
                error_message = self._text(error.get("message")) if isinstance(error, dict) else ""
                failures.append({"camera": camera_name, "error": error_message or "Vision returned no answer."})
                continue
            vision_data = vision_result.get("data") if isinstance(vision_result.get("data"), dict) else {}
            description = self._text(
                vision_data.get("description")
                or vision_data.get("text")
                or vision_result.get("summary_for_user")
            )
            if not description:
                failures.append({"camera": camera_name, "error": "Vision returned no description."})
                continue
            descriptions.append(
                {
                    "camera": camera_name,
                    "room": room,
                    "description": description,
                    "model": self._text(vision_data.get("model")),
                }
            )

        if not descriptions:
            failure_message = "; ".join(
                f"{row['camera']}: {row['error']}" for row in failures[:3]
            )
            return action_failure(
                code="tater_vision_failed",
                message=failure_message or "No selected camera returned a usable image.",
                say_hint="Explain that cameras were found, but Tater could not get or analyze their current images.",
            )

        summary = (
            descriptions[0]["description"]
            if len(descriptions) == 1
            else "\n".join(f"{row['camera']}: {row['description']}" for row in descriptions)
        )

        return action_success(
            facts={
                "camera_count": len(cameras),
                "vision_count": len(descriptions),
                "vision_failure_count": len(failures),
                "target_room": target_room,
                "selection_reason": selection_reason,
                "snapshot_bytes": snapshot_bytes,
                "weather_context_available": bool(weather_context),
            },
            data={
                "description": descriptions[0]["description"] if len(descriptions) == 1 else summary,
                "vision_descriptions": descriptions,
                "vision_failures": failures,
                "target_room": target_room,
            },
            summary_for_user=summary,
            say_hint=(
                "Answer the user's visual question directly using all returned camera descriptions. "
                "Combine multiple views when needed, and do not add details the vision model did not report."
            ),
        )

    async def handle_webui(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context=context, platform="webui")

    async def handle_homeassistant(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context=context, platform="homeassistant")

    async def handle_voice_core(self, args=None, llm_client=None, context=None):
        return await self._handle(args, llm_client, context=context, platform="voice_core")

    async def handle_macos(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context=context, platform="macos")

    async def handle_little_spud(self, args=None, llm_client=None, context=None):
        return await self._handle(args, llm_client, context=context, platform="little_spud")

    async def handle_xbmc(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context=context, platform="xbmc")

    async def handle_homekit(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context=context, platform="homekit")

    async def handle_discord(self, message, args, llm_client, context=None):
        payload = self._normalize_args(args or {})
        if not self._text(payload.get("query")):
            content = self._text(getattr(message, "content", ""))
            if content:
                payload["query"] = content
        return await self._handle(payload, llm_client, context=context, platform="discord")

    async def handle_telegram(self, update, context, args, llm_client):
        payload = self._normalize_args(args or {})
        if not self._text(payload.get("query")):
            message = getattr(update, "message", None)
            text = self._text(getattr(message, "text", ""))
            if text:
                payload["query"] = text
        return await self._handle(
            payload,
            llm_client,
            context=context if isinstance(context, dict) else None,
            platform="telegram",
        )

    async def handle_matrix(self, client, room, sender, body, args, llm_client, context=None):
        del client, room, sender
        payload = self._normalize_args(args or {})
        if not self._text(payload.get("query")) and self._text(body):
            payload["query"] = self._text(body)
        return await self._handle(payload, llm_client, context=context, platform="matrix")

    async def handle_irc(self, bot, channel, user, message, args, llm_client, context=None):
        del bot, channel, user
        payload = self._normalize_args(args or {})
        if not self._text(payload.get("query")) and self._text(message):
            payload["query"] = self._text(message)
        return await self._handle(payload, llm_client, context=context, platform="irc")

    async def handle_meshtastic(self, packet, args, llm_client, context=None):
        payload = self._normalize_args(args or {})
        if not self._text(payload.get("query")) and isinstance(packet, dict):
            decoded = packet.get("decoded") if isinstance(packet.get("decoded"), dict) else {}
            text = self._text(
                packet.get("text")
                or packet.get("message")
                or decoded.get("text")
                or decoded.get("payload")
            )
            if text:
                payload["query"] = text
        return await self._handle(payload, llm_client, context=context, platform="meshtastic")


verba = TaterVisionPlugin()
