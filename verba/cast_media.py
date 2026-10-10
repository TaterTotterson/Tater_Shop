from __future__ import annotations

import asyncio
import base64
import mimetypes
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse

from helpers import redis_blob_client
from integration_registry import (
    get_integration_devices_by_capability,
    get_integration_room_preferred_media_player,
)
from verba_base import ToolVerba
from verba_result import action_failure, action_success


class CastMediaPlugin(ToolVerba):
    name = "cast_media"
    verba_name = "Cast Media"
    pretty_name = "Playing on TV"
    version = "1.0.7"
    min_tater_version = "198"
    settings_category = None
    platforms = [
        "voice_core",
        "homeassistant",
        "webui",
        "tater_open_webui",
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

    description = "Play audio and video media files on a named TV or Google Cast device."
    verba_dec = description
    when_to_use = (
        "Play media on a TV or cast audio/video to a named Google Cast device. "
        "Use after a media-creation Verba returns a song, recording, or video and the user asks to play or cast it."
    )
    how_to_use = (
        "Pass the user's complete natural-language playback request unchanged in query. "
        "When an available artifact is the requested media, pass its exact artifact_id. "
        "The Verba resolves the TV name or room from Tater's Google Cast device registry."
    )
    usage = (
        '{"function":"cast_media","arguments":{"query":"Play the generated song on the office TV",'
        '"artifact_id":"<exact available artifact_id>"}}'
    )
    example_calls = [
        '{"function":"cast_media","arguments":{"query":"Play that song on the office TV","artifact_id":"att1"}}',
        '{"function":"cast_media","arguments":{"query":"Cast this video to the living room TV","artifact_id":"att2"}}',
        '{"function":"cast_media","arguments":{"query":"Play this link on the bedroom TV","source_url":"https://example.com/media.mp3"}}',
    ]
    routing_keywords = [
        "play on tv",
        "play it on the tv",
        "play this on the tv",
        "cast media",
        "cast audio",
        "cast video",
        "cast song",
        "chromecast",
        "google cast",
    ]
    tags = ["media", "google-cast", "tv", "playback"]
    common_needs = ["query"]
    missing_info_prompts = []
    required_settings = {}
    waiting_prompt_template = (
        "Write one short friendly message telling {mention} you are sending the media to the TV now. "
        "Only output that message."
    )
    argument_schema = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "The user's complete natural-language playback request, including the TV or room name."
                ),
            },
            "artifact_id": {
                "type": "string",
                "description": (
                    "Optional exact artifact_id of audio or video returned by another tool. "
                    "Use an ID from the available conversation artifacts; never invent one."
                ),
            },
            "target": {
                "type": "string",
                "description": "Optional TV, Cast device, or room name when it is known explicitly.",
            },
            "source_url": {
                "type": "string",
                "description": "Optional direct HTTP(S) audio or video URL when no artifact is being used.",
            },
            "volume_percent": {
                "type": "integer",
                "minimum": 0,
                "maximum": 100,
                "description": "Optional playback volume percentage.",
            },
        },
        "required": ["query"],
    }

    _AUDIO_SUFFIXES = {
        ".aac",
        ".flac",
        ".m4a",
        ".mka",
        ".mp3",
        ".oga",
        ".ogg",
        ".opus",
        ".wav",
    }
    _VIDEO_SUFFIXES = {
        ".m4v",
        ".mkv",
        ".mov",
        ".mp4",
        ".mpeg",
        ".mpg",
        ".webm",
    }
    _PLAYABLE_SUFFIXES = _AUDIO_SUFFIXES | _VIDEO_SUFFIXES
    _SECONDARY_VIDEO_MARKERS = {
        "behind the scenes",
        "deleted scene",
        "featurette",
        "sample",
        "trailer",
    }
    _GENERIC_TARGET_ALIASES = {
        "cast",
        "cast device",
        "chromecast",
        "device",
        "google cast",
        "media player",
        "player",
        "television",
        "the television",
        "the tv",
        "tv",
    }

    @staticmethod
    def _text(value: Any) -> str:
        if isinstance(value, (bytes, bytearray)):
            return bytes(value).decode("utf-8", "ignore").strip()
        return str(value or "").strip()

    @classmethod
    def _norm(cls, value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", " ", cls._text(value).lower()).strip()

    @staticmethod
    def _http_url(value: Any) -> str:
        text = str(value or "").strip().rstrip(".,;!?)\"]}")
        parsed = urlparse(text)
        if parsed.scheme.lower() in {"http", "https"} and parsed.netloc:
            return text
        return ""

    @classmethod
    def _url_from_query(cls, query: str) -> str:
        for match in re.findall(r"https?://[^\s<>]+", query or "", flags=re.IGNORECASE):
            url = cls._http_url(match)
            if url:
                return url
        return ""

    @classmethod
    def _query(cls, args: Dict[str, Any]) -> str:
        for key in ("query", "request", "message", "text", "prompt"):
            value = cls._text((args or {}).get(key))
            if value:
                return " ".join(value.split())
        return ""

    @classmethod
    def _target_hint(cls, args: Dict[str, Any]) -> str:
        for key in ("target", "device", "tv", "room", "area", "location", "where"):
            value = cls._text((args or {}).get(key))
            if value:
                return value
        return ""

    @classmethod
    def _context_origin(cls, args: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        origin = (args or {}).get("origin")
        if isinstance(origin, dict):
            return origin
        if isinstance(context, dict) and isinstance(context.get("origin"), dict):
            return context["origin"]
        return {}

    @classmethod
    def _artifact_rows(cls, args: Dict[str, Any], context: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        containers: List[Dict[str, Any]] = []
        for candidate in (
            args,
            (args or {}).get("origin"),
            context,
            (context or {}).get("origin") if isinstance(context, dict) else None,
        ):
            if isinstance(candidate, dict):
                containers.append(candidate)

        rows: List[Dict[str, Any]] = []
        seen = set()
        for container in containers:
            for key in ("available_artifacts", "input_artifacts", "attachments"):
                values = container.get(key)
                if not isinstance(values, list):
                    continue
                for raw in values:
                    if not isinstance(raw, dict):
                        continue
                    identity = tuple(
                        cls._text(raw.get(field))
                        for field in ("artifact_id", "blob_key", "file_id", "path", "url", "name")
                    )
                    if identity in seen:
                        continue
                    seen.add(identity)
                    rows.append(dict(raw))
        return rows

    @classmethod
    def _artifact_is_playable(cls, artifact: Dict[str, Any]) -> bool:
        artifact_type = cls._text(artifact.get("type")).lower()
        mimetype_value = cls._text(artifact.get("mimetype") or artifact.get("mime_type")).lower()
        name = cls._text(artifact.get("name") or artifact.get("filename") or artifact.get("path"))
        if artifact_type in {"audio", "video"}:
            return True
        if mimetype_value.startswith(("audio/", "video/")):
            return True
        return Path(urlparse(name).path).suffix.lower() in cls._PLAYABLE_SUFFIXES

    @staticmethod
    def _blob_bytes(blob_key: str) -> bytes:
        key = str(blob_key or "").strip()
        if not key:
            return b""
        try:
            raw = redis_blob_client.get(key.encode("utf-8"))
        except Exception:
            raw = None
        if not isinstance(raw, (bytes, bytearray)):
            try:
                raw = redis_blob_client.get(key)
            except Exception:
                raw = None
        return bytes(raw) if isinstance(raw, (bytes, bytearray)) else b""

    @classmethod
    def _file_id_bytes(cls, file_id: Any) -> bytes:
        token = cls._text(file_id)
        if not token:
            return b""
        if token.startswith("webui:file:"):
            return cls._blob_bytes(token)
        return cls._blob_bytes(f"webui:file:{token}")

    @classmethod
    def _artifact_bytes(cls, artifact: Dict[str, Any]) -> bytes:
        for key in ("bytes", "data"):
            value = artifact.get(key)
            if isinstance(value, (bytes, bytearray)):
                return bytes(value)

        data_value = cls._text(artifact.get("data") or artifact.get("data_url"))
        if data_value.lower().startswith("data:") and "," in data_value:
            try:
                return base64.b64decode(data_value.split(",", 1)[1])
            except Exception:
                pass

        blob = cls._blob_bytes(cls._text(artifact.get("blob_key")))
        if blob:
            return blob

        file_blob = cls._file_id_bytes(artifact.get("file_id") or artifact.get("id"))
        if file_blob:
            return file_blob

        path_value = cls._text(
            artifact.get("path") or artifact.get("file_path") or artifact.get("artifact_path")
        )
        if path_value:
            try:
                path = Path(path_value).expanduser()
                if path.is_file():
                    return path.read_bytes()
            except Exception:
                pass
        return b""

    @classmethod
    def _media_type(cls, artifact: Dict[str, Any], source_url: str, filename: str) -> str:
        supplied = cls._text(artifact.get("mimetype") or artifact.get("mime_type")).lower()
        if supplied.startswith(("audio/", "video/")):
            return supplied.split(";", 1)[0]
        guessed = mimetypes.guess_type(filename or urlparse(source_url).path)[0]
        if guessed and guessed.startswith(("audio/", "video/")):
            return guessed
        if cls._text(artifact.get("type")).lower() == "video":
            return "video/mp4"
        return "audio/mpeg"

    @classmethod
    def _materialize_artifact(cls, artifact: Dict[str, Any]) -> Dict[str, Any]:
        if not cls._artifact_is_playable(artifact):
            return {}
        source_url = cls._http_url(artifact.get("url") or artifact.get("uri"))
        binary = cls._artifact_bytes(artifact)
        if not source_url and not binary:
            return {}
        filename = cls._text(artifact.get("name") or artifact.get("filename"))
        if not filename:
            filename = Path(urlparse(source_url).path).name or "media.bin"
        media_type = cls._media_type(artifact, source_url, filename)
        return {
            "artifact_id": cls._text(artifact.get("artifact_id")),
            "source_url": source_url,
            "media_bytes": binary,
            "media_type": media_type,
            "media_content_type": "video" if media_type.startswith("video/") else "music",
            "filename": filename,
            "title": Path(filename).stem.replace("_", " ").strip() or "Media",
            "resolved_from": "artifact",
        }

    @classmethod
    def _requested_media_kind(cls, query: str) -> str:
        normalized = cls._norm(query)
        if re.search(r"\b(?:audio|music|podcast|recording|song|sound|track)\b", normalized):
            return "audio"
        if re.search(r"\b(?:episode|film|movie|show|video)\b", normalized):
            return "video"
        return ""

    @classmethod
    def _link_entry_media(cls, entry: Dict[str, Any], *, query: str) -> Tuple[Dict[str, Any], Tuple[Any, ...]]:
        source_url = ""
        url_preference = 0
        for preference, key in enumerate(
            (
                "stream_link",
                "stream",
                "media_url",
                "video_url",
                "audio_url",
                "source_url",
                "download_link",
                "link",
                "url",
            ),
            start=1,
        ):
            candidate = cls._http_url(entry.get(key))
            if candidate:
                source_url = candidate
                url_preference = 10 - preference
                break
        if not source_url:
            return {}, ()

        raw_name = cls._text(
            entry.get("path")
            or entry.get("name")
            or entry.get("filename")
            or entry.get("title")
        )
        filename = Path(urlparse(raw_name).path).name if raw_name else ""
        if not filename:
            filename = Path(urlparse(source_url).path).name
        filename = unquote(filename) or "media.bin"

        supplied_type = cls._text(entry.get("mimetype") or entry.get("mime_type")).lower()
        suffix = Path(filename).suffix.lower()
        if supplied_type.startswith("video/") or suffix in cls._VIDEO_SUFFIXES:
            media_kind = "video"
        elif supplied_type.startswith("audio/") or suffix in cls._AUDIO_SUFFIXES:
            media_kind = "audio"
        else:
            return {}, ()

        try:
            size = int(float(entry.get("size") or 0))
        except (TypeError, ValueError):
            size = 0
        normalized_name = cls._norm(raw_name or filename)
        is_primary = not any(marker in normalized_name for marker in cls._SECONDARY_VIDEO_MARKERS)
        requested_kind = cls._requested_media_kind(query)
        kind_match = 1 if not requested_kind or requested_kind == media_kind else 0
        default_kind = 1 if media_kind == "video" else 0

        media_type = cls._media_type(entry, source_url, filename)
        media = {
            "artifact_id": "",
            "source_url": source_url,
            "media_bytes": b"",
            "media_type": media_type,
            "media_content_type": "video" if media_kind == "video" else "music",
            "filename": filename,
            "title": Path(filename).stem.replace("_", " ").strip() or "Media",
            "resolved_from": "previous_tool_result_link",
        }
        return media, (kind_match, int(is_primary), default_kind, size, url_preference)

    @classmethod
    def _reference_matches_link(cls, reference: str, entry: Dict[str, Any], media: Dict[str, Any]) -> bool:
        wanted = unquote(cls._text(reference)).casefold()
        if not wanted:
            return False
        wanted_basename = Path(urlparse(wanted).path).name
        values = [
            entry.get("path"),
            entry.get("name"),
            entry.get("filename"),
            entry.get("title"),
            media.get("source_url"),
            media.get("filename"),
        ]
        for value in values:
            candidate = unquote(cls._text(value)).casefold()
            if not candidate:
                continue
            if candidate == wanted:
                return True
            candidate_basename = Path(urlparse(candidate).path).name
            if wanted_basename and candidate_basename == wanted_basename:
                return True
        return False

    @classmethod
    def _best_link_list_media(
        cls,
        containers: List[Dict[str, Any]],
        *,
        query: str,
        preferred_reference: str = "",
    ) -> Dict[str, Any]:
        ranked: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
        for container in containers:
            for key in ("direct_links", "links", "files", "items"):
                rows = container.get(key)
                if not isinstance(rows, list):
                    continue
                for entry in rows:
                    if not isinstance(entry, dict):
                        continue
                    media, rank = cls._link_entry_media(entry, query=query)
                    if not media:
                        continue
                    if preferred_reference:
                        if not cls._reference_matches_link(preferred_reference, entry, media):
                            continue
                        rank = (1,) + rank
                    ranked.append((rank, media))
        if not ranked:
            return {}
        ranked.sort(key=lambda item: item[0], reverse=True)
        return ranked[0][1]

    @classmethod
    def _prior_link_media(cls, origin: Dict[str, Any], *, query: str, reference: str) -> Dict[str, Any]:
        history = origin.get("tool_results_full") if isinstance(origin, dict) else None
        if not isinstance(history, list):
            return {}
        for raw_payload in reversed(history):
            if not isinstance(raw_payload, dict) or raw_payload.get("ok") is False:
                continue
            containers = [raw_payload]
            for key in ("facts", "data"):
                value = raw_payload.get(key)
                if isinstance(value, dict):
                    containers.append(value)
            media = cls._best_link_list_media(
                containers,
                query=query,
                preferred_reference=reference,
            )
            if media:
                return media
        return {}

    @classmethod
    def _prior_media_url(cls, origin: Dict[str, Any], *, query: str = "") -> Dict[str, Any]:
        history = origin.get("tool_results_full") if isinstance(origin, dict) else None
        if not isinstance(history, list):
            return {}
        for raw_payload in reversed(history):
            if not isinstance(raw_payload, dict) or raw_payload.get("ok") is False:
                continue
            containers = [raw_payload]
            for key in ("facts", "data"):
                value = raw_payload.get(key)
                if isinstance(value, dict):
                    containers.append(value)
            for container in containers:
                for key in ("media_url", "source_url", "audio_url", "video_url"):
                    source_url = cls._http_url(container.get(key))
                    if not source_url:
                        continue
                    filename = Path(urlparse(source_url).path).name or "media.bin"
                    media_type = cls._media_type({}, source_url, filename)
                    return {
                        "artifact_id": "",
                        "source_url": source_url,
                        "media_bytes": b"",
                        "media_type": media_type,
                        "media_content_type": "video" if media_type.startswith("video/") else "music",
                        "filename": filename,
                        "title": Path(filename).stem.replace("_", " ").strip() or "Media",
                        "resolved_from": "previous_tool_result",
                    }
            linked_media = cls._best_link_list_media(containers, query=query)
            if linked_media:
                return linked_media
        return {}

    @classmethod
    def _is_result_set_reference(cls, value: str) -> bool:
        return bool(re.fullmatch(r"rs\d+(?:[#:_-]\d+)?", cls._text(value), flags=re.IGNORECASE))

    @classmethod
    def _url_media(cls, source_url: str, *, resolved_from: str) -> Dict[str, Any]:
        url = cls._http_url(source_url)
        if not url:
            return {}
        filename = Path(urlparse(url).path).name or "media.bin"
        media_type = cls._media_type({}, url, filename)
        return {
            "artifact_id": "",
            "source_url": url,
            "media_bytes": b"",
            "media_type": media_type,
            "media_content_type": "video" if media_type.startswith("video/") else "music",
            "filename": filename,
            "title": Path(filename).stem.replace("_", " ").strip() or "Media",
            "resolved_from": resolved_from,
        }

    @classmethod
    def _resolve_media(
        cls,
        args: Dict[str, Any],
        context: Optional[Dict[str, Any]],
        query: str,
    ) -> Tuple[Dict[str, Any], str]:
        artifacts = cls._artifact_rows(args, context)
        artifact_id = cls._text((args or {}).get("artifact_id"))
        result_set_reference = False
        if artifact_id:
            artifact_url = cls._http_url(artifact_id)
            if artifact_url:
                return cls._url_media(artifact_url, resolved_from="artifact_url"), ""
            artifact = next(
                (
                    item
                    for item in artifacts
                    if cls._text(item.get("artifact_id")).casefold() == artifact_id.casefold()
                ),
                None,
            )
            if artifact is None:
                if cls._is_result_set_reference(artifact_id):
                    result_set_reference = True
                else:
                    linked_media = cls._prior_link_media(
                        cls._context_origin(args, context),
                        query=query,
                        reference=artifact_id,
                    )
                    if linked_media:
                        return linked_media, ""
                    return {}, f"Artifact `{artifact_id}` is not available in this conversation."
            else:
                media = cls._materialize_artifact(artifact)
                if not media:
                    return {}, f"Artifact `{artifact_id}` is not playable audio or video."
                return media, ""

        explicit_url = cls._http_url(
            (args or {}).get("source_url")
            or (args or {}).get("media_url")
            or (args or {}).get("url")
        )
        if explicit_url:
            return cls._url_media(explicit_url, resolved_from="source_url"), ""

        if result_set_reference:
            prior_media = cls._prior_media_url(cls._context_origin(args, context), query=query)
            if prior_media:
                return prior_media, ""

        for artifact in artifacts:
            media = cls._materialize_artifact(artifact)
            if media:
                return media, ""
        prior_media = cls._prior_media_url(cls._context_origin(args, context), query=query)
        if prior_media:
            return prior_media, ""

        query_url = cls._url_from_query(query)
        if query_url:
            return cls._url_media(query_url, resolved_from="query_url"), ""
        return {}, "No playable audio or video was found in the request or conversation artifacts."

    @classmethod
    def _device_aliases(cls, device: Dict[str, Any]) -> List[str]:
        details = device.get("details") if isinstance(device.get("details"), dict) else {}
        raw_aliases: List[Any] = [
            device.get("name"),
            device.get("display_name"),
            device.get("reported_name"),
            device.get("id"),
            device.get("ref"),
            details.get("host"),
        ]
        aliases_value = device.get("aliases")
        if isinstance(aliases_value, (list, tuple, set)):
            raw_aliases.extend(aliases_value)
        elif aliases_value:
            raw_aliases.append(aliases_value)
        aliases: List[str] = []
        for value in raw_aliases:
            token = cls._norm(value)
            if token and token not in aliases and token not in cls._GENERIC_TARGET_ALIASES:
                aliases.append(token)
        return aliases

    @classmethod
    def _cast_devices(cls) -> List[Dict[str, Any]]:
        try:
            rows = get_integration_devices_by_capability("media_player")
        except Exception:
            rows = []
        cast_rows = [
            dict(row)
            for row in rows or []
            if isinstance(row, dict)
            and cls._text(row.get("integration_id")).lower() == "google_cast"
        ]
        if cast_rows:
            return cast_rows
        try:
            rows = get_integration_devices_by_capability("media_player", refresh=True)
        except Exception:
            rows = []
        return [
            dict(row)
            for row in rows or []
            if isinstance(row, dict)
            and cls._text(row.get("integration_id")).lower() == "google_cast"
        ]

    @classmethod
    def _device_id_from_target(cls, value: Any) -> str:
        token = cls._text(value)
        lower = token.lower()
        for prefix in ("integration:google_cast:", "google_cast:", "cast:"):
            if lower.startswith(prefix):
                return unquote(token[len(prefix) :]).strip().lower()
        return ""

    @classmethod
    def _device_by_id(cls, devices: List[Dict[str, Any]], device_id: str) -> Optional[Dict[str, Any]]:
        wanted = cls._text(device_id).lower()
        if not wanted:
            return None
        for device in devices:
            for value in (device.get("id"), device.get("device_id"), device.get("ref")):
                token = cls._text(value).lower()
                if token == wanted or token.removeprefix("media_player:") == wanted:
                    return device
        return None

    @classmethod
    def _preferred_room_device(
        cls,
        devices: List[Dict[str, Any]],
        room_hints: List[str],
    ) -> Optional[Dict[str, Any]]:
        for room in room_hints:
            if not cls._text(room):
                continue
            try:
                preference = get_integration_room_preferred_media_player(room)
            except Exception:
                preference = {}
            target_id = cls._device_id_from_target((preference or {}).get("target"))
            device = cls._device_by_id(devices, target_id)
            if device is not None:
                return device
        return None

    @classmethod
    def _context_room_hints(cls, context: Optional[Dict[str, Any]], origin: Dict[str, Any]) -> List[str]:
        rows: List[str] = []
        for source in (context, origin):
            if not isinstance(source, dict):
                continue
            for key in ("room_name", "room", "area_name", "area", "room_id", "area_id"):
                value = cls._text(source.get(key))
                if value and value not in rows:
                    rows.append(value)
        return rows

    @classmethod
    def _select_device(
        cls,
        devices: List[Dict[str, Any]],
        *,
        args: Dict[str, Any],
        context: Optional[Dict[str, Any]],
        query: str,
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        hint = cls._target_hint(args)
        direct_id = cls._device_id_from_target(hint)
        if direct_id:
            direct = cls._device_by_id(devices, direct_id)
            return (direct, "") if direct else (None, f"Google Cast target `{hint}` was not found.")

        hint_norm = cls._norm(hint)
        query_norm = cls._norm(query)
        room_hints: List[str] = [hint] if hint else []
        for device in devices:
            room = cls._text(device.get("room") or device.get("area"))
            room_norm = cls._norm(room)
            if room and room_norm and room_norm != "unassigned" and room_norm in query_norm:
                room_hints.append(room)
        preferred = cls._preferred_room_device(devices, room_hints)
        if preferred is not None:
            return preferred, ""

        scored: List[Tuple[int, Dict[str, Any]]] = []
        for device in devices:
            score = 0
            aliases = cls._device_aliases(device)
            for alias in aliases:
                if hint_norm and hint_norm == alias:
                    score = max(score, 140)
                elif hint_norm and alias in hint_norm:
                    score = max(score, 110 + min(20, len(alias)))
                elif hint_norm and hint_norm in alias:
                    score = max(score, 110)
                if query_norm and alias in query_norm:
                    score = max(score, 95 + min(20, len(alias)))

            room = cls._norm(device.get("room") or device.get("area"))
            if room and room != "unassigned":
                if hint_norm and hint_norm == room:
                    score = max(score, 90)
                elif hint_norm and room in hint_norm:
                    score = max(score, 80)
                if query_norm and room in query_norm:
                    score = max(score, 70)
            if score:
                scored.append((score, device))

        if scored:
            scored.sort(key=lambda item: item[0], reverse=True)
            best_score = scored[0][0]
            best = [device for score, device in scored if score == best_score]
            if len(best) == 1:
                return best[0], ""
            names = ", ".join(cls._text(row.get("name")) or "unnamed Cast device" for row in best[:5])
            return None, f"The TV target is ambiguous between: {names}."

        origin = cls._context_origin(args, context)
        context_rooms = cls._context_room_hints(context, origin)
        preferred = cls._preferred_room_device(devices, context_rooms)
        if preferred is not None:
            return preferred, ""
        for room_hint in context_rooms:
            room_norm = cls._norm(room_hint)
            matches = [
                device
                for device in devices
                if room_norm
                and room_norm == cls._norm(device.get("room") or device.get("area"))
            ]
            if len(matches) == 1:
                return matches[0], ""

        if not hint and len(devices) == 1:
            return devices[0], ""
        if hint:
            return None, f"No Google Cast TV or device matched `{hint}`."
        return None, "The request did not identify which Google Cast TV or device to use."

    @classmethod
    def _volume_percent(cls, args: Dict[str, Any], query: str) -> Optional[int]:
        raw = (args or {}).get("volume_percent")
        if raw in (None, ""):
            match = re.search(
                r"\b(?:at|volume(?:\s+(?:to|at))?)\s*(\d{1,3})\s*(?:%|percent)\b",
                query or "",
                flags=re.IGNORECASE,
            )
            if match is None:
                return None
            raw = match.group(1)
        try:
            parsed = int(float(raw))
        except Exception:
            return None
        return max(0, min(100, parsed))

    @classmethod
    def _device_choices(cls, devices: List[Dict[str, Any]]) -> List[str]:
        rows: List[str] = []
        for device in devices[:8]:
            name = cls._text(device.get("name")) or cls._text(device.get("id")) or "Google Cast device"
            room = cls._text(device.get("room") or device.get("area"))
            if room and cls._norm(room) != "unassigned" and cls._norm(room) != cls._norm(name):
                name = f"{name} ({room})"
            rows.append(name)
        return rows

    async def _handle(
        self,
        args: Optional[Dict[str, Any]],
        llm_client: Any = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        del llm_client
        payload = dict(args or {})
        query = self._query(payload)
        if not query:
            return action_failure(
                code="missing_request",
                message="No natural-language TV playback request was provided.",
                needs=["Say what to play or cast and name the TV or room."],
                say_hint="Ask what media to play and which TV or room to use.",
            )

        media, media_error = self._resolve_media(payload, context, query)
        if not media:
            return action_failure(
                code="missing_media",
                message=media_error,
                needs=[
                    "Create or return audio/video first, choose an available media artifact, or provide a direct media URL."
                ],
                say_hint="Explain that no playable media was available and ask the user to create, attach, or link it.",
            )

        devices = await asyncio.to_thread(self._cast_devices)
        if not devices:
            return action_failure(
                code="no_cast_devices",
                message="No Google Cast TVs or media players were found.",
                needs=[
                    "Enable the Google Cast integration, keep Tater and the Cast device on the same network, then refresh devices."
                ],
                say_hint="Explain that Tater could not find a Google Cast TV and mention the integration/network check.",
            )

        device, target_error = self._select_device(
            devices,
            args=payload,
            context=context,
            query=query,
        )
        if device is None:
            choices = self._device_choices(devices)
            needs = [target_error]
            if choices:
                needs.append("Choose one of: " + ", ".join(choices) + ".")
            return action_failure(
                code="cast_target_not_resolved",
                message=target_error,
                needs=needs,
                say_hint="Ask which listed TV or Cast device should play the media.",
            )

        device_id = self._text(device.get("id") or device.get("device_id") or device.get("ref"))
        if device_id.lower().startswith("media_player:"):
            device_id = device_id.split(":", 1)[1]
        target = f"integration:google_cast:{quote(device_id, safe='')}"
        device_name = self._text(device.get("name")) or "Google Cast device"

        try:
            from media_playback import play_media_url_targets

            playback = await asyncio.to_thread(
                play_media_url_targets,
                target,
                self._text(media.get("source_url")),
                audio_bytes=(media.get("media_bytes") or None),
                media_type=self._text(media.get("media_type")) or "audio/mpeg",
                media_content_type=self._text(media.get("media_content_type")) or "music",
                filename=self._text(media.get("filename")) or "media.bin",
                title=self._text(media.get("title")) or "Media",
                text=f"Playing on {device_name}.",
                volume_percent=self._volume_percent(payload, query),
                timeout_s=360.0,
                respect_reply_playback=False,
                source_owner="cast_media_verba",
            )
        except Exception as exc:
            return action_failure(
                code="cast_playback_failed",
                message=f"Could not play media on {device_name}: {exc}",
                needs=["Check that the TV is online and reachable from Tater, then try again."],
                say_hint="Explain that casting failed and suggest checking the TV and network.",
            )

        if not isinstance(playback, dict) or playback.get("ok") is not True:
            error = self._text((playback or {}).get("error")) if isinstance(playback, dict) else ""
            return action_failure(
                code="cast_playback_failed",
                message=f"Could not play media on {device_name}: {error or 'Google Cast playback failed.'}",
                needs=["Check that the TV is online and reachable from Tater, then try again."],
                say_hint="Explain that casting failed and suggest checking the TV and network.",
            )

        filename = self._text(media.get("filename")) or "media"
        warnings = [
            self._text(item)
            for item in list(playback.get("warnings") or [])
            if self._text(item)
        ]
        summary = f"Started playing {filename} on {device_name}."
        if warnings:
            summary += f" Playback reported {len(warnings)} warning{'s' if len(warnings) != 1 else ''}."
        return action_success(
            facts={
                "action": "play_media",
                "integration_id": "google_cast",
                "device_id": device_id,
                "device_name": device_name,
                "artifact_id": self._text(media.get("artifact_id")),
                "filename": filename,
                "media_type": self._text(media.get("media_type")),
                "resolved_from": self._text(media.get("resolved_from")),
                "sent_count": int(playback.get("sent_count") or 0),
            },
            data={"warnings": warnings},
            summary_for_user=summary,
            say_hint="Confirm briefly that the media is playing on the named TV.",
        )

    async def handle_webui(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context)

    async def handle_tater_open_webui(self, args=None, llm_client=None, context=None, **_kwargs):
        return await self._handle(args, llm_client, context)

    async def handle_little_spud(self, args=None, llm_client=None, context=None, **_kwargs):
        return await self._handle(args, llm_client, context)

    async def handle_homeassistant(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context)

    async def handle_voice_core(self, args=None, llm_client=None, context=None, **_kwargs):
        return await self._handle(args, llm_client, context)

    async def handle_macos(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context)

    async def handle_xbmc(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context)

    async def handle_homekit(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context)

    async def handle_discord(self, message, args, llm_client):
        payload = dict(args or {})
        if not self._query(payload):
            content = self._text(getattr(message, "content", ""))
            if content:
                payload["query"] = content
        return await self._handle(payload, llm_client)

    async def handle_telegram(self, update, context, args, llm_client):
        payload = dict(args or {})
        if not self._query(payload):
            message = getattr(update, "message", None)
            text = self._text(getattr(message, "text", ""))
            if text:
                payload["query"] = text
        return await self._handle(payload, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        del client, room, sender
        payload = dict(args or {})
        if not self._query(payload) and body:
            payload["query"] = self._text(body)
        return await self._handle(payload, llm_client)

    async def handle_irc(self, bot, channel, user, message, args, llm_client):
        del bot, channel, user
        payload = dict(args or {})
        if not self._query(payload) and message:
            payload["query"] = self._text(message)
        return await self._handle(payload, llm_client)

    async def handle_meshtastic(self, packet, args, llm_client):
        del packet
        return await self._handle(args or {}, llm_client)


verba = CastMediaPlugin()
