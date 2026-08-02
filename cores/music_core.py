"""Provider-neutral music library, voice playback, queue, and built-in player for Tater."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote, urlencode, urlparse

import requests

from helpers import extract_json, get_llm_client_from_env, redis_client

try:
    from helpers import get_primary_llm_client_from_env as _get_primary_llm_client_from_env
except Exception:  # pragma: no cover - compatibility with older Tater runtimes.
    _get_primary_llm_client_from_env = get_llm_client_from_env


__version__ = "2.3.0"
MIN_TATER_VERSION = "98.6"
CORE_DESCRIPTION = (
    "Connect Tater Tube, Plex, Emby, Jellyfin, Navidrome, or Roon to Tater; "
    "browse music, build AI-named recommendations from listening history, and play "
    "voice-controlled queues across satellites, stereo pairs, and media players."
)
TAGS = [
    "music",
    "player",
    "tater-tube",
    "plex",
    "emby",
    "jellyfin",
    "navidrome",
    "roon",
    "satellite",
    "stereo",
    "multi-room",
    "queue",
    "recommendations",
]

logger = logging.getLogger("music_core")
logger.setLevel(logging.INFO)

CORE_SETTINGS = {
    "category": "Music Core Settings",
    "hydra_tools_require_running": True,
    "required": {
        "provider": {
            "label": "Music Provider",
            "type": "select",
            "default": "tater_tube",
            "options": [
                {"value": "tater_tube", "label": "Tater Tube Server"},
                {"value": "plex", "label": "Plex"},
                {"value": "emby", "label": "Emby"},
                {"value": "jellyfin", "label": "Jellyfin"},
                {"value": "navidrome", "label": "Navidrome"},
                {"value": "roon", "label": "Roon"},
            ],
            "description": "Music source used by Music Core. More providers can be added without changing the player.",
        },
        "catalog_sync_interval_seconds": {
            "label": "Catalog Sync Interval (sec)",
            "type": "number",
            "default": 900,
            "description": "How often Music Core refreshes artists, albums, genres, and tracks.",
        },
        "default_targets": {
            "label": "Default Players",
            "type": "text",
            "default": "",
            "description": "Fallback playback destinations when a room or speaking satellite is unavailable.",
        },
        "default_volume_percent": {
            "label": "Default Volume (%)",
            "type": "number",
            "default": 75,
            "description": "Starting volume for Tater satellite media sessions.",
        },
        "default_shuffle": {
            "label": "Shuffle Broad Requests",
            "type": "checkbox",
            "default": True,
            "description": "Shuffle genre, artist, and general music requests by default.",
        },
        "maximum_queue_tracks": {
            "label": "Maximum Queue Tracks",
            "type": "number",
            "default": 200,
            "description": "Maximum number of matched tracks placed in one queue.",
        },
        "recommendations_enabled": {
            "label": "Tater Recommendations",
            "type": "checkbox",
            "default": True,
            "description": "Use listening history and Tater's primary AI model to prepare named music mixes.",
        },
        "recommendation_interval_hours": {
            "label": "Recommendation Refresh (hours)",
            "type": "number",
            "default": 12,
            "description": "How often Music Core refreshes Tater Recommendations in the background.",
        },
        "recommendation_playlist_count": {
            "label": "Recommendation Playlists",
            "type": "number",
            "default": 3,
            "description": "Number of AI-named recommendation playlists to prepare.",
        },
        "recommendation_items_per_playlist": {
            "label": "Albums & Songs Per Playlist",
            "type": "number",
            "default": 6,
            "description": "Maximum recommended albums and songs in each playlist.",
        },
    },
    "tags": TAGS,
}

CORE_WEBUI_TAB = {
    "label": "Music",
    "order": 36,
    "requires_running": True,
}

SETTINGS_KEY = "music_core_settings"
RUNTIME_KEY = "music_core_runtime"
CATALOG_KEY = "music_core_catalog_v1"
PLAYER_KEY = "music_core_player_v1"
HISTORY_KEY = "music_core_listening_history_v1"
RECOMMENDATIONS_KEY = "music_core_recommendations_v1"
REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_SYNC_INTERVAL_SECONDS = 900
MAX_CATALOG_TRACKS = 20000
MAX_SEARCH_RESULTS = 100
MAX_HISTORY_EVENTS = 300
MAX_RECOMMENDATION_CANDIDATES = 200
PROVIDER_LABELS = {
    "tater_tube": "Tater Tube Server",
    "plex": "Plex",
    "emby": "Emby",
    "jellyfin": "Jellyfin",
    "navidrome": "Navidrome",
    "roon": "Roon",
}
CATALOG_PROVIDER_IDS = {"tater_tube", "plex", "emby", "jellyfin", "navidrome"}
GENERIC_SEARCH_WORDS = {
    "a",
    "an",
    "and",
    "for",
    "from",
    "me",
    "music",
    "of",
    "on",
    "play",
    "please",
    "some",
    "song",
    "songs",
    "the",
}

_state_lock = threading.RLock()
_artwork_cache: Dict[str, Dict[str, Any]] = {}
_recommendation_lock = threading.Lock()
_recommendation_started_at = 0.0
_recommendation_thread: Optional[threading.Thread] = None


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace").strip()
    return str(value).strip()


def _list(value: Any) -> List[str]:
    raw: List[Any]
    if isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        token = _text(value)
        parsed: Any = None
        if token.startswith("[") and token.endswith("]"):
            try:
                parsed = json.loads(token)
            except Exception:
                parsed = None
        if isinstance(parsed, list):
            raw = parsed
        else:
            raw = token.replace("\n", ",").split(",") if token else []
    result: List[str] = []
    seen = set()
    for item in raw:
        token = _text(item)
        key = token.casefold()
        if not token or key in seen:
            continue
        seen.add(key)
        result.append(token)
    return result


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _provider_id(value: Any, default: str = "tater_tube") -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "tatertube": "tater_tube",
        "tater_tube_server": "tater_tube",
        "jelly_fin": "jellyfin",
        "navidrome_server": "navidrome",
        "roon_core": "roon",
    }
    token = aliases.get(token, token)
    return token if token in PROVIDER_LABELS else default


def _decode_hash(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    return {_text(key): _text(value) for key, value in raw.items() if _text(key)}


def _settings(client: Any = None) -> Dict[str, str]:
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    try:
        return _decode_hash(store.hgetall(SETTINGS_KEY) or {})
    except Exception:
        return {}


def _runtime(client: Any = None) -> Dict[str, str]:
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    try:
        return _decode_hash(store.hgetall(RUNTIME_KEY) or {})
    except Exception:
        return {}


def _save_hash(client: Any, key: str, values: Dict[str, Any]) -> None:
    if client is None:
        return
    cleaned = {str(key): str(value) for key, value in values.items() if value is not None}
    if cleaned:
        client.hset(key, mapping=cleaned)


def _load_json(client: Any, key: str, default: Any) -> Any:
    if client is None:
        return default
    try:
        raw = client.get(key)
        if raw in (None, ""):
            return default
        return json.loads(_text(raw))
    except Exception:
        return default


def _save_json(client: Any, key: str, value: Any) -> None:
    if client is not None:
        client.set(key, json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _normalize_server_url(value: Any) -> str:
    raw = _text(value).rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Server URL must begin with http:// or https://")
    if raw.endswith("/api"):
        raw = raw[:-4]
    return raw.rstrip("/")


def _api_url(server_url: str, path: str) -> str:
    return f"{server_url.rstrip('/')}/api/{path.lstrip('/')}"


def _unwrap_response(response: Any) -> Any:
    try:
        body = response.json()
    except Exception:
        body = {}
    if not bool(getattr(response, "ok", False)):
        error = body.get("error") if isinstance(body, dict) else {}
        message = error.get("message") if isinstance(error, dict) else ""
        detail = _text(message) or f"Music provider returned HTTP {getattr(response, 'status_code', 0)}."
        if int(getattr(response, "status_code", 0) or 0) == 401:
            raise PermissionError(detail)
        raise RuntimeError(detail)
    if isinstance(body, dict) and body.get("success") is False:
        error = body.get("error")
        message = error.get("message") if isinstance(error, dict) else error
        raise RuntimeError(_text(message) or "The music provider rejected the request.")
    if isinstance(body, dict) and "data" in body:
        return body.get("data")
    return body


@dataclass
class TaterTubeMusicProvider:
    server_url: str
    token: str
    provider_id = "tater_tube"

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "TaterTubeMusicProvider":
        return cls(
            server_url=_normalize_server_url(
                settings.get("tater_tube_server_url") or settings.get("server_url")
            ),
            token=_text(settings.get("tater_tube_token") or settings.get("token")),
        )

    @property
    def connected(self) -> bool:
        return bool(self.server_url and self.token)

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
        authenticated: bool = True,
    ) -> Any:
        if not self.server_url:
            raise ValueError("Tater Tube Server URL is not configured.")
        headers = {"Accept": "application/json"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if authenticated and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        response = requests.request(
            method.upper(),
            _api_url(self.server_url, path),
            headers=headers,
            json=payload,
            timeout=max(5, int(timeout)),
        )
        return _unwrap_response(response)

    @classmethod
    def pair(cls, server_url: str, pin: str, name: str) -> Dict[str, Any]:
        provider = cls(server_url=_normalize_server_url(server_url), token="")
        return provider.request(
            "POST",
            "tater/players/pair",
            payload={"pin": pin, "name": name},
            authenticated=False,
        )

    def stream_url(self, track: Dict[str, Any]) -> str:
        category_id = _text(track.get("category_id"))
        if category_id.startswith("local:"):
            category_id = category_id[len("local:") :]
        path = _text(track.get("path"))
        if not category_id or not path:
            return _text(track.get("stream_url"))
        query = urlencode(
            {
                "category_id": category_id,
                "source": _as_int(track.get("source_index"), 0, 0, 10000),
                "path": path,
                "player_token": self.token,
            }
        )
        return f"{self.server_url}/api/tater/local/stream?{query}"

    def artwork_url(self, track: Dict[str, Any]) -> str:
        if not _as_bool(track.get("has_artwork"), False):
            return ""
        category_id = _text(track.get("category_id"))
        if category_id.startswith("local:"):
            category_id = category_id[len("local:") :]
        path = _text(track.get("path"))
        if not category_id or not path:
            return ""
        query = urlencode(
            {
                "category_id": category_id,
                "source": _as_int(track.get("source_index"), 0, 0, 10000),
                "path": path,
                "player_token": self.token,
            }
        )
        return f"{self.server_url}/api/tater/music/artwork?{query}"

    def catalog(self) -> Dict[str, Any]:
        try:
            data = self.request(
                "GET",
                f"tater/music/catalog?limit={MAX_CATALOG_TRACKS}",
                timeout=180,
            )
            if isinstance(data, dict) and isinstance(data.get("tracks"), list):
                return data
        except RuntimeError as exc:
            if "HTTP 404" not in _text(exc):
                raise
        return self._legacy_catalog()

    def _legacy_catalog(self) -> Dict[str, Any]:
        libraries_payload = self.request("GET", "tater/music/libraries", timeout=60)
        libraries = libraries_payload.get("libraries") if isinstance(libraries_payload, dict) else []
        tracks: List[Dict[str, Any]] = []
        library_names: Dict[str, str] = {}
        for library in libraries if isinstance(libraries, list) else []:
            if not isinstance(library, dict):
                continue
            library_id = _text(library.get("ratingKey") or library.get("key"))
            if not library_id:
                continue
            library_names[library_id] = _text(library.get("title"))
            albums_payload = self.request(
                "GET",
                f"tater/music/albums?category_id={quote(library_id, safe='')}",
                timeout=180,
            )
            albums = albums_payload.get("albums") if isinstance(albums_payload, dict) else []
            for album in albums if isinstance(albums, list) else []:
                if not isinstance(album, dict):
                    continue
                album_id = _text(album.get("ratingKey") or album.get("key"))
                if not album_id:
                    continue
                track_payload = self.request(
                    "GET",
                    f"tater/music/tracks?album_id={quote(album_id, safe='')}",
                    timeout=180,
                )
                rows = track_payload.get("tracks") if isinstance(track_payload, dict) else []
                for row in rows if isinstance(rows, list) else []:
                    if not isinstance(row, dict):
                        continue
                    item = dict(row)
                    item.setdefault("album", album.get("title"))
                    item.setdefault("artist", album.get("artist"))
                    item.setdefault("albumArtist", album.get("albumArtist"))
                    item.setdefault("genres", album.get("genres"))
                    tracks.append(item)
        return {
            "catalog_id": "",
            "tracks": tracks,
            "total": len(tracks),
            "libraries": library_names,
            "artists": [],
            "albums": [],
            "genres": [],
            "legacy": True,
        }


def _catalog_fingerprint(provider_id: str, rows: Iterable[Dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    digest.update(_text(provider_id).encode("utf-8"))
    for row in rows:
        digest.update(
            "\x00".join(
                [
                    _text(row.get("id") or row.get("ratingKey") or row.get("key")),
                    _text(row.get("title")),
                    _text(row.get("artist")),
                    _text(row.get("album")),
                    _text(row.get("duration_seconds") or row.get("duration")),
                    _text(row.get("size_bytes") or row.get("size")),
                ]
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()[:24]


@dataclass
class PlexMusicProvider:
    server_url: str
    token: str
    library_ids: List[str]
    provider_id = "plex"

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "PlexMusicProvider":
        return cls(
            server_url=_normalize_server_url(settings.get("plex_server_url")),
            token=_text(settings.get("plex_token")),
            library_ids=_list(settings.get("plex_library_ids")),
        )

    @property
    def connected(self) -> bool:
        return bool(self.server_url and self.token)

    def request(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ) -> Any:
        if not self.server_url:
            raise ValueError("Plex Server URL is not configured.")
        response = requests.get(
            f"{self.server_url}/{path.lstrip('/')}",
            headers={
                "Accept": "application/json",
                "X-Plex-Token": self.token,
                "X-Plex-Client-Identifier": "tater-music-core",
                "X-Plex-Product": "Tater Music Core",
                "X-Plex-Version": __version__,
            },
            params=params or {},
            timeout=max(5, int(timeout)),
        )
        return _unwrap_response(response)

    def stream_url(self, track: Dict[str, Any]) -> str:
        path = _text(track.get("stream_path"))
        if not path:
            return ""
        separator = "&" if "?" in path else "?"
        return f"{self.server_url}/{path.lstrip('/')}{separator}{urlencode({'X-Plex-Token': self.token})}"

    def catalog(self) -> Dict[str, Any]:
        payload = self.request("library/sections", timeout=60)
        container = payload.get("MediaContainer") if isinstance(payload, dict) else {}
        directories = container.get("Directory") if isinstance(container, dict) else []
        libraries: Dict[str, str] = {}
        tracks: List[Dict[str, Any]] = []
        selected = {value.casefold() for value in self.library_ids}
        for section in directories if isinstance(directories, list) else []:
            if not isinstance(section, dict) or _text(section.get("type")).lower() != "artist":
                continue
            section_id = _text(section.get("key"))
            title = _text(section.get("title")) or f"Plex Music {section_id}"
            if not section_id:
                continue
            if selected and section_id.casefold() not in selected and title.casefold() not in selected:
                continue
            libraries[section_id] = title
            offset = 0
            page_size = 1000
            while len(tracks) < MAX_CATALOG_TRACKS:
                page = self.request(
                    f"library/sections/{quote(section_id, safe='')}/all",
                    params={
                        "type": 10,
                        "includeMeta": 1,
                        "X-Plex-Container-Start": offset,
                        "X-Plex-Container-Size": min(
                            page_size,
                            MAX_CATALOG_TRACKS - len(tracks),
                        ),
                    },
                    timeout=180,
                )
                page_container = page.get("MediaContainer") if isinstance(page, dict) else {}
                metadata = (
                    page_container.get("Metadata")
                    if isinstance(page_container, dict)
                    else []
                )
                rows = metadata if isinstance(metadata, list) else []
                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    media = item.get("Media") if isinstance(item.get("Media"), list) else []
                    first_media = media[0] if media and isinstance(media[0], dict) else {}
                    parts = (
                        first_media.get("Part")
                        if isinstance(first_media.get("Part"), list)
                        else []
                    )
                    part = parts[0] if parts and isinstance(parts[0], dict) else {}
                    genres = [
                        _text(value.get("tag"))
                        for value in (
                            item.get("Genre")
                            if isinstance(item.get("Genre"), list)
                            else []
                        )
                        if isinstance(value, dict) and _text(value.get("tag"))
                    ]
                    tracks.append(
                        {
                            "id": f"plex:{_text(item.get('ratingKey'))}",
                            "title": item.get("title"),
                            "artist": item.get("grandparentTitle") or item.get("originalTitle"),
                            "album_artist": item.get("grandparentTitle"),
                            "album": item.get("parentTitle"),
                            "genres": genres,
                            "year": item.get("year"),
                            "track_number": item.get("index"),
                            "disc_number": item.get("parentIndex"),
                            "duration_seconds": _as_float(item.get("duration")) / 1000.0,
                            "size_bytes": part.get("size"),
                            "stream_path": part.get("key"),
                            "container": part.get("container") or first_media.get("container"),
                            "provider": self.provider_id,
                        }
                    )
                    if len(tracks) >= MAX_CATALOG_TRACKS:
                        break
                total = _as_int(
                    page_container.get("totalSize")
                    if isinstance(page_container, dict)
                    else len(rows),
                    len(rows),
                    0,
                    10**9,
                )
                offset += len(rows)
                if not rows or offset >= total:
                    break
        return {
            "catalog_id": _catalog_fingerprint(self.provider_id, tracks),
            "tracks": tracks,
            "total": len(tracks),
            "libraries": libraries,
        }


@dataclass
class MediaBrowserMusicProvider:
    server_url: str
    api_key: str
    user_id: str
    provider_id: str

    @classmethod
    def from_settings(
        cls,
        settings: Dict[str, Any],
        provider_id: str,
    ) -> "MediaBrowserMusicProvider":
        return cls(
            server_url=_normalize_server_url(settings.get(f"{provider_id}_server_url")),
            api_key=_text(settings.get(f"{provider_id}_api_key")),
            user_id=_text(settings.get(f"{provider_id}_user_id")),
            provider_id=provider_id,
        )

    @property
    def connected(self) -> bool:
        return bool(self.server_url and self.api_key)

    @property
    def api_prefix(self) -> str:
        if self.provider_id != "emby":
            return ""
        return "" if self.server_url.lower().endswith("/emby") else "/emby"

    def api_url(self, path: str) -> str:
        return f"{self.server_url}{self.api_prefix}/{path.lstrip('/')}"

    def request(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ) -> Any:
        if not self.server_url:
            raise ValueError(f"{PROVIDER_LABELS[self.provider_id]} Server URL is not configured.")
        response = requests.get(
            self.api_url(path),
            headers={
                "Accept": "application/json",
                "X-Emby-Token": self.api_key,
                "X-MediaBrowser-Token": self.api_key,
            },
            params=params or {},
            timeout=max(5, int(timeout)),
        )
        return _unwrap_response(response)

    def resolved_user_id(self) -> str:
        if self.user_id:
            return self.user_id
        users = self.request("Users", timeout=60)
        for user in users if isinstance(users, list) else []:
            if isinstance(user, dict) and _text(user.get("Id")):
                self.user_id = _text(user.get("Id"))
                return self.user_id
        raise ValueError(
            f"{PROVIDER_LABELS[self.provider_id]} did not return a user. Enter the User ID explicitly."
        )

    def stream_url(self, track: Dict[str, Any]) -> str:
        item_id = _text(track.get("provider_track_id"))
        if not item_id:
            return ""
        container = "".join(
            char
            for char in _text(track.get("container") or "mp3").lower()
            if char.isalnum()
        ) or "mp3"
        query = urlencode({"static": "true", "api_key": self.api_key})
        stream_path = f"Audio/{quote(item_id, safe='')}/stream.{container}"
        return f"{self.api_url(stream_path)}?{query}"

    def catalog(self) -> Dict[str, Any]:
        user_id = self.resolved_user_id()
        tracks: List[Dict[str, Any]] = []
        offset = 0
        page_size = 1000
        while len(tracks) < MAX_CATALOG_TRACKS:
            payload = self.request(
                f"Users/{quote(user_id, safe='')}/Items",
                params={
                    "IncludeItemTypes": "Audio",
                    "Recursive": "true",
                    "Fields": (
                        "Genres,MediaSources,Path,Album,AlbumArtist,Artists,"
                        "ProductionYear,IndexNumber,ParentIndexNumber"
                    ),
                    "SortBy": "AlbumArtist,Album,SortName",
                    "SortOrder": "Ascending",
                    "StartIndex": offset,
                    "Limit": min(page_size, MAX_CATALOG_TRACKS - len(tracks)),
                },
                timeout=180,
            )
            items = payload.get("Items") if isinstance(payload, dict) else []
            rows = items if isinstance(items, list) else []
            for item in rows:
                if not isinstance(item, dict):
                    continue
                sources = (
                    item.get("MediaSources")
                    if isinstance(item.get("MediaSources"), list)
                    else []
                )
                source = sources[0] if sources and isinstance(sources[0], dict) else {}
                path = _text(source.get("Path") or item.get("Path"))
                container = _text(source.get("Container")) or Path(path).suffix.lstrip(".")
                artists = item.get("Artists") if isinstance(item.get("Artists"), list) else []
                album_artists = (
                    item.get("AlbumArtists")
                    if isinstance(item.get("AlbumArtists"), list)
                    else []
                )
                album_artist = _text(item.get("AlbumArtist"))
                if not album_artist and album_artists:
                    first = album_artists[0]
                    album_artist = _text(
                        first.get("Name") if isinstance(first, dict) else first
                    )
                first_artist = artists[0] if artists else ""
                artist = _text(
                    first_artist.get("Name")
                    if isinstance(first_artist, dict)
                    else first_artist
                )
                tracks.append(
                    {
                        "id": f"{self.provider_id}:{_text(item.get('Id'))}",
                        "provider_track_id": item.get("Id"),
                        "title": item.get("Name"),
                        "artist": artist or album_artist,
                        "album_artist": album_artist,
                        "album": item.get("Album"),
                        "genres": item.get("Genres"),
                        "year": item.get("ProductionYear"),
                        "track_number": item.get("IndexNumber"),
                        "disc_number": item.get("ParentIndexNumber"),
                        "duration_seconds": _as_float(
                            source.get("RunTimeTicks") or item.get("RunTimeTicks")
                        )
                        / 10_000_000.0,
                        "size_bytes": source.get("Size"),
                        "path": path,
                        "container": container,
                        "provider": self.provider_id,
                    }
                )
                if len(tracks) >= MAX_CATALOG_TRACKS:
                    break
            total = _as_int(
                payload.get("TotalRecordCount") if isinstance(payload, dict) else len(rows),
                len(rows),
                0,
                10**9,
            )
            offset += len(rows)
            if not rows or offset >= total:
                break
        return {
            "catalog_id": _catalog_fingerprint(self.provider_id, tracks),
            "tracks": tracks,
            "total": len(tracks),
            "libraries": {},
            "resolved_user_id": user_id,
        }


@dataclass
class NavidromeMusicProvider:
    server_url: str
    username: str
    password: str
    api_key: str
    provider_id = "navidrome"

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "NavidromeMusicProvider":
        return cls(
            server_url=_normalize_server_url(settings.get("navidrome_server_url")),
            username=_text(settings.get("navidrome_username")),
            password=_text(settings.get("navidrome_password")),
            api_key=_text(settings.get("navidrome_api_key")),
        )

    @property
    def connected(self) -> bool:
        return bool(
            self.server_url
            and (self.api_key or (self.username and self.password))
        )

    def auth_params(self) -> Dict[str, str]:
        base = {"v": "1.16.1", "c": "TaterMusicCore", "f": "json"}
        if self.api_key:
            return {**base, "apiKey": self.api_key}
        salt = uuid.uuid4().hex[:16]
        token = hashlib.md5(f"{self.password}{salt}".encode("utf-8")).hexdigest()
        return {**base, "u": self.username, "s": salt, "t": token}

    def request(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        if not self.server_url:
            raise ValueError("Navidrome Server URL is not configured.")
        response = requests.get(
            f"{self.server_url}/rest/{endpoint.lstrip('/')}",
            params={**self.auth_params(), **(params or {})},
            headers={"Accept": "application/json"},
            timeout=max(5, int(timeout)),
        )
        body = _unwrap_response(response)
        envelope = body.get("subsonic-response") if isinstance(body, dict) else {}
        if not isinstance(envelope, dict):
            raise RuntimeError("Navidrome returned an invalid Subsonic response.")
        if _text(envelope.get("status")).lower() != "ok":
            error = envelope.get("error") if isinstance(envelope.get("error"), dict) else {}
            raise PermissionError(_text(error.get("message")) or "Navidrome authentication failed.")
        return envelope

    def stream_url(self, track: Dict[str, Any]) -> str:
        item_id = _text(track.get("provider_track_id"))
        if not item_id:
            return ""
        query = urlencode({**self.auth_params(), "id": item_id, "format": "raw"})
        return f"{self.server_url}/rest/stream.view?{query}"

    def catalog(self) -> Dict[str, Any]:
        tracks: List[Dict[str, Any]] = []
        offset = 0
        page_size = 500
        while len(tracks) < MAX_CATALOG_TRACKS:
            envelope = self.request(
                "search3.view",
                params={
                    "query": "",
                    "artistCount": 0,
                    "albumCount": 0,
                    "songCount": min(page_size, MAX_CATALOG_TRACKS - len(tracks)),
                    "songOffset": offset,
                },
                timeout=180,
            )
            search = (
                envelope.get("searchResult3")
                if isinstance(envelope.get("searchResult3"), dict)
                else {}
            )
            songs = search.get("song") if isinstance(search.get("song"), list) else []
            for song in songs:
                if not isinstance(song, dict):
                    continue
                genre_rows = song.get("genres") if isinstance(song.get("genres"), list) else []
                genres = [
                    _text(row.get("name") or row.get("value"))
                    for row in genre_rows
                    if isinstance(row, dict)
                ]
                if not genres and _text(song.get("genre")):
                    genres = [_text(song.get("genre"))]
                tracks.append(
                    {
                        "id": f"navidrome:{_text(song.get('id'))}",
                        "provider_track_id": song.get("id"),
                        "title": song.get("title"),
                        "artist": song.get("artist"),
                        "album_artist": song.get("displayAlbumArtist") or song.get("artist"),
                        "album": song.get("album"),
                        "genres": genres,
                        "year": song.get("year"),
                        "track_number": song.get("track"),
                        "disc_number": song.get("discNumber"),
                        "duration_seconds": song.get("duration"),
                        "size_bytes": song.get("size"),
                        "path": song.get("path"),
                        "container": song.get("suffix"),
                        "media_type": song.get("contentType"),
                        "provider": self.provider_id,
                    }
                )
                if len(tracks) >= MAX_CATALOG_TRACKS:
                    break
            offset += len(songs)
            if len(songs) < page_size:
                break
        return {
            "catalog_id": _catalog_fingerprint(self.provider_id, tracks),
            "tracks": tracks,
            "total": len(tracks),
            "libraries": {},
        }


class RoonMusicProvider:
    provider_id = "roon"

    @property
    def connected(self) -> bool:
        try:
            from tateros import integration_store as integration_store_module

            module = integration_store_module.integration_module("roon")
            status_fn = getattr(module, "integration_status", None) if module else None
            status = status_fn() if callable(status_fn) else {}
            return bool(
                isinstance(status, dict)
                and status.get("enabled")
                and status.get("configured")
            )
        except Exception:
            return False


def _provider(client: Any = None, provider_id: Any = "") -> Any:
    cfg = _settings(client)
    selected = _text(provider_id or cfg.get("provider") or "tater_tube").lower()
    if selected == "tater_tube":
        return TaterTubeMusicProvider.from_settings(cfg)
    if selected == "plex":
        return PlexMusicProvider.from_settings(cfg)
    if selected in {"emby", "jellyfin"}:
        return MediaBrowserMusicProvider.from_settings(cfg, selected)
    if selected == "navidrome":
        return NavidromeMusicProvider.from_settings(cfg)
    if selected == "roon":
        return RoonMusicProvider()
    raise ValueError(f"Unsupported music provider: {selected}")


def _paired(
    settings: Optional[Dict[str, Any]] = None,
    provider_id: Any = "",
) -> bool:
    cfg = settings if isinstance(settings, dict) else _settings()
    selected = _text(provider_id or cfg.get("provider") or "tater_tube").lower()
    try:
        return bool(_provider_from_settings(cfg, selected).connected)
    except Exception:
        return False


def _provider_from_settings(settings: Dict[str, Any], provider_id: str) -> Any:
    selected = _text(provider_id or "tater_tube").lower()
    if selected == "tater_tube":
        return TaterTubeMusicProvider.from_settings(settings)
    if selected == "plex":
        return PlexMusicProvider.from_settings(settings)
    if selected in {"emby", "jellyfin"}:
        return MediaBrowserMusicProvider.from_settings(settings, selected)
    if selected == "navidrome":
        return NavidromeMusicProvider.from_settings(settings)
    if selected == "roon":
        return RoonMusicProvider()
    raise ValueError(f"Unsupported music provider: {selected}")


def _genres(value: Any, fallback: Any = "") -> List[str]:
    raw: List[Any]
    if isinstance(value, list):
        raw = value
    else:
        text = _text(value or fallback)
        for separator in (";", "|"):
            text = text.replace(separator, ",")
        raw = text.split(",") if text else []
    result: List[str] = []
    seen = set()
    for item in raw:
        genre = _text(item)
        key = genre.casefold()
        if not genre or key in seen:
            continue
        seen.add(key)
        result.append(genre)
    return result


def _normalize_track(row: Dict[str, Any]) -> Dict[str, Any]:
    track_id = _text(row.get("ratingKey") or row.get("rating_key") or row.get("key") or row.get("id"))
    title = _text(row.get("title")) or "Untitled"
    artist = _text(row.get("artist"))
    album_artist = _text(row.get("albumArtist") or row.get("album_artist"))
    album = _text(row.get("album"))
    genres = _genres(row.get("genres"), row.get("genre"))
    duration = _as_float(row.get("durationSeconds") or row.get("duration_seconds") or row.get("duration"))
    if duration > 100000:
        duration /= 1000.0
    source_index = _as_int(row.get("sourceIndex") or row.get("source_index"), 0, 0, 10000)
    path = _text(row.get("path") or row.get("partKey") or row.get("part_key"))
    provider_id = _text(row.get("provider") or "tater_tube").lower()
    if not track_id:
        identity = "\x00".join(
            [
                _text(row.get("categoryId") or row.get("category_id")),
                str(source_index),
                path,
                artist,
                album,
                title,
            ]
        )
        track_id = "track:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return {
        "id": track_id,
        "title": title,
        "artist": artist,
        "album_artist": album_artist,
        "album": album,
        "genres": genres,
        "genre": ", ".join(genres),
        "year": _text(row.get("date") or row.get("year")),
        "track_number": _as_int(row.get("index") or row.get("track_number"), 0, 0, 10000),
        "disc_number": _as_int(
            row.get("disc") or row.get("disc_number"),
            0,
            0,
            1000,
        ),
        "duration_seconds": max(0.0, duration),
        "duration_display": _text(row.get("durationDisplay") or row.get("duration_display")),
        "category_id": _text(row.get("categoryId") or row.get("category_id")),
        "source_index": source_index,
        "path": path,
        "stream_path": _text(row.get("stream_path")),
        "provider_track_id": _text(
            row.get("provider_track_id")
            or row.get("ratingKey")
            or row.get("rating_key")
            or row.get("key")
        ),
        "container": _text(row.get("container") or Path(path).suffix.lstrip(".")).lower(),
        "media_type": _text(row.get("media_type") or row.get("content_type")).lower(),
        "size_bytes": _as_int(row.get("sizeBytes") or row.get("size_bytes"), 0, 0, 10**15),
        "modified_unix": _as_int(row.get("modifiedUnix") or row.get("modified_unix"), 0, 0, 10**12),
        "has_artwork": (
            _as_bool(row.get("hasArtwork"), False)
            if row.get("hasArtwork") is not None
            else _as_bool(row.get("has_artwork"), bool(_text(row.get("poster"))))
        ),
        "provider": provider_id,
    }


def _facet_values(tracks: Iterable[Dict[str, Any]], key: str) -> List[str]:
    values: Dict[str, str] = {}
    for track in tracks:
        raw_values = track.get(key)
        if isinstance(raw_values, list):
            candidates = raw_values
        else:
            candidates = [raw_values]
        for raw in candidates:
            value = _text(raw)
            if value:
                values[value.casefold()] = value
    return sorted(values.values(), key=str.casefold)


def _catalog(client: Any = None, provider_id: Any = "") -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, CATALOG_KEY, {})
    if not isinstance(payload, dict):
        return {}
    selected = _text(provider_id or _settings(store).get("provider") or "tater_tube").lower()
    cached_provider = _text(payload.get("provider") or "tater_tube").lower()
    return payload if cached_provider == selected else {}


def _sync_catalog(client: Any = None, provider_id: Any = "") -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    selected = _text(provider_id or _settings(store).get("provider") or "tater_tube").lower()
    if selected == "roon":
        raise ValueError(
            "Roon resolves its library through the Roon Core at play time and does not expose streamable tracks."
        )
    provider = _provider(store, selected)
    if not provider.connected:
        raise ValueError(
            f"Connect {PROVIDER_LABELS.get(selected, selected)} before syncing its music library."
        )
    raw = provider.catalog()
    tracks = [
        _normalize_track(row)
        for row in (raw.get("tracks") if isinstance(raw, dict) else []) or []
        if isinstance(row, dict)
    ]
    artists = _facet_values(
        [
            {
                "artist": _text(track.get("album_artist")) or _text(track.get("artist")),
            }
            for track in tracks
        ],
        "artist",
    )
    albums = _facet_values(tracks, "album")
    genres = _facet_values(tracks, "genres")
    payload = {
        "provider": selected,
        "catalog_id": _text(raw.get("catalog_id")) if isinstance(raw, dict) else "",
        "tracks": tracks,
        "artists": artists,
        "albums": albums,
        "genres": genres,
        "libraries": raw.get("libraries") if isinstance(raw, dict) and isinstance(raw.get("libraries"), dict) else {},
        "synced_at": time.time(),
        "legacy_provider_api": bool(raw.get("legacy")) if isinstance(raw, dict) else False,
    }
    resolved_user_id = _text(raw.get("resolved_user_id")) if isinstance(raw, dict) else ""
    if resolved_user_id and selected in {"emby", "jellyfin"}:
        _save_hash(store, SETTINGS_KEY, {f"{selected}_user_id": resolved_user_id})
    _save_json(store, CATALOG_KEY, payload)
    _save_hash(
        store,
        RUNTIME_KEY,
        {
            "status": "connected",
            "provider": selected,
            "last_sync_at": payload["synced_at"],
            "last_error": "",
            "track_count": len(tracks),
            "artist_count": len(artists),
            "album_count": len(albums),
            "genre_count": len(genres),
        },
    )
    return payload


def _clean_query_tokens(value: Any) -> List[str]:
    cleaned = "".join(char.lower() if char.isalnum() else " " for char in _text(value))
    tokens = [token for token in cleaned.split() if token and token not in GENERIC_SEARCH_WORDS]
    return tokens


def _track_haystack(track: Dict[str, Any]) -> str:
    return " ".join(
        [
            _text(track.get("title")),
            _text(track.get("artist")),
            _text(track.get("album_artist")),
            _text(track.get("album")),
            _text(track.get("genre")),
            _text(track.get("year")),
        ]
    ).casefold()


def _matches_filter(track: Dict[str, Any], key: str, value: Any) -> bool:
    wanted = _text(value).casefold()
    if not wanted:
        return True
    if key == "genre":
        values = [_text(item).casefold() for item in track.get("genres") or []]
        return any(wanted in item for item in values)
    if key == "artist":
        value = f"{_text(track.get('artist'))} {_text(track.get('album_artist'))}".casefold()
        return wanted in value
    return wanted in _text(track.get(key)).casefold()


def _score_track(track: Dict[str, Any], filters: Dict[str, Any]) -> int:
    score = 0
    for key, weight in (("title", 140), ("artist", 100), ("album", 80), ("genre", 60)):
        wanted = _text(filters.get(key)).casefold()
        if not wanted:
            continue
        if key == "genre":
            values = [_text(item).casefold() for item in track.get("genres") or []]
        elif key == "artist":
            values = [_text(track.get("artist")).casefold(), _text(track.get("album_artist")).casefold()]
        else:
            values = [_text(track.get(key)).casefold()]
        if wanted in values:
            score += weight
        elif any(wanted in item for item in values):
            score += max(1, weight // 2)
    query_tokens = _clean_query_tokens(filters.get("query"))
    haystack = _track_haystack(track)
    score += sum(12 for token in query_tokens if token in haystack)
    return score


def _search_tracks(
    *,
    query: Any = "",
    title: Any = "",
    artist: Any = "",
    album: Any = "",
    genre: Any = "",
    limit: int = MAX_SEARCH_RESULTS,
    client: Any = None,
    provider_id: Any = "",
) -> List[Dict[str, Any]]:
    payload = _catalog(client, provider_id)
    tracks = payload.get("tracks") if isinstance(payload.get("tracks"), list) else []
    filters = {
        "query": _text(query),
        "title": _text(title),
        "artist": _text(artist),
        "album": _text(album),
        "genre": _text(genre),
    }
    query_tokens = _clean_query_tokens(query)
    rows: List[Dict[str, Any]] = []
    for track in tracks:
        if not isinstance(track, dict):
            continue
        if any(
            not _matches_filter(track, key, filters[key])
            for key in ("title", "artist", "album", "genre")
            if filters[key]
        ):
            continue
        haystack = _track_haystack(track)
        if query_tokens and any(token not in haystack for token in query_tokens):
            continue
        rows.append({**track, "_score": _score_track(track, filters)})
    rows.sort(
        key=lambda row: (
            -_as_int(row.get("_score"), 0, 0, 100000),
            _text(row.get("album_artist") or row.get("artist")).casefold(),
            _text(row.get("album")).casefold(),
            _as_int(row.get("track_number"), 0, 0, 10000),
            _text(row.get("title")).casefold(),
        )
    )
    result = []
    for row in rows[: max(1, min(1000, int(limit)))]:
        cleaned = dict(row)
        cleaned.pop("_score", None)
        result.append(cleaned)
    return result


def _public_track(track: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        key: track.get(key)
        for key in (
            "id",
            "title",
            "artist",
            "album_artist",
            "album",
            "genres",
            "genre",
            "year",
            "track_number",
            "duration_seconds",
            "duration_display",
            "provider",
        )
    }
    result["artwork_url"] = _artwork_display_url(track) if track else ""
    return result


def _player(client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, PLAYER_KEY, {})
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("status", "idle")
    payload.setdefault("provider", _provider_id(_settings(store).get("provider")))
    payload.setdefault("queue", [])
    payload.setdefault("index", -1)
    targets = _normalize_stereo_targets(payload.get("targets") or payload.get("target"))
    payload["targets"] = targets
    payload["target"] = targets[0] if targets else ""
    payload.setdefault("shuffle", False)
    payload.setdefault("repeat", "off")
    return payload


def _save_player(player: Dict[str, Any], client: Any = None) -> None:
    store = client or globals().get("redis_client")
    targets = _normalize_stereo_targets(player.get("targets") or player.get("target"))
    player["targets"] = targets
    player["target"] = targets[0] if targets else ""
    player["updated_at"] = time.time()
    _save_json(store, PLAYER_KEY, player)


def _listening_history(client: Any = None) -> List[Dict[str, Any]]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, HISTORY_KEY, [])
    if not isinstance(payload, list):
        return []
    return [dict(row) for row in payload if isinstance(row, dict)]


def _record_listening_history(
    track: Dict[str, Any],
    targets: Any = None,
    *,
    client: Any = None,
) -> None:
    """Record successful starts without retaining credentials or stream URLs."""
    store = client or globals().get("redis_client")
    track_id = _text(track.get("id"))
    title = _text(track.get("title"))
    if store is None or not (track_id or title):
        return
    now = time.time()
    history = _listening_history(store)
    if history:
        latest = history[-1]
        if (
            _text(latest.get("track_id")) == track_id
            and now - _as_float(latest.get("played_at")) < 30.0
        ):
            return
    history.append(
        {
            "track_id": track_id,
            "title": title or "Untitled",
            "artist": _text(track.get("artist") or track.get("album_artist")),
            "album_artist": _text(track.get("album_artist") or track.get("artist")),
            "album": _text(track.get("album")),
            "genres": [_text(value) for value in track.get("genres") or [] if _text(value)],
            "provider": _provider_id(track.get("provider")),
            "targets": _list(targets),
            "played_at": now,
        }
    )
    _save_json(store, HISTORY_KEY, history[-MAX_HISTORY_EVENTS:])


def _recommendations(client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, RECOMMENDATIONS_KEY, {})
    return payload if isinstance(payload, dict) else {}


def _recommendation_candidates(
    catalog: Dict[str, Any],
    history: List[Dict[str, Any]],
    *,
    limit: int = MAX_RECOMMENDATION_CANDIDATES,
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    if not tracks:
        return [], {}

    artist_counts: Dict[str, int] = {}
    album_counts: Dict[str, int] = {}
    genre_counts: Dict[str, int] = {}
    recent_track_ids = set()
    recent_albums = set()
    for event in history[-120:]:
        artist = _text(event.get("album_artist") or event.get("artist")).casefold()
        album = _text(event.get("album")).casefold()
        if artist:
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
        if album:
            album_counts[album] = album_counts.get(album, 0) + 1
        for genre in event.get("genres") or []:
            token = _text(genre).casefold()
            if token:
                genre_counts[token] = genre_counts.get(token, 0) + 1
    for event in history[-40:]:
        track_id = _text(event.get("track_id"))
        album = _text(event.get("album")).casefold()
        if track_id:
            recent_track_ids.add(track_id)
        if album:
            recent_albums.add(album)

    def affinity(track: Dict[str, Any]) -> int:
        artist = _text(track.get("album_artist") or track.get("artist")).casefold()
        album = _text(track.get("album")).casefold()
        genres = [_text(value).casefold() for value in track.get("genres") or []]
        return (
            artist_counts.get(artist, 0) * 5
            + album_counts.get(album, 0) * 3
            + sum(genre_counts.get(genre, 0) * 2 for genre in genres if genre)
        )

    ordered_tracks = sorted(
        tracks,
        key=lambda track: (
            _text(track.get("id")) in recent_track_ids,
            -affinity(track),
            _text(track.get("artist") or track.get("album_artist")).casefold(),
            _text(track.get("album")).casefold(),
            _as_int(track.get("disc_number"), 0, 0, 1000),
            _as_int(track.get("track_number"), 0, 0, 10000),
            _text(track.get("title")).casefold(),
        ),
    )

    albums: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for track in tracks:
        artist = _text(track.get("album_artist") or track.get("artist"))
        album = _text(track.get("album"))
        if not album:
            continue
        albums.setdefault((artist.casefold(), album.casefold()), []).append(track)

    ranked_albums = sorted(
        albums.items(),
        key=lambda entry: (
            entry[0][1] in recent_albums,
            -max(affinity(track) for track in entry[1]),
            entry[0],
        ),
    )
    candidates: List[Dict[str, Any]] = []
    candidate_map: Dict[str, Dict[str, Any]] = {}
    album_limit = min(80, max(1, limit // 2))
    for (_artist_key, _album_key), album_tracks in ranked_albums[:album_limit]:
        album_tracks = sorted(
            album_tracks,
            key=lambda track: (
                _as_int(track.get("disc_number"), 0, 0, 1000),
                _as_int(track.get("track_number"), 0, 0, 10000),
                _text(track.get("title")).casefold(),
            ),
        )
        hero = next(
            (track for track in album_tracks if _as_bool(track.get("has_artwork"), False)),
            album_tracks[0],
        )
        artist = _text(hero.get("album_artist") or hero.get("artist"))
        album = _text(hero.get("album")) or "Untitled album"
        candidate_id = "album:" + hashlib.sha256(
            f"{_provider_id(hero.get('provider'))}\x00{artist.casefold()}\x00{album.casefold()}".encode("utf-8")
        ).hexdigest()[:18]
        candidate = {
            "id": candidate_id,
            "type": "album",
            "title": album,
            "artist": artist,
            "album": album,
            "genres": sorted(
                {
                    _text(genre)
                    for track in album_tracks
                    for genre in track.get("genres") or []
                    if _text(genre)
                }
            )[:8],
            "track_count": len(album_tracks),
            "track_ids": [_text(track.get("id")) for track in album_tracks if _text(track.get("id"))],
            "image_track_id": _text(hero.get("id")),
        }
        candidates.append({key: value for key, value in candidate.items() if key not in {"track_ids", "image_track_id"}})
        candidate_map[candidate_id] = candidate

    song_limit = max(0, limit - len(candidates))
    for track in ordered_tracks[:song_limit]:
        track_id = _text(track.get("id"))
        if not track_id:
            continue
        candidate_id = f"song:{track_id}"
        artist = _text(track.get("artist") or track.get("album_artist"))
        candidate = {
            "id": candidate_id,
            "type": "song",
            "title": _text(track.get("title")) or "Untitled",
            "artist": artist,
            "album": _text(track.get("album")),
            "genres": [_text(value) for value in track.get("genres") or [] if _text(value)][:8],
            "track_count": 1,
            "track_ids": [track_id],
            "image_track_id": track_id,
        }
        candidates.append({key: value for key, value in candidate.items() if key not in {"track_ids", "image_track_id"}})
        candidate_map[candidate_id] = candidate
    return candidates[:limit], candidate_map


def _music_llm_json(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    system_prompt: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    if llm_client is None:
        raise RuntimeError("No primary LLM is configured for Tater.")
    response = loop.run_until_complete(
        llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            max_tokens=None,
            temperature=0.4,
        )
    )
    raw = _text(((response or {}).get("message") or {}).get("content"))
    blob = extract_json(raw) or raw
    try:
        parsed = json.loads(blob)
    except Exception as exc:
        raise RuntimeError("The music recommendation model did not return valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("The music recommendation model returned an unsupported response.")
    return parsed


def _generate_recommendations_impl(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    client: Any = None,
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    provider_id = _provider_id(cfg.get("provider"))
    if provider_id not in CATALOG_PROVIDER_IDS:
        raise ValueError("Tater Recommendations require a catalog-based music provider.")
    if not _paired(cfg, provider_id):
        raise ValueError(f"Connect {PROVIDER_LABELS[provider_id]} before making recommendations.")
    history = [
        row
        for row in _listening_history(store)
        if _provider_id(row.get("provider")) == provider_id
    ]
    if not history:
        raise ValueError("Play at least one song before asking Tater for recommendations.")
    catalog = _catalog(store, provider_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(store, provider_id)
    candidates, candidate_map = _recommendation_candidates(catalog, history)
    if not candidates:
        raise ValueError("The active music library has no recommendation candidates.")

    playlist_count = _as_int(cfg.get("recommendation_playlist_count"), 3, 1, 6)
    item_count = _as_int(cfg.get("recommendation_items_per_playlist"), 6, 3, 12)
    artist_counts: Dict[str, int] = {}
    genre_counts: Dict[str, int] = {}
    for event in history[-120:]:
        artist = _text(event.get("album_artist") or event.get("artist"))
        if artist:
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
        for genre in event.get("genres") or []:
            token = _text(genre)
            if token:
                genre_counts[token] = genre_counts.get(token, 0) + 1
    recent = [
        {
            "title": _text(event.get("title")),
            "artist": _text(event.get("artist") or event.get("album_artist")),
            "album": _text(event.get("album")),
            "genres": list(event.get("genres") or [])[:6],
        }
        for event in reversed(history[-40:])
    ]
    result = _music_llm_json(
        loop,
        llm_client,
        (
            "You are Tater, a warm, imaginative personal music curator. Build named playlists from the "
            "listener's recent history and the supplied catalog candidates. Choose only exact candidate IDs "
            "from the catalog. Mix familiar taste with useful discovery, avoid filling every playlist with "
            "the same artist or album, and let playlists mix albums and individual songs when that makes sense. "
            "Give every playlist a short memorable name and a one-sentence description. Give every selection "
            "a short reason. Return JSON only in this exact shape: "
            '{"summary":"one friendly sentence","playlists":[{"name":"playlist name",'
            '"description":"one sentence","items":[{"candidate_id":"exact id","reason":"short reason"}]}]}. '
            f"Return up to {playlist_count} playlists with up to {item_count} unique selections in each."
        ),
        {
            "listening_patterns": {
                "top_artists": sorted(artist_counts.items(), key=lambda row: (-row[1], row[0]))[:20],
                "top_genres": sorted(genre_counts.items(), key=lambda row: (-row[1], row[0]))[:20],
            },
            "recent_listening": recent,
            "catalog_candidates": candidates,
        },
    )

    playlists: List[Dict[str, Any]] = []
    for position, raw_playlist in enumerate(result.get("playlists") or []):
        if not isinstance(raw_playlist, dict) or len(playlists) >= playlist_count:
            continue
        selections: List[Dict[str, Any]] = []
        flattened_track_ids: List[str] = []
        seen_candidates = set()
        seen_tracks = set()
        for raw_item in raw_playlist.get("items") or []:
            if isinstance(raw_item, str):
                raw_item = {"candidate_id": raw_item}
            if not isinstance(raw_item, dict) or len(selections) >= item_count:
                continue
            candidate_id = _text(raw_item.get("candidate_id"))
            candidate = candidate_map.get(candidate_id)
            if not candidate or candidate_id in seen_candidates:
                continue
            seen_candidates.add(candidate_id)
            track_ids = [
                track_id
                for track_id in candidate.get("track_ids") or []
                if track_id and track_id not in seen_tracks
            ]
            if not track_ids:
                continue
            seen_tracks.update(track_ids)
            flattened_track_ids.extend(track_ids)
            selections.append(
                {
                    "candidate_id": candidate_id,
                    "type": candidate["type"],
                    "title": candidate["title"],
                    "artist": candidate["artist"],
                    "album": candidate["album"],
                    "track_count": candidate["track_count"],
                    "track_ids": track_ids,
                    "image_track_id": candidate.get("image_track_id"),
                    "reason": _text(raw_item.get("reason"))[:240]
                    or "Tater thinks this fits the mood of this mix.",
                }
            )
        if not selections:
            continue
        playlists.append(
            {
                "id": uuid.uuid4().hex[:12],
                "name": _text(raw_playlist.get("name"))[:80] or f"Tater Mix {position + 1}",
                "description": _text(raw_playlist.get("description"))[:300]
                or "A fresh mix shaped by what has been playing lately.",
                "items": selections,
                "track_ids": flattened_track_ids,
            }
        )
    if not playlists:
        raise RuntimeError("The music recommendation model did not select any valid catalog items.")

    now = time.time()
    published = {
        "provider": provider_id,
        "generated_at": now,
        "summary": _text(result.get("summary"))[:500]
        or "Tater made a few fresh mixes from what has been playing lately.",
        "history_event_count": len(history),
        "playlists": playlists,
    }
    _save_json(store, RECOMMENDATIONS_KEY, published)
    _save_hash(
        store,
        RUNTIME_KEY,
        {
            "last_recommendation_at": now,
            "last_recommendation_attempt_at": now,
            "last_recommendation_error": "",
            "last_recommendation_error_at": "",
        },
    )
    return published


def _generate_recommendations(
    client: Any = None,
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    llm_client: Any = None,
) -> Dict[str, Any]:
    global _recommendation_started_at
    if not _recommendation_lock.acquire(blocking=False):
        raise RuntimeError("Tater music recommendations are already being refreshed.")
    _recommendation_started_at = time.time()
    owns_loop = loop is None
    active_loop = loop or asyncio.new_event_loop()
    if owns_loop:
        asyncio.set_event_loop(active_loop)
    try:
        model = llm_client if llm_client is not None else _get_primary_llm_client_from_env()
        return _generate_recommendations_impl(active_loop, model, client)
    except Exception as exc:
        now = time.time()
        _save_hash(
            client or globals().get("redis_client"),
            RUNTIME_KEY,
            {
                "last_recommendation_attempt_at": now,
                "last_recommendation_error": _text(exc)[:500],
                "last_recommendation_error_at": now,
            },
        )
        raise
    finally:
        if owns_loop:
            active_loop.close()
            asyncio.set_event_loop(None)
        _recommendation_started_at = 0.0
        _recommendation_lock.release()


def _schedule_recommendation_refresh(client: Any = None) -> bool:
    """Start one detached refresh so model latency never pauses queue advancement."""
    global _recommendation_thread
    with _state_lock:
        if _recommendation_thread is not None and _recommendation_thread.is_alive():
            return False

        def worker() -> None:
            try:
                _generate_recommendations(client)
            except Exception as exc:
                logger.warning("[Music] recommendation refresh failed: %s", exc)

        _recommendation_thread = threading.Thread(
            target=worker,
            name="music-recommendations",
            daemon=True,
        )
        _recommendation_thread.start()
        return True


def _origin_value(origin: Dict[str, Any], *keys: str) -> str:
    nested = origin.get("origin") if isinstance(origin.get("origin"), dict) else {}
    for key in keys:
        value = _text(origin.get(key) if origin.get(key) not in (None, "") else nested.get(key))
        if value:
            return value
    return ""


def _stereo_member_target_map() -> Dict[str, str]:
    """Map each configured stereo member to its pair playback target."""
    try:
        from tater_voice import stereo_pairs

        pairs = stereo_pairs.list_pairs()
    except Exception:
        return {}

    routes: Dict[str, str] = {}
    for pair in pairs if isinstance(pairs, list) else []:
        if not isinstance(pair, dict):
            continue
        pair_selector = _text(pair.get("selector"))
        if not pair_selector:
            pair_id = _text(pair.get("id"))
            pair_selector = f"stereo:{pair_id}" if pair_id else ""
        if not pair_selector:
            continue
        pair_target = (
            pair_selector
            if pair_selector.lower().startswith("voice_core:")
            else f"voice_core:{pair_selector}"
        )
        for key in ("left_selector", "right_selector"):
            member_selector = _text(pair.get(key))
            if not member_selector:
                continue
            member_target = (
                member_selector
                if member_selector.lower().startswith("voice_core:")
                else f"voice_core:{member_selector}"
            )
            routes[member_selector.casefold()] = pair_target
            routes[member_target.casefold()] = pair_target
    return routes


def _normalize_stereo_targets(value: Any) -> List[str]:
    """Replace paired satellite selections with one deduplicated stereo target."""
    routes = _stereo_member_target_map()
    return _list([routes.get(target.casefold(), target) for target in _list(value)])


def _target_options(
    current_values: Any = None,
    provider_id: Any = "",
    *,
    include_stereo_members: bool = False,
) -> List[Dict[str, str]]:
    try:
        from announcement_targets import build_announcement_target_options

        rows = build_announcement_target_options(
            homeassistant_base_url="",
            homeassistant_token="",
            include_homeassistant=True,
            include_sonos=True,
            include_voice_core=True,
            include_integrations=True,
            current_values=current_values,
        )
        options = [
            dict(row)
            for row in rows
            if isinstance(row, dict) and _text(row.get("value"))
        ]
        try:
            from integration_registry import get_integration_devices_by_capability

            roon_devices = get_integration_devices_by_capability(
                "media_player",
                globals().get("redis_client"),
            )
            known = {_text(row.get("value")) for row in options}
            for device in roon_devices:
                if not isinstance(device, dict):
                    continue
                if _text(device.get("integration_id")).lower() != "roon":
                    continue
                device_id = _text(device.get("id") or device.get("ref"))
                if not device_id:
                    continue
                value = f"integration:roon:{quote(device_id, safe='')}"
                if value in known:
                    continue
                known.add(value)
                name = _text(device.get("name")) or device_id
                room = _text(device.get("room") or device.get("area"))
                suffix = f" • {room}" if room and room.casefold() != name.casefold() else ""
                options.append({"value": value, "label": f"Roon: {name}{suffix}"})
        except Exception as exc:
            logger.debug("[Music] Roon target discovery unavailable: %s", exc)

        selected = _provider_id(provider_id, "") if _text(provider_id) else ""
        if selected == "roon":
            options = [
                row
                for row in options
                if _text(row.get("value")).lower().startswith("integration:roon:")
            ]
        elif selected in CATALOG_PROVIDER_IDS:
            options = [
                row
                for row in options
                if not _text(row.get("value")).lower().startswith("integration:roon:")
            ]
        if not include_stereo_members:
            paired_members = _stereo_member_target_map()
            options = [
                row
                for row in options
                if _text(row.get("value")).casefold() not in paired_members
            ]
        return sorted(options, key=lambda row: _text(row.get("label")).casefold())
    except Exception as exc:
        logger.debug("[Music] target discovery unavailable: %s", exc)
        return []


def _target_from_query(value: Any, options: Optional[List[Dict[str, str]]] = None) -> str:
    token = _text(value)
    if not token:
        return ""
    lower = token.casefold()
    if lower.startswith(("voice_core:", "ha:", "sonos:", "integration:")):
        return token
    candidates = options if isinstance(options, list) else _target_options()
    exact = [
        _text(row.get("value"))
        for row in candidates
        if lower in {_text(row.get("value")).casefold(), _text(row.get("label")).casefold()}
    ]
    if exact:
        return exact[0]
    partial = [
        _text(row.get("value"))
        for row in candidates
        if lower in _text(row.get("label")).casefold()
    ]
    return partial[0] if len(partial) == 1 else ""


def _preferred_room_target(room_names: Iterable[str], client: Any = None) -> str:
    names = [_text(value) for value in room_names if _text(value)]
    if not names:
        return ""
    try:
        from integration_registry import get_integration_room_preferred_media_player

        result = get_integration_room_preferred_media_player(names, client or redis_client)
        return _text(result.get("target")) if isinstance(result, dict) else ""
    except Exception:
        return ""


def _resolve_targets(
    requested: Any = "",
    *,
    room: Any = "",
    origin: Optional[Dict[str, Any]] = None,
    client: Any = None,
    provider_id: Any = "",
) -> List[str]:
    store = client or globals().get("redis_client")
    requested_values = _list(requested)
    options = _target_options(
        current_values=requested_values,
        provider_id=provider_id,
        include_stereo_members=True,
    )
    if requested_values:
        explicit = [_target_from_query(value, options) for value in requested_values]
        if any(not target for target in explicit):
            return []
        return _normalize_stereo_targets(explicit)

    context = origin if isinstance(origin, dict) else {}
    room_names = _list(room)
    if not room_names:
        room_names = _list(_origin_value(context, "room_name", "area_name", "room_id", "area_id"))
    if room_names:
        resolved_rooms: List[str] = []
        option_values = {
            _text(row.get("value")).casefold()
            for row in options
            if _text(row.get("value"))
        }
        for room_name in room_names:
            preferred = _preferred_room_target([room_name], store)
            if preferred and option_values and preferred.casefold() not in option_values:
                preferred = ""
            target = preferred or _target_from_query(room_name, options)
            if not target:
                return []
            resolved_rooms.append(target)
        return _normalize_stereo_targets(resolved_rooms)

    selector = _origin_value(
        context,
        "satellite_selector",
        "voice_core_selector",
        "device_selector",
    )
    if selector:
        return _normalize_stereo_targets(
            [selector if selector.startswith("voice_core:") else f"voice_core:{selector}"]
        )
    cfg = _settings(store)
    defaults = _list(cfg.get("default_targets") or cfg.get("default_target"))
    resolved_defaults = [_target_from_query(value, options) for value in defaults]
    return _normalize_stereo_targets([target for target in resolved_defaults if target])


def _resolve_target(
    requested: Any = "",
    *,
    room: Any = "",
    origin: Optional[Dict[str, Any]] = None,
    client: Any = None,
    provider_id: Any = "",
) -> str:
    targets = _resolve_targets(
        requested,
        room=room,
        origin=origin,
        client=client,
        provider_id=provider_id,
    )
    return targets[0] if targets else ""


def _target_summary(targets: Any) -> str:
    values = _list(targets)
    if not values:
        return "no players"
    if len(values) == 1:
        return values[0]
    return f"{len(values)} destinations"


def _track_label(track: Dict[str, Any]) -> str:
    title = _text(track.get("title")) or "Untitled"
    artist = _text(track.get("artist") or track.get("album_artist"))
    return f"{title} by {artist}" if artist else title


def _track_media_type(track: Dict[str, Any]) -> str:
    declared = _text(track.get("media_type")).split(";", 1)[0].strip().lower()
    if declared.startswith("audio/"):
        return declared
    extension = Path(_text(track.get("path"))).suffix.lower()
    if not extension and _text(track.get("container")):
        extension = "." + _text(track.get("container")).lower().lstrip(".")
    return {
        ".aac": "audio/aac",
        ".aiff": "audio/aiff",
        ".alac": "audio/x-alac",
        ".flac": "audio/flac",
        ".m4a": "audio/mp4",
        ".m4b": "audio/mp4",
        ".mp3": "audio/mpeg",
        ".ogg": "audio/ogg",
        ".opus": "audio/ogg",
        ".wav": "audio/wav",
        ".wma": "audio/x-ms-wma",
    }.get(extension, "application/octet-stream")


def _play_track(
    track: Dict[str, Any],
    targets: Any,
    *,
    volume_percent: int,
    client: Any = None,
) -> Dict[str, Any]:
    provider = _provider(client, track.get("provider"))
    source_url = provider.stream_url(track)
    if not source_url:
        raise RuntimeError(f"No stream is available for {_track_label(track)}.")
    from media_playback import play_media_url_targets

    duration = max(0.0, _as_float(track.get("duration_seconds")))
    result = play_media_url_targets(
        _list(targets),
        source_url,
        media_type=_track_media_type(track),
        media_content_type="music",
        filename=Path(_text(track.get("path")) or "music-track").name,
        text=f"Playing {_track_label(track)}.",
        volume_percent=volume_percent,
        timeout_s=max(180.0, duration + 120.0),
        respect_reply_playback=False,
    )
    if not isinstance(result, dict) or result.get("ok") is False:
        raise RuntimeError(_text((result or {}).get("error")) or "Music playback failed.")
    return result


def _stop_target(targets: Any) -> List[str]:
    warnings: List[str] = []
    try:
        from announcement_targets import split_announcement_targets

        grouped = split_announcement_targets(_list(targets))
    except Exception as exc:
        return [_text(exc)]

    selectors = list(grouped.get("voice_core_selectors") or [])
    if selectors:
        try:
            from tater_voice import native_satellite, stereo_pairs

            for selector in selectors:
                members = [selector]
                pair = stereo_pairs.get_pair(selector) if stereo_pairs.is_stereo_selector(selector) else {}
                if isinstance(pair, dict) and pair:
                    members = [
                        _text(pair.get("left_selector")),
                        _text(pair.get("right_selector")),
                    ]
                for member in members:
                    if not member:
                        continue
                    try:
                        native_satellite.run_on_runtime_loop(
                            native_satellite.send_command(
                                member,
                                "media.session.stop",
                                {"reason": "music_core_stop"},
                            ),
                            timeout=8.0,
                        )
                    except Exception as exc:
                        warnings.append(f"{member}: {exc}")
        except Exception as exc:
            warnings.append(_text(exc))

    integration_targets = list(grouped.get("integration_devices") or [])
    integration_targets.extend(
        {"integration_id": "homeassistant", "device_id": entity_id}
        for entity_id in grouped.get("homeassistant_media_players") or []
    )
    integration_targets.extend(
        {"integration_id": "sonos", "device_id": speaker}
        for speaker in grouped.get("sonos_speakers") or []
    )
    if integration_targets:
        try:
            from integration_registry import run_integration_device_action

            for row in integration_targets:
                try:
                    run_integration_device_action(
                        _text(row.get("integration_id")),
                        "stop",
                        _text(row.get("device_id")),
                        {},
                    )
                except Exception as exc:
                    warnings.append(
                        f"{_text(row.get('integration_id'))}:{_text(row.get('device_id'))}: {exc}"
                    )
        except Exception as exc:
            warnings.append(_text(exc))
    return warnings


def _start_player_index(index: int, *, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if not queue:
            raise ValueError("The music queue is empty.")
        if index < 0 or index >= len(queue):
            raise ValueError("The requested queue position is unavailable.")
        targets = _list(player.get("targets") or player.get("target"))
        if not targets:
            raise ValueError("Choose at least one satellite or media player before playing music.")
        track = queue[index]
        cfg = _settings(store)
        saved_volume = player.get("volume_percent")
        if saved_volume in (None, ""):
            saved_volume = cfg.get("default_volume_percent")
        volume = _as_int(
            saved_volume,
            75,
            0,
            100,
        )
        if _text(player.get("status")).lower() == "playing":
            _stop_target(targets)
        result = _play_track(track, targets, volume_percent=volume, client=store)
        playback_result = {
            key: result.get(key)
            for key in (
                "target_count",
                "sent_count",
                "homeassistant_target_count",
                "voice_core_sent_count",
                "sonos_sent_count",
                "integration_sent_count",
                "media_session_sent_count",
                "media_session_fallback_count",
            )
            if result.get(key) is not None
        }
        voice_core_sessions = [
            dict(row)
            for row in list(result.get("voice_core_sessions") or [])
            if isinstance(row, dict) and _text(row.get("session_id"))
        ]
        if voice_core_sessions:
            playback_result["voice_core_sessions"] = voice_core_sessions
        player.update(
            {
                "status": "playing",
                "index": index,
                "current": track,
                "started_at": time.time(),
                "duration_seconds": max(0.0, _as_float(track.get("duration_seconds"))),
                "volume_percent": volume,
                "last_error": "",
                "playback_result": playback_result,
                "warnings": [
                    _text(value)
                    for value in list(result.get("warnings") or [])
                    if _text(value)
                ],
            }
        )
        _save_player(player, store)
        _record_listening_history(track, targets, client=store)
        return player


def _create_and_start_queue(
    tracks: List[Dict[str, Any]],
    *,
    targets: List[str],
    shuffle: bool,
    volume_percent: int,
    client: Any = None,
) -> Dict[str, Any]:
    if not tracks:
        raise ValueError("No matching music was found.")
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    maximum = _as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000)
    original_queue = [dict(track) for track in tracks[:maximum]]
    queue = [dict(track) for track in original_queue]
    if shuffle and len(queue) > 1:
        random.SystemRandom().shuffle(queue)
    with _state_lock:
        previous = _player(store)
        old_targets = _list(previous.get("targets") or previous.get("target"))
        if previous.get("status") == "playing" and old_targets:
            _stop_target(old_targets)
        player = {
            "status": "queued",
            "provider": _provider_id(queue[0].get("provider"), _provider_id(cfg.get("provider"))),
            "queue": queue,
            "queue_original": original_queue,
            "index": 0,
            "current": queue[0],
            "targets": _list(targets),
            "shuffle": bool(shuffle),
            "repeat": _text(previous.get("repeat") or "off"),
            "volume_percent": volume_percent,
            "created_at": time.time(),
            "started_at": 0.0,
            "duration_seconds": 0.0,
            "last_error": "",
        }
        _save_player(player, store)
    return _start_player_index(0, client=store)


def _advance_player(direction: int, *, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if not queue:
            raise ValueError("The music queue is empty.")
        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        if direction < 0 and time.time() - _as_float(player.get("started_at")) > 8:
            next_index = index
        else:
            next_index = index + (1 if direction >= 0 else -1)
        repeat = _text(player.get("repeat") or "off").lower()
        if next_index >= len(queue):
            if repeat == "all":
                next_index = 0
            else:
                _stop_target(player.get("targets") or player.get("target"))
                player.update({"status": "finished", "index": len(queue) - 1, "started_at": 0.0})
                _save_player(player, store)
                return player
        if next_index < 0:
            next_index = len(queue) - 1 if repeat == "all" else 0
    return _start_player_index(next_index, client=store)


def _set_player_shuffle(enabled: bool, *, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        queue = [dict(track) for track in list(player.get("queue") or []) if isinstance(track, dict)]
        if not queue:
            player["shuffle"] = bool(enabled)
            _save_player(player, store)
            return player

        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        original = [
            dict(track)
            for track in list(player.get("queue_original") or queue)
            if isinstance(track, dict)
        ]
        player["queue_original"] = original
        if enabled:
            remaining = queue[index + 1 :]
            random.SystemRandom().shuffle(remaining)
        else:
            used = queue[: index + 1]

            def track_token(track: Dict[str, Any]) -> str:
                return _text(track.get("id") or track.get("url") or track.get("stream_url")) or json.dumps(
                    track,
                    sort_keys=True,
                    default=str,
                )

            used_counts: Dict[str, int] = {}
            for track in used:
                token = track_token(track)
                used_counts[token] = used_counts.get(token, 0) + 1
            remaining = []
            for track in original:
                token = track_token(track)
                if used_counts.get(token, 0) > 0:
                    used_counts[token] -= 1
                    continue
                remaining.append(dict(track))
        player["queue"] = [*queue[: index + 1], *remaining]
        player["shuffle"] = bool(enabled)
        _save_player(player, store)
        return player


def _stop_player(*, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    with _state_lock:
        player = _player(store)
        targets = _list(player.get("targets") or player.get("target"))
        warnings = _stop_target(targets) if targets else []
        player.update({"status": "stopped", "started_at": 0.0})
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store)
        return player


def _advance_finished_player(client: Any = None) -> None:
    store = client or globals().get("redis_client")
    player = _reconcile_native_playback(_player(store), store)
    if _text(player.get("status")).lower() != "playing":
        return
    if _provider_id(player.get("provider")) == "roon":
        return
    duration = _as_float(player.get("duration_seconds"))
    started = _as_float(player.get("started_at"))
    if duration <= 0 or started <= 0 or time.time() < started + duration + 1.0:
        return
    try:
        if _text(player.get("repeat")).lower() == "one":
            _start_player_index(_as_int(player.get("index"), 0, 0, 100000), client=store)
        else:
            _advance_player(1, client=store)
    except Exception as exc:
        player = _player(store)
        player.update({"status": "error", "last_error": _text(exc)[:500]})
        _save_player(player, store)


def _reconcile_native_playback(player: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    if _text(player.get("status")).lower() != "playing":
        return player
    playback_result = (
        player.get("playback_result")
        if isinstance(player.get("playback_result"), dict)
        else {}
    )
    sessions = [
        row
        for row in list(playback_result.get("voice_core_sessions") or [])
        if isinstance(row, dict) and _text(row.get("session_id"))
    ]
    if not sessions:
        return player
    try:
        from tater_voice import native_satellite

        snapshot = native_satellite.status_snapshot_sync()
    except Exception:
        return player
    clients = snapshot.get("clients") if isinstance(snapshot, dict) else {}
    if not isinstance(clients, dict):
        return player

    failed: List[str] = []
    for session in sessions:
        session_id = _text(session.get("session_id"))
        selectors = _list(session.get("selectors") or session.get("target"))
        states = []
        for selector in selectors:
            client_row = clients.get(selector)
            media_session = (
                client_row.get("media_session")
                if isinstance(client_row, dict) and isinstance(client_row.get("media_session"), dict)
                else {}
            )
            if _text(media_session.get("session_id")) == session_id:
                states.append(media_session)
        if not states or any(bool(state.get("active")) for state in states):
            continue
        finished_states = [state for state in states if _as_float(state.get("finished_ts")) > 0]
        if finished_states and any(state.get("ok") is False for state in finished_states):
            failed.append(_text(session.get("target")) or ", ".join(selectors))

    if not failed:
        return player
    warning = "Playback failed on " + ", ".join(failed) + "."
    warnings = [_text(value) for value in list(player.get("warnings") or []) if _text(value)]
    if warning not in warnings:
        warnings.append(warning)
    player["warnings"] = warnings
    sent_count = _as_int(playback_result.get("sent_count"), len(sessions), 0, 10000)
    voice_core_sent_count = _as_int(
        playback_result.get("voice_core_sent_count"),
        len(sessions),
        0,
        10000,
    )
    all_dispatched_targets_are_tracked_native_sessions = (
        len(sessions) >= voice_core_sent_count and sent_count <= voice_core_sent_count
    )
    if len(failed) == len(sessions) and all_dispatched_targets_are_tracked_native_sessions:
        player["status"] = "error"
        player["last_error"] = warning
        player["started_at"] = 0.0
    _save_player(player, client)
    return player


def _roon_device_targets(targets: Any) -> List[Dict[str, str]]:
    try:
        from announcement_targets import split_announcement_targets

        grouped = split_announcement_targets(_list(targets))
    except Exception as exc:
        raise ValueError(f"Could not read Roon zones: {exc}") from exc
    unsupported = (
        list(grouped.get("voice_core_selectors") or [])
        + list(grouped.get("homeassistant_media_players") or [])
        + list(grouped.get("sonos_speakers") or [])
        + list(grouped.get("unifi_protect_cameras") or [])
    )
    devices = [
        dict(row)
        for row in list(grouped.get("integration_devices") or [])
        if isinstance(row, dict)
    ]
    roon_devices = [
        row for row in devices if _text(row.get("integration_id")).lower() == "roon"
    ]
    if unsupported or len(roon_devices) != len(devices):
        raise ValueError(
            "Roon library playback can target only Roon zones. "
            "Roon does not expose its audio files for direct satellite or other media-player streaming."
        )
    if not roon_devices:
        raise ValueError("Choose one or more Roon zones for Roon playback.")
    return roon_devices


def _validate_catalog_provider_targets(targets: Any) -> None:
    roon_targets = [
        target
        for target in _list(targets)
        if target.lower().startswith("integration:roon:")
    ]
    if roon_targets:
        raise ValueError(
            "Roon zones can play only from the Roon provider. Choose Roon as the "
            "Music Provider or select satellites and other supported media players."
        )


def _roon_media_query(args: Dict[str, Any]) -> tuple[str, str]:
    title = _text(args.get("title") or args.get("track") or args.get("song"))
    artist = _text(args.get("artist"))
    album = _text(args.get("album"))
    genre = _text(args.get("genre"))
    query = _text(args.get("query") or args.get("music"))
    if title:
        query = f"{title} by {artist}" if artist else title
        return query, "track"
    if album:
        query = f"{album} by {artist}" if artist else album
        return query, "album"
    if artist:
        return artist, "artist"
    if genre:
        return genre, "genre"
    return query, _text(args.get("media_kind") or "any").lower()


def _roon_play_request(
    args: Dict[str, Any],
    origin: Optional[Dict[str, Any]],
    client: Any,
) -> Dict[str, Any]:
    requested_targets = (
        args.get("targets")
        or args.get("target")
        or args.get("destinations")
        or args.get("destination")
        or args.get("players")
        or args.get("player")
    )
    targets = _resolve_targets(
        requested_targets,
        room=args.get("rooms") or args.get("room"),
        origin=origin,
        client=client,
        provider_id="roon",
    )
    devices = _roon_device_targets(targets)
    query, media_kind = _roon_media_query(args)
    shuffle = _as_bool(args.get("shuffle"), media_kind in {"artist", "genre", "radio", "any"})
    if not query and not shuffle:
        raise ValueError("Tell Music Core what to play from Roon.")

    from integration_registry import run_integration_device_action

    successes: List[Dict[str, Any]] = []
    failures: List[str] = []
    for device in devices:
        device_id = _text(device.get("device_id"))
        try:
            result = run_integration_device_action(
                "roon",
                "play_media",
                device_id,
                {
                    "query": query,
                    "media_kind": media_kind,
                    "random": shuffle,
                },
            )
            if isinstance(result, dict) and result.get("ok") is False:
                failures.append(
                    f"{device_id}: {_text(result.get('message') or result.get('error')) or 'failed'}"
                )
            else:
                successes.append(result if isinstance(result, dict) else {"ok": True})
        except Exception as exc:
            failures.append(f"{device_id}: {exc}")
    if not successes:
        raise RuntimeError("; ".join(failures) or "Roon playback failed.")

    player = {
        "status": "playing",
        "provider": "roon",
        "queue": [],
        "index": -1,
        "current": {
            "id": "roon:dynamic",
            "title": query or "Random music",
            "artist": "",
            "album": "",
            "genres": [media_kind] if media_kind == "genre" else [],
            "genre": media_kind if media_kind == "genre" else "",
            "provider": "roon",
        },
        "targets": targets,
        "shuffle": shuffle,
        "repeat": "off",
        "started_at": time.time(),
        "duration_seconds": 0.0,
        "warnings": failures,
        "playback_result": {
            "target_count": len(targets),
            "sent_count": len(successes),
        },
    }
    _save_player(player, client)
    _record_listening_history(player["current"], targets, client=client)
    return {
        "ok": True,
        "provider": "roon",
        "target": targets[0],
        "targets": targets,
        "target_count": len(targets),
        "queue_count": 0,
        "shuffle": shuffle,
        "warnings": failures,
        "now_playing": _public_track(player["current"]),
        "summary_for_user": (
            f"Playing {query or 'music'} from Roon on {_target_summary(targets)}."
        ),
    }


def _roon_control(action: str, *, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    player = _player(store)
    targets = _list(player.get("targets") or player.get("target"))
    devices = _roon_device_targets(targets)
    control = {
        "replay": "play",
        "resume": "play",
        "play": "play",
        "previous": "previous",
        "next": "next",
        "pause": "pause",
        "stop": "stop",
    }.get(_text(action).lower())
    if not control:
        raise ValueError("Roon supports play, pause, stop, next, and previous controls.")
    from integration_registry import run_integration_device_action

    failures: List[str] = []
    sent = 0
    for device in devices:
        device_id = _text(device.get("device_id"))
        try:
            result = run_integration_device_action("roon", control, device_id, {})
            if isinstance(result, dict) and result.get("ok") is False:
                failures.append(
                    f"{device_id}: {_text(result.get('message') or result.get('error')) or 'failed'}"
                )
            else:
                sent += 1
        except Exception as exc:
            failures.append(f"{device_id}: {exc}")
    if sent <= 0:
        raise RuntimeError("; ".join(failures) or f"Roon {control} failed.")
    player["status"] = "stopped" if control == "stop" else "playing"
    player["warnings"] = failures
    _save_player(player, store)
    return player


def _play_request(args: Dict[str, Any], origin: Optional[Dict[str, Any]], client: Any) -> Dict[str, Any]:
    cfg = _settings(client)
    selected_provider = _provider_id(args.get("provider"), _provider_id(cfg.get("provider")))
    if _text(args.get("provider")) and selected_provider != _provider_id(cfg.get("provider")):
        _save_hash(client, SETTINGS_KEY, {"provider": selected_provider})
        cfg["provider"] = selected_provider
    if selected_provider == "roon":
        return _roon_play_request(args, origin, client)

    catalog = _catalog(client, selected_provider)
    if not isinstance(catalog.get("tracks"), list) or not catalog.get("tracks"):
        catalog = _sync_catalog(client, selected_provider)
    query = _text(args.get("query") or args.get("music"))
    title = _text(args.get("title") or args.get("track") or args.get("song"))
    artist = _text(args.get("artist"))
    album = _text(args.get("album"))
    genre = _text(args.get("genre"))
    matches = _search_tracks(
        query=query,
        title=title,
        artist=artist,
        album=album,
        genre=genre,
        limit=_as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000),
        client=client,
        provider_id=selected_provider,
    )
    requested_targets = (
        args.get("targets")
        or args.get("target")
        or args.get("destinations")
        or args.get("destination")
        or args.get("players")
        or args.get("player")
    )
    targets = _resolve_targets(
        requested_targets,
        room=args.get("rooms") or args.get("room"),
        origin=origin,
        client=client,
        provider_id=selected_provider,
    )
    if not targets:
        raise ValueError("Choose one or more satellites, stereo pairs, or media players for this music.")
    _validate_catalog_provider_targets(targets)
    broad_request = bool(genre or artist or (query and not title and not album))
    shuffle = _as_bool(
        args.get("shuffle"),
        _as_bool(cfg.get("default_shuffle"), True) if broad_request else False,
    )
    requested_volume = args.get("volume_percent")
    if requested_volume in (None, ""):
        requested_volume = args.get("volume")
    if requested_volume in (None, ""):
        requested_volume = cfg.get("default_volume_percent")
    volume = _as_int(requested_volume, 75, 0, 100)
    player = _create_and_start_queue(
        matches,
        targets=targets,
        shuffle=shuffle,
        volume_percent=volume,
        client=client,
    )
    return {
        "ok": True,
        "provider": selected_provider,
        "target": targets[0],
        "targets": targets,
        "target_count": len(targets),
        "queue_count": len(player.get("queue") or []),
        "shuffle": bool(player.get("shuffle")),
        "warnings": list(player.get("warnings") or []),
        "now_playing": _public_track(player.get("current") or {}),
        "summary_for_user": (
            f"Playing {_track_label(player.get('current') or {})} on {_target_summary(targets)}. "
            f"The queue has {len(player.get('queue') or [])} track"
            f"{'' if len(player.get('queue') or []) == 1 else 's'}."
        ),
    }


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    return [
        {
            "id": "music_play",
            "description": (
                "Play music from Music Core by song, artist, album, genre, or natural-language query. "
                "Use the speaking room automatically unless the user names one or more destinations. "
                "Pass multiple rooms or players as JSON arrays."
            ),
            "usage": (
                '{"function":"music_play","arguments":{"query":"reggae music","genre":"reggae",'
                '"artist":"","album":"","title":"","targets":["Kitchen","Living Room"],'
                '"rooms":[],"provider":"tater_tube|plex|emby|jellyfin|navidrome|roon",'
                '"shuffle":true,"volume_percent":75}}'
            ),
        },
        {
            "id": "music_search",
            "description": "Search Music Core without starting playback.",
            "usage": (
                '{"function":"music_search","arguments":{"query":"","genre":"","artist":"","album":"","title":"",'
                '"provider":"tater_tube|plex|emby|jellyfin|navidrome","limit":10}}'
            ),
        },
        {
            "id": "music_control",
            "description": (
                "Control the Music Core queue: next, previous, stop, replay, shuffle, repeat, "
                "or set one or more playback destinations."
            ),
            "usage": (
                '{"function":"music_control","arguments":'
                '{"action":"next|previous|stop|replay|shuffle|repeat|set_targets",'
                '"targets":["Kitchen","Living Room"],"enabled":true,"mode":"off|all|one"}}'
            ),
        },
        {
            "id": "music_now_playing",
            "description": "Read the current Music Core track, queue, target, and playback state.",
            "usage": '{"function":"music_now_playing","arguments":{}}',
        },
        {
            "id": "music_browse",
            "description": "Browse artists, albums, genres, or tracks from a catalog-based Music Core provider.",
            "usage": (
                '{"function":"music_browse","arguments":{"category":"artists|albums|genres|tracks",'
                '"provider":"tater_tube|plex|emby|jellyfin|navidrome","limit":50}}'
            ),
        },
    ]


async def run_hydra_kernel_tool(
    *,
    tool_id: str,
    args: Optional[Dict[str, Any]] = None,
    origin: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
    **_kwargs,
) -> Optional[Dict[str, Any]]:
    store = redis_client or globals().get("redis_client")
    values = args if isinstance(args, dict) else {}
    if tool_id == "music_play":
        try:
            return await asyncio.to_thread(_play_request, values, origin, store)
        except Exception as exc:
            return {
                "ok": False,
                "error": {"code": "music_play_failed", "message": _text(exc)},
                "say_hint": "Explain the music playback problem and ask for any missing song or destination detail.",
            }
    if tool_id == "music_search":
        try:
            cfg = _settings(store)
            selected_provider = _provider_id(
                values.get("provider"),
                _provider_id(cfg.get("provider")),
            )
            if selected_provider == "roon":
                raise ValueError(
                    "Roon searches its library at play time. Use music_play with "
                    "provider roon and a song, artist, album, or genre."
                )
            if not (_catalog(store, selected_provider).get("tracks") or []):
                await asyncio.to_thread(_sync_catalog, store, selected_provider)
            matches = _search_tracks(
                query=values.get("query"),
                title=values.get("title") or values.get("track") or values.get("song"),
                artist=values.get("artist"),
                album=values.get("album"),
                genre=values.get("genre"),
                limit=_as_int(values.get("limit"), 10, 1, 50),
                client=store,
                provider_id=selected_provider,
            )
            public = [_public_track(track) for track in matches]
            return {
                "ok": True,
                "provider": selected_provider,
                "count": len(public),
                "tracks": public,
                "summary_for_user": f"Found {len(public)} matching track{'' if len(public) == 1 else 's'}.",
            }
        except Exception as exc:
            return {"ok": False, "error": {"code": "music_search_failed", "message": _text(exc)}}
    if tool_id == "music_control":
        action = _text(values.get("action")).lower()
        try:
            current_player = _player(store)
            is_roon = _provider_id(current_player.get("provider")) == "roon"
            if is_roon and action in {"next", "previous", "stop", "replay", "play", "resume", "pause"}:
                player = await asyncio.to_thread(_roon_control, action, client=store)
            elif action == "next":
                player = await asyncio.to_thread(_advance_player, 1, client=store)
            elif action == "previous":
                player = await asyncio.to_thread(_advance_player, -1, client=store)
            elif action == "stop":
                player = await asyncio.to_thread(_stop_player, client=store)
            elif action in {"replay", "play", "resume"}:
                current = _player(store)
                player = await asyncio.to_thread(
                    _start_player_index,
                    _as_int(current.get("index"), 0, 0, 100000),
                    client=store,
                )
            elif action == "shuffle":
                player = _player(store)
                queue = player.get("queue") if isinstance(player.get("queue"), list) else []
                current = player.get("current") if isinstance(player.get("current"), dict) else {}
                remaining = [row for row in queue if _text(row.get("id")) != _text(current.get("id"))]
                if _as_bool(values.get("enabled"), True):
                    random.SystemRandom().shuffle(remaining)
                player["queue"] = ([current] if current else []) + remaining
                player["index"] = 0 if current else -1
                player["shuffle"] = _as_bool(values.get("enabled"), True)
                _save_player(player, store)
            elif action == "repeat":
                mode = _text(values.get("mode") or "off").lower()
                if mode not in {"off", "all", "one"}:
                    raise ValueError("Repeat mode must be off, all, or one.")
                player = _player(store)
                player["repeat"] = mode
                _save_player(player, store)
            elif action in {"set_target", "set_targets"}:
                player = _player(store)
                player_provider = _provider_id(player.get("provider"))
                targets = _resolve_targets(
                    values.get("targets") or values.get("target"),
                    room=values.get("rooms") or values.get("room"),
                    origin=origin,
                    client=store,
                    provider_id=player_provider,
                )
                if not targets:
                    raise ValueError("Choose one or more valid music destinations.")
                if player_provider == "roon":
                    _roon_device_targets(targets)
                else:
                    _validate_catalog_provider_targets(targets)
                old_targets = _list(player.get("targets") or player.get("target"))
                was_playing = _text(player.get("status")).lower() == "playing"
                if was_playing and old_targets and old_targets != targets:
                    if _provider_id(player.get("provider")) == "roon":
                        await asyncio.to_thread(_roon_control, "stop", client=store)
                    else:
                        _stop_target(old_targets)
                player["targets"] = targets
                _save_player(player, store)
                if was_playing and old_targets != targets:
                    if _provider_id(player.get("provider")) == "roon":
                        raise ValueError(
                            "Roon destinations changed. Start the Roon request again so it can play on the new zones."
                        )
                    player = await asyncio.to_thread(
                        _start_player_index,
                        _as_int(player.get("index"), 0, 0, 100000),
                        client=store,
                    )
            else:
                raise ValueError(
                    "Music control action must be next, previous, stop, replay, shuffle, repeat, or set_targets."
                )
            targets = _list(player.get("targets") or player.get("target"))
            return {
                "ok": True,
                "status": _text(player.get("status")),
                "target": targets[0] if targets else "",
                "targets": targets,
                "target_count": len(targets),
                "now_playing": _public_track(player.get("current") or {}),
                "queue_count": len(player.get("queue") or []),
                "summary_for_user": (
                    f"Music is {_text(player.get('status')) or 'idle'} on {_target_summary(targets)}."
                ),
            }
        except Exception as exc:
            return {"ok": False, "error": {"code": "music_control_failed", "message": _text(exc)}}
    if tool_id == "music_now_playing":
        player = _player(store)
        targets = _list(player.get("targets") or player.get("target"))
        return {
            "ok": True,
            "status": _text(player.get("status")),
            "target": targets[0] if targets else "",
            "targets": targets,
            "target_count": len(targets),
            "now_playing": _public_track(player.get("current") or {}),
            "queue_count": len(player.get("queue") or []),
            "queue_index": _as_int(player.get("index"), -1, -1, 100000),
            "shuffle": bool(player.get("shuffle")),
            "repeat": _text(player.get("repeat") or "off"),
            "summary_for_user": (
                f"{_track_label(player.get('current') or {})} is {_text(player.get('status'))} "
                f"on {_target_summary(targets)}."
                if player.get("current")
                else "Music Core is idle."
            ),
        }
    if tool_id == "music_browse":
        selected_provider = _provider_id(
            values.get("provider"),
            _provider_id(_settings(store).get("provider")),
        )
        if selected_provider == "roon":
            return {
                "ok": False,
                "error": {
                    "code": "music_browse_unsupported",
                    "message": (
                        "Roon resolves its library through Roon Browse at play time. "
                        "Use music_play with provider roon."
                    ),
                },
            }
        catalog = _catalog(store, selected_provider)
        if not (catalog.get("tracks") or []):
            catalog = await asyncio.to_thread(_sync_catalog, store, selected_provider)
        category = _text(values.get("category") or "genres").lower()
        limit = _as_int(values.get("limit"), 50, 1, 200)
        if category == "tracks":
            items: Any = [
                _public_track(row)
                for row in (catalog.get("tracks") or [])[:limit]
            ]
        elif category in {"artists", "albums", "genres"}:
            items = list(catalog.get(category) or [])[:limit]
        else:
            return {
                "ok": False,
                "error": {
                    "code": "music_browse_category",
                    "message": "Choose artists, albums, genres, or tracks.",
                },
            }
        return {
            "ok": True,
            "provider": selected_provider,
            "category": category,
            "items": items,
            "count": len(items),
            "summary_for_user": f"Music Core has {len(items)} {category} in this result.",
        }
    return None


def _format_time(timestamp: Any) -> str:
    value = _as_float(timestamp)
    if value <= 0:
        return "never"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def _art_data_uri(track: Dict[str, Any]) -> str:
    label = _text(track.get("album") or track.get("artist") or track.get("title") or "Music")
    digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
    color_a = f"#{digest[:6]}"
    color_b = f"#{digest[6:12]}"
    safe_label = (
        label.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )[:30]
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="360" height="360" viewBox="0 0 360 360">
<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="{color_a}"/><stop offset="1" stop-color="{color_b}"/></linearGradient></defs>
<rect width="360" height="360" rx="28" fill="#15111f"/><circle cx="180" cy="165" r="118" fill="url(#g)" opacity=".92"/>
<circle cx="180" cy="165" r="48" fill="#15111f"/><circle cx="180" cy="165" r="13" fill="#ffbd59"/>
<path d="M254 77v142c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V96l-88 19v124c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V95z" fill="#fff" opacity=".9"/>
<rect x="24" y="304" width="312" height="34" rx="17" fill="#15111f" opacity=".86"/><text x="180" y="327" fill="#fff" font-family="system-ui,sans-serif" font-size="17" text-anchor="middle">{safe_label}</text>
</svg>"""
    return "data:image/svg+xml;charset=utf-8," + quote(svg, safe="")


def _artwork_proxy_url(track: Dict[str, Any]) -> str:
    track_id = _text(track.get("id"))
    if not track_id or not _as_bool(track.get("has_artwork"), False):
        return ""
    query = {
        "track_id": track_id,
        "provider": _provider_id(track.get("provider")),
    }
    modified = _as_int(track.get("modified_unix"), 0, 0, 10**12)
    if modified:
        query["v"] = str(modified)
    return f"/api/cores/music_core/webhook/artwork?{urlencode(query)}"


def _artwork_display_url(track: Dict[str, Any]) -> str:
    return _artwork_proxy_url(track) or _art_data_uri(track)


def _facet_art_track(catalog: Dict[str, Any], singular: str, value: Any) -> Dict[str, Any]:
    wanted = _text(value).casefold()
    fallback: Dict[str, Any] = {}
    for track in catalog.get("tracks") or []:
        if not isinstance(track, dict):
            continue
        if singular == "album":
            matches = _text(track.get("album")).casefold() == wanted
        elif singular == "artist":
            matches = wanted in {
                _text(track.get("artist")).casefold(),
                _text(track.get("album_artist")).casefold(),
            }
        else:
            matches = wanted in {
                _text(genre).casefold() for genre in track.get("genres") or []
            }
        if not matches:
            continue
        fallback = fallback or track
        if _as_bool(track.get("has_artwork"), False):
            return track
    return fallback


def _provider_options() -> List[Dict[str, str]]:
    return [
        {"value": provider_id, "label": label}
        for provider_id, label in PROVIDER_LABELS.items()
    ]


def _player_item(
    player: Dict[str, Any],
    target_options: List[Dict[str, str]],
    active_provider: str,
) -> Dict[str, Any]:
    current = player.get("current") if isinstance(player.get("current"), dict) else {}
    status = _text(player.get("status") or "idle").upper()
    targets = _list(player.get("targets") or player.get("target"))
    target_summary = _target_summary(targets)
    player_provider = _provider_id(player.get("provider"), active_provider)
    is_roon = player_provider == "roon"
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    queue_count = len(queue)
    current_index = _as_int(player.get("index"), -1, -1, max(0, queue_count - 1))
    track_list = [
        {
            "id": f"queue:{index}",
            "position": index + 1,
            "title": _text(track.get("title")) or "Untitled",
            "artist": _text(track.get("artist") or track.get("album_artist")),
            "album": _text(track.get("album")),
            "duration": _text(track.get("duration_display")),
            "active": index == current_index,
            "image_src": _artwork_proxy_url(track),
            "image_alt": f"{_track_label(track)} artwork",
        }
        for index, track in enumerate(queue[:200])
        if isinstance(track, dict)
    ]
    targets_field = {
        "key": "targets",
        "label": "Play On",
        "type": "multiselect",
        "value": targets,
        "size": max(4, min(8, len(target_options))),
        "options": target_options or [{"value": "", "label": "No players discovered"}],
        "description": "Choose any mix of satellites, stereo pairs, and supported media players.",
    }
    return {
        "id": "player:main",
        "group": "player",
        "card_variant": "player_bar",
        "title": _track_label(current) if current else "Music Player",
        "subtitle": f"{status} · {_text(current.get('album')) or 'No album selected'}",
        "detail": (
            _text(player.get("last_error"))
            if status == "ERROR" and _text(player.get("last_error"))
            else (
                f"Playing from Roon on {target_summary}."
                if is_roon
                else (
                    f"Playing on {target_summary}. Queue position "
                    f"{current_index + 1} of {queue_count}."
                )
            )
            if current
            else "Search your connected music library and choose where it should play."
        ),
        "hero_image_src": _artwork_display_url(current),
        "hero_image_alt": f"{_track_label(current) if current else 'Music'} artwork",
        "hero_badges": [
            {
                "label": status,
                "tone": "good" if status == "PLAYING" else ("warn" if status == "ERROR" else "muted"),
            },
            {"label": PROVIDER_LABELS[player_provider].upper(), "tone": "muted"},
            {"label": f"{queue_count} TRACKS", "tone": "muted"},
            {"label": "SHUFFLE" if player.get("shuffle") else "IN ORDER", "tone": "muted"},
            {"label": f"REPEAT {_text(player.get('repeat') or 'off').upper()}", "tone": "muted"},
            {"label": f"{len(targets)} PLAYER{'' if len(targets) == 1 else 'S'}", "tone": "muted"},
        ],
        "summary_rows": [
            {"label": "Artist", "value": _text(current.get("artist") or current.get("album_artist")) or "—"},
            {"label": "Album", "value": _text(current.get("album")) or "—"},
            {"label": "Genre", "value": _text(current.get("genre")) or "—"},
            {"label": "Destinations", "value": target_summary if targets else "Choose below"},
        ],
        "fields_popup": False,
        "fields_dropdown": False,
        "popup_fields": [dict(targets_field)],
        "settings_title": "Choose Speakers & Players",
        "settings_label": "🔊",
        "settings_aria_label": "Choose speakers and players",
        "settings_tooltip": "Choose speakers and players",
        "show_save_button": False,
        "fields": [
            {
                "key": "volume_percent",
                "label": "Volume",
                "type": "range",
                "value": _as_int(player.get("volume_percent"), 75, 0, 100),
                "min": 0,
                "max": 100,
                "step": 1,
                "suffix": "%",
                "action": "music_ui_set_volume",
            },
        ],
        "track_list": track_list,
        "track_list_label": "Current Track List",
        "track_list_action": "music_ui_queue_play",
        "track_list_shuffle": bool(player.get("shuffle")),
        "track_list_shuffle_action": "music_ui_set_shuffle",
        "save_action": "music_ui_save_player",
        "save_label": "Set Player",
        "actions": [
            {
                "action": "music_ui_previous",
                "label": "⏮",
                "aria_label": "Previous track",
                "tooltip": "Previous track",
                "working_text": "Loading previous track...",
                "success_text": "Previous track started.",
            },
            {
                "action": "music_ui_play",
                "label": "▶",
                "aria_label": "Play music",
                "tooltip": "Play music",
                "working_text": "Finding and starting music...",
                "success_text": "Music started.",
            },
            {
                "action": "music_ui_stop",
                "label": "■",
                "aria_label": "Stop music",
                "tooltip": "Stop music",
                "tone": "danger",
                "working_text": "Stopping music...",
                "success_text": "Music stopped.",
            },
            {
                "action": "music_ui_next",
                "label": "⏭",
                "aria_label": "Next track",
                "tooltip": "Next track",
                "working_text": "Loading next track...",
                "success_text": "Next track started.",
            },
        ],
    }


def _search_item() -> Dict[str, Any]:
    return {
        "id": "search:music",
        "group": "search",
        "card_variant": "music_search",
        "title": "Search Your Library",
        "subtitle": "Find a genre, artist, album, or song and start a fresh track list.",
        "fields_dropdown": False,
        "fields": [
            {
                "key": "query",
                "label": "Find Music",
                "type": "text",
                "value": "",
                "placeholder": "Reggae, Bob Marley, Exodus, or a song title",
            },
        ],
        "run_action": "music_ui_play",
        "run_label": "Play Search",
    }


def _facet_items(catalog: Dict[str, Any], category: str, label: str) -> List[Dict[str, Any]]:
    items = []
    singular = category[:-1] if category.endswith("s") else category
    for value in list(catalog.get(category) or [])[:500]:
        artwork_track = _facet_art_track(catalog, singular, value)
        items.append(
            {
                "id": f"{singular}:{value}",
                "group": category,
                "card_variant": "library_tile",
                "title": _text(value),
                "subtitle": f"Browse and play this {singular}.",
                "hero_image_src": (
                    _artwork_display_url(artwork_track)
                    if artwork_track
                    else _art_data_uri({singular: value, "title": value})
                ),
                "hero_badges": [{"label": label.upper(), "tone": "muted"}],
                "run_action": "music_ui_facet_play",
                "run_label": f"Play {label}",
            }
        )
    return items


def _client_target_kind(value: Any) -> str:
    token = _text(value).lower()
    if token.startswith("voice_core:"):
        return "satellite"
    if token.startswith("integration:roon:"):
        return "roon_zone"
    if token.startswith(("ha:", "sonos:", "integration:")):
        return "media_player"
    return "player"


def get_client_music_state(
    *,
    query: Any = "",
    limit: int = 60,
    refresh: bool = False,
    client: Any = None,
) -> Dict[str, Any]:
    """Return the credential-free Music Core state used by trusted Tater clients."""
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    active_provider = _provider_id(cfg.get("provider"))
    connected = _paired(cfg, active_provider)
    catalog: Dict[str, Any] = {}
    if active_provider in CATALOG_PROVIDER_IDS:
        if refresh and connected:
            catalog = _sync_catalog(store, active_provider)
        else:
            catalog = _catalog(store, active_provider)
    player = _reconcile_native_playback(_player(store), store)
    clean_limit = _as_int(limit, 60, 1, 200)
    clean_query = _text(query)
    tracks = (
        _search_tracks(
            query=clean_query,
            limit=clean_limit,
            client=store,
            provider_id=active_provider,
        )
        if clean_query and active_provider in CATALOG_PROVIDER_IDS
        else list(catalog.get("tracks") or [])[:clean_limit]
    )
    targets = [
        {
            "id": _text(row.get("value")),
            "label": _text(row.get("label")) or _text(row.get("value")),
            "kind": _client_target_kind(row.get("value")),
        }
        for row in _target_options(
            current_values=player.get("targets"),
            provider_id=active_provider,
        )
        if isinstance(row, dict) and _text(row.get("value"))
    ]
    providers = [
        {
            "id": provider_id,
            "label": label,
            "connected": _paired(cfg, provider_id),
            "active": provider_id == active_provider,
            "local_playback": provider_id in CATALOG_PROVIDER_IDS,
        }
        for provider_id, label in PROVIDER_LABELS.items()
    ]
    player_targets = _list(player.get("targets") or player.get("target"))
    return {
        "ok": True,
        "available": True,
        "version": __version__,
        "provider": {
            "id": active_provider,
            "label": PROVIDER_LABELS[active_provider],
            "connected": connected,
            "local_playback": active_provider in CATALOG_PROVIDER_IDS,
        },
        "providers": providers,
        "tracks": [_public_track(track) for track in tracks],
        "track_count": len(catalog.get("tracks") or []),
        "artists": list(catalog.get("artists") or [])[:200],
        "albums": list(catalog.get("albums") or [])[:200],
        "genres": list(catalog.get("genres") or [])[:200],
        "targets": targets,
        "player": {
            "status": _text(player.get("status") or "idle"),
            "provider": _provider_id(player.get("provider"), active_provider),
            "current": _public_track(player.get("current") or {}),
            "targets": player_targets,
            "target": player_targets[0] if player_targets else "",
            "queue_count": len(player.get("queue") or []),
            "queue_index": _as_int(player.get("index"), -1, -1, 100000),
            "shuffle": bool(player.get("shuffle")),
            "repeat": _text(player.get("repeat") or "off"),
            "volume_percent": _as_int(
                player.get("volume_percent"),
                _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                0,
                100,
            ),
        },
        "synced_at": _as_float(catalog.get("synced_at")),
    }


def _client_track(track_id: Any, provider_id: str, client: Any) -> Dict[str, Any]:
    wanted = _text(track_id)
    if not wanted:
        raise ValueError("Choose a track first.")
    catalog = _catalog(client, provider_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(client, provider_id)
    for track in catalog.get("tracks") or []:
        if isinstance(track, dict) and _text(track.get("id")) == wanted:
            return dict(track)
    raise ValueError("That track is no longer in the active music library.")


def run_client_music_action(
    action: Any,
    payload: Optional[Dict[str, Any]] = None,
    *,
    client: Any = None,
) -> Dict[str, Any]:
    """Run a bounded Music Core action for native Tater clients."""
    store = client or globals().get("redis_client")
    values = payload if isinstance(payload, dict) else {}
    command = _text(action).lower()
    cfg = _settings(store)
    selected_provider = _provider_id(
        values.get("provider"),
        _provider_id(cfg.get("provider")),
    )
    if command in {"refresh", "sync"}:
        if selected_provider == "roon":
            if not _paired(cfg, "roon"):
                raise ValueError("Pair Roon in Tater Settings → Integrations → Roon first.")
        else:
            _sync_catalog(store, selected_provider)
        return get_client_music_state(client=store)
    if command in {"set_provider", "provider"}:
        if not _paired(cfg, selected_provider):
            raise ValueError(f"Connect {PROVIDER_LABELS[selected_provider]} first.")
        _save_hash(store, SETTINGS_KEY, {"provider": selected_provider})
        if selected_provider in CATALOG_PROVIDER_IDS:
            _sync_catalog(store, selected_provider)
        return get_client_music_state(client=store)
    if command == "play":
        track_id = _text(values.get("track_id"))
        if track_id:
            if selected_provider == "roon":
                raise ValueError("Roon chooses music by search instead of a streamable track ID.")
            track = _client_track(track_id, selected_provider, store)
            targets = _resolve_targets(
                values.get("targets") or values.get("target"),
                client=store,
                provider_id=selected_provider,
            )
            if not targets:
                raise ValueError("Choose a satellite or media player.")
            _validate_catalog_provider_targets(targets)
            player = _create_and_start_queue(
                [track],
                targets=targets,
                shuffle=False,
                volume_percent=_as_int(
                    values.get("volume_percent"),
                    _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                    0,
                    100,
                ),
                client=store,
            )
            _save_hash(store, SETTINGS_KEY, {"provider": selected_provider})
            result = {
                "ok": True,
                "summary_for_user": (
                    f"Playing {_track_label(track)} on {_target_summary(targets)}."
                ),
                "now_playing": _public_track(player.get("current") or {}),
            }
        else:
            result = _play_request(values, {}, store)
        return {
            **result,
            "state": get_client_music_state(client=store),
        }

    player = _reconcile_native_playback(_player(store), store)
    is_roon = _provider_id(player.get("provider")) == "roon"
    if command in {"next", "previous", "stop", "replay", "play", "resume", "pause"}:
        if is_roon:
            updated = _roon_control(command, client=store)
        elif command == "next":
            updated = _advance_player(1, client=store)
        elif command == "previous":
            updated = _advance_player(-1, client=store)
        elif command == "stop":
            updated = _stop_player(client=store)
        elif command in {"replay", "play", "resume"}:
            updated = _start_player_index(
                _as_int(player.get("index"), 0, 0, 100000),
                client=store,
            )
        else:
            raise ValueError("Pause is available for Roon and on-device playback only.")
        return {
            "ok": True,
            "player": {
                "status": _text(updated.get("status")),
                "current": _public_track(updated.get("current") or {}),
            },
            "state": get_client_music_state(client=store),
        }
    raise ValueError("Music action must be play, next, previous, stop, replay, refresh, or set_provider.")


def get_client_music_stream_source(
    track_id: Any,
    *,
    provider_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    """Resolve one configured-provider stream for Tater's authenticated client proxy."""
    store = client or globals().get("redis_client")
    selected_provider = _provider_id(
        provider_id,
        _provider_id(_settings(store).get("provider")),
    )
    if selected_provider == "roon":
        raise ValueError("Roon audio can play only through Roon zones.")
    track = _client_track(track_id, selected_provider, store)
    source_url = _provider(store, selected_provider).stream_url(track)
    if not source_url:
        raise ValueError("That track does not have a playable stream.")
    return {
        "source_url": source_url,
        "media_type": _track_media_type(track),
        "filename": Path(_text(track.get("path")) or "music-track").name,
        "track": _public_track(track),
        "provider": selected_provider,
    }


def _provider_connection_detail(
    cfg: Dict[str, Any],
    provider_id: str,
) -> str:
    if provider_id == "tater_tube":
        return _text(
            cfg.get("tater_tube_server_url") or cfg.get("server_url")
        ) or "Pair with a Player PIN from Tater Tube Server."
    if provider_id == "roon":
        return (
            "Roon uses Tater's existing Roon integration and plays through Roon zones. "
            "Pair it in Tater Settings → Integrations → Roon."
        )
    return _text(cfg.get(f"{provider_id}_server_url")) or (
        f"Enter the {PROVIDER_LABELS[provider_id]} server connection below."
    )


def _provider_fields(cfg: Dict[str, Any], provider_id: str) -> List[Dict[str, Any]]:
    if provider_id == "tater_tube":
        return [
            {
                "key": "server_url",
                "label": "Tater Tube Server URL",
                "type": "text",
                "required": True,
                "value": _text(
                    cfg.get("tater_tube_server_url") or cfg.get("server_url")
                ),
                "placeholder": "http://tater-tube-server:8080",
            },
            {
                "key": "name",
                "label": "Music Player Name",
                "type": "text",
                "value": _text(
                    cfg.get("tater_tube_player_name") or cfg.get("player_name")
                )
                or "Tater Music Core",
            },
            {
                "key": "pin",
                "label": "6-digit Player Pairing PIN",
                "type": "password",
                "value": "",
                "description": (
                    "Required for first connection. Leave blank to keep the existing pairing."
                ),
            },
        ]
    if provider_id == "plex":
        return [
            {
                "key": "server_url",
                "label": "Plex Media Server URL",
                "type": "text",
                "required": True,
                "value": _text(cfg.get("plex_server_url")),
                "placeholder": "http://plex-server:32400",
            },
            {
                "key": "token",
                "label": "Plex Token",
                "type": "password",
                "value": "",
                "description": "Leave blank to keep the saved token.",
            },
            {
                "key": "library_ids",
                "label": "Music Libraries (optional)",
                "type": "text",
                "value": _text(cfg.get("plex_library_ids")),
                "placeholder": "Music, 3",
                "description": (
                    "Comma-separated Plex library names or section IDs. "
                    "Blank includes every music library."
                ),
            },
        ]
    if provider_id in {"emby", "jellyfin"}:
        label = PROVIDER_LABELS[provider_id]
        return [
            {
                "key": "server_url",
                "label": f"{label} Server URL",
                "type": "text",
                "required": True,
                "value": _text(cfg.get(f"{provider_id}_server_url")),
                "placeholder": (
                    "http://emby-server:8096"
                    if provider_id == "emby"
                    else "http://jellyfin-server:8096"
                ),
            },
            {
                "key": "api_key",
                "label": f"{label} API Key",
                "type": "password",
                "value": "",
                "description": "Leave blank to keep the saved API key.",
            },
            {
                "key": "user_id",
                "label": "User ID (optional)",
                "type": "text",
                "value": _text(cfg.get(f"{provider_id}_user_id")),
                "description": "Blank automatically uses the first user returned by the server.",
            },
        ]
    if provider_id == "navidrome":
        return [
            {
                "key": "server_url",
                "label": "Navidrome Server URL",
                "type": "text",
                "required": True,
                "value": _text(cfg.get("navidrome_server_url")),
                "placeholder": "http://navidrome-server:4533",
            },
            {
                "key": "username",
                "label": "Username",
                "type": "text",
                "value": _text(cfg.get("navidrome_username")),
            },
            {
                "key": "password",
                "label": "Password",
                "type": "password",
                "value": "",
                "description": "Leave blank to keep the saved password.",
            },
            {
                "key": "api_key",
                "label": "OpenSubsonic API Key (optional)",
                "type": "password",
                "value": "",
                "description": "If set, this is used instead of username and password.",
            },
        ]
    return []


def _provider_cards(
    cfg: Dict[str, Any],
    catalog: Dict[str, Any],
    active_provider: str,
) -> List[Dict[str, Any]]:
    cards: List[Dict[str, Any]] = []
    cached_provider = _provider_id(catalog.get("provider"))
    for provider_id, label in PROVIDER_LABELS.items():
        connected = _paired(cfg, provider_id)
        active = provider_id == active_provider
        track_count = (
            len(catalog.get("tracks") or [])
            if provider_id == cached_provider
            else 0
        )
        badges = [
            {
                "label": "ACTIVE" if active else "AVAILABLE",
                "tone": "good" if active else "muted",
            },
            {
                "label": "CONNECTED" if connected else "SETUP NEEDED",
                "tone": "good" if connected else "warn",
            },
        ]
        if provider_id in CATALOG_PROVIDER_IDS:
            badges.append({"label": f"{track_count} TRACKS", "tone": "muted"})
        else:
            badges.append({"label": "ROON ZONES", "tone": "muted"})

        actions: List[Dict[str, Any]] = []
        if provider_id == "roon":
            actions.append(
                {
                    "action": "music_provider_activate",
                    "label": "Use Roon" if not active else "Refresh Roon Status",
                    "working_text": "Checking the Roon integration...",
                    "success_text": "Roon is ready for Music Core.",
                }
            )
        else:
            actions.append(
                {
                    "action": "music_provider_connect",
                    "label": "Connect / Test",
                    "working_text": f"Connecting to {label}...",
                    "success_text": f"{label} connected.",
                }
            )
            if connected:
                actions.append(
                    {
                        "action": "music_provider_activate",
                        "label": "Use Provider" if not active else "Rescan Library",
                        "working_text": f"Loading the {label} library...",
                        "success_text": f"{label} library loaded.",
                    }
                )
                actions.append(
                    {
                        "action": "music_provider_disconnect",
                        "label": "Disconnect",
                        "tone": "danger",
                        "confirm": f"Disconnect Music Core from {label}?",
                    }
                )
        cards.append(
            {
                "id": f"provider:{provider_id}",
                "group": "providers",
                "title": label,
                "subtitle": (
                    "Active music provider"
                    if active
                    else ("Connected" if connected else "Not connected")
                ),
                "detail": _provider_connection_detail(cfg, provider_id),
                "hero_badges": badges,
                "fields": _provider_fields(cfg, provider_id),
                "fields_popup": False,
                "fields_dropdown": bool(provider_id != "roon"),
                "actions": actions,
            }
        )
    return cards


def _recommendation_ui_items(
    cfg: Dict[str, Any],
    catalog: Dict[str, Any],
    runtime: Dict[str, Any],
    active_provider: str,
    client: Any = None,
) -> List[Dict[str, Any]]:
    history = [
        row
        for row in _listening_history(client)
        if _provider_id(row.get("provider")) == active_provider
    ]
    enabled = _as_bool(cfg.get("recommendations_enabled"), True)
    catalog_provider = active_provider in CATALOG_PROVIDER_IDS
    published = _recommendations(client)
    if _provider_id(published.get("provider"), "") != active_provider:
        published = {}
    generated_at = _as_float(published.get("generated_at"))
    last_error = _text(runtime.get("last_recommendation_error"))
    if not enabled:
        detail = "Turn on Tater Recommendations in Settings to create AI-named music mixes."
    elif not catalog_provider:
        detail = "Roon resolves music dynamically, so recommendations require a catalog-based provider."
    elif not history:
        detail = "Start playing music and Tater will learn enough to prepare your first mixes."
    elif last_error and not published:
        detail = last_error
    elif generated_at:
        detail = (
            f"Built from {len(history)} listening event{'' if len(history) == 1 else 's'} · "
            f"updated {_format_time(generated_at)}"
        )
    else:
        detail = "Tater has listening history and is ready to prepare your first mixes."

    items: List[Dict[str, Any]] = [
        {
            "id": "recommendations:overview",
            "group": "recommendations",
            "card_variant": "recommendations_intro",
            "title": "Tater Recommendations",
            "subtitle": _text(published.get("summary"))
            or "Named playlists made from what you actually listen to.",
            "detail": detail,
            "generated_at": generated_at,
            "history_event_count": len(history),
            "recommendations_enabled": enabled,
            "refresh_available": bool(enabled and catalog_provider and history),
            "refresh_running": bool(
                _recommendation_lock.locked()
                or (_recommendation_thread is not None and _recommendation_thread.is_alive())
            ),
            "run_action": "music_recommendations_refresh",
            "run_label": "Refresh Recommendations",
        }
    ]
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    for playlist in published.get("playlists") or []:
        if not isinstance(playlist, dict) or not _text(playlist.get("id")):
            continue
        recommendation_items = []
        album_count = 0
        song_count = 0
        for selection in playlist.get("items") or []:
            if not isinstance(selection, dict):
                continue
            selection_type = _text(selection.get("type")) or "song"
            if selection_type == "album":
                album_count += 1
            else:
                song_count += 1
            art_track = track_by_id.get(_text(selection.get("image_track_id"))) or {}
            recommendation_items.append(
                {
                    "id": _text(selection.get("candidate_id")),
                    "type": selection_type,
                    "title": _text(selection.get("title")) or "Untitled",
                    "artist": _text(selection.get("artist")),
                    "album": _text(selection.get("album")),
                    "reason": _text(selection.get("reason")),
                    "track_count": _as_int(selection.get("track_count"), 1, 1, 10000),
                    "image_src": _artwork_display_url(art_track) if art_track else "",
                    "image_alt": f"{_text(selection.get('title')) or 'Music'} artwork",
                }
            )
        if not recommendation_items:
            continue
        hero_src = _text(recommendation_items[0].get("image_src"))
        items.append(
            {
                "id": f"recommendation:{_text(playlist.get('id'))}",
                "group": "recommendations",
                "card_variant": "recommendation_playlist",
                "title": _text(playlist.get("name")) or "Tater Mix",
                "subtitle": _text(playlist.get("description")),
                "hero_image_src": hero_src,
                "hero_image_alt": f"{_text(playlist.get('name')) or 'Tater Mix'} artwork",
                "hero_badges": [
                    {"label": "AI PLAYLIST", "tone": "good"},
                    {"label": f"{album_count} ALBUM{'' if album_count == 1 else 'S'}", "tone": "muted"},
                    {"label": f"{song_count} SONG{'' if song_count == 1 else 'S'}", "tone": "muted"},
                    {"label": f"{len(playlist.get('track_ids') or [])} TRACKS", "tone": "muted"},
                ],
                "recommendation_items": recommendation_items,
                "run_action": "music_recommendation_play",
                "run_label": "Play Playlist",
            }
        )
    return items


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    cfg = _settings(store)
    active_provider = _provider_id(cfg.get("provider"))
    runtime = _runtime(store)
    catalog = _catalog(store, active_provider)
    player = _reconcile_native_playback(_player(store), store)
    connected = _paired(cfg, active_provider)
    saved_player_targets = _normalize_stereo_targets(
        player.get("targets") or player.get("target")
    )
    saved_default_targets = _normalize_stereo_targets(
        cfg.get("default_targets") or cfg.get("default_target")
    )
    saved_targets = _list([*saved_player_targets, *saved_default_targets])
    target_options = _target_options(
        current_values=saved_targets,
        provider_id=active_provider,
    )
    known_targets = {_text(row.get("value")) for row in target_options}
    for saved in saved_targets:
        if saved and saved not in known_targets:
            target_options.append({"value": saved, "label": f"Saved player: {saved}"})
            known_targets.add(saved)

    item_forms = [_player_item(player, target_options, active_provider), _search_item()]
    item_forms.extend(
        _recommendation_ui_items(cfg, catalog, runtime, active_provider, store)
    )
    item_forms.extend(_facet_items(catalog, "genres", "Genre"))
    item_forms.extend(_facet_items(catalog, "artists", "Artist"))
    item_forms.extend(_facet_items(catalog, "albums", "Album"))
    item_forms.extend(_provider_cards(cfg, catalog, active_provider))
    item_forms.extend(
        [
            {
                "id": "settings:music",
                "group": "settings",
                "title": "Music Core Settings",
                "subtitle": "Defaults for catalog refreshes, queues, and playback.",
                "fields": [
                    {
                        "key": "catalog_sync_interval_seconds",
                        "label": "Catalog Sync Interval (sec)",
                        "type": "number",
                        "value": _as_int(cfg.get("catalog_sync_interval_seconds"), 900, 60, 86400),
                    },
                    {
                        "key": "default_targets",
                        "label": "Default Players",
                        "type": "multiselect",
                        "value": saved_default_targets,
                        "size": max(4, min(8, len(target_options))),
                        "options": target_options,
                        "description": (
                            "Used only when the request does not name rooms or players "
                            "and did not originate from a satellite."
                        ),
                    },
                    {
                        "key": "default_volume_percent",
                        "label": "Default Volume (%)",
                        "type": "number",
                        "value": _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                    },
                    {
                        "key": "default_shuffle",
                        "label": "Shuffle Broad Requests",
                        "type": "checkbox",
                        "value": _as_bool(cfg.get("default_shuffle"), True),
                    },
                    {
                        "key": "maximum_queue_tracks",
                        "label": "Maximum Queue Tracks",
                        "type": "number",
                        "value": _as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000),
                    },
                    {
                        "key": "recommendations_enabled",
                        "label": "Tater Recommendations",
                        "type": "checkbox",
                        "value": _as_bool(cfg.get("recommendations_enabled"), True),
                        "description": (
                            "Uses listening metadata and Tater's primary AI model to make named playlists."
                        ),
                    },
                    {
                        "key": "recommendation_interval_hours",
                        "label": "Recommendation Refresh (hours)",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_interval_hours"), 12, 1, 168),
                    },
                    {
                        "key": "recommendation_playlist_count",
                        "label": "Recommendation Playlists",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_playlist_count"), 3, 1, 6),
                    },
                    {
                        "key": "recommendation_items_per_playlist",
                        "label": "Albums & Songs Per Playlist",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_items_per_playlist"), 6, 3, 12),
                    },
                ],
                "save_action": "music_save_settings",
                "save_label": "Save Music Settings",
            },
        ]
    )
    return {
        "summary": "Voice-controlled music libraries and a built-in player for Tater.",
        "stats": [
            {
                "label": "Provider",
                "value": (
                    PROVIDER_LABELS[active_provider]
                    if connected
                    else f"{PROVIDER_LABELS[active_provider]} · Setup Needed"
                ),
            },
            {"label": "Tracks", "value": len(catalog.get("tracks") or [])},
            {"label": "Artists", "value": len(catalog.get("artists") or [])},
            {"label": "Albums", "value": len(catalog.get("albums") or [])},
            {"label": "Genres", "value": len(catalog.get("genres") or [])},
            {
                "label": "Last Scan",
                "value": (
                    "On demand through Roon"
                    if active_provider == "roon"
                    else _format_time(
                        catalog.get("synced_at") or runtime.get("last_sync_at")
                    )
                ),
            },
        ],
        "items": [],
        "empty_message": (
            "Connect a music provider to load its library."
            if active_provider != "roon"
            else "Roon searches its library when playback starts."
        ),
        "ui": {
            "kind": "settings_manager",
            "title": "Music Core",
            "appearance": "music_library",
            "live_updates": True,
            "poll_interval_ms": 3000,
            "persistent_item_groups": ["player"],
            "default_tab": "library",
            "manager_tabs": [
                {
                    "key": "library",
                    "label": "Browse Library",
                    "source": "grouped_items",
                    "groups": [
                        {
                            "key": "search",
                            "label": "Search",
                            "item_group": "search",
                            "selector": False,
                            "empty_message": "Search is unavailable.",
                        },
                        {
                            "key": "genres",
                            "label": "Genres",
                            "item_group": "genres",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No genres are available in the active library.",
                        },
                        {
                            "key": "artists",
                            "label": "Artists",
                            "item_group": "artists",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No artists are available in the active library.",
                        },
                        {
                            "key": "albums",
                            "label": "Albums",
                            "item_group": "albums",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No albums are available in the active library.",
                        },
                    ],
                },
                {
                    "key": "recommendations",
                    "label": "Tater Recommendations",
                    "source": "items",
                    "item_group": "recommendations",
                    "empty_message": "Play some music to help Tater build recommendations.",
                },
                {"key": "providers", "label": "Providers", "source": "items", "item_group": "providers"},
                {"key": "settings", "label": "Settings", "source": "items", "item_group": "settings"},
            ],
            "item_fields_dropdown": True,
            "item_fields_popup": True,
            "item_forms": item_forms,
        },
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values") if isinstance(payload, dict) else {}
    return values if isinstance(values, dict) else {}


def _provider_from_card(payload: Dict[str, Any], fallback: Any = "") -> str:
    item_id = _text(payload.get("id"))
    if item_id.startswith("provider:"):
        candidate = item_id.split(":", 1)[1]
        if candidate in PROVIDER_LABELS:
            return candidate
    candidate = _text(payload.get("provider") or fallback)
    if candidate and _provider_id(candidate, "") in PROVIDER_LABELS:
        return _provider_id(candidate, "")
    raise ValueError("Music provider is invalid.")


def _connect_provider(
    provider_id: str,
    values: Dict[str, Any],
    client: Any,
) -> Dict[str, Any]:
    cfg = _settings(client)
    updates: Dict[str, Any] = {"provider": provider_id}
    if provider_id == "tater_tube":
        server_url = _normalize_server_url(
            values.get("server_url")
            or cfg.get("tater_tube_server_url")
            or cfg.get("server_url")
        )
        name = _text(
            values.get("name")
            or cfg.get("tater_tube_player_name")
            or cfg.get("player_name")
        ) or "Tater Music Core"
        pin = "".join(char for char in _text(values.get("pin")) if char.isdigit())
        token = _text(cfg.get("tater_tube_token") or cfg.get("token"))
        player_id = _text(cfg.get("tater_tube_player_id") or cfg.get("player_id"))
        if pin:
            if len(pin) != 6:
                raise ValueError("Enter the 6-digit Player PIN created by Tater Tube Server.")
            paired = TaterTubeMusicProvider.pair(server_url, pin, name)
            if not isinstance(paired, dict) or not _text(paired.get("token")):
                raise RuntimeError("Tater Tube Server did not return a music player token.")
            token = _text(paired.get("token"))
            player_id = _text(paired.get("player_id"))
            name = _text(paired.get("player_name")) or name
        if not token:
            raise ValueError("Enter a 6-digit Player PIN to pair Tater Tube Server.")
        updates.update(
            {
                "tater_tube_server_url": server_url,
                "tater_tube_player_name": name,
                "tater_tube_player_id": player_id,
                "tater_tube_token": token,
                # Keep the original keys so upgrades from Music Core 1.x remain reversible.
                "server_url": server_url,
                "player_name": name,
                "player_id": player_id,
                "token": token,
            }
        )
    elif provider_id == "plex":
        server_url = _normalize_server_url(
            values.get("server_url") or cfg.get("plex_server_url")
        )
        token = _text(values.get("token") or cfg.get("plex_token"))
        if not token:
            raise ValueError("Enter a Plex token.")
        updates.update(
            {
                "plex_server_url": server_url,
                "plex_token": token,
                "plex_library_ids": ",".join(_list(values.get("library_ids"))),
            }
        )
    elif provider_id in {"emby", "jellyfin"}:
        label = PROVIDER_LABELS[provider_id]
        server_url = _normalize_server_url(
            values.get("server_url") or cfg.get(f"{provider_id}_server_url")
        )
        api_key = _text(values.get("api_key") or cfg.get(f"{provider_id}_api_key"))
        if not api_key:
            raise ValueError(f"Enter a {label} API key.")
        updates.update(
            {
                f"{provider_id}_server_url": server_url,
                f"{provider_id}_api_key": api_key,
                f"{provider_id}_user_id": _text(
                    values.get("user_id") or cfg.get(f"{provider_id}_user_id")
                ),
            }
        )
    elif provider_id == "navidrome":
        server_url = _normalize_server_url(
            values.get("server_url") or cfg.get("navidrome_server_url")
        )
        username = _text(values.get("username") or cfg.get("navidrome_username"))
        password = _text(values.get("password") or cfg.get("navidrome_password"))
        api_key = _text(values.get("api_key") or cfg.get("navidrome_api_key"))
        if not api_key and not (username and password):
            raise ValueError("Enter a Navidrome API key or a username and password.")
        updates.update(
            {
                "navidrome_server_url": server_url,
                "navidrome_username": username,
                "navidrome_password": password,
                "navidrome_api_key": api_key,
            }
        )
    else:
        raise ValueError(f"{PROVIDER_LABELS.get(provider_id, provider_id)} is not a catalog provider.")

    _save_hash(client, SETTINGS_KEY, updates)
    catalog = _sync_catalog(client, provider_id)
    return {
        "ok": True,
        "message": (
            f"{PROVIDER_LABELS[provider_id]} connected and loaded "
            f"{len(catalog.get('tracks') or [])} tracks."
        ),
    }


def _disconnect_provider(provider_id: str, client: Any) -> Dict[str, Any]:
    if provider_id == "roon":
        raise ValueError("Manage the Roon connection in Tater Settings → Integrations → Roon.")
    fields = {
        "tater_tube": (
            "tater_tube_server_url",
            "tater_tube_player_name",
            "tater_tube_player_id",
            "tater_tube_token",
            "server_url",
            "player_name",
            "player_id",
            "token",
        ),
        "plex": ("plex_server_url", "plex_token", "plex_library_ids"),
        "emby": ("emby_server_url", "emby_api_key", "emby_user_id"),
        "jellyfin": ("jellyfin_server_url", "jellyfin_api_key", "jellyfin_user_id"),
        "navidrome": (
            "navidrome_server_url",
            "navidrome_username",
            "navidrome_password",
            "navidrome_api_key",
        ),
    }[provider_id]
    player = _player(client)
    if _provider_id(player.get("provider")) == provider_id:
        _stop_player(client=client)
    if client is not None:
        client.hdel(SETTINGS_KEY, *fields)
        cached = _load_json(client, CATALOG_KEY, {})
        if _provider_id(cached.get("provider")) == provider_id:
            client.delete(CATALOG_KEY)
    _save_hash(client, RUNTIME_KEY, {"status": "disconnected", "last_error": ""})
    return {"ok": True, "message": f"{PROVIDER_LABELS[provider_id]} disconnected locally."}


def _play_recommendation(item_id: Any, client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    recommendation_id = _text(item_id)
    if recommendation_id.startswith("recommendation:"):
        recommendation_id = recommendation_id.split(":", 1)[1]
    published = _recommendations(store)
    cfg = _settings(store)
    provider_id = _provider_id(cfg.get("provider"))
    if _provider_id(published.get("provider"), "") != provider_id:
        raise ValueError("Refresh Tater Recommendations for the active music provider first.")
    playlist = next(
        (
            row
            for row in published.get("playlists") or []
            if isinstance(row, dict) and _text(row.get("id")) == recommendation_id
        ),
        None,
    )
    if not isinstance(playlist, dict):
        raise ValueError("That Tater recommendation is no longer available.")
    catalog = _catalog(store, provider_id)
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    tracks = [
        dict(track_by_id[track_id])
        for track_id in playlist.get("track_ids") or []
        if _text(track_id) in track_by_id
    ]
    if not tracks:
        raise ValueError("Those recommended tracks are no longer in the active library. Refresh recommendations.")

    current = _player(store)
    requested_targets = _list(current.get("targets") or current.get("target")) or _list(
        cfg.get("default_targets") or cfg.get("default_target")
    )
    targets = _resolve_targets(
        requested_targets,
        client=store,
        provider_id=provider_id,
    )
    if not targets:
        raise ValueError("Choose one or more players in the Music Player before starting this playlist.")
    _validate_catalog_provider_targets(targets)
    volume = _as_int(
        current.get("volume_percent"),
        _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
        0,
        100,
    )
    return _create_and_start_queue(
        tracks,
        targets=targets,
        shuffle=False,
        volume_percent=volume,
        client=store,
    )


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    action_name = _text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)

    if action_name in {"music_pair_tater_tube", "music_provider_connect"}:
        provider_id = (
            "tater_tube"
            if action_name == "music_pair_tater_tube"
            else _provider_from_card(body)
        )
        return _connect_provider(provider_id, values, store)

    if action_name == "music_provider_activate":
        provider_id = _provider_from_card(body)
        if not _paired(_settings(store), provider_id):
            if provider_id == "roon":
                raise ValueError(
                    "Pair and enable Roon in Tater Settings → Integrations → Roon first."
                )
            raise ValueError(f"Connect {PROVIDER_LABELS[provider_id]} first.")
        _save_hash(store, SETTINGS_KEY, {"provider": provider_id})
        if provider_id == "roon":
            return {
                "ok": True,
                "message": "Roon is active. Choose Roon zones in the player, then search or ask Tater to play music.",
            }
        catalog = _sync_catalog(store, provider_id)
        return {
            "ok": True,
            "message": (
                f"{PROVIDER_LABELS[provider_id]} is active with "
                f"{len(catalog.get('tracks') or [])} tracks."
            ),
        }

    if action_name in {"music_disconnect", "music_provider_disconnect"}:
        provider_id = (
            "tater_tube"
            if action_name == "music_disconnect"
            else _provider_from_card(body)
        )
        return _disconnect_provider(provider_id, store)

    if action_name == "music_sync_now":
        selected_provider = _provider_id(_settings(store).get("provider"))
        catalog = _sync_catalog(store, selected_provider)
        return {"ok": True, "message": f"Music library updated with {len(catalog.get('tracks') or [])} tracks."}

    if action_name == "music_save_settings":
        allowed = {
            "catalog_sync_interval_seconds",
            "default_targets",
            "default_volume_percent",
            "default_shuffle",
            "maximum_queue_tracks",
            "recommendations_enabled",
            "recommendation_interval_hours",
            "recommendation_playlist_count",
            "recommendation_items_per_playlist",
        }
        updates = {key: values.get(key) for key in allowed if key in values}
        if "default_targets" in updates:
            updates["default_targets"] = json.dumps(
                _normalize_stereo_targets(updates["default_targets"])
            )
        _save_hash(store, SETTINGS_KEY, updates)
        return {"ok": True, "message": "Music Core settings saved."}

    if action_name == "music_recommendations_refresh":
        started = _schedule_recommendation_refresh(store)
        return {
            "ok": True,
            "message": (
                "Tater is preparing fresh recommendation playlists in the background."
                if started
                else "Tater is already refreshing music recommendations."
            ),
        }

    if action_name == "music_recommendation_play":
        player = _play_recommendation(body.get("id"), store)
        return {
            "ok": True,
            "message": f"Playing {_track_label(player.get('current') or {})} from Tater Recommendations.",
        }

    if action_name == "music_ui_play":
        existing = _player(store)
        selected_provider = _provider_id(
            values.get("provider"),
            _provider_id(_settings(store).get("provider")),
        )
        existing_provider = _provider_id(
            existing.get("provider"),
            _provider_id(_settings(store).get("provider")),
        )
        queue = existing.get("queue") if isinstance(existing.get("queue"), list) else []
        if (
            not _text(values.get("query"))
            and selected_provider == "roon"
            and existing_provider == "roon"
        ):
            player = _roon_control("play", client=store)
            return {
                "ok": True,
                "message": f"Resumed Roon on {_target_summary(player.get('targets'))}.",
            }
        if (
            not _text(values.get("query"))
            and queue
            and selected_provider == existing_provider
        ):
            old_targets = _list(existing.get("targets") or existing.get("target"))
            requested_targets = values.get("targets")
            if not _list(requested_targets):
                requested_targets = old_targets
            targets = _resolve_targets(
                requested_targets,
                client=store,
                provider_id=selected_provider,
            )
            if not targets:
                raise ValueError("Choose one or more valid satellites, stereo pairs, or media players.")
            _validate_catalog_provider_targets(targets)
            if old_targets and old_targets != targets:
                _stop_target(old_targets)
                existing["status"] = "stopped"
            existing["targets"] = targets
            existing["shuffle"] = _as_bool(values.get("shuffle"), bool(existing.get("shuffle")))
            existing["volume_percent"] = _as_int(
                values.get("volume_percent"),
                _as_int(existing.get("volume_percent"), 75, 0, 100),
                0,
                100,
            )
            _save_player(existing, store)
            player = _start_player_index(
                _as_int(existing.get("index"), 0, 0, max(0, len(queue) - 1)),
                client=store,
            )
            return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}
        requested_targets = values.get("targets")
        if not _list(requested_targets):
            requested_targets = _list(existing.get("targets") or existing.get("target"))
        result = _play_request(
            {
                "provider": selected_provider,
                "query": values.get("query"),
                "targets": requested_targets,
                "shuffle": (
                    values.get("shuffle")
                    if values.get("shuffle") is not None
                    else existing.get("shuffle")
                ),
                "volume_percent": (
                    values.get("volume_percent")
                    if values.get("volume_percent") is not None
                    else existing.get("volume_percent")
                ),
            },
            {},
            store,
        )
        return {"ok": True, "message": _text(result.get("summary_for_user"))}

    if action_name == "music_ui_save_player":
        player = _player(store)
        selected_provider = _provider_id(
            values.get("provider"),
            _provider_id(_settings(store).get("provider")),
        )
        old_targets = _list(player.get("targets") or player.get("target"))
        targets = _resolve_targets(
            values.get("targets") or values.get("target"),
            client=store,
            provider_id=selected_provider,
        )
        if not targets:
            raise ValueError("Choose one or more valid satellites, stereo pairs, or media players.")
        if selected_provider == "roon":
            _roon_device_targets(targets)
        was_playing = _text(player.get("status")).lower() == "playing"
        old_provider = _provider_id(
            player.get("provider"),
            _provider_id(_settings(store).get("provider")),
        )
        provider_changed = old_provider != selected_provider
        if was_playing and old_targets and (old_targets != targets or provider_changed):
            if old_provider == "roon":
                _roon_control("stop", client=store)
            else:
                _stop_target(old_targets)
            player["status"] = "stopped"
        _save_hash(store, SETTINGS_KEY, {"provider": selected_provider})
        player["provider"] = selected_provider
        player["targets"] = targets
        player["shuffle"] = _as_bool(values.get("shuffle"), bool(player.get("shuffle")))
        player["volume_percent"] = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        if provider_changed:
            player["status"] = "stopped"
        _save_player(player, store)
        if provider_changed:
            return {
                "ok": True,
                "message": (
                    f"Music provider set to {PROVIDER_LABELS[selected_provider]}. "
                    "Enter music to start playback."
                ),
            }
        if was_playing and old_targets != targets:
            if selected_provider == "roon":
                query = _text(
                    (player.get("current") or {}).get("title")
                    if isinstance(player.get("current"), dict)
                    else ""
                )
                result = _roon_play_request(
                    {
                        "query": query,
                        "targets": targets,
                        "shuffle": player.get("shuffle"),
                    },
                    {},
                    store,
                )
                return {"ok": True, "message": _text(result.get("summary_for_user"))}
            player = _start_player_index(
                _as_int(player.get("index"), 0, 0, 100000),
                client=store,
            )
            return {"ok": True, "message": f"Moved music to {_target_summary(targets)}."}
        return {"ok": True, "message": f"Music player set to {_target_summary(targets)}."}

    if action_name == "music_ui_set_volume":
        player = _player(store)
        volume = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        player["volume_percent"] = volume
        _save_player(player, store)
        return {"ok": True, "message": f"Music volume set to {volume}%."}

    if action_name == "music_ui_set_shuffle":
        player = _set_player_shuffle(
            _as_bool(values.get("shuffle"), False),
            client=store,
        )
        return {
            "ok": True,
            "message": "Shuffle is on." if player.get("shuffle") else "Shuffle is off.",
        }

    if action_name == "music_ui_stop":
        player = _player(store)
        if _provider_id(player.get("provider")) == "roon":
            _roon_control("stop", client=store)
        else:
            _stop_player(client=store)
        return {"ok": True, "message": "Music stopped."}

    if action_name == "music_ui_next":
        current = _player(store)
        player = (
            _roon_control("next", client=store)
            if _provider_id(current.get("provider")) == "roon"
            else _advance_player(1, client=store)
        )
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_previous":
        current = _player(store)
        player = (
            _roon_control("previous", client=store)
            if _provider_id(current.get("provider")) == "roon"
            else _advance_player(-1, client=store)
        )
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_queue_play":
        item_id = _text(body.get("id"))
        try:
            index = int(item_id.split(":", 1)[1])
        except Exception as exc:
            raise ValueError("Queue position is invalid.") from exc
        player = _start_player_index(index, client=store)
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_facet_play":
        item_id = _text(body.get("id"))
        facet, separator, value = item_id.partition(":")
        if not separator or not value or facet not in {"genre", "artist", "album"}:
            raise ValueError("Music category is invalid.")
        current_player = _player(store)
        current_settings = _settings(store)
        player_result = _play_request(
            {
                "provider": _provider_id(current_settings.get("provider")),
                facet: value,
                "targets": (
                    _list(current_player.get("targets") or current_player.get("target"))
                    or _list(
                        current_settings.get("default_targets")
                        or current_settings.get("default_target")
                    )
                ),
                "shuffle": facet != "album",
                "volume_percent": current_player.get("volume_percent"),
            },
            {},
            store,
        )
        return {"ok": True, "message": _text(player_result.get("summary_for_user"))}

    raise ValueError(f"Unknown Music Core action: {action_name}")


def _fetch_track_artwork(track: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    provider_id = _provider_id(track.get("provider"))
    provider = _provider(client, provider_id)
    artwork_url_fn = getattr(provider, "artwork_url", None)
    source_url = artwork_url_fn(track) if callable(artwork_url_fn) else ""
    source_url = _text(source_url)
    if not source_url:
        raise KeyError("This track does not have embedded artwork.")

    cache_key = hashlib.sha256(source_url.encode("utf-8")).hexdigest()
    with _state_lock:
        cached = _artwork_cache.get(cache_key)
        if isinstance(cached, dict) and cached.get("body"):
            return dict(cached)

    response = requests.get(
        source_url,
        headers={"Accept": "image/jpeg,image/png,image/webp,image/*"},
        timeout=30,
    )
    response.raise_for_status()
    body = bytes(response.content or b"")
    content_type = _text(response.headers.get("Content-Type")).split(";", 1)[0].lower()
    if not content_type.startswith("image/"):
        raise ValueError("The music provider did not return an image.")
    if not body or len(body) > 12 * 1024 * 1024:
        raise ValueError("The embedded artwork is empty or too large.")

    cached = {"body": body, "content_type": content_type}
    with _state_lock:
        if len(_artwork_cache) >= 256:
            _artwork_cache.clear()
        _artwork_cache[cache_key] = cached
    return dict(cached)


def handle_core_webhook(
    *,
    webhook: str,
    query: Optional[Dict[str, Any]] = None,
    redis_client=None,
    **_kwargs,
) -> Any:
    if _text(webhook).lower() != "artwork":
        raise KeyError(f"Unsupported Music Core webhook: {webhook}")
    params = query if isinstance(query, dict) else {}
    provider_id = _provider_id(
        params.get("provider"),
        _provider_id(_settings(redis_client).get("provider")),
    )
    track = _client_track(params.get("track_id"), provider_id, redis_client)
    artwork = _fetch_track_artwork(track, redis_client)
    from starlette.responses import Response

    return Response(
        content=artwork["body"],
        media_type=artwork["content_type"],
        headers={"Cache-Control": "private, max-age=86400"},
    )


def run(stop_event: Optional[object] = None) -> None:
    logger.info("[Music] Core starting.")
    try:
        while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
            cfg = _settings()
            active_provider = _provider_id(cfg.get("provider"))
            if not _paired(cfg, active_provider):
                _save_hash(redis_client, RUNTIME_KEY, {"status": "waiting_for_pairing"})
                time.sleep(1.0)
                continue
            runtime = _runtime()
            now = time.time()
            interval = _as_int(
                cfg.get("catalog_sync_interval_seconds"),
                DEFAULT_SYNC_INTERVAL_SECONDS,
                60,
                86400,
            )
            try:
                if (
                    active_provider in CATALOG_PROVIDER_IDS
                    and now - _as_float(runtime.get("last_sync_at")) >= interval
                ):
                    _sync_catalog(provider_id=active_provider)
                    runtime = _runtime()
                _advance_finished_player()
                recommendation_interval = (
                    _as_int(cfg.get("recommendation_interval_hours"), 12, 1, 168) * 3600
                )
                published = _recommendations()
                last_recommendation_cycle = max(
                    _as_float(runtime.get("last_recommendation_at")),
                    _as_float(runtime.get("last_recommendation_attempt_at")),
                )
                if _provider_id(published.get("provider"), "") != active_provider:
                    last_recommendation_cycle = 0.0
                has_history = any(
                    _provider_id(row.get("provider")) == active_provider
                    for row in _listening_history()
                )
                if (
                    _as_bool(cfg.get("recommendations_enabled"), True)
                    and active_provider in CATALOG_PROVIDER_IDS
                    and has_history
                    and now - last_recommendation_cycle >= recommendation_interval
                ):
                    _schedule_recommendation_refresh()
            except PermissionError as exc:
                logger.warning("[Music] provider authorization was revoked: %s", exc)
                authorization_fields = {
                    "tater_tube": ("tater_tube_token", "token"),
                    "plex": ("plex_token",),
                    "emby": ("emby_api_key",),
                    "jellyfin": ("jellyfin_api_key",),
                    "navidrome": ("navidrome_api_key", "navidrome_password"),
                }.get(active_provider, ())
                if authorization_fields:
                    redis_client.hdel(SETTINGS_KEY, *authorization_fields)
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {
                        "status": "authorization_revoked",
                        "last_error": _text(exc)[:500],
                        "last_error_at": now,
                    },
                )
            except Exception as exc:
                logger.warning("[Music] background cycle failed: %s", exc)
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {
                        "status": "error",
                        "last_error": _text(exc)[:500],
                        "last_error_at": now,
                    },
                )
            time.sleep(1.0)
    finally:
        logger.info("[Music] Core stopped.")
