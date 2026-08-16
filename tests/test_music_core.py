import asyncio
import io
import importlib.util
import json
import sys
import threading
import time
import types
import unittest
import wave
from pathlib import Path
from unittest.mock import Mock, patch


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value

    def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)
            self.hashes.pop(key, None)

    def hgetall(self, key):
        return dict(self.hashes.get(key) or {})

    def hset(self, key, mapping=None, **_kwargs):
        self.hashes.setdefault(key, {}).update(mapping or {})

    def hdel(self, key, *fields):
        row = self.hashes.setdefault(key, {})
        for field in fields:
            row.pop(field, None)


def load_music_core():
    helpers = types.ModuleType("helpers")
    helpers.redis_client = FakeRedis()
    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    sys.modules["helpers"] = helpers

    path = Path(__file__).resolve().parents[1] / "cores" / "music_core.py"
    spec = importlib.util.spec_from_file_location("music_core_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class MusicCoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_music_core()

    def setUp(self):
        self.core._recommendation_thread = None
        self.core._profile_thread = None
        self.core._continuation_thread = None
        with self.core._artwork_cache_lock:
            self.core._artwork_cache.clear()
            self.core._artwork_inflight.clear()
            self.core._artwork_failure_until.clear()
        self.redis = FakeRedis()
        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "provider": "tater_tube",
                "server_url": "http://tube.local:8080",
                "token": "player-token",
                "default_target": "voice_core:native:kitchen",
                "default_volume_percent": "70",
                "default_shuffle": "true",
                "maximum_queue_tracks": "200",
            },
        )
        self.tracks = [
            {
                "id": "track:one",
                "title": "Three Little Birds",
                "artist": "Bob Marley & The Wailers",
                "album_artist": "Bob Marley & The Wailers",
                "album": "Exodus",
                "genres": ["Reggae", "Roots Reggae"],
                "genre": "Reggae, Roots Reggae",
                "duration_seconds": 180,
                "category_id": "local:music",
                "source_index": 0,
                "path": "Bob Marley/Exodus/09 Three Little Birds.flac",
                "has_artwork": True,
                "modified_unix": 1234,
                "provider": "tater_tube",
            },
            {
                "id": "track:two",
                "title": "Blue in Green",
                "artist": "Miles Davis",
                "album_artist": "Miles Davis",
                "album": "Kind of Blue",
                "genres": ["Jazz"],
                "genre": "Jazz",
                "duration_seconds": 220,
                "category_id": "local:music",
                "source_index": 0,
                "path": "Miles Davis/Kind of Blue/03 Blue in Green.flac",
                "provider": "tater_tube",
            },
        ]
        self.redis.set(
            self.core.CATALOG_KEY,
            json.dumps(
                {
                    "tracks": self.tracks,
                    "artists": ["Bob Marley & The Wailers", "Miles Davis"],
                    "albums": ["Exodus", "Kind of Blue"],
                    "genres": ["Jazz", "Reggae", "Roots Reggae"],
                    "synced_at": 100,
                }
            ),
        )

    def test_core_system_tasks_publish_runnable_and_event_driven_music_jobs(self):
        self.redis.set(
            self.core.HISTORY_KEY,
            json.dumps(
                [
                    {
                        "track_id": "track:one",
                        "provider": "tater_tube",
                        "played_at": 123,
                    }
                ]
            ),
        )
        self.redis.hset(
            self.core.RUNTIME_KEY,
            mapping={
                "last_sync_at": "100",
                "last_recommendation_at": "200",
                "last_continuation_at": "300",
            },
        )

        payload = self.core.get_core_system_tasks(redis_client=self.redis)
        tasks = {row["id"]: row for row in payload["tasks"]}

        self.assertEqual(payload["label"], "Music Core")
        self.assertEqual(
            set(tasks),
            {
                "catalog_sync",
                "recommendation_refresh",
                "music_profile_refresh",
                "continuous_radio_refill",
            },
        )
        self.assertTrue(tasks["catalog_sync"]["available"])
        self.assertTrue(tasks["recommendation_refresh"]["available"])
        self.assertFalse(tasks["continuous_radio_refill"]["manual"])
        self.assertEqual(tasks["continuous_radio_refill"]["schedule_label"], "Event driven")
        self.assertEqual(tasks["continuous_radio_refill"]["next_run_label"], "Near queue end")

    def test_listening_history_publishes_privacy_safe_tater_tube_activity(self):
        track = {
            **self.tracks[0],
            "stream_url": "http://tube.local/private?token=secret",
            "path": "/private/music/song.flac",
        }
        self.core._record_listening_history(
            track,
            ["voice_core:native:kitchen"],
            person_id="person-secret",
            client=self.redis,
        )
        rows = self.core.get_tater_tube_activity_events(redis_client=self.redis)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Three Little Birds")
        encoded = json.dumps(rows[0])
        self.assertNotIn("token=secret", encoded)
        self.assertNotIn("/private/music", encoded)
        self.assertNotIn("person-secret", encoded)
        self.assertNotIn("kitchen", encoded)

    def test_core_system_task_runner_dispatches_manual_music_jobs(self):
        with patch.object(
            self.core,
            "_sync_catalog",
            return_value={"tracks": [{"id": "one"}, {"id": "two"}]},
        ) as sync_catalog:
            result = self.core.run_core_system_task(
                task_id="catalog_sync",
                redis_client=self.redis,
            )
        self.assertEqual(result, {"ok": True, "track_count": 2})
        sync_catalog.assert_called_once_with(self.redis, "tater_tube")

        with patch.object(
            self.core,
            "_generate_recommendations",
            return_value={"playlists": [{"id": "mix"}]},
        ) as refresh_recommendations:
            result = self.core.run_core_system_task(
                task_id="recommendation_refresh",
                redis_client=self.redis,
            )
        self.assertEqual(result, {"ok": True, "playlist_count": 1})
        refresh_recommendations.assert_called_once_with(self.redis)

        with patch.object(
            self.core,
            "_generate_music_prompt_profile",
            return_value={"person_id": "person-1", "history_event_count": 4},
        ) as refresh_profile:
            result = self.core.run_core_system_task(
                task_id="music_profile_refresh",
                redis_client=self.redis,
            )
        self.assertEqual(
            result,
            {"ok": True, "person_id": "person-1", "history_event_count": 4},
        )
        refresh_profile.assert_called_once_with(self.redis)

        with self.assertRaises(KeyError):
            self.core.run_core_system_task(
                task_id="continuous_radio_refill",
                redis_client=self.redis,
            )

    def test_searches_genres_and_ignores_generic_music_words(self):
        matches = self.core._search_tracks(
            query="play some reggae music",
            genre="reggae",
            client=self.redis,
        )
        self.assertEqual([row["id"] for row in matches], ["track:one"])

    def test_major_genre_families_expand_specific_tags(self):
        self.assertEqual(
            self.core._genres(["Dancehall", "Alternative Rock", "R&B", "House"]),
            [
                "Dancehall",
                "Reggae",
                "Alternative Rock",
                "Alternative",
                "Rock",
                "R&B/Soul",
                "House",
                "Electronic",
            ],
        )

    def test_reggae_request_matches_specific_reggae_family_tag(self):
        self.tracks[0]["genres"] = ["Dancehall"]
        self.tracks[0]["genre"] = "Dancehall"
        self.redis.set(
            self.core.CATALOG_KEY,
            json.dumps(
                {
                    "provider": "tater_tube",
                    "tracks": self.tracks,
                    "genres": ["Dancehall"],
                    "synced_at": 100,
                }
            ),
        )
        with self.core._catalog_memory_cache_lock:
            self.core._catalog_memory_cache["loaded_at"] = -1000.0

        matches = self.core._search_tracks(
            query="play reggae music",
            genre="reggae",
            client=self.redis,
        )
        self.assertEqual([row["id"] for row in matches], ["track:one"])
        self.assertIn("Reggae", self.core._catalog(self.redis)["genres"])

    def test_stream_url_is_derived_without_storing_provider_token_in_catalog(self):
        provider = self.core.TaterTubeMusicProvider.from_settings(
            self.redis.hgetall(self.core.SETTINGS_KEY)
        )
        url = provider.stream_url(self.tracks[0])
        self.assertIn("/api/tater/local/stream?", url)
        self.assertIn("player_token=player-token", url)
        self.assertNotIn("player-token", json.dumps(self.tracks))
        self.assertEqual(self.core._track_media_type(self.tracks[0]), "audio/flac")

        sync_url = provider.stream_url(self.tracks[0], audio_sync=True)
        self.assertIn("transcode=1", sync_url)
        self.assertIn("profile=audio_sync", sync_url)

        artwork_url = provider.artwork_url(self.tracks[0])
        self.assertEqual(artwork_url, "")

    def test_tater_tube_proxies_catalog_artwork_without_storing_player_token(self):
        track = self.core._normalize_track(
            {
                "ratingKey": "track:art",
                "title": "Covered Song",
                "categoryId": "local:music",
                "path": "Artist/Album/song.flac",
                "poster": (
                    "http://tube.local:8080/api/tater/music/artwork?"
                    "album_id=album%3Acovered&v=42&player_token=secret"
                ),
                "hasArtwork": True,
            }
        )
        self.assertTrue(track["has_artwork"])
        self.assertEqual(
            track["artwork_path"],
            "/api/tater/music/artwork?album_id=album%3Acovered&v=42",
        )
        self.assertEqual(track["artwork_version"], "42")
        self.assertNotIn("secret", json.dumps(track))
        public = self.core._public_track(track)
        self.assertTrue(public["artwork_url"].startswith("/api/cores/music_core/webhook/artwork?"))
        provider = self.core.TaterTubeMusicProvider(
            server_url="http://tube.local:8080",
            token="player-token",
        )
        artwork_url = provider.artwork_url(track)
        self.assertTrue(artwork_url.startswith("http://tube.local:8080/api/tater/music/artwork?"))
        self.assertIn("album_id=album%3Acovered", artwork_url)
        self.assertIn("v=42", artwork_url)
        self.assertIn("player_token=player-token", artwork_url)

    def test_tater_tube_rejects_non_artwork_provider_urls(self):
        provider = self.core.TaterTubeMusicProvider(
            server_url="http://tube.local:8080",
            token="player-token",
        )
        self.assertEqual(
            provider.artwork_url({"artwork_path": "https://example.com/not-an-artwork-route"}),
            "",
        )

    def test_catalog_artwork_schema_forces_one_refresh_after_upgrade(self):
        self.redis.set(
            self.core.CATALOG_KEY,
            json.dumps({"provider": "tater_tube", "artwork_schema": 2, "tracks": []}),
        )
        self.assertTrue(self.core._catalog_needs_artwork_refresh(client=self.redis))

        self.redis.set(
            self.core.CATALOG_KEY,
            json.dumps(
                {
                    "provider": "tater_tube",
                    "artwork_schema": self.core.CATALOG_ARTWORK_SCHEMA,
                    "tracks": [],
                }
            ),
        )
        with self.core._catalog_memory_cache_lock:
            self.core._catalog_memory_cache["loaded_at"] = -1000.0
        self.assertFalse(self.core._catalog_needs_artwork_refresh(client=self.redis))

    def test_artwork_webhook_proxies_provider_image_and_caches_it(self):
        response = Mock()
        response.content = b"\xff\xd8album-cover\xff\xd9"
        response.headers = {"Content-Type": "image/jpeg"}
        response.raise_for_status.return_value = None
        self.core._artwork_cache.clear()
        provider = types.SimpleNamespace(
            artwork_url=lambda _track: "http://provider.local/native-cover.jpg?token=secret"
        )
        starlette = types.ModuleType("starlette")
        starlette_responses = types.ModuleType("starlette.responses")

        class FakeResponse:
            def __init__(self, content=b"", media_type=None, headers=None, status_code=200):
                self.body = bytes(content)
                self.media_type = media_type
                self.headers = dict(headers or {})
                self.status_code = status_code

        starlette_responses.Response = FakeResponse
        starlette.responses = starlette_responses
        with patch.dict(
            sys.modules,
            {"starlette": starlette, "starlette.responses": starlette_responses},
        ), patch.object(self.core, "_provider", return_value=provider), patch.object(
            self.core.requests,
            "get",
            return_value=response,
        ) as fetch:
            first = self.core.handle_core_webhook(
                webhook="artwork",
                query={"track_id": "track:one", "provider": "tater_tube"},
                redis_client=self.redis,
            )
            second = self.core.handle_core_webhook(
                webhook="artwork",
                query={"track_id": "track:one", "provider": "tater_tube"},
                redis_client=self.redis,
            )
        self.assertEqual(first.body, b"\xff\xd8album-cover\xff\xd9")
        self.assertEqual(first.media_type, "image/jpeg")
        self.assertEqual(second.body, first.body)
        fetch.assert_called_once()
        self.assertEqual(
            fetch.call_args.kwargs["timeout"],
            (
                self.core.ARTWORK_CONNECT_TIMEOUT_SECONDS,
                self.core.ARTWORK_READ_TIMEOUT_SECONDS,
            ),
        )

    def test_artwork_fetch_deduplicates_simultaneous_provider_requests(self):
        response = Mock()
        response.content = b"\xff\xd8album-cover\xff\xd9"
        response.headers = {"Content-Type": "image/jpeg"}
        response.raise_for_status.return_value = None
        provider = types.SimpleNamespace(
            artwork_url=lambda _track: "http://provider.local/native-cover.jpg?token=secret"
        )
        fetch_started = threading.Event()
        release_fetch = threading.Event()
        results = []
        errors = []

        def slow_fetch(*_args, **_kwargs):
            fetch_started.set()
            release_fetch.wait(2.0)
            return response

        def load_artwork():
            try:
                results.append(self.core._fetch_track_artwork(self.tracks[0], self.redis))
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        with patch.object(self.core, "_provider", return_value=provider), patch.object(
            self.core.requests,
            "get",
            side_effect=slow_fetch,
        ) as fetch:
            first = threading.Thread(target=load_artwork)
            second = threading.Thread(target=load_artwork)
            first.start()
            self.assertTrue(fetch_started.wait(1.0))
            second.start()
            time.sleep(0.05)
            release_fetch.set()
            first.join(2.0)
            second.join(2.0)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["body"], results[1]["body"])
        fetch.assert_called_once()

    def test_artwork_webhook_returns_cached_placeholder_after_provider_failure(self):
        provider = types.SimpleNamespace(
            artwork_url=lambda _track: "http://provider.local/missing-cover.jpg?token=secret"
        )
        starlette = types.ModuleType("starlette")
        starlette_responses = types.ModuleType("starlette.responses")

        class FakeResponse:
            def __init__(self, content=b"", media_type=None, headers=None, status_code=200):
                self.body = bytes(content)
                self.media_type = media_type
                self.headers = dict(headers or {})
                self.status_code = status_code

        starlette_responses.Response = FakeResponse
        starlette.responses = starlette_responses
        with patch.dict(
            sys.modules,
            {"starlette": starlette, "starlette.responses": starlette_responses},
        ), patch.object(self.core, "_provider", return_value=provider), patch.object(
            self.core.requests,
            "get",
            side_effect=TimeoutError("provider timed out"),
        ) as fetch:
            first = self.core.handle_core_webhook(
                webhook="artwork",
                query={"track_id": "track:one", "provider": "tater_tube"},
                redis_client=self.redis,
            )
            second = self.core.handle_core_webhook(
                webhook="artwork",
                query={"track_id": "track:one", "provider": "tater_tube"},
                redis_client=self.redis,
            )

        self.assertEqual(first.media_type, "image/svg+xml")
        self.assertTrue(first.body.startswith(b"<svg"))
        self.assertEqual(first.headers["X-Tater-Artwork-Fallback"], "1")
        self.assertEqual(second.body, first.body)
        fetch.assert_called_once()

    def test_native_session_failure_updates_player_status(self):
        player = {
            "status": "playing",
            "started_at": 100,
            "targets": ["voice_core:native:kitchen"],
            "playback_result": {
                "voice_core_sessions": [
                    {
                        "target": "native:kitchen",
                        "session_id": "music-session-1",
                        "selectors": ["native:kitchen"],
                    }
                ]
            },
        }
        native_satellite = types.SimpleNamespace(
            status_snapshot_sync=lambda: {
                "clients": {
                    "native:kitchen": {
                        "media_session": {
                            "active": False,
                            "session_id": "music-session-1",
                            "ok": False,
                            "finished_ts": 101,
                        }
                    }
                }
            }
        )
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite
        with patch.dict(sys.modules, {"tater_voice": tater_voice}):
            reconciled = self.core._reconcile_native_playback(player, self.redis)

        self.assertEqual(reconciled["status"], "error")
        self.assertIn("native:kitchen", reconciled["last_error"])
        self.assertEqual(
            self.core._player(self.redis)["status"],
            "error",
        )

    def test_native_session_failure_is_only_a_warning_when_another_player_was_started(self):
        player = {
            "status": "playing",
            "started_at": 100,
            "playback_result": {
                "sent_count": 2,
                "voice_core_sent_count": 1,
                "voice_core_sessions": [
                    {
                        "target": "native:kitchen",
                        "session_id": "music-session-1",
                        "selectors": ["native:kitchen"],
                    }
                ],
            },
        }
        native_satellite = types.SimpleNamespace(
            status_snapshot_sync=lambda: {
                "clients": {
                    "native:kitchen": {
                        "media_session": {
                            "active": False,
                            "session_id": "music-session-1",
                            "ok": False,
                            "finished_ts": 101,
                        }
                    }
                }
            }
        )
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite
        with patch.dict(sys.modules, {"tater_voice": tater_voice}):
            reconciled = self.core._reconcile_native_playback(player, self.redis)

        self.assertEqual(reconciled["status"], "playing")
        self.assertTrue(reconciled["warnings"])

    def test_removed_provider_migrates_to_tater_tube_and_clears_stale_player(self):
        self.redis.hset(self.core.SETTINGS_KEY, mapping={"provider": "plex"})
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "provider": "plex",
                    "queue": [],
                    "index": -1,
                    "current": {"id": "plex:legacy", "title": "Jazz", "provider": "plex"},
                    "targets": ["voice_core:native:kitchen"],
                }
            ),
        )
        player = self.core._player(self.redis)
        self.assertEqual(self.core._provider_id("plex"), "tater_tube")
        self.assertEqual(player["provider"], "tater_tube")
        self.assertEqual(player["status"], "stopped")
        self.assertEqual(player["queue"], [])
        self.assertEqual(player["current"], {})
        self.assertEqual(player["targets"], ["voice_core:native:kitchen"])

    def test_target_picker_hides_roon_zones_from_music_core(self):
        announcement_targets = types.ModuleType("announcement_targets")
        announcement_targets.build_announcement_target_options = Mock(
            return_value=[
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                },
                {
                    "value": "integration:roon:zone-kitchen",
                    "label": "Roon: Kitchen",
                },
            ]
        )
        with patch.dict(
            sys.modules,
            {"announcement_targets": announcement_targets},
        ):
            stream_options = self.core._target_options(provider_id="tater_tube")
        self.assertEqual(
            [row["value"] for row in stream_options],
            ["voice_core:native:kitchen"],
        )
        self.assertEqual(stream_options[0]["label"], "Tater Sat: Kitchen")
        self.assertTrue(
            all(
                call.kwargs.get("include_homeassistant") is True
                for call in announcement_targets.build_announcement_target_options.call_args_list
            )
        )
        self.assertTrue(
            all(
                call.kwargs.get("include_airplay") is True
                for call in announcement_targets.build_announcement_target_options.call_args_list
            )
        )

    def test_target_picker_hides_satellites_that_belong_to_a_stereo_pair(self):
        announcement_targets = types.ModuleType("announcement_targets")
        announcement_targets.build_announcement_target_options = Mock(
            return_value=[
                {
                    "value": "voice_core:native:sat1",
                    "label": "Tater Satellite: Sat 1",
                },
                {
                    "value": "voice_core:native:voicepe",
                    "label": "Tater Satellite: Voice PE",
                },
                {
                    "value": "voice_core:stereo:bedroom12",
                    "label": "Tater Stereo: Bedroom",
                },
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                },
            ]
        )
        integration_registry = types.ModuleType("integration_registry")
        integration_registry.get_integration_devices_by_capability = Mock(return_value=[])
        stereo_pairs = types.ModuleType("tater_voice.stereo_pairs")
        stereo_pairs.list_pairs = Mock(
            return_value=[
                {
                    "selector": "stereo:bedroom12",
                    "left_selector": "native:sat1",
                    "right_selector": "native:voicepe",
                }
            ]
        )
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.stereo_pairs = stereo_pairs

        with patch.dict(
            sys.modules,
            {
                "announcement_targets": announcement_targets,
                "integration_registry": integration_registry,
                "tater_voice": tater_voice,
                "tater_voice.stereo_pairs": stereo_pairs,
            },
        ):
            visible = self.core._target_options(provider_id="tater_tube")
            resolution_options = self.core._target_options(
                provider_id="tater_tube",
                include_stereo_members=True,
            )

        self.assertEqual(
            {row["value"] for row in visible},
            {
                "voice_core:stereo:bedroom12",
                "voice_core:native:kitchen",
            },
        )
        self.assertEqual(len(resolution_options), 4)

    def test_target_resolution_routes_stereo_members_to_the_pair_and_deduplicates(self):
        member_routes = {
            "native:sat1": "voice_core:stereo:bedroom12",
            "voice_core:native:sat1": "voice_core:stereo:bedroom12",
            "native:voicepe": "voice_core:stereo:bedroom12",
            "voice_core:native:voicepe": "voice_core:stereo:bedroom12",
        }
        options = [
            {
                "value": "voice_core:native:sat1",
                "label": "Tater Satellite: Sat 1",
            },
            {
                "value": "voice_core:native:voicepe",
                "label": "Tater Satellite: Voice PE",
            },
            {
                "value": "voice_core:stereo:bedroom12",
                "label": "Tater Stereo: Bedroom",
            },
        ]
        with patch.object(
            self.core,
            "_stereo_member_target_map",
            return_value=member_routes,
        ), patch.object(
            self.core,
            "_target_options",
            return_value=options,
        ) as target_options:
            targets = self.core._resolve_targets(
                [
                    "Tater Satellite: Sat 1",
                    "voice_core:native:voicepe",
                    "voice_core:stereo:bedroom12",
                ],
                client=self.redis,
                provider_id="tater_tube",
            )
            origin_targets = self.core._resolve_targets(
                origin={"satellite_selector": "native:sat1"},
                client=self.redis,
            )

        self.assertEqual(targets, ["voice_core:stereo:bedroom12"])
        self.assertEqual(origin_targets, ["voice_core:stereo:bedroom12"])
        self.assertTrue(
            all(call.kwargs.get("include_stereo_members") is True for call in target_options.call_args_list)
        )

    def test_native_client_state_is_credential_free_and_exposes_targets(self):
        with patch.object(
            self.core,
            "_target_options",
            return_value=[
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                }
            ],
        ):
            payload = self.core.get_client_music_state(
                query="reggae",
                client=self.redis,
            )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["provider"]["id"], "tater_tube")
        self.assertEqual(
            [provider["id"] for provider in payload["providers"]],
            ["tater_tube"],
        )
        self.assertEqual(payload["tracks"][0]["id"], "track:one")
        self.assertEqual(payload["targets"][0]["kind"], "satellite")
        self.assertNotIn("player-token", json.dumps(payload))

    def test_native_client_library_returns_80_personalized_tracks_from_history_and_ai_picks(self):
        tracks = [
            {
                "id": f"track:{index}",
                "title": f"Song {index:03d}",
                "artist": "Favorite Artist" if index < 50 else f"Artist {index}",
                "album_artist": "Favorite Artist" if index < 50 else f"Artist {index}",
                "album": "Favorite Album" if index < 20 else f"Album {index}",
                "genres": ["Reggae"] if index < 50 else ["Jazz"],
                "provider": "tater_tube",
            }
            for index in range(100)
        ]
        self.redis.set(
            self.core.CATALOG_KEY,
            json.dumps(
                {
                    "provider": "tater_tube",
                    "tracks": tracks,
                    "artists": [],
                    "albums": [],
                    "genres": [],
                }
            ),
        )
        self.redis.set(
            self.core.HISTORY_KEY,
            json.dumps(
                [
                    {
                        "track_id": "track:0",
                        "artist": "Favorite Artist",
                        "album_artist": "Favorite Artist",
                        "album": "Favorite Album",
                        "genres": ["Reggae"],
                        "provider": "tater_tube",
                        "played_at": 123,
                    }
                ]
            ),
        )
        self.redis.set(
            self.core.RECOMMENDATIONS_KEY,
            json.dumps(
                {
                    "provider": "tater_tube",
                    "generated_at": 456,
                    "playlists": [{"id": "ai-mix", "track_ids": ["track:90"]}],
                }
            ),
        )

        with patch.object(self.core, "_target_options", return_value=[]):
            payload = self.core.get_client_music_state(limit=80, client=self.redis)

        self.assertEqual(len(payload["tracks"]), 80)
        self.assertEqual(payload["tracks"][0]["id"], "track:90")
        self.assertNotIn("track:0", [row["id"] for row in payload["tracks"][:20]])
        self.assertEqual(payload["track_feed"]["kind"], "personalized")
        self.assertEqual(payload["track_feed"]["title"], "For You")
        self.assertEqual(payload["track_feed"]["history_event_count"], 1)
        self.assertEqual(payload["track_feed"]["ai_seed_count"], 1)

    def test_native_client_library_falls_back_until_listening_history_exists(self):
        with patch.object(self.core, "_target_options", return_value=[]):
            payload = self.core.get_client_music_state(limit=80, client=self.redis)

        self.assertEqual(
            [row["id"] for row in payload["tracks"]],
            ["track:one", "track:two"],
        )
        self.assertEqual(payload["track_feed"]["kind"], "library")
        self.assertEqual(payload["track_feed"]["title"], "Library")
        self.assertIn("personalize", payload["track_feed"]["summary"])

    def test_native_client_state_exposes_unified_sonos_airplay_route_metadata(self):
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "paused",
                    "provider": "tater_tube",
                    "targets": ["airplay:804af2c57d78"],
                    "position_offset_seconds": 42,
                }
            ),
        )
        options = [
            {
                "value": "sonos:RINCON_KITCHEN",
                "label": "Sonos: Kitchen",
                "description": "Automatic uses AirPlay Bridge with Tater sats.",
                "target_aliases": ["airplay:804af2c57d78"],
                "airplay_bridge_target": "airplay:804af2c57d78",
                "transport_options": [
                    {"value": "auto", "label": "Automatic"},
                    {"value": "native", "label": "Native Sonos"},
                    {"value": "airplay", "label": "AirPlay Bridge"},
                ],
            }
        ]

        with patch.object(self.core, "_target_options", return_value=options):
            payload = self.core.get_client_music_state(client=self.redis)

        target = payload["targets"][0]
        self.assertEqual(target["id"], "sonos:RINCON_KITCHEN")
        self.assertEqual(target["kind"], "media_player")
        self.assertEqual(target["airplay_bridge_target"], "airplay:804af2c57d78")
        self.assertEqual(target["transport_mode"], "auto")
        self.assertEqual(
            [row["value"] for row in target["transport_options"]],
            ["auto", "native", "airplay"],
        )
        self.assertEqual(payload["player"]["targets"], ["sonos:RINCON_KITCHEN"])

    def test_native_client_state_exposes_live_queue_progress_and_recommendations(self):
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "paused",
                    "provider": "tater_tube",
                    "queue": self.tracks,
                    "index": 0,
                    "current": self.tracks[0],
                    "targets": ["voice_core:native:kitchen"],
                    "position_offset_seconds": 42,
                    "duration_seconds": 180,
                    "continuous_radio": True,
                    "radio_name": "Morning Roots",
                    "volume_percent": 63,
                }
            ),
        )
        self.redis.set(
            self.core.RECOMMENDATIONS_KEY,
            json.dumps(
                {
                    "provider": "tater_tube",
                    "generated_at": 456,
                    "summary": "Built from your recent reggae plays.",
                    "playlists": [
                        {
                            "id": "morning-roots",
                            "name": "Morning Roots",
                            "description": "Easy reggae for the morning.",
                            "track_ids": ["track:one", "track:two"],
                        }
                    ],
                }
            ),
        )
        with patch.object(self.core, "_target_options", return_value=[]):
            payload = self.core.get_client_music_state(client=self.redis)

        self.assertEqual([row["id"] for row in payload["player"]["queue"]], ["track:one", "track:two"])
        self.assertEqual(payload["player"]["position_seconds"], 42)
        self.assertEqual(payload["player"]["duration_seconds"], 180)
        self.assertTrue(payload["player"]["seekable"])
        self.assertEqual(payload["recommendations"][0]["id"], "morning-roots")
        self.assertEqual(
            [row["id"] for row in payload["recommendations"][0]["tracks"]],
            ["track:one", "track:two"],
        )

    def test_native_client_recommendation_honors_selected_targets_and_volume(self):
        self.redis.set(
            self.core.RECOMMENDATIONS_KEY,
            json.dumps(
                {
                    "provider": "tater_tube",
                    "playlists": [
                        {
                            "id": "morning-roots",
                            "name": "Morning Roots",
                            "track_ids": ["track:one", "track:two"],
                        }
                    ],
                }
            ),
        )
        selected_targets = [
            "voice_core:stereo:bedroom",
            "integration:sonos:kitchen",
        ]
        with patch.object(
            self.core,
            "_resolve_targets",
            return_value=selected_targets,
        ) as resolve_targets, patch.object(
            self.core,
            "_validate_catalog_provider_targets",
        ), patch.object(
            self.core,
            "_create_and_start_queue",
            return_value={"status": "playing", "targets": selected_targets},
        ) as create_queue:
            result = self.core._play_recommendation(
                "morning-roots",
                self.redis,
                requested_targets=selected_targets,
                volume_percent=38,
            )

        self.assertEqual(result["status"], "playing")
        self.assertEqual(resolve_targets.call_args.args[0], selected_targets)
        self.assertEqual(create_queue.call_args.kwargs["targets"], selected_targets)
        self.assertEqual(create_queue.call_args.kwargs["volume_percent"], 38)
        self.assertEqual(
            [row["id"] for row in create_queue.call_args.args[0]],
            ["track:one", "track:two"],
        )

    def test_native_client_live_volume_action_updates_active_players(self):
        target = "voice_core:native:kitchen"
        player = {
            "status": "playing",
            "provider": "tater_tube",
            "queue": self.tracks,
            "index": 0,
            "current": self.tracks[0],
            "targets": [target],
            "volume_percent": 70,
        }
        self.redis.set(self.core.PLAYER_KEY, json.dumps(player))
        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "player_calibrations": json.dumps(
                    {
                        target: {
                            "volume_percent": 70,
                            "sync_offset_ms": 24,
                        }
                    }
                )
            },
        )
        with patch.object(
            self.core,
            "_reconcile_native_playback",
            return_value=player,
        ), patch.object(
            self.core,
            "_set_target_volume",
            return_value={"sent_count": 1, "warnings": []},
        ) as set_volume, patch.object(self.core, "_target_options", return_value=[]):
            result = self.core.run_client_music_action(
                "set_volume",
                {"volume_percent": 31},
                client=self.redis,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(self.core._player(self.redis)["volume_percent"], 31)
        set_volume.assert_called_once_with(player, 31)
        calibration = self.core._player_calibrations(self.core._settings(self.redis))[target]
        self.assertEqual(calibration["volume_percent"], 31)
        self.assertEqual(calibration["sync_offset_ms"], 24)

        with patch.object(
            self.core,
            "_stop_target",
            return_value=[],
        ), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ) as play_track:
            advanced = self.core._advance_player(1, client=self.redis)

        self.assertEqual(advanced["current"]["id"], "track:two")
        self.assertEqual(play_track.call_args.kwargs["volume_percent"], 31)
        self.assertEqual(
            play_track.call_args.kwargs["player_settings"][target]["volume_percent"],
            31,
        )

    def test_little_spud_continuation_returns_ai_tracks_without_starting_remote_player(self):
        with patch.object(
            self.core,
            "_get_primary_llm_client_from_env",
            return_value=object(),
        ), patch.object(
            self.core,
            "_select_continuation_tracks",
            return_value=([self.tracks[1]], "Pocket Roots"),
        ) as select_tracks:
            result = self.core.run_client_music_action(
                "continue_local",
                {
                    "provider": "tater_tube",
                    "track_id": "track:one",
                    "track_ids": ["track:one"],
                    "queue_index": 0,
                    "queue_session_id": "little-spud-session",
                },
                client=self.redis,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["continuous_radio"])
        self.assertEqual(result["station_name"], "Pocket Roots")
        self.assertEqual([row["id"] for row in result["tracks"]], ["track:two"])
        self.assertIsNone(self.redis.get(self.core.PLAYER_KEY))
        synthetic_player = select_tracks.call_args.args[2]
        self.assertEqual(synthetic_player["current"]["id"], "track:one")
        self.assertEqual(synthetic_player["targets"], ["little_spud:local"])

    def test_little_spud_playback_is_recorded_in_shared_listening_history(self):
        result = self.core.run_client_music_action(
            "local_play_started",
            {
                "provider": "tater_tube",
                "track_id": "track:one",
            },
            client=self.redis,
        )

        self.assertTrue(result["ok"])
        history = self.core._listening_history(self.redis)
        self.assertEqual(history[-1]["track_id"], "track:one")
        self.assertEqual(history[-1]["targets"], ["little_spud:local"])

    def test_native_client_can_play_an_exact_track_and_resolve_local_stream(self):
        with patch.object(
            self.core,
            "_target_options",
            return_value=[
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                }
            ],
        ), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ):
            result = self.core.run_client_music_action(
                "play",
                {
                    "track_id": "track:two",
                    "target": "voice_core:native:kitchen",
                    "volume_percent": 42,
                },
                client=self.redis,
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["now_playing"]["id"], "track:two")
        self.assertEqual(
            self.core._player(self.redis)["volume_percent"],
            42,
        )
        source = self.core.get_client_music_stream_source(
            "track:two",
            client=self.redis,
        )
        self.assertEqual(source["track"]["id"], "track:two")
        self.assertIn("player_token=player-token", source["source_url"])

    def test_native_client_can_replace_queue_with_ordered_track_ids(self):
        selected_target = "voice_core:native:kitchen"
        with patch.object(
            self.core,
            "_target_options",
            return_value=[
                {
                    "value": selected_target,
                    "label": "Tater Satellite: Kitchen",
                }
            ],
        ), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ):
            result = self.core.run_client_music_action(
                "play_queue",
                {
                    "track_ids": ["track:two", "track:one"],
                    "target": selected_target,
                    "volume_percent": 44,
                },
                client=self.redis,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["now_playing"]["id"], "track:two")
        player = self.core._player(self.redis)
        self.assertEqual(
            [track["id"] for track in player["queue"]],
            ["track:two", "track:one"],
        )
        self.assertEqual(player["index"], 0)
        self.assertEqual(player["volume_percent"], 44)

    def test_target_resolution_prefers_room_then_speaking_satellite_then_default(self):
        with patch.object(self.core, "_target_options", return_value=[]), patch.object(
            self.core,
            "_preferred_room_target",
            return_value="integration:sonos:kitchen",
        ):
            self.assertEqual(
                self.core._resolve_target(
                    room="Kitchen",
                    origin={"satellite_selector": "native:kitchen"},
                    client=self.redis,
                ),
                "integration:sonos:kitchen",
            )
        with patch.object(self.core, "_target_options", return_value=[]), patch.object(
            self.core,
            "_preferred_room_target",
            return_value="",
        ):
            self.assertEqual(
                self.core._resolve_target(
                    origin={"satellite_selector": "native:kitchen"},
                    client=self.redis,
                ),
                "voice_core:native:kitchen",
            )
            self.assertEqual(
                self.core._resolve_target(client=self.redis),
                "voice_core:native:kitchen",
            )

    def test_explicit_room_overrides_a_speaking_satellite_or_supplied_target(self):
        options = [
            {
                "value": "voice_core:native:office",
                "label": "Tater Satellite: Office",
            },
            {
                "value": "sonos:family-room",
                "label": "Sonos: Family Room",
            },
        ]
        with patch.object(self.core, "_target_options", return_value=options), patch.object(
            self.core,
            "_preferred_room_target",
            return_value="sonos:family-room",
        ):
            targets = self.core._resolve_targets(
                requested="voice_core:native:office",
                room="Family Room",
                origin={
                    "room_name": "Office",
                    "satellite_selector": "native:office",
                },
                client=self.redis,
            )
            room_passed_as_target = self.core._resolve_targets(
                requested="Family Room",
                origin={"satellite_selector": "native:office"},
                client=self.redis,
            )
        self.assertEqual(targets, ["sonos:family-room"])
        self.assertEqual(room_passed_as_target, ["sonos:family-room"])

    def test_automatic_room_resolution_prefers_sonos_when_room_has_multiple_players(self):
        options = [
            {
                "value": "voice_core:native:family-room",
                "label": "Tater Satellite: Family Room",
            },
            {
                "value": "sonos:family-room",
                "label": "Sonos: Family Room",
            },
        ]
        with patch.object(self.core, "_target_options", return_value=options), patch.object(
            self.core,
            "_preferred_room_target",
            return_value="",
        ):
            targets = self.core._resolve_targets(
                room="Family Room",
                client=self.redis,
            )
        self.assertEqual(targets, ["sonos:family-room"])

    def test_target_resolution_supports_multiple_rooms_and_saved_defaults(self):
        with patch.object(self.core, "_target_options", return_value=[]), patch.object(
            self.core,
            "_preferred_room_target",
            side_effect=lambda rooms, _client: {
                "Kitchen": "voice_core:native:kitchen",
                "Living Room": "integration:homeassistant:media_player.living_room",
            }.get(rooms[0], ""),
        ):
            self.assertEqual(
                self.core._resolve_targets(
                    room=["Kitchen", "Living Room"],
                    client=self.redis,
                ),
                [
                    "voice_core:native:kitchen",
                    "integration:homeassistant:media_player.living_room",
                ],
            )

        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "default_targets": json.dumps(
                    [
                        "voice_core:native:kitchen",
                        "integration:homeassistant:media_player.living_room",
                    ]
                )
            },
        )
        with patch.object(self.core, "_target_options", return_value=[]):
            self.assertEqual(
                self.core._resolve_targets(client=self.redis),
                [
                    "voice_core:native:kitchen",
                    "integration:homeassistant:media_player.living_room",
                ],
            )

    def test_play_request_builds_queue_and_starts_multiple_selected_targets(self):
        with patch.object(
            self.core,
            "_target_options",
            return_value=[
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                },
                {
                    "value": "integration:homeassistant:media_player.living_room",
                    "label": "Living Room TV",
                },
            ],
        ), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1, "media_session_sent_count": 1},
        ) as play:
            result = self.core._play_request(
                {
                    "genre": "reggae",
                    "targets": ["Kitchen", "Living Room TV"],
                    "shuffle": False,
                    "volume_percent": 65,
                },
                {},
                self.redis,
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["target"], "voice_core:native:kitchen")
        self.assertEqual(
            result["targets"],
            [
                "voice_core:native:kitchen",
                "integration:homeassistant:media_player.living_room",
            ],
        )
        self.assertEqual(result["target_count"], 2)
        self.assertEqual(result["now_playing"]["id"], "track:one")
        self.assertEqual(play.call_args.args[1], result["targets"])
        player = self.core._player(self.redis)
        self.assertEqual(player["status"], "playing")
        self.assertEqual(player["targets"], result["targets"])
        self.assertEqual(player["volume_percent"], 65)
        history = self.core._listening_history(self.redis)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["track_id"], "track:one")
        self.assertEqual(history[0]["genres"], ["Reggae", "Roots Reggae"])
        self.assertNotIn("player-token", json.dumps(history))

    def test_recommendations_use_real_catalog_ids_and_publish_ai_named_playlists(self):
        self.core._record_listening_history(
            self.tracks[0],
            ["voice_core:native:kitchen"],
            client=self.redis,
        )
        candidates, _candidate_map = self.core._recommendation_candidates(
            self.core._catalog(self.redis),
            self.core._listening_history(self.redis),
        )
        album_candidate = next(
            row for row in candidates if row["type"] == "album" and row["title"] == "Exodus"
        )
        song_candidate = next(
            row
            for row in candidates
            if row["type"] == "song" and row["title"] == "Blue in Green"
        )

        class FakeLlm:
            async def chat(self, **_kwargs):
                return {
                    "message": {
                        "content": json.dumps(
                            {
                                "summary": "A mellow roots-and-jazz detour made for you.",
                                "playlists": [
                                    {
                                        "name": "Sunlit Side Roads",
                                        "description": "Easygoing favorites with a jazz turn.",
                                        "items": [
                                            {
                                                "candidate_id": album_candidate["id"],
                                                "reason": "It follows the roots sound you played.",
                                            },
                                            {
                                                "candidate_id": song_candidate["id"],
                                                "reason": "A calm exploratory change of pace.",
                                            },
                                            {
                                                "candidate_id": "song:not-in-the-library",
                                                "reason": "This must be discarded.",
                                            },
                                        ],
                                    }
                                ],
                            }
                        )
                    }
                }

        result = self.core._generate_recommendations(
            self.redis,
            llm_client=FakeLlm(),
        )
        self.assertEqual(result["playlists"][0]["name"], "Sunlit Side Roads")
        self.assertEqual(len(result["playlists"][0]["items"]), 2)
        self.assertEqual(
            result["playlists"][0]["track_ids"],
            ["track:one", "track:two"],
        )
        self.assertEqual(
            self.core._recommendations(self.redis)["summary"],
            "A mellow roots-and-jazz detour made for you.",
        )

    def test_music_prompt_profile_is_ai_generated_and_only_injected_for_selected_person(self):
        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "prompt_context_enabled": "true",
                "prompt_person_id": "person-1",
                "prompt_profile_interval_hours": "12",
            },
        )
        self.core._record_listening_history(
            self.tracks[0],
            ["sonos:family-room"],
            person_id="person-1",
            client=self.redis,
        )

        class FakeLlm:
            async def chat(self, **_kwargs):
                return {
                    "message": {
                        "content": json.dumps(
                            {
                                "taste_summary": "Roots reggae is a reliable favorite.",
                                "favorite_artists": ["Bob Marley & The Wailers", "Invented Artist"],
                                "favorite_genres": ["Reggae", "Invented Genre"],
                            }
                        )
                    }
                }

        with patch.object(self.core, "_people_person_name", return_value="Spud Lord"):
            profile = self.core._generate_music_prompt_profile(
                self.redis,
                llm_client=FakeLlm(),
            )
            matching = self.core.get_hydra_system_prompt_fragments(
                role="chat",
                redis_client=self.redis,
                origin={"person_id": "person-1"},
            )
            different = self.core.get_hydra_system_prompt_fragments(
                role="chat",
                redis_client=self.redis,
                origin={"person_id": "person-2"},
            )
            tasks = {
                row["id"]: row
                for row in self.core.get_core_system_tasks(redis_client=self.redis)["tasks"]
            }

        self.assertEqual(profile["person_id"], "person-1")
        self.assertEqual(profile["favorite_artists"], ["Bob Marley & The Wailers"])
        self.assertEqual(profile["favorite_genres"], ["Reggae"])
        self.assertIn("Roots reggae is a reliable favorite", matching["music_context"][0])
        self.assertIn("Three Little Birds by Bob Marley", matching["music_context"][0])
        self.assertEqual(different, {})
        self.assertTrue(tasks["music_profile_refresh"]["available"])

    def test_recommendation_playlist_plays_on_current_player_destinations(self):
        self.redis.set(
            self.core.RECOMMENDATIONS_KEY,
            json.dumps(
                {
                    "provider": "tater_tube",
                    "playlists": [
                        {
                            "id": "morning-mix",
                            "name": "Morning Mix",
                            "track_ids": ["track:two", "track:one"],
                        }
                    ],
                }
            ),
        )
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "stopped",
                    "targets": ["voice_core:native:kitchen"],
                    "volume_percent": 44,
                }
            ),
        )
        with patch.object(
            self.core,
            "_target_options",
            return_value=[
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                }
            ],
        ), patch.object(
            self.core,
            "_validate_catalog_provider_targets",
        ), patch.object(
            self.core,
            "_create_and_start_queue",
            return_value={"current": self.tracks[1]},
        ) as start:
            result = self.core.handle_htmlui_tab_action(
                action="music_recommendation_play",
                payload={"id": "recommendation:morning-mix", "values": {}},
                redis_client=self.redis,
            )
        self.assertTrue(result["ok"])
        self.assertEqual([row["id"] for row in start.call_args.args[0]], ["track:two", "track:one"])
        self.assertEqual(start.call_args.kwargs["targets"], ["voice_core:native:kitchen"])
        self.assertEqual(start.call_args.kwargs["volume_percent"], 44)

    def test_continuous_radio_ai_uses_current_song_and_appends_exact_catalog_tracks(self):
        similar_track = {
            **self.tracks[0],
            "id": "track:similar",
            "title": "Roots Companion",
            "artist": "The Island Players",
            "album_artist": "The Island Players",
            "album": "Warm Current",
        }
        catalog = json.loads(self.redis.get(self.core.CATALOG_KEY))
        catalog["tracks"] = [*self.tracks, similar_track]
        self.redis.set(self.core.CATALOG_KEY, json.dumps(catalog))
        player = {
            "status": "playing",
            "provider": "tater_tube",
            "queue": [self.tracks[0]],
            "queue_original": [self.tracks[0]],
            "index": 0,
            "current": self.tracks[0],
            "targets": ["voice_core:native:kitchen"],
            "queue_session_id": "radio-session",
            "created_at": 100,
        }
        self.redis.set(self.core.PLAYER_KEY, json.dumps(player))

        class FakeLlm:
            def __init__(self):
                self.messages = []

            async def chat(self, **kwargs):
                self.messages = kwargs["messages"]
                return {
                    "message": {
                        "content": json.dumps(
                            {
                                "station_name": "Roots Around the Corner",
                                "items": [
                                    {"track_id": "track:similar"},
                                    {"track_id": "track:not-real"},
                                ],
                            }
                        )
                    }
                }

        llm = FakeLlm()
        added = self.core._generate_continuation(
            player,
            "radio-session",
            self.redis,
            llm_client=llm,
        )
        updated = self.core._player(self.redis)
        prompt_payload = json.loads(llm.messages[1]["content"])
        self.assertGreaterEqual(added, 1)
        self.assertEqual(prompt_payload["currently_playing"]["title"], "Three Little Birds")
        self.assertIn("strongest signal", llm.messages[0]["content"])
        self.assertEqual(updated["queue"][1]["id"], "track:similar")
        self.assertEqual(updated["radio_name"], "Roots Around the Corner")
        self.assertNotIn("track:not-real", [row["id"] for row in updated["queue"]])

    def test_continuous_radio_discards_refill_for_a_replaced_queue(self):
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "provider": "tater_tube",
                    "queue": [self.tracks[0]],
                    "index": 0,
                    "current": self.tracks[0],
                    "queue_session_id": "new-session",
                }
            ),
        )
        added = self.core._append_continuation_tracks(
            "old-session",
            [self.tracks[1]],
            client=self.redis,
        )
        self.assertEqual(added, 0)
        self.assertEqual(
            [row["id"] for row in self.core._player(self.redis)["queue"]],
            ["track:one"],
        )

    def test_one_song_queue_refills_before_finishing(self):
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "provider": "tater_tube",
                    "queue": [self.tracks[0]],
                    "queue_original": [self.tracks[0]],
                    "index": 0,
                    "current": self.tracks[0],
                    "targets": ["voice_core:native:kitchen"],
                    "queue_session_id": "one-song-session",
                    "repeat": "off",
                    "started_at": 100,
                }
            ),
        )
        with patch.object(self.core, "_stop_target", return_value=[]), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ) as play:
            player = self.core._advance_player(1, client=self.redis)
        self.assertEqual(player["status"], "playing")
        self.assertEqual(player["current"]["id"], "track:two")
        self.assertGreaterEqual(len(player["queue"]), 2)
        self.assertEqual(play.call_args.args[0]["id"], "track:two")

    def test_short_queue_schedules_background_refill_as_soon_as_it_is_playing(self):
        player = {
            "status": "playing",
            "provider": "tater_tube",
            "queue": [self.tracks[0]],
            "index": 0,
            "current": self.tracks[0],
            "targets": ["voice_core:native:kitchen"],
            "queue_session_id": "short-queue-session",
            "repeat": "off",
        }
        self.redis.set(self.core.PLAYER_KEY, json.dumps(player))
        fake_thread = Mock()
        fake_thread.is_alive.return_value = False
        with patch.object(self.core.threading, "Thread", return_value=fake_thread):
            started = self.core._schedule_continuation_refresh(client=self.redis)
        self.assertTrue(started)
        fake_thread.start.assert_called_once()
        queued = self.core._player(self.redis)
        self.assertTrue(queued["continuous_radio"])
        self.assertTrue(queued["continuation_pending"])
        self.core._continuation_thread = None

    def test_play_track_forwards_mixed_targets_with_audio_sync_transcode(self):
        playback = types.ModuleType("media_playback")
        playback.play_media_url_targets = Mock(
            return_value={"ok": True, "sent_count": 2}
        )
        targets = [
            "voice_core:native:kitchen",
            "integration:homeassistant:media_player.living_room",
        ]
        with patch.dict(sys.modules, {"media_playback": playback}):
            result = self.core._play_track(
                self.tracks[0],
                targets,
                volume_percent=55,
                start_position_seconds=37,
                mixed_sync_adjustment_ms=125,
                player_settings={
                    targets[0]: {"volume_percent": 45, "sync_offset_ms": -20},
                    targets[1]: {"volume_percent": 65, "sync_offset_ms": 80},
                },
                client=self.redis,
            )
        self.assertTrue(result["ok"])
        self.assertEqual(playback.play_media_url_targets.call_args.args[0], targets)
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["volume_percent"],
            55,
        )
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["start_position_seconds"],
            37,
        )
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["mixed_sync_adjustment_ms"],
            125,
        )
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["target_volume_percent"],
            {targets[0]: 45, targets[1]: 65},
        )
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["target_sync_offset_ms"],
            {targets[0]: -20, targets[1]: 80},
        )
        source_url = playback.play_media_url_targets.call_args.args[1]
        self.assertIn("transcode=1", source_url)
        self.assertIn("profile=audio_sync", source_url)
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["media_type"],
            "audio/wav",
        )
        self.assertTrue(result["audio_sync_transcode_used"])

    def test_play_track_uses_audio_sync_transcode_for_native_and_airplay(self):
        playback = types.ModuleType("media_playback")
        playback.play_media_url_targets = Mock(
            return_value={"ok": True, "sent_count": 2}
        )
        targets = [
            "voice_core:native:kitchen",
            "sonos:RINCON_LIVING",
        ]
        with patch.dict(sys.modules, {"media_playback": playback}):
            result = self.core._play_track(
                self.tracks[0],
                targets,
                volume_percent=55,
                player_settings={
                    targets[0]: {"volume_percent": 55, "sync_offset_ms": 0},
                    targets[1]: {
                        "volume_percent": 55,
                        "sync_offset_ms": 0,
                        "transport_mode": "auto",
                    },
                },
                client=self.redis,
            )

        kwargs = playback.play_media_url_targets.call_args.kwargs
        source_url = playback.play_media_url_targets.call_args.args[1]
        self.assertIn("transcode=1", source_url)
        self.assertIn("profile=audio_sync", source_url)
        self.assertEqual(kwargs["media_type"], "audio/wav")
        self.assertEqual(kwargs["filename"], "09 Three Little Birds.sync.wav")
        self.assertTrue(result["audio_sync_transcode_used"])
        self.assertEqual(result["audio_sync_transcode_profile"], "audio_sync")

    def test_play_track_uses_audio_sync_for_native_sonos_transport(self):
        playback = types.ModuleType("media_playback")
        playback.play_media_url_targets = Mock(
            return_value={"ok": True, "sent_count": 2}
        )
        targets = [
            "voice_core:native:kitchen",
            "sonos:RINCON_LIVING",
        ]
        with patch.dict(sys.modules, {"media_playback": playback}):
            result = self.core._play_track(
                self.tracks[0],
                targets,
                volume_percent=55,
                player_settings={
                    targets[0]: {"volume_percent": 55, "sync_offset_ms": 0},
                    targets[1]: {
                        "volume_percent": 55,
                        "sync_offset_ms": 0,
                        "transport_mode": "native",
                    },
                },
                client=self.redis,
            )

        source_url = playback.play_media_url_targets.call_args.args[1]
        self.assertIn("transcode=1", source_url)
        self.assertIn("profile=audio_sync", source_url)
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["media_type"],
            "audio/wav",
        )
        self.assertTrue(result["audio_sync_transcode_used"])

    def test_play_track_uses_audio_sync_for_native_stereo_pair(self):
        playback = types.ModuleType("media_playback")
        playback.play_media_url_targets = Mock(
            return_value={"ok": True, "sent_count": 1}
        )
        targets = ["voice_core:stereo:office"]
        with patch.dict(sys.modules, {"media_playback": playback}):
            result = self.core._play_track(
                self.tracks[0],
                targets,
                volume_percent=55,
                client=self.redis,
            )

        source_url = playback.play_media_url_targets.call_args.args[1]
        self.assertIn("transcode=1", source_url)
        self.assertIn("profile=audio_sync", source_url)
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["media_type"],
            "audio/wav",
        )
        self.assertTrue(result["audio_sync_transcode_used"])

    def test_hydra_exposes_play_search_control_status_and_browse(self):
        self.assertTrue(self.core.CORE_SETTINGS["hydra_tools_require_running"])
        self.assertTrue(self.core.CORE_WEBUI_TAB["requires_running"])
        ids = {row["id"] for row in self.core.get_hydra_kernel_tools()}
        self.assertEqual(
            ids,
            {
                "music_play",
                "music_search",
                "music_control",
                "music_now_playing",
                "music_browse",
            },
        )
        result = asyncio.run(
            self.core.run_hydra_kernel_tool(
                tool_id="music_search",
                args={"artist": "Miles Davis"},
                redis_client=self.redis,
            )
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["tracks"][0]["title"], "Blue in Green")

    def test_player_ui_has_destination_and_transport_controls(self):
        with patch.object(
            self.core,
            "_target_options",
            return_value=[
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                },
                {
                    "value": "airplay:804af2c57d78",
                    "label": "AirPlay Bridge: Kitchen (Sonos • Era 100)",
                    "description": "Wall-clock scheduled through Tater AirPlay Bridge",
                },
            ],
        ):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)
        player = next(
            row
            for row in payload["ui"]["item_forms"]
            if row.get("id") == "player:main"
        )
        fields = {row["key"]: row for row in player["fields"]}
        self.assertEqual(set(fields), {"volume_percent"})
        self.assertEqual(fields["volume_percent"]["type"], "range")
        self.assertEqual(fields["volume_percent"]["action"], "music_ui_set_volume")
        self.assertEqual(player["track_list"], [])
        self.assertEqual(player["track_list_label"], "Playlist")
        self.assertEqual(player["track_list_action"], "music_ui_queue_play")
        self.assertEqual(player["track_list_shuffle_action"], "music_ui_set_shuffle")
        self.assertEqual(player["playback"]["seek_action"], "music_ui_seek")
        self.assertEqual(
            player["playback"]["seek_relative_action"],
            "music_ui_seek_relative",
        )
        self.assertEqual(player["playback"]["seek_step_seconds"], 15)
        self.assertFalse(player["playback"]["seekable"])
        self.assertEqual(
            [row["action"] for row in player["actions"]],
            [
                "music_ui_previous",
                "music_ui_play",
                "music_ui_stop",
                "music_ui_next",
            ],
        )
        self.assertEqual(player["save_action"], "music_ui_save_player")
        self.assertEqual(player["card_variant"], "player_bar")
        self.assertFalse(player["show_save_button"])
        self.assertEqual(player["settings_label"], "🔊")
        self.assertEqual(player["popup_fields"][0]["key"], "targets")
        self.assertEqual(player["popup_fields"][0]["label"], "Play On")
        self.assertEqual(player["popup_fields"][0]["type"], "multiselect")
        self.assertEqual(len(player["popup_fields"]), 1)
        self.assertEqual(player["test_sync_action"], "music_ui_test_sync")
        self.assertEqual(len(player["player_rows"]), 2)
        self.assertEqual(player["player_rows"][0]["target"], "voice_core:native:kitchen")
        self.assertEqual(player["player_rows"][0]["sync_quality"], "precise")
        self.assertEqual(player["player_rows"][0]["volume_percent"], 75)
        self.assertEqual(player["player_rows"][0]["sync_offset_ms"], 0)
        self.assertEqual(player["player_rows"][1]["target"], "airplay:804af2c57d78")
        self.assertEqual(player["player_rows"][1]["kind"], "airplay_bridge")
        self.assertEqual(player["player_rows"][1]["sync_quality"], "bridge")
        self.assertEqual(payload["ui"]["appearance"], "music_library")
        self.assertTrue(payload["ui"]["live_updates"])
        self.assertEqual(payload["ui"]["poll_interval_ms"], 3000)
        self.assertEqual(payload["ui"]["persistent_item_groups"], ["player"])
        self.assertEqual(payload["ui"]["default_tab"], "playlist")
        tabs = {row["key"]: row for row in payload["ui"]["manager_tabs"]}
        self.assertNotIn("player", tabs)
        self.assertEqual(tabs["playlist"]["source"], "player_queue")
        self.assertEqual(tabs["recommendations"]["item_group"], "recommendations")
        self.assertEqual(tabs["airplay"]["item_group"], "airplay")
        self.assertEqual(
            [row["key"] for row in tabs["library"]["groups"]],
            ["search", "genres", "artists", "albums"],
        )
        self.assertTrue(all(row["selector"] is False for row in tabs["library"]["groups"]))
        self.assertNotIn("queue", tabs)
        search = next(
            row
            for row in payload["ui"]["item_forms"]
            if row.get("id") == "search:music"
        )
        self.assertEqual(search["group"], "search")
        self.assertEqual(search["fields"][0]["key"], "query")
        self.assertEqual(search["run_action"], "music_ui_play")
        library_tiles = [
            row
            for row in payload["ui"]["item_forms"]
            if row.get("group") in {"genres", "artists", "albums"}
        ]
        self.assertTrue(library_tiles)
        self.assertTrue(all(row.get("card_variant") == "library_tile" for row in library_tiles))
        settings = next(
            row
            for row in payload["ui"]["item_forms"]
            if row.get("id") == "settings:music"
        )
        settings_fields = {row["key"]: row for row in settings["fields"]}
        self.assertEqual(settings["title"], "Playback Defaults")
        self.assertEqual(settings_fields["default_targets"]["type"], "player_multiselect")
        self.assertEqual(settings_fields["default_volume_percent"]["type"], "range")
        library_settings = next(
            row for row in payload["ui"]["item_forms"]
            if row.get("id") == "settings:library_sync"
        )
        library_fields = {row["key"]: row for row in library_settings["fields"]}
        self.assertEqual(library_fields["catalog_sync_interval_seconds"]["min"], 60)
        self.assertEqual(library_fields["mixed_sync_default_adjustment_ms"]["value"], 0)
        personalization = next(
            row for row in payload["ui"]["item_forms"]
            if row.get("id") == "settings:personalization"
        )
        personalization_fields = {row["key"]: row for row in personalization["fields"]}
        self.assertTrue(personalization_fields["recommendations_enabled"]["value"])
        self.assertTrue(personalization_fields["prompt_context_enabled"]["value"])
        self.assertEqual(personalization_fields["prompt_person_id"]["type"], "select")
        self.assertEqual(
            personalization_fields["prompt_person_id"]["options"][0]["label"],
            "Choose a person",
        )
        self.assertEqual(personalization_fields["prompt_profile_interval_hours"]["value"], 12)
        recommendation_intro = next(
            row
            for row in payload["ui"]["item_forms"]
            if row.get("id") == "recommendations:overview"
        )
        self.assertEqual(recommendation_intro["group"], "recommendations")
        self.assertFalse(recommendation_intro["refresh_available"])
        providers = {
            row["id"]: row
            for row in payload["ui"]["item_forms"]
            if row.get("group") == "providers"
        }
        self.assertEqual(
            set(providers),
            {"provider:tater_tube"},
        )
        self.assertEqual(tabs["providers"]["label"], "Tater Tube")
        self.assertEqual(providers["provider:tater_tube"]["fields"][2]["type"], "password")

    def test_recommendations_use_configured_assistant_name(self):
        self.redis.set("tater:first_name", "Totty")

        with patch.object(self.core, "_target_options", return_value=[]):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)

        tabs = {row["key"]: row for row in payload["ui"]["manager_tabs"]}
        overview = next(
            row
            for row in payload["ui"]["item_forms"]
            if row.get("id") == "recommendations:overview"
        )
        settings = next(
            row
            for row in payload["ui"]["item_forms"]
            if row.get("id") == "settings:personalization"
        )
        settings_fields = {row["key"]: row for row in settings["fields"]}
        tasks = {
            row["id"]: row
            for row in self.core.get_core_system_tasks(redis_client=self.redis)["tasks"]
        }

        self.assertEqual(tabs["recommendations"]["label"], "Totty's Recommendations")
        self.assertEqual(overview["title"], "Totty's Recommendations")
        self.assertEqual(overview["assistant_name"], "Totty")
        self.assertIn("Totty will learn", overview["detail"])
        self.assertEqual(
            settings_fields["recommendations_enabled"]["label"],
            "Totty's Recommendations",
        )
        self.assertEqual(
            tasks["recommendation_refresh"]["label"],
            "Totty's Recommendations",
        )

    def test_recommendations_use_natural_possessive_for_s_ending_name(self):
        self.redis.set("tater:first_name", "Jules")
        self.assertEqual(self.core._recommendations_label(self.redis), "Jules' Recommendations")

    def test_player_ui_upgrades_saved_stereo_members_without_readding_them(self):
        member_routes = {
            "native:sat1": "voice_core:stereo:bedroom12",
            "voice_core:native:sat1": "voice_core:stereo:bedroom12",
            "native:voicepe": "voice_core:stereo:bedroom12",
            "voice_core:native:voicepe": "voice_core:stereo:bedroom12",
        }
        pair_option = {
            "value": "voice_core:stereo:bedroom12",
            "label": "Tater Stereo: Bedroom",
        }
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "stopped",
                    "targets": [
                        "voice_core:native:sat1",
                        "voice_core:native:voicepe",
                    ],
                }
            ),
        )
        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "default_targets": json.dumps(
                    [
                        "voice_core:native:sat1",
                        "voice_core:native:voicepe",
                    ]
                )
            },
        )
        with patch.object(
            self.core,
            "_stereo_member_target_map",
            return_value=member_routes,
        ), patch.object(
            self.core,
            "_target_options",
            return_value=[pair_option],
        ):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)

        player = next(
            row for row in payload["ui"]["item_forms"] if row.get("id") == "player:main"
        )
        settings = next(
            row for row in payload["ui"]["item_forms"] if row.get("id") == "settings:music"
        )
        default_targets = next(
            row for row in settings["fields"] if row.get("key") == "default_targets"
        )
        self.assertEqual(
            player["popup_fields"][0]["value"],
            ["voice_core:stereo:bedroom12"],
        )
        self.assertEqual(default_targets["value"], ["voice_core:stereo:bedroom12"])
        self.assertEqual(default_targets["options"], [pair_option])

    def test_saving_defaults_routes_stereo_members_to_the_pair(self):
        member_routes = {
            "voice_core:native:sat1": "voice_core:stereo:bedroom12",
            "voice_core:native:voicepe": "voice_core:stereo:bedroom12",
        }
        with patch.object(
            self.core,
            "_stereo_member_target_map",
            return_value=member_routes,
        ):
            result = self.core.handle_htmlui_tab_action(
                action="music_save_settings",
                payload={
                    "values": {
                        "default_targets": [
                            "voice_core:native:sat1",
                            "voice_core:native:voicepe",
                        ]
                    }
                },
                redis_client=self.redis,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            json.loads(self.redis.hgetall(self.core.SETTINGS_KEY)["default_targets"]),
            ["voice_core:stereo:bedroom12"],
        )

    def test_player_track_list_marks_current_track_and_keeps_album_together(self):
        album_tracks = [
            {**self.tracks[0], "id": "album:1", "title": "First Song", "album": "Exodus"},
            {**self.tracks[0], "id": "album:2", "title": "Second Song", "album": "Exodus"},
        ]
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "queue": album_tracks,
                    "queue_original": album_tracks,
                    "index": 1,
                    "current": album_tracks[1],
                    "targets": ["voice_core:native:kitchen"],
                    "provider": "tater_tube",
                }
            ),
        )
        with patch.object(self.core, "_target_options", return_value=[]):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)
        player = next(row for row in payload["ui"]["item_forms"] if row.get("id") == "player:main")
        self.assertEqual([row["title"] for row in player["track_list"]], ["First Song", "Second Song"])
        self.assertEqual([row["active"] for row in player["track_list"]], [False, True])
        self.assertEqual([row["image_src"] for row in player["track_list"]], ["", ""])
        self.assertTrue(player["hero_image_src"].startswith("data:image/svg+xml"))

    def test_next_track_stops_active_session_before_starting_the_next_track(self):
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "queue": self.tracks,
                    "queue_original": self.tracks,
                    "index": 0,
                    "current": self.tracks[0],
                    "targets": ["voice_core:native:kitchen"],
                    "provider": "tater_tube",
                }
            ),
        )
        with patch.object(self.core, "_stop_target", return_value=[]) as stop, patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ) as play:
            player = self.core._advance_player(1, client=self.redis)
        stop.assert_called_once_with(
            ["voice_core:native:kitchen"],
            expected_voice_core_sessions=[],
        )
        self.assertEqual(play.call_args.args[0]["id"], "track:two")
        self.assertEqual(player["index"], 1)
        self.assertEqual(player["current"]["id"], "track:two")

    def test_starting_another_album_replaces_the_existing_track_list(self):
        first_album = [{**self.tracks[0], "id": "first:1"}, {**self.tracks[0], "id": "first:2"}]
        second_album = [{**self.tracks[1], "id": "second:1"}]
        with patch.object(self.core, "_stop_target", return_value=[]) as stop, patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ):
            self.core._create_and_start_queue(
                first_album,
                targets=["voice_core:native:kitchen"],
                shuffle=False,
                volume_percent=70,
                client=self.redis,
            )
            player = self.core._create_and_start_queue(
                second_album,
                targets=["voice_core:native:kitchen"],
                shuffle=False,
                volume_percent=70,
                client=self.redis,
            )
        self.assertEqual([row["id"] for row in player["queue"]], ["second:1"])
        self.assertEqual([row["id"] for row in player["queue_original"]], ["second:1"])
        stop.assert_called_once_with(
            ["voice_core:native:kitchen"],
            expected_voice_core_sessions=[],
        )

    def test_stop_target_uses_owned_voice_session_cleanup(self):
        sessions = [
            {
                "session_id": "music-session-1",
                "selectors": ["native:kitchen"],
            }
        ]
        announcement_targets = types.ModuleType("announcement_targets")
        announcement_targets.split_announcement_targets = lambda _targets: {
            "voice_core_selectors": ["native:kitchen"],
        }
        media_playback = types.ModuleType("media_playback")
        media_playback._voice_core_stop_media_sync = Mock(return_value=[])

        with patch.dict(
            sys.modules,
            {
                "announcement_targets": announcement_targets,
                "media_playback": media_playback,
            },
        ):
            warnings = self.core._stop_target(
                ["voice_core:native:kitchen"],
                expected_voice_core_sessions=sessions,
            )

        self.assertEqual(warnings, [])
        media_playback._voice_core_stop_media_sync.assert_called_once_with(
            [],
            expected_sessions=sessions,
            reason="music_core_stop",
        )

    def test_live_volume_and_shuffle_actions_update_the_player_without_restarting(self):
        queue = [
            {**self.tracks[0], "id": "track:one"},
            {**self.tracks[1], "id": "track:two"},
            {**self.tracks[1], "id": "track:three", "title": "So What"},
        ]
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "queue": queue,
                    "queue_original": queue,
                    "index": 0,
                    "current": queue[0],
                    "targets": ["voice_core:native:kitchen"],
                    "provider": "tater_tube",
                    "volume_percent": 70,
                }
            ),
        )
        with patch.object(
            self.core,
            "_set_target_volume",
            return_value={"sent_count": 1, "warnings": []},
        ) as set_target_volume:
            volume_result = self.core.handle_htmlui_tab_action(
                action="music_ui_set_volume",
                payload={"values": {"volume_percent": 42}},
                redis_client=self.redis,
            )
        shuffle_result = self.core.handle_htmlui_tab_action(
            action="music_ui_set_shuffle",
            payload={"values": {"shuffle": True}},
            redis_client=self.redis,
        )
        player = self.core._player(self.redis)
        self.assertTrue(volume_result["ok"])
        self.assertTrue(shuffle_result["ok"])
        self.assertEqual(player["volume_percent"], 42)
        set_target_volume.assert_called_once()
        self.assertEqual(set_target_volume.call_args.args[1], 42)
        self.assertTrue(player["shuffle"])
        self.assertEqual(player["queue"][0]["id"], "track:one")
        self.assertEqual(
            {row["id"] for row in player["queue"][1:]},
            {"track:two", "track:three"},
        )

    def test_seek_restarts_current_track_at_requested_position_without_history(self):
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "queue": self.tracks,
                    "index": 0,
                    "current": self.tracks[0],
                    "targets": ["voice_core:native:kitchen"],
                    "provider": "tater_tube",
                    "volume_percent": 70,
                    "duration_seconds": 180,
                    "started_at": 100,
                    "position_offset_seconds": 0,
                }
            ),
        )
        with patch.object(self.core, "_require_native_seek_support") as compatible, patch.object(
            self.core,
            "_stop_target",
            return_value=[],
        ), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ) as play_track, patch.object(self.core, "_record_listening_history") as history:
            result = self.core.handle_htmlui_tab_action(
                action="music_ui_seek",
                payload={"values": {"position_seconds": 75}},
                redis_client=self.redis,
            )

        self.assertTrue(result["ok"])
        compatible.assert_called_once_with(["voice_core:native:kitchen"])
        self.assertEqual(play_track.call_args.kwargs["start_position_seconds"], 75)
        history.assert_not_called()
        player = self.core._player(self.redis)
        self.assertEqual(player["position_offset_seconds"], 75)
        self.assertEqual(player["status"], "playing")

    def test_player_position_combines_seek_offset_with_live_elapsed_time(self):
        position = self.core._player_position_seconds(
            {
                "status": "playing",
                "position_offset_seconds": 40,
                "started_at": 100,
                "duration_seconds": 180,
            },
            now=112.5,
        )
        self.assertEqual(position, 52.5)

    def test_pause_stops_playback_and_persists_the_elapsed_position(self):
        target = "voice_core:native:kitchen"
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "provider": "tater_tube",
                    "queue": self.tracks,
                    "index": 0,
                    "current": self.tracks[0],
                    "targets": [target],
                    "started_at": 100.0,
                    "position_offset_seconds": 35.0,
                    "duration_seconds": 180.0,
                }
            ),
        )

        with patch.object(self.core.time, "time", return_value=112.5), patch.object(
            self.core,
            "_stop_target",
            return_value=[],
        ) as stop:
            result = self.core.handle_htmlui_tab_action(
                action="music_ui_pause",
                payload={"values": {}},
                redis_client=self.redis,
            )

        self.assertTrue(result["ok"])
        stop.assert_called_once_with(
            [target],
            expected_voice_core_sessions=[],
        )
        paused = self.core._player(self.redis)
        self.assertEqual(paused["status"], "paused")
        self.assertEqual(paused["started_at"], 0.0)
        self.assertEqual(paused["position_offset_seconds"], 47.5)

    def test_routing_paused_player_preserves_one_global_queue_and_position(self):
        old_target = "voice_core:native:kitchen"
        new_targets = [
            "voice_core:native:office",
            "voice_core:native:bedroom",
        ]
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "paused",
                    "provider": "tater_tube",
                    "queue": self.tracks,
                    "queue_original": self.tracks,
                    "index": 1,
                    "current": self.tracks[1],
                    "targets": [old_target],
                    "queue_session_id": "global-session",
                    "started_at": 0.0,
                    "position_offset_seconds": 67.0,
                    "duration_seconds": 180.0,
                }
            ),
        )

        with patch.object(self.core, "_stop_target") as stop, patch.object(
            self.core,
            "_play_track",
        ) as play:
            routed = self.core._route_player_targets(new_targets, client=self.redis)

        stop.assert_not_called()
        play.assert_not_called()
        self.assertEqual(routed["status"], "paused")
        self.assertEqual(routed["targets"], new_targets)
        self.assertEqual(routed["queue_session_id"], "global-session")
        self.assertEqual(routed["index"], 1)
        self.assertEqual(routed["current"]["id"], "track:two")
        self.assertEqual(routed["position_offset_seconds"], 67.0)
        self.assertEqual(
            [track["id"] for track in routed["queue"]],
            ["track:one", "track:two"],
        )

    def test_routing_playing_player_moves_same_global_session_at_current_position(self):
        old_target = "voice_core:native:kitchen"
        new_targets = ["voice_core:native:office"]
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "provider": "tater_tube",
                    "queue": self.tracks,
                    "queue_original": self.tracks,
                    "index": 1,
                    "current": self.tracks[1],
                    "targets": [old_target],
                    "queue_session_id": "global-session",
                    "started_at": 100.0,
                    "position_offset_seconds": 35.0,
                    "duration_seconds": 180.0,
                }
            ),
        )

        with patch.object(self.core.time, "time", return_value=112.5), patch.object(
            self.core,
            "_stop_target",
            return_value=[],
        ) as stop, patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ) as play, patch.object(self.core, "_record_listening_history") as history:
            routed = self.core._route_player_targets(new_targets, client=self.redis)

        stop.assert_called_once_with(
            [old_target],
            expected_voice_core_sessions=[],
        )
        self.assertEqual(play.call_args.args[1], new_targets)
        self.assertEqual(play.call_args.kwargs["start_position_seconds"], 47.5)
        history.assert_not_called()
        self.assertEqual(routed["status"], "playing")
        self.assertEqual(routed["targets"], new_targets)
        self.assertEqual(routed["queue_session_id"], "global-session")
        self.assertEqual(routed["index"], 1)
        self.assertEqual(routed["current"]["id"], "track:two")

    def test_client_pause_updates_selected_outputs_after_pausing_global_session(self):
        old_target = "voice_core:native:kitchen"
        new_target = "voice_core:native:office"
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "provider": "tater_tube",
                    "queue": self.tracks,
                    "queue_original": self.tracks,
                    "index": 0,
                    "current": self.tracks[0],
                    "targets": [old_target],
                    "queue_session_id": "global-session",
                    "started_at": 100.0,
                    "position_offset_seconds": 20.0,
                    "duration_seconds": 180.0,
                }
            ),
        )

        with patch.object(self.core.time, "time", return_value=105.0), patch.object(
            self.core,
            "_resolve_targets",
            return_value=[new_target],
        ), patch.object(self.core, "_validate_catalog_provider_targets"), patch.object(
            self.core,
            "_stop_target",
            return_value=[],
        ) as stop:
            result = self.core.run_client_music_action(
                "pause",
                {"targets": [new_target]},
                client=self.redis,
            )

        self.assertTrue(result["ok"])
        stop.assert_called_once_with(
            [old_target],
            expected_voice_core_sessions=[],
        )
        paused = self.core._player(self.redis)
        self.assertEqual(paused["status"], "paused")
        self.assertEqual(paused["targets"], [new_target])
        self.assertEqual(paused["queue_session_id"], "global-session")
        self.assertEqual(paused["position_offset_seconds"], 25.0)

    def test_client_resume_uses_selected_outputs_without_replacing_global_queue(self):
        old_target = "voice_core:native:kitchen"
        new_target = "voice_core:native:office"
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "paused",
                    "provider": "tater_tube",
                    "queue": self.tracks,
                    "queue_original": self.tracks,
                    "index": 1,
                    "current": self.tracks[1],
                    "targets": [old_target],
                    "queue_session_id": "global-session",
                    "started_at": 0.0,
                    "position_offset_seconds": 67.0,
                    "duration_seconds": 180.0,
                }
            ),
        )

        with patch.object(
            self.core,
            "_resolve_targets",
            return_value=[new_target],
        ), patch.object(self.core, "_validate_catalog_provider_targets"), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ) as play, patch.object(self.core, "_record_listening_history") as history:
            result = self.core.run_client_music_action(
                "resume",
                {"targets": [new_target]},
                client=self.redis,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(play.call_args.args[1], [new_target])
        self.assertEqual(play.call_args.kwargs["start_position_seconds"], 67.0)
        history.assert_not_called()
        resumed = self.core._player(self.redis)
        self.assertEqual(resumed["status"], "playing")
        self.assertEqual(resumed["targets"], [new_target])
        self.assertEqual(resumed["queue_session_id"], "global-session")
        self.assertEqual(resumed["index"], 1)

    def test_paused_player_reloads_with_resume_action_and_resumes_at_saved_position(self):
        target = "voice_core:native:kitchen"
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "paused",
                    "provider": "tater_tube",
                    "queue": self.tracks,
                    "index": 1,
                    "current": self.tracks[1],
                    "targets": [target],
                    "volume_percent": 58,
                    "started_at": 0.0,
                    "position_offset_seconds": 67.0,
                    "duration_seconds": 180.0,
                }
            ),
        )
        reloaded = self.core._player(self.redis)
        item = self.core._player_item(
            reloaded,
            [{"value": target, "label": "Tater Sat: Kitchen"}],
            "tater_tube",
            {},
        )
        toggle = item["actions"][1]
        self.assertEqual(toggle["action"], "music_ui_play")
        self.assertEqual(toggle["aria_label"], "Resume music")
        self.assertEqual(item["playback"]["position_seconds"], 67.0)

        with patch.object(
            self.core,
            "_resolve_targets",
            return_value=[target],
        ), patch.object(self.core, "_validate_catalog_provider_targets"), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ) as play, patch.object(self.core, "_record_listening_history") as history:
            result = self.core.handle_htmlui_tab_action(
                action="music_ui_play",
                payload={"values": {"volume_percent": 58}},
                redis_client=self.redis,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(play.call_args.kwargs["start_position_seconds"], 67.0)
        history.assert_not_called()
        resumed = self.core._player(self.redis)
        self.assertEqual(resumed["status"], "playing")
        self.assertEqual(resumed["position_offset_seconds"], 67.0)

    def test_playing_player_exposes_pause_as_the_primary_transport_action(self):
        item = self.core._player_item(
            {
                "status": "playing",
                "current": self.tracks[0],
                "queue": self.tracks,
                "index": 0,
                "started_at": 100.0,
            },
            [],
            "tater_tube",
            {},
        )

        toggle = item["actions"][1]
        self.assertEqual(toggle["action"], "music_ui_pause")
        self.assertEqual(toggle["aria_label"], "Pause music")
        self.assertEqual(toggle["label"], "⏸")

    def test_mixed_sync_adjustment_is_saved_per_exact_player_group(self):
        first_group = ["voice_core:native:kitchen", "sonos:RINCON_LIVING"]
        second_group = ["voice_core:native:office", "sonos:RINCON_LIVING"]

        saved = self.core._save_mixed_sync_adjustment(self.redis, first_group, 225)

        self.assertEqual(saved, 225)
        cfg = self.core._settings(self.redis)
        self.assertEqual(self.core._mixed_sync_adjustment(first_group, cfg), 225)
        self.assertEqual(self.core._mixed_sync_adjustment(second_group, cfg), 0)

    def test_player_calibrations_are_saved_per_destination(self):
        saved = self.core._save_player_calibrations(
            self.redis,
            {
                "voice_core:native:kitchen": {
                    "volume_percent": 46,
                    "sync_offset_ms": -120,
                },
                "sonos:RINCON_LIVING": {
                    "volume_percent": 61,
                    "sync_offset_ms": 80,
                },
            },
        )

        self.assertEqual(saved["voice_core:native:kitchen"]["volume_percent"], 46)
        self.assertEqual(saved["voice_core:native:kitchen"]["sync_offset_ms"], -120)
        cfg = self.core._settings(self.redis)
        self.assertEqual(
            self.core._target_calibration("sonos:RINCON_LIVING", cfg),
            {"volume_percent": 61, "sync_offset_ms": 80, "transport_mode": "auto"},
        )

    def test_sonos_player_row_exposes_automatic_transport_selection(self):
        target = "sonos:RINCON_KITCHEN"
        item = self.core._player_item(
            {"status": "stopped", "targets": [target], "volume_percent": 70},
            [
                {
                    "value": target,
                    "label": "Sonos: Kitchen",
                    "airplay_bridge_target": "airplay:804af2c57d78",
                    "transport_options": [
                        {"value": "auto", "label": "Automatic"},
                        {"value": "native", "label": "Native Sonos"},
                        {"value": "airplay", "label": "AirPlay Bridge"},
                    ],
                }
            ],
            "tater_tube",
            {},
        )

        row = item["player_rows"][0]
        self.assertEqual(row["target"], target)
        self.assertEqual(row["sync_quality"], "automatic")
        self.assertEqual(row["transport_mode"], "auto")
        self.assertEqual(row["airplay_bridge_target"], "airplay:804af2c57d78")

    def test_saved_sonos_airplay_target_migrates_to_the_unified_sonos_row(self):
        options = [
            {
                "value": "sonos:RINCON_KITCHEN",
                "label": "Sonos: Kitchen",
                "target_aliases": ["airplay:804af2c57d78"],
            }
        ]

        self.assertEqual(
            self.core._canonical_option_targets(["airplay:804af2c57d78"], options),
            ["sonos:RINCON_KITCHEN"],
        )
        with patch.object(self.core, "_target_options", return_value=options):
            self.assertEqual(
                self.core._resolve_targets("airplay:804af2c57d78", client=self.redis),
                ["sonos:RINCON_KITCHEN"],
            )

    def test_per_player_offsets_extend_existing_mixed_group_calibration(self):
        targets = ["voice_core:native:kitchen", "sonos:RINCON_LIVING"]
        settings = {
            targets[0]: {"volume_percent": 50, "sync_offset_ms": 100},
            targets[1]: {"volume_percent": 50, "sync_offset_ms": -50},
        }

        adjustment = self.core._mixed_sync_from_player_settings(targets, settings, 225)

        self.assertEqual(adjustment, 375)

    def test_sync_test_wav_contains_six_seconds_of_mono_click_audio(self):
        payload = self.core._sync_test_wav()

        with wave.open(io.BytesIO(payload), "rb") as wav_file:
            self.assertEqual(wav_file.getnchannels(), 1)
            self.assertEqual(wav_file.getframerate(), 16000)
            self.assertEqual(wav_file.getnframes(), 96000)

    def test_sync_test_uses_unsaved_row_settings_and_stops_current_music(self):
        targets = ["voice_core:native:kitchen", "sonos:RINCON_LIVING"]
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "targets": targets,
                    "provider": "tater_tube",
                    "volume_percent": 70,
                }
            ),
        )
        playback = types.ModuleType("media_playback")
        playback.play_media_url_targets = Mock(return_value={"ok": True, "sent_count": 2})
        with patch.dict(sys.modules, {"media_playback": playback}), patch.object(
            self.core,
            "_resolve_targets",
            return_value=targets,
        ), patch.object(self.core, "_validate_catalog_provider_targets"), patch.object(
            self.core,
            "_stop_target",
            return_value=[],
        ) as stop:
            result = self.core.handle_htmlui_tab_action(
                action="music_ui_test_sync",
                payload={
                    "values": {
                        "targets": targets,
                        "player_settings": {
                            targets[0]: {"volume_percent": 44, "sync_offset_ms": -80},
                            targets[1]: {"volume_percent": 63, "sync_offset_ms": 120},
                        },
                    }
                },
                redis_client=self.redis,
            )

        self.assertTrue(result["ok"])
        stop.assert_called_once_with(
            targets,
            expected_voice_core_sessions=[],
        )
        kwargs = playback.play_media_url_targets.call_args.kwargs
        self.assertGreater(len(kwargs["audio_bytes"]), 1000)
        self.assertEqual(kwargs["target_volume_percent"], {targets[0]: 44, targets[1]: 63})
        self.assertEqual(kwargs["target_sync_offset_ms"], {targets[0]: -80, targets[1]: 120})
        self.assertEqual(self.core._player(self.redis)["status"], "stopped")

    def test_live_volume_is_sent_to_the_active_native_media_session(self):
        calls = []
        announcement_targets = types.ModuleType("announcement_targets")
        announcement_targets.split_announcement_targets = lambda _targets: {
            "voice_core_selectors": ["native:kitchen"],
            "integration_devices": [],
            "homeassistant_media_players": [],
            "sonos_speakers": [],
        }
        native_satellite = types.ModuleType("tater_voice.native_satellite")

        async def has_capability(selector, capability):
            calls.append(("capability", selector, capability))
            return True

        async def send_request(selector, command, payload, timeout_s=0):
            calls.append(("request", selector, command, payload, timeout_s))
            return {"ok": True}

        native_satellite.client_has_capability = has_capability
        native_satellite.send_request = send_request
        native_satellite.run_on_runtime_loop = lambda awaitable, timeout=0: asyncio.run(awaitable)
        stereo_pairs = types.ModuleType("tater_voice.stereo_pairs")
        stereo_pairs.is_stereo_selector = lambda _selector: False
        stereo_pairs.get_pair = lambda _selector: {}
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.native_satellite = native_satellite
        tater_voice.stereo_pairs = stereo_pairs
        player = {
            "status": "playing",
            "targets": ["voice_core:native:kitchen"],
            "playback_result": {
                "voice_core_sessions": [
                    {
                        "session_id": "session-1",
                        "target": "native:kitchen",
                        "selectors": ["native:kitchen"],
                    }
                ]
            },
        }

        with patch.dict(
            sys.modules,
            {
                "announcement_targets": announcement_targets,
                "tater_voice": tater_voice,
                "tater_voice.native_satellite": native_satellite,
                "tater_voice.stereo_pairs": stereo_pairs,
            },
        ):
            result = self.core._set_target_volume(player, 42)

        self.assertEqual(result, {"sent_count": 1, "warnings": []})
        self.assertIn(
            ("capability", "native:kitchen", "media_session_volume"),
            calls,
        )
        request = next(row for row in calls if row[0] == "request")
        self.assertEqual(request[2], "media.session.volume")
        self.assertEqual(
            request[3],
            {"session_id": "session-1", "volume_percent": 42},
        )

    def test_player_search_keeps_destinations_selected_from_speaker_popup(self):
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "stopped",
                    "targets": ["voice_core:native:kitchen"],
                    "provider": "tater_tube",
                    "shuffle": False,
                    "volume_percent": 42,
                }
            ),
        )
        with patch.object(
            self.core,
            "_play_request",
            return_value={"summary_for_user": "Playing reggae."},
        ) as play_request:
            result = self.core.handle_htmlui_tab_action(
                action="music_ui_play",
                payload={"values": {"query": "reggae"}},
                redis_client=self.redis,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            play_request.call_args.args[0]["targets"],
            ["voice_core:native:kitchen"],
        )
        self.assertFalse(play_request.call_args.args[0]["shuffle"])
        self.assertEqual(play_request.call_args.args[0]["volume_percent"], 42)


    def test_empty_player_search_replays_current_queue_at_zero_volume(self):
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "stopped",
                    "queue": self.tracks,
                    "index": 1,
                    "current": self.tracks[1],
                    "target": "voice_core:native:kitchen",
                    "volume_percent": 25,
                }
            ),
        )
        with patch.object(
            self.core,
            "_target_options",
            return_value=[
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                }
            ],
        ), patch.object(
            self.core,
            "_play_track",
            return_value={"ok": True, "sent_count": 1},
        ) as play:
            result = self.core.handle_htmlui_tab_action(
                action="music_ui_play",
                payload={
                    "values": {
                        "query": "",
                        "targets": ["voice_core:native:kitchen"],
                        "volume_percent": 0,
                    }
                },
                redis_client=self.redis,
            )
        self.assertTrue(result["ok"])
        self.assertEqual(play.call_args.kwargs["volume_percent"], 0)
        self.assertEqual(self.core._player(self.redis)["current"]["id"], "track:two")

    def test_airplay_receiver_card_lists_native_airplay_and_bridged_sonos_destinations(self):
        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "airplay_receiver_enabled": "true",
                "airplay_receiver_name": "House Tater",
                "airplay_receiver_pin": "3939",
                "airplay_receiver_targets": json.dumps(
                    ["voice_core:native:kitchen", "voice_core:stereo:office"]
                ),
            },
        )
        runtime = types.SimpleNamespace(
            get_external_audio_status=lambda: {
                "enabled": True,
                "status": "ready",
                "receiver_running": True,
                "input_active": False,
                "targets": ["voice_core:native:kitchen", "voice_core:stereo:office"],
            }
        )
        with patch.object(
            self.core,
            "_external_audio_module",
            return_value=runtime,
        ), patch.object(
            self.core,
            "_target_options",
            return_value=[
                {"value": "voice_core:native:kitchen", "label": "Tater Sat: Kitchen"},
                {"value": "voice_core:stereo:office", "label": "Tater Stereo: Office"},
                {
                    "value": "sonos:den",
                    "label": "Sonos: Den",
                    "airplay_bridge_target": "airplay:sonos-den",
                },
                {"value": "sonos:patio", "label": "Sonos: Patio"},
                {"value": "airplay:living", "label": "AirPlay Bridge: Living"},
            ],
        ):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)

        card = next(
            row
            for row in payload["ui"]["item_forms"]
            if row.get("id") == "settings:airplay_receiver"
        )
        fields = {row["key"]: row for row in card["fields"]}
        self.assertEqual(card["group"], "airplay")
        self.assertEqual(card["card_variant"], "airplay_receiver")
        self.assertEqual(card["hero_badges"][0]["label"], "READY")
        self.assertEqual(fields["airplay_receiver_name"]["value"], "House Tater")
        self.assertEqual(fields["airplay_receiver_pin"]["type"], "password")
        self.assertEqual(fields["airplay_receiver_targets"]["type"], "player_multiselect")
        self.assertEqual(
            [row["value"] for row in fields["airplay_receiver_targets"]["options"]],
            [
                "voice_core:native:kitchen",
                "voice_core:stereo:office",
                "sonos:den",
                "airplay:living",
            ],
        )

    def test_local_airplay_receiver_is_not_offered_as_an_outbound_player(self):
        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "airplay_receiver_name": "Tater Music",
                "default_targets": json.dumps(["airplay:localreceiver"]),
            },
        )
        options = [
            {
                "value": "airplay:localreceiver",
                "label": "AirPlay: Tater Music (Mac • 192.168.1.10)",
            },
            {
                "value": "airplay:kitchen",
                "label": "AirPlay: Kitchen HomePod (Apple • 192.168.1.20)",
            },
            {
                "value": "voice_core:native:office",
                "label": "Tater Sat: Office",
            },
        ]
        with patch.object(self.core, "_target_options", return_value=options):
            payload = self.core.get_htmlui_tab_data(redis_client=self.redis)

        player = next(
            row for row in payload["ui"]["item_forms"] if row.get("id") == "player:main"
        )
        offered = {row["target"] for row in player["player_rows"]}
        self.assertNotIn("airplay:localreceiver", offered)
        self.assertIn("airplay:kitchen", offered)
        settings = next(
            row for row in payload["ui"]["item_forms"] if row.get("id") == "settings:music"
        )
        default_targets = next(
            row for row in settings["fields"] if row.get("key") == "default_targets"
        )
        self.assertEqual(default_targets["value"], [])
        receiver = next(
            row
            for row in payload["ui"]["item_forms"]
            if row.get("id") == "settings:airplay_receiver"
        )
        receiver_targets = next(
            row
            for row in receiver["fields"]
            if row.get("key") == "airplay_receiver_targets"
        )
        receiver_offered = {row["value"] for row in receiver_targets["options"]}
        self.assertNotIn("airplay:localreceiver", receiver_offered)
        self.assertIn("airplay:kitchen", receiver_offered)

    def test_saving_airplay_receiver_configures_tater_runtime(self):
        configure = Mock(return_value={"status": "ready"})
        runtime = types.SimpleNamespace(
            configure_external_audio_runtime=configure,
            get_external_audio_status=lambda: {"status": "ready"},
        )
        with patch.object(
            self.core,
            "_external_audio_module",
            return_value=runtime,
        ), patch.object(
            self.core,
            "_sonos_airplay_target",
            return_value="airplay:sonos-den",
        ):
            result = self.core.handle_htmlui_tab_action(
                action="music_save_settings",
                payload={
                    "values": {
                        "airplay_receiver_enabled": True,
                        "airplay_receiver_name": "House Tater",
                        "airplay_receiver_pin": "3939",
                        "airplay_receiver_targets": [
                            "voice_core:native:kitchen",
                            "sonos:den",
                            "airplay:living",
                        ],
                    }
                },
                redis_client=self.redis,
            )

        self.assertTrue(result["ok"])
        saved = self.redis.hgetall(self.core.SETTINGS_KEY)
        self.assertEqual(saved["airplay_receiver_pin"], "3939")
        self.assertEqual(
            json.loads(saved["airplay_receiver_targets"]),
            ["voice_core:native:kitchen", "sonos:den", "airplay:living"],
        )
        config = configure.call_args.args[0]
        self.assertTrue(config["enabled"])
        self.assertEqual(config["receiver_name"], "House Tater")
        self.assertEqual(
            config["targets"],
            ["voice_core:native:kitchen", "sonos:den", "airplay:living"],
        )
        self.assertEqual(config["volume_percent"], 100)
        self.assertEqual(
            config["target_volume_percent"],
            {
                "voice_core:native:kitchen": 100,
                "sonos:den": 100,
                "airplay:living": 100,
            },
        )
        self.assertEqual(config["target_transport_mode"], {"sonos:den": "airplay"})

    def test_airplay_receiver_rejects_invalid_pin_and_unsupported_target(self):
        with self.assertRaisesRegex(ValueError, "exactly four digits"):
            self.core.handle_htmlui_tab_action(
                action="music_save_settings",
                payload={"values": {"airplay_receiver_pin": "12345"}},
                redis_client=self.redis,
            )
        with self.assertRaisesRegex(ValueError, "Tater Native"):
            self.core.handle_htmlui_tab_action(
                action="music_save_settings",
                payload={"values": {"airplay_receiver_targets": ["ha:media_player.den"]}},
                redis_client=self.redis,
            )

    def test_airplay_receiver_rejects_sonos_without_a_matching_airplay_endpoint(self):
        with patch.object(self.core, "_sonos_airplay_target", return_value=""):
            with self.assertRaisesRegex(ValueError, "matching AirPlay endpoint"):
                self.core.handle_htmlui_tab_action(
                    action="music_save_settings",
                    payload={"values": {"airplay_receiver_targets": ["sonos:den"]}},
                    redis_client=self.redis,
                )

    def test_airplay_stop_action_uses_external_audio_runtime(self):
        stop = Mock(return_value={"status": "ready"})
        runtime = types.SimpleNamespace(stop_external_audio_input=stop)
        with patch.object(self.core, "_external_audio_module", return_value=runtime):
            result = self.core.handle_htmlui_tab_action(
                action="music_airplay_stop",
                payload={},
                redis_client=self.redis,
            )
        self.assertTrue(result["ok"])
        stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
