import asyncio
import importlib.util
import json
import sys
import types
import unittest
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
        self.core._continuation_thread = None
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
            {"catalog_sync", "recommendation_refresh", "continuous_radio_refill"},
        )
        self.assertTrue(tasks["catalog_sync"]["available"])
        self.assertTrue(tasks["recommendation_refresh"]["available"])
        self.assertFalse(tasks["continuous_radio_refill"]["manual"])
        self.assertEqual(tasks["continuous_radio_refill"]["schedule_label"], "Event driven")
        self.assertEqual(tasks["continuous_radio_refill"]["next_run_label"], "Near queue end")

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

    def test_stream_url_is_derived_without_storing_provider_token_in_catalog(self):
        provider = self.core.TaterTubeMusicProvider.from_settings(
            self.redis.hgetall(self.core.SETTINGS_KEY)
        )
        url = provider.stream_url(self.tracks[0])
        self.assertIn("/api/tater/local/stream?", url)
        self.assertIn("player_token=player-token", url)
        self.assertNotIn("player-token", json.dumps(self.tracks))
        self.assertEqual(self.core._track_media_type(self.tracks[0]), "audio/flac")

        artwork_url = provider.artwork_url(self.tracks[0])
        self.assertEqual(artwork_url, "")

    def test_tater_tube_uses_generated_artwork_instead_of_embedded_tag_extraction(self):
        track = self.core._normalize_track(
            {
                "ratingKey": "track:art",
                "title": "Covered Song",
                "categoryId": "local:music",
                "path": "Artist/Album/song.flac",
                "poster": "http://tube.local/artwork?player_token=secret",
            }
        )
        self.assertFalse(track["has_artwork"])
        self.assertNotIn("secret", json.dumps(track))
        public = self.core._public_track(track)
        self.assertTrue(public["artwork_url"].startswith("data:image/svg+xml"))
        provider = self.core.TaterTubeMusicProvider(
            server_url="http://tube.local:8080",
            token="player-token",
        )
        self.assertEqual(provider.artwork_url(track), "")

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

    def test_plex_catalog_and_stream_url(self):
        provider = self.core.PlexMusicProvider(
            server_url="http://plex.local:32400",
            token="plex-secret",
            library_ids=[],
        )
        with patch.object(
            provider,
            "request",
            side_effect=[
                {
                    "MediaContainer": {
                        "Directory": [
                            {"key": "3", "title": "Music", "type": "artist"}
                        ]
                    }
                },
                {
                    "MediaContainer": {
                        "totalSize": 1,
                        "Metadata": [
                            {
                                "ratingKey": "44",
                                "title": "Three Little Birds",
                                "grandparentTitle": "Bob Marley",
                                "parentTitle": "Exodus",
                                "parentThumb": "/library/metadata/album-9/thumb/1234",
                                "updatedAt": 1234,
                                "duration": 180000,
                                "Genre": [{"tag": "Reggae"}],
                                "Media": [
                                    {
                                        "container": "flac",
                                        "Part": [
                                            {
                                                "key": "/library/parts/44/file.flac",
                                                "size": 1234,
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                },
            ],
        ):
            catalog = provider.catalog()
        track = self.core._normalize_track(catalog["tracks"][0])
        self.assertEqual(track["provider"], "plex")
        self.assertEqual(track["artist"], "Bob Marley")
        self.assertEqual(track["duration_seconds"], 180)
        stream_url = provider.stream_url(track)
        self.assertIn("/library/parts/44/file.flac?", stream_url)
        self.assertIn("X-Plex-Token=plex-secret", stream_url)
        self.assertNotIn("plex-secret", json.dumps(catalog))
        self.assertTrue(track["has_artwork"])
        artwork_url = provider.artwork_url(track)
        self.assertIn("/library/metadata/album-9/thumb/1234?", artwork_url)
        self.assertIn("X-Plex-Token=plex-secret", artwork_url)
        self.assertNotIn("plex-secret", self.core._public_track(track)["artwork_url"])

    def test_emby_and_jellyfin_catalog_paths_and_stream_urls(self):
        for provider_id, expected_prefix in (("emby", "/emby/"), ("jellyfin", "/")):
            with self.subTest(provider=provider_id):
                provider = self.core.MediaBrowserMusicProvider(
                    server_url=f"http://{provider_id}.local:8096",
                    api_key=f"{provider_id}-secret",
                    user_id="user-1",
                    provider_id=provider_id,
                )
                with patch.object(
                    provider,
                    "request",
                    return_value={
                        "TotalRecordCount": 1,
                        "Items": [
                            {
                                "Id": "song-1",
                                "Name": "Blue in Green",
                                "Artists": ["Miles Davis"],
                                "AlbumArtist": "Miles Davis",
                                "Album": "Kind of Blue",
                                "AlbumId": "album-1",
                                "AlbumPrimaryImageTag": "album-image-tag",
                                "Genres": ["Jazz"],
                                "RunTimeTicks": 2_200_000_000,
                                "MediaSources": [
                                    {
                                        "Container": "flac",
                                        "Path": "/music/blue.flac",
                                        "Size": 4321,
                                    }
                                ],
                            }
                        ],
                    },
                ) as request:
                    catalog = provider.catalog()
                track = self.core._normalize_track(catalog["tracks"][0])
                self.assertEqual(track["provider"], provider_id)
                self.assertEqual(track["artist"], "Miles Davis")
                self.assertEqual(request.call_args.args[0], "Users/user-1/Items")
                stream_url = provider.stream_url(track)
                self.assertIn(expected_prefix, stream_url)
                self.assertIn("Audio/song-1/stream.flac", stream_url)
                self.assertIn(f"api_key={provider_id}-secret", stream_url)
                self.assertNotIn(f"{provider_id}-secret", json.dumps(catalog))
                self.assertTrue(track["has_artwork"])
                artwork_url = provider.artwork_url(track)
                self.assertIn("/Items/album-1/Images/Primary?", artwork_url)
                self.assertIn(f"api_key={provider_id}-secret", artwork_url)
                self.assertIn("tag=album-image-tag", artwork_url)
                self.assertNotIn(
                    f"{provider_id}-secret",
                    self.core._public_track(track)["artwork_url"],
                )

    def test_navidrome_open_subsonic_catalog_and_salted_stream_auth(self):
        provider = self.core.NavidromeMusicProvider(
            server_url="http://navidrome.local:4533",
            username="tater",
            password="super-secret",
            api_key="",
        )
        with patch.object(
            provider,
            "request",
            return_value={
                "searchResult3": {
                    "song": [
                        {
                            "id": "song-9",
                            "title": "Pressure Drop",
                            "artist": "Toots & The Maytals",
                            "album": "Funky Kingston",
                            "genre": "Reggae",
                            "duration": 185,
                            "suffix": "mp3",
                            "contentType": "audio/mpeg",
                            "coverArt": "cover-9",
                        }
                    ]
                }
            },
        ):
            catalog = provider.catalog()
        track = self.core._normalize_track(catalog["tracks"][0])
        self.assertEqual(track["provider"], "navidrome")
        stream_url = provider.stream_url(track)
        self.assertIn("/rest/stream.view?", stream_url)
        self.assertIn("u=tater", stream_url)
        self.assertIn("id=song-9", stream_url)
        self.assertIn("&s=", stream_url)
        self.assertIn("&t=", stream_url)
        self.assertNotIn("super-secret", stream_url)
        self.assertNotIn("super-secret", json.dumps(catalog))
        self.assertTrue(track["has_artwork"])
        artwork_url = provider.artwork_url(track)
        self.assertIn("/rest/getCoverArt.view?", artwork_url)
        self.assertIn("id=cover-9", artwork_url)
        self.assertNotIn("super-secret", artwork_url)
        self.assertNotIn("super-secret", self.core._public_track(track)["artwork_url"])

    def test_removed_roon_provider_migrates_to_tater_tube_and_clears_stale_player(self):
        self.redis.hset(self.core.SETTINGS_KEY, mapping={"provider": "roon"})
        self.redis.set(
            self.core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "provider": "roon",
                    "queue": [],
                    "index": -1,
                    "current": {"id": "roon:dynamic", "title": "Jazz", "provider": "roon"},
                    "targets": ["integration:roon:zone-kitchen"],
                }
            ),
        )
        player = self.core._player(self.redis)
        self.assertEqual(self.core._provider_id("roon"), "tater_tube")
        self.assertEqual(player["provider"], "tater_tube")
        self.assertEqual(player["status"], "stopped")
        self.assertEqual(player["queue"], [])
        self.assertEqual(player["current"], {})
        self.assertEqual(player["targets"], [])

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
            stream_options = self.core._target_options(provider_id="plex")
        self.assertEqual(
            [row["value"] for row in stream_options],
            ["voice_core:native:kitchen"],
        )
        self.assertTrue(
            all(
                call.kwargs.get("include_homeassistant") is True
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
        self.assertEqual(payload["tracks"][0]["id"], "track:one")
        self.assertEqual(payload["targets"][0]["kind"], "satellite")
        self.assertNotIn("player-token", json.dumps(payload))

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

    def test_play_track_forwards_mixed_targets_to_tater_router(self):
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
                client=self.redis,
            )
        self.assertTrue(result["ok"])
        self.assertEqual(playback.play_media_url_targets.call_args.args[0], targets)
        self.assertEqual(
            playback.play_media_url_targets.call_args.kwargs["volume_percent"],
            55,
        )

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
                }
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
        self.assertEqual(player["track_list_action"], "music_ui_queue_play")
        self.assertEqual(player["track_list_shuffle_action"], "music_ui_set_shuffle")
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
        self.assertEqual(payload["ui"]["appearance"], "music_library")
        self.assertTrue(payload["ui"]["live_updates"])
        self.assertEqual(payload["ui"]["poll_interval_ms"], 3000)
        self.assertEqual(payload["ui"]["persistent_item_groups"], ["player"])
        self.assertEqual(payload["ui"]["default_tab"], "library")
        tabs = {row["key"]: row for row in payload["ui"]["manager_tabs"]}
        self.assertNotIn("player", tabs)
        self.assertEqual(tabs["recommendations"]["item_group"], "recommendations")
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
        self.assertEqual(settings_fields["default_targets"]["type"], "multiselect")
        self.assertTrue(settings_fields["recommendations_enabled"]["value"])
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
            {
                "provider:tater_tube",
                "provider:plex",
                "provider:emby",
                "provider:jellyfin",
                "provider:navidrome",
            },
        )
        self.assertEqual(providers["provider:plex"]["fields"][1]["type"], "password")

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
        stop.assert_called_once_with(["voice_core:native:kitchen"])
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
        stop.assert_called_once_with(["voice_core:native:kitchen"])

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
        self.assertTrue(player["shuffle"])
        self.assertEqual(player["queue"][0]["id"], "track:one")
        self.assertEqual(
            {row["id"] for row in player["queue"][1:]},
            {"track:two", "track:three"},
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


if __name__ == "__main__":
    unittest.main()
