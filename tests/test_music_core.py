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

    def test_roon_playback_delegates_to_existing_integration(self):
        integration_registry = types.ModuleType("integration_registry")
        integration_registry.run_integration_device_action = Mock(
            return_value={"ok": True}
        )
        targets = [
            "integration:roon:zone-kitchen",
            "integration:roon:zone-living",
        ]
        devices = [
            {"integration_id": "roon", "device_id": "zone-kitchen"},
            {"integration_id": "roon", "device_id": "zone-living"},
        ]
        with patch.object(
            self.core,
            "_resolve_targets",
            return_value=targets,
        ), patch.object(
            self.core,
            "_roon_device_targets",
            return_value=devices,
        ), patch.dict(
            sys.modules,
            {"integration_registry": integration_registry},
        ):
            result = self.core._play_request(
                {
                    "provider": "roon",
                    "artist": "Bob Marley",
                    "targets": targets,
                    "shuffle": True,
                },
                {},
                self.redis,
            )
            player = self.core._roon_control("next", client=self.redis)
        self.assertTrue(result["ok"])
        self.assertEqual(result["provider"], "roon")
        self.assertEqual(result["target_count"], 2)
        calls = integration_registry.run_integration_device_action.call_args_list
        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[0].args[:3], ("roon", "play_media", "zone-kitchen"))
        self.assertEqual(calls[0].args[3]["query"], "Bob Marley")
        self.assertEqual(calls[-1].args[:3], ("roon", "next", "zone-living"))
        self.assertEqual(player["provider"], "roon")

    def test_target_picker_separates_roon_zones_from_stream_targets(self):
        announcement_targets = types.ModuleType("announcement_targets")
        announcement_targets.build_announcement_target_options = Mock(
            return_value=[
                {
                    "value": "voice_core:native:kitchen",
                    "label": "Tater Satellite: Kitchen",
                }
            ]
        )
        integration_registry = types.ModuleType("integration_registry")
        integration_registry.get_integration_devices_by_capability = Mock(
            return_value=[
                {
                    "integration_id": "roon",
                    "id": "zone-kitchen",
                    "name": "Kitchen",
                    "room": "Kitchen",
                }
            ]
        )
        with patch.dict(
            sys.modules,
            {
                "announcement_targets": announcement_targets,
                "integration_registry": integration_registry,
            },
        ):
            stream_options = self.core._target_options(provider_id="plex")
            roon_options = self.core._target_options(provider_id="roon")
        self.assertEqual(
            [row["value"] for row in stream_options],
            ["voice_core:native:kitchen"],
        )
        self.assertEqual(
            [row["value"] for row in roon_options],
            ["integration:roon:zone-kitchen"],
        )
        self.assertTrue(
            all(
                call.kwargs.get("include_homeassistant") is True
                for call in announcement_targets.build_announcement_target_options.call_args_list
            )
        )
        integration_registry.get_integration_devices_by_capability.assert_called_with(
            "media_player",
            self.core.redis_client,
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
                "provider:roon",
            },
        )
        self.assertEqual(providers["provider:plex"]["fields"][1]["type"], "password")
        self.assertEqual(providers["provider:roon"]["fields"], [])

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
