import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.test_tater_tube_core import FakeRedis, load_tater_tube_core


class TaterTubeVideoToolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.core = load_tater_tube_core()
        self.store = FakeRedis()
        self.store.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "server_url": "http://tube.local",
                "token": "core-token",
                "profile_id": "household",
            },
        )
        self.display = types.ModuleType("tater_voice.display_video")
        self.display.match_target = lambda value: "native:living-room" if "living room" in str(value).lower() else ""
        self.display.match_request_target = self.display.match_target
        self.display.resolve_target = lambda value, **_kwargs: "native:kitchen"
        self.display.target_playback_profile = lambda _selector: {
            "name": "Kitchen Show",
            "transcode_profile": "echo_show_720p",
            "output_container": "mpegts",
            "media_type": "video/mp2t",
            "width": 1280,
            "height": 800,
        }
        self.display.register_provider_handler = Mock()
        self.display.play = AsyncMock(
            return_value={"ok": True, "session": {"state": "playing", "position_ms": 0, "duration_ms": 90_000}}
        )
        tater_voice = types.ModuleType("tater_voice")
        tater_voice.display_video = self.display
        sys.modules["tater_voice"] = tater_voice
        sys.modules["tater_voice.display_video"] = self.display

    async def test_play_resolves_through_server_and_uses_screen_profile(self):
        resolved = {
            "playback_id": "play-1",
            "candidate_id": "movie-1",
            "title": "Movie",
            "subtitle": "Tater Tube",
            "stream_url": "http://tube.local/api/tater/local/stream?core_stream_token=secret",
            "duration_ms": 90_000,
            "position_ms": 0,
            "fps": 30,
            "transcode_profile": "echo_show_720p",
            "output_container": "mpegts",
            "stream_media_type": "video/mp2t",
        }
        with patch.object(self.core, "_api_request", return_value=resolved) as request:
            result = await self.core.run_hydra_kernel_tool(
                tool_id="tater_tube_play",
                args={"query": "Movie"},
                origin={"satellite_selector": "native:kitchen"},
                redis_client=self.store,
            )
        self.assertTrue(result["ok"], result)
        payload = request.call_args.kwargs["payload"]
        self.assertEqual(payload["transcode_profile"], "echo_show_720p")
        self.assertEqual(payload["output_container"], "mpegts")
        self.assertEqual(payload["viewing_profile_id"], "household")
        self.display.play.assert_awaited_once()
        play_kwargs = self.display.play.await_args.kwargs
        self.assertTrue(play_kwargs["server_seek"])
        self.assertEqual(play_kwargs["media_type"], "video/mp2t")
        self.assertEqual(play_kwargs["provider"], "tater_tube")
        self.assertEqual(play_kwargs["provider_data"]["playback_id"], "play-1")

    async def test_natural_language_room_is_resolved_before_speaking_satellite(self):
        with patch.object(
            self.core,
            "_api_request",
            return_value={
                "playback_id": "play-2",
                "title": "Movie",
                "stream_url": "http://tube.local/movie.ts",
                "transcode_profile": "echo_show_720p",
                "output_container": "mpegts",
                "stream_media_type": "video/mp2t",
            },
        ):
            result = await self.core.run_hydra_kernel_tool(
                tool_id="tater_tube_play",
                args={"query": "play Movie in the living room"},
                origin={"satellite_selector": "native:kitchen"},
                redis_client=self.store,
            )
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["selector"], "native:living-room")

    def test_tool_metadata_requests_complete_natural_language(self):
        tools = {row["id"]: row for row in self.core.get_hydra_kernel_tools()}
        self.assertEqual(
            tools["tater_tube_play"]["description"],
            "Use when the user asks to watch or play a movie, episode, or TV series.",
        )
        self.assertNotIn("tater_tube_video_control", tools)
        self.assertIn('"query"', tools["tater_tube_play"]["usage"])
        self.assertNotIn('"target"', tools["tater_tube_play"]["usage"])

    async def test_missing_same_room_display_returns_clear_spoken_failure(self):
        self.display.resolve_target = Mock(
            side_effect=ValueError("There is no screen or compatible TV available in Office to play the video.")
        )
        result = await self.core.run_hydra_kernel_tool(
            tool_id="tater_tube_play",
            args={"query": "play Movie"},
            origin={"satellite_selector": "native:office-dot"},
            redis_client=self.store,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "tater_tube_no_video_display")
        self.assertIn("nowhere available", result["say_hint"])


if __name__ == "__main__":
    unittest.main()
