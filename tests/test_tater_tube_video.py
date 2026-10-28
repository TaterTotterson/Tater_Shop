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
        self.display.resolve_target = lambda value, **_kwargs: "native:kitchen"
        self.display.target_playback_profile = lambda _selector: {
            "transcode_profile": "echo_show_720p",
            "width": 1280,
            "height": 800,
        }
        self.display.register_provider_handler = Mock()
        self.display.play = AsyncMock(
            return_value={"ok": True, "session": {"state": "playing", "position_ms": 0, "duration_ms": 90_000}}
        )
        self.display.control = AsyncMock(return_value={"ok": True, "session": {"state": "paused"}})
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
        self.assertEqual(payload["viewing_profile_id"], "household")
        self.display.play.assert_awaited_once()
        play_kwargs = self.display.play.await_args.kwargs
        self.assertTrue(play_kwargs["server_seek"])
        self.assertEqual(play_kwargs["provider"], "tater_tube")
        self.assertEqual(play_kwargs["provider_data"]["playback_id"], "play-1")

    async def test_control_stays_inside_tater_tube_core(self):
        result = await self.core.run_hydra_kernel_tool(
            tool_id="tater_tube_video_control",
            args={"action": "pause", "target": "Kitchen"},
            redis_client=self.store,
        )
        self.assertTrue(result["ok"], result)
        self.display.control.assert_awaited_once_with("native:kitchen", "pause")


if __name__ == "__main__":
    unittest.main()
