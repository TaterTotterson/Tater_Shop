from __future__ import annotations

import asyncio
import importlib.util
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "verba" / "comfyui_image_video.py"


def _action_success(**kwargs):
    return {"ok": True, **kwargs}


def _action_failure(*, code, message, **kwargs):
    return {"ok": False, "error": {"code": code, "message": message}, **kwargs}


def _load_module():
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})
    verba_result = types.ModuleType("verba_result")
    verba_result.action_success = _action_success
    verba_result.action_failure = _action_failure
    helpers = types.ModuleType("helpers")
    helpers.redis_client = SimpleNamespace(hgetall=lambda _key: {}, lrange=lambda *_args: [])
    helpers.redis_blob_client = SimpleNamespace(get=lambda _key: None)
    helpers.run_comfy_prompt = lambda *_args, **_kwargs: None
    moviepy = types.ModuleType("moviepy")
    moviepy_video = types.ModuleType("moviepy.video")
    moviepy_video_io = types.ModuleType("moviepy.video.io")
    image_sequence = types.ModuleType("moviepy.video.io.ImageSequenceClip")
    image_sequence.ImageSequenceClip = type("ImageSequenceClip", (), {})

    spec = importlib.util.spec_from_file_location("comfyui_image_video_test_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {
            "verba_base": verba_base,
            "verba_result": verba_result,
            "helpers": helpers,
            "moviepy": moviepy,
            "moviepy.video": moviepy_video,
            "moviepy.video.io": moviepy_video_io,
            "moviepy.video.io.ImageSequenceClip": image_sequence,
        },
    ):
        spec.loader.exec_module(module)
    return module


class ComfyUIImageVideoDiscordTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_module()

    def setUp(self):
        self.plugin = self.module.ComfyUIImageVideoPlugin()

    @staticmethod
    def _message(limit=1000):
        return SimpleNamespace(
            guild=SimpleNamespace(filesize_limit=limit),
            channel=SimpleNamespace(),
        )

    @staticmethod
    def _result(binary: bytes):
        return {
            "ok": True,
            "facts": {"artifact_count": 1},
            "artifacts": [
                {
                    "type": "video",
                    "name": "animated.mp4",
                    "mimetype": "video/mp4",
                    "size": len(binary),
                    "bytes": binary,
                }
            ],
        }

    def test_metadata_enables_discord_and_bumps_version(self):
        self.assertEqual(self.plugin.version, "1.0.7")
        self.assertIn("discord", self.plugin.platforms)

    def test_upload_limit_uses_guild_and_reserves_headroom(self):
        self.assertEqual(self.plugin._discord_upload_limit(self._message(50_000_000)), 50_000_000)
        target = self.plugin._discord_safe_target(50_000_000)
        self.assertLess(target, 50_000_000)
        self.assertGreater(target, 48_000_000)
        self.assertEqual(
            self.plugin._discord_upload_limit(SimpleNamespace(guild=None, channel=None)),
            self.module.DISCORD_DEFAULT_UPLOAD_LIMIT,
        )

    def test_discord_input_artifact_can_be_used_as_source_image(self):
        image_bytes = b"\x89PNG\r\n\x1a\nsource-image"
        context = {
            "origin": {
                "input_artifacts": [
                    {
                        "type": "image",
                        "name": "discord-upload.png",
                        "mimetype": "image/png",
                        "blob_key": "discord:source",
                    }
                ]
            }
        }
        with patch.object(self.module.redis_blob_client, "get", return_value=image_bytes):
            resolved, name = self.plugin._resolve_source_image({}, context=context)
        self.assertEqual(resolved, image_bytes)
        self.assertEqual(name, "discord-upload.png")

    def test_small_discord_video_is_returned_unchanged(self):
        generated = self._result(b"small-video")
        with patch.object(self.plugin, "_generate", AsyncMock(return_value=generated)), patch.object(
            self.plugin, "_compress_video_for_discord", wraps=self.plugin._compress_video_for_discord
        ) as compress:
            result = asyncio.run(self.plugin.handle_discord(self._message(1000), {}, None))

        self.assertTrue(result["ok"])
        self.assertEqual(result["artifacts"][0]["bytes"], b"small-video")
        self.assertFalse(result["facts"]["discord_compressed"])
        compress.assert_called_once()

    def test_large_discord_video_is_replaced_with_verified_copy(self):
        generated = self._result(b"x" * 1200)
        details = {
            "compressed": True,
            "original_size": 1200,
            "delivered_size": 800,
            "attempts": 1,
        }
        with patch.object(self.plugin, "_generate", AsyncMock(return_value=generated)), patch.object(
            self.plugin,
            "_compress_video_for_discord",
            return_value=(b"y" * 800, details),
        ):
            result = asyncio.run(self.plugin.handle_discord(self._message(1000), {}, None))

        self.assertTrue(result["ok"])
        self.assertEqual(result["artifacts"][0]["name"], "animated-discord.mp4")
        self.assertEqual(result["artifacts"][0]["size"], 800)
        self.assertLessEqual(result["artifacts"][0]["size"], result["facts"]["discord_target_size"])
        self.assertTrue(result["facts"]["discord_compressed"])

    def test_compression_failure_returns_clear_discord_error(self):
        with patch.object(
            self.plugin, "_generate", AsyncMock(return_value=self._result(b"x" * 1200))
        ), patch.object(
            self.plugin, "_compress_video_for_discord", side_effect=RuntimeError("encoder failed")
        ), patch.object(
            self.module.logger, "exception"
        ):
            result = asyncio.run(self.plugin.handle_discord(self._message(1000), {}, None))

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "discord_video_too_large")

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "FFmpeg is not installed")
    def test_real_ffmpeg_output_stays_under_requested_size(self):
        ffmpeg = shutil.which("ffmpeg")
        with tempfile.TemporaryDirectory(prefix="tater-discord-test-") as tmp_dir:
            source_path = Path(tmp_dir) / "source.mp4"
            subprocess.run(
                [
                    ffmpeg,
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "testsrc2=size=640x360:rate=24",
                    "-t",
                    "4",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "ultrafast",
                    "-crf",
                    "0",
                    "-pix_fmt",
                    "yuv420p",
                    str(source_path),
                ],
                check=True,
                timeout=120,
            )
            source = source_path.read_bytes()

        target = 160 * 1024
        self.assertGreater(len(source), target)
        delivered, details = self.plugin._compress_video_for_discord(source, target)
        self.assertTrue(details["compressed"])
        self.assertLessEqual(len(delivered), target)
        self.assertGreater(len(delivered), 0)


if __name__ == "__main__":
    unittest.main()
