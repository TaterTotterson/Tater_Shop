from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "verba" / "irc_admin_op.py"


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
    helpers.extract_json = lambda value: value
    helpers.redis_client = SimpleNamespace(hgetall=lambda _key: {})

    spec = importlib.util.spec_from_file_location("irc_admin_op_test_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {
            "verba_base": verba_base,
            "verba_result": verba_result,
            "helpers": helpers,
        },
    ):
        spec.loader.exec_module(module)
    return module


class _FakeLlm:
    def __init__(self, action: str):
        self.action = action

    async def chat(self, **_kwargs):
        return {"message": {"content": json.dumps({"action": self.action})}}


class IrcAdminOpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_module()

    def setUp(self):
        self.plugin = self.module.IrcAdminOpPlugin()
        self.bot = SimpleNamespace(mode=Mock(), privmsg=Mock())

    def _run(self, action: str):
        with patch.object(self.module, "_admin_nicks", return_value={"mastaful"}):
            return asyncio.run(
                self.plugin.handle_irc(
                    bot=self.bot,
                    channel="#gMp3",
                    user="mastaful",
                    raw_message=f"{action} me",
                    args={"query": f"{action} me"},
                    llm_client=_FakeLlm(action),
                )
            )

    def test_op_uses_direct_irc_mode_without_chanserv(self):
        result = self._run("op")

        self.assertTrue(result["ok"])
        self.bot.mode.assert_called_once_with("#gMp3", "+o", "mastaful")
        self.bot.privmsg.assert_not_called()
        self.assertEqual(result["facts"]["mode"], "+o")
        self.assertNotIn("ChanServ", result["summary_for_user"])

    def test_voice_uses_direct_irc_mode(self):
        result = self._run("voice")

        self.assertTrue(result["ok"])
        self.bot.mode.assert_called_once_with("#gMp3", "+v", "mastaful")
        self.bot.privmsg.assert_not_called()

    def test_mode_send_failure_is_reported(self):
        self.bot.mode.side_effect = RuntimeError("not connected")

        result = self._run("op")

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "irc_command_failed")
        self.bot.privmsg.assert_not_called()


if __name__ == "__main__":
    unittest.main()
