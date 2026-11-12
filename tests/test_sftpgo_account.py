from __future__ import annotations

import importlib.util
import json
import string
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


SHOP_ROOT = Path(__file__).resolve().parents[1]


class FakeRedis:
    def __init__(self, settings=None):
        self.settings = dict(settings or {})

    def hgetall(self, _key):
        return dict(self.settings)


def load_sftpgo_account(redis_settings=None):
    aiohttp = types.ModuleType("aiohttp")
    aiohttp.TCPConnector = object
    aiohttp.ClientSession = object

    helpers = types.ModuleType("helpers")
    helpers.redis_client = FakeRedis(redis_settings)

    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = object

    verba_result = types.ModuleType("verba_result")
    verba_result.action_failure = lambda **kwargs: {"ok": False, **kwargs}
    verba_result.action_success = lambda **kwargs: {"ok": True, **kwargs}

    stubs = {
        "aiohttp": aiohttp,
        "helpers": helpers,
        "verba_base": verba_base,
        "verba_result": verba_result,
    }
    previous = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        path = SHOP_ROOT / "verba" / "sftpgo_account.py"
        spec = importlib.util.spec_from_file_location("sftpgo_account_test_module", path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, old_module in previous.items():
            if old_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_module


class SFTPGoAccountPayloadTests(unittest.TestCase):
    def test_generated_passwords_are_strong_and_chat_safe(self):
        module = load_sftpgo_account()
        plugin = module.SFTPGoAccountPlugin()
        allowed = set(string.ascii_letters + string.digits + module.PASSWORD_SYMBOLS)

        for _ in range(100):
            password = plugin.generate_random_password()
            self.assertEqual(len(password), 20)
            self.assertTrue(set(password) <= allowed)
            for group in (string.ascii_lowercase, string.ascii_uppercase, string.digits, module.PASSWORD_SYMBOLS):
                self.assertTrue(set(password) & set(group))

        self.assertEqual(len(plugin.generate_random_password(32)), 32)
        with self.assertRaises(ValueError):
            plugin.generate_random_password(3)

    def test_default_settings_use_sftpgo_users_base_directory(self):
        module = load_sftpgo_account()
        plugin = module.SFTPGoAccountPlugin()

        settings = plugin.get_sftpgo_settings()

        self.assertEqual(settings["DEFAULT_HOME_DIR"], "")
        self.assertEqual(settings["SFTPGO_API_URL"], "https://localhost/api/v2")

    def test_legacy_placeholder_is_treated_as_unset(self):
        module = load_sftpgo_account(
            {"DEFAULT_HOME_DIR": "/your/default/home/dir"}
        )
        plugin = module.SFTPGoAccountPlugin()

        settings = plugin.get_sftpgo_settings()
        payload = plugin._user_payload("alice", "secret", settings)

        self.assertEqual(settings["DEFAULT_HOME_DIR"], "")
        self.assertNotIn("home_dir", payload)

    def test_payload_grants_full_access_within_the_user_home(self):
        module = load_sftpgo_account()
        payload = module.SFTPGoAccountPlugin._user_payload(
            "alice",
            "secret",
            {"DEFAULT_HOME_DIR": "", "SFTPGO_GROUP_NAME": "DNServ"},
        )

        self.assertEqual(payload["permissions"], {"/": ["*"]})
        self.assertNotIn("home_dir", payload)
        self.assertEqual(payload["groups"], [{"name": "DNServ", "type": 1}])

    def test_custom_home_directory_supports_username_template(self):
        module = load_sftpgo_account()
        payload = module.SFTPGoAccountPlugin._user_payload(
            "alice",
            "secret",
            {
                "DEFAULT_HOME_DIR": "/srv/custom/{username}",
                "SFTPGO_GROUP_NAME": "DNServ",
            },
        )

        self.assertEqual(payload["home_dir"], "/srv/custom/alice")


class FakeResponse:
    def __init__(self, status):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def text(self):
        return "Request rejected"


class FakeSFTPGoSession:
    def __init__(self, *, exists=False, create_status=201):
        self.exists = exists
        self.create_status = create_status
        self.created_payload = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def get(self, _url, **_kwargs):
        return FakeResponse(200 if self.exists else 404)

    def post(self, _url, **kwargs):
        # Exercise the JSON round trip used by aiohttp, including backslashes
        # and quotes that used to be generated in passwords.
        self.created_payload = json.loads(json.dumps(kwargs["json"]))
        return FakeResponse(self.create_status)


class SFTPGoAccountDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.module = load_sftpgo_account()
        self.plugin = self.module.SFTPGoAccountPlugin()
        self.plugin.get_jwt_token = AsyncMock(return_value="test-token")
        self.session = FakeSFTPGoSession()
        self.module.aiohttp.TCPConnector = lambda **_kwargs: None
        self.module.aiohttp.ClientSession = lambda **_kwargs: self.session
        self.module.discord = None
        self.message = types.SimpleNamespace(
            author=types.SimpleNamespace(name="alice", send=AsyncMock()),
            channel=types.SimpleNamespace(send=AsyncMock()),
        )
        self.password = 'Aa9\\_*~||```"!'

    async def test_exact_api_password_is_delivered_once_without_llm_copy(self):
        with patch.object(self.plugin, "generate_random_password", return_value=self.password):
            result = await self.plugin.handle_discord(self.message, {}, None)

        self.assertEqual(self.session.created_payload["password"], self.password)
        self.message.author.send.assert_awaited_once()
        content = self.message.author.send.await_args.args[0]
        self.assertIn(f"Password:\n````\n{self.password}\n````", content)
        self.assertTrue(result["ok"])
        self.assertTrue(result["facts"]["credentials_delivered"])
        self.assertEqual(result["facts"]["credential_delivery"], "discord_dm")
        self.assertNotIn("password", result["facts"])
        self.assertNotIn(self.password, json.dumps(result))
        self.message.channel.send.assert_not_awaited()

    async def test_blocked_dm_reports_created_account_without_leaking_password(self):
        self.message.author.send.side_effect = RuntimeError(self.password)
        with (
            patch.object(self.plugin, "generate_random_password", return_value=self.password),
            self.assertLogs(self.module.logger, level="WARNING") as logs,
        ):
            result = await self.plugin.handle_discord(self.message, {}, None)

        self.assertTrue(result["ok"])
        self.assertEqual(result["facts"]["state"], "created")
        self.assertFalse(result["facts"]["credentials_delivered"])
        self.assertIn("reset", result["summary_for_user"])
        self.assertNotIn(self.password, json.dumps(result))
        self.assertNotIn(self.password, " ".join(logs.output))
        self.message.channel.send.assert_not_awaited()

    async def test_existing_account_is_not_overwritten_and_receives_no_new_password(self):
        self.session.exists = True
        result = await self.plugin.handle_discord(self.message, {}, None)

        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "account_exists")
        self.assertIsNone(self.session.created_payload)
        self.message.author.send.assert_not_awaited()

    async def test_rejected_account_creation_never_sends_credentials(self):
        self.session.create_status = 400
        with self.assertLogs(self.module.logger, level="ERROR"):
            result = await self.plugin.handle_discord(self.message, {}, None)

        self.assertFalse(result["ok"])
        self.message.author.send.assert_not_awaited()

    async def test_webui_uses_same_password_as_api(self):
        with patch.object(self.plugin, "generate_random_password", return_value=self.password):
            result = await self.plugin.handle_webui({"username": "alice"}, None)

        self.assertTrue(result["ok"])
        self.assertEqual(result["facts"]["password"], self.session.created_payload["password"])
        self.message.author.send.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
