from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
