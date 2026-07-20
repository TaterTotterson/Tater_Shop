import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


class FakeRedis:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key):
        return self.values.get(key)


def load_tater_tube_core():
    helpers = types.ModuleType("helpers")
    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    helpers.redis_client = FakeRedis()
    sys.modules["helpers"] = helpers
    requests = types.ModuleType("requests")
    requests.request = lambda *_args, **_kwargs: None
    sys.modules["requests"] = requests

    path = Path(__file__).resolve().parents[1] / "cores" / "tater_tube_core.py"
    spec = importlib.util.spec_from_file_location("tater_tube_core_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class TaterTubeCoreAssistantNameTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_tater_tube_core()

    def test_reads_only_the_configured_first_name(self):
        client = FakeRedis({"tater:first_name": "  Totty Totterson  "})
        self.assertEqual(self.core._assistant_first_name(client), "Totty")

    def test_defaults_to_tater(self):
        self.assertEqual(self.core._assistant_first_name(FakeRedis()), "Tater")

    def test_sends_a_url_encoded_unicode_name_header(self):
        response = types.SimpleNamespace(
            ok=True,
            status_code=200,
            json=lambda: {"data": {"ok": True}},
        )
        client = FakeRedis({"tater:first_name": "José Totterson"})
        with patch.object(self.core.requests, "request", return_value=response) as request:
            self.core._api_request(
                "GET",
                "tater/core/context",
                server_url="https://tatertube.example",
                token="secret",
                redis_obj=client,
            )
        headers = request.call_args.kwargs["headers"]
        self.assertEqual(headers["X-Tater-Assistant-Name"], "Jos%C3%A9")

    def test_reloads_the_name_for_each_server_request(self):
        response = types.SimpleNamespace(
            ok=True,
            status_code=200,
            json=lambda: {"data": {"ok": True}},
        )
        client = FakeRedis({"tater:first_name": "Tater"})
        with patch.object(self.core.requests, "request", return_value=response) as request:
            self.core._api_request(
                "GET",
                "tater/core/context",
                server_url="https://tatertube.example",
                token="secret",
                redis_obj=client,
            )
            client.values["tater:first_name"] = "Totty"
            self.core._api_request(
                "GET",
                "tater/core/context",
                server_url="https://tatertube.example",
                token="secret",
                redis_obj=client,
            )
        self.assertEqual(
            request.call_args_list[-1].kwargs["headers"]["X-Tater-Assistant-Name"],
            "Totty",
        )


if __name__ == "__main__":
    unittest.main()
