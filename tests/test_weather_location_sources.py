from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import time
import types
import unittest
from pathlib import Path
from urllib.parse import unquote


SHOP_ROOT = Path(__file__).resolve().parents[1]


def _load_module(path: Path, module_name: str, stubs: dict[str, types.ModuleType]):
    previous = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
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


def _tateros_stub() -> types.ModuleType:
    integration_store = types.SimpleNamespace(integration_module=lambda _integration_id: None)
    module = types.ModuleType("tateros")
    module.integration_store = integration_store
    return module


def load_weather_forecast():
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None

    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = object

    helpers = types.ModuleType("helpers")
    async def llm_structured_chat(client, *, messages, **kwargs):
        return await client.chat(messages=messages, **kwargs)

    helpers.get_tater_name = lambda: ("Tater", "")
    helpers.llm_structured_chat = llm_structured_chat

    diagnostics = types.ModuleType("verba_diagnostics")
    diagnostics.needs_from_diagnosis = lambda *_args, **_kwargs: []

    result = types.ModuleType("verba_result")
    result.action_failure = lambda **kwargs: {"ok": False, **kwargs}
    result.action_success = lambda **kwargs: {"ok": True, **kwargs}

    return _load_module(
        SHOP_ROOT / "verba" / "weather_forecast.py",
        "weather_forecast_location_test_module",
        {
            "dotenv": dotenv,
            "verba_base": verba_base,
            "helpers": helpers,
            "tateros": _tateros_stub(),
            "verba_diagnostics": diagnostics,
            "verba_result": result,
        },
    )


def load_environment_core():
    helpers = types.ModuleType("helpers")
    async def llm_structured_chat(client, *, messages, **kwargs):
        return await client.chat(messages=messages, **kwargs)

    helpers.extract_json = lambda value: str(value or "")
    helpers.llm_structured_chat = llm_structured_chat
    helpers.redis_client = None
    return _load_module(
        SHOP_ROOT / "cores" / "environment_core.py",
        "environment_core_weather_source_test_module",
        {"helpers": helpers, "tateros": _tateros_stub()},
    )


def _reading(
    *,
    key: str,
    label: str,
    category: str,
    value: object,
    display: str,
    provider: str,
    provider_label: str,
    source_name: str,
    area: str,
) -> dict[str, object]:
    return {
        "key": key,
        "label": label,
        "category": category,
        "value": value,
        "display": display,
        "provider": provider,
        "provider_label": provider_label,
        "source_id": f"{provider}:test",
        "source_name": source_name,
        "area": area,
    }


class FakeLLM:
    def __init__(self, *responses: object) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        if not self.responses:
            raise RuntimeError("No fake AI response remains")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        content = response if isinstance(response, str) else json.dumps(response)
        return {"message": {"content": content}}


def _weather_settings() -> dict[str, object]:
    return {
        "WEATHERAPI_KEY": "test-weather-api-key",
        "DEFAULT_LOCATION": "60614",
        "DEFAULT_DAYS": 3,
        "DEFAULT_UNITS": "us",
        "INCLUDE_AQI": False,
        "INCLUDE_POLLEN": False,
        "INCLUDE_ALERTS": False,
        "SHOW_HOURLY_PEEK": 0,
        "MAX_RESPONSE_CHARS": 650,
        "TIMEOUT_SECONDS": 12,
    }


def _weather_intent(**overrides: object) -> dict[str, object]:
    intent: dict[str, object] = {
        "request_type": "current",
        "measurement": "temperature",
        "location_scope": "local_outdoor",
        "location": None,
        "area": None,
        "sensor": None,
        "provider": None,
        "date": None,
        "day": "current",
        "days": 1,
        "hours": 0,
        "units": "us",
        "temperature_focus": "current",
    }
    intent.update(overrides)
    return intent


class WeatherForecastLocationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_weather_forecast()
        cls.plugin = cls.module.WeatherForecastPlugin()

    def test_ai_interprets_local_and_named_place_scopes(self) -> None:
        local_llm = FakeLLM(_weather_intent())
        local = asyncio.run(
            self.plugin._interpret_weather_request_ai(
                request_text="What is the temperature outside?",
                args={},
                settings=_weather_settings(),
                llm_client=local_llm,
            )
        )
        self.assertEqual(local["location_scope"], "local_outdoor")
        self.assertIsNone(local["location"])

        named_llm = FakeLLM(
            _weather_intent(location_scope="named_place", location="São Paulo")
        )
        named = asyncio.run(
            self.plugin._interpret_weather_request_ai(
                request_text="What is the temperature in São Paulo?",
                args={},
                settings=_weather_settings(),
                llm_client=named_llm,
            )
        )
        self.assertEqual(named["location_scope"], "named_place")
        self.assertEqual(named["location"], "São Paulo")

    def test_ai_is_required_and_invalid_output_does_not_fall_back(self) -> None:
        with self.assertRaises(self.module.WeatherIntentError):
            asyncio.run(
                self.plugin._interpret_weather_request_ai(
                    request_text="What is the temperature in Chicago?",
                    args={},
                    settings=_weather_settings(),
                    llm_client=None,
                )
            )
        with self.assertRaises(self.module.WeatherIntentError):
            asyncio.run(
                self.plugin._interpret_weather_request_ai(
                    request_text="What is the temperature in Chicago?",
                    args={},
                    settings=_weather_settings(),
                    llm_client=FakeLLM("not json"),
                )
            )

    def test_question_punctuation_and_mentions_use_the_ai_location(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_fetch(**kwargs):
            calls.append(kwargs)
            return {
                "location": {"name": "Chicago", "region": "Illinois", "country": "United States"},
                "current": {
                    "temp_f": 70,
                    "temp_c": 21.1,
                    "feelslike_f": 70,
                    "feelslike_c": 21.1,
                    "wind_mph": 4,
                    "wind_kph": 6.4,
                    "humidity": 50,
                    "condition": {"text": "Clear"},
                },
                "forecast": {"forecastday": []},
            }, None

        old_settings = self.module.read_weatherapi_settings
        old_fetch = self.module.fetch_weatherapi_forecast
        self.module.read_weatherapi_settings = lambda: _weather_settings()
        self.module.fetch_weatherapi_forecast = fake_fetch
        try:
            llm = FakeLLM(
                _weather_intent(location_scope="named_place", location="Chicago"),
                "The temperature in Chicago is 70°F.",
            )
            answer = asyncio.run(
                self.plugin._get_weather_text(
                    {"request": "<@12345> What is the temperature in Chicago?", "units": "us"},
                    llm,
                )
            )
        finally:
            self.module.read_weatherapi_settings = old_settings
            self.module.fetch_weatherapi_forecast = old_fetch
        self.assertEqual(calls[0]["location"], "Chicago")
        self.assertEqual(answer, "The temperature in Chicago is 70°F.")
        self.assertEqual(len(llm.calls), 2)

    def test_tool_guidance_reserves_location_overrides_for_real_places(self) -> None:
        guidance = " ".join(
            (self.plugin.description, self.plugin.when_to_use, self.plugin.usage)
        ).lower()
        self.assertIn("ai", guidance)
        self.assertIn("outside", guidance)
        self.assertIn("geographic place", guidance)
        self.assertIn("regex", guidance)
        self.assertIn("current weather conditions and forecasts", guidance)
        self.assertIn("air quality", guidance)
        self.assertNotIn("environment core", guidance)
        self.assertNotIn("temperature status", guidance)


class EnvironmentKernelCurrentSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.core = load_environment_core()

    def _payload(self) -> dict[str, object]:
        now = time.time()
        ecowitt = {
            "provider": "ecowitt",
            "model": "Backyard Station",
            "received_at": now,
            "sample_time": now,
            "readings": [
                _reading(
                    key="tempf",
                    label="Outdoor Temperature",
                    category="temperature",
                    value=105,
                    display="105 °F",
                    provider="ecowitt",
                    provider_label="Ecowitt",
                    source_name="Backyard Station",
                    area="Outside",
                ),
                _reading(
                    key="tempinf",
                    label="Indoor Temperature",
                    category="temperature",
                    value=72,
                    display="72 °F",
                    provider="ecowitt",
                    provider_label="Ecowitt",
                    source_name="Backyard Station",
                    area="Inside",
                ),
                _reading(
                    key="humidity",
                    label="Outdoor Humidity",
                    category="humidity",
                    value=20,
                    display="20%",
                    provider="ecowitt",
                    provider_label="Ecowitt",
                    source_name="Backyard Station",
                    area="Outside",
                ),
            ],
        }
        weather_api = {
            "provider": "weather_api",
            "model": "Configured WeatherAPI Location",
            "received_at": now,
            "sample_time": now,
            "readings": [
                _reading(
                    key="weather_api_condition",
                    label="Current Condition",
                    category="condition",
                    value="Partly Cloudy",
                    display="Partly Cloudy",
                    provider="weather_api",
                    provider_label="WeatherAPI.com",
                    source_name="Configured WeatherAPI Location",
                    area="Forecast",
                ),
                _reading(
                    key="tempf",
                    label="WeatherAPI Temperature",
                    category="temperature",
                    value=78,
                    display="78 °F",
                    provider="weather_api",
                    provider_label="WeatherAPI.com",
                    source_name="Configured WeatherAPI Location",
                    area="Forecast",
                ),
            ],
        }
        home_assistant = {
            "provider": "homeassistant",
            "model": "Home Assistant Sensors",
            "received_at": now,
            "sample_time": now,
            "readings": [
                _reading(
                    key="homeassistant_sensor_bedroom_temperature_state",
                    label="Bedroom Temperature",
                    category="temperature",
                    value=69,
                    display="69 °F",
                    provider="homeassistant",
                    provider_label="Home Assistant",
                    source_name="Bedroom Temperature",
                    area="Bedroom",
                )
            ],
        }
        provider_snapshots = {
            "ecowitt": ecowitt,
            "weather_api": weather_api,
            "homeassistant": home_assistant,
        }
        combined = self.core._combined_snapshot(provider_snapshots)
        return {
            "provider_snapshots": provider_snapshots,
            "snapshot": combined,
            "settings": {
                "current_live_source": "provider:ecowitt",
                "current_condition_source": "provider:weather_api",
                "forecast_provider": "weather_api",
            },
            "selected_sensors": [],
            "provider_status": [],
            "received_at": combined["received_at"],
            "stale": False,
        }

    def test_current_temperature_matches_the_ui_readings_source(self) -> None:
        result = self.core._environment_conditions_kernel(
            {"category": "temperature", "area": "outside"},
            payload=self._payload(),
        )
        self.assertTrue(result["ok"])
        self.assertEqual([row["value"] for row in result["readings"]], ["105 °F"])
        self.assertEqual(result["readings"][0]["provider"], "Ecowitt")
        self.assertEqual(result["current_sources"]["readings"]["selection"], "provider:ecowitt")
        self.assertNotIn("area", result["filters"])

    def test_ai_intent_maps_outdoor_named_place_and_room_requests(self) -> None:
        payload = self._payload()
        outdoor = asyncio.run(
            self.core._hydra_ai_normalize_environment_args(
                {"request": "What is the temperature outside?"},
                payload,
                llm_client=FakeLLM(_weather_intent()),
            )
        )
        self.assertEqual(outdoor["category"], "temperature")
        self.assertEqual(outdoor["area"], "outside")
        self.assertEqual(outdoor["location_scope"], "local_outdoor")

        named = asyncio.run(
            self.core._hydra_ai_normalize_environment_args(
                {"request": "What is the temperature in Chicago?"},
                payload,
                llm_client=FakeLLM(
                    _weather_intent(location_scope="named_place", location="Chicago")
                ),
            )
        )
        self.assertEqual(named["location"], "Chicago")
        self.assertEqual(named["weather_intent"]["location_scope"], "named_place")

        room = asyncio.run(
            self.core._hydra_ai_normalize_environment_args(
                {"request": "What is the bedroom temperature?"},
                payload,
                llm_client=FakeLLM(
                    _weather_intent(location_scope="local_area", area="Bedroom")
                ),
            )
        )
        self.assertEqual(room["area"], "Bedroom")
        self.assertEqual(room["location_scope"], "local_area")

    def test_environment_ai_is_required(self) -> None:
        with self.assertRaises(self.core.EnvironmentIntentError):
            asyncio.run(
                self.core._hydra_ai_normalize_environment_args(
                    {"request": "What is the temperature outside?"},
                    self._payload(),
                    llm_client=None,
                )
            )

    def test_weather_components_use_the_same_intent_schema_version(self) -> None:
        weather_module = load_weather_forecast()
        self.assertEqual(
            self.core.WEATHER_INTENT_SCHEMA_VERSION,
            weather_module.WEATHER_INTENT_SCHEMA_VERSION,
        )

    def test_named_place_uses_an_on_demand_weatherapi_snapshot(self) -> None:
        payload = self._payload()
        named_snapshot = dict(payload["provider_snapshots"]["weather_api"])
        named_snapshot["model"] = "Chicago, Illinois, United States"
        for row in named_snapshot["readings"]:
            row["source_name"] = named_snapshot["model"]
        calls: list[str] = []

        def fake_lookup(location, args, client):
            del args, client
            calls.append(location)
            return named_snapshot, ""

        old_lookup = self.core._weather_api_snapshot_for_location
        self.core._weather_api_snapshot_for_location = fake_lookup
        try:
            args = self.core._hydra_normalized_filter_args(
                _weather_intent(location_scope="named_place", location="Chicago"),
                {},
            )
            updated, error = self.core._hydra_payload_with_weatherapi_location(
                args,
                payload,
                None,
            )
        finally:
            self.core._weather_api_snapshot_for_location = old_lookup
        self.assertEqual(error, "")
        self.assertEqual(calls, ["Chicago"])
        result = self.core._environment_conditions_kernel(args, payload=updated)
        self.assertEqual([row["value"] for row in result["readings"]], ["78 °F"])
        self.assertEqual(result["readings"][0]["source"], "Chicago, Illinois, United States")

    def test_broad_current_weather_combines_configured_readings_and_artwork_sources(self) -> None:
        result = self.core._environment_conditions_kernel(
            {"category": "weather", "location": "home"},
            payload=self._payload(),
        )
        values = {row["value"] for row in result["readings"]}
        self.assertIn("105 °F", values)
        self.assertIn("Partly Cloudy", values)
        self.assertNotIn("78 °F", values)
        self.assertNotIn("72 °F", values)
        self.assertEqual(result["current_sources"]["conditions"]["selection"], "provider:weather_api")

    def test_explicit_provider_or_room_still_searches_all_sources(self) -> None:
        weather_api = self.core._environment_conditions_kernel(
            {"category": "temperature", "provider": "weather_api"},
            payload=self._payload(),
        )
        self.assertEqual([row["value"] for row in weather_api["readings"]], ["78 °F"])
        self.assertEqual(weather_api["current_sources"], {})

        bedroom = self.core._environment_conditions_kernel(
            {"category": "temperature", "area": "Bedroom"},
            payload=self._payload(),
        )
        self.assertEqual([row["value"] for row in bedroom["readings"]], ["69 °F"])
        self.assertEqual(bedroom["readings"][0]["provider"], "Home Assistant")

    def test_stale_configured_source_falls_back_with_truthful_provenance(self) -> None:
        payload = self._payload()
        payload["provider_snapshots"]["ecowitt"]["received_at"] -= 7200
        payload["provider_snapshots"]["ecowitt"]["sample_time"] -= 7200
        payload["snapshot"] = self.core._combined_snapshot(payload["provider_snapshots"])
        result = self.core._environment_conditions_kernel(
            {"category": "temperature", "area": "outside"},
            payload=payload,
        )
        self.assertEqual([row["value"] for row in result["readings"]], ["78 °F"])
        source = result["current_sources"]["readings"]
        self.assertEqual(source["selection"], "provider:ecowitt")
        self.assertEqual(source["resolved_selection"], "provider:weather_api")
        self.assertTrue(source["fallback_used"])
        self.assertFalse(result["stale"])

    def test_missing_configured_source_reports_the_resolved_fallback(self) -> None:
        payload = self._payload()
        payload["provider_snapshots"].pop("ecowitt")
        payload["snapshot"] = self.core._combined_snapshot(payload["provider_snapshots"])
        result = self.core._environment_conditions_kernel(
            {"category": "temperature", "area": "outside"},
            payload=payload,
        )
        source = result["current_sources"]["readings"]
        self.assertEqual(source["resolved_selection"], "provider:weather_api")
        self.assertEqual(source["label"], "Configured WeatherAPI Location")
        self.assertTrue(source["fallback_used"])

    def test_kernel_description_explains_current_card_source_behavior(self) -> None:
        tool = next(
            row
            for row in self.core.get_hydra_kernel_tools()
            if row["id"] == "environment_conditions"
        )
        self.assertIn("Primary tool", tool["description"])
        self.assertIn("AI-interpret", tool["description"])
        self.assertIn("named city or geographic place", tool["description"])
        self.assertIn("Pass the user's complete request unchanged", tool["description"])
        self.assertNotIn("Temperature Status", tool["description"])

    def test_temperature_status_description_is_limited_to_named_local_targets(self) -> None:
        source = (SHOP_ROOT / "verba" / "temperature_status.py").read_text(encoding="utf-8")
        self.assertIn("Use it only when the user names that local target", source)
        self.assertIn("Do not use it for outside, home weather, general weather, forecasts", source)
        self.assertNotIn("Route outside weather", source)


class EnvironmentTrendTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.core = load_environment_core()

    def _snapshot(
        self,
        timestamp: float,
        value: object,
        *,
        key: str = "tempf",
        provider: str = "ecowitt",
        source_id: str = "ecowitt:backyard",
        category: str = "temperature",
        unit: str = "F",
    ) -> dict[str, object]:
        return {
            "provider": provider,
            "source_id": source_id,
            "model": "Backyard Station" if provider == "ecowitt" else "WeatherAPI Location",
            "sample_time": timestamp,
            "received_at": timestamp,
            "readings": [
                {
                    "key": key,
                    "label": key,
                    "category": category,
                    "unit": unit,
                    "value": value,
                    "display": f"{value} {unit}",
                    "provider": provider,
                    "source_id": source_id,
                    "source_name": "Backyard Station" if provider == "ecowitt" else "WeatherAPI Location",
                }
            ],
        }

    def test_temperature_trend_keeps_one_source_and_ignores_false_dips(self) -> None:
        history = [
            self._snapshot(6, 72),
            self._snapshot(5, 39, provider="weather_api", source_id="weather_api:home"),
            self._snapshot(4, "unavailable"),
            self._snapshot(3, 0),
            self._snapshot(2, 71),
            self._snapshot(1, 70),
        ]
        points = self.core._trend_points(history, "tempf", provider="ecowitt")
        self.assertEqual([point["value"] for point in points], [70.0, 71.0, 72.0])
        self.assertEqual({point["provider"] for point in points}, {"ecowitt"})

    def test_sustained_temperature_change_is_not_smoothed_away(self) -> None:
        history = [
            self._snapshot(4, 40),
            self._snapshot(3, 41),
            self._snapshot(2, 42),
            self._snapshot(1, 70),
        ]
        points = self.core._trend_points(history, "tempf", provider="ecowitt")
        self.assertEqual([point["value"] for point in points], [70.0, 42.0, 41.0, 40.0])

    def test_real_zero_wind_reading_is_preserved(self) -> None:
        history = [
            self._snapshot(3, 6, key="windspeedmph", category="wind", unit="mph"),
            self._snapshot(2, 0, key="windspeedmph", category="wind", unit="mph"),
            self._snapshot(1, 5, key="windspeedmph", category="wind", unit="mph"),
        ]
        points = self.core._trend_points(history, "windspeedmph", provider="ecowitt")
        self.assertEqual([point["value"] for point in points], [5.0, 0.0, 6.0])

    def test_chart_artwork_is_tater_themed_and_uses_a_smooth_path(self) -> None:
        points = self.core._trend_points(
            [self._snapshot(3, 72), self._snapshot(2, 71), self._snapshot(1, 70)],
            "tempf",
            provider="ecowitt",
        )
        uri = self.core._line_chart_data_uri("Outdoor Temperature", points, unit="F")
        svg = unquote(uri.split(",", 1)[1])
        self.assertIn("Tater trend chart: Outdoor Temperature", svg)
        self.assertIn("#17130f", svg)
        self.assertIn(" C ", svg)

    def test_trend_caption_explains_that_gaps_are_ignored(self) -> None:
        field = self.core._trend_image_field(
            [self._snapshot(1, 70)],
            "tempf",
            "Outdoor Temperature",
            "Recent temperature.",
            color_token="temperature",
        )
        self.assertIn("actual reading", field["caption"])
        self.assertIn("gaps and bad samples ignored", field["caption"])
        self.assertIn("Backyard Station", field["caption"])


if __name__ == "__main__":
    unittest.main()
