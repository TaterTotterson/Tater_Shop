# verba/weather_forecast.py
import asyncio
import json
import logging
from datetime import datetime, date
from typing import Any, Dict, Optional, Tuple, List

from dotenv import load_dotenv

from helpers import llm_structured_chat
from verba_base import ToolVerba
from tateros import integration_store as integration_store_module
from verba_diagnostics import needs_from_diagnosis
from verba_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("weather_forecast")
logger.setLevel(logging.INFO)

_DEFAULT_LOCATION_ALIASES = {
    "default",
    "default location",
    "the default",
    "use default",
    "my default",
    "home",
    "my home",
    "our home",
    "at home",
    "outside",
    "outdoors",
    "out there",
    "here",
    "near me",
    "around me",
    "local",
    "locally",
    "local area",
    "my location",
    "our location",
    "current location",
    "this location",
    "my area",
    "our area",
    "this area",
    "where i am",
    "where we are",
}

WEATHER_INTENT_SCHEMA_VERSION = 1
_WEATHER_REQUEST_TYPES = {"current", "forecast"}
_WEATHER_MEASUREMENTS = {
    "general",
    "condition",
    "temperature",
    "humidity",
    "wind",
    "rain",
    "snow",
    "air_quality",
    "pollen",
    "alerts",
}
_WEATHER_LOCATION_SCOPES = {"local_outdoor", "local_area", "named_place"}
_WEATHER_DAY_HINTS = {"current", "today", "tonight", "tomorrow", "weekend"}
_WEATHER_TEMPERATURE_FOCUS = {"current", "high", "low", "both"}


class WeatherIntentError(ValueError):
    """Raised when the required AI weather interpretation cannot be produced safely."""


def _weather_api_module():
    return integration_store_module.integration_module("weather_api")


def read_weatherapi_settings(*args, **kwargs) -> Dict[str, Any]:
    module = _weather_api_module()
    if module is not None:
        return module.read_weatherapi_settings(*args, **kwargs)
    return {
        "WEATHERAPI_KEY": "",
        "DEFAULT_LOCATION": "60614",
        "DEFAULT_DAYS": 3,
        "DEFAULT_UNITS": "us",
        "INCLUDE_AQI": True,
        "INCLUDE_POLLEN": True,
        "INCLUDE_ALERTS": True,
        "SHOW_HOURLY_PEEK": 6,
        "MAX_RESPONSE_CHARS": 650,
        "TIMEOUT_SECONDS": 12,
    }


def fetch_weatherapi_forecast(*args, **kwargs):
    module = _weather_api_module()
    if module is None:
        return None, "WeatherAPI is not configured. Enable the WeatherAPI.com integration in Settings > Integrations."
    return module.fetch_weatherapi_forecast(*args, **kwargs)


class WeatherForecastPlugin(ToolVerba):
    """
    Weather Forecast (WeatherAPI.com)

    Tool-call schema (explicit):
      - LLM passes structured arguments (location/days/date/hours/units).
      - Plugin calls WeatherAPI directly with those explicit args.
      - Then it calls the LLM again to answer ONLY what the user asked,
        using a compact facts block (prevents data dumps).

    Supports:
      - Current conditions
      - Forecast (daily + optional hourly peek)
      - Alerts, AQI, pollen (optional, plan permitting)
    """

    name = "weather_forecast"
    verba_name = "Weather Forecast"
    version = "1.2.2"
    min_tater_version = "59"
    routing_keywords = [
        "weatherapi",
        "weather api",
        "weather forecast",
        "forecast",
        "weather alerts",
        "air quality forecast",
        "pollen forecast",
    ]
    description = "Get current weather conditions and forecasts from WeatherAPI.com for the configured home location or any named place."
    verba_dec = "Get WeatherAPI.com conditions and forecasts for the configured home location or any named geographic place."
    when_to_use = "Use for current or future weather at home, outside, here, or near me, and for weather in a named city or geographic place. Handles conditions, temperature, rain, snow, wind, humidity, hourly and daily forecasts, alerts, air quality, UV, and pollen. Every request is interpreted by AI with no regex fallback."
    common_needs = ["weather request (e.g., current, tonight, tomorrow, multi-day)"]
    missing_info_prompts = [
        "What weather do you want (current conditions, tonight, tomorrow, or multi-day forecast)?",
    ]
    pretty_name = "Checking the Weather"
    settings_category = None

    usage = '{"function":"weather_forecast","arguments":{"request":"What is the temperature in Chicago right now?"}}'

    required_settings = {}

    waiting_prompt_template = (
        "Write one short, natural status update to {mention} that you are checking weather now. "
        "Use fresh wording (avoid repeating stock phrases). "
        "Do not use markdown. Only output the message."
    )

    platforms = ['discord', 'webui', 'little_spud', 'macos', 'irc', 'meshtastic', 'voice_core', 'homeassistant', 'matrix', 'homekit', 'xbmc', 'telegram']

    def _normalize_request_text(self, text: str) -> str:
        if not text:
            return ""
        return " ".join(str(text).strip().split())

    def _with_request_from_fallback(self, args: Dict[str, Any], fallback_text: str) -> Tuple[Dict[str, Any], str]:
        args2 = dict(args or {})
        req = (args2.get("request") or "").strip()
        if not req and fallback_text:
            req = str(fallback_text).strip()
        req = self._normalize_request_text(req)
        if req:
            args2["request"] = req
        return args2, req

    def _diagnosis(self) -> dict:
        settings = read_weatherapi_settings()
        api_key = str(settings.get("WEATHERAPI_KEY") or "").strip()
        location = self._normalize_location_value(settings.get("DEFAULT_LOCATION"))
        return {
            "weatherapi_key": "set" if len(api_key) >= 10 else ("missing" if not api_key else "invalid"),
            "default_location": "set" if len(location) >= 3 else ("missing" if not location else "invalid"),
        }

    def _to_contract(self, message: str, request_text: str) -> dict:
        msg = (message or "").strip()
        low = msg.lower()
        if not msg:
            return action_failure(
                code="empty_result",
                message="Weather tool returned no output.",
                diagnosis=self._diagnosis(),
                needs=["What forecast do you want (current, tonight, tomorrow, multi-day)?"],
                say_hint="Explain no output was returned and ask which forecast details the user wants; use the default location if configured.",
            )

        if "not configured" in low:
            diagnosis = self._diagnosis()
            needs = needs_from_diagnosis(
                diagnosis,
                {
                    "weatherapi_key": "Please set your WeatherAPI.com API key in settings.",
                    "default_location": "Please set a default location (city or ZIP) in settings.",
                },
            )
            return action_failure(
                code="weather_config_missing",
                message="WeatherAPI configuration is incomplete.",
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain that weather settings are missing and ask for the API key and default location.",
            )

        if "no weather request provided" in low:
            return action_failure(
                code="missing_request",
                message="No weather request was provided.",
                diagnosis=self._diagnosis(),
                needs=["What forecast do you want (current, tonight, tomorrow, multi-day)?"],
                say_hint="Ask which forecast details the user wants; default location will be used if set.",
            )

        if "weather request interpretation failed" in low:
            return action_failure(
                code="weather_interpretation_failed",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Please try the weather request again."],
                say_hint="Explain that the weather request could not be interpreted safely and ask the user to try again.",
            )

        if "no location provided" in low:
            return action_failure(
                code="missing_location",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Which city or ZIP should I use?"],
                say_hint="Explain no location is configured and ask for a city or ZIP.",
            )

        if "requested date is in the past" in low or "too far out" in low:
            return action_failure(
                code="invalid_date",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Please provide a date within the next 14 days."],
                say_hint="Explain the requested date is out of range and ask for a valid date.",
            )

        if "no weather data returned" in low or "weather failed" in low:
            return action_failure(
                code="weather_failed",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Try again or specify a different location."],
                say_hint="Explain the weather lookup failed and ask whether to retry.",
            )

        if "weatherapi error" in low:
            return action_failure(
                code="weatherapi_error",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Try a different location (city or ZIP)."],
                say_hint="Explain the weather provider returned an error and ask for a different location.",
            )

        return action_success(
            facts={"request": request_text, "result": msg},
            summary_for_user=msg,
            suggested_followups=["Want the forecast for another day or location?"],
        )

    # -------------------- Settings helpers --------------------

    def _get_settings(self) -> Dict[str, Any]:
        settings = read_weatherapi_settings()
        settings["DEFAULT_LOCATION"] = self._normalize_location_value(settings.get("DEFAULT_LOCATION"))
        return settings

    # -------------------- Utility helpers --------------------

    @staticmethod
    def _normalize_location_value(value: Any) -> str:
        text = " ".join(str(value or "").strip().split())
        if not text:
            return ""
        text = text.rstrip("?!. ")
        lowered = text.casefold()
        if lowered in _DEFAULT_LOCATION_ALIASES:
            return ""
        return text

    @staticmethod
    def _siri_flatten(text: Optional[str]) -> str:
        if not text:
            return "No weather available."
        out = str(text)
        for token in ("`", "*", "_"):
            out = out.replace(token, "")
        out = " ".join(out.split())
        return out[:450]

    @staticmethod
    def _safe_get(d: Dict[str, Any], *path, default=None):
        cur = d
        for p in path:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur

    @staticmethod
    def _parse_date(datestr: Optional[str]) -> Optional[date]:
        if not datestr:
            return None
        try:
            return datetime.strptime(datestr.strip(), "%Y-%m-%d").date()
        except Exception:
            return None

    @staticmethod
    def _us_epa_label(idx: Optional[int]) -> Optional[str]:
        if idx is None:
            return None
        try:
            idx = int(idx)
        except Exception:
            return None
        mapping = {
            1: "Good",
            2: "Moderate",
            3: "Unhealthy (Sensitive Groups)",
            4: "Unhealthy",
            5: "Very Unhealthy",
            6: "Hazardous",
        }
        return mapping.get(idx)

    # -------------------- AI weather intent --------------------

    @staticmethod
    def _decode_ai_object(content: Any) -> Dict[str, Any]:
        text = str(content or "").strip()
        if text.startswith("```") and text.endswith("```"):
            lines = text.splitlines()
            if len(lines) >= 3:
                text = "\n".join(lines[1:-1]).strip()
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise WeatherIntentError("The weather interpretation returned invalid JSON.") from exc
        if not isinstance(parsed, dict):
            raise WeatherIntentError("The weather interpretation did not return an object.")
        return parsed

    @staticmethod
    def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            number = int(value)
        except Exception:
            number = int(default)
        return max(minimum, min(number, maximum))

    async def _interpret_weather_request_ai(
        self,
        *,
        request_text: str,
        args: Dict[str, Any],
        settings: Dict[str, Any],
        llm_client: Any,
    ) -> Dict[str, Any]:
        if llm_client is None:
            raise WeatherIntentError("AI weather interpretation is unavailable.")
        if not request_text and not any(
            args.get(key) not in (None, "")
            for key in ("location", "area", "sensor", "provider", "days", "date", "hours", "units")
        ):
            raise WeatherIntentError("No weather request was provided.")

        today = date.today().isoformat()
        timezone_name = datetime.now().astimezone().tzname() or "local timezone"
        prompt_payload = {
            "weather_intent_schema_version": WEATHER_INTENT_SCHEMA_VERSION,
            "request": request_text,
            "explicit_arguments": {
                key: args.get(key)
                for key in ("location", "area", "sensor", "provider", "days", "date", "hours", "units")
                if args.get(key) not in (None, "")
            },
            "configured_default_location": self._normalize_location_value(settings.get("DEFAULT_LOCATION")),
            "configured_default_days": settings.get("DEFAULT_DAYS"),
            "configured_default_units": settings.get("DEFAULT_UNITS"),
            "today": today,
            "timezone": timezone_name,
        }
        system_prompt = (
            "Convert the user's weather request into one strict JSON object. Do not answer the weather question. "
            "Ignore chat mentions, user names, assistant names, and punctuation when deciding the location. "
            "Never infer or invent a named place. A named place is allowed only when the user or explicit arguments clearly provide "
            "a city, region, country, postal code, airport, landmark, or latitude/longitude. "
            "Words such as home, here, outside, outdoors, local, near me, my location, and where I am always mean location_scope=local_outdoor "
            "and location=null. A room, named local area, or sensor means location_scope=local_area. Preserve explicit arguments unless they conflict with those rules. "
            "Return exactly these keys: request_type, measurement, location_scope, location, area, sensor, provider, date, day, days, hours, units, temperature_focus. "
            "request_type is current or forecast. measurement is general, condition, temperature, humidity, wind, rain, snow, "
            "air_quality, pollen, or alerts. location_scope is local_outdoor, local_area, or named_place. location is a string only for named_place. "
            "date is YYYY-MM-DD or null. Resolve relative dates using today. day is current, today, tonight, tomorrow, weekend, or null. "
            "days is an integer from 1 to 14. hours is an integer from 0 to 48. units is us, metric, or null. "
            "temperature_focus is current, high, low, both, or null. Use forecast for future times, highs, lows, tonight, tomorrow, or later. "
            "Output JSON only."
        )
        try:
            response = await llm_structured_chat(
                llm_client,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False)},
                ],
                native_system_prompt=(
                    "Interpret the supplied weather request without answering it. Ignore mentions, names, and punctuation "
                    "when identifying location. Never invent a named place: use named_place only for an explicitly supplied "
                    "city, region, country, postal code, airport, landmark, or coordinates; home, here, outside, local, near "
                    "me, and my location mean local_outdoor with no named location; and rooms, named local areas, or sensors "
                    "mean local_area. Preserve explicit arguments unless they conflict with these rules. Resolve relative "
                    "dates from supplied today, and use forecast for future times, highs, lows, tonight, tomorrow, or later."
                ),
                schema={
                    "type": "object",
                    "properties": {
                        "request_type": {"type": "string", "enum": ["current", "forecast"]},
                        "measurement": {"type": "string", "enum": ["general", "condition", "temperature", "humidity", "wind", "rain", "snow", "air_quality", "pollen", "alerts"]},
                        "location_scope": {"type": "string", "enum": ["local_outdoor", "local_area", "named_place"]},
                        "location": {"type": ["string", "null"]},
                        "area": {"type": ["string", "null"]},
                        "sensor": {"type": ["string", "null"]},
                        "provider": {"type": ["string", "null"]},
                        "date": {"type": ["string", "null"]},
                        "day": {"type": ["string", "null"], "enum": ["current", "today", "tonight", "tomorrow", "weekend", None]},
                        "days": {"type": "integer", "minimum": 1, "maximum": 14},
                        "hours": {"type": "integer", "minimum": 0, "maximum": 48},
                        "units": {"type": ["string", "null"], "enum": ["us", "metric", None]},
                        "temperature_focus": {"type": ["string", "null"], "enum": ["current", "high", "low", "both", None]},
                    },
                    "required": ["request_type", "measurement", "location_scope", "location", "area", "sensor", "provider", "date", "day", "days", "hours", "units", "temperature_focus"],
                    "additionalProperties": False,
                },
                function_name="submit_weather_intent",
                temperature=0.0,
                max_tokens=360,
                timeout_ms=25_000,
            )
        except Exception as exc:
            raise WeatherIntentError("The AI weather interpretation failed.") from exc

        raw = self._decode_ai_object((response.get("message") or {}).get("content"))
        request_type = str(raw.get("request_type") or "").strip().lower()
        measurement = str(raw.get("measurement") or "").strip().lower()
        location_scope = str(raw.get("location_scope") or "").strip().lower()
        day_hint = str(raw.get("day") or "").strip().lower() or None
        units = str(raw.get("units") or "").strip().lower() or None
        temperature_focus = str(raw.get("temperature_focus") or "").strip().lower() or None
        location = self._normalize_location_value(raw.get("location"))
        date_text = str(raw.get("date") or "").strip() or None

        if request_type not in _WEATHER_REQUEST_TYPES:
            raise WeatherIntentError("The AI weather interpretation returned an invalid request type.")
        if measurement not in _WEATHER_MEASUREMENTS:
            raise WeatherIntentError("The AI weather interpretation returned an invalid measurement.")
        if location_scope not in _WEATHER_LOCATION_SCOPES:
            raise WeatherIntentError("The AI weather interpretation returned an invalid location scope.")
        if day_hint not in _WEATHER_DAY_HINTS:
            day_hint = None
        if units not in {"us", "metric"}:
            units = None
        if temperature_focus not in _WEATHER_TEMPERATURE_FOCUS:
            temperature_focus = None
        if date_text and self._parse_date(date_text) is None:
            raise WeatherIntentError("The AI weather interpretation returned an invalid date.")

        explicit_location_present = args.get("location") not in (None, "")
        explicit_location = self._normalize_location_value(args.get("location"))
        if explicit_location_present:
            if explicit_location:
                location_scope = "named_place"
                location = explicit_location
            else:
                location_scope = "local_outdoor"
                location = ""
        elif location_scope == "local_outdoor":
            location = ""
        elif location_scope == "local_area":
            raise WeatherIntentError("Local room and sensor weather requests require Environment Core.")
        elif not location:
            raise WeatherIntentError("The AI weather interpretation selected a named place without a location.")

        if args.get("date") not in (None, ""):
            explicit_date = str(args.get("date") or "").strip()
            if self._parse_date(explicit_date) is None:
                raise WeatherIntentError("The explicit weather date is invalid.")
            date_text = explicit_date
        if args.get("units") not in (None, ""):
            explicit_units = str(args.get("units") or "").strip().lower()
            if explicit_units not in {"us", "metric"}:
                raise WeatherIntentError("The explicit weather units must be us or metric.")
            units = explicit_units

        days = self._bounded_int(
            args.get("days") if args.get("days") not in (None, "") else raw.get("days"),
            settings.get("DEFAULT_DAYS") or 3,
            1,
            14,
        )
        hours = self._bounded_int(
            args.get("hours") if args.get("hours") not in (None, "") else raw.get("hours"),
            settings.get("SHOW_HOURLY_PEEK") or 0,
            0,
            48,
        )
        return {
            "schema_version": WEATHER_INTENT_SCHEMA_VERSION,
            "request_type": request_type,
            "measurement": measurement,
            "location_scope": location_scope,
            "location": location or None,
            "area": str(raw.get("area") or "").strip() or None,
            "sensor": str(raw.get("sensor") or "").strip() or None,
            "provider": str(raw.get("provider") or "").strip() or None,
            "date": date_text,
            "day": day_hint,
            "days": days,
            "hours": hours,
            "units": units or str(settings.get("DEFAULT_UNITS") or "us").lower(),
            "temperature_focus": temperature_focus,
        }

    # -------------------- Facts builder (for LLM) --------------------

    def _build_facts_block(
        self,
        data: Dict[str, Any],
        units: str,
        want_days: int,
        wanted_date: Optional[date],
        hourly_peek_hours: int,
        used_default_location: bool
    ) -> str:
        loc = data.get("location", {}) or {}
        cur = data.get("current", {}) or {}
        forecast = data.get("forecast", {}) or {}
        alerts = data.get("alerts", {}) or {}

        name = loc.get("name") or "Unknown location"
        region = loc.get("region") or ""
        country = loc.get("country") or ""
        loc_line = ", ".join([p for p in [name, region, country] if p])
        if used_default_location:
            loc_line += " (default location)"

        temp_unit = "°F" if units == "us" else "°C"

        facts: List[str] = []
        facts.append(f"Location: {loc_line}")

        cond = (cur.get("condition") or {}).get("text") or "Unknown"
        temp = cur.get("temp_f") if units == "us" else cur.get("temp_c")
        feels = cur.get("feelslike_f") if units == "us" else cur.get("feelslike_c")
        wind = cur.get("wind_mph") if units == "us" else cur.get("wind_kph")
        wind_unit = "mph" if units == "us" else "kph"
        humidity = cur.get("humidity")
        uv = cur.get("uv")

        facts.append(
            f"Current: {cond}, {temp}{temp_unit} (feels {feels}{temp_unit}), wind {wind} {wind_unit}, "
            f"humidity {humidity}%, UV {uv}."
        )

        # AQI (compact)
        aq = self._safe_get(cur, "air_quality", default=None)
        if isinstance(aq, dict):
            epa = aq.get("us-epa-index")
            epa_label = self._us_epa_label(epa)
            if epa_label:
                facts.append(f"AQI: US EPA {epa} ({epa_label}). PM2.5 {aq.get('pm2_5')} µg/m³.")

        # Pollen (compact)
        pollen = self._safe_get(cur, "pollen", default=None)
        if isinstance(pollen, dict) and pollen:
            keep = []
            for k in ("grass", "ragweed", "mugwort", "oak", "birch"):
                v = pollen.get(k)
                if v is not None:
                    keep.append(f"{k} {v}")
            if keep:
                facts.append("Pollen: " + ", ".join(keep) + ".")

        # Forecast (compact)
        fdays = (forecast.get("forecastday") or []) if isinstance(forecast, dict) else []
        if fdays:
            if wanted_date:
                fd = next((d for d in fdays if d.get("date") == wanted_date.strftime("%Y-%m-%d")), None)
                if fd:
                    facts.append("Forecast (requested date): " + self._format_one_day_plain(fd, units))
                else:
                    facts.append(f"Forecast note: requested date {wanted_date} not in returned window.")
            else:
                shown = fdays[: max(1, want_days)]
                facts.append("Forecast:")
                for fd in shown:
                    facts.append("- " + self._format_one_day_plain(fd, units))

            # Hourly peek (compact)
            if hourly_peek_hours and fdays:
                hours = (fdays[0].get("hour") or [])
                if hours:
                    now_epoch = cur.get("last_updated_epoch") or 0
                    upcoming = [h for h in hours if (h.get("time_epoch") or 0) >= now_epoch]
                    peek = upcoming[:hourly_peek_hours]
                    if peek:
                        bits = []
                        for h in peek:
                            t = (h.get("time") or "")[-5:]
                            ht = h.get("temp_f") if units == "us" else h.get("temp_c")
                            hc = (h.get("condition") or {}).get("text") or ""
                            pop = h.get("chance_of_rain")
                            bits.append(f"{t} {ht}{temp_unit} {hc} ({pop}% rain)")
                        facts.append("Next hours: " + " | ".join(bits))

        # Alerts (titles only)
        alert_list = alerts.get("alert") if isinstance(alerts, dict) else None
        if isinstance(alert_list, list) and alert_list:
            facts.append("Alerts:")
            for a in alert_list[:6]:
                headline = a.get("headline") or a.get("event") or "Alert"
                severity = a.get("severity") or ""
                expires = a.get("expires") or ""
                bits = headline
                if severity:
                    bits += f" — {severity}"
                if expires:
                    bits += f" (expires {expires})"
                facts.append(f"- {bits}")

        return "\n".join(facts)

    @staticmethod
    def _format_one_day_plain(fd: Dict[str, Any], units: str) -> str:
        d = fd.get("day") or {}
        date_str = fd.get("date") or "Unknown date"
        cond = (d.get("condition") or {}).get("text") or "Unknown"
        hi = d.get("maxtemp_f") if units == "us" else d.get("maxtemp_c")
        lo = d.get("mintemp_f") if units == "us" else d.get("mintemp_c")
        temp_unit = "°F" if units == "us" else "°C"
        rain_chance = d.get("daily_chance_of_rain")
        snow_chance = d.get("daily_chance_of_snow")
        maxwind = d.get("maxwind_mph") if units == "us" else d.get("maxwind_kph")
        wind_unit = "mph" if units == "us" else "kph"
        return (
            f"{date_str}: {cond}. High {hi}{temp_unit} / Low {lo}{temp_unit}. "
            f"Wind up to {maxwind} {wind_unit}. Rain {rain_chance}% (snow {snow_chance}%)."
        )

    # -------------------- Deterministic answer builder --------------------

    def _deterministic_answer(
        self,
        *,
        data: Dict[str, Any],
        intent: Dict[str, Any],
        units: str,
        wanted_date: Optional[date],
        days: int,
        max_chars: int,
    ) -> str:
        temp_unit = "°F" if units == "us" else "°C"
        wind_unit = "mph" if units == "us" else "kph"
        request_type = str(intent.get("request_type") or "current")
        measurement = str(intent.get("measurement") or "general")
        day_hint = str(intent.get("day") or "")
        temp_focus = str(intent.get("temperature_focus") or "current")

        loc = data.get("location") or {}
        loc_name = ", ".join(
            [str(loc.get("name") or "").strip(), str(loc.get("region") or "").strip(), str(loc.get("country") or "").strip()]
        )
        loc_name = ", ".join([part for part in loc_name.split(", ") if part])

        cur = data.get("current") or {}
        cond = ((cur.get("condition") or {}).get("text") or "Unknown").strip()
        temp = cur.get("temp_f") if units == "us" else cur.get("temp_c")
        feels = cur.get("feelslike_f") if units == "us" else cur.get("feelslike_c")
        wind = cur.get("wind_mph") if units == "us" else cur.get("wind_kph")
        humidity = cur.get("humidity")

        current_line = (
            f"Current in {loc_name}: {cond}, {temp}{temp_unit}, feels like {feels}{temp_unit}, "
            f"wind {wind} {wind_unit}, humidity {humidity}%."
        ).strip()
        current_temp_line = f"Current temperature in {loc_name} is {temp}{temp_unit}.".strip()

        forecast_days = ((data.get("forecast") or {}).get("forecastday") or [])

        target_day = None
        target_label = ""
        if wanted_date:
            target_day = next(
                (fd for fd in forecast_days if str(fd.get("date") or "").strip() == wanted_date.strftime("%Y-%m-%d")),
                None,
            )
            target_label = wanted_date.isoformat()
        elif day_hint == "tomorrow" and len(forecast_days) >= 2:
            target_day = forecast_days[1]
            target_label = "tomorrow"
        elif day_hint in {"today", "tonight"} and len(forecast_days) >= 1:
            target_day = forecast_days[0]
            target_label = day_hint
        elif request_type == "forecast" and len(forecast_days) >= 1:
            target_day = forecast_days[0]
            target_label = "today"

        if measurement == "temperature" and request_type == "forecast" and isinstance(target_day, dict):
            day_data = target_day.get("day") or {}
            hi = day_data.get("maxtemp_f") if units == "us" else day_data.get("maxtemp_c")
            lo = day_data.get("mintemp_f") if units == "us" else day_data.get("mintemp_c")
            if temp_focus == "high" and hi is not None:
                answer = f"The high for {target_label} in {loc_name} is {hi}{temp_unit}."
                return " ".join(answer.split())[:max_chars]
            if temp_focus == "low" and lo is not None:
                answer = f"The low for {target_label} in {loc_name} is {lo}{temp_unit}."
                return " ".join(answer.split())[:max_chars]
            if temp_focus == "both" and hi is not None and lo is not None:
                answer = f"For {target_label} in {loc_name}, the high is {hi}{temp_unit} and the low is {lo}{temp_unit}."
                return " ".join(answer.split())[:max_chars]

        if request_type == "current":
            if measurement == "temperature":
                answer = current_temp_line
            elif measurement == "humidity":
                answer = f"Current humidity in {loc_name} is {humidity}%."
            elif measurement == "wind":
                answer = f"Current wind in {loc_name} is {wind} {wind_unit}."
            elif measurement == "condition":
                answer = f"Current conditions in {loc_name}: {cond}."
            else:
                answer = current_line
            return " ".join(answer.split())[:max_chars]

        day_lines: List[str] = []
        for fd in forecast_days[: max(1, min(days, 3))]:
            day_lines.append(self._format_one_day_plain(fd, units))

        if wanted_date:
            match = next(
                (fd for fd in forecast_days if str(fd.get("date") or "").strip() == wanted_date.strftime("%Y-%m-%d")),
                None,
            )
            if match:
                answer = f"Forecast for {wanted_date.isoformat()} in {loc_name}: {self._format_one_day_plain(match, units)}"
            else:
                answer = f"I could not find forecast data for {wanted_date.isoformat()} in {loc_name}."
        elif day_hint in {"today", "tonight"} and len(forecast_days) >= 1:
            answer = f"Today in {loc_name}: {self._format_one_day_plain(forecast_days[0], units)}"
        elif day_hint == "tomorrow" and len(forecast_days) >= 2:
            answer = f"Tomorrow in {loc_name}: {self._format_one_day_plain(forecast_days[1], units)}"
        elif day_lines:
            answer = f"Forecast in {loc_name}: " + " ".join(day_lines[: max(1, min(days, 2))])
        else:
            answer = f"I could not find forecast data for {loc_name}."

        alert_list = ((data.get("alerts") or {}).get("alert") or [])
        if measurement == "alerts" and isinstance(alert_list, list) and alert_list:
            headline = str((alert_list[0] or {}).get("headline") or (alert_list[0] or {}).get("event") or "").strip()
            if headline:
                answer += f" Alert: {headline}."

        return " ".join(answer.split())[:max_chars]

    async def _llm_guided_answer(
        self,
        *,
        llm_client: Any,
        request_text: str,
        intent: Dict[str, Any],
        facts_block: str,
        max_chars: int,
        fallback_text: str,
    ) -> str:
        fallback = str(fallback_text or "").strip()
        request = str(request_text or "").strip()
        facts = str(facts_block or "").strip()
        if llm_client is None or not request or not facts:
            return fallback

        prompt_payload = {
            "user_request": request,
            "interpreted_weather_intent": intent,
            "weather_facts": facts,
            "max_chars": max_chars,
        }
        messages = [
            {
                "role": "system",
                "content": (
                    "Answer weather questions using ONLY the provided weather_facts.\n"
                    "Return only what the user requested.\n"
                    "Rules:\n"
                    "- Treat interpreted_weather_intent as authoritative for scope, location, time, and measurement.\n"
                    "- If user asks for today's high/low, return only that value.\n"
                    "- Do not include extra forecast days unless explicitly requested.\n"
                    "- Keep answer concise (normally one sentence).\n"
                    "- No markdown, no bullets, no JSON, no tool calls."
                ),
            },
            {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False)},
        ]
        try:
            response = await llm_client.chat(
                messages=messages,
                max_tokens=max(80, min(260, int(max_chars // 2) + 80)),
                temperature=0.0,
            )
            answer = str((response.get("message", {}) or {}).get("content", "")).strip()
        except Exception:
            return fallback

        if not answer:
            return fallback

        if "```" in answer:
            answer = answer.replace("```", " ").strip()

        prefix, separator, remainder = answer.partition(":")
        normalized_prefix = " ".join(prefix.replace("_", " ").replace("-", " ").upper().split())
        if separator and normalized_prefix in {"FINAL ANSWER", "NEED USER INFO"}:
            answer = remainder.strip()
        elif separator and normalized_prefix == "RETRY TOOL":
            return fallback

        if not answer:
            return fallback
        if answer.startswith("{") and ("function" in answer and "arguments" in answer):
            return fallback

        answer = " ".join(answer.split())
        if not answer:
            return fallback
        return answer[: max(60, int(max_chars or 650))]

    # -------------------- Core execution --------------------

    async def _get_weather_text(self, args: Dict[str, Any], llm_client) -> str:
        settings = self._get_settings()
        api_key = settings["WEATHERAPI_KEY"]
        if not api_key:
            return "Weather is not configured. Please set your WeatherAPI.com API key in Settings > Integrations > WeatherAPI.com."

        args = args or {}
        request_text = (args.get("request") or "").strip()
        try:
            intent = await self._interpret_weather_request_ai(
                request_text=request_text,
                args=args,
                settings=settings,
                llm_client=llm_client,
            )
        except WeatherIntentError as exc:
            return f"Weather request interpretation failed: {exc}"

        location = self._normalize_location_value(intent.get("location"))
        used_default_location = intent.get("location_scope") == "local_outdoor"
        if used_default_location:
            location = self._normalize_location_value(settings["DEFAULT_LOCATION"])

        if not location:
            return "No location provided and no default location is configured."

        days = self._bounded_int(intent.get("days"), settings["DEFAULT_DAYS"], 1, 14)
        hours = self._bounded_int(intent.get("hours"), settings["SHOW_HOURLY_PEEK"], 0, 48)
        units = str(intent.get("units") or settings["DEFAULT_UNITS"]).lower()
        date_str = str(intent.get("date") or "").strip() or None
        wanted_date = self._parse_date(date_str) if date_str else None
        if wanted_date:
            today = date.today()
            diff = (wanted_date - today).days
            if diff < 0:
                return "Requested date is in the past. Please provide a future date."
            if diff > 13:
                return "Requested date is too far out. WeatherAPI supports up to 14 days."
            days = max(days, min(14, diff + 1))
        max_chars = settings["MAX_RESPONSE_CHARS"]

        timeout = settings["TIMEOUT_SECONDS"]
        data, err = await asyncio.to_thread(
            fetch_weatherapi_forecast,
            location=location,
            days=days,
            include_aqi=settings["INCLUDE_AQI"],
            include_pollen=settings["INCLUDE_POLLEN"],
            include_alerts=settings["INCLUDE_ALERTS"],
            timeout_seconds=timeout,
        )
        if err:
            return err
        if not data:
            return "No weather data returned."

        facts = self._build_facts_block(
            data=data,
            units=units,
            want_days=days,
            wanted_date=wanted_date,
            hourly_peek_hours=hours,
            used_default_location=used_default_location,
        )

        if not request_text:
            request_text = json.dumps(intent, ensure_ascii=False)
        if not facts:
            return "No weather data returned."
        deterministic = self._deterministic_answer(
            data=data,
            intent=intent,
            units=units,
            wanted_date=wanted_date,
            days=days,
            max_chars=max_chars,
        )
        return await self._llm_guided_answer(
            llm_client=llm_client,
            request_text=request_text,
            intent=intent,
            facts_block=facts,
            max_chars=max_chars,
            fallback_text=deterministic,
        )

    # -------------------- Platform handlers --------------------

    async def handle_discord(self, message, args, llm_client):
        fallback = ""
        try:
            fallback = (getattr(message, "content", None) or "").strip()
        except Exception:
            fallback = ""
        args2, request_text = self._with_request_from_fallback(args, fallback)
        text = await self._get_weather_text(args2, llm_client=llm_client)
        return self._to_contract(text, request_text)

    async def handle_webui(self, args, llm_client):
        async def inner():
            args2, request_text = self._with_request_from_fallback(args or {}, "")
            text = await self._get_weather_text(args2, llm_client=llm_client)
            return self._to_contract(text, request_text)

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self.handle_webui(args or {}, llm_client)


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)

    async def handle_meshtastic(self, args=None, llm_client=None, context=None, **kwargs):
        args = args or {}
        ctx = context if isinstance(context, dict) else {}
        origin = ctx.get("origin") if isinstance(ctx.get("origin"), dict) else {}
        sender = ""
        source_from = origin.get("from")
        if isinstance(source_from, dict):
            sender = str(source_from.get("node_id") or source_from.get("long_name") or source_from.get("short_name") or "").strip()
        channel = str(ctx.get("channel") or origin.get("channel") or origin.get("target") or origin.get("channel_id") or "").strip()
        user = str(ctx.get("user") or origin.get("user") or origin.get("user_id") or sender or "").strip()
        raw_text = str(
            ctx.get("raw_message")
            or ctx.get("raw")
            or ctx.get("request_text")
            or origin.get("text")
            or origin.get("message")
            or origin.get("body")
            or ""
        ).strip()
        call_kwargs = {"args": args, "llm_client": llm_client}
        try:
            sig = __import__("inspect").signature(self.handle_irc)
        except Exception:
            sig = None
        if sig is not None:
            if "bot" in sig.parameters:
                call_kwargs["bot"] = None
            if "channel" in sig.parameters:
                call_kwargs["channel"] = channel
            if "user" in sig.parameters:
                call_kwargs["user"] = user
            if "raw_message" in sig.parameters:
                call_kwargs["raw_message"] = raw_text
            if "raw" in sig.parameters:
                call_kwargs["raw"] = raw_text
            if "context" in sig.parameters:
                call_kwargs["context"] = ctx
        return await self.handle_irc(**call_kwargs)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        args2, request_text = self._with_request_from_fallback(args or {}, raw_message)
        text = await self._get_weather_text(args2, llm_client=llm_client)
        return self._to_contract(text, request_text)

    async def handle_homeassistant(self, args, llm_client):
        args2, request_text = self._with_request_from_fallback(args or {}, "")
        text = await self._get_weather_text(args2, llm_client=llm_client)
        return self._to_contract(text, request_text)
    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        try:
            return await self.handle_homeassistant(args=args, llm_client=llm_client, context=context)
        except TypeError:
            try:
                return await self.handle_homeassistant(args=args, llm_client=llm_client)
            except TypeError:
                return await self.handle_homeassistant(args, llm_client)


    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        args2, request_text = self._with_request_from_fallback(args or {}, body)
        text = await self._get_weather_text(args2, llm_client=llm_client)
        return self._to_contract(text, request_text)

    async def handle_telegram(self, update, args, llm_client):
        fallback = ""
        try:
            if isinstance(update, dict):
                msg = update.get("message") or {}
                fallback = (msg.get("text") or msg.get("caption") or "").strip()
        except Exception:
            fallback = ""
        args2, request_text = self._with_request_from_fallback(args or {}, fallback)
        text = await self._get_weather_text(args2, llm_client=llm_client)
        return self._to_contract(text, request_text)

    async def handle_homekit(self, args, llm_client):
        args2, request_text = self._with_request_from_fallback(args or {}, "")
        text = await self._get_weather_text(args2, llm_client=llm_client)
        return self._to_contract(text, request_text)

    async def handle_xbmc(self, args, llm_client):
        args2, request_text = self._with_request_from_fallback(args or {}, "")
        text = await self._get_weather_text(args2, llm_client=llm_client)
        return self._to_contract(text, request_text)


verba = WeatherForecastPlugin()
