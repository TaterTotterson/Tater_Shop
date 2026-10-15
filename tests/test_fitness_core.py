import importlib.util
import json
import sys
import types
from pathlib import Path
from unittest.mock import patch


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value

    def hgetall(self, key):
        return dict(self.hashes.get(key) or {})

    def hset(self, key, field=None, value=None, mapping=None):
        row = self.hashes.setdefault(key, {})
        if mapping:
            row.update(mapping)
        elif field is not None:
            row[field] = value

    def scan_iter(self, pattern):
        prefix = pattern.rstrip("*")
        return iter([key for key in self.values if key.startswith(prefix)])


def load_core():
    helpers = types.ModuleType("helpers")
    helpers.redis_client = FakeRedis()
    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    sys.modules["helpers"] = helpers
    path = Path(__file__).resolve().parents[1] / "cores" / "fitness_core.py"
    spec = importlib.util.spec_from_file_location("fitness_core_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_daily_aggregates_are_normalized_and_summarized():
    core = load_core()
    store = FakeRedis()
    state = core.sync_client_fitness(
        node_id="phone-one",
        node_name="Alex on iPhone",
        client=store,
        payload={
            "platform": "ios-healthkit",
            "time_zone": "America/Chicago",
            "permissions": ["steps", "distance"],
            "days": [
                {
                    "date": "2026-10-14",
                    "steps": 12345,
                    "distance_m": 8123.45,
                    "active_energy_kcal": 511,
                    "exercise_minutes": 42,
                    "workouts": 1,
                }
            ],
        },
    )
    assert state["today"]["steps"] == 12345
    assert state["today"]["distance_m"] == 8123.45
    assert state["summary"]["headline"] == "Step goal reached"
    assert state["profile"]["display_name"] == "Alex on iPhone"
    stored = json.loads(store.hashes[core.DAILY_PREFIX + "phone-one"]["2026-10-14"])
    assert "date" not in stored


def test_profile_credentials_stay_private_and_challenge_profile_is_public():
    core = load_core()
    store = FakeRedis()
    store.hset(core.SETTINGS_KEY, mapping={"challenge_relay_url": "https://relay.example"})
    with patch.object(
        core,
        "_relay",
        return_value={"profile_id": "fit_123", "profile_secret": "secret-value"},
    ):
        state = core.run_client_fitness_profile_action(
            node_id="phone-one",
            node_name="Alex on iPhone",
            client=store,
            payload={"handle": "alex.walks", "display_name": "Alex", "discoverable": True},
        )
    assert state["profile"] == {"handle": "alex.walks", "display_name": "Alex", "discoverable": True}
    assert "relay_secret" not in state["profile"]
    private = json.loads(store.values[core.PROFILE_PREFIX + "phone-one"])
    assert private["relay_secret"] == "secret-value"


def test_same_tater_step_challenge_works_without_cloud_relay():
    core = load_core()
    store = FakeRedis()
    for node_id, handle, name, steps in (
        ("phone-one", "alex.walks", "Alex", 1000),
        ("phone-two", "sam.moves", "Sam", 2000),
    ):
        core.run_client_fitness_profile_action(
            node_id=node_id,
            node_name=name,
            client=store,
            payload={"handle": handle, "display_name": name, "discoverable": True},
        )
        core.sync_client_fitness(
            node_id=node_id,
            node_name=name,
            client=store,
            payload={"days": [{"date": "2026-10-14", "steps": steps}]},
        )
    created = core.run_client_fitness_challenge_action(
        node_id="phone-one",
        action="create",
        payload={"handle": "sam.moves", "duration": "3_hours", "steps": 1000},
        client=store,
    )
    challenge = created["challenges"][0]
    incoming = core.get_client_fitness_state(node_id="phone-two", client=store)["challenges"][0]
    assert incoming["incoming"] is True
    core.run_client_fitness_challenge_action(
        node_id="phone-two",
        action="accept",
        payload={"challenge_id": challenge["id"], "steps": 2000},
        client=store,
    )
    core.sync_client_fitness(
        node_id="phone-one",
        client=store,
        payload={"days": [{"date": "2026-10-14", "steps": 1250}], "challenge_steps": 1250},
    )
    state = core.get_client_fitness_state(node_id="phone-one", client=store)
    assert state["challenges"][0]["my_steps"] == 250


def test_invalid_daily_date_is_rejected():
    core = load_core()
    try:
        core.sync_client_fitness(
            node_id="phone-one",
            client=FakeRedis(),
            payload={"days": [{"date": "not-a-date", "steps": 10}]},
        )
    except ValueError as exc:
        assert "YYYY-MM-DD" in str(exc)
    else:
        raise AssertionError("invalid fitness date was accepted")
