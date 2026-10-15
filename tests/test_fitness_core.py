import importlib.util
import json
import sys
import types
from pathlib import Path


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


def people():
    return [
        {"id": "person_alex", "name": "Alex"},
        {"id": "person_sam", "name": "Sam"},
    ]


def test_ai_wellness_summary_is_automatic_and_not_a_setting():
    core = load_core()
    store = FakeRedis()

    class FakeModel:
        def __init__(self):
            self.calls = 0

        async def chat(self, **_kwargs):
            self.calls += 1
            return {
                "message": {
                    "content": json.dumps(
                        {"headline": "A steady week", "detail": "Your recent activity has been consistent."}
                    )
                }
            }

    model = FakeModel()
    core._get_primary_llm_client_from_env = lambda: model
    assert "ai_summaries_enabled" not in core.CORE_SETTINGS["required"]
    days = [{"date": "2026-10-14", "steps": 7000}]
    first = core._ai_summary(store, "person_alex", days)
    second = core._ai_summary(store, "person_alex", days)
    assert first == second
    assert first["headline"] == "A steady week"
    assert model.calls == 1


def test_daily_aggregates_are_stored_under_the_linked_person():
    core = load_core()
    store = FakeRedis()
    state = core.sync_client_fitness(
        node_id="phone-one",
        node_name="Alex on iPhone",
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        client=store,
        payload={
            "platform": "ios-healthkit",
            "time_zone": "America/Chicago",
            "permissions": ["steps", "distance"],
            "days": [{"date": "2026-10-14", "steps": 12345, "distance_m": 8123.45}],
        },
    )
    assert state["person"] == {"id": "person_alex", "name": "Alex"}
    assert state["today"]["steps"] == 12345
    assert state["summary"]["headline"] == "Step goal reached"
    assert state["challenge_people"] == [{"id": "person_sam", "name": "Sam"}]
    stored = json.loads(store.hashes[core.DAILY_PREFIX + "person_alex"]["2026-10-14"])
    assert "date" not in stored


def test_same_tater_step_challenge_uses_people_ids():
    core = load_core()
    store = FakeRedis()
    for person_id, name, node_id, steps in (
        ("person_alex", "Alex", "phone-one", 1000),
        ("person_sam", "Sam", "phone-two", 2000),
    ):
        core.sync_client_fitness(
            person_id=person_id,
            person_name=name,
            people=people(),
            node_id=node_id,
            node_name=f"{name}'s phone",
            client=store,
            payload={"days": [{"date": "2026-10-14", "steps": steps}]},
        )
    created = core.run_client_fitness_challenge_action(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        node_id="phone-one",
        action="create",
        payload={"person_id": "person_sam", "duration": "3_hours", "steps": 1000},
        client=store,
    )
    challenge = created["challenges"][0]
    assert challenge["opponent_id"] == "person_sam"
    incoming = core.get_client_fitness_state(
        person_id="person_sam", person_name="Sam", people=people(), client=store
    )["challenges"][0]
    assert incoming["incoming"] is True
    assert incoming["opponent_name"] == "Alex"
    core.run_client_fitness_challenge_action(
        person_id="person_sam",
        person_name="Sam",
        people=people(),
        node_id="phone-two",
        action="accept",
        payload={"challenge_id": challenge["id"], "steps": 2000},
        client=store,
    )
    core.sync_client_fitness(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        node_id="phone-one",
        client=store,
        payload={"days": [{"date": "2026-10-14", "steps": 1250}], "challenge_steps": 1250},
    )
    state = core.get_client_fitness_state(
        person_id="person_alex", person_name="Alex", people=people(), client=store
    )
    assert state["challenges"][0]["my_steps"] == 250


def test_challenge_rejects_unknown_or_self_person():
    core = load_core()
    for target in ("person_alex", "person_missing"):
        try:
            core.run_client_fitness_challenge_action(
                person_id="person_alex",
                person_name="Alex",
                people=people(),
                action="create",
                payload={"person_id": target},
                client=FakeRedis(),
            )
        except ValueError:
            pass
        else:
            raise AssertionError("invalid challenge target was accepted")


def test_invalid_daily_date_is_rejected():
    core = load_core()
    try:
        core.sync_client_fitness(
            person_id="person_alex",
            person_name="Alex",
            client=FakeRedis(),
            payload={"days": [{"date": "not-a-date", "steps": 10}]},
        )
    except ValueError as exc:
        assert "YYYY-MM-DD" in str(exc)
    else:
        raise AssertionError("invalid fitness date was accepted")
