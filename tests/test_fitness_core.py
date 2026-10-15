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
    now = [1000.0]
    core.time.time = lambda: now[0]

    class FakeModel:
        def __init__(self):
            self.calls = 0
            self.last_prompt = ""

        async def chat(self, **kwargs):
            self.calls += 1
            self.last_prompt = kwargs["messages"][0]["content"]
            return {
                "message": {
                    "content": json.dumps(
                        {
                            "headline": "A steady week",
                            "detail": "Your recent activity has been consistent.",
                            "insights": [
                                {
                                    "id": "steps",
                                    "headline": "Consistent steps",
                                    "detail": "Your recent step totals have been steady.",
                                }
                            ],
                        }
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
    assert first["insights"][0]["id"] == "steps"
    assert "positive, forward-looking overall wellness perspective" in model.last_prompt
    assert "Do not declare that the person is healthy or unhealthy" in model.last_prompt
    assert model.calls == 1
    changed = core._ai_summary(store, "person_alex", [{"date": "2026-10-14", "steps": 7100}])
    assert changed["headline"] == "A steady week"
    assert model.calls == 1
    now[0] += 24 * 3600 + 1
    changed = core._ai_summary(store, "person_alex", [{"date": "2026-10-14", "steps": 7100}])
    assert changed["headline"] == "A steady week"
    assert model.calls == 2


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
    assert state["summary"]["headline"] == "Your health overview"
    assert state["summary"]["insights"][0]["id"] == "steps"
    assert state["challenge_people"] == [{"id": "person_sam", "name": "Sam"}]
    stored = json.loads(store.hashes[core.DAILY_PREFIX + "person_alex"]["2026-10-14"])
    assert "date" not in stored


def test_two_phone_sources_merge_without_double_counting_or_last_write_wins():
    core = load_core()
    store = FakeRedis()
    now = [1000.0]
    core.time.time = lambda: now[0]

    core.sync_client_fitness(
        node_id="iphone-node",
        node_name="Alex's iPhone",
        person_id="person_alex",
        person_name="Alex",
        client=store,
        payload={
            "platform": "ios-healthkit",
            "days": [{"date": "2026-10-14", "steps": 8000, "sleep_minutes": 420}],
        },
    )
    now[0] = 1100.0
    state = core.sync_client_fitness(
        node_id="android-node",
        node_name="Alex's Android",
        person_id="person_alex",
        person_name="Alex",
        client=store,
        payload={
            "platform": "android-health-connect",
            "days": [
                {
                    "date": "2026-10-14",
                    "steps": 5000,
                    "weight_kg": 82.3,
                    "blood_pressure_systolic_mmhg": 118,
                    "blood_pressure_diastolic_mmhg": 76,
                }
            ],
        },
    )

    assert state["preferred_source_id"] == "iphone-node"
    assert {source["id"] for source in state["sources"]} == {"iphone-node", "android-node"}
    assert state["today"]["steps"] == 8000
    assert state["today"]["sleep_minutes"] == 420
    assert state["today"]["weight_kg"] == 82.3
    assert state["today"]["blood_pressure_systolic_mmhg"] == 118
    assert state["today"]["blood_pressure_diastolic_mmhg"] == 76


def test_secondary_phone_fills_missing_metrics_and_dates():
    core = load_core()
    store = FakeRedis()
    core.sync_client_fitness(
        node_id="iphone-node",
        person_id="person_alex",
        client=store,
        payload={
            "platform": "ios-healthkit",
            "days": [{"date": "2026-10-13", "steps": 7000}],
        },
    )
    state = core.sync_client_fitness(
        node_id="android-node",
        person_id="person_alex",
        client=store,
        payload={
            "platform": "android-health-connect",
            "days": [
                {"date": "2026-10-14", "steps": 6100, "distance_m": 4900},
                {"date": "2026-10-13", "distance_m": 5600},
            ],
        },
    )
    assert state["today"]["date"] == "2026-10-14"
    assert state["today"]["steps"] == 6100
    older = next(day for day in state["recent_days"] if day["date"] == "2026-10-13")
    assert older["steps"] == 7000
    assert older["distance_m"] == 5600


def test_secondary_phone_resync_does_not_inflate_preferred_source_challenge():
    core = load_core()
    store = FakeRedis()
    for person_id, node_id, steps in (
        ("person_alex", "iphone-node", 1000),
        ("person_sam", "sam-phone", 2000),
    ):
        core.sync_client_fitness(
            node_id=node_id,
            person_id=person_id,
            person_name="Alex" if person_id == "person_alex" else "Sam",
            people=people(),
            client=store,
            payload={"days": [{"date": "2026-10-14", "steps": steps}]},
        )
    created = core.run_client_fitness_challenge_action(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        action="create",
        payload={"person_id": "person_sam", "metric": "steps", "duration": "day"},
        client=store,
    )
    challenge_id = created["challenges"][0]["id"]
    core.run_client_fitness_challenge_action(
        person_id="person_sam",
        person_name="Sam",
        people=people(),
        action="accept",
        payload={"challenge_id": challenge_id},
        client=store,
    )
    state = core.sync_client_fitness(
        node_id="android-node",
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        client=store,
        payload={"days": [{"date": "2026-10-14", "steps": 1600, "weight_kg": 82.0}]},
    )
    challenge = next(row for row in state["challenges"] if row["id"] == challenge_id)
    assert challenge["my_score"] == 0
    assert state["today"]["steps"] == 1000


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
        payload={"person_id": "person_sam", "duration": "3_hours", "metric": "steps", "value": 1000},
        client=store,
    )
    challenge = created["challenges"][0]
    assert challenge["opponent_id"] == "person_sam"
    assert challenge["metric"] == "steps"
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
        payload={"challenge_id": challenge["id"], "metric": "steps", "value": 2000},
        client=store,
    )
    core.sync_client_fitness(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        node_id="phone-one",
        client=store,
        payload={"days": [{"date": "2026-10-14", "steps": 1250}]},
    )
    state = core.get_client_fitness_state(
        person_id="person_alex", person_name="Alex", people=people(), client=store
    )
    assert state["challenges"][0]["my_score"] == 250


def test_challenge_lifecycle_notifies_the_affected_people_once():
    core = load_core()
    store = FakeRedis()
    notifications = []

    def capture(_client, **payload):
        notifications.append(payload)

    core._dispatch_person_notification = capture
    created = core.run_client_fitness_challenge_action(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        action="create",
        payload={"person_id": "person_sam", "duration": "3_hours", "metric": "steps"},
        client=store,
    )
    challenge_id = created["challenges"][0]["id"]
    assert [(row["person_id"], row["event"]) for row in notifications] == [
        ("person_sam", "invited"),
    ]

    core.run_client_fitness_challenge_action(
        person_id="person_sam",
        person_name="Sam",
        people=people(),
        action="accept",
        payload={"challenge_id": challenge_id},
        client=store,
    )
    assert (notifications[-1]["person_id"], notifications[-1]["event"]) == ("person_alex", "accepted")

    stored = json.loads(store.hashes[core.CHALLENGE_RECORDS_KEY][challenge_id])
    stored.update({"end_at": 1, "challenger_score": 5000, "opponent_score": 4500})
    store.hashes[core.CHALLENGE_RECORDS_KEY][challenge_id] = json.dumps(stored)
    core._finish_local_challenges(store)
    completed = [row for row in notifications if row["event"] == "completed"]
    assert {row["person_id"] for row in completed} == {"person_alex", "person_sam"}
    assert any("You won" in row["content"] for row in completed if row["person_id"] == "person_alex")

    core._finish_local_challenges(store)
    assert len([row for row in notifications if row["event"] == "completed"]) == 2

    declined_state = core.run_client_fitness_challenge_action(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        action="create",
        payload={"person_id": "person_sam", "duration": "day", "metric": "distance_m"},
        client=store,
    )
    declined_id = next(row["id"] for row in declined_state["challenges"] if row["state"] == "pending")
    core.run_client_fitness_challenge_action(
        person_id="person_sam",
        person_name="Sam",
        people=people(),
        action="decline",
        payload={"challenge_id": declined_id},
        client=store,
    )
    assert (notifications[-1]["person_id"], notifications[-1]["event"]) == ("person_alex", "declined")

    cancelled_state = core.run_client_fitness_challenge_action(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        action="create",
        payload={"person_id": "person_sam", "duration": "week", "metric": "workouts"},
        client=store,
    )
    cancelled_id = next(row["id"] for row in cancelled_state["challenges"] if row["state"] == "pending")
    core.run_client_fitness_challenge_action(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        action="cancel",
        payload={"challenge_id": cancelled_id},
        client=store,
    )
    assert (notifications[-1]["person_id"], notifications[-1]["event"]) == ("person_sam", "cancelled")


def test_fitness_notifications_target_every_app_linked_to_the_person():
    core = load_core()
    store = FakeRedis()
    store.hashes[core.SPUD_LINK_NODES_KEY] = {
        "phone-one": json.dumps(
            {"id": "phone-one", "role": "little_spud", "linked_person_id": "person_alex"}
        ),
        "tablet-one": json.dumps(
            {"id": "tablet-one", "role": "little_spud", "linked_person_id": "person_alex"}
        ),
        "phone-two": json.dumps(
            {"id": "phone-two", "role": "little_spud", "linked_person_id": "person_sam"}
        ),
    }
    assert core._little_spud_node_ids_for_person(store, "person_alex") == ["phone-one", "tablet-one"]
    assert core._little_spud_node_ids_for_person(store, "person_sam") == ["phone-two"]

    dispatched = []
    notify_package = types.ModuleType("notify")
    notify_package.__path__ = []
    notify_core = types.ModuleType("notify.core")
    notify_core.dispatch_notification_sync = lambda *args, **kwargs: dispatched.append((args, kwargs)) or "Queued notification for little_spud"
    previous_notify = sys.modules.get("notify")
    previous_notify_core = sys.modules.get("notify.core")
    sys.modules["notify"] = notify_package
    sys.modules["notify.core"] = notify_core
    try:
        payload = {
            "person_id": "person_alex",
            "challenge_id": "challenge_one",
            "event": "invited",
            "title": "New fitness challenge",
            "content": "Sam challenged you.",
        }
        core._dispatch_person_notification(store, **payload)
        core._dispatch_person_notification(store, **payload)
    finally:
        if previous_notify is None:
            sys.modules.pop("notify", None)
        else:
            sys.modules["notify"] = previous_notify
        if previous_notify_core is None:
            sys.modules.pop("notify.core", None)
        else:
            sys.modules["notify.core"] = previous_notify_core

    assert [row[1]["targets"]["node_id"] for row in dispatched] == ["phone-one", "tablet-one"]


def test_missing_health_values_stay_missing_and_vitals_get_insights():
    core = load_core()
    row = core._normalized_day(
        {
            "date": "2026-10-14",
            "steps": None,
            "heart_rate_avg_bpm": 74.2,
            "heart_rate_min_bpm": 58,
            "heart_rate_max_bpm": 121,
            "blood_pressure_systolic_mmhg": 118,
            "blood_pressure_diastolic_mmhg": 76,
        }
    )
    assert "steps" not in row
    assert row["heart_rate_avg_bpm"] == 74
    summary = core._summary([row])
    ids = {item["id"] for item in summary["insights"]}
    assert {"heart_rate", "blood_pressure"}.issubset(ids)


def test_weight_challenge_compares_checkins_not_weight_values():
    core = load_core()
    store = FakeRedis()
    for person_id, name, weight in (("person_alex", "Alex", 82.0), ("person_sam", "Sam", 64.0)):
        core.sync_client_fitness(
            person_id=person_id,
            person_name=name,
            people=people(),
            client=store,
            payload={"days": [{"date": "2026-10-14", "weight_kg": weight}]},
        )
    created = core.run_client_fitness_challenge_action(
        person_id="person_alex",
        person_name="Alex",
        people=people(),
        action="create",
        payload={"person_id": "person_sam", "metric": "weight_checkins", "duration": "week"},
        client=store,
    )
    challenge = created["challenges"][0]
    accepted = core.run_client_fitness_challenge_action(
        person_id="person_sam",
        person_name="Sam",
        people=people(),
        action="accept",
        payload={"challenge_id": challenge["id"]},
        client=store,
    )["challenges"][0]
    assert accepted["metric"] == "weight_checkins"
    assert accepted["my_score"] == 1
    assert accepted["their_score"] == 1
    stored = json.loads(store.hashes[core.CHALLENGE_RECORDS_KEY][challenge["id"]])
    assert stored["challenger_last_value"] == 1
    assert stored["opponent_last_value"] == 1


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
