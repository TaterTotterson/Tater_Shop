from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from cores import meshtastic_core


ROOT = Path(__file__).parents[1]


class FakeRedis:
    def __init__(self, values):
        self.values = values

    def hgetall(self, key):
        return self.values.get(key, {})

    def hset(self, key, field=None, value=None, *, mapping=None):
        row = self.values.setdefault(key, {})
        if mapping is not None:
            row.update(mapping)
        else:
            row[field] = value


class FakeCoreClient:
    transport_name = "fake"

    def __init__(self, *, pairing_result=None):
        self.closed = False
        self.sent = []
        self.paired = []
        self.pairing_result = pairing_result or {"ok": True}

    def close(self):
        self.closed = True

    def send_message(self, *, text, channel, destination):
        self.sent.append((text, channel, destination))
        return {"ok": True}

    def scan_devices(self):
        return {
            "devices": [{"name": "Mesh Radio", "address": "AA:BB", "rssi": -54}],
            "finished_at": "2026-10-09T12:00:00+00:00",
        }

    def configure_pairing(self, *, device_name, device_address, pin, selector="", address_type=1):
        del selector, address_type
        self.paired.append((device_name, device_address, pin))
        return self.pairing_result


def test_connection_settings_preserve_portal_values_until_core_values_exist() -> None:
    redis = FakeRedis(
        {
            "meshtastic_portal_settings": {
                b"bridge_url": b"http://portal.local:8433",
                b"request_timeout_sec": b"9",
            },
            "meshtastic_core_settings": {
                b"bridge_url": b"http://core.local:8433",
                b"history_limit": b"250",
            },
        }
    )

    settings = meshtastic_core.resolve_connection_settings(redis_client=redis)

    assert settings.bridge_url == "http://core.local:8433"
    assert settings.request_timeout_sec == 9
    assert settings.history_limit == 250


def test_meshtastic_protobuf_codec_encodes_text_without_runtime_dependency() -> None:
    raw, packet_id = meshtastic_core._encode_text_message(
        "hello mesh",
        channel=2,
        destination="!1234abcd",
    )
    outer = meshtastic_core._pb_fields(raw)
    packet = meshtastic_core._pb_fields(meshtastic_core._pb_first(outer, 1, b""))
    decoded = meshtastic_core._pb_fields(meshtastic_core._pb_first(packet, 4, b""))

    assert meshtastic_core._pb_u32(meshtastic_core._pb_first(packet, 2)) == 0x1234ABCD
    assert meshtastic_core._pb_first(packet, 3) == 2
    assert meshtastic_core._pb_u32(meshtastic_core._pb_first(packet, 6)) == packet_id
    assert meshtastic_core._pb_first(decoded, 1) == 1
    assert meshtastic_core._pb_first(decoded, 2) == b"hello mesh"


def test_echo_pairing_requires_confirmed_encrypted_bond_and_saves_no_pin(monkeypatch) -> None:
    redis = FakeRedis({})
    transport = meshtastic_core.EchoGATTTransport(
        selector="native:kitchen",
        address="aa:bb:cc:dd:ee:ff",
        address_type=1,
        timeout=5,
        redis_client=redis,
    )
    calls = []

    def request(kind, **payload):
        calls.append((kind, payload))
        if kind == "pair":
            return {"ok": True, "bonded": True, "encrypted": True, "authenticated": True}
        return {"ok": True}

    monkeypatch.setattr(transport, "_request", request)
    result = transport.configure_pairing(
        device_name="Mesh Radio",
        device_address="AA:BB:CC:DD:EE:FF",
        pin="123456",
        selector="native:kitchen",
        address_type=1,
    )

    assert result == {
        "ok": True,
        "bonded": True,
        "encrypted": True,
        "authenticated": True,
        "restart_required": False,
    }
    assert calls == [
        ("connect", {"addr": "aa:bb:cc:dd:ee:ff", "addr_type": 1}),
        ("pair", {"addr": "aa:bb:cc:dd:ee:ff", "pin": "123456"}),
    ]
    saved = redis.values["meshtastic_core_settings"]
    assert saved["transport"] == "echo"
    assert saved["echo_selector"] == "native:kitchen"
    assert saved["device_address"] == "aa:bb:cc:dd:ee:ff"
    assert "123456" not in json.dumps(saved)


@pytest.mark.parametrize("kind", ["connect", "pair"])
def test_echo_long_operations_outwait_rook_bluez_without_exceeding_satellite_correlation(
    monkeypatch,
    kind,
) -> None:
    observed = {}
    request_token = object()
    native_satellite = types.ModuleType("tater_voice.native_satellite")

    def send_request(selector, message_type, payload, *, timeout_s):
        observed.update(
            selector=selector,
            message_type=message_type,
            payload=payload,
            request_timeout=timeout_s,
        )
        return request_token

    def run_on_runtime_loop(awaitable, *, timeout):
        assert awaitable is request_token
        observed["loop_timeout"] = timeout
        return {"ok": True}

    native_satellite.send_request = send_request
    native_satellite.run_on_runtime_loop = run_on_runtime_loop
    tater_voice = types.ModuleType("tater_voice")
    tater_voice.__path__ = []
    tater_voice.native_satellite = native_satellite
    monkeypatch.setitem(sys.modules, "tater_voice", tater_voice)
    monkeypatch.setitem(sys.modules, "tater_voice.native_satellite", native_satellite)

    transport = meshtastic_core.EchoGATTTransport(
        selector="native:rook",
        address="aa:bb:cc:dd:ee:ff",
        address_type=1,
        timeout=5,
    )
    transport._request(kind, addr=transport.address)

    assert observed["selector"] == "native:rook"
    assert observed["message_type"] == "ble.gatt"
    assert observed["payload"]["t"] == kind
    assert observed["request_timeout"] == 40.0
    assert observed["loop_timeout"] == 42.0
    assert observed["request_timeout"] < 45.0


def test_echo_pairing_forgets_only_after_stale_bond_restore_failure(monkeypatch) -> None:
    transport = meshtastic_core.EchoGATTTransport(
        selector="native:kitchen",
        address="aa:bb:cc:dd:ee:ff",
        address_type=1,
        timeout=5,
    )
    calls = []
    connect_attempts = 0

    def request(kind, **payload):
        nonlocal connect_attempts
        calls.append((kind, payload))
        if kind == "connect":
            connect_attempts += 1
            if connect_attempts == 1:
                raise RuntimeError("Echo Bluetooth connect failed: ble: restore bond: encryption failed")
        if kind == "pair":
            return {"ok": True, "bonded": True, "encrypted": True, "authenticated": True}
        return {"ok": True}

    monkeypatch.setattr(transport, "_request", request)
    monkeypatch.setattr(transport, "_save_selection", lambda: None)
    transport.configure_pairing(
        device_name="Mesh Radio",
        device_address="AA:BB:CC:DD:EE:FF",
        pin="123456",
        selector="native:kitchen",
        address_type=1,
    )

    assert calls == [
        ("connect", {"addr": "aa:bb:cc:dd:ee:ff", "addr_type": 1}),
        ("forget", {"addr": "aa:bb:cc:dd:ee:ff"}),
        ("connect", {"addr": "aa:bb:cc:dd:ee:ff", "addr_type": 1}),
        ("pair", {"addr": "aa:bb:cc:dd:ee:ff", "pin": "123456"}),
    ]


def test_echo_transports_share_phone_api_history() -> None:
    address = "c0:00:00:00:00:77"
    first = meshtastic_core.EchoGATTTransport(
        selector="native:shared-test",
        address=address,
        address_type=1,
        timeout=5,
    )
    second = meshtastic_core.EchoGATTTransport(
        selector="native:shared-test",
        address=address,
        address_type=1,
        timeout=5,
    )
    first.messages.append({"event_id": 991, "text": "shared"})

    assert second.get_messages(since_id=0)["messages"][-1]["text"] == "shared"


def test_echo_history_is_persisted_with_monotonic_event_ids() -> None:
    selector = "native:persistent-test"
    address = "c0:00:00:00:00:79"
    redis = FakeRedis({})
    session_key = f"{selector}|{address}"
    with meshtastic_core._echo_sessions_lock:
        meshtastic_core._echo_sessions.pop(session_key, None)

    first = meshtastic_core.EchoGATTTransport(
        selector=selector,
        address=address,
        address_type=1,
        timeout=5,
        redis_client=redis,
    )
    first._record_message(
        {
            "event_id": 900000,
            "message_id": "900000",
            "direction": "inbound",
            "channel": 0,
            "timestamp": "2026-10-09T12:00:00+00:00",
            "from": {"node_id": "!00000001", "num": 1},
            "text": "first",
        }
    )
    first._record_message(
        {
            "event_id": 12,
            "message_id": "12",
            "direction": "inbound",
            "channel": 0,
            "timestamp": "2026-10-09T12:01:00+00:00",
            "from": {"node_id": "!00000002", "num": 2},
            "text": "second",
        }
    )

    assert [row["event_id"] for row in first.messages] == [1, 2]
    assert json.loads(redis.values[meshtastic_core.MESSAGE_HISTORY_KEY][f"messages:{address}"])[-1]["text"] == "second"

    with meshtastic_core._echo_sessions_lock:
        meshtastic_core._echo_sessions.pop(session_key, None)
    restored = meshtastic_core.EchoGATTTransport(
        selector=selector,
        address=address,
        address_type=1,
        timeout=5,
        redis_client=redis,
    )

    assert [row["text"] for row in restored.get_messages(since_id=0)["messages"]] == ["first", "second"]
    assert [row["text"] for row in restored.get_messages(since_id=1)["messages"]] == ["second"]


def test_node_cards_use_human_readable_mesh_details() -> None:
    now = 1_800_000_000.0
    relative, freshness, exact = meshtastic_core._node_last_seen(now - 3700, now=now)

    assert relative == "1 hr ago"
    assert freshness == "recent"
    assert exact.startswith("Last report ")

    items = meshtastic_core._node_items(
        [
            {
                "node_id": "!00000001",
                "num": 1,
                "long_name": "Tater",
                "last_seen": now,
                "snr": 0.0,
                "hops_away": 0,
            },
            {
                "node_id": "!00000002",
                "num": 2,
                "long_name": "Phooey",
                "last_seen": now - 90,
                "snr": 7.25,
                "hops_away": 1,
            },
        ],
        local_node={"node_id": "!00000001", "num": 1},
    )

    assert items[0]["card_variant"] == "mesh_node"
    assert items[0]["hide_core_key"] is True
    assert items[0]["hero_badges"][0] == {"label": "LOCAL", "tone": "accent"}
    assert items[0]["summary_rows"] == [
        {"label": "Last heard", "value": "Connected now"},
        {"label": "Link quality", "value": "Local radio"},
        {"label": "Route", "value": "This radio"},
    ]
    assert items[1]["hero_badges"][1]["label"] == "1 HOP"
    assert items[1]["summary_rows"][1]["value"] == "Good · +7.2 dB"


def test_echo_config_request_id_is_not_restarted(monkeypatch) -> None:
    transport = meshtastic_core.EchoGATTTransport(
        selector="native:config-test",
        address="c0:00:00:00:00:78",
        address_type=1,
        timeout=5,
    )
    transport.handles["toradio"] = 10
    transport.handles["fromradio"] = 11
    writes = []
    monkeypatch.setattr(transport, "_write", lambda handle, value: writes.append((handle, value)))
    monkeypatch.setattr(
        transport,
        "_read",
        lambda _handle: meshtastic_core._pb_varint(7, transport.config_id),
    )

    transport._start_config()
    request_id = transport.config_id
    transport.configured = False
    transport._start_config()

    assert transport.config_id == request_id
    assert len(writes) == 1


def test_auto_transport_uses_echo_for_initial_scan(monkeypatch) -> None:
    monkeypatch.setattr(meshtastic_core, "_available_echo_selector", lambda _preferred="": "native:scan-test")

    client = meshtastic_core.build_portal_client(
        overrides={"transport": "auto", "device_address": "", "echo_selector": ""},
    )

    assert client.transport_name == "echo_gatt"
    assert client.transport.selector == "native:scan-test"


def test_echo_scan_decodes_meshtastic_128_bit_uuid_and_uses_gatt_capable_observer(monkeypatch) -> None:
    address = "c6:26:49:99:cd:6a"
    raw_advert = "0201061107fdea73e2ca5da89f1f46a81518b2a16b"
    snapshot = {
        "observations": [
            {
                "address": address,
                "address_type": 1,
                "selector": "native:passive-only",
                "room": "Office",
                "rssi": -50,
                "data": raw_advert,
            },
            {
                "address": address,
                "address_type": 1,
                "selector": "native:active-gatt",
                "room": "Game Room",
                "rssi": -76,
                "data": raw_advert,
            },
        ],
        "devices": [
            {
                "address": address,
                "display_name": "BLE 99:CD:6A",
                "service_uuids": [],
                "strongest_selector": "native:passive-only",
                "strongest_room": "Office",
                "strongest_rssi": -50,
            }
        ],
    }
    native_ble = types.ModuleType("tater_voice.native_ble")
    native_ble.snapshot = lambda **_kwargs: snapshot
    tater_voice = types.ModuleType("tater_voice")
    tater_voice.__path__ = []
    tater_voice.native_ble = native_ble
    monkeypatch.setitem(sys.modules, "tater_voice", tater_voice)
    monkeypatch.setitem(sys.modules, "tater_voice.native_ble", native_ble)
    monkeypatch.setattr(
        meshtastic_core,
        "_native_satellites",
        lambda: {
            "native:passive-only": {"connected": True, "capabilities": {"ble_gatt": False}},
            "native:active-gatt": {"connected": True, "capabilities": {"ble_gatt": True}},
        },
    )

    transport = meshtastic_core.EchoGATTTransport(
        selector="native:active-gatt",
        address="",
        address_type=1,
        timeout=5,
    )
    result = transport.scan_devices()

    assert meshtastic_core.MESHTASTIC_SERVICE_UUID in meshtastic_core._advertisement_service_uuids(raw_advert)
    assert result["count"] == 1
    assert result["devices"] == [
        {
            "name": "BLE 99:CD:6A",
            "address": address,
            "address_type": 1,
            "selector": "native:active-gatt",
            "rssi": -76,
            "room": "Game Room",
        }
    ]


def test_tab_uses_live_channel_chat_contract(monkeypatch) -> None:
    snapshot = {
        "running": True,
        "transport": "bridge_http",
        "last_refresh": "2026-10-09T12:00:00+00:00",
        "last_error": "",
        "status": {
            "connected": True,
            "local_node": {"node_id": "!1234", "long_name": "Kitchen Mesh"},
        },
        "messages": [
            {
                "event_id": 44,
                "direction": "inbound",
                "channel": 0,
                "timestamp": "2026-10-09T11:59:00+00:00",
                "from": {"node_id": "!abcd", "long_name": "Alice"},
                "text": "Hello from the mesh",
            }
        ],
        "channels": [{"index": 0, "name": "Primary"}, {"index": 2, "name": "Ops"}],
        "nodes": [{"node_id": "!abcd", "long_name": "Alice", "snr": 8.5, "hops_away": 1}],
        "scan": {
            "devices": [{"name": "Mesh Radio", "address": "AA:BB", "rssi": -54}],
            "finished_at": "2026-10-09T11:58:00+00:00",
            "error": "",
        },
        "pairing": {"device_name": "", "device_address": "", "configured_at": ""},
    }
    monkeypatch.setattr(meshtastic_core, "_state_snapshot", lambda: snapshot)

    result = meshtastic_core.get_htmlui_tab_data()

    ui = result["ui"]
    assert ui["kind"] == "channel_chat"
    assert ui["status"]["connected"] is True
    assert [channel["label"] for channel in ui["channels"]] == ["Primary", "Ops"]
    assert ui["channels"][0]["message_count"] == 1
    assert ui["messages"] == [
        {
            "id": "44",
            "event_id": 44,
            "message_id": "",
            "channel": "0",
            "direction": "inbound",
            "sender_name": "Alice",
            "sender_id": "!abcd",
            "recipient_id": "",
            "timestamp": "2026-10-09T11:59:00+00:00",
            "text": "Hello from the mesh",
        }
    ]
    assert ui["composer"]["action"] == "send_message"
    assert [tab["key"] for tab in ui["manager_tabs"]] == ["nodes", "bluetooth"]
    forms = ui["item_forms"]
    assert {form["group"] for form in forms} >= {
        "nodes",
        "pairing_controls",
        "pairing_devices",
    }
    pairing = next(form for form in forms if form["group"] == "pairing_devices")
    pin = next(field for field in pairing["fields"] if field["key"] == "pin")
    assert pin["type"] == "password"
    assert pin["value"] == ""


def test_manager_actions_read_nested_values_and_never_return_pin(monkeypatch) -> None:
    client = FakeCoreClient(pairing_result={"ok": True, "ble_pin": "123456", "restart_required": True})
    monkeypatch.setattr(meshtastic_core, "_fresh_client", lambda _redis=None: client)

    sent = meshtastic_core.handle_htmlui_tab_action(
        action="send_message",
        payload={"id": "compose:message", "values": {"text": "Hi", "channel": "2", "destination": "broadcast"}},
    )
    paired = meshtastic_core.handle_htmlui_tab_action(
        action="pair_device",
        payload={
            "id": "pairing:device:AA:BB",
            "values": {"device_name": "Mesh Radio", "device_address": "AA:BB", "pin": "123456"},
        },
    )

    assert sent["ok"] is True
    assert client.sent == [("Hi", 2, "broadcast")]
    assert client.paired == [("Mesh Radio", "AA:BB", "123456")]
    assert "123456" not in json.dumps(paired)
    assert "ble_pin" not in paired
    assert paired["restart_required"] is True
    assert client.closed is True


@pytest.mark.parametrize("pin", ["", "12345", "1234567", "abcdef", "12 34"])
def test_pairing_requires_exactly_six_digits(monkeypatch, pin) -> None:
    client = FakeCoreClient()
    monkeypatch.setattr(meshtastic_core, "_fresh_client", lambda _redis=None: client)

    with pytest.raises(ValueError, match="exactly six digits"):
        meshtastic_core.handle_htmlui_tab_action(
            action="pair_device",
            payload={"values": {"device_name": "Mesh Radio", "device_address": "AA:BB", "pin": pin}},
        )

    assert client.paired == []
    assert client.closed is True


def test_shop_portal_keeps_fallback_and_optionally_uses_core() -> None:
    source = (ROOT / "portals" / "meshtastic_portal.py").read_text(encoding="utf-8")

    assert "from cores.meshtastic_core import build_portal_client as _build_core_client" in source
    assert "class BridgeClient:" in source
    assert "self.bridge = _bridge_client_from_settings()" in source
