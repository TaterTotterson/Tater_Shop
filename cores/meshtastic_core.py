from __future__ import annotations

import logging
import base64
import random
import re
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol
from urllib.parse import urljoin

import requests


__version__ = "0.2.0"
MIN_TATER_VERSION = "187"
CORE_DESCRIPTION = (
    "Run Meshtastic chat, history, node status, Bluetooth discovery, and secure pairing "
    "through a transport-neutral Tater core."
)
TAGS = ["radio", "mesh", "offgrid", "bluetooth"]

logger = logging.getLogger("meshtastic.core")

DEFAULT_BRIDGE_URL = "http://127.0.0.1:8433"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 15.0
DEFAULT_REFRESH_INTERVAL_SECONDS = 3.0
DEFAULT_HISTORY_LIMIT = 100

CORE_SETTINGS = {
    "category": "Meshtastic Core Settings",
    "tags": TAGS,
    "required": {
        "transport": {
            "label": "Radio Transport",
            "type": "select",
            "default": "auto",
            "options": [
                {"label": "Automatic (prefer Echo)", "value": "auto"},
                {"label": "Echo satellite Bluetooth", "value": "echo"},
                {"label": "Compatibility bridge", "value": "bridge"},
            ],
            "description": "Automatic uses an Echo satellite when a paired radio is selected, with the bridge as a migration fallback.",
        },
        "echo_selector": {
            "label": "Echo Satellite",
            "type": "string",
            "default": "",
            "description": "Native satellite selector chosen during Bluetooth pairing.",
        },
        "device_address": {
            "label": "Meshtastic Bluetooth Address",
            "type": "string",
            "default": "",
            "description": "Saved automatically after secure pairing.",
        },
        "device_address_type": {
            "label": "Bluetooth Address Type",
            "type": "number",
            "default": 1,
            "description": "0 for public and 1 for random BLE addresses.",
        },
        "bridge_url": {
            "label": "Compatibility Bridge URL",
            "type": "string",
            "default": DEFAULT_BRIDGE_URL,
            "description": "Temporary HTTP transport used until Echo remote-GATT is available.",
        },
        "api_token": {
            "label": "Compatibility Bridge API Token",
            "type": "password",
            "default": "",
            "description": "Optional token required by the current Meshtastic bridge.",
        },
        "request_timeout_sec": {
            "label": "Request Timeout (sec)",
            "type": "number",
            "default": DEFAULT_REQUEST_TIMEOUT_SECONDS,
            "description": "Timeout for compatibility-bridge operations.",
        },
        "refresh_interval_sec": {
            "label": "Refresh Interval (sec)",
            "type": "number",
            "default": DEFAULT_REFRESH_INTERVAL_SECONDS,
            "description": "How frequently the core refreshes portal state.",
        },
        "history_limit": {
            "label": "Portal History Limit",
            "type": "number",
            "default": DEFAULT_HISTORY_LIMIT,
            "description": "Maximum recent radio messages shown in the core portal.",
        },
    },
}

CORE_WEBUI_TAB = {
    "label": "Meshtastic",
    "order": 55,
    "requires_running": True,
}


class MeshtasticTransport(Protocol):
    name: str

    def close(self) -> None: ...

    def get_status(self) -> Dict[str, Any]: ...

    def get_messages(self, *, since_id: int, limit: int) -> Dict[str, Any]: ...

    def get_channels(self) -> Dict[str, Any]: ...

    def get_nodes(self) -> Dict[str, Any]: ...

    def send_message(self, *, text: str, channel: int, destination: str) -> Dict[str, Any]: ...

    def scan_devices(self) -> Dict[str, Any]: ...

    def configure_pairing(
        self,
        *,
        device_name: str,
        device_address: str,
        pin: str,
        selector: str = "",
        address_type: int = 1,
    ) -> Dict[str, Any]: ...


class BridgeHTTPTransport:
    """Compatibility transport for the existing standalone bridge.

    The core and portal depend on the transport contract rather than this HTTP
    implementation. Echo remote-GATT can therefore replace it without changing
    the chat or portal layers.
    """

    name = "bridge_http"

    def __init__(
        self,
        *,
        base_url: str,
        api_token: str,
        timeout: float,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = str(base_url or DEFAULT_BRIDGE_URL).rstrip("/") + "/"
        self.api_token = str(api_token or "").strip()
        self.timeout = max(2.0, float(timeout or DEFAULT_REQUEST_TIMEOUT_SECONDS))
        self.session = session or requests.Session()

    def close(self) -> None:
        close = getattr(self.session, "close", None)
        if callable(close):
            close()

    def _headers(self, *, json_body: bool = False) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if json_body:
            headers["Content-Type"] = "application/json"
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def _url(self, path: str) -> str:
        return urljoin(self.base_url, str(path or "").lstrip("/"))

    @staticmethod
    def _json(response: Any) -> Dict[str, Any]:
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Meshtastic transport returned a non-object response.")
        return payload

    def _get(self, path: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._json(
            self.session.get(
                self._url(path),
                headers=self._headers(),
                params=params,
                timeout=self.timeout,
            )
        )

    def _post(self, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._json(
            self.session.post(
                self._url(path),
                headers=self._headers(json_body=True),
                json=payload or {},
                timeout=self.timeout,
            )
        )

    def get_status(self) -> Dict[str, Any]:
        return self._get("/status")

    def get_messages(self, *, since_id: int = 0, limit: int = 50) -> Dict[str, Any]:
        return self._get(
            "/messages",
            params={
                "since_id": max(0, int(since_id or 0)),
                "limit": max(1, min(1000, int(limit or 50))),
            },
        )

    def get_channels(self) -> Dict[str, Any]:
        return self._get("/channels")

    def get_nodes(self) -> Dict[str, Any]:
        return self._get("/nodes")

    def send_message(self, *, text: str, channel: int, destination: str) -> Dict[str, Any]:
        return self._post(
            "/send",
            {
                "text": str(text or "").strip(),
                "channel": int(channel or 0),
                "destination": str(destination or "broadcast").strip() or "broadcast",
            },
        )

    def scan_devices(self) -> Dict[str, Any]:
        return self._post("/ble/scan")

    def configure_pairing(
        self,
        *,
        device_name: str,
        device_address: str,
        pin: str,
        selector: str = "",
        address_type: int = 1,
    ) -> Dict[str, Any]:
        del selector, address_type
        payload = {
            "device_name": str(device_name or "").strip(),
            "device_address": str(device_address or "").strip(),
            "ble_pair": True,
            "ble_pin": str(pin or "").strip(),
        }
        return self._post("/settings", payload)


MESHTASTIC_SERVICE_UUID = "6ba1b218-15a8-461f-9fa8-5dcae273eafd"
MESHTASTIC_FROMRADIO_UUID = "2c55e69e-4993-11ed-b878-0242ac120002"
MESHTASTIC_TORADIO_UUID = "f75c76d2-129e-4dad-a1dd-7866124401e7"
MESHTASTIC_FROMNUM_UUID = "ed9da18c-a800-4f66-a670-aa7547e34453"
GATT_CCCD_UUID = "00002902-0000-1000-8000-00805f9b34fb"

_echo_request_lock = threading.Lock()
_echo_request_id = random.randint(1, 0x7FFFFFFF)


@dataclass
class _EchoSessionState:
    """Process-local PhoneAPI state shared by the core and portal actions."""

    lock: Any = field(default_factory=threading.RLock)
    handles: Dict[str, int] = field(default_factory=dict)
    connected: bool = False
    configured: bool = False
    config_id: int = 0
    local_node_num: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    messages: List[Dict[str, Any]] = field(default_factory=list)
    nodes: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    channels: Dict[int, Dict[str, Any]] = field(default_factory=dict)


_echo_sessions_lock = threading.Lock()
_echo_sessions: Dict[str, _EchoSessionState] = {}


def _echo_session_state(selector: str, address: str) -> _EchoSessionState:
    key = f"{str(selector or '').strip()}|{str(address or '').strip().lower()}"
    with _echo_sessions_lock:
        state = _echo_sessions.get(key)
        if state is None:
            state = _EchoSessionState()
            _echo_sessions[key] = state
        return state


def _next_echo_request_id() -> int:
    global _echo_request_id
    with _echo_request_lock:
        _echo_request_id = (_echo_request_id + 1) & 0xFFFFFFFF
        if _echo_request_id == 0:
            _echo_request_id = 1
        return _echo_request_id


def _encode_varint(value: int) -> bytes:
    number = int(value)
    if number < 0:
        number &= 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while number > 0x7F:
        out.append((number & 0x7F) | 0x80)
        number >>= 7
    out.append(number)
    return bytes(out)


def _pb_varint(field: int, value: int) -> bytes:
    return _encode_varint((int(field) << 3) | 0) + _encode_varint(value)


def _pb_fixed32(field: int, value: int) -> bytes:
    return _encode_varint((int(field) << 3) | 5) + struct.pack("<I", int(value) & 0xFFFFFFFF)


def _pb_bytes(field: int, value: bytes) -> bytes:
    raw = bytes(value)
    return _encode_varint((int(field) << 3) | 2) + _encode_varint(len(raw)) + raw


def _read_varint(raw: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(raw) and shift < 70:
        byte = raw[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return value, offset
        shift += 7
    raise ValueError("Malformed Meshtastic protobuf varint.")


def _pb_fields(raw: bytes) -> List[tuple[int, int, Any]]:
    fields: List[tuple[int, int, Any]] = []
    offset = 0
    while offset < len(raw):
        key, offset = _read_varint(raw, offset)
        number, wire = key >> 3, key & 7
        if number <= 0:
            raise ValueError("Malformed Meshtastic protobuf field.")
        if wire == 0:
            value, offset = _read_varint(raw, offset)
        elif wire == 1:
            if offset + 8 > len(raw):
                raise ValueError("Truncated Meshtastic protobuf fixed64 field.")
            value, offset = raw[offset : offset + 8], offset + 8
        elif wire == 2:
            size, offset = _read_varint(raw, offset)
            if offset + size > len(raw):
                raise ValueError("Truncated Meshtastic protobuf bytes field.")
            value, offset = raw[offset : offset + size], offset + size
        elif wire == 5:
            if offset + 4 > len(raw):
                raise ValueError("Truncated Meshtastic protobuf fixed32 field.")
            value, offset = raw[offset : offset + 4], offset + 4
        else:
            raise ValueError(f"Unsupported Meshtastic protobuf wire type {wire}.")
        fields.append((number, wire, value))
    return fields


def _pb_first(fields: List[tuple[int, int, Any]], number: int, default: Any = None) -> Any:
    for field, _wire, value in fields:
        if field == number:
            return value
    return default


def _pb_u32(value: Any, default: int = 0) -> int:
    if isinstance(value, (bytes, bytearray)) and len(value) == 4:
        return int(struct.unpack("<I", bytes(value))[0])
    try:
        return int(value)
    except Exception:
        return int(default)


def _pb_int32(value: Any, default: int = 0) -> int:
    try:
        number = int(value)
    except Exception:
        return int(default)
    if number >= 1 << 63:
        number -= 1 << 64
    elif number >= 1 << 31:
        number -= 1 << 32
    return number


def _pb_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (bytes, bytearray)) and len(value) == 4:
        return float(struct.unpack("<f", bytes(value))[0])
    return float(default)


def _node_id(number: int) -> str:
    return f"!{int(number) & 0xFFFFFFFF:08x}"


def _parse_user(raw: bytes) -> Dict[str, Any]:
    fields = _pb_fields(raw)
    text = lambda number: bytes(_pb_first(fields, number, b"")).decode("utf-8", errors="replace")
    return {
        "id": text(1),
        "long_name": text(2),
        "short_name": text(3),
        "hw_model": int(_pb_first(fields, 5, 0) or 0),
    }


def _parse_node(raw: bytes) -> Dict[str, Any]:
    fields = _pb_fields(raw)
    number = int(_pb_first(fields, 1, 0) or 0)
    user_raw = _pb_first(fields, 2, b"")
    user = _parse_user(bytes(user_raw)) if user_raw else {}
    return {
        "node_id": user.get("id") or _node_id(number),
        "num": number,
        "long_name": user.get("long_name") or "",
        "short_name": user.get("short_name") or "",
        "user": user,
        "snr": _pb_float(_pb_first(fields, 4, b"")),
        "last_seen": _pb_u32(_pb_first(fields, 5, 0)),
        "channel": int(_pb_first(fields, 7, 0) or 0),
        "hops_away": int(_pb_first(fields, 9, 0) or 0),
    }


def _parse_channel(raw: bytes) -> Dict[str, Any]:
    fields = _pb_fields(raw)
    settings_raw = _pb_first(fields, 2, b"")
    settings = _pb_fields(bytes(settings_raw)) if settings_raw else []
    return {
        "index": int(_pb_first(fields, 1, 0) or 0),
        "name": bytes(_pb_first(settings, 3, b"")).decode("utf-8", errors="replace") if settings else "",
        "role": int(_pb_first(fields, 3, 0) or 0),
    }


def _parse_mesh_packet(raw: bytes) -> Dict[str, Any]:
    fields = _pb_fields(raw)
    decoded_raw = _pb_first(fields, 4, b"")
    decoded = _pb_fields(bytes(decoded_raw)) if decoded_raw else []
    portnum = int(_pb_first(decoded, 1, 0) or 0)
    payload = bytes(_pb_first(decoded, 2, b""))
    sender = _pb_u32(_pb_first(fields, 1, 0))
    destination = _pb_u32(_pb_first(fields, 2, 0))
    packet_id = _pb_u32(_pb_first(fields, 6, 0))
    received = _pb_u32(_pb_first(fields, 7, 0))
    return {
        "event_id": packet_id,
        "message_id": str(packet_id),
        "direction": "inbound",
        "channel": int(_pb_first(fields, 3, 0) or 0),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(received or time.time())),
        "from": {"node_id": _node_id(sender), "num": sender},
        "to": {"node_id": "broadcast" if destination == 0xFFFFFFFF else _node_id(destination), "num": destination},
        "text": payload.decode("utf-8", errors="replace") if portnum == 1 else "",
        "portnum": "TEXT_MESSAGE_APP" if portnum == 1 else str(portnum),
        "snr": _pb_float(_pb_first(fields, 8, b"")),
        "rssi": _pb_int32(_pb_first(fields, 12, 0)),
    }


def _encode_text_message(text: str, *, channel: int, destination: str, want_ack: bool = True) -> tuple[bytes, int]:
    packet_id = random.randint(1, 0xFFFFFFFF)
    token = str(destination or "broadcast").strip().lower()
    if token in {"", "broadcast", "all", "^all"}:
        destination_num = 0xFFFFFFFF
    elif token.startswith("!"):
        destination_num = int(token[1:], 16)
    else:
        destination_num = int(token, 0)
    data = _pb_varint(1, 1) + _pb_bytes(2, str(text).encode("utf-8"))
    packet = (
        _pb_fixed32(2, destination_num)
        + _pb_varint(3, max(0, int(channel)))
        + _pb_bytes(4, data)
        + _pb_fixed32(6, packet_id)
        + (_pb_varint(10, 1) if want_ack else b"")
    )
    return _pb_bytes(1, packet), packet_id


def _native_satellites() -> Dict[str, Dict[str, Any]]:
    try:
        from tater_voice import native_satellite

        snapshot = native_satellite.status_snapshot_sync()
    except Exception:
        return {}
    clients = snapshot.get("clients") if isinstance(snapshot, dict) else {}
    return clients if isinstance(clients, dict) else {}


def _default_echo_selector() -> str:
    for selector, row in _native_satellites().items():
        record = row if isinstance(row, dict) else {}
        capabilities = record.get("capabilities") if isinstance(record.get("capabilities"), dict) else {}
        if bool(record.get("connected")) and bool(capabilities.get("ble_gatt")):
            return str(selector)
    return ""


def _available_echo_selector(preferred: str = "") -> str:
    wanted = str(preferred or "").strip()
    fallback = ""
    for selector, row in _native_satellites().items():
        record = row if isinstance(row, dict) else {}
        capabilities = record.get("capabilities") if isinstance(record.get("capabilities"), dict) else {}
        if not bool(record.get("connected")) or not bool(capabilities.get("ble_gatt")):
            continue
        if str(selector) == wanted:
            return str(selector)
        if not fallback:
            fallback = str(selector)
    return fallback


class EchoGATTTransport:
    """Meshtastic PhoneAPI over an authenticated Echo satellite GATT link."""

    name = "echo_gatt"

    def __init__(self, *, selector: str, address: str, address_type: int, timeout: float, redis_client: Any = None) -> None:
        self.selector = str(selector or "").strip() or _default_echo_selector()
        self.address = str(address or "").strip().lower()
        self.address_type = 1 if int(address_type or 0) else 0
        self.timeout = max(3.0, float(timeout or DEFAULT_REQUEST_TIMEOUT_SECONDS))
        self.redis_client = redis_client
        self._state = _echo_session_state(self.selector, self.address)

    def _bind_session(self) -> None:
        self._state = _echo_session_state(self.selector, self.address)

    @property
    def handles(self) -> Dict[str, int]:
        return self._state.handles

    @property
    def connected(self) -> bool:
        return self._state.connected

    @connected.setter
    def connected(self, value: bool) -> None:
        self._state.connected = bool(value)

    @property
    def configured(self) -> bool:
        return self._state.configured

    @configured.setter
    def configured(self, value: bool) -> None:
        self._state.configured = bool(value)

    @property
    def config_id(self) -> int:
        return self._state.config_id

    @config_id.setter
    def config_id(self, value: int) -> None:
        self._state.config_id = int(value or 0)

    @property
    def local_node_num(self) -> int:
        return self._state.local_node_num

    @local_node_num.setter
    def local_node_num(self, value: int) -> None:
        self._state.local_node_num = int(value or 0)

    @property
    def metadata(self) -> Dict[str, Any]:
        return self._state.metadata

    @property
    def messages(self) -> List[Dict[str, Any]]:
        return self._state.messages

    @property
    def nodes(self) -> Dict[int, Dict[str, Any]]:
        return self._state.nodes

    @property
    def channels(self) -> Dict[int, Dict[str, Any]]:
        return self._state.channels

    def _reset_link_state(self) -> None:
        with self._state.lock:
            self.connected = False
            self.configured = False
            self.config_id = 0
            self.handles.clear()

    @property
    def base_url(self) -> str:
        return f"echo://{self.selector}/{self.address}" if self.selector or self.address else "echo://unselected"

    def close(self) -> None:
        # The satellite owns the link and keeps the encrypted bond warm across
        # short-lived portal actions. The core process disconnects only when a
        # different radio is selected or the satellite restarts.
        return None

    def _request(self, kind: str, **payload: Any) -> Dict[str, Any]:
        if not self.selector:
            raise RuntimeError("No connected Echo satellite advertises active Bluetooth GATT support.")
        from tater_voice import native_satellite

        body = {"t": kind, "req": _next_echo_request_id(), **payload}
        result = native_satellite.run_on_runtime_loop(
            native_satellite.send_request(
                self.selector,
                "ble.gatt",
                body,
                timeout_s=max(self.timeout, 25.0 if kind in {"connect", "pair"} else self.timeout),
            ),
            timeout=max(self.timeout + 1.0, 27.0 if kind in {"connect", "pair"} else self.timeout + 1.0),
        )
        if not isinstance(result, dict):
            raise RuntimeError("Echo GATT returned an invalid response.")
        error_code = str(result.get("error") or "")
        if not bool(result.get("ok")) and not (kind == "connect" and error_code == "already_connected"):
            if error_code in {"not_connected", "disconnected", "not_running"}:
                self._reset_link_state()
            detail = str(result.get("detail") or result.get("error") or "GATT request failed")
            raise RuntimeError(f"Echo Bluetooth {kind} failed: {detail}")
        return result

    def _ensure_selected(self) -> None:
        if not self.selector:
            self.selector = _default_echo_selector()
            self._bind_session()
        if not self.address:
            raise RuntimeError("Pair a Meshtastic radio from the Bluetooth tab first.")

    def _ensure_connected(self) -> None:
        self._ensure_selected()
        if not self.connected:
            self._request("connect", addr=self.address, addr_type=self.address_type)
            self.connected = True
        if not self.handles:
            result = self._request("services", addr=self.address)
            self._remember_handles(list(result.get("services") or []))
        if not self.configured:
            self._start_config()

    def _remember_handles(self, services: List[Any]) -> None:
        for service in services:
            if not isinstance(service, dict) or str(service.get("uuid") or "").lower() != MESHTASTIC_SERVICE_UUID:
                continue
            for characteristic in service.get("chars") or []:
                if not isinstance(characteristic, dict):
                    continue
                uuid = str(characteristic.get("uuid") or "").lower()
                value_handle = int(characteristic.get("value_handle") or 0)
                if uuid == MESHTASTIC_FROMRADIO_UUID:
                    self.handles["fromradio"] = value_handle
                elif uuid == MESHTASTIC_TORADIO_UUID:
                    self.handles["toradio"] = value_handle
                elif uuid == MESHTASTIC_FROMNUM_UUID:
                    self.handles["fromnum"] = value_handle
                    for descriptor in characteristic.get("descs") or []:
                        if isinstance(descriptor, dict) and str(descriptor.get("uuid") or "").lower() == GATT_CCCD_UUID:
                            self.handles["fromnum_cccd"] = int(descriptor.get("handle") or 0)
        missing = [name for name in ("fromradio", "toradio", "fromnum") if not self.handles.get(name)]
        if missing:
            raise RuntimeError(f"Selected BLE device is not a Meshtastic radio (missing {', '.join(missing)}).")
        if self.handles.get("fromnum_cccd"):
            self._write(self.handles["fromnum_cccd"], b"\x01\x00")

    def _read(self, handle: int) -> bytes:
        result = self._request("read", addr=self.address, handle=int(handle))
        value = result.get("value")
        if not value:
            return b""
        return base64.b64decode(str(value), validate=True)

    def _write(self, handle: int, value: bytes) -> None:
        self._request(
            "write",
            addr=self.address,
            handle=int(handle),
            value=base64.b64encode(bytes(value)).decode("ascii"),
            response=True,
        )

    def _start_config(self) -> None:
        # Keep one request id until the radio acknowledges config_complete.
        # Reissuing want_config_id on every portal refresh can restart a slow
        # config dump indefinitely on a busy mesh.
        if not self.config_id:
            self.config_id = random.randint(1, 0xFFFFFFFF)
            self._write(self.handles["toradio"], _pb_varint(3, self.config_id))
        deadline = time.monotonic() + max(5.0, self.timeout)
        while time.monotonic() < deadline and not self.configured:
            raw = self._read(self.handles["fromradio"])
            if raw:
                self._ingest_fromradio(raw)
                continue
            time.sleep(0.1)
        # Some firmware sends config_complete after the first polling window;
        # retain the populated snapshot and continue draining next refresh.

    def _drain(self, *, limit: int = 256) -> None:
        for _ in range(max(1, limit)):
            raw = self._read(self.handles["fromradio"])
            if not raw:
                return
            self._ingest_fromradio(raw)

    def _ingest_fromradio(self, raw: bytes) -> None:
        fields = _pb_fields(raw)
        packet = _pb_first(fields, 2, b"")
        if packet:
            message = _parse_mesh_packet(bytes(packet))
            if message.get("portnum") == "TEXT_MESSAGE_APP":
                sender_num = int((message.get("from") or {}).get("num") or 0)
                node = self.nodes.get(sender_num) or {}
                message["from"] = {**(message.get("from") or {}), **{k: v for k, v in node.items() if k in {"node_id", "long_name", "short_name"}}}
                self.messages.append(message)
                del self.messages[:-1000]
        my_info = _pb_first(fields, 3, b"")
        if my_info:
            self.local_node_num = int(_pb_first(_pb_fields(bytes(my_info)), 1, 0) or 0)
        node_info = _pb_first(fields, 4, b"")
        if node_info:
            node = _parse_node(bytes(node_info))
            self.nodes[int(node.get("num") or 0)] = node
        complete = int(_pb_first(fields, 7, 0) or 0)
        if complete and complete == self.config_id:
            self.configured = True
        channel = _pb_first(fields, 10, b"")
        if channel:
            parsed = _parse_channel(bytes(channel))
            self.channels[int(parsed.get("index") or 0)] = parsed
        metadata = _pb_first(fields, 13, b"")
        if metadata:
            metadata_fields = _pb_fields(bytes(metadata))
            self.metadata["firmware_version"] = bytes(_pb_first(metadata_fields, 1, b"")).decode("utf-8", errors="replace")

    def get_status(self) -> Dict[str, Any]:
        self._ensure_selected()
        with self._state.lock:
            self._ensure_connected()
            self._drain()
            local = self.nodes.get(self.local_node_num) or {"node_id": _node_id(self.local_node_num) if self.local_node_num else ""}
            return {
                "ok": True,
                "connected": self.connected,
                "transport": self.name,
                "selector": self.selector,
                "device_address": self.address,
                "local_node": local,
                **self.metadata,
            }

    def get_messages(self, *, since_id: int = 0, limit: int = 50) -> Dict[str, Any]:
        with self._state.lock:
            rows = [row for row in self.messages if int(row.get("event_id") or 0) > int(since_id or 0)]
            return {"ok": True, "messages": rows[-max(1, min(1000, int(limit or 50))):]}

    def get_channels(self) -> Dict[str, Any]:
        with self._state.lock:
            return {"ok": True, "channels": [self.channels[key] for key in sorted(self.channels)]}

    def get_nodes(self) -> Dict[str, Any]:
        with self._state.lock:
            return {"ok": True, "nodes": list(self.nodes.values())}

    def send_message(self, *, text: str, channel: int, destination: str) -> Dict[str, Any]:
        body = str(text or "").strip()
        if not body:
            raise ValueError("Message text is required.")
        self._ensure_selected()
        with self._state.lock:
            self._ensure_connected()
            encoded, packet_id = _encode_text_message(body, channel=int(channel or 0), destination=destination)
            self._write(self.handles["toradio"], encoded)
            outbound = {
                "event_id": packet_id,
                "message_id": str(packet_id),
                "direction": "outbound",
                "channel": int(channel or 0),
                "timestamp": _iso_now(),
                "from": self.nodes.get(self.local_node_num) or {"node_id": _node_id(self.local_node_num)},
                "to": {"node_id": str(destination or "broadcast")},
                "text": body,
                "portnum": "TEXT_MESSAGE_APP",
            }
            self.messages.append(outbound)
            del self.messages[:-1000]
            return {"ok": True, "connected": True, "message": outbound}

    def scan_devices(self) -> Dict[str, Any]:
        from tater_voice import native_ble

        snapshot = native_ble.snapshot(max_age_s=30.0, include_observations=True, limit=500)
        observations = snapshot.get("observations") if isinstance(snapshot, dict) else []
        address_types: Dict[str, int] = {}
        for row in observations or []:
            if isinstance(row, dict):
                address_types[str(row.get("address") or "").lower()] = int(row.get("address_type") or 0)
        devices: List[Dict[str, Any]] = []
        for row in snapshot.get("devices") or []:
            if not isinstance(row, dict):
                continue
            name = str(row.get("advertised_name") or row.get("display_name") or "").strip()
            uuids = {str(value).lower() for value in row.get("service_uuids") or []}
            if MESHTASTIC_SERVICE_UUID not in uuids and "meshtastic" not in name.lower():
                continue
            address = str(row.get("address") or "").lower()
            devices.append(
                {
                    "name": name or "Meshtastic radio",
                    "address": address,
                    "address_type": address_types.get(address, 1),
                    "selector": str(row.get("strongest_selector") or self.selector or ""),
                    "rssi": int(row.get("strongest_rssi") or -127),
                    "room": str(row.get("strongest_room") or ""),
                }
            )
        return {"ok": True, "transport": self.name, "finished_at": _iso_now(), "devices": devices, "count": len(devices)}

    def _save_selection(self) -> None:
        store = self.redis_client if self.redis_client is not None else _default_redis_client()
        if store is None:
            return
        values = {
            "transport": "echo",
            "echo_selector": self.selector,
            "device_address": self.address,
            "device_address_type": str(self.address_type),
        }
        try:
            store.hset("meshtastic_core_settings", mapping=values)
        except TypeError:
            for key, value in values.items():
                store.hset("meshtastic_core_settings", key, value)

    def configure_pairing(
        self,
        *,
        device_name: str,
        device_address: str,
        pin: str,
        selector: str = "",
        address_type: int = 1,
    ) -> Dict[str, Any]:
        del device_name
        self.selector = str(selector or self.selector or _default_echo_selector()).strip()
        self.address = str(device_address or self.address).strip().lower()
        self.address_type = 1 if int(address_type or 0) else 0
        self._bind_session()
        with self._state.lock:
            self._reset_link_state()
            self._ensure_selected()
            try:
                self._request("connect", addr=self.address, addr_type=self.address_type)
            except RuntimeError as exc:
                # If the peripheral cleared its bond, reconnecting with the saved
                # LTK fails before a new pairing can begin. A user who supplied a
                # new PIN has explicitly requested re-pairing, so discard only
                # this radio's stale satellite-side key and try once more.
                message = str(exc).lower()
                if "restore bond" not in message and "encryption failed" not in message:
                    raise
                self._request("forget", addr=self.address)
                self._request("connect", addr=self.address, addr_type=self.address_type)
            self.connected = True
            result = self._request("pair", addr=self.address, pin=str(pin))
            if not (result.get("bonded") and result.get("encrypted") and result.get("authenticated")):
                raise RuntimeError("Echo satellite did not confirm an authenticated encrypted bond.")
            self._save_selection()
            return {"ok": True, "bonded": True, "encrypted": True, "authenticated": True, "restart_required": False}


class MeshtasticCoreClient:
    """Transport-independent API shared by the core UI and Tater portal."""

    def __init__(self, transport: MeshtasticTransport) -> None:
        self.transport = transport

    @property
    def transport_name(self) -> str:
        return str(getattr(self.transport, "name", "unknown") or "unknown")

    @property
    def base_url(self) -> str:
        """Compatibility label used by the existing portal's diagnostics."""
        return str(getattr(self.transport, "base_url", self.transport_name) or self.transport_name)

    def close(self) -> None:
        self.transport.close()

    def get_status(self) -> Dict[str, Any]:
        return self.transport.get_status()

    def get_messages(self, *, since_id: int, limit: int = 50) -> Dict[str, Any]:
        return self.transport.get_messages(since_id=since_id, limit=limit)

    def get_channels(self) -> Dict[str, Any]:
        return self.transport.get_channels()

    def get_nodes(self) -> Dict[str, Any]:
        return self.transport.get_nodes()

    def send_message(self, *, text: str, channel: int, destination: str) -> Dict[str, Any]:
        return self.transport.send_message(text=text, channel=channel, destination=destination)

    def scan_devices(self) -> Dict[str, Any]:
        return self.transport.scan_devices()

    def configure_pairing(
        self,
        *,
        device_name: str,
        device_address: str,
        pin: str,
        selector: str = "",
        address_type: int = 1,
    ) -> Dict[str, Any]:
        return self.transport.configure_pairing(
            device_name=device_name,
            device_address=device_address,
            pin=pin,
            selector=selector,
            address_type=address_type,
        )


@dataclass(frozen=True)
class CoreConnectionSettings:
    transport: str = "auto"
    echo_selector: str = ""
    device_address: str = ""
    device_address_type: int = 1
    bridge_url: str = DEFAULT_BRIDGE_URL
    api_token: str = ""
    request_timeout_sec: float = DEFAULT_REQUEST_TIMEOUT_SECONDS
    refresh_interval_sec: float = DEFAULT_REFRESH_INTERVAL_SECONDS
    history_limit: int = DEFAULT_HISTORY_LIMIT


def _plain_hash(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    result: Dict[str, Any] = {}
    for key, value in raw.items():
        clean_key = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
        clean_value = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
        result[clean_key] = clean_value
    return result


def _read_settings_hash(redis_client: Any, key: str) -> Dict[str, Any]:
    if redis_client is None:
        return {}
    try:
        return _plain_hash(redis_client.hgetall(key) or {})
    except Exception:
        return {}


def _default_redis_client() -> Any:
    try:
        from helpers import redis_client

        return redis_client
    except Exception:
        return None


def _float_value(value: Any, default: float, *, minimum: float, maximum: float) -> float:
    try:
        parsed = float(str(value).strip())
    except Exception:
        parsed = float(default)
    return max(minimum, min(maximum, parsed))


def _int_value(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(str(value).strip()))
    except Exception:
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def resolve_connection_settings(
    *,
    redis_client: Any = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> CoreConnectionSettings:
    store = redis_client if redis_client is not None else _default_redis_client()
    merged: Dict[str, Any] = {
        "transport": "auto",
        "echo_selector": "",
        "device_address": "",
        "device_address_type": 1,
        "bridge_url": DEFAULT_BRIDGE_URL,
        "api_token": "",
        "request_timeout_sec": DEFAULT_REQUEST_TIMEOUT_SECONDS,
        "refresh_interval_sec": DEFAULT_REFRESH_INTERVAL_SECONDS,
        "history_limit": DEFAULT_HISTORY_LIMIT,
    }
    # Existing portal settings keep current installations working. Core settings
    # take ownership once the user saves them from the Cores screen.
    merged.update(_read_settings_hash(store, "meshtastic_portal_settings"))
    merged.update(_read_settings_hash(store, "meshtastic_core_settings"))
    if isinstance(overrides, dict):
        merged.update({key: value for key, value in overrides.items() if value not in (None, "")})

    return CoreConnectionSettings(
        transport=str(merged.get("transport") or "auto").strip().lower(),
        echo_selector=str(merged.get("echo_selector") or "").strip(),
        device_address=str(merged.get("device_address") or "").strip(),
        device_address_type=_int_value(merged.get("device_address_type"), 1, minimum=0, maximum=1),
        bridge_url=str(merged.get("bridge_url") or DEFAULT_BRIDGE_URL).strip() or DEFAULT_BRIDGE_URL,
        api_token=str(merged.get("api_token") or "").strip(),
        request_timeout_sec=_float_value(
            merged.get("request_timeout_sec"),
            DEFAULT_REQUEST_TIMEOUT_SECONDS,
            minimum=2.0,
            maximum=120.0,
        ),
        refresh_interval_sec=_float_value(
            merged.get("refresh_interval_sec"),
            DEFAULT_REFRESH_INTERVAL_SECONDS,
            minimum=1.0,
            maximum=60.0,
        ),
        history_limit=_int_value(
            merged.get("history_limit"),
            DEFAULT_HISTORY_LIMIT,
            minimum=10,
            maximum=1000,
        ),
    )


def build_portal_client(
    *,
    redis_client: Any = None,
    overrides: Optional[Dict[str, Any]] = None,
    session: Optional[requests.Session] = None,
) -> MeshtasticCoreClient:
    settings = resolve_connection_settings(redis_client=redis_client, overrides=overrides)
    transport = settings.transport if settings.transport in {"auto", "echo", "bridge"} else "auto"
    available_echo = _available_echo_selector(settings.echo_selector)
    # In automatic mode an available Echo must also own the initial scan;
    # requiring a saved address here would make it impossible to migrate off
    # the compatibility bridge through the portal itself.
    use_echo = transport == "echo" or (transport == "auto" and bool(available_echo))
    if use_echo:
        return MeshtasticCoreClient(
            EchoGATTTransport(
                selector=settings.echo_selector if transport == "echo" else available_echo,
                address=settings.device_address,
                address_type=settings.device_address_type,
                timeout=settings.request_timeout_sec,
                redis_client=redis_client,
            )
        )
    return MeshtasticCoreClient(
        BridgeHTTPTransport(
            base_url=settings.bridge_url,
            api_token=settings.api_token,
            timeout=settings.request_timeout_sec,
            session=session,
        )
    )


_state_lock = threading.RLock()
_state: Dict[str, Any] = {
    "running": False,
    "transport": "bridge_http",
    "last_refresh": "",
    "last_error": "",
    "status": {},
    "messages": [],
    "channels": [],
    "nodes": [],
    "scan": {"devices": [], "finished_at": "", "error": ""},
    "pairing": {"device_name": "", "device_address": "", "configured_at": ""},
}


def _iso_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _state_update(values: Dict[str, Any]) -> None:
    with _state_lock:
        _state.update(values)


def _state_snapshot() -> Dict[str, Any]:
    with _state_lock:
        snapshot = dict(_state)
        snapshot["status"] = dict(_state.get("status") or {})
        snapshot["messages"] = list(_state.get("messages") or [])
        snapshot["channels"] = list(_state.get("channels") or [])
        snapshot["nodes"] = list(_state.get("nodes") or [])
        snapshot["scan"] = dict(_state.get("scan") or {})
        snapshot["scan"]["devices"] = list(snapshot["scan"].get("devices") or [])
        snapshot["pairing"] = dict(_state.get("pairing") or {})
        return snapshot


def _refresh(client: MeshtasticCoreClient, *, history_limit: int) -> None:
    status = client.get_status()
    messages_payload = client.get_messages(since_id=0, limit=history_limit)
    channels_payload = client.get_channels()
    nodes_payload = client.get_nodes()
    _state_update(
        {
            "transport": client.transport_name,
            "last_refresh": _iso_now(),
            "last_error": "",
            "status": status,
            "messages": list(messages_payload.get("messages") or []),
            "channels": list(channels_payload.get("channels") or []),
            "nodes": list(nodes_payload.get("nodes") or []),
        }
    )


def run(stop_event: Optional[threading.Event] = None) -> None:
    settings = resolve_connection_settings()
    client = build_portal_client()
    last_logged_error = ""
    last_error_log_at = 0.0
    _state_update({"running": True, "transport": client.transport_name, "last_error": ""})
    logger.info("[Meshtastic Core] started with %s transport", client.transport_name)
    try:
        while not (stop_event and stop_event.is_set()):
            try:
                _refresh(client, history_limit=settings.history_limit)
                if last_logged_error:
                    logger.info("[Meshtastic Core] transport recovered")
                    last_logged_error = ""
                    last_error_log_at = 0.0
            except Exception as exc:
                message = str(exc).strip() or type(exc).__name__
                _state_update({"last_error": message, "last_refresh": _iso_now()})
                now = time.monotonic()
                if message != last_logged_error or now - last_error_log_at >= 300.0:
                    logger.warning("[Meshtastic Core] refresh failed: %s", message)
                    last_logged_error = message
                    last_error_log_at = now
            if stop_event:
                stop_event.wait(settings.refresh_interval_sec)
            else:
                time.sleep(settings.refresh_interval_sec)
    finally:
        client.close()
        _state_update({"running": False})
        logger.info("[Meshtastic Core] stopped")


def _text(value: Any) -> str:
    return str("" if value is None else value).strip()


def _record(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first_text(row: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(row.get(key))
        if value:
            return value
    return ""


def _display_name(message: Dict[str, Any]) -> str:
    sender = message.get("from") if isinstance(message.get("from"), dict) else {}
    return _text(
        sender.get("long_name")
        or sender.get("short_name")
        or sender.get("node_id")
        or message.get("direction")
        or "Mesh"
    )


def _channel_options(channels: List[Any]) -> List[Dict[str, str]]:
    options: List[Dict[str, str]] = []
    seen = set()
    for position, raw in enumerate(channels):
        row = _record(raw)
        channel = _first_text(row, "index", "channel", "id") or str(position)
        if channel in seen:
            continue
        seen.add(channel)
        label = _first_text(row, "name", "long_name", "label") or f"Channel {channel}"
        options.append({"value": channel, "label": f"{label} ({channel})"})
    if "0" not in seen:
        options.insert(0, {"value": "0", "label": "Primary (0)"})
    return options


def _status_item(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    status = _record(snapshot.get("status"))
    local_node = _record(status.get("local_node"))
    connected = bool(status.get("connected"))
    device = _first_text(local_node, "long_name", "short_name", "node_id")
    if not device:
        device = _first_text(status, "device_name", "device", "port")
    detail = _text(snapshot.get("last_error"))
    if not detail:
        detail = (
            f"Last refreshed {_text(snapshot.get('last_refresh'))}."
            if snapshot.get("last_refresh")
            else "Waiting for the first radio refresh."
        )
    return {
        "id": "status:connection",
        "group": "status",
        "title": "Meshtastic radio",
        "subtitle": "Connected" if connected else "Disconnected",
        "detail": detail,
        "hero_badges": [
            {"label": "CONNECTED" if connected else "OFFLINE", "tone": "good" if connected else "warn"},
            {"label": _text(snapshot.get("transport")) or "unknown transport", "tone": "muted"},
            *([{"label": device, "tone": "muted"}] if device else []),
        ],
        "run_action": "refresh",
        "run_label": "Refresh now",
    }


def _compose_item(channels: List[Any]) -> Dict[str, Any]:
    return {
        "id": "compose:message",
        "group": "compose",
        "title": "Send a mesh message",
        "subtitle": "Messages use the same connection and history as the Meshtastic portal.",
        "fields": [
            {
                "key": "text",
                "label": "Message",
                "type": "textarea",
                "value": "",
                "rows": 4,
                "placeholder": "Type a message for the mesh…",
                "full_width": True,
            },
            {
                "key": "channel",
                "label": "Channel",
                "type": "select",
                "value": "0",
                "options": _channel_options(channels),
            },
            {
                "key": "destination",
                "label": "Destination",
                "type": "text",
                "value": "broadcast",
                "placeholder": "broadcast or node ID",
            },
        ],
        "save_action": "send_message",
        "save_label": "Send message",
        "save_success_text": "Message queued for Meshtastic.",
    }


def _message_items(messages: List[Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for position, raw in enumerate(reversed(messages)):
        row = _record(raw)
        event_id = _first_text(row, "event_id", "message_id", "id") or str(position)
        direction = _first_text(row, "direction") or "inbound"
        channel = _first_text(row, "channel") or "0"
        timestamp = _first_text(row, "timestamp", "received_at", "created_at")
        subtitle_parts = [direction.title(), f"channel {channel}"]
        if timestamp:
            subtitle_parts.append(timestamp)
        items.append(
            {
                "id": f"message:{event_id}",
                "group": "messages",
                "title": _display_name(row),
                "subtitle": " · ".join(subtitle_parts),
                "detail": _first_text(row, "text") or "(empty message)",
                "hero_badges": [
                    {
                        "label": direction.upper(),
                        "tone": "good" if direction.lower() == "outbound" else "muted",
                    }
                ],
            }
        )
    return items


def _node_items(nodes: List[Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for position, raw in enumerate(nodes):
        row = _record(raw)
        user = _record(row.get("user"))
        node_id = _first_text(row, "node_id", "id", "num") or _first_text(user, "id") or str(position)
        title = (
            _first_text(row, "long_name", "short_name", "name")
            or _first_text(user, "longName", "shortName", "long_name", "short_name")
            or node_id
        )
        last_seen = _first_text(row, "last_seen", "last_heard", "lastHeard") or "Unknown"
        signal = _first_text(row, "snr", "rssi") or "Unknown"
        hops = _first_text(row, "hops_away", "hopsAway", "hops") or "Unknown"
        items.append(
            {
                "id": f"node:{node_id}",
                "group": "nodes",
                "title": title,
                "subtitle": node_id,
                "summary_rows": [
                    {"label": "Last seen", "value": last_seen},
                    {"label": "Signal", "value": signal},
                    {"label": "Hops", "value": hops},
                ],
            }
        )
    return items


def _pairing_items(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    scan = _record(snapshot.get("scan"))
    pairing = _record(snapshot.get("pairing"))
    devices = list(scan.get("devices") or [])
    error = _text(scan.get("error"))
    finished_at = _text(scan.get("finished_at"))
    detail = error or (
        f"Last scan finished {finished_at}." if finished_at else "Scan for nearby Meshtastic Bluetooth radios."
    )
    items: List[Dict[str, Any]] = [
        {
            "id": "pairing:scan",
            "group": "pairing_controls",
            "title": "Discover Bluetooth radios",
            "subtitle": f"{len(devices)} device{'s' if len(devices) != 1 else ''} found",
            "detail": detail,
            "run_action": "scan_devices",
            "run_label": "Scan for devices",
        }
    ]
    configured_name = _first_text(pairing, "device_name", "device_address")
    if configured_name:
        items.append(
            {
                "id": "pairing:last",
                "group": "pairing_controls",
                "title": "Last configured radio",
                "subtitle": configured_name,
                "detail": _text(pairing.get("configured_at")),
            }
        )
    for position, raw in enumerate(devices):
        row = _record(raw)
        name = _first_text(row, "name", "device_name", "local_name") or "Meshtastic radio"
        address = _first_text(row, "address", "device_address", "mac")
        selector = _first_text(row, "selector", "echo_selector")
        address_type = int(row.get("address_type") or 0)
        identity = address or f"scan-{position}"
        rssi = _first_text(row, "rssi", "signal")
        items.append(
            {
                "id": f"pairing:device:{identity}",
                "group": "pairing_devices",
                "title": name,
                "subtitle": address or "Address unavailable",
                "detail": f"Signal: {rssi}" if rssi else "Enter the radio's six-digit PIN to pair securely.",
                "fields": [
                    {"key": "device_name", "label": "Device name", "type": "hidden", "value": name},
                    {"key": "device_address", "label": "Device address", "type": "hidden", "value": address},
                    {"key": "selector", "label": "Echo satellite", "type": "hidden", "value": selector},
                    {"key": "address_type", "label": "Address type", "type": "hidden", "value": str(address_type)},
                    {
                        "key": "pin",
                        "label": "Six-digit Bluetooth PIN",
                        "type": "password",
                        "value": "",
                        "placeholder": "000000",
                        "description": "The PIN is submitted only for pairing and is never returned to the browser.",
                    },
                ],
                "save_action": "pair_device",
                "save_label": "Pair securely",
            }
        )
    return items


def get_htmlui_tab_data(
    *,
    redis_client: Any = None,
    core_key: str = "meshtastic_core",
    core_tab: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    del redis_client, core_key, core_tab
    snapshot = _state_snapshot()
    status = snapshot.get("status") or {}
    messages = snapshot.get("messages") or []
    connected = bool(status.get("connected"))
    item_forms = [
        _status_item(snapshot),
        _compose_item(list(snapshot.get("channels") or [])),
        *_message_items(list(messages)),
        *_node_items(list(snapshot.get("nodes") or [])),
        *_pairing_items(snapshot),
    ]

    return {
        "summary": "Meshtastic chat and radio management powered by the Tater core.",
        "stats": [
            {"label": "Radio", "value": "Connected" if connected else "Disconnected"},
            {"label": "Transport", "value": snapshot.get("transport") or "unknown"},
            {"label": "Messages", "value": len(messages)},
            {"label": "Nodes", "value": len(snapshot.get("nodes") or [])},
        ],
        "items": [],
        "empty_message": "No Meshtastic messages have been received.",
        "ui": {
            "kind": "settings_manager",
            "title": "Meshtastic",
            "live_updates": True,
            "poll_interval_ms": 2000,
            "persistent_item_groups": ["status"],
            "default_tab": "chat",
            "manager_tabs": [
                {
                    "key": "chat",
                    "label": "Chat",
                    "source": "grouped_items",
                    "groups": [
                        {"key": "compose", "label": "Compose", "item_group": "compose"},
                        {
                            "key": "history",
                            "label": "History",
                            "item_group": "messages",
                            "page_size": 50,
                            "empty_message": "No Meshtastic messages have been received.",
                        },
                    ],
                },
                {
                    "key": "nodes",
                    "label": "Nodes",
                    "source": "items",
                    "item_group": "nodes",
                    "empty_message": "No mesh nodes are available yet.",
                },
                {
                    "key": "bluetooth",
                    "label": "Bluetooth pairing",
                    "source": "grouped_items",
                    "groups": [
                        {"key": "scan", "label": "Scan", "item_group": "pairing_controls"},
                        {
                            "key": "devices",
                            "label": "Devices",
                            "item_group": "pairing_devices",
                            "empty_message": "Run a scan to find nearby Meshtastic radios.",
                        },
                    ],
                },
            ],
            "item_forms": item_forms,
        },
    }


def _require_pin(raw: Any) -> str:
    pin = str(raw or "").strip().replace(" ", "")
    if not re.fullmatch(r"\d{6}", pin):
        raise ValueError("Meshtastic Bluetooth PIN must contain exactly six digits.")
    return pin


def _fresh_client(redis_client: Any = None) -> MeshtasticCoreClient:
    return build_portal_client(redis_client=redis_client)


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client: Any = None,
    core_key: str = "meshtastic_core",
) -> Dict[str, Any]:
    del core_key
    name = str(action or "").strip().lower()
    body = payload if isinstance(payload, dict) else {}
    values = body.get("values") if isinstance(body.get("values"), dict) else body
    client = _fresh_client(redis_client)
    try:
        if name == "refresh":
            settings = resolve_connection_settings(redis_client=redis_client)
            _refresh(client, history_limit=settings.history_limit)
            return {"ok": True, "message": "Meshtastic portal refreshed."}

        if name == "send_message":
            text = str(values.get("text") or "").strip()
            if not text:
                raise ValueError("Message text is required.")
            result = client.send_message(
                text=text,
                channel=int(values.get("channel") or 0),
                destination=str(values.get("destination") or "broadcast").strip() or "broadcast",
            )
            return {"ok": True, "message": "Message queued for Meshtastic.", "result": result}

        if name == "scan_devices":
            result = client.scan_devices()
            devices = list(result.get("devices") or [])
            _state_update(
                {
                    "scan": {
                        "devices": devices,
                        "finished_at": str(result.get("finished_at") or _iso_now()),
                        "error": "",
                    }
                }
            )
            return {
                "ok": True,
                "message": f"Found {len(devices)} Meshtastic Bluetooth device{'s' if len(devices) != 1 else ''}.",
                "devices": devices,
            }

        if name == "pair_device":
            pin = _require_pin(values.get("pin"))
            device_name = str(values.get("device_name") or "").strip()
            device_address = str(values.get("device_address") or "").strip()
            if not device_name and not device_address:
                raise ValueError("Select a Meshtastic Bluetooth device before pairing.")
            result = client.configure_pairing(
                device_name=device_name,
                device_address=device_address,
                pin=pin,
                selector=str(values.get("selector") or "").strip(),
                address_type=int(values.get("address_type") or 0),
            )
            _state_update(
                {
                    "pairing": {
                        "device_name": device_name,
                        "device_address": device_address,
                        "configured_at": _iso_now(),
                    }
                }
            )
            # Never return the compatibility bridge's settings payload because it
            # may contain the submitted PIN or API token.
            restart_required = bool(result.get("restart_required"))
            suffix = " Restart the compatibility bridge to apply it." if restart_required else ""
            return {
                "ok": True,
                "message": f"Bluetooth pairing configured for {device_name or device_address}.{suffix}",
                "restart_required": restart_required,
            }

        raise ValueError(f"Unsupported Meshtastic core action: {name or '(empty)'}")
    except Exception as exc:
        if name == "scan_devices":
            current = _state_snapshot().get("scan") or {}
            _state_update(
                {
                    "scan": {
                        "devices": list(current.get("devices") or []),
                        "finished_at": str(current.get("finished_at") or ""),
                        "error": str(exc).strip() or type(exc).__name__,
                    }
                }
            )
        raise
    finally:
        client.close()
