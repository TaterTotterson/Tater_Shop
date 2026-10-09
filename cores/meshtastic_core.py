from __future__ import annotations

import logging
import base64
import json
import random
import re
import struct
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol
from urllib.parse import urljoin

import requests


__version__ = "0.3.2"
MIN_TATER_VERSION = "187"
CORE_DESCRIPTION = (
    "Run Meshtastic chat, history, node status, Bluetooth discovery, and secure pairing "
    "through a transport-neutral Tater core."
)
TAGS = ["radio", "mesh", "offgrid", "bluetooth"]

logger = logging.getLogger("meshtastic.core")

DEFAULT_BRIDGE_URL = "http://127.0.0.1:8433"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 15.0
ECHO_GATT_LONG_REQUEST_TIMEOUT_SECONDS = 40.0
DEFAULT_REFRESH_INTERVAL_SECONDS = 3.0
DEFAULT_HISTORY_LIMIT = 100
MAX_PERSISTED_MESSAGES = 1000
MESSAGE_HISTORY_KEY = "meshtastic_core_history"

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

    def unpair_device(self) -> Dict[str, Any]: ...


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

    def unpair_device(self) -> Dict[str, Any]:
        raise RuntimeError("Unpairing is available for radios connected through an Echo satellite.")


MESHTASTIC_SERVICE_UUID = "6ba1b218-15a8-461f-9fa8-5dcae273eafd"
MESHTASTIC_FROMRADIO_UUID = "2c55e69e-4993-11ed-b878-0242ac120002"
MESHTASTIC_TORADIO_UUID = "f75c76d2-129e-4dad-a1dd-7866124401e7"
MESHTASTIC_FROMNUM_UUID = "ed9da18c-a800-4f66-a670-aa7547e34453"
GATT_CCCD_UUID = "00002902-0000-1000-8000-00805f9b34fb"


def _advertisement_service_uuids(data_hex: Any) -> set[str]:
    """Decode service UUID lists directly from one raw BLE advertisement.

    Tater's presence inventory historically decoded 16-bit service UUIDs but
    left 128-bit AD types in the raw payload. Meshtastic advertises its PhoneAPI
    service as a 128-bit UUID, so discovery must not depend on the derived
    presence metadata being complete.
    """
    try:
        payload = bytes.fromhex(str(data_hex or "").strip())
    except ValueError:
        return set()
    found: set[str] = set()
    offset = 0
    while offset < len(payload):
        field_length = payload[offset]
        field_end = offset + field_length + 1
        if field_length < 1 or field_end > len(payload):
            break
        field_type = payload[offset + 1]
        value = payload[offset + 2 : field_end]
        if field_type in {0x02, 0x03}:
            for index in range(0, len(value) - 1, 2):
                found.add(f"{int.from_bytes(value[index:index + 2], 'little'):04x}")
        elif field_type in {0x04, 0x05}:
            for index in range(0, len(value) - 3, 4):
                found.add(f"{int.from_bytes(value[index:index + 4], 'little'):08x}")
        elif field_type in {0x06, 0x07}:
            for index in range(0, len(value) - 15, 16):
                # Multi-byte Bluetooth UUIDs are carried least-significant
                # octet first in advertising data.
                found.add(str(uuid.UUID(bytes=bytes(value[index:index + 16][::-1]))))
        offset = field_end
    return found

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
    message_keys: set[str] = field(default_factory=set)
    next_event_id: int = 1
    history_loaded: bool = False
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
        self._load_history()

    def _bind_session(self) -> None:
        self._state = _echo_session_state(self.selector, self.address)
        self._load_history()

    def _history_field(self) -> str:
        return f"messages:{self.address or 'unselected'}"

    def _history_store(self) -> Any:
        return self.redis_client if self.redis_client is not None else _default_redis_client()

    @staticmethod
    def _message_identity(message: Dict[str, Any]) -> str:
        sender = message.get("from") if isinstance(message.get("from"), dict) else {}
        message_id = str(message.get("message_id") or "").strip()
        sender_id = str(sender.get("node_id") or sender.get("num") or "").strip()
        channel = str(message.get("channel") or 0)
        if message_id and message_id != "0":
            return f"packet:{message_id}:{sender_id}:{channel}"
        return "content:{direction}:{sender}:{channel}:{timestamp}:{text}".format(
            direction=str(message.get("direction") or ""),
            sender=sender_id,
            channel=channel,
            timestamp=str(message.get("timestamp") or ""),
            text=str(message.get("text") or ""),
        )

    def _load_history(self) -> None:
        with self._state.lock:
            if self._state.history_loaded:
                return
            store = self._history_store()
            if store is None:
                return
            self._state.history_loaded = True
            try:
                raw = _read_settings_hash(store, MESSAGE_HISTORY_KEY).get(self._history_field())
                decoded = json.loads(str(raw or "[]"))
            except Exception:
                logger.warning("[Meshtastic Core] ignored unreadable persisted message history")
                return
            if not isinstance(decoded, list):
                return
            next_event_id = 1
            for raw_message in decoded[-MAX_PERSISTED_MESSAGES:]:
                if not isinstance(raw_message, dict):
                    continue
                message = dict(raw_message)
                try:
                    event_id = int(message.get("event_id") or 0)
                except (TypeError, ValueError):
                    event_id = 0
                if event_id < next_event_id:
                    event_id = next_event_id
                message["event_id"] = event_id
                identity = self._message_identity(message)
                if identity in self._state.message_keys:
                    continue
                self._state.messages.append(message)
                self._state.message_keys.add(identity)
                next_event_id = event_id + 1
            self._state.next_event_id = next_event_id

    def _persist_history(self) -> None:
        store = self._history_store()
        if store is None:
            return
        value = json.dumps(self.messages[-MAX_PERSISTED_MESSAGES:], separators=(",", ":"), ensure_ascii=False)
        try:
            try:
                store.hset(MESSAGE_HISTORY_KEY, mapping={self._history_field(): value})
            except TypeError:
                store.hset(MESSAGE_HISTORY_KEY, self._history_field(), value)
        except Exception as exc:
            logger.warning("[Meshtastic Core] could not persist message history: %s", exc)

    def _record_message(self, raw_message: Dict[str, Any]) -> Dict[str, Any]:
        message = dict(raw_message)
        identity = self._message_identity(message)
        if identity in self._state.message_keys:
            for existing in reversed(self.messages):
                if self._message_identity(existing) == identity:
                    return existing
            return message
        message["event_id"] = self._state.next_event_id
        self._state.next_event_id += 1
        self.messages.append(message)
        self._state.message_keys.add(identity)
        if len(self.messages) > MAX_PERSISTED_MESSAGES:
            removed = self.messages[:-MAX_PERSISTED_MESSAGES]
            del self.messages[:-MAX_PERSISTED_MESSAGES]
            for old in removed:
                old_identity = self._message_identity(old)
                if not any(self._message_identity(current) == old_identity for current in self.messages):
                    self._state.message_keys.discard(old_identity)
        self._persist_history()
        return message

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
        # Rook's BlueZ backend permits up to 25 seconds for connection and
        # 30 seconds for authenticated pairing. Keep the controller wait below
        # the satellite's 45-second correlation lifetime while leaving enough
        # margin for the backend to return its real result.
        request_timeout = (
            ECHO_GATT_LONG_REQUEST_TIMEOUT_SECONDS
            if kind in {"connect", "pair"}
            else self.timeout
        )
        result = native_satellite.run_on_runtime_loop(
            native_satellite.send_request(
                self.selector,
                "ble.gatt",
                body,
                timeout_s=request_timeout,
            ),
            timeout=request_timeout + 2.0,
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
                self._record_message(message)
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
            outbound = self._record_message(outbound)
            return {"ok": True, "connected": True, "message": outbound}

    def scan_devices(self) -> Dict[str, Any]:
        from tater_voice import native_ble

        snapshot = native_ble.snapshot(max_age_s=30.0, include_observations=True, limit=500)
        observations = snapshot.get("observations") if isinstance(snapshot, dict) else []
        satellites = _native_satellites()
        active_selectors = {
            str(selector)
            for selector, row in satellites.items()
            if isinstance(row, dict)
            and bool(row.get("connected"))
            and isinstance(row.get("capabilities"), dict)
            and bool(row["capabilities"].get("ble_gatt"))
        }
        address_types: Dict[str, int] = {}
        raw_service_uuids: Dict[str, set[str]] = {}
        active_observers: Dict[str, Dict[str, Any]] = {}
        for row in observations or []:
            if isinstance(row, dict):
                address = str(row.get("address") or "").lower()
                if not address:
                    continue
                address_types[address] = int(row.get("address_type") or 0)
                raw_service_uuids.setdefault(address, set()).update(
                    _advertisement_service_uuids(row.get("data"))
                )
                selector = str(row.get("selector") or "")
                if selector in active_selectors:
                    rssi = int(row.get("rssi") or -127)
                    previous = active_observers.get(address)
                    if previous is None or rssi > int(previous.get("rssi") or -127):
                        active_observers[address] = {
                            "selector": selector,
                            "rssi": rssi,
                            "room": str(row.get("room") or ""),
                        }
        devices: List[Dict[str, Any]] = []
        for row in snapshot.get("devices") or []:
            if not isinstance(row, dict):
                continue
            name = str(row.get("advertised_name") or row.get("display_name") or "").strip()
            address = str(row.get("address") or "").lower()
            uuids = {str(value).lower() for value in row.get("service_uuids") or []}
            uuids.update(raw_service_uuids.get(address, set()))
            if MESHTASTIC_SERVICE_UUID not in uuids and "meshtastic" not in name.lower():
                continue
            observer = active_observers.get(address) or {}
            selector = str(observer.get("selector") or "")
            if not selector:
                strongest = str(row.get("strongest_selector") or "")
                if strongest in active_selectors:
                    selector = strongest
                elif self.selector in active_selectors:
                    selector = self.selector
                else:
                    selector = _available_echo_selector(self.selector)
            devices.append(
                {
                    "name": name or "Meshtastic radio",
                    "address": address,
                    "address_type": address_types.get(address, 1),
                    # Pairing must be routed through a satellite that actually
                    # implements active GATT, even when a passive-only proxy
                    # happened to report the strongest advertisement.
                    "selector": selector,
                    "rssi": int(observer.get("rssi", row.get("strongest_rssi") or -127)),
                    "room": str(observer.get("room") or row.get("strongest_room") or ""),
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

    def _clear_selection(self) -> None:
        store = self.redis_client if self.redis_client is not None else _default_redis_client()
        if store is None:
            return
        values = {
            "transport": "echo",
            "echo_selector": self.selector,
            "device_address": "",
            "device_address_type": "1",
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

    def unpair_device(self) -> Dict[str, Any]:
        self._ensure_selected()
        address = self.address
        with self._state.lock:
            self._request("forget", addr=address)
            self._reset_link_state()
        self._clear_selection()
        self.address = ""
        self.address_type = 1
        self._bind_session()
        return {"ok": True, "device_address": address, "forgotten": True}


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

    def unpair_device(self) -> Dict[str, Any]:
        return self.transport.unpair_device()


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
_active_client_lock = threading.RLock()
_active_client_operation_lock = threading.RLock()
_active_client: Optional[MeshtasticCoreClient] = None


def _set_active_client(client: Optional[MeshtasticCoreClient]) -> None:
    global _active_client
    with _active_client_lock:
        _active_client = client


def _get_active_client() -> Optional[MeshtasticCoreClient]:
    with _active_client_lock:
        return _active_client


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
    with _active_client_operation_lock:
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
    _set_active_client(client)
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
        if _get_active_client() is client:
            _set_active_client(None)
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


def _chat_channels(channels: List[Any], messages: List[Any]) -> List[Dict[str, Any]]:
    records: Dict[str, Dict[str, Any]] = {}
    for position, raw in enumerate(channels):
        row = _record(raw)
        channel_id = _first_text(row, "index", "channel", "id") or str(position)
        label = _first_text(row, "name", "long_name", "label")
        records[channel_id] = {
            "id": channel_id,
            "label": label or ("Primary" if channel_id == "0" else f"Channel {channel_id}"),
            "subtitle": f"Channel {channel_id}",
            "message_count": 0,
        }
    records.setdefault(
        "0",
        {"id": "0", "label": "Primary", "subtitle": "Channel 0", "message_count": 0},
    )
    for raw in messages:
        row = _record(raw)
        channel_id = _first_text(row, "channel") or "0"
        records.setdefault(
            channel_id,
            {
                "id": channel_id,
                "label": "Primary" if channel_id == "0" else f"Channel {channel_id}",
                "subtitle": f"Channel {channel_id}",
                "message_count": 0,
            },
        )
        records[channel_id]["message_count"] += 1

    def sort_key(item: Dict[str, Any]) -> tuple[int, str]:
        value = str(item.get("id") or "")
        return (int(value) if value.isdigit() else 9999, value.lower())

    return sorted(records.values(), key=sort_key)


def _chat_messages(messages: List[Any], nodes: List[Any]) -> List[Dict[str, Any]]:
    nodes_by_id: Dict[str, Dict[str, Any]] = {}
    nodes_by_num: Dict[int, Dict[str, Any]] = {}
    for raw in nodes:
        row = _record(raw)
        node_id = _first_text(row, "node_id", "id")
        if node_id:
            nodes_by_id[node_id.lower()] = row
        try:
            number = int(row.get("num") or 0)
        except (TypeError, ValueError):
            number = 0
        if number:
            nodes_by_num[number] = row

    result: List[Dict[str, Any]] = []
    for position, raw in enumerate(messages):
        row = _record(raw)
        sender = _record(row.get("from"))
        sender_id = _first_text(sender, "node_id", "id")
        try:
            sender_num = int(sender.get("num") or 0)
        except (TypeError, ValueError):
            sender_num = 0
        node = nodes_by_num.get(sender_num) or nodes_by_id.get(sender_id.lower()) or {}
        direction = (_first_text(row, "direction") or "inbound").lower()
        sender_name = (
            _first_text(sender, "long_name", "short_name")
            or _first_text(node, "long_name", "short_name", "name")
            or ("You" if direction == "outbound" else sender_id)
            or "Mesh"
        )
        result.append(
            {
                "id": _first_text(row, "event_id", "message_id", "id") or str(position),
                "event_id": row.get("event_id") or 0,
                "message_id": _first_text(row, "message_id", "id"),
                "channel": _first_text(row, "channel") or "0",
                "direction": direction,
                "sender_name": "You" if direction == "outbound" else sender_name,
                "sender_id": sender_id,
                "recipient_id": _first_text(_record(row.get("to")), "node_id", "id"),
                "timestamp": _first_text(row, "timestamp", "received_at", "created_at"),
                "text": _first_text(row, "text"),
            }
        )
    return result


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


def _node_last_seen(raw: Any, *, now: Optional[float] = None) -> tuple[str, str, str]:
    value = _text(raw)
    try:
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
    except (TypeError, ValueError):
        timestamp = 0.0
    if timestamp <= 0:
        return "Not reported", "unknown", "No last-heard time reported."

    current = float(time.time() if now is None else now)
    age = max(0.0, current - timestamp)
    if age < 10:
        relative = "Just now"
    elif age < 60:
        relative = f"{int(age)} sec ago"
    elif age < 3600:
        minutes = max(1, int(age // 60))
        relative = f"{minutes} min ago"
    elif age < 86400:
        hours = max(1, int(age // 3600))
        relative = f"{hours} hr{'s' if hours != 1 else ''} ago"
    elif age < 604800:
        days = max(1, int(age // 86400))
        relative = f"{days} day{'s' if days != 1 else ''} ago"
    else:
        weeks = max(1, int(age // 604800))
        relative = f"{weeks} week{'s' if weeks != 1 else ''} ago"

    if age <= 900:
        freshness = "active"
    elif age <= 86400:
        freshness = "recent"
    else:
        freshness = "stale"
    exact = time.strftime("Last report %b %d, %Y at %I:%M %p.", time.localtime(timestamp))
    exact = exact.replace(" 0", " ").replace(" at 0", " at ")
    return relative, freshness, exact


def _node_signal(row: Dict[str, Any], *, is_local: bool) -> str:
    if is_local:
        return "Local radio"
    for key in ("snr", "signal_to_noise"):
        if row.get(key) not in (None, ""):
            try:
                value = float(row[key])
            except (TypeError, ValueError):
                break
            quality = "Excellent" if value >= 10 else "Good" if value >= 5 else "Fair" if value >= 0 else "Weak"
            return f"{quality} · {value:+.1f} dB"
    if row.get("rssi") not in (None, ""):
        try:
            return f"{int(float(row['rssi']))} dBm"
        except (TypeError, ValueError):
            pass
    return "Not reported"


def _node_items(nodes: List[Any], *, local_node: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    local = _record(local_node)
    local_id = _first_text(local, "node_id", "id")
    try:
        local_num = int(local.get("num") or 0)
    except (TypeError, ValueError):
        local_num = 0
    for position, raw in enumerate(nodes):
        row = _record(raw)
        user = _record(row.get("user"))
        node_id = _first_text(row, "node_id", "id", "num") or _first_text(user, "id") or str(position)
        title = (
            _first_text(row, "long_name", "short_name", "name")
            or _first_text(user, "longName", "shortName", "long_name", "short_name")
            or node_id
        )
        try:
            node_num = int(row.get("num") or 0)
        except (TypeError, ValueError):
            node_num = 0
        is_local = bool((local_id and node_id.lower() == local_id.lower()) or (local_num and node_num == local_num))
        last_seen_raw = row.get("last_seen") or row.get("last_heard") or row.get("lastHeard")
        last_seen, freshness, exact_seen = _node_last_seen(last_seen_raw)
        if is_local:
            last_seen, freshness, exact_seen = "Connected now", "active", "This is the radio connected to Tater."
        signal = _node_signal(row, is_local=is_local)
        try:
            hop_count = int(row.get("hops_away") if row.get("hops_away") is not None else row.get("hopsAway") or row.get("hops") or 0)
        except (TypeError, ValueError):
            hop_count = -1
        if is_local:
            route = "This radio"
        elif hop_count == 0:
            route = "Direct"
        elif hop_count == 1:
            route = "1 hop"
        elif 1 < hop_count < 255:
            route = f"{hop_count} hops"
        else:
            route = "Unknown"
        state_label = "LOCAL" if is_local else "ACTIVE" if freshness == "active" else "RECENT" if freshness == "recent" else "STALE"
        state_tone = "accent" if is_local else "success" if freshness == "active" else "accent" if freshness == "recent" else "muted"
        items.append(
            {
                "id": f"node:{node_id}",
                "group": "nodes",
                "card_variant": "mesh_node",
                "hide_core_key": True,
                "title": title,
                "subtitle": node_id,
                "detail": exact_seen,
                "hero_badges": [
                    {"label": state_label, "tone": state_tone},
                    {"label": route.upper(), "tone": "muted"},
                ],
                "summary_rows": [
                    {"label": "Last heard", "value": last_seen},
                    {"label": "Link quality", "value": signal},
                    {"label": "Route", "value": route},
                ],
            }
        )
    return items


def _friendly_scan_time(raw: Any) -> str:
    value = _text(raw)
    if not value:
        return "Not scanned yet"
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        relative, _, _ = _node_last_seen(parsed.timestamp())
        return relative
    except (TypeError, ValueError):
        return value


def _ble_signal(raw: Any) -> str:
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        return "Not reported"
    quality = "Strong" if value >= -60 else "Good" if value >= -75 else "Fair" if value >= -90 else "Weak"
    return f"{quality} · {value} dBm"


def _pairing_satellite_label(snapshot: Dict[str, Any], *, selector: str, address: str) -> str:
    scan = _record(snapshot.get("scan"))
    for raw in scan.get("devices") or []:
        row = _record(raw)
        row_address = _first_text(row, "address", "device_address", "mac").lower()
        if address and row_address == address.lower():
            room = _first_text(row, "room", "satellite_name")
            if room:
                return room
    satellite = _record(_native_satellites().get(selector)) if selector else {}
    return _first_text(satellite, "room", "name", "display_name") or selector or "Automatic"


def _pairing_items(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    scan = _record(snapshot.get("scan"))
    pairing = _record(snapshot.get("pairing"))
    status = _record(snapshot.get("status"))
    devices = list(scan.get("devices") or [])
    error = _text(scan.get("error"))
    finished_at = _text(scan.get("finished_at"))
    current_address = _first_text(status, "device_address") or _first_text(pairing, "device_address")
    selector = _first_text(status, "selector")
    connected = bool(status.get("connected") and current_address)
    local_node = _record(status.get("local_node"))
    radio_name = _first_text(local_node, "long_name", "short_name", "node_id") or _first_text(pairing, "device_name")
    satellite_label = _pairing_satellite_label(snapshot, selector=selector, address=current_address)
    transport = _text(snapshot.get("transport") or status.get("transport") or "unknown")
    transport_label = "Echo GATT" if transport == "echo_gatt" else "Compatibility bridge" if transport == "bridge_http" else transport

    if current_address:
        current_item: Dict[str, Any] = {
            "id": f"pairing:current:{current_address}",
            "group": "pairing_current",
            "card_variant": "pairing_current",
            "hide_core_key": True,
            "title": radio_name or "Meshtastic radio",
            "subtitle": current_address,
            "detail": (
                f"Secure Bluetooth link through {satellite_label}."
                if connected
                else "This radio is saved, but it is not currently connected."
            ),
            "hero_badges": [
                {"label": "CONNECTED" if connected else "OFFLINE", "tone": "success" if connected else "muted"},
                {"label": "PAIRED", "tone": "accent"},
            ],
            "summary_rows": [
                {"label": "Mesh ID", "value": _first_text(local_node, "node_id") or "Not reported"},
                {"label": "Satellite", "value": satellite_label},
                {"label": "Transport", "value": transport_label or "Unknown"},
            ],
        }
        if transport == "echo_gatt":
            current_item["actions"] = [
                {
                    "action": "unpair_device",
                    "label": "Unpair radio",
                    "tone": "danger",
                    "confirm": (
                        f"Unpair {radio_name or current_address}? This disconnects it and removes its saved "
                        "Bluetooth bond. You will need the six-digit PIN to pair it again."
                    ),
                    "success_text": "Meshtastic radio unpaired.",
                }
            ]
    else:
        current_item = {
            "id": "pairing:current:none",
            "group": "pairing_current",
            "card_variant": "pairing_current_empty",
            "hide_core_key": True,
            "title": "No radio paired",
            "subtitle": "Bluetooth is ready",
            "detail": "Scan for a nearby Meshtastic radio, then enter its six-digit PIN to connect securely.",
            "hero_badges": [{"label": "READY TO PAIR", "tone": "muted"}],
        }

    scan_detail = error or (
        f"Last scan completed {_friendly_scan_time(finished_at)}."
        if finished_at
        else "Search through Bluetooth-capable Echo satellites for nearby Meshtastic radios."
    )
    items: List[Dict[str, Any]] = [
        current_item,
        {
            "id": "pairing:scan",
            "group": "pairing_controls",
            "card_variant": "pairing_scan",
            "hide_core_key": True,
            "title": "Find a Meshtastic radio",
            "subtitle": f"{len(devices)} radio{'s' if len(devices) != 1 else ''} found",
            "detail": scan_detail,
            "hero_badges": [
                {"label": "BLUETOOTH", "tone": "accent"},
                {"label": "SECURE PAIRING", "tone": "muted"},
            ],
            "run_action": "scan_devices",
            "run_label": "Scan for radios",
        }
    ]
    for position, raw in enumerate(devices):
        row = _record(raw)
        name = _first_text(row, "name", "device_name", "local_name") or "Meshtastic radio"
        address = _first_text(row, "address", "device_address", "mac")
        selector = _first_text(row, "selector", "echo_selector")
        address_type = int(row.get("address_type") or 0)
        identity = address or f"scan-{position}"
        room = _first_text(row, "room") or selector or "Automatic"
        is_current = bool(address and current_address and address.lower() == current_address.lower())
        item: Dict[str, Any] = {
            "id": f"pairing:device:{identity}",
            "group": "pairing_devices",
            "card_variant": "pairing_device",
            "hide_core_key": True,
            "title": name,
            "subtitle": address or "Address unavailable",
            "detail": "This is your current radio." if is_current else "Enter the radio's six-digit PIN to pair securely.",
            "hero_badges": [
                {"label": "CONNECTED" if is_current and connected else "NEARBY", "tone": "success" if is_current and connected else "accent"},
                {"label": room.upper(), "tone": "muted"},
            ],
            "summary_rows": [
                {"label": "Signal", "value": _ble_signal(row.get("rssi", row.get("signal")))},
                {"label": "Via", "value": room},
            ],
        }
        if not is_current:
            item.update(
                {
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
                            "description": "The PIN is used only during pairing and is never returned to the browser.",
                        },
                    ],
                    "save_action": "pair_device",
                    "save_label": "Pair securely",
                }
            )
        items.append(item)
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
    channels = list(snapshot.get("channels") or [])
    nodes = list(snapshot.get("nodes") or [])
    connected = bool(status.get("connected"))
    local_node = _record(status.get("local_node"))
    item_forms = [
        *_node_items(nodes, local_node=local_node),
        *_pairing_items(snapshot),
    ]
    radio_name = _first_text(local_node, "long_name", "short_name", "node_id")

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
            "kind": "channel_chat",
            "title": "Meshtastic",
            "live_updates": True,
            "poll_interval_ms": 2000,
            "default_tab": "chat",
            "default_channel": "0",
            "status": {
                "connected": connected,
                "label": "Connected" if connected else "Disconnected",
                "transport": snapshot.get("transport") or "unknown",
                "radio_name": radio_name,
                "last_refresh": snapshot.get("last_refresh") or "",
                "error": snapshot.get("last_error") or "",
                "refresh_action": "refresh",
            },
            "channels": _chat_channels(channels, list(messages)),
            "messages": _chat_messages(list(messages), nodes),
            "composer": {
                "action": "send_message",
                "destination": "broadcast",
                "placeholder": "Message the mesh…",
                "send_label": "Send",
                "max_length": 200,
            },
            "manager_tabs": [
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
                    "featured_item_group": "pairing_current",
                    "featured_label": "Current connection",
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
    client = _get_active_client() if name in {"pair_device", "unpair_device"} else None
    close_client = client is None
    if client is None:
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
            with _active_client_operation_lock:
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

        if name == "unpair_device":
            status = _record(_state_snapshot().get("status"))
            radio_name = _first_text(_record(status.get("local_node")), "long_name", "short_name", "node_id")
            with _active_client_operation_lock:
                result = client.unpair_device()
                next_status = dict(status)
                next_status.update({"connected": False, "device_address": "", "local_node": {}})
                _state_update(
                    {
                        "last_error": "",
                        "last_refresh": _iso_now(),
                        "status": next_status,
                        "pairing": {"device_name": "", "device_address": "", "configured_at": ""},
                    }
                )
            return {
                "ok": True,
                "message": f"{radio_name or 'Meshtastic radio'} was disconnected and unpaired.",
                "forgotten": bool(result.get("forgotten")),
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
        if close_client:
            client.close()
