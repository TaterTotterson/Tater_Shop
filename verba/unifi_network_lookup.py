# verba/unifi_network_lookup.py
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from verba_base import ToolVerba
from helpers import redis_client, get_tater_name, get_tater_personality
from verba_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("unifi_network_lookup")
logger.setLevel(logging.INFO)


class UnifiNetworkLookupPlugin(ToolVerba):
    """
    UniFi Network (Official Integration API) plugin.

    Goals:
    - Fetch full data from UniFi (correct totals)
    - Precompute counts + highlights in Python (fast + accurate)
    - Send ONLY compact/top items to LLM by default (fast)
    - Still allow "list all" / "show all" style requests

    API:
      https://<console>/proxy/network/integration/v1/...
      Auth: X-API-KEY header
    """

    name = "unifi_network_lookup"
    verba_name = "UniFi Network Lookup"
    version = "1.0.2"
    min_tater_version = "59"
    pretty_name = "UniFi Network Lookup"
    tags = ["unifi", "lookup"]
    fixed_intent = "find_any"
    description = (
        "Find UniFi clients or devices by name, hostname, IP, or MAC."
    )
    verba_dec = (
        "Look up a UniFi client/device and return matching identity and network details."
    )
    when_to_use = (
        "Use when you need to find one specific UniFi client or device."
    )
    how_to_use = (
        "Set query to the lookup request (for example: what is the IP of hdhomerun), or pass target directly."
    )
    example_calls = [
        '{"function":"unifi_network_lookup","arguments":{"query":"what is the IP address of hdhomerun"}}',
        '{"function":"unifi_network_lookup","arguments":{"query":"find living room ap"}}',
        '{"function":"unifi_network_lookup","arguments":{"target":"printer"}}',
    ]
    common_needs = ["Lookup query with a client/device target name, hostname, IP, or MAC."]
    routing_keywords = [
        "unifi",
        "unifi network",
        "lookup",
        "find",
        "search",
        "ip",
        "mac",
        "client",
        "clients",
        "device",
        "devices",
    ]
    settings_category = "UniFi Network"
    platforms = ['webui', 'macos', 'voice_core', 'homeassistant', 'homekit', 'xbmc', 'discord', 'telegram', 'matrix', 'irc', 'meshtastic']

    usage = '{"function":"unifi_network_lookup","arguments":{"query":"what is the IP address of hdhomerun"}}'
    argument_schema = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Lookup request (for example: find hdhomerun, what is the IP of office printer, mac address for living room ap).",
            },
            "target": {
                "type": "string",
                "description": "Optional lookup target name/hostname/IP/MAC when query is omitted.",
            },
            "name": {
                "type": "string",
                "description": "Alias of target.",
            },
        },
        "required": [],
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking the UniFi network now. "
        "Only output that message."
    )

    required_settings = {}

    # ---- internal defaults (not exposed as settings) ----
    _DEFAULT_VERIFY_SSL = False
    _DEFAULT_TIMEOUT = 20

    # paging: we fetch ALL pages, but do it in chunks for reliability
    _PAGE_LIMIT = 200

    # LLM context caps (we still FETCH all; we just don't SEND all)
    _TOP_CLIENTS_DEFAULT = 25
    _TOP_DEVICES_DEFAULT = 25
    _TOP_MATCHES_DEFAULT = 10
    _INTENT_VALUES = {
        "summary",
        "health",
        "list_clients",
        "list_devices",
        "find_any",
        "find_client",
        "find_device",
        "count_clients_total",
        "count_clients_wired",
        "count_clients_wireless",
        "count_devices_total",
        "count_devices_offline",
    }
    missing_info_prompts = []


    # -------------------------
    # Settings / HTTP helpers
    # -------------------------
    def _get_settings(self) -> Dict[str, str]:
        return {
            "UNIFI_BASE_URL": (redis_client.get("tater:unifi_network:base_url") or "https://10.4.20.1").strip().rstrip("/"),
            "UNIFI_API_KEY": (redis_client.get("tater:unifi_network:api_key") or "").strip(),
        }

    def _base(self, s: Dict[str, str]) -> str:
        return (s.get("UNIFI_BASE_URL") or "https://10.4.20.1").rstrip("/")

    def _api_key(self, s: Dict[str, str]) -> str:
        key = (s.get("UNIFI_API_KEY") or "").strip()
        if not key:
            raise ValueError("UNIFI API key is missing. Set it in WebUI Settings -> Integrations -> UniFi Network.")
        return key

    def _headers(self, api_key: str) -> Dict[str, str]:
        return {"X-API-KEY": api_key, "Accept": "application/json"}

    def _request(self, method: str, url: str, *, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Any:
        verify = self._DEFAULT_VERIFY_SSL
        timeout = self._DEFAULT_TIMEOUT

        # Silence urllib3 "InsecureRequestWarning" when verify=False
        if not verify:
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

        r = requests.request(method, url, headers=headers, params=params, timeout=timeout, verify=verify)
        if r.status_code >= 400:
            snippet = (r.text or "")[:300]
            raise RuntimeError(f"UniFi HTTP {r.status_code} calling {url} params={params}: {snippet}")
        try:
            return r.json()
        except Exception:
            return r.text

    # -------------------------
    # UniFi Integration API calls
    # -------------------------
    def _integration_url(self, base: str, path: str) -> str:
        path = path if path.startswith("/") else f"/{path}"
        return f"{base}/proxy/network/integration{path}"

    def _get_sites(self, base: str, headers: Dict[str, str]) -> Dict[str, Any]:
        url = self._integration_url(base, "/v1/sites")
        return self._request("GET", url, headers=headers)

    def _pick_site(self, sites_payload: Dict[str, Any]) -> Tuple[str, str]:
        data = (sites_payload or {}).get("data") or []
        if not isinstance(data, list) or not data:
            raise RuntimeError("No UniFi sites returned from /v1/sites.")
        first = data[0] or {}
        site_id = (first.get("id") or "").strip()
        site_name = (first.get("name") or first.get("internalReference") or "Unknown").strip()
        if not site_id:
            raise RuntimeError("UniFi sites response missing site id.")
        return site_id, site_name or "Unknown"

    def _get_paged(self, *, base: str, headers: Dict[str, str], path: str) -> Dict[str, Any]:
        """
        Page through endpoints shaped like:
          {"offset":0,"limit":25,"count":25,"totalCount":66,"data":[...]}
        and return a combined payload with all data.
        """
        url = self._integration_url(base, path)

        all_items: List[Any] = []
        offset = 0
        total: Optional[int] = None
        max_pages = 2000  # safety guard

        for _ in range(max_pages):
            params = {"offset": str(offset), "limit": str(self._PAGE_LIMIT)}
            payload = self._request("GET", url, headers=headers, params=params)

            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected response type from {url}: {type(payload)}")

            page_data = payload.get("data") or []
            if isinstance(page_data, list) and page_data:
                all_items.extend(page_data)

            if total is None:
                try:
                    total = int(payload.get("totalCount"))
                except Exception:
                    total = None

            try:
                count = int(payload.get("count"))
            except Exception:
                count = len(page_data) if isinstance(page_data, list) else 0

            # Stop conditions
            if total is not None and len(all_items) >= total:
                break
            if count < self._PAGE_LIMIT:
                break

            offset += self._PAGE_LIMIT

        return {
            "offset": 0,
            "limit": self._PAGE_LIMIT,
            "count": len(all_items),
            "totalCount": total if total is not None else len(all_items),
            "data": all_items,
        }

    def _get_clients_all(self, base: str, headers: Dict[str, str], site_id: str) -> Dict[str, Any]:
        return self._get_paged(base=base, headers=headers, path=f"/v1/sites/{site_id}/clients")

    def _get_devices_all(self, base: str, headers: Dict[str, str], site_id: str) -> Dict[str, Any]:
        return self._get_paged(base=base, headers=headers, path=f"/v1/sites/{site_id}/devices")

    # -------------------------
    # Big-list optimization helpers
    # -------------------------
    @staticmethod
    def _q_norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip().lower())

    def _query_wants_full_list(self, q: str) -> bool:
        """
        If the user explicitly asks to list/show everything, include the full compact list.
        Otherwise we keep LLM context small for speed.
        """
        t = self._q_norm(q)
        triggers = (
            "list all", "show all", "everything", "full list", "all clients", "all devices",
            "dump", "print all"
        )
        return any(x in t for x in triggers)

    def _query_kind(self, q: str) -> str:
        """
        Lightweight intent detection to:
          - answer some questions without LLM (fast)
          - decide how to build compact context
        """
        t = self._q_norm(q)

        # "find" style
        if any(w in t for w in ("find ", "search ", "look up", "lookup", "where is", "ip address", "mac address")):
            return "find"

        # list style
        if any(w in t for w in ("list clients", "show clients", "who is online", "who's online", "clients online")):
            return "clients"
        if any(w in t for w in ("list devices", "show devices", "devices", "aps", "access points", "switches", "udm", "ups")):
            return "devices"

        # quick health
        if any(w in t for w in ("health", "status", "how's the network", "hows the network", "network ok", "internet", "wan")):
            return "health"

        # fallback
        return "general"

    def _extract_find_target(self, query: str) -> str:
        text = (query or "").strip()
        if not text:
            return ""

        patterns = (
            r"(?:ip(?:\s+address)?|mac(?:\s+address)?)\s+(?:of|for)\s+(.+)$",
            r"(?:find|search|look up|lookup|where is)\s+(.+)$",
            r"(?:client|device)\s+named\s+(.+)$",
        )
        target = ""
        for pat in patterns:
            m = re.search(pat, text, flags=re.I)
            if m:
                target = m.group(1).strip()
                break

        if not target:
            target = text

        # cleanup filler words and trailing nouns
        target = re.sub(r"^[Tt]he\s+", "", target)
        target = re.sub(r"\b(?:client|device)\b\s*$", "", target, flags=re.I)
        target = re.sub(r"\b(?:please|thanks|thank you)\b", "", target, flags=re.I)
        target = re.sub(r"[?.!,]+$", "", target).strip()
        target = re.sub(r"\s+", " ", target).strip()
        if target.lower() in {"it", "that", "this", "something"}:
            return ""
        return target

    def _fixed_intent_payload(self, query: str, args: Dict[str, Any]) -> Dict[str, Any]:
        fixed_intent = str(getattr(self, "fixed_intent", "") or "").strip().lower()
        if fixed_intent not in self._INTENT_VALUES:
            raise ValueError("Invalid fixed_intent for this UniFi Network verba.")

        target_hint = str(args.get("target") or args.get("name") or "").strip()
        payload: Dict[str, Any] = {
            "intent": fixed_intent,
            "target": "",
            "target_kind": "any",
            "full_list": self._query_wants_full_list(query),
        }

        if fixed_intent in {"find_any", "find_client", "find_device"}:
            target = target_hint or self._extract_find_target(query)
            payload["target"] = target
            if fixed_intent == "find_client":
                payload["target_kind"] = "client"
            elif fixed_intent == "find_device":
                payload["target_kind"] = "device"
        return payload

    def _client_link_counts(self, clients_payload: Dict[str, Any]) -> Dict[str, int]:
        """
        Count wired/wireless using reliable client fields.
        Prefer explicit type values like 'WIRED' / 'WIRELESS'.
        """
        data = (clients_payload or {}).get("data") or []
        wired = 0
        wireless = 0
        unknown = 0

        for c in data:
            if not isinstance(c, dict):
                continue

            t = (c.get("type") or c.get("connectionType") or c.get("linkType") or "").strip().upper()

            if t == "WIRED":
                wired += 1
                continue
            if t == "WIRELESS":
                wireless += 1
                continue

            # fallbacks some builds use
            if c.get("isWireless") is True:
                wireless += 1
                continue
            if c.get("isWired") is True:
                wired += 1
                continue

            unknown += 1

        return {
            "wired": wired,
            "wireless": wireless,
            "unknown": unknown,
            "total_listed": wired + wireless + unknown,
        }

    def _device_status_counts(self, devices_payload: Dict[str, Any]) -> Dict[str, int]:
        """
        Try to count online/offline for devices. Different builds expose different fields.
        We'll do best-effort and keep 'unknown' if we can't tell.
        """
        data = (devices_payload or {}).get("data") or []
        online = 0
        offline = 0
        unknown = 0

        for d in data:
            if not isinstance(d, dict):
                continue

            # Common-ish fields (best effort)
            state = (d.get("state") or d.get("status") or "").strip().upper()
            if state in ("ONLINE", "CONNECTED", "ADOPTED"):
                online += 1
                continue
            if state in ("OFFLINE", "DISCONNECTED", "ISOLATED"):
                offline += 1
                continue

            if d.get("isOnline") is True or d.get("online") is True:
                online += 1
                continue
            if d.get("isOnline") is False or d.get("online") is False:
                offline += 1
                continue

            unknown += 1

        return {"online": online, "offline": offline, "unknown": unknown, "total_listed": online + offline + unknown}

    @staticmethod
    def _safe_str(v: Any) -> str:
        return "" if v is None else str(v)

    def _compact_client(self, c: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": c.get("name") or c.get("hostname") or c.get("displayName") or "",
            "type": c.get("type") or c.get("connectionType") or c.get("linkType") or "",
            "ip": c.get("ipAddress") or c.get("ip") or "",
            "mac": c.get("macAddress") or c.get("mac") or "",
            "connectedAt": c.get("connectedAt") or c.get("firstSeen") or c.get("lastSeen") or "",
            "uplinkDeviceId": c.get("uplinkDeviceId") or c.get("uplink") or "",
            "id": c.get("id") or "",
        }

    def _compact_device(self, d: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": d.get("name") or d.get("displayName") or "",
            "model": d.get("model") or d.get("type") or "",
            "state": d.get("state") or d.get("status") or "",
            "ip": d.get("ipAddress") or d.get("ip") or "",
            "mac": d.get("macAddress") or d.get("mac") or "",
            "version": d.get("version") or d.get("firmwareVersion") or "",
            "id": d.get("id") or "",
        }

    def _search_clients(self, clients_payload: Dict[str, Any], needle: str, max_hits: int) -> List[Dict[str, Any]]:
        n = self._q_norm(needle)
        if not n:
            return []

        hits: List[Tuple[int, Dict[str, Any]]] = []
        for c in (clients_payload or {}).get("data") or []:
            if not isinstance(c, dict):
                continue

            name = self._q_norm(self._safe_str(c.get("name") or c.get("hostname") or c.get("displayName")))
            ip = self._q_norm(self._safe_str(c.get("ipAddress") or c.get("ip")))
            mac = self._q_norm(self._safe_str(c.get("macAddress") or c.get("mac")))

            blob = f"{name} {ip} {mac}".strip()

            score = 0
            if n == name or n == ip or n == mac:
                score = 100
            elif n and n in blob:
                score = 50
                if name.startswith(n):
                    score += 10

            if score > 0:
                hits.append((score, c))

        hits.sort(key=lambda x: x[0], reverse=True)
        return [self._compact_client(c) for _, c in hits[:max_hits]]

    def _search_devices(self, devices_payload: Dict[str, Any], needle: str, max_hits: int) -> List[Dict[str, Any]]:
        n = self._q_norm(needle)
        if not n:
            return []

        hits: List[Tuple[int, Dict[str, Any]]] = []
        for d in (devices_payload or {}).get("data") or []:
            if not isinstance(d, dict):
                continue

            name = self._q_norm(self._safe_str(d.get("name") or d.get("displayName")))
            model = self._q_norm(self._safe_str(d.get("model") or d.get("type")))
            ip = self._q_norm(self._safe_str(d.get("ipAddress") or d.get("ip")))
            mac = self._q_norm(self._safe_str(d.get("macAddress") or d.get("mac")))
            blob = f"{name} {model} {ip} {mac}".strip()

            score = 0
            if n in {name, ip, mac}:
                score = 100
            elif n and n in blob:
                score = 55
                if name.startswith(n):
                    score += 10

            if score > 0:
                hits.append((score, d))

        hits.sort(key=lambda x: x[0], reverse=True)
        return [self._compact_device(d) for _, d in hits[:max_hits]]

    def _build_compact_context(
        self,
        *,
        query: str,
        site_name: str,
        sites: Dict[str, Any],
        clients: Dict[str, Any],
        devices: Dict[str, Any],
        intent: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        intent = intent or {}
        intent_name = str(intent.get("intent") or "").strip().lower() or "summary"
        wants_full = bool(intent.get("full_list")) or self._query_wants_full_list(query)

        kind_map = {
            "list_clients": "clients",
            "list_devices": "devices",
            "find_any": "find",
            "find_client": "find",
            "find_device": "find",
            "health": "health",
        }
        kind = kind_map.get(intent_name) or self._query_kind(query)

        client_counts = self._client_link_counts(clients)
        device_counts = self._device_status_counts(devices)

        all_clients = (clients or {}).get("data") or []
        all_devices = (devices or {}).get("data") or []

        compact_clients = [self._compact_client(c) for c in all_clients if isinstance(c, dict)]
        compact_devices = [self._compact_device(d) for d in all_devices if isinstance(d, dict)]

        top_clients = compact_clients[:self._TOP_CLIENTS_DEFAULT]
        top_devices = compact_devices[:self._TOP_DEVICES_DEFAULT]

        if wants_full:
            top_clients = compact_clients
            top_devices = compact_devices

        matches: List[Dict[str, Any]] = []
        target = str(intent.get("target") or "").strip()
        if kind == "find":
            needle = target or query
            find_intent = intent_name if intent_name in {"find_any", "find_client", "find_device"} else "find_any"
            if find_intent in {"find_any", "find_client"}:
                c_hits = self._search_clients(clients, needle, self._TOP_MATCHES_DEFAULT)
                matches.extend([{**m, "kind": "client"} for m in c_hits])
            if find_intent in {"find_any", "find_device"}:
                d_hits = self._search_devices(devices, needle, self._TOP_MATCHES_DEFAULT)
                matches.extend([{**m, "kind": "device"} for m in d_hits])
            matches = matches[: self._TOP_MATCHES_DEFAULT]

        anomalies: List[str] = []
        if device_counts.get("offline", 0) > 0:
            anomalies.append(f"{device_counts['offline']} UniFi device(s) appear OFFLINE.")
        if (clients or {}).get("error"):
            anomalies.append(f"Clients fetch error: {(clients.get('error') or '')[:120]}")
        if (devices or {}).get("error"):
            anomalies.append(f"Devices fetch error: {(devices.get('error') or '')[:120]}")
        if client_counts.get("unknown", 0) > 0:
            anomalies.append(f"{client_counts['unknown']} client(s) did not report wired/wireless type.")

        facts = {
            "site": site_name,
            "intent": intent_name,
            "kind": kind,
            "wants_full_list": wants_full,
            "target": target,
            "counts": {
                "clients_total": int((clients or {}).get("totalCount") or len(compact_clients)),
                "clients_wired": client_counts["wired"],
                "clients_wireless": client_counts["wireless"],
                "clients_unknown_link": client_counts["unknown"],
                "devices_total": int((devices or {}).get("totalCount") or len(compact_devices)),
                "devices_online": device_counts["online"],
                "devices_offline": device_counts["offline"],
                "devices_unknown_status": device_counts["unknown"],
            },
            "anomalies": anomalies,
            "matches": matches,
            "top_clients": top_clients,
            "top_devices": top_devices,
            "note": (
                "Counts are computed from full fetched lists. "
                "top_clients/top_devices may be capped unless user asked for full lists."
            ),
        }

        return {"question": query, "facts": facts, "sites": sites}

    # -------------------------
    # LLM response
    # -------------------------
    @staticmethod
    def _strip_code_fences(s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
        s = re.sub(r"\s*```$", "", s)
        return s.strip()

    def _assistant_identity(self) -> Tuple[str, str]:
        first, last = get_tater_name()
        assistant_name = f"{(first or '').strip()} {(last or '').strip()}".strip() or "Tater"
        personality = (get_tater_personality() or "").strip()
        return assistant_name, personality

    def _direct_answer_if_simple(
        self, query: str, compact_ctx: Dict[str, Any], intent: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        q = self._q_norm(query)
        facts = (compact_ctx or {}).get("facts") or {}
        counts = facts.get("counts") or {}
        intent_name = str((intent or {}).get("intent") or facts.get("intent") or "").strip().lower()
        matches = facts.get("matches") or []
        target = str(facts.get("target") or "").strip()

        if intent_name == "count_clients_total":
            return f"There are {counts.get('clients_total', 0)} clients in the UniFi client list right now."
        if intent_name == "count_clients_wireless":
            return (
                f"There are {counts.get('clients_wireless', 0)} wireless clients "
                f"(and {counts.get('clients_wired', 0)} wired, {counts.get('clients_unknown_link', 0)} unknown)."
            )
        if intent_name == "count_clients_wired":
            return (
                f"There are {counts.get('clients_wired', 0)} wired clients "
                f"(and {counts.get('clients_wireless', 0)} wireless, {counts.get('clients_unknown_link', 0)} unknown)."
            )
        if intent_name == "count_devices_total":
            return f"There are {counts.get('devices_total', 0)} UniFi devices in the current site."
        if intent_name == "count_devices_offline":
            off = counts.get("devices_offline", 0)
            if off:
                return f"Yes — {off} UniFi device(s) appear offline."
            return "No — I don’t see any UniFi devices reported as offline."

        if any(x in q for x in ("how many clients", "number of clients", "client count")):
            return f"There are {counts.get('clients_total', 0)} clients in the UniFi client list right now."
        if any(x in q for x in ("how many wireless", "wireless clients")):
            return f"There are {counts.get('clients_wireless', 0)} wireless clients (and {counts.get('clients_wired', 0)} wired, {counts.get('clients_unknown_link', 0)} unknown)."
        if any(x in q for x in ("how many wired", "wired clients")):
            return f"There are {counts.get('clients_wired', 0)} wired clients (and {counts.get('clients_wireless', 0)} wireless, {counts.get('clients_unknown_link', 0)} unknown)."
        if any(x in q for x in ("any devices offline", "devices offline", "offline devices")) and "how" not in q:
            off = counts.get("devices_offline", 0)
            if off:
                return f"Yes — {off} UniFi device(s) appear offline."
            return "No — I don’t see any UniFi devices reported as offline."

        if intent_name in {"find_any", "find_client", "find_device"} or self._query_kind(query) == "find":
            if not isinstance(matches, list) or not matches:
                look_for = target or query
                return f"No UniFi client/device matches found for: {look_for}."

            first = matches[0] if isinstance(matches[0], dict) else {}
            name = str(first.get("name") or first.get("id") or "the matching item").strip()
            ip = str(first.get("ip") or "").strip()
            mac = str(first.get("mac") or "").strip()
            kind = str(first.get("kind") or "item").strip()
            wants_ip = "ip address" in q or re.search(r"\bip\b", q) is not None
            wants_mac = "mac address" in q or re.search(r"\bmac\b", q) is not None

            if wants_ip and ip:
                return f"{name} ({kind}) is at IP {ip}."
            if wants_mac and mac:
                return f"{name} ({kind}) has MAC {mac}."

            if len(matches) == 1:
                detail = f"IP {ip}" if ip else (f"MAC {mac}" if mac else "no IP/MAC reported")
                return f"I found {name} ({kind}) with {detail}."

            preview_parts: List[str] = []
            for m in matches[:3]:
                if not isinstance(m, dict):
                    continue
                nm = str(m.get("name") or m.get("id") or "match").strip()
                nip = str(m.get("ip") or "").strip()
                nmac = str(m.get("mac") or "").strip()
                token = nip or nmac or "no ip/mac"
                preview_parts.append(f"{nm} ({token})")
            preview = ", ".join(preview_parts) if preview_parts else "multiple candidates"
            return f"I found {len(matches)} possible matches. Top matches: {preview}."

        return None

    async def _answer_with_llm(self, query: str, compact_ctx: Dict[str, Any], llm_client) -> Optional[str]:
        if not llm_client:
            return None

        facts = (compact_ctx or {}).get("facts") or {}
        system = (
            "You answer UniFi Network questions using only provided facts.\n"
            "Be concise (1-3 sentences), direct, and do not invent values.\n"
            "If data is missing, say exactly what is missing."
        )
        user = json.dumps(
            {"question": query, "facts": facts},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        try:
            resp = await llm_client.chat(messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ])
            content = ((resp or {}).get("message") or {}).get("content", "")
            text = self._strip_code_fences(str(content or "").strip())
            return text or None
        except Exception:
            return None

    def _summary_from_context(self, compact_ctx: Dict[str, Any]) -> str:
        facts = (compact_ctx or {}).get("facts") or {}
        counts = facts.get("counts") or {}
        matches = facts.get("matches") or []
        if isinstance(matches, list) and matches:
            first = matches[0] if isinstance(matches[0], dict) else {}
            name = str(first.get("name") or first.get("entity_id") or "match").strip()
            kind = str(first.get("kind") or "item").strip()
            return f"Found {len(matches)} matching {kind} result(s). First match: {name}."
        return (
            f"Clients: {counts.get('clients_total', 0)} total "
            f"({counts.get('clients_wireless', 0)} wireless, {counts.get('clients_wired', 0)} wired). "
            f"Devices: {counts.get('devices_total', 0)} total, {counts.get('devices_offline', 0)} offline."
        )

    # -------------------------
    # Platform entrypoints
    # -------------------------
    async def handle_webui(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
    async def handle_homeassistant(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)
    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        try:
            return await self.handle_homeassistant(args=args, llm_client=llm_client, context=context)
        except TypeError:
            try:
                return await self.handle_homeassistant(args=args, llm_client=llm_client)
            except TypeError:
                return await self.handle_homeassistant(args, llm_client)


    # ✅ New: HomeKit + XBMC handlers (same core logic)
    async def handle_homekit(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_xbmc(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_discord(self, message, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self._handle(args, llm_client)


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
        return await self._handle(args, llm_client)

    async def _handle(self, args: Dict[str, Any], llm_client):
        args = args or {}
        query = (args.get("query") or args.get("request") or args.get("prompt") or "").strip()
        fixed_intent = str(getattr(self, "fixed_intent", "") or "").strip().lower()
        target_hint = (args.get("target") or args.get("name") or "").strip()

        if not query:
            if fixed_intent == "list_clients":
                query = "list clients"
            elif fixed_intent == "list_devices":
                query = "list devices"
            elif fixed_intent == "health":
                query = "how is the network right now"
            elif fixed_intent in {"find_any", "find_client", "find_device"} and target_hint:
                query = f"find {target_hint}"

        if not query:
            return action_failure(
                code="missing_query",
                message="Please provide a UniFi Network request in `query`.",
                needs=["Provide a natural-language UniFi Network request in `query`."],
                say_hint="Ask what UniFi information the user wants (status, list, or lookup).",
            )

        try:
            intent = self._fixed_intent_payload(query, args)
        except Exception as e:
            return action_failure(
                code="invalid_intent",
                message=f"This UniFi Network verba has an invalid fixed intent: {e}",
                say_hint="Explain that this UniFi Network tool has an invalid fixed intent setting.",
            )

        if fixed_intent in {"find_any", "find_client", "find_device"} and not str(intent.get("target") or "").strip():
            return action_failure(
                code="missing_target",
                message="Please include the client or device name to look up in query (or pass target).",
                needs=["Provide the client/device name, hostname, IP, or MAC to look up."],
                say_hint="Ask the user which UniFi client/device to find.",
            )

        s = self._get_settings()
        try:
            base = self._base(s)
            api_key = self._api_key(s)
        except Exception as e:
            return action_failure(
                code="unifi_network_not_configured",
                message=f"UniFi Network is not configured: {e}",
                say_hint="Explain required UniFi Network settings are missing.",
            )

        headers = self._headers(api_key)

        # Sites
        try:
            sites = self._get_sites(base, headers)
            site_id, site_name = self._pick_site(sites)
        except Exception as e:
            logger.exception("[unifi_network] sites fetch failed")
            return action_failure(
                code="unifi_sites_failed",
                message=f"I couldn't reach UniFi to list sites: {e}",
                say_hint="Explain that listing UniFi sites failed.",
            )

        # Fetch ALL clients + ALL devices (paged)
        try:
            clients = self._get_clients_all(base, headers, site_id)
        except Exception as e:
            logger.exception("[unifi_network] clients fetch failed")
            clients = {"error": str(e), "data": [], "totalCount": 0, "count": 0, "offset": 0, "limit": self._PAGE_LIMIT}

        try:
            devices = self._get_devices_all(base, headers, site_id)
        except Exception as e:
            logger.exception("[unifi_network] devices fetch failed")
            devices = {"error": str(e), "data": [], "totalCount": 0, "count": 0, "offset": 0, "limit": self._PAGE_LIMIT}

        compact_ctx = self._build_compact_context(
            query=query,
            site_name=site_name,
            sites=sites,
            clients=clients,
            devices=devices,
            intent=intent,
        )

        direct = self._direct_answer_if_simple(query, compact_ctx, intent=intent)
        if direct:
            return action_success(
                facts=(compact_ctx or {}).get("facts") or {},
                summary_for_user=direct,
                say_hint="Provide the direct network answer from the computed counts.",
            )

        llm_answer = await self._answer_with_llm(query, compact_ctx, llm_client)
        if llm_answer:
            return action_success(
                facts=(compact_ctx or {}).get("facts") or {},
                summary_for_user=llm_answer,
                say_hint="Answer the network question using only provided UniFi facts.",
            )

        return action_success(
            facts=(compact_ctx or {}).get("facts") or {},
            summary_for_user=self._summary_from_context(compact_ctx),
            say_hint="Summarize network counts and any matched items from the provided facts.",
        )


verba = UnifiNetworkLookupPlugin()
