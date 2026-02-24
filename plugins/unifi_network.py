# plugins/unifi_network.py
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client
from plugin_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("unifi_network")
logger.setLevel(logging.INFO)


class UnifiNetworkPlugin(ToolPlugin):
    """
    UniFi Network (Official Integration API) plugin — Cerberus-first "toolbox" mode.

    Design:
    - Plugin returns structured data (facts) only.
    - Cerberus decides what to call and how to answer.
    - Minimal intent logic inside plugin; ops are deterministic.

    API:
      https://<console>/proxy/network/integration/v1/...
      Auth: X-API-KEY header
    """

    # NOTE: Tater's registry typically keys by `name` (function name).
    name = "unifi_network"
    plugin_name = "UniFi Network"
    version = "2.0.0"
    min_tater_version = "50"
    pretty_name = "UniFi Network"

    description = (
        "UniFi Network toolbox (official Integration API): list sites/clients/devices, "
        "find clients/devices, and fetch health snapshots. Returns structured data."
    )
    plugin_dec = "UniFi Network toolbox that returns structured UniFi data for Cerberus to reason over."
    when_to_use = (
        "Use for UniFi Network status, client/device listings, lookups, counts, "
        "offline device checks, or network health snapshots."
    )

    # Cerberus calls this with structured args; we no longer accept raw natural language as the primary API.
    # Keep it compact and explicit.
    usage = (
        '{"function":"unifi_network","arguments":{'
        '"op":"sites_list|health_snapshot|clients_list|devices_list|clients_all|devices_all|client_find|device_find",'
        '"site_id":"optional (if omitted: first site)",'
        '"query":"required for *_find ops",'
        '"offset":0,'
        '"limit":200,'
        '"max_hits":10'
        '}}'
    )

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking the UniFi network now. "
        "Only output that message."
    )

    settings_category = "UniFi Network"
    platforms = ["webui", "homeassistant", "homekit", "xbmc", "discord", "telegram", "matrix", "irc"]

    required_settings = {
        "UNIFI_BASE_URL": {
            "label": "UniFi Console Base URL",
            "type": "string",
            "default": "https://10.4.20.1",
            "description": "Base URL of your UniFi console (UDM/CloudKey), e.g. https://10.4.20.1",
        },
        "UNIFI_API_KEY": {
            "label": "UniFi API Key",
            "type": "string",
            "default": "",
            "description": "Create in UniFi Network → Settings → Control Plane → Integrations (API Key).",
        },
    }

    # ---- internal defaults (not exposed as settings) ----
    _DEFAULT_VERIFY_SSL = False
    _DEFAULT_TIMEOUT = 20

    # paging: when doing *_all, we fetch ALL pages, but do it in chunks for reliability
    _PAGE_LIMIT = 200

    # caps (plugin-level) to prevent accidental huge payload returns
    _MAX_LIMIT = 500
    _MAX_HITS = 50

    # -------------------------
    # Settings / HTTP helpers
    # -------------------------
    def _get_settings(self) -> Dict[str, str]:
        s = (
            redis_client.hgetall(f"plugin_settings:{self.pretty_name}") or
            redis_client.hgetall(f"plugin_settings:{self.settings_category}") or
            redis_client.hgetall(f"plugin_settings: {self.pretty_name}") or
            redis_client.hgetall(f"plugin_settings: {self.settings_category}") or
            {}
        )
        return s or {}

    def _base(self, s: Dict[str, str]) -> str:
        return (s.get("UNIFI_BASE_URL") or "https://10.4.20.1").rstrip("/")

    def _api_key(self, s: Dict[str, str]) -> str:
        key = (s.get("UNIFI_API_KEY") or "").strip()
        if not key:
            raise ValueError("UNIFI_API_KEY is missing in UniFi Network plugin settings.")
        return key

    def _headers(self, api_key: str) -> Dict[str, str]:
        return {"X-API-KEY": api_key, "Accept": "application/json"}

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: Dict[str, str],
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
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

    def _pick_site(self, sites_payload: Dict[str, Any], preferred_site_id: str = "") -> Tuple[str, str]:
        data = (sites_payload or {}).get("data") or []
        if not isinstance(data, list) or not data:
            raise RuntimeError("No UniFi sites returned from /v1/sites.")

        pref = (preferred_site_id or "").strip()
        if pref:
            for s in data:
                if not isinstance(s, dict):
                    continue
                if str(s.get("id") or "").strip() == pref:
                    site_id = pref
                    site_name = (s.get("name") or s.get("internalReference") or "Unknown").strip()
                    return site_id, site_name or "Unknown"

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

    def _get_clients_page(self, base: str, headers: Dict[str, str], site_id: str, offset: int, limit: int) -> Dict[str, Any]:
        url = self._integration_url(base, f"/v1/sites/{site_id}/clients")
        params = {"offset": str(max(0, int(offset))), "limit": str(max(1, int(limit)))}
        payload = self._request("GET", url, headers=headers, params=params)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected response type from clients page: {type(payload)}")
        return payload

    def _get_devices_page(self, base: str, headers: Dict[str, str], site_id: str, offset: int, limit: int) -> Dict[str, Any]:
        url = self._integration_url(base, f"/v1/sites/{site_id}/devices")
        params = {"offset": str(max(0, int(offset))), "limit": str(max(1, int(limit)))}
        payload = self._request("GET", url, headers=headers, params=params)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected response type from devices page: {type(payload)}")
        return payload

    def _get_clients_all(self, base: str, headers: Dict[str, str], site_id: str) -> Dict[str, Any]:
        return self._get_paged(base=base, headers=headers, path=f"/v1/sites/{site_id}/clients")

    def _get_devices_all(self, base: str, headers: Dict[str, str], site_id: str) -> Dict[str, Any]:
        return self._get_paged(base=base, headers=headers, path=f"/v1/sites/{site_id}/devices")

    # -------------------------
    # Data shaping helpers
    # -------------------------
    @staticmethod
    def _safe_str(v: Any) -> str:
        return "" if v is None else str(v)

    @staticmethod
    def _q_norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip().lower())

    def _compact_client(self, c: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": c.get("id") or "",
            "name": c.get("name") or c.get("hostname") or c.get("displayName") or "",
            "type": c.get("type") or c.get("connectionType") or c.get("linkType") or "",
            "ip": c.get("ipAddress") or c.get("ip") or "",
            "mac": c.get("macAddress") or c.get("mac") or "",
            "connectedAt": c.get("connectedAt") or c.get("firstSeen") or c.get("lastSeen") or "",
            "uplinkDeviceId": c.get("uplinkDeviceId") or c.get("uplink") or "",
        }

    def _compact_device(self, d: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": d.get("id") or "",
            "name": d.get("name") or d.get("displayName") or "",
            "model": d.get("model") or d.get("type") or "",
            "state": d.get("state") or d.get("status") or "",
            "ip": d.get("ipAddress") or d.get("ip") or "",
            "mac": d.get("macAddress") or d.get("mac") or "",
            "version": d.get("version") or d.get("firmwareVersion") or "",
        }

    def _client_link_counts(self, clients: List[Dict[str, Any]]) -> Dict[str, int]:
        wired = 0
        wireless = 0
        unknown = 0

        for c in clients:
            if not isinstance(c, dict):
                continue

            t = (c.get("type") or c.get("connectionType") or c.get("linkType") or "").strip().upper()

            if t == "WIRED":
                wired += 1
                continue
            if t == "WIRELESS":
                wireless += 1
                continue

            if c.get("isWireless") is True:
                wireless += 1
                continue
            if c.get("isWired") is True:
                wired += 1
                continue

            unknown += 1

        return {"wired": wired, "wireless": wireless, "unknown": unknown, "total_listed": wired + wireless + unknown}

    def _device_status_counts(self, devices: List[Dict[str, Any]]) -> Dict[str, int]:
        online = 0
        offline = 0
        unknown = 0

        for d in devices:
            if not isinstance(d, dict):
                continue

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
            elif n in blob:
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
            state = self._q_norm(self._safe_str(d.get("state") or d.get("status")))

            blob = f"{name} {model} {ip} {mac} {state}".strip()

            score = 0
            if n == name or n == ip or n == mac:
                score = 100
            elif n in blob:
                score = 50
                if name.startswith(n):
                    score += 10

            if score > 0:
                hits.append((score, d))

        hits.sort(key=lambda x: x[0], reverse=True)
        return [self._compact_device(d) for _, d in hits[:max_hits]]

    # -------------------------
    # Platform entrypoints
    # -------------------------
    async def handle_webui(self, args: Dict[str, Any], llm_client):
        return self._handle(args)

    async def handle_homeassistant(self, args: Dict[str, Any], llm_client):
        return self._handle(args)

    async def handle_homekit(self, args: Dict[str, Any], llm_client):
        return self._handle(args)

    async def handle_xbmc(self, args: Dict[str, Any], llm_client):
        return self._handle(args)

    async def handle_discord(self, message, args, llm_client):
        return self._handle(args)

    async def handle_telegram(self, update, args, llm_client):
        return self._handle(args)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return self._handle(args)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return self._handle(args)

    # -------------------------
    # Core handler (toolbox ops)
    # -------------------------
    def _handle(self, args: Dict[str, Any]):
        args = args or {}
        op = (args.get("op") or "").strip().lower()

        # Optional args
        site_id_arg = (args.get("site_id") or "").strip()
        query = (args.get("query") or "").strip()

        # clamp paging / hits
        offset = 0
        limit = self._PAGE_LIMIT
        max_hits = 10
        try:
            offset = int(args.get("offset") or 0)
        except Exception:
            offset = 0
        try:
            limit = int(args.get("limit") or self._PAGE_LIMIT)
        except Exception:
            limit = self._PAGE_LIMIT
        try:
            max_hits = int(args.get("max_hits") or 10)
        except Exception:
            max_hits = 10

        offset = max(0, offset)
        limit = max(1, min(self._MAX_LIMIT, limit))
        max_hits = max(1, min(self._MAX_HITS, max_hits))

        if not op:
            return action_failure(
                code="missing_op",
                message="Missing required argument: op",
                needs=["op"],
                say_hint="Ask for op.",
            )

        # Load settings
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

        # sites_list does not require site selection
        if op == "sites_list":
            try:
                sites = self._get_sites(base, headers)
                data = (sites or {}).get("data") or []
                sites_out = []
                for x in data:
                    if not isinstance(x, dict):
                        continue
                    sites_out.append(
                        {
                            "id": (x.get("id") or "").strip(),
                            "name": (x.get("name") or x.get("internalReference") or "Unknown").strip(),
                        }
                    )
                return action_success(
                    facts={"sites": sites_out},
                    summary_for_user="",
                    say_hint="Return structured sites list only.",
                )
            except Exception as e:
                logger.exception("[unifi_network] sites_list failed")
                return action_failure(
                    code="unifi_sites_failed",
                    message=f"Failed to list UniFi sites: {e}",
                    say_hint="Explain UniFi sites listing failed.",
                )

        # For all other ops, we need site context
        try:
            sites = self._get_sites(base, headers)
            site_id, site_name = self._pick_site(sites, preferred_site_id=site_id_arg)
        except Exception as e:
            logger.exception("[unifi_network] site pick failed")
            return action_failure(
                code="unifi_sites_failed",
                message=f"I couldn't reach UniFi to list/pick sites: {e}",
                say_hint="Explain that listing UniFi sites failed.",
            )

        site_ctx = {"id": site_id, "name": site_name}

        # ---- health_snapshot ----
        # Fetch ALL clients + ALL devices and return computed counts + anomalies + small samples
        if op == "health_snapshot":
            try:
                clients_payload = self._get_clients_all(base, headers, site_id)
            except Exception as e:
                logger.exception("[unifi_network] health_snapshot clients fetch failed")
                clients_payload = {"error": str(e), "data": [], "totalCount": 0, "count": 0, "offset": 0, "limit": self._PAGE_LIMIT}

            try:
                devices_payload = self._get_devices_all(base, headers, site_id)
            except Exception as e:
                logger.exception("[unifi_network] health_snapshot devices fetch failed")
                devices_payload = {"error": str(e), "data": [], "totalCount": 0, "count": 0, "offset": 0, "limit": self._PAGE_LIMIT}

            clients_list = [c for c in (clients_payload or {}).get("data") or [] if isinstance(c, dict)]
            devices_list = [d for d in (devices_payload or {}).get("data") or [] if isinstance(d, dict)]

            client_counts = self._client_link_counts(clients_list)
            device_counts = self._device_status_counts(devices_list)

            anomalies: List[str] = []
            if device_counts.get("offline", 0) > 0:
                anomalies.append(f"{device_counts['offline']} UniFi device(s) appear OFFLINE.")
            if (clients_payload or {}).get("error"):
                anomalies.append(f"Clients fetch error: {(clients_payload.get('error') or '')[:160]}")
            if (devices_payload or {}).get("error"):
                anomalies.append(f"Devices fetch error: {(devices_payload.get('error') or '')[:160]}")
            if client_counts.get("unknown", 0) > 0:
                anomalies.append(f"{client_counts['unknown']} client(s) did not report wired/wireless type.")

            top_clients = [self._compact_client(c) for c in clients_list[:25]]
            top_devices = [self._compact_device(d) for d in devices_list[:25]]

            facts = {
                "site": site_ctx,
                "counts": {
                    "clients_total": int((clients_payload or {}).get("totalCount") or len(clients_list)),
                    "clients_wired": client_counts["wired"],
                    "clients_wireless": client_counts["wireless"],
                    "clients_unknown_link": client_counts["unknown"],
                    "devices_total": int((devices_payload or {}).get("totalCount") or len(devices_list)),
                    "devices_online": device_counts["online"],
                    "devices_offline": device_counts["offline"],
                    "devices_unknown_status": device_counts["unknown"],
                },
                "anomalies": anomalies,
                "sample_clients": top_clients,
                "sample_devices": top_devices,
            }

            return action_success(
                facts=facts,
                summary_for_user="",
                say_hint="Return health snapshot facts only (counts/anomalies/samples).",
            )

        # ---- clients_list ----
        if op == "clients_list":
            try:
                page = self._get_clients_page(base, headers, site_id, offset, limit)
                data = (page or {}).get("data") or []
                compact = [self._compact_client(c) for c in data if isinstance(c, dict)]
                facts = {
                    "site": site_ctx,
                    "meta": {
                        "offset": int(page.get("offset") or offset),
                        "limit": int(page.get("limit") or limit),
                        "count": int(page.get("count") or len(compact)),
                        "totalCount": int(page.get("totalCount") or len(compact)),
                    },
                    "clients": compact,
                }
                return action_success(facts=facts, summary_for_user="", say_hint="Return a compact clients page.")
            except Exception as e:
                logger.exception("[unifi_network] clients_list failed")
                return action_failure(
                    code="unifi_clients_list_failed",
                    message=f"Failed to fetch clients page: {e}",
                    say_hint="Explain clients page fetch failed.",
                )

        # ---- devices_list ----
        if op == "devices_list":
            try:
                page = self._get_devices_page(base, headers, site_id, offset, limit)
                data = (page or {}).get("data") or []
                compact = [self._compact_device(d) for d in data if isinstance(d, dict)]
                facts = {
                    "site": site_ctx,
                    "meta": {
                        "offset": int(page.get("offset") or offset),
                        "limit": int(page.get("limit") or limit),
                        "count": int(page.get("count") or len(compact)),
                        "totalCount": int(page.get("totalCount") or len(compact)),
                    },
                    "devices": compact,
                }
                return action_success(facts=facts, summary_for_user="", say_hint="Return a compact devices page.")
            except Exception as e:
                logger.exception("[unifi_network] devices_list failed")
                return action_failure(
                    code="unifi_devices_list_failed",
                    message=f"Failed to fetch devices page: {e}",
                    say_hint="Explain devices page fetch failed.",
                )

        # ---- clients_all ----
        if op == "clients_all":
            try:
                payload = self._get_clients_all(base, headers, site_id)
                all_clients = [c for c in (payload or {}).get("data") or [] if isinstance(c, dict)]
                compact = [self._compact_client(c) for c in all_clients]
                facts = {
                    "site": site_ctx,
                    "meta": {
                        "count": len(compact),
                        "totalCount": int((payload or {}).get("totalCount") or len(compact)),
                    },
                    "clients": compact,
                }
                return action_success(facts=facts, summary_for_user="", say_hint="Return full compact clients list.")
            except Exception as e:
                logger.exception("[unifi_network] clients_all failed")
                return action_failure(
                    code="unifi_clients_all_failed",
                    message=f"Failed to fetch all clients: {e}",
                    say_hint="Explain clients fetch failed.",
                )

        # ---- devices_all ----
        if op == "devices_all":
            try:
                payload = self._get_devices_all(base, headers, site_id)
                all_devices = [d for d in (payload or {}).get("data") or [] if isinstance(d, dict)]
                compact = [self._compact_device(d) for d in all_devices]
                facts = {
                    "site": site_ctx,
                    "meta": {
                        "count": len(compact),
                        "totalCount": int((payload or {}).get("totalCount") or len(compact)),
                    },
                    "devices": compact,
                }
                return action_success(facts=facts, summary_for_user="", say_hint="Return full compact devices list.")
            except Exception as e:
                logger.exception("[unifi_network] devices_all failed")
                return action_failure(
                    code="unifi_devices_all_failed",
                    message=f"Failed to fetch all devices: {e}",
                    say_hint="Explain devices fetch failed.",
                )

        # ---- client_find ----
        if op == "client_find":
            if not query:
                return action_failure(
                    code="missing_query",
                    message="query is required for client_find",
                    needs=["query"],
                    say_hint="Ask for the client query.",
                )
            try:
                payload = self._get_clients_all(base, headers, site_id)
                hits = self._search_clients(payload, query, max_hits=max_hits)
                facts = {"site": site_ctx, "query": query, "max_hits": max_hits, "matches": hits}
                return action_success(facts=facts, summary_for_user="", say_hint="Return compact client matches.")
            except Exception as e:
                logger.exception("[unifi_network] client_find failed")
                return action_failure(
                    code="unifi_client_find_failed",
                    message=f"Failed to find client(s): {e}",
                    say_hint="Explain client find failed.",
                )

        # ---- device_find ----
        if op == "device_find":
            if not query:
                return action_failure(
                    code="missing_query",
                    message="query is required for device_find",
                    needs=["query"],
                    say_hint="Ask for the device query.",
                )
            try:
                payload = self._get_devices_all(base, headers, site_id)
                hits = self._search_devices(payload, query, max_hits=max_hits)
                facts = {"site": site_ctx, "query": query, "max_hits": max_hits, "matches": hits}
                return action_success(facts=facts, summary_for_user="", say_hint="Return compact device matches.")
            except Exception as e:
                logger.exception("[unifi_network] device_find failed")
                return action_failure(
                    code="unifi_device_find_failed",
                    message=f"Failed to find device(s): {e}",
                    say_hint="Explain device find failed.",
                )

        return action_failure(
            code="bad_op",
            message=f"Unknown op: {op}",
            needs=["op"],
            say_hint="Ask for a valid op.",
        )


plugin = UnifiNetworkPlugin()
