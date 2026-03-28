# cores/rss_core.py
import asyncio
import time
import os
import json
import feedparser
import logging
import requests
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from helpers import get_llm_client_from_env, redis_client
from notify import core_notifier_platforms, dispatch_notification, notifier_destination_catalog
from rss_store import get_all_feeds, set_feed, update_feed, ensure_feed, delete_feed
__version__ = "1.0.7"


logger = logging.getLogger("rss")
logger.setLevel(logging.INFO)

load_dotenv()

CORE_SETTINGS = {
    "category": "RSS Core Settings",
    "required": {
        "poll_interval": {
            "label": "Poll Interval (sec)",
            "type": "number",
            "default": 60,
            "description": "Seconds between feed checks",
        },
    },
}

CORE_WEBUI_TAB = {
    "label": "RSS Feeds",
    "order": 30,
    "requires_running": True,
}

MAX_ARTICLE_CHARS = 12000  # keep well under model context; tune per model
_RSS_NTFY_DEFAULT_SERVER = "https://ntfy.sh"
_RSS_NTFY_DEFAULT_PRIORITY = "3"
_RSS_WORDPRESS_STATUS_OPTIONS = ["draft", "publish", "pending", "private"]


def _as_bool_flag(value: Any, default: bool = True) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _rss_clean_targets_dict(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for key, value in raw.items():
        token = _clean_text(key)
        text = _clean_text(value)
        if token and text:
            out[token] = text
    return out


def _rss_load_destination_catalog(client: Any) -> Dict[str, Any]:
    if client is None:
        return {"platforms": []}
    try:
        payload = notifier_destination_catalog(redis_client=client, limit=250)
    except Exception:
        return {"platforms": []}
    if not isinstance(payload, dict):
        return {"platforms": []}
    platforms = payload.get("platforms")
    if not isinstance(platforms, list):
        payload["platforms"] = []
    return payload


def _rss_catalog_platform_row(catalog: Dict[str, Any], platform: str) -> Dict[str, Any]:
    rows = catalog.get("platforms") if isinstance(catalog, dict) else []
    if not isinstance(rows, list):
        return {}
    wanted = _clean_text(platform).lower()
    for row in rows:
        if not isinstance(row, dict):
            continue
        token = _clean_text(row.get("platform")).lower()
        if token == wanted:
            return row
    return {}


def _rss_encode_destination_value(platform: str, targets: Dict[str, Any]) -> str:
    plat = _clean_text(platform).lower()
    if not plat:
        return ""
    payload = {
        "platform": plat,
        "targets": _rss_clean_targets_dict(targets),
    }
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)
    except Exception:
        return ""


def _rss_decode_destination_value(raw_value: Any) -> Tuple[str, Dict[str, str]]:
    value = _clean_text(raw_value)
    if not value:
        return "", {}
    try:
        parsed = json.loads(value)
    except Exception:
        return "", {}
    if not isinstance(parsed, dict):
        return "", {}
    platform = _clean_text(parsed.get("platform")).lower()
    targets = _rss_clean_targets_dict(parsed.get("targets"))
    return platform, targets


def _rss_target_from_text(platform: str, text_value: Any) -> Dict[str, str]:
    platform_name = _clean_text(platform).lower()
    text = _clean_text(text_value)
    if not text:
        return {}
    if platform_name == "discord":
        if text.isdigit():
            return {"channel_id": text}
        return {"channel": text}
    if platform_name == "irc":
        return {"channel": text}
    if platform_name == "matrix":
        return {"room_id": text}
    if platform_name == "telegram":
        return {"chat_id": text}
    return {}


def _rss_destination_options_for_platform(
    platform: str,
    *,
    catalog: Dict[str, Any],
    current_targets: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    platform_name = _clean_text(platform).lower()
    row = _rss_catalog_platform_row(catalog, platform_name)
    requires_target = bool(row.get("requires_target")) if isinstance(row, dict) else True

    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    if requires_target:
        out.append({"value": "", "label": "(Select destination)"})
        seen.add("")
    else:
        default_value = _rss_encode_destination_value(platform_name, {})
        out.append({"value": default_value, "label": "Portal defaults"})
        seen.add(default_value)

    destinations = row.get("destinations") if isinstance(row, dict) else []
    if isinstance(destinations, list):
        for item in destinations:
            if not isinstance(item, dict):
                continue
            targets = _rss_clean_targets_dict(item.get("targets"))
            value = _rss_encode_destination_value(platform_name, targets)
            if not value or value in seen:
                continue
            label = _clean_text(item.get("label")) or value
            out.append({"value": value, "label": label})
            seen.add(value)

    current_clean = _rss_clean_targets_dict(current_targets)
    if current_clean:
        current_value = _rss_encode_destination_value(platform_name, current_clean)
        if current_value and current_value not in seen:
            out.append({"value": current_value, "label": "Current target"})

    return out


def _portal_row(portals: Dict[str, Any], portal_key: str) -> Dict[str, Any]:
    if not isinstance(portals, dict):
        return {}
    row = portals.get(portal_key)
    return row if isinstance(row, dict) else {}


def _portal_targets(portals: Dict[str, Any], portal_key: str) -> Dict[str, Any]:
    row = _portal_row(portals, portal_key)
    raw_targets = row.get("targets")
    return raw_targets if isinstance(raw_targets, dict) else {}


def _feed_form_from_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    portals = cfg.get("portals") if isinstance(cfg.get("portals"), dict) else {}

    discord_targets = _portal_targets(portals, "discord")
    irc_targets = _portal_targets(portals, "irc")
    matrix_targets = _portal_targets(portals, "matrix")
    ha_targets = _portal_targets(portals, "homeassistant")
    ntfy_targets = _portal_targets(portals, "ntfy")
    telegram_targets = _portal_targets(portals, "telegram")
    wp_targets = _portal_targets(portals, "wordpress")

    wp_post_status = _clean_text(wp_targets.get("post_status") or "draft").lower()
    if wp_post_status not in _RSS_WORDPRESS_STATUS_OPTIONS:
        wp_post_status = "draft"

    persistent_value = ha_targets.get("persistent")
    if persistent_value is True:
        ha_persistent_mode = "true"
    elif persistent_value is False:
        ha_persistent_mode = "false"
    else:
        ha_persistent_mode = "use_default"

    return {
        "discord": {
            "enabled": _as_bool_flag(_portal_row(portals, "discord").get("enabled"), False),
            "destination": _rss_encode_destination_value("discord", discord_targets) if discord_targets else "",
            "channel_id": _clean_text(discord_targets.get("channel_id")),
        },
        "irc": {
            "enabled": _as_bool_flag(_portal_row(portals, "irc").get("enabled"), False),
            "destination": _rss_encode_destination_value("irc", irc_targets) if irc_targets else "",
            "channel": _clean_text(irc_targets.get("channel")),
        },
        "matrix": {
            "enabled": _as_bool_flag(_portal_row(portals, "matrix").get("enabled"), False),
            "destination": _rss_encode_destination_value("matrix", matrix_targets) if matrix_targets else "",
            "room_id": _clean_text(matrix_targets.get("room_id")),
        },
        "homeassistant": {
            "enabled": _as_bool_flag(_portal_row(portals, "homeassistant").get("enabled"), False),
            "device_service": _clean_text(ha_targets.get("device_service")),
            "persistent_mode": ha_persistent_mode,
        },
        "ntfy": {
            "enabled": _as_bool_flag(_portal_row(portals, "ntfy").get("enabled"), False),
            "server": _clean_text(ntfy_targets.get("ntfy_server") or _RSS_NTFY_DEFAULT_SERVER),
            "topic": _clean_text(ntfy_targets.get("ntfy_topic")),
            "priority": _clean_text(ntfy_targets.get("ntfy_priority") or _RSS_NTFY_DEFAULT_PRIORITY),
            "tags": _clean_text(ntfy_targets.get("ntfy_tags")),
            "click_from_first_url": _as_bool_flag(ntfy_targets.get("ntfy_click_from_first_url"), True),
            "token": _clean_text(ntfy_targets.get("ntfy_token")),
            "username": _clean_text(ntfy_targets.get("ntfy_username")),
            "password": _clean_text(ntfy_targets.get("ntfy_password")),
        },
        "telegram": {
            "enabled": _as_bool_flag(_portal_row(portals, "telegram").get("enabled"), False),
            "destination": _rss_encode_destination_value("telegram", telegram_targets) if telegram_targets else "",
            "chat_id": _clean_text(telegram_targets.get("chat_id")),
        },
        "webui": {
            "enabled": _as_bool_flag(_portal_row(portals, "webui").get("enabled"), False),
        },
        "wordpress": {
            "enabled": _as_bool_flag(_portal_row(portals, "wordpress").get("enabled"), False),
            "site_url": _clean_text(wp_targets.get("wordpress_site_url")),
            "username": _clean_text(wp_targets.get("wordpress_username")),
            "app_password": _clean_text(wp_targets.get("wordpress_app_password")),
            "post_status": wp_post_status,
            "category_id": _clean_text(wp_targets.get("category_id")),
        },
    }


def _portals_from_form(portals_form: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    form = portals_form if isinstance(portals_form, dict) else {}

    def _row(portal_key: str) -> Dict[str, Any]:
        row = form.get(portal_key)
        return row if isinstance(row, dict) else {}

    out: Dict[str, Dict[str, Any]] = {}

    discord = _row("discord")
    discord_enabled = _as_bool_flag(discord.get("enabled"), False)
    discord_destination = _clean_text(discord.get("destination"))
    discord_channel = _clean_text(discord.get("channel_id"))
    discord_dest_platform, discord_dest_targets = _rss_decode_destination_value(discord_destination)
    if discord_dest_platform == "discord":
        discord_targets: Dict[str, Any] = dict(discord_dest_targets)
    elif discord_destination:
        discord_targets = _rss_target_from_text("discord", discord_destination)
    else:
        discord_targets = {}
    if discord_channel:
        discord_targets["channel_id"] = discord_channel
    if (not discord_enabled) or discord_targets:
        out["discord"] = {"enabled": discord_enabled, "targets": discord_targets}

    irc = _row("irc")
    irc_enabled = _as_bool_flag(irc.get("enabled"), False)
    irc_destination = _clean_text(irc.get("destination"))
    irc_channel = _clean_text(irc.get("channel"))
    irc_dest_platform, irc_dest_targets = _rss_decode_destination_value(irc_destination)
    if irc_dest_platform == "irc":
        irc_targets: Dict[str, Any] = dict(irc_dest_targets)
    elif irc_destination:
        irc_targets = _rss_target_from_text("irc", irc_destination)
    else:
        irc_targets = {}
    if irc_channel:
        irc_targets["channel"] = irc_channel
    if (not irc_enabled) or irc_targets:
        out["irc"] = {"enabled": irc_enabled, "targets": irc_targets}

    matrix = _row("matrix")
    matrix_enabled = _as_bool_flag(matrix.get("enabled"), False)
    matrix_destination = _clean_text(matrix.get("destination"))
    matrix_room = _clean_text(matrix.get("room_id"))
    matrix_dest_platform, matrix_dest_targets = _rss_decode_destination_value(matrix_destination)
    if matrix_dest_platform == "matrix":
        matrix_targets: Dict[str, Any] = dict(matrix_dest_targets)
    elif matrix_destination:
        matrix_targets = _rss_target_from_text("matrix", matrix_destination)
    else:
        matrix_targets = {}
    if matrix_room:
        matrix_targets["room_id"] = matrix_room
    if (not matrix_enabled) or matrix_targets:
        out["matrix"] = {"enabled": matrix_enabled, "targets": matrix_targets}

    homeassistant = _row("homeassistant")
    ha_enabled = _as_bool_flag(homeassistant.get("enabled"), False)
    ha_device = _clean_text(homeassistant.get("device_service"))
    ha_persistent_mode = _clean_text(homeassistant.get("persistent_mode")).lower()
    ha_targets: Dict[str, Any] = {}
    if ha_device:
        ha_targets["device_service"] = ha_device
    if ha_persistent_mode in {"true", "force_on", "on"}:
        ha_targets["persistent"] = True
    elif ha_persistent_mode in {"false", "force_off", "off"}:
        ha_targets["persistent"] = False
    if (not ha_enabled) or ha_targets:
        out["homeassistant"] = {"enabled": ha_enabled, "targets": ha_targets}

    ntfy = _row("ntfy")
    ntfy_enabled = _as_bool_flag(ntfy.get("enabled"), False)
    ntfy_targets: Dict[str, Any] = {}
    ntfy_server = _clean_text(ntfy.get("server"))
    if ntfy_server and ntfy_server != _RSS_NTFY_DEFAULT_SERVER:
        ntfy_targets["ntfy_server"] = ntfy_server
    ntfy_topic = _clean_text(ntfy.get("topic"))
    if ntfy_topic:
        ntfy_targets["ntfy_topic"] = ntfy_topic
    ntfy_priority = _clean_text(ntfy.get("priority"))
    if ntfy_priority and ntfy_priority != _RSS_NTFY_DEFAULT_PRIORITY:
        ntfy_targets["ntfy_priority"] = ntfy_priority
    ntfy_tags = _clean_text(ntfy.get("tags"))
    if ntfy_tags:
        ntfy_targets["ntfy_tags"] = ntfy_tags
    ntfy_click = _as_bool_flag(ntfy.get("click_from_first_url"), True)
    if ntfy_click is not True:
        ntfy_targets["ntfy_click_from_first_url"] = ntfy_click
    ntfy_token = _clean_text(ntfy.get("token"))
    if ntfy_token:
        ntfy_targets["ntfy_token"] = ntfy_token
    ntfy_username = _clean_text(ntfy.get("username"))
    if ntfy_username:
        ntfy_targets["ntfy_username"] = ntfy_username
    ntfy_password = _clean_text(ntfy.get("password"))
    if ntfy_password:
        ntfy_targets["ntfy_password"] = ntfy_password
    if (not ntfy_enabled) or ntfy_targets:
        out["ntfy"] = {"enabled": ntfy_enabled, "targets": ntfy_targets}

    telegram = _row("telegram")
    telegram_enabled = _as_bool_flag(telegram.get("enabled"), False)
    telegram_destination = _clean_text(telegram.get("destination"))
    telegram_chat = _clean_text(telegram.get("chat_id"))
    telegram_dest_platform, telegram_dest_targets = _rss_decode_destination_value(telegram_destination)
    if telegram_dest_platform == "telegram":
        telegram_targets: Dict[str, Any] = dict(telegram_dest_targets)
    elif telegram_destination:
        telegram_targets = _rss_target_from_text("telegram", telegram_destination)
    else:
        telegram_targets = {}
    if telegram_chat:
        telegram_targets["chat_id"] = telegram_chat
    if (not telegram_enabled) or telegram_targets:
        out["telegram"] = {"enabled": telegram_enabled, "targets": telegram_targets}

    webui = _row("webui")
    webui_enabled = _as_bool_flag(webui.get("enabled"), False)
    if not webui_enabled:
        out["webui"] = {"enabled": False, "targets": {}}

    wordpress = _row("wordpress")
    wp_enabled = _as_bool_flag(wordpress.get("enabled"), False)
    wp_targets: Dict[str, Any] = {}
    wp_site_url = _clean_text(wordpress.get("site_url"))
    if wp_site_url:
        wp_targets["wordpress_site_url"] = wp_site_url
    wp_username = _clean_text(wordpress.get("username"))
    if wp_username:
        wp_targets["wordpress_username"] = wp_username
    wp_app_password = _clean_text(wordpress.get("app_password"))
    if wp_app_password:
        wp_targets["wordpress_app_password"] = wp_app_password
    wp_post_status = _clean_text(wordpress.get("post_status")).lower()
    if wp_post_status in _RSS_WORDPRESS_STATUS_OPTIONS and wp_post_status != "draft":
        wp_targets["post_status"] = wp_post_status
    wp_category = _clean_text(wordpress.get("category_id"))
    if wp_category:
        wp_targets["category_id"] = wp_category
    if (not wp_enabled) or wp_targets:
        out["wordpress"] = {"enabled": wp_enabled, "targets": wp_targets}

    return out


def _feed_rows(client) -> List[Dict[str, Any]]:
    feeds = get_all_feeds(client) or {}
    rows: List[Dict[str, Any]] = []
    for feed_url, cfg in sorted((feeds or {}).items(), key=lambda item: str(item[0]).lower()):
        url = _clean_text(feed_url)
        if not url:
            continue
        raw = cfg if isinstance(cfg, dict) else {}
        try:
            last_ts = float(raw.get("last_ts") or 0.0)
        except Exception:
            last_ts = 0.0
        last_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_ts)) if last_ts > 0 else "never"
        rows.append(
            {
                "url": url,
                "enabled": _as_bool_flag(raw.get("enabled"), False),
                "last_ts": last_ts,
                "last_text": last_text,
                "portals_raw": raw.get("portals") if isinstance(raw.get("portals"), dict) else {},
                "portals": _feed_form_from_cfg(raw),
            }
        )
    return rows


def _as_choices(values: List[str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for value in values or []:
        token = _clean_text(value)
        if not token:
            continue
        rows.append({"value": token, "label": token})
    return rows


def _rss_manager_ui(client) -> Dict[str, Any]:
    catalog = _rss_load_destination_catalog(client)
    forms = []
    for feed in _feed_rows(client):
        url = _clean_text(feed.get("url"))
        portals = feed.get("portals") if isinstance(feed.get("portals"), dict) else {}
        portals_raw = feed.get("portals_raw") if isinstance(feed.get("portals_raw"), dict) else {}
        discord = portals.get("discord") if isinstance(portals.get("discord"), dict) else {}
        irc = portals.get("irc") if isinstance(portals.get("irc"), dict) else {}
        matrix = portals.get("matrix") if isinstance(portals.get("matrix"), dict) else {}
        homeassistant = portals.get("homeassistant") if isinstance(portals.get("homeassistant"), dict) else {}
        ntfy = portals.get("ntfy") if isinstance(portals.get("ntfy"), dict) else {}
        telegram = portals.get("telegram") if isinstance(portals.get("telegram"), dict) else {}
        webui = portals.get("webui") if isinstance(portals.get("webui"), dict) else {}
        wordpress = portals.get("wordpress") if isinstance(portals.get("wordpress"), dict) else {}
        discord_targets = _portal_targets(portals_raw, "discord")
        irc_targets = _portal_targets(portals_raw, "irc")
        matrix_targets = _portal_targets(portals_raw, "matrix")
        telegram_targets = _portal_targets(portals_raw, "telegram")

        discord_destination_options = _rss_destination_options_for_platform(
            "discord",
            catalog=catalog,
            current_targets=discord_targets,
        )
        irc_destination_options = _rss_destination_options_for_platform(
            "irc",
            catalog=catalog,
            current_targets=irc_targets,
        )
        matrix_destination_options = _rss_destination_options_for_platform(
            "matrix",
            catalog=catalog,
            current_targets=matrix_targets,
        )
        telegram_destination_options = _rss_destination_options_for_platform(
            "telegram",
            catalog=catalog,
            current_targets=telegram_targets,
        )

        discord_destination_value = _clean_text(discord.get("destination"))
        if not discord_destination_value and discord_targets:
            discord_destination_value = _rss_encode_destination_value("discord", discord_targets)
        irc_destination_value = _clean_text(irc.get("destination"))
        if not irc_destination_value and irc_targets:
            irc_destination_value = _rss_encode_destination_value("irc", irc_targets)
        matrix_destination_value = _clean_text(matrix.get("destination"))
        if not matrix_destination_value and matrix_targets:
            matrix_destination_value = _rss_encode_destination_value("matrix", matrix_targets)
        telegram_destination_value = _clean_text(telegram.get("destination"))
        if not telegram_destination_value and telegram_targets:
            telegram_destination_value = _rss_encode_destination_value("telegram", telegram_targets)

        forms.append(
            {
                "id": url,
                "title": url,
                "subtitle": f"Last posted: {_clean_text(feed.get('last_text') or 'never')}",
                "save_action": "rss_save_feed",
                "remove_action": "rss_remove_feed",
                "remove_confirm": f"Remove RSS feed {url}?",
                "fields": [
                    {
                        "key": "enabled",
                        "label": "Feed Enabled",
                        "type": "checkbox",
                        "value": _as_bool_flag(feed.get("enabled"), False),
                    },
                ],
                "sections": [
                    {
                        "label": "Discord",
                        "fields": [
                            {
                                "key": "portals.discord.enabled",
                                "label": "Enable Discord Route",
                                "type": "checkbox",
                                "value": _as_bool_flag(discord.get("enabled"), False),
                            },
                            {
                                "key": "portals.discord.destination",
                                "label": "Room / Destination",
                                "type": "select",
                                "description": "Choose a Discord destination from discovered rooms/channels.",
                                "options": discord_destination_options,
                                "value": discord_destination_value,
                            },
                            {
                                "key": "portals.discord.channel_id",
                                "label": "Channel ID (manual fallback)",
                                "type": "text",
                                "value": _clean_text(discord.get("channel_id")),
                            },
                        ],
                    },
                    {
                        "label": "IRC",
                        "fields": [
                            {
                                "key": "portals.irc.enabled",
                                "label": "Enable IRC Route",
                                "type": "checkbox",
                                "value": _as_bool_flag(irc.get("enabled"), False),
                            },
                            {
                                "key": "portals.irc.destination",
                                "label": "Room / Destination",
                                "type": "select",
                                "description": "Choose an IRC destination from discovered channels.",
                                "options": irc_destination_options,
                                "value": irc_destination_value,
                            },
                            {
                                "key": "portals.irc.channel",
                                "label": "Channel (manual fallback)",
                                "type": "text",
                                "value": _clean_text(irc.get("channel")),
                            },
                        ],
                    },
                    {
                        "label": "Matrix",
                        "fields": [
                            {
                                "key": "portals.matrix.enabled",
                                "label": "Enable Matrix Route",
                                "type": "checkbox",
                                "value": _as_bool_flag(matrix.get("enabled"), False),
                            },
                            {
                                "key": "portals.matrix.destination",
                                "label": "Room / Destination",
                                "type": "select",
                                "description": "Choose a Matrix destination from discovered rooms.",
                                "options": matrix_destination_options,
                                "value": matrix_destination_value,
                            },
                            {
                                "key": "portals.matrix.room_id",
                                "label": "Room ID (manual fallback)",
                                "type": "text",
                                "value": _clean_text(matrix.get("room_id")),
                            },
                        ],
                    },
                    {
                        "label": "Home Assistant",
                        "fields": [
                            {
                                "key": "portals.homeassistant.enabled",
                                "label": "Enable Home Assistant Route",
                                "type": "checkbox",
                                "value": _as_bool_flag(homeassistant.get("enabled"), False),
                            },
                            {
                                "key": "portals.homeassistant.device_service",
                                "label": "Device Service",
                                "type": "text",
                                "value": _clean_text(homeassistant.get("device_service")),
                            },
                            {
                                "key": "portals.homeassistant.persistent_mode",
                                "label": "Persistent Notification",
                                "type": "select",
                                "options": _as_choices(["use_default", "true", "false"]),
                                "value": _clean_text(homeassistant.get("persistent_mode") or "use_default"),
                            },
                        ],
                    },
                    {
                        "label": "ntfy",
                        "fields": [
                            {
                                "key": "portals.ntfy.enabled",
                                "label": "Enable ntfy Route",
                                "type": "checkbox",
                                "value": _as_bool_flag(ntfy.get("enabled"), False),
                            },
                            {
                                "key": "portals.ntfy.server",
                                "label": "Server",
                                "type": "text",
                                "value": _clean_text(ntfy.get("server") or _RSS_NTFY_DEFAULT_SERVER),
                            },
                            {
                                "key": "portals.ntfy.topic",
                                "label": "Topic",
                                "type": "text",
                                "value": _clean_text(ntfy.get("topic")),
                            },
                            {
                                "key": "portals.ntfy.priority",
                                "label": "Priority",
                                "type": "select",
                                "options": _as_choices(["1", "2", "3", "4", "5"]),
                                "value": _clean_text(ntfy.get("priority") or _RSS_NTFY_DEFAULT_PRIORITY),
                            },
                            {
                                "key": "portals.ntfy.tags",
                                "label": "Tags (comma-separated)",
                                "type": "text",
                                "value": _clean_text(ntfy.get("tags")),
                            },
                            {
                                "key": "portals.ntfy.click_from_first_url",
                                "label": "Use first article URL as click action",
                                "type": "checkbox",
                                "value": _as_bool_flag(ntfy.get("click_from_first_url"), True),
                            },
                            {
                                "key": "portals.ntfy.token",
                                "label": "Bearer Token",
                                "type": "password",
                                "value": _clean_text(ntfy.get("token")),
                            },
                            {
                                "key": "portals.ntfy.username",
                                "label": "Username",
                                "type": "text",
                                "value": _clean_text(ntfy.get("username")),
                            },
                            {
                                "key": "portals.ntfy.password",
                                "label": "Password",
                                "type": "password",
                                "value": _clean_text(ntfy.get("password")),
                            },
                        ],
                    },
                    {
                        "label": "Telegram",
                        "fields": [
                            {
                                "key": "portals.telegram.enabled",
                                "label": "Enable Telegram Route",
                                "type": "checkbox",
                                "value": _as_bool_flag(telegram.get("enabled"), False),
                            },
                            {
                                "key": "portals.telegram.destination",
                                "label": "Room / Destination",
                                "type": "select",
                                "description": "Choose a Telegram destination from discovered chats.",
                                "options": telegram_destination_options,
                                "value": telegram_destination_value,
                            },
                            {
                                "key": "portals.telegram.chat_id",
                                "label": "Chat ID (manual fallback)",
                                "type": "text",
                                "value": _clean_text(telegram.get("chat_id")),
                            },
                        ],
                    },
                    {
                        "label": "WebUI",
                        "fields": [
                            {
                                "key": "portals.webui.enabled",
                                "label": "Enable WebUI Route",
                                "type": "checkbox",
                                "value": _as_bool_flag(webui.get("enabled"), False),
                            },
                        ],
                    },
                    {
                        "label": "WordPress",
                        "fields": [
                            {
                                "key": "portals.wordpress.enabled",
                                "label": "Enable WordPress Route",
                                "type": "checkbox",
                                "value": _as_bool_flag(wordpress.get("enabled"), False),
                            },
                            {
                                "key": "portals.wordpress.site_url",
                                "label": "Site URL",
                                "type": "text",
                                "value": _clean_text(wordpress.get("site_url")),
                            },
                            {
                                "key": "portals.wordpress.username",
                                "label": "Username",
                                "type": "text",
                                "value": _clean_text(wordpress.get("username")),
                            },
                            {
                                "key": "portals.wordpress.app_password",
                                "label": "App Password",
                                "type": "password",
                                "value": _clean_text(wordpress.get("app_password")),
                            },
                            {
                                "key": "portals.wordpress.post_status",
                                "label": "Post Status",
                                "type": "select",
                                "options": _as_choices(_RSS_WORDPRESS_STATUS_OPTIONS),
                                "value": _clean_text(wordpress.get("post_status") or "draft"),
                            },
                            {
                                "key": "portals.wordpress.category_id",
                                "label": "Category ID",
                                "type": "text",
                                "value": _clean_text(wordpress.get("category_id")),
                            },
                        ],
                    },
                ],
            }
        )

    return {
        "kind": "settings_manager",
        "title": "RSS Feed Manager",
        "empty_message": "No RSS feeds configured yet.",
        "item_fields_dropdown": True,
        "item_fields_dropdown_label": "Feed Settings",
        "item_fields_popup": True,
        "item_fields_popup_label": "Feed Settings",
        "item_sections_in_dropdown": True,
        "manager_tabs": [
            {
                "key": "create",
                "label": "Add Feed",
                "source": "add_form",
            },
            {
                "key": "feeds",
                "label": "Current Feeds",
                "source": "items",
                "empty_message": "No RSS feeds configured yet.",
            },
        ],
        "default_tab": "feeds",
        "add_form": {
            "action": "rss_add_feed",
            "submit_label": "Add Feed",
            "fields": [
                {
                    "key": "url",
                    "label": "Feed URL",
                    "type": "text",
                    "required": True,
                    "placeholder": "https://example.com/feed.xml",
                }
            ],
        },
        "item_forms": forms,
    }


def _get_poll_interval() -> int:
    env_default = int(os.getenv("RSS_POLL_INTERVAL", 60))
    settings = redis_client.hgetall("rss_core_settings") or {}
    raw = settings.get("poll_interval")
    if raw is None or str(raw).strip() == "":
        return env_default
    try:
        val = int(float(raw))
        return max(5, val)
    except Exception:
        return env_default


def _coerce_targets(payload) -> dict:
    if isinstance(payload, dict):
        return dict(payload)
    return {}


def _rss_notifier_rules(settings: dict) -> dict:
    del settings
    return {
        "discord": {"enabled": False, "targets": {}},
        "irc": {"enabled": False, "targets": {}},
        "matrix": {"enabled": False, "targets": {}},
        "homeassistant": {"enabled": False, "targets": {}},
        "ntfy": {"enabled": False, "targets": {}},
        "telegram": {"enabled": False, "targets": {}},
        "webui": {"enabled": False, "targets": {}},
        "wordpress": {"enabled": False, "targets": {}},
    }

def _merge_feed_rules(global_rules: dict, feed_platforms: dict) -> dict:
    merged = {}
    for key, rule in (global_rules or {}).items():
        merged[key] = {
            "enabled": bool(rule.get("enabled", False)),
            "targets": _coerce_targets(rule.get("targets")),
        }

    for key, conf in (feed_platforms or {}).items():
        if not isinstance(conf, dict):
            continue
        if key not in merged:
            merged[key] = {
                "enabled": bool(conf.get("enabled", False)),
                "targets": _coerce_targets(conf.get("targets")),
            }
            continue
        if "enabled" in conf:
            merged[key]["enabled"] = bool(conf.get("enabled", False))
        targets = conf.get("targets") or {}
        if targets:
            merged[key]["targets"] = _coerce_targets(targets)

    return merged


def _build_summary_messages(title: str, source_name: str, content: str):
    """
    Build a punchy, newsletter-style brief.
    """
    safe_content = (content or "")[:MAX_ARTICLE_CHARS]

    system = (
        "You are a witty, conversational news writer who crafts short, engaging summaries for a newsletter audience.\n"
        "Write in a natural, human tone — think punchy intros, short paragraphs, light humor, and clear takeaways.\n\n"
        "Guidelines:\n"
        "- Be concise (about 150–200 words) but write in full sentences and short paragraphs.\n"
        "- Start with a lively hook or observation to draw the reader in.\n"
        "- Explain what happened and why it matters, with 2–4 short paragraphs.\n"
        "- You can use bullet points or short lists *only if they make sense* for clarity or emphasis.\n"
        "- Avoid repeating the title or link — the header and URL are already provided elsewhere.\n"
        "- Keep it conversational, confident, and easy to read — like a quick newsletter blurb, not a report.\n\n"
    )

    user = (
        f"Source: {source_name.strip() if source_name else '(unknown)'}\n"
        f"Original Title: {title.strip() if title else '(untitled)'}\n\n"
        f"Article Content:\n{safe_content}"
    )

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def fetch_web_summary(webpage_url, retries=3, backoff=2):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "DNT": "1",
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(webpage_url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(
                    f"[fetch_web_summary] Non-200 status code on attempt {attempt}: {response.status_code}"
                )
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
                element.decompose()

            container = soup.find("article") or soup.find("main") or soup.body
            if not container:
                return None

            text = container.get_text(separator="\n")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            article_text = "\n".join(lines)

            if len(article_text.split()) > 3000:
                article_text = " ".join(article_text.split()[:3000])

            logger.info(f"[fetch_web_summary] Extracted {len(article_text)} characters from {webpage_url}")
            return article_text

        except Exception as e:
            logger.warning(f"[fetch_web_summary] Attempt {attempt} failed for {webpage_url}: {e}")
            if attempt < retries:
                time.sleep(backoff ** attempt)

    logger.error(f"[fetch_web_summary] All {retries} attempts failed: {webpage_url}")
    return None


class RSSManager:
    def __init__(self, llm_client):
        self.llm_client = llm_client
        self.redis = redis_client
        self.feeds_key = "rss:feeds"

    def add_feed(self, feed_url: str) -> bool:
        parsed_feed = feedparser.parse(feed_url)
        if parsed_feed.bozo:
            logger.error(f"Failed to parse feed: {feed_url}")
            return False

        try:
            # Set last_ts=0 so the poller posts only the newest item once.
            ensure_feed(self.redis, feed_url, 0.0)
            logger.info(f"Added feed: {feed_url} (initial post will send latest item)")
            return True
        except Exception as e:
            logger.error(f"Error adding feed {feed_url}: {e}")
            return False

    def remove_feed(self, feed_url: str) -> bool:
        try:
            removed = delete_feed(self.redis, feed_url)
            if removed:
                logger.info(f"Removed feed: {feed_url}")
                return True
            else:
                logger.info(f"Feed not found: {feed_url}")
                return False
        except Exception as e:
            logger.error(f"Error removing feed {feed_url}: {e}")
            return False

    def get_feeds(self) -> dict:
        try:
            return get_all_feeds(self.redis)
        except Exception as e:
            logger.error(f"Error fetching feeds: {e}")
            return {}

    async def process_entry(self, feed_title: str, entry: dict, feed_platforms: dict):
        entry_title = entry.get("title", "No Title")
        link = entry.get("link", "").strip()
        if not link:
            logger.info(f"[RSS] Entry has no link: '{entry_title}' from {feed_title}")
            summary_text = "No article link was provided in this feed item."
        else:
            logger.info(f"Processing entry: {entry_title} from {feed_title}")
            loop = asyncio.get_running_loop()
            article_text = await loop.run_in_executor(None, fetch_web_summary, link)

            if not article_text:
                summary_text = "Could not retrieve a summary for this article."
            else:
                try:
                    messages = _build_summary_messages(entry_title, feed_title, article_text)
                    summarization_response = await self.llm_client.chat(
                        messages=messages,
                        stream=False,
                    )
                    summary_text = (
                        summarization_response["message"].get("content", "").strip()
                        or "Failed to generate a summary from the article."
                    )
                except Exception as e:
                    logger.error(f"Error summarizing article {link}: {e}")
                    summary_text = "Error summarizing article."

        announcement = (
            f"📰 **New article from {feed_title}**\n"
            f"**{entry_title}**\n"
            f"{link}\n\n"
            f"{summary_text}"
        )

        rules = _rss_notifier_rules({})
        merged_rules = _merge_feed_rules(rules, feed_platforms)
        origin = {
            "platform": "automation",
            "request_id": f"rss:{feed_title}",
        }

        for platform_key in core_notifier_platforms():
            rule = merged_rules.get(platform_key) or {"enabled": False, "targets": {}}
            if not rule.get("enabled", False):
                continue

            targets = _coerce_targets(rule.get("targets"))
            try:
                result = await dispatch_notification(
                    platform=platform_key,
                    title=entry_title,
                    content=announcement,
                    targets=targets,
                    origin=origin,
                    meta={"tags": ["rss"]},
                )
                if isinstance(result, str) and result.startswith("Cannot queue"):
                    logger.warning("[RSS] %s (%s)", result, platform_key)
            except Exception as e:
                logger.warning("[RSS] %s notifier failed: %s", platform_key, e)

    def any_notifier_enabled(self) -> bool:
        rules = _rss_notifier_rules({})
        feeds = self.get_feeds()

        enabled_notifiers = []
        for platform_key in core_notifier_platforms():
            global_enabled = rules.get(platform_key, {}).get("enabled", False)
            if global_enabled:
                enabled_notifiers.append(platform_key)
                continue

            # Global disabled: check per-feed overrides for this platform.
            for _url, cfg in feeds.items():
                if not cfg.get("enabled", False):
                    continue
                platforms = cfg.get("portals") or {}
                pcfg = platforms.get(platform_key)
                if pcfg and pcfg.get("enabled", False):
                    enabled_notifiers.append(platform_key)
                    break

        logger.debug(
            "[RSS] Number of built-in notifier tools: %s | Number of enabled notifier routes: %s",
            len(core_notifier_platforms()),
            len(enabled_notifiers),
        )
        return bool(enabled_notifiers)

    async def poll_feeds(self, stop_event=None):
        logger.info("Starting RSS feed polling...")
        no_notifier_logged = False
        try:
            while not (stop_event and stop_event.is_set()):
                if not self.any_notifier_enabled():
                    if not no_notifier_logged:
                        no_notifier_logged = True
                        logger.info("[RSS] No notifier routes are enabled. Polling is idle.")
                    await asyncio.sleep(_get_poll_interval())
                    continue
                no_notifier_logged = False

                feeds = self.get_feeds()
                for feed_url, feed_cfg in feeds.items():
                    if stop_event and stop_event.is_set():
                        break
                    try:
                        if not feed_cfg.get("enabled", False):
                            continue
                        last_ts = float(feed_cfg.get("last_ts") or 0.0)
                        feed_platforms = feed_cfg.get("portals") or {}
                        parsed_feed = await asyncio.to_thread(feedparser.parse, feed_url)
                        if parsed_feed.bozo:
                            logger.error(
                                f"Error parsing feed {feed_url}: {parsed_feed.bozo_exception}"
                            )
                            continue

                        feed_title = parsed_feed.feed.get("title", feed_url)
                        new_last_ts = last_ts

                        def get_entry_ts(entry) -> float:
                            if "published_parsed" in entry and entry.published_parsed:
                                return time.mktime(entry.published_parsed)
                            if "updated_parsed" in entry and entry.updated_parsed:
                                return time.mktime(entry.updated_parsed)
                            return 0.0

                        sorted_entries = sorted(
                            parsed_feed.entries,
                            key=get_entry_ts,
                        )

                        # If this feed has no known last_ts, only post the latest item once.
                        if last_ts <= 0:
                            latest_entry = None
                            latest_ts = 0.0
                            for entry in sorted_entries:
                                ts = get_entry_ts(entry)
                                if ts <= 0:
                                    continue
                                if ts > latest_ts:
                                    latest_ts = ts
                                    latest_entry = entry
                            if latest_entry is not None and latest_ts > 0:
                                await self.process_entry(feed_title, latest_entry, feed_platforms)
                                update_feed(self.redis, feed_url, {"last_ts": latest_ts})
                            continue

                        for entry in sorted_entries:
                            if stop_event and stop_event.is_set():
                                break
                            ts = get_entry_ts(entry)
                            if ts <= 0:
                                continue
                            if ts > last_ts:
                                await self.process_entry(feed_title, entry, feed_platforms)
                                if ts > new_last_ts:
                                    new_last_ts = ts
                                    update_feed(self.redis, feed_url, {"last_ts": new_last_ts})
                    except Exception as e:
                        logger.error(f"Error processing feed {feed_url}: {e}")

                if stop_event and stop_event.is_set():
                    break
                await asyncio.sleep(_get_poll_interval())
        except asyncio.CancelledError:
            logger.info("RSS polling task cancelled; exiting cleanly.")
            return


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> dict:
    client = redis_client or globals().get("redis_client")
    feeds = get_all_feeds(client) or {}
    enabled_count = 0
    items = []

    for feed_url, cfg in sorted(feeds.items(), key=lambda row: str(row[0]).lower()):
        feed_url = _clean_text(feed_url)
        config = cfg if isinstance(cfg, dict) else {}
        enabled = _as_bool_flag(config.get("enabled"), False)
        if enabled:
            enabled_count += 1

        portals = config.get("portals") if isinstance(config.get("portals"), dict) else {}
        enabled_portals = sorted(
            [
                str(name).strip()
                for name, portal_cfg in portals.items()
                if str(name).strip() and isinstance(portal_cfg, dict) and bool(portal_cfg.get("enabled", False))
            ]
        )
        routes_text = ", ".join(enabled_portals) if enabled_portals else "default routing"

        try:
            last_ts = float(config.get("last_ts") or 0.0)
        except Exception:
            last_ts = 0.0
        if last_ts > 0:
            last_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_ts))
        else:
            last_text = "never"

        items.append(
            {
                "title": feed_url or "(missing feed URL)",
                "subtitle": f"{'Enabled' if enabled else 'Disabled'} · portals: {routes_text}",
                "detail": f"Last posted item timestamp: {last_text}",
            }
        )

    total_count = len(feeds)
    disabled_count = total_count - enabled_count

    return {
        "summary": "RSS feed polling and delivery routing overview.",
        "stats": [
            {"label": "Feeds", "value": total_count},
            {"label": "Enabled", "value": enabled_count},
            {"label": "Disabled", "value": disabled_count},
            {"label": "Poll (sec)", "value": _get_poll_interval()},
        ],
        "items": items,
        "empty_message": "No RSS feeds configured yet.",
        "ui": _rss_manager_ui(client),
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def _payload_text(payload: Dict[str, Any], key: str) -> str:
    return _clean_text(payload.get(key))


def _payload_value(values: Dict[str, Any], key: str, default: Any = "") -> Any:
    if key in values:
        return values.get(key)
    return default


def _payload_portals_from_values(values: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "discord": {
            "enabled": _as_bool_flag(_payload_value(values, "portals.discord.enabled", False), False),
            "destination": _clean_text(_payload_value(values, "portals.discord.destination", "")),
            "channel_id": _clean_text(_payload_value(values, "portals.discord.channel_id", "")),
        },
        "irc": {
            "enabled": _as_bool_flag(_payload_value(values, "portals.irc.enabled", False), False),
            "destination": _clean_text(_payload_value(values, "portals.irc.destination", "")),
            "channel": _clean_text(_payload_value(values, "portals.irc.channel", "")),
        },
        "matrix": {
            "enabled": _as_bool_flag(_payload_value(values, "portals.matrix.enabled", False), False),
            "destination": _clean_text(_payload_value(values, "portals.matrix.destination", "")),
            "room_id": _clean_text(_payload_value(values, "portals.matrix.room_id", "")),
        },
        "homeassistant": {
            "enabled": _as_bool_flag(_payload_value(values, "portals.homeassistant.enabled", False), False),
            "device_service": _clean_text(_payload_value(values, "portals.homeassistant.device_service", "")),
            "persistent_mode": _clean_text(_payload_value(values, "portals.homeassistant.persistent_mode", "use_default")),
        },
        "ntfy": {
            "enabled": _as_bool_flag(_payload_value(values, "portals.ntfy.enabled", False), False),
            "server": _clean_text(_payload_value(values, "portals.ntfy.server", _RSS_NTFY_DEFAULT_SERVER)),
            "topic": _clean_text(_payload_value(values, "portals.ntfy.topic", "")),
            "priority": _clean_text(_payload_value(values, "portals.ntfy.priority", _RSS_NTFY_DEFAULT_PRIORITY)),
            "tags": _clean_text(_payload_value(values, "portals.ntfy.tags", "")),
            "click_from_first_url": _as_bool_flag(
                _payload_value(values, "portals.ntfy.click_from_first_url", True),
                True,
            ),
            "token": _clean_text(_payload_value(values, "portals.ntfy.token", "")),
            "username": _clean_text(_payload_value(values, "portals.ntfy.username", "")),
            "password": _clean_text(_payload_value(values, "portals.ntfy.password", "")),
        },
        "telegram": {
            "enabled": _as_bool_flag(_payload_value(values, "portals.telegram.enabled", False), False),
            "destination": _clean_text(_payload_value(values, "portals.telegram.destination", "")),
            "chat_id": _clean_text(_payload_value(values, "portals.telegram.chat_id", "")),
        },
        "webui": {
            "enabled": _as_bool_flag(_payload_value(values, "portals.webui.enabled", False), False),
        },
        "wordpress": {
            "enabled": _as_bool_flag(_payload_value(values, "portals.wordpress.enabled", False), False),
            "site_url": _clean_text(_payload_value(values, "portals.wordpress.site_url", "")),
            "username": _clean_text(_payload_value(values, "portals.wordpress.username", "")),
            "app_password": _clean_text(_payload_value(values, "portals.wordpress.app_password", "")),
            "post_status": _clean_text(_payload_value(values, "portals.wordpress.post_status", "draft")),
            "category_id": _clean_text(_payload_value(values, "portals.wordpress.category_id", "")),
        },
    }


def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    action_name = _clean_text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)

    if action_name == "rss_add_feed":
        feed_url = _payload_text(body, "url") or _clean_text(values.get("url"))
        if not feed_url:
            raise ValueError("Feed URL is required.")

        existing = get_all_feeds(client) or {}
        if feed_url in existing:
            raise ValueError("That feed is already configured.")

        try:
            parsed = feedparser.parse(feed_url)
        except Exception:
            parsed = None
        if not parsed or (getattr(parsed, "bozo", 0) and not getattr(parsed, "entries", None)):
            raise ValueError("Failed to parse that feed URL.")

        set_feed(
            client,
            feed_url,
            {
                "last_ts": 0.0,
                "enabled": False,
                "portals": {},
            },
        )
        return {"ok": True, "url": feed_url, "message": "Feed added."}

    if action_name == "rss_save_feed":
        feed_url = _payload_text(body, "id") or _payload_text(body, "url")
        if not feed_url:
            raise ValueError("Feed URL is required.")

        existing = get_all_feeds(client) or {}
        if feed_url not in existing:
            raise KeyError("Feed not found.")

        enabled = _as_bool_flag(_payload_value(values, "enabled", False), False)
        portals = _portals_from_form(_payload_portals_from_values(values))
        update_feed(
            client,
            feed_url,
            {
                "enabled": enabled,
                "portals": portals,
            },
        )
        return {"ok": True, "url": feed_url, "message": "Feed settings saved."}

    if action_name == "rss_remove_feed":
        feed_url = _payload_text(body, "id") or _payload_text(body, "url")
        if not feed_url:
            raise ValueError("Feed URL is required.")

        deleted = delete_feed(client, feed_url)
        if not deleted:
            raise KeyError("Feed not found.")
        return {"ok": True, "url": feed_url, "message": "Feed removed."}

    raise ValueError(f"Unknown action: {action_name}")


def run(stop_event=None):
    logger.info("[RSS] Core starting.")
    llm_client = get_llm_client_from_env()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    backoff = 1.0
    max_backoff = 10.0

    while not (stop_event and stop_event.is_set()):
        try:
            rss_manager = RSSManager(llm_client=llm_client)
            feed_count = len(rss_manager.get_feeds())
            logger.info("[RSS] Poller active with %d configured feed(s).", feed_count)
            loop.run_until_complete(rss_manager.poll_feeds(stop_event=stop_event))

            if stop_event and stop_event.is_set():
                break

            logger.warning("poll_feeds() returned; restarting shortly…")
            time.sleep(1.0)
            backoff = 1.0

        except asyncio.CancelledError:
            logger.info("RSS poller cancelled; exiting thread.")
            break
        except KeyboardInterrupt:
            logger.info("RSS poller interrupted; exiting thread.")
            break
        except Exception as e:
            logger.error(f"RSS crashed: {e}", exc_info=True)
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

    try:
        loop.close()
    except Exception:
        pass
    logger.info("[RSS] Core stopped.")
