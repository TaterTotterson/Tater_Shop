import asyncio
import email
import hashlib
import imaplib
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from helpers import extract_json, get_llm_client_from_env, redis_client
from notify import core_notifier_platforms, dispatch_notification, notifier_destination_catalog

__version__ = "1.0.31"

load_dotenv()

logger = logging.getLogger("personal_core")
logger.setLevel(logging.INFO)


CORE_SETTINGS = {
    "category": "Personal Core Settings",
    # Allow Hydra tools to search/summarize cached email data even if the scanner loop is stopped.
    "hydra_tools_require_running": False,
    "required": {
        "interval_seconds": {
            "label": "Scan Interval (sec)",
            "type": "select",
            "default": "300",
            "options": [
                {"value": "60", "label": "1 minute"},
                {"value": "120", "label": "2 minutes"},
                {"value": "300", "label": "5 minutes"},
                {"value": "600", "label": "10 minutes"},
                {"value": "900", "label": "15 minutes"},
                {"value": "1800", "label": "30 minutes"},
                {"value": "3600", "label": "1 hour"},
            ],
            "description": "How often Personal Core checks connected inboxes.",
        },
        "provider": {
            "label": "Email Provider",
            "type": "select",
            "default": "gmail",
            "options": [
                {"value": "gmail", "label": "Gmail"},
                {"value": "apple", "label": "Apple / iCloud"},
                {"value": "yahoo", "label": "Yahoo"},
                {"value": "outlook", "label": "Outlook / Office365"},
                {"value": "aol", "label": "AOL"},
                {"value": "custom", "label": "Custom IMAP"},
            ],
            "description": "Primary provider to scan. Use app passwords where required.",
        },
        "email_address": {
            "label": "Email Address",
            "type": "string",
            "default": "",
            "description": "Primary account login email.",
        },
        "email_password": {
            "label": "Email App Password",
            "type": "string",
            "default": "",
            "description": "IMAP/app password for the primary account.",
        },
        "imap_host": {
            "label": "Custom IMAP Host (optional)",
            "type": "string",
            "default": "",
            "description": "Leave blank to use provider defaults.",
        },
        "imap_port": {
            "label": "IMAP Port",
            "type": "number",
            "default": 993,
            "description": "IMAP SSL port (usually 993).",
        },
        "mailbox": {
            "label": "Mailbox",
            "type": "string",
            "default": "INBOX",
            "description": "Mailbox/folder to scan.",
        },
        "extra_accounts_json": {
            "label": "Extra Accounts JSON (optional)",
            "type": "textarea",
            "default": "",
            "description": (
                "Optional JSON list of extra accounts. Example: "
                "[{\"provider\":\"yahoo\",\"email_address\":\"name@yahoo.com\",\"email_password\":\"app-pass\"}]"
            ),
        },
        "lookback_limit": {
            "label": "Emails Per Scan",
            "type": "number",
            "default": 40,
            "description": "Maximum new emails fetched per account each scan.",
        },
        "scan_days": {
            "label": "Initial Scan Days",
            "type": "number",
            "default": 21,
            "description": "When first connecting, pull this many days of email history.",
        },
        "max_stored_emails": {
            "label": "Stored Emails Per Account",
            "type": "number",
            "default": 1500,
            "description": "How many normalized emails remain searchable per account.",
        },
        "min_confidence": {
            "label": "Min Confidence",
            "type": "number",
            "default": 0.62,
            "description": "Minimum confidence needed before storing extracted profile updates.",
        },
        "max_spending_entries": {
            "label": "Max Spending Rows",
            "type": "number",
            "default": 600,
            "description": "Maximum stored spending observations per account profile.",
        },
        "max_note_entries": {
            "label": "Max Important Notes",
            "type": "number",
            "default": 300,
            "description": "Maximum stored important-note items per account profile.",
        },
        "max_event_entries": {
            "label": "Max Events",
            "type": "number",
            "default": 260,
            "description": "Maximum stored events per account profile.",
        },
        "prompt_upcoming_days": {
            "label": "Prompt Upcoming Days",
            "type": "number",
            "default": 45,
            "description": "How far ahead to include upcoming plans in Hydra prompt context.",
        },
        "prompt_upcoming_limit": {
            "label": "Prompt Event Limit",
            "type": "number",
            "default": 8,
            "description": "Maximum events injected into Hydra prompt context.",
        },
        "prompt_include_discord": {
            "label": "Inject Into Discord",
            "type": "checkbox",
            "default": False,
            "description": "Allow Personal Core prompt context on Discord conversations.",
        },
        "prompt_include_irc": {
            "label": "Inject Into IRC",
            "type": "checkbox",
            "default": False,
            "description": "Allow Personal Core prompt context on IRC conversations.",
        },
        "prompt_include_telegram": {
            "label": "Inject Into Telegram",
            "type": "checkbox",
            "default": False,
            "description": "Allow Personal Core prompt context on Telegram conversations.",
        },
        "prompt_include_matrix": {
            "label": "Inject Into Matrix",
            "type": "checkbox",
            "default": False,
            "description": "Allow Personal Core prompt context on Matrix conversations.",
        },
    },
}

CORE_WEBUI_TAB = {
    "label": "Personal",
    "order": 25,
    "requires_running": True,
}


_IMAP_PROVIDER_DEFAULTS = {
    "gmail": {"host": "imap.gmail.com", "port": 993},
    "apple": {"host": "imap.mail.me.com", "port": 993},
    "icloud": {"host": "imap.mail.me.com", "port": 993},
    "yahoo": {"host": "imap.mail.yahoo.com", "port": 993},
    "outlook": {"host": "outlook.office365.com", "port": 993},
    "aol": {"host": "imap.aol.com", "port": 993},
    "custom": {"host": "", "port": 993},
}

_PERSONAL_SETTINGS_KEY = "personal_core_settings"
_PERSONAL_STATS_KEY = "personal:stats:core"
_PERSONAL_ACCOUNTS_SET_KEY = "personal:accounts"
_PERSONAL_PROFILE_PREFIX = "personal:profile"
_PERSONAL_HISTORY_PREFIX = "personal:email_history"
_PERSONAL_CURSOR_PREFIX = "personal:cursor_uid"
_PERSONAL_PROCESSED_PREFIX = "personal:processed_msg"
_PERSONAL_NOTIFY_SENT_PREFIX = "personal:notify_sent"

_PROFILE_ENTRY_KIND_TO_BUCKET = {
    "event": "upcoming_events",
    "subscription": "subscriptions",
    "delivery": "deliveries",
    "action": "action_items",
    "spending": "spending_habits",
    "note": "important_notes",
    "favorite": "favorite_places",
}

_AMOUNT_RE = re.compile(r"(?:USD|US\$|\$)\s*([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MONTH_WORDS_RE = re.compile(
    r"\b("
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
    r")\s+([0-9]{1,2})(?:,\s*([0-9]{4}))?",
    re.IGNORECASE,
)
_ISO_DATE_RE = re.compile(r"\b(20[0-9]{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12][0-9]|3[01])\b")
_TRACKING_TOKEN_RE = re.compile(r"\b([A-Z0-9]{10,24})\b")
_DELIVERY_TOKEN_RE = re.compile(r"\b([A-Z0-9][A-Z0-9\-]{8,40})\b")
_ORDER_REF_RE = re.compile(
    r"\b(?:order|ord(?:er)?)\s*(?:number|no\.?|num(?:ber)?|#)?\s*[:#-]?\s*([A-Z0-9][A-Z0-9\-]{2,31})\b",
    re.IGNORECASE,
)
_TRACKING_REF_RE = re.compile(
    r"\b(?:tracking(?:\s*(?:number|#|id))?|track(?:ing)?(?:\s*(?:number|#|id))?)\s*[:#-]?\s*([A-Z0-9][A-Z0-9\-]{6,40})\b",
    re.IGNORECASE,
)
_DELIVERY_ITEM_LINE_RE = re.compile(
    r"\b(?:item(?:s)?|product(?:s)?|contents?|package(?:\s*contains)?|shipped item(?:s)?)\s*[:#-]\s*([^\n\r.;]{4,140})",
    re.IGNORECASE,
)
_DELIVERY_SUBJECT_ITEM_RE = re.compile(
    r"\b(?:your|the)\s+(.{4,90}?)\s+(?:has|have)\s+(?:shipped|been shipped|is on the way)\b",
    re.IGNORECASE,
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="replace").strip()
        except Exception:
            return str(value).strip()
    return str(value).strip()


def _as_int(value: Any, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    if maximum is not None:
        out = min(int(maximum), out)
    return out


def _as_float(value: Any, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    if minimum is not None:
        out = max(float(minimum), out)
    if maximum is not None:
        out = min(float(maximum), out)
    return out


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled", "y"}:
        return True
    if token in {"0", "false", "no", "off", "disabled", "n"}:
        return False
    return bool(default)


def _slug(value: Any, *, default: str = "unknown") -> str:
    text = _text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or default


def _provider_key(value: Any, *, default: str = "custom") -> str:
    token = _slug(value, default=default)
    if token in _IMAP_PROVIDER_DEFAULTS:
        return token
    if "apple" in token or "icloud" in token or "me_com" in token:
        return "apple"
    if "office365" in token or "office_365" in token or "outlook" in token or "hotmail" in token or "live" in token:
        return "outlook"
    if "google" in token or "gmail" in token:
        return "gmail"
    if "yahoo" in token:
        return "yahoo"
    if "aol" in token:
        return "aol"
    return token


def _mask_email(value: Any) -> str:
    raw = _text(value)
    if not raw:
        return ""
    if "@" not in raw:
        if len(raw) <= 2:
            return raw[0] + "***" if raw else ""
        return raw[:2] + "***"
    local, domain = raw.split("@", 1)
    local = local.strip()
    domain = domain.strip()
    if not domain:
        return _mask_email(local)
    if len(local) <= 1:
        local_mask = (local[:1] or "u") + "***"
    elif len(local) == 2:
        local_mask = local[:1] + "***"
    else:
        local_mask = local[:2] + "***"
    return f"{local_mask}@{domain}"


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except Exception:
        return _text(value)


def _iso_from_ts(ts: Any) -> str:
    try:
        value = float(ts)
    except Exception:
        return ""
    if value <= 0:
        return ""
    return datetime.fromtimestamp(value).strftime("%Y-%m-%dT%H:%M:%S")


def _parse_iso_to_ts(value: Any) -> float:
    raw = _text(value)
    if not raw:
        return 0.0
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return 0.0
    if dt.tzinfo:
        return dt.timestamp()
    return dt.replace(tzinfo=timezone.utc).timestamp()


def _decode_header_text(value: Any) -> str:
    raw = _text(value)
    if not raw:
        return ""
    out: List[str] = []
    for part, charset in decode_header(raw):
        if isinstance(part, bytes):
            encoding = charset or "utf-8"
            try:
                out.append(part.decode(encoding, errors="replace"))
            except Exception:
                out.append(part.decode("utf-8", errors="replace"))
        else:
            out.append(str(part))
    return "".join(out).strip()


def _clean_text_blob(value: Any, *, max_chars: int = 6000) -> str:
    text = _text(value)
    if not text:
        return ""
    text = unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    limit = int(max_chars)
    if limit > 0 and len(text) > max(256, limit):
        text = text[: max(256, limit)].rstrip() + "..."
    return text.strip()


def _strip_html(html: Any, *, max_chars: int = 6000) -> str:
    text = _text(html)
    if not text:
        return ""
    text = re.sub(r"<\s*(script|style)[^>]*>.*?<\s*/\s*(script|style)\s*>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = _HTML_TAG_RE.sub(" ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    limit = int(max_chars)
    if limit > 0 and len(text) > max(256, limit):
        text = text[: max(256, limit)].rstrip() + "..."
    return text.strip()


def _normalize_tracking_id(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    text = re.sub(
        r"(?i)^(?:tracking(?:\s*(?:number|#|id))?|track(?:ing)?(?:\s*(?:number|#|id))?)\s*[:#-]?\s*",
        "",
        text,
    ).strip()
    text = text.strip(".,:;()[]{}<>")
    text = text.lstrip("#")
    text = re.sub(r"\s+", "", text)
    return text[:40]


def _normalize_order_number(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    text = re.sub(
        r"(?i)^(?:order|ord(?:er)?)(?:\s*(?:number|no\.?|num(?:ber)?|#))?\s*[:#-]?\s*",
        "",
        text,
    ).strip()
    text = text.strip(".,:;()[]{}<>")
    text = text.lstrip("#")
    text = re.sub(r"\s+", "", text)
    return text[:40]


def _looks_like_order_reference(value: Any) -> bool:
    raw = _text(value)
    if not raw:
        return False
    upper = raw.upper()
    if "ORDER" in upper:
        return True
    token = _normalize_order_number(raw)
    if not token:
        return False
    bare = token.replace("-", "")
    if len(bare) <= 9 and bare.isdigit():
        return True
    if re.fullmatch(r"[A-Z]{0,3}\d{3,8}", bare):
        return True
    return False


def _looks_like_tracking_reference(value: Any) -> bool:
    token = _normalize_tracking_id(value)
    if not token:
        return False
    bare = token.replace("-", "")
    if len(bare) < 8:
        return False
    if bare.isdigit():
        return len(bare) >= 12
    has_alpha = any(ch.isalpha() for ch in bare)
    has_digit = any(ch.isdigit() for ch in bare)
    if has_alpha and has_digit:
        if _looks_like_order_reference(token) and len(bare) < 11:
            return False
        return True
    return False


def _extract_order_number_from_text(*parts: Any) -> str:
    for part in parts:
        text = _text(part)
        if not text:
            continue
        for match in _ORDER_REF_RE.finditer(text):
            token = _normalize_order_number(match.group(1))
            if token:
                return token
    return ""


def _extract_tracking_id_from_text(*parts: Any) -> str:
    for part in parts:
        text = _text(part)
        if not text:
            continue
        for match in _TRACKING_REF_RE.finditer(text):
            token = _normalize_tracking_id(match.group(1))
            if token and _looks_like_tracking_reference(token):
                return token

    blob = " ".join([_text(part).upper() for part in parts if _text(part)])
    for token in _DELIVERY_TOKEN_RE.findall(blob):
        normalized = _normalize_tracking_id(token)
        if not normalized:
            continue
        if _looks_like_order_reference(normalized):
            continue
        if _looks_like_tracking_reference(normalized):
            return normalized
    return ""


def _extract_delivery_item_from_text(*parts: Any) -> str:
    for part in parts:
        text = _text(part)
        if not text:
            continue
        match = _DELIVERY_ITEM_LINE_RE.search(text)
        if match:
            item = _clean_text_blob(match.group(1), max_chars=140).strip(" .,:;-")
            if item:
                return item[:120]

        match = _DELIVERY_SUBJECT_ITEM_RE.search(text)
        if match:
            item = _clean_text_blob(match.group(1), max_chars=140)
            item = re.sub(r"(?i)^(?:your|the)\s+", "", item).strip(" .,:;-")
            if item:
                return item[:120]
    return ""


def _extract_message_text(msg: Any) -> str:
    if msg is None:
        return ""

    plain_parts: List[str] = []
    html_parts: List[str] = []

    try:
        parts = msg.walk() if msg.is_multipart() else [msg]
    except Exception:
        parts = [msg]

    for part in parts:
        try:
            content_type = _text(part.get_content_type()).lower()
        except Exception:
            content_type = ""
        disposition = _text(part.get("Content-Disposition")).lower()
        if "attachment" in disposition:
            continue
        if content_type not in {"text/plain", "text/html"}:
            continue

        payload_bytes = None
        try:
            payload_bytes = part.get_payload(decode=True)
        except Exception:
            payload_bytes = None

        if payload_bytes is None:
            raw_payload = part.get_payload()
            if isinstance(raw_payload, str):
                text_payload = raw_payload
            else:
                text_payload = _text(raw_payload)
        else:
            charset = _text(part.get_content_charset()) or "utf-8"
            try:
                text_payload = payload_bytes.decode(charset, errors="replace")
            except Exception:
                text_payload = payload_bytes.decode("utf-8", errors="replace")

        if content_type == "text/plain":
            plain_parts.append(_clean_text_blob(text_payload, max_chars=8000))
        elif content_type == "text/html":
            html_parts.append(_strip_html(text_payload, max_chars=8000))

    plain = "\n\n".join([p for p in plain_parts if p]).strip()
    if plain:
        return _clean_text_blob(plain, max_chars=8000)

    html = "\n\n".join([h for h in html_parts if h]).strip()
    if html:
        return _clean_text_blob(html, max_chars=8000)

    return ""


def _parse_email_date(header_value: Any) -> float:
    raw = _text(header_value)
    if not raw:
        return 0.0
    try:
        dt = parsedate_to_datetime(raw)
    except Exception:
        return 0.0
    if dt is None:
        return 0.0
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).timestamp()
    return dt.timestamp()


def _stable_message_id(*, account_id: str, uid: int, message_id: str, subject: str) -> str:
    base = _text(message_id) or f"{uid}:{_text(subject)}"
    digest = hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:20]
    return f"{account_id}:{digest}"


def _account_storage_id(account: Dict[str, Any]) -> str:
    provider = _provider_key(account.get("provider"), default="imap")
    email_addr = _text(account.get("email_address")).lower()
    local = _slug(email_addr.split("@", 1)[0] if "@" in email_addr else email_addr, default="account")
    digest = hashlib.sha1(email_addr.encode("utf-8", errors="ignore")).hexdigest()[:10] if email_addr else "anon"
    return f"{provider}_{local[:24]}_{digest}"


def _history_key(account_id: str) -> str:
    return f"{_PERSONAL_HISTORY_PREFIX}:{_slug(account_id)}"


def _profile_key(account_id: str) -> str:
    return f"{_PERSONAL_PROFILE_PREFIX}:{_slug(account_id)}"


def _cursor_key(account_id: str) -> str:
    return f"{_PERSONAL_CURSOR_PREFIX}:{_slug(account_id)}"


def _processed_key(account_id: str) -> str:
    return f"{_PERSONAL_PROCESSED_PREFIX}:{_slug(account_id)}"


def _load_settings() -> Dict[str, Any]:
    raw = redis_client.hgetall(_PERSONAL_SETTINGS_KEY) or {}
    settings = {
        "interval_seconds": _as_int(raw.get("interval_seconds"), 300, minimum=30, maximum=3600),
        "provider": _slug(raw.get("provider"), default="gmail"),
        "email_address": _text(raw.get("email_address")),
        "email_password": _text(raw.get("email_password")),
        "imap_host": _text(raw.get("imap_host")),
        "imap_port": _as_int(raw.get("imap_port"), 993, minimum=1, maximum=65535),
        "mailbox": _text(raw.get("mailbox")) or "INBOX",
        "extra_accounts_json": _text(raw.get("extra_accounts_json")),
        "lookback_limit": _as_int(raw.get("lookback_limit"), 40, minimum=1, maximum=300),
        "scan_days": _as_int(raw.get("scan_days"), 21, minimum=1, maximum=365),
        "max_stored_emails": _as_int(raw.get("max_stored_emails"), 1500, minimum=100, maximum=50000),
        "min_confidence": _as_float(raw.get("min_confidence"), 0.62, minimum=0.0, maximum=1.0),
        "max_spending_entries": _as_int(raw.get("max_spending_entries"), 600, minimum=20, maximum=5000),
        "max_note_entries": _as_int(raw.get("max_note_entries"), 300, minimum=20, maximum=3000),
        "max_event_entries": _as_int(raw.get("max_event_entries"), 260, minimum=20, maximum=2000),
        "prompt_upcoming_days": _as_int(raw.get("prompt_upcoming_days"), 45, minimum=1, maximum=365),
        "prompt_upcoming_limit": _as_int(raw.get("prompt_upcoming_limit"), 8, minimum=1, maximum=50),
        "prompt_include_discord": _as_bool(raw.get("prompt_include_discord"), False),
        "prompt_include_irc": _as_bool(raw.get("prompt_include_irc"), False),
        "prompt_include_telegram": _as_bool(raw.get("prompt_include_telegram"), False),
        "prompt_include_matrix": _as_bool(raw.get("prompt_include_matrix"), False),
        "notifications_enabled": _as_bool(raw.get("notifications_enabled"), False),
        "notify_on_deliveries": _as_bool(raw.get("notify_on_deliveries"), True),
        "notify_on_spending": _as_bool(raw.get("notify_on_spending"), True),
        "notify_on_plans": _as_bool(raw.get("notify_on_plans"), True),
        "notification_platform": _slug(raw.get("notification_platform"), default="webui"),
        "notification_destinations": _normalize_destination_values(raw.get("notification_destinations")),
        "notification_extra_destinations": _normalize_destination_values(raw.get("notification_extra_destinations")),
        "notification_max_per_cycle": _as_int(raw.get("notification_max_per_cycle"), 3, minimum=1, maximum=25),
        "notification_ha_api_notification": _as_bool(raw.get("notification_ha_api_notification"), False),
        "notification_ha_device_services": _text(raw.get("notification_ha_device_services")),
    }
    return settings


def _personal_prompt_enabled_for_platform(*, platform: Any, settings: Optional[Dict[str, Any]] = None) -> bool:
    normalized = _slug(platform, default="")
    if not normalized:
        return True
    cfg = settings if isinstance(settings, dict) else _load_settings()

    if normalized == "discord" or normalized.startswith("discord_"):
        return _as_bool(cfg.get("prompt_include_discord"), False)
    if normalized == "irc" or normalized.startswith("irc_"):
        return _as_bool(cfg.get("prompt_include_irc"), False)
    if normalized == "telegram" or normalized.startswith("telegram_"):
        return _as_bool(cfg.get("prompt_include_telegram"), False)
    if normalized == "matrix" or normalized.startswith("matrix_"):
        return _as_bool(cfg.get("prompt_include_matrix"), False)
    return True


def _normalize_destination_values(raw: Any) -> List[str]:
    values: List[Any]
    if isinstance(raw, list):
        values = list(raw)
    else:
        text = _text(raw)
        if not text:
            values = []
        elif text.startswith("["):
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                values = parsed
            else:
                values = [text]
        else:
            values = [text]

    out: List[str] = []
    seen: set[str] = set()
    for item in values:
        token = _text(item)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _clean_targets_dict(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for key, value in raw.items():
        token = _text(key)
        item = _text(value)
        if token and item:
            out[token] = item
    return out


def _load_destination_catalog() -> Dict[str, Any]:
    try:
        payload = notifier_destination_catalog(redis_client=redis_client, limit=250)
    except Exception:
        return {"platforms": []}
    if not isinstance(payload, dict):
        return {"platforms": []}
    rows = payload.get("platforms")
    if not isinstance(rows, list):
        payload["platforms"] = []
    return payload


def _catalog_platform_row(catalog: Dict[str, Any], platform: str) -> Dict[str, Any]:
    rows = catalog.get("platforms") if isinstance(catalog, dict) else []
    if not isinstance(rows, list):
        return {}
    wanted = _slug(platform, default="")
    for row in rows:
        if not isinstance(row, dict):
            continue
        token = _slug(row.get("platform"), default="")
        if token == wanted:
            return row
    return {}


def _encode_destination_value(platform: str, targets: Dict[str, Any]) -> str:
    plat = _slug(platform, default="")
    if not plat:
        return ""
    payload = {
        "platform": plat,
        "targets": _clean_targets_dict(targets),
    }
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)
    except Exception:
        return ""


def _decode_destination_value(raw_value: Any) -> Tuple[str, Dict[str, str]]:
    value = _text(raw_value)
    if not value:
        return "", {}
    try:
        parsed = json.loads(value)
    except Exception:
        return "", {}
    if not isinstance(parsed, dict):
        return "", {}
    platform = _slug(parsed.get("platform"), default="")
    targets = _clean_targets_dict(parsed.get("targets"))
    return platform, targets


def _target_from_text(platform: str, text_value: Any) -> Dict[str, str]:
    platform_name = _slug(platform, default="")
    text = _text(text_value)
    if not text:
        return {}
    if platform_name == "discord":
        if text.isdigit():
            return {"channel_id": text}
        return {"channel": text}
    if platform_name == "irc":
        return {"channel": text if text.startswith("#") else f"#{text}"}
    if platform_name == "matrix":
        return {"room_id": text}
    if platform_name == "telegram":
        return {"chat_id": text}
    if platform_name == "macos":
        return {"scope": text}
    if platform_name == "homeassistant":
        return {"device_service": text}
    return {}


def _destination_options_for_platform(
    platform: str,
    *,
    catalog: Dict[str, Any],
    current_targets: Optional[Any] = None,
) -> List[Dict[str, str]]:
    platform_name = _slug(platform, default="")
    row = _catalog_platform_row(catalog, platform_name)
    requires_target = bool(row.get("requires_target")) if isinstance(row, dict) else True

    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    if requires_target:
        out.append({"value": "", "label": "(Select destination)"})
        seen.add("")
    else:
        default_value = _encode_destination_value(platform_name, {})
        out.append({"value": default_value, "label": "Portal defaults"})
        seen.add(default_value)

    destinations = row.get("destinations") if isinstance(row, dict) else []
    if isinstance(destinations, list):
        for item in destinations:
            if not isinstance(item, dict):
                continue
            targets = _clean_targets_dict(item.get("targets"))
            value = _encode_destination_value(platform_name, targets)
            if not value or value in seen:
                continue
            label = _text(item.get("label")) or value
            out.append({"value": value, "label": label})
            seen.add(value)

    current_rows: List[Dict[str, str]] = []
    if isinstance(current_targets, list):
        for row in current_targets:
            clean = _clean_targets_dict(row)
            if clean:
                current_rows.append(clean)
    elif isinstance(current_targets, dict):
        clean = _clean_targets_dict(current_targets)
        if clean:
            current_rows.append(clean)

    for current_clean in current_rows:
        current_value = _encode_destination_value(platform_name, current_clean)
        if current_value and current_value not in seen:
            out.append({"value": current_value, "label": "Current target"})
            seen.add(current_value)

    return out


def _notification_platform_options(*, catalog: Dict[str, Any]) -> List[Dict[str, str]]:
    row_map: Dict[str, Dict[str, Any]] = {}
    for row in catalog.get("platforms") if isinstance(catalog.get("platforms"), list) else []:
        if not isinstance(row, dict):
            continue
        platform = _slug(row.get("platform"), default="")
        if platform:
            row_map[platform] = row

    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for platform in core_notifier_platforms():
        token = _slug(platform, default="")
        if not token or token in seen:
            continue
        seen.add(token)
        row = row_map.get(token) or {}
        label = _text(row.get("label")) or token
        out.append({"value": token, "label": label})
    return out


def _destination_options_all_platforms(
    *,
    catalog: Dict[str, Any],
    current_values: Optional[Any] = None,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    platform_rows = catalog.get("platforms") if isinstance(catalog.get("platforms"), list) else []
    for row in platform_rows:
        if not isinstance(row, dict):
            continue
        platform_name = _slug(row.get("platform"), default="")
        if not platform_name:
            continue
        platform_label = _text(row.get("label")) or platform_name
        requires_target = bool(row.get("requires_target"))

        if not requires_target:
            default_value = _encode_destination_value(platform_name, {})
            if default_value and default_value not in seen:
                out.append({"value": default_value, "label": f"{platform_label}: defaults"})
                seen.add(default_value)

        destinations = row.get("destinations")
        if not isinstance(destinations, list):
            continue
        for destination in destinations:
            if not isinstance(destination, dict):
                continue
            targets = _clean_targets_dict(destination.get("targets"))
            encoded = _encode_destination_value(platform_name, targets)
            if not encoded or encoded in seen:
                continue
            label = _text(destination.get("label")) or encoded
            out.append({"value": encoded, "label": f"{platform_label}: {label}"})
            seen.add(encoded)

    for raw_value in _normalize_destination_values(current_values):
        platform_name, targets = _decode_destination_value(raw_value)
        if not platform_name:
            continue
        encoded = _encode_destination_value(platform_name, targets)
        if not encoded or encoded in seen:
            continue
        seen.add(encoded)
        out.append({"value": encoded, "label": f"{platform_name}: current target"})
    return out


def _platform_requires_target(platform: str, *, catalog: Optional[Dict[str, Any]] = None) -> bool:
    platform_name = _slug(platform, default="")
    if catalog:
        row = _catalog_platform_row(catalog, platform_name)
        if isinstance(row, dict) and row:
            return bool(row.get("requires_target"))
    return platform_name in {"discord", "irc", "matrix", "telegram", "macos"}


def _ha_config() -> Dict[str, str]:
    settings = redis_client.hgetall("homeassistant_settings") or {}
    base = _text(settings.get("HA_BASE_URL")).rstrip("/")
    token = _text(settings.get("HA_TOKEN"))
    return {"base": base, "token": token}


def _ha_headers(token: str, *, json_content: bool = True) -> Dict[str, str]:
    headers = {"Authorization": f"Bearer {token}"}
    if json_content:
        headers["Content-Type"] = "application/json"
    return headers


def _normalize_notify_service(raw: Any) -> str:
    value = _text(raw)
    if not value:
        return ""
    if value.lower().startswith("notify."):
        value = value.split(".", 1)[1].strip()
    return _slug(value, default="")


def _normalize_notify_services(raw: Any) -> List[str]:
    values: List[Any]
    if isinstance(raw, list):
        values = list(raw)
    else:
        text = _text(raw)
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                values = parsed
            else:
                values = [part.strip() for part in text.split(",")]
        else:
            values = [part.strip() for part in text.split(",")]

    out: List[str] = []
    seen: set[str] = set()
    for item in values:
        token = _normalize_notify_service(item)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _ha_notify_service_pairs() -> List[Tuple[str, str]]:
    cfg = _ha_config()
    base = _text(cfg.get("base")).rstrip("/")
    token = _text(cfg.get("token"))
    if not base or not token:
        return []
    try:
        response = requests.get(
            f"{base}/api/services",
            headers=_ha_headers(token, json_content=False),
            timeout=5,
        )
        response.raise_for_status()
        domains = response.json() or []
    except Exception:
        return []

    out: List[Tuple[str, str]] = []
    for domain_row in domains:
        if not isinstance(domain_row, dict):
            continue
        if _slug(domain_row.get("domain"), default="") != "notify":
            continue
        services = domain_row.get("services")
        if not isinstance(services, dict):
            continue
        for service_name, service_meta in services.items():
            service = _normalize_notify_service(service_name)
            if not service:
                continue
            meta = service_meta if isinstance(service_meta, dict) else {}
            pretty = _text(meta.get("name"))
            full_service = f"notify.{service}"
            label = f"{pretty} ({full_service})" if pretty else full_service
            out.append((service, label))

    deduped: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for service, label in sorted(out, key=lambda row: row[1].lower()):
        if service in seen:
            continue
        seen.add(service)
        deduped.append((service, label))
    return deduped


def _multiselect_choices_from_pairs(
    pairs: List[Tuple[str, str]],
    *,
    current_values: Any,
) -> List[Dict[str, str]]:
    current = _normalize_notify_services(current_values)
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    for value, label in pairs:
        token = _normalize_notify_service(value)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append({"value": token, "label": _text(label) or f"notify.{token}"})

    for token in current:
        if token in seen:
            continue
        seen.add(token)
        out.append({"value": token, "label": f"notify.{token} (saved)"})
    return out


def _notification_routes_from_settings(
    settings: Dict[str, Any],
    *,
    catalog: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    selected_values = _normalize_destination_values(settings.get("notification_extra_destinations"))

    routes: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _append_route(*, platform: str, targets: Dict[str, Any]) -> None:
        platform_name = _slug(platform, default="")
        if not platform_name:
            return
        clean_targets = _clean_targets_dict(targets)
        encoded = _encode_destination_value(platform_name, clean_targets)
        if not encoded:
            return
        identity = f"{platform_name}|{encoded}"
        if identity in seen:
            return
        seen.add(identity)
        routes.append({"platform": platform_name, "targets": clean_targets})

    for raw_value in selected_values:
        selected_platform, selected_targets = _decode_destination_value(raw_value)
        if not selected_platform:
            continue
        _append_route(platform=selected_platform, targets=selected_targets)

    return routes

def _resolve_accounts(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    primary = {
        "provider": _provider_key(settings.get("provider"), default="gmail"),
        "email_address": _text(settings.get("email_address")),
        "email_password": _text(settings.get("email_password")),
        "imap_host": _text(settings.get("imap_host")),
        "imap_port": _as_int(settings.get("imap_port"), 993, minimum=1, maximum=65535),
        "mailbox": _text(settings.get("mailbox")) or "INBOX",
        "enabled": True,
    }
    out.append(primary)

    extra_json = _text(settings.get("extra_accounts_json"))
    if extra_json:
        try:
            parsed = json.loads(extra_json)
        except Exception:
            parsed = []
        if isinstance(parsed, dict):
            parsed = [parsed]
        if isinstance(parsed, list):
            for row in parsed:
                if not isinstance(row, dict):
                    continue
                enabled = _as_bool(row.get("enabled"), True)
                if not enabled:
                    continue
                merged = {
                    "provider": _provider_key(row.get("provider") or primary.get("provider"), default="gmail"),
                    "email_address": _text(row.get("email_address") or row.get("username")),
                    "email_password": _text(row.get("email_password") or row.get("password")),
                    "imap_host": _text(row.get("imap_host")),
                    "imap_port": _as_int(row.get("imap_port"), 993, minimum=1, maximum=65535),
                    "mailbox": _text(row.get("mailbox")) or "INBOX",
                    "enabled": True,
                }
                out.append(merged)

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for account in out:
        account_id = _account_storage_id(account)
        if account_id in seen:
            continue
        seen.add(account_id)
        account_copy = dict(account)
        account_copy["account_id"] = account_id
        deduped.append(account_copy)
    return deduped


def _imap_host_port(account: Dict[str, Any]) -> Tuple[str, int]:
    provider = _provider_key(account.get("provider"), default="custom")
    defaults = _IMAP_PROVIDER_DEFAULTS.get(provider) or _IMAP_PROVIDER_DEFAULTS.get("custom") or {"host": "", "port": 993}
    host = _text(account.get("imap_host")) or _text(defaults.get("host"))
    port = _as_int(account.get("imap_port"), _as_int(defaults.get("port"), 993, minimum=1, maximum=65535), minimum=1, maximum=65535)
    return host, port


def _parse_uid_numbers(search_data: Any) -> List[int]:
    raw_uid_line = b""
    if isinstance(search_data, list) and search_data:
        raw_uid_line = search_data[0] or b""
    uid_values = _text(raw_uid_line).split()
    uid_numbers = [
        _as_int(token, 0, minimum=0)
        for token in uid_values
        if _as_int(token, 0, minimum=0) > 0
    ]
    return sorted(set(uid_numbers))


def _imap_uid_search(imap: Any, *, last_uid: int, scan_days: int) -> Tuple[List[int], str]:
    if last_uid > 0:
        criteria_options = [
            f"(UID {last_uid + 1}:*)",
            f"UID {last_uid + 1}:*",
        ]
    else:
        since_date = (datetime.utcnow() - timedelta(days=scan_days)).strftime("%d-%b-%Y")
        criteria_options = [
            f'(SINCE "{since_date}")',
            f"(SINCE {since_date})",
            f"SINCE {since_date}",
        ]

    attempts: List[str] = []
    had_ok_empty = False
    for criteria in criteria_options:
        status, data = imap.uid("search", None, criteria)
        status_text = _text(status).upper()
        if status_text == "OK":
            parsed = _parse_uid_numbers(data)
            if parsed:
                return parsed, criteria
            had_ok_empty = True
            attempts.append(f"{criteria} -> OK(0)")
            continue
        attempts.append(f"{criteria} -> {status_text or 'UNKNOWN'}")

    # First run: if recent-window search is empty, fallback to ALL so initial backfill
    # still picks up mailbox history (bounded later by lookback_limit).
    if last_uid <= 0:
        for criteria in ("ALL", "(ALL)"):
            status, data = imap.uid("search", None, criteria)
            status_text = _text(status).upper()
            if status_text == "OK":
                parsed = _parse_uid_numbers(data)
                if parsed:
                    return parsed, criteria
                had_ok_empty = True
                attempts.append(f"{criteria} -> OK(0)")
                continue
            attempts.append(f"{criteria} -> {status_text or 'UNKNOWN'}")

    if had_ok_empty:
        # Valid search responses with no matching messages.
        return [], criteria_options[0] if criteria_options else "UNKNOWN"

    attempt_text = "; ".join(attempts) if attempts else "no attempts"
    raise RuntimeError(f"IMAP search failed ({attempt_text})")


def _extract_raw_message_bytes(fetch_data: Any) -> bytes:
    if isinstance(fetch_data, (bytes, bytearray)):
        data = bytes(fetch_data)
        return data if data else b""
    if not isinstance(fetch_data, list):
        return b""
    for item in fetch_data:
        if isinstance(item, tuple) and len(item) >= 2:
            payload = item[1]
            if isinstance(payload, (bytes, bytearray)):
                data = bytes(payload)
                if data:
                    return data
        elif isinstance(item, (bytes, bytearray)):
            data = bytes(item)
            if data and (b"From:" in data or b"Subject:" in data or b"Date:" in data):
                return data
    return b""


def _normalize_email_row(msg: Any, *, account_id: str, uid: int) -> Optional[Dict[str, Any]]:
    if msg is None:
        return None
    subject = _decode_header_text(msg.get("Subject"))
    from_value = _decode_header_text(msg.get("From"))
    to_value = _decode_header_text(msg.get("To"))
    message_id = _text(msg.get("Message-ID"))
    ts = _parse_email_date(msg.get("Date"))
    if ts <= 0:
        ts = time.time()
    body = _extract_message_text(msg)
    snippet = _clean_text_blob(body, max_chars=350)
    if not snippet and subject:
        snippet = subject
    if not subject and not snippet:
        return None

    stable_id = _stable_message_id(account_id=account_id, uid=uid, message_id=message_id, subject=subject)
    row = {
        "id": stable_id,
        "account_id": account_id,
        "uid": int(uid),
        "message_id": message_id,
        "from": from_value,
        "to": to_value,
        "subject": subject,
        "date_ts": float(ts),
        "date_iso": _iso_from_ts(ts),
        "snippet": snippet,
        "body": _clean_text_blob(body, max_chars=7000),
    }
    return row


def _fetch_new_emails_for_account(account: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    account_id = _text(account.get("account_id")) or _account_storage_id(account)
    provider = _provider_key(account.get("provider"), default="custom")
    email_address = _text(account.get("email_address"))
    masked_email = _mask_email(email_address)
    email_password = _text(account.get("email_password"))
    mailbox = _text(account.get("mailbox")) or "INBOX"
    lookback_limit = _as_int(settings.get("lookback_limit"), 40, minimum=1, maximum=300)
    scan_days = _as_int(settings.get("scan_days"), 21, minimum=1, maximum=365)

    if not email_address or not email_password:
        error_text = "Missing email credentials in settings."
        logger.warning(
            "[personal_core] account=%s provider=%s email=%s %s",
            account_id,
            provider,
            masked_email or "n/a",
            error_text,
        )
        return {
            "ok": False,
            "account_id": account_id,
            "error": error_text,
            "emails": [],
            "max_uid_seen": _as_int(redis_client.get(_cursor_key(account_id)), 0, minimum=0),
        }

    last_uid = _as_int(redis_client.get(_cursor_key(account_id)), 0, minimum=0)
    host, port = _imap_host_port(account)
    logger.debug(
        "[personal_core] account=%s provider=%s email=%s scan start host=%s port=%s mailbox=%s last_uid=%s",
        account_id,
        provider,
        masked_email or "n/a",
        host or "n/a",
        port,
        mailbox,
        last_uid,
    )
    if not host:
        error_text = "Missing IMAP host for this account."
        logger.warning(
            "[personal_core] account=%s provider=%s email=%s %s",
            account_id,
            provider,
            masked_email or "n/a",
            error_text,
        )
        return {
            "ok": False,
            "account_id": account_id,
            "error": error_text,
            "emails": [],
            "max_uid_seen": _as_int(redis_client.get(_cursor_key(account_id)), 0, minimum=0),
        }

    imap = None
    try:
        try:
            imap = imaplib.IMAP4_SSL(host=host, port=port, timeout=30)
        except Exception as exc:
            error_text = f"IMAP connection failed for {host}:{port}: {exc}"
            logger.warning(
                "[personal_core] account=%s provider=%s email=%s %s",
                account_id,
                provider,
                masked_email or "n/a",
                error_text,
            )
            return {
                "ok": False,
                "account_id": account_id,
                "emails": [],
                "max_uid_seen": last_uid,
                "error": error_text,
            }

        try:
            imap.login(email_address, email_password)
        except imaplib.IMAP4.error as exc:
            error_text = f"IMAP login failed for {masked_email or 'account'}: {exc}"
            logger.warning(
                "[personal_core] account=%s provider=%s email=%s %s",
                account_id,
                provider,
                masked_email or "n/a",
                error_text,
            )
            return {
                "ok": False,
                "account_id": account_id,
                "emails": [],
                "max_uid_seen": last_uid,
                "error": error_text,
            }
        except Exception as exc:
            error_text = f"IMAP login error for {masked_email or 'account'}: {exc}"
            logger.warning(
                "[personal_core] account=%s provider=%s email=%s %s",
                account_id,
                provider,
                masked_email or "n/a",
                error_text,
            )
            return {
                "ok": False,
                "account_id": account_id,
                "emails": [],
                "max_uid_seen": last_uid,
                "error": error_text,
            }

        try:
            select_status, _ = imap.select(mailbox, readonly=True)
        except Exception as exc:
            error_text = f"Unable to open mailbox '{mailbox}': {exc}"
            logger.warning(
                "[personal_core] account=%s provider=%s email=%s %s",
                account_id,
                provider,
                masked_email or "n/a",
                error_text,
            )
            return {
                "ok": False,
                "account_id": account_id,
                "emails": [],
                "max_uid_seen": last_uid,
                "error": error_text,
            }
        if _text(select_status).upper() != "OK":
            error_text = f"Unable to open mailbox '{mailbox}' (status={_text(select_status) or 'UNKNOWN'})"
            logger.warning(
                "[personal_core] account=%s provider=%s email=%s %s",
                account_id,
                provider,
                masked_email or "n/a",
                error_text,
            )
            return {
                "ok": False,
                "account_id": account_id,
                "emails": [],
                "max_uid_seen": last_uid,
                "error": error_text,
            }

        uid_numbers, criteria_used = _imap_uid_search(
            imap,
            last_uid=last_uid,
            scan_days=scan_days,
        )
        if uid_numbers:
            logger.debug(
                "[personal_core] account=%s provider=%s email=%s search criteria=%s uid_candidates=%s",
                account_id,
                provider,
                masked_email or "n/a",
                criteria_used,
                len(uid_numbers),
            )
        else:
            logger.debug(
                "[personal_core] account=%s provider=%s email=%s search criteria=%s uid_candidates=0",
                account_id,
                provider,
                masked_email or "n/a",
                criteria_used,
            )
        if not uid_numbers:
            try:
                imap.close()
            except Exception:
                pass
            try:
                imap.logout()
            except Exception:
                pass
            return {
                "ok": True,
                "account_id": account_id,
                "emails": [],
                "max_uid_seen": last_uid,
            }

        target_uids = uid_numbers[-lookback_limit:]
        logger.debug(
            "[personal_core] account=%s provider=%s email=%s fetching_uids=%s (lookback_limit=%s)",
            account_id,
            provider,
            masked_email or "n/a",
            len(target_uids),
            lookback_limit,
        )
        rows: List[Dict[str, Any]] = []
        max_uid_seen = last_uid
        fetch_attempted = len(target_uids)
        fetch_status_ok = 0
        fetch_non_ok = 0
        raw_bytes_count = 0
        raw_missing_count = 0
        parsed_count = 0
        parse_error_count = 0
        dropped_empty_count = 0
        for uid in target_uids:
            raw_bytes = b""
            uid_fetch_ok = False
            for fetch_query in ("(BODY.PEEK[])", "(RFC822)"):
                fetch_status, fetch_data = imap.uid("fetch", str(uid), fetch_query)
                if _text(fetch_status).upper() != "OK":
                    continue
                uid_fetch_ok = True
                fetch_status_ok += 1
                raw_bytes = _extract_raw_message_bytes(fetch_data)
                if raw_bytes:
                    break
            if not uid_fetch_ok:
                fetch_non_ok += 1
                continue
            if uid > max_uid_seen:
                max_uid_seen = uid
            if not raw_bytes:
                raw_missing_count += 1
                continue
            raw_bytes_count += 1
            try:
                msg = email.message_from_bytes(raw_bytes)
            except Exception:
                parse_error_count += 1
                continue
            parsed_count += 1
            normalized = _normalize_email_row(msg, account_id=account_id, uid=uid)
            if normalized:
                rows.append(normalized)
            else:
                dropped_empty_count += 1

        rows.sort(key=lambda row: int(row.get("uid") or 0))
        logger.debug(
            "[personal_core] account=%s provider=%s email=%s fetch summary: uid_candidates=%s selected=%s attempted=%s ok=%s non_ok=%s raw=%s raw_missing=%s parsed=%s parse_errors=%s normalized=%s dropped_empty=%s",
            account_id,
            provider,
            masked_email or "n/a",
            len(uid_numbers),
            len(target_uids),
            fetch_attempted,
            fetch_status_ok,
            fetch_non_ok,
            raw_bytes_count,
            raw_missing_count,
            parsed_count,
            parse_error_count,
            len(rows),
            dropped_empty_count,
        )

        try:
            imap.close()
        except Exception:
            pass
        try:
            imap.logout()
        except Exception:
            pass

        return {
            "ok": True,
            "account_id": account_id,
            "emails": rows,
            "max_uid_seen": max_uid_seen,
            "uid_candidates": len(uid_numbers),
            "uids_selected": len(target_uids),
            "fetch_attempted": fetch_attempted,
            "fetch_status_ok": fetch_status_ok,
            "fetch_non_ok": fetch_non_ok,
            "raw_bytes_count": raw_bytes_count,
            "raw_missing_count": raw_missing_count,
            "parsed_count": parsed_count,
            "parse_error_count": parse_error_count,
            "normalized_count": len(rows),
            "dropped_empty_count": dropped_empty_count,
        }
    except imaplib.IMAP4.error as exc:
        error_text = f"IMAP auth/connect error: {exc}"
        logger.warning(
            "[personal_core] account=%s provider=%s email=%s %s",
            account_id,
            provider,
            masked_email or "n/a",
            error_text,
        )
        return {
            "ok": False,
            "account_id": account_id,
            "emails": [],
            "max_uid_seen": last_uid,
            "error": error_text,
        }
    except Exception as exc:
        error_text = f"IMAP scan failed: {exc}"
        logger.warning(
            "[personal_core] account=%s provider=%s email=%s %s",
            account_id,
            provider,
            masked_email or "n/a",
            error_text,
        )
        return {
            "ok": False,
            "account_id": account_id,
            "emails": [],
            "max_uid_seen": last_uid,
            "error": error_text,
        }
    finally:
        if imap is not None:
            try:
                imap.logout()
            except Exception:
                pass


def _persist_normalized_emails(account_id: str, rows: List[Dict[str, Any]], *, max_stored: int) -> Dict[str, Any]:
    history_key = _history_key(account_id)
    processed_key = _processed_key(account_id)
    inserted: List[Dict[str, Any]] = []

    for row in rows:
        row_id = _text(row.get("id"))
        if not row_id:
            continue
        try:
            added = int(redis_client.sadd(processed_key, row_id) or 0)
        except Exception:
            added = 1
        if added <= 0:
            continue
        inserted.append(dict(row))
        try:
            redis_client.rpush(history_key, json.dumps(row, ensure_ascii=False))
        except Exception:
            continue

    if max_stored > 0:
        try:
            redis_client.ltrim(history_key, -int(max_stored), -1)
        except Exception:
            pass

    try:
        redis_client.sadd(_PERSONAL_ACCOUNTS_SET_KEY, account_id)
    except Exception:
        pass

    total_stored = _as_int(redis_client.llen(history_key), 0, minimum=0)
    return {
        "inserted": inserted,
        "inserted_count": len(inserted),
        "total_stored": total_stored,
    }


def _load_email_history(account_id: str, *, limit: int = 2000) -> List[Dict[str, Any]]:
    history_key = _history_key(account_id)
    rows: List[Dict[str, Any]] = []
    raw_items = []
    try:
        raw_items = redis_client.lrange(history_key, -max(1, int(limit)), -1) or []
    except Exception:
        raw_items = []
    for raw in raw_items:
        try:
            parsed = json.loads(_text(raw))
        except Exception:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    rows.sort(key=lambda row: _as_float(row.get("date_ts"), 0.0))
    return rows


def _default_profile(account_id: str) -> Dict[str, Any]:
    now_ts = time.time()
    return {
        "schema_version": 1,
        "account_id": _text(account_id),
        "created_at": now_ts,
        "last_updated": 0.0,
        "spending_habits": [],
        "favorite_places": [],
        "important_notes": [],
        "upcoming_events": [],
        "subscriptions": [],
        "deliveries": [],
        "action_items": [],
        "stats": {
            "emails_stored": 0,
            "emails_scanned_total": 0,
            "emails_new_last_run": 0,
            "llm_updates_total": 0,
            "heuristic_updates_total": 0,
            "spending_total": 0.0,
            "spending_30d": 0.0,
            "upcoming_events": 0,
            "subscriptions": 0,
            "deliveries_open": 0,
            "action_items_open": 0,
            "last_scan_ts": 0.0,
            "last_scan_status": "idle",
            "last_error": "",
        },
    }


def _load_profile(account_id: str) -> Dict[str, Any]:
    key = _profile_key(account_id)
    raw = _text(redis_client.get(key))
    if not raw:
        return _default_profile(account_id)
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = {}
    if not isinstance(parsed, dict):
        return _default_profile(account_id)
    base = _default_profile(account_id)
    base.update(parsed)
    stats = base.get("stats") if isinstance(base.get("stats"), dict) else {}
    merged_stats = dict(_default_profile(account_id).get("stats") or {})
    merged_stats.update(stats)
    base["stats"] = merged_stats
    for key_name in (
        "spending_habits",
        "favorite_places",
        "important_notes",
        "upcoming_events",
        "subscriptions",
        "deliveries",
        "action_items",
    ):
        rows = base.get(key_name)
        if not isinstance(rows, list):
            base[key_name] = []
    return base


def _save_profile(account_id: str, profile: Dict[str, Any]) -> None:
    key = _profile_key(account_id)
    try:
        redis_client.set(key, json.dumps(profile, ensure_ascii=False))
        redis_client.sadd(_PERSONAL_ACCOUNTS_SET_KEY, account_id)
    except Exception:
        return


def _merchant_from_sender(sender: str) -> str:
    source = _text(sender)
    if not source:
        return ""
    if "<" in source:
        source = source.split("<", 1)[0].strip()
    source = source.replace('"', "").strip()
    source = re.sub(r"\s+", " ", source)
    return source[:80].strip()


def _extract_first_amount(text: str) -> Optional[float]:
    match = _AMOUNT_RE.search(_text(text))
    if not match:
        return None
    return _as_float(match.group(1), 0.0, minimum=0.0)


def _coerce_event_ts(candidate_text: str, fallback_ts: float) -> float:
    text = _text(candidate_text)
    if not text:
        return 0.0

    iso = _ISO_DATE_RE.search(text)
    if iso:
        year = _as_int(iso.group(1), 0, minimum=2000, maximum=2200)
        month = _as_int(iso.group(2), 1, minimum=1, maximum=12)
        day = _as_int(iso.group(3), 1, minimum=1, maximum=31)
        try:
            dt = datetime(year, month, day, 12, 0, 0)
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            pass

    month_match = _MONTH_WORDS_RE.search(text)
    if month_match:
        month_name = _text(month_match.group(1)).lower()[:3]
        month_index = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }.get(month_name, 0)
        day = _as_int(month_match.group(2), 1, minimum=1, maximum=31)
        year = _as_int(month_match.group(3), datetime.utcnow().year, minimum=2000, maximum=2200)
        if month_index > 0:
            try:
                dt = datetime(year, month_index, day, 12, 0, 0)
                return dt.replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                pass

    return 0.0 if fallback_ts <= 0 else float(fallback_ts)


def _heuristic_extract_updates(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    spending: List[Dict[str, Any]] = []
    notes: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    subscriptions: List[Dict[str, Any]] = []
    deliveries: List[Dict[str, Any]] = []
    action_items: List[Dict[str, Any]] = []

    purchase_keywords = {
        "receipt",
        "order",
        "charged",
        "payment",
        "invoice",
        "purchase",
        "ticket",
        "reservation",
    }
    event_keywords = {
        "trip",
        "flight",
        "hotel",
        "reservation",
        "itinerary",
        "concert",
        "movie",
        "show",
        "appointment",
        "booking",
    }
    important_keywords = {
        "important",
        "action required",
        "due",
        "confirm",
        "update",
        "alert",
        "verification",
    }
    subscription_keywords = {
        "subscription",
        "renewal",
        "renews",
        "auto-renew",
        "membership",
        "plan",
        "monthly",
        "annual",
        "yearly",
        "billing",
        "next charge",
        "next payment",
    }
    delivery_keywords = {
        "tracking",
        "track your package",
        "shipped",
        "in transit",
        "out for delivery",
        "delivered",
        "package",
        "shipment",
        "delivery update",
        "label created",
    }
    action_keywords = {
        "action required",
        "respond by",
        "please respond",
        "due",
        "deadline",
        "verify",
        "verification required",
        "confirm",
        "confirm by",
        "payment due",
        "past due",
    }

    for row in rows:
        email_id = _text(row.get("id"))
        subject = _text(row.get("subject"))
        sender = _text(row.get("from"))
        snippet = _text(row.get("snippet"))
        body = _text(row.get("body"))
        ts = _as_float(row.get("date_ts"), time.time(), minimum=0.0)

        corpus = " ".join([subject.lower(), snippet.lower(), body.lower()])

        if any(token in corpus for token in purchase_keywords):
            amount = _extract_first_amount(" ".join([subject, snippet, body]))
            if amount is not None and amount > 0:
                merchant = _merchant_from_sender(sender) or subject.split("-", 1)[0].strip()
                spending.append(
                    {
                        "merchant": merchant or "Unknown Merchant",
                        "amount": float(amount),
                        "currency": "USD",
                        "category": "purchase",
                        "confidence": 0.66,
                        "email_id": email_id,
                        "observed_at": _iso_from_ts(ts),
                        "observed_ts": ts,
                    }
                )

        if any(token in corpus for token in important_keywords):
            notes.append(
                {
                    "title": subject or "Important email",
                    "summary": snippet or _clean_text_blob(body, max_chars=220),
                    "kind": "important",
                    "confidence": 0.6,
                    "email_id": email_id,
                    "date_iso": _iso_from_ts(ts),
                    "date_ts": ts,
                }
            )

        if any(token in corpus for token in event_keywords):
            event_ts = _coerce_event_ts(" ".join([subject, snippet, body]), fallback_ts=0.0)
            if event_ts > 0:
                kind = "event"
                lower = corpus
                if "movie" in lower or "ticket" in lower:
                    kind = "movie"
                elif "trip" in lower or "flight" in lower or "hotel" in lower:
                    kind = "trip"
                elif "appointment" in lower:
                    kind = "appointment"
                events.append(
                    {
                        "title": subject or "Upcoming event",
                        "kind": kind,
                        "starts_at": _iso_from_ts(event_ts),
                        "starts_ts": event_ts,
                        "location": "",
                        "summary": snippet,
                        "confidence": 0.58,
                        "email_id": email_id,
                    }
                )

        if any(token in corpus for token in subscription_keywords):
            amount = _extract_first_amount(" ".join([subject, snippet, body]))
            cadence = "unknown"
            if any(token in corpus for token in {"monthly", "per month"}):
                cadence = "monthly"
            elif any(token in corpus for token in {"annual", "yearly", "per year"}):
                cadence = "yearly"
            elif "weekly" in corpus:
                cadence = "weekly"
            elif "quarterly" in corpus:
                cadence = "quarterly"

            next_charge_ts = _coerce_event_ts(" ".join([subject, snippet, body]), fallback_ts=0.0)
            if amount is not None or next_charge_ts > 0:
                merchant = _merchant_from_sender(sender) or subject.split("-", 1)[0].strip() or "Unknown Subscription"
                plan = subject[:120].strip() or merchant
                confidence = 0.6 if amount is not None else 0.55
                stable = hashlib.sha1(
                    f"sub|{merchant}|{plan}|{next_charge_ts}|{email_id}".encode("utf-8", errors="ignore")
                ).hexdigest()[:24]
                subscriptions.append(
                    {
                        "id": stable,
                        "merchant": merchant,
                        "plan": plan,
                        "amount": float(amount) if amount is not None else 0.0,
                        "currency": "USD",
                        "cadence": cadence,
                        "next_charge_at": _iso_from_ts(next_charge_ts) if next_charge_ts > 0 else "",
                        "next_charge_ts": next_charge_ts if next_charge_ts > 0 else 0.0,
                        "confidence": confidence,
                        "email_id": email_id,
                    }
                )

        if any(token in corpus for token in delivery_keywords):
            upper_blob = " ".join([subject, snippet, body]).upper()
            tracking_id = _extract_tracking_id_from_text(subject, snippet, body)
            order_number = _extract_order_number_from_text(subject, snippet, body)
            if tracking_id and not order_number and _looks_like_order_reference(tracking_id):
                order_number = _normalize_order_number(tracking_id)
                tracking_id = ""
            if order_number and not tracking_id and _looks_like_tracking_reference(order_number):
                tracking_id = _normalize_tracking_id(order_number)
                order_number = ""
            item_description = _extract_delivery_item_from_text(subject, snippet, body)

            carrier = ""
            if "UPS" in upper_blob:
                carrier = "UPS"
            elif "USPS" in upper_blob:
                carrier = "USPS"
            elif "FEDEX" in upper_blob:
                carrier = "FedEx"
            elif "DHL" in upper_blob:
                carrier = "DHL"
            elif "AMAZON" in upper_blob:
                carrier = "Amazon Logistics"

            status = "update"
            if "delivered" in corpus:
                status = "delivered"
            elif "out for delivery" in corpus:
                status = "out_for_delivery"
            elif "in transit" in corpus or "shipped" in corpus:
                status = "in_transit"
            elif "label created" in corpus:
                status = "label_created"
            elif "exception" in corpus:
                status = "exception"

            eta_ts = _coerce_event_ts(" ".join([subject, snippet, body]), fallback_ts=0.0)
            if tracking_id or order_number or eta_ts > 0 or status in {"in_transit", "out_for_delivery", "delivered"}:
                merchant = _merchant_from_sender(sender) or "Unknown Merchant"
                confidence = 0.64 if tracking_id else (0.6 if order_number else 0.56)
                stable = hashlib.sha1(
                    f"delivery|{carrier}|{tracking_id}|{order_number}|{status}|{eta_ts}|{email_id}".encode("utf-8", errors="ignore")
                ).hexdigest()[:24]
                deliveries.append(
                    {
                        "id": stable,
                        "carrier": carrier,
                        "tracking_id": tracking_id,
                        "order_number": order_number,
                        "status": status,
                        "eta_at": _iso_from_ts(eta_ts) if eta_ts > 0 else "",
                        "eta_ts": eta_ts if eta_ts > 0 else 0.0,
                        "merchant": merchant,
                        "item_description": item_description,
                        "summary": snippet,
                        "confidence": confidence,
                        "email_id": email_id,
                    }
                )

        if any(token in corpus for token in action_keywords):
            due_ts = _coerce_event_ts(" ".join([subject, snippet, body]), fallback_ts=0.0)
            due_at = _iso_from_ts(due_ts) if due_ts > 0 else ""
            kind = "task"
            if "payment due" in corpus or "invoice due" in corpus:
                kind = "payment_due"
            elif "verify" in corpus or "verification" in corpus:
                kind = "verification"
            elif "respond" in corpus:
                kind = "response_required"
            confidence = 0.64 if due_ts > 0 else 0.57
            stable = hashlib.sha1(
                f"action|{subject}|{kind}|{due_at}|{email_id}".encode("utf-8", errors="ignore")
            ).hexdigest()[:24]
            action_items.append(
                {
                    "id": stable,
                    "title": subject or "Action required",
                    "summary": snippet or _clean_text_blob(body, max_chars=220),
                    "kind": kind,
                    "due_at": due_at,
                    "due_ts": due_ts if due_ts > 0 else 0.0,
                    "status": "open",
                    "confidence": confidence,
                    "email_id": email_id,
                }
            )

    return {
        "spending_habits": spending,
        "favorite_places": [],
        "important_notes": notes,
        "upcoming_events": events,
        "subscriptions": subscriptions,
        "deliveries": deliveries,
        "action_items": action_items,
    }


def _llm_extract_updates(
    llm_client: Any,
    *,
    account_id: str,
    rows: List[Dict[str, Any]],
    profile: Dict[str, Any],
    settings: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if llm_client is None:
        return None
    if not rows:
        return {
            "spending_habits": [],
            "favorite_places": [],
            "important_notes": [],
            "upcoming_events": [],
            "subscriptions": [],
            "deliveries": [],
            "action_items": [],
        }

    messages_payload: List[Dict[str, Any]] = []
    for row in rows:
        messages_payload.append(
            {
                "id": _text(row.get("id")),
                "date_iso": _text(row.get("date_iso")),
                "from": _text(row.get("from")),
                "subject": _text(row.get("subject")),
                "snippet": _clean_text_blob(row.get("snippet"), max_chars=0),
                "body": _clean_text_blob(row.get("body"), max_chars=0),
            }
        )

    profile_snapshot = {
        "favorite_places": list(profile.get("favorite_places") or []),
        "upcoming_events": list(profile.get("upcoming_events") or []),
        "important_notes": list(profile.get("important_notes") or []),
        "subscriptions": list(profile.get("subscriptions") or []),
        "deliveries": list(profile.get("deliveries") or []),
        "action_items": list(profile.get("action_items") or []),
    }

    payload = {
        "account_id": account_id,
        "emails": messages_payload,
        "current_profile": profile_snapshot,
        "today": datetime.utcnow().strftime("%Y-%m-%d"),
    }

    system_prompt = (
        "Extract personal-insight updates from user email messages. Return strict JSON only with this shape:\n"
        "{\n"
        "  \"spending_habits\":[{\"merchant\":\"\",\"amount\":0.0,\"currency\":\"USD\",\"category\":\"\",\"confidence\":0.0,\"email_id\":\"\",\"observed_at\":\"YYYY-MM-DDTHH:MM:SS\"}],\n"
        "  \"favorite_places\":[{\"name\":\"\",\"reason\":\"\",\"confidence\":0.0}],\n"
        "  \"important_notes\":[{\"title\":\"\",\"summary\":\"\",\"kind\":\"\",\"confidence\":0.0,\"email_id\":\"\",\"date_iso\":\"YYYY-MM-DDTHH:MM:SS\"}],\n"
        "  \"upcoming_events\":[{\"title\":\"\",\"kind\":\"movie|trip|appointment|payment_due|event|other\",\"starts_at\":\"YYYY-MM-DDTHH:MM:SS\",\"ends_at\":\"\",\"location\":\"\",\"summary\":\"\",\"confidence\":0.0,\"email_id\":\"\"}],\n"
        "  \"subscriptions\":[{\"merchant\":\"\",\"plan\":\"\",\"amount\":0.0,\"currency\":\"USD\",\"cadence\":\"monthly|yearly|weekly|quarterly|unknown\",\"next_charge_at\":\"YYYY-MM-DDTHH:MM:SS\",\"confidence\":0.0,\"email_id\":\"\"}],\n"
        "  \"deliveries\":[{\"carrier\":\"\",\"tracking_id\":\"\",\"order_number\":\"\",\"item_description\":\"\",\"status\":\"label_created|in_transit|out_for_delivery|delivered|exception|update\",\"eta_at\":\"YYYY-MM-DDTHH:MM:SS\",\"merchant\":\"\",\"summary\":\"\",\"confidence\":0.0,\"email_id\":\"\"}],\n"
        "  \"action_items\":[{\"title\":\"\",\"summary\":\"\",\"kind\":\"payment_due|verification|response_required|task\",\"due_at\":\"YYYY-MM-DDTHH:MM:SS\",\"status\":\"open|done\",\"confidence\":0.0,\"email_id\":\"\"}]\n"
        "}\n"
        "Rules:\n"
        "- Only use evidence present in emails.\n"
        "- Do not invent transactions, event dates, locations, or purchases.\n"
        "- For amounts, output numeric values only (not strings).\n"
        "- Use confidence in [0,1].\n"
        "- Include email_id that matches one of provided ids for every extracted row.\n"
        "- upcoming_events should only include events with explicit/plausible future schedule details.\n"
        "- subscriptions should capture recurring or renewal billing when present.\n"
        "- deliveries should capture shipping/tracking status when present.\n"
        "- Put purchase identifiers in order_number (not tracking_id).\n"
        "- Use tracking_id only for carrier tracking references.\n"
        "- Include item_description when the shipped item can be identified.\n"
        "- action_items should capture user-facing required actions and due dates.\n"
        "- Keep notes concise and practical.\n"
        "- If no valid updates exist for a category, return an empty array for that category."
    )

    async def _call() -> Dict[str, Any]:
        return await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0.1,
        )

    try:
        response = asyncio.run(_call())
    except Exception as exc:
        logger.warning("[personal_core] LLM extraction failed for %s: %s", account_id, exc)
        return None

    text = _text(((response or {}).get("message") or {}).get("content"))
    if not text:
        return {
            "spending_habits": [],
            "favorite_places": [],
            "important_notes": [],
            "upcoming_events": [],
            "subscriptions": [],
            "deliveries": [],
            "action_items": [],
        }

    blob = extract_json(text) or text
    try:
        parsed = json.loads(blob)
    except Exception:
        logger.warning("[personal_core] extraction was not valid JSON for account=%s", account_id)
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _normalize_spending_row(row: Dict[str, Any], *, min_conf: float) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    merchant = _text(row.get("merchant") or row.get("name"))
    amount = _as_float(row.get("amount"), 0.0, minimum=0.0)
    confidence = _as_float(row.get("confidence"), 0.0, minimum=0.0, maximum=1.0)
    if not merchant or amount <= 0:
        return None
    if confidence < min_conf:
        return None

    observed_at = _text(row.get("observed_at") or row.get("date_iso"))
    observed_ts = _parse_iso_to_ts(observed_at)
    if observed_ts <= 0:
        observed_ts = time.time()
        observed_at = _iso_from_ts(observed_ts)

    email_id = _text(row.get("email_id"))
    stable = hashlib.sha1(f"{merchant}|{amount:.2f}|{email_id}|{observed_at}".encode("utf-8", errors="ignore")).hexdigest()[:24]

    return {
        "id": stable,
        "merchant": merchant,
        "amount": float(amount),
        "currency": _text(row.get("currency")) or "USD",
        "category": _text(row.get("category")) or "purchase",
        "confidence": confidence,
        "email_id": email_id,
        "observed_at": observed_at,
        "observed_ts": observed_ts,
    }


def _normalize_note_row(row: Dict[str, Any], *, min_conf: float) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    title = _text(row.get("title"))
    summary = _text(row.get("summary"))
    confidence = _as_float(row.get("confidence"), 0.0, minimum=0.0, maximum=1.0)
    if not title and not summary:
        return None
    if confidence < min_conf:
        return None

    date_iso = _text(row.get("date_iso"))
    date_ts = _parse_iso_to_ts(date_iso)
    if date_ts <= 0:
        date_ts = time.time()
        date_iso = _iso_from_ts(date_ts)

    email_id = _text(row.get("email_id"))
    kind = _slug(row.get("kind"), default="important")
    stable = hashlib.sha1(f"{title}|{summary}|{email_id}|{date_iso}".encode("utf-8", errors="ignore")).hexdigest()[:24]
    return {
        "id": stable,
        "title": title or "Important email",
        "summary": summary,
        "kind": kind,
        "confidence": confidence,
        "email_id": email_id,
        "date_iso": date_iso,
        "date_ts": date_ts,
    }


def _normalize_event_row(row: Dict[str, Any], *, min_conf: float) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    title = _text(row.get("title"))
    starts_at = _text(row.get("starts_at"))
    starts_ts = _parse_iso_to_ts(starts_at)
    if starts_ts <= 0:
        starts_ts = _coerce_event_ts(" ".join([title, _text(row.get("summary")), _text(row.get("location"))]), fallback_ts=0.0)
        starts_at = _iso_from_ts(starts_ts)
    confidence = _as_float(row.get("confidence"), 0.0, minimum=0.0, maximum=1.0)
    if not title or starts_ts <= 0:
        return None
    if confidence < min_conf:
        return None

    ends_at = _text(row.get("ends_at"))
    ends_ts = _parse_iso_to_ts(ends_at)
    if ends_ts <= 0:
        ends_at = ""
        ends_ts = 0.0

    email_id = _text(row.get("email_id"))
    kind = _slug(row.get("kind"), default="event")
    stable = hashlib.sha1(f"{title}|{starts_at}|{email_id}|{kind}".encode("utf-8", errors="ignore")).hexdigest()[:24]
    return {
        "id": stable,
        "title": title,
        "kind": kind,
        "starts_at": starts_at,
        "starts_ts": starts_ts,
        "ends_at": ends_at,
        "ends_ts": ends_ts,
        "location": _text(row.get("location")),
        "summary": _text(row.get("summary")),
        "confidence": confidence,
        "email_id": email_id,
    }


def _normalize_favorite_place_row(row: Dict[str, Any], *, min_conf: float) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    name = _text(row.get("name") or row.get("merchant"))
    if not name:
        return None
    confidence = _as_float(row.get("confidence"), 0.0, minimum=0.0, maximum=1.0)
    if confidence < min_conf:
        return None
    reason = _text(row.get("reason"))
    stable = hashlib.sha1(f"{name}|{reason}".encode("utf-8", errors="ignore")).hexdigest()[:24]
    return {
        "id": stable,
        "name": name,
        "reason": reason,
        "confidence": confidence,
    }


def _normalize_subscription_row(row: Dict[str, Any], *, min_conf: float) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    merchant = _text(row.get("merchant") or row.get("name"))
    if not merchant:
        return None
    confidence = _as_float(row.get("confidence"), 0.0, minimum=0.0, maximum=1.0)
    if confidence < min_conf:
        return None

    amount = _as_float(row.get("amount"), 0.0, minimum=0.0)
    cadence = _slug(row.get("cadence"), default="unknown")
    if cadence not in {"monthly", "yearly", "weekly", "quarterly", "unknown"}:
        cadence = "unknown"

    next_charge_at = _text(row.get("next_charge_at") or row.get("next_charge"))
    next_charge_ts = _parse_iso_to_ts(next_charge_at)
    if next_charge_ts <= 0:
        next_charge_ts = _coerce_event_ts(" ".join([_text(row.get("plan")), _text(row.get("summary"))]), fallback_ts=0.0)
        next_charge_at = _iso_from_ts(next_charge_ts) if next_charge_ts > 0 else ""

    email_id = _text(row.get("email_id"))
    stable = _text(row.get("id"))
    if not stable:
        stable = hashlib.sha1(
            f"sub|{merchant}|{_text(row.get('plan'))}|{amount:.2f}|{next_charge_at}|{email_id}".encode("utf-8", errors="ignore")
        ).hexdigest()[:24]

    return {
        "id": stable,
        "merchant": merchant,
        "plan": _text(row.get("plan")),
        "amount": float(amount),
        "currency": _text(row.get("currency")) or "USD",
        "cadence": cadence,
        "next_charge_at": next_charge_at,
        "next_charge_ts": next_charge_ts if next_charge_ts > 0 else 0.0,
        "confidence": confidence,
        "email_id": email_id,
    }


def _normalize_delivery_row(row: Dict[str, Any], *, min_conf: float) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    confidence = _as_float(row.get("confidence"), 0.0, minimum=0.0, maximum=1.0)
    if confidence < min_conf:
        return None

    status = _slug(row.get("status"), default="update")
    if status not in {"label_created", "in_transit", "out_for_delivery", "delivered", "exception", "update"}:
        status = "update"
    tracking_id = _normalize_tracking_id(row.get("tracking_id") or row.get("tracking_number") or row.get("tracking"))
    order_number = _normalize_order_number(row.get("order_number") or row.get("order_id") or row.get("order"))
    carrier = _text(row.get("carrier"))
    item_description = _clean_text_blob(
        row.get("item_description") or row.get("item_name") or row.get("product"),
        max_chars=160,
    )
    if not item_description:
        item_description = _extract_delivery_item_from_text(_text(row.get("summary")), _text(row.get("title")))

    if tracking_id and not order_number and _looks_like_order_reference(tracking_id):
        order_number = _normalize_order_number(tracking_id)
        tracking_id = ""
    if order_number and not tracking_id and _looks_like_tracking_reference(order_number):
        tracking_id = _normalize_tracking_id(order_number)
        order_number = ""

    eta_at = _text(row.get("eta_at") or row.get("estimated_delivery"))
    eta_ts = _parse_iso_to_ts(eta_at)
    if eta_ts <= 0:
        eta_ts = _coerce_event_ts(" ".join([_text(row.get("summary")), _text(row.get("title"))]), fallback_ts=0.0)
        eta_at = _iso_from_ts(eta_ts) if eta_ts > 0 else ""

    if not tracking_id and not order_number and not carrier and not eta_at and status == "update":
        return None

    email_id = _text(row.get("email_id"))
    stable = _text(row.get("id"))
    if not stable:
        stable = hashlib.sha1(
            f"delivery|{carrier}|{tracking_id}|{order_number}|{status}|{eta_at}|{email_id}".encode("utf-8", errors="ignore")
        ).hexdigest()[:24]

    return {
        "id": stable,
        "carrier": carrier,
        "tracking_id": tracking_id,
        "order_number": order_number,
        "status": status,
        "eta_at": eta_at,
        "eta_ts": eta_ts if eta_ts > 0 else 0.0,
        "merchant": _text(row.get("merchant")),
        "item_description": item_description,
        "summary": _text(row.get("summary")),
        "confidence": confidence,
        "email_id": email_id,
    }


def _normalize_action_item_row(row: Dict[str, Any], *, min_conf: float) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    title = _text(row.get("title"))
    summary = _text(row.get("summary"))
    if not title and not summary:
        return None
    confidence = _as_float(row.get("confidence"), 0.0, minimum=0.0, maximum=1.0)
    if confidence < min_conf:
        return None

    due_at = _text(row.get("due_at") or row.get("due_date"))
    due_ts = _parse_iso_to_ts(due_at)
    if due_ts <= 0:
        due_ts = _coerce_event_ts(" ".join([title, summary]), fallback_ts=0.0)
        due_at = _iso_from_ts(due_ts) if due_ts > 0 else ""

    kind = _slug(row.get("kind"), default="task")
    if kind not in {"payment_due", "verification", "response_required", "task"}:
        kind = "task"
    status = _slug(row.get("status"), default="open")
    if status not in {"open", "done"}:
        status = "open"

    email_id = _text(row.get("email_id"))
    stable = _text(row.get("id"))
    if not stable:
        stable = hashlib.sha1(
            f"action|{title}|{kind}|{due_at}|{email_id}".encode("utf-8", errors="ignore")
        ).hexdigest()[:24]

    return {
        "id": stable,
        "title": title or "Action required",
        "summary": summary,
        "kind": kind,
        "due_at": due_at,
        "due_ts": due_ts if due_ts > 0 else 0.0,
        "status": status,
        "confidence": confidence,
        "email_id": email_id,
    }


def _normalize_extracted_payload(payload: Dict[str, Any], *, min_confidence: float) -> Dict[str, List[Dict[str, Any]]]:
    out = {
        "spending_habits": [],
        "favorite_places": [],
        "important_notes": [],
        "upcoming_events": [],
        "subscriptions": [],
        "deliveries": [],
        "action_items": [],
    }
    if not isinstance(payload, dict):
        return out

    for row in payload.get("spending_habits") or []:
        normalized = _normalize_spending_row(row, min_conf=min_confidence)
        if normalized:
            out["spending_habits"].append(normalized)

    for row in payload.get("favorite_places") or []:
        normalized = _normalize_favorite_place_row(row, min_conf=min_confidence)
        if normalized:
            out["favorite_places"].append(normalized)

    for row in payload.get("important_notes") or []:
        normalized = _normalize_note_row(row, min_conf=min_confidence)
        if normalized:
            out["important_notes"].append(normalized)

    for row in payload.get("upcoming_events") or []:
        normalized = _normalize_event_row(row, min_conf=min_confidence)
        if normalized:
            out["upcoming_events"].append(normalized)

    for row in payload.get("subscriptions") or []:
        normalized = _normalize_subscription_row(row, min_conf=min_confidence)
        if normalized:
            out["subscriptions"].append(normalized)

    for row in payload.get("deliveries") or []:
        normalized = _normalize_delivery_row(row, min_conf=min_confidence)
        if normalized:
            out["deliveries"].append(normalized)

    for row in payload.get("action_items") or []:
        normalized = _normalize_action_item_row(row, min_conf=min_confidence)
        if normalized:
            out["action_items"].append(normalized)

    return out


def _merge_rows_by_id(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]], *, max_items: int, sort_key: str, reverse: bool) -> List[Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    for row in existing:
        if not isinstance(row, dict):
            continue
        row_id = _text(row.get("id"))
        if not row_id:
            continue
        rows[row_id] = dict(row)
    for row in incoming:
        if not isinstance(row, dict):
            continue
        row_id = _text(row.get("id"))
        if not row_id:
            continue
        rows[row_id] = dict(row)

    ordered = list(rows.values())
    ordered.sort(key=lambda item: _as_float(item.get(sort_key), 0.0), reverse=bool(reverse))
    return ordered[: max(1, int(max_items))]


def _rebuild_favorite_places(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    counts: Dict[str, Dict[str, Any]] = {}
    for row in profile.get("spending_habits") if isinstance(profile.get("spending_habits"), list) else []:
        if not isinstance(row, dict):
            continue
        name = _text(row.get("merchant"))
        if not name:
            continue
        bucket = counts.setdefault(name, {"count": 0, "spend": 0.0, "last_ts": 0.0})
        bucket["count"] = int(bucket.get("count") or 0) + 1
        bucket["spend"] = _as_float(bucket.get("spend"), 0.0, minimum=0.0) + _as_float(row.get("amount"), 0.0, minimum=0.0)
        bucket["last_ts"] = max(_as_float(bucket.get("last_ts"), 0.0), _as_float(row.get("observed_ts"), 0.0))

    for row in profile.get("favorite_places") if isinstance(profile.get("favorite_places"), list) else []:
        if not isinstance(row, dict):
            continue
        name = _text(row.get("name"))
        if not name:
            continue
        bucket = counts.setdefault(name, {"count": 0, "spend": 0.0, "last_ts": 0.0})
        bucket["count"] = int(bucket.get("count") or 0) + 1

    out: List[Dict[str, Any]] = []
    for name, meta in counts.items():
        stable = hashlib.sha1(f"fav|{name}".encode("utf-8", errors="ignore")).hexdigest()[:24]
        out.append(
            {
                "id": stable,
                "name": name,
                "count": int(meta.get("count") or 0),
                "spend": round(_as_float(meta.get("spend"), 0.0, minimum=0.0), 2),
                "last_seen": _iso_from_ts(meta.get("last_ts")),
            }
        )
    out.sort(key=lambda row: (-int(row.get("count") or 0), -_as_float(row.get("spend"), 0.0), _text(row.get("name"))))
    return out[:50]


def _compute_spending_totals(rows: List[Dict[str, Any]]) -> Tuple[float, float]:
    total = 0.0
    total_30d = 0.0
    cutoff = time.time() - (30 * 86400)
    for row in rows:
        if not isinstance(row, dict):
            continue
        amount = _as_float(row.get("amount"), 0.0, minimum=0.0)
        total += amount
        observed_ts = _as_float(row.get("observed_ts"), 0.0, minimum=0.0)
        if observed_ts >= cutoff:
            total_30d += amount
    return round(total, 2), round(total_30d, 2)


def _repair_delivery_row_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    fixed = dict(row) if isinstance(row, dict) else {}
    tracking_id = _normalize_tracking_id(fixed.get("tracking_id"))
    order_number = _normalize_order_number(fixed.get("order_number") or fixed.get("order_id") or fixed.get("order"))
    if tracking_id and not order_number and _looks_like_order_reference(tracking_id):
        order_number = _normalize_order_number(tracking_id)
        tracking_id = ""
    if order_number and not tracking_id and _looks_like_tracking_reference(order_number):
        tracking_id = _normalize_tracking_id(order_number)
        order_number = ""

    item_description = _clean_text_blob(fixed.get("item_description"), max_chars=160)
    if not item_description:
        item_description = _extract_delivery_item_from_text(fixed.get("summary"), fixed.get("title"))

    fixed["tracking_id"] = tracking_id
    fixed["order_number"] = order_number
    fixed["item_description"] = item_description
    return fixed


def _cleanup_events(rows: List[Dict[str, Any]], *, max_items: int) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    cutoff = time.time() - (30 * 86400)
    for row in rows:
        if not isinstance(row, dict):
            continue
        starts_ts = _as_float(row.get("starts_ts"), 0.0, minimum=0.0)
        if starts_ts > 0 and starts_ts < cutoff:
            continue
        kept.append(dict(row))
    kept.sort(key=lambda row: _as_float(row.get("starts_ts"), 0.0))
    return kept[: max(1, int(max_items))]


def _cleanup_deliveries(rows: List[Dict[str, Any]], *, max_items: int) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    stale_cutoff = time.time() - (21 * 86400)
    for row in rows:
        if not isinstance(row, dict):
            continue
        fixed = _repair_delivery_row_fields(row)
        status = _slug(fixed.get("status"), default="update")
        eta_ts = _as_float(fixed.get("eta_ts"), 0.0, minimum=0.0)
        if status == "delivered" and eta_ts > 0 and eta_ts < stale_cutoff:
            continue
        kept.append(fixed)
    kept.sort(
        key=lambda row: (
            _as_float(row.get("eta_ts"), 0.0, minimum=0.0),
            _text(row.get("order_number")),
            _text(row.get("carrier")),
            _text(row.get("tracking_id")),
        ),
        reverse=True,
    )
    return kept[: max(1, int(max_items))]


def _cleanup_action_items(rows: List[Dict[str, Any]], *, max_items: int) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    stale_cutoff = time.time() - (30 * 86400)
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = _slug(row.get("status"), default="open")
        due_ts = _as_float(row.get("due_ts"), 0.0, minimum=0.0)
        if status == "done" and due_ts > 0 and due_ts < stale_cutoff:
            continue
        kept.append(dict(row))
    kept.sort(
        key=lambda row: (
            _as_float(row.get("due_ts"), 0.0, minimum=0.0),
            _text(row.get("title")),
        )
    )
    return kept[: max(1, int(max_items))]


def _merge_profile_updates(
    profile: Dict[str, Any],
    updates: Dict[str, List[Dict[str, Any]]],
    *,
    settings: Dict[str, Any],
    source_kind: str,
    emails_stored: int,
    emails_new: int,
    scan_ok: bool,
    error_text: str = "",
) -> Dict[str, Any]:
    merged = dict(profile)

    spending_existing = list(merged.get("spending_habits") or [])
    spending_incoming = list(updates.get("spending_habits") or [])
    merged["spending_habits"] = _merge_rows_by_id(
        spending_existing,
        spending_incoming,
        max_items=_as_int(settings.get("max_spending_entries"), 600, minimum=20, maximum=5000),
        sort_key="observed_ts",
        reverse=True,
    )

    notes_existing = list(merged.get("important_notes") or [])
    notes_incoming = list(updates.get("important_notes") or [])
    merged["important_notes"] = _merge_rows_by_id(
        notes_existing,
        notes_incoming,
        max_items=_as_int(settings.get("max_note_entries"), 300, minimum=20, maximum=3000),
        sort_key="date_ts",
        reverse=True,
    )

    events_existing = list(merged.get("upcoming_events") or [])
    events_incoming = list(updates.get("upcoming_events") or [])
    merged_events = _merge_rows_by_id(
        events_existing,
        events_incoming,
        max_items=_as_int(settings.get("max_event_entries"), 260, minimum=20, maximum=2000),
        sort_key="starts_ts",
        reverse=False,
    )
    merged["upcoming_events"] = _cleanup_events(
        merged_events,
        max_items=_as_int(settings.get("max_event_entries"), 260, minimum=20, maximum=2000),
    )

    favorite_existing = list(merged.get("favorite_places") or [])
    favorite_incoming = list(updates.get("favorite_places") or [])
    combined_favorites = _merge_rows_by_id(
        favorite_existing,
        favorite_incoming,
        max_items=200,
        sort_key="confidence",
        reverse=True,
    )
    merged["favorite_places"] = combined_favorites
    merged["favorite_places"] = _rebuild_favorite_places(merged)

    subscriptions_existing = list(merged.get("subscriptions") or [])
    subscriptions_incoming = list(updates.get("subscriptions") or [])
    merged["subscriptions"] = _merge_rows_by_id(
        subscriptions_existing,
        subscriptions_incoming,
        max_items=300,
        sort_key="next_charge_ts",
        reverse=True,
    )

    deliveries_existing = list(merged.get("deliveries") or [])
    deliveries_incoming = list(updates.get("deliveries") or [])
    merged_deliveries = _merge_rows_by_id(
        deliveries_existing,
        deliveries_incoming,
        max_items=500,
        sort_key="eta_ts",
        reverse=True,
    )
    merged["deliveries"] = _cleanup_deliveries(merged_deliveries, max_items=500)

    action_existing = list(merged.get("action_items") or [])
    action_incoming = list(updates.get("action_items") or [])
    merged_actions = _merge_rows_by_id(
        action_existing,
        action_incoming,
        max_items=300,
        sort_key="due_ts",
        reverse=False,
    )
    merged["action_items"] = _cleanup_action_items(merged_actions, max_items=300)

    spend_total, spend_30d = _compute_spending_totals(list(merged.get("spending_habits") or []))

    now_ts = time.time()
    stats = merged.get("stats") if isinstance(merged.get("stats"), dict) else {}
    stats["emails_stored"] = int(emails_stored)
    stats["emails_new_last_run"] = int(emails_new)
    stats["emails_scanned_total"] = _as_int(stats.get("emails_scanned_total"), 0, minimum=0) + int(emails_new)
    stats["spending_total"] = float(spend_total)
    stats["spending_30d"] = float(spend_30d)
    stats["upcoming_events"] = len(list(merged.get("upcoming_events") or []))
    stats["subscriptions"] = len(list(merged.get("subscriptions") or []))
    stats["deliveries_open"] = len(
        [
            row
            for row in list(merged.get("deliveries") or [])
            if isinstance(row, dict) and _slug(row.get("status"), default="update") != "delivered"
        ]
    )
    stats["action_items_open"] = len(
        [
            row
            for row in list(merged.get("action_items") or [])
            if isinstance(row, dict) and _slug(row.get("status"), default="open") == "open"
        ]
    )
    stats["last_scan_ts"] = now_ts
    stats["last_scan_status"] = "ok" if scan_ok else "error"
    stats["last_error"] = _text(error_text)

    llm_update_count = sum(
        [
            len(spending_incoming),
            len(notes_incoming),
            len(events_incoming),
            len(favorite_incoming),
            len(subscriptions_incoming),
            len(deliveries_incoming),
            len(action_incoming),
        ]
    )
    if source_kind == "llm":
        stats["llm_updates_total"] = _as_int(stats.get("llm_updates_total"), 0, minimum=0) + int(llm_update_count)
    elif source_kind == "heuristic":
        stats["heuristic_updates_total"] = _as_int(stats.get("heuristic_updates_total"), 0, minimum=0) + int(llm_update_count)

    merged["stats"] = stats
    merged["last_updated"] = now_ts
    return merged


def _extract_updates_for_rows(
    llm_client: Any,
    *,
    account_id: str,
    inserted_rows: List[Dict[str, Any]],
    profile: Dict[str, Any],
    settings: Dict[str, Any],
) -> Tuple[Dict[str, List[Dict[str, Any]]], str]:
    min_confidence = _as_float(settings.get("min_confidence"), 0.62, minimum=0.0, maximum=1.0)

    llm_payload = _llm_extract_updates(
        llm_client,
        account_id=account_id,
        rows=inserted_rows,
        profile=profile,
        settings=settings,
    )
    if llm_payload is not None:
        normalized = _normalize_extracted_payload(llm_payload, min_confidence=min_confidence)
        return normalized, "llm"

    heuristic_payload = _heuristic_extract_updates(inserted_rows)
    normalized = _normalize_extracted_payload(heuristic_payload, min_confidence=max(0.5, min_confidence - 0.15))
    return normalized, "heuristic"


def _notification_claim_key(account_id: str, kind: str, item_id: str) -> str:
    return (
        f"{_PERSONAL_NOTIFY_SENT_PREFIX}:"
        f"{_slug(account_id, default='account')}:"
        f"{_slug(kind, default='item')}:"
        f"{_slug(item_id, default='unknown')}"
    )


def _claim_notification_once(*, account_id: str, kind: str, item_id: str, ttl_seconds: int = 90 * 86400) -> Tuple[bool, str]:
    key = _notification_claim_key(account_id, kind, item_id)
    try:
        claimed = redis_client.set(key, "1", nx=True, ex=max(60, int(ttl_seconds)))
        return bool(claimed), key
    except Exception:
        # Fail open for notifier reliability if Redis writes fail unexpectedly.
        return True, key


def _release_notification_claim(claim_key: str) -> None:
    key = _text(claim_key)
    if not key:
        return
    try:
        redis_client.delete(key)
    except Exception:
        pass


def _delivery_status_text(status: Any) -> str:
    token = _slug(status, default="update")
    if token == "label_created":
        return "label created"
    if token == "in_transit":
        return "in transit"
    if token == "out_for_delivery":
        return "out for delivery"
    if token == "delivered":
        return "delivered"
    if token == "exception":
        return "delayed/exception"
    return "updated"


def _notification_candidates_from_updates(
    *,
    updates: Dict[str, List[Dict[str, Any]]],
    settings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    if _as_bool(settings.get("notify_on_deliveries"), True):
        deliveries = list(updates.get("deliveries") or [])
        deliveries.sort(key=lambda row: _as_float((row or {}).get("eta_ts"), 0.0, minimum=0.0), reverse=True)
        for row in deliveries:
            if not isinstance(row, dict):
                continue
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            carrier = _text(row.get("carrier")) or "A shipment"
            tracking = _text(row.get("tracking_id"))
            order_number = _text(row.get("order_number"))
            item_description = _text(row.get("item_description"))
            status_text = _delivery_status_text(row.get("status"))
            eta = _text(row.get("eta_at"))
            merchant = _text(row.get("merchant"))
            ref_text = f" ({tracking})" if tracking else (f" (order {order_number})" if order_number else "")
            eta_text = f" ETA {eta}." if eta else "."
            merchant_text = f" from {merchant}" if merchant else ""
            item_text = f" for {item_description}" if item_description else ""
            message = f"It looks like your delivery{merchant_text}{item_text} via {carrier}{ref_text} is now {status_text}.{eta_text}"
            out.append(
                {
                    "kind": "deliveries",
                    "id": row_id,
                    "title": "Personal update: delivery",
                    "message": message,
                    "rewrite_context": {
                        "carrier": carrier,
                        "tracking_id": tracking,
                        "order_number": order_number,
                        "item_description": item_description,
                        "status": _slug(row.get("status"), default="update"),
                        "eta_at": eta,
                        "merchant": merchant,
                    },
                }
            )

    if _as_bool(settings.get("notify_on_spending"), True):
        spending_rows = list(updates.get("spending_habits") or [])
        spending_rows.sort(key=lambda row: _as_float((row or {}).get("observed_ts"), 0.0, minimum=0.0), reverse=True)
        for row in spending_rows:
            if not isinstance(row, dict):
                continue
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            merchant = _text(row.get("merchant")) or "an unknown merchant"
            amount = _as_float(row.get("amount"), 0.0, minimum=0.0)
            observed = _text(row.get("observed_at"))
            when_text = f" on {observed}" if observed else ""
            message = f"I noticed a new purchase: {merchant} for ${amount:.2f}{when_text}."
            out.append(
                {
                    "kind": "spending",
                    "id": row_id,
                    "title": "Personal update: spending",
                    "message": message,
                    "rewrite_context": {
                        "merchant": merchant,
                        "amount": round(amount, 2),
                        "currency": _text(row.get("currency")) or "USD",
                        "observed_at": observed,
                    },
                }
            )

    if _as_bool(settings.get("notify_on_plans"), True):
        plans = list(updates.get("upcoming_events") or [])
        plans.sort(key=lambda row: _as_float((row or {}).get("starts_ts"), 0.0, minimum=0.0))
        for row in plans:
            if not isinstance(row, dict):
                continue
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            title = _text(row.get("title")) or "an upcoming event"
            starts = _text(row.get("starts_at")) or "an upcoming time"
            location = _text(row.get("location"))
            location_text = f" at {location}" if location else ""
            message = f"I see you have a plan coming up: {title} at {starts}{location_text}."
            out.append(
                {
                    "kind": "plans",
                    "id": row_id,
                    "title": "Personal update: upcoming plan",
                    "message": message,
                    "rewrite_context": {
                        "title": title,
                        "starts_at": starts,
                        "location": location,
                        "event_kind": _text(row.get("kind")) or "event",
                    },
                }
            )

    return out


def _run_async_blocking(coro: Any) -> Any:
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            try:
                loop.close()
            except Exception:
                pass


def _normalize_notification_copy_text(value: Any, *, max_chars: int) -> str:
    text = _clean_text_blob(value, max_chars=max_chars).replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_notification_rewrite_text(*, text: str, fallback_title: str, fallback_message: str) -> Dict[str, str]:
    raw = _text(text)
    if not raw:
        return {"title": fallback_title, "message": fallback_message}

    blob = extract_json(raw) or ""
    if blob:
        try:
            parsed = json.loads(blob)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            title_text = _normalize_notification_copy_text(parsed.get("title"), max_chars=72) or fallback_title
            message_text = _normalize_notification_copy_text(parsed.get("message"), max_chars=320) or fallback_message
            return {"title": title_text, "message": message_text}

    # Accept plain-text rewrite output too (for providers that ignore strict-JSON instruction).
    lines = [_normalize_notification_copy_text(line, max_chars=320) for line in raw.splitlines() if _text(line)]
    explicit_title = ""
    explicit_message = ""
    for line in lines:
        lower = line.lower()
        if lower.startswith("title:"):
            explicit_title = _normalize_notification_copy_text(line.split(":", 1)[1], max_chars=72)
        elif lower.startswith("message:"):
            explicit_message = _normalize_notification_copy_text(line.split(":", 1)[1], max_chars=320)

    if not explicit_title and len(lines) >= 2 and len(lines[0]) <= 60:
        explicit_title = _normalize_notification_copy_text(lines[0], max_chars=72)
    if not explicit_message:
        if explicit_title and len(lines) >= 2:
            explicit_message = _normalize_notification_copy_text(" ".join(lines[1:]), max_chars=320)
        else:
            explicit_message = _normalize_notification_copy_text(raw, max_chars=320)

    return {
        "title": explicit_title or fallback_title,
        "message": explicit_message or fallback_message,
    }


def _notification_rewrite_prompt_for_kind(*, kind: str, is_test: bool) -> str:
    kind_token = _slug(kind, default="update")
    mode_text = "test preview" if bool(is_test) else "live notification"

    base_rules = [
        "Return strict JSON only with keys title and message.",
        "Keep facts exactly as provided.",
        "No hallucinations or extra claims.",
        "Keep title <= 60 chars.",
        "Keep message <= 220 chars and concise.",
        "Friendly tone, no emojis, no markdown.",
    ]

    if kind_token == "deliveries":
        task = (
            "You write personal assistant delivery alerts.\n"
            f"Context: this is a {mode_text} for a newly detected delivery update."
        )
        extra = [
            "Make it clear this is a new delivery/update.",
            "Use item_description, merchant, carrier, and tracking/order refs when available.",
            "If only partial data exists, still sound useful and natural.",
        ]
    elif kind_token == "spending":
        task = (
            "You write personal assistant spending alerts.\n"
            f"Context: this is a {mode_text} for a newly detected purchase/spend signal."
        )
        extra = [
            "Make it clear a new purchase/spend event was detected.",
            "Use merchant and amount when available.",
            "Keep wording short and practical.",
        ]
    elif kind_token == "plans":
        task = (
            "You write personal assistant upcoming-plan alerts.\n"
            f"Context: this is a {mode_text} for a newly detected upcoming plan/event."
        )
        extra = [
            "Make it clear this is an upcoming plan/event reminder.",
            "Use event title, time, and location when available.",
            "Keep phrasing natural and timely.",
        ]
    else:
        task = (
            "You rewrite personal assistant notification copy.\n"
            f"Context: this is a {mode_text}."
        )
        extra = [
            "Use available context details when present.",
            "Avoid repetitive boilerplate wording.",
        ]

    lines = [task, "Rules:"] + [f"- {line}" for line in base_rules + extra]
    return "\n".join(lines)


async def _rewrite_notification_copy_async(
    *,
    llm_client: Any,
    kind: str,
    title: str,
    message: str,
    is_test: bool,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    fallback = {
        "title": _normalize_notification_copy_text(title, max_chars=72) or "Personal update",
        "message": _normalize_notification_copy_text(message, max_chars=320),
    }
    if llm_client is None:
        return fallback

    prompt = _notification_rewrite_prompt_for_kind(kind=kind, is_test=is_test)
    payload = {
        "kind": _slug(kind, default="update"),
        "mode": "test" if bool(is_test) else "live",
        "variation_hint": str(int(time.time() * 1000) % 997),
        "event_context": context if isinstance(context, dict) else {},
        "current_title": fallback["title"],
        "current_message": fallback["message"],
    }

    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0.55,
        )
    except Exception as exc:
        logger.debug("[personal_core] notification rewrite failed: %s", exc)
        return fallback

    text = _text(((response or {}).get("message") or {}).get("content"))
    return _parse_notification_rewrite_text(
        text=text,
        fallback_title=fallback["title"],
        fallback_message=fallback["message"],
    )


def _notification_copy_for_send(
    *,
    row: Dict[str, Any],
    llm_client: Any,
    is_test: bool,
) -> Dict[str, str]:
    title = _text(row.get("title")) or ("Personal notification test" if is_test else "Personal update")
    message = _text(row.get("message"))
    fallback = {
        "title": _normalize_notification_copy_text(title, max_chars=72) or "Personal update",
        "message": _normalize_notification_copy_text(message, max_chars=320),
    }
    rewrite_context = row.get("rewrite_context") if isinstance(row.get("rewrite_context"), dict) else {}
    if not fallback["message"]:
        return fallback
    if llm_client is None:
        logger.info(
            "[personal_core] notification rewrite fallback kind=%s test=%s reason=llm_unavailable",
            _slug(row.get("kind"), default="update"),
            "1" if bool(is_test) else "0",
        )
        return fallback
    rewritten = _run_async_blocking(
        _rewrite_notification_copy_async(
            llm_client=llm_client,
            kind=_text(row.get("kind")),
            title=fallback["title"],
            message=fallback["message"],
            is_test=is_test,
            context=rewrite_context,
        )
    )
    if not isinstance(rewritten, dict):
        return fallback
    out_title = _normalize_notification_copy_text(rewritten.get("title"), max_chars=72) or fallback["title"]
    out_message = _normalize_notification_copy_text(rewritten.get("message"), max_chars=320) or fallback["message"]
    if out_title == fallback["title"] and out_message == fallback["message"]:
        logger.info(
            "[personal_core] notification rewrite fallback kind=%s test=%s reason=no_change",
            _slug(row.get("kind"), default="update"),
            "1" if bool(is_test) else "0",
        )
    return {"title": out_title, "message": out_message}


async def _dispatch_personal_notification_async(
    *,
    platform: str,
    title: str,
    message: str,
    target_routes: List[Dict[str, Any]],
    account_id: str,
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    origin = {
        "platform": "personal_core",
        "source": "personal_core",
        "account_id": account_id,
    }
    meta = {
        "priority": "normal",
        "tags": ["personal", "email"],
    }

    platform_name = _slug(platform, default="webui")
    routes = [dict(row) for row in (target_routes or []) if isinstance(row, dict)]
    if not routes and not _platform_requires_target(platform_name):
        routes = [{}]

    if platform_name == "homeassistant":
        api_notification = _as_bool(settings.get("notification_ha_api_notification"), False)
        global_services = _normalize_notify_services(settings.get("notification_ha_device_services"))
        sent_count = 0
        errors: List[str] = []

        async def _dispatch_ha_route(ha_targets: Dict[str, Any]) -> None:
            nonlocal sent_count
            try:
                result = await dispatch_notification(
                    platform="homeassistant",
                    title=title,
                    content=message,
                    targets=ha_targets,
                    origin=origin,
                    meta=meta,
                )
            except Exception as exc:
                errors.append(str(exc))
                return
            result_text = _text(result)
            if result_text.lower().startswith("queued notification"):
                sent_count += 1
            else:
                errors.append(result_text or "homeassistant notifier returned empty result")

        if not routes:
            routes = [{}]

        for route in routes:
            base_targets = dict(route)
            route_api_enabled = _as_bool(base_targets.get("api_notification"), api_notification)
            route_device_service = _normalize_notify_service(base_targets.get("device_service"))
            route_services = list(global_services)
            if route_device_service and route_device_service not in route_services:
                route_services.append(route_device_service)
            if not (route_api_enabled or route_services):
                errors.append("homeassistant route has api_notification disabled and no device services configured")
                continue

            if route_api_enabled:
                api_targets = dict(base_targets)
                api_targets.pop("device_service", None)
                api_targets["persistent"] = False
                api_targets["api_notification"] = True
                await _dispatch_ha_route(api_targets)

            for service in route_services:
                svc_targets = dict(base_targets)
                svc_targets["persistent"] = False
                svc_targets["api_notification"] = False
                svc_targets["device_service"] = service
                await _dispatch_ha_route(svc_targets)

        if sent_count > 0:
            return {"ok": True, "sent_count": sent_count, "errors": errors}
        return {"ok": False, "sent_count": 0, "errors": errors}

    sent_count = 0
    errors: List[str] = []
    for route in routes:
        try:
            result = await dispatch_notification(
                platform=platform_name,
                title=title,
                content=message,
                targets=route,
                origin=origin,
                meta=meta,
            )
        except Exception as exc:
            errors.append(str(exc))
            continue

        result_text = _text(result)
        if result_text.lower().startswith("queued notification"):
            sent_count += 1
        else:
            errors.append(result_text or "notification not queued")

    if sent_count > 0:
        return {"ok": True, "sent_count": sent_count, "errors": errors}
    return {"ok": False, "sent_count": 0, "errors": errors}


def _send_new_update_notifications(
    *,
    account_id: str,
    updates: Dict[str, List[Dict[str, Any]]],
    settings: Dict[str, Any],
    llm_client: Any = None,
) -> Dict[str, Any]:
    if not _as_bool(settings.get("notifications_enabled"), False):
        return {"sent_count": 0, "attempted_count": 0, "error_count": 0, "errors": [], "skipped": "disabled"}

    catalog = _load_destination_catalog()
    grouped_routes, config_errors = _group_notification_routes(settings=settings, catalog=catalog)

    if not grouped_routes:
        return {
            "sent_count": 0,
            "attempted_count": 0,
            "error_count": max(1, len(config_errors)),
            "errors": config_errors or ["No valid notification destinations configured."],
            "skipped": "missing_target",
        }

    max_per_cycle = _as_int(settings.get("notification_max_per_cycle"), 3, minimum=1, maximum=25)
    candidates = _notification_candidates_from_updates(updates=updates, settings=settings)
    if not candidates:
        return {"sent_count": 0, "attempted_count": 0, "error_count": 0, "errors": [], "skipped": "no_candidates"}

    sent_count = 0
    attempted_count = 0
    errors: List[str] = []

    for row in candidates:
        if attempted_count >= max_per_cycle:
            break
        kind = _slug(row.get("kind"), default="update")
        row_id = _text(row.get("id"))
        if not row_id:
            continue
        claimed, claim_key = _claim_notification_once(account_id=account_id, kind=kind, item_id=row_id)
        if not claimed:
            continue

        attempted_count += 1
        item_sent = 0
        item_errors: List[str] = []
        copy = _notification_copy_for_send(row=row, llm_client=llm_client, is_test=False)
        title_text = _text(copy.get("title")) or _text(row.get("title")) or "Personal update"
        message_text = _text(copy.get("message")) or _text(row.get("message"))
        for route_platform, route_targets in grouped_routes.items():
            result = _run_async_blocking(
                _dispatch_personal_notification_async(
                    platform=route_platform,
                    title=title_text,
                    message=message_text,
                    target_routes=route_targets,
                    account_id=account_id,
                    settings=settings,
                )
            )
            result_map = result if isinstance(result, dict) else {}
            if bool(result_map.get("ok")):
                item_sent += max(1, _as_int(result_map.get("sent_count"), 1, minimum=1, maximum=50))
            for err in list(result_map.get("errors") or []):
                err_text = _clean_text_blob(err, max_chars=240)
                if err_text:
                    item_errors.append(err_text)

        if item_sent > 0:
            sent_count += item_sent
            continue

        _release_notification_claim(claim_key)
        errors.extend(item_errors)

    if config_errors:
        for err in config_errors[:3]:
            logger.warning("[personal_core] notification config issue account=%s: %s", account_id, err)

    if errors:
        for err in errors[:3]:
            logger.warning("[personal_core] notification issue account=%s: %s", account_id, err)

    all_errors = list(config_errors) + list(errors)

    return {
        "sent_count": sent_count,
        "attempted_count": attempted_count,
        "error_count": len(all_errors),
        "errors": all_errors,
    }


def _group_notification_routes(*, settings: Dict[str, Any], catalog: Dict[str, Any]) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    configured_routes = _notification_routes_from_settings(settings, catalog=catalog)
    supported_platforms = {_slug(item, default="") for item in core_notifier_platforms()}
    grouped_routes: Dict[str, List[Dict[str, Any]]] = {}
    config_errors: List[str] = []

    for route in configured_routes:
        if not isinstance(route, dict):
            continue
        platform_name = _slug(route.get("platform"), default="")
        targets = _clean_targets_dict(route.get("targets"))
        if not platform_name:
            continue
        if platform_name not in supported_platforms:
            config_errors.append(f"Unsupported notification platform: {platform_name}")
            continue
        if _platform_requires_target(platform_name, catalog=catalog) and not targets:
            config_errors.append(f"Platform '{platform_name}' requires a destination target.")
            continue
        grouped_routes.setdefault(platform_name, []).append(targets)
    return grouped_routes, config_errors


def _latest_saved_items_for_notification_tests() -> Dict[str, Optional[Dict[str, Any]]]:
    latest_rows: Dict[str, Optional[Dict[str, Any]]] = {
        "deliveries": None,
        "spending": None,
        "plans": None,
    }
    latest_scores: Dict[str, float] = {
        "deliveries": -1.0,
        "spending": -1.0,
        "plans": -1.0,
    }

    for account_id in _all_account_ids():
        profile = _load_profile(account_id)
        email_ts_index: Dict[str, float] = {}
        for hist in _load_email_history(account_id, limit=5000):
            if not isinstance(hist, dict):
                continue
            hid = _text(hist.get("id"))
            if not hid:
                continue
            email_ts_index[hid] = _as_float(hist.get("date_ts"), 0.0, minimum=0.0)

        for row in profile.get("deliveries") if isinstance(profile.get("deliveries"), list) else []:
            if not isinstance(row, dict):
                continue
            email_ts = _as_float(email_ts_index.get(_text(row.get("email_id"))), 0.0, minimum=0.0)
            score = max(email_ts, _as_float(row.get("eta_ts"), 0.0, minimum=0.0))
            if score >= latest_scores["deliveries"]:
                latest_scores["deliveries"] = score
                row_copy = dict(row)
                row_copy["account_id"] = account_id
                latest_rows["deliveries"] = row_copy

        for row in profile.get("spending_habits") if isinstance(profile.get("spending_habits"), list) else []:
            if not isinstance(row, dict):
                continue
            email_ts = _as_float(email_ts_index.get(_text(row.get("email_id"))), 0.0, minimum=0.0)
            score = max(email_ts, _as_float(row.get("observed_ts"), 0.0, minimum=0.0))
            if score >= latest_scores["spending"]:
                latest_scores["spending"] = score
                row_copy = dict(row)
                row_copy["account_id"] = account_id
                latest_rows["spending"] = row_copy

        for row in profile.get("upcoming_events") if isinstance(profile.get("upcoming_events"), list) else []:
            if not isinstance(row, dict):
                continue
            email_ts = _as_float(email_ts_index.get(_text(row.get("email_id"))), 0.0, minimum=0.0)
            score = max(email_ts, _as_float(row.get("starts_ts"), 0.0, minimum=0.0))
            if score >= latest_scores["plans"]:
                latest_scores["plans"] = score
                row_copy = dict(row)
                row_copy["account_id"] = account_id
                latest_rows["plans"] = row_copy
    return latest_rows


def _notification_test_messages_from_saved_items(settings: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    rows = _latest_saved_items_for_notification_tests()
    out: List[Dict[str, Any]] = []
    config = settings if isinstance(settings, dict) else {}
    deliveries_enabled = _as_bool(config.get("notify_on_deliveries"), True)
    spending_enabled = _as_bool(config.get("notify_on_spending"), True)
    plans_enabled = _as_bool(config.get("notify_on_plans"), True)

    delivery = rows.get("deliveries")
    if deliveries_enabled and isinstance(delivery, dict):
        carrier = _text(delivery.get("carrier")) or "A shipment"
        tracking = _text(delivery.get("tracking_id"))
        order_number = _text(delivery.get("order_number"))
        item_description = _text(delivery.get("item_description"))
        status_text = _delivery_status_text(delivery.get("status"))
        eta = _text(delivery.get("eta_at"))
        merchant = _text(delivery.get("merchant"))
        ref_text = f" ({tracking})" if tracking else (f" (order {order_number})" if order_number else "")
        eta_text = f" ETA {eta}." if eta else "."
        merchant_text = f" from {merchant}" if merchant else ""
        item_text = f" for {item_description}" if item_description else ""
        out.append(
            {
                "kind": "deliveries",
                "title": "Test: delivery notification",
                "message": f"It looks like your delivery{merchant_text}{item_text} via {carrier}{ref_text} is now {status_text}.{eta_text}",
                "rewrite_context": {
                    "carrier": carrier,
                    "tracking_id": tracking,
                    "order_number": order_number,
                    "item_description": item_description,
                    "status": _slug(delivery.get("status"), default="update"),
                    "eta_at": eta,
                    "merchant": merchant,
                },
            }
        )

    spending = rows.get("spending")
    if spending_enabled and isinstance(spending, dict):
        merchant = _text(spending.get("merchant")) or "an unknown merchant"
        amount = _as_float(spending.get("amount"), 0.0, minimum=0.0)
        observed = _text(spending.get("observed_at"))
        when_text = f" on {observed}" if observed else ""
        out.append(
            {
                "kind": "spending",
                "title": "Test: spending notification",
                "message": f"I noticed a new purchase: {merchant} for ${amount:.2f}{when_text}.",
                "rewrite_context": {
                    "merchant": merchant,
                    "amount": round(amount, 2),
                    "currency": _text(spending.get("currency")) or "USD",
                    "observed_at": observed,
                },
            }
        )

    plan = rows.get("plans")
    if plans_enabled and isinstance(plan, dict):
        title = _text(plan.get("title")) or "an upcoming event"
        starts = _text(plan.get("starts_at")) or "an upcoming time"
        location = _text(plan.get("location"))
        location_text = f" at {location}" if location else ""
        out.append(
            {
                "kind": "plans",
                "title": "Test: plan notification",
                "message": f"I see you have a plan coming up: {title} at {starts}{location_text}.",
                "rewrite_context": {
                    "title": title,
                    "starts_at": starts,
                    "location": location,
                    "event_kind": _text(plan.get("kind")) or "event",
                },
            }
        )

    return out


def _send_notification_tests_with_settings(settings: Dict[str, Any], llm_client: Any = None) -> Dict[str, Any]:
    catalog = _load_destination_catalog()
    grouped_routes, config_errors = _group_notification_routes(settings=settings, catalog=catalog)
    if not grouped_routes:
        return {
            "ok": False,
            "sent_count": 0,
            "errors": config_errors or ["No valid notification destinations configured for tests."],
            "message": "No valid notification destinations configured for tests.",
        }

    tests = _notification_test_messages_from_saved_items(settings)
    if not tests:
        return {
            "ok": False,
            "sent_count": 0,
            "errors": [],
            "message": "No saved data found for the currently enabled test categories yet.",
        }

    sent_count = 0
    errors: List[str] = []
    for test in tests:
        copy = _notification_copy_for_send(row=test, llm_client=llm_client, is_test=True)
        title_text = _text(copy.get("title")) or _text(test.get("title")) or "Personal notification test"
        message_text = _text(copy.get("message")) or _text(test.get("message"))
        for route_platform, route_targets in grouped_routes.items():
            result = _run_async_blocking(
                _dispatch_personal_notification_async(
                    platform=route_platform,
                    title=title_text,
                    message=message_text,
                    target_routes=route_targets,
                    account_id="test",
                    settings=settings,
                )
            )
            result_map = result if isinstance(result, dict) else {}
            if bool(result_map.get("ok")):
                sent_count += max(1, _as_int(result_map.get("sent_count"), 1, minimum=1, maximum=200))
            for err in list(result_map.get("errors") or []):
                err_text = _clean_text_blob(err, max_chars=240)
                if err_text:
                    errors.append(err_text)

    all_errors = list(config_errors) + errors
    if all_errors:
        for err in all_errors[:3]:
            logger.warning("[personal_core] notification test issue: %s", err)

    if sent_count <= 0:
        return {
            "ok": False,
            "sent_count": 0,
            "errors": all_errors,
            "message": "Notification test failed to send.",
        }

    return {
        "ok": True,
        "sent_count": sent_count,
        "errors": all_errors,
        "message": f"Sent {sent_count} test notification delivery(ies).",
    }


def _run_account_cycle(llm_client: Any, account: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    account_id = _text(account.get("account_id")) or _account_storage_id(account)

    fetch_result = _fetch_new_emails_for_account(account, settings)
    scan_ok = bool(fetch_result.get("ok"))
    fetch_error = _text(fetch_result.get("error"))
    fetched_rows = list(fetch_result.get("emails") or [])
    uid_candidates = _as_int(fetch_result.get("uid_candidates"), 0, minimum=0)
    selected_count = _as_int(fetch_result.get("uids_selected"), 0, minimum=0)
    fetched_count = _as_int(fetch_result.get("raw_bytes_count"), len(fetched_rows), minimum=0)
    normalized_count = _as_int(fetch_result.get("normalized_count"), len(fetched_rows), minimum=0)
    parse_error_count = _as_int(fetch_result.get("parse_error_count"), 0, minimum=0)
    raw_missing_count = _as_int(fetch_result.get("raw_missing_count"), 0, minimum=0)
    fetch_non_ok = _as_int(fetch_result.get("fetch_non_ok"), 0, minimum=0)
    max_uid_seen = _as_int(fetch_result.get("max_uid_seen"), _as_int(redis_client.get(_cursor_key(account_id)), 0, minimum=0), minimum=0)

    persisted = _persist_normalized_emails(
        account_id,
        fetched_rows,
        max_stored=_as_int(settings.get("max_stored_emails"), 1500, minimum=100, maximum=50000),
    )
    inserted_rows = list(persisted.get("inserted") or [])

    if max_uid_seen > 0:
        try:
            redis_client.set(_cursor_key(account_id), str(max_uid_seen))
        except Exception:
            pass

    profile = _load_profile(account_id)
    updates: Dict[str, List[Dict[str, Any]]] = {
        "spending_habits": [],
        "favorite_places": [],
        "important_notes": [],
        "upcoming_events": [],
        "subscriptions": [],
        "deliveries": [],
        "action_items": [],
    }
    source_kind = "none"
    if inserted_rows:
        updates, source_kind = _extract_updates_for_rows(
            llm_client,
            account_id=account_id,
            inserted_rows=inserted_rows,
            profile=profile,
            settings=settings,
        )

    merged_profile = _merge_profile_updates(
        profile,
        updates,
        settings=settings,
        source_kind=source_kind,
        emails_stored=_as_int(persisted.get("total_stored"), 0, minimum=0),
        emails_new=len(inserted_rows),
        scan_ok=scan_ok,
        error_text=fetch_error,
    )
    _save_profile(account_id, merged_profile)
    notification_result = _send_new_update_notifications(
        account_id=account_id,
        updates=updates,
        settings=settings,
        llm_client=llm_client,
    )

    if fetch_error:
        logger.warning("[personal_core] account=%s scan issue: %s", account_id, fetch_error)

    return {
        "account_id": account_id,
        "ok": scan_ok,
        "error": fetch_error,
        "uid_candidates": uid_candidates,
        "selected_count": selected_count,
        "fetched_count": fetched_count,
        "normalized_count": normalized_count,
        "parse_error_count": parse_error_count,
        "raw_missing_count": raw_missing_count,
        "fetch_non_ok_count": fetch_non_ok,
        "inserted_count": len(inserted_rows),
        "stored_count": _as_int(persisted.get("total_stored"), 0, minimum=0),
        "updated_spending": len(updates.get("spending_habits") or []),
        "updated_notes": len(updates.get("important_notes") or []),
        "updated_events": len(updates.get("upcoming_events") or []),
        "updated_favorites": len(updates.get("favorite_places") or []),
        "updated_subscriptions": len(updates.get("subscriptions") or []),
        "updated_deliveries": len(updates.get("deliveries") or []),
        "updated_actions": len(updates.get("action_items") or []),
        "notifications_sent": _as_int(notification_result.get("sent_count"), 0, minimum=0),
        "notification_attempted": _as_int(notification_result.get("attempted_count"), 0, minimum=0),
        "notification_errors": _as_int(notification_result.get("error_count"), 0, minimum=0),
        "source_kind": source_kind,
        "upcoming_events_count": len(list(merged_profile.get("upcoming_events") or [])),
        "open_deliveries_count": _as_int((merged_profile.get("stats") or {}).get("deliveries_open"), 0, minimum=0),
        "open_actions_count": _as_int((merged_profile.get("stats") or {}).get("action_items_open"), 0, minimum=0),
    }


def _run_cycle(llm_client: Any, settings: Dict[str, Any]) -> Dict[str, Any]:
    accounts = _resolve_accounts(settings)
    if not accounts:
        return {
            "account_count": 0,
            "ok_count": 0,
            "error_count": 0,
            "uid_candidates": 0,
            "selected_count": 0,
            "fetched_count": 0,
            "normalized_count": 0,
            "parse_error_count": 0,
            "raw_missing_count": 0,
            "fetch_non_ok_count": 0,
            "inserted_count": 0,
            "updated_spending": 0,
            "updated_notes": 0,
            "updated_events": 0,
            "updated_favorites": 0,
            "updated_subscriptions": 0,
            "updated_deliveries": 0,
            "updated_actions": 0,
            "notifications_sent": 0,
            "notification_attempted": 0,
            "notification_errors": 0,
            "upcoming_events_count": 0,
            "open_deliveries_count": 0,
            "open_actions_count": 0,
            "errors": ["No personal email accounts configured."],
            "accounts": [],
        }

    account_rows: List[Dict[str, Any]] = []
    ok_count = 0
    error_count = 0
    uid_candidates = 0
    selected_count = 0
    fetched_count = 0
    normalized_count = 0
    parse_error_count = 0
    raw_missing_count = 0
    fetch_non_ok_count = 0
    inserted_count = 0
    updated_spending = 0
    updated_notes = 0
    updated_events = 0
    updated_favorites = 0
    updated_subscriptions = 0
    updated_deliveries = 0
    updated_actions = 0
    notifications_sent = 0
    notification_attempted = 0
    notification_errors = 0
    upcoming_events_count = 0
    open_deliveries_count = 0
    open_actions_count = 0
    errors: List[str] = []

    for account in accounts:
        row = _run_account_cycle(llm_client, account, settings)
        account_rows.append(row)
        if bool(row.get("ok")):
            ok_count += 1
        else:
            error_count += 1
            error_text = _text(row.get("error"))
            if error_text:
                errors.append(f"{_text(row.get('account_id'))}: {error_text}")
        uid_candidates += _as_int(row.get("uid_candidates"), 0, minimum=0)
        selected_count += _as_int(row.get("selected_count"), 0, minimum=0)
        fetched_count += _as_int(row.get("fetched_count"), 0, minimum=0)
        normalized_count += _as_int(row.get("normalized_count"), 0, minimum=0)
        parse_error_count += _as_int(row.get("parse_error_count"), 0, minimum=0)
        raw_missing_count += _as_int(row.get("raw_missing_count"), 0, minimum=0)
        fetch_non_ok_count += _as_int(row.get("fetch_non_ok_count"), 0, minimum=0)
        inserted_count += _as_int(row.get("inserted_count"), 0, minimum=0)
        updated_spending += _as_int(row.get("updated_spending"), 0, minimum=0)
        updated_notes += _as_int(row.get("updated_notes"), 0, minimum=0)
        updated_events += _as_int(row.get("updated_events"), 0, minimum=0)
        updated_favorites += _as_int(row.get("updated_favorites"), 0, minimum=0)
        updated_subscriptions += _as_int(row.get("updated_subscriptions"), 0, minimum=0)
        updated_deliveries += _as_int(row.get("updated_deliveries"), 0, minimum=0)
        updated_actions += _as_int(row.get("updated_actions"), 0, minimum=0)
        notifications_sent += _as_int(row.get("notifications_sent"), 0, minimum=0)
        notification_attempted += _as_int(row.get("notification_attempted"), 0, minimum=0)
        notification_errors += _as_int(row.get("notification_errors"), 0, minimum=0)
        upcoming_events_count += _as_int(row.get("upcoming_events_count"), 0, minimum=0)
        open_deliveries_count += _as_int(row.get("open_deliveries_count"), 0, minimum=0)
        open_actions_count += _as_int(row.get("open_actions_count"), 0, minimum=0)

    return {
        "account_count": len(accounts),
        "ok_count": ok_count,
        "error_count": error_count,
        "uid_candidates": uid_candidates,
        "selected_count": selected_count,
        "fetched_count": fetched_count,
        "normalized_count": normalized_count,
        "parse_error_count": parse_error_count,
        "raw_missing_count": raw_missing_count,
        "fetch_non_ok_count": fetch_non_ok_count,
        "inserted_count": inserted_count,
        "updated_spending": updated_spending,
        "updated_notes": updated_notes,
        "updated_events": updated_events,
        "updated_favorites": updated_favorites,
        "updated_subscriptions": updated_subscriptions,
        "updated_deliveries": updated_deliveries,
        "updated_actions": updated_actions,
        "notifications_sent": notifications_sent,
        "notification_attempted": notification_attempted,
        "notification_errors": notification_errors,
        "upcoming_events_count": upcoming_events_count,
        "open_deliveries_count": open_deliveries_count,
        "open_actions_count": open_actions_count,
        "errors": errors,
        "accounts": account_rows,
    }


def _save_cycle_stats(stats: Dict[str, Any], *, cycle_start: float) -> None:
    mapping = {
        "last_run_ts": str(float(cycle_start)),
        "account_count": str(_as_int(stats.get("account_count"), 0, minimum=0)),
        "ok_count": str(_as_int(stats.get("ok_count"), 0, minimum=0)),
        "error_count": str(_as_int(stats.get("error_count"), 0, minimum=0)),
        "uid_candidates": str(_as_int(stats.get("uid_candidates"), 0, minimum=0)),
        "selected_count": str(_as_int(stats.get("selected_count"), 0, minimum=0)),
        "fetched_count": str(_as_int(stats.get("fetched_count"), 0, minimum=0)),
        "normalized_count": str(_as_int(stats.get("normalized_count"), 0, minimum=0)),
        "parse_error_count": str(_as_int(stats.get("parse_error_count"), 0, minimum=0)),
        "raw_missing_count": str(_as_int(stats.get("raw_missing_count"), 0, minimum=0)),
        "fetch_non_ok_count": str(_as_int(stats.get("fetch_non_ok_count"), 0, minimum=0)),
        "inserted_count": str(_as_int(stats.get("inserted_count"), 0, minimum=0)),
        "updated_spending": str(_as_int(stats.get("updated_spending"), 0, minimum=0)),
        "updated_notes": str(_as_int(stats.get("updated_notes"), 0, minimum=0)),
        "updated_events": str(_as_int(stats.get("updated_events"), 0, minimum=0)),
        "updated_favorites": str(_as_int(stats.get("updated_favorites"), 0, minimum=0)),
        "updated_subscriptions": str(_as_int(stats.get("updated_subscriptions"), 0, minimum=0)),
        "updated_deliveries": str(_as_int(stats.get("updated_deliveries"), 0, minimum=0)),
        "updated_actions": str(_as_int(stats.get("updated_actions"), 0, minimum=0)),
        "notifications_sent": str(_as_int(stats.get("notifications_sent"), 0, minimum=0)),
        "notification_attempted": str(_as_int(stats.get("notification_attempted"), 0, minimum=0)),
        "notification_errors": str(_as_int(stats.get("notification_errors"), 0, minimum=0)),
        "upcoming_events_count": str(_as_int(stats.get("upcoming_events_count"), 0, minimum=0)),
        "open_deliveries_count": str(_as_int(stats.get("open_deliveries_count"), 0, minimum=0)),
        "open_actions_count": str(_as_int(stats.get("open_actions_count"), 0, minimum=0)),
        "errors": json.dumps(list(stats.get("errors") or []), ensure_ascii=False),
    }
    try:
        redis_client.hset(_PERSONAL_STATS_KEY, mapping=mapping)
    except Exception:
        return


def _load_cycle_stats() -> Dict[str, Any]:
    raw = redis_client.hgetall(_PERSONAL_STATS_KEY) or {}

    errors_raw = _text(raw.get("errors"))
    errors: List[str] = []
    if errors_raw:
        try:
            parsed = json.loads(errors_raw)
        except Exception:
            parsed = []
        if isinstance(parsed, list):
            errors = [_text(item) for item in parsed if _text(item)]

    out = {
        "last_run_ts": _as_float(raw.get("last_run_ts"), 0.0, minimum=0.0),
        "account_count": _as_int(raw.get("account_count"), 0, minimum=0),
        "ok_count": _as_int(raw.get("ok_count"), 0, minimum=0),
        "error_count": _as_int(raw.get("error_count"), 0, minimum=0),
        "uid_candidates": _as_int(raw.get("uid_candidates"), 0, minimum=0),
        "selected_count": _as_int(raw.get("selected_count"), 0, minimum=0),
        "fetched_count": _as_int(raw.get("fetched_count"), 0, minimum=0),
        "normalized_count": _as_int(raw.get("normalized_count"), 0, minimum=0),
        "parse_error_count": _as_int(raw.get("parse_error_count"), 0, minimum=0),
        "raw_missing_count": _as_int(raw.get("raw_missing_count"), 0, minimum=0),
        "fetch_non_ok_count": _as_int(raw.get("fetch_non_ok_count"), 0, minimum=0),
        "inserted_count": _as_int(raw.get("inserted_count"), 0, minimum=0),
        "updated_spending": _as_int(raw.get("updated_spending"), 0, minimum=0),
        "updated_notes": _as_int(raw.get("updated_notes"), 0, minimum=0),
        "updated_events": _as_int(raw.get("updated_events"), 0, minimum=0),
        "updated_favorites": _as_int(raw.get("updated_favorites"), 0, minimum=0),
        "updated_subscriptions": _as_int(raw.get("updated_subscriptions"), 0, minimum=0),
        "updated_deliveries": _as_int(raw.get("updated_deliveries"), 0, minimum=0),
        "updated_actions": _as_int(raw.get("updated_actions"), 0, minimum=0),
        "notifications_sent": _as_int(raw.get("notifications_sent"), 0, minimum=0),
        "notification_attempted": _as_int(raw.get("notification_attempted"), 0, minimum=0),
        "notification_errors": _as_int(raw.get("notification_errors"), 0, minimum=0),
        "upcoming_events_count": _as_int(raw.get("upcoming_events_count"), 0, minimum=0),
        "open_deliveries_count": _as_int(raw.get("open_deliveries_count"), 0, minimum=0),
        "open_actions_count": _as_int(raw.get("open_actions_count"), 0, minimum=0),
        "errors": errors,
    }
    out["last_run_text"] = _iso_from_ts(out.get("last_run_ts"))
    return out


def _sleep_with_stop(seconds: int, stop_event: Optional[object]) -> None:
    target = max(1, int(seconds))
    elapsed = 0.0
    while elapsed < target:
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
            return
        step = min(0.5, target - elapsed)
        time.sleep(step)
        elapsed += step


def run(stop_event: Optional[object] = None) -> None:
    logger.info("[personal_core] started")
    llm_client = None
    llm_ready_logged = False

    while True:
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
            break

        settings = _load_settings()
        interval_seconds = _as_int(settings.get("interval_seconds"), 300, minimum=30, maximum=3600)

        if llm_client is None:
            try:
                llm_client = get_llm_client_from_env()
                if llm_client is not None and not llm_ready_logged:
                    llm_ready_logged = True
                    logger.info("[personal_core] LLM client initialized")
            except Exception as exc:
                logger.warning("[personal_core] could not initialize LLM client: %s", exc)
                llm_client = None

        cycle_start = time.time()
        try:
            stats = _run_cycle(llm_client, settings)
            _save_cycle_stats(stats, cycle_start=cycle_start)
            logger.info(
                "[personal_core] cycle: accounts=%s ok=%s errors=%s new_emails=%s events=%s deliveries=%s actions=%s notifications=%s",
                _as_int(stats.get("account_count"), 0, minimum=0),
                _as_int(stats.get("ok_count"), 0, minimum=0),
                _as_int(stats.get("error_count"), 0, minimum=0),
                _as_int(stats.get("inserted_count"), 0, minimum=0),
                _as_int(stats.get("updated_events"), 0, minimum=0),
                _as_int(stats.get("updated_deliveries"), 0, minimum=0),
                _as_int(stats.get("updated_actions"), 0, minimum=0),
                _as_int(stats.get("notifications_sent"), 0, minimum=0),
            )
            logger.debug(
                "[personal_core] cycle details: uid_candidates=%s selected=%s fetched=%s normalized=%s parse_errors=%s raw_missing=%s fetch_non_ok=%s notification_attempted=%s notification_errors=%s",
                _as_int(stats.get("uid_candidates"), 0, minimum=0),
                _as_int(stats.get("selected_count"), 0, minimum=0),
                _as_int(stats.get("fetched_count"), 0, minimum=0),
                _as_int(stats.get("normalized_count"), 0, minimum=0),
                _as_int(stats.get("parse_error_count"), 0, minimum=0),
                _as_int(stats.get("raw_missing_count"), 0, minimum=0),
                _as_int(stats.get("fetch_non_ok_count"), 0, minimum=0),
                _as_int(stats.get("notification_attempted"), 0, minimum=0),
                _as_int(stats.get("notification_errors"), 0, minimum=0),
            )
            errors = list(stats.get("errors") or [])
            for err in errors[:5]:
                err_text = _clean_text_blob(err, max_chars=260)
                if err_text:
                    logger.warning("[personal_core] cycle error: %s", err_text)
        except Exception as exc:
            logger.exception("[personal_core] cycle failed: %s", exc)

        _sleep_with_stop(interval_seconds, stop_event)

    logger.info("[personal_core] stopped")


def _all_account_ids() -> List[str]:
    out: List[str] = []
    seen = set()

    try:
        for raw in redis_client.smembers(_PERSONAL_ACCOUNTS_SET_KEY) or []:
            account_id = _text(raw)
            if account_id and account_id not in seen:
                seen.add(account_id)
                out.append(account_id)
    except Exception:
        pass

    try:
        for raw_key in redis_client.scan_iter(match=f"{_PERSONAL_HISTORY_PREFIX}:*", count=200):
            key = _text(raw_key)
            if not key:
                continue
            account_id = key.split(f"{_PERSONAL_HISTORY_PREFIX}:", 1)[-1].strip()
            if account_id and account_id not in seen:
                seen.add(account_id)
                out.append(account_id)
    except Exception:
        pass

    out.sort()
    return out


def _aggregate_profiles() -> Dict[str, Any]:
    accounts = _all_account_ids()
    profiles: List[Dict[str, Any]] = []
    for account_id in accounts:
        profiles.append(_load_profile(account_id))

    total_emails = 0
    total_spending = 0.0
    total_spending_30d = 0.0
    upcoming_events: List[Dict[str, Any]] = []
    subscriptions: List[Dict[str, Any]] = []
    deliveries: List[Dict[str, Any]] = []
    action_items: List[Dict[str, Any]] = []
    merchant_totals: Dict[str, float] = {}
    account_rows: List[Dict[str, Any]] = []
    open_deliveries = 0
    open_actions = 0

    for profile in profiles:
        account_id = _text(profile.get("account_id"))
        stats = profile.get("stats") if isinstance(profile.get("stats"), dict) else {}
        emails_stored = _as_int(stats.get("emails_stored"), 0, minimum=0)
        spending_total = _as_float(stats.get("spending_total"), 0.0, minimum=0.0)
        spending_30d = _as_float(stats.get("spending_30d"), 0.0, minimum=0.0)

        total_emails += emails_stored
        total_spending += spending_total
        total_spending_30d += spending_30d

        for row in profile.get("upcoming_events") if isinstance(profile.get("upcoming_events"), list) else []:
            if not isinstance(row, dict):
                continue
            row_copy = dict(row)
            row_copy["account_id"] = account_id
            upcoming_events.append(row_copy)

        for row in profile.get("subscriptions") if isinstance(profile.get("subscriptions"), list) else []:
            if not isinstance(row, dict):
                continue
            row_copy = dict(row)
            row_copy["account_id"] = account_id
            subscriptions.append(row_copy)

        for row in profile.get("deliveries") if isinstance(profile.get("deliveries"), list) else []:
            if not isinstance(row, dict):
                continue
            row_copy = dict(row)
            row_copy["account_id"] = account_id
            deliveries.append(row_copy)

        for row in profile.get("action_items") if isinstance(profile.get("action_items"), list) else []:
            if not isinstance(row, dict):
                continue
            row_copy = dict(row)
            row_copy["account_id"] = account_id
            action_items.append(row_copy)

        for spend in profile.get("spending_habits") if isinstance(profile.get("spending_habits"), list) else []:
            if not isinstance(spend, dict):
                continue
            merchant = _text(spend.get("merchant"))
            if not merchant:
                continue
            merchant_totals[merchant] = merchant_totals.get(merchant, 0.0) + _as_float(spend.get("amount"), 0.0, minimum=0.0)

        account_rows.append(
            {
                "account_id": account_id,
                "emails": emails_stored,
                "events": len(list(profile.get("upcoming_events") or [])),
                "subscriptions": len(list(profile.get("subscriptions") or [])),
                "deliveries_open": len(
                    [
                        row
                        for row in list(profile.get("deliveries") or [])
                        if isinstance(row, dict) and _slug(row.get("status"), default="update") != "delivered"
                    ]
                ),
                "actions_open": len(
                    [
                        row
                        for row in list(profile.get("action_items") or [])
                        if isinstance(row, dict) and _slug(row.get("status"), default="open") == "open"
                    ]
                ),
                "spending_total": round(spending_total, 2),
                "last_scan": _iso_from_ts(stats.get("last_scan_ts")),
                "status": _text(stats.get("last_scan_status")) or "idle",
                "error": _text(stats.get("last_error")),
            }
        )

        open_deliveries += _as_int(stats.get("deliveries_open"), 0, minimum=0)
        open_actions += _as_int(stats.get("action_items_open"), 0, minimum=0)

    upcoming_events.sort(key=lambda row: _as_float(row.get("starts_ts"), 0.0))
    subscriptions.sort(key=lambda row: _as_float(row.get("next_charge_ts"), 0.0))
    deliveries.sort(key=lambda row: _as_float(row.get("eta_ts"), 0.0), reverse=True)
    action_items.sort(key=lambda row: _as_float(row.get("due_ts"), 0.0))

    merchant_rows = [
        {"merchant": name, "amount": round(amount, 2)}
        for name, amount in merchant_totals.items()
    ]
    merchant_rows.sort(key=lambda row: (-_as_float(row.get("amount"), 0.0), _text(row.get("merchant"))))

    return {
        "profiles": profiles,
        "account_rows": account_rows,
        "account_count": len(accounts),
        "total_emails": total_emails,
        "total_spending": round(total_spending, 2),
        "total_spending_30d": round(total_spending_30d, 2),
        "upcoming_events": upcoming_events,
        "subscriptions": subscriptions,
        "deliveries": deliveries,
        "action_items": action_items,
        "open_deliveries": open_deliveries,
        "open_actions": open_actions,
        "merchant_rows": merchant_rows,
    }


def _history_query_day_bounds(date_value: Any) -> Tuple[float, float]:
    raw = _text(date_value).strip()
    if not raw:
        return 0.0, 0.0
    token = raw.replace("/", "-")
    if "T" in token:
        token = token.split("T", 1)[0]
    if " " in token:
        token = token.split(" ", 1)[0]
    try:
        dt = datetime.fromisoformat(token)
    except Exception:
        return 0.0, 0.0
    start_dt = datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=timezone.utc)
    start_ts = start_dt.timestamp()
    end_ts = start_ts + 86399.0
    return start_ts, end_ts


def _history_query_date_iso(date_value: Any) -> str:
    start_ts, _end_ts = _history_query_day_bounds(date_value)
    if start_ts <= 0:
        return ""
    return datetime.fromtimestamp(start_ts, timezone.utc).strftime("%Y-%m-%d")


def _history_query_matches(query_text: str, haystack: str) -> bool:
    query_token = _text(query_text).strip().lower()
    if not query_token:
        return True
    corpus = _text(haystack).lower()
    if query_token in corpus:
        return True
    terms = [part for part in re.split(r"[^a-z0-9]+", query_token) if len(part) >= 2]
    if not terms:
        return False
    return all(term in corpus for term in terms)


def _email_tool_plan_defaults(
    *,
    args: Dict[str, Any],
    default_days: int,
    default_limit: int,
) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    query_text = _text(payload.get("query") or payload.get("text"))
    return {
        "intent": "search",
        "mode": "search",
        "query_terms": query_text,
        "days": _as_int(payload.get("days"), default_days, minimum=0, maximum=3650),
        "limit": _as_int(payload.get("limit"), default_limit, minimum=1, maximum=50),
        "account_id": _text(payload.get("account_id")),
        "date_from": _history_query_date_iso(payload.get("date_from")),
        "date_to": _history_query_date_iso(payload.get("date_to")),
    }


def _merge_email_tool_plan(parsed: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(fallback)
    if not isinstance(parsed, dict):
        return out

    mode = _slug(parsed.get("mode"), default="")
    if mode in {"search", "latest"}:
        out["mode"] = mode

    intent = _slug(parsed.get("intent"), default="")
    if intent in {"search", "summary", "summarize", "latest"}:
        out["intent"] = intent
        if intent == "latest":
            out["mode"] = "latest"

    query_terms = _text(parsed.get("query_terms") or parsed.get("query"))
    if query_terms:
        out["query_terms"] = query_terms

    if parsed.get("days") is not None:
        out["days"] = _as_int(parsed.get("days"), out.get("days"), minimum=0, maximum=3650)
    if parsed.get("limit") is not None:
        out["limit"] = _as_int(parsed.get("limit"), out.get("limit"), minimum=1, maximum=50)

    account_id = _text(parsed.get("account_id"))
    if account_id:
        out["account_id"] = account_id

    date_from = _history_query_date_iso(parsed.get("date_from"))
    date_to = _history_query_date_iso(parsed.get("date_to"))
    if date_from:
        out["date_from"] = date_from
    if date_to:
        out["date_to"] = date_to
    return out


async def _plan_email_tool_query_async(
    *,
    args: Dict[str, Any],
    llm_client: Any,
    tool_name: str,
    default_days: int,
    default_limit: int,
) -> Dict[str, Any]:
    fallback = _email_tool_plan_defaults(args=args, default_days=default_days, default_limit=default_limit)
    if llm_client is None:
        return fallback

    planner_prompt = (
        "You convert natural-language email requests into structured filters for a cached email tool.\n"
        "Return strict JSON only with this schema:\n"
        "{\n"
        "  \"intent\":\"search|summary|latest\",\n"
        "  \"mode\":\"search|latest\",\n"
        "  \"query_terms\":\"\",\n"
        "  \"days\":90,\n"
        "  \"limit\":8,\n"
        "  \"date_from\":\"YYYY-MM-DD or empty\",\n"
        "  \"date_to\":\"YYYY-MM-DD or empty\",\n"
        "  \"account_id\":\"\"\n"
        "}\n"
        "Rules:\n"
        "- Use mode=latest when user asks for newest/latest/most recent email.\n"
        "- Use date_from/date_to when user asks for a specific day or date range.\n"
        "- query_terms should contain only topical terms to match email text.\n"
        "- Keep days in [0,3650] and limit in [1,50].\n"
        "- Never include prose outside JSON."
    )
    planner_payload = {
        "tool": tool_name,
        "today_utc": datetime.utcnow().strftime("%Y-%m-%d"),
        "request": {
            "query": _text(args.get("query") or args.get("text")),
            "days": args.get("days"),
            "limit": args.get("limit"),
            "account_id": args.get("account_id"),
            "date_from": args.get("date_from"),
            "date_to": args.get("date_to"),
        },
        "defaults": {
            "days": fallback.get("days"),
            "limit": fallback.get("limit"),
        },
    }

    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": planner_prompt},
                {"role": "user", "content": json.dumps(planner_payload, ensure_ascii=False)},
            ],
            temperature=0.0,
        )
    except Exception as exc:
        logger.warning("[personal_core] NL planner failed for tool=%s: %s", tool_name, exc)
        return fallback

    text = _text(((response or {}).get("message") or {}).get("content"))
    if not text:
        return fallback
    blob = extract_json(text) or text
    try:
        parsed = json.loads(blob)
    except Exception:
        logger.warning("[personal_core] NL planner returned invalid JSON for tool=%s", tool_name)
        return fallback
    if not isinstance(parsed, dict):
        return fallback
    return _merge_email_tool_plan(parsed, fallback)


def _history_search(
    *,
    query: str,
    limit: int,
    days: int,
    account_id: str = "",
    mode: str = "search",
    date_from: str = "",
    date_to: str = "",
) -> List[Dict[str, Any]]:
    query_text = _text(query).lower()
    account_filter = _text(account_id)
    mode_token = _slug(mode, default="search")
    cutoff = 0.0
    if int(days) > 0:
        cutoff = time.time() - (max(1, int(days)) * 86400)

    date_from_start, _date_from_end = _history_query_day_bounds(date_from)
    _date_to_start, date_to_end = _history_query_day_bounds(date_to)
    if date_from_start > 0 and date_to_end > 0 and date_to_end < date_from_start:
        date_from_start, date_to_end = date_to_end, date_from_start + 86399.0

    accounts = [account_filter] if account_filter else _all_account_ids()
    hits: List[Dict[str, Any]] = []
    for aid in accounts:
        rows = _load_email_history(aid, limit=5000)
        for row in rows:
            ts = _as_float(row.get("date_ts"), 0.0, minimum=0.0)
            if cutoff > 0 and ts < cutoff:
                continue
            if date_from_start > 0 and ts < date_from_start:
                continue
            if date_to_end > 0 and ts > date_to_end:
                continue

            date_iso = _text(row.get("date_iso")) or _iso_from_ts(ts)
            haystack = " ".join(
                [
                    date_iso,
                    _text(row.get("subject")),
                    _text(row.get("from")),
                    _text(row.get("snippet")),
                    _text(row.get("body")),
                ]
            ).lower()
            if mode_token != "latest" and not _history_query_matches(query_text, haystack):
                continue

            hits.append(
                {
                    "id": _text(row.get("id")),
                    "account_id": aid,
                    "date_iso": date_iso,
                    "date_ts": ts,
                    "from": _text(row.get("from")),
                    "subject": _text(row.get("subject")),
                    "snippet": _clean_text_blob(row.get("snippet"), max_chars=320),
                }
            )

    hits.sort(key=lambda row: _as_float(row.get("date_ts"), 0.0), reverse=True)
    return hits[: max(1, int(limit))]


async def _tool_personal_email_search_async(args: Dict[str, Any], llm_client: Any) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    query = _text(payload.get("query") or payload.get("text"))

    plan = await _plan_email_tool_query_async(
        args=payload,
        llm_client=llm_client,
        tool_name="personal_email_search",
        default_days=90,
        default_limit=8,
    )

    hits = _history_search(
        query=_text(plan.get("query_terms")),
        limit=_as_int(plan.get("limit"), 8, minimum=1, maximum=50),
        days=_as_int(plan.get("days"), 90, minimum=0, maximum=3650),
        account_id=_text(plan.get("account_id")),
        mode=_text(plan.get("mode")),
        date_from=_text(plan.get("date_from")),
        date_to=_text(plan.get("date_to")),
    )
    summary_for_user = "No matching emails found."
    if hits:
        if _slug(plan.get("mode"), default="search") == "latest":
            summary_for_user = "Latest email(s): " + "; ".join(
                [
                    f"{_text(hit.get('date_iso'))} - {_text(hit.get('subject'))}"
                    for hit in hits[:6]
                ]
            )
        else:
            summary_for_user = "Found " + str(len(hits)) + " matching email(s): " + "; ".join(
                [
                    f"{_text(hit.get('date_iso'))} - {_text(hit.get('subject'))}"
                    for hit in hits[:6]
                ]
            )

    return {
        "tool": "personal_email_search",
        "ok": True,
        "query": query,
        "days": _as_int(plan.get("days"), 90, minimum=0, maximum=3650),
        "limit": _as_int(plan.get("limit"), 8, minimum=1, maximum=50),
        "account_id": _text(plan.get("account_id")),
        "mode": _text(plan.get("mode")) or "search",
        "date_from": _text(plan.get("date_from")),
        "date_to": _text(plan.get("date_to")),
        "resolved_query_terms": _text(plan.get("query_terms")),
        "matches": hits,
        "match_count": len(hits),
        "summary_for_user": summary_for_user,
    }


def _fallback_summary_text(hits: List[Dict[str, Any]]) -> str:
    if not hits:
        return "No relevant emails were found."
    lines = ["Email summary:"]
    for row in hits[:8]:
        lines.append(
            f"- {_text(row.get('date_iso'))}: {_text(row.get('subject'))} ({_text(row.get('from'))})"
        )
    return "\n".join(lines)


async def _tool_personal_email_summarize_async(args: Dict[str, Any], llm_client: Any) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    query = _text(payload.get("query") or payload.get("text"))

    plan = await _plan_email_tool_query_async(
        args=payload,
        llm_client=llm_client,
        tool_name="personal_email_summarize",
        default_days=30,
        default_limit=12,
    )
    limit = _as_int(plan.get("limit"), 12, minimum=1, maximum=50)
    days = _as_int(plan.get("days"), 30, minimum=0, maximum=3650)
    account_id = _text(plan.get("account_id"))

    hits = _history_search(
        query=_text(plan.get("query_terms")),
        limit=limit,
        days=days,
        account_id=account_id,
        mode=_text(plan.get("mode")),
        date_from=_text(plan.get("date_from")),
        date_to=_text(plan.get("date_to")),
    )
    if not hits:
        return {
            "tool": "personal_email_summarize",
            "ok": True,
            "query": query,
            "summary": "No matching emails found.",
            "matches": [],
            "match_count": 0,
            "mode": _text(plan.get("mode")) or "search",
            "date_from": _text(plan.get("date_from")),
            "date_to": _text(plan.get("date_to")),
            "resolved_query_terms": _text(plan.get("query_terms")),
            "summary_for_user": "No matching emails found.",
        }

    if llm_client is None:
        fallback = _fallback_summary_text(hits)
        return {
            "tool": "personal_email_summarize",
            "ok": True,
            "query": query,
            "summary": fallback,
            "matches": hits,
            "match_count": len(hits),
            "mode": _text(plan.get("mode")) or "search",
            "date_from": _text(plan.get("date_from")),
            "date_to": _text(plan.get("date_to")),
            "resolved_query_terms": _text(plan.get("query_terms")),
            "summary_for_user": fallback,
        }

    summary_payload = {
        "query": query,
        "emails": [
            {
                "date_iso": _text(row.get("date_iso")),
                "from": _text(row.get("from")),
                "subject": _text(row.get("subject")),
                "snippet": _text(row.get("snippet")),
            }
            for row in hits
        ],
    }

    prompt = (
        "Summarize matched personal emails for assistant context.\n"
        "Return concise plain text with:\n"
        "1) Key themes\n"
        "2) Any action items/deadlines\n"
        "3) Any upcoming events mentioned\n"
        "4) Any spending patterns if present\n"
        "Keep it factual and avoid guessing."
    )

    summary_text = ""
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(summary_payload, ensure_ascii=False)},
            ],
            temperature=0.2,
        )
        summary_text = _text(((response or {}).get("message") or {}).get("content"))
    except Exception as exc:
        logger.warning("[personal_core] personal_email_summarize LLM failed: %s", exc)
        summary_text = ""

    if not summary_text:
        summary_text = _fallback_summary_text(hits)

    return {
        "tool": "personal_email_summarize",
        "ok": True,
        "query": query,
        "summary": summary_text,
        "matches": hits,
        "match_count": len(hits),
        "mode": _text(plan.get("mode")) or "search",
        "date_from": _text(plan.get("date_from")),
        "date_to": _text(plan.get("date_to")),
        "resolved_query_terms": _text(plan.get("query_terms")),
        "summary_for_user": summary_text,
    }


def _upcoming_events_for_prompt(*, days: int, limit: int) -> List[Dict[str, Any]]:
    horizon_days = max(1, int(days))
    max_items = max(1, int(limit))
    start_ts = time.time()
    end_ts = start_ts + (horizon_days * 86400)

    all_events: List[Dict[str, Any]] = []
    for account_id in _all_account_ids():
        profile = _load_profile(account_id)
        for row in profile.get("upcoming_events") if isinstance(profile.get("upcoming_events"), list) else []:
            if not isinstance(row, dict):
                continue
            starts_ts = _as_float(row.get("starts_ts"), 0.0, minimum=0.0)
            if starts_ts <= 0 or starts_ts < start_ts or starts_ts > end_ts:
                continue
            row_copy = dict(row)
            row_copy["account_id"] = account_id
            all_events.append(row_copy)

    all_events.sort(key=lambda row: _as_float(row.get("starts_ts"), 0.0))
    return all_events[:max_items]


def _open_deliveries_for_prompt(*, limit: int) -> List[Dict[str, Any]]:
    max_items = max(1, int(limit))
    rows: List[Dict[str, Any]] = []
    for account_id in _all_account_ids():
        profile = _load_profile(account_id)
        for row in profile.get("deliveries") if isinstance(profile.get("deliveries"), list) else []:
            if not isinstance(row, dict):
                continue
            status = _slug(row.get("status"), default="update")
            if status == "delivered":
                continue
            row_copy = dict(row)
            row_copy["account_id"] = account_id
            rows.append(row_copy)
    rows.sort(key=lambda row: _as_float(row.get("eta_ts"), 0.0, minimum=0.0), reverse=True)
    return rows[:max_items]


def _due_actions_for_prompt(*, days: int, limit: int) -> List[Dict[str, Any]]:
    horizon_days = max(1, int(days))
    max_items = max(1, int(limit))
    now_ts = time.time()
    end_ts = now_ts + (horizon_days * 86400)
    rows: List[Dict[str, Any]] = []
    for account_id in _all_account_ids():
        profile = _load_profile(account_id)
        for row in profile.get("action_items") if isinstance(profile.get("action_items"), list) else []:
            if not isinstance(row, dict):
                continue
            status = _slug(row.get("status"), default="open")
            if status != "open":
                continue
            due_ts = _as_float(row.get("due_ts"), 0.0, minimum=0.0)
            if due_ts > 0 and due_ts > end_ts:
                continue
            row_copy = dict(row)
            row_copy["account_id"] = account_id
            rows.append(row_copy)
    rows.sort(key=lambda row: _as_float(row.get("due_ts"), now_ts + 999999999))
    return rows[:max_items]


def _upcoming_subscriptions_for_prompt(*, days: int, limit: int) -> List[Dict[str, Any]]:
    horizon_days = max(1, int(days))
    max_items = max(1, int(limit))
    now_ts = time.time()
    end_ts = now_ts + (horizon_days * 86400)
    rows: List[Dict[str, Any]] = []
    for account_id in _all_account_ids():
        profile = _load_profile(account_id)
        for row in profile.get("subscriptions") if isinstance(profile.get("subscriptions"), list) else []:
            if not isinstance(row, dict):
                continue
            charge_ts = _as_float(row.get("next_charge_ts"), 0.0, minimum=0.0)
            if charge_ts <= 0 or charge_ts > end_ts:
                continue
            row_copy = dict(row)
            row_copy["account_id"] = account_id
            rows.append(row_copy)
    rows.sort(key=lambda row: _as_float(row.get("next_charge_ts"), 0.0))
    return rows[:max_items]


def get_hydra_personal_context_payload(
    *,
    redis_client: Any = None,
    **_kwargs,
) -> Dict[str, Any]:
    del redis_client
    settings = _load_settings()
    days = _as_int(settings.get("prompt_upcoming_days"), 45, minimum=1, maximum=365)
    limit = _as_int(settings.get("prompt_upcoming_limit"), 8, minimum=1, maximum=50)

    events = _upcoming_events_for_prompt(days=days, limit=limit)
    merchant_rows = _aggregate_profiles().get("merchant_rows") or []
    deliveries = _open_deliveries_for_prompt(limit=max(2, min(10, limit)))
    actions = _due_actions_for_prompt(days=max(7, min(30, days)), limit=max(2, min(10, limit)))
    subscriptions = _upcoming_subscriptions_for_prompt(days=max(30, days), limit=max(2, min(10, limit)))

    return {
        "upcoming_events": events,
        "top_merchants": list(merchant_rows)[:8],
        "open_deliveries": deliveries,
        "action_items": actions,
        "upcoming_subscriptions": subscriptions,
        "summary_char_limit": 0,
        "horizon_days": days,
    }


def _personal_prompt_message_from_payload(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    events = payload.get("upcoming_events") if isinstance(payload.get("upcoming_events"), list) else []
    merchants = payload.get("top_merchants") if isinstance(payload.get("top_merchants"), list) else []
    deliveries = payload.get("open_deliveries") if isinstance(payload.get("open_deliveries"), list) else []
    action_items = payload.get("action_items") if isinstance(payload.get("action_items"), list) else []
    subscriptions = payload.get("upcoming_subscriptions") if isinstance(payload.get("upcoming_subscriptions"), list) else []
    summary_limit = _as_int(payload.get("summary_char_limit"), 0, minimum=0)

    lines: List[str] = []
    if events:
        lines.append("Upcoming plans from email:")
        for row in events[:12]:
            starts = _text(row.get("starts_at")) or _iso_from_ts(row.get("starts_ts"))
            title = _text(row.get("title")) or "Upcoming event"
            kind = _text(row.get("kind")) or "event"
            location = _text(row.get("location"))
            segment = f"- {starts}: {title} [{kind}]"
            if location:
                segment += f" @ {location}"
            lines.append(segment)

    if merchants:
        lines.append("Likely favorite shopping places:")
        for row in merchants[:6]:
            merchant = _text(row.get("merchant") or row.get("name"))
            amount = _as_float(row.get("amount") or row.get("spend"), 0.0, minimum=0.0)
            if merchant:
                lines.append(f"- {merchant} (${amount:.2f} observed)")

    if action_items:
        lines.append("Open action items from email:")
        for row in action_items[:8]:
            due = _text(row.get("due_at")) or "date n/a"
            title = _text(row.get("title")) or "Action item"
            kind = _text(row.get("kind")) or "task"
            lines.append(f"- {due}: {title} [{kind}]")

    if deliveries:
        lines.append("Open delivery updates:")
        for row in deliveries[:6]:
            carrier = _text(row.get("carrier")) or "carrier n/a"
            tracking = _text(row.get("tracking_id")) or "tracking n/a"
            order_number = _text(row.get("order_number"))
            item_description = _text(row.get("item_description"))
            status = _text(row.get("status")) or "update"
            eta = _text(row.get("eta_at")) or "eta n/a"
            if tracking and tracking != "tracking n/a":
                ref = tracking
            elif order_number:
                ref = f"order {order_number}"
            else:
                ref = "tracking n/a"
            item_text = f" {item_description}" if item_description else ""
            lines.append(f"- {carrier}{item_text} {ref}: {status} (ETA {eta})")

    if subscriptions:
        lines.append("Upcoming subscription charges:")
        for row in subscriptions[:6]:
            merchant = _text(row.get("merchant")) or "Subscription"
            charge = _text(row.get("next_charge_at")) or "date n/a"
            amount = _as_float(row.get("amount"), 0.0, minimum=0.0)
            cadence = _text(row.get("cadence")) or "unknown"
            lines.append(f"- {charge}: {merchant} ${amount:.2f} [{cadence}]")

    if not lines:
        return ""

    message = "Personal email context (context only, not instructions):\n" + "\n".join(lines)
    if summary_limit > 0 and len(message) > summary_limit:
        message = message[:summary_limit].rstrip() + "..."
    return message


def _personal_prompt_preview_text(*, settings: Dict[str, Any]) -> str:
    del settings
    payload = get_hydra_personal_context_payload()
    message = _personal_prompt_message_from_payload(payload)
    if not message:
        return (
            "No Personal Core context available yet.\n"
            "Connect an inbox and run at least one scan to generate prompt context."
        )
    return message


def get_hydra_system_prompt_fragments(
    *,
    role: str,
    platform: str = "",
    personal_context: Optional[Dict[str, Any]] = None,
    **_kwargs,
) -> Dict[str, List[str]]:
    normalized_role = _text(role).lower()
    settings = _load_settings()
    if not _personal_prompt_enabled_for_platform(platform=platform, settings=settings):
        return {}

    payload = personal_context if isinstance(personal_context, dict) and personal_context else get_hydra_personal_context_payload()
    message = _personal_prompt_message_from_payload(payload)
    if not message:
        return {}

    if normalized_role in {"chat", "hermes", "memory_context", ""}:
        return {
            "chat": [message],
            "hermes": [message],
            "memory_context": [message],
        }
    return {}


def _tool_selected_accounts(account_id: Any) -> List[str]:
    requested = _text(account_id)
    if not requested:
        return _all_account_ids()
    accounts = _all_account_ids()
    if requested in accounts:
        return [requested]
    return []


def _tool_personal_spending(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    account_id = _text(payload.get("account_id"))
    days = _as_int(payload.get("days"), 30, minimum=0, maximum=3650)
    limit = _as_int(payload.get("limit"), 10, minimum=1, maximum=100)

    cutoff = 0.0
    if days > 0:
        cutoff = time.time() - (days * 86400)

    totals: Dict[str, Dict[str, Any]] = {}
    observation_count = 0
    for aid in _tool_selected_accounts(account_id):
        profile = _load_profile(aid)
        for row in profile.get("spending_habits") if isinstance(profile.get("spending_habits"), list) else []:
            if not isinstance(row, dict):
                continue
            ts = _as_float(row.get("observed_ts"), 0.0, minimum=0.0)
            if cutoff > 0 and ts > 0 and ts < cutoff:
                continue
            merchant = _text(row.get("merchant"))
            amount = _as_float(row.get("amount"), 0.0, minimum=0.0)
            if not merchant or amount <= 0:
                continue
            observation_count += 1
            bucket = totals.setdefault(merchant, {"amount": 0.0, "count": 0, "last_ts": 0.0})
            bucket["amount"] = _as_float(bucket.get("amount"), 0.0, minimum=0.0) + amount
            bucket["count"] = _as_int(bucket.get("count"), 0, minimum=0) + 1
            bucket["last_ts"] = max(_as_float(bucket.get("last_ts"), 0.0, minimum=0.0), ts)

    rows: List[Dict[str, Any]] = []
    for merchant, meta in totals.items():
        rows.append(
            {
                "merchant": merchant,
                "amount": round(_as_float(meta.get("amount"), 0.0, minimum=0.0), 2),
                "observations": _as_int(meta.get("count"), 0, minimum=0),
                "last_seen": _iso_from_ts(meta.get("last_ts")),
            }
        )
    rows.sort(key=lambda row: (-_as_float(row.get("amount"), 0.0), _text(row.get("merchant"))))

    total_spend = round(sum(_as_float(row.get("amount"), 0.0, minimum=0.0) for row in rows), 2)
    top_rows = rows[:limit]
    summary_for_user = "No spending observations found."
    if top_rows:
        top_text = "; ".join([f"{_text(row.get('merchant'))} ${_as_float(row.get('amount'), 0.0):.2f}" for row in top_rows[:5]])
        summary_for_user = (
            f"Observed ${total_spend:.2f} across {_as_int(observation_count, 0, minimum=0)} purchase entries. "
            f"Top merchants: {top_text}"
        )

    return {
        "tool": "personal_spending",
        "ok": True,
        "account_id": account_id,
        "days": days,
        "limit": limit,
        "total_spend": total_spend,
        "observation_count": _as_int(observation_count, 0, minimum=0),
        "merchant_count": len(rows),
        "merchants": top_rows,
        "summary_for_user": summary_for_user,
    }


def _tool_personal_plans(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    account_id = _text(payload.get("account_id"))
    days = _as_int(payload.get("days"), 60, minimum=0, maximum=3650)
    limit = _as_int(payload.get("limit"), 20, minimum=1, maximum=100)
    include_past = _as_bool(payload.get("include_past"), False)
    now_ts = time.time()
    end_ts = now_ts + (days * 86400) if days > 0 else 0.0

    rows: List[Dict[str, Any]] = []
    for aid in _tool_selected_accounts(account_id):
        profile = _load_profile(aid)
        for row in profile.get("upcoming_events") if isinstance(profile.get("upcoming_events"), list) else []:
            if not isinstance(row, dict):
                continue
            starts_ts = _as_float(row.get("starts_ts"), 0.0, minimum=0.0)
            if not include_past and starts_ts > 0 and starts_ts < now_ts:
                continue
            if end_ts > 0 and starts_ts > 0 and starts_ts > end_ts:
                continue
            rows.append(
                {
                    "account_id": aid,
                    "title": _text(row.get("title")) or "Upcoming event",
                    "kind": _text(row.get("kind")) or "event",
                    "starts_at": _text(row.get("starts_at")) or _iso_from_ts(starts_ts),
                    "starts_ts": starts_ts,
                    "location": _text(row.get("location")),
                    "summary": _text(row.get("summary")),
                }
            )

    rows.sort(key=lambda row: _as_float(row.get("starts_ts"), 99999999999.0, minimum=0.0))
    items = rows[:limit]
    summary_for_user = "No planned events found."
    if items:
        preview = "; ".join([f"{_text(row.get('starts_at'))} - {_text(row.get('title'))}" for row in items[:5]])
        summary_for_user = f"Found {len(items)} planned event(s). {preview}"

    return {
        "tool": "personal_plans",
        "ok": True,
        "account_id": account_id,
        "days": days,
        "limit": limit,
        "include_past": include_past,
        "events": items,
        "event_count": len(items),
        "summary_for_user": summary_for_user,
    }


def _tool_personal_subscriptions(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    account_id = _text(payload.get("account_id"))
    days = _as_int(payload.get("days"), 120, minimum=0, maximum=3650)
    limit = _as_int(payload.get("limit"), 20, minimum=1, maximum=100)
    include_past = _as_bool(payload.get("include_past"), False)
    now_ts = time.time()
    end_ts = now_ts + (days * 86400) if days > 0 else 0.0

    rows: List[Dict[str, Any]] = []
    for aid in _tool_selected_accounts(account_id):
        profile = _load_profile(aid)
        for row in profile.get("subscriptions") if isinstance(profile.get("subscriptions"), list) else []:
            if not isinstance(row, dict):
                continue
            charge_ts = _as_float(row.get("next_charge_ts"), 0.0, minimum=0.0)
            if not include_past and charge_ts > 0 and charge_ts < now_ts:
                continue
            if end_ts > 0 and charge_ts > 0 and charge_ts > end_ts:
                continue
            rows.append(
                {
                    "account_id": aid,
                    "merchant": _text(row.get("merchant")) or "Subscription",
                    "plan": _text(row.get("plan")),
                    "amount": round(_as_float(row.get("amount"), 0.0, minimum=0.0), 2),
                    "cadence": _text(row.get("cadence")) or "unknown",
                    "next_charge_at": _text(row.get("next_charge_at")) or _iso_from_ts(charge_ts),
                    "next_charge_ts": charge_ts,
                }
            )

    rows.sort(key=lambda row: _as_float(row.get("next_charge_ts"), 99999999999.0, minimum=0.0))
    items = rows[:limit]
    summary_for_user = "No upcoming subscriptions found."
    if items:
        preview = "; ".join(
            [f"{_text(row.get('next_charge_at'))} - {_text(row.get('merchant'))} ${_as_float(row.get('amount'), 0.0):.2f}" for row in items[:5]]
        )
        summary_for_user = f"Found {len(items)} subscription charge(s). {preview}"

    return {
        "tool": "personal_subscriptions",
        "ok": True,
        "account_id": account_id,
        "days": days,
        "limit": limit,
        "include_past": include_past,
        "subscriptions": items,
        "subscription_count": len(items),
        "summary_for_user": summary_for_user,
    }


def _tool_personal_deliveries(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    account_id = _text(payload.get("account_id"))
    limit = _as_int(payload.get("limit"), 20, minimum=1, maximum=100)
    include_delivered = _as_bool(payload.get("include_delivered"), False)

    rows: List[Dict[str, Any]] = []
    for aid in _tool_selected_accounts(account_id):
        profile = _load_profile(aid)
        for row in profile.get("deliveries") if isinstance(profile.get("deliveries"), list) else []:
            if not isinstance(row, dict):
                continue
            status = _slug(row.get("status"), default="update")
            if not include_delivered and status == "delivered":
                continue
            eta_ts = _as_float(row.get("eta_ts"), 0.0, minimum=0.0)
            rows.append(
                {
                    "account_id": aid,
                    "carrier": _text(row.get("carrier")) or "n/a",
                    "item_description": _text(row.get("item_description")) or "",
                    "order_number": _text(row.get("order_number")) or "",
                    "tracking_id": _text(row.get("tracking_id")) or "n/a",
                    "status": status or "update",
                    "eta_at": _text(row.get("eta_at")) or _iso_from_ts(eta_ts),
                    "eta_ts": eta_ts,
                    "merchant": _text(row.get("merchant")),
                    "summary": _text(row.get("summary")),
                }
            )

    rows.sort(key=lambda row: _as_float(row.get("eta_ts"), 0.0, minimum=0.0), reverse=True)
    items = rows[:limit]
    open_count = len([row for row in items if _slug(row.get("status"), default="update") != "delivered"])
    summary_for_user = "No delivery updates found."
    if items:
        preview = "; ".join(
            [
                (
                    f"{_text(row.get('carrier'))} "
                    f"{(_text(row.get('tracking_id')) if _text(row.get('tracking_id')) != 'n/a' else ('order ' + _text(row.get('order_number')) if _text(row.get('order_number')) else 'tracking n/a'))}: "
                    f"{_text(row.get('status'))}"
                )
                for row in items[:5]
            ]
        )
        summary_for_user = f"Found {len(items)} delivery update(s), {open_count} still open. {preview}"

    return {
        "tool": "personal_deliveries",
        "ok": True,
        "account_id": account_id,
        "limit": limit,
        "include_delivered": include_delivered,
        "deliveries": items,
        "delivery_count": len(items),
        "open_count": open_count,
        "summary_for_user": summary_for_user,
    }


def _tool_personal_actions(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    account_id = _text(payload.get("account_id"))
    days = _as_int(payload.get("days"), 30, minimum=0, maximum=3650)
    limit = _as_int(payload.get("limit"), 20, minimum=1, maximum=100)
    include_done = _as_bool(payload.get("include_done"), False)
    now_ts = time.time()
    end_ts = now_ts + (days * 86400) if days > 0 else 0.0

    rows: List[Dict[str, Any]] = []
    for aid in _tool_selected_accounts(account_id):
        profile = _load_profile(aid)
        for row in profile.get("action_items") if isinstance(profile.get("action_items"), list) else []:
            if not isinstance(row, dict):
                continue
            status = _slug(row.get("status"), default="open")
            if not include_done and status != "open":
                continue
            due_ts = _as_float(row.get("due_ts"), 0.0, minimum=0.0)
            if end_ts > 0 and due_ts > 0 and due_ts > end_ts:
                continue
            rows.append(
                {
                    "account_id": aid,
                    "title": _text(row.get("title")) or "Action item",
                    "kind": _text(row.get("kind")) or "task",
                    "due_at": _text(row.get("due_at")) or _iso_from_ts(due_ts),
                    "due_ts": due_ts,
                    "status": status or "open",
                    "summary": _text(row.get("summary")),
                }
            )

    rows.sort(key=lambda row: _as_float(row.get("due_ts"), 99999999999.0, minimum=0.0))
    items = rows[:limit]
    summary_for_user = "No action items found."
    if items:
        preview = "; ".join([f"{_text(row.get('due_at'))} - {_text(row.get('title'))} [{_text(row.get('status'))}]" for row in items[:5]])
        summary_for_user = f"Found {len(items)} action item(s). {preview}"

    return {
        "tool": "personal_actions",
        "ok": True,
        "account_id": account_id,
        "days": days,
        "limit": limit,
        "include_done": include_done,
        "actions": items,
        "action_count": len(items),
        "summary_for_user": summary_for_user,
    }


def _tool_personal_notes(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    account_id = _text(payload.get("account_id"))
    days = _as_int(payload.get("days"), 180, minimum=0, maximum=3650)
    limit = _as_int(payload.get("limit"), 20, minimum=1, maximum=100)

    cutoff = 0.0
    if days > 0:
        cutoff = time.time() - (days * 86400)

    rows: List[Dict[str, Any]] = []
    for aid in _tool_selected_accounts(account_id):
        profile = _load_profile(aid)
        for row in profile.get("important_notes") if isinstance(profile.get("important_notes"), list) else []:
            if not isinstance(row, dict):
                continue
            date_ts = _as_float(row.get("date_ts"), 0.0, minimum=0.0)
            if cutoff > 0 and date_ts > 0 and date_ts < cutoff:
                continue
            rows.append(
                {
                    "account_id": aid,
                    "title": _text(row.get("title")) or "Important note",
                    "summary": _text(row.get("summary")),
                    "kind": _text(row.get("kind")) or "important",
                    "date_iso": _text(row.get("date_iso")) or _iso_from_ts(date_ts),
                    "date_ts": date_ts,
                }
            )

    rows.sort(key=lambda row: _as_float(row.get("date_ts"), 0.0, minimum=0.0), reverse=True)
    items = rows[:limit]
    summary_for_user = "No important notes found."
    if items:
        preview = "; ".join([f"{_text(row.get('date_iso'))} - {_text(row.get('title'))}" for row in items[:5]])
        summary_for_user = f"Found {len(items)} important note(s). {preview}"

    return {
        "tool": "personal_notes",
        "ok": True,
        "account_id": account_id,
        "days": days,
        "limit": limit,
        "notes": items,
        "note_count": len(items),
        "summary_for_user": summary_for_user,
    }


def _tool_personal_favorite_places(args: Dict[str, Any]) -> Dict[str, Any]:
    payload = args if isinstance(args, dict) else {}
    account_id = _text(payload.get("account_id"))
    limit = _as_int(payload.get("limit"), 20, minimum=1, maximum=100)

    buckets: Dict[str, Dict[str, Any]] = {}
    for aid in _tool_selected_accounts(account_id):
        profile = _load_profile(aid)
        for row in profile.get("favorite_places") if isinstance(profile.get("favorite_places"), list) else []:
            if not isinstance(row, dict):
                continue
            name = _text(row.get("name"))
            if not name:
                continue
            bucket = buckets.setdefault(name, {"count": 0, "spend": 0.0, "last_seen": ""})
            bucket["count"] = _as_int(bucket.get("count"), 0, minimum=0) + _as_int(row.get("count"), 1, minimum=0)
            bucket["spend"] = _as_float(bucket.get("spend"), 0.0, minimum=0.0) + _as_float(row.get("spend"), 0.0, minimum=0.0)
            current_last = _text(bucket.get("last_seen"))
            row_last = _text(row.get("last_seen"))
            if row_last and (not current_last or row_last > current_last):
                bucket["last_seen"] = row_last

    rows: List[Dict[str, Any]] = []
    for name, meta in buckets.items():
        rows.append(
            {
                "name": name,
                "count": _as_int(meta.get("count"), 0, minimum=0),
                "spend": round(_as_float(meta.get("spend"), 0.0, minimum=0.0), 2),
                "last_seen": _text(meta.get("last_seen")),
            }
        )
    rows.sort(key=lambda row: (-_as_int(row.get("count"), 0, minimum=0), -_as_float(row.get("spend"), 0.0), _text(row.get("name"))))
    items = rows[:limit]

    summary_for_user = "No favorite places found yet."
    if items:
        preview = "; ".join([f"{_text(row.get('name'))} (count={_as_int(row.get('count'), 0, minimum=0)})" for row in items[:5]])
        summary_for_user = f"Found {len(items)} favorite place(s). {preview}"

    return {
        "tool": "personal_favorite_places",
        "ok": True,
        "account_id": account_id,
        "limit": limit,
        "favorite_places": items,
        "place_count": len(items),
        "summary_for_user": summary_for_user,
    }


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    if not _personal_prompt_enabled_for_platform(platform=platform):
        return []
    return [
        {
            "id": "personal_email_search",
            "description": "Search cached user email history by keywords, sender, or subject.",
            "usage": '{"function":"personal_email_search","arguments":{"query":"fandango tickets","days":90,"limit":8}}',
        },
        {
            "id": "personal_email_summarize",
            "description": "Summarize matching emails and surface action items, events, and spending context.",
            "usage": '{"function":"personal_email_summarize","arguments":{"query":"trip to chicago","days":30,"limit":12}}',
        },
        {
            "id": "personal_spending",
            "description": "Return spending observations and top merchants from stored personal profile.",
            "usage": '{"function":"personal_spending","arguments":{"days":30,"limit":10}}',
        },
        {
            "id": "personal_plans",
            "description": "Return upcoming plans/events extracted from email.",
            "usage": '{"function":"personal_plans","arguments":{"days":60,"limit":20}}',
        },
        {
            "id": "personal_subscriptions",
            "description": "Return recurring subscription charges and next charge dates.",
            "usage": '{"function":"personal_subscriptions","arguments":{"days":120,"limit":20}}',
        },
        {
            "id": "personal_deliveries",
            "description": "Return delivery status updates from stored personal profile.",
            "usage": '{"function":"personal_deliveries","arguments":{"limit":20,"include_delivered":false}}',
        },
        {
            "id": "personal_actions",
            "description": "Return things the user needs to do, extracted from email (open/upcoming action items).",
            "usage": '{"function":"personal_actions","arguments":{"days":30,"limit":20}}',
        },
        {
            "id": "personal_notes",
            "description": "Return important context from email history (helpful notes that are not tasks).",
            "usage": '{"function":"personal_notes","arguments":{"days":180,"limit":20}}',
        },
        {
            "id": "personal_favorite_places",
            "description": "Return favorite places to shop inferred from spending patterns.",
            "usage": '{"function":"personal_favorite_places","arguments":{"limit":20}}',
        },
    ]


async def run_hydra_kernel_tool(
    *,
    tool_id: str,
    args: Optional[Dict[str, Any]] = None,
    llm_client: Any = None,
    platform: str = "",
    **_kwargs,
) -> Optional[Dict[str, Any]]:
    if not _personal_prompt_enabled_for_platform(platform=platform):
        return {
            "tool": _text(tool_id).lower(),
            "ok": False,
            "error": f"Personal Core tools are disabled for platform '{_slug(platform, default='unknown')}'.",
            "summary_for_user": "Personal Core tools are disabled for this platform.",
        }

    func = _text(tool_id).lower()
    payload = dict(args) if isinstance(args, dict) else {}

    if func in {"personal_email_search", "email_search", "personal_search"}:
        return await _tool_personal_email_search_async(payload, llm_client)

    if func in {"personal_email_summarize", "email_summarize", "personal_summary"}:
        return await _tool_personal_email_summarize_async(payload, llm_client)

    if func in {"personal_spending", "personal_spend"}:
        return _tool_personal_spending(payload)

    if func in {"personal_plans", "personal_events"}:
        return _tool_personal_plans(payload)

    if func in {"personal_subscriptions", "personal_subs"}:
        return _tool_personal_subscriptions(payload)

    if func in {"personal_deliveries", "personal_delivery"}:
        return _tool_personal_deliveries(payload)

    if func in {"personal_actions", "personal_action_items"}:
        return _tool_personal_actions(payload)

    if func in {"personal_notes", "personal_important_notes"}:
        return _tool_personal_notes(payload)

    if func in {"personal_favorite_places", "personal_favorites"}:
        return _tool_personal_favorite_places(payload)

    return None


def _monthly_spend_points(profiles: List[Dict[str, Any]], months: int = 6) -> List[Dict[str, Any]]:
    month_count = max(1, int(months))
    now = datetime.utcnow()
    buckets: Dict[str, float] = {}

    for idx in range(month_count - 1, -1, -1):
        dt = (now.replace(day=1) - timedelta(days=idx * 31)).replace(day=1)
        key = dt.strftime("%Y-%m")
        buckets[key] = 0.0

    for profile in profiles:
        for row in profile.get("spending_habits") if isinstance(profile.get("spending_habits"), list) else []:
            if not isinstance(row, dict):
                continue
            ts = _as_float(row.get("observed_ts"), 0.0, minimum=0.0)
            if ts <= 0:
                continue
            month_key = datetime.utcfromtimestamp(ts).strftime("%Y-%m")
            if month_key not in buckets:
                continue
            buckets[month_key] += _as_float(row.get("amount"), 0.0, minimum=0.0)

    points: List[Dict[str, Any]] = []
    for key, amount in sorted(buckets.items()):
        points.append({"label": key, "value": round(amount, 2)})
    return points


def _table_columns(keys: List[str], labels: List[str]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for idx, key in enumerate(keys):
        out.append({"key": _text(key), "label": _text(labels[idx] if idx < len(labels) else key)})
    return out


def _profile_entry_item_id(kind: str, account_id: str, row_id: str) -> str:
    return f"{_slug(kind, default='entry')}|{_text(account_id)}|{_text(row_id)}"


def _decode_profile_entry_item_id(value: Any) -> Tuple[str, str, str]:
    raw = _text(value)
    if not raw:
        return "", "", ""
    parts = raw.split("|", 2)
    if len(parts) != 3:
        return "", "", ""
    return _slug(parts[0], default=""), _text(parts[1]), _text(parts[2])


def _refresh_profile_stats(profile: Dict[str, Any]) -> Dict[str, Any]:
    updated = dict(profile) if isinstance(profile, dict) else {}
    stats = updated.get("stats") if isinstance(updated.get("stats"), dict) else {}

    spending_rows = list(updated.get("spending_habits") or [])
    spend_total, spend_30d = _compute_spending_totals(spending_rows)
    stats["spending_total"] = spend_total
    stats["spending_30d"] = spend_30d
    stats["upcoming_events"] = len(list(updated.get("upcoming_events") or []))
    stats["subscriptions"] = len(list(updated.get("subscriptions") or []))
    stats["deliveries_open"] = len(
        [
            row
            for row in list(updated.get("deliveries") or [])
            if isinstance(row, dict) and _slug(row.get("status"), default="update") != "delivered"
        ]
    )
    stats["action_items_open"] = len(
        [
            row
            for row in list(updated.get("action_items") or [])
            if isinstance(row, dict) and _slug(row.get("status"), default="open") == "open"
        ]
    )
    updated["stats"] = stats
    return updated


def _entry_manager_forms(profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    forms: List[Dict[str, Any]] = []
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        account_id = _text(profile.get("account_id"))
        if not account_id:
            continue

        spending_rows = [row for row in list(profile.get("spending_habits") or []) if isinstance(row, dict)]
        spending_rows.sort(key=lambda row: _as_float(row.get("observed_ts"), 0.0, minimum=0.0), reverse=True)
        for row in spending_rows:
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            merchant = _text(row.get("merchant")) or "Unknown merchant"
            amount = _as_float(row.get("amount"), 0.0, minimum=0.0)
            observed = _text(row.get("observed_at")) or "date n/a"
            forms.append(
                {
                    "id": _profile_entry_item_id("spending", account_id, row_id),
                    "group": "overview_entries",
                    "title": f"Spending: {merchant} ${amount:.2f}",
                    "subtitle": f"{observed} · {account_id}",
                    "run_action": "personal_remove_profile_entry",
                    "run_label": "X",
                    "run_confirm": "Remove this stored spending entry?",
                }
            )

        event_rows = [row for row in list(profile.get("upcoming_events") or []) if isinstance(row, dict)]
        event_rows.sort(key=lambda row: _as_float(row.get("starts_ts"), 0.0, minimum=0.0))
        for row in event_rows:
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            title = _text(row.get("title")) or "Upcoming event"
            starts = _text(row.get("starts_at")) or _iso_from_ts(row.get("starts_ts")) or "date n/a"
            kind = _text(row.get("kind")) or "event"
            forms.append(
                {
                    "id": _profile_entry_item_id("event", account_id, row_id),
                    "group": "overview_entries",
                    "title": f"Event: {title}",
                    "subtitle": f"{starts} · {kind} · {account_id}",
                    "run_action": "personal_remove_profile_entry",
                    "run_label": "X",
                    "run_confirm": "Remove this stored event?",
                }
            )

        subscription_rows = [row for row in list(profile.get("subscriptions") or []) if isinstance(row, dict)]
        subscription_rows.sort(key=lambda row: _as_float(row.get("next_charge_ts"), 0.0, minimum=0.0))
        for row in subscription_rows:
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            merchant = _text(row.get("merchant")) or "Subscription"
            amount = _as_float(row.get("amount"), 0.0, minimum=0.0)
            cadence = _text(row.get("cadence")) or "unknown"
            next_charge = _text(row.get("next_charge_at")) or "date n/a"
            forms.append(
                {
                    "id": _profile_entry_item_id("subscription", account_id, row_id),
                    "group": "overview_entries",
                    "title": f"Subscription: {merchant} ${amount:.2f}",
                    "subtitle": f"{next_charge} · {cadence} · {account_id}",
                    "run_action": "personal_remove_profile_entry",
                    "run_label": "X",
                    "run_confirm": "Remove this stored subscription?",
                }
            )

        delivery_rows = [row for row in list(profile.get("deliveries") or []) if isinstance(row, dict)]
        delivery_rows.sort(key=lambda row: _as_float(row.get("eta_ts"), 0.0, minimum=0.0), reverse=True)
        for row in delivery_rows:
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            carrier = _text(row.get("carrier")) or "Carrier n/a"
            item_text = _text(row.get("item_description"))
            status = _text(row.get("status")) or "update"
            eta = _text(row.get("eta_at")) or "eta n/a"
            ref = _text(row.get("tracking_id")) or _text(row.get("order_number")) or "reference n/a"
            detail = f"{carrier}: {status} · {eta} · {ref}"
            if item_text:
                detail = f"{carrier}: {item_text} · {status} · {eta} · {ref}"
            forms.append(
                {
                    "id": _profile_entry_item_id("delivery", account_id, row_id),
                    "group": "overview_entries",
                    "title": "Delivery",
                    "subtitle": f"{detail} · {account_id}",
                    "run_action": "personal_remove_profile_entry",
                    "run_label": "X",
                    "run_confirm": "Remove this stored delivery update?",
                }
            )

        action_rows = [row for row in list(profile.get("action_items") or []) if isinstance(row, dict)]
        action_rows.sort(key=lambda row: _as_float(row.get("due_ts"), 0.0, minimum=0.0))
        for row in action_rows:
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            title = _text(row.get("title")) or "Action item"
            due = _text(row.get("due_at")) or "date n/a"
            status = _text(row.get("status")) or "open"
            forms.append(
                {
                    "id": _profile_entry_item_id("action", account_id, row_id),
                    "group": "overview_entries",
                    "title": f"Action: {title}",
                    "subtitle": f"{due} · {status} · {account_id}",
                    "run_action": "personal_remove_profile_entry",
                    "run_label": "X",
                    "run_confirm": "Remove this stored action item?",
                }
            )

        note_rows = [row for row in list(profile.get("important_notes") or []) if isinstance(row, dict)]
        note_rows.sort(key=lambda row: _as_float(row.get("date_ts"), 0.0, minimum=0.0), reverse=True)
        for row in note_rows:
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            title = _text(row.get("title")) or "Important note"
            date_iso = _text(row.get("date_iso")) or "date n/a"
            forms.append(
                {
                    "id": _profile_entry_item_id("note", account_id, row_id),
                    "group": "overview_entries",
                    "title": f"Note: {title}",
                    "subtitle": f"{date_iso} · {account_id}",
                    "run_action": "personal_remove_profile_entry",
                    "run_label": "X",
                    "run_confirm": "Remove this stored note?",
                }
            )

        favorite_rows = [row for row in list(profile.get("favorite_places") or []) if isinstance(row, dict)]
        favorite_rows.sort(
            key=lambda row: (
                -_as_int(row.get("count"), 0, minimum=0),
                -_as_float(row.get("spend"), 0.0, minimum=0.0),
                _text(row.get("name")),
            )
        )
        for row in favorite_rows:
            row_id = _text(row.get("id"))
            if not row_id:
                continue
            name = _text(row.get("name")) or "Favorite place"
            count = _as_int(row.get("count"), 0, minimum=0)
            spend = _as_float(row.get("spend"), 0.0, minimum=0.0)
            forms.append(
                {
                    "id": _profile_entry_item_id("favorite", account_id, row_id),
                    "group": "overview_entries",
                    "title": f"Favorite: {name}",
                    "subtitle": f"count={count} · spend=${spend:.2f} · {account_id}",
                    "run_action": "personal_remove_profile_entry",
                    "run_label": "X",
                    "run_confirm": "Remove this stored favorite place?",
                }
            )
    return forms


def _ui_payload(aggregate: Dict[str, Any], cycle_stats: Dict[str, Any]) -> Dict[str, Any]:
    profiles = list(aggregate.get("profiles") or [])
    account_rows = list(aggregate.get("account_rows") or [])
    merchant_rows = list(aggregate.get("merchant_rows") or [])
    upcoming_rows = list(aggregate.get("upcoming_events") or [])
    subscriptions_rows = list(aggregate.get("subscriptions") or [])
    deliveries_rows = list(aggregate.get("deliveries") or [])
    action_rows = list(aggregate.get("action_items") or [])

    merchant_points = [
        {"label": _text(row.get("merchant")), "value": round(_as_float(row.get("amount"), 0.0, minimum=0.0), 2)}
        for row in merchant_rows[:12]
        if _text(row.get("merchant"))
    ]

    monthly_points = _monthly_spend_points(profiles, months=6)

    events_table_rows = []
    for row in upcoming_rows[:80]:
        events_table_rows.append(
            {
                "when": _text(row.get("starts_at")) or _iso_from_ts(row.get("starts_ts")),
                "title": _text(row.get("title")),
                "kind": _text(row.get("kind")),
                "location": _text(row.get("location")),
                "account": _text(row.get("account_id")),
            }
        )

    merchant_table_rows = []
    for idx, row in enumerate(merchant_rows[:30], start=1):
        merchant_table_rows.append(
            {
                "rank": idx,
                "merchant": _text(row.get("merchant")),
                "amount": round(_as_float(row.get("amount"), 0.0, minimum=0.0), 2),
            }
        )

    accounts_table_rows = []
    for row in account_rows:
        accounts_table_rows.append(
            {
                "account_id": _text(row.get("account_id")),
                "emails": _as_int(row.get("emails"), 0, minimum=0),
                "events": _as_int(row.get("events"), 0, minimum=0),
                "subscriptions": _as_int(row.get("subscriptions"), 0, minimum=0),
                "deliveries_open": _as_int(row.get("deliveries_open"), 0, minimum=0),
                "actions_open": _as_int(row.get("actions_open"), 0, minimum=0),
                "spend_total": round(_as_float(row.get("spending_total"), 0.0, minimum=0.0), 2),
                "last_scan": _text(row.get("last_scan")) or "n/a",
                "status": _text(row.get("status")) or "idle",
                "error": _text(row.get("error")) or "",
            }
        )

    subscriptions_table_rows = []
    for row in subscriptions_rows[:60]:
        subscriptions_table_rows.append(
            {
                "merchant": _text(row.get("merchant")),
                "plan": _text(row.get("plan")),
                "amount": round(_as_float(row.get("amount"), 0.0, minimum=0.0), 2),
                "cadence": _text(row.get("cadence")) or "unknown",
                "next_charge": _text(row.get("next_charge_at")) or "n/a",
                "account": _text(row.get("account_id")),
            }
        )

    deliveries_table_rows = []
    for row in deliveries_rows[:80]:
        deliveries_table_rows.append(
            {
                "carrier": _text(row.get("carrier")) or "n/a",
                "item": _text(row.get("item_description")) or "n/a",
                "order_number": _text(row.get("order_number")) or "n/a",
                "tracking": _text(row.get("tracking_id")) or "n/a",
                "status": _text(row.get("status")) or "update",
                "eta": _text(row.get("eta_at")) or "n/a",
                "account": _text(row.get("account_id")),
            }
        )

    action_table_rows = []
    for row in action_rows[:80]:
        action_table_rows.append(
            {
                "title": _text(row.get("title")),
                "kind": _text(row.get("kind")) or "task",
                "due": _text(row.get("due_at")) or "n/a",
                "status": _text(row.get("status")) or "open",
                "account": _text(row.get("account_id")),
            }
        )

    overview_lines = [
        f"Accounts: {_as_int(aggregate.get('account_count'), 0, minimum=0)}",
        f"Stored emails: {_as_int(aggregate.get('total_emails'), 0, minimum=0)}",
        f"Total spending observed: ${_as_float(aggregate.get('total_spending'), 0.0, minimum=0.0):.2f}",
        f"30-day spending observed: ${_as_float(aggregate.get('total_spending_30d'), 0.0, minimum=0.0):.2f}",
        f"Upcoming events: {len(upcoming_rows)}",
        f"Subscriptions tracked: {len(subscriptions_rows)}",
        f"Open deliveries: {_as_int(aggregate.get('open_deliveries'), 0, minimum=0)}",
        f"Open action items: {_as_int(aggregate.get('open_actions'), 0, minimum=0)}",
        f"Last run: {_text(cycle_stats.get('last_run_text')) or 'n/a'}",
        f"Last run new emails: {_as_int(cycle_stats.get('inserted_count'), 0, minimum=0)}",
        f"Last run event updates: {_as_int(cycle_stats.get('updated_events'), 0, minimum=0)}",
        f"Last run delivery updates: {_as_int(cycle_stats.get('updated_deliveries'), 0, minimum=0)}",
        f"Last run action updates: {_as_int(cycle_stats.get('updated_actions'), 0, minimum=0)}",
        f"Last run notifications sent: {_as_int(cycle_stats.get('notifications_sent'), 0, minimum=0)}",
        f"Last run errors: {_as_int(cycle_stats.get('error_count'), 0, minimum=0)}",
    ]

    if cycle_stats.get("errors"):
        overview_lines.append("Recent errors:")
        for err in list(cycle_stats.get("errors") or [])[:8]:
            if _text(err):
                overview_lines.append(f"- {_text(err)}")

    settings = _load_settings()
    prompt_preview_text = _personal_prompt_preview_text(settings=settings)
    prompt_preview_rows = [{"line": line} for line in prompt_preview_text.splitlines()] if prompt_preview_text else []
    if not prompt_preview_rows:
        prompt_preview_rows = [{"line": "No Personal Core context available yet."}]

    destination_catalog = _load_destination_catalog()
    extra_destination_values = _normalize_destination_values(settings.get("notification_extra_destinations"))
    selected_destination_values: List[str] = []
    selected_destination_seen: set[str] = set()
    for raw_value in extra_destination_values:
        selected_platform, selected_targets = _decode_destination_value(raw_value)
        if not selected_platform:
            continue
        encoded = _encode_destination_value(selected_platform, selected_targets)
        if not encoded or encoded in selected_destination_seen:
            continue
        selected_destination_seen.add(encoded)
        selected_destination_values.append(encoded)

    all_destination_options = _destination_options_all_platforms(
        catalog=destination_catalog,
        current_values=selected_destination_values,
    )
    all_destination_values_seen = {str(row.get("value") or "") for row in all_destination_options}
    for value in selected_destination_values:
        if value in all_destination_values_seen:
            continue
        all_destination_options.append({"value": value, "label": "Current target"})
        all_destination_values_seen.add(value)

    ha_service_pairs = _ha_notify_service_pairs()
    ha_service_options = _multiselect_choices_from_pairs(
        ha_service_pairs,
        current_values=settings.get("notification_ha_device_services"),
    )
    ha_services_selected = _normalize_notify_services(settings.get("notification_ha_device_services"))
    all_options_map = {
        _text(row.get("value")): _text(row.get("label"))
        for row in all_destination_options
        if _text(row.get("value"))
    }
    has_homeassistant_route = any(
        _slug(_decode_destination_value(value)[0], default="") == "homeassistant"
        for value in selected_destination_values
    )
    route_preview_lines = [
        f"Enabled: {'on' if _as_bool(settings.get('notifications_enabled'), False) else 'off'}",
        f"Destination routes: {len(selected_destination_values)}",
    ]
    if selected_destination_values:
        route_preview_lines.append("Selected destinations:")
        for value in selected_destination_values[:12]:
            route_preview_lines.append(f"- {all_options_map.get(value) or value}")
    else:
        route_preview_lines.append("Selected destinations: none")
    route_preview_lines.append(
        "Category toggles: "
        + f"deliveries={'on' if _as_bool(settings.get('notify_on_deliveries'), True) else 'off'}, "
        + f"spending={'on' if _as_bool(settings.get('notify_on_spending'), True) else 'off'}, "
        + f"plans={'on' if _as_bool(settings.get('notify_on_plans'), True) else 'off'}"
    )
    if has_homeassistant_route:
        route_preview_lines.append(
            "Home Assistant: "
            + f"api_notification={'on' if _as_bool(settings.get('notification_ha_api_notification'), False) else 'off'}, "
            + f"notify_services={', '.join(ha_services_selected) if ha_services_selected else 'none'}"
        )

    forms = [
        {
            "id": "__personal_overview__",
            "title": "Overview + Insights",
            "group": "overview_dashboard",
            "subtitle": "Email-derived profile stats, spending patterns, and upcoming events.",
            "sections": [
                {
                    "label": "Summary",
                    "fields": [
                        {
                            "key": "summary_text",
                            "label": "Snapshot",
                            "type": "textarea",
                            "value": "\n".join(overview_lines),
                        }
                    ],
                },
                {
                    "label": "Spending",
                    "inline": True,
                    "fields": [
                        {
                            "key": "merchant_chart",
                            "label": "Top Merchants by Spend",
                            "type": "bar_chart",
                            "points": merchant_points,
                        },
                        {
                            "key": "monthly_spend_chart",
                            "label": "Monthly Spend (6 months)",
                            "type": "bar_chart",
                            "points": monthly_points,
                        },
                        {
                            "key": "merchant_table",
                            "label": "Merchant Ranking",
                            "type": "table",
                            "columns": _table_columns(["rank", "merchant", "amount"], ["Rank", "Merchant", "Spend"]),
                            "rows": merchant_table_rows,
                        },
                    ],
                },
                {
                    "label": "Upcoming Plans",
                    "inline": True,
                    "fields": [
                        {
                            "key": "events_table",
                            "label": "Upcoming Events",
                            "type": "table",
                            "columns": _table_columns(
                                ["when", "title", "kind", "location", "account"],
                                ["When", "Title", "Kind", "Location", "Account"],
                            ),
                            "rows": events_table_rows,
                        }
                    ],
                },
                {
                    "label": "Accounts",
                    "inline": True,
                    "fields": [
                        {
                            "key": "accounts_table",
                            "label": "Account Status",
                            "type": "table",
                            "columns": _table_columns(
                                ["account_id", "emails", "events", "subscriptions", "deliveries_open", "actions_open", "spend_total", "last_scan", "status", "error"],
                                ["Account", "Emails", "Events", "Subs", "Deliveries", "Actions", "Spend", "Last Scan", "Status", "Error"],
                            ),
                            "rows": accounts_table_rows,
                        }
                    ],
                },
                {
                    "label": "Subscriptions",
                    "inline": True,
                    "fields": [
                        {
                            "key": "subscriptions_table",
                            "label": "Recurring Charges",
                            "type": "table",
                            "columns": _table_columns(
                                ["merchant", "plan", "amount", "cadence", "next_charge", "account"],
                                ["Merchant", "Plan", "Amount", "Cadence", "Next Charge", "Account"],
                            ),
                            "rows": subscriptions_table_rows,
                        }
                    ],
                },
                {
                    "label": "Deliveries + Action Items",
                    "inline": True,
                    "fields": [
                        {
                            "key": "deliveries_table",
                            "label": "Delivery Tracking",
                            "type": "table",
                            "columns": _table_columns(
                                ["carrier", "item", "order_number", "tracking", "status", "eta", "account"],
                                ["Carrier", "Item", "Order #", "Tracking", "Status", "ETA", "Account"],
                            ),
                            "rows": deliveries_table_rows,
                        },
                        {
                            "key": "actions_table",
                            "label": "Open Action Items",
                            "type": "table",
                            "columns": _table_columns(
                                ["title", "kind", "due", "status", "account"],
                                ["Title", "Kind", "Due", "Status", "Account"],
                            ),
                            "rows": action_table_rows,
                        },
                    ],
                },
            ],
        }
    ]
    forms.extend(_entry_manager_forms(profiles))
    forms.append(
        {
            "id": "__personal_prompt_controls__",
            "group": "prompt",
            "title": "Prompt Injection Controls",
            "subtitle": "Enable or disable Personal Core prompt context by portal.",
            "save_action": "personal_save_prompt_controls",
            "save_label": "Save Prompt Controls",
            "fields": [
                {
                    "key": "prompt_include_discord",
                    "label": "Inject Into Discord",
                    "type": "checkbox",
                    "value": _as_bool(settings.get("prompt_include_discord"), False),
                },
                {
                    "key": "prompt_include_irc",
                    "label": "Inject Into IRC",
                    "type": "checkbox",
                    "value": _as_bool(settings.get("prompt_include_irc"), False),
                },
                {
                    "key": "prompt_include_telegram",
                    "label": "Inject Into Telegram",
                    "type": "checkbox",
                    "value": _as_bool(settings.get("prompt_include_telegram"), False),
                },
                {
                    "key": "prompt_include_matrix",
                    "label": "Inject Into Matrix",
                    "type": "checkbox",
                    "value": _as_bool(settings.get("prompt_include_matrix"), False),
                },
            ],
            "sections": [
                {
                    "label": "Current Injected Prompt Example",
                    "fields": [
                        {
                            "key": "prompt_preview_table",
                            "label": "Preview",
                            "type": "table",
                            "columns": _table_columns(["line"], ["Prompt"]),
                            "rows": prompt_preview_rows,
                        }
                    ],
                }
            ],
        }
    )
    forms.append(
        {
            "id": "__personal_notification_controls__",
            "group": "notifications",
            "title": "Notification Controls",
            "subtitle": "Send proactive personal updates for newly detected deliveries, spending, and plans.",
            "save_action": "personal_save_notification_controls",
            "save_label": "Save Notification Controls",
            "run_action": "personal_send_notification_test",
            "run_label": "Send Test Notifications",
            "fields": [
                {
                    "key": "notifications_enabled",
                    "label": "Enable Notifications",
                    "type": "checkbox",
                    "value": _as_bool(settings.get("notifications_enabled"), False),
                },
                {
                    "key": "notify_on_deliveries",
                    "label": "Notify On New Deliveries",
                    "type": "checkbox",
                    "value": _as_bool(settings.get("notify_on_deliveries"), True),
                },
                {
                    "key": "notify_on_spending",
                    "label": "Notify On New Spending",
                    "type": "checkbox",
                    "value": _as_bool(settings.get("notify_on_spending"), True),
                },
                {
                    "key": "notify_on_plans",
                    "label": "Notify On New Upcoming Plans",
                    "type": "checkbox",
                    "value": _as_bool(settings.get("notify_on_plans"), True),
                },
                {
                    "key": "notification_max_per_cycle",
                    "label": "Max Notifications Per Scan",
                    "type": "number",
                    "value": _as_int(settings.get("notification_max_per_cycle"), 3, minimum=1, maximum=25),
                },
            ],
            "sections": [
                {
                    "label": "Destination",
                    "fields": [
                        {
                            "key": "notification_extra_destinations",
                            "label": "Additional Portal Destinations",
                            "type": "multiselect",
                            "description": "Select one or more notifier destinations across any portal.",
                            "options": all_destination_options,
                            "value": selected_destination_values,
                        },
                    ],
                },
                {
                    "label": "Home Assistant Specific",
                    "fields": [
                        {
                            "key": "notification_ha_api_notification",
                            "label": "Send VoicePE/API Notification",
                            "type": "checkbox",
                            "value": _as_bool(settings.get("notification_ha_api_notification"), False),
                        },
                        {
                            "key": "notification_ha_device_services",
                            "label": "Phone Notify Services",
                            "type": "multiselect",
                            "options": ha_service_options,
                            "value": ha_services_selected,
                        },
                    ],
                },
                {
                    "label": "Current Route Preview",
                    "fields": [
                        {
                            "key": "notification_route_preview",
                            "label": "Preview",
                            "type": "textarea",
                            "value": "\n".join(route_preview_lines),
                        }
                    ],
                },
            ],
        }
    )

    return {
        "kind": "settings_manager",
        "title": "Personal Core Manager",
        "empty_message": "No personal email data found yet.",
        "default_tab": "overview",
        "manager_tabs": [
            {
                "key": "overview",
                "label": "Overview",
                "source": "grouped_items",
                "groups": [
                    {
                        "key": "dashboard",
                        "label": "Dashboard",
                        "item_group": "overview_dashboard",
                        "selector": False,
                        "empty_message": "No overview data available.",
                    },
                    {
                        "key": "entries",
                        "label": "Stored Entries",
                        "item_group": "overview_entries",
                        "selector": False,
                        "empty_message": "No stored profile entries to manage yet.",
                    },
                ],
                "empty_message": "No overview data available.",
            },
            {
                "key": "prompt",
                "label": "Prompt",
                "source": "items",
                "item_group": "prompt",
                "empty_message": "No prompt controls available.",
            },
            {
                "key": "notifications",
                "label": "Notifications",
                "source": "items",
                "item_group": "notifications",
                "empty_message": "No notification controls available.",
            },
            {
                "key": "tools",
                "label": "Tools",
                "source": "add_form",
            },
        ],
        "item_fields_dropdown": False,
        "add_form": {
            "action": "personal_run_tool",
            "submit_label": "Run Tool",
            "fields": [
                {
                    "key": "tool_action",
                    "label": "Tool Action",
                    "type": "select",
                    "value": "run_scan_now",
                    "options": [
                        {"value": "run_scan_now", "label": "Run Email Scan Now"},
                        {"value": "remove_event", "label": "Remove One Event"},
                        {"value": "wipe_all_personal_data", "label": "Wipe All Personal Core Data"},
                    ],
                },
                {
                    "key": "account_id",
                    "label": "Account ID (for remove_event)",
                    "type": "text",
                    "value": "",
                    "description": "Needed only when removing a specific event.",
                },
                {
                    "key": "event_id",
                    "label": "Event ID (for remove_event)",
                    "type": "text",
                    "value": "",
                    "description": "Use event id from stored profile data.",
                },
                {
                    "key": "confirm_text",
                    "label": "Type WIPE To Confirm Full Wipe",
                    "type": "text",
                    "value": "",
                    "placeholder": "WIPE",
                    "description": "Used only for wipe action.",
                },
            ],
        },
        "item_forms": forms,
    }


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    del redis_client
    aggregate = _aggregate_profiles()
    cycle_stats = _load_cycle_stats()

    upcoming = list(aggregate.get("upcoming_events") or [])
    items: List[Dict[str, Any]] = []
    for row in upcoming[:25]:
        items.append(
            {
                "title": _text(row.get("title")) or "Upcoming event",
                "subtitle": f"{_text(row.get('kind')) or 'event'} · {_text(row.get('account_id'))}",
                "detail": f"{_text(row.get('starts_at')) or _iso_from_ts(row.get('starts_ts'))} · {_text(row.get('location')) or 'location n/a'}",
            }
        )

    return {
        "summary": "Personal profile insights from connected email inboxes.",
        "stats": [
            {"label": "Accounts", "value": _as_int(aggregate.get("account_count"), 0, minimum=0)},
            {"label": "Emails", "value": _as_int(aggregate.get("total_emails"), 0, minimum=0)},
            {"label": "Upcoming", "value": len(upcoming)},
            {"label": "Subs", "value": len(list(aggregate.get("subscriptions") or []))},
            {"label": "Deliveries", "value": _as_int(aggregate.get("open_deliveries"), 0, minimum=0)},
            {"label": "Actions", "value": _as_int(aggregate.get("open_actions"), 0, minimum=0)},
            {"label": "Spend (30d)", "value": f"${_as_float(aggregate.get('total_spending_30d'), 0.0, minimum=0.0):.2f}"},
            {"label": "Notifs", "value": _as_int(cycle_stats.get("notifications_sent"), 0, minimum=0)},
            {"label": "Last Run", "value": _text(cycle_stats.get("last_run_text")) or "n/a"},
            {"label": "Run Errors", "value": _as_int(cycle_stats.get("error_count"), 0, minimum=0)},
        ],
        "items": items,
        "empty_message": "No upcoming personal events discovered yet.",
        "ui": _ui_payload(aggregate, cycle_stats),
    }


def _wipe_all_personal_data(*, preserve_settings: bool = True) -> Dict[str, Any]:
    patterns = [
        _PERSONAL_STATS_KEY,
        _PERSONAL_ACCOUNTS_SET_KEY,
        f"{_PERSONAL_PROFILE_PREFIX}:*",
        f"{_PERSONAL_HISTORY_PREFIX}:*",
        f"{_PERSONAL_CURSOR_PREFIX}:*",
        f"{_PERSONAL_PROCESSED_PREFIX}:*",
        f"{_PERSONAL_NOTIFY_SENT_PREFIX}:*",
    ]

    deleted = 0
    for pattern in patterns:
        if "*" in pattern:
            try:
                keys = [_text(k) for k in redis_client.scan_iter(match=pattern, count=400)]
            except Exception:
                keys = []
            for key in keys:
                if not key:
                    continue
                try:
                    deleted += _as_int(redis_client.delete(key), 0, minimum=0)
                except Exception:
                    continue
        else:
            try:
                deleted += _as_int(redis_client.delete(pattern), 0, minimum=0)
            except Exception:
                continue

    if not preserve_settings:
        try:
            deleted += _as_int(redis_client.delete(_PERSONAL_SETTINGS_KEY), 0, minimum=0)
        except Exception:
            pass

    return {"ok": True, "deleted_total": deleted}


def _remove_profile_event(account_id: str, event_id: str) -> Dict[str, Any]:
    aid = _text(account_id)
    eid = _text(event_id)
    if not aid:
        return {"ok": False, "error": "account_id is required."}
    if not eid:
        return {"ok": False, "error": "event_id is required."}

    profile = _load_profile(aid)
    rows = list(profile.get("upcoming_events") or [])
    next_rows = [row for row in rows if _text((row or {}).get("id")) != eid]
    if len(next_rows) == len(rows):
        return {"ok": False, "error": "Event not found."}

    profile["upcoming_events"] = next_rows
    stats = profile.get("stats") if isinstance(profile.get("stats"), dict) else {}
    stats["upcoming_events"] = len(next_rows)
    profile["stats"] = stats
    profile["last_updated"] = time.time()
    _save_profile(aid, profile)
    return {"ok": True, "removed": 1}


def _remove_profile_entry(account_id: str, *, bucket: str, row_id: str) -> Dict[str, Any]:
    aid = _text(account_id)
    rid = _text(row_id)
    bucket_name = _text(bucket)
    if not aid:
        return {"ok": False, "error": "account_id is required."}
    if not rid:
        return {"ok": False, "error": "entry id is required."}
    if not bucket_name:
        return {"ok": False, "error": "entry bucket is required."}

    profile = _load_profile(aid)
    rows = list(profile.get(bucket_name) or [])
    next_rows = [row for row in rows if _text((row or {}).get("id")) != rid]
    if len(next_rows) == len(rows):
        return {"ok": False, "error": "Entry not found."}

    profile[bucket_name] = next_rows
    if bucket_name == "spending_habits":
        profile["favorite_places"] = _compute_favorite_places(next_rows)
    profile = _refresh_profile_stats(profile)
    profile["last_updated"] = time.time()
    _save_profile(aid, profile)
    return {"ok": True, "removed": 1, "bucket": bucket_name}


def _remove_profile_entry_from_item_id(item_id: Any) -> Dict[str, Any]:
    kind, account_id, row_id = _decode_profile_entry_item_id(item_id)
    if not kind or not account_id or not row_id:
        return {"ok": False, "error": "Invalid entry id."}
    bucket = _PROFILE_ENTRY_KIND_TO_BUCKET.get(kind)
    if not bucket:
        return {"ok": False, "error": f"Unsupported entry type: {kind}"}
    return _remove_profile_entry(account_id, bucket=bucket, row_id=row_id)


def _run_ui_tool_action(
    *,
    tool_action: Any,
    account_id: Any,
    event_id: Any,
    confirm_text: Any,
) -> str:
    action = _slug(tool_action, default="")
    if not action:
        raise ValueError("Select a tool action.")

    if action == "run_scan_now":
        settings = _load_settings()
        llm_client = None
        try:
            llm_client = get_llm_client_from_env()
        except Exception:
            llm_client = None
        cycle_start = time.time()
        stats = _run_cycle(llm_client, settings)
        _save_cycle_stats(stats, cycle_start=cycle_start)
        message = (
            "Personal scan complete: "
            f"accounts={_as_int(stats.get('account_count'), 0, minimum=0)}, "
            f"new_emails={_as_int(stats.get('inserted_count'), 0, minimum=0)}, "
            f"event_updates={_as_int(stats.get('updated_events'), 0, minimum=0)}, "
            f"delivery_updates={_as_int(stats.get('updated_deliveries'), 0, minimum=0)}, "
            f"action_updates={_as_int(stats.get('updated_actions'), 0, minimum=0)}, "
            f"notifications={_as_int(stats.get('notifications_sent'), 0, minimum=0)}, "
            f"errors={_as_int(stats.get('error_count'), 0, minimum=0)}"
        )
        errors = list(stats.get("errors") or [])
        if errors:
            first_error = _clean_text_blob(errors[0], max_chars=220)
            if first_error:
                message += f"; first_error={first_error}"
        return message

    if action == "remove_event":
        result = _remove_profile_event(_text(account_id), _text(event_id))
        if not bool(result.get("ok")):
            raise ValueError(_text(result.get("error")) or "Could not remove event.")
        return "Event removed from profile."

    if action == "wipe_all_personal_data":
        if _text(confirm_text).upper() != "WIPE":
            raise ValueError("Type WIPE to confirm the full personal-data wipe.")
        result = _wipe_all_personal_data(preserve_settings=True)
        if not bool(result.get("ok")):
            raise ValueError(_text(result.get("error")) or "Wipe failed.")
        return f"Wiped personal core data ({_as_int(result.get('deleted_total'), 0, minimum=0)} key(s) removed)."

    raise ValueError(f"Unknown tool action: {action}")


def _save_prompt_controls(
    *,
    prompt_include_discord: Any,
    prompt_include_irc: Any,
    prompt_include_telegram: Any,
    prompt_include_matrix: Any,
) -> Dict[str, Any]:
    current = _load_settings()
    discord_enabled = _as_bool(prompt_include_discord, _as_bool(current.get("prompt_include_discord"), False))
    irc_enabled = _as_bool(prompt_include_irc, _as_bool(current.get("prompt_include_irc"), False))
    telegram_enabled = _as_bool(prompt_include_telegram, _as_bool(current.get("prompt_include_telegram"), False))
    matrix_enabled = _as_bool(prompt_include_matrix, _as_bool(current.get("prompt_include_matrix"), False))

    redis_client.hset(
        _PERSONAL_SETTINGS_KEY,
        mapping={
            "prompt_include_discord": "1" if discord_enabled else "0",
            "prompt_include_irc": "1" if irc_enabled else "0",
            "prompt_include_telegram": "1" if telegram_enabled else "0",
            "prompt_include_matrix": "1" if matrix_enabled else "0",
        },
    )
    try:
        redis_client.hdel(_PERSONAL_SETTINGS_KEY, "prompt_preview_platform")
    except Exception:
        pass
    return {
        "ok": True,
        "prompt_include_discord": discord_enabled,
        "prompt_include_irc": irc_enabled,
        "prompt_include_telegram": telegram_enabled,
        "prompt_include_matrix": matrix_enabled,
    }


def _save_notification_controls(
    *,
    notifications_enabled: Any,
    notify_on_deliveries: Any,
    notify_on_spending: Any,
    notify_on_plans: Any,
    notification_platform: Any,
    notification_extra_destinations: Any,
    notification_max_per_cycle: Any,
    notification_ha_api_notification: Any,
    notification_ha_device_services: Any,
) -> Dict[str, Any]:
    current = _load_settings()
    enabled = _as_bool(notifications_enabled, _as_bool(current.get("notifications_enabled"), False))
    deliveries_enabled = _as_bool(notify_on_deliveries, _as_bool(current.get("notify_on_deliveries"), True))
    spending_enabled = _as_bool(notify_on_spending, _as_bool(current.get("notify_on_spending"), True))
    plans_enabled = _as_bool(notify_on_plans, _as_bool(current.get("notify_on_plans"), True))
    platform = _slug(notification_platform, default=_slug(current.get("notification_platform"), default="webui"))
    if not platform:
        platform = "webui"

    extra_values = _normalize_destination_values(
        notification_extra_destinations
        if notification_extra_destinations is not None
        else current.get("notification_extra_destinations")
    )
    clean_extra_values: List[str] = []
    seen_extra: set[str] = set()
    for raw_value in extra_values:
        destination_platform, destination_targets = _decode_destination_value(raw_value)
        if not destination_platform:
            continue
        normalized_value = _encode_destination_value(destination_platform, destination_targets)
        if not normalized_value or normalized_value in seen_extra:
            continue
        seen_extra.add(normalized_value)
        clean_extra_values.append(normalized_value)

    if clean_extra_values and not _slug(notification_platform, default=""):
        first_platform, _ = _decode_destination_value(clean_extra_values[0])
        if first_platform:
            platform = first_platform

    max_per_cycle = _as_int(
        notification_max_per_cycle,
        _as_int(current.get("notification_max_per_cycle"), 3, minimum=1, maximum=25),
        minimum=1,
        maximum=25,
    )
    ha_api_notification = _as_bool(
        notification_ha_api_notification,
        _as_bool(current.get("notification_ha_api_notification"), False),
    )
    ha_services = _normalize_notify_services(notification_ha_device_services)

    redis_client.hset(
        _PERSONAL_SETTINGS_KEY,
        mapping={
            "notifications_enabled": "1" if enabled else "0",
            "notify_on_deliveries": "1" if deliveries_enabled else "0",
            "notify_on_spending": "1" if spending_enabled else "0",
            "notify_on_plans": "1" if plans_enabled else "0",
            "notification_platform": platform,
            "notification_destinations": json.dumps([], ensure_ascii=False),
            "notification_extra_destinations": json.dumps(clean_extra_values, ensure_ascii=False),
            "notification_max_per_cycle": str(max_per_cycle),
            "notification_ha_api_notification": "1" if ha_api_notification else "0",
            "notification_ha_device_services": json.dumps(ha_services, ensure_ascii=False),
        },
    )
    try:
        redis_client.hdel(_PERSONAL_SETTINGS_KEY, "notification_target_text")
    except Exception:
        pass

    return {
        "ok": True,
        "notifications_enabled": enabled,
        "notify_on_deliveries": deliveries_enabled,
        "notify_on_spending": spending_enabled,
        "notify_on_plans": plans_enabled,
        "notification_platform": platform,
        "notification_destinations": [],
        "notification_extra_destinations": clean_extra_values,
        "notification_max_per_cycle": max_per_cycle,
        "notification_ha_api_notification": ha_api_notification,
        "notification_ha_device_services": ha_services,
    }


def _notification_test_settings_from_values(
    *,
    current: Dict[str, Any],
    notifications_enabled: Any,
    notify_on_deliveries: Any,
    notify_on_spending: Any,
    notify_on_plans: Any,
    notification_platform: Any,
    notification_extra_destinations: Any,
    notification_max_per_cycle: Any,
    notification_ha_api_notification: Any,
    notification_ha_device_services: Any,
) -> Dict[str, Any]:
    settings = dict(current or {})
    settings["notifications_enabled"] = _as_bool(
        notifications_enabled,
        _as_bool(settings.get("notifications_enabled"), False),
    )
    settings["notify_on_deliveries"] = _as_bool(
        notify_on_deliveries,
        _as_bool(settings.get("notify_on_deliveries"), True),
    )
    settings["notify_on_spending"] = _as_bool(
        notify_on_spending,
        _as_bool(settings.get("notify_on_spending"), True),
    )
    settings["notify_on_plans"] = _as_bool(
        notify_on_plans,
        _as_bool(settings.get("notify_on_plans"), True),
    )
    settings["notification_platform"] = _slug(
        notification_platform,
        default=_slug(settings.get("notification_platform"), default="webui"),
    ) or "webui"
    extra_values = _normalize_destination_values(
        notification_extra_destinations
        if notification_extra_destinations is not None
        else settings.get("notification_extra_destinations")
    )
    clean_values: List[str] = []
    seen_values: set[str] = set()
    for raw_value in extra_values:
        selected_platform, selected_targets = _decode_destination_value(raw_value)
        if not selected_platform:
            continue
        encoded = _encode_destination_value(selected_platform, selected_targets)
        if not encoded or encoded in seen_values:
            continue
        seen_values.add(encoded)
        clean_values.append(encoded)
    settings["notification_destinations"] = []
    settings["notification_extra_destinations"] = clean_values

    if clean_values and not _slug(notification_platform, default=""):
        first_platform, _ = _decode_destination_value(clean_values[0])
        if first_platform:
            settings["notification_platform"] = first_platform
    settings["notification_max_per_cycle"] = _as_int(
        notification_max_per_cycle,
        _as_int(settings.get("notification_max_per_cycle"), 3, minimum=1, maximum=25),
        minimum=1,
        maximum=25,
    )
    settings["notification_ha_api_notification"] = _as_bool(
        notification_ha_api_notification,
        _as_bool(settings.get("notification_ha_api_notification"), False),
    )
    settings["notification_ha_device_services"] = json.dumps(
        _normalize_notify_services(
            notification_ha_device_services
            if notification_ha_device_services is not None
            else settings.get("notification_ha_device_services")
        ),
        ensure_ascii=False,
    )
    return settings


def handle_htmlui_tab_action(*, action: str, payload: Dict[str, Any], redis_client=None, **_kwargs) -> Dict[str, Any]:
    del redis_client
    body = payload if isinstance(payload, dict) else {}
    values = body.get("values") if isinstance(body.get("values"), dict) else {}
    action_name = _slug(action, default="")

    def _value(key: str, default: Any = "") -> Any:
        if key in values:
            return values.get(key)
        return body.get(key, default)

    if action_name == "personal_run_tool":
        message = _run_ui_tool_action(
            tool_action=_value("tool_action"),
            account_id=_value("account_id"),
            event_id=_value("event_id"),
            confirm_text=_value("confirm_text"),
        )
        return {"ok": True, "message": message}

    if action_name == "personal_save_prompt_controls":
        result = _save_prompt_controls(
            prompt_include_discord=_value("prompt_include_discord"),
            prompt_include_irc=_value("prompt_include_irc"),
            prompt_include_telegram=_value("prompt_include_telegram"),
            prompt_include_matrix=_value("prompt_include_matrix"),
        )
        return {
            "ok": True,
            "message": (
                "Saved prompt controls: "
                f"discord={'on' if _as_bool(result.get('prompt_include_discord'), False) else 'off'}, "
                f"irc={'on' if _as_bool(result.get('prompt_include_irc'), False) else 'off'}, "
                f"telegram={'on' if _as_bool(result.get('prompt_include_telegram'), False) else 'off'}, "
                f"matrix={'on' if _as_bool(result.get('prompt_include_matrix'), False) else 'off'}."
            ),
        }

    if action_name == "personal_save_notification_controls":
        result = _save_notification_controls(
            notifications_enabled=_value("notifications_enabled"),
            notify_on_deliveries=_value("notify_on_deliveries"),
            notify_on_spending=_value("notify_on_spending"),
            notify_on_plans=_value("notify_on_plans"),
            notification_platform=_value("notification_platform", None),
            notification_extra_destinations=_value("notification_extra_destinations", None),
            notification_max_per_cycle=_value("notification_max_per_cycle"),
            notification_ha_api_notification=_value("notification_ha_api_notification"),
            notification_ha_device_services=_value("notification_ha_device_services"),
        )
        return {
            "ok": True,
            "message": (
                "Saved notification controls: "
                f"enabled={'on' if _as_bool(result.get('notifications_enabled'), False) else 'off'}, "
                f"routes={len(_normalize_destination_values(result.get('notification_extra_destinations')))}, "
                f"deliveries={'on' if _as_bool(result.get('notify_on_deliveries'), True) else 'off'}, "
                f"spending={'on' if _as_bool(result.get('notify_on_spending'), True) else 'off'}, "
                f"plans={'on' if _as_bool(result.get('notify_on_plans'), True) else 'off'}."
            ),
        }

    if action_name == "personal_send_notification_test":
        current = _load_settings()
        llm_client = None
        try:
            llm_client = get_llm_client_from_env()
        except Exception:
            llm_client = None
        test_settings = _notification_test_settings_from_values(
            current=current,
            notifications_enabled=_value("notifications_enabled", current.get("notifications_enabled")),
            notify_on_deliveries=_value("notify_on_deliveries", current.get("notify_on_deliveries")),
            notify_on_spending=_value("notify_on_spending", current.get("notify_on_spending")),
            notify_on_plans=_value("notify_on_plans", current.get("notify_on_plans")),
            notification_platform=_value("notification_platform", None),
            notification_extra_destinations=_value(
                "notification_extra_destinations",
                None,
            ),
            notification_max_per_cycle=_value("notification_max_per_cycle", current.get("notification_max_per_cycle")),
            notification_ha_api_notification=_value(
                "notification_ha_api_notification",
                current.get("notification_ha_api_notification"),
            ),
            notification_ha_device_services=_value(
                "notification_ha_device_services",
                current.get("notification_ha_device_services"),
            ),
        )
        result = _send_notification_tests_with_settings(test_settings, llm_client=llm_client)
        return {
            "ok": bool(result.get("ok")),
            "message": _text(result.get("message")) or "Notification test finished.",
            "data": result,
        }

    if action_name == "personal_remove_profile_entry":
        result = _remove_profile_entry_from_item_id(_value("id"))
        if not bool(result.get("ok")):
            raise ValueError(_text(result.get("error")) or "Could not remove profile entry.")
        return {"ok": True, "message": "Profile entry removed."}

    if action_name == "personal_remove_event":
        result = _remove_profile_event(_text(_value("account_id")), _text(_value("event_id")))
        if not bool(result.get("ok")):
            raise ValueError(_text(result.get("error")) or "Could not remove event.")
        return {"ok": True, "message": "Event removed."}

    if action_name == "personal_wipe_all":
        if _text(_value("confirm_text")).upper() != "WIPE":
            raise ValueError("Type WIPE to confirm the full personal-data wipe.")
        result = _wipe_all_personal_data(preserve_settings=True)
        return {
            "ok": True,
            "message": f"Wiped personal core data ({_as_int(result.get('deleted_total'), 0, minimum=0)} key(s) removed).",
        }

    raise ValueError(f"Unknown action: {action_name}")
