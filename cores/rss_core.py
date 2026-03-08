# cores/rss_core.py
import asyncio
import time
import os
import feedparser
import logging
import redis
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from helpers import get_llm_client_from_env
from notify import core_notifier_platforms, dispatch_notification
from rss_store import get_all_feeds, set_feed, update_feed, ensure_feed, delete_feed
__version__ = "1.0.4"


logger = logging.getLogger("rss")
logger.setLevel(logging.INFO)

load_dotenv()

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))

redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

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
        "discord": {"enabled": True, "targets": {}},
        "irc": {"enabled": True, "targets": {}},
        "matrix": {"enabled": True, "targets": {}},
        "homeassistant": {"enabled": True, "targets": {}},
        "ntfy": {"enabled": True, "targets": {}},
        "telegram": {"enabled": True, "targets": {}},
        "wordpress": {"enabled": True, "targets": {}},
    }

def _merge_feed_rules(global_rules: dict, feed_platforms: dict) -> dict:
    merged = {}
    for key, rule in (global_rules or {}).items():
        merged[key] = {
            "enabled": bool(rule.get("enabled", True)),
            "targets": _coerce_targets(rule.get("targets")),
        }

    for key, conf in (feed_platforms or {}).items():
        if not isinstance(conf, dict):
            continue
        if key not in merged:
            merged[key] = {
                "enabled": bool(conf.get("enabled", True)),
                "targets": _coerce_targets(conf.get("targets")),
            }
            continue
        if "enabled" in conf:
            merged[key]["enabled"] = bool(conf.get("enabled", True))
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
            rule = merged_rules.get(platform_key) or {"enabled": True, "targets": {}}
            if not rule.get("enabled", True):
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
            global_enabled = rules.get(platform_key, {}).get("enabled", True)
            if global_enabled:
                enabled_notifiers.append(platform_key)
                continue

            # Global disabled: check per-feed overrides for this platform.
            for _url, cfg in feeds.items():
                if not cfg.get("enabled", True):
                    continue
                platforms = cfg.get("portals") or {}
                pcfg = platforms.get(platform_key)
                if pcfg and pcfg.get("enabled", True):
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
        try:
            while not (stop_event and stop_event.is_set()):
                if not self.any_notifier_enabled():
                    logger.debug("No notifier routes are enabled. Skipping RSS check.")
                    await asyncio.sleep(_get_poll_interval())
                    continue

                feeds = self.get_feeds()
                for feed_url, feed_cfg in feeds.items():
                    if stop_event and stop_event.is_set():
                        break
                    try:
                        if not feed_cfg.get("enabled", True):
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


def render_webui_tab(**_kwargs):
    import streamlit as st

    def _as_bool(value: object, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        token = str(value).strip().lower()
        if token in ("1", "true", "yes", "on", "enabled"):
            return True
        if token in ("0", "false", "no", "off", "disabled"):
            return False
        return default

    st.title("RSS Feeds")
    st.caption("Add feeds and customize delivery per feed. Leave targets blank to use default routing.")

    add_url = st.text_input("RSS Feed URL", key="rss_add_url")
    cols = st.columns([1, 1, 2])
    if cols[0].button("Add Feed", key="rss_add_btn"):
        feed_url = (add_url or "").strip()
        if not feed_url:
            st.warning("Please enter a feed URL.")
        else:
            existing = get_all_feeds(redis_client) or {}
            if feed_url in existing:
                st.warning("That feed is already configured.")
            else:
                try:
                    parsed = feedparser.parse(feed_url)
                except Exception:
                    parsed = None
                if not parsed or (getattr(parsed, "bozo", 0) and not getattr(parsed, "entries", None)):
                    st.error("Failed to parse that feed URL.")
                else:
                    # Set last_ts=0 so the poller posts only the newest item once.
                    set_feed(redis_client, feed_url, {"last_ts": 0.0, "enabled": True, "portals": {}})
                    st.success("Feed added.")
                    st.rerun()

    feeds = get_all_feeds(redis_client) or {}
    if not feeds:
        st.info("No feeds configured yet.")
        return

    default_cfg = {
        "send_discord": True,
        "discord_channel_id": "",
        "send_irc": True,
        "irc_channel": "",
        "send_matrix": True,
        "matrix_room_id": "",
        "send_homeassistant": True,
        "ha_device_service": "",
        "send_ntfy": True,
        "ntfy_server": "https://ntfy.sh",
        "ntfy_topic": "",
        "ntfy_priority": "3",
        "ntfy_tags": "",
        "ntfy_click_from_first_url": True,
        "ntfy_token": "",
        "ntfy_username": "",
        "ntfy_password": "",
        "send_telegram": True,
        "telegram_chat_id": "",
        "send_wordpress": True,
        "wordpress_site_url": "",
        "wordpress_username": "",
        "wordpress_app_password": "",
        "wordpress_post_status": "draft",
        "wordpress_category_id": "",
    }

    for idx, (feed_url, cfg) in enumerate(sorted(feeds.items(), key=lambda kv: kv[0].lower())):
        exp_key = f"rss_feed_{idx}"
        with st.expander(feed_url, expanded=False):
            enabled_val = st.checkbox("Enabled", value=cfg.get("enabled", True), key=f"{exp_key}_enabled")

            portals = cfg.get("portals") or {}

            discord_override = portals.get("discord") or {}
            discord_enabled = st.checkbox(
                "Send to Discord",
                value=discord_override.get("enabled", default_cfg["send_discord"]),
                key=f"{exp_key}_discord_enabled",
            )
            discord_channel_id = st.text_input(
                "Discord Channel ID",
                value=(discord_override.get("targets") or {}).get("channel_id", ""),
                placeholder=default_cfg["discord_channel_id"],
                key=f"{exp_key}_discord_channel_id",
            )

            irc_override = portals.get("irc") or {}
            irc_enabled = st.checkbox(
                "Send to IRC",
                value=irc_override.get("enabled", default_cfg["send_irc"]),
                key=f"{exp_key}_irc_enabled",
            )
            irc_channel = st.text_input(
                "IRC Channel",
                value=(irc_override.get("targets") or {}).get("channel", ""),
                placeholder=default_cfg["irc_channel"],
                key=f"{exp_key}_irc_channel",
            )

            matrix_override = portals.get("matrix") or {}
            matrix_enabled = st.checkbox(
                "Send to Matrix",
                value=matrix_override.get("enabled", default_cfg["send_matrix"]),
                key=f"{exp_key}_matrix_enabled",
            )
            matrix_room_id = st.text_input(
                "Matrix Room ID or Alias",
                value=(matrix_override.get("targets") or {}).get("room_id", ""),
                placeholder=default_cfg["matrix_room_id"],
                key=f"{exp_key}_matrix_room_id",
            )

            ha_override = portals.get("homeassistant") or {}
            ha_enabled = st.checkbox(
                "Send to Home Assistant Notifications",
                value=ha_override.get("enabled", default_cfg["send_homeassistant"]),
                key=f"{exp_key}_ha_enabled",
            )
            ha_device = st.text_input(
                "HA Mobile Notify Service (optional override)",
                value=(ha_override.get("targets") or {}).get("device_service", ""),
                placeholder=default_cfg["ha_device_service"],
                key=f"{exp_key}_ha_device_service",
            )
            ha_persistent = (ha_override.get("targets") or {}).get("persistent")
            if isinstance(ha_persistent, str):
                ha_persistent = ha_persistent.strip().lower() in ("1", "true", "yes", "on")
            if ha_persistent is True:
                ha_persist_choice = "Force on"
            elif ha_persistent is False:
                ha_persist_choice = "Force off"
            else:
                ha_persist_choice = "Use default"
            ha_persist_choice = st.selectbox(
                "HA Persistent Notification",
                options=["Use default", "Force on", "Force off"],
                index=["Use default", "Force on", "Force off"].index(ha_persist_choice),
                key=f"{exp_key}_ha_persist_choice",
            )

            ntfy_override = portals.get("ntfy") or {}
            ntfy_enabled = st.checkbox(
                "Send to Ntfy",
                value=ntfy_override.get("enabled", default_cfg["send_ntfy"]),
                key=f"{exp_key}_ntfy_enabled",
            )
            ntfy_targets = ntfy_override.get("targets") or {}
            ntfy_server = st.text_input(
                "Ntfy Server",
                value=str(ntfy_targets.get("ntfy_server") or ntfy_targets.get("server") or ""),
                placeholder=default_cfg["ntfy_server"],
                key=f"{exp_key}_ntfy_server",
            )
            ntfy_topic = st.text_input(
                "Ntfy Topic",
                value=str(ntfy_targets.get("ntfy_topic") or ntfy_targets.get("topic") or ""),
                placeholder=default_cfg["ntfy_topic"],
                key=f"{exp_key}_ntfy_topic",
            )
            ntfy_priority_options = ["1", "2", "3", "4", "5"]
            ntfy_priority_current = str(
                ntfy_targets.get("ntfy_priority") or ntfy_targets.get("priority") or default_cfg["ntfy_priority"]
            ).strip()
            if ntfy_priority_current not in ntfy_priority_options:
                ntfy_priority_current = default_cfg["ntfy_priority"]
            ntfy_priority = st.selectbox(
                "Ntfy Priority",
                options=ntfy_priority_options,
                index=ntfy_priority_options.index(ntfy_priority_current),
                key=f"{exp_key}_ntfy_priority",
            )
            ntfy_tags = st.text_input(
                "Ntfy Tags",
                value=str(ntfy_targets.get("ntfy_tags") or ntfy_targets.get("tags") or ""),
                placeholder=default_cfg["ntfy_tags"],
                key=f"{exp_key}_ntfy_tags",
            )
            ntfy_click = st.checkbox(
                "Ntfy Click URL from first link",
                value=_as_bool(
                    ntfy_targets.get("ntfy_click_from_first_url", ntfy_targets.get("click_from_first_url")),
                    default=default_cfg["ntfy_click_from_first_url"],
                ),
                key=f"{exp_key}_ntfy_click_from_first_url",
            )
            ntfy_token = st.text_input(
                "Ntfy Token",
                value=str(ntfy_targets.get("ntfy_token") or ntfy_targets.get("token") or ""),
                placeholder=default_cfg["ntfy_token"],
                type="password",
                key=f"{exp_key}_ntfy_token",
            )
            ntfy_username = st.text_input(
                "Ntfy Username",
                value=str(ntfy_targets.get("ntfy_username") or ntfy_targets.get("username") or ""),
                placeholder=default_cfg["ntfy_username"],
                key=f"{exp_key}_ntfy_username",
            )
            ntfy_password = st.text_input(
                "Ntfy Password",
                value=str(ntfy_targets.get("ntfy_password") or ntfy_targets.get("password") or ""),
                placeholder=default_cfg["ntfy_password"],
                type="password",
                key=f"{exp_key}_ntfy_password",
            )

            telegram_override = portals.get("telegram") or {}
            telegram_enabled = st.checkbox(
                "Send to Telegram",
                value=telegram_override.get("enabled", default_cfg["send_telegram"]),
                key=f"{exp_key}_telegram_enabled",
            )
            telegram_targets = telegram_override.get("targets") or {}
            telegram_chat_id = st.text_input(
                "Telegram Chat ID / Username",
                value=str(
                    telegram_targets.get("chat_id")
                    or telegram_targets.get("channel_id")
                    or telegram_targets.get("channel")
                    or ""
                ),
                placeholder=default_cfg["telegram_chat_id"],
                key=f"{exp_key}_telegram_chat_id",
            )

            wp_override = portals.get("wordpress") or {}
            wp_enabled = st.checkbox(
                "Send to WordPress",
                value=wp_override.get("enabled", default_cfg["send_wordpress"]),
                key=f"{exp_key}_wp_enabled",
            )
            wp_targets = wp_override.get("targets") or {}
            wp_site_url = st.text_input(
                "WordPress Site URL",
                value=str(wp_targets.get("wordpress_site_url") or wp_targets.get("site_url") or ""),
                placeholder=default_cfg["wordpress_site_url"],
                key=f"{exp_key}_wordpress_site_url",
            )
            wp_username = st.text_input(
                "WordPress Username",
                value=str(wp_targets.get("wordpress_username") or wp_targets.get("username") or ""),
                placeholder=default_cfg["wordpress_username"],
                key=f"{exp_key}_wordpress_username",
            )
            wp_app_password = st.text_input(
                "WordPress App Password",
                value=str(
                    wp_targets.get("wordpress_app_password")
                    or wp_targets.get("app_password")
                    or wp_targets.get("password")
                    or ""
                ),
                placeholder=default_cfg["wordpress_app_password"],
                type="password",
                key=f"{exp_key}_wordpress_app_password",
            )
            wp_post_status_options = ["draft", "publish", "pending", "private"]
            wp_post_status_current = str(
                wp_targets.get("post_status") or default_cfg["wordpress_post_status"]
            ).strip().lower()
            if wp_post_status_current not in wp_post_status_options:
                wp_post_status_current = default_cfg["wordpress_post_status"]
            wp_post_status = st.selectbox(
                "WordPress Post Status",
                options=wp_post_status_options,
                index=wp_post_status_options.index(wp_post_status_current),
                key=f"{exp_key}_wordpress_post_status",
            )
            wp_category_id = st.text_input(
                "WordPress Category ID",
                value=str(wp_targets.get("category_id") or ""),
                placeholder=default_cfg["wordpress_category_id"],
                key=f"{exp_key}_wordpress_category_id",
            )

            save_cols = st.columns([1, 1, 2])
            if save_cols[0].button("Save Feed Settings", key=f"{exp_key}_save"):
                new_portals = {}

                if discord_enabled != default_cfg["send_discord"] or discord_channel_id:
                    new_portals["discord"] = {
                        "enabled": discord_enabled,
                        "targets": {"channel_id": discord_channel_id} if discord_channel_id else {},
                    }
                if irc_enabled != default_cfg["send_irc"] or irc_channel:
                    new_portals["irc"] = {
                        "enabled": irc_enabled,
                        "targets": {"channel": irc_channel} if irc_channel else {},
                    }
                if matrix_enabled != default_cfg["send_matrix"] or matrix_room_id:
                    new_portals["matrix"] = {
                        "enabled": matrix_enabled,
                        "targets": {"room_id": matrix_room_id} if matrix_room_id else {},
                    }
                if ha_enabled != default_cfg["send_homeassistant"] or ha_device or ha_persist_choice != "Use default":
                    targets = {}
                    if ha_device:
                        targets["device_service"] = ha_device
                    if ha_persist_choice == "Force on":
                        targets["persistent"] = True
                    elif ha_persist_choice == "Force off":
                        targets["persistent"] = False
                    new_portals["homeassistant"] = {
                        "enabled": ha_enabled,
                        "targets": targets,
                    }
                ntfy_targets: dict[str, object] = {}
                if ntfy_server:
                    ntfy_targets["ntfy_server"] = ntfy_server
                if ntfy_topic:
                    ntfy_targets["ntfy_topic"] = ntfy_topic
                if ntfy_priority != default_cfg["ntfy_priority"]:
                    ntfy_targets["ntfy_priority"] = ntfy_priority
                if ntfy_tags:
                    ntfy_targets["ntfy_tags"] = ntfy_tags
                if ntfy_click != default_cfg["ntfy_click_from_first_url"]:
                    ntfy_targets["ntfy_click_from_first_url"] = bool(ntfy_click)
                if ntfy_token:
                    ntfy_targets["ntfy_token"] = ntfy_token
                if ntfy_username:
                    ntfy_targets["ntfy_username"] = ntfy_username
                if ntfy_password:
                    ntfy_targets["ntfy_password"] = ntfy_password
                if ntfy_enabled != default_cfg["send_ntfy"] or ntfy_targets:
                    new_portals["ntfy"] = {
                        "enabled": ntfy_enabled,
                        "targets": ntfy_targets,
                    }

                telegram_targets: dict[str, str] = {}
                if telegram_chat_id:
                    telegram_targets["chat_id"] = telegram_chat_id
                if telegram_enabled != default_cfg["send_telegram"] or telegram_targets:
                    new_portals["telegram"] = {
                        "enabled": telegram_enabled,
                        "targets": telegram_targets,
                    }

                wp_targets_save: dict[str, str] = {}
                if wp_site_url:
                    wp_targets_save["wordpress_site_url"] = wp_site_url
                if wp_username:
                    wp_targets_save["wordpress_username"] = wp_username
                if wp_app_password:
                    wp_targets_save["wordpress_app_password"] = wp_app_password
                if wp_post_status != default_cfg["wordpress_post_status"]:
                    wp_targets_save["post_status"] = wp_post_status
                if wp_category_id:
                    wp_targets_save["category_id"] = wp_category_id
                if wp_enabled != default_cfg["send_wordpress"] or wp_targets_save:
                    new_portals["wordpress"] = {
                        "enabled": wp_enabled,
                        "targets": wp_targets_save,
                    }

                update_feed(redis_client, feed_url, {"enabled": enabled_val, "portals": new_portals})
                st.success("Feed settings saved.")
                st.rerun()

            if save_cols[1].button("Remove Feed", key=f"{exp_key}_remove"):
                delete_feed(redis_client, feed_url)
                st.success("Feed removed.")
                st.rerun()


def run(stop_event=None):
    llm_client = get_llm_client_from_env()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    backoff = 1.0
    max_backoff = 10.0

    while not (stop_event and stop_event.is_set()):
        try:
            rss_manager = RSSManager(llm_client=llm_client)
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
