import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

from verba_base import ToolVerba
from helpers import redis_client
from verba_result import action_failure, action_success

logger = logging.getLogger("moltbook_latest_posts")
logger.setLevel(logging.INFO)

MOLTBOOK_BASE_URL = "https://www.moltbook.com"
MOLTBOOK_HOST = "www.moltbook.com"
MOLTBOOK_API_PREFIX = "/api/v1/"

MOLTBOOK_PORTAL_SETTINGS_KEY = "moltbook_portal_settings"
MOLTBOOK_PORTAL_STATE_KEY = "tater:moltbook:state"
MOLTBOOK_FOLLOWED_AGENTS_KEY = "tater:moltbook:followed_agents"
MOLTBOOK_SUBSCRIBED_SUBMOLTS_KEY = "tater:moltbook:subscribed_submolts"
MOLTBOOK_FELLOW_TATERS_KEY = "tater:moltbook:fellow_tater_agents"

VALID_ROUTES = {
    "latest_posts",
    "following",
    "profile_link",
    "account_summary",
    "home_overview",
    "latest_announcement",
    "activity_on_my_posts",
    "subscriptions",
    "monitoring_submolts",
    "fellow_taters",
    "agent_profile",
    "help",
}


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _decode_text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "ignore")
    if value is None:
        return ""
    return str(value)


def _coalesce_str(*values: Any, default: str = "") -> str:
    for item in values:
        text = _decode_text(item).strip()
        if text:
            return text
    return default


def _safe_int(value: Any, default: int = 0, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        parsed = int(float(str(value).strip()))
    except Exception:
        parsed = int(default)
    if minimum is not None and parsed < minimum:
        parsed = minimum
    if maximum is not None and parsed > maximum:
        parsed = maximum
    return parsed


def _limit_text(value: Any, max_chars: int) -> str:
    text = _decode_text(value).strip()
    if max_chars <= 0:
        return ""
    return text[:max_chars]


def _parse_csv_or_json_list(value: Any) -> List[str]:
    text = _decode_text(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                out: List[str] = []
                seen = set()
                for item in parsed:
                    token = _decode_text(item).strip().lower()
                    if not token or token in seen:
                        continue
                    seen.add(token)
                    out.append(token)
                return out
        except Exception:
            pass

    out: List[str] = []
    seen = set()
    for token in re.split(r"[,\n\r\t]+", text):
        item = _decode_text(token).strip().lower()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_author_name(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    author = payload.get("author")
    if isinstance(author, dict):
        return _coalesce_str(author.get("name"), author.get("username"), default="")
    return _coalesce_str(payload.get("author_name"), payload.get("authorName"), default="")


def _extract_submolt_name(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    submolt = payload.get("submolt")
    if isinstance(submolt, dict):
        return _coalesce_str(submolt.get("name"), default="")
    return _coalesce_str(payload.get("submolt_name"), payload.get("submolt"), default="")


def _extract_post_id(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    return _coalesce_str(payload.get("id"), payload.get("post_id"), payload.get("postId"), default="")


def _extract_post_title(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    return _coalesce_str(payload.get("title"), payload.get("name"), default="")


def _extract_posts(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("posts"), list):
            return [row for row in payload.get("posts") if isinstance(row, dict)]
        if isinstance(payload.get("items"), list):
            return [row for row in payload.get("items") if isinstance(row, dict)]
        if isinstance(payload.get("results"), list):
            return [row for row in payload.get("results") if isinstance(row, dict)]
    return []


class MoltbookLatestPostsPlugin(ToolVerba):
    name = "moltbook_latest_posts"
    verba_name = "Moltbook Latest Posts"
    version = "1.0.0"
    min_tater_version = "59"
    pretty_name = "Moltbook Latest Posts"
    settings_category = "Moltbook Info"
    tags = ['moltbook', 'latest_posts']
    fixed_route = "latest_posts"
    platforms = ['webui', 'macos', 'homeassistant', 'homekit', 'xbmc', 'discord', 'telegram', 'matrix', 'irc']

    usage = (
        "{\"function\":\"moltbook_latest_posts\",\"arguments\":{\"query\":\"what are your latest Moltbook posts\",\"limit\":5}}"
    )
    description = (
        "List recent self-authored Moltbook posts."
    )
    verba_dec = "Fetch latest posts authored by this Moltbook agent."
    when_to_use = (
        "Use when the user asks for your latest or recent Moltbook posts."
    )
    how_to_use = (
        "Set `query` to a latest-posts request and optionally set `limit`."
    )
    common_needs = ['A latest-posts request in query.']
    routing_keywords = ['moltbook', 'latest posts', 'recent posts', 'my posts', 'your posts']
    missing_info_prompts = []
    example_calls = ['{"function":"moltbook_latest_posts","arguments":{"query":"what are your latest Moltbook posts"}}', '{"function":"moltbook_latest_posts","arguments":{"query":"show your recent posts","limit":8}}']
    argument_schema = {'type': 'object', 'properties': {'query': {'type': 'string', 'description': 'The latest-posts request (for example: show your recent posts).'}, 'limit': {'type': 'integer', 'description': 'Optional number of posts to return (1-20).'}}, 'required': []}
    waiting_prompt_template = (
        "Write a short friendly message that tells {mention} you are checking Moltbook now. "
        "Only output that message."
    )
    required_settings = {
        "MOLTBOOK_API_KEY_OVERRIDE": {
            "label": "Moltbook API Key Override (optional)",
            "type": "password",
            "default": "",
            "description": "Optional. Leave blank to use api_key from moltbook_portal_settings.",
        },
        "MOLTBOOK_TIMEOUT_SECONDS": {
            "label": "Moltbook Request Timeout (seconds)",
            "type": "number",
            "default": 18,
            "description": "HTTP timeout for Moltbook requests.",
        },
        "MOLTBOOK_SELF_POST_SCAN_LIMIT": {
            "label": "Max Posts to Scan for Latest Self Posts",
            "type": "number",
            "default": 180,
            "description": "Upper bound when scanning /posts?sort=new for self-authored posts.",
        },
        "MOLTBOOK_FOLLOWING_SAMPLE_LIMIT": {
            "label": "Following Feed Sample Limit",
            "type": "number",
            "default": 60,
            "description": "How many following-feed posts to inspect for active followed authors.",
        },
    }

    # ----------------------------
    # Settings helpers
    # ----------------------------
    @staticmethod
    def _decode_map(raw: Dict[Any, Any] | None) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = _decode_text(key)
            out[k] = _decode_text(value)
        return out

    def _plugin_settings(self) -> Dict[str, str]:
        merged: Dict[str, str] = {}
        for key in (
            "verba_settings:Moltbook Info",
            "verba_settings: Moltbook Info",
            "verba_settings:Moltbook",
            "verba_settings: Moltbook",
        ):
            merged.update(self._decode_map(redis_client.hgetall(key) or {}))
        return merged

    def _portal_settings(self) -> Dict[str, str]:
        return self._decode_map(redis_client.hgetall(MOLTBOOK_PORTAL_SETTINGS_KEY) or {})

    def _portal_state(self) -> Dict[str, str]:
        return self._decode_map(redis_client.hgetall(MOLTBOOK_PORTAL_STATE_KEY) or {})

    def _get_int_setting(self, key: str, default: int, minimum: int, maximum: int) -> int:
        raw = self._plugin_settings().get(key)
        return _safe_int(raw, default=default, minimum=minimum, maximum=maximum)

    def _timeout(self) -> int:
        return self._get_int_setting("MOLTBOOK_TIMEOUT_SECONDS", default=18, minimum=5, maximum=60)

    def _self_post_scan_limit(self) -> int:
        return self._get_int_setting("MOLTBOOK_SELF_POST_SCAN_LIMIT", default=180, minimum=40, maximum=1000)

    def _following_sample_limit(self) -> int:
        return self._get_int_setting("MOLTBOOK_FOLLOWING_SAMPLE_LIMIT", default=60, minimum=20, maximum=100)

    # ----------------------------
    # Moltbook API safety + calls
    # ----------------------------
    def _resolve_api_key(self) -> str:
        plugin_key = _coalesce_str(self._plugin_settings().get("MOLTBOOK_API_KEY_OVERRIDE"), default="")
        if plugin_key:
            return plugin_key
        return _coalesce_str(self._portal_settings().get("api_key"), default="")

    def _build_url(self, path_or_url: str) -> str:
        raw = _decode_text(path_or_url).strip()
        if not raw:
            raise RuntimeError("missing_moltbook_path")
        if raw.startswith("https://") or raw.startswith("http://"):
            return raw
        if not raw.startswith("/"):
            raw = f"/{raw}"
        return urljoin(MOLTBOOK_BASE_URL, raw)

    def _assert_safe_url(self, url: str, *, auth_required: bool) -> None:
        parsed = urlparse(url)
        host = _decode_text(parsed.netloc).strip().lower()
        scheme = _decode_text(parsed.scheme).strip().lower()
        path = _decode_text(parsed.path).strip()
        if host != MOLTBOOK_HOST:
            raise RuntimeError("unsafe_moltbook_host")
        if scheme != "https":
            raise RuntimeError("unsafe_moltbook_scheme")
        if auth_required and not path.startswith(MOLTBOOK_API_PREFIX):
            raise RuntimeError("unsafe_moltbook_auth_path")

    def _api_request(
        self,
        method: str,
        path_or_url: str,
        *,
        auth_required: bool = True,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        api_key = self._resolve_api_key() if auth_required else ""
        if auth_required and not api_key:
            raise RuntimeError("missing_moltbook_api_key")

        url = self._build_url(path_or_url)
        self._assert_safe_url(url, auth_required=auth_required)

        headers: Dict[str, str] = {"Accept": "application/json"}
        if auth_required:
            headers["Authorization"] = f"Bearer {api_key}"

        response = requests.request(
            method=method.strip().upper() or "GET",
            url=url,
            headers=headers,
            params=params,
            timeout=self._timeout(),
            allow_redirects=False,
        )

        if 300 <= response.status_code < 400:
            raise RuntimeError(f"moltbook_redirect_blocked:{response.status_code}")

        payload: Any
        ctype = _decode_text(response.headers.get("Content-Type")).lower()
        if "application/json" in ctype:
            try:
                payload = response.json()
            except Exception:
                payload = {"raw": response.text}
        else:
            payload = {"raw": response.text}

        if response.status_code >= 400:
            message = ""
            if isinstance(payload, dict):
                message = _coalesce_str(payload.get("error"), payload.get("message"), default="")
            if not message:
                message = _limit_text(response.text, 220)
            raise RuntimeError(f"moltbook_http_{response.status_code}:{message}")

        return payload

    def _api_get(self, path_or_url: str, *, params: Optional[Dict[str, Any]] = None) -> Tuple[Any, str]:
        try:
            return self._api_request("GET", path_or_url, auth_required=True, params=params), ""
        except Exception as exc:
            return None, str(exc)

    # ----------------------------
    # Route resolver
    # ----------------------------
    @staticmethod
    def _extract_query(args: Dict[str, Any]) -> str:
        for key in ("query", "request", "text", "message", "content", "prompt"):
            value = args.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _extract_target_name(args: Dict[str, Any], query: str) -> str:
        explicit = _coalesce_str(args.get("target_name"), default="").strip().lower()
        if explicit:
            return explicit
        q = _decode_text(query).strip().lower()
        match = re.search(
            r"(?:profile\s+(?:for|of)|show\s+profile\s+(?:for|of)?|agent\s+profile\s+(?:for|of)?)\s+([a-z0-9_.-]{2,64})",
            q,
        )
        if match:
            return _decode_text(match.group(1)).strip().lower()
        return ""

    def _resolve_route(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = self._extract_query(args)
        route = _coalesce_str(getattr(self, "fixed_route", ""), default="help").strip().lower()
        if route not in VALID_ROUTES:
            route = "help"
        limit = _safe_int(args.get("limit"), default=5, minimum=1, maximum=20)
        target_name = self._extract_target_name(args, query)
        return {"route": route, "limit": limit, "target_name": target_name, "query": query}

    # ----------------------------
    # Data builders
    # ----------------------------
    def _get_me(self) -> Tuple[Dict[str, Any], str]:
        payload, err = self._api_get(f"{MOLTBOOK_API_PREFIX}agents/me")
        if err:
            return {}, err
        if isinstance(payload, dict) and isinstance(payload.get("agent"), dict):
            return payload.get("agent") or {}, ""
        if isinstance(payload, dict):
            return payload, ""
        return {}, "unexpected_agents_me_payload"

    def _get_status(self) -> Tuple[Dict[str, Any], str]:
        payload, err = self._api_get(f"{MOLTBOOK_API_PREFIX}agents/status")
        if err:
            return {}, err
        return payload if isinstance(payload, dict) else {}, ""

    def _get_home(self) -> Tuple[Dict[str, Any], str]:
        payload, err = self._api_get(f"{MOLTBOOK_API_PREFIX}home")
        if err:
            return {}, err
        return payload if isinstance(payload, dict) else {}, ""

    def _profile_url_for(self, name: str, me_payload: Dict[str, Any], status_payload: Dict[str, Any]) -> str:
        url = _coalesce_str(
            me_payload.get("profile_url"),
            me_payload.get("profileUrl"),
            me_payload.get("url"),
            status_payload.get("profile_url"),
            status_payload.get("profileUrl"),
            default="",
        )
        if url:
            return url
        if name:
            return f"{MOLTBOOK_BASE_URL}/u/{name}"
        return MOLTBOOK_BASE_URL

    def _fetch_latest_self_posts(self, self_name: str, *, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        if not self_name:
            return [], "missing_self_name"
        limit = max(1, min(int(limit), 20))
        scan_limit = self._self_post_scan_limit()

        results: List[Dict[str, Any]] = []
        cursor = ""
        scanned = 0
        seen_ids = set()
        while scanned < scan_limit and len(results) < limit:
            params: Dict[str, Any] = {"sort": "new", "limit": 50}
            if cursor:
                params["cursor"] = cursor

            payload, err = self._api_get(f"{MOLTBOOK_API_PREFIX}posts", params=params)
            if err:
                return results, err
            posts = _extract_posts(payload)
            if not posts:
                break

            for post in posts:
                if not isinstance(post, dict):
                    continue
                scanned += 1
                if scanned > scan_limit:
                    break
                pid = _extract_post_id(post)
                if pid and pid in seen_ids:
                    continue
                if pid:
                    seen_ids.add(pid)
                author = _extract_author_name(post).strip().lower()
                if author != self_name.strip().lower():
                    continue
                results.append(
                    {
                        "id": pid,
                        "title": _extract_post_title(post),
                        "submolt": _extract_submolt_name(post) or "general",
                        "created_at": _coalesce_str(post.get("created_at"), post.get("createdAt"), default=""),
                        "upvotes": _safe_int(post.get("upvotes"), 0),
                        "comments": _safe_int(post.get("comment_count"), _safe_int(post.get("comments_count"), 0)),
                    }
                )
                if len(results) >= limit:
                    break

            if len(results) >= limit:
                break
            has_more = bool(isinstance(payload, dict) and payload.get("has_more"))
            next_cursor = _coalesce_str(payload.get("next_cursor"), default="") if isinstance(payload, dict) else ""
            if not has_more or not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

        return results[:limit], ""

    def _local_set_members(self, key: str) -> List[str]:
        try:
            values = redis_client.smembers(key) or set()
            out = []
            for item in values:
                token = _decode_text(item).strip().lower()
                if token:
                    out.append(token)
            return sorted(set(out))
        except Exception:
            return []

    @staticmethod
    def _summarize_items(title: str, items: List[str], *, max_items: int = 12) -> str:
        clean = [item for item in items if item]
        if not clean:
            return f"{title}: (none)"
        shown = clean[:max_items]
        more = max(0, len(clean) - len(shown))
        line = ", ".join(shown)
        if more > 0:
            line += f" (+{more} more)"
        return f"{title}: {line}"

    # ----------------------------
    # Route handlers
    # ----------------------------
    def _handle_account_summary(self) -> Dict[str, Any]:
        me, me_err = self._get_me()
        if me_err:
            return action_failure(
                code="moltbook_me_failed",
                message=f"Could not fetch account profile: {me_err}",
                say_hint="Explain that Moltbook profile lookup failed and suggest retrying.",
            )
        status, status_err = self._get_status()
        home, _ = self._get_home()
        if status_err:
            status = {}

        name = _coalesce_str(me.get("name"), me.get("agent_name"), default="")
        claim_status = _coalesce_str(status.get("status"), default="unknown")
        karma = _safe_int(me.get("karma"), default=0)
        followers = _safe_int(me.get("follower_count"), default=0)
        following = _safe_int(me.get("following_count"), default=0)
        posts = _safe_int(me.get("posts_count"), default=0)
        comments = _safe_int(me.get("comments_count"), default=0)
        profile_url = self._profile_url_for(name, me, status)

        account_blob = home.get("your_account") if isinstance(home.get("your_account"), dict) else {}
        unread = _safe_int(
            _coalesce_str(
                account_blob.get("unread_notification_count"),
                account_blob.get("unread_notifications"),
                home.get("unread_notifications"),
                default="0",
            ),
            0,
            minimum=0,
        )

        summary = (
            f"Moltbook account: {name or '(unknown)'}\n"
            f"Claim status: {claim_status}\n"
            f"Karma: {karma} | Followers: {followers} | Following: {following}\n"
            f"Posts: {posts} | Comments: {comments} | Unread notifications: {unread}\n"
            f"Profile link: {profile_url}"
        )
        return action_success(
            facts={
                "agent_name": name,
                "claim_status": claim_status,
                "karma": karma,
                "follower_count": followers,
                "following_count": following,
                "posts_count": posts,
                "comments_count": comments,
                "unread_notifications": unread,
                "profile_url": profile_url,
            },
            summary_for_user=summary,
            say_hint="Summarize account status and key stats.",
        )

    def _handle_profile_link(self) -> Dict[str, Any]:
        me, me_err = self._get_me()
        if me_err:
            return action_failure(
                code="moltbook_profile_link_failed",
                message=f"Could not fetch profile data: {me_err}",
                say_hint="Explain that profile link lookup failed.",
            )
        status, _ = self._get_status()
        name = _coalesce_str(me.get("name"), me.get("agent_name"), default="")
        profile_url = self._profile_url_for(name, me, status)
        summary = f"Your Moltbook profile link is: {profile_url}"
        return action_success(
            facts={"agent_name": name, "profile_url": profile_url},
            summary_for_user=summary,
            say_hint="Return the profile URL plainly.",
        )

    def _handle_latest_posts(self, *, limit: int) -> Dict[str, Any]:
        me, me_err = self._get_me()
        if me_err:
            return action_failure(
                code="moltbook_latest_posts_failed",
                message=f"Could not fetch account profile: {me_err}",
                say_hint="Explain that latest post lookup failed.",
            )
        name = _coalesce_str(me.get("name"), me.get("agent_name"), default="")
        posts, err = self._fetch_latest_self_posts(name, limit=limit)
        if err:
            return action_failure(
                code="moltbook_posts_fetch_failed",
                message=f"Could not fetch latest posts: {err}",
                say_hint="Explain that latest post lookup failed and suggest retrying.",
            )

        if not posts:
            return action_success(
                facts={"agent_name": name, "latest_posts": []},
                summary_for_user=f"I could not find recent posts by {name or 'this agent'} in the latest feed scan.",
                say_hint="Report no recent self-authored posts were found.",
            )

        lines = [f"Latest posts for {name}:"]
        for idx, post in enumerate(posts, 1):
            title = _limit_text(post.get("title"), 180) or "(untitled)"
            submolt = _limit_text(post.get("submolt"), 60) or "general"
            pid = _limit_text(post.get("id"), 64)
            created = _limit_text(post.get("created_at"), 40)
            upvotes = _safe_int(post.get("upvotes"), 0)
            comments = _safe_int(post.get("comments"), 0)
            lines.append(
                f"{idx}. [m/{submolt}] {title} (id: {pid}, upvotes: {upvotes}, comments: {comments}, created: {created})"
            )

        return action_success(
            facts={"agent_name": name, "latest_posts": posts},
            summary_for_user="\n".join(lines),
            say_hint="List latest self-authored posts with submolt and basic stats.",
        )

    def _handle_following(self, *, limit: int) -> Dict[str, Any]:
        me, me_err = self._get_me()
        if me_err:
            return action_failure(
                code="moltbook_following_failed",
                message=f"Could not fetch account profile: {me_err}",
                say_hint="Explain that following lookup failed.",
            )
        following_count = _safe_int(me.get("following_count"), 0, minimum=0)

        local_follows = self._local_set_members(MOLTBOOK_FOLLOWED_AGENTS_KEY)
        sample_limit = min(self._following_sample_limit(), max(25, limit * 5))
        feed_payload, feed_err = self._api_get(
            f"{MOLTBOOK_API_PREFIX}feed",
            params={"filter": "following", "sort": "new", "limit": sample_limit},
        )
        if feed_err:
            return action_failure(
                code="moltbook_following_feed_failed",
                message=f"Could not fetch following feed: {feed_err}",
                say_hint="Explain that following feed lookup failed.",
            )

        active_authors: List[str] = []
        seen = set()
        for post in _extract_posts(feed_payload):
            author = _extract_author_name(post).strip().lower()
            if not author or author in seen:
                continue
            seen.add(author)
            active_authors.append(author)

        combined = []
        seen2 = set()
        for name in local_follows + active_authors:
            token = _decode_text(name).strip().lower()
            if not token or token in seen2:
                continue
            seen2.add(token)
            combined.append(token)

        lines = [f"Moltbook following_count: {following_count}"]
        lines.append(self._summarize_items("Known followed agents (portal state)", local_follows, max_items=20))
        lines.append(self._summarize_items("Active authors seen in following feed", active_authors, max_items=20))

        return action_success(
            facts={
                "following_count": following_count,
                "known_followed_agents": local_follows,
                "active_following_authors": active_authors,
                "combined_following_view": combined,
            },
            summary_for_user="\n".join(lines),
            say_hint="Report who the account follows using platform count plus known local follows.",
        )

    def _handle_home_overview(self) -> Dict[str, Any]:
        home, err = self._get_home()
        if err:
            return action_failure(
                code="moltbook_home_failed",
                message=f"Could not fetch /home: {err}",
                say_hint="Explain that the Moltbook home overview failed.",
            )

        account = home.get("your_account") if isinstance(home.get("your_account"), dict) else {}
        activity = _as_list(home.get("activity_on_your_posts"))
        dms = _as_list(home.get("your_direct_messages"))
        followed_preview = _as_list(home.get("posts_from_accounts_you_follow"))
        actions = _as_list(home.get("what_to_do_next"))

        unread = _safe_int(
            _coalesce_str(
                account.get("unread_notification_count"),
                account.get("unread_notifications"),
                home.get("unread_notifications"),
                default="0",
            ),
            0,
            minimum=0,
        )

        summary = (
            "Moltbook /home overview:\n"
            f"- Unread notifications: {unread}\n"
            f"- Activity buckets on your posts: {len(activity)}\n"
            f"- DM items: {len(dms)}\n"
            f"- Followed-account preview posts: {len(followed_preview)}\n"
            f"- Suggested actions: {len(actions)}"
        )
        return action_success(
            facts={
                "unread_notifications": unread,
                "activity_on_your_posts_count": len(activity),
                "direct_messages_count": len(dms),
                "followed_preview_count": len(followed_preview),
                "what_to_do_next_count": len(actions),
            },
            summary_for_user=summary,
            say_hint="Summarize the key counts from /home.",
        )

    def _handle_latest_announcement(self) -> Dict[str, Any]:
        home, err = self._get_home()
        if err:
            return action_failure(
                code="moltbook_announcement_failed",
                message=f"Could not fetch /home: {err}",
                say_hint="Explain that announcement lookup failed.",
            )

        ann = home.get("latest_moltbook_announcement")
        if isinstance(ann, dict):
            title = _coalesce_str(ann.get("title"), ann.get("headline"), default="")
            content = _coalesce_str(ann.get("content"), ann.get("summary"), ann.get("text"), default="")
            url = _coalesce_str(ann.get("url"), ann.get("link"), default="")
        else:
            title = ""
            content = _decode_text(ann).strip()
            url = ""

        if not title and not content:
            return action_success(
                facts={"announcement": None},
                summary_for_user="No latest Moltbook announcement was present in /home.",
                say_hint="Report that no announcement was present.",
            )

        summary = f"Latest Moltbook announcement: {title or '(untitled)'}"
        if content:
            summary += f"\n{_limit_text(content, 900)}"
        if url:
            summary += f"\nLink: {url}"
        return action_success(
            facts={"announcement": {"title": title, "content": content, "url": url}},
            summary_for_user=summary,
            say_hint="Provide the latest announcement title and a short summary.",
        )

    def _handle_activity_on_my_posts(self, *, limit: int) -> Dict[str, Any]:
        home, err = self._get_home()
        if err:
            return action_failure(
                code="moltbook_activity_failed",
                message=f"Could not fetch /home: {err}",
                say_hint="Explain that activity lookup failed.",
            )

        raw = home.get("activity_on_your_posts")
        if isinstance(raw, dict):
            items = _as_list(raw.get("items")) or _as_list(raw.get("posts"))
        else:
            items = _as_list(raw)

        entries: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            post_id = _extract_post_id(item)
            preview = _coalesce_str(item.get("preview"), item.get("summary"), item.get("text"), default="")
            latest_commenters = _as_list(item.get("latest_commenters")) or _as_list(item.get("commenters"))
            suggested = _coalesce_str(item.get("suggested_action"), item.get("action"), default="")
            entries.append(
                {
                    "post_id": post_id,
                    "preview": _limit_text(preview, 240),
                    "latest_commenters": [_decode_text(v).strip() for v in latest_commenters if _decode_text(v).strip()],
                    "suggested_action": suggested,
                }
            )
            if len(entries) >= max(1, min(int(limit), 20)):
                break

        if not entries:
            return action_success(
                facts={"activity_on_your_posts": []},
                summary_for_user="No activity on your posts is currently shown in /home.",
                say_hint="Report that there is no post activity right now.",
            )

        lines = ["Activity on your posts:"]
        for idx, row in enumerate(entries, 1):
            commenters = ", ".join(row.get("latest_commenters") or []) or "none"
            lines.append(
                f"{idx}. post_id={row.get('post_id') or '(unknown)'} | commenters={commenters} | "
                f"suggested={row.get('suggested_action') or 'n/a'} | preview={row.get('preview') or '(none)'}"
            )
        return action_success(
            facts={"activity_on_your_posts": entries},
            summary_for_user="\n".join(lines),
            say_hint="List current /home activity buckets for your posts.",
        )

    def _handle_subscriptions(self) -> Dict[str, Any]:
        subs = self._local_set_members(MOLTBOOK_SUBSCRIBED_SUBMOLTS_KEY)
        if not subs:
            return action_success(
                facts={"subscribed_submolts": []},
                summary_for_user="No subscribed submolts are currently tracked in local Moltbook portal state.",
                say_hint="Report no local subscribed submolts.",
            )
        return action_success(
            facts={"subscribed_submolts": subs},
            summary_for_user=self._summarize_items("Tracked subscribed submolts", subs, max_items=30),
            say_hint="List tracked subscribed submolts.",
        )

    def _handle_monitoring_submolts(self) -> Dict[str, Any]:
        settings = self._portal_settings()
        monitored = _parse_csv_or_json_list(settings.get("submolts_to_monitor"))
        preferred = _parse_csv_or_json_list(settings.get("submolts_to_prefer_for_posting"))
        avoid = _parse_csv_or_json_list(settings.get("submolts_to_avoid"))

        summary = (
            self._summarize_items("Monitored submolts", monitored, max_items=30)
            + "\n"
            + self._summarize_items("Preferred posting submolts", preferred, max_items=30)
            + "\n"
            + self._summarize_items("Avoided submolts", avoid, max_items=30)
        )
        return action_success(
            facts={
                "submolts_to_monitor": monitored,
                "submolts_to_prefer_for_posting": preferred,
                "submolts_to_avoid": avoid,
            },
            summary_for_user=summary,
            say_hint="Summarize monitored/preferred/avoided submolt configuration.",
        )

    def _handle_fellow_taters(self) -> Dict[str, Any]:
        me, me_err = self._get_me()
        me_name = _coalesce_str(me.get("name"), me.get("agent_name"), default="").strip().lower()
        if me_err:
            me_name = ""
        fellows = [item for item in self._local_set_members(MOLTBOOK_FELLOW_TATERS_KEY) if item and item != me_name]
        if not fellows:
            return action_success(
                facts={"fellow_tater_agents": []},
                summary_for_user="No fellow Tater agents are currently tracked in local Moltbook state.",
                say_hint="Report no tracked fellow Tater agents.",
            )
        return action_success(
            facts={"fellow_tater_agents": fellows},
            summary_for_user=self._summarize_items("Known fellow Tater agents", fellows, max_items=40),
            say_hint="List known fellow Tater agents.",
        )

    def _handle_agent_profile(self, target_name: str) -> Dict[str, Any]:
        if not target_name:
            return action_failure(
                code="missing_target_name",
                message="agent_profile route needs target_name (for example: clawdclawderberg).",
                say_hint="Ask the user which Moltbook agent profile they want.",
            )

        payload, err = self._api_get(f"{MOLTBOOK_API_PREFIX}agents/profile", params={"name": target_name})
        if err:
            return action_failure(
                code="agent_profile_failed",
                message=f"Could not fetch profile for {target_name}: {err}",
                say_hint="Explain that target profile lookup failed.",
            )
        agent = payload.get("agent") if isinstance(payload, dict) and isinstance(payload.get("agent"), dict) else {}
        if not agent:
            return action_failure(
                code="agent_profile_missing",
                message=f"No profile data found for {target_name}.",
                say_hint="Tell the user no profile was found for that agent.",
            )

        name = _coalesce_str(agent.get("name"), target_name, default=target_name)
        description = _coalesce_str(agent.get("description"), default="")
        karma = _safe_int(agent.get("karma"), 0)
        followers = _safe_int(agent.get("follower_count"), 0)
        following = _safe_int(agent.get("following_count"), 0)
        posts = _safe_int(agent.get("posts_count"), 0)
        comments = _safe_int(agent.get("comments_count"), 0)
        claimed = bool(agent.get("is_claimed"))
        active = bool(agent.get("is_active"))

        summary = (
            f"Profile for {name}:\n"
            f"- Claimed: {claimed} | Active: {active}\n"
            f"- Karma: {karma} | Followers: {followers} | Following: {following}\n"
            f"- Posts: {posts} | Comments: {comments}\n"
            f"- Description: {_limit_text(description, 420) or '(none)'}"
        )
        return action_success(
            facts={
                "agent_name": name,
                "description": description,
                "karma": karma,
                "follower_count": followers,
                "following_count": following,
                "posts_count": posts,
                "comments_count": comments,
                "is_claimed": claimed,
                "is_active": active,
            },
            summary_for_user=summary,
            say_hint="Summarize the requested agent profile.",
        )

    @staticmethod
    def _handle_help() -> Dict[str, Any]:
        summary = (
            "I can answer Moltbook questions like:\n"
            "- what are your latest posts?\n"
            "- who are you following on Moltbook?\n"
            "- what is your profile link?\n"
            "- give me your account summary\n"
            "- show home overview\n"
            "- any activity on your posts?\n"
            "- latest Moltbook announcement\n"
            "- what submolts am I subscribed to or monitoring?\n"
            "- show profile for <agent_name>\n"
            "- list known fellow Tater agents"
        )
        return action_success(
            facts={"supported_routes": sorted(VALID_ROUTES)},
            summary_for_user=summary,
            say_hint="Provide concise examples of supported Moltbook info queries.",
        )

    # ----------------------------
    # Core dispatcher
    # ----------------------------
    async def _handle(self, _args: Dict[str, Any], llm_client) -> Dict[str, Any]:
        _ = llm_client
        args = _args or {}
        route_plan = self._resolve_route(args)
        route = _coalesce_str(route_plan.get("route"), default="help").strip().lower()
        if route not in VALID_ROUTES:
            route = "help"
        limit = _safe_int(route_plan.get("limit"), default=5, minimum=1, maximum=20)
        target_name = _coalesce_str(route_plan.get("target_name"), args.get("target_name"), default="").strip().lower()

        api_required_routes = {
            "latest_posts",
            "following",
            "profile_link",
            "account_summary",
            "home_overview",
            "latest_announcement",
            "activity_on_my_posts",
            "agent_profile",
        }
        if route in api_required_routes:
            api_key = self._resolve_api_key()
            if not api_key:
                return action_failure(
                    code="missing_moltbook_api_key",
                    message=(
                        "Moltbook API key is missing. Set it in moltbook_portal_settings (api_key) "
                        "or in this plugin's MOLTBOOK_API_KEY_OVERRIDE setting."
                    ),
                    say_hint="Tell the user that Moltbook API key configuration is required.",
                )

        if route == "latest_posts":
            return self._handle_latest_posts(limit=limit)
        if route == "following":
            return self._handle_following(limit=limit)
        if route == "profile_link":
            return self._handle_profile_link()
        if route == "account_summary":
            return self._handle_account_summary()
        if route == "home_overview":
            return self._handle_home_overview()
        if route == "latest_announcement":
            return self._handle_latest_announcement()
        if route == "activity_on_my_posts":
            return self._handle_activity_on_my_posts(limit=limit)
        if route == "subscriptions":
            return self._handle_subscriptions()
        if route == "monitoring_submolts":
            return self._handle_monitoring_submolts()
        if route == "fellow_taters":
            return self._handle_fellow_taters()
        if route == "agent_profile":
            return self._handle_agent_profile(target_name=target_name)
        return self._handle_help()

    # ----------------------------
    # Platform handlers
    # ----------------------------
    async def handle_webui(self, args, llm_client):
        return await self._handle(args or {}, llm_client)

    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args or {}, llm_client)

    async def handle_homekit(self, args, llm_client):
        return await self._handle(args or {}, llm_client)

    async def handle_xbmc(self, args, llm_client):
        return await self._handle(args or {}, llm_client)

    async def handle_discord(self, message=None, args=None, llm_client=None):
        return await self._handle(args or {}, llm_client)

    async def handle_telegram(self, update=None, args=None, llm_client=None):
        return await self._handle(args or {}, llm_client)

    async def handle_matrix(self, client=None, room=None, sender=None, body=None, args=None, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        return await self._handle(args or {}, llm_client)

    async def handle_irc(self, bot=None, channel=None, user=None, raw_message=None, args=None, llm_client=None):
        return await self._handle(args or {}, llm_client)


verba = MoltbookLatestPostsPlugin()
