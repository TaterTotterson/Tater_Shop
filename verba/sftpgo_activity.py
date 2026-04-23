# verba/sftpgo_activity.py
import os
import asyncio
import aiohttp
import base64
from helpers import redis_client
from verba_base import ToolVerba
from verba_result import action_failure, action_success

class SFTPGoActivityPlugin(ToolVerba):
    name = "sftpgo_activity"
    verba_name = "SFTPGo Activity"
    version = "1.0.2"
    min_tater_version = "59"
    usage = '{"function":"sftpgo_activity","arguments":{}}'
    description = "Retrieves current connection activity from the SFTPGo server."
    verba_dec = "Show current connection activity on the SFTPGo server."
    pretty_name = "Checking FTP Activity"
    settings_category = "SFTPGo"
    waiting_prompt_template = "Write a friendly message telling {mention} you’re accessing the server to see who’s using it now! Only output that message."
    platforms = ["discord", "webui", "macos", "irc", "meshtastic", "matrix", "telegram"]
    required_settings = {
        "SFTPGO_API_URL": {
            "label": "SFTPGo API URL",
            "type": "text",
            "default": "https://localhost",
            "description": "Enter the base URL for the SFTPGo API (do not include /api/v2)."
        },
        "SFTPGO_USERNAME": {
            "label": "SFTPGo Username",
            "type": "text",
            "default": "username",
            "description": "The username to authenticate with the SFTPGo API."
        },
        "SFTPGO_PASSWORD": {
            "label": "SFTPGo Password",
            "type": "password",
            "default": "password",
            "description": "The password to authenticate with the SFTPGo API."
        }
    }
    when_to_use = ""
    common_needs = []
    missing_info_prompts = []


    def get_sftpgo_settings(self):
        client = redis_client
        key = f"verba_settings:{self.settings_category}"
        settings = client.hgetall(key)
        defaults = {
            "SFTPGO_API_URL": "https://localhost",
            "SFTPGO_USERNAME": "username",
            "SFTPGO_PASSWORD": "password"
        }
        for k, default in defaults.items():
            if not settings.get(k):
                settings[k] = default
        if "/api/v2" not in settings["SFTPGO_API_URL"]:
            settings["SFTPGO_API_URL"] = settings["SFTPGO_API_URL"].rstrip("/") + "/api/v2"
        return settings

    async def get_jwt_token(self, settings):
        auth_header = base64.b64encode(
            f"{settings['SFTPGO_USERNAME']}:{settings['SFTPGO_PASSWORD']}".encode("utf-8")
        ).decode("ascii")
        connector = aiohttp.TCPConnector(ssl=False)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    f"{settings['SFTPGO_API_URL']}/token",
                    headers={"Authorization": f"Basic {auth_header}"}
                ) as response:
                    if response.status == 200:
                        return (await response.json()).get("access_token")
        except Exception as e:
            print(f"JWT error: {e}")
        return None

    async def _get_activity_summary(self, llm_client):
        settings = self.get_sftpgo_settings()
        jwt_token = await self.get_jwt_token(settings)
        if not jwt_token:
            return action_failure(
                code="sftpgo_auth_failed",
                message="Failed to obtain JWT token.",
                say_hint="Explain SFTPGo authentication failed and ask to verify API settings.",
            )

        connector = aiohttp.TCPConnector(ssl=False)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    f"{settings['SFTPGO_API_URL']}/connections",
                    headers={"Authorization": f"Bearer {jwt_token}"}
                ) as resp:
                    if resp.status != 200:
                        return action_failure(
                            code="sftpgo_connections_failed",
                            message=f"Failed to retrieve connections: HTTP {resp.status}.",
                            say_hint="Explain that retrieving SFTPGo connections failed.",
                        )

                    conns = await resp.json()
                    if not conns:
                        return action_success(
                            facts={"active_count": 0, "connections": []},
                            summary_for_user="No active SFTPGo connections.",
                            say_hint="Report that there are no active SFTPGo connections.",
                        )

                    compact = []
                    for c in conns[:25]:
                        if not isinstance(c, dict):
                            continue
                        compact.append(
                            {
                                "username": c.get("username"),
                                "client_version": c.get("client_version"),
                                "protocol": c.get("protocol"),
                                "last_activity": c.get("last_activity"),
                            }
                        )
                    return action_success(
                        facts={"active_count": len(conns), "connections": compact},
                        summary_for_user=f"Found {len(conns)} active SFTPGo connection(s).",
                        say_hint="Report active connection count and key connection details.",
                    )

        except Exception as e:
            return action_failure(
                code="sftpgo_activity_failed",
                message=f"Error retrieving activity: {e}",
                say_hint="Explain that reading SFTPGo activity failed and suggest retrying.",
            )

    # Discord
    async def handle_discord(self, message, args, llm_client):
        return await self._get_activity_summary(llm_client)

    # IRC (return string; the platform will send it)

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
        return await self._get_activity_summary(llm_client)

    # WebUI
    async def handle_webui(self, args, llm_client):
        try:
            asyncio.get_running_loop()
            return await self._get_activity_summary(llm_client)
        except RuntimeError:
            return asyncio.run(self._get_activity_summary(llm_client))

    # Telegram

    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args, llm_client)

    # Matrix
    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        """
        Return plain text; Matrix platform will post it (and chunk if needed).
        """
        return await self._get_activity_summary(llm_client)


verba = SFTPGoActivityPlugin()
