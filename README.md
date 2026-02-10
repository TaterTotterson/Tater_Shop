<p align="center">
  <img 
    src="https://github.com/user-attachments/assets/e0a6e4ba-fa97-4faa-8028-d8aaee6ffda1" 
    alt="screenshot"
    width="300"
  />
</p>

<!-- AUTO:PLUGIN_TABLES:BEGIN -->
## 🧩 Tater Plugin Store (Tater Shop)

### 💬 Interactive / Conversational Plugins

| Plugin Name | Description | Platform |
|------------|-------------|----------|
| `ai_tasks` | Schedule an AI task. Supports one-shot or recurring runs via every_seconds. At run time, AI can answer directly or call one tool before sending the final response. | discord, irc, matrix, homeassistant, telegram, webui |
| `automatic_plugin` | Generate an image from a text prompt using your Automatic1111 server. | discord, webui, telegram |
| `broadcast` | Send a one-time spoken announcement to your Home Assistant media players. | homeassistant, homekit, xbmc, webui, discord, telegram, matrix, irc |
| `comfyui_audio_ace` | Compose a music track from a prompt with ComfyUI Audio Ace. | discord, webui, homeassistant, matrix, telegram |
| `comfyui_image_plugin` | Generate a still image from a text prompt using your ComfyUI workflow. | discord, webui, matrix, telegram |
| `comfyui_image_video` | Animate the most recent image in chat into a looping WebP or MP4 via ComfyUI. | webui |
| `comfyui_music_video` | Build a full AI music video—lyrics, music, and animated visuals—using ComfyUI. | webui |
| `comfyui_video_plugin` | Create a short video from a text prompt by stitching ComfyUI-generated clips. | webui |
| `device_compare` | Compare two devices with spec tables and per-game FPS benchmarks. | discord, webui, matrix, telegram |
| `emoji_ai_responder` | Pick a contextual emoji reaction. | passive |
| `events_query` | Answer questions about stored household events by area and timeframe. | webui, homeassistant, homekit, discord, telegram, matrix, irc |
| `find_my_phone` | Ping or ring your phone through Home Assistant so you can locate it. | webui, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `ftp_browser` | Browse and download files from the configured FTP server. | discord |
| `get_notifications` | Fetch queued notifications from the Home Assistant bridge. | webui, homeassistant, discord, telegram, matrix, irc |
| `ha_control` | Control Home Assistant devices. | homeassistant, webui, xbmc, homekit, discord, telegram, matrix, irc |
| `lowfi_video` | Create a cozy lofi video by generating music and looping a matching animation. | webui |
| `mister_remote` | Control your MiSTer FPGA setup—launch games, check status, or take screenshots. | discord, webui, irc, homeassistant, matrix, homekit, telegram |
| `music_assistant` | Play music and control playback via Music Assistant in Home Assistant. | webui, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `obsidian_note` | Save markdown content to Obsidian with predictable note naming, append/overwrite controls, and tags. | webui |
| `obsidian_search` | Search your Obsidian vault quickly and return relevant snippets with optional AI synthesis. | webui |
| `overseerr_details` | Fetch details for a specific title from Overseerr. | discord, webui, irc, homeassistant, matrix, homekit, telegram |
| `overseerr_request` | Request a movie or TV show in Overseerr by title. | webui, homeassistant, homekit, discord, telegram, matrix, irc |
| `overseerr_trending` | List trending or upcoming movies/TV from Overseerr. | discord, webui, irc, homeassistant, matrix, homekit, telegram |
| `premiumize_download` | Check if a link is cached on Premiumize and return direct download links. | discord, webui, irc, matrix, telegram |
| `premiumize_torrent` | Check the latest torrent or magnet against Premiumize cache and provide direct links. | discord, matrix |
| `rss_list` | Show the RSS feeds currently being watched. | discord, webui, irc, matrix, telegram |
| `rss_unwatch` | Remove an RSS feed from the watch list. | discord, webui, irc, matrix, telegram |
| `rss_watch` | Add an RSS/Atom feed to the watch list and post only the newest item once. | discord, webui, irc, matrix, telegram |
| `send_message` | Queue a message via the notifier system. If no destination is provided, it defaults to the origin platform (Discord/IRC/Matrix/Home Assistant/Telegram). Room/channel names like #tater are accepted for routing. When used from Discord, attached files are forwarded automatically. | discord, irc, matrix, homeassistant, telegram, webui |
| `sftpgo_account` | Create an SFTPGo account for the user and return login details. | discord, webui, irc, matrix, telegram |
| `sftpgo_activity` | Show current connection activity on the SFTPGo server. | discord, webui, irc, matrix, telegram |
| `tater_gits_add_feed` | Add a GitHub releases feed to the tater-gits watcher with smart naming. | webui, discord, irc, matrix, telegram |
| `unifi_network` | Fetch UniFi Network sites/clients/devices via the official API and let the LLM answer questions from computed facts + compact lists. | webui, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_protect` | Get UniFi Protect sensor status and camera snapshot vision summaries. | webui, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `voicepe_remote_timer` | Start, cancel, or check a Voice PE (ESPHome) timer device. | homeassistant, homekit, xbmc, webui, discord, telegram, matrix, irc |
| `weather_forecast` | Fetch WeatherAPI.com weather and answer only what the user asked (LLM-guided). | discord, webui, irc, homeassistant, matrix, homekit, xbmc, telegram |
| `web_summary` | Summarize the main points of a webpage from its URL. | discord, webui, irc, matrix, telegram |
| `webdav_browser` | Browse and download files from the configured WebDAV server. | discord |
| `youtube_summary` | Summarize a YouTube video using its transcript. | discord, webui, irc, matrix, telegram |

### ⚙️ Automation Plugins (Home Assistant)

| Plugin Name | Description | Platform |
|------------|-------------|----------|
| `camera_event` | Capture a camera snapshot, describe it, store an event, and optionally notify via Home Assistant Notifier. | automation |
| `doorbell_alert` | Handle doorbell events: snapshot, describe with vision, announce, and log notifications. | automation |
| `events_query_brief` | Produce a terse dashboard-friendly summary of recent home events and optionally write it to Home Assistant. | automation |
| `weather_brief` | Give a short automation-friendly recap of recent weather conditions. | automation |
| `zen_greeting` | Generate a calming daily greeting and message for dashboards. | automation |

### 📡 RSS Notifier Plugins

*(none)*

<!-- AUTO:PLUGIN_TABLES:END -->
