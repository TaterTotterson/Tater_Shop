<p align="center">
  <img 
    src="https://github.com/user-attachments/assets/b5eded08-f1d4-4af7-a50e-75db228a327e" 
    alt="screenshot"
    width="300"
  />
</p>

<!-- AUTO:PLUGIN_TABLES:BEGIN -->
## 🧠 Core Store

| Core ID | Module | Description |
|-------------|--------|-------------|
| `ai_task` | `ai_task_core` | AI Task Scheduler Core integration core for Tater. |
| `memory` | `memory_core` | Memory Core integration core for Tater. |
| `rss` | `rss_core` | RSS Core integration core for Tater. |

## 🌐 Portal Store

| Portal ID | Module | Description |
|-------------|--------|-------------|
| `discord` | `discord_portal` | Discord integration portal for Tater. |
| `ha_automations` | `ha_automations_portal` | Automation integration portal for Tater. |
| `homeassistant` | `homeassistant_portal` | Home Assistant integration portal for Tater. |
| `homekit` | `homekit_portal` | HomeKit / Siri integration portal for Tater. |
| `irc` | `irc_portal` | IRC integration portal for Tater. |
| `macos` | `macos_portal` | macOS integration portal for Tater. |
| `matrix` | `matrix_portal` | Matrix integration portal for Tater. |
| `moltbook` | `moltbook_portal` | Moltbook social/research integration portal for Tater. |
| `telegram` | `telegram_portal` | Telegram integration portal for Tater. |
| `xbmc` | `xbmc_portal` | XBMC / Original Xbox integration portal for Tater. |

## 🧩 Verba Plugin Store

### 💬 Interactive / Conversational Plugins

| Plugin Name | Description | Portals |
|------------|-------------|----------|
| `ai_tasks` | Schedule recurring AI tasks using natural phrases or cron-like schedules in local time. Prefer passing `task_prompt` as a concise execution instruction and `when`/`cron` for scheduling. | discord, irc, matrix, homeassistant, telegram, webui, macos |
| `automatic_plugin` | Create one image from a natural-language image request. | discord, webui, macos, telegram |
| `broadcast` | Send a one-time spoken announcement to your Home Assistant media players. | homeassistant, homekit, xbmc, webui, macos, discord, telegram, matrix, irc |
| `comfyui_audio_ace` | Compose a music track from a prompt with ComfyUI Audio Ace. | discord, webui, macos, homeassistant, matrix, telegram |
| `comfyui_image_plugin` | Generate a still image from a text prompt using your ComfyUI workflow. | discord, webui, macos, matrix, telegram |
| `comfyui_image_video` | Animate the most recent image in chat into a looping WebP or MP4 via ComfyUI. | webui, macos |
| `comfyui_music_video` | Build a full AI music video—lyrics, music, and animated visuals—using ComfyUI. | webui, macos |
| `comfyui_video_plugin` | Create a short video from a text prompt by stitching ComfyUI-generated clips. | webui, macos |
| `device_compare` | Natural-language device comparison with spec tables and optional per-game FPS benchmarks. | discord, webui, macos, matrix, telegram |
| `discord_admin` | Discord server setup, admin changes, and current-room always-respond control. | discord |
| `events_query` | Natural-language semantic event-history search. | webui, macos, homeassistant, homekit, discord, telegram, matrix, irc |
| `find_my_phone` | Ping or ring your phone through Home Assistant so you can locate it. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `ftp_browser` | Browse and download files from the configured FTP server. | discord |
| `get_notifications` | Fetch queued notifications from the Home Assistant bridge. | webui, macos, homeassistant, discord, telegram, matrix, irc |
| `ha_control` | Control Home Assistant devices. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `jackett_search` | Search Jackett, browse recent uploads, inspect indexers, and return structured torrent results. | discord, webui, macos, irc, homeassistant, homekit, matrix, telegram, xbmc |
| `joke_api` | Fetch one joke from JokeAPI. | webui, macos, homeassistant, homekit, discord, telegram, matrix, irc, xbmc |
| `lowfi_video` | Create a cozy lofi video by generating music and looping a matching animation. | webui, macos |
| `mister_remote` | Control your MiSTer FPGA setup—launch games, check status, or take screenshots. | discord, webui, macos, irc, homeassistant, matrix, homekit, telegram |
| `moltbook_info` | Natural-language Moltbook account intelligence and status lookups. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `music_assistant` | Play music and control playback via Music Assistant in Home Assistant. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `obsidian_note` | Save markdown content to Obsidian with predictable note naming, append/overwrite controls, and tags. | webui, macos |
| `obsidian_search` | Search your Obsidian vault quickly and return relevant snippets with optional AI synthesis. | webui, macos |
| `overseerr_details` | Fetch details for a specific movie or TV show from Overseerr. | discord, webui, macos, irc, homeassistant, matrix, homekit, telegram |
| `overseerr_request` | Request a movie or TV show in Overseerr by title. | webui, macos, homeassistant, homekit, discord, telegram, matrix, irc |
| `overseerr_trending` | List trending or upcoming movies/TV from Overseerr. | discord, webui, macos, irc, homeassistant, matrix, homekit, telegram |
| `premiumize_download` | Send explicit magnet or HTTP(S) links to Premiumize, monitor transfer progress, browse cloud files, and retrieve direct or stream links. | discord, webui, macos, irc, matrix, telegram, homeassistant, homekit, xbmc |
| `sftpgo_account` | Create an SFTPGo account for the user and return login details. | discord, webui, macos, irc, matrix, telegram |
| `sftpgo_activity` | Show current connection activity on the SFTPGo server. | discord, webui, macos, irc, matrix, telegram |
| `tater_gits_add_feed` | Add a GitHub releases feed to the tater-gits watcher with smart naming. | webui, macos, discord, irc, matrix, telegram |
| `unifi_network` | Fetch UniFi Network sites/clients/devices via the official API and answer natural-language requests. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_protect` | Get UniFi Protect camera snapshot descriptions and sensor status. Preferred for 'what do you see' or 'what is happening' camera questions. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `voicepe_remote_timer` | Start, cancel, or check a Voice PE (ESPHome) timer device. | homeassistant, homekit, xbmc, webui, macos, discord, telegram, matrix, irc |
| `weather_forecast` | Fetch WeatherAPI.com weather and answer only what the user asked (LLM-guided). | discord, webui, macos, irc, homeassistant, matrix, homekit, xbmc, telegram |
| `webdav_browser` | Browse and download files from the configured WebDAV server. | discord |
| `youtube_summary` | Summarize a YouTube video using its transcript. | discord, webui, macos, irc, matrix, telegram |

### ⚙️ Automation Plugins (Home Assistant)

| Plugin Name | Description | Portals |
|------------|-------------|----------|
| `camera_event` | Capture a camera snapshot, describe it, store an event, and optionally notify via Home Assistant Notifier. | automation |
| `doorbell_alert` | Handle doorbell events: snapshot, describe with vision, announce, and log notifications. | automation |
| `events_query_brief` | Produce a terse dashboard-friendly summary of recent home events and optionally write it to Home Assistant. | automation |
| `weather_brief` | Give a short automation-friendly recap of recent weather conditions. | automation |
| `zen_greeting` | Generate a calming daily greeting and message for dashboards. | automation |

### 📡 RSS Notifier Plugins

*(none)*

<!-- AUTO:PLUGIN_TABLES:END -->
