<p align="center">
  <img 
    src="https://github.com/user-attachments/assets/ec6b41ef-4ba1-4f41-b6f6-a0a9be0a6786" 
    alt="screenshot"
    width="300"
  />
</p>

<!-- AUTO:VERBA_TABLES:BEGIN -->
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

## 🧩 Verba Store

### 💬 Interactive / Conversational Verbas

| Verba ID | Description | Portals |
|------------|-------------|----------|
| `automatic_plugin` | Create one image from a natural-language image request. | discord, webui, macos, telegram |
| `broadcast` | Send a one-time spoken announcement to your Home Assistant media players. | homeassistant, homekit, xbmc, webui, macos, discord, telegram, matrix, irc |
| `comfyui_audio_ace` | Compose a music track from a prompt with ComfyUI Audio Ace. | discord, webui, macos, homeassistant, matrix, telegram |
| `comfyui_image_plugin` | Generate a still image from a text prompt using your ComfyUI workflow. | discord, webui, macos, matrix, telegram |
| `comfyui_image_video` | Animate the most recent image in chat into a looping WebP or MP4 via ComfyUI. | webui, macos |
| `comfyui_music_video` | Build a full AI music video—lyrics, music, and animated visuals—using ComfyUI. | webui, macos |
| `comfyui_video_plugin` | Create a short video from a text prompt by stitching ComfyUI-generated clips. | webui, macos |
| `device_compare` | Natural-language device comparison with spec tables and optional per-game FPS benchmarks. | discord, webui, macos, matrix, telegram |
| `discord_admin_response_mode` | Discord response-mode channel controls. | discord |
| `discord_admin_roles` | Discord role listing and role assignment commands. | discord |
| `discord_admin_setup` | Discord server setup and structural admin changes. | discord |
| `events_query` | Natural-language semantic event-history search. | webui, macos, homeassistant, homekit, discord, telegram, matrix, irc |
| `find_my_phone` | Ping or ring your phone through Home Assistant so you can locate it. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `ftp_browser` | Browse and download files from the configured FTP server. | discord |
| `get_notifications` | Fetch queued notifications from the Home Assistant bridge. | webui, macos, homeassistant, discord, telegram, matrix, irc |
| `ha_climate` | Control Home Assistant thermostats and HVAC entities. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_covers` | Control Home Assistant covers and garage doors. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_fans` | Control Home Assistant fans. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_lights` | Control Home Assistant lights. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_locks` | Control Home Assistant lock entities. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_media_players` | Control Home Assistant media players. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_remotes` | Control Home Assistant remotes and remote commands. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_scenes` | Control Home Assistant scenes. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_scripts` | Control Home Assistant scripts. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_sensors` | Read Home Assistant sensor entities. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_switches` | Control Home Assistant switches and plugs. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `ha_temperature` | Read Home Assistant ambient temperatures. | homeassistant, webui, macos, xbmc, homekit, discord, telegram, matrix, irc |
| `jackett_recent_uploads` | List and rank recent Jackett uploads across selected indexers. | discord, webui, macos, irc, homeassistant, homekit, matrix, telegram, xbmc |
| `jackett_search_torrents` | Run Jackett torrent searches with filters and ranked results. | discord, webui, macos, irc, homeassistant, homekit, matrix, telegram, xbmc |
| `joke_api` | Fetch one joke from JokeAPI. | webui, macos, homeassistant, homekit, discord, telegram, matrix, irc, xbmc |
| `lowfi_video` | Create a cozy lofi video by generating music and looping a matching animation. | webui, macos |
| `mister_remote` | Control your MiSTer FPGA setup—launch games, check status, or take screenshots. | discord, webui, macos, irc, homeassistant, matrix, homekit, telegram |
| `moltbook_account_summary` | Moltbook account snapshot with profile link and key counters. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_activity_on_posts` | Read /home activity buckets for your posts and summarize engagement. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_agent_profile` | Lookup target Moltbook agent profile details. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_fellow_taters` | Show tracked fellow Tater Moltbook agents. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_following` | List known followed agents and active authors from following feed. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_home_overview` | Moltbook /home dashboard overview with unread/activity counters. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_latest_announcement` | Get the newest Moltbook announcement/news entry. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_latest_posts` | Fetch latest posts authored by this Moltbook agent. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_monitoring_submolts` | Read Moltbook portal settings for submolts to monitor/prefer/avoid. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_profile_link` | Get the current Moltbook profile URL. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `moltbook_subscriptions` | Show subscribed submolts from Moltbook portal state. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `music_assistant` | Play music and control playback via Music Assistant in Home Assistant. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `obsidian_note` | Save markdown content to Obsidian with predictable note naming, append/overwrite controls, and tags. | webui, macos |
| `obsidian_search` | Search your Obsidian vault quickly and return relevant snippets with optional AI synthesis. | webui, macos |
| `overseerr_details` | Fetch details for a specific movie or TV show from Overseerr. | discord, webui, macos, irc, homeassistant, matrix, homekit, telegram |
| `overseerr_request` | Request a movie or TV show in Overseerr by title. | webui, macos, homeassistant, homekit, discord, telegram, matrix, irc |
| `overseerr_trending` | List trending or upcoming movies/TV from Overseerr. | discord, webui, macos, irc, homeassistant, matrix, homekit, telegram |
| `premiumize_add_transfer` | Create Premiumize transfers from explicit magnet or HTTP(S) source links. | discord, webui, macos, irc, matrix, telegram, homeassistant, homekit, xbmc |
| `premiumize_check_transfer` | Inspect a Premiumize transfer and report status, progress, and related files. | discord, webui, macos, irc, matrix, telegram, homeassistant, homekit, xbmc |
| `premiumize_get_links` | Get Premiumize stream/download links by magnet/URL or by chosen cloud item. | discord, webui, macos, irc, matrix, telegram, homeassistant, homekit, xbmc |
| `premiumize_list_files` | List Premiumize cloud folder contents with file/folder counts. | discord, webui, macos, irc, matrix, telegram, homeassistant, homekit, xbmc |
| `premiumize_list_transfers` | Show Premiumize transfer queue/status with active, finished, and failed counts. | discord, webui, macos, irc, matrix, telegram, homeassistant, homekit, xbmc |
| `sftpgo_account` | Create an SFTPGo account for the user and return login details. | discord, webui, macos, irc, matrix, telegram |
| `sftpgo_activity` | Show current connection activity on the SFTPGo server. | discord, webui, macos, irc, matrix, telegram |
| `tater_gits_add_feed` | Add a GitHub releases feed to the tater-gits watcher with smart naming. | webui, macos, discord, irc, matrix, telegram |
| `unifi_network_clients` | List UniFi clients with wired/wireless counts and client details. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_network_devices` | List UniFi devices with online/offline status and core details. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_network_health` | Get UniFi network health summary including client/device totals and offline device signals. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_network_lookup` | Look up a UniFi client/device and return matching identity and network details. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_protect_area` | Describe activity across multiple cameras in a named area. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_protect_camera` | Describe a single UniFi Protect camera snapshot. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_protect_camera_info` | List UniFi Protect cameras. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `unifi_protect_sensors` | Check UniFi Protect sensor status and single-sensor detail. | webui, macos, homeassistant, homekit, xbmc, discord, telegram, matrix, irc |
| `voicepe_remote_timer` | Start, cancel, or check a Voice PE (ESPHome) timer device. | homeassistant, homekit, xbmc, webui, macos, discord, telegram, matrix, irc |
| `weather_forecast` | Fetch WeatherAPI.com weather and answer only what the user asked (LLM-guided). | discord, webui, macos, irc, homeassistant, matrix, homekit, xbmc, telegram |
| `webdav_browser` | Browse and download files from the configured WebDAV server. | discord |
| `youtube_summary` | Summarize a YouTube video using its transcript. | discord, webui, macos, irc, matrix, telegram |

### ⚙️ Automation Verbas (Home Assistant)

| Verba ID | Description | Portals |
|------------|-------------|----------|
| `camera_event` | Capture a camera snapshot, describe it, store an event, and optionally notify via Home Assistant Notifier. | automation |
| `doorbell_alert` | Handle doorbell events: snapshot, describe with vision, announce, and log notifications. | automation |
| `events_query_brief` | Produce a terse dashboard-friendly summary of recent home events and optionally write it to Home Assistant. | automation |
| `weather_brief` | Give a short automation-friendly recap of recent weather conditions. | automation |
| `zen_greeting` | Generate a calming daily greeting and message for dashboards. | automation |

### 📡 RSS Notifier Verbas

*(none)*

<!-- AUTO:VERBA_TABLES:END -->
