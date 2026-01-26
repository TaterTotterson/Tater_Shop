
<p align="center">
  <img 
    src="https://github.com/user-attachments/assets/e0a6e4ba-fa97-4faa-8028-d8aaee6ffda1" 
    alt="screenshot"
    width="600"
  />
</p>

## 🧩 Tater Plugin Store (Tater Shop)

Welcome to the **Tater Shop** — the live plugin distribution system for Tater.

Plugins are no longer bundled with Tater itself.  
This repository is the **release channel** for all Tater plugins.

👉 Main Tater repository:  
https://github.com/TaterTotterson/Tater

### 💬 Interactive / Conversational Plugins

| Plugin Name                  | Description                                                                 | Platform                                   |
|------------------------------|-----------------------------------------------------------------------------|--------------------------------------------|
| `automatic_plugin`           | Generates images using AUTOMATIC1111 API based on user prompt               | discord, webui                             |
| `broadcast`                  | Sends a whole-house spoken announcement using Home Assistant TTS (Piper, Cloud, etc.) | webui, homeassistant, homekit, xbmc        |
| `comfyui_audio_ace`          | Composes full-length songs using AceStep. Generates lyrics, tags, and MP3s  | discord, webui, homeassistant, matrix      |
| `comfyui_image_plugin`       | Generates images with ComfyUI using custom workflow templates               | discord, webui, matrix                     |
| `comfyui_image_video`        | Animates images into WebP loops using ComfyUI.                              | webui                                      |
| `comfyui_music_video_plugin` | Generates complete AI music videos with lyrics, audio, and visuals          | webui                                      |
| `comfyui_video_plugin`       | Creates videos from prompts using ComfyUI and video workflows               | webui                                      |
| `device_compare`             | Compares two devices, fetching specs and FPS benchmarks from online sources | discord, webui, matrix                     |
| `emoji_ai_responder`         | Picks a relevant emoji based on a message when someone reacts to it         | discord                                   |
| `events_query`               | Summarizes all stored events by time, area, or activity                     | webui, homeassistant, homekit              |
| `find_my_phone`              | Rings your phone using the Home Assistant Companion App notify service      | webui, homeassistant, homekit, xbmc        |
| `ftp_browser`                | Allows users to browse FTP servers via Discord                              | discord                                   |
| `ha_control`                 | Controls Home Assistant devices (lights, switches, climate, etc.)           | webui, homeassistant, homekit, xbmc        |
| `lowfi_video`                | Generates lofi music videos, outputs 20-minute MP4s                         | webui                                      |
| `mister_remote`              | Controls MiSTer FPGA via MiSTer Remote API                                  | discord, webui, irc, homeassistant, matrix, homekit |
| `obsidian_note`              | Creates new notes in your Obsidian vault with AI-generated content          | webui                                      |
| `obsidian_search`            | Searches your Obsidian vault and extracts relevant notes                    | webui                                      |
| `overseerr_trending`         | Lists trending or upcoming movies / TV shows from Overseerr                 | discord, webui, irc, matrix, homekit       |
| `overseerr_details`          | Fetches full details for a specific movie or TV show from Overseerr         | discord, webui, irc, matrix, homeassistant, homekit |
| `overseerr_request`          | Adds a movie or TV show request to Overseerr                                | webui, homeassistant, homekit              |
| `premiumize_download`        | Checks Premiumize for cached file links and returns downloads               | discord, webui, irc, matrix                |
| `premiumize_torrent`         | Checks if a torrent is cached on Premiumize                                 | discord                                   |
| `sftpgo_account`             | Creates SFTPGo user accounts and credentials                                | discord, webui, irc, matrix                |
| `sftpgo_activity`            | Views SFTPGo user activity like transfers and sessions                      | discord, webui, irc, matrix                |
| `tater_gits_add_feed`        | Adds a GitHub releases feed to the Tater Gits watcher                        | discord, webui, irc                        |
| `unifi_network`              | Queries UniFi Network (clients/devices/site health) via the Integration API | webui, homeassistant, homekit, xbmc        |
| `unifi_protect`              | Queries UniFi Protect sensors and camera snapshots via the Integration API  | webui, homeassistant, homekit, xbmc        |
| `vision_describer`           | Analyzes uploaded images and returns AI-generated descriptions              | discord, webui, matrix                     |
| `voicepe_remote_timer`       | Starts a device-local timer on a Voice PE via ESPHome                       | webui, homeassistant, homekit, xbmc        |
| `weather_forecast`           | Gets current weather + forecast (optional AQI/pollen/alerts) from WeatherAPI.com | discord, webui, irc, homeassistant, matrix, homekit, xbmc |
| `web_search`                 | Performs web searches to help answer questions                              | discord, webui, irc, homeassistant, matrix, homekit, xbmc |
| `web_summary`                | Summarizes content from a provided URL                                      | discord, webui, irc, matrix                |
| `webdav_browser`             | Allows browsing and downloading files from WebDAV servers                   | discord                                   |
| `youtube_summary`            | Summarizes YouTube videos                                                   | discord, webui, irc, matrix                |

---

## ⚙️ Automation Plugins (Home Assistant)

These plugins are **not conversational**.  
They are designed to be triggered from **Home Assistant automations** using the  
**“Call Tater automation tool”** action provided by the  
[Tater Automations](https://github.com/TaterTotterson/tater_automations) custom component.

| Plugin Name              | Description |
|--------------------------|-------------|
| `camera_event`           | Captures a camera snapshot on motion, describes it with Vision AI, and logs a structured event with cooldown. |
| `doorbell_alert`         | Doorbell-triggered snapshot + Vision AI description, spoken announcement, and event logging. |
| `events_query_brief`     | Returns a very short, sensor-safe summary of recent household events by area and timeframe. |
| `phone_events_alert`     | Captures a camera snapshot on trigger, describes what’s happening with Vision AI, and sends a high-priority phone notification with cooldown control. |
| `weather_query_brief`    | Reads recent Home Assistant weather sensors and returns a short, dashboard-safe weather summary. |
| `zen_greeting`           | Generates a calm, AI-written Zen-style message of the day for dashboards and daily automations. |

---

## 📡 RSS Feed Watcher (Built-in)

This system runs in the background and posts summarized RSS feed updates.

| Plugin Name        | Description                                           | Type            | Platform            |
|--------------------|-------------------------------------------------------|------------------|---------------------|
| `discord_notifier` | Posts RSS updates directly to a Discord channel       | RSS Notifier     | plugin-triggered    |
| `telegram_notifier`| Sends RSS updates to Telegram                         | RSS Notifier     | plugin-triggered    |
| `wordpress_poster` | Posts RSS updates to WordPress                        | RSS Notifier     | plugin-triggered    |
| `ntfy_notifier`    | Sends RSS updates to an ntfy topic                    | RSS Notifier     | plugin-triggered    |
| `list_feeds`       | Lists all watched RSS feeds                           | RSS Management   | discord, webui, irc |
| `watch_feed`       | Adds a feed to the RSS watcher                        | RSS Management   | discord, webui, irc |
| `unwatch_feed`     | Removes a feed from the RSS watcher                   | RSS Management   | discord, webui, irc |

---
