# Changelog

## 2026-11-19 — Meshtastic Core 0.3.6

- Start an explicit 10-second discovery window when the user scans for radios;
  use Tater 3.0.5's bounded Bluetooth discovery API instead of background room
  presence observations. Require compatible on-demand proxy firmware.
- Use generic satellite Bluetooth labels for Native ESP32, Echo, ThirdReality,
  and supported Linux proxies. Preserve transport IDs, saved satellite/radio
  selections, bond keys, chat history, and the compatibility bridge transport.
- Keep existing active radio connections open when discovery stops. Older Tater
  versions show a clear upgrade message when discovery is requested.

Requires Tater 3.0.5 (build 229) for satellite discovery. Pairing and physical
connection/reconnect/voice coexistence testing remain pending.

## 2026-11-18 — Music Core 3.6.6

Adds the Music workspace support used by Tater 3.0.2:

- Browse albums, artists, genres, and paginated song search without interrupting
  playback. Play a specific catalog track or the displayed selection.
- Match category selections exactly, keeping similarly named album editions
  separate, and order multi-disc albums by disc and track number.
- Publish playback progress, seek availability, repeat state, recent listening,
  and playback warnings for the persistent player.
- Show stereo pairs as one destination with two named left/right speakers.

Existing settings, playback routes, sync tests, recommendations, voice tools, and
legacy UI actions remain available. Update Tater to 3.0.2 for the new workspace;
older Tater versions can continue using the existing Music Core interface.

## 2026-11-16 — Awareness Core 4.12.10

Removed the Cameras, Doorbells, Sensors, and List View toggles from Awareness
event history. All event types now appear together in the regular timeline cards
with their descriptions and media previews. Previously saved filter and list
view choices no longer hide events or suppress previews. Monitoring settings,
notifications, and event pagination are unchanged.

## 2026-11-15 — Library first Core workspaces

All twelve Cores now publish the screen metadata used by Tater 3.0.1's focused
workspaces. Update Tater and your installed Cores for the complete visual refresh.
Existing Core behavior, settings, tool descriptions, and action handlers are
unchanged; older apps can continue using the existing UI contract.

| Core | Previous version | New version | Workspace changes |
| --- | --- | --- | --- |
| AI Task Scheduler | 1.2.6 | 1.2.7 | Task list, schedule and delivery details, explicit task editor |
| Automation | 1.6.5 | 1.6.6 | Rule details, run timeline, trigger/condition/action/audio editor |
| Awareness | 4.12.8 | 4.12.9 | Event timeline, media previews, monitored source details |
| Environment | 1.6.4 | 1.6.5 | Current conditions, room readings, forecasts, source context |
| Guardian | 1.3.14 | 1.3.15 | Network briefings, device review, change timeline |
| Memory | 1.1.4 | 1.1.5 | Remembered context, people, insights, data management |
| Meshtastic | 0.3.4 | 0.3.5 | Radio context, node details, pairing views |
| Music | 3.6.4 | 3.6.5 | Library, playlist, recommendation screen metadata |
| Personal | 1.0.59 | 1.0.60 | Agenda, personal context, account details, cleanup views |
| Reachy | 1.2.7 | 1.2.8 | Per-device behavior workspace and existing control groups |
| RSS | 1.0.13 | 1.0.14 | Feed details, destinations, explicit feed editor |
| Tater Tube | 1.5.3 | 1.5.4 | Media context, pick cards, viewing history |

The Core manifest includes the new versions and refreshed file checksums.
Workspace metadata tests validate the screen recipes and manifest records.
