# Tater Assistant Wiki

Static wiki generator for the Tater Assistant docs site.

This repo is set up for a Linux host with this layout:

```text
/home/taterassistant/
  public_html/
    assets/
    ...
  scripts/
    build_wiki.py
    sync_wiki_sources.py
    Tater/
    Tater_Shop/
```

## What it does

- builds the website into `public_html`
- reads core runtime, Cerberus, and kernel-tool info from `Tater`
- reads portal source/settings from `Tater_Shop/portals` (no fallback to `Tater/portals`)
- reads core source/settings from `Tater_Shop/cores` (no fallback to `Tater/cores`)
- reads Verba Plugin inventory from `Tater_Shop/manifest.json`
- reads plugin detail metadata from the actual plugin files in `Tater_Shop/plugins`
- can clone/update both repos automatically and rebuild the site when they change

## Main files

- `scripts/build_wiki.py`
  Generates the HTML site in `public_html`
- `scripts/sync_wiki_sources.py`
  Clones or updates `Tater` and `Tater_Shop`, then runs the wiki build when source heads change
- `scripts/.wiki-sync-state.json`
  Stores the last built source heads so unchanged runs can skip the rebuild
- `public_html/assets/`
  Shared CSS, JS, and images used by the generated site

## Requirements

- Python 3
- Git

## Manual build

If `scripts/Tater` and `scripts/Tater_Shop` already exist:

```bash
python3 /home/taterassistant/scripts/build_wiki.py
```

## Manual sync and rebuild

This is the normal command to run by hand:

```bash
python3 /home/taterassistant/scripts/sync_wiki_sources.py
```

What it does:

- clones `Tater` into `scripts/Tater` if missing
- clones `Tater_Shop` into `scripts/Tater_Shop` if missing
- fast-forwards clean repos
- skips rebuilds when nothing changed
- blocks rebuilds if a source repo is dirty or diverged

Useful flags:

```bash
python3 /home/taterassistant/scripts/sync_wiki_sources.py --force-build
python3 /home/taterassistant/scripts/sync_wiki_sources.py --skip-fetch
python3 /home/taterassistant/scripts/sync_wiki_sources.py --allow-dirty-build
```

## Force rebuild

To force a rebuild even when the repo heads did not change:

```bash
python3 /home/taterassistant/scripts/sync_wiki_sources.py --force-build
```

To reset the sync state and let the next normal run rebuild:

```bash
rm -f /home/taterassistant/scripts/.wiki-sync-state.json
```

## Cron / Webmin

For a headless Linux server, use cron.

Cron line:

```cron
0 4,16 * * * /bin/bash -lc '/usr/bin/python3 /home/taterassistant/scripts/sync_wiki_sources.py >> /home/taterassistant/scripts/wiki-sync.log 2>&1'
```

Webmin cron job values:

- user: `taterassistant`
- minute: `0`
- hour: `4,16`
- command: `/bin/bash -lc '/usr/bin/python3 /home/taterassistant/scripts/sync_wiki_sources.py >> /home/taterassistant/scripts/wiki-sync.log 2>&1'`

For the Linux-specific sync notes, see `docs/auto-sync-linux.md`.

## Notes

- Keep `public_html/assets` in place. The generator writes HTML pages, but the site assets live there.
- Generated HTML should be treated as build output.
- The source of truth for plugin inventory, portal source files, and core source files is `Tater_Shop`.
