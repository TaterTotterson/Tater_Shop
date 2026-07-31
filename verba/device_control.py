from category_device_control import CategoryDeviceControlBase


class DeviceControlPlugin(CategoryDeviceControlBase):
    name = "device_control"
    verba_name = "Device Control"
    pretty_name = "Device Control"
    version = "1.0.0"
    min_tater_version = "98.4"
    settings_category = "Device Control"
    platforms = [
        "voice_core",
        "homeassistant",
        "webui",
        "little_spud",
        "macos",
        "xbmc",
        "homekit",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    description = (
        "Use for any request to control or check smart-home devices across integrations, "
        "including lights, switches, plugs, fans, covers, garage doors, locks, thermostats, "
        "cameras, media players, remotes, scenes, and scripts."
    )
    verba_dec = description
    when_to_use = (
        "Use for any request to control or inspect an integrated smart-home device. "
        "Choose this tool from the requested action and target name; do not guess the device's "
        "technical category from words such as lights, plug, or switch."
    )
    how_to_use = (
        "Pass the user's natural-language request unchanged in query. The verba searches every "
        "action-compatible device, resolves names and aliases, and dispatches through the owning integration."
    )
    tags = ["device", "smart-home", "integration", "light", "switch", "plug"]
    routing_keywords = [
        "device",
        "light",
        "switch",
        "plug",
        "outlet",
        "fan",
        "thermostat",
        "cover",
        "blind",
        "lock",
        "camera",
        "remote",
        "scene",
        "script",
        "turn on",
        "turn off",
    ]
    forced_route = "device"
    forced_domain_hint = "device"
    usage = '{"function":"device_control","arguments":{"query":"turn on the Christmas tree lights"}}'
    example_calls = [
        '{"function":"device_control","arguments":{"query":"turn on the Christmas tree lights"}}',
        '{"function":"device_control","arguments":{"query":"set the kitchen lights to 30 percent"}}',
        '{"function":"device_control","arguments":{"query":"close the bedroom blinds"}}',
        '{"function":"device_control","arguments":{"query":"lock the front door"}}',
        '{"function":"device_control","arguments":{"query":"take a snapshot from the porch camera"}}',
    ]
    common_needs = [
        "The requested action and a room, device name, or user-facing device alias when the target is not global."
    ]
    missing_info_prompts = ["Which room or device should I use?"]

    inventory_scope = "all"
    category_id = "device"
    category_label = "devices"
    singular_label = "device"
    max_candidates_setting = "DEVICE_MAX_CANDIDATES"
    allowed_actions = {
        "list",
        "status",
        "turn_on",
        "turn_off",
        "toggle",
        "set_brightness",
        "set_color",
        "set_percentage",
        "open",
        "close",
        "stop",
        "set_position",
        "lock",
        "unlock",
        "set_temperature",
        "set_hvac_mode",
        "camera_snapshot",
        "send_command",
        "activate",
        "run",
        "playpause",
        "play",
        "pause",
        "next",
        "previous",
        "mute",
        "unmute",
        "set_volume",
        "volume_up",
        "volume_down",
        "announce",
        "play_media",
    }
    control_actions = allowed_actions - {"list", "status"}
    ignored_target_words = {
        "device",
        "devices",
        "entity",
        "entities",
        "light",
        "lights",
        "lamp",
        "lamps",
        "bulb",
        "bulbs",
        "dimmer",
        "dimmers",
        "switch",
        "switches",
        "relay",
        "relays",
        "plug",
        "plugs",
        "outlet",
        "outlets",
        "fan",
        "fans",
        "cover",
        "covers",
        "shade",
        "shades",
        "blind",
        "blinds",
        "curtain",
        "curtains",
        "lock",
        "locks",
        "thermostat",
        "thermostats",
        "camera",
        "cameras",
        "remote",
        "remotes",
        "scene",
        "scenes",
        "script",
        "scripts",
        "the",
        "my",
        "all",
    }


verba = DeviceControlPlugin()
