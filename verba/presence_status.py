from category_device_control import CategoryDeviceControlBase


class PresenceStatusPlugin(CategoryDeviceControlBase):
    name = "presence_status"
    verba_name = "Presence Status"
    pretty_name = "Presence Status"
    description = "Check occupancy, presence, and connected-client status across integrations."
    verba_dec = "Read presence from UniFi Network clients, Ecobee/HomeKit occupancy sensors, and other integrations."
    tags = ["presence", "occupancy", "network", "sensor", "integration"]
    routing_keywords = ["presence", "occupancy", "occupied", "connected", "client online"]
    forced_route = "presence"
    forced_domain_hint = "presence device"
    usage = '{"function":"presence_status","arguments":{"query":"is my phone connected?"}}'
    category_id = "presence"
    category_label = "presence devices"
    singular_label = "presence device"
    ignored_target_words = {"presence", "occupancy", "occupied", "client", "connected", "device", "devices", "the", "my", "all"}


verba = PresenceStatusPlugin()
