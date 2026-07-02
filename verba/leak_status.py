from category_device_control import CategoryDeviceControlBase


class LeakStatusPlugin(CategoryDeviceControlBase):
    name = "leak_status"
    verba_name = "Leak Sensor Status"
    pretty_name = "Leak Sensor Status"
    description = "Check water, flood, moisture, and leak sensors across integrations."
    verba_dec = "Read leak sensor status from UniFi Protect, Home Assistant, and other integrations."
    tags = ["leak", "water", "flood", "sensor", "integration"]
    routing_keywords = ["leak", "water sensor", "flood", "moisture", "wet", "dry"]
    forced_route = "leak"
    forced_domain_hint = "leak sensor"
    usage = '{"function":"leak_status","arguments":{"query":"is the laundry leak sensor dry?"}}'
    category_id = "leak"
    category_label = "leak sensors"
    singular_label = "leak sensor"
    ignored_target_words = {"leak", "water", "flood", "moisture", "sensor", "sensors", "wet", "dry", "the", "my", "all"}


verba = LeakStatusPlugin()
