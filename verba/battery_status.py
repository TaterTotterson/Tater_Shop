from category_device_control import CategoryDeviceControlBase


class BatteryStatusPlugin(CategoryDeviceControlBase):
    name = "battery_status"
    verba_name = "Battery Status"
    pretty_name = "Battery Status"
    description = "Check battery levels and low-battery sensors across integrations."
    verba_dec = "Read battery status from Hue, Ecobee/HomeKit, Aladdin, UniFi Protect, Home Assistant, and other integrations."
    tags = ["battery", "sensor", "integration"]
    routing_keywords = ["battery", "battery level", "low battery"]
    forced_route = "battery"
    forced_domain_hint = "battery sensor"
    usage = '{"function":"battery_status","arguments":{"query":"which sensors have low battery?"}}'
    category_id = "battery"
    category_label = "battery sensors"
    singular_label = "battery sensor"
    ignored_target_words = {"battery", "batteries", "level", "low", "sensor", "sensors", "the", "my", "all"}


verba = BatteryStatusPlugin()
