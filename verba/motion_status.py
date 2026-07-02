from category_device_control import CategoryDeviceControlBase


class MotionStatusPlugin(CategoryDeviceControlBase):
    name = "motion_status"
    verba_name = "Motion Status"
    pretty_name = "Motion Status"
    description = "Check motion and occupancy sensors across integrations."
    verba_dec = "Read motion status from Home Assistant, Hue, UniFi Protect, Ecobee/HomeKit, and other integrations."
    tags = ["motion", "occupancy", "sensor", "integration"]
    routing_keywords = ["motion", "motion sensor", "occupancy", "occupied", "is there motion"]
    forced_route = "motion"
    forced_domain_hint = "motion sensor"
    usage = '{"function":"motion_status","arguments":{"query":"is there motion in the hallway?"}}'
    category_id = "motion"
    category_label = "motion sensors"
    singular_label = "motion sensor"
    ignored_target_words = {"motion", "occupancy", "occupied", "sensor", "sensors", "the", "my", "all"}


verba = MotionStatusPlugin()
