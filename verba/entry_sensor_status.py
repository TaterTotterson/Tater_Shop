from category_device_control import CategoryDeviceControlBase


class EntrySensorStatusPlugin(CategoryDeviceControlBase):
    name = "entry_sensor_status"
    verba_name = "Door & Window Sensor Status"
    pretty_name = "Door & Window Sensor Status"
    description = "Check door, window, contact, and opening sensors across integrations."
    verba_dec = "Read entry sensor status from Home Assistant, Hue, UniFi Protect, Ecobee/HomeKit, and other integrations."
    tags = ["entry-sensor", "door", "window", "sensor", "integration"]
    routing_keywords = ["door sensor", "window sensor", "contact sensor", "is the door open", "is the window closed"]
    forced_route = "entry_sensor"
    forced_domain_hint = "door window sensor"
    usage = '{"function":"entry_sensor_status","arguments":{"query":"is the front door open?"}}'
    category_id = "entry_sensor"
    category_label = "door and window sensors"
    singular_label = "entry sensor"
    ignored_target_words = {"door", "doors", "window", "windows", "contact", "sensor", "sensors", "entry", "the", "my", "all"}


verba = EntrySensorStatusPlugin()
