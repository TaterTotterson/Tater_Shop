from category_device_control import CategoryDeviceControlBase


class IlluminanceStatusPlugin(CategoryDeviceControlBase):
    name = "illuminance_status"
    verba_name = "Light Sensor Status"
    pretty_name = "Light Sensor Status"
    description = "Read light-level and illuminance sensors across integrations."
    verba_dec = "Read lux and light-level sensors from Hue, Shelly, Home Assistant, UniFi Protect, and other integrations."
    tags = ["illuminance", "lux", "light-sensor", "sensor", "integration"]
    routing_keywords = ["light sensor", "illuminance", "lux", "light level"]
    forced_route = "illuminance"
    forced_domain_hint = "light sensor"
    usage = '{"function":"illuminance_status","arguments":{"query":"what is the porch light level?"}}'
    category_id = "illuminance"
    category_label = "light sensors"
    singular_label = "light sensor"
    ignored_target_words = {"light", "level", "illuminance", "lux", "sensor", "sensors", "the", "my", "all"}


verba = IlluminanceStatusPlugin()
