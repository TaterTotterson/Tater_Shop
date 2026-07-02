from category_device_control import CategoryDeviceControlBase


class TemperatureStatusPlugin(CategoryDeviceControlBase):
    name = "temperature_status"
    verba_name = "Temperature Status"
    pretty_name = "Temperature Status"
    description = "Read temperature sensors across integrations."
    verba_dec = "Read temperature from Ecobee/HomeKit, Home Assistant, Hue, Shelly, UniFi Protect, and other integrations."
    tags = ["temperature", "sensor", "integration"]
    routing_keywords = ["temperature", "temp", "how warm", "how cold", "degrees"]
    forced_route = "temperature"
    forced_domain_hint = "temperature sensor"
    usage = '{"function":"temperature_status","arguments":{"query":"what is the bedroom temperature?"}}'
    category_id = "temperature"
    category_label = "temperature sensors"
    singular_label = "temperature sensor"
    ignored_target_words = {"temperature", "temp", "sensor", "sensors", "degrees", "the", "my", "all"}


verba = TemperatureStatusPlugin()
