from category_device_control import CategoryDeviceControlBase


class HumidityStatusPlugin(CategoryDeviceControlBase):
    name = "humidity_status"
    verba_name = "Humidity Status"
    pretty_name = "Humidity Status"
    description = "Read humidity sensors across integrations."
    verba_dec = "Read humidity from Ecobee/HomeKit, Home Assistant, Hue, Shelly, UniFi Protect, and other integrations."
    tags = ["humidity", "sensor", "integration"]
    routing_keywords = ["humidity", "relative humidity", "humid"]
    forced_route = "humidity"
    forced_domain_hint = "humidity sensor"
    usage = '{"function":"humidity_status","arguments":{"query":"what is the basement humidity?"}}'
    category_id = "humidity"
    category_label = "humidity sensors"
    singular_label = "humidity sensor"
    ignored_target_words = {"humidity", "relative", "sensor", "sensors", "humid", "the", "my", "all"}


verba = HumidityStatusPlugin()
