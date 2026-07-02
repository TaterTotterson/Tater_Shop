from category_device_control import CategoryDeviceControlBase


class SensorStatusPlugin(CategoryDeviceControlBase):
    name = "sensor_status"
    verba_name = "Sensor Status"
    pretty_name = "Sensor Status"
    description = "Check miscellaneous sensors that do not fit a more specific device category."
    verba_dec = "Read other sensor status from all enabled integrations through the shared device registry."
    tags = ["sensor", "device", "integration"]
    routing_keywords = ["sensor", "sensors", "sensor status"]
    forced_route = "sensor"
    forced_domain_hint = "sensor"
    usage = '{"function":"sensor_status","arguments":{"query":"list the sensors"}}'
    category_id = "sensor"
    category_label = "sensors"
    singular_label = "sensor"
    ignored_target_words = {"sensor", "sensors", "device", "devices", "the", "my", "all"}


verba = SensorStatusPlugin()
