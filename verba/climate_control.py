from category_device_control import CategoryDeviceControlBase


class ClimateControlPlugin(CategoryDeviceControlBase):
    name = "climate_control"
    verba_name = "Climate Control"
    pretty_name = "Climate Control"
    description = "Control thermostats and climate devices across all enabled integrations."
    verba_dec = "Control Ecobee/HomeKit, Home Assistant climate entities, and any integration that exposes climate devices."
    tags = ["climate", "thermostat", "temperature", "device", "integration"]
    routing_keywords = ["climate", "thermostat", "thermostats", "hvac", "heat", "cool", "temperature"]
    forced_route = "climate"
    forced_domain_hint = "climate"
    usage = '{"function":"climate_control","arguments":{"query":"set the ecobee to 70 degrees"}}'
    example_calls = [
        '{"function":"climate_control","arguments":{"query":"set the ecobee to 70 degrees"}}',
        '{"function":"climate_control","arguments":{"query":"set the thermostat to cool"}}',
    ]
    category_id = "climate"
    category_label = "climate devices"
    singular_label = "climate device"
    allowed_actions = {"list", "status", "set_temperature", "set_hvac_mode"}
    control_actions = {"set_temperature", "set_hvac_mode"}
    ignored_target_words = {"climate", "thermostat", "thermostats", "hvac", "temperature", "heat", "cool", "the", "my", "all"}


verba = ClimateControlPlugin()
