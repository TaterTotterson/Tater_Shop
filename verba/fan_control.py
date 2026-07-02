from category_device_control import CategoryDeviceControlBase


class FanControlPlugin(CategoryDeviceControlBase):
    name = "fan_control"
    verba_name = "Fan Control"
    pretty_name = "Fan Control"
    description = "Control fans across all enabled integrations."
    verba_dec = "Control fans and fan speed from Home Assistant and any integration that exposes fan devices."
    tags = ["fan", "device", "integration"]
    routing_keywords = ["fan", "fans", "fan speed"]
    forced_route = "fan"
    forced_domain_hint = "fan"
    usage = '{"function":"fan_control","arguments":{"query":"set the office fan to 40 percent"}}'
    example_calls = [
        '{"function":"fan_control","arguments":{"query":"turn on the bedroom fan"}}',
        '{"function":"fan_control","arguments":{"query":"set the office fan to 40 percent"}}',
    ]
    category_id = "fan"
    category_label = "fans"
    singular_label = "fan"
    allowed_actions = {"list", "status", "turn_on", "turn_off", "toggle", "set_percentage"}
    control_actions = allowed_actions - {"list", "status"}
    ignored_target_words = {"fan", "fans", "speed", "the", "my", "all"}


verba = FanControlPlugin()
