from category_device_control import CategoryDeviceControlBase


class SwitchControlPlugin(CategoryDeviceControlBase):
    name = "switch_control"
    verba_name = "Switch Control"
    pretty_name = "Switch Control"
    description = "Control switches across all enabled integrations using the shared device registry."
    verba_dec = "Control switches from Home Assistant, Shelly, and any integration that exposes switch devices."
    tags = ["switch", "device", "integration"]
    routing_keywords = ["switch", "switches", "relay", "relays", "turn on switch", "turn off switch"]
    forced_route = "switch"
    forced_domain_hint = "switch"
    usage = '{"function":"switch_control","arguments":{"query":"turn on the hallway switch"}}'
    example_calls = [
        '{"function":"switch_control","arguments":{"query":"turn on the hallway switch"}}',
        '{"function":"switch_control","arguments":{"query":"is the garage relay on?"}}',
    ]
    category_id = "switch"
    category_label = "switches"
    singular_label = "switch"
    allowed_actions = {"list", "status", "turn_on", "turn_off", "toggle"}
    control_actions = {"turn_on", "turn_off", "toggle"}
    ignored_target_words = {"switch", "switches", "relay", "relays", "device", "devices", "the", "my", "all"}


verba = SwitchControlPlugin()
