from category_device_control import CategoryDeviceControlBase


class PlugControlPlugin(CategoryDeviceControlBase):
    name = "plug_control"
    verba_name = "Plug Control"
    pretty_name = "Plug Control"
    description = "Control smart plugs and outlets across all enabled integrations."
    verba_dec = "Control plugs, outlets, and plug-style switch devices from any integration."
    tags = ["plug", "outlet", "device", "integration"]
    routing_keywords = ["plug", "plugs", "outlet", "outlets", "smart plug", "turn on plug", "turn off outlet"]
    forced_route = "plug"
    forced_domain_hint = "plug"
    usage = '{"function":"plug_control","arguments":{"query":"turn off the desk outlet"}}'
    example_calls = [
        '{"function":"plug_control","arguments":{"query":"turn on the arcade plug"}}',
        '{"function":"plug_control","arguments":{"query":"is the desk outlet on?"}}',
    ]
    category_id = "plug"
    category_label = "plugs"
    singular_label = "plug"
    allowed_actions = {"list", "status", "turn_on", "turn_off", "toggle"}
    control_actions = {"turn_on", "turn_off", "toggle"}
    ignored_target_words = {"plug", "plugs", "outlet", "outlets", "smart", "device", "devices", "the", "my", "all"}


verba = PlugControlPlugin()
