from category_device_control import CategoryDeviceControlBase


class CoverControlPlugin(CategoryDeviceControlBase):
    name = "cover_control"
    verba_name = "Cover Control"
    pretty_name = "Cover Control"
    description = "Control covers, blinds, shades, curtains, and open/close devices across integrations."
    verba_dec = "Control covers from Home Assistant, Shelly, and any integration that exposes cover devices."
    tags = ["cover", "shade", "blind", "curtain", "device", "integration"]
    routing_keywords = ["cover", "covers", "shade", "shades", "blind", "blinds", "curtain", "curtains"]
    forced_route = "cover"
    forced_domain_hint = "cover"
    usage = '{"function":"cover_control","arguments":{"query":"close the living room blinds"}}'
    example_calls = [
        '{"function":"cover_control","arguments":{"query":"open the bedroom shades"}}',
        '{"function":"cover_control","arguments":{"query":"set the living room blinds to 50 percent"}}',
    ]
    category_id = "cover"
    category_label = "covers"
    singular_label = "cover"
    allowed_actions = {"list", "status", "open", "close", "stop", "set_position"}
    control_actions = {"open", "close", "stop", "set_position"}
    ignored_target_words = {"cover", "covers", "shade", "shades", "blind", "blinds", "curtain", "curtains", "the", "my", "all"}


verba = CoverControlPlugin()
