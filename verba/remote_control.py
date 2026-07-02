from category_device_control import CategoryDeviceControlBase


class RemoteControlPlugin(CategoryDeviceControlBase):
    name = "remote_control"
    verba_name = "Remote Control"
    pretty_name = "Remote Control"
    description = "Control remotes and button-command targets across integrations."
    verba_dec = "Send button commands to remotes from Home Assistant and any integration that exposes remote devices."
    tags = ["remote", "button", "device", "integration"]
    routing_keywords = ["remote", "remote control", "button", "mute", "volume up", "home", "back"]
    forced_route = "remote"
    forced_domain_hint = "remote"
    usage = '{"function":"remote_control","arguments":{"query":"mute the living room remote"}}'
    example_calls = [
        '{"function":"remote_control","arguments":{"query":"mute the living room remote"}}',
        '{"function":"remote_control","arguments":{"query":"press home on the den remote"}}',
    ]
    category_id = "remote"
    category_label = "remotes"
    singular_label = "remote"
    allowed_actions = {"list", "status", "turn_on", "turn_off", "send_command"}
    control_actions = {"turn_on", "turn_off", "send_command"}
    ignored_target_words = {"remote", "remotes", "control", "button", "the", "my", "all"}


verba = RemoteControlPlugin()
