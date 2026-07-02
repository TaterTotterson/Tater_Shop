from category_device_control import CategoryDeviceControlBase


class LockControlPlugin(CategoryDeviceControlBase):
    name = "lock_control"
    verba_name = "Lock Control"
    pretty_name = "Lock Control"
    description = "Control locks across all enabled integrations."
    verba_dec = "Lock, unlock, and check smart locks from Home Assistant and any integration that exposes lock devices."
    tags = ["lock", "door", "device", "integration"]
    routing_keywords = ["lock", "locks", "door lock", "unlock"]
    forced_route = "lock"
    forced_domain_hint = "lock"
    usage = '{"function":"lock_control","arguments":{"query":"lock the front door"}}'
    example_calls = [
        '{"function":"lock_control","arguments":{"query":"lock the front door"}}',
        '{"function":"lock_control","arguments":{"query":"is the back door locked"}}',
    ]
    category_id = "lock"
    category_label = "locks"
    singular_label = "lock"
    allowed_actions = {"list", "status", "lock", "unlock"}
    control_actions = {"lock", "unlock"}
    ignored_target_words = {"lock", "locks", "door", "doors", "the", "my", "all"}


verba = LockControlPlugin()
