from category_device_control import CategoryDeviceControlBase


class GarageDoorControlPlugin(CategoryDeviceControlBase):
    name = "garage_door_control"
    verba_name = "Garage Door Control"
    pretty_name = "Garage Door Control"
    description = "Control garage doors across all enabled integrations using the shared device registry."
    verba_dec = "Control garage doors from Aladdin, Home Assistant, UniFi Protect, and any garage-capable integration."
    tags = ["garage", "garage-door", "device", "integration"]
    routing_keywords = ["garage", "garage door", "garage doors", "open garage", "close garage"]
    forced_route = "garage_door"
    forced_domain_hint = "garage door"
    usage = '{"function":"garage_door_control","arguments":{"query":"is the garage door closed?"}}'
    example_calls = [
        '{"function":"garage_door_control","arguments":{"query":"open the garage door"}}',
        '{"function":"garage_door_control","arguments":{"query":"close the left garage door"}}',
        '{"function":"garage_door_control","arguments":{"query":"is the garage door closed?"}}',
    ]
    category_id = "garage_door"
    category_label = "garage doors"
    singular_label = "garage door"
    allowed_actions = {"list", "status", "open", "close", "stop"}
    control_actions = {"open", "close", "stop"}
    ignored_target_words = {"garage", "garages", "door", "doors", "opener", "openers", "the", "my", "all"}


verba = GarageDoorControlPlugin()
