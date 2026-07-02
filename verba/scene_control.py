from category_device_control import CategoryDeviceControlBase


class SceneControlPlugin(CategoryDeviceControlBase):
    name = "scene_control"
    verba_name = "Scene Control"
    pretty_name = "Scene Control"
    description = "Activate scenes across all enabled integrations."
    verba_dec = "List, check, and activate scenes from Home Assistant and any integration that exposes scenes."
    tags = ["scene", "automation", "device", "integration"]
    routing_keywords = ["scene", "scenes", "activate scene", "run scene"]
    forced_route = "scene"
    forced_domain_hint = "scene"
    usage = '{"function":"scene_control","arguments":{"query":"activate movie time scene"}}'
    example_calls = [
        '{"function":"scene_control","arguments":{"query":"activate movie time scene"}}',
        '{"function":"scene_control","arguments":{"query":"list scenes"}}',
    ]
    category_id = "scene"
    category_label = "scenes"
    singular_label = "scene"
    allowed_actions = {"list", "status", "activate"}
    control_actions = {"activate"}
    ignored_target_words = {"scene", "scenes", "the", "my", "all"}


verba = SceneControlPlugin()
