from category_device_control import CategoryDeviceControlBase


class CameraControlPlugin(CategoryDeviceControlBase):
    name = "camera_control"
    verba_name = "Camera Control"
    pretty_name = "Camera Control"
    version = "1.0.1"
    min_tater_version = "59"
    settings_category = "Device Control"
    description = "Check cameras and request snapshots as image artifacts across all enabled integrations."
    verba_dec = "Use cameras from Home Assistant, UniFi Protect, and any integration that exposes camera devices. Snapshot results are returned as image artifacts so Tater can describe them with vision."
    tags = ["camera", "snapshot", "vision", "device", "integration"]
    routing_keywords = ["camera", "cameras", "snapshot", "photo", "picture", "image", "vision", "describe", "doorbell"]
    forced_route = "camera"
    forced_domain_hint = "camera"
    usage = '{"function":"camera_control","arguments":{"query":"take a snapshot from the porch camera"}}'
    example_calls = [
        '{"function":"camera_control","arguments":{"query":"show the driveway camera status"}}',
        '{"function":"camera_control","arguments":{"query":"take a snapshot from the porch camera"}}',
        '{"function":"camera_control","arguments":{"query":"look at the front door camera and tell me what you see"}}',
    ]
    category_id = "camera"
    category_label = "cameras"
    singular_label = "camera"
    allowed_actions = {"list", "status", "camera_snapshot"}
    control_actions = {"camera_snapshot"}
    ignored_target_words = {"camera", "cameras", "snapshot", "photo", "picture", "image", "doorbell", "the", "my", "all"}


verba = CameraControlPlugin()
