from category_device_control import CategoryDeviceControlBase


class ScriptControlPlugin(CategoryDeviceControlBase):
    name = "script_control"
    verba_name = "Script Control"
    pretty_name = "Script Control"
    description = "Run scripts across all enabled integrations."
    verba_dec = "List, check, and run scripts from Home Assistant and any integration that exposes scripts."
    tags = ["script", "automation", "device", "integration"]
    routing_keywords = ["script", "scripts", "run script"]
    forced_route = "script"
    forced_domain_hint = "script"
    usage = '{"function":"script_control","arguments":{"query":"run bedtime script"}}'
    example_calls = [
        '{"function":"script_control","arguments":{"query":"run bedtime script"}}',
        '{"function":"script_control","arguments":{"query":"list scripts"}}',
    ]
    category_id = "script"
    category_label = "scripts"
    singular_label = "script"
    allowed_actions = {"list", "status", "run"}
    control_actions = {"run"}
    ignored_target_words = {"script", "scripts", "the", "my", "all"}


verba = ScriptControlPlugin()
