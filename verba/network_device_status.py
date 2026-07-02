from category_device_control import CategoryDeviceControlBase


class NetworkDeviceStatusPlugin(CategoryDeviceControlBase):
    name = "network_device_status"
    verba_name = "Network Device Status"
    pretty_name = "Network Device Status"
    description = "Check network devices, clients, access points, and hosts across integrations."
    verba_dec = "Read network device status from UniFi Network and any integration that exposes network devices."
    tags = ["network", "client", "device", "integration"]
    routing_keywords = ["network device", "client", "access point", "gateway", "host", "is online", "is connected"]
    forced_route = "network_device"
    forced_domain_hint = "network device"
    usage = '{"function":"network_device_status","arguments":{"query":"is the garage access point online?"}}'
    category_id = "network_device"
    category_label = "network devices"
    singular_label = "network device"
    ignored_target_words = {"network", "client", "clients", "device", "devices", "host", "hosts", "access", "point", "gateway", "the", "my", "all"}


verba = NetworkDeviceStatusPlugin()
