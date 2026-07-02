from category_device_control import CategoryDeviceControlBase


class EnergyStatusPlugin(CategoryDeviceControlBase):
    name = "energy_status"
    verba_name = "Energy Status"
    pretty_name = "Energy Status"
    description = "Read power, energy, voltage, and current meters across integrations."
    verba_dec = "Read energy and power data from Shelly, Home Assistant, and other integrations."
    tags = ["energy", "power", "meter", "sensor", "integration"]
    routing_keywords = ["energy", "power", "watts", "voltage", "current", "meter"]
    forced_route = "energy"
    forced_domain_hint = "energy meter"
    usage = '{"function":"energy_status","arguments":{"query":"how much power is the washer using?"}}'
    category_id = "energy"
    category_label = "energy meters"
    singular_label = "energy meter"
    ignored_target_words = {"energy", "power", "meter", "meters", "watts", "voltage", "current", "the", "my", "all"}


verba = EnergyStatusPlugin()
