"""Fixed layout of the cluster: where each broker runs and who leads what.

Every broker stores a copy of every partition. Each partition has one leader
at a time. The preference order decides who takes over first: the first
broker leads when the cluster starts (so each broker leads one partition),
and when a leader dies the next live broker in line takes over. Clients also
try brokers in this order when they don't know the leader.
"""

BROKERS = {
    1: "http://127.0.0.1:8001",
    2: "http://127.0.0.1:8002",
    3: "http://127.0.0.1:8003",
}

PREFERENCE = {
    "login": [1, 2, 3],
    "search": [2, 3, 1],
    "upload": [3, 1, 2],
}

PARTITIONS = list(PREFERENCE)


def initial_leader(partition):
    """Return the broker that should lead `partition` when the cluster starts."""
    return PREFERENCE[partition][0]


def port_of(broker_id):
    """Return the port a broker listens on, taken from its URL."""
    return int(BROKERS[broker_id].rsplit(":", 1)[1])
