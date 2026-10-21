"""Fixed layout of the cluster: where each broker runs and who leads what.

Every broker stores a copy of every partition. Each partition has one leader,
and leadership is spread out so that each broker leads one partition. The
preference order is who a client tries next when the leader can't be reached
(and, from M6, who takes over when the leader dies).
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
    """Return the broker that leads `partition` when the cluster starts."""
    return PREFERENCE[partition][0]


def port_of(broker_id):
    """Return the port a broker listens on, taken from its URL."""
    return int(BROKERS[broker_id].rsplit(":", 1)[1])
