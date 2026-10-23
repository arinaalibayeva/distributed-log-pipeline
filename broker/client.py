"""Sends requests to whichever broker leads a partition.

Producers and consumers use this so they don't have to know who the leader
is. The client remembers its best guess for each partition's leader. If a
broker answers "not leader", the client switches to the broker it names; if a
broker can't be reached, or doesn't know the leader either (during a
takeover), the client tries the next one in preference order.
If the leader answers that it can't commit right now (503), the client gives
up at once: no other broker can help, and the caller should retry later.
"""

import httpx

from broker import config


class NoLeaderAvailable(Exception):
    """Raised when no broker could handle a request for a partition."""


class ClusterClient:
    # The timeout must be longer than the broker's ack_timeout, or a slow
    # commit would look like a dead broker.
    def __init__(self, http=None, brokers=config.BROKERS, preference=config.PREFERENCE, timeout=3.0):
        self.http = http or httpx.Client(timeout=timeout)
        self.brokers = brokers
        self.preference = preference
        self.leader = {name: order[0] for name, order in preference.items()}

    def call(self, partition, method, path, **kwargs):
        """Send a request to `partition`'s leader and return the JSON reply.

        `path` is relative to the broker's URL; extra keyword arguments go to
        httpx (for example `json=` or `params=`). Raises NoLeaderAvailable if
        the leader can't commit right now, or once every broker has been
        tried twice without success, which bounds the number of redirects if
        brokers disagree about who leads.
        """
        for _ in range(2 * len(self.brokers)):
            broker = self.leader[partition]
            try:
                response = self.http.request(method, self.brokers[broker] + path, **kwargs)
            except httpx.TransportError:
                self.leader[partition] = self._next_broker(partition, broker)
                continue
            if response.status_code == 421:
                named = response.json()["leader"]
                self.leader[partition] = named if named is not None else self._next_broker(partition, broker)
                continue
            if response.status_code == 503:
                raise NoLeaderAvailable(partition)
            if response.status_code >= 500:
                self.leader[partition] = self._next_broker(partition, broker)
                continue
            response.raise_for_status()
            return response.json()
        raise NoLeaderAvailable(partition)

    def _next_broker(self, partition, broker):
        order = self.preference[partition]
        return order[(order.index(broker) + 1) % len(order)]
