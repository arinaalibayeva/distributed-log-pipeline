"""Reads committed lines from the cluster, front to back.

A consumer remembers its own offset for each partition (the next offset it
wants) and asks the leader for whatever has been committed since. Consumers
never change the log, so any number of them can read independently.
"""

from broker import config
from broker.client import NoLeaderAvailable


class Consumer:
    def __init__(self, client, partitions=config.PARTITIONS, offsets=None, batch=100):
        self.client = client
        self.partitions = list(partitions)
        self.offsets = {name: 0 for name in self.partitions}
        self.offsets.update(offsets or {})
        self.batch = batch

    def poll(self):
        """Return new committed entries from every partition.

        Entries from one partition come back in offset order. A partition
        whose leader can't be reached is skipped and tried again next time.
        """
        entries = []
        for name in self.partitions:
            try:
                reply = self.client.call(
                    name, "GET", f"/partitions/{name}/read",
                    params={"offset": self.offsets[name], "limit": self.batch},
                )
            except NoLeaderAvailable:
                continue
            if reply["entries"]:
                entries.extend(reply["entries"])
                self.offsets[name] = reply["entries"][-1]["offset"] + 1
        return entries
