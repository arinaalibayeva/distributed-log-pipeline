"""The state and rules of one broker, without any networking.

A broker holds a copy of every partition and leads some of them. Only the
leader of a partition accepts new lines and serves reads; any other broker
answers with NotLeaderError so the client can go to the right one. The web
server in server.py is a thin layer on top of this class.
"""

from broker import config
from broker.partition import Partition


class NotLeaderError(Exception):
    """Raised when a request reaches a broker that doesn't lead the partition."""

    def __init__(self, partition, leader):
        super().__init__(f"broker does not lead {partition}; broker {leader} does")
        self.partition = partition
        self.leader = leader


class UnknownPartitionError(Exception):
    """Raised for a partition name that isn't part of the cluster."""


class BrokerNode:
    def __init__(self, broker_id, partitions=config.PARTITIONS):
        self.id = broker_id
        self.partitions = {name: Partition(name) for name in partitions}
        self.leaders = {name: config.initial_leader(name) for name in partitions}

    def is_leader(self, name):
        return self.leaders[name] == self.id

    def append(self, name, line):
        """Store a line in partition `name` and return its offset.

        Only the leader accepts lines. A retried line (same msg_id) gets
        its existing offset back instead of being stored twice.
        """
        partition = self._leader_partition(name)
        offset = partition.append(line)
        # No replication yet (that's M4), so the leader is the only copy and
        # commits right away. M4 commits only once a majority has the line.
        partition.commit(len(partition) - 1)
        return offset

    def read(self, name, offset=0, limit=100):
        """Return up to `limit` committed entries of `name`, from `offset` on."""
        partition = self._leader_partition(name)
        return partition.read(offset)[:limit]

    def state(self, tail=5):
        """Return a summary of every partition, for the dashboard."""
        return {
            "broker_id": self.id,
            "partitions": {
                name: {
                    "role": "leader" if self.is_leader(name) else "follower",
                    "leader": self.leaders[name],
                    "epoch": partition.epoch,
                    "length": len(partition),
                    "commit_index": partition.commit_index,
                    "last_entries": partition.read_uncommitted(max(0, len(partition) - tail)),
                }
                for name, partition in self.partitions.items()
            },
        }

    def _leader_partition(self, name):
        if name not in self.partitions:
            raise UnknownPartitionError(name)
        if not self.is_leader(name):
            raise NotLeaderError(name, self.leaders[name])
        return self.partitions[name]
