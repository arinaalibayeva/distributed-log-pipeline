"""The state and rules of one broker, without any networking.

A broker holds a copy of every partition and leads some of them. Only the
leader of a partition accepts new lines and serves reads; any other broker
answers with NotLeaderError so the client can go to the right one. The
leader copies its lines to the followers (see replication.py) and commits a
line once a majority of the brokers have it. The web server in server.py is
a thin layer on top of this class.
"""

import time

from broker import config
from broker.partition import GapError, Partition, commit_index_from_lengths


class NotLeaderError(Exception):
    """Raised when a request reaches a broker that doesn't lead the partition."""

    def __init__(self, partition, leader):
        super().__init__(f"broker does not lead {partition}; broker {leader} does")
        self.partition = partition
        self.leader = leader


class NotFollowerError(Exception):
    """Raised when copied entries come from a broker that isn't the leader."""

    def __init__(self, partition, leader):
        super().__init__(f"only broker {leader} may send entries for {partition}")
        self.partition = partition
        self.leader = leader


class UnknownPartitionError(Exception):
    """Raised for a partition name that isn't part of the cluster."""


class BrokerNode:
    def __init__(self, broker_id, broker_ids=tuple(config.BROKERS), partitions=config.PARTITIONS):
        self.id = broker_id
        self.peers = [b for b in broker_ids if b != broker_id]
        self.partitions = {name: Partition(name) for name in partitions}
        self.leaders = {name: config.initial_leader(name) for name in partitions}
        # On the leader: how many of its entries each follower is known to
        # have, counting only entries the follower confirmed match its own.
        # It starts at 0 and grows as the follower accepts messages.
        self.follower_lengths = {
            name: {peer: 0 for peer in self.peers} for name in partitions if self.is_leader(name)
        }
        # On a follower: when it last heard from the leader (time.monotonic()).
        self.last_heard = {name: None for name in partitions}

    def is_leader(self, name):
        return self.leaders[name] == self.id

    def append(self, name, line):
        """Store a line in partition `name` and return its offset.

        Only the leader accepts lines. The line is not committed yet: that
        happens once a follower has it too (see record_follower). A retried
        line (same msg_id) gets its existing offset back instead of being
        stored twice.
        """
        return self._leader_partition(name).append(line)

    def read(self, name, offset=0, limit=100):
        """Return up to `limit` committed entries of `name`, from `offset` on."""
        partition = self._leader_partition(name)
        return partition.read(offset)[:limit]

    def message_for(self, name, follower, batch):
        """Build the next replication message for `follower`.

        It carries up to `batch` entries the follower is missing (none when
        it is up to date, which makes it a heartbeat), the msg_id and epoch
        of the entry just before them so the follower can check its log
        matches, and the leader's commit index and epoch.
        """
        partition = self.partitions[name]
        start = min(self.follower_lengths[name][follower], len(partition))
        prev = partition.entries[start - 1] if start > 0 else None
        return {
            "leader": self.id,
            "epoch": partition.epoch,
            "start": start,
            "prev_msg_id": prev["line"]["msg_id"] if prev else None,
            "prev_epoch": prev["epoch"] if prev else None,
            "entries": partition.read_uncommitted(start)[:batch],
            "commit_index": partition.commit_index,
        }

    def record_reply(self, name, follower, message, accepted, length):
        """Update what `follower` is known to have from its reply to `message`.

        If it accepted, it now matches the leader up to the end of the
        message. If not, the message didn't attach, and `length` is where the
        follower asks the leader to resend from (never later than one entry
        before this message). Returns True if the commit index moved.
        """
        start = message["start"]
        if accepted:
            return self.record_follower(name, follower, start + len(message["entries"]))
        self.follower_lengths[name][follower] = min(length, max(start - 1, 0))
        return False

    def is_behind(self, name, follower):
        return self.follower_lengths[name][follower] < len(self.partitions[name])

    def record_follower(self, name, follower, length):
        """Record that `follower` matches the leader's first `length` entries.

        Then commit whatever a majority of brokers have. The length can go
        down, for example when a follower restarts empty; the leader then
        resends from there. The commit index never goes down. Returns True
        if the commit index moved forward.
        """
        self.follower_lengths[name][follower] = length
        partition = self.partitions[name]
        own = len(partition)
        lengths = [own] + [min(n, own) for n in self.follower_lengths[name].values()]
        majority_offset = commit_index_from_lengths(lengths)
        if majority_offset > partition.commit_index:
            partition.commit(majority_offset)
            return True
        return False

    def replicate(self, name, leader, epoch, start, entries, leader_commit,
                  prev_msg_id=None, prev_epoch=None, now=None):
        """Store entries the leader sent and follow its commit index.

        Returns (accepted, length). When accepted, `length` is this copy's
        new length. `accepted` is False when the entries don't attach to this
        copy, and `length` is then where the leader should resend from: the
        end of this log if entries are missing, or just after the committed
        entries if the entry before the message differs (committed entries
        always match the leader's). The commit index only follows the leader
        up to the end of an accepted message, since only that much is known
        to match.
        """
        partition = self._partition(name)
        if self.is_leader(name) or leader != self.leaders[name]:
            raise NotFollowerError(name, self.leaders[name])
        self.last_heard[name] = time.monotonic() if now is None else now
        try:
            partition.replicate(start, entries, epoch, prev_msg_id, prev_epoch)
        except GapError:
            if start > len(partition):
                return False, len(partition)
            return False, partition.commit_index + 1
        partition.follow_commit(leader_commit, verified_end=start + len(entries) - 1)
        return True, len(partition)

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
                    "follower_lengths": self.follower_lengths.get(name),
                    "last_entries": partition.read_uncommitted(max(0, len(partition) - tail)),
                }
                for name, partition in self.partitions.items()
            },
        }

    def _partition(self, name):
        if name not in self.partitions:
            raise UnknownPartitionError(name)
        return self.partitions[name]

    def _leader_partition(self, name):
        partition = self._partition(name)
        if not self.is_leader(name):
            raise NotLeaderError(name, self.leaders[name])
        return partition
