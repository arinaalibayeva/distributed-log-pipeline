"""The state and rules of one broker, without any networking.

A broker holds a copy of every partition and leads some of them. Only the
leader of a partition accepts new lines and serves reads; any other broker
answers with NotLeaderError so the client can go to the right one. The
leader copies its lines to the followers (see replication.py) and commits a
line once a majority of the brokers have it. The web server in server.py is
a thin layer on top of this class.

Leader takeover. A follower that hasn't heard from the leader for
`leader_timeout` seconds (plus `stagger` seconds per place it sits down the
partition's preference order, so the next broker in line goes first) asks
the other brokers to let it take over with a new, higher epoch. A broker
agrees only if it hasn't heard from a leader recently either, and it agrees
to at most one broker per epoch, so a takeover needs a majority and there is
never more than one leader per epoch. Agreeing brokers send back the end of
their logs. The new leader keeps whichever log is most up to date (newest
last epoch, then longest) and marks its uncommitted lines with its own epoch
before serving: every committed line is on a majority, so one of the brokers
in the takeover has it. A leader steps down when it learns of a newer epoch
or can't reach a majority, and drops the lines it never committed.
"""

import time

from broker import config
from broker.partition import GapError, Partition, StaleEpochError, commit_index_from_lengths


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
    def __init__(self, broker_id, broker_ids=tuple(config.BROKERS), partitions=config.PARTITIONS,
                 preference=config.PREFERENCE, leader_timeout=1.5, stagger=0.5, leaders=None, now=None):
        """Create a broker's state.

        A broker starts out leading nothing and knowing no leader: leaders
        are decided by takeover (see the module docstring), so a broker that
        restarts with an empty log can't come back believing it still leads.
        `leaders` maps partitions to a starting leader, for tests.
        """
        self.id = broker_id
        self.peers = [b for b in broker_ids if b != broker_id]
        self.majority = (len(self.peers) + 1) // 2 + 1
        self.partitions = {name: Partition(name) for name in partitions}
        self.preference = preference
        self.leader_timeout = leader_timeout
        self.stagger = stagger
        self.leaders = {name: (leaders or {}).get(name) for name in partitions}
        # On the leader: how many of its entries each follower is known to
        # have, counting only entries the follower confirmed match its own.
        # It starts at 0 and grows as the follower accepts messages.
        self.follower_lengths = {
            name: {peer: 0 for peer in self.peers} for name in partitions if self.is_leader(name)
        }
        # On the leader: when each follower last answered (time.monotonic()).
        started = _now(now)
        self.last_contact = {name: {peer: started for peer in self.peers} for name in self.follower_lengths}
        # On a follower: when it last heard from a leader, or agreed to a
        # takeover (None if never). The takeover timer counts from startup
        # until then.
        self.started = started
        self.last_heard = {name: None for name in partitions}
        # The highest epoch other brokers have mentioned, so the next
        # takeover attempt asks for one above it.
        self.seen_epochs = {name: 0 for name in partitions}

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

    def record_reply(self, name, follower, message, accepted, length, now=None):
        """Update what `follower` is known to have from its reply to `message`.

        If it accepted, it now matches the leader up to the end of the
        message. If not, the message didn't attach, and `length` is where the
        follower asks the leader to resend from (never later than one entry
        before this message). A reply to a message sent in an earlier epoch
        is ignored. Returns True if the commit index moved.
        """
        if not self.is_leader(name) or message["epoch"] != self.partitions[name].epoch:
            return False
        self.last_contact[name][follower] = _now(now)
        start = message["start"]
        if accepted:
            return self.record_follower(name, follower, start + len(message["entries"]))
        self.follower_lengths[name][follower] = min(length, max(start - 1, 0))
        return False

    def is_behind(self, name, follower):
        return self.follower_lengths[name][follower] < len(self.partitions[name])

    def record_follower(self, name, follower, length):
        """Record that `follower` matches the leader's first `length` entries.

        Then commit whatever a majority of brokers have, as long as that
        line carries this leader's epoch: lines from earlier epochs are only
        committed by committing a line of the current epoch after them (a new
        leader marks the lines it inherits with its epoch, so this doesn't
        hold anything up). The length can go down, for example when a
        follower restarts empty; the leader then resends from there. The
        commit index never goes down. Returns True if the commit index moved
        forward.
        """
        self.follower_lengths[name][follower] = length
        partition = self.partitions[name]
        own = len(partition)
        lengths = [own] + [min(n, own) for n in self.follower_lengths[name].values()]
        majority_offset = commit_index_from_lengths(lengths)
        if (majority_offset > partition.commit_index
                and partition.entries[majority_offset]["epoch"] == partition.epoch):
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

        Messages from an older epoch raise StaleEpochError. A newer epoch
        means another broker has taken over: this broker follows it,
        stepping down first if it was the leader.
        """
        partition = self._partition(name)
        now = _now(now)
        if epoch < partition.epoch:
            raise StaleEpochError(epoch, partition.epoch)
        if epoch > partition.epoch:
            if self.is_leader(name):
                self.step_down(name, now)
            partition.set_epoch(epoch)
            self.leaders[name] = leader
        elif self.leaders[name] is None:
            self.leaders[name] = leader
        if self.is_leader(name) or leader != self.leaders[name]:
            raise NotFollowerError(name, self.leaders[name])
        self.last_heard[name] = now
        try:
            partition.replicate(start, entries, epoch, prev_msg_id, prev_epoch)
        except GapError:
            if start > len(partition):
                return False, len(partition)
            return False, partition.commit_index + 1
        partition.follow_commit(leader_commit, verified_end=start + len(entries) - 1)
        return True, len(partition)

    def rank(self, name):
        """This broker's place in `name`'s preference order (0 is first)."""
        return self.preference[name].index(self.id)

    def should_take_over(self, name, now=None):
        """Return True if this broker should try to take over `name` now.

        It waits `leader_timeout` after it last heard from a leader, plus
        `stagger` for each broker ahead of it in the preference order, so
        brokers further down the order only try if the ones before them
        haven't taken over.
        """
        heard = self.last_heard[name]
        waited = _now(now) - (self.started if heard is None else heard)
        return not self.is_leader(name) and waited >= self.leader_timeout + self.rank(name) * self.stagger

    def wait_again(self, name, now=None):
        """Restart the takeover timer, after an attempt that didn't succeed."""
        self.last_heard[name] = _now(now)

    def note_epoch(self, name, epoch):
        """Remember an epoch another broker mentioned."""
        self.seen_epochs[name] = max(self.seen_epochs[name], epoch)

    def next_epoch(self, name):
        """The epoch to ask for in the next takeover attempt."""
        return max(self.partitions[name].epoch, self.seen_epochs[name]) + 1

    def grant(self, name, candidate, epoch, commit_index, dry_run=False, now=None):
        """Answer `candidate`'s request to take over `name` with `epoch`.

        This broker refuses if it leads `name`, if it heard from a leader in
        the last `leader_timeout` seconds, if it started less than half that
        ago (a restarted broker first gives the current leader a chance to
        reach it), or if it already knows `epoch` (it agrees to at most one
        broker per epoch). With `dry_run`, it only says
        whether it would agree, so a broker cut off from the others can find
        out without raising anyone's epoch. Otherwise agreeing moves this
        broker to `epoch`, so it ignores the old leader from then on, and the
        reply carries this broker's entries after `commit_index` (the
        candidate's commit index), for the candidate to take over if they
        are more up to date than its own.
        """
        partition = self._partition(name)
        now = _now(now)
        heard = self.last_heard[name]
        if self.is_leader(name):
            reason = "leader"
        elif heard is not None and now - heard < self.leader_timeout:
            reason = "leader_alive"
        elif now - self.started < self.leader_timeout / 2:
            reason = "just_started"
        elif epoch <= partition.epoch:
            reason = "epoch_taken"
        else:
            reason = None
        if reason is not None or dry_run:
            return {"granted": reason is None, "epoch": partition.epoch, "reason": reason}
        partition.set_epoch(epoch)
        self.leaders[name] = None
        self.last_heard[name] = now
        return {
            "granted": True,
            "epoch": epoch,
            "reason": None,
            "commit_index": partition.commit_index,
            "length": len(partition),
            "last_epoch": partition.last_epoch,
            "entries": partition.read_uncommitted(commit_index + 1),
        }

    def start_campaign(self, name, epoch, now=None):
        """Agree to this broker's own takeover of `name` with `epoch`.

        Returns False if `epoch` is no longer new (this broker agreed to
        another broker's takeover meanwhile) or this broker already leads.
        """
        partition = self.partitions[name]
        if self.is_leader(name) or epoch <= partition.epoch:
            return False
        partition.set_epoch(epoch)
        self.leaders[name] = None
        self.last_heard[name] = _now(now)
        return True

    def become_leader(self, name, epoch, commit_index, replies, now=None):
        """Finish taking over `name`, using the replies of agreeing brokers.

        `commit_index` is the one this broker sent with its request (each
        reply's entries start just after it). Keeps the most up-to-date log
        of this broker and the replies, commits whatever any of them had
        committed, and marks the rest with `epoch`. Returns False, changing
        nothing, if another broker has taken over in the meantime.
        """
        partition = self.partitions[name]
        if partition.epoch != epoch or self.leaders[name] is not None or partition.commit_index != commit_index:
            return False
        best = max(replies, key=lambda r: (r["last_epoch"], r["length"]), default=None)
        if (best is not None and (best["last_epoch"], best["length"]) > (partition.last_epoch, len(partition))
                and best["length"] > commit_index):
            partition.adopt(commit_index + 1, best["entries"])
        committed = max([commit_index] + [r["commit_index"] for r in replies])
        partition.commit(min(committed, len(partition) - 1))
        partition.restamp(partition.commit_index + 1, epoch)

        now = _now(now)
        self.leaders[name] = self.id
        # A follower's copy is only known to match up to the committed lines;
        # the first message checks from there and the follower answers with
        # where to resend from if it has less.
        self.follower_lengths[name] = {peer: partition.commit_index + 1 for peer in self.peers}
        self.last_contact[name] = {peer: now for peer in self.peers}
        return True

    def step_down(self, name, now=None, epoch=None):
        """Stop leading `name`, and move to `epoch` if a newer one is known.

        The lines this broker never committed were never acknowledged, so
        they are dropped; the next leader's log replaces them.
        """
        if epoch is not None:
            self.note_epoch(name, epoch)
            if epoch > self.partitions[name].epoch:
                self.partitions[name].set_epoch(epoch)
        if self.is_leader(name):
            self.leaders[name] = None
            del self.follower_lengths[name]
            del self.last_contact[name]
            self.partitions[name].truncate_to_commit()
        self.last_heard[name] = _now(now)

    def lost_majority(self, name, now=None):
        """Return True if this leader hasn't reached a majority for `leader_timeout`."""
        if not self.is_leader(name):
            return False
        now = _now(now)
        recent = sum(now - t < self.leader_timeout for t in self.last_contact[name].values())
        return 1 + recent < self.majority

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


def _now(now):
    return time.monotonic() if now is None else now
