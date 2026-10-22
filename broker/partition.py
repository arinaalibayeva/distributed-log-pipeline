"""An append-only log for one service.

A partition stores log lines in the order they arrive. Each line gets an
offset (0, 1, 2, ...) that never changes once assigned. This module has no
networking code, so it can be tested on its own.
"""


class StaleEpochError(Exception):
    """Raised when a message comes from a leader with an older epoch."""


class GapError(Exception):
    """Raised when copied entries don't attach to this log.

    Either they start past the end (earlier entries are missing), or the
    entry just before them isn't the one the leader has there.
    """


class ConflictError(Exception):
    """Raised when copied entries disagree with a committed entry."""


class Partition:
    def __init__(self, name):
        self.name = name
        self.entries = []
        self.msg_ids = {}  # msg_id -> offset, for rejecting duplicates
        self.commit_index = -1  # highest committed offset; -1 means none yet
        self.epoch = 0  # increases every time a new leader takes over

    def __len__(self):
        return len(self.entries)

    def set_epoch(self, epoch):
        """Move to a newer epoch, or raise StaleEpochError for an older one."""
        if epoch < self.epoch:
            raise StaleEpochError(f"epoch {epoch} is older than current epoch {self.epoch}")
        self.epoch = epoch

    def append(self, line, epoch=None):
        """Store a log line at the end of the log and return its offset.

        If a line with the same msg_id is already stored (a producer retried
        after a lost ack), nothing is added and the existing offset is returned.

        `epoch` is the epoch of the leader writing the line and defaults to
        the current one. A line from an older epoch is rejected, so a leader
        that was cut off and comes back cannot overwrite anything.
        """
        if epoch is None:
            epoch = self.epoch
        self.set_epoch(epoch)
        msg_id = line["msg_id"]
        if msg_id in self.msg_ids:
            return self.msg_ids[msg_id]
        offset = len(self.entries)
        self.entries.append({"offset": offset, "epoch": epoch, "line": line})
        self.msg_ids[msg_id] = offset
        return offset

    def replicate(self, start, entries, epoch, prev_msg_id=None):
        """Store entries copied from the leader, beginning at offset `start`.

        Followers use this instead of append(), so every copy keeps the
        leader's offsets. `prev_msg_id` is the msg_id the leader has at
        offset start - 1; checking it proves this log matches the leader's
        up to `start`, so a follower never mistakes different lines at the
        same offsets for its leader's. Entries this log already has (same
        msg_id at the same offset) are skipped. An entry that differs from
        an uncommitted one replaces it and everything after it, because the
        leader's log wins. Returns the new length.

        Raises GapError if the entries don't attach (see GapError),
        ConflictError if an entry differs from a committed one,
        StaleEpochError if `epoch` is older than this partition's, and
        ValueError if the entries' offsets don't run consecutively from
        `start`. No entries are stored when it raises.
        """
        self.set_epoch(epoch)
        if [entry["offset"] for entry in entries] != list(range(start, start + len(entries))):
            raise ValueError("entry offsets must run consecutively from start")
        if start > len(self.entries):
            raise GapError(f"log ends at offset {len(self.entries)}, entries start at {start}")
        if start > 0 and self.entries[start - 1]["line"]["msg_id"] != prev_msg_id:
            raise GapError(f"entry at offset {start - 1} isn't the leader's")
        for entry in entries:
            offset = entry["offset"]
            if offset < len(self.entries):
                if self.entries[offset]["line"]["msg_id"] == entry["line"]["msg_id"]:
                    continue
                if offset <= self.commit_index:
                    raise ConflictError(f"entry at committed offset {offset} differs from the leader's")

        for entry in entries:
            offset = entry["offset"]
            if offset < len(self.entries):
                if self.entries[offset]["line"]["msg_id"] == entry["line"]["msg_id"]:
                    continue
                self._truncate_from(offset)
            self.entries.append({"offset": offset, "epoch": entry["epoch"], "line": entry["line"]})
            self.msg_ids[entry["line"]["msg_id"]] = offset
        return len(self.entries)

    def follow_commit(self, leader_commit, verified_end=None):
        """Move the commit index up to the leader's, capped at what's known to match.

        A follower can't commit lines it doesn't have, or lines it hasn't
        confirmed are the leader's. `verified_end` is the offset up to which
        this log is known to match the leader (defaults to the end of the log).
        """
        end = len(self.entries) - 1 if verified_end is None else min(verified_end, len(self.entries) - 1)
        self.commit(min(leader_commit, end))

    def commit(self, offset):
        """Mark every line up to and including `offset` as committed.

        The commit index only moves forward, so an older or repeated value
        is ignored.
        """
        if offset >= len(self.entries):
            raise ValueError("cannot commit past the end of the log")
        if offset > self.commit_index:
            self.commit_index = offset

    def truncate_to_commit(self):
        """Drop every line after the commit index and return how many were dropped.

        An old leader calls this when it rejoins: lines it stored but never
        committed were never acknowledged, so the new leader's log replaces them.
        """
        return self._truncate_from(self.commit_index + 1)

    def _truncate_from(self, offset):
        dropped = self.entries[offset:]
        del self.entries[offset:]
        for entry in dropped:
            del self.msg_ids[entry["line"]["msg_id"]]
        return len(dropped)

    def read(self, start=0):
        """Return every committed entry from offset `start` onward.

        `start` is the next offset a reader wants, so a consumer that has
        already read offsets 0-4 calls read(5). Lines that are stored but not
        yet committed are left out, because they could still be lost.
        """
        if start < 0:
            raise ValueError("start must be 0 or greater")
        return self.entries[start:self.commit_index + 1]

    def read_uncommitted(self, start=0):
        """Return every stored entry from offset `start` onward.

        The leader uses this to send lines to followers before they are
        committed. Consumers should use read() instead.
        """
        if start < 0:
            raise ValueError("start must be 0 or greater")
        return self.entries[start:]


def commit_index_from_lengths(lengths):
    """Return the highest offset stored on a majority of replicas.

    `lengths` holds the log length of every replica, the leader included.
    With three replicas this is the second-highest length minus one, because
    that offset is on at least two brokers. Returns -1 if no offset is on a
    majority yet.
    """
    if not lengths:
        raise ValueError("need at least one replica")
    majority = len(lengths) // 2 + 1
    return sorted(lengths, reverse=True)[majority - 1] - 1
