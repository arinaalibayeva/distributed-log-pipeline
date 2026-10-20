"""An append-only log for one service.

A partition stores log lines in the order they arrive. Each line gets an
offset (0, 1, 2, ...) that never changes once assigned. This module has no
networking code, so it can be tested on its own.
"""


class StaleEpochError(Exception):
    """Raised when a message comes from a leader with an older epoch."""


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
        dropped = self.entries[self.commit_index + 1:]
        del self.entries[self.commit_index + 1:]
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
