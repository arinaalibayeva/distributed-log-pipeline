"""An append-only log for one service.

A partition stores log lines in the order they arrive. Each line gets an
offset (0, 1, 2, ...) that never changes once assigned. This module has no
networking code, so it can be tested on its own.
"""


class Partition:
    def __init__(self, name):
        self.name = name
        self.entries = []

    def __len__(self):
        return len(self.entries)

    def append(self, line):
        """Store a log line at the end of the log and return its offset."""
        offset = len(self.entries)
        self.entries.append({"offset": offset, "line": line})
        return offset

    def read(self, start=0):
        """Return every entry from offset `start` onward.

        `start` is the next offset a reader wants, so a consumer that has
        already read offsets 0-4 calls read(5).
        """
        if start < 0:
            raise ValueError("start must be 0 or greater")
        return self.entries[start:]
