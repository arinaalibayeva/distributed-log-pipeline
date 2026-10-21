"""Writes every committed line to a file, one file per partition.

    python -m consumers.archive [--dir data/archive]

Each file holds one JSON entry per line, in offset order. On restart the
archive picks up where it left off: a file with N lines already holds
offsets 0 to N-1, so reading resumes from offset N. Brokers keep their logs
in memory, so a restarted cluster starts again from offset 0; the files only
match the cluster they were written from, and start_cluster.sh clears them.
"""

import argparse
import json
import time
from pathlib import Path

from broker import config
from broker.client import ClusterClient
from consumers.consumer import Consumer


class Archive:
    def __init__(self, client, directory, partitions=config.PARTITIONS):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        offsets = {name: self._count_lines(name) for name in partitions}
        self.consumer = Consumer(client, partitions, offsets=offsets)

    def path(self, partition):
        return self.directory / f"{partition}.jsonl"

    def poll(self):
        """Append new committed entries to their files; return how many."""
        entries = self.consumer.poll()
        by_partition = {}
        for entry in entries:
            by_partition.setdefault(entry["line"]["service"], []).append(entry)
        for partition, batch in by_partition.items():
            with self.path(partition).open("a") as f:
                for entry in batch:
                    f.write(json.dumps(entry) + "\n")
        return len(entries)

    def _count_lines(self, partition):
        path = self.path(partition)
        if not path.exists():
            return 0
        with path.open() as f:
            return sum(1 for _ in f)


def main():
    parser = argparse.ArgumentParser(description="Write every committed log line to files.")
    parser.add_argument("--dir", default="data/archive", help="where to write (default data/archive)")
    args = parser.parse_args()

    archive = Archive(ClusterClient(), args.dir)
    print(f"archive writing to {archive.directory}/, starting at {archive.consumer.offsets}", flush=True)
    total = 0
    last_report = time.monotonic()
    try:
        while True:
            total += archive.poll()
            if time.monotonic() - last_report >= 5:
                print(f"archived {total} lines", flush=True)
                last_report = time.monotonic()
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
