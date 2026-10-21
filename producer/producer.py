"""Simulates one service sending its log lines to the cluster.

Run one per service:

    python -m producer.producer login [--rate 5]

Each line gets a msg_id the moment it is created, and that ID is reused on
every retry, so a line resent after a lost ack is stored only once. Lines wait
in a buffer until a leader acks them, oldest first, so they reach the
partition in the order the service produced them.
"""

import argparse
import random
import time
import uuid
from collections import deque

from broker import config
from broker.client import ClusterClient, NoLeaderAvailable

MESSAGES = {
    "login": {
        "INFO": ["user {n} logged in", "user {n} logged out", "session refreshed for user {n}"],
        "WARN": ["slow password check for user {n}", "user {n} has 2 failed attempts"],
        "ERROR": ["invalid password for user {n}", "auth database timeout for user {n}"],
    },
    "search": {
        "INFO": ["query by user {n} returned 12 results", "query by user {n} served from cache"],
        "WARN": ["query by user {n} took 900 ms", "index shard {n} is lagging"],
        "ERROR": ["query by user {n} failed: index unavailable", "search worker {n} crashed"],
    },
    "upload": {
        "INFO": ["user {n} uploaded a file", "upload {n} finished processing"],
        "WARN": ["upload {n} is close to the size limit", "upload {n} retried a chunk"],
        "ERROR": ["upload {n} failed: storage full", "virus scan failed for upload {n}"],
    },
}


class LineGenerator:
    """Makes realistic-looking log lines for one service.

    Most lines are INFO, some WARN and a few ERROR. Now and then it produces
    a burst of errors in quick succession, so the alert consumer has
    something to catch.
    """

    def __init__(self, service, rng=None, clock=time.time, burst_chance=0.01):
        self.service = service
        self.rng = rng or random.Random()
        self.clock = clock
        self.burst_chance = burst_chance

    def next_lines(self):
        """Return the next line, or a burst of 3-5 ERROR lines."""
        if self.rng.random() < self.burst_chance:
            return [self.make_line("ERROR") for _ in range(self.rng.randint(3, 5))]
        level = self.rng.choices(["INFO", "WARN", "ERROR"], weights=[85, 13, 2])[0]
        return [self.make_line(level)]

    def make_line(self, level):
        text = self.rng.choice(MESSAGES[self.service][level]).format(n=self.rng.randint(1, 999))
        return {
            "msg_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "service": self.service,
            "level": level,
            "text": text,
            "ts": self.clock(),
        }


class Producer:
    """Buffers lines and sends them to the leader until each one is acked."""

    def __init__(self, service, client):
        self.service = service
        self.client = client
        self.pending = deque()
        self.acked = 0

    def add(self, lines):
        self.pending.extend(lines)

    def flush(self):
        """Send buffered lines, oldest first, until the buffer is empty.

        Stops early and keeps the rest buffered if no leader can be reached,
        so nothing is dropped. Returns True if the buffer was emptied.
        """
        path = f"/partitions/{self.service}/append"
        while self.pending:
            try:
                self.client.call(self.service, "POST", path, json=self.pending[0])
            except NoLeaderAvailable:
                return False
            self.pending.popleft()
            self.acked += 1
        return True


def main():
    parser = argparse.ArgumentParser(description="Simulate one service sending log lines.")
    parser.add_argument("service", choices=config.PARTITIONS)
    parser.add_argument("--rate", type=float, default=5.0, help="lines per second (default 5)")
    args = parser.parse_args()

    generator = LineGenerator(args.service)
    producer = Producer(args.service, ClusterClient())
    interval = 1.0 / args.rate
    last_report = time.monotonic()
    print(f"{args.service} producer sending {args.rate:g} lines/s", flush=True)
    try:
        while True:
            producer.add(generator.next_lines())
            if not producer.flush():
                time.sleep(0.5)  # no leader reachable; back off, keep buffering
            if time.monotonic() - last_report >= 5:
                leader = producer.client.leader[args.service]
                print(f"{args.service}: acked {producer.acked}, buffered {len(producer.pending)}, "
                      f"leader guess broker {leader}", flush=True)
                last_report = time.monotonic()
            time.sleep(interval)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
