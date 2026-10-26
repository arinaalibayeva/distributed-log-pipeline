"""Simulates one service sending its log lines to the cluster.

Run one per service:

    python -m producer.producer login [--rate 5]

Each line gets a msg_id the moment it is created, and that ID is reused on
every retry, so a line resent after a lost ack is stored only once. Lines wait
in a buffer until a leader acks them, oldest first, so they reach the
partition in the order the service produced them.

The producer reports how it's doing to the dashboard through a small file
(see ProducerFiles), and takes commands back the same way: pause, resume, or
send a burst of errors so an alert can be shown on demand.
"""

import argparse
import json
import os
import random
import time
import uuid
from collections import deque
from pathlib import Path

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

    def burst(self, count=5):
        """Return `count` ERROR lines at once, enough to set off an alert."""
        return [self.make_line("ERROR") for _ in range(count)]

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


class ProducerFiles:
    """The files a producer shares with the dashboard.

    The producer writes its status to <directory>/<service>.json and reads
    commands from <directory>/<service>.control.json, which the dashboard
    writes: {"paused": true/false, "burst": n}. `burst` is a counter; each
    time it goes up, the producer sends one burst of errors.
    """

    def __init__(self, directory, service):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.status_path = self.directory / f"{service}.json"
        self.control_path = self.directory / f"{service}.control.json"

    def write_status(self, status):
        # Write to a temporary file and rename it, so the dashboard never
        # reads a half-written file.
        tmp = self.status_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({**status, "updated": time.time()}))
        os.replace(tmp, self.status_path)

    def read_control(self):
        try:
            control = json.loads(self.control_path.read_text())
        except (OSError, ValueError):
            control = {}
        return {"paused": bool(control.get("paused", False)), "burst": int(control.get("burst", 0))}


def main():
    parser = argparse.ArgumentParser(description="Simulate one service sending log lines.")
    parser.add_argument("service", choices=config.PARTITIONS)
    parser.add_argument("--rate", type=float, default=5.0, help="lines per second (default 5)")
    parser.add_argument("--dir", default="data/producers", help="where to share status with the dashboard")
    args = parser.parse_args()

    generator = LineGenerator(args.service)
    producer = Producer(args.service, ClusterClient())
    files = ProducerFiles(args.dir, args.service)
    bursts_done = files.read_control()["burst"]
    interval = 1.0 / args.rate
    last_report = time.monotonic()
    print(f"{args.service} producer sending {args.rate:g} lines/s", flush=True)
    try:
        while True:
            control = files.read_control()
            if control["burst"] > bursts_done:
                bursts_done = control["burst"]
                producer.add(generator.burst())
                print(f"{args.service}: sending a burst of errors", flush=True)
            if not control["paused"]:
                producer.add(generator.next_lines())
            flushed = producer.flush()
            files.write_status({
                "service": args.service,
                "acked": producer.acked,
                "waiting": len(producer.pending),
                "leader": producer.client.leader[args.service],
                "paused": control["paused"],
                "rate": args.rate,
            })
            if not flushed:
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
