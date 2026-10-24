"""Failure scenarios: network partitions and crashes, checked against the guarantees.

A cut-off broker can still be reached by producers and consumers, but not by
the other brokers, so these tests also show what a client on the wrong side
of a partition sees.
"""

import threading
import time

from conftest import check_invariants, make_line, wait_for_copies, wait_until
from producer.producer import Producer


def produce(cluster, service, numbers):
    """Send lines until all are acked (retrying through takeovers); return their msg_ids."""
    producer = Producer(service, cluster.client())
    lines = [make_line(n, service=service) for n in numbers]
    producer.add(lines)
    wait_until(producer.flush, message=f"{service} lines to be acked")
    return [line["msg_id"] for line in lines]


def msg_ids(cluster, broker_id, partition):
    return [e["line"]["msg_id"] for e in cluster.entries(broker_id, partition)]


class Writer(threading.Thread):
    """Keeps producing lines one at a time, recording each one that is acked."""

    def __init__(self, cluster, service):
        super().__init__(daemon=True)
        self.service = service
        self.producer = Producer(service, cluster.client())
        self.acked = []
        self.stopping = threading.Event()

    def run(self):
        n = 0
        while not self.stopping.is_set():
            line = make_line(n, service=self.service)
            n += 1
            self.producer.add([line])
            while not self.producer.flush():
                if self.stopping.is_set():
                    return
                time.sleep(0.02)
            self.acked.append(line["msg_id"])
            time.sleep(0.005)

    def stop(self):
        self.stopping.set()
        self.join(timeout=10)


def test_cut_off_leader_stops_committing_and_the_majority_takes_over(cluster):
    acked = produce(cluster, "login", range(10))
    cluster.cut_off(1)
    committed_when_cut = cluster.node(1).partitions["login"].commit_index
    cluster.node(1).append("login", make_line(99))  # reaches the old leader only, so it's never acked

    new_leader = cluster.wait_for_leader("login", among=(2, 3))
    assert new_leader == 2
    acked += produce(cluster, "login", range(10, 15))  # producers move to the majority side
    wait_until(lambda: not cluster.node(1).is_leader("login"), message="broker 1 to step down")
    assert cluster.node(1).partitions["login"].commit_index == committed_when_cut

    cluster.heal()
    wait_for_copies(cluster, "login", 15)
    assert cluster.node(1).leaders["login"] == 2
    assert cluster.node(1).partitions["login"].epoch == cluster.node(2).partitions["login"].epoch
    assert all("login-99" not in msg_ids(cluster, b, "login") for b in (1, 2, 3))
    check_invariants(cluster, "login", 2, acked)


def test_cut_off_follower_catches_up_without_disturbing_the_leader(cluster):
    acked = produce(cluster, "login", range(5))
    cluster.cut_off(3)
    acked += produce(cluster, "login", range(5, 15))  # the leader still has a majority with broker 2
    assert len(cluster.entries(3, "login")) <= 5
    # Broker 3 stops hearing from login's leader and keeps trying to take
    # over, but it can't reach anyone, so it never raises the epoch.
    time.sleep(1.0)
    assert cluster.node(3).partitions["login"].epoch == 1

    cluster.heal()
    wait_for_copies(cluster, "login", 15)
    assert cluster.leader("login") == 1
    assert all(cluster.node(b).partitions["login"].epoch == 1 for b in (1, 2, 3))
    check_invariants(cluster, "login", 1, acked)
    # Broker 3 led upload, so upload moved to the majority side, and broker 3
    # now follows it.
    upload_leader = cluster.wait_for_leader("upload", among=(1, 2))
    wait_until(lambda: cluster.node(3).leaders["upload"] == upload_leader, message="broker 3 to follow upload's leader")


def test_both_sides_of_a_partition_never_commit_different_lines(cluster):
    # Broker 1 leads login and is cut off while a producer keeps writing to
    # whichever broker answers. Whatever each side committed must end up in
    # one history with no line twice.
    writer = Writer(cluster, "login")
    writer.start()
    try:
        wait_until(lambda: len(writer.acked) >= 10, message="some lines to be acked")
        cluster.cut_off(1)
        cluster.wait_for_leader("login", among=(2, 3))
        count = len(writer.acked)
        wait_until(lambda: len(writer.acked) >= count + 10, message="writes to resume on the majority side")
        cluster.heal()
        wait_until(lambda: cluster.node(1).leaders["login"] == cluster.leader("login"), message="broker 1 to rejoin")
        count = len(writer.acked)
        wait_until(lambda: len(writer.acked) >= count + 10, message="writes to continue after healing")
    finally:
        writer.stop()
    leader = cluster.wait_for_leader("login")
    wait_for_copies(cluster, "login", len(cluster.entries(leader, "login")))
    check_invariants(cluster, "login", leader, writer.acked)


def test_writes_survive_repeated_leader_crashes(cluster):
    writer = Writer(cluster, "login")
    writer.start()
    try:
        for _ in range(3):
            count = len(writer.acked)
            wait_until(lambda: len(writer.acked) >= count + 10, message="writes to make progress")
            leader = cluster.wait_for_leader("login")
            cluster.stop(leader)
            new_leader = cluster.wait_for_leader("login", among=[b for b in (1, 2, 3) if b != leader])
            cluster.start(leader)
            # One failure at a time: let the restarted broker catch up before the next crash.
            target = cluster.node(new_leader).partitions["login"].commit_index
            wait_until(lambda: cluster.node(leader).partitions["login"].commit_index >= target,
                       message=f"broker {leader} to catch up")
    finally:
        writer.stop()
    leader = cluster.wait_for_leader("login")
    wait_for_copies(cluster, "login", len(cluster.entries(leader, "login")))
    check_invariants(cluster, "login", leader, writer.acked)
    assert len(writer.acked) >= 30


def test_line_stored_only_on_a_crashed_leader_is_stored_at_most_once(cluster):
    # The leader stores a line but can't copy it before it crashes, so the
    # line was never acked. The producer retries it with the new leader.
    acked = produce(cluster, "login", range(3))
    cluster.cut_off(1)
    producer = Producer("login", cluster.client())
    producer.add([make_line(3)])
    assert producer.flush() is False
    cluster.stop(1)
    cluster.heal()
    leader = cluster.wait_for_leader("login", among=(2, 3))
    wait_until(producer.flush, message="the retried line to be acked")
    wait_for_copies(cluster, "login", 4, brokers=(2, 3))
    check_invariants(cluster, "login", leader, acked + ["login-3"], brokers=(2, 3))
