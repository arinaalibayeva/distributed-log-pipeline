"""Replication across three live brokers, checked against the guarantees.

After each scenario, check_invariants() verifies what the system promises:
every acknowledged line is stored exactly once on the leader, no copy holds
a msg_id twice, and every copy agrees with the leader up to its commit index.
"""

import pytest

from broker.client import NoLeaderAvailable
from conftest import LocalCluster, make_line, send, wait_until
from producer.producer import Producer


def check_invariants(cluster, partition, leader, acked, brokers=(1, 2, 3)):
    leader_entries = cluster.entries(leader, partition)
    leader_ids = [e["line"]["msg_id"] for e in leader_entries]
    for msg_id in acked:
        assert leader_ids.count(msg_id) == 1, f"acked line {msg_id} stored {leader_ids.count(msg_id)} times"
    for broker_id in brokers:
        entries = cluster.entries(broker_id, partition)
        ids = [e["line"]["msg_id"] for e in entries]
        assert len(ids) == len(set(ids)), f"broker {broker_id} holds a msg_id twice"
        committed = cluster.node(broker_id).partitions[partition].commit_index
        assert entries[:committed + 1] == leader_entries[:committed + 1], (
            f"broker {broker_id} disagrees with the leader below its commit index"
        )


def wait_for_copies(cluster, partition, length, brokers=(1, 2, 3)):
    """Wait until every broker holds `length` entries and has committed them all."""
    def caught_up():
        return all(
            len(cluster.entries(b, partition)) == length
            and cluster.node(b).partitions[partition].commit_index == length - 1
            for b in brokers
        )
    wait_until(caught_up, message=f"{partition} to reach {length} committed entries on {brokers}")


def produce(cluster, service, numbers):
    producer = Producer(service, cluster.client())
    lines = [make_line(n, service=service) for n in numbers]
    producer.add(lines)
    assert producer.flush() is True
    return [line["msg_id"] for line in lines]


def test_every_partition_is_copied_to_all_brokers(cluster):
    acked = {service: produce(cluster, service, range(20)) for service in ("login", "search", "upload")}
    for service, leader in (("login", 1), ("search", 2), ("upload", 3)):
        wait_for_copies(cluster, service, 20)
        check_invariants(cluster, service, leader, acked[service])
        assert cluster.entries(1, service) == cluster.entries(2, service) == cluster.entries(3, service)


def test_followers_learn_the_commit_index_from_heartbeats(cluster):
    produce(cluster, "login", range(5))
    wait_until(lambda: all(cluster.node(b).partitions["login"].commit_index == 4 for b in (2, 3)),
               message="followers to commit")


def test_writes_continue_with_one_follower_down(cluster):
    cluster.stop(3)
    acked = produce(cluster, "login", range(10))
    wait_for_copies(cluster, "login", 10, brokers=(1, 2))
    check_invariants(cluster, "login", 1, acked, brokers=(1, 2))


def test_no_ack_without_a_majority(cluster):
    cluster.stop(2)
    cluster.stop(3)
    client = cluster.client()
    with pytest.raises(NoLeaderAvailable):
        send(client, make_line(0))
    assert cluster.node(1).partitions["login"].commit_index == -1


def test_leader_without_a_majority_steps_down_and_drops_unacked_lines(cluster):
    cluster.stop(2)
    cluster.stop(3)
    cluster.node(1).append("login", make_line(0))
    wait_until(lambda: not cluster.node(1).is_leader("login"), message="broker 1 to step down")
    assert cluster.entries(1, "login") == []
    with pytest.raises(NoLeaderAvailable):
        send(cluster.client(), make_line(1))


def test_unacked_line_is_stored_once_after_a_majority_returns(cluster):
    cluster.stop(2)
    cluster.stop(3)
    producer = Producer("login", cluster.client())
    producer.add([make_line(0)])
    assert producer.flush() is False
    cluster.start(2)
    wait_until(producer.flush, message="the retried line to be acked")
    leader = cluster.wait_for_leader("login")
    wait_for_copies(cluster, "login", 1, brokers=(1, 2))
    check_invariants(cluster, "login", leader, ["login-0"], brokers=(1, 2))


def test_follower_takes_over_when_the_leader_dies(cluster):
    acked = produce(cluster, "login", range(10))
    cluster.stop(1)
    assert cluster.wait_for_leader("login", among=(2, 3)) == 2  # next in login's preference order
    assert cluster.node(2).partitions["login"].epoch == 2  # the first leaders took over in epoch 1
    acked += produce(cluster, "login", range(10, 15))
    wait_for_copies(cluster, "login", 15, brokers=(2, 3))
    check_invariants(cluster, "login", 2, acked, brokers=(2, 3))


def test_restarted_follower_catches_up_from_empty(cluster):
    first = produce(cluster, "login", range(30))
    cluster.stop(2)
    second = produce(cluster, "login", range(30, 60))
    cluster.start(2)
    assert len(cluster.entries(2, "login")) < 60
    wait_for_copies(cluster, "login", 60)
    check_invariants(cluster, "login", 1, first + second)


def test_catch_up_larger_than_one_batch():
    cluster = LocalCluster(batch=7)
    try:
        cluster.stop(3)
        acked = produce(cluster, "login", range(40))
        cluster.start(3)
        wait_for_copies(cluster, "login", 40)
        check_invariants(cluster, "login", 1, acked)
    finally:
        cluster.stop_all()


def test_lost_acks_do_not_create_duplicates_on_any_copy(cluster):
    transport = cluster.transport()
    producer = Producer("upload", cluster.client(transport))
    transport.lose_replies[3] = 3
    lines = [make_line(n, service="upload") for n in range(10)]
    producer.add(lines)
    # Three lost acks in a row use up the client's attempts, so the first
    # flush gives up with lines still buffered; the producer's loop retries.
    wait_until(producer.flush, message="the producer to flush")
    wait_for_copies(cluster, "upload", 10)
    check_invariants(cluster, "upload", 3, [line["msg_id"] for line in lines])


def test_both_followers_restart_and_rebuild_the_copies(cluster):
    acked = produce(cluster, "search", range(15))
    cluster.stop(1)
    cluster.stop(3)
    cluster.start(1)
    cluster.start(3)
    wait_for_copies(cluster, "search", 15)
    check_invariants(cluster, "search", 2, acked)


def test_restarted_leader_rejoins_as_a_follower_and_catches_up(cluster):
    # A leader that restarts comes back with an empty log. It must not lead
    # again with that log: another broker takes over, and the restarted one
    # follows it and copies everything back.
    acked = produce(cluster, "upload", range(10))
    cluster.stop(3)
    cluster.start(3)
    leader = cluster.wait_for_leader("upload", among=(1, 2))
    acked += produce(cluster, "upload", range(10, 15))
    wait_for_copies(cluster, "upload", 15)
    assert cluster.node(3).leaders["upload"] == leader
    check_invariants(cluster, "upload", leader, acked)
