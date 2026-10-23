import random

from conftest import make_line, wait_until
from producer.producer import LineGenerator, Producer


def msg_ids(cluster, broker_id, partition):
    return [e["line"]["msg_id"] for e in cluster.entries(broker_id, partition)]


def test_generator_makes_valid_lines():
    gen = LineGenerator("search", rng=random.Random(1), clock=lambda: 100.0)
    lines = [line for _ in range(200) for line in gen.next_lines()]
    assert all(line["service"] == "search" for line in lines)
    assert all(line["level"] in {"INFO", "WARN", "ERROR"} for line in lines)
    assert all(line["ts"] == 100.0 for line in lines)
    assert len({line["msg_id"] for line in lines}) == len(lines)


def test_generator_is_mostly_info():
    gen = LineGenerator("login", rng=random.Random(2), burst_chance=0)
    levels = [gen.next_lines()[0]["level"] for _ in range(1000)]
    assert levels.count("INFO") > 700
    assert levels.count("ERROR") < 100


def test_generator_bursts_are_three_to_five_errors():
    gen = LineGenerator("upload", rng=random.Random(3), burst_chance=1.0)
    for _ in range(20):
        burst = gen.next_lines()
        assert 3 <= len(burst) <= 5
        assert all(line["level"] == "ERROR" for line in burst)


def test_flush_sends_lines_in_order(cluster):
    producer = Producer("login", cluster.client())
    producer.add([make_line(n) for n in range(5)])
    assert producer.flush() is True
    assert producer.acked == 5
    assert msg_ids(cluster, 1, "login") == [f"login-{n}" for n in range(5)]


def test_acked_lines_are_on_a_majority(cluster):
    producer = Producer("login", cluster.client())
    producer.add([make_line(n) for n in range(5)])
    producer.flush()
    copies = sum(len(cluster.entries(b, "login")) == 5 for b in (1, 2, 3))
    assert copies >= 2


def test_lines_buffered_while_the_leader_is_down_reach_the_new_leader(cluster):
    producer = Producer("login", cluster.client())
    producer.add([make_line(0)])
    assert producer.flush() is True
    cluster.stop(1)
    producer.add([make_line(n) for n in range(1, 4)])
    wait_until(producer.flush, message="the producer to reach the new leader")
    assert cluster.leader("login") == 2
    assert msg_ids(cluster, 2, "login") == [f"login-{n}" for n in range(4)]


def test_lines_stay_buffered_without_a_majority(cluster):
    producer = Producer("login", cluster.client())
    cluster.stop(2)
    cluster.stop(3)
    producer.add([make_line(0), make_line(1)])
    assert producer.flush() is False
    assert producer.acked == 0
    assert len(producer.pending) == 2

    cluster.start(3)
    wait_until(producer.flush, message="the producer to flush once a majority is back")
    for b in (1, 3):
        wait_until(lambda: msg_ids(cluster, b, "login") == ["login-0", "login-1"], message=f"broker {b} to have both")


def test_lost_ack_is_retried_without_duplicate(cluster):
    transport = cluster.transport()
    producer = Producer("login", cluster.client(transport))
    transport.lose_replies[1] = 1
    producer.add([make_line(0), make_line(1)])
    assert producer.flush() is True
    wait_until(lambda: all(len(cluster.entries(b, "login")) == 2 for b in (1, 2, 3)), message="replication")
    for b in (1, 2, 3):
        assert msg_ids(cluster, b, "login") == ["login-0", "login-1"]


def test_producer_finds_leader_from_wrong_guess(cluster):
    client = cluster.client()
    client.leader["upload"] = 2
    producer = Producer("upload", client)
    producer.add([make_line(0, service="upload")])
    assert producer.flush() is True
    assert msg_ids(cluster, 3, "upload") == ["upload-0"]
