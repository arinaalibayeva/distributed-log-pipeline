import random

from conftest import make_line
from producer.producer import LineGenerator, Producer


def stored_ids(cluster, broker_id, partition):
    return [e["line"]["msg_id"] for e in cluster.node(broker_id).read(partition, limit=1000)]


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
    assert stored_ids(cluster, 1, "login") == [f"login-{n}" for n in range(5)]


def test_lines_stay_buffered_while_no_leader(cluster):
    producer = Producer("login", cluster.client())
    cluster.down.add(1)
    producer.add([make_line(n) for n in range(3)])
    assert producer.flush() is False
    assert len(producer.pending) == 3

    cluster.down.discard(1)
    producer.add([make_line(3)])
    assert producer.flush() is True
    assert stored_ids(cluster, 1, "login") == [f"login-{n}" for n in range(4)]


def test_lost_ack_is_retried_without_duplicate(cluster):
    producer = Producer("login", cluster.client())
    cluster.lose_replies[1] = 1
    producer.add([make_line(0), make_line(1)])
    assert producer.flush() is True
    assert stored_ids(cluster, 1, "login") == ["login-0", "login-1"]


def test_producer_finds_leader_from_wrong_guess(cluster):
    client = cluster.client()
    client.leader["upload"] = 2
    producer = Producer("upload", client)
    producer.add([make_line(0, service="upload")])
    assert producer.flush() is True
    assert stored_ids(cluster, 3, "upload") == ["upload-0"]
