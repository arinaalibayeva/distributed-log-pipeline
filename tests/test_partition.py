import pytest

from broker.partition import Partition


def make_line(n):
    return {"msg_id": f"id-{n}", "service": "login", "level": "INFO", "text": f"line {n}", "ts": float(n)}


def test_new_partition_is_empty():
    p = Partition("login")
    assert len(p) == 0
    assert p.read() == []


def test_append_returns_increasing_offsets():
    p = Partition("login")
    assert p.append(make_line(0)) == 0
    assert p.append(make_line(1)) == 1
    assert p.append(make_line(2)) == 2
    assert len(p) == 3


def test_read_returns_lines_in_order():
    p = Partition("login")
    for n in range(3):
        p.append(make_line(n))
    entries = p.read()
    assert [e["offset"] for e in entries] == [0, 1, 2]
    assert [e["line"]["msg_id"] for e in entries] == ["id-0", "id-1", "id-2"]


def test_read_from_offset_skips_earlier_lines():
    p = Partition("login")
    for n in range(5):
        p.append(make_line(n))
    assert [e["offset"] for e in p.read(3)] == [3, 4]


def test_read_past_end_returns_nothing():
    p = Partition("login")
    p.append(make_line(0))
    assert p.read(1) == []
    assert p.read(10) == []


def test_read_negative_offset_is_rejected():
    p = Partition("login")
    with pytest.raises(ValueError):
        p.read(-1)


def test_duplicate_msg_id_is_stored_once():
    p = Partition("login")
    assert p.append(make_line(0)) == 0
    assert p.append(make_line(1)) == 1
    assert p.append(make_line(0)) == 0
    assert len(p) == 2


def test_line_without_msg_id_is_rejected():
    p = Partition("login")
    with pytest.raises(KeyError):
        p.append({"service": "login", "level": "INFO", "text": "no id", "ts": 0.0})
