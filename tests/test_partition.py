import pytest

from broker.partition import Partition, StaleEpochError, commit_index_from_lengths


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
    p.commit(2)
    entries = p.read()
    assert [e["offset"] for e in entries] == [0, 1, 2]
    assert [e["line"]["msg_id"] for e in entries] == ["id-0", "id-1", "id-2"]


def test_read_from_offset_skips_earlier_lines():
    p = Partition("login")
    for n in range(5):
        p.append(make_line(n))
    p.commit(4)
    assert [e["offset"] for e in p.read(3)] == [3, 4]


def test_read_past_end_returns_nothing():
    p = Partition("login")
    p.append(make_line(0))
    p.commit(0)
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


def test_uncommitted_lines_are_not_read():
    p = Partition("login")
    for n in range(3):
        p.append(make_line(n))
    assert p.read() == []
    p.commit(1)
    assert [e["offset"] for e in p.read()] == [0, 1]


def test_read_uncommitted_returns_everything_stored():
    p = Partition("login")
    for n in range(3):
        p.append(make_line(n))
    p.commit(0)
    assert [e["offset"] for e in p.read_uncommitted()] == [0, 1, 2]
    assert [e["offset"] for e in p.read_uncommitted(1)] == [1, 2]


def test_commit_index_never_moves_backwards():
    p = Partition("login")
    for n in range(3):
        p.append(make_line(n))
    p.commit(2)
    p.commit(0)
    assert p.commit_index == 2


def test_commit_past_end_is_rejected():
    p = Partition("login")
    p.append(make_line(0))
    with pytest.raises(ValueError):
        p.commit(1)


def test_entries_record_their_epoch():
    p = Partition("login")
    p.append(make_line(0))
    p.append(make_line(1), epoch=2)
    p.commit(1)
    assert [e["epoch"] for e in p.read()] == [0, 2]
    assert p.epoch == 2


def test_append_from_older_epoch_is_rejected():
    p = Partition("login")
    p.set_epoch(3)
    with pytest.raises(StaleEpochError):
        p.append(make_line(0), epoch=2)
    assert len(p) == 0


def test_epoch_never_moves_backwards():
    p = Partition("login")
    p.set_epoch(3)
    with pytest.raises(StaleEpochError):
        p.set_epoch(1)
    assert p.epoch == 3


def test_truncate_drops_uncommitted_lines():
    p = Partition("login")
    for n in range(5):
        p.append(make_line(n))
    p.commit(1)
    assert p.truncate_to_commit() == 3
    assert [e["offset"] for e in p.read_uncommitted()] == [0, 1]


def test_truncated_msg_id_can_be_stored_again():
    p = Partition("login")
    p.append(make_line(0))
    p.append(make_line(1))
    p.commit(0)
    p.truncate_to_commit()
    assert p.append(make_line(1)) == 1
    assert len(p) == 2


def test_truncate_with_nothing_committed_empties_the_log():
    p = Partition("login")
    p.append(make_line(0))
    assert p.truncate_to_commit() == 1
    assert len(p) == 0


def test_commit_index_is_second_highest_length_of_three():
    assert commit_index_from_lengths([5, 3, 1]) == 2
    assert commit_index_from_lengths([1, 5, 3]) == 2


def test_commit_index_when_two_replicas_are_level():
    assert commit_index_from_lengths([4, 4, 0]) == 3


def test_commit_index_with_only_leader_ahead_is_nothing():
    assert commit_index_from_lengths([3, 0, 0]) == -1


def test_commit_index_needs_a_replica():
    with pytest.raises(ValueError):
        commit_index_from_lengths([])
