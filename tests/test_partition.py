import pytest

from broker.partition import ConflictError, GapError, Partition, StaleEpochError, commit_index_from_lengths


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


def make_entry(n, offset=None, epoch=0, msg=None):
    line = make_line(n)
    if msg is not None:
        line["msg_id"] = msg
    return {"offset": n if offset is None else offset, "epoch": epoch, "line": line}


def test_replicate_stores_entries_at_leader_offsets():
    p = Partition("login")
    assert p.replicate(0, [make_entry(0), make_entry(1)], epoch=0) == 2
    assert p.replicate(2, [make_entry(2)], epoch=0, prev_msg_id="id-1") == 3
    assert [e["offset"] for e in p.read_uncommitted()] == [0, 1, 2]
    assert p.commit_index == -1


def test_replicate_keeps_the_leaders_epoch_on_each_entry():
    p = Partition("login")
    p.replicate(0, [make_entry(0, epoch=1), make_entry(1, epoch=2)], epoch=2)
    assert [e["epoch"] for e in p.read_uncommitted()] == [1, 2]
    assert p.epoch == 2


def test_replicate_with_a_gap_is_rejected():
    p = Partition("login")
    p.replicate(0, [make_entry(0)], epoch=0)
    with pytest.raises(GapError):
        p.replicate(2, [make_entry(2)], epoch=0)
    assert len(p) == 1


def test_replicate_skips_entries_it_already_has():
    p = Partition("login")
    p.replicate(0, [make_entry(0), make_entry(1)], epoch=0)
    assert p.replicate(0, [make_entry(0), make_entry(1), make_entry(2)], epoch=0) == 3
    assert [e["line"]["msg_id"] for e in p.read_uncommitted()] == ["id-0", "id-1", "id-2"]


def test_empty_replicate_is_a_heartbeat():
    p = Partition("login")
    p.replicate(0, [make_entry(0)], epoch=0)
    assert p.replicate(1, [], epoch=0, prev_msg_id="id-0") == 1
    assert p.replicate(0, [], epoch=0) == 1


def test_replicate_replaces_conflicting_uncommitted_entries():
    p = Partition("login")
    p.replicate(0, [make_entry(0), make_entry(1, msg="old-1"), make_entry(2, msg="old-2")], epoch=0)
    p.commit(0)
    assert p.replicate(1, [make_entry(1, msg="new-1")], epoch=1, prev_msg_id="id-0") == 2
    assert [e["line"]["msg_id"] for e in p.read_uncommitted()] == ["id-0", "new-1"]
    assert "old-2" not in p.msg_ids
    assert p.append(make_line(0) | {"msg_id": "old-2"}, epoch=1) == 2


def test_replicate_never_changes_committed_entries():
    p = Partition("login")
    p.replicate(0, [make_entry(0), make_entry(1)], epoch=0)
    p.commit(1)
    with pytest.raises(ConflictError):
        p.replicate(1, [make_entry(1, msg="other"), make_entry(2)], epoch=0, prev_msg_id="id-0")
    assert [e["line"]["msg_id"] for e in p.read_uncommitted()] == ["id-0", "id-1"]


def test_replicate_from_older_epoch_is_rejected():
    p = Partition("login")
    p.set_epoch(2)
    with pytest.raises(StaleEpochError):
        p.replicate(0, [make_entry(0)], epoch=1)
    assert len(p) == 0


def test_replicate_needs_consecutive_offsets():
    p = Partition("login")
    with pytest.raises(ValueError):
        p.replicate(0, [make_entry(0), make_entry(2)], epoch=0)
    with pytest.raises(ValueError):
        p.replicate(1, [make_entry(0)], epoch=0)
    assert len(p) == 0


def test_replicated_msg_ids_reject_duplicate_appends():
    p = Partition("login")
    p.replicate(0, [make_entry(0)], epoch=0)
    assert p.append(make_line(0)) == 0
    assert len(p) == 1


def test_follow_commit_is_capped_at_the_end_of_the_log():
    p = Partition("login")
    p.replicate(0, [make_entry(0), make_entry(1)], epoch=0)
    p.follow_commit(10)
    assert p.commit_index == 1


def test_follow_commit_never_moves_backwards():
    p = Partition("login")
    p.replicate(0, [make_entry(0), make_entry(1)], epoch=0)
    p.follow_commit(1)
    p.follow_commit(0)
    assert p.commit_index == 1


def test_follow_commit_on_empty_log_does_nothing():
    p = Partition("login")
    p.follow_commit(5)
    assert p.commit_index == -1


def test_replicate_checks_the_entry_before_start():
    p = Partition("login")
    p.replicate(0, [make_entry(0), make_entry(1)], epoch=0)
    with pytest.raises(GapError):
        p.replicate(2, [make_entry(2)], epoch=0, prev_msg_id="someone-else")
    with pytest.raises(GapError):
        p.replicate(2, [make_entry(2)], epoch=0)
    assert len(p) == 2


def test_heartbeat_with_mismatched_previous_entry_is_rejected():
    p = Partition("login")
    p.replicate(0, [make_entry(0)], epoch=0)
    with pytest.raises(GapError):
        p.replicate(1, [], epoch=0, prev_msg_id="other-0")


def test_follow_commit_stops_at_the_verified_end():
    p = Partition("login")
    p.replicate(0, [make_entry(0), make_entry(1), make_entry(2)], epoch=0)
    p.follow_commit(10, verified_end=0)
    assert p.commit_index == 0
