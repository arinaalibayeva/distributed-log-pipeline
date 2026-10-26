import pytest

from broker.node import NotFollowerError, NotLeaderError, UnknownPartitionError
from broker.partition import StaleEpochError
from conftest import make_line, make_node


def entries_of(node, name):
    return node.partitions[name].read_uncommitted()


def test_each_broker_leads_one_partition():
    assert [make_node(1).is_leader(p) for p in ["login", "search", "upload"]] == [True, False, False]
    assert [make_node(2).is_leader(p) for p in ["login", "search", "upload"]] == [False, True, False]
    assert [make_node(3).is_leader(p) for p in ["login", "search", "upload"]] == [False, False, True]


def test_leader_tracks_only_the_partitions_it_leads():
    node = make_node(1)
    assert node.peers == [2, 3]
    assert node.follower_lengths == {"login": {2: 0, 3: 0}}


def test_append_is_not_committed_until_a_follower_has_it():
    node = make_node(1)
    assert node.append("login", make_line(0)) == 0
    assert node.read("login") == []
    assert node.record_follower("login", 2, 1) is True
    assert [e["offset"] for e in node.read("login")] == [0]


def test_one_follower_is_enough_for_a_majority():
    node = make_node(1)
    for n in range(3):
        node.append("login", make_line(n))
    node.record_follower("login", 3, 2)
    assert node.partitions["login"].commit_index == 1


def test_commit_follows_the_second_highest_copy():
    node = make_node(1)
    for n in range(5):
        node.append("login", make_line(n))
    node.record_follower("login", 2, 2)
    node.record_follower("login", 3, 4)
    assert node.partitions["login"].commit_index == 3


def test_follower_restart_lowers_its_length_but_not_the_commit():
    node = make_node(1)
    for n in range(3):
        node.append("login", make_line(n))
    node.record_follower("login", 2, 3)
    assert node.record_follower("login", 2, 0) is False
    assert node.partitions["login"].commit_index == 2
    assert node.message_for("login", 2, batch=10)["start"] == 0


def test_follower_longer_than_leader_does_not_commit_past_leader():
    node = make_node(1)
    node.append("login", make_line(0))
    node.record_follower("login", 2, 50)
    assert node.partitions["login"].commit_index == 0


def test_message_sends_what_the_follower_is_missing():
    node = make_node(1)
    for n in range(5):
        node.append("login", make_line(n))
    node.record_follower("login", 2, 2)
    message = node.message_for("login", 2, batch=2)
    assert (message["leader"], message["epoch"], message["start"]) == (1, 0, 2)
    assert (message["prev_msg_id"], message["prev_epoch"]) == ("login-1", 0)
    assert [e["offset"] for e in message["entries"]] == [2, 3]
    assert message["commit_index"] == 1
    assert node.is_behind("login", 2)


def test_message_from_the_start_has_no_previous_entry():
    node = make_node(1)
    node.append("login", make_line(0))
    assert node.message_for("login", 2, batch=10)["prev_msg_id"] is None


def test_message_for_up_to_date_follower_is_a_heartbeat():
    node = make_node(1)
    node.append("login", make_line(0))
    node.record_follower("login", 2, 1)
    message = node.message_for("login", 2, batch=10)
    assert (message["start"], message["prev_msg_id"], message["entries"]) == (1, "login-0", [])
    assert not node.is_behind("login", 2)


def test_accepted_reply_counts_only_what_was_sent():
    node = make_node(1)
    for n in range(3):
        node.append("login", make_line(n))
    message = node.message_for("login", 2, batch=2)
    assert node.record_reply("login", 2, message, accepted=True, length=50) is True
    assert node.follower_lengths["login"][2] == 2
    assert node.partitions["login"].commit_index == 1


def test_rejected_reply_resends_from_the_followers_length():
    node = make_node(1)
    for n in range(5):
        node.append("login", make_line(n))
    node.record_follower("login", 2, 4)
    message = node.message_for("login", 2, batch=10)
    node.record_reply("login", 2, message, accepted=False, length=1)
    assert node.message_for("login", 2, batch=10)["start"] == 1


def test_rejected_reply_on_mismatch_steps_back_one_entry():
    node = make_node(1)
    for n in range(5):
        node.append("login", make_line(n))
    node.record_follower("login", 2, 4)
    message = node.message_for("login", 2, batch=10)
    node.record_reply("login", 2, message, accepted=False, length=9)
    assert node.message_for("login", 2, batch=10)["start"] == 3


def test_follower_stores_entries_and_follows_commit():
    leader, follower = make_node(1), make_node(2)
    for n in range(3):
        leader.append("login", make_line(n))
    leader.record_follower("login", 3, 3)
    m = leader.message_for("login", 2, batch=10)
    accepted, length = follower.replicate(
        "login", m["leader"], m["epoch"], m["start"], m["entries"], m["commit_index"],
        m["prev_msg_id"], m["prev_epoch"], now=5.0,
    )
    assert (accepted, length) == (True, 3)
    assert entries_of(follower, "login") == entries_of(leader, "login")
    assert follower.partitions["login"].commit_index == 2
    assert follower.last_heard["login"] == 5.0


def test_follower_reports_its_length_on_a_gap():
    leader, follower = make_node(1), make_node(2)
    for n in range(3):
        leader.append("login", make_line(n))
    entries = leader.partitions["login"].read_uncommitted(2)
    assert follower.replicate("login", 1, 0, 2, entries, -1) == (False, 0)


def test_follower_rejects_entries_from_a_non_leader():
    with pytest.raises(NotFollowerError):
        make_node(2).replicate("login", 3, 0, 0, [], -1)


def test_leader_rejects_entries_for_its_own_partition():
    with pytest.raises(NotFollowerError):
        make_node(1).replicate("login", 1, 0, 0, [], -1)


def test_follower_rejects_older_epoch():
    follower = make_node(2)
    follower.partitions["login"].set_epoch(3)
    with pytest.raises(StaleEpochError):
        follower.replicate("login", 1, 2, 0, [], -1)


def test_retried_line_is_stored_once():
    node = make_node(1)
    node.append("login", make_line(0))
    assert node.append("login", make_line(0)) == 0
    assert len(node.partitions["login"]) == 1


def test_follower_rejects_append_and_names_leader():
    node = make_node(1)
    with pytest.raises(NotLeaderError) as info:
        node.append("search", make_line(0, service="search"))
    assert info.value.leader == 2
    assert len(node.partitions["search"]) == 0


def test_follower_rejects_read():
    with pytest.raises(NotLeaderError):
        make_node(1).read("upload")


def test_unknown_partition_is_rejected():
    with pytest.raises(UnknownPartitionError):
        make_node(1).append("billing", make_line(0, service="billing"))
    with pytest.raises(UnknownPartitionError):
        make_node(2).replicate("billing", 1, 0, 0, [], -1)


def test_read_respects_offset_and_limit():
    node = make_node(1)
    for n in range(10):
        node.append("login", make_line(n))
    node.record_follower("login", 2, 10)
    assert [e["offset"] for e in node.read("login", offset=3, limit=4)] == [3, 4, 5, 6]


def test_state_reports_roles_progress_and_last_entries():
    node = make_node(1)
    for n in range(7):
        node.append("login", make_line(n))
    node.record_follower("login", 2, 7)
    node.record_follower("login", 3, 4)
    state = node.state(tail=3)
    assert state["broker_id"] == 1
    login = state["partitions"]["login"]
    assert (login["role"], login["leader"], login["length"], login["commit_index"]) == ("leader", 1, 7, 6)
    assert login["follower_lengths"] == {2: 7, 3: 4}
    assert [e["offset"] for e in login["last_entries"]] == [4, 5, 6]
    search = state["partitions"]["search"]
    assert (search["role"], search["follower_lengths"], search["last_entries"]) == ("follower", None, [])


def test_follower_with_different_history_never_commits_leaders_lines():
    # A leader restarted empty must not convince a follower holding other
    # committed lines that its new line 0 is already there.
    follower = make_node(2)
    follower.replicate("login", 1, 0, 0, [{"offset": 0, "epoch": 0, "line": make_line(7)}], 0)
    restarted_leader = make_node(1)
    restarted_leader.append("login", make_line(0))
    heartbeat = {"epoch": 0, "start": 0, "entries": []}
    accepted, length = follower.replicate("login", 1, 0, 0, [], -1)
    restarted_leader.record_reply("login", 2, heartbeat, accepted, length)
    assert restarted_leader.partitions["login"].commit_index == -1
    assert restarted_leader.message_for("login", 2, batch=10)["entries"][0]["line"]["msg_id"] == "login-0"


def test_follower_with_a_different_tail_asks_to_resend_after_its_commit():
    follower = make_node(2)
    entries = [{"offset": n, "epoch": 0, "line": make_line(n)} for n in range(4)]
    follower.replicate("login", 1, 0, 0, entries, 0)
    assert follower.replicate("login", 1, 1, 4, [], 0, "login-3", 1) == (False, 1)


def test_fingerprint_matches_for_copies_with_the_same_committed_lines():
    leader, follower = make_node(1), make_node(2)
    for n in range(4):
        leader.append("login", make_line(n))
    m = leader.message_for("login", 2, batch=10)
    follower.replicate("login", 1, 0, 0, m["entries"], 2)
    leader.record_follower("login", 2, 4)
    assert leader.fingerprint("login", 2) == follower.fingerprint("login", 2)
    assert leader.fingerprint("login", 2)["upto"] == 2
    assert leader.fingerprint("login", 99)["upto"] == 3  # capped at the commit index
    assert leader.fingerprint("login", 1)["sha"] != leader.fingerprint("login", 2)["sha"]
    assert leader.fingerprint("login", 2)["unique"] is True


def test_fingerprint_of_an_empty_copy():
    assert make_node(2).fingerprint("login", 5) == make_node(3).fingerprint("login", -1)
