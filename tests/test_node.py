import pytest

from broker.node import BrokerNode, NotLeaderError, UnknownPartitionError
from conftest import make_line


def test_each_broker_leads_one_partition():
    assert [BrokerNode(1).is_leader(p) for p in ["login", "search", "upload"]] == [True, False, False]
    assert [BrokerNode(2).is_leader(p) for p in ["login", "search", "upload"]] == [False, True, False]
    assert [BrokerNode(3).is_leader(p) for p in ["login", "search", "upload"]] == [False, False, True]


def test_leader_append_commits_right_away():
    node = BrokerNode(1)
    assert node.append("login", make_line(0)) == 0
    assert node.append("login", make_line(1)) == 1
    assert [e["offset"] for e in node.read("login")] == [0, 1]


def test_retried_line_is_stored_once():
    node = BrokerNode(1)
    node.append("login", make_line(0))
    assert node.append("login", make_line(0)) == 0
    assert len(node.read("login")) == 1


def test_follower_rejects_append_and_names_leader():
    node = BrokerNode(1)
    with pytest.raises(NotLeaderError) as info:
        node.append("search", make_line(0, service="search"))
    assert info.value.leader == 2
    assert len(node.partitions["search"]) == 0


def test_follower_rejects_read():
    with pytest.raises(NotLeaderError):
        BrokerNode(1).read("upload")


def test_unknown_partition_is_rejected():
    with pytest.raises(UnknownPartitionError):
        BrokerNode(1).append("billing", make_line(0, service="billing"))


def test_read_respects_offset_and_limit():
    node = BrokerNode(1)
    for n in range(10):
        node.append("login", make_line(n))
    assert [e["offset"] for e in node.read("login", offset=3, limit=4)] == [3, 4, 5, 6]


def test_state_reports_roles_and_last_entries():
    node = BrokerNode(1)
    for n in range(7):
        node.append("login", make_line(n))
    state = node.state(tail=3)
    assert state["broker_id"] == 1
    login = state["partitions"]["login"]
    assert (login["role"], login["leader"], login["length"], login["commit_index"]) == ("leader", 1, 7, 6)
    assert [e["offset"] for e in login["last_entries"]] == [4, 5, 6]
    assert state["partitions"]["search"]["role"] == "follower"
    assert state["partitions"]["search"]["last_entries"] == []
