"""Tests for the leader takeover rules, with brokers talking in memory."""

import pytest

from broker.node import BrokerNode, NotLeaderError
from broker.partition import StaleEpochError
from conftest import make_line

TIMEOUT = 1.5  # BrokerNode's default leader_timeout
LATER = 10.0  # a time when every broker's takeover timer has run out


def cluster(now=0.0):
    return {b: BrokerNode(b, now=now) for b in (1, 2, 3)}


def take_over(candidate, voters, name="login", now=LATER, attempts=2):
    """Run takeover attempts; return True if `candidate` ends up leading.

    A refusal that mentions a newer epoch makes the next attempt ask for a
    higher one, as the real takeover loop does.
    """
    for _ in range(attempts):
        epoch = candidate.next_epoch(name)
        if not candidate.start_campaign(name, epoch, now):
            return False
        commit = candidate.partitions[name].commit_index
        replies = [v.grant(name, candidate.id, epoch, commit, now=now) for v in voters]
        for reply in replies:
            candidate.note_epoch(name, reply["epoch"])
        granted = [r for r in replies if r["granted"]]
        if len(granted) + 1 >= candidate.majority:
            return candidate.become_leader(name, epoch, commit, granted, now)
    return False


def deliver(leader, follower, name="login", now=LATER):
    """Send one replication message and record the reply."""
    m = leader.message_for(name, follower.id, batch=100)
    accepted, length = follower.replicate(
        name, m["leader"], m["epoch"], m["start"], m["entries"], m["commit_index"],
        m["prev_msg_id"], m["prev_epoch"], now=now,
    )
    leader.record_reply(name, follower.id, m, accepted, length, now=now)


def sync(leader, follower, name="login", now=LATER):
    """Replicate until `follower` has everything and knows the commit index."""
    for _ in range(10):
        deliver(leader, follower, name, now)
    assert not leader.is_behind(name, follower.id)


def msg_ids(node, name="login"):
    return [e["line"]["msg_id"] for e in node.partitions[name].entries]


def test_next_broker_in_line_times_out_first():
    nodes = cluster()
    # login's preference is 1, 2, 3, so broker 2 waits 1.5 s plus one 0.5 s
    # stagger, and broker 3 waits 1.5 s plus two.
    assert not nodes[2].should_take_over("login", now=1.9)
    assert nodes[2].should_take_over("login", now=2.0)
    assert not nodes[3].should_take_over("login", now=2.4)
    assert nodes[3].should_take_over("login", now=2.5)
    assert not nodes[1].should_take_over("login", now=LATER)  # it leads


def test_hearing_from_the_leader_restarts_the_timer():
    nodes = cluster()
    nodes[1].append("login", make_line(0))
    deliver(nodes[1], nodes[2], now=1.0)
    assert not nodes[2].should_take_over("login", now=2.9)
    assert nodes[2].should_take_over("login", now=3.0)


def test_broker_refuses_while_it_hears_from_the_leader():
    nodes = cluster()
    deliver(nodes[1], nodes[3], now=5.0)
    reply = nodes[3].grant("login", 2, 1, -1, now=5.0 + TIMEOUT - 0.1)
    assert (reply["granted"], reply["reason"]) == (False, "leader_alive")
    assert nodes[3].leaders["login"] == 1


def test_a_freshly_started_broker_waits_before_agreeing():
    node = BrokerNode(3, now=100.0)
    assert node.grant("login", 2, 1, -1, now=100.5)["reason"] == "leader_alive"
    assert node.grant("login", 2, 1, -1, now=100.0 + TIMEOUT)["granted"] is True


def test_leader_refuses_to_be_replaced():
    nodes = cluster()
    reply = nodes[1].grant("login", 2, 1, -1, now=LATER)
    assert (reply["granted"], reply["reason"]) == (False, "leader")


def test_broker_agrees_to_one_takeover_per_epoch():
    nodes = cluster()
    assert nodes[3].grant("login", 2, 1, -1, now=LATER)["granted"] is True
    reply = nodes[3].grant("login", 1, 1, -1, now=LATER + TIMEOUT)
    assert (reply["granted"], reply["reason"], reply["epoch"]) == (False, "epoch_taken", 1)


def test_dry_run_changes_nothing():
    nodes = cluster()
    reply = nodes[3].grant("login", 2, 1, -1, dry_run=True, now=LATER)
    assert reply == {"granted": True, "epoch": 0, "reason": None}
    assert (nodes[3].partitions["login"].epoch, nodes[3].leaders["login"]) == (0, 1)


def test_agreeing_moves_to_the_new_epoch_and_sends_the_log_tail():
    nodes = cluster()
    for n in range(4):
        nodes[1].append("login", make_line(n))
    sync(nodes[1], nodes[3], now=1.0)
    reply = nodes[3].grant("login", 2, 1, 1, now=LATER)
    assert (reply["commit_index"], reply["length"], reply["last_epoch"]) == (3, 4, 0)
    assert [e["offset"] for e in reply["entries"]] == [2, 3]
    assert (nodes[3].partitions["login"].epoch, nodes[3].leaders["login"]) == (1, None)


def test_takeover_needs_another_broker():
    nodes = cluster()
    assert take_over(nodes[2], []) is False
    assert nodes[2].leaders["login"] is None


def test_campaign_gives_up_if_it_agreed_to_another_broker_meanwhile():
    nodes = cluster()
    nodes[2].grant("login", 3, 1, -1, now=LATER)
    assert nodes[2].start_campaign("login", 1, now=LATER) is False


def test_new_leader_keeps_every_committed_line():
    nodes = cluster()
    for n in range(5):
        nodes[1].append("login", make_line(n))
    sync(nodes[1], nodes[3], now=1.0)  # lines 0-4 committed on brokers 1 and 3
    # Broker 1 dies. Broker 2 never got anything, but takes over with broker 3.
    assert take_over(nodes[2], [nodes[3]]) is True
    assert nodes[2].is_leader("login")
    assert msg_ids(nodes[2]) == [f"login-{n}" for n in range(5)]
    assert nodes[2].partitions["login"].commit_index == 4
    assert nodes[2].partitions["login"].epoch == 1


def test_new_leader_marks_inherited_uncommitted_lines_with_its_epoch():
    nodes = cluster()
    for n in range(3):
        nodes[1].append("login", make_line(n))
    deliver(nodes[1], nodes[3], now=1.0)  # broker 3 has all 3, but nobody learned they're committed
    nodes[3].partitions["login"].commit_index = -1
    assert take_over(nodes[2], [nodes[3]]) is True
    assert [e["epoch"] for e in nodes[2].partitions["login"].entries] == [1, 1, 1]
    sync(nodes[2], nodes[3])
    assert nodes[2].partitions["login"].commit_index == 2
    assert nodes[3].partitions["login"].commit_index == 2


def test_new_leader_keeps_its_own_log_when_it_is_more_up_to_date():
    nodes = cluster()
    for n in range(4):
        nodes[1].append("login", make_line(n))
    sync(nodes[1], nodes[2], now=1.0)
    assert take_over(nodes[2], [nodes[3]]) is True  # broker 3 is empty
    assert msg_ids(nodes[2]) == [f"login-{n}" for n in range(4)]


def test_newer_epoch_beats_a_longer_log():
    # Broker 3 holds two lines from epoch 0 that were never committed. Broker
    # 1 holds a line that a later leader (epoch 2) re-marked and committed.
    # The newer history must win even though it is shorter.
    a, b, c = BrokerNode(1, now=0.0), BrokerNode(2, now=0.0), BrokerNode(3, now=0.0)
    c.replicate("login", 1, 0, 0, [
        {"offset": 0, "epoch": 0, "line": make_line(0)},
        {"offset": 1, "epoch": 0, "line": make_line(1)},
    ], -1, now=0.0)
    a.partitions["login"].entries.clear()
    a.partitions["login"].msg_ids.clear()
    a.step_down("login", now=0.0, epoch=2)
    a.replicate("login", 2, 2, 0, [{"offset": 0, "epoch": 2, "line": make_line(9)}], 0, now=0.0)
    assert take_over(c, [a]) is True  # the first attempt learns that epoch 2 exists
    assert c.partitions["login"].epoch == 3
    assert msg_ids(c) == ["login-9"]
    assert c.partitions["login"].commit_index == 0


def test_old_leader_learns_of_the_new_one_and_drops_unacked_lines():
    nodes = cluster()
    for n in range(3):
        nodes[1].append("login", make_line(n))
    sync(nodes[1], nodes[3], now=1.0)
    nodes[1].append("login", make_line(3))  # stored on broker 1 only, never acked
    assert take_over(nodes[2], [nodes[3]]) is True
    nodes[2].append("login", make_line(4))
    sync(nodes[2], nodes[3])
    sync(nodes[2], nodes[1])
    assert not nodes[1].is_leader("login")
    assert nodes[1].leaders["login"] == 2
    assert msg_ids(nodes[1]) == msg_ids(nodes[2]) == ["login-0", "login-1", "login-2", "login-4"]
    with pytest.raises(NotLeaderError):
        nodes[1].append("login", make_line(5))


def test_old_leader_cannot_commit_once_a_follower_agreed_to_a_takeover():
    nodes = cluster()
    nodes[1].append("login", make_line(0))
    nodes[3].grant("login", 2, 1, -1, now=LATER)
    with pytest.raises(StaleEpochError) as info:
        deliver(nodes[1], nodes[3])
    assert info.value.current == 1
    assert nodes[1].partitions["login"].commit_index == -1


def test_step_down_drops_uncommitted_lines_and_learns_the_epoch():
    nodes = cluster()
    for n in range(3):
        nodes[1].append("login", make_line(n))
    nodes[1].record_follower("login", 2, 1)
    nodes[1].step_down("login", now=LATER, epoch=4)
    assert not nodes[1].is_leader("login")
    assert msg_ids(nodes[1]) == ["login-0"]
    assert nodes[1].partitions["login"].epoch == 4
    assert nodes[1].next_epoch("login") == 5
    assert "login" not in nodes[1].follower_lengths


def test_leader_notices_it_lost_the_majority():
    nodes = cluster()
    assert not nodes[1].lost_majority("login", now=1.0)
    assert nodes[1].lost_majority("login", now=TIMEOUT)
    deliver(nodes[1], nodes[2], now=5.0)
    assert not nodes[1].lost_majority("login", now=5.0 + TIMEOUT - 0.1)


def test_reply_to_a_message_from_an_earlier_epoch_is_ignored():
    nodes = cluster()
    nodes[1].append("login", make_line(0))
    old = nodes[1].message_for("login", 2, batch=10)
    nodes[1].step_down("login", now=1.0)
    nodes[1].start_campaign("login", 1, now=LATER)
    nodes[1].become_leader("login", 1, -1, [], now=LATER)
    assert nodes[1].record_reply("login", 2, old, True, 1) is False
    assert nodes[1].follower_lengths["login"][2] == 0


def test_lines_from_an_older_epoch_wait_for_one_of_the_current_epoch():
    node = BrokerNode(1, now=0.0)
    node.append("login", make_line(0))  # epoch 0
    node.partitions["login"].set_epoch(1)  # a new epoch, without re-marking
    node.record_follower("login", 2, 1)
    assert node.partitions["login"].commit_index == -1
    node.append("login", make_line(1))  # epoch 1
    node.record_follower("login", 2, 2)
    assert node.partitions["login"].commit_index == 1
