"""Endpoint tests for a single broker whose peers can't be reached.

With no follower answering, nothing can be committed, which is what these
tests rely on. Replication between live brokers is in test_replication.py.
"""

import time

import pytest
from fastapi.testclient import TestClient

from broker.server import create_app
from conftest import free_port, make_line


def unreachable_brokers():
    return {b: f"http://127.0.0.1:{free_port()}" for b in (1, 2, 3)}


def app(broker_id):
    # A long leader_timeout keeps a leader from stepping down mid-test, though
    # no follower ever answers.
    return create_app(broker_id, brokers=unreachable_brokers(), interval=0.05, ack_timeout=0.1, leader_timeout=60)


@pytest.fixture
def broker1():
    """Broker 1, leading login in epoch 1 (no other broker can be reached to take over)."""
    with TestClient(app(1)) as client:
        node = client.app.state.node
        node.start_campaign("login", 1)
        node.become_leader("login", 1, -1, [])
        yield client


@pytest.fixture
def broker2():
    with TestClient(app(2)) as client:
        yield client


def replicate(client, start, entries, leader=1, epoch=0, commit_index=-1):
    return client.post("/partitions/login/replicate", json={
        "leader": leader, "epoch": epoch, "start": start, "entries": entries, "commit_index": commit_index,
    })


def entry(n):
    return {"offset": n, "epoch": 0, "line": make_line(n)}


def test_append_without_a_majority_is_not_acked(broker1):
    started = time.monotonic()
    reply = broker1.post("/partitions/login/append", json=make_line(0))
    assert reply.status_code == 503
    assert reply.json() == {"error": "not_committed", "partition": "login", "offset": 0}
    assert time.monotonic() - started < 2


def test_unacked_line_is_stored_once_and_not_readable(broker1):
    broker1.post("/partitions/login/append", json=make_line(0))
    assert broker1.post("/partitions/login/append", json=make_line(0)).json()["offset"] == 0
    assert broker1.post("/partitions/login/append", json=make_line(1)).json()["offset"] == 1
    reply = broker1.get("/partitions/login/read").json()
    assert reply == {"entries": [], "commit_index": -1}
    assert broker1.get("/state").json()["partitions"]["login"]["length"] == 2


def test_follower_accepts_replicated_entries(broker2):
    reply = replicate(broker2, 0, [entry(0), entry(1)], commit_index=0)
    assert reply.json() == {"accepted": True, "length": 2}
    login = broker2.get("/state").json()["partitions"]["login"]
    assert (login["role"], login["length"], login["commit_index"]) == ("follower", 2, 0)


def test_follower_reports_length_on_gap(broker2):
    replicate(broker2, 0, [entry(0)])
    assert replicate(broker2, 3, [entry(3)]).json() == {"accepted": False, "length": 1}


def test_follower_rejects_wrong_leader_and_stale_epoch(broker2):
    replicate(broker2, 0, [], leader=1)
    assert replicate(broker2, 0, [], leader=3).status_code == 409
    replicate(broker2, 0, [], epoch=2)
    reply = replicate(broker2, 0, [], epoch=1)
    assert reply.status_code == 409
    assert (reply.json()["error"], reply.json()["epoch"]) == ("stale_epoch", 2)


def test_follower_learns_the_leader_from_its_messages(broker2):
    assert broker2.get("/state").json()["partitions"]["login"]["leader"] is None
    replicate(broker2, 0, [], leader=3, epoch=1)
    assert broker2.get("/state").json()["partitions"]["login"]["leader"] == 3


def test_leader_rejects_replication_for_its_partition(broker1):
    reply = replicate(broker1, 0, [entry(0)], epoch=1)
    assert reply.status_code == 409
    assert reply.json()["error"] == "not_follower"


def test_replicate_with_bad_offsets_is_400(broker2):
    assert replicate(broker2, 0, [entry(1)]).status_code == 400


def test_replicate_conflicting_with_committed_entry_is_409(broker2):
    replicate(broker2, 0, [entry(0)], commit_index=0)
    other = {"offset": 0, "epoch": 0, "line": make_line(99)}
    reply = replicate(broker2, 0, [other])
    assert reply.status_code == 409
    assert reply.json()["error"] == "conflict"


def test_malformed_replicate_is_422(broker2):
    assert broker2.post("/partitions/login/replicate", json={"leader": 1}).status_code == 422
    assert replicate(broker2, -1, []).status_code == 422


def test_follower_answers_421_with_leader(broker2):
    replicate(broker2, 0, [], leader=1)
    reply = broker2.post("/partitions/login/append", json=make_line(0))
    assert reply.status_code == 421
    assert reply.json() == {"error": "not_leader", "partition": "login", "leader": 1}
    assert broker2.get("/partitions/login/read").status_code == 421


def test_broker_that_knows_no_leader_answers_421_with_null(broker1):
    reply = broker1.post("/partitions/search/append", json=make_line(0, service="search"))
    assert reply.status_code == 421
    assert reply.json() == {"error": "not_leader", "partition": "search", "leader": None}


def test_unknown_partition_is_404(broker1):
    assert broker1.post("/partitions/billing/append", json=make_line(0, service="billing")).status_code == 404
    assert broker1.get("/partitions/billing/read").status_code == 404


def test_line_for_another_partition_is_400(broker1):
    assert broker1.post("/partitions/login/append", json=make_line(0, service="search")).status_code == 400


def test_malformed_lines_are_422(broker1):
    bad_level = dict(make_line(0), level="DEBUG")
    missing_id = {k: v for k, v in make_line(0).items() if k != "msg_id"}
    assert broker1.post("/partitions/login/append", json=bad_level).status_code == 422
    assert broker1.post("/partitions/login/append", json=missing_id).status_code == 422


def test_negative_offset_is_422(broker1):
    assert broker1.get("/partitions/login/read", params={"offset": -1}).status_code == 422


def test_state_endpoint(broker1):
    state = broker1.get("/state").json()
    assert state["broker_id"] == 1
    assert set(state["partitions"]) == {"login", "search", "upload"}
    assert state["partitions"]["login"]["follower_lengths"] == {"2": 0, "3": 0}


def takeover(client, epoch=1, candidate=3, dry_run=False, name="login"):
    return client.post(f"/partitions/{name}/takeover", json={
        "candidate": candidate, "epoch": epoch, "commit_index": -1, "dry_run": dry_run,
    })


def test_takeover_is_refused_by_the_leader(broker1):
    assert takeover(broker1, epoch=2).json() == {"granted": False, "epoch": 1, "reason": "leader"}


def test_takeover_is_refused_right_after_startup(broker2):
    assert takeover(broker2).json()["reason"] == "just_started"


def test_takeover_is_granted_once_per_epoch(broker2):
    broker2.app.state.node.started -= 60  # as if broker 2 had been up for a minute
    assert takeover(broker2, dry_run=True).json() == {"granted": True, "epoch": 0, "reason": None}
    reply = takeover(broker2).json()
    assert (reply["granted"], reply["epoch"], reply["length"], reply["entries"]) == (True, 1, 0, [])
    assert takeover(broker2, candidate=1).json()["granted"] is False


def test_bad_takeover_requests(broker2):
    assert takeover(broker2, candidate=2).status_code == 400  # itself
    assert takeover(broker2, epoch=0).status_code == 422
    assert takeover(broker2, name="billing").status_code == 404
