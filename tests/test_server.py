import pytest
from fastapi.testclient import TestClient

from broker.server import create_app
from conftest import make_line


@pytest.fixture
def broker1():
    return TestClient(create_app(1))


def test_append_then_read(broker1):
    assert broker1.post("/partitions/login/append", json=make_line(0)).json() == {"offset": 0}
    assert broker1.post("/partitions/login/append", json=make_line(1)).json() == {"offset": 1}
    reply = broker1.get("/partitions/login/read", params={"offset": 1}).json()
    assert reply["commit_index"] == 1
    assert [(e["offset"], e["line"]["msg_id"]) for e in reply["entries"]] == [(1, "login-1")]


def test_retried_append_returns_same_offset(broker1):
    broker1.post("/partitions/login/append", json=make_line(0))
    assert broker1.post("/partitions/login/append", json=make_line(0)).json() == {"offset": 0}


def test_follower_answers_421_with_leader(broker1):
    reply = broker1.post("/partitions/search/append", json=make_line(0, service="search"))
    assert reply.status_code == 421
    assert reply.json() == {"error": "not_leader", "partition": "search", "leader": 2}
    assert broker1.get("/partitions/search/read").status_code == 421


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
    broker1.post("/partitions/login/append", json=make_line(0))
    state = broker1.get("/state").json()
    assert state["broker_id"] == 1
    assert state["partitions"]["login"]["length"] == 1
    assert set(state["partitions"]) == {"login", "search", "upload"}
