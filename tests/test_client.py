import httpx
import pytest

from broker.client import NoLeaderAvailable
from conftest import make_line, send


def test_request_goes_straight_to_leader(cluster):
    transport = cluster.transport()
    assert send(cluster.client(transport), make_line(0, service="search")) == {"offset": 0}
    assert transport.requests == [(2, "POST", "/partitions/search/append")]


def test_wrong_guess_follows_redirect(cluster):
    transport = cluster.transport()
    client = cluster.client(transport)
    client.leader["upload"] = 1
    assert send(client, make_line(0, service="upload")) == {"offset": 0}
    assert [b for b, _, _ in transport.requests] == [1, 3]
    assert client.leader["upload"] == 3


def test_unreachable_broker_is_skipped(cluster):
    client = cluster.client()
    client.leader["login"] = 2
    cluster.stop(2)
    assert send(client, make_line(0)) == {"offset": 0}
    assert client.leader["login"] == 1


def test_client_finds_the_new_leader_after_a_takeover(cluster):
    client = cluster.client()
    cluster.stop(1)
    cluster.wait_for_leader("login", among=(2, 3))
    assert send(client, make_line(0)) == {"offset": 0}
    assert client.leader["login"] == 2


def test_broker_without_a_leader_sends_the_client_onwards(cluster):
    transport = cluster.transport()
    client = cluster.client(transport)
    cluster.node(1).leaders["search"] = None  # as if broker 1 hadn't heard who leads search
    client.leader["search"] = 1
    assert send(client, make_line(0, service="search")) == {"offset": 0}
    assert [b for b, _, _ in transport.requests] == [1, 2]


def test_leader_that_cannot_commit_is_not_retried_elsewhere(cluster):
    transport = cluster.transport()
    cluster.stop(2)
    cluster.stop(3)
    with pytest.raises(NoLeaderAvailable):
        send(cluster.client(transport), make_line(0))
    assert transport.requests == [(1, "POST", "/partitions/login/append")]


def test_client_errors_are_raised_not_retried(cluster):
    transport = cluster.transport()
    with pytest.raises(httpx.HTTPStatusError):
        send(cluster.client(transport), dict(make_line(0), level="DEBUG"))
    assert len(transport.requests) == 1
