import httpx
import pytest

from broker.client import NoLeaderAvailable
from conftest import make_line


def append(client, line):
    return client.call(line["service"], "POST", f"/partitions/{line['service']}/append", json=line)


def test_request_goes_straight_to_leader(cluster):
    client = cluster.client()
    assert append(client, make_line(0, service="search")) == {"offset": 0}
    assert cluster.requests == [(2, "POST", "/partitions/search/append")]


def test_wrong_guess_follows_redirect(cluster):
    client = cluster.client()
    client.leader["upload"] = 1
    assert append(client, make_line(0, service="upload")) == {"offset": 0}
    assert [b for b, _, _ in cluster.requests] == [1, 3]
    assert client.leader["upload"] == 3


def test_unreachable_broker_is_skipped(cluster):
    client = cluster.client()
    client.leader["login"] = 2
    cluster.down.add(2)
    assert append(client, make_line(0)) == {"offset": 0}
    assert client.leader["login"] == 1


def test_no_leader_when_leader_is_down(cluster):
    # Until leader takeover exists (M6), a partition whose leader is down has
    # no leader: the other brokers keep pointing back at the dead one.
    client = cluster.client()
    cluster.down.add(1)
    with pytest.raises(NoLeaderAvailable):
        append(client, make_line(0))
    assert len(cluster.requests) <= 2 * 3


def test_client_errors_are_raised_not_retried(cluster):
    client = cluster.client()
    with pytest.raises(httpx.HTTPStatusError):
        append(client, dict(make_line(0), level="DEBUG"))
    assert len(cluster.requests) == 1
