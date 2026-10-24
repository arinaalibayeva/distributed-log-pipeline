"""Tests for the chaos controls: losing messages between brokers."""

import asyncio
import random

import httpx
import pytest
from fastapi.testclient import TestClient

from broker.chaos import Chaos, ChaosTransport
from broker.server import create_app
from conftest import check_invariants, free_port, make_line, wait_for_copies, wait_until
from producer.producer import Producer

BROKERS = {1: "http://127.0.0.1:9001", 2: "http://127.0.0.1:9002", 3: "http://127.0.0.1:9003"}


def test_nothing_is_lost_by_default():
    chaos = Chaos()
    assert chaos.settings() == {"blocked": [], "drop_rate": 0.0}
    assert {chaos.fate(2) for _ in range(100)} == {"send"}


def test_blocked_broker_gets_nothing():
    chaos = Chaos()
    chaos.update(blocked=[2])
    assert chaos.fate(2) == "drop_request"
    assert chaos.fate(3) == "send"


def test_drop_rate_loses_that_share_of_messages_both_ways():
    chaos = Chaos(rng=random.Random(7))
    chaos.update(drop_rate=0.3)
    fates = [chaos.fate(2) for _ in range(2000)]
    lost = 2000 - fates.count("send")
    assert 0.25 < lost / 2000 < 0.35
    assert fates.count("drop_request") > 0 and fates.count("drop_reply") > 0


def test_update_changes_only_what_is_given_and_heal_clears_everything():
    chaos = Chaos()
    chaos.update(blocked=[2, 3], drop_rate=0.5)
    chaos.update(drop_rate=0.1)
    assert chaos.settings() == {"blocked": [2, 3], "drop_rate": 0.1}
    with pytest.raises(ValueError):
        chaos.update(drop_rate=2)
    chaos.heal()
    assert chaos.settings() == {"blocked": [], "drop_rate": 0.0}


def send_through(chaos):
    """Send one request through a ChaosTransport; return (handled, error)."""
    handled = []

    def handler(request):
        handled.append(request.url.port)
        return httpx.Response(200, json={"ok": True})

    async def scenario():
        transport = ChaosTransport(chaos, BROKERS, inner=httpx.MockTransport(handler))
        async with httpx.AsyncClient(transport=transport) as client:
            try:
                await client.post(f"{BROKERS[2]}/partitions/login/replicate", json={})
                return None
            except httpx.TransportError as exc:
                return exc

    error = asyncio.run(scenario())
    return handled, error


def test_transport_delivers_normally():
    assert send_through(Chaos()) == ([9002], None)


def test_transport_drops_messages_to_a_blocked_broker():
    chaos = Chaos()
    chaos.update(blocked=[2])
    handled, error = send_through(chaos)
    assert handled == []
    assert isinstance(error, httpx.ConnectError)


def test_lost_reply_means_the_message_was_handled():
    chaos = Chaos()
    chaos.fate = lambda target: "drop_reply"
    handled, error = send_through(chaos)
    assert handled == [9002]
    assert isinstance(error, httpx.ReadTimeout)


@pytest.fixture
def broker():
    brokers = {b: f"http://127.0.0.1:{free_port()}" for b in (1, 2, 3)}
    with TestClient(create_app(1, brokers=brokers, interval=0.05)) as client:
        yield client


def test_chaos_endpoints(broker):
    assert broker.get("/admin/chaos").json() == {"blocked": [], "drop_rate": 0.0}
    assert broker.post("/admin/chaos", json={"blocked": [2, 3]}).json() == {"blocked": [2, 3], "drop_rate": 0.0}
    assert broker.post("/admin/chaos", json={"drop_rate": 0.3}).json() == {"blocked": [2, 3], "drop_rate": 0.3}
    assert broker.get("/state").json()["chaos"] == {"blocked": [2, 3], "drop_rate": 0.3}
    assert broker.post("/admin/heal").json() == {"blocked": [], "drop_rate": 0.0}


def test_bad_chaos_settings_are_rejected(broker):
    assert broker.post("/admin/chaos", json={"blocked": [1]}).status_code == 400  # itself
    assert broker.post("/admin/chaos", json={"blocked": [7]}).status_code == 400
    assert broker.post("/admin/chaos", json={"drop_rate": 1.5}).status_code == 422
    assert broker.get("/admin/chaos").json() == {"blocked": [], "drop_rate": 0.0}


def admin(cluster, broker_id, path, **body):
    response = httpx.post(f"{cluster.brokers[broker_id]}{path}", json=body or None)
    response.raise_for_status()
    return response.json()


def cut_off(cluster, target):
    """Cut `target` off the way the dashboard does: both sides of every link."""
    for b in cluster.brokers:
        blocked = [o for o in cluster.brokers if o != b] if b == target else [target]
        admin(cluster, b, "/admin/chaos", blocked=blocked)


def produce(cluster, service, numbers, timeout=5.0):
    producer = Producer(service, cluster.client())
    lines = [make_line(n, service=service) for n in numbers]
    producer.add(lines)
    wait_until(producer.flush, timeout=timeout, message=f"{service} lines to be acked")
    return [line["msg_id"] for line in lines]


def test_cutting_off_the_leader_through_the_endpoints(cluster):
    acked = produce(cluster, "search", range(5))
    cut_off(cluster, 2)
    new_leader = cluster.wait_for_leader("search", among=(1, 3))
    acked += produce(cluster, "search", range(5, 10))
    for b in cluster.brokers:
        admin(cluster, b, "/admin/heal")
    wait_for_copies(cluster, "search", 10)
    wait_until(lambda: cluster.node(2).leaders["search"] == new_leader, message="broker 2 to follow the new leader")
    check_invariants(cluster, "search", new_leader, acked)


def test_writes_stay_correct_while_brokers_lose_30_percent_of_messages(cluster):
    for b in cluster.brokers:
        admin(cluster, b, "/admin/chaos", drop_rate=0.3)
    acked = produce(cluster, "upload", range(40), timeout=30)
    for b in cluster.brokers:
        admin(cluster, b, "/admin/heal")
    leader = cluster.wait_for_leader("upload")
    wait_for_copies(cluster, "upload", len(cluster.entries(leader, "upload")))
    check_invariants(cluster, "upload", leader, acked)
    assert cluster.entries(1, "upload") == cluster.entries(2, "upload") == cluster.entries(3, "upload")
