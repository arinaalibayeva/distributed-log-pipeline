"""Tests for the leader's replication messages, with followers faked."""

import asyncio
import json

import httpx

from broker.node import BrokerNode
from broker.replication import Replicator
from conftest import make_line

BROKERS = {1: "http://b1", 2: "http://b2", 3: "http://b3"}


def run(coroutine):
    return asyncio.run(coroutine)


def replicator_with(handler, node=None):
    node = node or BrokerNode(1, list(BROKERS))
    replicator = Replicator(node, BROKERS, batch=2)
    replicator.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return replicator


def test_send_once_sends_missing_entries_and_commits():
    sent = []

    def handler(request):
        sent.append((request.url.host, json.loads(request.content)))
        body = sent[-1][1]
        return httpx.Response(200, json={"accepted": True, "length": body["start"] + len(body["entries"])})

    replicator = replicator_with(handler)
    for n in range(3):
        replicator.node.append("login", make_line(n))

    assert run(replicator.send_once("login", 2)) is True
    host, body = sent[0]
    assert host == "b2"
    assert (body["leader"], body["epoch"], body["start"], body["commit_index"]) == (1, 0, 0, -1)
    assert body["prev_msg_id"] is None
    assert [e["offset"] for e in body["entries"]] == [0, 1]
    assert replicator.node.follower_lengths["login"][2] == 2
    assert replicator.node.partitions["login"].commit_index == 1


def test_gap_reply_resends_from_the_followers_length():
    def handler(request):
        return httpx.Response(200, json={"accepted": False, "length": 1})

    replicator = replicator_with(handler)
    for n in range(4):
        replicator.node.append("login", make_line(n))
    replicator.node.follower_lengths["login"][2] = 3

    assert run(replicator.send_once("login", 2)) is False
    assert replicator.node.message_for("login", 2, batch=10)["start"] == 1


def test_unreachable_follower_changes_nothing():
    def handler(request):
        raise httpx.ConnectError("down", request=request)

    replicator = replicator_with(handler)
    replicator.node.append("login", make_line(0))
    assert run(replicator.send_once("login", 2)) is False
    assert replicator.status[("login", 2)] == "unreachable"
    assert replicator.node.partitions["login"].commit_index == -1


def test_rejected_message_is_reported_not_recorded():
    def handler(request):
        return httpx.Response(409, json={"error": "stale_epoch"})

    replicator = replicator_with(handler)
    replicator.node.append("login", make_line(0))
    assert run(replicator.send_once("login", 2)) is False
    assert replicator.status[("login", 2)].startswith("rejected")
    assert replicator.node.follower_lengths["login"][2] == 0


def test_status_changes_are_printed_once(capsys):
    def handler(request):
        raise httpx.ConnectError("down", request=request)

    replicator = replicator_with(handler)
    run(replicator.send_once("login", 2))
    run(replicator.send_once("login", 2))
    assert capsys.readouterr().out == "login: follower 2 unreachable\n"


def test_wait_committed():
    replicator = replicator_with(lambda request: httpx.Response(500))
    replicator.node.append("login", make_line(0))

    async def commit_soon():
        await asyncio.sleep(0.05)
        replicator.node.record_follower("login", 2, 1)

    async def scenario():
        assert await replicator.wait_committed("login", 0, timeout=0.01) is False
        asyncio.get_running_loop().create_task(commit_soon())
        return await replicator.wait_committed("login", 0, timeout=1.0)

    assert run(scenario()) is True
