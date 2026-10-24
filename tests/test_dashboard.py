import sys

import pytest
from fastapi.testclient import TestClient

from conftest import make_line, send, wait_until
from dashboard.server import BrokerProcesses, create_app


def sleeper(broker_id):
    """Stands in for a broker process in tests."""
    return [sys.executable, "-c", "import time; time.sleep(60)"]


@pytest.fixture
def processes(tmp_path):
    procs = BrokerProcesses(command=sleeper, log_dir=tmp_path)
    yield procs
    procs.kill_all()


def test_cluster_reports_every_broker_and_null_for_a_dead_one(cluster):
    send(cluster.client(), make_line(0))
    cluster.stop(3)
    with TestClient(create_app(brokers=cluster.brokers)) as client:
        data = client.get("/cluster").json()
    assert data["partitions"] == ["login", "search", "upload"]
    assert data["controls"] is False
    assert data["brokers"]["3"] is None
    login = data["brokers"]["1"]["partitions"]["login"]
    assert (login["role"], login["length"], login["commit_index"]) == ("leader", 1, 0)


def test_page_is_served():
    with TestClient(create_app(brokers={})) as client:
        reply = client.get("/")
    assert reply.status_code == 200
    assert "<title>Log Pipeline</title>" in reply.text


def test_buttons_need_brokers_started_by_the_dashboard():
    with TestClient(create_app(brokers={1: "http://127.0.0.1:1"})) as client:
        assert client.post("/brokers/1/kill").status_code == 409


def test_dashboard_starts_kills_and_restarts_brokers(processes):
    with TestClient(create_app(brokers={1: "http://127.0.0.1:1", 2: "http://127.0.0.1:2", 3: "http://127.0.0.1:3"},
                               processes=processes)) as client:
        assert all(processes.is_running(b) for b in (1, 2, 3))
        assert client.get("/cluster").json()["controls"] is True
        assert client.post("/brokers/2/kill").json() == {"broker": 2, "running": False}
        assert not processes.is_running(2)
        assert client.post("/brokers/2/restart").json() == {"broker": 2, "running": True}
        assert processes.is_running(2)
        assert client.post("/brokers/9/kill").status_code == 404
    assert not any(processes.is_running(b) for b in (1, 2, 3))


def test_restart_of_a_running_broker_keeps_it(processes):
    processes.start(1)
    first = processes.processes[1]
    processes.start(1)
    assert processes.processes[1] is first
    wait_until(lambda: processes.is_running(1))


def chaos_of(cluster):
    return {b: cluster.apps[b].state.chaos.settings() for b in cluster.brokers}


def test_cut_off_blocks_both_directions_and_heal_undoes_it(cluster):
    with TestClient(create_app(brokers=cluster.brokers)) as client:
        reply = client.post("/chaos/cut-off/2").json()
        assert reply["brokers"]["2"] == {"blocked": [1, 3], "drop_rate": 0.0}
        assert chaos_of(cluster) == {
            1: {"blocked": [2], "drop_rate": 0.0},
            2: {"blocked": [1, 3], "drop_rate": 0.0},
            3: {"blocked": [2], "drop_rate": 0.0},
        }
        client.post("/chaos/cut-off/3")  # adds to what broker 1 already blocks
        assert chaos_of(cluster)[1] == {"blocked": [2, 3], "drop_rate": 0.0}
        client.post("/chaos/heal")
        assert all(c == {"blocked": [], "drop_rate": 0.0} for c in chaos_of(cluster).values())


def test_drop_rate_is_set_on_every_broker_and_skips_dead_ones(cluster):
    cluster.stop(3)
    with TestClient(create_app(brokers=cluster.brokers)) as client:
        reply = client.post("/chaos/drop", json={"rate": 0.3}).json()
        assert reply["brokers"]["3"] is None
        assert client.post("/chaos/drop", json={"rate": 2}).status_code == 422
        assert client.post("/chaos/cut-off/9").status_code == 404
    assert [cluster.apps[b].state.chaos.drop_rate for b in (1, 2)] == [0.3, 0.3]
