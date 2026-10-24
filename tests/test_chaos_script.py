import argparse

import pytest
from fastapi.testclient import TestClient

from dashboard.server import create_app
from scripts.chaos import parse_rate, parser, run


def chaos(client, *argv):
    return run(parser().parse_args(list(argv)), client)


@pytest.fixture
def dashboard(cluster):
    with TestClient(create_app(brokers=cluster.brokers)) as client:
        yield client


def test_parse_rate_accepts_percentages_and_fractions():
    assert parse_rate("30%") == parse_rate("30") == parse_rate("0.3") == pytest.approx(0.3)
    assert parse_rate("0") == 0
    with pytest.raises(argparse.ArgumentTypeError):
        parse_rate("150%")


def test_status_shows_brokers_and_leaders(dashboard):
    assert chaos(dashboard, "status").splitlines() == [
        "broker 1: up", "broker 2: up", "broker 3: up",
        "login: led by broker 1 in epoch 1",
        "search: led by broker 2 in epoch 1",
        "upload: led by broker 3 in epoch 1",
    ]


def test_cut_off_drop_and_heal(cluster, dashboard):
    assert chaos(dashboard, "cut-off", "3") == "cut off broker 3"
    assert cluster.apps[3].state.chaos.blocked == {1, 2}
    assert chaos(dashboard, "drop", "30%") == "brokers now lose 30% of the messages they send each other"
    status = chaos(dashboard, "status").splitlines()
    assert status[2] == "broker 3: up (not reaching broker 1, 2; losing 30% of messages)"
    assert chaos(dashboard, "heal") == "network healed"
    assert all(cluster.apps[b].state.chaos.settings() == {"blocked": [], "drop_rate": 0.0} for b in (1, 2, 3))


def test_dead_brokers_are_reported(cluster, dashboard):
    cluster.stop(2)
    assert chaos(dashboard, "heal") == "network healed (broker 2 is down and was skipped)"
    assert "broker 2: down" in chaos(dashboard, "status")


def test_kill_needs_the_dashboard_that_started_the_brokers(dashboard):
    assert chaos(dashboard, "kill", "1") == "failed: 409 this dashboard didn't start the brokers"
