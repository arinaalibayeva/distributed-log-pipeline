from datetime import datetime

from scripts.watch import render


def copy(role, length, commit):
    return {"role": role, "length": length, "commit_index": commit}


def state(login, search, upload):
    return {"partitions": {"login": login, "search": search, "upload": upload}}


def test_render_shows_leaders_lag_and_down_brokers():
    states = {
        1: state(copy("leader", 120, 119), copy("follower", 50, 49), copy("follower", 7, 6)),
        2: state(copy("follower", 79, 78), copy("leader", 50, 49), copy("follower", 7, 6)),
        3: None,
    }
    lines = render(states, now=datetime(2026, 10, 27, 15, 2, 7)).splitlines()
    assert lines[0].startswith("Cluster state  15:02:07")
    assert lines[3].split() == ["up", "up", "DOWN"]
    assert lines[4].split() == ["login", "*120", "[119]", "79", "[78]", "-41", "-"]
    assert lines[5].split() == ["search", "50", "[49]", "*50", "[49]", "-"]
    assert lines[6].split() == ["upload", "7", "[6]", "7", "[6]", "-"]


def test_render_without_the_leader_shows_no_lag():
    states = {1: None, 2: state(copy("follower", 3, 2), copy("leader", 0, -1), copy("follower", 0, -1)), 3: None}
    assert render(states).splitlines()[4].split() == ["login", "-", "3", "[2]", "-"]
