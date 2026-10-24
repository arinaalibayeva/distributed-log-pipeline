from datetime import datetime

from scripts.watch import leader_of, render


def copy(role, length, commit, epoch=1):
    return {"role": role, "length": length, "commit_index": commit, "epoch": epoch}


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
    assert lines[4].split() == ["login", "*120", "[119]", "79", "[78]", "-41", "-", "leader", "1,", "epoch", "1"]
    assert lines[5].split() == ["search", "50", "[49]", "*50", "[49]", "-", "leader", "2,", "epoch", "1"]
    assert lines[6].split() == ["upload", "7", "[6]", "7", "[6]", "-", "no", "leader"]


def test_leader_after_a_takeover_is_the_one_with_the_newest_epoch():
    # Broker 1 led login in epoch 1 and hasn't heard that broker 2 took over.
    states = {
        1: state(copy("leader", 10, 8, epoch=1), copy("follower", 0, -1), copy("follower", 0, -1)),
        2: state(copy("leader", 14, 13, epoch=2), copy("follower", 0, -1), copy("follower", 0, -1)),
        3: state(copy("follower", 14, 12, epoch=2), copy("follower", 0, -1), copy("follower", 0, -1)),
    }
    assert leader_of(states, "login") == (2, 2)
    row = render(states).splitlines()[4].split()
    assert row == ["login", "10", "[8]", "-4", "*14", "[13]", "14", "[12]", "leader", "2,", "epoch", "2"]


def test_render_without_the_leader_shows_no_lag():
    states = {1: None, 2: state(copy("follower", 3, 2), copy("leader", 0, -1), copy("follower", 0, -1)), 3: None}
    assert render(states).splitlines()[4].split() == ["login", "-", "3", "[2]", "-", "no", "leader"]
