from dashboard.events import EventLog, leader_of

PARTITIONS = ["login", "search", "upload"]


def copy(role, length, epoch=1):
    return {"role": role, "length": length, "epoch": epoch}


def state(login, search=None, upload=None):
    return {"partitions": {"login": login, "search": search or copy("follower", 0),
                           "upload": upload or copy("follower", 0)}}


def texts(log):
    return [e["text"] for e in log.recent()][::-1]  # oldest first


def healthy(length=10):
    return {
        1: state(copy("leader", length), copy("follower", length), copy("follower", length)),
        2: state(copy("follower", length), copy("leader", length), copy("follower", length)),
        3: state(copy("follower", length), copy("follower", length), copy("leader", length)),
    }


def test_leader_of_prefers_the_newest_epoch():
    states = {1: state(copy("leader", 5, epoch=1)), 2: state(copy("leader", 7, epoch=2)), 3: None}
    assert leader_of(states, "login") == (2, 2, 7)
    assert leader_of({1: None, 2: None, 3: None}, "login") is None


def test_first_look_records_the_leaders_only():
    log = EventLog(PARTITIONS, clock=lambda: 1.0)
    log.observe(healthy())
    assert texts(log) == [
        "broker 1 became login leader (epoch 1)",
        "broker 2 became search leader (epoch 1)",
        "broker 3 became upload leader (epoch 1)",
    ]
    log.observe(healthy(11))
    assert len(texts(log)) == 3  # nothing changed


def test_a_leader_crash_and_takeover():
    log = EventLog(PARTITIONS)
    log.observe(healthy())
    states = healthy()
    states[1] = None
    log.observe(states, processes={1: "stopped", 2: "running", 3: "running"})
    states[2]["partitions"]["login"] = copy("leader", 10, epoch=2)
    log.observe(states)
    assert texts(log)[3:] == ["broker 1 stopped", "login has no leader", "broker 2 became login leader (epoch 2)"]


def test_a_broker_that_stops_answering_while_running():
    log = EventLog(PARTITIONS)
    log.observe(healthy())
    states = healthy()
    states[3] = None
    log.observe(states, processes={1: "running", 2: "running", 3: "running"})
    assert "broker 3 stopped answering" in texts(log)


def test_a_restarted_broker_comes_up_and_catches_up():
    log = EventLog(PARTITIONS)
    states = healthy(100)
    states[2] = None
    log.observe(states)
    states[2] = state(copy("follower", 0), copy("follower", 0), copy("follower", 0))
    log.observe(states)
    for length in (40, 99):
        states[2]["partitions"]["login"] = copy("follower", length)
        log.observe(states)
    assert "broker 2 is up" in texts(log)
    assert {e["kind"] for e in log.recent() if e["text"].startswith("broker 2 is up")} == {"up"}
    assert texts(log).count("broker 2 caught up on login") == 1


def test_small_lags_are_not_reported():
    log = EventLog(PARTITIONS)
    states = healthy(100)
    states[3]["partitions"]["login"] = copy("follower", 90)
    log.observe(states)
    states[3]["partitions"]["login"] = copy("follower", 100)
    log.observe(states)
    assert not any("caught up" in t for t in texts(log))


def test_added_events_come_back_newest_first():
    times = iter([1.0, 2.0])
    log = EventLog(PARTITIONS, clock=lambda: next(times))
    log.add("action", "killed broker 1")
    log.add("leader", "broker 2 became login leader (epoch 2)")
    assert log.recent() == [
        {"time": 2.0, "kind": "leader", "text": "broker 2 became login leader (epoch 2)"},
        {"time": 1.0, "kind": "action", "text": "killed broker 1"},
    ]
