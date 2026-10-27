"""The dashboard's event timeline: what happened in the cluster, and when.

The dashboard server looks at every broker's state about once a second and
passes it to EventLog.observe(), which compares it with the previous look
and records what changed: a broker stopping or coming back, a partition
getting a new leader or losing it, and a copy catching up after falling
behind. The server also records its own actions (kill, cut off, ...) with
EventLog.add().
"""

import time
from collections import deque

BEHIND = 20  # a copy this many lines behind its leader counts as fallen behind
IN_SYNC = 2  # and this few lines behind as caught up again (lines are always in flight)


def leader_of(states, name):
    """Return (broker ID, epoch, length) of `name`'s leader, or None.

    If two brokers say they lead (an old leader that hasn't heard of the
    takeover yet), the one with the newer epoch is the real leader.
    """
    leading = [(state["partitions"][name]["epoch"], b, state["partitions"][name]["length"])
               for b, state in states.items() if state and state["partitions"][name]["role"] == "leader"]
    if not leading:
        return None
    epoch, broker_id, length = max(leading)
    return broker_id, epoch, length


class EventLog:
    def __init__(self, partitions, limit=200, clock=time.time):
        self.partitions = list(partitions)
        self.events = deque(maxlen=limit)
        self.clock = clock
        self.up = None  # broker ID -> answered on the last look; None before the first look
        self.leaders = {}  # partition -> (broker ID, epoch) on the last look
        self.behind = set()  # (partition, broker ID) copies that fell behind and haven't caught up

    def add(self, kind, text):
        """Record an event. `kind` is "action", "down", "up", "leader" or "sync"."""
        self.events.append({"time": self.clock(), "kind": kind, "text": text})

    def recent(self, count=50):
        """The latest `count` events, newest first."""
        return list(self.events)[-count:][::-1]

    def observe(self, states, processes=None):
        """Compare `states` ({broker ID: /state or None}) with the last look.

        `processes` ({broker ID: "running" or "stopped"}) tells a broker that
        stopped apart from one that stopped answering.
        """
        up = {b: state is not None for b, state in states.items()}
        if self.up is not None:
            for b, now_up in up.items():
                if self.up.get(b) and not now_up:
                    stopped = processes is None or processes.get(b) != "running"
                    self.add("down", f"broker {b} {'stopped' if stopped else 'stopped answering'}")
                    self.behind = {(name, other) for name, other in self.behind if other != b}
                elif now_up and not self.up.get(b):
                    self.add("up", f"broker {b} is up")
        self.up = up

        for name in self.partitions:
            leader = leader_of(states, name)
            key = leader[:2] if leader else None
            previous = self.leaders.get(name)
            if key != previous:
                if key:
                    self.add("leader", f"broker {key[0]} became {name} leader (epoch {key[1]})")
                elif previous:
                    self.add("leader", f"{name} has no leader")
                self.leaders[name] = key
            if not leader:
                continue
            for b, state in states.items():
                if not state or b == leader[0]:
                    continue
                lag = leader[2] - state["partitions"][name]["length"]
                if lag > BEHIND:
                    self.behind.add((name, b))
                elif lag <= IN_SYNC and (name, b) in self.behind:
                    self.behind.discard((name, b))
                    self.add("sync", f"broker {b} caught up on {name}")
