"""A live view of the cluster in the terminal, redrawn every second.

    python scripts/watch.py

For every partition it shows each broker's copy: its length and, in
brackets, its commit index. `*` marks the leader, and copies behind the
leader say how many lines they're missing. A broker that doesn't answer is
shown as DOWN.
"""

import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from broker import config  # noqa: E402

COLUMN = 22


def fetch_states(http, brokers=config.BROKERS):
    """Return {broker ID: state dict, or None if the broker didn't answer}."""
    states = {}
    for broker_id, url in brokers.items():
        try:
            states[broker_id] = http.get(f"{url}/state").json()
        except httpx.HTTPError:
            states[broker_id] = None
    return states


def render(states, partitions=config.PARTITIONS, now=None):
    now = now or datetime.now()
    ids = list(states)
    lines = [f"Cluster state  {now:%H:%M:%S}   (Ctrl-C to quit)", ""]
    lines.append(" " * 10 + "".join(f"broker {b}".ljust(COLUMN) for b in ids))
    lines.append(" " * 10 + "".join(("up" if states[b] else "DOWN").ljust(COLUMN) for b in ids))
    for name in partitions:
        leader = config.initial_leader(name)
        leader_state = states.get(leader)
        leader_length = leader_state["partitions"][name]["length"] if leader_state else None
        cells = []
        for b in ids:
            if states[b] is None:
                cells.append("-")
                continue
            copy = states[b]["partitions"][name]
            cell = f"{'*' if copy['role'] == 'leader' else ' '}{copy['length']} [{copy['commit_index']}]"
            if leader_length is not None and copy["length"] < leader_length:
                cell += f" -{leader_length - copy['length']}"
            cells.append(cell)
        lines.append(name.ljust(10) + "".join(cell.ljust(COLUMN) for cell in cells))
    lines += ["", "* leader   length [commit index]   -N lines behind the leader"]
    return "\n".join(lines)


def main():
    http = httpx.Client(timeout=0.5)
    try:
        while True:
            screen = render(fetch_states(http))
            print("\033[2J\033[H" + screen, flush=True)  # clear the terminal, then draw
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
