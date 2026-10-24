"""Break the running cluster on purpose, from the terminal.

    python scripts/chaos.py kill 2        # kill broker 2 (its log is lost)
    python scripts/chaos.py restart 2     # start it again, empty
    python scripts/chaos.py cut-off 1     # lose every message between broker 1 and the others
    python scripts/chaos.py drop 30%      # lose 30% of all messages between brokers
    python scripts/chaos.py heal          # stop losing messages
    python scripts/chaos.py status        # who is up, who leads, what is being lost

It sends each command to the dashboard server (started by start_cluster.sh),
which controls the brokers; the dashboard page shows the effect.
"""

import argparse
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.watch import leader_of  # noqa: E402

DASHBOARD = "http://127.0.0.1:8000"


def parse_rate(text):
    """Accept "30%", "30" or "0.3" and return 0.3."""
    value = float(text.rstrip("%"))
    if text.endswith("%") or value > 1:
        value /= 100
    if not 0 <= value <= 1:
        raise argparse.ArgumentTypeError("the rate must be between 0% and 100%")
    return value


def describe(cluster):
    """Summarize /cluster in a few lines."""
    brokers = cluster["brokers"]
    lines = []
    for broker_id, state in brokers.items():
        if state is None:
            lines.append(f"broker {broker_id}: down")
            continue
        chaos = state["chaos"]
        notes = []
        if chaos["blocked"]:
            notes.append(f"not reaching broker {', '.join(map(str, chaos['blocked']))}")
        if chaos["drop_rate"]:
            notes.append(f"losing {chaos['drop_rate']:.0%} of messages")
        lines.append(f"broker {broker_id}: up" + (f" ({'; '.join(notes)})" if notes else ""))
    states = {b: s for b, s in brokers.items()}
    for name in cluster["partitions"]:
        leader = leader_of(states, name)
        lines.append(f"{name}: " + (f"led by broker {leader[0]} in epoch {leader[1]}" if leader else "no leader"))
    return "\n".join(lines)


def run(args, http):
    """Carry out one command; return what to print."""
    if args.command == "status":
        return describe(http.get("/cluster").json())
    if args.command in ("kill", "restart"):
        reply = http.post(f"/brokers/{args.broker}/{args.command}")
        done = f"{'killed' if args.command == 'kill' else 'restarted'} broker {args.broker}"
    elif args.command == "cut-off":
        reply = http.post(f"/chaos/cut-off/{args.broker}")
        done = f"cut off broker {args.broker}"
    elif args.command == "drop":
        reply = http.post("/chaos/drop", json={"rate": args.rate})
        done = f"brokers now lose {args.rate:.0%} of the messages they send each other"
    else:
        reply = http.post("/chaos/heal")
        done = "network healed"
    if reply.status_code != 200:
        return f"failed: {reply.status_code} {reply.json().get('detail', reply.text)}"
    down = [b for b, settings in reply.json().get("brokers", {}).items() if settings is None]
    if down:
        done += f" (broker {', '.join(down)} is down and was skipped)"
    return done


def parser():
    p = argparse.ArgumentParser(description="Break the running cluster on purpose.")
    p.add_argument("--url", default=DASHBOARD, help=f"dashboard server (default {DASHBOARD})")
    commands = p.add_subparsers(dest="command", required=True)
    for name, help_text in (("kill", "kill a broker; its log is lost"),
                            ("restart", "start a killed broker again, empty"),
                            ("cut-off", "lose every message between a broker and the others")):
        commands.add_parser(name, help=help_text).add_argument("broker", type=int)
    commands.add_parser("drop", help="lose a share of all messages between brokers").add_argument("rate", type=parse_rate)
    commands.add_parser("heal", help="stop losing messages")
    commands.add_parser("status", help="show who is up, who leads, and what is being lost")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        with httpx.Client(base_url=args.url, timeout=5) as http:
            print(run(args, http))
    except httpx.TransportError:
        sys.exit(f"can't reach the dashboard at {args.url}; is the cluster running (scripts/start_cluster.sh)?")


if __name__ == "__main__":
    main()
