# Distributed Log Pipeline

A small Kafka-style log pipeline in Python. Three fake services (`login`, `search`, `upload`) send log lines to a cluster of three brokers. Consumers read the stored lines to print alerts and archive them.

## What it does

- Each service has its own partition: an append-only log with offsets.
- Every partition is copied to all three brokers. A line is only acknowledged once 2 of the 3 brokers have it, so one broker can crash without losing acknowledged lines.
- Each partition has one leader. If the leader stops sending heartbeats for about 2 seconds, another broker takes over with a higher epoch. A leader that can't reach the others steps down, so an old leader can't overwrite anything.
- A crashed broker comes back empty and copies what it missed from the leader.
- Producers keep retrying until a line is acknowledged. Retries reuse the same `msg_id`, so a line is never stored twice.
- The alert consumer prints an alert when one service logs 3 errors within 5 seconds. The archive consumer writes every line to a file.
- A dashboard shows the whole cluster live and lets you cause failures with buttons.

## Running it

Needs Python 3.10 or newer.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

./scripts/start_cluster.sh   # Ctrl-C stops everything
```

Then open http://127.0.0.1:8000. Give it a couple of seconds to pick the first leaders.

Alerts print in the terminal. Logs go to `data/logs/` and archived lines to `data/archive/`.

## The dashboard

Each cell is one broker's copy of a partition. The leader has a blue border and shows its latest lines. Followers show whether they are in sync or how many lines behind they are. On the right you can see the producers and a timeline of what happened.

Things to try:

- **Kill a follower.** The partition keeps going on the other two. **Restart** it and it catches up.
- **Kill a leader.** Another broker takes over in about 2 seconds and the epoch goes up.
- **Cut off a leader.** It steps down and the other two carry on. **Heal network** and it comes back as a follower.
- **Drop 30%** of the messages between brokers. All copies still end up the same.
- **Kill one broker and cut off another.** No partition has a majority, so producers show lines waiting instead of losing them. Heal and restart, and they go through.

## From the terminal

The same controls without the dashboard:

```bash
python scripts/chaos.py status
python scripts/chaos.py kill 2        # or: restart 2
python scripts/chaos.py cut-off 1
python scripts/chaos.py drop 30%
python scripts/chaos.py heal
```

`python scripts/watch.py` shows the cluster in a second terminal instead of the browser.

## Tests

```bash
pytest
```

The tests start three real brokers, cut them off from each other, drop messages and crash leaders while writing, then check that every acknowledged line is stored exactly once and all copies match.

## Layout

```
broker/      partition log, replication, leader takeover, failure injection, server
producer/    fake service that sends log lines
consumers/   alert and archive consumers
dashboard/   the web page and its server (it also starts the brokers)
scripts/     start_cluster.sh, chaos.py, watch.py
tests/       unit tests and tests against three real brokers
```
