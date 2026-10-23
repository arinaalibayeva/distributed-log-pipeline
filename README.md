# Distributed Log Pipeline

A small Kafka-style log pipeline in Python. Three simulated services (`login`, `search`, `upload`) send log lines to a cluster of three brokers, and consumers read the stored lines to raise alerts and archive them.

## Current state

Working now:

- **Partitions:** append-only logs with offsets, duplicate rejection by `msg_id`, a commit index, epochs and truncation.
- **Brokers:** three FastAPI servers, each leading one partition. A request sent to the wrong broker is redirected to the leader.
- **Replication:** every partition is copied to all three brokers. A line is acknowledged only once 2 of 3 brokers have it, so one broker can fail without losing acknowledged lines. A restarted follower catches up from the leader.
- **Producers:** buffer each line and retry until the leader acknowledges it. Retries reuse the `msg_id`, so a line is never stored twice.
- **Alert consumer:** prints an alert when one service logs 3 errors within 5 seconds, using each line's own timestamp.
- **Archive consumer:** writes every line to a file per partition.
- **Dashboard:** a web page showing every broker's copy of each partition live, with buttons to kill and restart brokers.

Not yet built:

- Leader takeover. Until then, a partition stops accepting lines while its leader is down, and stays stopped after the leader restarts.
- Failure tests, and controls for dropping messages and cutting brokers off from each other.

## Running it

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

./scripts/start_cluster.sh   # Ctrl-C stops everything
```

Alerts print in the terminal. Process logs go to `data/logs/` and archived lines to `data/archive/`. Each broker's API can be explored at http://127.0.0.1:8001/docs (ports 8001-8003).

Open the dashboard at http://127.0.0.1:8000. Each cell is one broker's copy of a partition: its length, commit index and last lines. The leader has a blue border and copies that are behind are amber. **Kill** stops a broker at once (its log is lost) and **Restart** brings it back empty, to catch up from the leader.

For a terminal view instead, run this in a second terminal:

```bash
python scripts/watch.py
```

It shows every broker's copy of each partition (length and commit index), which broker leads, and how far behind each copy is.

## Tests

```bash
pytest
```

## Layout

```
broker/      partition log, broker logic, replication, HTTP server, cluster client, config
producer/    simulated service that sends log lines
consumers/   alert detector, alert consumer, archive consumer
dashboard/   web page and the server that runs it (it also starts the brokers)
scripts/     start_cluster.sh, watch.py
tests/       unit tests and integration tests against three real brokers
```
