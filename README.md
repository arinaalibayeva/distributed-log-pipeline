# Distributed Log Pipeline

A small Kafka-style log pipeline in Python. Three simulated services (`login`, `search`, `upload`) send log lines to a cluster of three brokers, and consumers read the stored lines to raise alerts and archive them.

## Current state

Working now:

- **Partitions:** append-only logs with offsets, duplicate rejection by `msg_id`, a commit index, epochs and truncation.
- **Brokers:** three FastAPI servers, each leading one partition. A request sent to the wrong broker is redirected to the leader.
- **Producers:** buffer each line and retry until the leader acknowledges it. Retries reuse the `msg_id`, so a line is never stored twice.
- **Alert consumer:** prints an alert when one service logs 3 errors within 5 seconds, using each line's own timestamp.
- **Archive consumer:** writes every line to a file per partition.

Not yet built:

- Replication across brokers, so each partition currently exists only on its leader.
- Leader takeover when a broker dies.
- Failure tests and chaos controls.
- A live dashboard.

## Running it

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

./scripts/start_cluster.sh   # Ctrl-C stops everything
```

Alerts print in the terminal. Process logs go to `data/logs/` and archived lines to `data/archive/`. Each broker's API can be explored at http://127.0.0.1:8001/docs (ports 8001-8003).

## Tests

```bash
pytest
```

## Layout

```
broker/      partition log, broker logic, HTTP server, cluster client, config
producer/    simulated service that sends log lines
consumers/   alert detector, alert consumer, archive consumer
scripts/     start_cluster.sh
tests/       unit tests and an in-process fake cluster
```
