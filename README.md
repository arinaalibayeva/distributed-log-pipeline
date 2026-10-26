# Distributed Log Pipeline

A small Kafka-style log pipeline in Python. Three simulated services (`login`, `search`, `upload`) send log lines to a cluster of three brokers, and consumers read the stored lines to raise alerts and archive them.

## Current state

Working now:

- **Partitions:** append-only logs with offsets, duplicate rejection by `msg_id`, a commit index, epochs and truncation.
- **Brokers:** three FastAPI servers. Each partition has one leader at a time, and leadership is spread so each broker normally leads one. A request sent to the wrong broker is redirected to the leader.
- **Replication:** every partition is copied to all three brokers. A line is acknowledged only once 2 of 3 brokers have it, so one broker can fail without losing acknowledged lines. A restarted broker catches up from the leader.
- **Leader takeover:** if a leader stops sending heartbeats for about 2 seconds, the next broker in the partition's preference order takes over with a higher epoch, after copying any lines it was missing from the other broker. A leader that can't reach a majority steps down and drops the lines it never acknowledged, and brokers ignore messages from an older epoch, so a cut-off leader can't overwrite anything. A restarted broker rejoins as a follower.
- **Producers:** buffer each line and retry until the leader acknowledges it. Retries reuse the `msg_id`, so a line is never stored twice.
- **Alert consumer:** prints an alert when one service logs 3 errors within 5 seconds, using each line's own timestamp.
- **Archive consumer:** writes every line to a file per partition.
- **Dashboard:** a web page showing every broker's copy of each partition live, the producers (lines acknowledged and waiting), alerts, a timeline of what happened, and a regular check that all copies agree. Buttons kill and restart brokers, cut a broker off from the others, lose a share of the messages between brokers, pause producers and trigger an error burst.
- **Failure controls:** the same failures from the terminal with `scripts/chaos.py`.

## Running it

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

./scripts/start_cluster.sh   # Ctrl-C stops everything
```

Alerts print in the terminal. Process logs go to `data/logs/` and archived lines to `data/archive/`. Each broker's API can be explored at http://127.0.0.1:8001/docs (ports 8001-8003).

The brokers choose their first leaders in the first couple of seconds; producers hold their lines until then.

Open the dashboard at http://127.0.0.1:8000. Each cell is one broker's copy of a partition: its role and epoch, its length, commit index and last lines. The leader has a blue border and copies that are behind are amber.

Things to try on the dashboard:

- **Kill** a follower: the partition keeps committing on the other two brokers. **Restart** brings it back empty, and it catches up.
- **Kill** a leader: within about 2 seconds another broker takes over, the blue border moves and the epoch goes up.
- **Cut off** a leader: it stops committing and steps down, and the other two brokers carry on. **Heal network** and it rejoins as a follower. Producers can still reach a cut-off broker, so this also shows the cluster refusing to acknowledge lines it can't store on a majority.
- Set the drop rate to **30%**: brokers lose that share of the messages they send each other, yet every copy stays the same.
- Press **Error burst** on a producer: it sends 5 errors at once and an alert appears within a second or two.
- Kill one broker and cut off another: no partition has a majority, so the producers show lines **waiting to retry** instead of losing them. Heal and restart, and the waiting lines go through.

The **What happened** panel keeps a timeline of every step, and the **Correctness check** compares committed message IDs and their order among available copies close to the leader every few seconds. It does not compare message contents or prove that every acknowledged line survived.

The same controls from a terminal:

```bash
python scripts/chaos.py status
python scripts/chaos.py kill 2          # or: restart 2
python scripts/chaos.py cut-off 1
python scripts/chaos.py drop 30%
python scripts/chaos.py heal
```

For a terminal view instead, run this in a second terminal:

```bash
python scripts/watch.py
```

It shows every broker's copy of each partition (length and commit index), which broker leads and in which epoch, and how far behind each copy is.

## Tests

```bash
pytest
```

The tests run three real brokers in the test process. `tests/test_failures.py` and `tests/test_chaos.py` cut brokers off from each other, lose 30% of the messages between them, and crash leaders under constant writes, then check that every acknowledged line is stored exactly once and all copies agree.

## Layout

```
broker/      partition log, broker logic, replication, leader takeover, failure injection, HTTP server, cluster client, config
producer/    simulated service that sends log lines
consumers/   alert detector, alert consumer, archive consumer
dashboard/   web page and the server that runs it (it also starts the brokers)
scripts/     start_cluster.sh, watch.py, chaos.py
tests/       unit tests and integration tests against three real brokers
```
