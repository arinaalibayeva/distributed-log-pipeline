# Project Plan: Distributed Log Aggregation and Alerting Pipeline

---

## 1. What this project is

This project is a small, Kafka-style distributed log. Several simulated services (a login service, a search service and an image upload service) constantly produce short log lines describing what they are doing, including errors. Instead of each service writing to its own file, they all send their lines to a shared cluster of three **broker** nodes. The cluster stores the lines in ordered, append-only lists called **partitions** (one per service), keeps a copy of every partition on all three brokers so that a crash does not destroy any evidence, and lets several independent **consumers** read the lines at their own pace. The first consumer is an alerting program that raises an alert when a service fails repeatedly. Later consumers archive the lines and index them for search.

The interesting part is not storing log lines, which is easy on one machine, but storing them correctly when machines crash, messages get lost and the network splits in two. The whole project is built around a small set of promises (section 3) and a set of experiments that try to break them (section 9). By the end, the system should be able to show, on screen, that it keeps those promises under every failure we can inject.

## 2. Why it matters

Almost every company runs something like this. When an incident happens, the logs are what engineers use to understand it, which means the log system must keep working, and must not lose data, at exactly the moment other systems are failing. Jay Kreps (2013) argues that an ordered, append-only log is the simplest and most fundamental abstraction for moving data between systems, and Kafka was built around that idea at LinkedIn. Building a small version myself is a way to understand *why* Kafka is designed the way it is: why partitions, why replicas, why a leader, and why it cares so much about when a write counts as "done".

## 3. Guarantees (the promises the system makes)

These are the core of the project. Every design decision below exists to keep one of them, and every failure test checks them.

**When is a write committed?** A log line is *committed* once it is stored on a **majority of the partition's replicas: 2 of 3 brokers**. Only then does the leader send an acknowledgement (ack) back to the producer. Before that point the line may still be lost, and the producer knows to keep retrying.

1. **No lost writes.** An acknowledged line is never lost, as long as no more than one of the three brokers fails. This works because any two majorities of three nodes share at least one node, so whichever two brokers survive, at least one of them has every committed line.
2. **No duplicates.** A line that is sent more than once (because a producer retried after a lost ack) is stored once. Every line carries a unique `msg_id`, and the leader ignores `msg_id`s it has already stored. The alert consumer also tracks `msg_id`s, so a line is never *counted* twice either.
3. **Same order everywhere.** Within a partition, all replicas hold the same lines in the same order. There is no ordering promise *across* partitions, and none is needed: the alert for the login service only cares about login lines.
4. **Consumers only see committed lines.** A consumer can never read a line that might later disappear, so an alert is never raised on data the system later "forgets".

**CAP choice: CP.** During a network partition, the system chooses **consistency over availability**. A leader that cannot reach a majority of replicas stops acknowledging writes. The affected producers buffer their lines and keep retrying until they can reach a leader that has a majority. This means some lines may be *delayed* during a partition, but no acknowledged line is ever lost, and the replicas never end up with two different histories that need to be merged.

## 4. Architecture overview

```
   PRODUCERS                    BROKER CLUSTER                     CONSUMERS
                     ┌──────────┬──────────┬──────────┐
  login service  ──► │ broker1  │ broker2  │ broker3  │ ──►  alert consumer
  search service ──► │          │          │          │ ──►  archive consumer
  upload service ──► │ login  L │ login  F │ login  F │ ──►  (later) search index
                     │ search F │ search L │ search F │
                     │ upload F │ upload F │ upload L │
                     └──────────┴──────────┴──────────┘
                           ▲          ▲          ▲
                           └── state ─┴── state ─┘
                                     │
                                DASHBOARD  +  CHAOS CONTROLS

   L = leader for that partition     F = follower (replica)
```

Every broker stores a copy of every partition (replication factor 3), but each partition has exactly one **leader** at a time. Producers write only to the leader; the leader copies each line to the two **followers**. Leadership is spread out (broker1 leads `login`, broker2 leads `search`, broker3 leads `upload`) so that no single broker does all the work.

## 5. Components

### 5.1 Partition (`broker/partition.py`)
A partition is the basic storage unit: an append-only list of log lines for one service. Each line's position in the list is its **offset** (0, 1, 2, …), which works like a page number that never changes once assigned. Besides the lines themselves, a partition tracks the set of `msg_id`s it has stored (for rejecting duplicates), its **commit index** (the highest offset known to be stored on a majority of replicas), and its **epoch** (a counter that increases every time a new leader takes over, used to reject messages from an outdated leader). This class contains no networking code at all. That is deliberate: it can be fully tested with plain pytest, and it works the same no matter how brokers end up talking to each other.

### 5.2 Broker (`broker/server.py`, `broker/replication.py`)
A broker is a long-running process that owns three partitions and talks to the other brokers. For partitions it leads, it accepts new lines from producers, copies them to followers, tracks how far each follower has gotten, and moves the commit index forward when a majority has a line. For partitions it follows, it accepts copies from the leader and watches the leader's heartbeats. A broker also exposes its full state (for the dashboard) and a set of admin controls (for injecting failures). The three brokers run the same code with different IDs and ports.

### 5.3 Producer (`producer/producer.py`)
A producer simulates one service. It generates a stream of log lines (mostly `INFO`, sometimes `WARN` and `ERROR`, plus occasional "bursts" of errors so that the alert has something to catch). Each line gets a `msg_id` the moment it is created, and that ID is reused on every retry. The producer keeps unacknowledged lines in a small buffer and resends them until it gets an ack. If it sends to a broker that isn't the leader, the broker replies with the current leader's address and the producer switches.

### 5.4 Consumers (`consumers/`)
A consumer reads one or more partitions from front to back, remembering its own **consumer offset** (how far it has read). Consumers never block each other and never delete anything; the log stays intact and a new consumer can start from offset 0 at any time. The **alert consumer** keeps a sliding window of recent `ERROR` timestamps for each service and prints an ALERT when it sees three errors from the same service within five seconds (using the timestamp in the log line, not the time it arrived, so delays don't distort the window). The **archive consumer** writes every line to a file. After the demo, a **search consumer** builds a searchable index and an **error-rate consumer** tracks a rolling error percentage per service.

### 5.5 Dashboard (`dashboard/`)
A web page that asks every broker for its state once per second and draws it: which brokers are up or down, which broker leads each partition, each replica's last few lines, each replica's log length and commit index, and the current epoch. Replicas that have fallen behind or disagree are highlighted. This is the main way of *seeing* that the system is doing what it claims, and it is what the demo video is built around.

### 5.6 Chaos controls (`scripts/chaos.py`, plus dashboard buttons)
A way to break the system on purpose: kill a broker, restart it, set a message drop rate on a broker, block traffic between specific brokers (a network partition), and heal everything. Each failure test and each demo scenario is built from these controls.

## 6. Data model

**A log line:**
```json
{
  "msg_id": "6f1c2e9a-…",      // unique, created once by the producer, reused on retries
  "service": "login",           // also decides which partition it goes to
  "level": "ERROR",             // INFO | WARN | ERROR
  "text": "invalid password for user 42",
  "ts": 1761000000.123          // when the event happened, at the service
}
```

**A stored entry** is the same line plus the `offset` it was assigned and the `epoch` of the leader that wrote it.

**Per-partition state on a broker:** the list of entries, the set of stored `msg_id`s, the commit index, the current epoch, the current leader's ID, and (on the leader only) how far each follower has replicated.

## 7. How a log line flows through the system

1. The login producer creates a line with a fresh `msg_id` and sends it to the leader of the `login` partition.
2. The leader checks the `msg_id`. If it has already stored it, it simply acks again (this is what makes retries safe). Otherwise it appends the line at the next offset.
3. The leader sends the new line to both followers. Each follower appends it at the same offset and replies with how far its log now reaches.
4. As soon as the line is on two of the three brokers (the leader plus at least one follower), the leader advances its commit index and acks the producer. It does not wait for the slower follower, which will catch up later.
5. Followers learn the new commit index with the next replication message or heartbeat.
6. Consumers ask the leader for lines after their offset and receive everything up to the commit index, never beyond it.

## 8. Replication and leadership

**Replication and catch-up.** The leader keeps track of each follower's log length. Whenever a follower is behind (because it was down, or because messages to it were dropped), the leader sends it the missing lines starting from where it stopped. A restarted broker starts empty and catches up this way, which makes for a good demo moment: the node comes back and visibly fills up.

**Computing the commit index.** With three replicas, the commit index is the *second-highest* log length among the three: it is the highest offset that at least two brokers have.

**Heartbeats.** The leader sends a small "I'm alive" message to followers several times per second. If a follower stops hearing from the leader for a while (around 1.5 seconds), it assumes the leader is gone.

**Leader takeover (temporary rule for the demo).** Each partition has a fixed preference order, for example `login: broker1 → broker2 → broker3`. When the leader is considered dead, the next live broker in that order can take over, but **only if it can reach a majority** (itself plus one other). The new leader increases the epoch, then, before accepting any new writes, copies any lines the other surviving replica has that it is missing, so no committed line is lost. Brokers ignore messages that carry an older epoch, so a leader that was cut off and comes back cannot overwrite anything. When an old leader rejoins, it discards anything after its last committed offset (those lines were never acknowledged) and catches up from the new leader.

**Known weakness, fixed later by Raft.** This rule is simple on purpose and has gaps. For example, two brokers can briefly disagree about who is alive, and correctness depends on the "copy before serving" step. Raft solves this properly with voting, terms and a rule that only an up-to-date node can win an election. Replacing the temporary rule with Raft after the demo, and explaining what goes wrong without it, is one of the main learning goals.

**Behavior during a network partition (CP).** A leader that cannot get acks from a majority cannot commit anything, so it stops acknowledging writes, and it steps down once it has been unable to reach a majority for a while. On the majority side, a new leader takes over and writes continue. When the network heals, the old leader sees a higher epoch, steps down, and catches up. At no point do two sides both commit lines.

## 9. Failure scenarios and what should happen

| Scenario | Expected behavior | How it is checked |
|---|---|---|
| **Message drops** (e.g. 30% of messages between brokers, and between producers and brokers) | Producers retry, the leader resends missing lines to followers, duplicates are rejected by `msg_id`. | After the run, all three replicas of every partition are identical; every acked `msg_id` appears exactly once. |
| **Follower crash and restart** | The leader still has a majority, so writes keep committing. The restarted follower catches up from the leader. | Writes never stop; afterwards all replicas are identical. |
| **Leader crash** | Followers notice missing heartbeats, the next broker in line takes over with a higher epoch, producers are redirected. | Writes pause briefly, then resume; no acked line is missing. |
| **Network partition: leader isolated** | The isolated leader stops acking; the majority side elects a new leader and continues; after healing, the old leader steps down and catches up. | The minority side commits nothing during the partition; afterwards all replicas are identical. |
| **Network partition: one follower isolated** | The leader still has a majority with the other follower; nothing visible changes except the isolated follower falling behind. | The isolated follower catches up after healing. |
| **Crash during replication** (leader dies after storing a line but before acking) | The line was never acked, so it may or may not survive, and either outcome is allowed. If the producer retries, `msg_id` checking prevents a duplicate. | The line appears at most once. |

**Invariants checked after every test:**
- every line the producer received an ack for is present in the partition;
- no `msg_id` appears twice in any partition;
- all replicas agree on every entry up to the commit index;
- the alert consumer raised exactly the alerts it should have for the input.

## 10. Testing strategy

Testing happens at three levels. **Unit tests** cover the pure logic with no network: the partition (append, duplicate rejection, read from offset, commit index, truncation) and the alert consumer's sliding window. These are fast and exact. **Integration tests** start three brokers, run producers and consumers for a few seconds, and check the invariants above. **Failure tests** are integration tests with chaos injected partway through: drops, crashes and partitions from section 9. Because failure tests involve real timing, each is run several times to catch problems that only show up occasionally.

## 11. Folder structure

```
distributed-log-pipeline/
├── README.md               # what it is, how to run it, guarantees
├── requirements.txt
├── config.py               # broker addresses, partitions, leader preference order
├── docs/
│   └── PLAN.md             # this file
├── broker/
│   ├── partition.py        # one append-only log: offsets, msg_ids, commit index, epoch
│   ├── server.py           # broker endpoints: append, read, replicate, heartbeat, state, admin
│   └── replication.py      # sending to followers, catch-up, heartbeats, takeover
├── producer/
│   └── producer.py         # simulated service with retry buffer
├── consumers/
│   ├── alert.py            # 3 errors in 5 seconds → ALERT
│   └── archive.py          # writes every line to a file
├── dashboard/
│   └── index.html          # live view of the cluster
├── scripts/
│   ├── start_cluster.sh    # start all three brokers
│   └── chaos.py            # kill / restart / drop / partition / heal
└── tests/
    ├── test_partition.py
    ├── test_alert.py
    ├── test_replication.py
    └── test_failures.py
```

