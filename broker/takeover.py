"""Takes over partitions whose leader has gone quiet, and steps down when cut off.

Each broker runs one loop per partition. On a follower, the loop waits until
the broker hasn't heard from a leader for long enough (see
BrokerNode.should_take_over), then tries to take over in two rounds:

1. It asks the other brokers whether they would agree, without changing
   anything. A broker cut off from the others gets no answers and stops
   here, so it doesn't push up the epoch and disturb the leader when it
   reconnects.
2. It moves to the new epoch itself and asks again for real. Brokers that
   agree move to the new epoch too and send the end of their logs. With a
   majority (itself and one other), it becomes leader.

On a leader, the loop steps down once the broker hasn't reached a majority
for a while, so it stops claiming a partition it can no longer commit to.
"""

import asyncio
import time

import httpx

from broker import config


class Takeover:
    def __init__(self, node, replicator, brokers=config.BROKERS, check_every=0.05, timeout=0.5, transport=None):
        self.node = node
        self.replicator = replicator
        self.brokers = brokers
        self.check_every = check_every  # seconds between checks of the timers
        self.timeout = timeout  # seconds to wait for another broker's answer
        self.transport = transport
        self.http = None
        self.tasks = []

    def start(self):
        """Start one loop per partition. Call from the event loop."""
        self.http = httpx.AsyncClient(timeout=self.timeout, transport=self.transport)
        self.tasks = [asyncio.create_task(self._run(name)) for name in self.node.partitions]

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        if self.http is not None:
            await self.http.aclose()

    async def _run(self, name):
        while True:
            try:
                await self.check(name)
            except Exception as exc:  # keep watching even if one check fails oddly
                print(f"{name}: takeover check failed: {exc!r}", flush=True)
            await asyncio.sleep(self.check_every)

    async def check(self, name):
        """Step down or try to take over `name` if its timers say so."""
        now = time.monotonic()
        if self.node.lost_majority(name, now):
            self.node.step_down(name, now)
            print(f"{name}: can't reach a majority; stepped down", flush=True)
        elif self.node.should_take_over(name, now):
            await self.attempt(name)

    async def attempt(self, name):
        """Try once to take over `name`; return True if this broker now leads it."""
        node = self.node
        partition = node.partitions[name]
        epoch = node.next_epoch(name)

        replies = await self._ask(name, epoch, partition.commit_index, dry_run=True)
        if 1 + sum(r["granted"] for r in replies) < node.majority:
            node.wait_again(name)
            return False
        # Hearing from a leader while asking means there's no need to take over.
        if not node.should_take_over(name) or not node.start_campaign(name, epoch):
            return False

        commit_index = partition.commit_index
        replies = await self._ask(name, epoch, commit_index, dry_run=False)
        granted = [r for r in replies if r["granted"]]
        if 1 + len(granted) < node.majority:
            return False
        if not node.become_leader(name, epoch, commit_index, granted):
            return False
        print(f"{name}: took over as leader (epoch {epoch})", flush=True)
        self.replicator.wake(name)
        return True

    async def _ask(self, name, epoch, commit_index, dry_run):
        """Ask every other broker to agree; return the answers that came back."""
        request = {"candidate": self.node.id, "epoch": epoch, "commit_index": commit_index, "dry_run": dry_run}

        async def ask(peer):
            try:
                response = await self.http.post(f"{self.brokers[peer]}/partitions/{name}/takeover", json=request)
            except httpx.TransportError:
                return None
            return response.json() if response.status_code == 200 else None

        answers = await asyncio.gather(*(ask(peer) for peer in self.node.peers))
        replies = [a for a in answers if a is not None]
        for reply in replies:
            self.node.note_epoch(name, reply["epoch"])
        return replies
