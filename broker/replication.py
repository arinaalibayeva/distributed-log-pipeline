"""Copies a leader's partitions to its followers.

For every partition it leads, a broker runs one loop per follower. Each round
the loop sends the follower the entries it is missing (or, when it is up to
date, an empty heartbeat), together with the leader's commit index and epoch
and the msg_id of the entry just before, which the follower checks against
its own log. When the follower accepts, the leader commits whatever a
majority of brokers now have. A new line wakes the loops right away, so the
producer doesn't wait for the next round.

A follower that is down or slow just falls behind: the leader keeps trying
every round, and once the follower answers again it is sent everything it
missed. If a message doesn't attach to the follower's log, the leader steps
back and resends from earlier until it does.

Leadership can move (see takeover.py), so there is a loop for every
partition and peer; it only sends while this broker leads the partition. A
follower that answers with a newer epoch means another broker has taken
over, and this one steps down.
"""

import asyncio
import time

import httpx

from broker import config


class Replicator:
    def __init__(self, node, brokers=config.BROKERS, interval=0.2, batch=500, timeout=0.5, transport=None):
        self.node = node
        self.brokers = brokers
        self.interval = interval  # seconds between rounds when there's nothing new
        self.batch = batch  # most entries sent in one message
        self.timeout = timeout  # seconds to wait for a follower's reply
        self.transport = transport
        self.http = None
        self.tasks = []
        self.wakeups = {}  # (partition, follower) -> asyncio.Event
        self.status = {}  # (partition, follower) -> last reported status

    def start(self):
        """Start one loop per (partition, peer). Call from the event loop."""
        self.http = httpx.AsyncClient(timeout=self.timeout, transport=self.transport)
        for name in self.node.partitions:
            for follower in self.node.peers:
                self.wakeups[(name, follower)] = asyncio.Event()
                self.tasks.append(asyncio.create_task(self._run(name, follower)))

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        if self.http is not None:
            await self.http.aclose()

    def wake(self, name):
        """Send partition `name`'s new entries now instead of next round."""
        for follower in self.node.peers:
            event = self.wakeups.get((name, follower))
            if event is not None:
                event.set()

    async def wait_committed(self, name, offset, msg_id, timeout):
        """Wait until line `msg_id`, stored at `offset` of `name`, is committed.

        Returns False if `timeout` passes first, or if this broker stops
        leading meanwhile: it then drops its uncommitted lines, and a new
        leader may commit a different line at that offset, so it can't
        vouch for this one.
        """
        node = self.node
        partition = node.partitions[name]
        epoch = partition.epoch
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if not node.is_leader(name) or partition.epoch != epoch:
                return False
            if partition.commit_index >= offset:
                return partition.entries[offset]["line"]["msg_id"] == msg_id
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(0.005)

    async def send_once(self, name, follower):
        """Send `follower` what it is missing (or a heartbeat) and record its reply.

        Returns True if the follower accepted the message.
        """
        if not self.node.is_leader(name):
            return False
        message = self.node.message_for(name, follower, self.batch)
        url = f"{self.brokers[follower]}/partitions/{name}/replicate"
        try:
            response = await self.http.post(url, json=message)
        except httpx.TransportError:
            self._report(name, follower, "unreachable")
            return False
        if response.status_code != 200:
            self._report(name, follower, f"rejected entries ({response.status_code} {response.text})")
            self._learn_epoch(name, follower, response)
            return False
        reply = response.json()
        self.node.record_reply(name, follower, message, reply["accepted"], reply["length"], time.monotonic())
        self._report(name, follower, "ok")
        return reply["accepted"]

    def _learn_epoch(self, name, follower, response):
        """Step down if a follower says another broker took over with a newer epoch."""
        try:
            reply = response.json()
        except ValueError:
            return
        if not isinstance(reply, dict) or reply.get("error") != "stale_epoch":
            return
        epoch = reply.get("epoch")
        if isinstance(epoch, int) and epoch > self.node.partitions[name].epoch:
            self.node.step_down(name, epoch=epoch)
            print(f"{name}: follower {follower} is in epoch {epoch}; stepped down", flush=True)

    async def _run(self, name, follower):
        wakeup = self.wakeups[(name, follower)]
        while True:
            wakeup.clear()
            if not self.node.is_leader(name):
                self.status.pop((name, follower), None)  # report afresh if this broker leads again
                await self._sleep(wakeup)
                continue
            try:
                await self.send_once(name, follower)
            except Exception as exc:  # keep replicating even if one round fails oddly
                self._report(name, follower, f"error: {exc!r}")
            # A follower that answered but is still behind either took a batch
            # and needs the next one, or said where to resend from: send now.
            if (self.status.get((name, follower)) == "ok" and self.node.is_leader(name)
                    and self.node.is_behind(name, follower)):
                continue
            await self._sleep(wakeup)

    async def _sleep(self, wakeup):
        """Wait for the next round, or until woken."""
        try:
            await asyncio.wait_for(wakeup.wait(), self.interval)
        except asyncio.TimeoutError:
            pass

    def _report(self, name, follower, status):
        """Print a follower's status when it changes, so logs show outages."""
        if self.status.get((name, follower)) != status:
            self.status[(name, follower)] = status
            print(f"{name}: follower {follower} {status}", flush=True)
