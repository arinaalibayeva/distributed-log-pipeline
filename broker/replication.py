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
"""

import asyncio

import httpx

from broker import config


class Replicator:
    def __init__(self, node, brokers=config.BROKERS, interval=0.2, batch=500, timeout=0.5):
        self.node = node
        self.brokers = brokers
        self.interval = interval  # seconds between rounds when there's nothing new
        self.batch = batch  # most entries sent in one message
        self.timeout = timeout  # seconds to wait for a follower's reply
        self.http = None
        self.tasks = []
        self.wakeups = {}  # (partition, follower) -> asyncio.Event
        self.status = {}  # (partition, follower) -> last reported status

    def start(self):
        """Start one loop per (led partition, follower). Call from the event loop."""
        self.http = httpx.AsyncClient(timeout=self.timeout)
        for name in self.node.follower_lengths:
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

    async def wait_committed(self, name, offset, timeout):
        """Wait until `offset` of `name` is committed; False if `timeout` passes first."""
        partition = self.node.partitions[name]
        deadline = asyncio.get_running_loop().time() + timeout
        while partition.commit_index < offset:
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(0.005)
        return True

    async def send_once(self, name, follower):
        """Send `follower` what it is missing (or a heartbeat) and record its reply.

        Returns True if the follower accepted the message.
        """
        message = self.node.message_for(name, follower, self.batch)
        url = f"{self.brokers[follower]}/partitions/{name}/replicate"
        try:
            response = await self.http.post(url, json=message)
        except httpx.TransportError:
            self._report(name, follower, "unreachable")
            return False
        if response.status_code != 200:
            self._report(name, follower, f"rejected entries ({response.status_code} {response.text})")
            return False
        reply = response.json()
        self.node.record_reply(name, follower, message, reply["accepted"], reply["length"])
        self._report(name, follower, "ok")
        return reply["accepted"]

    async def _run(self, name, follower):
        wakeup = self.wakeups[(name, follower)]
        while True:
            wakeup.clear()
            try:
                await self.send_once(name, follower)
            except Exception as exc:  # keep replicating even if one round fails oddly
                self._report(name, follower, f"error: {exc!r}")
            # A follower that answered but is still behind either took a batch
            # and needs the next one, or said where to resend from: send now.
            if self.status.get((name, follower)) == "ok" and self.node.is_behind(name, follower):
                continue
            try:
                await asyncio.wait_for(wakeup.wait(), self.interval)
            except asyncio.TimeoutError:
                pass

    def _report(self, name, follower, status):
        """Print a follower's status when it changes, so logs show outages."""
        if self.status.get((name, follower)) != status:
            self.status[(name, follower)] = status
            print(f"{name}: follower {follower} {status}", flush=True)
