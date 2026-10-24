"""Failure injection for one broker: losing the messages it sends to other brokers.

Every message a broker sends to another broker (replication, heartbeats,
takeover requests) goes through ChaosTransport, which can lose it:

- `blocked` brokers get nothing, like a broken network link. Cutting a broker
  off from the cluster means it blocks every other broker and every other
  broker blocks it (the dashboard and scripts/chaos.py do both sides).
- `drop_rate` is the share of messages to any broker that are lost. Half of
  the lost messages never arrive; for the other half the message arrives
  and is handled, but the reply is lost, so the sender can't tell whether
  it got through. Both must be safe: the sender just retries.

Producers and consumers are not affected; their lost acks are covered by
the tests (see TestTransport in tests/conftest.py).
"""

import random

import httpx


class Chaos:
    def __init__(self, rng=None):
        self.rng = rng or random.Random()
        self.blocked = set()
        self.drop_rate = 0.0

    def settings(self):
        return {"blocked": sorted(self.blocked), "drop_rate": self.drop_rate}

    def update(self, blocked=None, drop_rate=None):
        """Change the settings given; leave the others as they are."""
        if blocked is not None:
            self.blocked = set(blocked)
        if drop_rate is not None:
            if not 0 <= drop_rate <= 1:
                raise ValueError("drop_rate must be between 0 and 1")
            self.drop_rate = drop_rate

    def heal(self):
        self.blocked = set()
        self.drop_rate = 0.0

    def fate(self, target):
        """Decide what happens to a message for broker `target`.

        Returns "send", "drop_request" (it never arrives) or "drop_reply"
        (it arrives, but the reply is lost).
        """
        if target in self.blocked:
            return "drop_request"
        if self.drop_rate and self.rng.random() < self.drop_rate:
            return self.rng.choice(["drop_request", "drop_reply"])
        return "send"


class ChaosTransport(httpx.AsyncBaseTransport):
    """An HTTP transport to the other brokers that loses messages as `chaos` says."""

    def __init__(self, chaos, brokers, inner=None):
        self.chaos = chaos
        self.broker_at = {httpx.URL(url).port: broker_id for broker_id, url in brokers.items()}
        self.inner = inner or httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request):
        fate = self.chaos.fate(self.broker_at.get(request.url.port))
        if fate == "drop_request":
            raise httpx.ConnectError("message lost (chaos)", request=request)
        response = await self.inner.handle_async_request(request)
        if fate == "drop_reply":
            await response.aread()
            await response.aclose()
            raise httpx.ReadTimeout("reply lost (chaos)", request=request)
        return response

    async def aclose(self):
        await self.inner.aclose()
