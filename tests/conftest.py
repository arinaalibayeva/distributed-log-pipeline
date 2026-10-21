"""A fake three-broker cluster that runs inside the test process.

Requests that a ClusterClient sends to a broker's URL are routed straight to
that broker's FastAPI app, so tests exercise the real server code without
opening ports. Tests can also take a broker down or lose a reply.
"""

import httpx
import pytest
from fastapi.testclient import TestClient

from broker import config
from broker.client import ClusterClient
from broker.server import create_app


class FakeCluster:
    def __init__(self):
        self.apps = {broker_id: create_app(broker_id) for broker_id in config.BROKERS}
        self.servers = {config.port_of(b): TestClient(app) for b, app in self.apps.items()}
        self.down = set()  # broker IDs that refuse connections
        self.lose_replies = {}  # broker ID -> how many replies to drop after handling
        self.requests = []  # (broker ID, method, path) of every request that arrived

    def node(self, broker_id):
        return self.apps[broker_id].state.node

    def client(self, **kwargs):
        http = httpx.Client(transport=httpx.MockTransport(self._handle))
        return ClusterClient(http=http, **kwargs)

    def _handle(self, request):
        broker_id = next(b for b in config.BROKERS if config.port_of(b) == request.url.port)
        if broker_id in self.down:
            raise httpx.ConnectError("broker is down", request=request)
        self.requests.append((broker_id, request.method, request.url.path))
        reply = self.servers[request.url.port].request(
            request.method, request.url.path, params=request.url.params,
            content=request.content, headers={"content-type": request.headers.get("content-type", "")},
        )
        if self.lose_replies.get(broker_id, 0) > 0:
            self.lose_replies[broker_id] -= 1
            raise httpx.ReadTimeout("reply lost", request=request)
        return httpx.Response(reply.status_code, content=reply.content, headers={"content-type": "application/json"})


@pytest.fixture
def cluster():
    return FakeCluster()


def make_line(n, service="login", level="INFO", ts=None):
    return {
        "msg_id": f"{service}-{n}",
        "service": service,
        "level": level,
        "text": f"line {n}",
        "ts": float(n if ts is None else ts),
    }
