"""A real three-broker cluster that runs inside the test process.

Each broker is a real uvicorn server on a free port, in its own thread, so
tests exercise the actual HTTP and replication code. Tests can stop a broker
(it refuses connections) and start it again with an empty log, like a crashed
broker coming back. Clients can count their requests and lose replies.
"""

import socket
import threading
import time

import httpx
import pytest
import uvicorn

from broker import config
from broker.client import ClusterClient
from broker.server import create_app


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(condition, timeout=5.0, message="condition"):
    """Poll `condition` until it returns something truthy, or fail the test."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(0.01)
    pytest.fail(f"timed out after {timeout}s waiting for {message}")


class LocalCluster:
    def __init__(self, **app_options):
        self.brokers = {b: f"http://127.0.0.1:{free_port()}" for b in config.BROKERS}
        self.options = {"interval": 0.05, "ack_timeout": 0.5, **app_options}
        self.apps = {}
        self.servers = {}
        self.threads = {}
        for broker_id in self.brokers:
            self.start(broker_id)

    def start(self, broker_id):
        """Start a broker with an empty log."""
        app = create_app(broker_id, brokers=self.brokers, **self.options)
        port = int(self.brokers[broker_id].rsplit(":", 1)[1])
        server = uvicorn.Server(uvicorn.Config(
            app, host="127.0.0.1", port=port, log_level="warning", timeout_graceful_shutdown=1,
        ))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        wait_until(lambda: server.started, timeout=10, message=f"broker {broker_id} to start")
        self.apps[broker_id] = app
        self.servers[broker_id] = server
        self.threads[broker_id] = thread

    def stop(self, broker_id):
        """Stop a broker, like a crash: it stops answering and its log is gone."""
        server = self.servers.pop(broker_id)
        server.should_exit = True
        self.threads.pop(broker_id).join(timeout=10)

    def stop_all(self):
        for broker_id in list(self.servers):
            self.stop(broker_id)

    def node(self, broker_id):
        return self.apps[broker_id].state.node

    def client(self, transport=None):
        http = httpx.Client(transport=transport or httpx.HTTPTransport())
        return ClusterClient(http=http, brokers=self.brokers)

    def transport(self):
        return TestTransport(self.brokers)

    def entries(self, broker_id, partition):
        """Return a copy of every entry broker `broker_id` stores for `partition`."""
        return list(self.node(broker_id).partitions[partition].entries)


class TestTransport(httpx.HTTPTransport):
    """An HTTP transport that records requests and can lose replies.

    `lose_replies[broker]` is how many replies from that broker to drop
    after the broker has handled the request, like an ack lost on the way.
    """

    __test__ = False  # not a test class, despite the name

    def __init__(self, brokers):
        super().__init__()
        self.broker_at = {int(url.rsplit(":", 1)[1]): b for b, url in brokers.items()}
        self.requests = []  # (broker ID, method, path)
        self.lose_replies = {}

    def handle_request(self, request):
        broker_id = self.broker_at[request.url.port]
        self.requests.append((broker_id, request.method, request.url.path))
        response = super().handle_request(request)
        if self.lose_replies.get(broker_id, 0) > 0:
            self.lose_replies[broker_id] -= 1
            response.read()
            response.close()
            raise httpx.ReadTimeout("reply lost", request=request)
        return response


@pytest.fixture
def cluster():
    c = LocalCluster()
    yield c
    c.stop_all()


def make_line(n, service="login", level="INFO", ts=None):
    return {
        "msg_id": f"{service}-{n}",
        "service": service,
        "level": level,
        "text": f"line {n}",
        "ts": float(n if ts is None else ts),
    }


def send(client, line):
    """Append `line` through the cluster; returns once it is committed."""
    return client.call(line["service"], "POST", f"/partitions/{line['service']}/append", json=line)
