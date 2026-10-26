import json

import pytest

from conftest import make_line, send
from consumers.alert_consumer import AlertConsumer, format_alert, record_alert
from consumers.archive import Archive
from consumers.consumer import Consumer


@pytest.fixture
def writer(cluster):
    """Sends lines through the cluster; each call returns once committed."""
    client = cluster.client()
    return lambda line: send(client, line)


def test_consumer_reads_each_line_once(cluster, writer):
    consumer = Consumer(cluster.client())
    writer(make_line(0))
    writer(make_line(0, service="upload"))
    assert sorted(e["line"]["msg_id"] for e in consumer.poll()) == ["login-0", "upload-0"]
    assert consumer.poll() == []

    writer(make_line(1))
    assert [e["line"]["msg_id"] for e in consumer.poll()] == ["login-1"]
    assert consumer.offsets == {"login": 2, "search": 0, "upload": 1}


def test_consumer_never_sees_uncommitted_lines(cluster, writer):
    consumer = Consumer(cluster.client())
    writer(make_line(0))
    cluster.stop(2)
    cluster.stop(3)
    cluster.node(1).append("login", make_line(1))  # stored on the leader only
    assert [e["line"]["msg_id"] for e in consumer.poll()] == ["login-0"]
    assert consumer.poll() == []


def test_consumer_reads_in_batches(cluster, writer):
    consumer = Consumer(cluster.client(), partitions=["login"], batch=2)
    for n in range(5):
        writer(make_line(n))
    assert [len(consumer.poll()) for _ in range(4)] == [2, 2, 1, 0]


def test_consumer_skips_unreachable_partition(cluster, writer):
    consumer = Consumer(cluster.client())
    writer(make_line(0))
    writer(make_line(0, service="search"))
    cluster.stop(2)
    assert [e["line"]["msg_id"] for e in consumer.poll()] == ["login-0"]


def test_alert_consumer_raises_alert_for_burst(cluster, writer):
    alert_consumer = AlertConsumer(Consumer(cluster.client()))
    for n in range(3):
        writer(make_line(n, level="ERROR", ts=100 + n))
    writer(make_line(0, service="search", level="ERROR", ts=100))
    alerts = alert_consumer.poll()
    assert alerts == [{"service": "login", "count": 3, "start": 100.0, "end": 102.0}]
    assert alert_consumer.poll() == []


def test_format_alert():
    text = format_alert({"service": "login", "count": 3, "start": 100.0, "end": 102.5})
    assert text.startswith("ALERT ")
    assert text.endswith("login: 3 errors in 2.5s")


def test_archive_writes_one_file_per_partition(cluster, writer, tmp_path):
    archive = Archive(cluster.client(), tmp_path)
    for n in range(3):
        writer(make_line(n))
    writer(make_line(0, service="upload"))
    assert archive.poll() == 4

    login = [json.loads(row) for row in archive.path("login").read_text().splitlines()]
    assert [(e["offset"], e["line"]["msg_id"]) for e in login] == [(0, "login-0"), (1, "login-1"), (2, "login-2")]
    assert len(archive.path("upload").read_text().splitlines()) == 1
    assert not archive.path("search").exists()


def test_archive_resumes_where_it_left_off(cluster, writer, tmp_path):
    for n in range(3):
        writer(make_line(n))
    Archive(cluster.client(), tmp_path).poll()

    writer(make_line(3))
    restarted = Archive(cluster.client(), tmp_path)
    assert restarted.consumer.offsets["login"] == 3
    assert restarted.poll() == 1
    ids = [json.loads(row)["line"]["msg_id"] for row in restarted.path("login").read_text().splitlines()]
    assert ids == ["login-0", "login-1", "login-2", "login-3"]


def test_alerts_are_recorded_for_the_dashboard(tmp_path):
    path = tmp_path / "alerts.jsonl"
    record_alert(path, {"service": "login", "count": 3, "start": 100.0, "end": 102.0})
    record_alert(path, {"service": "upload", "count": 4, "start": 200.0, "end": 201.0})
    alerts = [json.loads(line) for line in path.read_text().splitlines()]
    assert [(a["service"], a["count"]) for a in alerts] == [("login", 3), ("upload", 4)]
    assert all("detected" in a for a in alerts)
