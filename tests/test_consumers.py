import json

from conftest import make_line
from consumers.alert_consumer import AlertConsumer, format_alert
from consumers.archive import Archive
from consumers.consumer import Consumer


def append(cluster, line):
    leader = {"login": 1, "search": 2, "upload": 3}[line["service"]]
    cluster.node(leader).append(line["service"], line)


def test_consumer_reads_each_line_once(cluster):
    consumer = Consumer(cluster.client())
    append(cluster, make_line(0))
    append(cluster, make_line(0, service="upload"))
    assert sorted(e["line"]["msg_id"] for e in consumer.poll()) == ["login-0", "upload-0"]
    assert consumer.poll() == []

    append(cluster, make_line(1))
    assert [e["line"]["msg_id"] for e in consumer.poll()] == ["login-1"]
    assert consumer.offsets == {"login": 2, "search": 0, "upload": 1}


def test_consumer_reads_in_batches(cluster):
    consumer = Consumer(cluster.client(), partitions=["login"], batch=2)
    for n in range(5):
        append(cluster, make_line(n))
    assert [len(consumer.poll()) for _ in range(4)] == [2, 2, 1, 0]


def test_consumer_skips_unreachable_partition(cluster):
    consumer = Consumer(cluster.client())
    append(cluster, make_line(0))
    append(cluster, make_line(0, service="search"))
    cluster.down.add(2)
    assert [e["line"]["msg_id"] for e in consumer.poll()] == ["login-0"]

    cluster.down.discard(2)
    assert [e["line"]["msg_id"] for e in consumer.poll()] == ["search-0"]


def test_alert_consumer_raises_alert_for_burst(cluster):
    alert_consumer = AlertConsumer(Consumer(cluster.client()))
    for n in range(3):
        append(cluster, make_line(n, level="ERROR", ts=100 + n))
    append(cluster, make_line(0, service="search", level="ERROR", ts=100))
    alerts = alert_consumer.poll()
    assert alerts == [{"service": "login", "count": 3, "start": 100.0, "end": 102.0}]
    assert alert_consumer.poll() == []


def test_format_alert():
    text = format_alert({"service": "login", "count": 3, "start": 100.0, "end": 102.5})
    assert text.startswith("ALERT ")
    assert text.endswith("login: 3 errors in 2.5s")


def test_archive_writes_one_file_per_partition(cluster, tmp_path):
    archive = Archive(cluster.client(), tmp_path)
    for n in range(3):
        append(cluster, make_line(n))
    append(cluster, make_line(0, service="upload"))
    assert archive.poll() == 4

    login = [json.loads(row) for row in archive.path("login").read_text().splitlines()]
    assert [(e["offset"], e["line"]["msg_id"]) for e in login] == [(0, "login-0"), (1, "login-1"), (2, "login-2")]
    assert len(archive.path("upload").read_text().splitlines()) == 1
    assert not archive.path("search").exists()


def test_archive_resumes_where_it_left_off(cluster, tmp_path):
    for n in range(3):
        append(cluster, make_line(n))
    Archive(cluster.client(), tmp_path).poll()

    append(cluster, make_line(3))
    restarted = Archive(cluster.client(), tmp_path)
    assert restarted.consumer.offsets["login"] == 3
    assert restarted.poll() == 1
    ids = [json.loads(row)["line"]["msg_id"] for row in restarted.path("login").read_text().splitlines()]
    assert ids == ["login-0", "login-1", "login-2", "login-3"]
