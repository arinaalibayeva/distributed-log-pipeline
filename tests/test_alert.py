from consumers.alert import AlertDetector


def make_line(n, ts, level="ERROR", service="login"):
    return {"msg_id": f"{service}-{n}", "service": service, "level": level, "text": f"line {n}", "ts": float(ts)}


def feed(detector, lines):
    """Process every line and return the alerts that were raised."""
    alerts = [detector.process(line) for line in lines]
    return [a for a in alerts if a is not None]


def test_three_errors_within_window_raise_an_alert():
    d = AlertDetector()
    alerts = feed(d, [make_line(0, 10), make_line(1, 11), make_line(2, 12)])
    assert alerts == [{"service": "login", "count": 3, "start": 10.0, "end": 12.0}]


def test_alert_is_returned_by_the_line_that_completes_the_burst():
    d = AlertDetector()
    assert d.process(make_line(0, 10)) is None
    assert d.process(make_line(1, 11)) is None
    assert d.process(make_line(2, 12)) is not None


def test_errors_spread_over_more_than_window_do_not_alert():
    d = AlertDetector()
    assert feed(d, [make_line(0, 0), make_line(1, 3), make_line(2, 6)]) == []


def test_errors_exactly_window_apart_count_as_within():
    d = AlertDetector()
    assert len(feed(d, [make_line(0, 0), make_line(1, 2), make_line(2, 5)])) == 1


def test_info_and_warn_lines_are_ignored():
    d = AlertDetector()
    lines = [make_line(0, 10, "INFO"), make_line(1, 11, "WARN"), make_line(2, 12), make_line(3, 13)]
    assert feed(d, lines) == []


def test_services_are_counted_separately():
    d = AlertDetector()
    lines = [make_line(0, 10, service="login"), make_line(1, 11, service="search"), make_line(2, 12, service="login")]
    assert feed(d, lines) == []


def test_duplicate_msg_id_is_counted_once():
    d = AlertDetector()
    line = make_line(0, 10)
    assert feed(d, [line, line, line]) == []
    assert feed(d, [make_line(1, 11)]) == []


def test_out_of_order_lines_still_alert():
    d = AlertDetector()
    alerts = feed(d, [make_line(0, 12), make_line(1, 10), make_line(2, 11)])
    assert alerts == [{"service": "login", "count": 3, "start": 10.0, "end": 12.0}]


def test_late_line_can_complete_an_earlier_burst():
    d = AlertDetector()
    lines = [make_line(0, 10), make_line(1, 12), make_line(2, 30), make_line(3, 11)]
    alerts = feed(d, lines)
    assert alerts == [{"service": "login", "count": 3, "start": 10.0, "end": 12.0}]


def test_late_line_outside_every_window_does_not_alert():
    d = AlertDetector()
    assert feed(d, [make_line(0, 10), make_line(1, 12), make_line(2, 1)]) == []


def test_more_errors_in_the_same_burst_do_not_alert_again():
    d = AlertDetector()
    lines = [make_line(n, 10 + n * 0.5) for n in range(6)]
    assert len(feed(d, lines)) == 1


def test_a_later_burst_raises_a_new_alert():
    d = AlertDetector()
    first = [make_line(0, 10), make_line(1, 11), make_line(2, 12)]
    second = [make_line(3, 30), make_line(4, 31), make_line(5, 32)]
    alerts = feed(d, first + second)
    assert [a["start"] for a in alerts] == [10.0, 30.0]


def test_each_service_gets_its_own_alert():
    d = AlertDetector()
    lines = []
    for n in range(3):
        lines.append(make_line(n, 10 + n, service="login"))
        lines.append(make_line(n, 10 + n, service="upload"))
    assert [a["service"] for a in feed(d, lines)] == ["login", "upload"]


def test_threshold_and_window_can_be_changed():
    d = AlertDetector(threshold=2, window=1.0)
    assert feed(d, [make_line(0, 10), make_line(1, 12)]) == []
    assert len(feed(d, [make_line(2, 12.5)])) == 1


def test_late_line_from_an_old_burst_does_not_alert_again():
    d = AlertDetector()
    lines = [make_line(n, ts) for n, ts in enumerate([10, 11, 12, 30, 31, 32])]
    assert len(feed(d, lines)) == 2
    assert feed(d, [make_line(99, 11.5)]) == []


def test_line_in_two_bursts_alerts_for_the_one_not_yet_covered():
    d = AlertDetector()
    assert len(feed(d, [make_line(0, 10), make_line(1, 11), make_line(2, 12)])) == 1
    assert feed(d, [make_line(3, 19), make_line(4, 20)]) == []
    alerts = feed(d, [make_line(5, 15)])
    assert alerts == [{"service": "login", "count": 3, "start": 15.0, "end": 20.0}]
