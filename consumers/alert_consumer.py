"""Reads every partition and prints an ALERT for each burst of errors.

    python -m consumers.alert_consumer [--file data/alerts.jsonl]

Each alert is also added to a file, one JSON object per line, for the
dashboard to show.
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

from broker.client import ClusterClient
from consumers.alert import AlertDetector
from consumers.consumer import Consumer


class AlertConsumer:
    def __init__(self, consumer, detector=None):
        self.consumer = consumer
        self.detector = detector or AlertDetector()

    def poll(self):
        """Fetch new committed lines and return the alerts they raise."""
        alerts = []
        for entry in self.consumer.poll():
            alert = self.detector.process(entry["line"])
            if alert is not None:
                alerts.append(alert)
        return alerts


def format_alert(alert):
    start = datetime.fromtimestamp(alert["start"]).strftime("%H:%M:%S")
    span = alert["end"] - alert["start"]
    return f"ALERT {start}  {alert['service']}: {alert['count']} errors in {span:.1f}s"


def record_alert(path, alert):
    """Append `alert` to the file at `path`, with the time it was detected."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps({**alert, "detected": time.time()}) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Print an alert for each burst of errors.")
    parser.add_argument("--file", default="data/alerts.jsonl", help="where to record alerts for the dashboard")
    args = parser.parse_args()

    alert_consumer = AlertConsumer(Consumer(ClusterClient()))
    print("alert consumer watching login, search, upload", flush=True)
    try:
        while True:
            for alert in alert_consumer.poll():
                print(format_alert(alert), flush=True)
                record_alert(args.file, alert)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
