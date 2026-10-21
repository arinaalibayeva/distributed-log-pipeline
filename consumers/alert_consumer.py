"""Reads every partition and prints an ALERT for each burst of errors.

    python -m consumers.alert_consumer
"""

import time
from datetime import datetime

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


def main():
    alert_consumer = AlertConsumer(Consumer(ClusterClient()))
    print("alert consumer watching login, search, upload", flush=True)
    try:
        while True:
            for alert in alert_consumer.poll():
                print(format_alert(alert), flush=True)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
