"""Detects bursts of errors in a stream of log lines.

The detector raises an alert when one service logs `threshold` ERROR lines
within `window` seconds (3 errors in 5 seconds by default). It uses the `ts`
written into each line, not the time the line arrived, so lines that show up
late or out of order still land in the right window. This module has no
networking code, so it can be tested on its own.
"""

from bisect import bisect_left, bisect_right, insort


class AlertDetector:
    def __init__(self, threshold=3, window=5.0):
        self.threshold = threshold
        self.window = window
        self.seen = set()  # msg_ids of ERROR lines already counted
        self.errors = {}  # service -> sorted list of ERROR timestamps
        self.alerts = {}  # service -> sorted list of end times of its alerts

    def process(self, line):
        """Count one log line and return an alert if it completes a burst.

        Returns None for non-ERROR lines, for lines already counted (same
        msg_id), and for bursts that an earlier alert already covers. A
        service gets at most one alert per `window` seconds, so a fourth error
        in the same burst doesn't alert again, and neither does a late error
        that belongs to a burst that already alerted.
        """
        if line["level"] != "ERROR" or line["msg_id"] in self.seen:
            return None
        self.seen.add(line["msg_id"])

        service = line["service"]
        times = self.errors.setdefault(service, [])
        insort(times, line["ts"])
        alerted = self.alerts.setdefault(service, [])

        for start, end, count in self._bursts_including(times, line["ts"]):
            if not self._near_alert(alerted, end):
                insort(alerted, end)
                return {"service": service, "count": count, "start": start, "end": end}
        return None

    def _bursts_including(self, times, ts):
        """Yield (start, end, count) for every burst that includes `ts`.

        Every window that contains `ts` starts at a stored timestamp between
        ts - window and ts, so only those starts need checking.
        """
        first = bisect_left(times, ts - self.window)
        last = bisect_right(times, ts)
        for i in range(first, last):
            end_index = bisect_right(times, times[i] + self.window)
            count = end_index - i
            if count >= self.threshold:
                yield times[i], times[end_index - 1], count

    def _near_alert(self, alerted, end):
        """Return True if an earlier alert ended within `window` of `end`."""
        i = bisect_left(alerted, end - self.window)
        return i < len(alerted) and alerted[i] <= end + self.window
