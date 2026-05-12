"""
anomaly_bolt.py — AnomalyBolt
================================
Compares each district's sliding window crime count against
a configurable baseline threshold. If the count exceeds the
threshold, emits a structured anomaly tuple for AlertBolt.
"""

import logging
from datetime import datetime

logger = logging.getLogger("AnomalyBolt")


class AnomalyBolt:
    """
    Bolt 4 in the topology pipeline.
    Detects anomalous crime activity spikes per district.
    """

    def __init__(self, threshold=5):
        """
        Args:
            threshold: Number of crimes in a window that triggers an anomaly.
        """
        self.threshold = threshold
        self.anomalies_detected = 0
        self.events_checked = 0

    def process(self, event):
        """
        Check if the district's window count exceeds the anomaly threshold.

        Args:
            event: dict — a windowed crime event from WindowBolt.

        Returns:
            dict — structured anomaly alert if threshold exceeded, else None.
        """
        self.events_checked += 1
        window_count = event.get("window_count", 0)

        if window_count < self.threshold:
            return None

        # ── Threshold exceeded — generate anomaly alert ──
        self.anomalies_detected += 1

        alert = {
            "alert_id": f"ALERT-{self.anomalies_detected:06d}",
            "alert_type": "HIGH_CRIME_RATE",
            "district": event.get("routed_district", "UNKNOWN"),
            "crime_count_in_window": window_count,
            "window_seconds": event.get("window_seconds", 0),
            "threshold": self.threshold,
            "latest_crime_type": event.get("primary_type", ""),
            "latest_case_number": event.get("case_number", ""),
            "latest_block": event.get("block", ""),
            "latitude": event.get("latitude"),
            "longitude": event.get("longitude"),
            "triggered_at": datetime.utcnow().isoformat(),
            "severity": self._calculate_severity(window_count)
        }

        logger.warning(
            f"🚨 ANOMALY [{alert['severity']}] District {alert['district']}: "
            f"{window_count} crimes in {alert['window_seconds']}s window "
            f"(threshold: {self.threshold}) — "
            f"Latest: {alert['latest_crime_type']} ({alert['latest_case_number']})"
        )

        return alert

    def _calculate_severity(self, count):
        """
        Assign severity level based on how far the count exceeds threshold.

        Returns one of: CRITICAL, HIGH, MEDIUM, LOW
        """
        ratio = count / self.threshold if self.threshold > 0 else 0
        if ratio >= 3.0:
            return "CRITICAL"
        elif ratio >= 2.0:
            return "HIGH"
        elif ratio >= 1.5:
            return "MEDIUM"
        else:
            return "LOW"

    def get_stats(self):
        """Return bolt statistics."""
        return {
            "events_checked": self.events_checked,
            "anomalies_detected": self.anomalies_detected,
            "threshold": self.threshold
        }
