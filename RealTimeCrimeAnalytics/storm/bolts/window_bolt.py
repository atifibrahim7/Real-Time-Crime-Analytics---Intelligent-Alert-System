"""
window_bolt.py — WindowBolt
=============================
Maintains a configurable sliding time window per district.
Counts how many crimes have occurred in each district within
the window period and emits per-district crime counts.
"""

import logging
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger("WindowBolt")


class WindowBolt:
    """
    Bolt 3 in the topology pipeline.
    Implements a sliding count window over time per district.
    """

    def __init__(self, window_size_seconds=300, slide_interval_seconds=60):
        """
        Args:
            window_size_seconds:    Total window duration in seconds (default: 5 min).
            slide_interval_seconds: How often to slide/emit in seconds (default: 1 min).
        """
        self.window_size = window_size_seconds
        self.slide_interval = slide_interval_seconds

        # district → list of (timestamp, event_summary) tuples
        self.windows = defaultdict(list)
        self.processed = 0

    def process(self, event):
        """
        Add the event to its district's window, prune expired entries,
        and annotate the event with the current window count.

        Args:
            event: dict — a district-tagged crime event from DistrictBolt.

        Returns:
            dict — the same event with 'window_count' and 'window_seconds' added.
        """
        district = event.get("routed_district", "UNKNOWN")
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=self.window_size)

        # Record this event in the window
        self.windows[district].append({
            "timestamp": now,
            "case_number": event.get("case_number", ""),
            "primary_type": event.get("primary_type", "")
        })

        # Prune expired entries (older than the window cutoff)
        self.windows[district] = [
            entry for entry in self.windows[district]
            if entry["timestamp"] > cutoff
        ]

        window_count = len(self.windows[district])

        # Annotate the event
        event["window_count"] = window_count
        event["window_seconds"] = self.window_size
        event["window_timestamp"] = now.isoformat()

        self.processed += 1

        if self.processed % 100 == 0:
            active = {d: len(w) for d, w in self.windows.items() if len(w) > 0}
            logger.debug(f"Window snapshot — active districts: {active}")

        return event

    def get_window_snapshot(self):
        """Return current crime counts per district in the active window."""
        return {
            district: len(entries)
            for district, entries in self.windows.items()
            if len(entries) > 0
        }

    def get_stats(self):
        """Return bolt statistics."""
        return {
            "processed": self.processed,
            "window_size_seconds": self.window_size,
            "slide_interval_seconds": self.slide_interval,
            "active_districts": len([
                d for d, w in self.windows.items() if len(w) > 0
            ])
        }
