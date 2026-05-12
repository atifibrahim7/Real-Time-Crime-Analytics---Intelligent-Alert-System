"""
district_bolt.py — DistrictBolt
=================================
Groups incoming crime event tuples by their police district
and emits district-tagged tuples downstream for windowed counting.
"""

import logging
from collections import defaultdict

logger = logging.getLogger("DistrictBolt")


class DistrictBolt:
    """
    Bolt 2 in the topology pipeline.
    Routes and groups crimes by their police district field.
    """

    def __init__(self):
        self.district_counts = defaultdict(int)
        self.processed = 0
        self.unknown_district = 0

    def process(self, event):
        """
        Extract and normalize the district field from the event.
        Tags the event with a 'routed_district' key for downstream use.

        Args:
            event: dict — a validated crime event from ParseBolt.

        Returns:
            dict — the same event with 'routed_district' added.
        """
        district = event.get("district", "")

        # Normalize: strip whitespace, default to UNKNOWN
        if not district or (isinstance(district, str) and district.strip() == ""):
            district = "UNKNOWN"
            self.unknown_district += 1
        else:
            district = str(district).strip()

        event["routed_district"] = district
        self.district_counts[district] += 1
        self.processed += 1

        return event

    def get_district_summary(self):
        """Return a snapshot of crime counts per district."""
        return dict(self.district_counts)

    def get_stats(self):
        """Return bolt statistics."""
        return {
            "processed": self.processed,
            "unknown_district": self.unknown_district,
            "active_districts": len(self.district_counts)
        }
