"""
parse_bolt.py — ParseBolt
==========================
Deserializes raw JSON bytes into Python dicts and validates that
all required fields are present. Malformed or incomplete messages
are logged and discarded — never forwarded downstream.
"""

import json
import logging

logger = logging.getLogger("ParseBolt")

# Fields that MUST be present for a valid crime event
REQUIRED_FIELDS = ["case_number", "primary_type", "district"]

# All fields we attempt to extract from each message
EXTRACT_FIELDS = [
    "id", "case_number", "date", "block", "primary_type",
    "description", "location_description", "arrest", "domestic",
    "beat", "district", "ward", "community_area",
    "latitude", "longitude", "ingested_at"
]


class ParseBolt:
    """
    Bolt 1 in the topology pipeline.
    Deserializes JSON and validates required fields.
    """

    def __init__(self):
        self.parsed = 0
        self.malformed = 0
        self.missing_fields = 0

    def process(self, raw_message):
        """
        Parse a raw Kafka message (bytes or str) into a validated dict.

        Args:
            raw_message: Raw bytes or string from KafkaSpout.

        Returns:
            dict — a validated crime event, or
            None — if the message is malformed / missing required fields.
        """
        # --- Step 1: Decode bytes → str ---
        try:
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")
        except UnicodeDecodeError as e:
            self.malformed += 1
            logger.warning(f"Cannot decode message bytes: {e}")
            return None

        # --- Step 2: Deserialize JSON ---
        try:
            data = json.loads(raw_message)
        except (json.JSONDecodeError, ValueError) as e:
            self.malformed += 1
            logger.warning(f"Malformed JSON discarded: {e}")
            return None

        # --- Step 3: Validate required fields ---
        for field in REQUIRED_FIELDS:
            value = data.get(field)
            if value is None or (isinstance(value, str) and value.strip() == ""):
                self.missing_fields += 1
                logger.debug(
                    f"Discarded event — missing required field '{field}': "
                    f"case={data.get('case_number', '?')}"
                )
                return None

        # --- Step 4: Extract and normalize fields ---
        event = {}
        for field in EXTRACT_FIELDS:
            event[field] = data.get(field, "")

        # Cast numeric fields safely
        for num_field in ["latitude", "longitude"]:
            try:
                val = event[num_field]
                event[num_field] = float(val) if val not in (None, "") else None
            except (ValueError, TypeError):
                event[num_field] = None

        # Cast boolean fields
        for bool_field in ["arrest", "domestic"]:
            val = event[bool_field]
            if isinstance(val, bool):
                pass  # already boolean
            elif isinstance(val, str):
                event[bool_field] = val.lower() == "true"
            else:
                event[bool_field] = False

        self.parsed += 1
        return event

    def get_stats(self):
        """Return bolt statistics."""
        return {
            "parsed": self.parsed,
            "malformed": self.malformed,
            "missing_fields": self.missing_fields
        }
