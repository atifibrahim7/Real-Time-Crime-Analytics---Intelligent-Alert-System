"""
producer.py — Kafka Crime Event Simulator
==========================================
Reads data/crimes.csv row-by-row, converts each row to a JSON message,
and publishes it to the 'crime_events' Kafka topic at a configurable rate
to simulate a real-time crime data feed.

Usage (inside the Kafka or Streamlit container, or locally):
    python producer.py
    python producer.py --delay 0.5          # faster: 2 rows/sec
    python producer.py --broker kafka:9092  # explicit broker
"""

import csv
import json
import time
import sys
import argparse
import logging
from datetime import datetime

import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
def load_config(config_path="config/config.yaml"):
    """Load configuration from YAML file, with fallback defaults."""
    defaults = {
        "kafka": {
            "broker": "kafka:9092",
            "topic": "crime_events"
        },
        "producer": {
            "csv_path": "data/crimes.csv",
            "delay_seconds": 1.0
        }
    }
    try:
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)
            logger.info(f"Loaded config from {config_path}")
            return cfg
    except FileNotFoundError:
        logger.warning(f"Config file '{config_path}' not found. Using defaults.")
        return defaults


# ─────────────────────────────────────────────
# Fields to extract from each CSV row
# ─────────────────────────────────────────────
REQUIRED_FIELDS = [
    "id", "case_number", "date", "block", "primary_type",
    "description", "location_description", "arrest", "domestic",
    "beat", "district", "ward", "community_area",
    "latitude", "longitude"
]


def build_crime_event(row):
    """
    Extract required fields from a CSV row dict and return a clean
    JSON-serializable dictionary. Returns None if critical fields are missing.
    """
    event = {}
    for field in REQUIRED_FIELDS:
        value = row.get(field, "").strip() if row.get(field) else ""
        event[field] = value

    # Validate critical fields — skip row if case_number or primary_type is empty
    if not event.get("case_number") or not event.get("primary_type"):
        return None

    # Cast numeric fields safely
    for num_field in ["latitude", "longitude"]:
        try:
            event[num_field] = float(event[num_field]) if event[num_field] else None
        except (ValueError, TypeError):
            event[num_field] = None

    # Cast boolean fields
    for bool_field in ["arrest", "domestic"]:
        event[bool_field] = event[bool_field].lower() == "true" if event[bool_field] else False

    # Add ingestion metadata
    event["ingested_at"] = datetime.utcnow().isoformat()

    return event


def create_producer(broker):
    """Create and return a KafkaProducer with JSON serialization."""
    logger.info(f"Connecting to Kafka broker at {broker} …")

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        retry_backoff_ms=500
    )

    logger.info("✓ Connected to Kafka broker.")
    return producer


def run_producer(csv_path, broker, topic, delay):
    """
    Main loop: read CSV → build event → publish to Kafka.
    Gracefully handles file errors, bad rows, and Kafka failures.
    """
    producer = create_producer(broker)

    sent_count = 0
    error_count = 0
    skipped_count = 0

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            logger.info(f"Reading from: {csv_path}")
            logger.info(f"Publishing to topic: {topic}")
            logger.info(f"Delay between messages: {delay}s\n")

            for row_num, row in enumerate(reader, start=1):
                try:
                    # Build the JSON event from the CSV row
                    event = build_crime_event(row)

                    if event is None:
                        skipped_count += 1
                        continue

                    # Use district as the Kafka partition key for locality
                    key = event.get("district", "UNKNOWN")

                    # Send to Kafka
                    future = producer.send(topic, key=key, value=event)
                    future.get(timeout=10)  # Block until acknowledged

                    sent_count += 1

                    if sent_count % 50 == 0:
                        logger.info(
                            f"Sent {sent_count} messages "
                            f"(skipped: {skipped_count}, errors: {error_count})"
                        )

                    # Simulate real-time delay
                    time.sleep(delay)

                except KafkaError as ke:
                    error_count += 1
                    logger.error(f"Kafka error on row {row_num}: {ke}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing row {row_num}: {e}")

    except FileNotFoundError:
        logger.critical(f"CSV file not found: {csv_path}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Fatal error reading CSV: {e}")
        sys.exit(1)
    finally:
        producer.flush()
        producer.close()
        logger.info(
            f"\n{'='*50}\n"
            f"Producer finished.\n"
            f"  Total sent:    {sent_count}\n"
            f"  Skipped:       {skipped_count}\n"
            f"  Errors:        {error_count}\n"
            f"{'='*50}"
        )


# ─────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    config = load_config()

    parser = argparse.ArgumentParser(
        description="Kafka Crime Event Simulator — publishes CSV rows as JSON to Kafka"
    )
    parser.add_argument(
        "--csv", default=config["producer"]["csv_path"],
        help="Path to crimes.csv (default from config.yaml)"
    )
    parser.add_argument(
        "--broker", default=config["kafka"]["broker"],
        help="Kafka broker address (default from config.yaml)"
    )
    parser.add_argument(
        "--topic", default=config["kafka"]["topic"],
        help="Kafka topic name (default: crime_events)"
    )
    parser.add_argument(
        "--delay", type=float, default=config["producer"]["delay_seconds"],
        help="Seconds between messages (default: 1.0)"
    )

    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("  Chicago Crime Kafka Producer")
    logger.info("=" * 50)

    run_producer(
        csv_path=args.csv,
        broker=args.broker,
        topic=args.topic,
        delay=args.delay
    )
