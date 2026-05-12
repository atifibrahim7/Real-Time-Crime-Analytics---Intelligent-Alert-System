"""
crime_topology.py — Main Storm Topology Wiring
================================================
Wires together the complete multi-bolt pipeline:

    KafkaSpout → ParseBolt → DistrictBolt → WindowBolt → AnomalyBolt → AlertBolt

All configuration is externalized to config/config.yaml.

Usage (inside a container on the Docker network):
    python -m storm.topology.crime_topology

Or as a standalone script:
    python crime_topology.py
"""

import sys
import os
import logging
import signal

import yaml

# Add project root to path so we can import spouts/bolts
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spouts.kafka_spout import KafkaSpout
from bolts.parse_bolt import ParseBolt
from bolts.district_bolt import DistrictBolt
from bolts.window_bolt import WindowBolt
from bolts.anomaly_bolt import AnomalyBolt
from bolts.alert_bolt import AlertBolt

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("CrimeTopology")


# ─────────────────────────────────────────────
# Configuration Loader
# ─────────────────────────────────────────────
def load_config(config_path=None):
    """
    Load topology configuration from a YAML file.
    Searches multiple paths for flexibility across environments.
    """
    search_paths = [
        config_path,
        "config/config.yaml",
        "/app/config/config.yaml",
        "../config/config.yaml",
        os.path.join(os.path.dirname(__file__), "../../config/config.yaml"),
    ]

    for path in search_paths:
        if path and os.path.exists(path):
            with open(path, "r") as f:
                cfg = yaml.safe_load(f)
                logger.info(f"✓ Loaded config from: {path}")
                return cfg

    # Fallback defaults
    logger.warning("Config file not found — using built-in defaults.")
    return {
        "kafka": {"broker": "kafka:9092", "topic": "crime_events"},
        "storm": {
            "anomaly_threshold": 5,
            "window_seconds": 300,
            "slide_interval_seconds": 60
        },
        "postgresql": {
            "host": "postgres", "port": 5432,
            "database": "crime_db", "user": "admin", "password": "admin"
        },
        "mongodb": {
            "host": "mongodb", "port": 27017, "database": "crime_db"
        }
    }


# ─────────────────────────────────────────────
# Topology Definition
# ─────────────────────────────────────────────
class CrimeTopology:
    """
    Orchestrates the complete Storm-style topology.

    Pipeline:
        KafkaSpout → ParseBolt → DistrictBolt → WindowBolt → AnomalyBolt → AlertBolt
    """

    def __init__(self, config):
        self.config = config
        kafka_cfg = config["kafka"]
        storm_cfg = config["storm"]
        pg_cfg = config["postgresql"]
        mongo_cfg = config["mongodb"]

        # ── Initialize Spout ──
        self.spout = KafkaSpout(
            broker=kafka_cfg["broker"],
            topic=kafka_cfg["topic"]
        )

        # ── Initialize Bolts ──
        self.parse_bolt = ParseBolt()

        self.district_bolt = DistrictBolt()

        self.window_bolt = WindowBolt(
            window_size_seconds=storm_cfg.get("window_seconds", 300),
            slide_interval_seconds=storm_cfg.get("slide_interval_seconds", 60)
        )

        self.anomaly_bolt = AnomalyBolt(
            threshold=storm_cfg.get("anomaly_threshold", 5)
        )

        self.alert_bolt = AlertBolt(
            pg_config=pg_cfg,
            mongo_config=mongo_cfg
        )

        self._running = True

    def _pipeline(self, raw_message):
        """
        Process a single message through the entire bolt chain.
        Each bolt receives the output of the previous bolt.
        If any bolt returns None, the message is discarded.
        """
        # Bolt 1: Parse & Validate
        event = self.parse_bolt.process(raw_message)
        if event is None:
            return  # malformed — discarded

        # Bolt 2: Route by District
        event = self.district_bolt.process(event)

        # Bolt 3: Sliding Window Count
        event = self.window_bolt.process(event)

        # Bolt 4: Anomaly Detection
        alert = self.anomaly_bolt.process(event)

        # Bolt 5: Persist Alert (only if anomaly was detected)
        if alert is not None:
            self.alert_bolt.process(alert)

    def submit(self):
        """Start the topology — blocks until interrupted."""
        storm_cfg = self.config["storm"]
        kafka_cfg = self.config["kafka"]

        logger.info("=" * 65)
        logger.info("  🌪️  Storm Topology — Real-Time Crime Anomaly Detection")
        logger.info("=" * 65)
        logger.info(f"  Kafka Broker:        {kafka_cfg['broker']}")
        logger.info(f"  Kafka Topic:         {kafka_cfg['topic']}")
        logger.info(f"  Window Size:         {storm_cfg.get('window_seconds', 300)}s")
        logger.info(f"  Slide Interval:      {storm_cfg.get('slide_interval_seconds', 60)}s")
        logger.info(f"  Anomaly Threshold:   {storm_cfg.get('anomaly_threshold', 5)} crimes")
        logger.info("=" * 65)
        logger.info("")
        logger.info("  Pipeline: KafkaSpout → ParseBolt → DistrictBolt")
        logger.info("            → WindowBolt → AnomalyBolt → AlertBolt")
        logger.info("")
        logger.info("=" * 65 + "\n")

        # Register graceful shutdown handler
        def shutdown_handler(signum, frame):
            logger.info("\nReceived shutdown signal. Shutting down gracefully …")
            self._running = False

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        try:
            self.spout.next_tuple(self._pipeline)
        finally:
            self._shutdown()

    def _shutdown(self):
        """Print final stats and close all connections."""
        self.spout.close()
        self.alert_bolt.close()

        spout_stats = self.spout.get_stats()
        parse_stats = self.parse_bolt.get_stats()
        district_stats = self.district_bolt.get_stats()
        window_stats = self.window_bolt.get_stats()
        anomaly_stats = self.anomaly_bolt.get_stats()
        alert_stats = self.alert_bolt.get_stats()

        logger.info("\n" + "=" * 65)
        logger.info("  Topology Shutdown — Final Statistics")
        logger.info("=" * 65)
        logger.info(f"  KafkaSpout:    {spout_stats['messages_consumed']} consumed, "
                     f"{spout_stats['errors']} errors")
        logger.info(f"  ParseBolt:     {parse_stats['parsed']} parsed, "
                     f"{parse_stats['malformed']} malformed, "
                     f"{parse_stats['missing_fields']} missing fields")
        logger.info(f"  DistrictBolt:  {district_stats['processed']} routed, "
                     f"{district_stats['unknown_district']} unknown, "
                     f"{district_stats['active_districts']} districts")
        logger.info(f"  WindowBolt:    {window_stats['processed']} windowed, "
                     f"{window_stats['active_districts']} active districts")
        logger.info(f"  AnomalyBolt:   {anomaly_stats['events_checked']} checked, "
                     f"{anomaly_stats['anomalies_detected']} anomalies")
        logger.info(f"  AlertBolt:     {alert_stats['alerts_written']} alerts written, "
                     f"{alert_stats['pg_errors']} PG errors, "
                     f"{alert_stats['mongo_errors']} Mongo errors")
        logger.info("=" * 65 + "\n")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    config = load_config()
    topology = CrimeTopology(config)
    topology.submit()
