"""
alert_bolt.py — AlertBolt
===========================
Consumes anomaly tuples from AnomalyBolt and writes structured
alert records to both MongoDB (alert_logs collection) and
PostgreSQL (speed_layer_alerts table).

Implements connection retry logic so that transient database
failures do not crash the topology.
"""

import logging
import time

import psycopg2
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

logger = logging.getLogger("AlertBolt")

# Max retries for database reconnection
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


class AlertBolt:
    """
    Bolt 5 (terminal bolt) in the topology pipeline.
    Persists anomaly alerts to MongoDB and PostgreSQL.
    """

    def __init__(self, pg_config, mongo_config):
        """
        Args:
            pg_config:    dict with keys: host, port, database, user, password
            mongo_config: dict with keys: host, port, database
        """
        self.pg_config = pg_config
        self.mongo_config = mongo_config
        self.alerts_written = 0
        self.pg_errors = 0
        self.mongo_errors = 0

        # Initialize connections
        self.pg_conn = None
        self.mongo_collection = None
        self.mongo_client = None

        self._init_postgres()
        self._init_mongodb()

    # ── PostgreSQL ──────────────────────────────

    def _init_postgres(self):
        """
        Create the speed_layer_alerts table if it doesn't exist
        and establish a persistent connection.
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.pg_conn = psycopg2.connect(
                    host=self.pg_config["host"],
                    port=self.pg_config["port"],
                    dbname=self.pg_config["database"],
                    user=self.pg_config["user"],
                    password=self.pg_config["password"]
                )
                self.pg_conn.autocommit = True

                cursor = self.pg_conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS speed_layer_alerts (
                        id              SERIAL PRIMARY KEY,
                        alert_id        VARCHAR(50),
                        alert_type      VARCHAR(50),
                        district        VARCHAR(20),
                        crime_count_in_window INTEGER,
                        window_seconds  INTEGER,
                        threshold       INTEGER,
                        latest_crime_type    VARCHAR(100),
                        latest_case_number   VARCHAR(50),
                        latest_block         VARCHAR(200),
                        latitude        DOUBLE PRECISION,
                        longitude       DOUBLE PRECISION,
                        triggered_at    TIMESTAMP,
                        severity        VARCHAR(20),
                        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cursor.close()
                logger.info("✓ PostgreSQL connection established.")
                return

            except psycopg2.OperationalError as e:
                logger.warning(
                    f"PostgreSQL connection attempt {attempt}/{MAX_RETRIES} failed: {e}"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_SECONDS)

        logger.error("✗ Could not connect to PostgreSQL after retries.")
        self.pg_conn = None

    def _write_to_postgres(self, alert):
        """Insert a single alert record into PostgreSQL."""
        if self.pg_conn is None:
            self._init_postgres()
            if self.pg_conn is None:
                self.pg_errors += 1
                return

        try:
            cursor = self.pg_conn.cursor()
            cursor.execute("""
                INSERT INTO speed_layer_alerts
                    (alert_id, alert_type, district, crime_count_in_window,
                     window_seconds, threshold, latest_crime_type,
                     latest_case_number, latest_block, latitude, longitude,
                     triggered_at, severity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                alert["alert_id"],
                alert["alert_type"],
                alert["district"],
                alert["crime_count_in_window"],
                alert["window_seconds"],
                alert["threshold"],
                alert["latest_crime_type"],
                alert["latest_case_number"],
                alert["latest_block"],
                alert["latitude"],
                alert["longitude"],
                alert["triggered_at"],
                alert["severity"]
            ))
            cursor.close()

        except psycopg2.OperationalError as e:
            self.pg_errors += 1
            logger.error(f"PostgreSQL write failed (will reconnect): {e}")
            self.pg_conn = None  # force reconnection on next write

        except Exception as e:
            self.pg_errors += 1
            logger.error(f"PostgreSQL write error: {e}")

    # ── MongoDB ─────────────────────────────────

    def _init_mongodb(self):
        """Connect to MongoDB and get the alert_logs collection."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.mongo_client = MongoClient(
                    host=self.mongo_config["host"],
                    port=self.mongo_config["port"],
                    serverSelectionTimeoutMS=5000
                )
                # Force a connection check
                self.mongo_client.admin.command("ping")

                db = self.mongo_client[self.mongo_config["database"]]
                self.mongo_collection = db["alert_logs"]
                logger.info("✓ MongoDB connection established.")
                return

            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                logger.warning(
                    f"MongoDB connection attempt {attempt}/{MAX_RETRIES} failed: {e}"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_SECONDS)

        logger.error("✗ Could not connect to MongoDB after retries.")
        self.mongo_client = None
        self.mongo_collection = None

    def _write_to_mongodb(self, alert):
        """Insert a single alert document into MongoDB."""
        if self.mongo_collection is None:
            self._init_mongodb()
            if self.mongo_collection is None:
                self.mongo_errors += 1
                return

        try:
            # MongoDB modifies dicts in-place (_id), so copy
            self.mongo_collection.insert_one(alert.copy())

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            self.mongo_errors += 1
            logger.error(f"MongoDB write failed (will reconnect): {e}")
            self.mongo_collection = None  # force reconnection

        except Exception as e:
            self.mongo_errors += 1
            logger.error(f"MongoDB write error: {e}")

    # ── Public Interface ────────────────────────

    def process(self, alert):
        """
        Write an anomaly alert to both MongoDB and PostgreSQL.

        Args:
            alert: dict — structured anomaly alert from AnomalyBolt.
                   If None, this is a no-op.
        """
        if alert is None:
            return

        self._write_to_postgres(alert)
        self._write_to_mongodb(alert)
        self.alerts_written += 1

    def close(self):
        """Gracefully close all database connections."""
        if self.pg_conn:
            try:
                self.pg_conn.close()
                logger.info("PostgreSQL connection closed.")
            except Exception:
                pass

        if self.mongo_client:
            try:
                self.mongo_client.close()
                logger.info("MongoDB connection closed.")
            except Exception:
                pass

    def get_stats(self):
        """Return bolt statistics."""
        return {
            "alerts_written": self.alerts_written,
            "pg_errors": self.pg_errors,
            "mongo_errors": self.mongo_errors
        }
