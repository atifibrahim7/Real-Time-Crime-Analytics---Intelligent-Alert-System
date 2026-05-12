"""
kafka_spout.py — KafkaSpout
============================
Reads JSON messages from the 'crime_events' Kafka topic.
Passes raw message bytes downstream to the ParseBolt.

Implements reconnection logic and graceful error handling
so the topology never crashes on transient Kafka issues.
"""

import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

logger = logging.getLogger("KafkaSpout")


class KafkaSpout:
    """
    Spout that continuously consumes messages from a Kafka topic
    and feeds them into the bolt pipeline.
    """

    def __init__(self, broker, topic, group_id="storm-topology-group"):
        """
        Args:
            broker:   Kafka broker address (e.g. 'kafka:9092')
            topic:    Kafka topic to subscribe to (e.g. 'crime_events')
            group_id: Kafka consumer group ID for offset tracking
        """
        self.broker = broker
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.messages_consumed = 0
        self.errors = 0

    def _connect(self):
        """Create (or recreate) the KafkaConsumer with retry-safe settings."""
        logger.info(f"Connecting to Kafka broker at {self.broker}, topic={self.topic} …")

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.broker,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=self.group_id,
            consumer_timeout_ms=-1,          # block indefinitely
            value_deserializer=lambda x: x,  # raw bytes — ParseBolt decodes
            reconnect_backoff_ms=1000,
            reconnect_backoff_max_ms=10000,
        )

        logger.info("✓ Connected to Kafka. Waiting for messages …\n")

    def open(self):
        """Initialize the spout — called once at topology startup."""
        self._connect()

    def next_tuple(self, pipeline_callback):
        """
        Main consumption loop. For each message received from Kafka,
        invoke the pipeline_callback with the raw message value.

        Args:
            pipeline_callback: function(raw_bytes) that processes the message
                               through the downstream bolt chain.
        """
        if self.consumer is None:
            self.open()

        try:
            for message in self.consumer:
                try:
                    self.messages_consumed += 1
                    pipeline_callback(message.value)

                    if self.messages_consumed % 50 == 0:
                        logger.info(
                            f"Consumed {self.messages_consumed} messages "
                            f"(errors: {self.errors})"
                        )
                except Exception as e:
                    self.errors += 1
                    logger.error(f"Error in pipeline for message: {e}")

        except NoBrokersAvailable:
            logger.error("No Kafka brokers available. Will retry on next call.")
            self.consumer = None
        except KafkaError as ke:
            logger.error(f"Kafka error: {ke}")
            self.consumer = None
        except KeyboardInterrupt:
            logger.info("KafkaSpout: Received shutdown signal (Ctrl+C).")

    def close(self):
        """Clean up the consumer connection."""
        if self.consumer:
            self.consumer.close()
            logger.info(
                f"KafkaSpout closed. Total consumed: {self.messages_consumed}, "
                f"Errors: {self.errors}"
            )

    def get_stats(self):
        """Return spout statistics."""
        return {
            "messages_consumed": self.messages_consumed,
            "errors": self.errors
        }
