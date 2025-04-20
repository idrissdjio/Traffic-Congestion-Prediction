from confluent_kafka import Producer, KafkaError
import json
import time
import pandas as pd
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'traffic-data-producer',
    'acks': 'all',
    'retries': 5,
    'compression.type': 'snappy',
    'batch.size': 16384,
    'linger.ms': 5,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 1000,
    'socket.timeout.ms': 60000,
    'message.timeout.ms': 300000
}

TOPIC_NAME = "raw-traffic-data"

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def main():
    # Initialize Kafka Producer
    try:
        producer = Producer(KAFKA_CONFIG)
        logger.info("✅ Kafka producer connected successfully.")
    except Exception as e:
        logger.error(f"❌ Error: Failed to create Kafka producer: {e}")
        return

    # Load Raw Traffic Data in Chunks
    BASE_DIR = Path(__file__).parent.parent.parent
    DATA_FILE = BASE_DIR / "data" / "raw" / "traffic-data.csv"

    logger.info(f"📂 Checking for data file at: {DATA_FILE}")
    if not DATA_FILE.exists():
        logger.error(f"❌ Error: Data file not found at {DATA_FILE}")
        return

    logger.info(f"🚀 Streaming data to Kafka topic: {TOPIC_NAME}")
    try:
        # Read the CSV file in chunks
        chunk_size = 1000  # Number of rows per chunk
        batch = []
        batch_size = 500  # Send in batches of 500 rows
        total_messages = 0  # Counter for total messages sent
        limit = 10000  # Limit of messages to send
        messages_in_queue = 0

        for chunk in pd.read_csv(DATA_FILE, chunksize=chunk_size):
            for _, row in chunk.iterrows():
                if total_messages >= limit:
                    logger.info(f"✅ Reached the limit of {limit} messages. Stopping the producer.")
                    producer.flush()
                    return

                message = row.to_dict()
                batch.append(message)
                total_messages += 1
                messages_in_queue += 1

                if len(batch) >= batch_size:
                    for msg in batch:
                        try:
                            producer.produce(
                                TOPIC_NAME,
                                json.dumps(msg).encode('utf-8'),
                                callback=delivery_report
                            )
                            messages_in_queue -= 1
                        except BufferError:
                            logger.warning('Local producer queue is full, waiting for messages to be delivered...')
                            producer.poll(0.1)  # Wait for some messages to be delivered
                            try:
                                producer.produce(
                                    TOPIC_NAME,
                                    json.dumps(msg).encode('utf-8'),
                                    callback=delivery_report
                                )
                                messages_in_queue -= 1
                            except BufferError:
                                logger.error('Failed to produce message after retry')
                                continue

                    # Poll for delivery reports
                    producer.poll(0)
                    logger.info(f"📤 Sent batch of {batch_size} messages. Total sent: {total_messages}")
                    batch = []  # Clear batch after sending
                    time.sleep(0.1)  # Small pause to prevent overloading Kafka

        # Send any remaining messages
        if batch:
            for msg in batch:
                try:
                    producer.produce(
                        TOPIC_NAME,
                        json.dumps(msg).encode('utf-8'),
                        callback=delivery_report
                    )
                    messages_in_queue -= 1
                except BufferError:
                    producer.poll(0.1)
                    try:
                        producer.produce(
                            TOPIC_NAME,
                            json.dumps(msg).encode('utf-8'),
                            callback=delivery_report
                        )
                        messages_in_queue -= 1
                    except BufferError:
                        logger.error('Failed to produce final message after retry')
                        continue

            # Poll for delivery reports
            producer.poll(0)
            logger.info(f"📤 Sent final batch of {len(batch)} messages. Total sent: {total_messages}")

        # Wait for any outstanding messages to be delivered
        while messages_in_queue > 0:
            producer.poll(0.1)
            time.sleep(0.1)

        producer.flush()
        logger.info("✅ Data streaming completed!")

    except Exception as e:
        logger.error(f"❌ Error while sending data to Kafka: {e}")
    finally:
        producer.flush()
        logger.info("✅ Kafka producer closed successfully.")

if __name__ == "__main__":
    main()
