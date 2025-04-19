from confluent_kafka import Producer, KafkaError
import json
import time
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Changed to localhost for local access
    'client.id': 'traffic-data-producer',
    'acks': 'all',
    'retries': 5,
    'compression.type': 'snappy',
    'batch.size': 16384,
    'linger.ms': 5
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
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_FILE = os.path.join(BASE_DIR, "data", "raw", "traffic-data.csv")

    logger.info(f"📂 Checking for data file at: {DATA_FILE}")
    if not os.path.exists(DATA_FILE):
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

        for chunk in pd.read_csv(DATA_FILE, chunksize=chunk_size):
            for _, row in chunk.iterrows():
                if total_messages >= limit:
                    logger.info(f"✅ Reached the limit of {limit} messages. Stopping the producer.")
                    producer.flush()
                    return

                message = row.to_dict()
                batch.append(message)
                total_messages += 1

                if len(batch) >= batch_size:
                    for msg in batch:
                        try:
                            producer.produce(
                                TOPIC_NAME,
                                json.dumps(msg).encode('utf-8'),
                                callback=delivery_report
                            )
                            producer.poll(0)  # Serve delivery callbacks
                        except BufferError:
                            logger.warning('Local producer queue is full, waiting for messages to be delivered...')
                            producer.poll(0.1)
                            producer.produce(
                                TOPIC_NAME,
                                json.dumps(msg).encode('utf-8'),
                                callback=delivery_report
                            )

                    logger.info(f"📤 Sent batch of {batch_size} messages. Total sent: {total_messages}")
                    batch = []  # Clear batch after sending
                    time.sleep(0.5)  # Pause to prevent overloading Kafka

        # Send any remaining messages
        if batch:
            for msg in batch:
                try:
                    producer.produce(
                        TOPIC_NAME,
                        json.dumps(msg).encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.poll(0)
                except BufferError:
                    producer.poll(0.1)
                    producer.produce(
                        TOPIC_NAME,
                        json.dumps(msg).encode('utf-8'),
                        callback=delivery_report
                    )
            logger.info(f"📤 Sent final batch of {len(batch)} messages. Total sent: {total_messages}")

        # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
        producer.flush()
        logger.info("✅ Data streaming completed!")

    except Exception as e:
        logger.error(f"❌ Error while sending data to Kafka: {e}")
    finally:
        producer.flush()
        logger.info("✅ Kafka producer closed successfully.")

if __name__ == "__main__":
    main()
