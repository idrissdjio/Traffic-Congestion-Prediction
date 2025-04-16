from kafka import KafkaProducer, errors
import json
import time
import pandas as pd
import os

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "raw-traffic-data"

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        max_block_ms=60000,  # Increase max block time to avoid metadata timeout
        buffer_memory=33554432,  # Increase buffer memory (32MB)
        batch_size=16384,  # Increase batch size
    )
    print("✅ Kafka producer connected successfully.")
except errors.NoBrokersAvailable:
    print("❌ Error: No Kafka brokers available. Ensure Kafka is running and accessible.")
    exit(1)

# Load Raw Traffic Data in Chunks
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_FILE = os.path.join(BASE_DIR, "data", "raw", "traffic-data.csv")

print(f"📂 Checking for data file at: {DATA_FILE}")
if not os.path.exists(DATA_FILE):
    print(f"❌ Error: Data file not found at {DATA_FILE}. Please check the file path.")
    print("ℹ️ Hint: Place the file in the correct directory or update the path.")
    exit(1)

print(f"🚀 Streaming data to Kafka topic: {TOPIC_NAME}")
try:
    # Read the CSV file in chunks
    chunk_size = 1000  # Number of rows per chunk
    batch = []
    batch_size = 500  # Send in batches of 500 rows
    total_messages = 0  # Counter for total messages sent
    limit = 100000  # Increase the limit to 100,000 messages

    for chunk in pd.read_csv(DATA_FILE, chunksize=chunk_size):
        for _, row in chunk.iterrows():
            if total_messages >= limit:
                print(f"✅ Reached the limit of {limit} messages. Stopping the producer.")
                producer.flush()
                producer.close()
                exit(0)

            message = row.to_dict()
            batch.append(message)
            total_messages += 1

            if len(batch) >= batch_size:
                for msg in batch:  # ✅ Send each message individually
                    producer.send(TOPIC_NAME, value=msg).get(timeout=30)

                print(f"📤 Sent batch of {batch_size} messages. Total sent: {total_messages}")
                batch = []  # Clear batch after sending
                time.sleep(0.5)  # Pause to prevent overloading Kafka

    # Send any remaining messages
    if batch:
        for msg in batch:
            producer.send(TOPIC_NAME, value=msg).get(timeout=30)
        print(f"📤 Sent final batch of {len(batch)} messages. Total sent: {total_messages}")

    print("✅ Data streaming completed!")

except Exception as e:
    print(f"❌ Error while sending data to Kafka: {e}")
    print("ℹ️ Hint: Ensure the topic exists and Kafka is running.")
    exit(1)

producer.flush()
producer.close()
print("✅ Kafka producer closed successfully.")