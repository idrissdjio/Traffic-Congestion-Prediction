import os
import logging
import json
import uuid
import signal
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, KafkaError
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# ---- Configuration ----
BATCH_SIZE = 500
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'traffic_prediction_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'heartbeat.interval.ms': 2000,
    'socket.timeout.ms': 60000,
    'max.poll.interval.ms': 300000
}

# ---- Setup Logging ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create output directories
OUTPUT_DIR = Path.home() / "traffic_data"
OUTPUT_DIR.mkdir(exist_ok=True)
RAW_DIR = OUTPUT_DIR / "raw"
PROCESSED_DIR = OUTPUT_DIR / "processed"
RAW_DIR.mkdir(exist_ok=True)
PROCESSED_DIR.mkdir(exist_ok=True)

def create_new_csv_file(prefix: str, output_dir: Path) -> str:
    """Creates a new CSV filename with timestamp+random suffix."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rid = uuid.uuid4().hex[:8]
    fname = f"{prefix}_{ts}_{rid}.csv"
    file_path = output_dir / fname
    logger.info(f"Created new file: {file_path}")
    return str(file_path)

def save_to_csv(df: pd.DataFrame, csv_path: str):
    """Append a DataFrame to csv_path, writing header only if file is new."""
    try:
        df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)
    except Exception as e:
        logger.error(f"Error saving to CSV {csv_path}: {e}")
        raise

def process_batch_python(batch: list[dict], processed_csv: str):
    """
    Given a list of raw records (dicts), run:
     - Pandas -> features -> StandardScaler -> KMeans
     - Map clusters to High/Medium/Low based on avg v_Vel
     - Save enriched batch to processed_csv
    """
    try:
        df = pd.DataFrame(batch)
        
        # 1) Feature sub-DF
        feat_cols = ["v_Vel", "v_Acc", "Space_Headway", "Time_Headway"]
        X = df[feat_cols].fillna(0).to_numpy()
        
        # 2) Scale
        scaler = StandardScaler()
        Xs = scaler.fit_transform(X)
        
        # 3) Cluster
        km = KMeans(n_clusters=3, random_state=42)
        labels = km.fit_predict(Xs)
        df["Cluster_Label"] = labels
        
        # 4) Map clusters -> congestion levels
        speed_means = df.groupby("Cluster_Label")["v_Vel"].mean().sort_values()
        order = speed_means.index.tolist()
        mapping = {
            order[0]: "High",    # slowest cluster → High congestion
            order[1]: "Medium",
            order[2]: "Low"      # fastest cluster → Low congestion
        }
        df["Congestion_Level"] = df["Cluster_Label"].map(mapping)
        
        # 5) Save enriched batch
        save_to_csv(df, processed_csv)
        logger.info(f"Processed & saved {len(df)} records to {processed_csv}")
    except Exception as e:
        logger.error(f"Error in process_batch_python: {e}")
        raise

def process_message(message: dict, raw_csv: str, pending_records: list) -> bool:
    """
    - Save raw to CSV
    - Buffer into pending_records
    - If buffer ≥ BATCH_SIZE, process batch and clear buffer
    """
    try:
        # convert NaNs
        for k, v in message.items():
            if isinstance(v, float) and pd.isna(v):
                message[k] = None
        
        # raw
        save_to_csv(pd.DataFrame([message]), raw_csv)
        
        # buffer
        pending_records.append(message)
        logger.debug(f"Buffered message, {len(pending_records)}/{BATCH_SIZE}")
        
        if len(pending_records) >= BATCH_SIZE:
            logger.info("Batch threshold reached, running clustering...")
            process_batch_python(pending_records.copy(), raw_csv.replace("raw", "processed"))
            pending_records.clear()
        
        return True
    except Exception as e:
        logger.error(f"Error in process_message: {e}")
        return False

def shutdown_handler(signum, frame):
    logger.info("Graceful shutdown requested")
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Create both raw & processed CSVs
    current_raw_csv = create_new_csv_file("traffic_raw", RAW_DIR)
    current_processed_csv = create_new_csv_file("traffic_processed", PROCESSED_DIR)
    pending_records = []
    
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(["raw-traffic-data"])
    logger.info("Subscribed to topic: raw-traffic-data")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("End of partition")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
            
            try:
                record = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                continue
            
            if process_message(record, current_raw_csv, pending_records):
                consumer.commit(msg)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Process any remaining records
        if pending_records:
            try:
                process_batch_python(pending_records, current_processed_csv)
            except Exception as e:
                logger.error(f"Error processing final batch: {e}")
        
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()
