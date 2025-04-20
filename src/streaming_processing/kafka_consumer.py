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
from sklearn.ensemble import RandomForestClassifier

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

# ---- Logging ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---- Output Dirs ----
OUTPUT_DIR    = Path.home() / "traffic_data"
RAW_DIR       = OUTPUT_DIR / "raw"
PROCESSED_DIR = OUTPUT_DIR / "processed"
for d in (RAW_DIR, PROCESSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

def create_new_csv_file(prefix: str, output_dir: Path) -> str:
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    rid  = uuid.uuid4().hex[:8]
    name = f"{prefix}_{ts}_{rid}.csv"
    path = output_dir / name
    logger.info(f"→ New file: {path}")
    return str(path)

def save_to_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, mode='a', header=not os.path.exists(path), index=False)

def process_batch_python(batch: list[dict],
                         processed_csv: str,
                         weights_csv: str):
    """
    1) KMeans → df['Cluster_Label'], df['Congestion_Level']
    2) RF ← predict Cluster_Label from features
    3) dump enriched df + feature_importances
    """
    try:
        df = pd.DataFrame(batch)
        feat_cols = ["v_Vel", "v_Acc", "Space_Headway", "Time_Headway"]

        # Prepare feature matrix
        X = df[feat_cols].fillna(0).to_numpy()
        Xs = StandardScaler().fit_transform(X)

        # 1) Cluster
        km = KMeans(n_clusters=3, random_state=42)
        labels = km.fit_predict(Xs)
        df["Cluster_Label"] = labels

        # Map to congestion levels
        speed_means = df.groupby("Cluster_Label")["v_Vel"].mean().sort_values()
        order = speed_means.index.tolist()
        mapping = { order[0]: "High", order[1]: "Medium", order[2]: "Low" }
        df["Congestion_Level"] = df["Cluster_Label"].map(mapping)

        # Save enriched batch
        save_to_csv(df, processed_csv)
        logger.info(f"→ Saved {len(df)} records to {processed_csv}")

        # 2) RF on cluster labels
        rf = RandomForestClassifier(n_estimators=100, random_state=42)
        rf.fit(Xs, labels)

        # 3) Feature importances
        imp_df = pd.DataFrame({
            "feature": feat_cols,
            "importance": rf.feature_importances_
        })
        save_to_csv(imp_df, weights_csv)
        logger.info(f"→ Saved feature importances to {weights_csv}")

    except Exception as e:
        logger.error(f"process_batch_python error: {e}")
        raise

def process_message(msg: dict,
                    raw_csv: str,
                    processed_csv: str,
                    weights_csv: str,
                    buffer: list):
    try:
        # normalize NaNs
        for k, v in msg.items():
            if isinstance(v, float) and pd.isna(v):
                msg[k] = None

        # 1) save raw
        save_to_csv(pd.DataFrame([msg]), raw_csv)

        # 2) buffer
        buffer.append(msg)
        logger.debug(f"Buffered {len(buffer)}/{BATCH_SIZE}")

        # 3) once full, run batch
        if len(buffer) >= BATCH_SIZE:
            logger.info("Batch full → running batch processing")
            process_batch_python(buffer.copy(),
                                 processed_csv,
                                 weights_csv)
            buffer.clear()

        return True
    except Exception as e:
        logger.error(f"process_message error: {e}")
        return False

def shutdown_handler(signum, frame):
    logger.info("Shutdown signal → exiting")
    sys.exit(0)

def main():
    # graceful exit
    signal.signal(signal.SIGINT,  shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # rolling CSV paths
    raw_csv       = create_new_csv_file("traffic_raw",       RAW_DIR)
    processed_csv = create_new_csv_file("traffic_processed", PROCESSED_DIR)
    weights_csv   = create_new_csv_file("rf_weights",        PROCESSED_DIR)

    pending = []
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(["raw-traffic-data"])
    logger.info("Subscribed to topic: raw-traffic-data")

    try:
        while True:
            m = consumer.poll(1.0)
            if m is None: continue
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Partition EOF")
                else:
                    logger.error(f"Kafka error: {m.error()}")
                continue

            try:
                rec = json.loads(m.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode failed: {e}")
                continue

            if process_message(rec, raw_csv, processed_csv, weights_csv, pending):
                consumer.commit(m)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # final flush
        if pending:
            try:
                process_batch_python(pending, processed_csv, weights_csv)
            except Exception as e:
                logger.error(f"Final batch error: {e}")
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()
