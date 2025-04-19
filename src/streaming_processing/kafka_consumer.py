import os
import logging
import json
import uuid
import signal
import sys
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer, KafkaError
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# ---- Configuration ----
BATCH_SIZE = 500                   # same as your Spark BATCH_SIZE
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'traffic_prediction_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# ---- Globals ----
current_raw_csv = None
current_processed_csv = None
pending_records = []

# ---- Setup Logging ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_new_csv_file(prefix: str) -> str:
    """Creates a new CSV filename with timestamp+random suffix."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rid = uuid.uuid4().hex[:8]
    fname = f"{prefix}_{ts}_{rid}.csv"
    logger.info(f"Created new file: {fname}")
    return fname


def save_to_csv(df: pd.DataFrame, csv_path: str):
    """Append a DataFrame to csv_path, writing header only if file is new."""
    df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)


def process_batch_python(batch: list[dict]):
    """
    Given a list of raw records (dicts), run:
     - Pandas -> features -> StandardScaler -> KMeans
     - Map clusters to High/Medium/Low based on avg v_Vel
     - Save enriched batch to current_processed_csv
    """
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
    save_to_csv(df, current_processed_csv)
    logger.info(f"Processed & saved {len(df)} records to {current_processed_csv}")


def process_message(message: dict) -> bool:
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
        save_to_csv(pd.DataFrame([message]), current_raw_csv)
        
        # buffer
        pending_records.append(message)
        logger.info(f"Buffered message, {len(pending_records)}/{BATCH_SIZE}")
        
        if len(pending_records) >= BATCH_SIZE:
            logger.info("Batch threshold reached, running clustering...")
            process_batch_python(pending_records)
            pending_records.clear()
        
        return True
    except Exception as e:
        logger.error(f"Error in process_message: {e}")
        return False


def shutdown_handler(signum, frame):
    logger.info("Graceful shutdown requested")
    sys.exit(0)


def main():
    global current_raw_csv, current_processed_csv
    
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Create both raw & processed CSVs
    current_raw_csv = create_new_csv_file("traffic_raw")
    current_processed_csv = create_new_csv_file("traffic_processed")
    
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
            
            if process_message(record):
                consumer.commit(msg)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    main()


# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, when, udf
# from pyspark.sql import functions as F
# from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
# from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
# from pyspark.ml.clustering import KMeans
# from pathlib import Path

# # Kafka Configuration
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "raw-traffic-data"
# BATCH_SIZE = 10000  # Number of records to process in each batch

# # Define Schema for Incoming Data
# schema = StructType() \
#     .add("Vehicle_ID", IntegerType()) \
#     .add("Frame_ID", IntegerType()) \
#     .add("Total_Frames", IntegerType()) \
#     .add("Global_Time", IntegerType()) \
#     .add("Local_X", FloatType()) \
#     .add("Local_Y", FloatType()) \
#     .add("Global_X", FloatType()) \
#     .add("Global_Y", FloatType()) \
#     .add("v_length", FloatType()) \
#     .add("v_Width", FloatType()) \
#     .add("v_Class", IntegerType()) \
#     .add("v_Vel", FloatType()) \
#     .add("v_Acc", FloatType()) \
#     .add("Lane_ID", IntegerType()) \
#     .add("O_Zone", FloatType()) \
#     .add("D_Zone", FloatType()) \
#     .add("Int_ID", FloatType()) \
#     .add("Section_ID", FloatType()) \
#     .add("Direction", FloatType()) \
#     .add("Movement", FloatType()) \
#     .add("Preceding", IntegerType()) \
#     .add("Following", IntegerType()) \
#     .add("Space_Headway", FloatType()) \
#     .add("Time_Headway", FloatType()) \
#     .add("Location", StringType())

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("KafkaConsumer") \
#     .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
#     .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
#     .getOrCreate()

# # Use a directory in the user's home directory
# home_dir = str(Path.home())
# processed_folder = os.path.join(home_dir, "traffic_data_processed")
# os.makedirs(processed_folder, exist_ok=True)

# # Try to clear existing files, but don't fail if we can't
# try:
#     if os.path.exists(processed_folder):
#         for file in os.listdir(processed_folder):
#             file_path = os.path.join(processed_folder, file)
#             try:
#                 os.remove(file_path)
#             except PermissionError:
#                 print(f"⚠️ Could not remove {file_path} due to permissions. Continuing...")
#     print(f"✅ Output directory set to: {processed_folder}")
# except Exception as e:
#     print(f"⚠️ Warning: Could not clear existing data: {str(e)}")

# # Read Data from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", TOPIC_NAME) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Deserialize JSON Messages
# df = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# # Data Cleaning: Handle Missing Values
# df = df.fillna({
#     "v_Vel": 0.0,
#     "v_Acc": 0.0,
#     "Lane_ID": -1,
#     "Section_ID": -1,
#     "Space_Headway": 0.0,
#     "Time_Headway": 0.0
# })

# # Feature Engineering: Add Congestion Level
# df = df.withColumn("Congestion_Level", when(col("v_Vel") < 10, "High")
#                    .when((col("v_Vel") >= 10) & (col("v_Vel") < 30), "Medium")
#                    .otherwise("Low"))

# # # Limit the number of records to 100,000
# # LIMIT = 100000
# # record_counter = spark.sparkContext.accumulator(0)

# def write_to_csv(batch_df, batch_id):
#     global record_counter
#     if record_counter.value >= LIMIT:
#         print("✅ Reached the limit of 100,000 records. Stopping the stream.")
#         query.stop()
#         return

#     # Write batch to CSV
#     output_path = os.path.join(processed_folder, "processed_data.csv")
#     try:
#         batch_df.write.mode("append").csv(output_path, header=True)
#         # Update record counter
#         record_counter += batch_df.count()
#         print(f"📤 Processed {record_counter.value} records so far.")
#     except Exception as e:
#         print(f"⚠️ Error writing to CSV: {str(e)}")

# # Set a record limit for output
# LIMIT = 10000
# record_counter = spark.sparkContext.accumulator(0)
# pending_records = spark.sparkContext.accumulator(0)

# def process_batch(batch_df, batch_id):
#     global record_counter, pending_records
#     if batch_df.count() == 0:
#         return

#     # Add a timestamp column for ordering
#     batch_df = batch_df.withColumn("processing_timestamp", F.current_timestamp())

#     print(f"--- Processing batch ID: {batch_id} ---")
    
#     # Update pending records counter
#     pending_records.add(batch_df.count())
    
#     # Only process if we have enough records
#     if pending_records.value >= BATCH_SIZE:
#         print(f"Processing batch of {pending_records.value} records...")
        
#         # Assemble selected features into a vector column "features_raw"
#         assembler = VectorAssembler(inputCols=["v_Vel", "v_Acc", "Space_Headway", "Time_Headway"],
#                                   outputCol="features_raw")
#         batch_df = assembler.transform(batch_df)

#         # Scale features using StandardScaler
#         scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
#         scaler_model = scaler.fit(batch_df)
#         batch_df = scaler_model.transform(batch_df)

#         # Apply KMeans clustering (3 clusters)
#         kmeans = KMeans(featuresCol="features", predictionCol="Cluster_Label", k=3, seed=42)
#         kmeans_model = kmeans.fit(batch_df)
#         clustered_df = kmeans_model.transform(batch_df)

#         # Compute average v_Vel per cluster (to decide congestion levels)
#         cluster_stats_df = clustered_df.groupBy("Cluster_Label").avg("v_Vel") \
#                                      .withColumnRenamed("avg(v_Vel)", "avg_speed") \
#                                      .orderBy("avg_speed")
#         stats = cluster_stats_df.collect()
        
#         mapping_dict = {}
#         if len(stats) == 3:
#             mapping_dict[stats[0]["Cluster_Label"]] = "High"
#             mapping_dict[stats[1]["Cluster_Label"]] = "Medium"
#             mapping_dict[stats[2]["Cluster_Label"]] = "Low"
#         else:
#             mapping_dict = {0: "Low", 1: "Medium", 2: "High"}

#         print(f"--- Batch ID: {batch_id} ---")
#         print("Cluster mapping based on average speed:")
#         for cl, level in mapping_dict.items():
#             print(f"   Cluster {cl} : {level}")

#         # Define a UDF to map cluster label to congestion level string
#         def map_cluster(cluster_label):
#             return mapping_dict.get(cluster_label, "Unknown")

#         map_cluster_udf = udf(map_cluster, StringType())
#         clustered_df = clustered_df.withColumn("Congestion_Level", map_cluster_udf(col("Cluster_Label")))

#         # # Train Random Forest model
#         # rf_features = ["v_Vel", "v_Acc", "Space_Headway", "Time_Headway", "Lane_ID", "Section_ID"]
#         # rf_model, accuracy, importance = train_random_forest(clustered_df, rf_features)
        
#         # # Get predictions
#         # predictions = get_predictions(rf_model, clustered_df)

#         # Show selected results from the clustering and classification
#         # predictions.select("Vehicle_ID", "v_Vel", "v_Acc", "Space_Headway", "Time_Headway", 
#         #                  "Cluster_Label", "Congestion_Level", "prediction").show(truncate=False)
        
#         # # Write the enriched results to CSV
#         # output_path = "/app/data/processed/traffic/processed_data.csv"
#         # predictions.write.mode("append").csv(output_path, header=True)

#         # Update record counter and check against LIMIT
#         num_records = batch_df.count()
#         record_counter.add(num_records)
#         pending_records.value = 0  # Reset pending records counter
#         print(f"📤 Processed {record_counter.value} records so far.")
#         if record_counter.value >= LIMIT:
#             print("✅ Reached the limit of 100,000 records. Stopping the stream.")
#             query.stop()
#     else:
#         print(f"Accumulating records... Currently have {pending_records.value} records")

# # def write_to_csv(batch_df, batch_id):
# #     # Write the batch DataFrame to CSV
# #     process_batch(batch_df, batch_id)

# # Write Stream to CSV
# query = df.writeStream \
#     .foreachBatch(write_to_csv) \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()
