import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "raw-traffic-data"

# Define Schema for Incoming Data
schema = StructType() \
    .add("Vehicle_ID", IntegerType()) \
    .add("Frame_ID", IntegerType()) \
    .add("Total_Frames", IntegerType()) \
    .add("Global_Time", IntegerType()) \
    .add("Local_X", FloatType()) \
    .add("Local_Y", FloatType()) \
    .add("Global_X", FloatType()) \
    .add("Global_Y", FloatType()) \
    .add("v_length", FloatType()) \
    .add("v_Width", FloatType()) \
    .add("v_Class", IntegerType()) \
    .add("v_Vel", FloatType()) \
    .add("v_Acc", FloatType()) \
    .add("Lane_ID", IntegerType()) \
    .add("O_Zone", FloatType()) \
    .add("D_Zone", FloatType()) \
    .add("Int_ID", FloatType()) \
    .add("Section_ID", FloatType()) \
    .add("Direction", FloatType()) \
    .add("Movement", FloatType()) \
    .add("Preceding", IntegerType()) \
    .add("Following", IntegerType()) \
    .add("Space_Headway", FloatType()) \
    .add("Time_Headway", FloatType()) \
    .add("Location", StringType())

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Clear existing processed data
processed_folder = "/app/data/processed/traffic"
if os.path.exists(processed_folder):
    for file in os.listdir(processed_folder):
        file_path = os.path.join(processed_folder, file)
        os.remove(file_path)
    print(f"✅ Cleared existing processed data in {processed_folder}")

# Read Data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON Messages
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Data Cleaning: Handle Missing Values
df = df.fillna({
    "v_Vel": 0.0,
    "v_Acc": 0.0,
    "Lane_ID": -1,
    "Section_ID": -1,
    "Space_Headway": 0.0,
    "Time_Headway": 0.0
})

# Feature Engineering: Add Congestion Level
df = df.withColumn("Congestion_Level", when(col("v_Vel") < 10, "High")
                   .when((col("v_Vel") >= 10) & (col("v_Vel") < 30), "Medium")
                   .otherwise("Low"))

# Limit the number of records to 100,000
LIMIT = 100000
record_counter = spark.sparkContext.accumulator(0)

def write_to_csv(batch_df, batch_id):
    global record_counter
    if record_counter.value >= LIMIT:
        print("✅ Reached the limit of 100,000 records. Stopping the stream.")
        query.stop()
        return

    # Write batch to CSV
    batch_df.write.mode("append").csv("/app/data/processed/traffic/processed_data.csv", header=True)

    # Update record counter
    record_counter += batch_df.count()
    print(f"📤 Processed {record_counter.value} records so far.")

# Write Stream to CSV
query = df.writeStream \
    .foreachBatch(write_to_csv) \
    .outputMode("append") \
    .start()

query.awaitTermination()