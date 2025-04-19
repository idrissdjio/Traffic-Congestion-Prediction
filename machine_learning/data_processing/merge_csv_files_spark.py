from pyspark.sql import SparkSession

# Define paths
PROCESSED_FOLDER = "/app/data/processed/traffic/processed_data.csv"  # Treat as a folder
OUTPUT_FILE = "/app/machine_learning/data_warehouse/traffic_data_processed.csv"

def merge_csv_files_with_spark(input_folder, output_file):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MergeCSVFiles") \
        .getOrCreate()

    print(f"📂 Reading CSV files from folder: {input_folder}")

    # Read all CSV files in the folder
    df = spark.read.option("header", "true").csv(f"{input_folder}/*.csv")

    print(f"✅ Read {df.count()} rows from CSV files.")

    # Write the merged data to a single CSV file
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_file)

    print(f"✅ Merged data saved to: {output_file}")

if __name__ == "__main__":
    merge_csv_files_with_spark(PROCESSED_FOLDER, OUTPUT_FILE)
