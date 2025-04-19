import os
import pandas as pd

# Define paths
PROCESSED_FOLDER = "/Users/idrissdjiofack/Desktop/CU Boulder Projects/Spring2025/Big-Data-Analytics/data/processed/traffic/processed_csv"
OUTPUT_FILE = "/Users/idrissdjiofack/Desktop/CU Boulder Projects/Spring2025/Big-Data-Analytics/machine_learning/data_warehouse/traffic_data_processed.csv"

def merge_csv_files(input_folder, output_file):
    # List all .csv files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
    if not csv_files:
        print("❌ No CSV files found in the folder.")
        return

    print(f"📂 Found {len(csv_files)} CSV files. Merging...")

    # Read and concatenate all CSV files
    dataframes = []
    for file in csv_files:
        file_path = os.path.join(input_folder, file)
        print(f"📄 Reading file: {file_path}")
        df = pd.read_csv(file_path)
        dataframes.append(df)

    # Concatenate all dataframes
    merged_df = pd.concat(dataframes, ignore_index=True)
    print(f"✅ Merged {len(dataframes)} files with a total of {len(merged_df)} rows.")

    # Save the merged dataframe to the output file
    merged_df.to_csv(output_file, index=False)
    print(f"✅ Merged data saved to: {output_file}")

if __name__ == "__main__":
    merge_csv_files(PROCESSED_FOLDER, OUTPUT_FILE)
