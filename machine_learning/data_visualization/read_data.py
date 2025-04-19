import pandas as pd

# Define the path to the merged data
DATA_FILE = "/Users/idrissdjiofack/Desktop/CU Boulder Projects/Spring2025/Big-Data-Analytics/machine_learning/data_warehouse/traffic_cleaned_data.csv"

def read_top_rows(data_file, n=10):
    # Load the data
    print(f"📂 Loading data from: {data_file}")
    try:
        df = pd.read_csv(data_file)
        print(f"✅ Data loaded successfully with {len(df)} rows and {len(df.columns)} columns.")
    except FileNotFoundError:
        print(f"❌ Error: File not found at {data_file}. Please check the path.")
        return
    # Display the top n rows
    print(f"📊 Displaying the top {n} rows:")
    print(df.head(n))

def read_single_row(data_file, row_number):
    """
    Reads a single row from the dataset based on the given row number.

    Args:
        data_file (str): Path to the dataset file.
        row_number (int): The row number to read (0-indexed).

    Returns:
        dict: A dictionary containing the data for the specified row.
    """
    try:
        df = pd.read_csv(data_file)
        print(f"📂 Loaded data with {len(df)} rows and {len(df.columns)} columns.")
        if row_number < 0 or row_number >= len(df):
            print(f"❌ Error: Row number {row_number} is out of range.")
            return None
        row_data = df.iloc[row_number].to_dict()
        print(f"✅ Data for row {row_number}:")
        for key, value in row_data.items():
            print(f"{key}: {value}")
        return row_data
    except FileNotFoundError:
        print(f"❌ Error: File not found at {data_file}. Please check the path.")
        return None

def list_columns(data_file):
    # Load the data
    print(f"📂 Loading data from: {data_file}")
    try:
        df = pd.read_csv(data_file)
        print(f"✅ Data loaded successfully with {len(df)} rows and {len(df.columns)} columns.")
    except FileNotFoundError:
        print(f"❌ Error: File not found at {data_file}. Please check the path.")
        return

    # List all columns
    print("📋 Columns in the dataset:")
    for col in df.columns:
        print(f"- {col}")

if __name__ == "__main__":
    row_number = 24899  # Change this to the desired row number
    read_single_row(DATA_FILE, row_number)
