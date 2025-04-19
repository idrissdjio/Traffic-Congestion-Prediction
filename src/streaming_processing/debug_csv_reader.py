import pandas as pd
import os

# Dynamically construct the file path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_FILE = os.path.join(BASE_DIR, "data", "raw", "traffic-data.csv")

# Check if the file exists
if not os.path.exists(DATA_FILE):
    print(f"❌ Error: Data file not found at {DATA_FILE}. Please check the file path.")
    print("ℹ️ Hint: Ensure the file exists and the relative path is correct.")
else:
    print(f"✅ Data file found at {DATA_FILE}. Reading the file...")

    # Try reading the CSV file
    try:
        df = pd.read_csv(DATA_FILE)
        print("✅ File read successfully!")
        print(f"📊 Data preview:\n{df.head()}")
    except Exception as e:
        print(f"❌ Error while reading the file: {e}")
