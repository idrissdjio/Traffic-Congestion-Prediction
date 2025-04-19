import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Define the path to the merged data
DATA_FILE = "/Users/idrissdjiofack/Desktop/CU Boulder Projects/Spring2025/Big-Data-Analytics/machine_learning/data_warehouse/traffic_cleaned_data.csv"
FIGURES_FOLDER = "./figures"  # Save figures in a folder under data_visualization

def visualize_traffic_data(data_file, figures_folder):
    # Ensure the figures folder exists
    os.makedirs(figures_folder, exist_ok=True)

    # Load the data
    print(f"📂 Loading data from: {data_file}")
    try:
        df = pd.read_csv(data_file)
        print(f"✅ Data loaded successfully with {len(df)} rows and {len(df.columns)} columns.")
    except FileNotFoundError:
        print(f"❌ Error: File not found at {data_file}. Please check the path.")
        return

    # Set up seaborn style
    sns.set(style="whitegrid")

    # Visualization 1: Distribution of Congestion Levels
    plt.figure(figsize=(8, 6))
    sns.countplot(data=df, x="Congestion_Level", palette="viridis", order=["Low", "Medium", "High"])
    plt.title("Distribution of Congestion Levels", fontsize=16)
    plt.xlabel("Congestion Level", fontsize=12)
    plt.ylabel("Count", fontsize=12)
    plt.tight_layout()
    output_path = os.path.join(figures_folder, "congestion_level_distribution.png")
    plt.savefig(output_path)
    print(f"📊 Saved: {output_path}")
    plt.show()

    # Visualization 2: Average Velocity by Lane
    if "Lane_ID" in df.columns and "v_Vel" in df.columns:
        plt.figure(figsize=(10, 6))
        sns.barplot(data=df, x="Lane_ID", y="v_Vel", palette="coolwarm", ci=None)
        plt.title("Average Velocity by Lane", fontsize=16)
        plt.xlabel("Lane ID", fontsize=12)
        plt.ylabel("Average Velocity (m/s)", fontsize=12)
        plt.tight_layout()
        output_path = os.path.join(figures_folder, "average_velocity_by_lane.png")
        plt.savefig(output_path)
        print(f"📊 Saved: {output_path}")
        plt.show()

    # Visualization 3: Vehicle Count by Lane
    if "Lane_ID" in df.columns:
        plt.figure(figsize=(10, 6))
        sns.countplot(data=df, x="Lane_ID", palette="magma")
        plt.title("Vehicle Count by Lane", fontsize=16)
        plt.xlabel("Lane ID", fontsize=12)
        plt.ylabel("Vehicle Count", fontsize=12)
        plt.tight_layout()
        output_path = os.path.join(figures_folder, "vehicle_count_by_lane.png")
        plt.savefig(output_path)
        print(f"📊 Saved: {output_path}")
        plt.show()

    # Visualization 4: Space Headway by Lane
    if "Lane_ID" in df.columns and "Space_Headway" in df.columns:
        plt.figure(figsize=(10, 6))
        sns.boxplot(data=df, x="Lane_ID", y="Space_Headway", palette="Set2")
        plt.title("Space Headway by Lane", fontsize=16)
        plt.xlabel("Lane ID", fontsize=12)
        plt.ylabel("Space Headway (meters)", fontsize=12)
        plt.tight_layout()
        output_path = os.path.join(figures_folder, "space_headway_by_lane.png")
        plt.savefig(output_path)
        print(f"📊 Saved: {output_path}")
        plt.show()

if __name__ == "__main__":
    # Call the visualization function
    visualize_traffic_data(DATA_FILE, FIGURES_FOLDER)
