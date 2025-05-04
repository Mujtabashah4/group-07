import pandas as pd
import matplotlib.pyplot as plt

def main():
    # Step 1: Fetch Data
    print("Reading data...")
    # Assume a dataset with sales figures and other fields is provided.
    df = pd.read_csv("data/analytics_data.csv")
    print(f"Data shape: {df.shape}")

    # Step 2: Validate Data
    print("Validating data...")
    missing_values = df.isnull().sum()
    print("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()

    # Step 3: Transform Data
    print("Transforming data...")
    # For example, if there is a "sales" column, create a normalized version.
    if "sales" in df_clean.columns:
        df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()

    # Step 4: Generate Analytics Report
    print("Generating analytics report...")
    summary = df_clean.describe()
    summary.to_csv("data/analytics_summary.csv")
    print("Summary statistics saved to data/analytics_summary.csv")

    # Step 5: Create a Histogram for Sales Distribution
    if "sales" in df_clean.columns:
        plt.hist(df_clean["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("data/sales_histogram.png")
        plt.close()
        print("Sales histogram saved to data/sales_histogram.png")

    print("Analytics pipeline completed.")

if __name__ == "__main__":
    main()
