import pandas as pd
import os

def merge_parquet_to_csv(parquet_dir, output_csv):
    # List all .parquet files in the directory
    parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]
    
    # Initialize an empty list to store DataFrames
    dataframes = []
    
    # Read each .parquet file and append to the list
    for file in parquet_files:
        file_path = os.path.join(parquet_dir, file)
        df = pd.read_parquet(file_path)
        dataframes.append(df)
    
    # Concatenate all DataFrames
    merged_df = pd.concat(dataframes, ignore_index=True)
    
    # Save the merged DataFrame to a .csv file
    merged_df.to_csv(output_csv, index=False)
    print(f"Merged CSV saved to {output_csv}")

if __name__ == "__main__":
    parquet_dir = "parquet_data"  # Directory containing .parquet files
    output_csv = "merged_data.csv"  # Output .csv file name
    merge_parquet_to_csv(parquet_dir, output_csv)
