import pandas as pd
import os
import glob
import logging

# Setup basic logging
logging.basicConfig(
    filename='merge_csvs.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def discover_csv_files(folder=None):
    """
    Finds all CSV files in the given folder.
    """
    folder = folder or os.getcwd()
    pattern = os.path.join(folder, '*.csv')
    files = glob.glob(pattern)
    logging.info(f"Discovered CSV files: {files}")
    return files

def read_csv_file(path):
    """
    Reads a CSV file into a DataFrame.
    """
    try:
        df = pd.read_csv(path)
        logging.info(f"Read CSV: {path} with {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Failed to read {path}: {e}")
        return None

def merge_csvs(output_name='merged_output.csv', folder=None):
    """
    Discovers all CSV files, merges them, and saves to output_name.
    """
    files = discover_csv_files(folder)
    if not files:
        print("No CSV files found to merge.")
        return

    dfs = [read_csv_file(f) for f in files]
    dfs = [df for df in dfs if df is not None and not df.empty]

    if not dfs:
        print("No valid (non-empty) CSV files to merge.")
        return

    merged = pd.concat(dfs, ignore_index=True)
    try:
        merged.to_csv(output_name, index=False)
        print(f"Successfully merged {len(dfs)} files into: {output_name}")
        logging.info(f"Merged into {output_name}")
    except Exception as e:
        print(f"Error saving merged file: {e}")
        logging.error(f"Error saving output: {e}")

if __name__ == "__main__":
    merge_csvs()
