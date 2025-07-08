import pandas as pd
import os
import glob
import logging

# Setup basic logging
logging.basicConfig(
    filename='merge_files.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def discover_files(folder=None):
    """
    Finds all CSV and Excel files in the given folder.
    """
    folder = folder or os.getcwd()
    patterns = ['*.csv', '*.xlsx', '*.xls']
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(folder, pat)))
    logging.info(f"Discovered files: {files}")
    return files

def read_file(path):
    """
    Reads a file into a DataFrame, based on its extension.
    """
    try:
        ext = os.path.splitext(path)[1].lower()
        if ext == '.csv':
            df = pd.read_csv(path)
        elif ext in ['.xlsx', '.xls']:
            df = pd.read_excel(path)
        else:
            logging.warning(f"Skipping unsupported file type: {path}")
            return None
        if df.empty:
            logging.info(f"Empty DataFrame from {path}")
            return None
        logging.info(f"Read {path} with {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Failed to read {path}: {e}")
        return None

def merge_files(output_name='final_output.csv', folder=None):
    """
    Discovers CSV and Excel files, merges them, and saves as CSV.
    """
    files = discover_files(folder)
    if not files:
        print("No CSV or Excel files found to merge.")
        return

    dfs = [read_file(f) for f in files]
    dfs = [df for df in dfs if df is not None]

    if not dfs:
        print("No valid files to merge.")
        return

    merged = pd.concat(dfs, ignore_index=True)
    try:
        merged.to_csv(output_name, index=False, encoding='utf-8')
        print(f"Successfully merged {len(dfs)} files into: {output_name}")
        logging.info(f"Merged {len(dfs)} files into {output_name}")
    except Exception as e:
        print(f"Error saving merged file: {e}")
        logging.error(f"Error saving output: {e}")

if __name__ == "__main__":
    merge_files()
