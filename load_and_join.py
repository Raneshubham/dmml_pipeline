import sqlite3
import pandas as pd
import logging
from datetime import datetime

# Configure logging
LOG_FILE = "execution.log"  # Updated log file name
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# File paths
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b67220bf321739d3fe3dba0b615aefa8ef39b24d
CSV_FILE = r"data\ecom_transactions.csv"  # Updated CSV file path
DB_FILE = "customer_churn.db"
TABLE_NAME = "transactions"  # SQLite table to read data from
OUTPUT_CSV = r"data\merged_data.csv"  # New CSV file for the joined data
<<<<<<< HEAD
=======
=======
CSV_FILE = r"C:\Users\ranes\Desktop\DMML\data\ecom_transactions.csv"  # Updated CSV file path
DB_FILE = "customer_churn.db"
TABLE_NAME = "transactions"  # SQLite table to read data from
OUTPUT_CSV = r"C:\Users\ranes\Desktop\DMML\data\merged_data.csv"  # New CSV file for the joined data
>>>>>>> 4938a3879c8c6939f02eb9ea837b0ea36efa3a5c
>>>>>>> b67220bf321739d3fe3dba0b615aefa8ef39b24d

def log_message(message, level="info"):
    """Logs a message to the execution.log file."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)  # Also print to console

def load_from_csv(csv_file):
    """Loads data directly from a CSV file."""
    try:
        df = pd.read_csv(csv_file, encoding="utf-8")
        log_message(f"Loaded {len(df)} rows from CSV file '{csv_file}'.")
        return df
    except Exception as e:
        log_message(f"Error loading CSV '{csv_file}': {e}", "error")
        return None

def load_from_sqlite(db_file, table_name):
    """Loads data from an SQLite table."""
    try:
        conn = sqlite3.connect(db_file)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        conn.close()
        log_message(f"Loaded {len(df)} rows from SQLite table '{table_name}'.")
        return df
    except Exception as e:
        log_message(f"Error loading SQLite table '{table_name}': {e}", "error")
        return None

def full_outer_join(df_csv, df_sqlite):
    """Performs a FULL OUTER JOIN between two DataFrames."""
    if df_csv is None or df_sqlite is None:
        log_message("Cannot join data - one of the datasets is missing!", "error")
        return None

    # Ensure InvoiceNo and StockCode are treated as keys
    df_csv["InvoiceNo"] = df_csv["InvoiceNo"].astype(str)
    df_csv["StockCode"] = df_csv["StockCode"].astype(str)

    df_sqlite["InvoiceNo"] = df_sqlite["InvoiceNo"].astype(str)
    df_sqlite["StockCode"] = df_sqlite["StockCode"].astype(str)

    # Perform FULL OUTER JOIN using merge with 'outer'
    df_joined = pd.concat([df_csv, df_sqlite], ignore_index=True).drop_duplicates()

    log_message(f"Full Outer Joined data has {len(df_joined)} rows.")
    return df_joined

def save_to_csv(df, csv_file):
    """Saves the DataFrame to a CSV file."""
    try:
        df.to_csv(csv_file, index=False, encoding="utf-8")
        log_message(f"Saved {len(df)} rows to CSV file '{csv_file}'.")
    except Exception as e:
        log_message(f"Error saving to CSV file '{csv_file}': {e}", "error")

if __name__ == "__main__":
    log_message("Ingestion process started.")

    # Load both datasets
    df_csv = load_from_csv(CSV_FILE)
    df_sqlite = load_from_sqlite(DB_FILE, TABLE_NAME)

    # Perform FULL OUTER JOIN
    df_final = full_outer_join(df_csv, df_sqlite)

    # Save joined data to CSV
    if df_final is not None:
        save_to_csv(df_final, OUTPUT_CSV)
        log_message("Ingestion process completed successfully.")
