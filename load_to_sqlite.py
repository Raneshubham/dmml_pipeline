import sqlite3
import pandas as pd
import logging

# Configure logging
LOG_FILE = "execution.log"  # Renamed log file
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# File paths
<<<<<<< HEAD
CSV_FILE = "data/transactions.csv"
=======
CSV_FILE = "C:/Users/ranes/Desktop/DMML/data/transactions.csv"
>>>>>>> 4938a3879c8c6939f02eb9ea837b0ea36efa3a5c
DB_FILE = "customer_churn.db"
TABLE_NAME = "transactions"

def load_csv_to_sqlite(csv_file, db_file, table_name):
    """Loads articles CSV file into an SQLite database."""
    
    # Step 1: Connect to SQLite database (Creates if not exists)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Step 2: Create table if it does not exist
<<<<<<< HEAD

    cursor.execute("DROP TABLE IF EXISTS transactions")

=======
>>>>>>> 4938a3879c8c6939f02eb9ea837b0ea36efa3a5c
    create_table_query = f"""
    CREATE TABLE transactions (
        InvoiceNo VARCHAR(20) NOT NULL,
        StockCode VARCHAR(20) NOT NULL,
        Description TEXT,
        Quantity INT NOT NULL,
        InvoiceDate DATETIME NOT NULL,
        UnitPrice DECIMAL(10,2) NOT NULL,
        CustomerID INT,
        Country VARCHAR(50) NOT NULL,
        Active BOOLEAN NOT NULL
    );
    """
    cursor.execute(create_table_query)
    
    # Step 3: Load CSV into pandas DataFrame
    try:
        df = pd.read_csv(csv_file)
        logging.info("CSV file loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading CSV file: {e}")
        return
    
    # Step 4: Insert data into the table
    df.to_sql(table_name, conn, if_exists="append", index=False)
    
    # Step 5: Close connection
    conn.commit()
    conn.close()
    
    logging.info(f"Data from {csv_file} loaded into {table_name} in {db_file}")

# Run the function
if __name__ == "__main__":
    load_csv_to_sqlite(CSV_FILE, DB_FILE, TABLE_NAME)
