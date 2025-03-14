import os
import shutil
import pandas as pd
import logging

# Configure logging
log_file = r"execution.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')

def log(message):
    logging.info(message)
    print(message)


# Simulate a list of files with timestamps
files = [
    "data_2024-01-15.csv", "data_2024-02-10.csv", "data_2024-02-28.csv",
    "data_2024-03-05.csv", "data_2024-03-21.csv"
]

# Create bucketed directories if not exist
for file in files:
    date_str = file.split("_")[1].split(".")[0]  # Extract date part
    date = pd.to_datetime(date_str)
    month_folder = f"data_buckets/{date.strftime('%Y-%m')}"  # Format as '2024-01'
    logging.info("Creating month folder " + month_folder)

    os.makedirs(month_folder, exist_ok=True)  # Create the folder if it doesn't exist
    filepath = os.path.join("data", file) 
    print(filepath)
    #filepath = r"data\" + file
    shutil.move(filepath, os.path.join(month_folder, file))
    logging.info("Successfully bucketed file " + filepath + " to " + os.path.join(month_folder, file))