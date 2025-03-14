import pandas as pd
import re
import logging
from fpdf import FPDF
import os
<<<<<<< HEAD
import shutil

# File paths
data_file = r"data\merged_data.csv"
log_file = r"execution.log"
output_file = r"data\preprocessed_data.csv"
pdf_report = r"reports\Data Quality Report After PreProcessing.pdf"
=======

# File paths
data_file = r"C:\Users\ranes\Desktop\DMML\data\merged_data.csv"
log_file = r"C:\Users\ranes\Desktop\DMML\execution.log"
output_file = r"C:\Users\ranes\Desktop\DMML\data\preprocessed_data.csv"
pdf_report = r"C:\Users\ranes\Desktop\DMML\reports\Data Quality Report After PreProcessing.pdf"
>>>>>>> 4938a3879c8c6939f02eb9ea837b0ea36efa3a5c

# Configure logging
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')

def log(message):
    logging.info(message)
    print(message)

<<<<<<< HEAD
# Simulate a list of files with timestamps
files = [
    "data_2024-01-15.csv", "data_2024-02-10.csv", "data_2024-02-28.csv",
    "data_2024-03-05.csv", "data_2024-03-21.csv", "data_2024-04-12.csv"
]

# Create bucketed directories if not exist
for file in files:
    date_str = file.split("_")[1].split(".")[0]  # Extract date part
    date = pd.to_datetime(date_str)
    month_folder = f"data_buckets/{date.strftime('%Y-%m')}"  # Format as '2024-01'

    os.makedirs(month_folder, exist_ok=True)  # Create the folder if it doesn't exist
    shutil.move(file, os.path.join(month_folder, file))

=======
>>>>>>> 4938a3879c8c6939f02eb9ea837b0ea36efa3a5c
# Load data
data_df = pd.read_csv(data_file)
initial_shape = data_df.shape  # Store initial shape
log(f"Initial data shape: {initial_shape}")

# Identify missing values
missing_values = data_df.isnull().sum()
log(f"Missing values per column: \n{missing_values[missing_values > 0]}")

# Drop rows with missing values
data_df = data_df.dropna()
log(f"Data shape after dropping missing values: {data_df.shape}")

# Function to check if a string is numeric
def is_numeric(s):
    return bool(re.fullmatch(r'\d+', str(s)))

# Count rows before filtering StockCode
stockcode_before = data_df.shape[0]

# Filter 'StockCode' to keep only numeric values
data_df = data_df[data_df['StockCode'].apply(is_numeric)]
stockcode_dropped = stockcode_before - data_df.shape[0]
log(f"Number of rows dropped due to inconsistent StockCode: {stockcode_dropped}")
log(f"Data shape after filtering numeric StockCode: {data_df.shape}")

# Find CustomerIDs marked both Active and Inactive
active_customers = set(data_df[data_df['Active'] == 'Active']['CustomerID'])
inactive_customers = set(data_df[data_df['Active'] == 'Inactive']['CustomerID'])
intersection_customers = active_customers.intersection(inactive_customers)
log(f"Number of CustomerIDs marked as both Active and Inactive: {len(intersection_customers)}")

# Count rows before removing incorrect CustomerIDs
customer_before = data_df.shape[0]

# Remove conflicting CustomerIDs
data_df = data_df[~data_df['CustomerID'].isin(intersection_customers)]
customer_dropped = customer_before - data_df.shape[0]
log(f"Number of rows dropped due to incorrect CustomerIDs: {customer_dropped}")
log(f"Data shape after removing conflicting CustomerIDs: {data_df.shape}")

# Save preprocessed data
data_df.to_csv(output_file, index=False)
log("Preprocessed data saved successfully.")

# Generate PDF report
class PDF(FPDF):
    def header(self):
        self.set_font("Arial", style='B', size=16)
        self.cell(200, 10, "Data Quality Report After PreProcessing", ln=True, align='C')
        self.ln(10)

    def chapter_title(self, title):
        self.set_font("Arial", style='B', size=12)
        self.cell(0, 10, title, ln=True, align='L')
        self.ln(5)

    def chapter_body(self, body):
        self.set_font("Arial", size=10)
        self.multi_cell(0, 8, body)
        self.ln()

pdf = PDF()
pdf.set_auto_page_break(auto=True, margin=15)
pdf.add_page()

pdf.chapter_title("Initial Data Shape")
pdf.chapter_body(f"The data initially had {initial_shape[0]} rows and {initial_shape[1]} columns.")

pdf.chapter_title("Missing Data")
pdf.chapter_body(str(missing_values))
pdf.chapter_body("As we have a huge dataset, instead of treating missing data, we are directly dropping the rows containing missing values.")

pdf.chapter_title("Inconsistent Data")
pdf.chapter_body(f"We removed {stockcode_dropped} rows where the 'StockCode' contained alphabets or special characters to ensure only numeric values remain.")

pdf.chapter_title("Incorrect Data")
pdf.chapter_body(f"Number of CustomerIDs marked as both Active and Inactive: {len(intersection_customers)}")
pdf.chapter_body(f"We removed {customer_dropped} rows where CustomerIDs had conflicting active/inactive statuses.")

pdf.chapter_title("Preprocessed Data Shape")
pdf.chapter_body(f"After all preprocessing, the dataset now has {data_df.shape[0]} rows and {data_df.shape[1]} columns.")

pdf.output(pdf_report)
log("PDF report generated successfully.")
