import pandas as pd
import matplotlib.pyplot as plt
import os
import logging
from fpdf import FPDF
from io import StringIO

# File paths
DATA_FILE = r"data\merged_data.csv"
HISTOGRAM_PATH = r"images\histogram.png"
PDF_REPORT_PATH = r"reports\Data Quality Report Before PreProcessing.pdf"

# Ensure output directories exist
os.makedirs(os.path.dirname(HISTOGRAM_PATH), exist_ok=True)
os.makedirs(os.path.dirname(PDF_REPORT_PATH), exist_ok=True)

# Configure logging
LOG_FILE = "execution.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_message(message, level="info"):
    """Logs messages to file and prints to console."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

try:
    # Load dataset
    df = pd.read_csv(DATA_FILE)
    log_message(f"Successfully loaded dataset from '{DATA_FILE}'.")

    # Get dataset information
    df_info_buf = StringIO()
    df.info(buf=df_info_buf)
    df_info_str = df_info_buf.getvalue()

    # Get dataset shape
    num_rows, num_cols = df.shape
    shape_text = f"Our dataset consists of {num_rows} rows and {num_cols} columns."
    log_message(shape_text)

    # Plot histogram of 'Active' column
    plt.figure(figsize=(8, 5))
    bars = df["Active"].value_counts().plot(kind="bar", color=["blue", "orange"])
    plt.xlabel("Active Column Value", fontsize=12)
    plt.ylabel("Count of Rows", fontsize=12)
    plt.title("Distribution of Active Column", fontsize=14)
    plt.xticks(rotation=0)

    # Annotate bars with counts
    for bar in bars.patches:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{int(bar.get_height())}",
                 ha='center', va='bottom', fontsize=12, fontweight='bold')

    # Save histogram as PNG
    plt.savefig(HISTOGRAM_PATH, bbox_inches='tight')
    plt.close()
    log_message(f"Histogram saved at '{HISTOGRAM_PATH}'.")

    # Generate PDF report
    class PDF(FPDF):
        def header(self):
            self.set_font("Arial", "B", 16)
            self.cell(200, 10, "Data Report Before Preprocessing", ln=True, align="C")
            self.ln(10)

        def chapter_title(self, title):
            self.set_font("Arial", "B", 12)
            self.cell(0, 10, title, ln=True, align="L")
            self.ln(5)

        def chapter_body(self, body):
            self.set_font("Arial", "", 10)
            self.multi_cell(0, 7, body)
            self.ln(5)

    def add_summary_statistics(pdf, df):
        """Adds summary statistics in tabular format to the PDF."""
        pdf.chapter_title("Summary Statistics")

        # Extract summary statistics as a DataFrame
        summary_df = df.describe()

        # Define column widths
        col_widths = [35] + [25] * len(summary_df.columns)

        # Set font for table headers
        pdf.set_font("Arial", "B", 9)

        # Add column headers
        pdf.cell(col_widths[0], 10, "Statistic", border=1, align="C")
        for col in summary_df.columns:
            pdf.cell(col_widths[1], 10, str(col), border=1, align="C")
        pdf.ln()

        # Set font for table values
        pdf.set_font("Arial", "", 9)

        # Add rows for each statistic
        for row_name, row_data in summary_df.iterrows():
            pdf.cell(col_widths[0], 10, row_name, border=1, align="L")  # Row title
            for value in row_data:
                pdf.cell(col_widths[1], 10, f"{value:.2f}", border=1, align="C")  # Rounded values
            pdf.ln()

    # Create PDF object
    pdf = PDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Add sections to the PDF
    pdf.chapter_title("Dataset Overview")
    pdf.chapter_body(shape_text)

    pdf.chapter_title("Dataset Information")
    pdf.chapter_body(df_info_str)

    # Add summary statistics table
    add_summary_statistics(pdf, df)

    # Add histogram to the PDF
    pdf.chapter_title("Active Column Distribution")
    pdf.image(HISTOGRAM_PATH, x=30, w=150)

    # Save the PDF report
    pdf.output(PDF_REPORT_PATH)
    log_message(f"PDF report saved at '{PDF_REPORT_PATH}'.")

except Exception as e:
    log_message(f"Error encountered: {e}", "error")
