import pandas as pd
from datetime import timedelta
import feast
from feast import ValueType  # ✅ Import the correct ValueType


# Load data
file_path = "data/processed_data.csv"
df = pd.read_csv(file_path)

# 🔍 Step 1: Identify non-numeric values
df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors='coerce')

# 🔍 Step 2: Drop rows where CustomerID is NaN (invalid values)
df = df.dropna(subset=["CustomerID"])

# 🔍 Step 3: Convert CustomerID to Int64 safely
df["CustomerID"] = df["CustomerID"].astype("Int64")

# Save cleaned data
df.to_csv("data/processed_data_cleaned.csv", index=False)

print("✅ CustomerID column successfully cleaned and converted to Int64!")
# Convert InvoiceDate to UTC format safely
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors='coerce', utc=True)
df = df.dropna(subset=["InvoiceDate"])  # Drop missing timestamps

# Convert column types
df["InvoiceNo"] = df["InvoiceNo"].astype(str)
df["StockCode"] = df["StockCode"].astype(str)
df["Description"] = df["Description"].astype(str)
df["Quantity"] = df["Quantity"].astype("Int64")
df["UnitPrice"] = df["UnitPrice"].astype(float)
df["Country"] = df["Country"].astype(str)
df["Status"] = df["Status"].astype(str)

# Save as Parquet inside Feast repo
parquet_path = "feature_repo/data/processed_data.parquet"
df.to_parquet(parquet_path, index=False)

# Define Feast entity
customer_entity = feast.Entity(
    name="customer_id",
    value_type=ValueType.INT64,  # ✅ Corrected
    join_keys=["CustomerID"]
)

# Define FileSource with explicit timestamp field
customer_source = feast.FileSource(
    path=parquet_path,
    event_timestamp_column="InvoiceDate",
)

# Define Feature View
customer_feature_view = feast.FeatureView(
    name="customer_features",
    entities=[customer_entity],
    ttl=timedelta(days=1),
    schema=[
        feast.Field(name="InvoiceNo", dtype=feast.types.String),
        feast.Field(name="StockCode", dtype=feast.types.String),
        feast.Field(name="Description", dtype=feast.types.String),
        feast.Field(name="Quantity", dtype=feast.types.Int64),
        feast.Field(name="InvoiceDate", dtype=feast.types.UnixTimestamp),
        feast.Field(name="UnitPrice", dtype=feast.types.Float64),
        feast.Field(name="Country", dtype=feast.types.String),
        feast.Field(name="Status", dtype=feast.types.String),
    ],
    source=customer_source,
)

# Define Feature Store and apply
fs = feast.FeatureStore(repo_path="feature_repo")
fs.apply([customer_entity, customer_feature_view])

print("✅ Feature store created successfully!")
