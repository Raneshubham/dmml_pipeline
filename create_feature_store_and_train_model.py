import pandas as pd

# Load the uploaded CSV file
file_path = "data/processed_data.csv"
df = pd.read_csv(file_path)

# Display basic information about the dataset
df.info(), df.head()

#!pip install feast

#pip install --upgrade feast

#!feast init feature_repo

parquet_path = "feature_repo/feature_repo/data/processed_data.parquet"
df.to_parquet(parquet_path, index=False)


from datetime import datetime, timedelta
import feast

# Ensure InvoiceDate is in datetime format with UTC
#df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors='coerce', utc=True)

df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors='coerce')

# Drop rows where InvoiceDate is still NaT (missing timestamp)
df = df.dropna(subset=["InvoiceDate"])

# Convert timezone-naive timestamps to UTC
df["InvoiceDate"] = df["InvoiceDate"].dt.tz_localize(None).dt.tz_localize("UTC")
# Define Feast entity
customer_entity = feast.Entity(name="customer_id", join_keys=["CustomerID"])

# Define Feature View
customer_feature_view = feast.FeatureView(
    name="customer_features",
    entities=[customer_entity],
    ttl=timedelta(days=1),
    schema=[
        feast.Field(name="InvoiceNo", dtype=feast.types.Float64),
        feast.Field(name="StockCode", dtype=feast.types.Float64),
        feast.Field(name="Description", dtype=feast.types.Float64),
        feast.Field(name="Quantity", dtype=feast.types.Float64),
        feast.Field(name="InvoiceDate", dtype=feast.types.String),  # Convert to string for storage
        feast.Field(name="UnitPrice", dtype=feast.types.Float64),
        feast.Field(name="Country", dtype=feast.types.Float64),
        feast.Field(name="Status", dtype=feast.types.Float64),
    ],
    source=feast.FileSource(path="data/processed_data.parquet"),
)

# Define the Feature Store registry
fs = feast.FeatureStore(repo_path="feature_repo/feature_repo")
fs.apply([customer_entity, customer_feature_view])

print("Feature store created successfully!")

# Define a list of entity keys
entity_df = pd.DataFrame({
    "CustomerID": df["CustomerID"],
    "event_timestamp": pd.to_datetime(df["InvoiceDate"])  # Ensure timestamp format
})

# Fetch feature values from the feature store
training_df = fs.get_historical_features(
    entity_df=entity_df,
    features=[
        "customer_features:InvoiceNo",
        "customer_features:StockCode",
        "customer_features:Description",
        "customer_features:Quantity",
        "customer_features:InvoiceDate",
        "customer_features:UnitPrice",
        "customer_features:Country",
        "customer_features:Status"
    ]
).to_df()

print(training_df.head())  # Check the processed dataset

