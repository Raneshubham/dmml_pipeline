from pyspark.sql import SparkSession
from delta import *

# Initialize Spark with Delta Lake support
spark = (SparkSession.builder
         .appName("FeatureStore")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

# Define feature schema
data = [
    ("user_1", 0.8, 23, "2024-03-13"),
    ("user_2", 0.3, 30, "2024-03-12"),
    ("user_3", 0.6, 25, "2024-03-10")
]

columns = ["user_id", "purchase_probability", "age", "date"]

# Convert to Delta format
df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("overwrite").save("feature_store/delta_table")

print("Delta Lake initialized!")
