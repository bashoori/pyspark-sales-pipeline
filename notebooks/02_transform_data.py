# 02_transform_data.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round

# Load saved DataFrame
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/FileStore/output/clean_sales_data")

# Convert timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Calculate order total
df = df.withColumn("order_value", round(col("quantity") * col("price"), 2))

# Drop nulls (if any)
df_clean = df.dropna()

# Show transformed data
df_clean.show()

# Optional: Save transformed output
df_clean.write.mode("overwrite").parquet("/FileStore/output/transformed_sales_data")


