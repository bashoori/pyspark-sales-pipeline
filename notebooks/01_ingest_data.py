# 01_ingest_data.py


from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("SalesDataIngestion").getOrCreate()

# Load data from DBFS
df = spark.read.csv("/FileStore/tables/sample_sales_data.csv", header=True, inferSchema=True)

# Show preview
df.printSchema()
df.show()

# Save for next stage
df.write.mode("overwrite").parquet("/FileStore/output/clean_sales_data")

