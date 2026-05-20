#03_generate_kpis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, desc

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load transformed data
df = spark.read.parquet("/FileStore/output/transformed_sales_data")

# Total revenue
total_revenue = df.agg(sum("order_value").alias("total_revenue"))
total_revenue.show()

# Top 5 products by quantity sold
top_products = df.groupBy("product_id")     .agg(sum("quantity").alias("total_sold"))     .orderBy(desc("total_sold"))     .limit(5)
top_products.show()

# Average cart size
avg_cart_size = df.groupBy("order_id")     .agg(sum("quantity").alias("cart_size"))     .agg(avg("cart_size").alias("avg_cart_size"))
avg_cart_size.show()