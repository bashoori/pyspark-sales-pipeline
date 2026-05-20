# PySpark Sales Pipeline

Distributed data processing pipeline demonstrating Spark fundamentals: partitioning, transformations, aggregations, and KPI generation at scale.

## Overview

When transaction data grows from gigabytes to terabytes, Pandas and single-machine Python hit a wall. This pipeline demonstrates how to use Apache Spark to process large-scale sales data efficiently and generate business insights.

**Key Topics**
- PySpark DataFrames and lazy evaluation
- Partitioning for distributed processing
- SQL queries on Spark (SparkSQL)
- Aggregation and grouping at scale
- Performance optimization (caching, partitioning)

## The Problem

You have millions of sales transactions—each with customer, product, date, amount, store, region. Business asks: "What are total sales by category? Top products? Regional trends?"

On a laptop (Pandas): data doesn't fit in memory.  
On a cluster (Spark): data is distributed, processed in parallel, results are fast.

This pipeline shows the difference.

## Architecture

### Input Data
Raw sales transactions (CSV or Parquet):
- transaction_id, customer_id, product_id, amount, date, store, region

### Processing Stages

**1. Ingest**
- Read from file or cloud storage
- Infer or enforce schema
- Handle missing/malformed records

**2. Transform**
- Calculate derived columns (month, quarter, year)
- Create product hierarchies (category, subcategory)
- Flag anomalies (negative amounts, future dates)

**3. Aggregate**
- Sum sales by product, category, region, date
- Calculate running totals and year-over-year trends
- Compute KPIs (average order value, purchase frequency)

**4. Output**
- Sales KPIs (total, average, trend)
- Top products by revenue and volume
- Regional and category breakdowns
- Time-series for trend analysis

## Key Spark Concepts Demonstrated

### 1. DataFrames (vs RDDs)
- **Structured** — column names, types, schema
- **Optimized** — Catalyst optimizer plans queries
- **SQL-friendly** — write SQL directly on DataFrames

### 2. Partitioning
Data split across workers for parallel processing.
```python
df.repartition("region")  # Organize data by region
df.write.partitionBy("year", "month")  # Write in partitioned structure
```
Why: Speeds up filters (scan only relevant partitions), enables parallel writes.

### 3. Lazy Evaluation
Spark doesn't compute until you ask.
```python
df_filtered = df.filter(col("amount") > 100)  # Not executed yet
df_filtered.show()  # NOW it runs
```
Why: Spark can optimize the full pipeline before execution.

### 4. Aggregation Patterns
```python
# Group by region, sum sales
df.groupBy("region").agg(sum("amount"), count("*"))

# Window functions (running totals, ranks)
window = Window.partitionBy("region").orderBy("date")
df.withColumn("running_total", sum("amount").over(window))
```

### 5. Performance Optimization
- **Caching**: `df.cache()` — keep frequently-used data in memory
- **Partitioning**: distribute data smartly, prune partitions on filter
- **Columnar storage**: Parquet (not CSV) for faster reads
- **Broadcasting**: small lookup tables sent to all workers

## Project Structure

```
pyspark-sales-pipeline/
├── data/
│   └── sample_sales_data.csv
├── notebooks/
│   ├── 01_ingest_data.py
│   ├── 02_transform_data.py
│   └── 03_generate_kpis.py
├── docs/
│   └── etl_diagram.png
├── requirements.txt
└── README.md
```

## Running It

### Local (Single Machine)
```bash
pip install -r requirements.txt
python notebooks/01_ingest_data.py
python notebooks/02_transform_data.py
python notebooks/03_generate_kpis.py
```

### Cluster (AWS EMR, Databricks, etc.)
```bash
spark-submit --master yarn --deploy-mode cluster notebooks/01_ingest_data.py
```

## Sample KPIs Generated

| Metric | Value |
|---|---|
| Total Sales | $15.2M |
| Avg Transaction | $245 |
| Transactions | 62K |
| Top Category | Electronics (38% of revenue) |
| Top Region | Northeast (32% of revenue) |
| YoY Growth | +18% |

## What This Demonstrates

**Distributed Computing Thinking**
- Partitioning data for parallel processing
- Understanding when Spark is needed (size matters)
- Optimizing for cluster execution (avoiding shuffles, caching)

**Spark Skills**
- DataFrame operations (filter, join, aggregate)
- SparkSQL for complex queries
- Window functions for time-series
- Performance profiling and tuning

**Data Pipeline Design**
- Modular stages (ingest → transform → aggregate)
- Reusable transformations
- Schema validation

## What I'd Do Differently

1. **Incremental processing** — merge-only-new-data instead of full reload
2. **Delta Lake** — ACID transactions, data versioning, time travel
3. **Structured Streaming** — real-time KPI updates as data arrives
4. **Data quality checks** — assertions on row counts, value ranges
5. **Cost monitoring** — track Spark job cost, optimize by partition pruning

## Performance Tips

**Do:**
- Use Parquet (columnar) instead of CSV
- Partition output by date/region
- Cache intermediate results if reused
- Use window functions instead of self-joins

**Don't:**
- Broadcast large tables (causes OOM)
- Shuffle unnecessarily (expensive network operation)
- Use RDDs (DataFrames are faster)
- Ignore partitioning (kills performance)

## Next Steps

- Add streaming version (Spark Structured Streaming)
- Migrate to Delta Lake (ACID + versioning)
- Implement incremental processing
- Add Spark job monitoring and metrics
- Build real-time dashboard on top

---

**The point:** This pipeline shows the difference between "it works" and "it scales." Spark enables efficient processing of data that single-machine tools can't touch.
