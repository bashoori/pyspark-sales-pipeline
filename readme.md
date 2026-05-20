# 🚀 Real-Time Sales Analytics Pipeline with PySpark, Databricks & AWS

This project demonstrates how to build an end-to-end data pipeline using **PySpark** on **Databricks**, processing sales data from CSV input through transformation and KPI generation.

---

## 📌 Project Overview

The pipeline ingests raw sales data, cleans and transforms it, and computes KPIs such as total revenue, top products, and average cart size. Each notebook is modular and runs independently.

---

## 📁 Folder Structure

```
pyspark-sales-pipeline/
├── data/
│   └── sample_sales_data.csv
├── notebooks/
│   ├── 01_ingest_data.py
│   ├── 02_transform_data.py
│   └── 03_generate_kpis.py
├── docs/
│   ├── etl_diagram.png
│   └── databricks_cluster_execution_diagram.png
├── requirements.txt
├── README.md
```

---

## 🛠 Tech Stack

- **Apache Spark (PySpark)**
- **Databricks Community Edition**
- **AWS S3 / DBFS**
- **Delta Lake (optional)**

---

## 🧱 ETL Flow Summary

1. **Ingest** CSV data from `/FileStore/tables/sample_sales_data.csv`
2. **Transform** it: clean timestamps, compute order value
3. **Output** KPIs including total revenue, top products, and average cart size

📊 Diagrams available in [`docs/`](docs/)

---

## 🚀 How to Run

### Step 1: Upload CSV
- Upload `sample_sales_data.csv` via Databricks UI → FileStore → Tables

### Step 2: Run the Notebooks
- `01_ingest_data.py`: Loads and saves raw data
- `02_transform_data.py`: Cleans data and saves output
- `03_generate_kpis.py`: Loads transformed data and prints KPIs

Each notebook is independent and uses `.parquet` for shared intermediate storage.

---

## 📈 Sample KPIs Generated

- ✅ Total Revenue
- ✅ Top 5 Products by Quantity
- ✅ Average Cart Size

---

## 🙋‍♀️ Author

**Bita Ashoori**  
_Data Engineer & Digital Entrepreneur_  
🌐 [GitHub](https://github.com/bitadigitalmarketer)

---

## ⭐️ Show Your Support
If you found this helpful, please ⭐ the repo or connect on LinkedIn!
