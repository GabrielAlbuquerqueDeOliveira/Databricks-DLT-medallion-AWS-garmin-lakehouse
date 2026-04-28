## Triathlon Performance Data Lakehouse 🏊‍♂️🚴‍♂️🏃‍♂️
This project implements a complete End-to-End Data Lakehouse on Databricks (AWS) using the Medallion Architecture. It ingests personal fitness data from Garmin and compares it against global benchmarks from Kaggle/Strava, utilizing professional data engineering patterns.

# 🏗️ Architecture Overview

The pipeline follows the Medallion Architecture, ensuring data quality and reliability at every stage:

Cloud Storage: AWS S3 buckets serve as the physical storage layer.

Ingestion (Bronze): Incremental loading of raw CSV files using Databricks Auto Loader (cloudFiles).

Transformation (Silver): Data cleansing, unit normalization, schema mapping (PT to EN), and deduplication using Delta Live Tables (DLT).

Analytics (Gold): Business-level aggregations and global benchmarking.

Serving Layer: Databricks SQL Warehouse (Serverless) powering interactive BI Dashboards.

# 🛠️ Tech Stack

Platform: Databricks **(AWS Marketplace)**

Compute: Serverless Clusters & SQL Warehouses

Orchestration: Delta Live Tables (DLT Pipelines)

Storage: Delta Lake on AWS S3

Languages: PySpark (ETL) and SQL (Analytics)

Governance: Unity Catalog

# 🚀 Key Engineering Features

1. Incremental Ingestion with Auto Loader

Implemented spark.readStream with cloudFiles to efficiently ingest new workout files dropped into S3 without reprocessing old data.

# 3. Data Quality & Idempotency

Expectations: Used DLT Expectations to automatically drop outliers (e.g., GPS glitches showing >100km runs).

Deduplication: Applied .dropDuplicates() to ensure that even if the same workout file was uploaded multiple times, the final metrics remained accurate (idempotency).

Unit Normalization: Handled sensor-specific scaling issues, such as converting swimming distance from meters (raw) to kilometers (standardized).

# 3. Localization & Mapping

Standardized categorical data by mapping Portuguese activity types (e.g., Natação, Corrida) to English (Swim, Run) to align with international Kaggle benchmarks.

# 📊 Dashboard & Insights

The final layer utilizes a Serverless SQL Warehouse to serve a unified view of:

Personal Performance: Monthly distance trends across all three disciplines.

Global Benchmarking: Comparison of personal totals against a dataset of millions of Strava activities.

Data Consistency: Implementation of UNION ALL views and ROUND() functions for professional-grade reporting.

# 🔧 How to Run

Configure an AWS S3 Bucket for the landing zone.

Set up a Databricks Pipeline pointing to the my_transformation.py notebook.

Deploy the pipeline in Triggered mode to optimize costs.

Execute the SQL Dashboard queries using a Serverless Warehouse.

# 💡 Engineering Note: Cost Management

This project was developed using a pay-as-you-go model on AWS. Efficient resource management was achieved by utilizing Triggered DLT pipelines and Auto-Stop configurations on SQL Warehouses to minimize DBU consumption.
