# Databricks eCommerce Lakehouse Capstone

## Project Overview

This project demonstrates an end-to-end lakehouse pipeline built on Databricks using a real public eCommerce dataset.

The pipeline loads online retail transaction data into a raw Delta table, cleans and transforms it with PySpark, creates curated and summary Delta tables, performs incremental upserts with MERGE, and documents recovery using Delta Lake time travel.

## Dataset

Dataset used: UCI Online Retail dataset.

The dataset contains real eCommerce transaction data with columns such as invoice number, product code, description, quantity, invoice date, unit price, customer ID, and country.

## Technologies Used

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- MERGE INTO
- Delta Lake Time Travel
- Databricks Jobs workflow planning

## Lakehouse Layers

### Raw Layer

The raw table stores the original eCommerce source data in Delta format.

Table:

```text
workspace.retail_capstone.raw_online_retail

```

### Curated Layer

The curated table removes invalid rows and adds derived columns.

Cleaning steps include:

- Removing null invoice numbers
- Removing null product codes
- Removing null customer IDs
- Removing cancelled invoices
- Removing zero or negative quantity
- Removing zero or negative unit price
- Calculating revenue

Table:

```text
workspace.retail_capstone.curated_online_retail
```

### Summary Layer

The project creates summary tables for analytics.

Tables:

```text
workspace.retail_capstone.daily_country_revenue
workspace.retail_capstone.product_revenue_summary
workspace.retail_capstone.customer_summary
```

## Pipeline Steps

### 1. Ingest Raw Data

The source online retail data is loaded into a raw Delta table.  
This layer preserves the original transaction data and acts as the landing zone.

### 2. Clean and Transform Data

The raw data is transformed using PySpark DataFrame operations such as:

- `select()`
- `filter()`
- `withColumn()`

Invalid records are removed, cancelled invoices are filtered out, and a new `revenue` column is created.

Revenue is calculated as:

```text
quantity * unit_price
```

### 3. Create Summary Tables

The curated data is aggregated using `groupBy()` and `agg()`.

Summary tables include:

- Daily revenue by country
- Product revenue summary
- Customer summary

### 4. Incremental MERGE

The project uses `MERGE INTO` to handle incremental updates.

The match condition uses:

```text
invoice_no + stock_code
```

This is useful because one invoice can contain multiple products.

If a matching row already exists, it is updated.  
If no matching row exists, it is inserted.

This makes the pipeline idempotent and helps handle duplicate upstream retries.

### 5. Delta Lake Time Travel and Recovery

Delta Lake history is checked using:

```sql
DESCRIBE HISTORY workspace.retail_capstone.raw_online_retail;
```

Older versions can be queried using:

```sql
SELECT *
FROM workspace.retail_capstone.raw_online_retail
VERSION AS OF 0;
```

A restore command is documented as part of the recovery runbook:

```sql
RESTORE TABLE workspace.retail_capstone.raw_online_retail
TO VERSION AS OF 0;
```

The restore command should only be executed when recovery is actually required.

## Delta Lake Recovery

The project includes a recovery runbook using:

- `DESCRIBE HISTORY`
- `VERSION AS OF`
- `RESTORE TABLE`

This demonstrates how Delta Lake supports auditing, time travel, and recovery through its append-only transaction log.

## Workflow Plan

The pipeline is designed as a multi-task Databricks Job:

```text
Ingest Raw Data
      ↓
Clean Data
      ↓
Aggregate Metrics
      ↓
Dashboard Refresh
```

Dedicated job compute is recommended for production scheduling because it provides reliability, isolation, and cost control.

## Repository Structure

```text
databricks-ecommerce-lakehouse-capstone/
│
├── README.md
├── notebooks/
│   └── ecommerce_lakehouse_capstone.py
│
├── docs/
│   ├── recovery_runbook.md
│   └── workflow_plan.md
│
└── sql/
    ├── dashboard_queries.sql
    └── delta_time_travel.sql
```

## Key SQL Files

### Dashboard Queries

The `sql/dashboard_queries.sql` file contains reporting queries for:

- Daily country revenue
- Product revenue summary
- Customer summary

### Delta Time Travel Queries

The `sql/delta_time_travel.sql` file contains queries for:

- Checking Delta table history
- Querying older table versions
- Validating previous row counts
- Documenting restore logic
