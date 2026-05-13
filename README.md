# Databricks eCommerce Lakehouse Capstone

## Project Overview

This project demonstrates an end-to-end lakehouse pipeline built on Databricks using a real public eCommerce dataset.

The pipeline loads online retail transaction data into a raw Delta table, cleans and transforms it with PySpark, creates curated and summary Delta tables, performs incremental upserts with `MERGE`, and documents recovery using Delta Lake time travel.

The project is being extended in phases. Phase 1 focuses on sales transaction analytics. Phase 2 extends the project into an inventory management pipeline using multiple data sources and medallion architecture.

---

## Project Phases

### Phase 1: Sales Lakehouse Capstone

The first phase of this project builds a Databricks Lakehouse pipeline for eCommerce sales transactions.

It includes:

- Raw Delta table
- Curated Delta table
- Summary analytics tables
- PySpark transformations
- Incremental `MERGE INTO` logic
- Delta Lake time travel
- Recovery runbook
- Databricks Jobs workflow plan

### Phase 2: Inventory Management Pipeline

The second phase extends the project into a more realistic inventory management pipeline.

It adds:

- Multiple data sources
- Product master data
- Inventory stock data
- Supplier data
- Warehouse data
- Bronze, Silver, and Gold medallion architecture
- Inventory KPIs
- Stockout risk logic
- Reorder recommendation logic

---

## Dataset

### Phase 1 Dataset

Dataset used: UCI Online Retail dataset.

The dataset contains real eCommerce transaction data with columns such as invoice number, product code, description, quantity, invoice date, unit price, customer ID, and country.

Example columns:

```text
InvoiceNo
StockCode
Description
Quantity
InvoiceDate
UnitPrice
CustomerID
Country
```

### Phase 2 Data Sources

Phase 2 extends the project with additional sample data sources.

Planned sources:

```text
Sales transactions
Product master data
Inventory stock data
Supplier data
Warehouse data
```

These sources will be combined to support inventory analytics and stock management use cases.

---

## Technologies Used

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Delta Lake Time Travel
- `MERGE INTO`
- Medallion architecture
- Databricks Jobs workflow planning

---

## Phase 1: Sales Lakehouse Pipeline

## Lakehouse Layers

### Raw Layer

The raw table stores the original eCommerce source data in Delta format.

Table:

```text
workspace.retail_capstone.raw_online_retail
```

The raw layer preserves the original transaction data and acts as the landing zone.

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

---

## Phase 1 Pipeline Steps

### 1. Ingest Raw Data

The source online retail data is loaded into a raw Delta table.

This layer preserves the original transaction data and allows the pipeline to reprocess data if needed.

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

---

## Delta Lake Recovery

The project includes a recovery runbook using:

- `DESCRIBE HISTORY`
- `VERSION AS OF`
- `RESTORE TABLE`

This demonstrates how Delta Lake supports auditing, time travel, and recovery through its append-only transaction log.

The recovery runbook is available here:

```text
docs/recovery_runbook.md
```

---

## Workflow Plan

The pipeline is designed as a multi-task Databricks Job.

Phase 1 workflow:

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

The workflow plan is available here:

```text
docs/workflow_plan.md
```

---

## Phase 2: Inventory Management Pipeline

Phase 2 extends the project from sales analytics into inventory management.

The goal is to combine sales, product, inventory, supplier, and warehouse data to answer business questions such as:

- Which products are selling fast?
- Which products are at risk of going out of stock?
- Which products should be reordered?
- What is the current value of inventory?
- Which suppliers have longer lead times?
- Which warehouses have low available stock?

---

## Phase 2 Medallion Architecture

Phase 2 will follow the Bronze, Silver, and Gold medallion architecture.

### Bronze Layer

The Bronze layer stores raw data from each source with minimal transformation.

Planned Bronze tables:

```text
workspace.retail_capstone.bronze_sales
workspace.retail_capstone.bronze_products
workspace.retail_capstone.bronze_inventory
workspace.retail_capstone.bronze_suppliers
workspace.retail_capstone.bronze_warehouses
```

Purpose of Bronze:

- Preserve original source data
- Store raw data safely in Delta format
- Keep history for audit and reprocessing
- Avoid applying business logic too early

### Silver Layer

The Silver layer stores cleaned and standardized data.

Planned Silver tables:

```text
workspace.retail_capstone.silver_sales
workspace.retail_capstone.silver_products
workspace.retail_capstone.silver_inventory
workspace.retail_capstone.silver_suppliers
workspace.retail_capstone.silver_warehouses
```

Silver transformations include:

- Standardizing column names
- Casting data types
- Removing invalid records
- Removing duplicate records
- Filtering cancelled transactions
- Calculating revenue
- Calculating available stock

### Gold Layer

The Gold layer stores business-ready analytics tables.

Planned Gold tables:

```text
workspace.retail_capstone.gold_inventory_status
workspace.retail_capstone.gold_product_sales_velocity
workspace.retail_capstone.gold_stockout_risk
workspace.retail_capstone.gold_reorder_recommendations
workspace.retail_capstone.gold_inventory_value
```

Gold tables are designed for dashboards, reporting, and business decisions.

---

## Phase 2 Inventory KPIs

The inventory management pipeline will calculate the following KPIs:

| KPI | Description |
|---|---|
| Current Stock | Units physically available in the warehouse |
| Reserved Stock | Units already reserved for orders |
| Available Stock | Current stock minus reserved stock |
| Sales Velocity | Average quantity sold per day |
| Days of Inventory Remaining | Available stock divided by average daily sales |
| Stockout Risk | Risk level based on inventory remaining and supplier lead time |
| Reorder Flag | Indicates whether a product should be reordered |
| Inventory Value | Current stock multiplied by unit cost |
| Slow-Moving Products | Products with low sales activity |
| Fast-Moving Products | Products with high sales activity |

---

## Stockout Risk Logic

Stockout risk is calculated using available stock, average daily sales, and supplier lead time.

Example logic:

```text
If days_of_inventory_remaining <= lead_time_days:
    High Risk

If days_of_inventory_remaining <= lead_time_days + 7:
    Medium Risk

Otherwise:
    Low Risk
```

---

## Phase 2 Pipeline Flow

```text
Multiple Source Data
        ↓
Bronze Delta Tables
        ↓
Silver Cleaned Tables
        ↓
Gold Inventory KPI Tables
        ↓
Dashboards / Reports / Alerts
```

---

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

---

## Documentation

The `docs` folder contains project documentation.

```text
docs/recovery_runbook.md
docs/workflow_plan.md
docs/architecture.md
docs/data_quality_rules.md
```

### Recovery Runbook

```text
docs/recovery_runbook.md
```

Explains how to use Delta Lake history, time travel, and restore commands for recovery.

### Workflow Plan

```text
docs/workflow_plan.md
```

Explains how the pipeline can be scheduled as a multi-task Databricks Job.

### Architecture

```text
docs/architecture.md
```

Explains the Phase 2 inventory management pipeline design and medallion architecture.

### Data Quality Rules

```text
docs/data_quality_rules.md
```

Defines data quality rules for sales, product, inventory, supplier, and warehouse data.

---

## Repository Structure

```text
databricks-ecommerce-lakehouse-capstone/
│
├── README.md
│
├── notebooks/
│   └── ecommerce_lakehouse_capstone.py
│
├── docs/
│   ├── architecture.md
│   ├── data_quality_rules.md
│   ├── recovery_runbook.md
│   └── workflow_plan.md
│
└── sql/
    ├── dashboard_queries.sql
    └── delta_time_travel.sql
```

---

## Current Status

### Completed

- Phase 1 sales lakehouse pipeline
- Raw Delta table
- Curated Delta table
- Summary Delta tables
- Incremental `MERGE INTO`
- Delta Lake time travel validation
- Recovery runbook
- Workflow plan
- Phase 2 architecture document
- Phase 2 data quality rules

### In Progress

- Phase 2 inventory management pipeline
- Bronze/Silver/Gold implementation
- Inventory KPI tables
- Reorder recommendation logic
