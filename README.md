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


Lakehouse Architecture

The project follows a simple Lakehouse architecture with three layers:

Source Data
    ↓
Raw Delta Table
    ↓
Curated Delta Table
    ↓
Summary Delta Tables
1. Raw Layer

The raw layer stores the original source data in Delta format.

The purpose of the raw table is to preserve the original transaction data before applying business rules or cleaning logic.

Raw table:

workspace.retail_capstone.raw_online_retail

In this layer, the source eCommerce data is saved as a Delta table using schema enforcement.

2. Curated Layer

The curated layer contains cleaned and transformed transaction data.

Curated table:

workspace.retail_capstone.curated_online_retail

Cleaning steps include:

Removing records with null invoice numbers
Removing records with null product codes
Removing records with null product descriptions
Removing records with null customer IDs
Removing cancelled invoices
Removing rows with zero or negative quantity
Removing rows with zero or negative unit price
Converting invoice date into timestamp format
Creating a derived revenue column

Revenue is calculated as:

revenue = quantity * unit_price
3. Summary Layer

The summary layer contains business-ready analytical tables.

Summary tables created in this project:

workspace.retail_capstone.daily_country_revenue
workspace.retail_capstone.product_revenue_summary
workspace.retail_capstone.customer_summary
Daily Country Revenue

This table calculates daily revenue by country.

Metrics:

Invoice date
Country
Total revenue
Total orders
Product Revenue Summary

This table calculates product-level performance.

Metrics:

Stock code
Product description
Total revenue
Total quantity sold
Total orders
Customer Summary

This table calculates customer-level purchasing behavior.

Metrics:

Customer ID
Country
Customer total revenue
Customer total orders
Customer total quantity purchased
Incremental Processing with MERGE

This project uses MERGE INTO to simulate incremental data processing.

The MERGE operation inserts new rows and updates existing rows.

The match condition uses:

InvoiceNo + StockCode

This is because one invoice can contain multiple products, so InvoiceNo alone is not unique enough.

The MERGE logic supports:

Insert path for new transaction lines
Update path for existing transaction lines
Duplicate retry handling from upstream systems
Idempotent pipeline behavior

Delta Lake records the MERGE operation as a single atomic transaction in the Delta log. This means the table is not partially updated. Either the full MERGE succeeds or the table remains unchanged.

Delta Lake Time Travel and Recovery

This project includes Delta Lake recovery concepts using:

DESCRIBE HISTORY
VERSION AS OF
RESTORE TABLE

These commands help audit and recover data after a bad load, incorrect merge, or accidental overwrite.

Example time travel query:

SELECT *
FROM workspace.retail_capstone.raw_online_retail
VERSION AS OF 0;

Example history query:

DESCRIBE HISTORY workspace.retail_capstone.raw_online_retail;

Example restore command:

RESTORE TABLE workspace.retail_capstone.raw_online_retail
TO VERSION AS OF 0;

In this project, the restore command is documented but not executed because the current table is valid.

Workflow Plan

The pipeline is designed as a multi-task Databricks Job.

Recommended task flow:

Ingest Raw Data
      ↓
Clean Data
      ↓
Aggregate Metrics
      ↓
Dashboard Refresh

Task design:

Task	Purpose
Ingest Raw Data	Load source eCommerce data into a raw Delta table
Clean Data	Remove invalid rows and create curated transaction data
Aggregate Metrics	Build daily revenue, product summary, and customer summary tables
Incremental MERGE	Insert new rows and update existing rows from staging data
Dashboard Refresh	Refresh SQL queries for reporting users

Dedicated job compute is recommended for scheduled production pipelines because it provides a reliable and isolated runtime environment. It avoids conflicts with shared development clusters and can be configured specifically for the workload.

SQL Dashboard Queries

The project includes SQL queries for:

Daily revenue by country
Product revenue summary
Customer summary
Delta table history
Delta time travel validation

These queries are stored in the sql/ folder.
