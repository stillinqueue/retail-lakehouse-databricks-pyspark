# Databricks eCommerce Lakehouse Capstone

## Project Overview

This project demonstrates an end-to-end lakehouse pipeline built on Databricks using a real public eCommerce dataset.

The pipeline loads online retail transaction data into Delta tables, cleans and transforms it with PySpark, creates curated and summary analytics tables, performs incremental upserts with `MERGE`, and documents recovery using Delta Lake time travel.

The project is extended in phases:

- **Phase 1** focuses on sales transaction analytics.
- **Phase 2** extends the project into an inventory management pipeline using multiple data sources and medallion architecture.
- **Phase 3** extends the project with MLflow experiment tracking, model registration, and Databricks Model Serving.

---

## Project Phases

### Phase 1: Sales Lakehouse Capstone

The first phase builds a Databricks Lakehouse pipeline for eCommerce sales transactions.

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

### Phase 3: MLflow Classification and Model Serving

The third phase extends the inventory pipeline with machine learning.

It adds:

- ML training feature tables
- Exploratory data analysis graphs
- Stockout risk classification model
- Reorder flag classification model
- MLflow experiment tracking
- Model comparison using accuracy and weighted F1 score
- Feature importance graphs
- Unity Catalog Model Registry
- Model aliases: `@challenger` and `@champion`
- Databricks Model Serving endpoint
- Model serving test notebook
- Endpoint request and response documentation

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

Sources:

```text
Sales transactions
Product master data
Inventory stock data
Supplier data
Warehouse data
```

These sources are combined to support inventory analytics and stock management use cases.

### Phase 3 ML Training Data

Phase 3 uses Gold inventory KPI tables from Phase 2 to create ML training feature tables.

ML training tables:

```text
workspace.retail_capstone.ml_stockout_training_data
workspace.retail_capstone.ml_reorder_training_data
```

The ML training data is synthetically expanded from the inventory business rules for learning and demonstration purposes.

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
- MLflow
- scikit-learn
- Pandas
- Matplotlib
- Unity Catalog Model Registry
- Databricks Model Serving

---

# Phase 1: Sales Lakehouse Pipeline

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

# Phase 2: Inventory Management Medallion Pipeline

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

Phase 2 follows the Bronze, Silver, and Gold medallion architecture.

```text
Bronze → Silver → Gold
```

---

## Bronze Layer

The Bronze layer stores raw data from each source with minimal transformation.

Bronze tables:

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

---

## Silver Layer

The Silver layer stores cleaned and standardized data.

Silver tables:

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

---

## Gold Layer

The Gold layer stores business-ready analytics tables.

Gold tables:

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

The inventory management pipeline calculates the following KPIs:

| KPI | Description |
|---|---|
| Current Stock | Units physically available in the warehouse |
| Reserved Stock | Units already reserved for orders |
| Available Stock | Current stock minus reserved stock |
| Sales Velocity | Average quantity sold per day |
| Days of Inventory Remaining | Available stock divided by average daily sales |
| Stockout Risk | Risk level based on inventory remaining and supplier lead time |
| Reorder Flag | Indicates whether a product should be reordered |
| Recommended Reorder Quantity | Suggested reorder quantity based on product rules |
| Inventory Value | Current stock multiplied by unit cost |

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

If a product has no recent sales, it is marked as:

```text
No Recent Sales
```

---

## Reorder Recommendation Logic

A product is marked for reorder when available stock is less than or equal to the reorder level.

Example logic:

```text
If available_stock <= reorder_level:
    Reorder Needed

Otherwise:
    No Reorder Needed
```

The recommended reorder quantity comes from the product master data.

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

# Phase 3: MLflow Classification and Model Serving

Phase 3 extends the inventory pipeline with machine learning.

The goal is to demonstrate how Gold tables from the lakehouse can be used to train, track, compare, register, and serve machine learning models.

---

## Phase 3 ML Use Cases

Two classification models are trained.

### Model 1: Stockout Risk Classifier

This model predicts the stockout risk level of a product.

Target column:

```text
stockout_risk_level
```

Example classes:

```text
High Risk
Medium Risk
Low Risk
No Recent Sales
```

Example features:

```text
available_stock
avg_daily_sales
days_of_inventory_remaining
lead_time_days
reliability_score
category
warehouse_id
```

Business value:

- Helps identify products likely to go out of stock
- Supports proactive inventory planning
- Helps prioritize replenishment actions

---

### Model 2: Reorder Flag Classifier

This model predicts whether a product should be reordered.

Target column:

```text
reorder_flag
```

Example classes:

```text
Reorder Needed
No Reorder Needed
```

Example features:

```text
available_stock
reorder_level
reorder_quantity
lead_time_days
category
warehouse_id
```

Business value:

- Supports automated reorder decisions
- Helps inventory teams prioritize purchase orders
- Reduces manual review effort

---

## Phase 3 ML Pipeline Steps

### 1. Load Gold Tables

The ML notebook loads Gold inventory tables created in Phase 2.

Main source tables:

```text
workspace.retail_capstone.gold_stockout_risk
workspace.retail_capstone.gold_reorder_recommendations
workspace.retail_capstone.gold_inventory_value
workspace.retail_capstone.gold_product_sales_velocity
```

### 2. Create ML Training Tables

The project creates ML feature tables:

```text
workspace.retail_capstone.ml_stockout_training_data
workspace.retail_capstone.ml_reorder_training_data
```

Because the sample inventory data is small, the training data is synthetically expanded from inventory business rules for learning and demonstration purposes.

### 3. Exploratory Data Analysis

The ML training notebook includes graphs for:

- Stockout risk label distribution
- Reorder flag distribution
- Available stock distribution
- Average daily sales distribution
- Days of inventory remaining distribution
- Available stock vs average daily sales by risk level

These visualizations help explain the ML problem and business scenario.

### 4. MLflow Experiment Tracking

MLflow is used to track model experiments.

The project trains three model configurations for the stockout classifier:

- Logistic Regression
- Random Forest Classifier
- Gradient Boosting Classifier

The project also trains three model configurations for the reorder classifier:

- Logistic Regression
- Random Forest Classifier
- Gradient Boosting Classifier

That gives six MLflow runs in total.

Tracked metrics include:

```text
accuracy
f1_weighted
```

Weighted F1 score is used because classification datasets can become imbalanced.

### 5. Model Comparison

The notebook compares MLflow runs using:

- Model type
- Hyperparameters
- Accuracy
- Weighted F1 score

The best model for each use case is selected based on weighted F1 score.

### 6. Feature Importance

Feature importance graphs are created to explain which input features influence model predictions.

For stockout risk, important features may include:

- Days of inventory remaining
- Available stock
- Average daily sales
- Supplier lead time
- Supplier reliability

For reorder prediction, important features may include:

- Available stock
- Reorder level
- Reorder quantity
- Supplier lead time

### 7. Unity Catalog Model Registry

The best models are registered in Unity Catalog Model Registry.

Registered model names:

```text
workspace.retail_capstone.stockout_risk_classifier
workspace.retail_capstone.reorder_flag_classifier
```

Model aliases are used for lifecycle management:

```text
@challenger
@champion
```

The new model version is first assigned the `@challenger` alias.  
After validation, it is promoted to the `@champion` alias.

### 8. Model Serving Test

The serving test notebook loads the `@champion` models and sends sample input records for prediction.

Model URIs:

```text
models:/workspace.retail_capstone.stockout_risk_classifier@champion
models:/workspace.retail_capstone.reorder_flag_classifier@champion
```

This confirms that registered champion models can be loaded and used for inference.

### 9. Databricks Model Serving Endpoint

The stockout risk classifier is deployed to a Databricks Model Serving endpoint.

Endpoint name:

```text
stockout-risk-classifier-endpoint
```

Served model:

```text
workspace.retail_capstone.stockout_risk_classifier@champion
```

The endpoint is tested with sample inventory feature records and returns stockout risk predictions.

---

## Model Serving Evidence

The stockout risk classifier was deployed to a Databricks Model Serving endpoint.

Endpoint name:

```text
stockout-risk-classifier-endpoint
```

Served model:

```text
workspace.retail_capstone.stockout_risk_classifier@champion
```

Evidence screenshots:

```text
images/model_serving_endpoint_ready.png
images/stockout_endpoint_response.png
```

The endpoint was tested with sample inventory feature records and returned stockout risk predictions.

---

## Phase 3 Pipeline Flow

```text
Gold Inventory KPI Tables
        ↓
ML Feature Tables
        ↓
EDA and Feature Analysis
        ↓
MLflow Experiments
        ↓
Model Comparison
        ↓
Unity Catalog Model Registry
        ↓
Champion Model Serving Test
        ↓
Databricks Model Serving Endpoint
```

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

Phase 2 workflow:

```text
Ingest Multiple Sources
      ↓
Create Bronze Tables
      ↓
Create Silver Tables
      ↓
Create Gold KPI Tables
      ↓
Dashboard / Alerts
```

Phase 3 workflow:

```text
Gold KPI Tables
      ↓
Create ML Feature Tables
      ↓
Train and Track Models
      ↓
Register Best Models
      ↓
Test Champion Models
      ↓
Deploy Model Serving Endpoint
```

Dedicated job compute is recommended for production scheduling because it provides reliability, isolation, and cost control.

The workflow plan is available here:

```text
docs/workflow_plan.md
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

### Inventory KPI Queries

The `sql/inventory_kpi_queries.sql` file contains reporting queries for:

- Inventory status
- Product sales velocity
- Stockout risk
- Reorder recommendations
- Inventory value

### ML Feature Queries

The `sql/ml_feature_queries.sql` file contains queries for:

- Stockout ML training features
- Reorder ML training features
- Stockout label distribution
- Reorder label distribution

---

## Documentation

The `docs` folder contains project documentation.

```text
docs/recovery_runbook.md
docs/workflow_plan.md
docs/architecture.md
docs/data_quality_rules.md
docs/ml_modeling_plan.md
docs/model_serving.md
docs/enterprise_readiness.md
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

### ML Modeling Plan

```text
docs/ml_modeling_plan.md
```

Explains the Phase 3 ML use cases, features, targets, experiment tracking plan, and model selection approach.

### Model Serving Plan

```text
docs/model_serving.md
```

Explains how the registered models can be served and tested for inference.

### Enterprise Readiness

```text
docs/enterprise_readiness.md
```

Explains workspace organization, Git integration, reproducibility, and production readiness considerations.

---

## Notebook Formats

The `notebooks/` folder includes both `.py` and `.ipynb` versions of the Databricks notebooks.

- `.py` files are useful for source control and code review.
- `.ipynb` files are useful for viewing notebook outputs, markdown, and charts.

---

## Repository Structure

```text
databricks-ecommerce-lakehouse-capstone/
│
├── README.md
├── notebooks/
│   ├── ecommerce_lakehouse_capstone.py
│   ├── 01_bronze_ingestion.py
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_cleaning.py
│   ├── 02_silver_cleaning.ipynb
│   ├── 03_gold_inventory_kpis.py
│   ├── 03_gold_inventory_kpis.ipynb
│   ├── 04_ml_stockout_and_reorder_training.py
│   ├── 04_ml_stockout_and_reorder_training.ipynb
│   ├── 05_model_serving_test.py
│   └── 05_model_serving_test.ipynb
│
├── docs/
│   ├── architecture.md
│   ├── data_quality_rules.md
│   ├── recovery_runbook.md
│   ├── workflow_plan.md
│   ├── ml_modeling_plan.md
│   ├── model_serving.md
│   └── enterprise_readiness.md
│
├── sql/
│   ├── dashboard_queries.sql
│   ├── delta_time_travel.sql
│   ├── inventory_kpi_queries.sql
│   └── ml_feature_queries.sql
│
├── data_samples/
│   ├── products_sample.csv
│   ├── inventory_sample.csv
│   ├── suppliers_sample.csv
│   └── warehouses_sample.csv
│
└── images/
    ├── model_serving_endpoint_ready.png
    └── stockout_endpoint_response.png
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
- Phase 2 sample data sources
- Bronze layer implementation
- Silver layer implementation
- Gold inventory KPI tables
- Inventory KPI SQL queries
- Phase 3 ML modeling plan
- Phase 3 model serving plan
- Phase 3 enterprise readiness notes
- ML training feature tables
- EDA graphs for ML understanding
- Stockout risk classification model
- Reorder flag classification model
- Six MLflow experiment runs
- Model comparison using weighted F1 score
- Feature importance graphs
- Unity Catalog model registration
- Model aliases: `@challenger` and `@champion`
- Champion model serving test notebook
- Databricks Model Serving endpoint created
- Endpoint tested with sample request payload
- Endpoint response documented
- Model serving screenshots added

### Possible Future Improvements

- Convert the pipeline into Delta Live Tables
- Add automated data quality expectations
- Add streaming ingestion with Spark Structured Streaming
- Add Databricks SQL dashboards
- Add reorder model serving endpoint
- Add MLflow experiment screenshots
- Add architecture diagram image
- Add product recommendation features
- Add inventory forecasting model
- Add CI/CD workflow for notebook deployment
- Add model monitoring and drift detection
- Add Hugging Face or local MLflow model export workflow
- Add sovereign/local inference comparison as an advanced extension
