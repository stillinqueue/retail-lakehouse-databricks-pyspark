# Architecture: Inventory Management Lakehouse Pipeline

## Project Extension

This is Phase 2 of the Databricks eCommerce Lakehouse project.

Phase 1 focused on building a sales transaction pipeline using a real online retail dataset. Phase 2 extends the project into an inventory management pipeline using multiple data sources and medallion architecture.

## Goal

The goal of this extension is to build a realistic inventory analytics pipeline for an eCommerce business.

The pipeline combines sales, product, inventory, supplier, and warehouse data to answer business questions such as:

- Which products are selling fast?
- Which products are at risk of going out of stock?
- Which products need to be reordered?
- What is the value of current inventory?
- Which suppliers have longer lead times?
- Which warehouses have low available stock?

## Data Sources

The project uses multiple data sources.

### 1. Sales Transactions

Source: UCI Online Retail dataset

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


The goal of this extension is to build a realistic inventory analytics pipeline for an eCommerce business.

The pipeline combines sales, product, inventory, supplier, and warehouse data to answer business questions such as:

- Which products are selling fast?
- Which products are at risk of going out of stock?
- Which products need to be reordered?
- What is the value of current inventory?
- Which suppliers have longer lead times?
- Which warehouses have low available stock?

## Data Sources

The project uses multiple data sources.

### 1. Sales Transactions

Source: UCI Online Retail dataset

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

Purpose:

Sales data shows which products were sold, when they were sold, how many units were sold, and how much revenue was generated.

### 2. Product Master Data

Example columns:

```text
stock_code
product_name
category
brand
supplier_id
unit_cost
reorder_level
reorder_quantity
```

Purpose:

Product master data describes each product and defines reorder rules.

### 3. Inventory Data

Example columns:

```text
stock_code
warehouse_id
current_stock
reserved_stock
last_stock_update
```

Purpose:

Inventory data shows how much stock is currently available in each warehouse.

### 4. Supplier Data

Example columns:

```text
supplier_id
supplier_name
supplier_country
lead_time_days
reliability_score
```

Purpose:

Supplier data helps calculate reorder risk and understand supplier performance.

### 5. Warehouse Data

Example columns:

```text
warehouse_id
warehouse_name
warehouse_country
region
capacity
```

Purpose:

Warehouse data gives context about where inventory is stored.

## Medallion Architecture

The project follows the Bronze, Silver, and Gold medallion architecture.

The architecture separates the pipeline into three layers:

```text
Bronze → Silver → Gold
```

Each layer has a clear responsibility.

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
- Keep raw history for reprocessing
- Store data safely in Delta format
- Avoid applying business logic too early
- Support audit and debugging

Example:

Sales transaction data from the UCI Online Retail dataset is first stored as a Bronze Delta table before any cleaning or business transformation is applied.

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
- Applying data quality rules

Example:

In the Silver sales table, cancelled invoices are removed, invalid quantities are filtered out, and revenue is calculated using quantity multiplied by unit price.

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

Gold tables combine cleaned Silver data from sales, products, inventory, suppliers, and warehouses.

## Inventory KPIs

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
| Supplier Lead Time Risk | Risk caused by long supplier delivery time |

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

This helps identify products that may run out before the supplier can deliver new stock.

## Reorder Recommendation Logic

The reorder recommendation uses available stock and reorder level.

Example logic:

```text
If available_stock <= reorder_level:
    Reorder Needed

Otherwise:
    No Reorder Needed
```

The recommended reorder quantity comes from the product master data.

## Inventory Value Logic

Inventory value is calculated as:

```text
current_stock * unit_cost
```

This helps estimate how much money is currently tied up in inventory.

## Pipeline Flow

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

## Table Flow

```text
bronze_sales
        ↓
silver_sales
        ↓
gold_product_sales_velocity

bronze_products
        ↓
silver_products
        ↓
gold_inventory_status

bronze_inventory
        ↓
silver_inventory
        ↓
gold_stockout_risk

bronze_suppliers
        ↓
silver_suppliers
        ↓
gold_reorder_recommendations

bronze_warehouses
        ↓
silver_warehouses
        ↓
gold_inventory_value
```

## Incremental Processing

The project can use Delta Lake `MERGE INTO` for incremental updates.

Example use cases:

- New sales transactions arrive daily
- Product information changes
- Inventory stock levels are updated
- Supplier lead times change
- Warehouse information is corrected

`MERGE INTO` makes the pipeline idempotent because existing records can be updated and new records can be inserted without creating duplicates.

## Data Quality

Data quality rules are applied in the Silver layer before building Gold tables.

Examples:

- Sales quantity must be greater than 0
- Unit price must be greater than 0
- Stock code must not be null
- Supplier ID must not be null
- Current stock must not be negative
- Reserved stock must not exceed current stock
- Lead time must be greater than 0

The detailed rules are documented in:

```text
docs/data_quality_rules.md
```

## Why This Architecture Is Useful

This architecture separates raw data, cleaned data, and business-ready data.

This makes the pipeline easier to understand, maintain, debug, and scale.

The Bronze layer supports audit and reprocessing.

The Silver layer supports data quality and standardization.

The Gold layer supports business analytics and decision-making.

## Business Value

This inventory management pipeline can help an eCommerce company:

- Reduce stockouts
- Improve reorder decisions
- Understand fast-moving and slow-moving products
- Track inventory value
- Improve supplier planning
- Build reliable inventory dashboards
- Support data-driven operations

## Interview Relevance

This project extension demonstrates practical data engineering skills relevant to modern eCommerce analytics:

- Multiple data source ingestion
- Medallion architecture
- Delta Lake table design
- PySpark transformations
- Data quality checks
- Incremental processing with MERGE
- Inventory KPI modeling
- Production-style pipeline design
