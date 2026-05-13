# Data Quality Rules

## Purpose

This document defines the data quality rules for the inventory management lakehouse pipeline.

Data quality checks are applied mainly in the Silver layer before creating Gold analytics tables.

## Sales Data Rules

Sales records should meet the following conditions:

| Rule | Reason |
|---|---|
| Invoice number must not be null | Required to identify a transaction |
| Stock code must not be null | Required to identify a product |
| Quantity must be greater than 0 | Negative or zero quantities are invalid for completed sales |
| Unit price must be greater than 0 | Revenue calculation requires valid price |
| Customer ID must not be null | Required for customer-level analytics |
| Cancelled invoices should be excluded | Cancelled transactions should not count as sales |

## Product Data Rules

Product records should meet the following conditions:

| Rule | Reason |
|---|---|
| Stock code must not be null | Required product key |
| Product name must not be null | Required for reporting |
| Category must not be null | Required for category analytics |
| Supplier ID must not be null | Required for supplier linkage |
| Unit cost must be greater than 0 | Required for inventory value calculation |
| Reorder level must be greater than or equal to 0 | Required for reorder logic |
| Reorder quantity must be greater than 0 | Required for reorder recommendation |

## Inventory Data Rules

Inventory records should meet the following conditions:

| Rule | Reason |
|---|---|
| Stock code must not be null | Required to join with products |
| Warehouse ID must not be null | Required to locate inventory |
| Current stock must be greater than or equal to 0 | Stock cannot be negative |
| Reserved stock must be greater than or equal to 0 | Reserved stock cannot be negative |
| Reserved stock should not exceed current stock | Available stock cannot be negative |
| Last stock update must not be null | Required for freshness tracking |

## Supplier Data Rules

Supplier records should meet the following conditions:

| Rule | Reason |
|---|---|
| Supplier ID must not be null | Required supplier key |
| Supplier name must not be null | Required for reporting |
| Lead time days must be greater than 0 | Required for stockout risk calculation |
| Reliability score should be between 0 and 1 | Required for supplier performance analytics |

## Warehouse Data Rules

Warehouse records should meet the following conditions:

| Rule | Reason |
|---|---|
| Warehouse ID must not be null | Required warehouse key |
| Warehouse name must not be null | Required for reporting |
| Region must not be null | Required for regional inventory analytics |
| Capacity must be greater than 0 | Required for warehouse utilization analysis |

## Silver Layer Cleaning Approach

The Silver layer applies the data quality rules by:

- Selecting only required columns
- Renaming columns into a consistent format
- Casting fields into correct data types
- Filtering invalid records
- Removing duplicates
- Creating derived fields such as revenue and available stock

## Gold Layer Protection

Gold tables should only be built from Silver tables.

This ensures that business dashboards and KPI tables are created from cleaned and trusted data.
