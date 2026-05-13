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
