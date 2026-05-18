# ML Modeling Plan

## Purpose

This document explains Phase 3 of the Databricks eCommerce Lakehouse project.

Phase 3 extends the inventory management pipeline with machine learning models for stockout risk prediction and reorder recommendation.

The goal is to demonstrate how business-ready Gold tables from the lakehouse can be used to train, track, compare, register, and serve machine learning models.

## Business Context

In an eCommerce business, inventory availability is critical. If a product goes out of stock, the business may lose revenue and customer trust. If a product is overstocked, the company may waste warehouse space and working capital.

This phase uses inventory, sales velocity, supplier, and warehouse data to support two ML use cases:

1. Predict stockout risk level
2. Predict whether a product needs reorder

## Source Tables

The ML pipeline uses Gold tables created in Phase 2.

```text
workspace.retail_capstone.gold_stockout_risk
workspace.retail_capstone.gold_reorder_recommendations
workspace.retail_capstone.gold_inventory_value
workspace.retail_capstone.gold_product_sales_velocity
