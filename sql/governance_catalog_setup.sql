-- ============================================================
-- Governance Catalog and Schema Setup
-- Databricks eCommerce Lakehouse Capstone
-- Phase 5: Production Governance and MLOps
-- ============================================================

-- NOTE:
-- In a production workspace, this project can use a dedicated governed catalog.
-- Example catalog: retail_governance
--
-- If your workspace does not allow catalog creation, use the existing:
-- workspace.retail_capstone

-- ============================================================
-- Option A: Recommended production catalog design
-- ============================================================

-- Create governed catalog
-- Run only if you have permissions to create catalogs.

CREATE CATALOG IF NOT EXISTS retail_governance
COMMENT 'Governed catalog for the Databricks eCommerce Lakehouse Capstone project';

-- Create schemas for medallion and ML/GenAI layers

CREATE SCHEMA IF NOT EXISTS retail_governance.bronze
COMMENT 'Raw source data layer for sales, product, inventory, supplier, and warehouse data';

CREATE SCHEMA IF NOT EXISTS retail_governance.silver
COMMENT 'Cleaned and standardized data layer';

CREATE SCHEMA IF NOT EXISTS retail_governance.gold
COMMENT 'Business-ready analytics and KPI layer';

CREATE SCHEMA IF NOT EXISTS retail_governance.ml
COMMENT 'Machine learning feature tables and model training datasets';

CREATE SCHEMA IF NOT EXISTS retail_governance.genai
COMMENT 'GenAI and RAG document tables';

-- Create volume for raw or unstructured files

CREATE VOLUME IF NOT EXISTS retail_governance.bronze.inventory_files
COMMENT 'Volume for raw inventory files, sample data, and unstructured artifacts';

-- ============================================================
-- Option B: Current learning workspace adaptation
-- ============================================================

-- Existing project schema:
-- workspace.retail_capstone

CREATE SCHEMA IF NOT EXISTS workspace.retail_capstone
COMMENT 'Learning workspace schema for the Databricks eCommerce Lakehouse Capstone project';

CREATE VOLUME IF NOT EXISTS workspace.retail_capstone.inventory_files
COMMENT 'Volume for raw inventory files and unstructured artifacts used by the capstone project';

-- ============================================================
-- Table and Column Comments for Existing Project Tables
-- ============================================================

COMMENT ON TABLE workspace.retail_capstone.bronze_sales IS
'Bronze table containing raw online retail sales transaction data';

COMMENT ON TABLE workspace.retail_capstone.bronze_products IS
'Bronze table containing raw product master data';

COMMENT ON TABLE workspace.retail_capstone.silver_sales IS
'Silver table containing cleaned and standardized sales transactions';

COMMENT ON TABLE workspace.retail_capstone.silver_inventory IS
'Silver table containing cleaned inventory stock records with available stock calculation';

COMMENT ON TABLE workspace.retail_capstone.gold_stockout_risk IS
'Gold table containing product-level stockout risk metrics and supplier lead time context';

COMMENT ON TABLE workspace.retail_capstone.gold_reorder_recommendations IS
'Gold table containing reorder recommendation logic for inventory planning';

-- Column comments for important Gold table columns

ALTER TABLE workspace.retail_capstone.gold_stockout_risk
ALTER COLUMN stock_code COMMENT 'Product identifier from the source retail dataset';

ALTER TABLE workspace.retail_capstone.gold_stockout_risk
ALTER COLUMN product_name COMMENT 'Human-readable product name';

ALTER TABLE workspace.retail_capstone.gold_stockout_risk
ALTER COLUMN warehouse_id COMMENT 'Warehouse identifier where the product is stored';

ALTER TABLE workspace.retail_capstone.gold_stockout_risk
ALTER COLUMN available_stock COMMENT 'Current stock minus reserved stock';

ALTER TABLE workspace.retail_capstone.gold_stockout_risk
ALTER COLUMN avg_daily_sales COMMENT 'Average quantity sold per active sales day';

ALTER TABLE workspace.retail_capstone.gold_stockout_risk
ALTER COLUMN days_of_inventory_remaining COMMENT 'Estimated number of days current available stock can support demand';

ALTER TABLE workspace.retail_capstone.gold_stockout_risk
ALTER COLUMN lead_time_days COMMENT 'Supplier lead time in days';

ALTER TABLE workspace.retail_capstone.gold_stockout_risk
ALTER COLUMN stockout_risk_level COMMENT 'Business risk label: High Risk, Medium Risk, Low Risk, or No Recent Sales';

-- Column comments for reorder recommendation table

ALTER TABLE workspace.retail_capstone.gold_reorder_recommendations
ALTER COLUMN stock_code COMMENT 'Product identifier from the source retail dataset';

ALTER TABLE workspace.retail_capstone.gold_reorder_recommendations
ALTER COLUMN available_stock COMMENT 'Current stock available after subtracting reserved stock';

ALTER TABLE workspace.retail_capstone.gold_reorder_recommendations
ALTER COLUMN reorder_level COMMENT 'Threshold below which reorder should be considered';

ALTER TABLE workspace.retail_capstone.gold_reorder_recommendations
ALTER COLUMN recommended_reorder_quantity COMMENT 'Suggested reorder quantity from product master data';

ALTER TABLE workspace.retail_capstone.gold_reorder_recommendations
ALTER COLUMN reorder_flag COMMENT 'Indicates whether reorder is needed';
