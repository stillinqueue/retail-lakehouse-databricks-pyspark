-- ============================================================
-- Governance GRANT and REVOKE Statements
-- Databricks eCommerce Lakehouse Capstone
-- Phase 5: Production Governance and MLOps
-- ============================================================

-- NOTE:
-- Replace group and service principal names with names that exist in your workspace.
--
-- Example principals:
-- `data_engineers`
-- `data_analysts`
-- `ml_engineers`
-- `genai_engineers`
-- `retail-capstone-job-sp`

-- ============================================================
-- Catalog-level privileges
-- ============================================================

-- Allow data engineers to use the catalog

GRANT USE CATALOG ON CATALOG workspace TO `data_engineers`;

-- Allow analysts to use the catalog

GRANT USE CATALOG ON CATALOG workspace TO `data_analysts`;

-- Allow ML engineers to use the catalog

GRANT USE CATALOG ON CATALOG workspace TO `ml_engineers`;

-- Allow GenAI engineers to use the catalog

GRANT USE CATALOG ON CATALOG workspace TO `genai_engineers`;

-- ============================================================
-- Schema-level privileges
-- ============================================================

GRANT USE SCHEMA ON SCHEMA workspace.retail_capstone TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA workspace.retail_capstone TO `data_analysts`;
GRANT USE SCHEMA ON SCHEMA workspace.retail_capstone TO `ml_engineers`;
GRANT USE SCHEMA ON SCHEMA workspace.retail_capstone TO `genai_engineers`;

-- ============================================================
-- Table-level privileges
-- ============================================================

-- Data engineers can read and modify tables in the project schema

GRANT SELECT, MODIFY ON SCHEMA workspace.retail_capstone TO `data_engineers`;

-- Analysts can read Gold tables and reporting tables.
-- In a stricter production setup, grant SELECT only on specific Gold tables.

GRANT SELECT ON TABLE workspace.retail_capstone.gold_inventory_status TO `data_analysts`;
GRANT SELECT ON TABLE workspace.retail_capstone.gold_product_sales_velocity TO `data_analysts`;
GRANT SELECT ON TABLE workspace.retail_capstone.gold_stockout_risk TO `data_analysts`;
GRANT SELECT ON TABLE workspace.retail_capstone.gold_reorder_recommendations TO `data_analysts`;
GRANT SELECT ON TABLE workspace.retail_capstone.gold_inventory_value TO `data_analysts`;

-- ML engineers can read Gold and ML feature tables

GRANT SELECT ON TABLE workspace.retail_capstone.gold_stockout_risk TO `ml_engineers`;
GRANT SELECT ON TABLE workspace.retail_capstone.gold_reorder_recommendations TO `ml_engineers`;
GRANT SELECT ON TABLE workspace.retail_capstone.ml_stockout_training_data TO `ml_engineers`;
GRANT SELECT ON TABLE workspace.retail_capstone.ml_reorder_training_data TO `ml_engineers`;

-- GenAI engineers can read Gold and RAG document tables

GRANT SELECT ON TABLE workspace.retail_capstone.gold_stockout_risk TO `genai_engineers`;
GRANT SELECT ON TABLE workspace.retail_capstone.gold_reorder_recommendations TO `genai_engineers`;
GRANT SELECT ON TABLE workspace.retail_capstone.inventory_rag_documents TO `genai_engineers`;

-- ============================================================
-- Volume privileges
-- ============================================================

GRANT READ VOLUME ON VOLUME workspace.retail_capstone.inventory_files TO `data_engineers`;
GRANT WRITE VOLUME ON VOLUME workspace.retail_capstone.inventory_files TO `data_engineers`;

-- ============================================================
-- Service principal permissions
-- ============================================================

-- Replace with your actual service principal application ID or display name.

GRANT USE CATALOG ON CATALOG workspace TO `retail-capstone-job-sp`;
GRANT USE SCHEMA ON SCHEMA workspace.retail_capstone TO `retail-capstone-job-sp`;
GRANT SELECT, MODIFY ON SCHEMA workspace.retail_capstone TO `retail-capstone-job-sp`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME workspace.retail_capstone.inventory_files TO `retail-capstone-job-sp`;

-- ============================================================
-- Example REVOKE statements
-- ============================================================

-- Example: remove write access from analysts.
-- This demonstrates least privilege.

REVOKE MODIFY ON SCHEMA workspace.retail_capstone FROM `data_analysts`;

-- Example: remove volume write access from analysts if it was accidentally granted.

REVOKE WRITE VOLUME ON VOLUME workspace.retail_capstone.inventory_files FROM `data_analysts`;
