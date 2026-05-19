-- ============================================================
-- SHOW GRANTS Verification Queries
-- Databricks eCommerce Lakehouse Capstone
-- Phase 5: Production Governance and MLOps
-- ============================================================

-- Catalog-level grants

SHOW GRANTS ON CATALOG workspace;

-- Schema-level grants

SHOW GRANTS ON SCHEMA workspace.retail_capstone;

-- Volume-level grants

SHOW GRANTS ON VOLUME workspace.retail_capstone.inventory_files;

-- Table-level grants for important tables

SHOW GRANTS ON TABLE workspace.retail_capstone.gold_stockout_risk;

SHOW GRANTS ON TABLE workspace.retail_capstone.gold_reorder_recommendations;

SHOW GRANTS ON TABLE workspace.retail_capstone.ml_stockout_training_data;

SHOW GRANTS ON TABLE workspace.retail_capstone.inventory_rag_documents;
