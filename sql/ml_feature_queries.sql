-- Stockout ML training feature table

SELECT
  available_stock,
  avg_daily_sales,
  days_of_inventory_remaining,
  lead_time_days,
  reliability_score,
  category,
  warehouse_id,
  stockout_risk_level
FROM workspace.retail_capstone.ml_stockout_training_data;


-- Reorder ML training feature table

SELECT
  available_stock,
  reorder_level,
  reorder_quantity,
  lead_time_days,
  category,
  warehouse_id,
  reorder_flag
FROM workspace.retail_capstone.ml_reorder_training_data;


-- Stockout label distribution

SELECT
  stockout_risk_level,
  COUNT(*) AS record_count
FROM workspace.retail_capstone.ml_stockout_training_data
GROUP BY stockout_risk_level
ORDER BY record_count DESC;


-- Reorder label distribution

SELECT
  reorder_flag,
  COUNT(*) AS record_count
FROM workspace.retail_capstone.ml_reorder_training_data
GROUP BY reorder_flag
ORDER BY record_count DESC;
