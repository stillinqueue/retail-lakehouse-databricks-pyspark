-- Inventory status

SELECT
  stock_code,
  product_name,
  category,
  warehouse_id,
  warehouse_name,
  current_stock,
  reserved_stock,
  available_stock,
  reorder_level
FROM workspace.retail_capstone.gold_inventory_status
ORDER BY available_stock ASC;


-- Product sales velocity

SELECT
  stock_code,
  product_name,
  category,
  total_quantity_sold,
  active_sales_days,
  avg_daily_sales
FROM workspace.retail_capstone.gold_product_sales_velocity
ORDER BY avg_daily_sales DESC;


-- Stockout risk

SELECT
  stock_code,
  product_name,
  category,
  warehouse_id,
  warehouse_name,
  available_stock,
  avg_daily_sales,
  days_of_inventory_remaining,
  supplier_name,
  lead_time_days,
  stockout_risk_level
FROM workspace.retail_capstone.gold_stockout_risk
ORDER BY
  CASE
    WHEN stockout_risk_level = 'High Risk' THEN 1
    WHEN stockout_risk_level = 'Medium Risk' THEN 2
    WHEN stockout_risk_level = 'Low Risk' THEN 3
    ELSE 4
  END,
  days_of_inventory_remaining ASC;


-- Reorder recommendations

SELECT
  stock_code,
  product_name,
  category,
  warehouse_id,
  warehouse_name,
  available_stock,
  reorder_level,
  recommended_reorder_quantity,
  supplier_name,
  lead_time_days,
  reorder_flag
FROM workspace.retail_capstone.gold_reorder_recommendations
WHERE reorder_flag = 'Reorder Needed'
ORDER BY available_stock ASC;


-- Inventory value

SELECT
  category,
  ROUND(SUM(inventory_value), 2) AS total_inventory_value
FROM workspace.retail_capstone.gold_inventory_value
GROUP BY category
ORDER BY total_inventory_value DESC;
