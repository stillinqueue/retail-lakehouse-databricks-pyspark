-- Daily revenue by country

SELECT
  invoice_date_only,
  country,
  total_revenue,
  total_orders
FROM workspace.retail_capstone.daily_country_revenue
ORDER BY invoice_date_only DESC, total_revenue DESC;


-- Product revenue summary

SELECT
  stock_code,
  product_description,
  total_revenue,
  total_quantity_sold,
  total_orders
FROM workspace.retail_capstone.product_revenue_summary
ORDER BY total_revenue DESC;


-- Customer summary

SELECT
  customer_id,
  country,
  customer_total_revenue,
  customer_total_orders,
  customer_total_quantity
FROM workspace.retail_capstone.customer_summary
ORDER BY customer_total_revenue DESC;
