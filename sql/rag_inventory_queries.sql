-- View inventory RAG documents

SELECT
  document_id,
  stock_code,
  product_name,
  warehouse_id,
  stockout_risk_level,
  reorder_flag,
  business_priority_score,
  rag_document_text
FROM workspace.retail_capstone.inventory_rag_documents;


-- High stockout risk documents

SELECT
  document_id,
  stock_code,
  product_name,
  warehouse_id,
  stockout_risk_level,
  business_priority_score,
  rag_document_text
FROM workspace.retail_capstone.inventory_rag_documents
WHERE stockout_risk_level = 'High Risk'
ORDER BY business_priority_score DESC;


-- Reorder needed documents

SELECT
  document_id,
  stock_code,
  product_name,
  warehouse_id,
  reorder_flag,
  business_priority_score,
  rag_document_text
FROM workspace.retail_capstone.inventory_rag_documents
WHERE reorder_flag = 'Reorder Needed'
ORDER BY business_priority_score DESC;


-- Top business priority inventory records

SELECT
  document_id,
  stock_code,
  product_name,
  warehouse_id,
  stockout_risk_level,
  reorder_flag,
  business_priority_score
FROM workspace.retail_capstone.inventory_rag_documents
ORDER BY business_priority_score DESC;
