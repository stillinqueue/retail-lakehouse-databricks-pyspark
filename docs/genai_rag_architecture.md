# GenAI RAG Architecture

## Purpose

This document describes Phase 4 of the Databricks eCommerce Lakehouse project.

Phase 4 extends the project with a Retrieval-Augmented Generation style inventory assistant.

The assistant retrieves relevant inventory, supplier, product, sales velocity, stockout risk, and reorder information from Gold tables and uses the retrieved context to answer business questions.

## Business Goal

The goal is to help inventory and operations teams answer questions such as:

- Which products are at high stockout risk?
- Which products should be reordered?
- Why is a product considered risky?
- Which suppliers have long lead times?
- Which warehouses have low available stock?
- Which products have high sales velocity and low inventory?
- Explain the reorder recommendation for a specific product.

## Source Tables

The RAG assistant uses Gold tables created in Phase 2.

```text
workspace.retail_capstone.gold_inventory_status
workspace.retail_capstone.gold_product_sales_velocity
workspace.retail_capstone.gold_stockout_risk
workspace.retail_capstone.gold_reorder_recommendations
workspace.retail_capstone.gold_inventory_value
RAG Document Table

The first step is to convert structured Gold records into text documents.

Target table:

workspace.retail_capstone.inventory_rag_documents

Each row represents a product and warehouse inventory record as natural language context.

The document table will contain fields such as:

Column	Description
product_id	Unique product identifier
description	Product description
warehouse_id	Warehouse where the product is stored
supplier_id	Supplier associated with the product
rag_document	Natural language inventory context
risk_level	Stockout risk classification
reorder_flag	Whether the product should be reordered
priority_score	Business priority score used for ranking

Example document:

Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, is stored in warehouse WH001.
Available stock is 210 units.
Average daily sales is 8.5 units.
Days of inventory remaining is 24.7.
Supplier lead time is 7 days.
Stockout risk level is Low Risk.
Reorder flag is No Reorder Needed.
Business Priority Score

The project uses a business priority score to rank risky inventory records higher.

This score helps the RAG assistant prioritize products that are more operationally important.

Example risk score mapping:

Risk Level	Risk Score
High Risk	1.0
Medium Risk	0.75
Low Risk	0.4
No Recent Sales	0.2

The priority score can also consider supplier reliability.

Example formula:

priority_score = risk_score * (1 + (1 - reliability_score))

This increases the priority of risky products supplied by less reliable suppliers.

For example:

risk_score = 1.0
reliability_score = 0.80

priority_score = 1.0 * (1 + (1 - 0.80))
priority_score = 1.2

This means a high-risk product from a less reliable supplier receives a stronger ranking boost.

Retrieval and Ranking

The retrieval process combines text relevance and business priority.

The first implementation uses keyword-style retrieval in Spark SQL for learning and portability.

A later version can add Databricks Vector Search or embedding-based retrieval.

Example ranking formula:

final_score = retrieval_score * (0.6 + 0.4 * priority_score)

This means a result must be relevant to the query, but high-risk inventory records receive a ranking boost.

Example interpretation:

Retrieval Score	Priority Score	Final Score
0.90	1.00	0.90
0.90	0.75	0.81
0.90	0.40	0.684
0.90	0.20	0.612
Pipeline Flow
Gold Inventory Tables
        ↓
Inventory RAG Documents
        ↓
Keyword / Vector Retrieval
        ↓
Business Priority Scoring
        ↓
Reranked Context
        ↓
LLM Answer Generation
        ↓
Evaluation
Initial Implementation

The first implementation uses Spark SQL and Delta tables.

The main goal is to keep the RAG pipeline simple, explainable, and easy to run inside Databricks.

The first version includes:

Creating inventory RAG documents from Gold tables
Storing RAG documents in a Delta table
Searching documents using SQL keyword filters
Ranking results using business priority score
Returning relevant context for answer generation
Preparing the project for future Databricks Vector Search integration
Example User Questions

The assistant should be able to answer questions such as:

Which products are at high stockout risk?
Which products should be reordered this week?
Why is product 85123A marked as high risk?
Which supplier has long lead times?
Show products with low stock but high sales velocity.
Explain the reorder recommendation for product 85123A.
Which warehouse has the highest inventory risk?
Future Improvements

Future improvements can include:

Add Databricks Vector Search
Add an embedding model endpoint
Add LLM answer generation with a foundation model endpoint
Add feedback collection from users
Add evaluation with LLM-as-judge
Add monitoring for retrieval quality and answer quality
Add a dashboard for RAG assistant usage
Add permissions and governance through Unity Catalog
