# GenAI RAG Architecture

## Purpose

This document describes Phase 4 of the Databricks eCommerce Lakehouse project.

Phase 4 adds a GenAI inventory assistant using a Retrieval-Augmented Generation approach.

The assistant retrieves relevant inventory, supplier, product, sales velocity, stockout risk, and reorder information from Gold tables. It then uses that retrieved context to answer business questions.

## Business Goal

The goal is to help inventory and operations teams answer questions such as:

- Which products are at high stockout risk?
- Which products should be reordered?
- Why is a product considered risky?
- Which suppliers have long lead times?
- Which warehouses have low available stock?
- Which products have high sales velocity and low inventory?

## Source Tables

The RAG assistant uses Gold tables created in Phase 2.

```text
workspace.retail_capstone.gold_inventory_status
workspace.retail_capstone.gold_product_sales_velocity
workspace.retail_capstone.gold_stockout_risk
workspace.retail_capstone.gold_reorder_recommendations
workspace.retail_capstone.gold_inventory_value
```

## RAG Document Table

The first step is to convert structured Gold records into text documents.

Target table:

```text
workspace.retail_capstone.inventory_rag_documents
```

Each row represents a product and warehouse inventory record as natural language context.

Example document:

```text
Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, is stored in warehouse WH001.
Available stock is 210 units.
Average daily sales is 8.5 units.
Days of inventory remaining is 24.7.
Supplier lead time is 7 days.
Stockout risk level is Low Risk.
Reorder flag is No Reorder Needed.
```

## Business Priority Score

The project uses a business priority score to rank important inventory records higher.

High-risk products, reorder-needed products, and products with supplier issues should appear higher in the retrieved results.

Example risk score mapping:

| Risk Level | Risk Score |
|---|---:|
| High Risk | 1.0 |
| Medium Risk | 0.75 |
| Low Risk | 0.4 |
| No Recent Sales | 0.2 |

Example formula:

```text
priority_score = risk_score * (1 + (1 - reliability_score))
```

This gives more priority to risky products from less reliable suppliers.

## Retrieval and Ranking

The retrieval process combines text relevance and business priority.

Example formula:

```text
final_score = retrieval_score * (0.6 + 0.4 * priority_score)
```

This means the result must match the question, but important inventory records receive a ranking boost.

## Pipeline Flow

```text
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
```

## Initial Implementation

The first implementation uses keyword-style retrieval in Spark SQL.

This keeps the project simple and easy to understand.

Later, the project can be improved with Databricks Vector Search and embedding-based retrieval.

## Future Improvements

- Add Databricks Vector Search
- Add embedding model endpoint
- Add LLM answer generation
- Add user feedback
- Add LLM-as-judge evaluation
