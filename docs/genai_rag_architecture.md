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
```

## RAG Document Table

The first step is to convert structured Gold records into text documents.

Target table:

```text
workspace.retail_capstone.inventory_rag_documents
```

Each row in this table represents a product and warehouse inventory record as natural language context.

Instead of only storing numeric fields, the RAG document table creates readable inventory summaries that can be retrieved and passed to an LLM.

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

## RAG Document Table Structure

The RAG document table can include the following columns:

| Column | Description |
|---|---|
| product_id | Unique product identifier |
| description | Product description |
| warehouse_id | Warehouse where the product is stored |
| supplier_id | Supplier associated with the product |
| supplier_name | Name of the supplier |
| available_stock | Current available stock quantity |
| avg_daily_sales | Average daily sales quantity |
| days_of_inventory_remaining | Estimated number of days before stock runs out |
| lead_time_days | Supplier lead time in days |
| reliability_score | Supplier reliability score |
| risk_level | Stockout risk level |
| reorder_flag | Indicates whether reorder is recommended |
| reorder_quantity | Suggested reorder quantity |
| inventory_value | Current value of inventory |
| rag_document | Natural language document used for retrieval |
| priority_score | Business priority score used for ranking |

## Business Priority Score

The project uses a business priority score to rank risky inventory records higher.

This helps the assistant prioritize answers based not only on keyword relevance, but also on operational importance.

For example, if two products match a user question, the product with higher stockout risk and lower supplier reliability should appear higher in the retrieved results.

## Risk Score Mapping

Example risk score mapping:

| Risk Level | Risk Score |
|---|---:|
| High Risk | 1.0 |
| Medium Risk | 0.75 |
| Low Risk | 0.4 |
| No Recent Sales | 0.2 |

## Supplier Reliability Adjustment

The score can also consider supplier reliability.

Example formula:

```text
priority_score = risk_score * (1 + (1 - reliability_score))
```

This increases the priority of risky products supplied by less reliable suppliers.

Example calculation:

```text
risk_score = 1.0
reliability_score = 0.80

priority_score = 1.0 * (1 + (1 - 0.80))
priority_score = 1.2
```

In this example, a high-risk product from a supplier with 80% reliability receives a boosted priority score of 1.2.

## Retrieval and Ranking

The retrieval process combines text relevance and business priority.

A basic retrieval process can search the `rag_document` column for matching product names, product IDs, risk terms, supplier names, or warehouse IDs.

The initial implementation uses keyword-style retrieval in Spark SQL for learning and portability.

A later version can add Databricks Vector Search or embedding-based retrieval.

## Ranking Formula

Example ranking formula:

```text
final_score = retrieval_score * (0.6 + 0.4 * priority_score)
```

This means a result must still be relevant to the query, but high-risk inventory records receive a ranking boost.

Example ranking interpretation:

| Retrieval Score | Priority Score | Final Score |
|---:|---:|---:|
| 0.90 | 1.00 | 0.90 |
| 0.90 | 0.75 | 0.81 |
| 0.90 | 0.40 | 0.684 |
| 0.90 | 0.20 | 0.612 |

The formula keeps retrieval relevance as the main factor while still allowing business risk to influence the final ranking.

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

## Detailed Phase 4 Flow

The Phase 4 GenAI RAG flow contains the following steps:

1. Read Gold inventory, stockout risk, sales velocity, reorder, supplier, and warehouse data.
2. Join the Gold tables into a complete inventory context.
3. Convert each structured inventory record into a natural language RAG document.
4. Store the generated documents in a Delta table.
5. Retrieve relevant documents based on user questions.
6. Apply business priority scoring to boost risky or urgent inventory records.
7. Return the top-ranked context records.
8. Use the retrieved context to generate a grounded answer.
9. Evaluate whether the retrieved context and generated answer are correct and useful.

## Initial Implementation

The first implementation uses Spark SQL and Delta tables.

The goal is to keep the RAG pipeline simple, explainable, and easy to run inside Databricks.

The initial version includes:

- Creating inventory RAG documents from Gold tables
- Storing RAG documents in a Delta table
- Searching documents using SQL keyword filters
- Ranking results using a business priority score
- Returning relevant context for answer generation
- Preparing the project for future Databricks Vector Search integration

## Planned Notebooks

Phase 4 can add the following notebooks:

```text
notebooks/06_genai_inventory_rag_documents.py
notebooks/07_genai_inventory_rag_retrieval.py
notebooks/08_genai_rag_evaluation.py
```

### 06_genai_inventory_rag_documents.py

This notebook creates the inventory RAG document table.

Main responsibilities:

- Read Gold inventory tables
- Join inventory, supplier, sales velocity, stockout risk, and reorder data
- Generate natural language inventory documents
- Add business priority score
- Write the final RAG document table to Delta

### 07_genai_inventory_rag_retrieval.py

This notebook performs retrieval from the RAG document table.

Main responsibilities:

- Accept example business questions
- Search RAG documents using keyword logic
- Calculate retrieval score
- Combine retrieval score with priority score
- Return top-ranked context records
- Demonstrate how retrieved context can support answer generation

### 08_genai_rag_evaluation.py

This notebook evaluates retrieval and answer quality.

Main responsibilities:

- Define sample evaluation questions
- Define expected products, suppliers, or risk categories
- Check whether retrieval returns relevant records
- Measure simple retrieval quality
- Document evaluation results
- Prepare the project for future LLM-as-judge evaluation

## Planned Documentation Files

Phase 4 can add the following documentation files:

```text
docs/genai_rag_architecture.md
docs/rag_evaluation_plan.md
docs/genai_assistant_prompting.md
```

### genai_rag_architecture.md

Describes the overall RAG architecture, source tables, document table, scoring logic, retrieval flow, and future improvements.

### rag_evaluation_plan.md

Describes how retrieval and generated answers will be evaluated.

It can include:

- Evaluation questions
- Expected retrieved records
- Expected answer characteristics
- Retrieval quality checks
- Answer quality checks
- Future LLM-as-judge evaluation approach

### genai_assistant_prompting.md

Describes how the inventory assistant should be prompted.

It can include:

- System prompt
- User prompt examples
- Context formatting rules
- Answer style rules
- Safety and grounding rules
- Refusal rules when context is missing

## Planned SQL File

Phase 4 can add the following SQL file:

```text
sql/rag_inventory_queries.sql
```

This SQL file can include useful queries for:

- Viewing inventory RAG documents
- Searching by product ID
- Searching by warehouse
- Searching by stockout risk level
- Searching by reorder flag
- Ranking products by priority score
- Finding high-risk products with low inventory
- Finding suppliers with long lead times

## Example User Questions

The assistant should be able to answer questions such as:

- Which products are at high stockout risk?
- Which products should be reordered this week?
- Why is product 85123A marked as high risk?
- Which supplier has long lead times?
- Show products with low stock but high sales velocity.
- Explain the reorder recommendation for product 85123A.
- Which warehouse has the highest inventory risk?
- Which high-risk products have unreliable suppliers?
- Which products have low inventory value but high stockout risk?
- Which supplier creates the most reorder risk?

## Example Retrieved Context

For a question such as:

```text
Which products should be reordered this week?
```

The retrieval layer may return context like:

```text
Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, is stored in warehouse WH001.
Available stock is 25 units.
Average daily sales is 8.5 units.
Days of inventory remaining is 2.9.
Supplier lead time is 7 days.
Stockout risk level is High Risk.
Reorder flag is Reorder Recommended.
Recommended reorder quantity is 500 units.
```

The assistant can then answer:

```text
Product 85123A should be reordered because it has only 2.9 days of inventory remaining, while supplier lead time is 7 days. This means the product may run out before replenishment arrives.
```

## Grounding Rules

The assistant should only answer using retrieved inventory context.

If the retrieved context does not contain enough information, the assistant should say that the available inventory context is insufficient.

The assistant should avoid inventing:

- Product IDs
- Supplier names
- Warehouse IDs
- Stock levels
- Reorder quantities
- Risk levels
- Lead times
- Reliability scores

## Evaluation Approach

The RAG assistant should be evaluated using a small set of business questions.

Evaluation should check whether:

- The correct products are retrieved
- The correct suppliers are retrieved
- High-risk products rank above low-risk products when relevant
- Reorder explanations are supported by the retrieved context
- The assistant does not invent unsupported facts
- The answer is clear and useful for inventory operations teams

## Future Improvements

Future improvements can include:

- Add Databricks Vector Search
- Add an embedding model endpoint
- Add LLM answer generation with a foundation model endpoint
- Add feedback collection from users
- Add evaluation with LLM-as-judge
- Add monitoring for retrieval quality and answer quality
- Add a dashboard for RAG assistant usage
- Add permissions and governance through Unity Catalog
- Add automated Databricks Jobs for RAG document refresh
- Add lineage tracking for RAG source tables
- Add model serving integration for the GenAI assistant
