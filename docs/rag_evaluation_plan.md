# RAG Evaluation Plan

## Purpose

This document describes how the inventory RAG assistant will be evaluated.

The evaluation checks whether the retrieval system returns relevant inventory context for business questions.

The goal is to make sure the assistant retrieves the correct products, suppliers, warehouses, stockout risks, and reorder recommendations before those results are used for answer generation.

## Evaluation Goals

The RAG evaluation process should answer these questions:

- Does the retrieval system return relevant inventory documents?
- Do high-risk products appear near the top of the results?
- Are reorder-related queries retrieving products that actually need reorder?
- Are supplier-related queries retrieving supplier and lead-time information?
- Are warehouse-related queries retrieving warehouse and stock information?
- Does the ranking formula improve business usefulness?
- Does the assistant avoid using unsupported or missing information?

## Initial Evaluation Dataset

The first evaluation dataset contains handwritten business queries.

These queries are designed to test the most important inventory assistant use cases.

| Query | Expected Signal |
|---|---|
| Which products are high stockout risk? | High Risk |
| Which products need reorder? | Reorder Needed |
| Which products have low available stock? | available_stock |
| Which suppliers have long lead times? | lead_time_days |
| Which products have high sales velocity? | avg_daily_sales |
| Which warehouse has low available stock? | warehouse_id, available_stock |
| Which products have low stock but high sales velocity? | available_stock, avg_daily_sales |
| Which products are supplied by unreliable suppliers? | reliability_score |
| Which products may run out before replenishment arrives? | days_of_inventory_remaining, lead_time_days |
| Which products should be prioritized for review? | priority_score |

## Example Evaluation Dataset

The evaluation dataset can be represented as a small table.

| eval_id | query | expected_signal | expected_field |
|---|---|---|---|
| EVAL001 | Which products are high stockout risk? | High Risk | risk_level |
| EVAL002 | Which products need reorder? | Reorder Needed | reorder_flag |
| EVAL003 | Which products have low available stock? | Low stock value | available_stock |
| EVAL004 | Which suppliers have long lead times? | High lead time | lead_time_days |
| EVAL005 | Which products have high sales velocity? | High sales velocity | avg_daily_sales |
| EVAL006 | Which warehouse has low available stock? | Warehouse and stock | warehouse_id, available_stock |
| EVAL007 | Which products have low stock but high sales velocity? | Low stock and high sales velocity | available_stock, avg_daily_sales |
| EVAL008 | Which products may run out before replenishment arrives? | Inventory days less than lead time | days_of_inventory_remaining, lead_time_days |
| EVAL009 | Which high-risk products have unreliable suppliers? | High risk and low reliability | risk_level, reliability_score |
| EVAL010 | Which products should be prioritized for review? | High priority score | priority_score |

## Metrics

The evaluation uses standard retrieval metrics.

| Metric | Meaning |
|---|---|
| MRR | Measures how high the first relevant result appears |
| Precision@k | Percentage of top-k results that are relevant |
| Recall@k | Percentage of relevant records retrieved in top-k |
| NDCG@k | Rewards relevant results that appear higher in ranking |

## Metric Definitions

### MRR

MRR stands for Mean Reciprocal Rank.

It measures whether the first relevant result appears near the top of the retrieved results.

Example:

```text
If the first relevant result appears at rank 1, reciprocal rank = 1.0
If the first relevant result appears at rank 2, reciprocal rank = 0.5
If the first relevant result appears at rank 4, reciprocal rank = 0.25
```

Higher MRR means relevant results are appearing earlier.

### Precision@k

Precision@k measures how many of the top-k retrieved results are relevant.

Example:

```text
If top 5 results contain 4 relevant records:

Precision@5 = 4 / 5
Precision@5 = 0.80
```

Higher Precision@k means the top results contain less noise.

### Recall@k

Recall@k measures how many total relevant records were found in the top-k results.

Example:

```text
If there are 10 relevant records in the dataset and top 5 results contain 4 of them:

Recall@5 = 4 / 10
Recall@5 = 0.40
```

Higher Recall@k means the retrieval system is finding more of the relevant records.

### NDCG@k

NDCG@k stands for Normalized Discounted Cumulative Gain.

It rewards relevant results that appear higher in the ranking.

A highly relevant result at rank 1 receives more credit than the same result at rank 5.

This is useful when some records are more important than others, such as high-risk inventory records.

## Relevance Method

The initial version uses term-matching relevance.

Example:

```text
Query: Which products need reorder?
Expected term: Reorder Needed

If a retrieved document contains the expected term, it is considered relevant.
```

For the first version, this is simple, explainable, and easy to implement in Spark SQL or PySpark.

## Example Relevance Rules

| Query Type | Relevance Rule |
|---|---|
| Stockout risk query | Retrieved document contains High Risk or Medium Risk |
| Reorder query | Retrieved document contains Reorder Needed or Reorder Recommended |
| Low stock query | Retrieved record has low available_stock |
| Supplier lead time query | Retrieved record contains lead_time_days or high lead time |
| Sales velocity query | Retrieved record contains avg_daily_sales or high sales velocity |
| Warehouse query | Retrieved record contains warehouse_id and available_stock |
| Priority query | Retrieved record has high priority_score |

## Limitation

Term matching is simple and portable, but it can miss semantically relevant results that use different wording.

For example:

```text
Needs replenishment
```

may mean the same thing as:

```text
Reorder Needed
```

but term matching may not count it as relevant.

It can also miss cases where the document uses different wording, such as:

```text
Stock is expected to run out soon.
```

instead of:

```text
High Risk
```

Because of this, term-matching evaluation is useful for the first version, but it should be improved in a later version.

## Ranking Formula Experiment

The project can compare different ranking formulas.

```text
final_score = retrieval_score
final_score = retrieval_score * (0.8 + 0.2 * priority_score)
final_score = retrieval_score * (0.6 + 0.4 * priority_score)
final_score = retrieval_score * (0.5 + 0.5 * priority_score)
```

This helps evaluate how much business priority should influence retrieval ranking.

## Experiment Goal

The goal of the ranking experiment is to compare pure retrieval relevance against business-aware ranking.

The baseline formula is:

```text
final_score = retrieval_score
```

This ranks results only by text relevance.

The business-aware formulas boost records with higher priority scores.

For inventory operations, this is useful because a highly relevant result with high stockout risk may be more important than a highly relevant result with low risk.

## Expected Ranking Behavior

The ranking formula should:

- Keep query relevance as the main requirement
- Boost high-risk products when they match the query
- Boost reorder-needed products when they match the query
- Boost products with low supplier reliability when relevant
- Avoid ranking irrelevant products highly only because they have high priority scores

## Evaluation Process

The evaluation process follows these steps:

1. Create a small set of evaluation queries.
2. Define expected signals for each query.
3. Run each query against the RAG document table.
4. Retrieve the top-k results.
5. Check whether each result is relevant.
6. Calculate MRR, Precision@k, Recall@k, and NDCG@k.
7. Compare ranking formulas.
8. Record the best-performing formula.
9. Document any retrieval failures.
10. Use the results to improve the retrieval logic.

## Suggested Top-k Values

The first evaluation can use:

```text
k = 3
k = 5
k = 10
```

Suggested metrics:

```text
Precision@3
Precision@5
Recall@5
Recall@10
NDCG@5
MRR
```

## Example Evaluation Output

Example results table:

| Query | Formula | MRR | Precision@5 | Recall@5 | NDCG@5 |
|---|---|---:|---:|---:|---:|
| Which products are high stockout risk? | retrieval_score | 0.80 | 0.60 | 0.45 | 0.70 |
| Which products are high stockout risk? | retrieval_score * priority boost | 1.00 | 0.80 | 0.55 | 0.88 |
| Which products need reorder? | retrieval_score | 0.75 | 0.60 | 0.50 | 0.68 |
| Which products need reorder? | retrieval_score * priority boost | 1.00 | 0.80 | 0.65 | 0.90 |

## Success Criteria

The initial RAG evaluation is considered successful if:

- High-risk queries return high-risk products in the top results
- Reorder queries return products marked for reorder
- Supplier queries return supplier and lead-time context
- Warehouse queries return warehouse and stock context
- Business-priority ranking improves or maintains Precision@k and NDCG@k
- The assistant can explain answers using retrieved context
- The evaluation results are reproducible in Databricks

## Failure Cases to Track

The evaluation should track cases where:

- No relevant documents are retrieved
- Relevant documents appear too low in the ranking
- Low-risk products appear above high-risk products for risk-related queries
- Reorder-needed products are missing from reorder-related queries
- Supplier questions retrieve product-only context without supplier details
- Warehouse questions retrieve records without warehouse information
- The retrieved context is relevant but not enough to answer the question

## Future V2 Evaluation

A stronger evaluation would use LLM-judged relevance.

V2 plan:

1. Generate 100 to 250 evaluation queries from inventory documents.
2. Retrieve top-k contexts for each query.
3. Use an LLM judge to score whether each retrieved context answers the query.
4. Compute MRR, Precision@k, Recall@k, and NDCG@k.
5. Compare different ranking formulas.
6. Evaluate answer quality in addition to retrieval quality.
7. Track hallucination and unsupported claims.
8. Store evaluation results in a Delta table.

## LLM-as-Judge Evaluation

A later version can use an LLM judge to score retrieved context and generated answers.

The judge can evaluate:

| Evaluation Area | Question |
|---|---|
| Context relevance | Does the retrieved context answer the user query? |
| Groundedness | Is the answer supported by the retrieved context? |
| Completeness | Does the answer include the key inventory facts? |
| Clarity | Is the answer understandable for an operations user? |
| Safety | Does the assistant avoid inventing unsupported facts? |

## Example LLM Judge Prompt

```text
You are evaluating an inventory RAG assistant.

User question:
{question}

Retrieved context:
{retrieved_context}

Generated answer:
{answer}

Evaluate the answer using the following criteria:

1. Relevance: Does the retrieved context answer the question?
2. Groundedness: Is the answer fully supported by the context?
3. Completeness: Does the answer include the important inventory facts?
4. Clarity: Is the answer clear for an inventory operations user?
5. Unsupported claims: Does the answer invent any facts?

Return a score from 1 to 5 and a short explanation.
```

## Evaluation Storage

Evaluation results can be stored in a Delta table.

Example table:

```text
workspace.retail_capstone.rag_evaluation_results
```

Suggested columns:

| Column | Description |
|---|---|
| eval_id | Evaluation query identifier |
| query | Business question |
| ranking_formula | Ranking formula being tested |
| top_k | Number of retrieved records evaluated |
| mrr | Mean reciprocal rank score |
| precision_at_k | Precision@k score |
| recall_at_k | Recall@k score |
| ndcg_at_k | NDCG@k score |
| evaluation_notes | Notes about retrieval quality |
| evaluated_at | Timestamp of evaluation run |

## Future Improvements

Future improvements can include:

- Add larger evaluation dataset
- Add LLM-judged relevance scoring
- Add generated answer evaluation
- Add hallucination checks
- Add regression testing for retrieval quality
- Add comparison between keyword retrieval and vector retrieval
- Add comparison between different priority-score weights
- Add dashboard for RAG evaluation results
- Add automated evaluation as part of a Databricks Job
