# RAG Evaluation Plan

## Purpose

This document describes how the inventory RAG assistant will be evaluated.

The evaluation checks whether the retrieval system returns relevant inventory context for business questions.

## Evaluation Goal

The goal is to confirm that the assistant retrieves the right records before generating an answer.

For example:

- High-risk questions should return high-risk products.
- Reorder questions should return products marked for reorder.
- Supplier questions should return supplier and lead-time information.
- Warehouse questions should return warehouse and stock information.

## Initial Evaluation Dataset

The first evaluation dataset contains handwritten business queries.

| Query | Expected Signal |
|---|---|
| Which products are high stockout risk? | High Risk |
| Which products need reorder? | Reorder Needed |
| Which products have low available stock? | available_stock |
| Which suppliers have long lead times? | lead_time_days |
| Which products have high sales velocity? | avg_daily_sales |
| Which warehouse has low available stock? | warehouse_id, available_stock |

## Metrics

The evaluation uses standard retrieval metrics.

| Metric | Meaning |
|---|---|
| MRR | Measures how high the first relevant result appears |
| Precision@k | Percentage of top-k results that are relevant |
| Recall@k | Percentage of relevant records retrieved in top-k |
| NDCG@k | Rewards relevant results that appear higher in ranking |

## Relevance Method

The initial version uses term-matching relevance.

Example:

```text
Query: Which products need reorder?
Expected term: Reorder Needed
```

If a retrieved document contains the expected term, it is considered relevant.

## Limitation

Term matching is simple and portable, but it can miss similar meanings.

For example:

```text
Needs replenishment
```

may mean the same thing as:

```text
Reorder Needed
```

but term matching may not count it as relevant.

## Ranking Formula Experiment

The project can compare these formulas:

```text
final_score = retrieval_score
final_score = retrieval_score * (0.8 + 0.2 * priority_score)
final_score = retrieval_score * (0.6 + 0.4 * priority_score)
final_score = retrieval_score * (0.5 + 0.5 * priority_score)
```

This helps evaluate how much business priority should influence retrieval ranking.

## Success Criteria

The evaluation is successful if:

- Relevant products appear in the top results.
- High-risk products rank higher for risk-related questions.
- Reorder-needed products appear for reorder questions.
- Supplier and warehouse questions return the right context.
- The ranking formula improves useful results.

## Future Improvements

- Create more evaluation questions
- Add LLM-judged relevance
- Compare keyword retrieval with vector retrieval
- Store evaluation results in a Delta table
- Add evaluation to a Databricks Job
