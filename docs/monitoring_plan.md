# Monitoring Plan

## Purpose

This document describes the Lakehouse Monitoring plan for the Databricks eCommerce Lakehouse project.

Monitoring is part of Phase 5: Production Governance and MLOps.

---

## Table Selected for Monitoring

Recommended table:

```text
workspace.retail_capstone.gold_stockout_risk
```

## Why This Table

The `gold_stockout_risk` table is business-critical because it identifies products that may go out of stock.

It is used by:

- Inventory dashboards
- Reorder planning
- ML stockout risk classification
- GenAI inventory assistant retrieval

Monitoring this table helps ensure that downstream analytics and ML use reliable data.

---

## Important Columns to Monitor

| Column | Reason |
|---|---|
| stock_code | Product identifier should not be null |
| warehouse_id | Warehouse identifier should not be null |
| available_stock | Important for stockout calculations |
| avg_daily_sales | Important for demand signal |
| days_of_inventory_remaining | Important risk metric |
| lead_time_days | Important supplier metric |
| stockout_risk_level | Main business label |
| reliability_score | Supplier quality signal |

---

## Monitoring Checks

Recommended checks:

| Check | Expected Behavior |
|---|---|
| Row count | Should not unexpectedly drop to zero |
| Freshness | Table should be updated after pipeline runs |
| Completeness | Key fields should not contain unexpected nulls |
| Distribution | Risk label distribution should remain explainable |
| Drift | Major changes in available stock or sales velocity should be reviewed |

---

## Suggested Alert Thresholds

| Metric | Threshold | Action |
|---|---|---|
| Row count | Drops by more than 30% from previous scan | Investigate upstream ingestion |
| Freshness | Table not updated within expected schedule | Check Databricks Job run |
| Null stock_code | Greater than 0 | Stop downstream reporting and investigate |
| Null stockout_risk_level | Greater than 0 | Check Gold transformation logic |
| High Risk percentage | Sudden increase above 50% | Notify inventory operations |

---

## Example Response to Alert

If freshness is breached:

1. Check the Databricks Job run history.
2. Review failed tasks.
3. Check Bronze and Silver tables.
4. Re-run the failed task after fixing the issue.
5. Validate Gold table row count and quality.
6. Confirm dashboards and downstream models are using updated data.

---

## Evidence to Capture

For the capstone submission, capture:

- Last scan timestamp
- Row count
- Completeness status
- Freshness status
- Screenshot of monitoring results

Recommended screenshot path:

```text
images/lakehouse_monitoring_results.png
```

---

## Future Improvements

- Add monitoring to all Gold tables
- Add ML feature table monitoring
- Add endpoint monitoring for model serving
- Add drift monitoring for ML predictions
- Add automated alerting through email or Slack
