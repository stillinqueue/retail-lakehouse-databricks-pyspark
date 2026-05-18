# Model Serving Plan

## Purpose

This document explains how the trained machine learning models from Phase 3 can be served for real-time inference.

Phase 3 extends the Databricks eCommerce Lakehouse project with MLflow experiment tracking, Unity Catalog Model Registry, and Databricks Model Serving.

The goal is to show how models trained from lakehouse Gold tables can be registered, promoted, deployed, and tested by external applications.

---

## Models

This phase includes two classification models.

| Model | Type | Purpose |
|---|---|---|
| Stockout Risk Classifier | Multi-class classification | Predict whether a product is at High, Medium, Low, or No Recent Sales stockout risk |
| Reorder Flag Classifier | Binary classification | Predict whether a product should be reordered |

---

## Source Data

The models are trained from Gold inventory tables created in Phase 2.

Main source tables:

```text
workspace.retail_capstone.gold_stockout_risk
workspace.retail_capstone.gold_reorder_recommendations
workspace.retail_capstone.gold_product_sales_velocity
workspace.retail_capstone.gold_inventory_value
```

These tables are produced from the medallion pipeline:

```text
Bronze → Silver → Gold → ML Training Data
```

---

## Model 1: Stockout Risk Classifier

### Prediction Goal

Predict the stockout risk level for a product in a warehouse.

### Target Column

```text
stockout_risk_level
```

### Possible Output Classes

```text
High Risk
Medium Risk
Low Risk
No Recent Sales
```

### Example Input Features

```text
available_stock
avg_daily_sales
days_of_inventory_remaining
lead_time_days
reliability_score
category
warehouse_id
```

### Example Business Use Case

An inventory dashboard or operational application can call the model to predict which products are likely to go out of stock soon.

---

## Model 2: Reorder Flag Classifier

### Prediction Goal

Predict whether a product needs to be reordered.

### Target Column

```text
reorder_flag
```

### Possible Output Classes

```text
Reorder Needed
No Reorder Needed
```

### Example Input Features

```text
available_stock
reorder_level
reorder_quantity
lead_time_days
category
warehouse_id
```

### Example Business Use Case

A replenishment workflow can call the model to decide which products should be prioritized for purchase orders.

---

## Model Registry

After training and experiment comparison, the selected model is registered in the Unity Catalog Model Registry.

Example model names:

```text
workspace.retail_capstone.stockout_risk_classifier
workspace.retail_capstone.reorder_flag_classifier
```

A three-level namespace is used:

```text
catalog.schema.model_name
```

For this project:

```text
catalog = workspace
schema = retail_capstone
model_name = stockout_risk_classifier
model_name = reorder_flag_classifier
```

---

## Model Lifecycle with Aliases

Unity Catalog uses model aliases instead of the older legacy model stages.

Recommended aliases:

```text
@challenger
@champion
```

Lifecycle process:

```text
New trained model version
        ↓
Register in Unity Catalog
        ↓
Assign @challenger alias
        ↓
Validate predictions and metrics
        ↓
Promote to @champion
        ↓
Deploy champion model to serving endpoint
```

### Alias Meaning

| Alias | Meaning |
|---|---|
| `@challenger` | A new candidate model under validation |
| `@champion` | The approved production model |

---

## Serving Endpoint

The champion model can be deployed using Databricks Model Serving.

Example endpoint names:

```text
stockout-risk-classifier-endpoint
reorder-flag-classifier-endpoint
```

The serving endpoint allows external applications to send input data and receive predictions through a REST API.

---

## Example Input: Stockout Risk Classifier

A real-time application can send feature values like this:

```json
{
  "dataframe_records": [
    {
      "available_stock": 25,
      "avg_daily_sales": 8.5,
      "days_of_inventory_remaining": 2.9,
      "lead_time_days": 7,
      "reliability_score": 0.91,
      "category": "Home Decor",
      "warehouse_id": "WH001"
    }
  ]
}
```

---

## Example Output: Stockout Risk Classifier

Example response:

```json
{
  "predictions": ["High Risk"]
}
```

This means the model predicts that the product may run out of stock soon.

---

## Example Input: Reorder Flag Classifier

```json
{
  "dataframe_records": [
    {
      "available_stock": 25,
      "reorder_level": 100,
      "reorder_quantity": 300,
      "lead_time_days": 7,
      "category": "Home Decor",
      "warehouse_id": "WH001"
    }
  ]
}
```

---

## Example Output: Reorder Flag Classifier

```json
{
  "predictions": ["Reorder Needed"]
}
```

This means the model predicts that the product should be reordered.

---

## Example Python Test

A Databricks notebook or external Python application can test the endpoint using `requests`.

```python
import requests
import json

endpoint_url = "https://<databricks-workspace-url>/serving-endpoints/stockout-risk-classifier-endpoint/invocations"
token = "<databricks-token>"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

payload = {
    "dataframe_records": [
        {
            "available_stock": 25,
            "avg_daily_sales": 8.5,
            "days_of_inventory_remaining": 2.9,
            "lead_time_days": 7,
            "reliability_score": 0.91,
            "category": "Home Decor",
            "warehouse_id": "WH001"
        }
    ]
}

response = requests.post(
    endpoint_url,
    headers=headers,
    data=json.dumps(payload)
)

print(response.status_code)
print(response.text)
```

Do not commit real tokens to GitHub.

---

## Example curl Test

The endpoint can also be tested with `curl`.

```bash
curl -X POST \
  https://<databricks-workspace-url>/serving-endpoints/stockout-risk-classifier-endpoint/invocations \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "dataframe_records": [
      {
        "available_stock": 25,
        "avg_daily_sales": 8.5,
        "days_of_inventory_remaining": 2.9,
        "lead_time_days": 7,
        "reliability_score": 0.91,
        "category": "Home Decor",
        "warehouse_id": "WH001"
      }
    ]
  }'
```

Do not commit real workspace URLs or tokens if the repository is public.

---

## Serving Test Validation

A successful serving test should confirm:

- The endpoint is reachable
- The request payload matches the model input schema
- The response returns a prediction
- The prediction is reasonable for the sample input
- The result can be explained using the business context

Example validation:

```text
Input available_stock = 25
Input avg_daily_sales = 8.5
Input lead_time_days = 7
Calculated inventory days ≈ 2.9

Since inventory days are less than supplier lead time, a High Risk prediction is reasonable.
```

---

## Security Notes

Do not store secrets in GitHub.

Avoid committing:

```text
Databricks personal access tokens
Workspace URLs with private identifiers
Cluster IDs
Endpoint credentials
Production customer data
```

Use environment variables or Databricks Secrets for production credentials.

---

## Business Use Cases

The serving endpoint can support several business workflows.

### Inventory Dashboard

A dashboard can call the endpoint to show predicted risk levels for products and warehouses.

### Replenishment Workflow

A scheduled job can call the reorder model and create a list of products that need purchase orders.

### Product Availability Alerts

An alerting system can notify inventory managers when a product is predicted to be at high stockout risk.

### Application Integration

An internal inventory management application can send product and stock features to the endpoint and receive predictions in real time.

---

## Cloud Serving vs Local Serving

Databricks Model Serving provides:

- Managed infrastructure
- REST API endpoint
- Integration with Unity Catalog models
- Scalable cloud-hosted inference
- Easier enterprise governance

Local serving can be useful for:

- Offline testing
- Sovereign inference requirements
- Cost-sensitive experiments
- Data locality constraints

In this project, Databricks Model Serving is the primary serving approach. Local inference can be added later as an extension.

---

## Summary

This serving plan shows how the trained ML models can move from experimentation to production-style inference.

The full workflow is:

```text
Gold Inventory Tables
        ↓
ML Training Dataset
        ↓
MLflow Experiment Tracking
        ↓
Best Model Selection
        ↓
Unity Catalog Model Registry
        ↓
@challenger Validation
        ↓
@champion Promotion
        ↓
Databricks Model Serving Endpoint
        ↓
Real-Time Prediction
```
