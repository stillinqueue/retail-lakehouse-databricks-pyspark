# Model Serving Plan

## Purpose

This document explains how the trained machine learning models can be served for inference.

The serving workflow is part of Phase 3 of the Databricks eCommerce Lakehouse project.

Phase 3 extends the project from lakehouse data engineering into machine learning by training, registering, and testing classification models for inventory management.

---

## Models

The project includes two classification models:

1. Stockout Risk Classifier
2. Reorder Flag Classifier

---

## Model 1: Stockout Risk Classifier

### Purpose

The stockout risk classifier predicts the risk level of a product going out of stock.

### Target Column

```text
stockout_risk_level
```

### Example Classes

```text
High Risk
Medium Risk
Low Risk
No Recent Sales
```

### Example Features

```text
available_stock
avg_daily_sales
days_of_inventory_remaining
lead_time_days
reliability_score
category
warehouse_id
```

### Business Use Case

This model helps inventory teams identify products that may go out of stock before supplier replenishment arrives.

The prediction can support:

- Inventory alerts
- Replenishment planning
- Purchase order prioritization
- Dashboard risk indicators

---

## Model 2: Reorder Flag Classifier

### Purpose

The reorder flag classifier predicts whether a product should be reordered.

### Target Column

```text
reorder_flag
```

### Example Classes

```text
Reorder Needed
No Reorder Needed
```

### Example Features

```text
available_stock
reorder_level
reorder_quantity
lead_time_days
category
warehouse_id
```

### Business Use Case

This model supports automated replenishment decisions.

The prediction can support:

- Reorder recommendations
- Inventory planning
- Supplier coordination
- Operational decision-making

---

## Source Tables

The models are trained from ML feature tables created from the Gold inventory layer.

```text
workspace.retail_capstone.ml_stockout_training_data
workspace.retail_capstone.ml_reorder_training_data
```

These ML feature tables are derived from Gold tables such as:

```text
workspace.retail_capstone.gold_stockout_risk
workspace.retail_capstone.gold_reorder_recommendations
workspace.retail_capstone.gold_inventory_value
workspace.retail_capstone.gold_product_sales_velocity
```

---

## Model Registry

The best models from MLflow experiments are registered in Unity Catalog Model Registry.

Registered model names:

```text
workspace.retail_capstone.stockout_risk_classifier
workspace.retail_capstone.reorder_flag_classifier
```

---

## Model Aliases

Unity Catalog model aliases are used to manage the model lifecycle.

Aliases used:

```text
@challenger
@champion
```

The normal lifecycle process is:

```text
New model version
      ↓
@challenger
      ↓
Validation
      ↓
@champion
```

The `@challenger` alias marks a new model candidate.

The `@champion` alias marks the validated model version that should be used for inference or serving.

---

## Notebook-Based Serving Test

Before creating a real-time serving endpoint, the champion models are tested inside a Databricks notebook.

Notebook:

```text
notebooks/05_model_serving_test.py
notebooks/05_model_serving_test.ipynb
```

The notebook loads the registered champion models using MLflow.

### Stockout Champion Model URI

```text
models:/workspace.retail_capstone.stockout_risk_classifier@champion
```

### Reorder Champion Model URI

```text
models:/workspace.retail_capstone.reorder_flag_classifier@champion
```

---

## Example Stockout Model Input

```json
{
  "dataframe_records": [
    {
      "available_stock": 20,
      "avg_daily_sales": 8.5,
      "days_of_inventory_remaining": 2.35,
      "lead_time_days": 7,
      "reliability_score": 0.91,
      "category": "Home Decor",
      "warehouse_id": "WH001"
    },
    {
      "available_stock": 200,
      "avg_daily_sales": 3.0,
      "days_of_inventory_remaining": 66.67,
      "lead_time_days": 7,
      "reliability_score": 0.94,
      "category": "Home Decor",
      "warehouse_id": "WH001"
    }
  ]
}
```

---

## Example Stockout Model Output

Example response:

```json
{
  "predictions": [
    "High Risk",
    "Low Risk"
  ]
}
```

The exact prediction may vary depending on the trained model version.

---

## Example Reorder Model Input

```json
{
  "dataframe_records": [
    {
      "available_stock": 30,
      "reorder_level": 100,
      "reorder_quantity": 300,
      "lead_time_days": 7,
      "category": "Home Decor",
      "warehouse_id": "WH001"
    },
    {
      "available_stock": 250,
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

## Example Reorder Model Output

Example response:

```json
{
  "predictions": [
    "Reorder Needed",
    "No Reorder Needed"
  ]
}
```

The exact prediction may vary depending on the trained model version.

---

# Databricks Model Serving Endpoint

The stockout risk classifier can be deployed to a Databricks Model Serving endpoint.

Endpoint name:

```text
stockout-risk-classifier-endpoint
```

Model served:

```text
workspace.retail_capstone.stockout_risk_classifier@champion
```

The endpoint exposes the registered champion model as a REST API that can be queried by external applications.

---

## Endpoint Request Format

Example request body:

```json
{
  "dataframe_records": [
    {
      "available_stock": 20,
      "avg_daily_sales": 8.5,
      "days_of_inventory_remaining": 2.35,
      "lead_time_days": 7,
      "reliability_score": 0.91,
      "category": "Home Decor",
      "warehouse_id": "WH001"
    }
  ]
}
```

---

## Endpoint Response Format

Example response:

```json
{
  "predictions": [
    "High Risk"
  ]
}
```

The exact prediction may vary depending on the trained model version.

---

## Python Endpoint Test

The endpoint can be tested from Python using the `requests` library.

Sensitive values such as workspace URL and access token should not be committed to GitHub.

```python
import requests
import json

DATABRICKS_HOST = "https://<your-databricks-workspace-url>"
DATABRICKS_TOKEN = "<your-databricks-personal-access-token>"

endpoint_name = "stockout-risk-classifier-endpoint"

url = f"{DATABRICKS_HOST}/serving-endpoints/{endpoint_name}/invocations"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "dataframe_records": [
        {
            "available_stock": 20,
            "avg_daily_sales": 8.5,
            "days_of_inventory_remaining": 2.35,
            "lead_time_days": 7,
            "reliability_score": 0.91,
            "category": "Home Decor",
            "warehouse_id": "WH001"
        }
    ]
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

print("Status code:", response.status_code)
print("Response:")
print(response.text)
```

---

## Example curl Request

Replace the workspace URL and token before running.

Do not commit real tokens to GitHub.

```bash
curl -X POST https://<your-databricks-workspace-url>/serving-endpoints/stockout-risk-classifier-endpoint/invocations \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "dataframe_records": [
      {
        "available_stock": 20,
        "avg_daily_sales": 8.5,
        "days_of_inventory_remaining": 2.35,
        "lead_time_days": 7,
        "reliability_score": 0.91,
        "category": "Home Decor",
        "warehouse_id": "WH001"
      }
    ]
  }'
```

---

## Security Notes

Do not commit any of the following to GitHub:

```text
Databricks personal access tokens
Workspace secrets
Private workspace URLs if they should not be public
Service principal credentials
Secret scope values
Endpoint authentication headers
```

For production usage, secrets should be managed with Databricks secrets or another secure secret management system.

Example placeholder:

```python
DATABRICKS_TOKEN = dbutils.secrets.get(
    scope="<your-secret-scope>",
    key="<your-token-key>"
)
```

---

## Evidence to Capture

For project documentation, useful screenshots include:

```text
images/model_serving_endpoint_ready.png
images/stockout_endpoint_response.png
images/mlflow_experiments_screenshot.png
```

Recommended screenshots:

1. Model Serving endpoint page showing `Ready`
2. Query response showing predictions
3. MLflow Experiments UI showing model comparison
4. Registered model page showing `@challenger` and `@champion` aliases

Do not include access tokens or sensitive workspace details in screenshots.

---

## Production Considerations

Before using this workflow in production, the following should be added:

- Endpoint monitoring
- Model drift monitoring
- Data drift monitoring
- Access controls
- Secret management
- CI/CD deployment process
- Automated validation before promotion to `@champion`
- Model performance tracking over time
- Logging of prediction requests and responses where appropriate

---

## Summary

This serving workflow demonstrates how trained ML models can move from experimentation to registry and inference.

The project includes:

- MLflow experiment tracking
- Model comparison
- Unity Catalog Model Registry
- Model aliases for lifecycle management
- Notebook-based champion model testing
- Databricks Model Serving endpoint design
- REST API request examples

This completes the serving design for the stockout risk and reorder classification models.
