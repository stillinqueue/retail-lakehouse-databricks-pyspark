# ML Modeling Plan

## Purpose

This document explains Phase 3 of the Databricks eCommerce Lakehouse project.

Phase 3 extends the inventory management pipeline with machine learning models for stockout risk prediction and reorder recommendation.

The goal is to demonstrate how business-ready Gold tables from the lakehouse can be used to train, track, compare, register, and serve machine learning models.

---

## Business Context

In an eCommerce business, inventory availability is critical.

If a product goes out of stock, the business may lose revenue and customer trust.

If a product is overstocked, the company may waste warehouse space and working capital.

This phase uses inventory, sales velocity, supplier, and warehouse data to support two ML use cases:

1. Predict stockout risk level
2. Predict whether a product needs reorder

---

## Source Tables

The ML pipeline uses Gold tables created in Phase 2.

```text
workspace.retail_capstone.gold_stockout_risk
workspace.retail_capstone.gold_reorder_recommendations
workspace.retail_capstone.gold_inventory_value
workspace.retail_capstone.gold_product_sales_velocity
```

These tables were created from the medallion architecture pipeline:

```text
Bronze → Silver → Gold
```

---

## Model 1: Stockout Risk Classification

### Target Column

```text
stockout_risk_level
```

### Prediction Goal

Predict whether a product is at risk of going out of stock.

Possible classes:

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

### Business Value

This model helps inventory teams identify products that may go out of stock before supplier replenishment arrives.

Example business use cases:

- Alert inventory managers about high-risk products
- Prioritize replenishment planning
- Support inventory dashboards
- Reduce lost revenue from stockouts

---

## Model 2: Reorder Flag Classification

### Target Column

```text
reorder_flag
```

### Prediction Goal

Predict whether a product should be reordered.

Possible classes:

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

### Business Value

This model supports automated replenishment decisions and helps inventory managers prioritize purchase orders.

Example business use cases:

- Recommend products for reorder
- Support purchasing workflows
- Reduce manual inventory checks
- Improve stock availability

---

## Training Data Note

The current project uses small sample product, inventory, supplier, and warehouse data.

For machine learning practice, the training dataset is synthetically expanded from the inventory business rules.

This is done for learning and demonstration purposes.

The goal is not to claim a production-ready model, but to demonstrate the complete ML engineering workflow:

- Feature preparation
- Exploratory data analysis
- Train/test split
- Experiment tracking
- Model comparison
- Model registration
- Model serving workflow

---

## ML Feature Table

A feature table will be created from the Gold inventory tables.

Example feature table:

```text
workspace.retail_capstone.ml_inventory_features
```

Expected columns:

```text
stock_code
product_name
category
brand
warehouse_id
available_stock
avg_daily_sales
days_of_inventory_remaining
lead_time_days
reliability_score
reorder_level
reorder_quantity
stockout_risk_level
reorder_flag
```

---

## Exploratory Data Analysis

The Databricks notebook will include visual exploration of the training data.

Planned charts:

| Chart | Purpose |
|---|---|
| Stockout risk label distribution | Shows how many records belong to each risk class |
| Reorder flag distribution | Shows how many records require reorder |
| Available stock distribution | Shows the spread of stock availability |
| Average daily sales distribution | Shows demand variation |
| Days of inventory remaining distribution | Explains stockout risk logic |
| Feature importance chart | Shows which features influenced model predictions |

These graphs help explain the scenario during interviews and make the project easier to understand for new readers.

---

## Experiment Tracking

MLflow is used to track machine learning experiments.

Each run logs:

- Model name
- Model type
- Hyperparameters
- Accuracy
- F1 score
- Model artifact
- Training notebook reference

At least three model runs are compared.

Example experiments:

| Run | Model | Example Hyperparameters |
|---|---|---|
| Run 1 | Logistic Regression | maxIter = 20 |
| Run 2 | Random Forest | numTrees = 20, maxDepth = 5 |
| Run 3 | Random Forest | numTrees = 50, maxDepth = 8 |

---

## Evaluation Metrics

The models will be evaluated using classification metrics.

Planned metrics:

```text
accuracy
f1_score
```

Optional metrics:

```text
precision
recall
auc
```

F1 score is important because classification datasets can be imbalanced.

For example, there may be many "Low Risk" products and fewer "High Risk" products.

---

## Model Selection Criteria

The best model is selected based on:

1. Higher F1 score
2. Good accuracy
3. Stable performance across classes
4. Explainable model behavior
5. Suitability for serving

The selected model will be documented with a short decision rationale.

Example rationale:

```text
The Random Forest model was selected because it achieved the best F1 score and handled non-linear relationships between inventory, sales velocity, and supplier lead time better than the baseline model.
```

---

## Model Registry

The selected models will be registered in the Unity Catalog Model Registry.

Example model names:

```text
workspace.retail_capstone.stockout_risk_classifier
workspace.retail_capstone.reorder_flag_classifier
```

Unity Catalog uses model aliases instead of legacy stages.

Planned aliases:

```text
@challenger
@champion
```

Lifecycle process:

```text
New model version
      ↓
Assign @challenger
      ↓
Validate model
      ↓
Promote to @champion
```

---

## Serving Plan

The champion model can be deployed to a Databricks Model Serving endpoint.

Example endpoint names:

```text
stockout-risk-classifier-endpoint
reorder-flag-classifier-endpoint
```

Example real-time use case:

```text
An inventory application sends product inventory features to the endpoint and receives a predicted risk class or reorder recommendation.
```

Example request fields for the stockout risk model:

```json
{
  "available_stock": 25,
  "avg_daily_sales": 8.5,
  "days_of_inventory_remaining": 2.9,
  "lead_time_days": 7,
  "reliability_score": 0.91,
  "category": "Home Decor",
  "warehouse_id": "WH001"
}
```

Example response:

```json
{
  "prediction": "High Risk"
}
```

---

## Pipeline Flow

```text
Bronze Tables
      ↓
Silver Cleaned Tables
      ↓
Gold Inventory KPI Tables
      ↓
ML Feature Table
      ↓
Exploratory Data Analysis
      ↓
MLflow Experiments
      ↓
Model Comparison
      ↓
Unity Catalog Model Registry
      ↓
Model Serving Endpoint
```

---

## GitHub Deliverables

Phase 3 will add the following files to the repository:

```text
docs/ml_modeling_plan.md
docs/model_serving.md
docs/enterprise_readiness.md

notebooks/04_ml_stockout_and_reorder_training.py
notebooks/05_model_serving_test.py

sql/ml_feature_queries.sql

images/mlflow_experiments_screenshot.png
images/serving_endpoint_test.png
```

The `images/` folder will contain screenshots and charts that help explain the model training and serving workflow.

---

## Interview Explanation

A simple way to explain this phase in an interview:

```text
After building the inventory Gold tables, I used them as the foundation for machine learning. I created a classification problem to predict stockout risk and another classification problem to predict whether a product should be reordered.

I used MLflow to track multiple model runs, compared models using accuracy and F1 score, selected the best model, and planned model registration and serving through Databricks Model Serving.
```

---

## Limitations

This phase is designed for learning and demonstration.

Current limitations:

- Training data is synthetically expanded from business rules
- The model is not production-ready
- The sample dataset is small
- Real-world deployment would need more historical inventory movement data
- Production use would require monitoring, retraining, and data drift checks

---

## Future Improvements

Possible future improvements:

- Add real historical inventory movement data
- Add time-series demand forecasting
- Add model monitoring
- Add feature store integration
- Add automated retraining workflows
- Add Databricks Model Serving endpoint tests
- Add CI/CD for ML notebooks
- Add Hugging Face or MLflow model export workflow
