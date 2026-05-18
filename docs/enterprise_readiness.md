# Enterprise Readiness

## Purpose

This document explains how the Databricks eCommerce Lakehouse project can be organized for development, production, version control, reproducibility, and model deployment readiness.

The goal is to show how the project can move from a learning capstone into a more production-style workflow.

## Project Context

This project has three phases:

1. Sales Lakehouse Pipeline
2. Inventory Management Medallion Pipeline
3. MLflow Stockout and Reorder Classification

Phase 3 adds machine learning capabilities using MLflow, Unity Catalog Model Registry, and Databricks Model Serving.

Enterprise readiness focuses on making the project easier to manage, reproduce, test, and deploy.

---

# 1. Databricks Workspace Folder Structure

A production-ready Databricks workspace should separate development work from production-ready notebooks.

## Recommended Workspace Structure

```text
/Workspace/Users/<user-email>/retail-capstone/
│
├── dev/
│   ├── 01_bronze_ingestion
│   ├── 02_silver_cleaning
│   ├── 03_gold_inventory_kpis
│   ├── 04_ml_stockout_and_reorder_training
│   └── 05_model_serving_test
│
├── prod/
│   ├── 01_bronze_ingestion
│   ├── 02_silver_cleaning
│   ├── 03_gold_inventory_kpis
│   ├── 04_ml_stockout_and_reorder_training
│   └── 05_model_serving_test
│
└── experiments/
    └── mlflow_runs
```

## Development Folder

The `dev` folder is used for:

- Writing new code
- Testing transformations
- Debugging errors
- Trying different model parameters
- Running experiments
- Making changes before production promotion

Example:

```text
/Workspace/Users/<user-email>/retail-capstone/dev
```

## Production Folder

The `prod` folder is used for:

- Stable notebooks
- Scheduled Databricks Jobs
- Production-ready pipeline runs
- Model training workflows
- Serving workflow validation

Example:

```text
/Workspace/Users/<user-email>/retail-capstone/prod
```

## Why Separate Dev and Prod?

Separating development and production helps prevent unfinished or experimental code from affecting stable pipeline runs.

This separation supports:

- Safer development
- Better testing
- Cleaner production execution
- Easier debugging
- More reliable scheduled jobs

---

# 2. GitHub Repository Structure

The project code is stored in GitHub so that changes can be tracked and reviewed.

## Repository Structure

```text
databricks-ecommerce-lakehouse-capstone/
│
├── README.md
│
├── notebooks/
│   ├── ecommerce_lakehouse_capstone.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_cleaning.py
│   ├── 03_gold_inventory_kpis.py
│   ├── 04_ml_stockout_and_reorder_training.py
│   └── 05_model_serving_test.py
│
├── docs/
│   ├── architecture.md
│   ├── data_quality_rules.md
│   ├── recovery_runbook.md
│   ├── workflow_plan.md
│   ├── ml_modeling_plan.md
│   ├── model_serving.md
│   └── enterprise_readiness.md
│
├── sql/
│   ├── dashboard_queries.sql
│   ├── delta_time_travel.sql
│   ├── inventory_kpi_queries.sql
│   └── ml_feature_queries.sql
│
├── data_samples/
│   ├── products_sample.csv
│   ├── inventory_sample.csv
│   ├── suppliers_sample.csv
│   └── warehouses_sample.csv
│
└── images/
    ├── mlflow_experiments_screenshot.png
    ├── serving_endpoint_test.png
    └── architecture_diagram.png
```

## Why Use GitHub?

GitHub provides:

- Version control
- Commit history
- Code backup
- Collaboration support
- Pull request workflow
- Clear project documentation
- Reproducibility for interviews and future work

---

# 3. Databricks Repos Integration

Databricks Repos can connect the Databricks workspace directly to the GitHub repository.

This allows notebooks to be version-controlled from inside Databricks.

## Recommended Databricks Repos Path

```text
/Repos/<user-email>/databricks-ecommerce-lakehouse-capstone
```

## Benefits of Databricks Repos

Databricks Repos supports:

- Pulling latest code from GitHub
- Committing notebook changes
- Creating branches
- Switching branches
- Reviewing commit history
- Keeping Databricks notebooks synchronized with GitHub

## Recommended Workflow

```text
GitHub Repository
      ↓
Databricks Repos
      ↓
Development Notebook Changes
      ↓
Commit Changes
      ↓
Push to GitHub
      ↓
Promote Stable Version to Production
```

---

# 4. Git Branching Strategy

A simple branching strategy can make the project easier to manage.

## Recommended Branches

```text
main
dev
feature/ml-stockout-model
feature/model-serving
```

## Branch Purpose

| Branch | Purpose |
|---|---|
| `main` | Stable version of the project |
| `dev` | Development and testing changes |
| `feature/ml-stockout-model` | Work on ML training notebook |
| `feature/model-serving` | Work on serving endpoint test notebook |

## Beginner-Friendly Workflow

1. Keep the stable project in `main`
2. Create feature branches for new work
3. Test changes in Databricks
4. Commit the notebook changes
5. Merge into `main` after validation

---

# 5. Commit History

The project should include multiple commits to show progress and reproducibility.

## Example Commit Messages

```text
Add bronze ingestion notebook
Add silver cleaning notebook
Add gold inventory KPI notebook
Add ML modeling plan
Add stockout and reorder training notebook
Add model serving test notebook
Update README with Phase 3 ML pipeline
```

## Why Commit History Matters

Commit history shows:

- How the project evolved
- Which changes were made
- When notebooks were added
- How documentation improved
- That the work is version-controlled

For the capstone, the training notebook should have at least two commits to demonstrate version control.

---

# 6. Reproducibility

A reproducible project means another person can understand and rerun the pipeline.

## Reproducibility Requirements

The repository should include:

- Source notebook files
- SQL queries
- Sample CSV data
- Data quality rules
- Architecture documentation
- ML modeling plan
- Model serving plan
- Recovery runbook
- Workflow plan

## Reproducible Pipeline Flow

```text
Sample Data
      ↓
Bronze Delta Tables
      ↓
Silver Cleaned Tables
      ↓
Gold KPI Tables
      ↓
ML Feature Table
      ↓
MLflow Experiments
      ↓
Model Registry
      ↓
Serving Endpoint
```

## Environment Notes

The project is designed for Databricks with:

- Python
- PySpark
- Delta Lake
- MLflow
- Unity Catalog
- Databricks Model Serving

---

# 7. MLflow Experiment Tracking

MLflow is used to track machine learning experiments.

Each MLflow run should log:

- Model type
- Hyperparameters
- Accuracy
- F1 score
- Training data version
- Model artifacts
- Feature columns
- Run timestamp

## Why MLflow Helps

MLflow makes the model training process reproducible and auditable.

It allows users to compare different model runs and understand why a specific model was selected.

## Example Metrics

```text
accuracy
f1_score
weighted_f1_score
```

## Example Parameters

```text
model_type
max_depth
num_trees
regularization
feature_columns
target_column
```

---

# 8. Unity Catalog Model Registry

The best model should be registered in Unity Catalog Model Registry.

## Example Registered Models

```text
workspace.retail_capstone.stockout_risk_classifier
workspace.retail_capstone.reorder_flag_classifier
```

## Model Aliases

Unity Catalog uses aliases instead of the older model stage transitions.

Recommended aliases:

```text
@challenger
@champion
```

## Model Lifecycle

```text
New model version
      ↓
Assign @challenger
      ↓
Validate model metrics and predictions
      ↓
Promote to @champion
      ↓
Deploy champion model to serving endpoint
```

## Why Aliases Matter

Aliases make it easier to control which model version is used by production systems.

For example, an application can call the `@champion` model without needing to know the exact model version number.

---

# 9. Model Serving Readiness

The selected model can be deployed using Databricks Model Serving.

## Example Serving Endpoints

```text
stockout-risk-classifier-endpoint
reorder-flag-classifier-endpoint
```

## Serving Input Example

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

## Serving Output Example

```json
{
  "predictions": ["High Risk"]
}
```

## Serving Validation

The serving endpoint should be tested using:

- Databricks Model Serving UI
- Python request
- curl command

The test should confirm that the endpoint returns a valid prediction for sample input data.

---

# 10. Development to Production Promotion

A simple promotion process can be used.

## Recommended Promotion Flow

```text
Develop notebook in dev folder
      ↓
Test notebook manually
      ↓
Commit notebook to GitHub
      ↓
Review results and metrics
      ↓
Copy or promote notebook to prod folder
      ↓
Schedule as Databricks Job
```

## Promotion Criteria

A notebook should move to production only after:

- It runs successfully end-to-end
- Output tables are validated
- MLflow metrics are logged
- The selected model is registered
- Documentation is updated
- GitHub commit is completed

---

# 11. Scheduled Jobs

Production notebooks can be scheduled using Databricks Jobs.

## Recommended Job Tasks

```text
01 Bronze Ingestion
      ↓
02 Silver Cleaning
      ↓
03 Gold Inventory KPIs
      ↓
04 ML Stockout and Reorder Training
      ↓
05 Model Serving Test
```

## Why Use Databricks Jobs?

Databricks Jobs provide:

- Automated execution
- Task dependencies
- Retry handling
- Monitoring
- Alerts
- Cluster configuration
- Scheduled production runs

---

# 12. Security and Access Control

A production environment should include access control.

## Recommended Controls

- Use Unity Catalog permissions
- Restrict production tables to approved users
- Store secrets in Databricks secrets
- Avoid hardcoding tokens
- Do not commit credentials to GitHub
- Use service principals for production jobs where possible

## Files That Should Not Be Committed

Do not commit:

```text
Databricks tokens
Workspace access tokens
Personal credentials
Private keys
Cloud credentials
Production secrets
```

---

# 13. Monitoring and Maintenance

A production ML pipeline should be monitored over time.

## Data Pipeline Monitoring

Monitor:

- Job success and failure
- Row counts
- Data freshness
- Null values
- Schema changes
- Data quality failures

## Model Monitoring

Monitor:

- Prediction volume
- Prediction distribution
- Input feature drift
- Model accuracy over time
- Endpoint latency
- Endpoint errors

---

# 14. Current Project Status

The current project demonstrates the structure required for enterprise readiness.

Completed:

- GitHub repository
- Notebook organization
- Medallion architecture documentation
- Data quality rules
- Recovery runbook
- Workflow plan
- ML modeling plan
- Model serving plan

Planned / in progress:

- ML training notebook
- MLflow experiment tracking
- Model registry aliases
- Serving endpoint test
- Architecture diagram
- MLflow screenshot
- Serving test screenshot

---

# 15. Summary

This enterprise readiness plan shows how the project can be organized for real-world development and production use.

The key practices are:

- Separate development and production folders
- Use GitHub for version control
- Connect Databricks Repos to GitHub
- Track experiments with MLflow
- Register models in Unity Catalog
- Use model aliases for lifecycle management
- Test serving endpoints
- Document architecture and workflow decisions
- Avoid committing secrets or credentials
- Use scheduled jobs for production pipelines
