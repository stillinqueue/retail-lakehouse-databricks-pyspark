# Governance Design

## Purpose

This document describes Phase 5 of the Databricks eCommerce Lakehouse project.

Phase 5 adds a production governance and MLOps layer around the existing lakehouse, inventory, ML, and GenAI pipelines.

The goal is to demonstrate how a Databricks project can be governed, version-controlled, automated, and monitored in a production-style environment.

---

## Governance Scope

This project includes the following governance areas:

- Unity Catalog organization
- Schema and table design
- Delta table documentation
- Volumes for file storage
- Access control using `GRANT` and `REVOKE`
- Service principal automation
- Databricks CLI verification
- GitHub integration
- Branching strategy
- GitHub Actions CI
- Databricks Jobs
- Lakehouse Monitoring

---

## Existing Project Context

The project currently uses the following catalog and schema:

```text
workspace.retail_capstone
```

The schema contains tables from multiple project phases:

- Sales lakehouse tables
- Bronze, Silver, and Gold inventory tables
- ML training feature tables
- RAG document tables

---

## Recommended Production Catalog Design

For a production workspace, the project could be organized with a dedicated governed catalog.

Example catalog:

```text
retail_governance
```

Example schemas:

```text
retail_governance.bronze
retail_governance.silver
retail_governance.gold
retail_governance.ml
retail_governance.genai
```

## Schema Purpose

| Schema | Purpose |
|---|---|
| bronze | Raw ingested source data |
| silver | Cleaned and standardized data |
| gold | Business-ready analytics tables |
| ml | ML feature tables and model training data |
| genai | RAG documents and retrieval/evaluation tables |

---

## Current Repo Adaptation

Because this project was built in a learning workspace, the implementation uses:

```text
workspace.retail_capstone
```

The governance SQL scripts document both:

- The recommended production layout
- The current project-compatible layout

This keeps the project easy to run in a learning workspace while still showing how it could be adapted for production.

---

## Governed Assets

Important governed assets include the following tables.

### Bronze Tables

```text
bronze_sales
bronze_products
bronze_inventory
bronze_suppliers
bronze_warehouses
```

### Silver Tables

```text
silver_sales
silver_products
silver_inventory
silver_suppliers
silver_warehouses
```

### Gold Tables

```text
gold_inventory_status
gold_product_sales_velocity
gold_stockout_risk
gold_reorder_recommendations
gold_inventory_value
```

### ML Tables

```text
ml_stockout_training_data
ml_reorder_training_data
```

### GenAI Tables

```text
inventory_rag_documents
```

---

## Access Control Design

Recommended access control roles:

| Role | Access |
|---|---|
| data_engineer | Can read/write Bronze, Silver, and Gold tables |
| data_analyst | Can read Gold tables only |
| ml_engineer | Can read Gold and ML tables, register models |
| genai_engineer | Can read Gold and GenAI tables |
| service_principal | Can run production jobs and write pipeline outputs |

---

## Access Control Principles

The governance design follows least privilege.

- Analysts should not modify raw or Silver tables.
- Production jobs should run using a service principal.
- ML engineers should access only the tables needed for model development.
- Gold tables should be the main source for dashboards and business reporting.
- RAG assistants should retrieve from controlled Gold and GenAI tables.

---

## Volume Design

A Unity Catalog volume can be used for raw files, documentation, or unstructured artifacts.

Recommended production volume:

```text
retail_governance.bronze.inventory_files
```

Current project-compatible volume:

```text
workspace.retail_capstone.inventory_files
```

Example use cases:

- Store raw CSV extracts
- Store uploaded inventory files
- Store unstructured files for future RAG use cases
- Store monitoring exports or validation artifacts

---

## Monitoring Strategy

Lakehouse Monitoring should be enabled on at least one important Gold table.

Recommended monitored table:

```text
workspace.retail_capstone.gold_stockout_risk
```

Why this table:

- It supports operational inventory decisions.
- It includes business-critical risk labels.
- Freshness and completeness are important.
- It is used downstream by ML and GenAI phases.

---

## CI/CD Strategy

GitHub Actions is used for lightweight CI.

The first CI workflow checks:

- Python syntax
- Basic code quality
- Repository structure

Future CI improvements can include:

- Notebook validation
- SQL linting
- Unit tests
- Databricks Asset Bundles validation
- Automated job deployment

---

## Branching Strategy

The recommended development workflow uses feature branches.

Example branch:

```text
feature/phase5-governance
```

Recommended workflow:

1. Create a feature branch.
2. Add governance documentation and scripts.
3. Run CI checks.
4. Open a pull request.
5. Review changes.
6. Merge into the main branch.

---

## Databricks Jobs Strategy

Production pipelines should be scheduled using Databricks Jobs.

A production job can run:

- Bronze ingestion notebook
- Silver cleaning notebook
- Gold KPI notebook
- ML training notebook
- Model serving test notebook
- RAG document generation notebook
- RAG retrieval or evaluation notebook

Production jobs should use:

- Job compute
- Git-backed notebooks
- A service principal for automation
- Clear task dependencies
- Monitoring and failure alerts

---

## Databricks CLI Verification

Databricks CLI commands can be used to verify governed resources.

Example checks:

- Confirm catalog exists
- Confirm schemas exist
- Confirm service principal exists
- Confirm grants are applied
- Confirm jobs are available

The outputs can be saved in a `cli_outputs/` folder as evidence.

---

## Production Readiness Summary

Phase 5 demonstrates how this lakehouse project can move from learning/demo mode toward production readiness by adding:

- Governance documentation
- Access control scripts
- Service principal automation
- GitHub CI/CD
- Databricks Jobs
- Lakehouse Monitoring

---

