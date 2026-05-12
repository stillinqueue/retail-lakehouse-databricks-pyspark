# Workflow Plan

## Job Name

Retail Online Sales Lakehouse Pipeline

## Schedule

The pipeline should run every night at 1:00 AM after daily sales data arrives from upstream systems.

## Task Design

| Task | Step | Depends On | Purpose |
|---|---|---|---|
| 1 | Ingest Raw Data | None | Load source eCommerce data into a raw Delta table |
| 2 | Clean Data | Task 1 | Remove invalid rows, cancellations, null customers, and calculate revenue |
| 3 | Aggregate Metrics | Task 2 | Build daily country revenue, product revenue, and customer summary tables |
| 4 | Incremental MERGE | Task 1 / staging load | Insert new rows and update existing rows |
| 5 | Dashboard Refresh | Task 3 | Refresh reporting queries for business users |

## Dependency Flow

```text
Ingest Raw Data
      ↓
Clean Data
      ↓
Aggregate Metrics
      ↓
Dashboard Refresh
