# Recovery Runbook

## Purpose

This recovery runbook explains how to audit and recover a Delta table after a bad data load, accidental overwrite, or incorrect merge.

## Step 1: Inspect Table History

```sql
DESCRIBE HISTORY workspace.retail_capstone.raw_online_retail;
