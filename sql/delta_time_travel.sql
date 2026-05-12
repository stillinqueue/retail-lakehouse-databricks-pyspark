-- Check Delta table history

DESCRIBE HISTORY workspace.retail_capstone.raw_online_retail;


-- Query older version using Delta time travel

SELECT *
FROM workspace.retail_capstone.raw_online_retail
VERSION AS OF 0;


-- Validate older version row count

SELECT COUNT(*)
FROM workspace.retail_capstone.raw_online_retail
VERSION AS OF 0;


-- Example restore command
-- Do not run unless restore is required

RESTORE TABLE workspace.retail_capstone.raw_online_retail
TO VERSION AS OF 0;
