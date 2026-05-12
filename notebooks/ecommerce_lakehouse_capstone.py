# Databricks notebook source
spark

# COMMAND ----------

print("Databricks Spark is ready!")

# COMMAND ----------

spark.sql("SELECT current_catalog(), current_schema()").show()

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.retail_capstone")
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA retail_capstone")

# COMMAND ----------

spark.sql("SELECT current_catalog(), current_schema()").show()

# COMMAND ----------

source_df = spark.table("workspace.retail_capstone.online_retail_source")

display(source_df)

# COMMAND ----------

source_df.printSchema()

# COMMAND ----------

raw_table = "workspace.retail_capstone.raw_online_retail"

source_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(raw_table)

# COMMAND ----------

spark.sql("DESCRIBE DETAIL workspace.retail_capstone.raw_online_retail").display()

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, round, current_timestamp

# COMMAND ----------

raw_df = spark.table("workspace.retail_capstone.raw_online_retail")

curated_df = raw_df.select(
    col("InvoiceNo").alias("invoice_no"),
    col("StockCode").alias("stock_code"),
    col("Description").alias("product_description"),
    col("Quantity").alias("quantity"),
    col("InvoiceDate").alias("invoice_date"),
    col("UnitPrice").alias("unit_price"),
    col("CustomerID").alias("customer_id"),
    col("Country").alias("country")
).filter(
    col("invoice_no").isNotNull()
).filter(
    col("stock_code").isNotNull()
).filter(
    col("product_description").isNotNull()
).filter(
    col("customer_id").isNotNull()
).filter(
    col("quantity") > 0
).filter(
    col("unit_price") > 0
).filter(
    ~col("invoice_no").startswith("C")
).withColumn(
    "invoice_timestamp",
    to_timestamp(col("invoice_date"))
).withColumn(
    "revenue",
    round(col("quantity") * col("unit_price"), 2)
).withColumn(
    "processed_timestamp",
    current_timestamp()
)

# COMMAND ----------

curated_table = "workspace.retail_capstone.curated_online_retail"

curated_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(curated_table)

# COMMAND ----------

display(spark.table("workspace.retail_capstone.curated_online_retail"))

# COMMAND ----------

from pyspark.sql.functions import col, to_date, round, sum, countDistinct

# COMMAND ----------

curated_df = spark.table("workspace.retail_capstone.curated_online_retail")

daily_country_revenue_df = curated_df.withColumn(
    "invoice_date_only",
    to_date(col("invoice_timestamp"))
).groupBy(
    "invoice_date_only",
    "country"
).agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    countDistinct("invoice_no").alias("total_orders")
)

# COMMAND ----------

daily_country_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.retail_capstone.daily_country_revenue")

# COMMAND ----------

display(spark.table("workspace.retail_capstone.daily_country_revenue"))

# COMMAND ----------

product_revenue_summary_df = curated_df.groupBy(
    "stock_code",
    "product_description"
).agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    sum("quantity").alias("total_quantity_sold"),
    countDistinct("invoice_no").alias("total_orders")
)

# COMMAND ----------

product_revenue_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.retail_capstone.product_revenue_summary")

# COMMAND ----------

display(spark.table("workspace.retail_capstone.product_revenue_summary"))

# COMMAND ----------

customer_summary_df = curated_df.groupBy(
    "customer_id",
    "country"
).agg(
    round(sum("revenue"), 2).alias("customer_total_revenue"),
    countDistinct("invoice_no").alias("customer_total_orders"),
    sum("quantity").alias("customer_total_quantity")
)

# COMMAND ----------

customer_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.retail_capstone.customer_summary")

# COMMAND ----------

display(spark.table("workspace.retail_capstone.customer_summary"))

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.retail_capstone").show(truncate=False)

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

incremental_data = [
    Row(
        invoice_no="999999",
        stock_code="TEST001",
        product_description="Interview Practice Product",
        quantity=3,
        invoice_date="2026-05-12 10:00:00",
        unit_price=25.0,
        customer_id=99999.0,
        country="Germany"
    ),
    Row(
        invoice_no="999998",
        stock_code="TEST002",
        product_description="Databricks Practice Product",
        quantity=2,
        invoice_date="2026-05-12 11:00:00",
        unit_price=40.0,
        customer_id=99998.0,
        country="Germany"
    )
]

incremental_df = spark.createDataFrame(incremental_data)

display(incremental_df)

# COMMAND ----------

incremental_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.retail_capstone.staging_online_retail")

# COMMAND ----------

display(spark.table("workspace.retail_capstone.staging_online_retail"))

# COMMAND ----------

spark.sql("""
MERGE INTO workspace.retail_capstone.raw_online_retail AS target
USING workspace.retail_capstone.staging_online_retail AS source
ON target.InvoiceNo = source.invoice_no
AND target.StockCode = source.stock_code

WHEN MATCHED THEN UPDATE SET
  target.Description = source.product_description,
  target.Quantity = source.quantity,
  target.InvoiceDate = source.invoice_date,
  target.UnitPrice = source.unit_price,
  target.CustomerID = source.customer_id,
  target.Country = source.country

WHEN NOT MATCHED THEN INSERT (
  InvoiceNo,
  StockCode,
  Description,
  Quantity,
  InvoiceDate,
  UnitPrice,
  CustomerID,
  Country
)
VALUES (
  source.invoice_no,
  source.stock_code,
  source.product_description,
  source.quantity,
  source.invoice_date,
  source.unit_price,
  source.customer_id,
  source.country
)
""")

# COMMAND ----------

spark.sql("""
SELECT *
FROM workspace.retail_capstone.raw_online_retail
WHERE InvoiceNo IN ('999999', '999998')
""").display()

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY workspace.retail_capstone.raw_online_retail
""").display()

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY workspace.retail_capstone.raw_online_retail
""").display()

# COMMAND ----------

spark.sql("""
SELECT *
FROM workspace.retail_capstone.raw_online_retail
VERSION AS OF 0
LIMIT 20
""").display()

# COMMAND ----------

spark.sql("""
SELECT *
FROM workspace.retail_capstone.raw_online_retail
VERSION AS OF 0
WHERE InvoiceNo IN ('999999', '999998')
""").display()

# COMMAND ----------

spark.sql("""
SELECT *
FROM workspace.retail_capstone.raw_online_retail
WHERE InvoiceNo IN ('999999', '999998')
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Recovery Runbook
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This recovery runbook explains how to audit and recover a Delta table after a bad load, accidental overwrite, or incorrect merge.
# MAGIC
# MAGIC ## Step 1: Inspect table history
# MAGIC
# MAGIC Use DESCRIBE HISTORY to review previous versions of the Delta table.
# MAGIC
# MAGIC SQL command:
# MAGIC
# MAGIC     DESCRIBE HISTORY workspace.retail_capstone.raw_online_retail;
# MAGIC
# MAGIC This shows the table version, timestamp, user, operation type, and operation parameters.
# MAGIC
# MAGIC ## Step 2: Identify the correct version
# MAGIC
# MAGIC Find the version before the bad operation. For example, if a bad MERGE happened at version 4, then version 3 may be the last correct version.
# MAGIC
# MAGIC ## Step 3: Validate the older version before restoring
# MAGIC
# MAGIC Before restoring, query the older version using Delta time travel.
# MAGIC
# MAGIC SQL command:
# MAGIC
# MAGIC     SELECT *
# MAGIC     FROM workspace.retail_capstone.raw_online_retail
# MAGIC     VERSION AS OF 3;
# MAGIC
# MAGIC Also validate the row count:
# MAGIC
# MAGIC     SELECT COUNT(*)
# MAGIC     FROM workspace.retail_capstone.raw_online_retail
# MAGIC     VERSION AS OF 3;
# MAGIC
# MAGIC This validation step is important because restoring without checking can bring back the wrong version.
# MAGIC
# MAGIC ## Step 4: Restore the table
# MAGIC
# MAGIC After confirming the correct version, restore the table.
# MAGIC
# MAGIC SQL command:
# MAGIC
# MAGIC     RESTORE TABLE workspace.retail_capstone.raw_online_retail
# MAGIC     TO VERSION AS OF 3;
# MAGIC
# MAGIC ## Step 5: Validate after restore
# MAGIC
# MAGIC After the restore, check the current table again.
# MAGIC
# MAGIC     SELECT COUNT(*)
# MAGIC     FROM workspace.retail_capstone.raw_online_retail;
# MAGIC
# MAGIC Then check the history again.
# MAGIC
# MAGIC     DESCRIBE HISTORY workspace.retail_capstone.raw_online_retail;
# MAGIC
# MAGIC The restore itself appears as a new operation in the Delta transaction log.
# MAGIC
# MAGIC ## Why the Delta log is append-only
# MAGIC
# MAGIC Delta Lake uses an append-only transaction log. Each operation creates a new table version instead of deleting the previous history. This makes it possible to audit changes, query older versions using time travel, and restore the table to a known good state.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.retail_capstone.raw_online_retail
# MAGIC VERSION AS OF 0;

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.retail_capstone.raw_online_retail
VERSION AS OF 0
""").display()

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.retail_capstone.raw_online_retail
""").display()