# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Create Silver schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS vendor_catalog.silver

# COMMAND ----------

# DBTITLE 1,Silver — customers (deduped)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.silver.ss_customers AS
# MAGIC WITH ranked AS (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _commit_timestamp DESC) AS rn
# MAGIC   FROM vendor_catalog.bronze.customers
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC )
# MAGIC SELECT * EXCEPT (rn)
# MAGIC FROM ranked
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,Silver — products (deduped)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.silver.ss_products AS
# MAGIC WITH ranked AS (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _commit_timestamp DESC) AS rn
# MAGIC   FROM vendor_catalog.bronze.products
# MAGIC   WHERE product_id IS NOT NULL
# MAGIC )
# MAGIC SELECT * EXCEPT (rn)
# MAGIC FROM ranked
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,Silver — orders (deduped)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.silver.ss_orders AS
# MAGIC WITH ranked AS (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _commit_timestamp DESC) AS rn
# MAGIC   FROM vendor_catalog.bronze.orders
# MAGIC   WHERE order_id IS NOT NULL
# MAGIC     AND customer_id IS NOT NULL
# MAGIC     AND product_id IS NOT NULL
# MAGIC )
# MAGIC SELECT * EXCEPT (rn)
# MAGIC FROM ranked
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,Silver row count validation
# MAGIC %sql
# MAGIC SELECT 'ss_customers' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.silver.ss_customers
# MAGIC UNION ALL
# MAGIC SELECT 'ss_products',  COUNT(*) FROM vendor_catalog.silver.ss_products
# MAGIC UNION ALL
# MAGIC SELECT 'ss_orders',    COUNT(*) FROM vendor_catalog.silver.ss_orders
# MAGIC ORDER BY table_name

# COMMAND ----------

# DBTITLE 1,Sample — Silver orders
# MAGIC %sql
# MAGIC SELECT * FROM vendor_catalog.silver.ss_orders ORDER BY order_id LIMIT 10