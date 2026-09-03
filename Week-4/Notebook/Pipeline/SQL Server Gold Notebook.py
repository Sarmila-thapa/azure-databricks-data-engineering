# Databricks notebook source
# DBTITLE 1,Create Gold schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS vendor_catalog.gold

# COMMAND ----------

# DBTITLE 1,Gold — order sales (joined)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.gold.ss_order_sales AS
# MAGIC SELECT
# MAGIC   o.order_id,
# MAGIC   o.order_date,
# MAGIC   o.customer_id,
# MAGIC   CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
# MAGIC   c.country,
# MAGIC   c.city,
# MAGIC   o.product_id,
# MAGIC   p.product_name,
# MAGIC   p.category,
# MAGIC   o.quantity,
# MAGIC   CAST(p.price AS DECIMAL(10,2))                       AS price,
# MAGIC   CAST(o.quantity * p.price AS DECIMAL(12,2))          AS sales_amount
# MAGIC FROM vendor_catalog.silver.ss_orders    o
# MAGIC JOIN vendor_catalog.silver.ss_customers c ON o.customer_id = c.customer_id
# MAGIC JOIN vendor_catalog.silver.ss_products  p ON o.product_id  = p.product_id

# COMMAND ----------

# DBTITLE 1,Gold — sales by category
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.gold.ss_sales_by_category AS
# MAGIC SELECT
# MAGIC   category,
# MAGIC   COUNT(DISTINCT order_id)           AS total_orders,
# MAGIC   SUM(quantity)                      AS total_units,
# MAGIC   CAST(SUM(sales_amount) AS DECIMAL(14,2)) AS total_sales
# MAGIC FROM vendor_catalog.gold.ss_order_sales
# MAGIC GROUP BY category
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# DBTITLE 1,Gold — sales by country
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.gold.ss_sales_by_country AS
# MAGIC SELECT
# MAGIC   country,
# MAGIC   COUNT(DISTINCT order_id)                AS total_orders,
# MAGIC   CAST(SUM(sales_amount) AS DECIMAL(14,2)) AS total_sales,
# MAGIC   CAST(AVG(sales_amount) AS DECIMAL(14,2)) AS average_order_value
# MAGIC FROM vendor_catalog.gold.ss_order_sales
# MAGIC GROUP BY country
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# DBTITLE 1,Gold row count validation
# MAGIC %sql
# MAGIC SELECT 'ss_order_sales'      AS table_name, COUNT(*) AS row_count FROM vendor_catalog.gold.ss_order_sales
# MAGIC UNION ALL
# MAGIC SELECT 'ss_sales_by_category', COUNT(*) FROM vendor_catalog.gold.ss_sales_by_category
# MAGIC UNION ALL
# MAGIC SELECT 'ss_sales_by_country',  COUNT(*) FROM vendor_catalog.gold.ss_sales_by_country
# MAGIC ORDER BY table_name

# COMMAND ----------

# DBTITLE 1,Gold sample — order sales
# MAGIC %sql
# MAGIC SELECT * FROM vendor_catalog.gold.ss_order_sales ORDER BY order_date, order_id LIMIT 10

# COMMAND ----------

# DBTITLE 1,Gold sample — sales by category
# MAGIC %sql
# MAGIC SELECT * FROM vendor_catalog.gold.ss_sales_by_category

# COMMAND ----------

# DBTITLE 1,Gold sample — sales by country
# MAGIC %sql
# MAGIC SELECT * FROM vendor_catalog.gold.ss_sales_by_country