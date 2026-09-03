# Databricks notebook source
# DBTITLE 1,Build Gold sales table
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS vendor_catalog.gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.gold.vendor_order_sales AS
# MAGIC SELECT o.order_date,
# MAGIC        o.order_id,
# MAGIC        o.customer_id,
# MAGIC        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
# MAGIC        c.country,
# MAGIC        c.city,
# MAGIC        o.product_id,
# MAGIC        p.product_name,
# MAGIC        p.category,
# MAGIC        o.quantity,
# MAGIC        p.price,
# MAGIC        CAST(o.quantity * p.price AS DECIMAL(12,2)) AS sales_amount
# MAGIC FROM vendor_catalog.silver.vendor_orders o
# MAGIC LEFT JOIN vendor_catalog.silver.vendor_customers c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC LEFT JOIN vendor_catalog.silver.vendor_products p
# MAGIC   ON o.product_id = p.product_id;

# COMMAND ----------

# DBTITLE 1,Query Gold sample data
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vendor_catalog.gold.vendor_order_sales
# MAGIC ORDER BY order_date, order_id
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Query Gold sales by category
# MAGIC %sql
# MAGIC SELECT category,
# MAGIC        COUNT(DISTINCT order_id) AS total_orders,
# MAGIC        SUM(quantity) AS total_units,
# MAGIC        SUM(sales_amount) AS total_sales
# MAGIC FROM vendor_catalog.gold.vendor_order_sales
# MAGIC GROUP BY category
# MAGIC ORDER BY total_sales DESC;

# COMMAND ----------

# DBTITLE 1,Query Gold sales by country
# MAGIC %sql
# MAGIC SELECT country,
# MAGIC        COUNT(DISTINCT order_id) AS total_orders,
# MAGIC        SUM(sales_amount) AS total_sales,
# MAGIC        AVG(sales_amount) AS average_order_value
# MAGIC FROM vendor_catalog.gold.vendor_order_sales
# MAGIC GROUP BY country
# MAGIC ORDER BY total_sales DESC;