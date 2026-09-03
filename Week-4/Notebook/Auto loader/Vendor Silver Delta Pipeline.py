# Databricks notebook source
# DBTITLE 1,Build Silver Delta tables
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS vendor_catalog.silver;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.silver.vendor_products AS
# MAGIC SELECT product_id,
# MAGIC        product_name,
# MAGIC        category,
# MAGIC        price,
# MAGIC        _ingest_timestamp,
# MAGIC        _source_file
# MAGIC FROM (
# MAGIC   SELECT CAST(product_id AS INT) AS product_id,
# MAGIC          TRIM(product_name) AS product_name,
# MAGIC          TRIM(category) AS category,
# MAGIC          CAST(price AS DECIMAL(10,2)) AS price,
# MAGIC          _ingest_timestamp,
# MAGIC          _source_file,
# MAGIC          ROW_NUMBER() OVER (
# MAGIC            PARTITION BY CAST(product_id AS INT), TRIM(product_name), TRIM(category), CAST(price AS DECIMAL(10,2))
# MAGIC            ORDER BY _ingest_timestamp DESC, _source_file DESC
# MAGIC          ) AS rn
# MAGIC   FROM vendor_catalog.bronze.vendor_products_csv
# MAGIC )
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.silver.vendor_customers AS
# MAGIC SELECT customer_id,
# MAGIC        first_name,
# MAGIC        last_name,
# MAGIC        email,
# MAGIC        country,
# MAGIC        city,
# MAGIC        _ingest_timestamp,
# MAGIC        _source_file
# MAGIC FROM (
# MAGIC   SELECT CAST(customer_id AS INT) AS customer_id,
# MAGIC          TRIM(first_name) AS first_name,
# MAGIC          TRIM(last_name) AS last_name,
# MAGIC          LOWER(TRIM(email)) AS email,
# MAGIC          TRIM(country) AS country,
# MAGIC          TRIM(city) AS city,
# MAGIC          _ingest_timestamp,
# MAGIC          _source_file,
# MAGIC          ROW_NUMBER() OVER (
# MAGIC            PARTITION BY CAST(customer_id AS INT)
# MAGIC            ORDER BY _ingest_timestamp DESC, TRIM(city) DESC, LOWER(TRIM(email)) DESC
# MAGIC          ) AS rn
# MAGIC   FROM vendor_catalog.bronze.vendor_customers_csv
# MAGIC )
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE vendor_catalog.silver.vendor_orders AS
# MAGIC SELECT order_id,
# MAGIC        customer_id,
# MAGIC        product_id,
# MAGIC        quantity,
# MAGIC        order_date,
# MAGIC        _ingest_timestamp,
# MAGIC        _source_file
# MAGIC FROM (
# MAGIC   SELECT CAST(order_id AS INT) AS order_id,
# MAGIC          CAST(customer_id AS INT) AS customer_id,
# MAGIC          CAST(product_id AS INT) AS product_id,
# MAGIC          CAST(quantity AS INT) AS quantity,
# MAGIC          TO_DATE(order_date) AS order_date,
# MAGIC          _ingest_timestamp,
# MAGIC          _source_file,
# MAGIC          ROW_NUMBER() OVER (
# MAGIC            PARTITION BY CAST(order_id AS INT), CAST(customer_id AS INT), CAST(product_id AS INT), CAST(quantity AS INT), TO_DATE(order_date)
# MAGIC            ORDER BY _ingest_timestamp DESC, _source_file DESC
# MAGIC          ) AS rn
# MAGIC   FROM vendor_catalog.bronze.vendor_orders_csv
# MAGIC )
# MAGIC WHERE rn = 1;

# COMMAND ----------

# DBTITLE 1,Query Silver row counts
# MAGIC %sql
# MAGIC SELECT 'vendor_products' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.silver.vendor_products
# MAGIC UNION ALL
# MAGIC SELECT 'vendor_customers' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.silver.vendor_customers
# MAGIC UNION ALL
# MAGIC SELECT 'vendor_orders' AS table_name, COUNT(*) AS row_count FROM vendor_catalog.silver.vendor_orders
# MAGIC ORDER BY table_name;

# COMMAND ----------

# DBTITLE 1,Query Silver customer sample
# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC        first_name,
# MAGIC        last_name,
# MAGIC        email,
# MAGIC        country,
# MAGIC        city
# MAGIC FROM vendor_catalog.silver.vendor_customers
# MAGIC ORDER BY customer_id
# MAGIC LIMIT 5;