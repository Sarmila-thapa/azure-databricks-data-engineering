from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="vendor_catalog.gold.vendor_order_sales_gold",
    comment="Business-ready vendor order sales table"
)
def vendor_order_sales():
    orders = spark.read.table("vendor_catalog.silver.vendor_orders_silver")
    customers = spark.read.table("vendor_catalog.silver.vendor_customers_silver")
    products = spark.read.table("vendor_catalog.silver.vendor_products_silver")

    return (
        orders.alias("o")
        .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(products.alias("p"), F.col("o.product_id") == F.col("p.product_id"), "left")
        .select(
            F.col("o.order_date").alias("order_date"),
            F.col("o.order_id").alias("order_id"),
            F.col("o.customer_id").alias("customer_id"),
            F.concat_ws(" ", F.col("c.first_name"), F.col("c.last_name")).alias("customer_name"),
            F.col("c.country").alias("country"),
            F.col("c.city").alias("city"),
            F.col("o.product_id").alias("product_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.category").alias("category"),
            F.col("o.quantity").alias("quantity"),
            F.col("p.price").alias("price"),
            (F.col("o.quantity") * F.col("p.price")).cast("decimal(12,2)").alias("sales_amount")
        )
    )