from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="vendor_catalog.gold.vendor_sales_by_category_gold",
    comment="Category-level sales summary for vendor orders"
)
def vendor_sales_by_category():
    sales = spark.read.table("vendor_catalog.gold.vendor_order_sales_gold")

    return sales.groupBy("category").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units"),
        F.sum("sales_amount").cast("decimal(14,2)").alias("total_sales")
    )