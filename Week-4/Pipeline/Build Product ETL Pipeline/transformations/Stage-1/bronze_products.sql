CREATE OR REFRESH STREAMING TABLE bronze_products
AS
SELECT
    explode(products) AS product
FROM STREAM read_files(
    '/Volumes/products_catalog/default/product_pipeline_volume/raw/products/',
    format => 'json',
    multiLine => true
);
