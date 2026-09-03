CREATE OR REFRESH STREAMING TABLE silver_products
AS
SELECT
    CAST(product.id AS INT) AS product_id,

    TRIM(product.title) AS title,
    TRIM(product.description) AS description,
    TRIM(product.category) AS category,
    TRIM(product.brand) AS brand,
    TRIM(product.sku) AS sku,

    CAST(product.price AS DECIMAL(12,2)) AS price,
    CAST(product.discountPercentage AS DECIMAL(5,2)) AS discount_percentage,
    CAST(product.rating AS DECIMAL(3,2)) AS rating,
    CAST(product.stock AS INT) AS stock,
    CAST(product.weight AS DECIMAL(10,2)) AS weight,

    -- Dimensions
    CAST(product.dimensions.width AS DECIMAL(10,2)) AS width,
    CAST(product.dimensions.height AS DECIMAL(10,2)) AS height,
    CAST(product.dimensions.depth AS DECIMAL(10,2)) AS depth,

    -- Product information
    TRIM(product.warrantyInformation) AS warranty_information,
    TRIM(product.shippingInformation) AS shipping_information,
    TRIM(product.availabilityStatus) AS availability_status,
    TRIM(product.returnPolicy) AS return_policy,
    CAST(product.minimumOrderQuantity AS INT) AS minimum_order_quantity,

    -- Arrays
    product.tags AS tags,
    product.images AS images,

    -- Metadata
    CAST(product.meta.createdAt AS TIMESTAMP) AS created_at,
    CAST(product.meta.updatedAt AS TIMESTAMP) AS updated_at,
    product.meta.barcode AS barcode,
    product.meta.qrCode AS qr_code,

    product.thumbnail AS thumbnail

FROM STREAM(live.bronze_products)

WHERE product.id IS NOT NULL;