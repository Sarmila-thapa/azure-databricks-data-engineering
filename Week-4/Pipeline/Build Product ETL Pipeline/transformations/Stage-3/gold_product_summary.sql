CREATE OR REFRESH STREAMING TABLE gold_products
AS
SELECT
    product_id,
    title,
    description,
    category,
    brand,
    sku,

    -- Original pricing
    price,
    discount_percentage,

    -- Calculated pricing
    CAST(
        price * (1 - discount_percentage / 100)
        AS DECIMAL(12,2)
    ) AS final_price,

    CAST(
        price * discount_percentage / 100
        AS DECIMAL(12,2)
    ) AS discount_amount,

    -- Product rating
    rating,

    CASE
        WHEN rating >= 4.5 THEN 'Excellent'
        WHEN rating >= 4.0 THEN 'Good'
        WHEN rating >= 3.0 THEN 'Average'
        ELSE 'Poor'
    END AS rating_category,

    -- Inventory
    stock,

    CASE
        WHEN stock = 0 THEN 'Out of Stock'
        WHEN stock <= 10 THEN 'Low Stock'
        WHEN stock <= 50 THEN 'Medium Stock'
        ELSE 'High Stock'
    END AS stock_status,

    -- Inventory value
    CAST(
        price * stock
        AS DECIMAL(14,2)
    ) AS inventory_value,

    -- Physical information
    weight,
    width,
    height,
    depth,

    -- Product information
    warranty_information,
    shipping_information,
    availability_status,
    return_policy,
    minimum_order_quantity,

    -- Product metadata
    barcode,
    created_at,
    updated_at,

    -- Images
    thumbnail,

    -- Arrays retained from Silver
    tags,
    images,

    -- Pipeline audit timestamp
    current_timestamp() AS gold_processed_at

FROM STREAM(live.silver_products)

WHERE product_id IS NOT NULL;