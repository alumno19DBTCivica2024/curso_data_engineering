    WITH stg_orders AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__orders')}}
    ),
    stg_order_items AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__order_items')}}
    ),
    stg_products AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__products')}}        
    ),
    stg_promos AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__promos')}}
    ),    
    fact_order_items_gold AS (
        SELECT 
            oi.order_id,
            oi.product_id,
            p.name AS product_name,
            oi.quantity,
            p.price AS unit_price,
            ROUND((oi.quantity * p.price),2) AS total_price,
            COALESCE(pr.discount,0) AS discount_percentage,
            ROUND(oi.quantity * p.price * (COALESCE(pr.discount, 0) / 100), 2) AS discount_applied
        FROM stg_order_items oi 
            LEFT JOIN stg_products p ON oi.product_id = p.product_id
            LEFT JOIN stg_orders o ON oi.order_id = o.order_id 
            LEFT JOIN stg_promos pr ON o.promo_id = pr.promo_id
    )

    SELECT * FROM fact_order_items_gold