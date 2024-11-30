    WITH stg_products AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__products')}}
    ),
    dim_products_gold AS (
        SELECT
            p.product_id,
            p.name,
            p.price,
            p.inventory,
            p.stock_status
        FROM stg_products p
        
    )

SELECT * FROM dim_products_gold