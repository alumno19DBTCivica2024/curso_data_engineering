    WITH stg_promos AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__promos')}}
    ),

    stg_orders AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__orders')}}
    ),


dim_promos_gold AS (
    SELECT
        p.promo_id,
        p.status,
        p.discount,
        COUNT(o.order_id) AS total_orders,
        SUM(o.item_order_cost_usd * (p.discount / 100)) as total_discount_given
    FROM stg_promos p
    LEFT JOIN 
        stg_orders o ON p.promo_id = o.promo_id
    GROUP BY 
       p.promo_id, p.status, p.discount
)

SELECT * FROM dim_promos_gold