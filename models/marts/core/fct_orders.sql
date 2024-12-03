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

    stg_events AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__events')}}
    ),

    stg_promos AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__promos')}}
    ),    

    fact_orders_gold AS (
        SELECT
            o.order_id,
            o.user_id,
            o.address_id,
            o.promo_id,
            e.session_id,
            o.created_at_utc,
            o.delivered_at_utc,
            o.total_order_cost_usd, -- Coste total incluyendo envío
            o.item_order_cost_usd,  -- Coste total de los productos
            o.shipping_cost_usd,    -- Coste del envío
            o.shipping_service_id,  -- Servicio de envío utilizado
            COUNT(oi.product_id) AS total_items, -- Total de productos distintos en el pedido
            -- Calculamos el descuento aplicado utilizando la relación con la tabla de promociones
            ROUND(COALESCE(SUM(oi.quantity * p.price * (pr.discount / 100)), 0),2) AS discount_applied,
            DATEDIFF('DAY', o.created_at_utc, o.delivered_at_utc) AS delivery_time -- Tiempo de entrega en días
        FROM 
            stg_orders o
        LEFT JOIN 
            stg_order_items oi ON o.order_id = oi.order_id
        LEFT JOIN 
            stg_products p ON oi.product_id = p.product_id
        LEFT JOIN 
            stg_promos pr ON o.promo_id = pr.promo_id -- Relación con promociones
        LEFT JOIN 
            stg_events e ON o.order_id = e.order_id
        GROUP BY 
            o.order_id, 
            o.user_id, 
            o.address_id, 
            o.promo_id, 
            e.session_id, 
            o.total_order_cost_usd, 
            o.item_order_cost_usd, 
            o.shipping_cost_usd, 
            o.shipping_service_id,
            o.created_at_utc, 
            o.delivered_at_utc
    )

    SELECT * FROM fact_orders_gold
