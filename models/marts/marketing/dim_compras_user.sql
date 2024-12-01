WITH dim_users AS (
    SELECT *
    FROM {{ ref('dim_users') }} -- Información del usuario
),
fct_orders AS (
    SELECT *
    FROM {{ ref('fct_orders') }} -- Información del usuario
),
fct_orders AS (
    SELECT *
    FROM {{ ref('fct_orders') }} -- Información del usuario
),
fct_order_items AS (
    SELECT *
    FROM {{ ref('fct_order_items') }} -- Información del usuario
),
dim_address AS (
    SELECT *
    FROM {{ ref('dim_addresses') }} -- Información del usuario
),

dim_orders_aggregated AS (
    -- Agrupamos por usuario para evitar duplicados
    SELECT
        user_id,
        SUM(total_order_cost_usd) AS total_order_cost_usd,
        SUM(shipping_cost_usd) AS total_shipping_cost_usd,
        COUNT(order_id) AS total_number_orders
    FROM fct_orders
    GROUP BY user_id
),

order_items_aggregated AS (
    -- Agrupamos por usuario para calcular productos totales y diferentes
    SELECT
        o.user_id,
        SUM(oi.quantity) AS total_quantity_product,
        COUNT(DISTINCT oi.product_id) AS total_different_products
    FROM fct_order_items oi
    JOIN fct_orders o ON oi.order_id = o.order_id
    GROUP BY o.user_id
),

user_orders AS (
    SELECT
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.phone_number,
        u.created_at_utc,
        u.updated_at_utc,
        a.address,
        a.zipcode_desc,
        a.state_desc,
        a.country_desc,
        o.total_number_orders,                  -- Total de pedidos realizados
        ROUND(o.total_order_cost_usd, 2) AS total_order_cost_usd, -- Total gastado
        ROUND(o.total_shipping_cost_usd, 2) AS total_shipping_cost_usd, -- Total de gastos de envío
        COALESCE(i.total_quantity_product, 0) AS total_quantity_product, -- Total de productos comprados
        COALESCE(i.total_different_products, 0) AS total_different_products -- Total de productos diferentes comprados
    FROM 
        dim_users u
    LEFT JOIN 
        dim_address a ON u.address_id = a.address_id
    LEFT JOIN 
        dim_orders_aggregated o ON u.user_id = o.user_id
    LEFT JOIN 
        order_items_aggregated i ON u.user_id = i.user_id
)

SELECT *
FROM user_orders