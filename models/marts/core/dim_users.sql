    WITH stg_users AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__users')}}
    ),

    stg_orders AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__orders')}}
    ),


dim_users_gold AS (
    SELECT
          u.user_id
        , u.first_name
        , u.last_name
        , u.email
        , u.phone_number
        , u.created_at_utc
        , u.updated_at_utc
        , u.address_id
        , COUNT(o.order_id) AS total_orders
        ,  ROUND(SUM(o.total_order_cost_usd), 2) AS total_spend
        , MIN(o.created_at_utc) AS first_order_date
    FROM stg_users u 
    LEFT JOIN 
        stg_orders o ON u.user_id = o.user_id
    GROUP BY 
        u.user_id, u.first_name, u.last_name, u.email, u.phone_number, u.created_at_utc,u.updated_at_utc, u.address_id
    )

SELECT * FROM dim_users_gold