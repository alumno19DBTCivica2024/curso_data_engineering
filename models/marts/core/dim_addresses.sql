    WITH stg_address AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__addresses')}}
    ),

    stg_orders AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__orders')}}
    ),


dim_address_gold AS (
    SELECT
        a.address_id,
        a.address,
        a.zipcode_id,
        a.zipcode_desc,
        a.country_id,
        a.country_desc,
        a.state_id,
        a.state_desc,
        COUNT(o.order_id) AS total_orders,
        MAX(o.created_at_utc) AS last_order_date
    FROM stg_address a
    LEFT JOIN 
        stg_orders o ON a.address_id = o.address_id
    GROUP BY 
        a.address_id, a.address, a.zipcode_id,a.zipcode_desc,a.country_desc, a.state_id,a.state_desc, a.country_id
)

SELECT * FROM dim_address_gold