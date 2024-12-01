{{
  config(
    materialized='ephemeral'
  )
}}

{% set order_statuses = obtener_valores(ref('stg_sql_server_dbo__orders'), 'status') %}

WITH stg_orders AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

order_status_summary AS (
    SELECT
        user_id,
        {%- for status in order_statuses %}
        SUM(CASE WHEN status = '{{status}}' THEN 1 ELSE 0 END) AS {{status}}_orders
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM stg_orders
    GROUP BY user_id
)

SELECT * FROM order_status_summary