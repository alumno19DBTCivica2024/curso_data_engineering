/*
    Vista que unifica todos los eventos categorizados
     (checkout, package_shipped, add_to_cart, page_view),
      listo para análisis en capas posteriores.
*/
{{
  config(
    materialized='view'
  )
}}

WITH base_checkout AS (
    SELECT *
    FROM {{ref('base_orders_checkout')}}
),

base_package_shipped AS (
    SELECT *
    FROM {{ref('base_orders_package_shipped')}}
),

base_add_to_cart AS (
    SELECT *
    FROM {{ref('base_orders_add_to_cart')}}
),

base_page_view AS (
    SELECT *
    FROM {{ref('base_orders_page_view')}}
),

-- Unificar bases en una sola estructura
unified_events AS(
    SELECT 
        bc.EVENT_ID AS event_id,
        bc.PAGE_URL AS page_url,
        bc.EVENT_TYPE AS event_type,
        bc.USER_ID AS user_id,
        bc.PRODUCT_ID AS product_id,
        bc.SESSION_ID AS session_id,
        bc.CREATED_AT_UTC AS created_at_utc,
        bc.ORDER_ID AS order_id,
        bc.IS_DELETED is_deleted,  -- Asumimos que no hay datos eliminados en staging
        bc.DATE_LOAD AS date_load
    FROM base_checkout bc
    UNION ALL 

    SELECT 
        bps.EVENT_ID AS event_id,
        bps.PAGE_URL AS page_url,
        bps.EVENT_TYPE AS event_type,
        bps.USER_ID AS user_id,
        bps.PRODUCT_ID AS product_id,
        bps.SESSION_ID AS session_id,
        bps.CREATED_AT_UTC AS created_at_utc,
        bps.ORDER_ID AS order_id,
        bps.IS_DELETED is_deleted,  -- Asumimos que no hay datos eliminados en staging
        bps.DATE_LOAD AS date_load
    FROM base_package_shipped bps

    UNION ALL 

    SELECT 
        batc.EVENT_ID AS event_id,
        batc.PAGE_URL AS page_url,
        batc.EVENT_TYPE AS event_type,
        batc.USER_ID AS user_id,
        batc.PRODUCT_ID AS product_id,
        batc.SESSION_ID AS session_id,
        batc.CREATED_AT_UTC AS created_at_utc,
        batc.ORDER_ID AS order_id,
        batc.IS_DELETED is_deleted,  -- Asumimos que no hay datos eliminados en staging
        batc.DATE_LOAD AS date_load
    FROM base_add_to_cart batc

    UNION ALL

    SELECT 
        bpv.EVENT_ID AS event_id,
        bpv.PAGE_URL AS page_url,
        bpv.EVENT_TYPE AS event_type,
        bpv.USER_ID AS user_id,
        bpv.PRODUCT_ID AS product_id,
        bpv.SESSION_ID AS session_id,
        bpv.CREATED_AT_UTC AS created_at_utc,
        bpv.ORDER_ID AS order_id,
        bpv.IS_DELETED is_deleted,  -- Asumimos que no hay datos eliminados en staging
        bpv.DATE_LOAD AS date_load
    FROM base_page_view bpv
)


SELECT
    event_id,
    page_url,
    event_type,
    user_id,
    product_id,
    session_id,
    created_at_utc,
    order_id,
    date_load
FROM unified_events