{{
  config(
    materialized='view'
  )
}}

WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
          event_id
        , page_url
        , user_id
        , event_type
        , product_id
        , session_id
        , CONVERT_TIMEZONE('UTC', CREATED_AT) AS created_at_utc
        , null as  order_id
        , UPPER(COALESCE(_FIVETRAN_DELETED, 'false')) as is_deleted
        , _fivetran_synced AS date_load
    FROM src_events
    WHERE EVENT_TYPE = 'add_to_cart'
    )

SELECT * FROM renamed_casted