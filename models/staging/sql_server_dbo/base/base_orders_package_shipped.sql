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
        , {{ dbt_utils.generate_surrogate_key(['null']) }} as PRODUCT_ID
        , session_id
        , CONVERT_TIMEZONE('UTC', CREATED_AT) AS created_at_utc
        , order_id
        , UPPER(COALESCE(_FIVETRAN_DELETED, 'false')) as is_deleted
        , _fivetran_synced AS date_load
    FROM src_events
    WHERE EVENT_TYPE = 'package_shipped'
    )

SELECT * FROM renamed_casted