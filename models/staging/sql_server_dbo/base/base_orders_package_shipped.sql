{{
  config(
    materialized='incremental',
    unique_key = 'event_id',
    on_schema_change='fail'
  )
}}

WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    WHERE EVENT_TYPE = 'package_shipped'
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
    {% if is_incremental() %}
        WHERE DATE_LOAD > (   SELECT MAX(DATE_LOAD) FROM {{ this }})
    {% endif %}     
    )

SELECT * FROM renamed_casted