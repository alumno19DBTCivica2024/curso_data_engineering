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
    {% if is_incremental() %}
	  WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )
    {% endif %}
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
        , null as order_id
        , UPPER(COALESCE(_FIVETRAN_DELETED, 'false')) as is_deleted
        , _fivetran_synced AS date_load
    FROM src_events
    WHERE EVENT_TYPE = 'page_view'
    )

SELECT * FROM renamed_casted