    WITH stg_events AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__events')}}
    ),

    dim_sessions_gold AS (
        SELECT
            ev.session_id AS SESSION_ID,
            ev.user_id AS USER_ID,
            MIN(ev.created_at_utc) AS start_time,
            MAX(ev.created_at_utc) AS end_time,
            COUNT(ev.event_id) AS total_events,
            COUNT(CASE WHEN ev.event_type = 'page_view' THEN 1 END) AS page_views,
            COUNT(CASE WHEN ev.event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_events,
            COUNT(CASE WHEN ev.event_type = 'checkout' THEN 1 END) AS checkout_events
        FROM stg_events ev
        GROUP BY session_id, user_id
    )

SELECT * FROM dim_sessions_gold