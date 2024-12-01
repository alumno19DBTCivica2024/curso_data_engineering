WITH stg_events AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

event_counts AS (
    SELECT *
    FROM {{ ref('obtener_tipos_eventos_din') }}  -- Llamada al modelo `obtener_tipos_eventos.sql`
),

dim_sessions_gold AS (
    SELECT
        ev.session_id AS SESSION_ID,                      -- ID de la sesión
        ev.user_id AS USER_ID,                            -- ID del usuario asociado
        MIN(ev.created_at_utc) AS start_time,             -- Inicio de la sesión
        MAX(ev.created_at_utc) AS end_time,               -- Fin de la sesión
        TIMESTAMPDIFF('MINUTE', start_time, end_time) AS session_length_minutes, -- Duración en minutos AS session_length_minutes, -- Duración de la sesión
        ec.page_view_amount AS page_views,                -- Conteo dinámico de "page_view"
        ec.add_to_cart_amount AS add_to_cart_events,      -- Conteo dinámico de "add_to_cart"
        ec.checkout_amount AS checkout_events,            -- Conteo dinámico de "checkout"
        ec.package_shipped_amount AS package_shipped_events -- Conteo dinámico de "package_shipped"
    FROM stg_events ev
    LEFT JOIN event_counts ec                              -- Relacionar con los conteos de eventos
    ON ev.user_id = ec.user_id                             -- Relación por usuario
    GROUP BY ev.session_id, ev.user_id, ec.page_view_amount, ec.add_to_cart_amount, ec.checkout_amount, ec.package_shipped_amount
)

SELECT * FROM dim_sessions_gold