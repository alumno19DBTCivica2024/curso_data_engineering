/*
Modelo: session_user_details
Propósito:
  Este modelo combina datos de sesiones y usuarios para proporcionar un resumen detallado de cada sesión. 
  Incluye información del usuario, detalles de inicio y fin de la sesión, duración en minutos, y métricas clave 
  sobre eventos como vistas de página, productos añadidos al carrito, procesos de checkout y envíos completados.

Contexto:
  - Este modelo está diseñado específicamente para el equipo de producto.
  - Permite analizar el comportamiento de los usuarios durante las sesiones, ayudando a identificar patrones 
    de interacción y áreas de mejora en la experiencia del usuario.

Características principales:
  - Proporciona información detallada por sesión y usuario, como:
    - Nombre y correo electrónico del usuario.
    - Hora del primer y último evento de la sesión.
    - Tiempo total de duración de la sesión (en minutos).
    - Totales de eventos: page_view, add_to_cart, checkout, package_shipped.
*/

WITH stg_sessions AS (
    SELECT *
    FROM {{ ref('dim_sessions') }}  -- Dimensión de sesiones
),

stg_users AS (
    SELECT *
    FROM {{ ref('dim_users') }}  -- Dimensión de usuarios
),
    session_user_details AS (
        SELECT 
            s.session_id,
            s.user_id,
            u.first_name,
            u.email,
            s.start_time AS first_event_time_utc,
            s.end_time AS last_event_time_utc,
            TIMESTAMPDIFF('MINUTE', s.start_time, s.end_time) AS session_length_minutes,
            s.page_views,
            s.add_to_cart_events AS add_to_cart,
            s.checkout_events AS checkout,
            s.package_shipped_events AS package_shipped
        FROM 
            stg_sessions s 
        LEFT JOIN 
            stg_users u ON s.user_id = u.user_id 
    )
    SELECT * FROM session_user_details