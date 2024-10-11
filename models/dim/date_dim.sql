{{ config(
    materialized='table',
    unique_key='date_id',
    name='date_dim'
) }}

WITH date_data AS (
    SELECT
        DISTINCT order_date,
                DATE(order_date) AS date_only,
                EXTRACT(YEAR FROM order_date) AS year,
                EXTRACT(MONTH FROM order_date) AS month,
                EXTRACT(DAY FROM order_date) AS day,
                dayofweek(order_date) AS day_of_week,
                dayname(order_date) AS day_name_of_week,
                CASE
                    WHEN dayofweek(order_date) IN (1, 7) THEN 'Weekend'
                    ELSE 'Weekday'
                END AS weekend_or_weekday
    FROM {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }}
    {% if is_incremental() %}
      WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY order_date) AS date_id,
    order_date,
    date_only,
    year,
    month,
    day,
    day_of_week,
    day_name_of_week,
    weekend_or_weekday
FROM date_data