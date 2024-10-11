{{ config(
    materialized='table',
    name='daily_sales_fact',
    unique_key='order_id'
) }}

WITH sales_data AS (
    SELECT
        v.order_id,
        v.order_date,
        d.year,
        d.month,
        d.day,
        d.weekend_or_weekday,
        v.amount
        from
        {{ source(var('schema_name_dim'), var('value_dim')) }} v
        LEFT JOIN {{ source(var('schema_name_dim'), var('date_dim')) }}  d ON d.date_only = v.order_date
)

select order_date,weekend_or_weekday,year,month,day,sum(amount) as daily_sales
from sales_data
group by order_date,weekend_or_weekday,year,month,day
order by order_date desc