{{ config(
    materialized='table',
    name='daily_sales_fact',
    sort='order_date'
) }}

WITH sales_data AS (
    select
    a.id,b.date_only as order_date,b.year,b.month,b.day,b.weekend_or_weekday,c.amount
    from {{ ref('sales_fact')}} a
    left join {{ source(var('schema_name_dim'), var('date_dim')) }} b on a.date_id=b.date_id
    left join {{ source(var('schema_name_dim'), var('order_dim')) }} c on a.id=c.id
)

select order_date,weekend_or_weekday,year,month,day,sum(amount) as daily_sales
from sales_data
group by order_date,weekend_or_weekday,year,month,day
order by order_date desc