{{ config(
    materialized='table',
    name='city_sales_fact',
    sort='ship_city'
) }}


select
  ship_city,
  ship_state,
  Category,
  style,
  sales,
  dense_rank() over (
    partition by ship_city
    order by
      sales desc
  ) as rnk
from
  (
    select
      ship_city,
      ship_state,
      Category,
      style,
      sum(amount) as sales
    from
      (
        select b.ship_city,b.ship_state,c.status,c.amount,d.category,d.style
        from  {{ ref('sales_fact')}} a
        left join {{ source(var('schema_name_dim'), var('address_dim')) }} b
            on a.address_id=b.address_id
        left join {{ source(var('schema_name_dim'), var('order_dim')) }} c
            on a.id=c.id
        left join {{ source(var('schema_name_dim'), var('product_dim')) }} d
            on a.product_id=d.product_id
        where
          (
            c.status != 'cancelled'
            and c.status not like '%returned%'
          )
      ) tbl
    group by
      ship_city,
      ship_state,
      Category,
      style
  ) data
order by
  ship_city,
  rnk asc
