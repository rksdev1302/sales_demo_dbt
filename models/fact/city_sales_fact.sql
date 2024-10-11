{{ config(
    materialized='table',
    name='city_sales_fact'
) }}


select
  ship_city,
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
      Category,
      style,
      sum(amount) as sales
    from
      (
        select
          a.ship_city,
          b.Category,
          b.style,
          c.amount
        from
          {{ source(
            var('schema_name_dim'),
            var('address_dim')
          ) }} a
          left join {{ source(
            var('schema_name_dim'),
            var('product_dim')
          ) }} b on a.id = b.id
          left join {{ source(
            var('schema_name_dim'),
            var('value_dim')
          ) }} c on a.id = c.id
        where
          (
            c.status != 'cancelled'
            and c.status not like '%Returned to Seller%'
          )
      ) tbl
    group by
      ship_city,
      Category,
      style
  ) data
order by
  ship_city,
  rnk asc
