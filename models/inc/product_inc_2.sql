{{ config(
    materialized='incremental',
    unique_key='SKU||ASIN',
    name='product_inc_2'
) }}

    {% if is_incremental() %}
        with max_row_num as (
            select max(product_id) as max_rn from {{this}}
        )
    {% endif %}


select SKU,ASIN,Style,Category,Size,
    ROW_NUMBER() OVER (ORDER BY SKU asc,ASIN asc)
    {% if is_incremental() %}
        + (SELECT max_rn FROM max_row_num)
    {% endif %}
               AS product_id
    from
    (
    SELECT
        distinct
        a.SKU,
        a.ASIN,
        a.Style,
        a.Category,
        a.Size
    FROM {{ ref('product_inc') }} a
    {% if is_incremental() %}
      left join {{ this }} b on a.SKU=b.SKU and a.ASIN=b.ASIN
      where (b.SKU is null and b.ASIN is null)
    {% endif %}
    ) data
