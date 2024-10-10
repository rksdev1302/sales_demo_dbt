{{ config(
    materialized='incremental',
    unique_key='id',
    name='product_dim'
) }}


    SELECT
        `index` as id,
        a.order_id,
        b.product_id
    FROM {{ source(var('schema_name'), var('raw_sales')) }} a
    LEFT JOIN {{ ref('product_inc_2') }} b
    ON
    a.SKU=b.SKU and a.ASIN=b.ASIN
    {% if is_incremental() %}
      WHERE `index` > (SELECT MAX(id) FROM {{ this }})
    {% endif %}
