{{ config(
    materialized='incremental',
    unique_key='id',
    name='product_inc'
) }}


    SELECT
        `index` as id,
        SKU,
        ASIN,
        Style,
        Category,
        Size
    FROM {{ source(var('schema_name'), var('raw_sales')) }}
    {% if is_incremental() %}
      WHERE `index` > (SELECT MAX(id) FROM {{ this }})
    {% endif %}
