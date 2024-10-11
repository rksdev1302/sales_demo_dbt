{{ config(
    materialized='incremental',
    unique_key='id',
    name='product_dim'
) }}


    SELECT
        id,
        order_id,
        SKU,
        ASIN,
        Style,
        Category,
        Size
    FROM {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }}
    {% if is_incremental() %}
      WHERE id > (SELECT MAX(id) FROM {{ this }})
    {% endif %}
