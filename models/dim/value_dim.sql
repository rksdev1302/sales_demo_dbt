{{ config(
    materialized='incremental',
    unique_key='id',
    name='value_dim'
) }}


    SELECT
        a.id,
        a.order_id,
        a.order_date,
        a.Status,
        a.Qty,
        a.currency,
        a.Amount,
        b.SKU,
        b.ASIN
    FROM {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }} a
    left join {{ ref('product_dim') }} b
    on a.id=b.id
    {% if is_incremental() %}
      WHERE a.id > (SELECT MAX(id) FROM {{ this }})
    {% endif %}
