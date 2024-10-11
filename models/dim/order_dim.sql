{{ config(
    materialized='incremental',
    unique_key='id',
    name='order_dim'
) }}


    SELECT
        id,
        order_id,
        order_date,
        Status,
        promotion_ids,
        B2B,
        Qty,
        currency,
        Amount
    FROM {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }}
    {% if is_incremental() %}
      WHERE id > (SELECT MAX(id) FROM {{ this }})
    {% endif %}
