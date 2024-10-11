{{ config(
    materialized='incremental',
    unique_key='id',
    name='address_dim'
) }}


    SELECT
        id,
        order_id,
        ship_city,
        ship_state,
        ship_postal_code,
        ship_country
    FROM {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }}
    {% if is_incremental() %}
      WHERE id > (SELECT MAX(id) FROM {{ this }})
    {% endif %}
