{{ config(
    materialized='incremental',
    unique_key='id',
    name='shipment_dim'
) }}


    SELECT
        id,
        order_id,
        order_date,
        Status,
        Courier_Status,
        Fulfilment,
        Sales_Channel,
        ship_service_level,
        fulfilled_by
    FROM {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }}
    {% if is_incremental() %}
      WHERE id > (SELECT MAX(id) FROM {{ this }})
    {% endif %}
