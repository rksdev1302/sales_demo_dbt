{{ config(
    materialized='table',
    unique_key='shipment_status_id',
    name='shipment_dim'
) }}

select         Status,
               Courier_Status,
               Fulfilment,
               Sales_Channel,
               ship_service_level,
               fulfilled_by,
               row_number() over ( order by Status asc,Courier_Status asc,Fulfilment asc,
               Sales_Channel,ship_service_level,fulfilled_by) as shipment_status_id
               from (
    SELECT
        distinct
        Status,
        Courier_Status,
        Fulfilment,
        Sales_Channel,
        ship_service_level,
        fulfilled_by

    FROM {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }}
    ) d