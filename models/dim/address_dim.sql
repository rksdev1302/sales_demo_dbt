{{ config(
    materialized='table',
    unique_key='address_id',
    name='address_dim'
) }}


select
ship_city,ship_state,ship_postal_code,ship_country,
row_number() over ( order by ship_city asc,ship_state asc,ship_postal_code asc) as address_id
from (
	    SELECT
        distinct
        ship_city,
        ship_state,
        ship_postal_code,
        ship_country
        from {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }}
     )r


