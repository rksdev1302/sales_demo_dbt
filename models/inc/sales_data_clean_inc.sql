{{ config(
    materialized='incremental',
    unique_key='id',
    name='sales_data_clean_inc'
) }}


         SELECT
                COALESCE(CAST(`index` AS UNSIGNED INTEGER), 0) AS ID,
                LOWER(COALESCE(Order_ID,'NA')) AS Order_ID,
                COALESCE(STR_TO_DATE(Date, '%m-%d-%y'), '1970-01-01') AS order_date,
                LOWER(COALESCE(Status, 'unknown')) AS Status,
                LOWER(COALESCE(Courier_Status, 'unknown')) AS Courier_Status,
                LOWER(COALESCE(Fulfilment, 'unknown')) AS Fulfilment,
                LOWER(COALESCE(Sales_Channel, 'unknown')) AS Sales_Channel,
                LOWER(COALESCE(ship_service_level, 'unknown')) AS ship_service_level,
                LOWER(COALESCE(Style, 'unknown')) AS Style,
                LOWER(COALESCE(SKU, 'unknown')) AS SKU,
                LOWER(COALESCE(Category, 'unknown')) AS Category,
                LOWER(COALESCE(Size, 'unknown')) AS Size,
                LOWER(COALESCE(ASIN, 'unknown')) AS ASIN,
                COALESCE(CAST(Qty AS UNSIGNED INTEGER), 0) AS Qty,
                LOWER(COALESCE(currency, 'unknown')) AS currency,
                COALESCE(CAST(Amount AS DECIMAL(10, 2)), 0.0) AS Amount,
                LOWER(COALESCE(ship_city, 'unknown')) AS ship_city,
                LOWER(COALESCE(ship_state, 'unknown')) AS ship_state,
                COALESCE(CAST(ship_postal_code AS CHAR(6)), '000000') AS ship_postal_code,
                LOWER(COALESCE(ship_country, 'unknown')) AS ship_country,
                LOWER(COALESCE(promotion_ids, 'none')) AS promotion_ids,
                COALESCE(CASE WHEN B2B = 1 THEN TRUE ELSE FALSE END, FALSE) AS B2B,
                LOWER(COALESCE(fulfilled_by, 'unknown')) AS fulfilled_by
    FROM {{ source(var('schema_name_raw'), var('sales_raw')) }}
    {% if is_incremental() %}
      WHERE CAST(`index` AS SIGNED INTEGER) > (SELECT MAX(ID) FROM {{ this }})
    {% endif %}
