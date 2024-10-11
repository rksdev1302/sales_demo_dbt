{{ config(
    materialized='table',
    unique_key='id',
    name='sales_fact'
) }}


    SELECT
        a.id,
        f.order_id,
        b.shipment_status_id,
        c.product_id,
        d.address_id,
        e.date_id
    FROM {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }} a
    left join {{ source(var('schema_name_dim'), var('shipment_dim')) }} b on
            a.Status=b.Status and
            a.Courier_Status=b.Courier_Status and
            a.Fulfilment=b.Fulfilment and
            a.Sales_Channel= b.Sales_Channel and
            a.ship_service_level=b.ship_service_level and
            a.fulfilled_by=b.fulfilled_by
    left join {{ source(var('schema_name_dim'), var('product_dim')) }} c on
    a.SKU=c.SKU and a.ASIN=c.ASIN
    left join {{ source(var('schema_name_dim'), var('address_dim')) }} d on
                a.ship_city=d.ship_city and
                a.ship_state=d.ship_state and
                a.ship_postal_code=d.ship_postal_code and
                a.ship_country=d.ship_country
    left join {{ source(var('schema_name_dim'), var('date_dim')) }} e on
                DATE(a.order_date)=e.date_only
    left join {{ source(var('schema_name_dim'), var('order_dim')) }} f on
                a.id=f.id