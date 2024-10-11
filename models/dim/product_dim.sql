{{ config(
    materialized='table',
    unique_key='product_id',
    name='product_dim'
) }}


    select SKU,
        ASIN,
        Style,
        Category,
        Size,
        row_number() over ( order by SKU asc,ASIN asc) as product_id
        from (
    SELECT
        distinct
        SKU,
        ASIN,
        Style,
        Category,
        Size
        from {{ source(var('schema_name_inc'), var('sales_data_clean_inc')) }}
        )t