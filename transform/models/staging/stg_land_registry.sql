{% set bucket = var('raw_bucket') %}
{% set monthly_key = var('land_registry_monthly_key') %}

with raw as (
    select * from read_csv_auto(
        's3://{{ bucket }}/{{ monthly_key }}',
        header=false
    )
)
select
    column0 as transaction_unique_identifier,
    try_cast(column1 as bigint) as price,
    try_cast(column2 as date) as date_of_transfer,
    column3 as postcode,
    column4 as property_type,
    column5 as old_new,
    column6 as duration,
    column7 as paon,
    column8 as saon,
    column9 as street,
    column10 as locality,
    column11 as town_city,
    column12 as district,
    column13 as county,
    column14 as ppd_category_type,
    column15 as record_status
from raw
