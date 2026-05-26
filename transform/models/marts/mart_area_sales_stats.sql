{{ config(
    materialized='external',
    format='parquet',
    location="s3://" ~ var('raw_bucket') ~ "/" ~ var('curated_prefix') ~ "/marts/mart_area_sales_stats.parquet"
) }}

with base as (
    select
        district as area_id,
        date_trunc('month', date_of_transfer) as month,
        price
    from {{ ref('stg_land_registry') }}
    where price is not null
)
select
    area_id,
    month,
    quantile_cont(price, 0.25) as p25_price,
    quantile_cont(price, 0.5) as median_price,
    quantile_cont(price, 0.75) as p75_price,
    avg(price) as mean_price,
    count(*) as sale_count
from base
group by area_id, month
