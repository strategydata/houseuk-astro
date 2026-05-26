{{ config(
    materialized='external',
    format='parquet',
    location="s3://" ~ var('raw_bucket') ~ "/" ~ var('curated_prefix') ~ "/marts/mart_area_rent_stats.parquet"
) }}

with base as (
    select
        try_cast(area_code as varchar) as area_id,
        try_cast(bedrooms as int) as bedrooms,
        date_trunc('month', try_cast(listed_date as date)) as month,
        try_cast(rent_pcm as double) as rent_pcm
    from {{ ref('stg_rightmove_rent') }}
    where rent_pcm is not null
)
select
    area_id,
    bedrooms,
    month,
    quantile_cont(rent_pcm, 0.25) as p25_rent,
    quantile_cont(rent_pcm, 0.5) as median_rent,
    quantile_cont(rent_pcm, 0.75) as p75_rent,
    avg(rent_pcm) as mean_rent,
    count(*) as listing_count
from base
group by area_id, bedrooms, month
