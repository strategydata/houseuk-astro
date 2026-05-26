{{ config(
    materialized='external',
    format='parquet',
    location="s3://" ~ var('raw_bucket') ~ "/" ~ var('curated_prefix') ~ "/marts/mart_rightmove_normalized.parquet"
) }}

with listings as (
    select
        try_cast(listing_id as varchar) as listing_id,
        try_cast(area_code as varchar) as area_id,
        try_cast(bedrooms as int) as bedrooms,
        try_cast(price as double) as price,
        try_cast(rent_pcm as double) as rent_pcm,
        try_cast(listed_date as date) as listed_date
    from {{ ref('stg_rightmove_sale') }}
    union all
    select
        try_cast(listing_id as varchar) as listing_id,
        try_cast(area_code as varchar) as area_id,
        try_cast(bedrooms as int) as bedrooms,
        try_cast(price as double) as price,
        try_cast(rent_pcm as double) as rent_pcm,
        try_cast(listed_date as date) as listed_date
    from {{ ref('stg_rightmove_rent') }}
),
rent_stats as (
    select * from {{ ref('mart_area_rent_stats') }}
),
sale_stats as (
    select
        area_id,
        month,
        p25_price,
        median_price,
        p75_price
    from {{ ref('mart_area_sales_stats') }}
),
sale_stats_latest as (
    select *
    from (
        select
            *,
            row_number() over (partition by area_id order by month desc) as rn
        from sale_stats
    ) ranked
    where rn = 1
)
select
    listings.*,
    rent_stats.median_rent,
    rent_stats.p25_rent,
    rent_stats.p75_rent,
    sale_stats_latest.median_price,
    sale_stats_latest.p25_price,
    sale_stats_latest.p75_price,
    case
        when rent_stats.p25_rent is not null and listings.rent_pcm < rent_stats.p25_rent then true
        else false
    end as cheap_rent_flag,
    case
        when sale_stats_latest.p25_price is not null and listings.price < sale_stats_latest.p25_price then true
        else false
    end as cheap_sale_flag
from listings
left join rent_stats
    on listings.area_id = rent_stats.area_id
    and listings.bedrooms = rent_stats.bedrooms
left join sale_stats_latest
    on listings.area_id = sale_stats_latest.area_id
