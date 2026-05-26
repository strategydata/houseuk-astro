{% set bucket = var('raw_bucket') %}
{% set prefix = var('raw_prefix') %}
{% set cities = var('airbnb_cities') %}

with unioned as (
{% for city in cities %}
    select
        '{{ city }}' as city,
        *
    from read_csv_auto(
        's3://{{ bucket }}/{{ prefix }}/airbnb/{{ city }}/latest/listings.csv.gz',
        header=true,
        compression='gzip'
    )
    {% if not loop.last %}union all{% endif %}
{% endfor %}
)
select
    city,
    try_cast(id as bigint) as listing_id,
    try_cast(latitude as double) as latitude,
    try_cast(longitude as double) as longitude,
    try_cast(bedrooms as int) as bedrooms,
    try_cast(accommodates as int) as accommodates,
    room_type,
    try_cast(minimum_nights as int) as minimum_nights,
    try_cast(maximum_nights as int) as maximum_nights,
    try_cast(availability_365 as int) as availability_365,
    try_cast(regexp_replace(price, '[$,]', '', 'g') as double) as price_per_night,
    last_scraped
from unioned
