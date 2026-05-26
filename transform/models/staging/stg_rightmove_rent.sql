{% set bucket = var('raw_bucket') %}
{% set key = var('rightmove_rent_key') %}

select *
from read_json_auto('s3://{{ bucket }}/{{ key }}')
