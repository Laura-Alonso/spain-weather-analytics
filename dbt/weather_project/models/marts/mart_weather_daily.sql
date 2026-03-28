{{ config(
    materialized='incremental',
    unique_key=['city_id', 'date'],
    incremental_strategy='delete+insert'
) }}

with observations as (
    
    select * from {{ ref('int_weather_observations') }}
    {% if is_incremental() %}
    where timestamp >= toStartOfDay(now() - interval 1 day)
    {% endif %}
),

cities as (

    select * from {{ ref('stg_dim_city') }}

),

country as (

    select * from {{ ref('stg_dim_country') }}

),

aggregated as (

    SELECT
      date(o.timestamp) as date, 
      ci.city_name as city,
      co.country_name as country,
      avg(o.temperature) as avg_temperature,
      min(o.temperature) as min_temperature,
      max(o.temperature) as max_temperature,
      (max(o.temperature) - min(o.temperature)) as thermal_range,
      sum(o.precipitation) as total_precipitation,
      countIf(o.precipitation > 0) as rainy_hours,
      max(o.wind_speed) as max_wind_speed,
      avg(o.wind_speed) as avg_wind_speed,
      max(o.wind_gusts) as max_wind_gusts,
      avg(o.humidity) as avg_humidity,
      avg(o.pressure) as avg_pressure,
      now() as update_ts

    from observations as o
    left join cities as ci
     on o.city_id = ci.city_id
    left join country as co
     on ci.country_id = co.country_id
    group by 1,2,3

)

select
    agg.*,
    {% if is_incremental() %}
    coalesce(existing.ingestion_ts, now()) as ingestion_ts
    {% else %}
    now() as ingestion_ts
    {% endif %}

from aggregated as agg
{% if is_incremental() %}
left join {{ this }} as existing
 on agg.city = existing.city
 and agg.date = existing.date
{% endif %}
