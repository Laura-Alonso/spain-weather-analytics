{{ config(
    materialized='incremental',
    engine = 'MergeTree()',
    unique_key=['city', 'observation_timestamp'],  
    incremental_strategy='delete+insert',
    post_hook="{{ apply_column_comments() }}",
    partition_by='toYYYYMM(observation_timestamp)', 
    order_by='(city, observation_timestamp)' 
) }}

with observations as (
    
    select * from {{ ref('int_weather_observations') }}
    {% if is_incremental() %}
    where timestamp >= toStartOfDay(now() - interval 2 day)
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
      o.timestamp as observation_timestamp, 
      ci.city_name as city,
      ci.longitude as longitude,
      ci.latitude as latitude,
      co.country_name as country,
      o.temperature as temperature,
      o.precipitation as precipitation,
      o.wind_speed as wind_speed,
      o.wind_gusts as wind_gusts,
      o.humidity as humidity,
      o.pressure as pressure,
      cityHash64(ci.city_name, toString(o.timestamp)) as tech_key,
      now() as update_ts

    from observations as o
    left join cities as ci
     on o.city_id = ci.city_id
    left join country as co
     on ci.country_id = co.country_id

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
 and agg.observation_timestamp = existing.observation_timestamp
{% endif %}