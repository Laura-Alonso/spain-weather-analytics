{{ config(
    materialized='incremental',
    unique_key=['city_id', 'timestamp'],
    incremental_strategy='delete+insert'
) }}

with weather_hourly as (

    select * from {{ ref('stg_raw_weather_hourly') }}

),

weather_2years as(

    select * from {{ ref('stg_raw_weather_2years') }}
),

unioned as (

    select * from weather_hourly
    union all
    select * from weather_2years

),

weather_observations as (
    select * except(rn),
        now() as update_ts
    from (
        select *,
            row_number() over (
                partition by city_id, timestamp 
                order by ingestion_ts desc
            ) as rn
        from unioned
        {% if is_incremental() %}
        where timestamp >= now() - interval 2 day
        {% endif %}        
    )
    where rn = 1

)

select *
from weather_observations
