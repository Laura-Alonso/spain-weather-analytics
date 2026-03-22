with source as (

    select * from {{ source('weather', 'raw_weather_hourly') }}

),

staged as (

    select
        timestamp,
        city_id,
        temperature,
        wind_speed,
        wind_gusts,
        precipitation,
        humidity,
        pressure,
        ingestion_ts

    from source

)

select * from staged