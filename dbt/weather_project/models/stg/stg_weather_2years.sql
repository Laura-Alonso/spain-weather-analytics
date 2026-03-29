{{ config(
    materialized='table',
    engine='MergeTree()',
    post_hook="{{ apply_column_comments() }}",
    partition_by='toYYYYMM(timestamp)',
    order_by='(city_id, timestamp)'
) }}


with source as (

    select * from {{ source('weather', 'raw_weather_2years') }}

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
        cityHash64(toString(city_id), toString(timestamp)) as tech_key,
        ingestion_ts

    from source

)

select * from staged