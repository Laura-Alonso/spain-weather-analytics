with source as (

    select * from {{ source('weather', 'dim_city') }}

),

staged as (

    select
        city_id,
        city_name,
        latitude,
        longitude,
        country_id

    from source

)

select * from staged