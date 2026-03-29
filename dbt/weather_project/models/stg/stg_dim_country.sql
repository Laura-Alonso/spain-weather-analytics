{{ config(
    post_hook="{{ apply_column_comments() }}",
) }}

with source as (

    select * from {{ source('weather', 'dim_country') }}

),

staged as (

    select
        country_id,
        country_name

    from source

)

select * from staged