{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select * from {{ source('staging', 'fhv_tripdata') }}
    where extract(year from pickup_datetime) = 2019
),

renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        cast(pickup_datetime as timestamp) as pickup_datetime_converted,
        cast(dropoff_datetime as timestamp) as dropoff_datetime_converted,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed
