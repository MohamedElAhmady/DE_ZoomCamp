{{ config(materialized='table') }}

with cleaned_fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
    where dropoff_locationid is not null and pickup_locationid is not null
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),
fact_table as (
    select 
        trip.*,
        zones.borough as pickup_borough,
        zones.zone as pickup_zone,
        replace(zones.service_zone, 'Boro', 'Green') as pickup_service_zone,
        zones_borough_dropoff.borough as dropoff_borough,
        zones_borough_dropoff.zone as dropoff_zone,
        replace(zones_borough_dropoff.service_zone, 'Boro', 'Green') as dropoff_service_zone
    from cleaned_fhv_tripdata trip
    inner join dim_zones zones
        on trip.pickup_locationid = zones.locationid
    inner join dim_zones zones_borough_dropoff
        on trip.dropoff_locationid = zones_borough_dropoff.locationid
)

select *
from fact_table
