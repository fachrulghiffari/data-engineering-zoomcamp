{{
    config(
        materialized='table'
    )
}}

with 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv.tripid,
    dispatching_base_num,
    fhv.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv.pickup_datetime, 
    fhv.dropoff_datetime,
    sr_flag,
    affiliated_base_number

from {{ ref('stg_fhv_tripdata') }} as fhv
inner join dim_zones as pickup_zone
on fhv.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.dropoff_locationid = dropoff_zone.locationid