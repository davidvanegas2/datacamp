{{ config(materialized="table") }}

with
    fhv_data as (
        select *
        from {{ ref("stg_staging__fhv_data_non_partitoned") }}
        where pickup_locationid is not null and dropoff_locationid is not null
    ),
    dim_zones as (
        select *
        from {{ ref("stg_staging__taxi_zone_non_partitoned") }}
        where borough != 'Unknown'
    )
select
    fhv_data.tripid,
    extract(month from pickup_datetime) as month,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
from fhv_data
inner join
    dim_zones as pickup_zone on fhv_data.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone on fhv_data.dropoff_locationid = dropoff_zone.locationid
