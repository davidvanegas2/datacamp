with 

source as (

    select * from {{ source('staging', 'taxi_zone_non_partitoned') }}

),

renamed as (

    select
        locationid,
        borough,
        zone,
        service_zone

    from source

)

select * from renamed
