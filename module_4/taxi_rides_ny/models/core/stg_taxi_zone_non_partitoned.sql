-- generating model for source('staging', 'taxi_zone_non_partitoned')...

with zone_data as
(
    select *
    from {{ source("staging", "taxi_zone_non_partitoned") }}
)
select *
from zone_data