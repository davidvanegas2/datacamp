
select *
from {{ ref('stg_taxi_zone_non_partitoned') }}
