select *
from {{ ref('stg_cycle_trips') }}
where trip_duration_minutes < 0