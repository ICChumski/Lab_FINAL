select
    day_of_week,
    count(*) as trip_count,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    min(trip_duration_minutes) as min_trip_duration_minutes,
    max(trip_duration_minutes) as max_trip_duration_minutes
from {{ ref('stg_cycle_trips') }}
where trip_duration_minutes is not null
group by day_of_week
order by day_of_week