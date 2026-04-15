select
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    count(*) as trip_count,
    avg(trip_duration_minutes) as avg_trip_duration_minutes
from {{ ref('stg_cycle_trips') }}
where start_station_id is not null
  and end_station_id is not null
group by
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name