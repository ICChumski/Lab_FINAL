select
    trip_id,
    cast(start_date as timestamp) as started_at,
    start_station_name,
    cast(start_station_id as integer) as start_station_id,

    cast(end_date as timestamp) as ended_at,
    end_station_name,
    cast(end_station_id as integer) as end_station_id,

    cast(bike_id as integer) as bike_id,
    bike_model,

    total_duration,
    cast(total_duration_ms as numeric) as total_duration_ms,

    source_file,

    cast(total_duration_ms as numeric) / 1000.0 as trip_duration_seconds,
    cast(total_duration_ms as numeric) / 60000.0 as trip_duration_minutes,

    cast(start_date as date) as trip_date,
    extract(hour from cast(start_date as timestamp)) as trip_hour,
    extract(dow from cast(start_date as timestamp)) as day_of_week,
    extract(week from cast(start_date as timestamp)) as week_of_year,
    extract(month from cast(start_date as timestamp)) as month,
    extract(year from cast(start_date as timestamp)) as year,

    case
        when start_station_id = end_station_id then true
        else false
    end as is_round_trip

from raw.raw_cycle_trips
where start_date is not null
  and (
        end_date is null
        or cast(end_date as timestamp) >= cast(start_date as timestamp)
      )