select
    trip_id,

    {{ dbt_utils.generate_surrogate_key(['trip_id']) }} as trip_key,
    {{ dbt_utils.generate_surrogate_key(['start_station_id', 'start_station_name']) }} as start_station_key,
    {{ dbt_utils.generate_surrogate_key(['end_station_id', 'end_station_name']) }} as end_station_key,
    {{ dbt_utils.generate_surrogate_key(['trip_date']) }} as date_key,

    start_station_id,
    end_station_id,

    started_at,
    ended_at,

    trip_duration_minutes,
    trip_duration_seconds,
    {{ duration_bucket('trip_duration_minutes') }} as duration_bucket,

    trip_date,
    trip_hour,
    day_of_week,
    week_of_year,
    month,
    year,

    bike_id,
    bike_model,

    is_round_trip

from {{ ref('stg_cycle_trips') }}