with pickups as (

    select
        start_station_id as station_id,
        start_station_name as station_name,
        trip_date,
        trip_hour,
        count(*) as pickups
    from {{ ref('stg_cycle_trips') }}
    group by 1, 2, 3, 4

),

returns as (

    select
        end_station_id as station_id,
        end_station_name as station_name,
        trip_date,
        trip_hour,
        count(*) as returns
    from {{ ref('stg_cycle_trips') }}
    where end_station_id is not null
    group by 1, 2, 3, 4

),

combined as (

    select
        coalesce(p.station_id, r.station_id) as station_id,
        coalesce(p.station_name, r.station_name) as station_name,
        coalesce(p.trip_date, r.trip_date) as trip_date,
        coalesce(p.trip_hour, r.trip_hour) as trip_hour,
        coalesce(p.pickups, 0) as pickups,
        coalesce(r.returns, 0) as returns
    from pickups p
    full outer join returns r
        on p.station_id = r.station_id
       and p.trip_date = r.trip_date
       and p.trip_hour = r.trip_hour

)

select
    station_id,
    station_name,
    trip_date,
    trip_hour,
    pickups,
    returns,
    returns - pickups as net_flow,
    pickups + returns as total_movements
from combined
where station_id is not null