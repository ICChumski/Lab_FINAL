with pickups as (

    select
        start_station_id as station_id,
        start_station_name as station_name,
        count(*) as total_pickups
    from {{ ref('stg_cycle_trips') }}
    group by 1, 2

),

returns as (

    select
        end_station_id as station_id,
        end_station_name as station_name,
        count(*) as total_returns
    from {{ ref('stg_cycle_trips') }}
    where end_station_id is not null
    group by 1, 2

),

combined as (

    select
        coalesce(p.station_id, r.station_id) as station_id,
        coalesce(p.station_name, r.station_name) as station_name,
        coalesce(p.total_pickups, 0) as total_pickups,
        coalesce(r.total_returns, 0) as total_returns
    from pickups p
    full outer join returns r
        on p.station_id = r.station_id

)

select
    station_id,
    station_name,
    total_pickups,
    total_returns,
    total_pickups + total_returns as total_movements,
    total_returns - total_pickups as net_flow,
    abs(total_pickups - total_returns) as imbalance_absolute,
    case
        when (total_pickups + total_returns) = 0 then 0
        else abs(total_pickups - total_returns)::numeric / (total_pickups + total_returns)
    end as imbalance_ratio
from combined
where station_id is not null