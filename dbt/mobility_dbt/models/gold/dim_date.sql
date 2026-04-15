select distinct
    {{ dbt_utils.generate_surrogate_key(['trip_date']) }} as date_key,
    trip_date as date_day,
    year,
    month,
    week_of_year,
    day_of_week,
    case
        when day_of_week = 0 then 'Sunday'
        when day_of_week = 1 then 'Monday'
        when day_of_week = 2 then 'Tuesday'
        when day_of_week = 3 then 'Wednesday'
        when day_of_week = 4 then 'Thursday'
        when day_of_week = 5 then 'Friday'
        when day_of_week = 6 then 'Saturday'
    end as day_name,
    case
        when day_of_week in (0, 6) then true
        else false
    end as is_weekend
from {{ ref('stg_cycle_trips') }}
where trip_date is not null