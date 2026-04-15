select *
from {{ ref('stg_cycle_trips') }}
where ended_at < started_at