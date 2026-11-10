-- disrup / day / severity / obj type / status

with disruption_days as (
  select f.service_date, d.disruption_id, d.severity, d.embedded_type, d.status,
  bool_or(d.is_effective) as is_effective
  from {{ ref('int_flux_disruptions') }}  as d
  join {{ ref('int_flux_deduplicated') }} as f using (trip_id, station_id)
  group by 1, 2, 3, 4, 5
),

durations as (
  select p.disruption_id, sum(p.period_end - p.period_start) as duration_sum
  from {{ ref('stg_disruption_periods') }} as p
  group by 1
)

select dd.service_date, dd.severity, dd.embedded_type, dd.status,
  count(*)                                  as nb_disruptions,
  count(*) filter (where dd.is_effective)   as nb_effective_disruptions,
  count(du.duration_sum)                        as nb_with_duration,
  sum(extract(epoch from du.duration_sum)) / 60 as sum_duration_min
from disruption_days as dd
left join durations as du using(disruption_id)
group by 1, 2, 3, 4
