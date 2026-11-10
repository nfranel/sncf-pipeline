with matches as (
  select f.trip_id, f.station_id, f.passage_time, o.disruption_id, o.embedded_type
  from {{ ref('int_flux_deduplicated') }} f
  join {{ ref('stg_disrupted_objects') }} o
    on o.embedded_type = 'trip' and o.object_id = f.trip_id

  union all
  select f.trip_id, f.station_id, f.passage_time, o.disruption_id, o.embedded_type
  from {{ ref('int_flux_deduplicated') }} f
  join {{ ref('stg_disrupted_objects') }} o
    on o.embedded_type = 'line' and o.object_id = f.line_id

  union all
  select f.trip_id, f.station_id, f.passage_time, o.disruption_id, o.embedded_type
  from {{ ref('int_flux_deduplicated') }} f
  join {{ ref('stg_disrupted_objects') }} o
    on o.embedded_type = 'network' and o.object_id = f.network_id

  union all
  select f.trip_id, f.station_id, f.passage_time, o.disruption_id, o.embedded_type
  from {{ ref('int_flux_deduplicated') }} f
  join {{ ref('stg_disrupted_objects') }} o
    on o.embedded_type = 'stop_point' and o.object_id = f.station_id

  union all
  select f.trip_id, f.station_id, f.passage_time, o.disruption_id, o.embedded_type
  from {{ ref('int_flux_deduplicated') }} f
  join {{ ref('stg_disrupted_objects') }} o
    on o.embedded_type = 'stop_area' and o.object_id = f.area_id
),

flagged as (
  select
    m.trip_id, m.station_id, m.disruption_id, m.embedded_type,
    d.status, d.severity,
    (d.severity = 'trip canceled') as is_canceled,
    (d.severity = 'trip modified') as is_modified,
    (d.severity = 'additional service') as is_additional,
    (d.severity = 'detour') as is_detour,
    (d.severity = 'reduced service') as is_reduced_service,
    (d.severity = 'trip delayed') as is_delayed,    
    exists (
      select 1 from {{ ref('stg_disrupted_stations') }} as ds
      where ds.disruption_id = m.disruption_id and ds.station_id = m.station_id
    ) as is_station_declared,
    exists (
      select 1 from {{ ref('stg_disruption_periods') }} as p
        where p.disruption_id = m.disruption_id
          and m.passage_time between p.period_start and p.period_end
    ) as is_within_period
  from matches m
  join {{ ref('stg_disruptions') }} d using (disruption_id)
)

select
  *,
  (
    severity <> 'additional service'
    and (is_canceled
      or (is_within_period and (embedded_type <> 'trip' or is_station_declared)))
    ) as is_effective
from flagged