with agg as (
  select
    trip_id, station_id,
    count(distinct disruption_id) filter (where is_effective)     as nb_disruptions,
    coalesce(bool_or(is_effective), false)                        as is_disrupted,
    coalesce(bool_or(is_canceled), false)                         as is_canceled,
    coalesce(bool_or(is_effective and is_modified), false)        as is_modified,
    coalesce(bool_or(is_additional), false)                       as is_additional,
    coalesce(bool_or(is_effective and is_detour), false)          as is_detour,
    coalesce(bool_or(is_effective and is_reduced_service), false) as is_reduced_service,
    coalesce(bool_or(is_effective and is_delayed), false)         as is_delayed,
    coalesce(bool_or(is_effective and embedded_type = 'trip'), false)       as has_trip_disruption,
    coalesce(bool_or(is_effective and embedded_type = 'line'), false)       as has_line_disruption,
    coalesce(bool_or(is_effective and embedded_type = 'network'), false)    as has_network_disruption,
    coalesce(bool_or(is_effective and embedded_type = 'stop_point'), false) as has_station_disruption,
    coalesce(bool_or(is_effective and embedded_type = 'stop_area'), false)  as has_area_disruption
  from {{ ref('int_flux_disruptions') }}
  group by trip_id, station_id
)

select
  f.*,
  coalesce(a.nb_disruptions, 0)              as nb_disruptions,
  coalesce(a.is_disrupted, false)            as is_disrupted,
  coalesce(a.is_canceled, false)             as is_canceled,
  coalesce(a.is_modified, false)             as is_modified,
  coalesce(a.is_additional, false)           as is_additional,
  coalesce(a.is_detour, false)               as is_detour,
  coalesce(a.is_reduced_service, false)      as is_reduced_service,
  coalesce(a.is_delayed, false)              as is_delayed,
  coalesce(a.has_trip_disruption, false)     as has_trip_disruption,
  coalesce(a.has_line_disruption, false)     as has_line_disruption,
  coalesce(a.has_network_disruption, false)  as has_network_disruption,
  coalesce(a.has_station_disruption, false)  as has_station_disruption,
  coalesce(a.has_area_disruption, false)     as has_area_disruption
from {{ ref('int_flux_deduplicated') }} as f
left join agg as a using (trip_id, station_id)