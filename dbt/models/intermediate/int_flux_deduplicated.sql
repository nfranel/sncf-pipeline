with sorted_flux as (
  select
    f.*, 
    row_number() over (partition by f.trip_id, f.station_id order by (f.vehicle_journey_id like '%:RealTime:%') desc, f.created_at desc, f.vehicle_journey_id desc) as version_rank,
    count(*) over (partition by f.trip_id, f.station_id) as number_versions
    from {{ ref('stg_flux') }} as f
),

flux_latest as (select * from sorted_flux where version_rank = 1)



select 
  f.trip_id,
  f.station_id,
  f.vehicle_journey_id,
  f.number_versions,
  (f.vehicle_journey_id like '%:RealTime:%') as has_realtime_update,

  f.line_id,
  f.network_id,
  f.physical_mode,
  f.service_date,
  f.service_hour,
  f.service_week_day,
  
  coalesce(f.departure_delay_min, f.arrival_delay_min) as delay_min,
  coalesce(f.scheduled_departure_time, f.scheduled_arrival_time, f.actual_departure_time, f.actual_arrival_time) as passage_time,

  f.scheduled_arrival_time,
  f.actual_arrival_time,
  f.scheduled_departure_time,
  f.actual_departure_time,

  s.station_name,
  s.area_id,
  a.area_name,
  l.line_name,
  n.network_name,
  
  f.created_at as collected_at
from flux_latest as f
join {{ ref('stg_stations') }} s using (station_id)
join {{ ref('stg_stop_areas') }} a on a.area_id = s.area_id
join {{ ref('stg_lines') }} l using (line_id)
join {{ ref('stg_networks') }} n using (network_id)