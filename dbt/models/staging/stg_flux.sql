select 
  vehicle_journey_id,
  station_id,
  regexp_replace(regexp_replace(vehicle_journey_id, '^vehicle_journey:', ''), ':RealTime:[0-9a-f-]+$', '') as trip_id,
  origin_id,
  terminus_id,
  line_id,
  network_id,
  physical_mode,
  
  base_time_in as scheduled_arrival_time,
  time_in as actual_arrival_time,
  base_time_out as scheduled_departure_time,
  time_out as actual_departure_time,
  
  extract(epoch from (time_in - base_time_in)) / 60.0 as arrival_delay_min,
  extract(epoch from (time_out - base_time_out)) / 60.0 as departure_delay_min,

  (coalesce(base_time_out, base_time_in, time_out, time_in) at time zone 'Europe/Paris')::date as service_date,
  
  extract(hour from (coalesce(base_time_out, base_time_in, time_out, time_in) at time zone 'Europe/Paris')) as service_hour,
  extract(isodow from (coalesce(base_time_out, base_time_in, time_out, time_in) AT TIME ZONE 'Europe/Paris')) as service_week_day,

  created_at, 
  updated_at
from {{ source('navitia_structured', 'flux') }}