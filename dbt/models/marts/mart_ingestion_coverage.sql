select service_date,
  count(*)                                    as nb_passages,
  sum(number_versions)                        as sum_number_versions,
  count(*) filter (where number_versions = 1) as nb_single_version,
  count(*) filter (where number_versions > 1) as nb_multiple_versions,
  count(distinct trip_id)                     as nb_unique_trip,
  count(distinct station_id)                  as nb_unique_station, 
  count(distinct area_id)                     as nb_unique_area,
  count(distinct line_id)                     as nb_unique_line,
  count(distinct network_id)                  as nb_unique_network,
  count(*) filter (where has_realtime_update) as nb_passages_realtime,
  count(*) filter (where scheduled_departure_time is null and scheduled_arrival_time is null) as nb_without_scheduled_time
from {{ ref('int_flux_flagged') }}
group by 1