-- flux / service_date, area_id, station_id, physical_mode, network_id, line_id
-- The lines are also grouped by area_name, station_name, line_name, network_name but it doesn't change anything as the relation between area_name, station_name, line_name, network_name and area_id, station_id, line_id, network_id is a 1:1

select service_date, area_id, station_id, physical_mode, network_id, line_id,
  area_name, station_name, line_name, network_name,
  sum(nb_disruptions)                         as nb_passage_disruptions_pairs,
  count(*)                                    as nb_passages,
  count(*) filter (where is_disrupted)        as nb_passages_disrupted,
  count(*) filter (where is_canceled)         as nb_passages_canceled,
  count(*) filter (where is_modified)         as nb_passages_modified,
  count(*) filter (where is_additional)       as nb_passages_additional,
  count(*) filter (where is_detour)           as nb_passages_detour,
  count(*) filter (where is_reduced_service)  as nb_passages_reduced_service,
  count(*) filter (where is_delayed)          as nb_passages_delayed,
  count(*) filter (where has_realtime_update) as nb_passages_realtime,  
  count(*) filter (where delay_min > 5 and not is_canceled)      as nb_delay_over_5min,
  count(*) filter (where delay_min > 10 and not is_canceled)     as nb_delay_over_10min,
  count(*) filter (where delay_min > 30 and not is_canceled)     as nb_delay_over_30min,
  count(*) filter (where delay_min > 60 and not is_canceled)     as nb_delay_over_60min,
  count(*) filter (where delay_min > 120 and not is_canceled)    as nb_delay_over_120min,
  count(delay_min) filter (where not is_canceled)                         as nb_delay_obs,
  sum(delay_min)   filter (where not is_canceled and delay_min > 0)       as sum_late_min,
  count(delay_min) filter (where not is_canceled and is_disrupted)        as nb_delay_disrupted,
  sum(delay_min)   filter (where not is_canceled and is_disrupted and delay_min > 0) as sum_late_min_disrupted,
  count(delay_min) filter (where not is_canceled and has_realtime_update)            as nb_delay_realtime,
  count(*)         filter (where not is_canceled and has_realtime_update and delay_min < 0) as nb_early_realtime,
  count(*)         filter (where not is_canceled and has_realtime_update and delay_min = 0) as nb_on_time_realtime,
  count(*)         filter (where not is_canceled and has_realtime_update and delay_min > 0) as nb_late_realtime,
  sum(delay_min)   filter (where not is_canceled and has_realtime_update and delay_min > 0) as sum_late_min_realtime,
  count(*)         filter (where is_disrupted and has_realtime_update) as nb_passages_disrupted_realtime,
  count(delay_min) filter (where not is_canceled and is_disrupted and has_realtime_update) as nb_delay_obs_disrupted_realtime,
  sum(delay_min)   filter (where not is_canceled and is_disrupted and has_realtime_update and delay_min > 0) as sum_late_min_disrupted_realtime
from {{ ref('int_flux_flagged') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10