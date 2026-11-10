select 
  disruption_id,
  station_id
from {{ source('navitia_structured', 'disrupted_stations') }}