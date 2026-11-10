select 
  station_id,
  station_name, 
  area_id
from {{ source('navitia_structured', 'stations') }}