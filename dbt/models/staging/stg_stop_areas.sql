select 
  area_id,
  name as area_name,
  lon as area_longitude, 
  lat as area_latitude
from {{ source('navitia_structured', 'stop_areas') }}

