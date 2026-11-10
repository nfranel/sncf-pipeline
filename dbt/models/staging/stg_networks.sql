select 
  network_id,
  network_name
from {{ source('navitia_structured', 'networks') }}