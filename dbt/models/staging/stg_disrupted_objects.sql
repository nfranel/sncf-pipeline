select 
  disruption_id,
  object_id,
  embedded_type,
  physical_mode
from {{ source('navitia_structured', 'disrupted_objects') }}
