select 
  id as disruption_id,
  status,
  severity
from {{ source('navitia_structured', 'disruptions') }}