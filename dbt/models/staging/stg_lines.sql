select 
  line_id,
  line_name
from {{ source('navitia_structured', 'lines') }}